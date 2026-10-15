-- Independently scoped source template; no SQL execution.
-- Complete genuine schema and current constructor/ACL closure; root rollback route only.
-- One-time CloudTMS schema/data migration: weekly_source_rule_sets_schema
-- WP-F1: additive admin configuration, immutable versions and a default-off
-- group feature gate. No policy hash, financial owner or existing identity changes.



create table public.weekly_source_rule_sets (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  agency_id uuid not null,
  environment text not null check (environment in ('TEST','LIVE')),
  code text not null check (code ~ '^[A-Z][A-Z0-9_]{1,79}$'),
  display_name text not null check (char_length(btrim(display_name)) between 1 and 160),
  version bigint not null default 1 check (version>=1),
  creation_request_id uuid not null,
  creation_request_sha256 text not null check (creation_request_sha256 ~ '^[0-9a-f]{64}$'),
  created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default transaction_timestamp(),
  updated_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  updated_at_utc timestamptz not null default transaction_timestamp(),
  unique (agency_id,environment,code),
  unique (agency_id,environment,creation_request_id)
);

create table public.weekly_source_rule_set_versions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  rule_set_id uuid not null references public.weekly_source_rule_sets(id) on delete restrict,
  version_number bigint not null check (version_number>=1),
  values_json jsonb not null,
  values_sha256 text not null check (values_sha256 ~ '^[0-9a-f]{64}$'),
  save_request_id uuid not null,
  save_request_sha256 text not null check (save_request_sha256 ~ '^[0-9a-f]{64}$'),
  created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default transaction_timestamp(),
  unique (rule_set_id,version_number),
  unique (rule_set_id,save_request_id),
  check (jsonb_typeof(values_json)='object' and values_json ?& array['checking_profile_kind','self_bill_source','final_report_profile_kind','invoice_membership','pay_membership','invoice_amount_basis','source_amount_basis','line_vat_method','invoice_vat_method','rate_difference_policy','negative_adjustment_policy','lookback_warning','identical_rekey','authorised_late_addition','equal_protected_hours','signed_comparison','unfinalised_action','unfinalised_chase','changed_query_clocks','invoice_sender','earliest_final_weekday','earliest_final_local_time','attach_healthroster_section']::text[] and values_json - array['checking_profile_kind','self_bill_source','final_report_profile_kind','invoice_membership','pay_membership','invoice_amount_basis','source_amount_basis','line_vat_method','invoice_vat_method','rate_difference_policy','negative_adjustment_policy','lookback_warning','identical_rekey','authorised_late_addition','equal_protected_hours','signed_comparison','unfinalised_action','unfinalised_chase','changed_query_clocks','invoice_sender','earliest_final_weekday','earliest_final_local_time','attach_healthroster_section']::text[]='{}'::jsonb),
  check (jsonb_typeof(values_json->'checking_profile_kind')='string' and values_json->>'checking_profile_kind' in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')),
  check (jsonb_typeof(values_json->'self_bill_source')='string' and values_json->>'self_bill_source' in ('SAME_FILE','SEPARATE_FINAL_REPORT')),
  check (jsonb_typeof(values_json->'final_report_profile_kind')='string' and values_json->>'final_report_profile_kind' in ('NONE','ACACIUM_SELF_BILL_REPORT_V1')),
  check (jsonb_typeof(values_json->'invoice_membership')='string' and values_json->>'invoice_membership' in ('FINALISED_IN_CYCLE_WINDOW','FOLLOW_FINAL_REPORT_LINES')),
  check (jsonb_typeof(values_json->'pay_membership')='string' and values_json->>'pay_membership' in ('ALL_FINALISED_IN_FILE','ON_ACCEPTED_REPORT_OR_PROTECTED')),
  check (jsonb_typeof(values_json->'invoice_amount_basis')='string' and values_json->>'invoice_amount_basis' in ('CONTRACT_CALCULATION','SOURCE_AMOUNT')),
  check (jsonb_typeof(values_json->'source_amount_basis')='string' and values_json->>'source_amount_basis' in ('NONE','EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY','REPORT_LINE_NET')),
  check (jsonb_typeof(values_json->'line_vat_method')='string' and values_json->>'line_vat_method' in ('ROUND_EACH_LINE_FROM_NET','LINE_GROSS_MINUS_NET_FROM_SOURCE','LINE_VAT_FROM_REPORT','ROUND_TOTAL_FROM_EXACT_NET')),
  check (jsonb_typeof(values_json->'invoice_vat_method')='string' and values_json->>'invoice_vat_method' in ('SUM_OF_LINES','ONCE_ON_TOTAL_TRUNCATE','ONCE_ON_TOTAL_HALF_UP')),
  check (jsonb_typeof(values_json->'rate_difference_policy')='string' and values_json->>'rate_difference_policy' in ('NOT_APPLICABLE','FLAG_ALLOW_IGNORE')),
  check (jsonb_typeof(values_json->'negative_adjustment_policy')='string' and values_json->>'negative_adjustment_policy' in ('NOT_ALLOWED','NEGATIVE_ADJUSTMENT_LINE')),
  check (jsonb_typeof(values_json->'lookback_warning')='string' and values_json->>'lookback_warning' in ('OFF','WARN_IF_AFTER_OLDEST_PROTECTED')),
  check (jsonb_typeof(values_json->'identical_rekey')='string' and values_json->>'identical_rekey' in ('COMPLETE_AUTOMATICALLY')),
  check (jsonb_typeof(values_json->'authorised_late_addition')='string' and values_json->>'authorised_late_addition' in ('COMPLETE_AUTOMATICALLY')),
  check (jsonb_typeof(values_json->'equal_protected_hours')='string' and values_json->>'equal_protected_hours' in ('CLEAR_AUTOMATICALLY')),
  check (jsonb_typeof(values_json->'signed_comparison')='string' and values_json->>'signed_comparison' in ('ON_EVERY_PUBLICATION')),
  check (jsonb_typeof(values_json->'unfinalised_action')='string' and values_json->>'unfinalised_action' in ('ASK_CLIENT_TO_FINALISE')),
  check (jsonb_typeof(values_json->'unfinalised_chase')='string' and values_json->>'unfinalised_chase' in ('ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT')),
  check (jsonb_typeof(values_json->'changed_query_clocks')='string' and values_json->>'changed_query_clocks' in ('REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED')),
  check (jsonb_typeof(values_json->'invoice_sender')='string' and values_json->>'invoice_sender' in ('CLIENT_SELF_BILL','AGENCY_INVOICE')),
  check (jsonb_typeof(values_json->'attach_healthroster_section')='boolean'),
  check ((values_json->'earliest_final_weekday'='null'::jsonb and values_json->'earliest_final_local_time'='null'::jsonb)
    or (jsonb_typeof(values_json->'earliest_final_weekday')='number'
      and values_json->>'earliest_final_weekday' ~ '^[0-6]$'
      and jsonb_typeof(values_json->'earliest_final_local_time')='string'
      and values_json->>'earliest_final_local_time' ~ '^([01][0-9]|2[0-3]):[0-5][0-9]$')),
  constraint weekly_source_rule_values_r1 check (values_json->>'self_bill_source'<>'SAME_FILE'
    or (values_json->>'final_report_profile_kind'='NONE' and values_json->>'invoice_membership'='FINALISED_IN_CYCLE_WINDOW'
      and values_json->>'pay_membership'='ALL_FINALISED_IN_FILE')),
  constraint weekly_source_rule_values_r2 check ((values_json->>'invoice_membership'<>'FOLLOW_FINAL_REPORT_LINES'
    and values_json->>'pay_membership'<>'ON_ACCEPTED_REPORT_OR_PROTECTED')
    or (values_json->>'self_bill_source'='SEPARATE_FINAL_REPORT' and values_json->>'final_report_profile_kind'<>'NONE')),
  constraint weekly_source_rule_values_r3 check ((values_json->>'invoice_amount_basis'<>'SOURCE_AMOUNT' or values_json->>'source_amount_basis'<>'NONE')
    and (values_json->>'source_amount_basis'<>'REPORT_LINE_NET' or values_json->>'self_bill_source'='SEPARATE_FINAL_REPORT')
    and (values_json->>'source_amount_basis'<>'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
      or values_json->>'checking_profile_kind'<>'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')),
  constraint weekly_source_rule_values_r4 check ((values_json->>'line_vat_method'<>'LINE_VAT_FROM_REPORT' or values_json->>'final_report_profile_kind'<>'NONE')
    and (values_json->>'line_vat_method'<>'LINE_GROSS_MINUS_NET_FROM_SOURCE'
      or values_json->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY')
    and (values_json->>'line_vat_method'<>'ROUND_TOTAL_FROM_EXACT_NET' or values_json->>'invoice_amount_basis'='CONTRACT_CALCULATION')),
  constraint weekly_source_rule_values_r6 check ((values_json->>'rate_difference_policy'='FLAG_ALLOW_IGNORE')=(values_json->>'invoice_amount_basis'='SOURCE_AMOUNT')),
  constraint weekly_source_rule_values_r9 check ((values_json->>'checking_profile_kind'='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
      or values_json->>'invoice_sender'='AGENCY_INVOICE')=(values_json->'earliest_final_weekday'='null'::jsonb)),
  constraint weekly_source_rule_values_r13 check (values_json->>'invoice_sender'<>'AGENCY_INVOICE'
    or values_json - 'attach_healthroster_section'='{"self_bill_source":"SAME_FILE","final_report_profile_kind":"NONE","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","pay_membership":"ALL_FINALISED_IN_FILE","invoice_amount_basis":"CONTRACT_CALCULATION","source_amount_basis":"NONE","line_vat_method":"ROUND_EACH_LINE_FROM_NET","invoice_vat_method":"SUM_OF_LINES","rate_difference_policy":"NOT_APPLICABLE","negative_adjustment_policy":"NEGATIVE_ADJUSTMENT_LINE","lookback_warning":"WARN_IF_AFTER_OLDEST_PROTECTED","identical_rekey":"COMPLETE_AUTOMATICALLY","authorised_late_addition":"COMPLETE_AUTOMATICALLY","equal_protected_hours":"CLEAR_AUTOMATICALLY","signed_comparison":"ON_EVERY_PUBLICATION","unfinalised_action":"ASK_CLIENT_TO_FINALISE","unfinalised_chase":"ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT","changed_query_clocks":"REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED","invoice_sender":"AGENCY_INVOICE","earliest_final_weekday":null,"earliest_final_local_time":null,"checking_profile_kind":"HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1"}'::jsonb),
  check (values_sha256=encode(extensions.digest(convert_to(values_json::text,'UTF8'),'sha256'),'hex'))
);

create table public.weekly_source_group_rule_assignments (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  rule_set_version_id uuid references public.weekly_source_rule_set_versions(id) on delete restrict,
  effective_from_week_ending date not null,
  effective_to_week_ending date,
  version bigint not null default 1 check (version>=1),
  assignment_request_id uuid not null,
  assignment_request_sha256 text not null check (assignment_request_sha256 ~ '^[0-9a-f]{64}$'),
  assignment_receipt_json jsonb not null check (jsonb_typeof(assignment_receipt_json)='object'),
  created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default transaction_timestamp(),
  feature_enabled boolean not null default false,
  feature_purpose text check (feature_purpose in ('RULE_SET','SIGNED_CHECK_ONLY')),
  feature_changed_by_user_id uuid references public.tms_users(id) on delete restrict,
  feature_changed_at_utc timestamptz,
  feature_change_reason text check (feature_change_reason is null or char_length(btrim(feature_change_reason)) between 1 and 500),
  feature_release_id text references private.cloudtms_database_releases(release_id) on delete restrict,
  feature_request_id uuid,
  feature_request_sha256 text check (feature_request_sha256 is null or feature_request_sha256 ~ '^[0-9a-f]{64}$'),
  feature_receipt_json jsonb check (feature_receipt_json is null or jsonb_typeof(feature_receipt_json)='object'),
  unique (source_group_id,assignment_request_id),
  check (effective_to_week_ending is null or (effective_to_week_ending>=effective_from_week_ending
    and (effective_to_week_ending-effective_from_week_ending)%7=0)),
  check (not feature_enabled or (feature_purpose is not null and feature_changed_by_user_id is not null
    and feature_changed_at_utc is not null and feature_change_reason is not null and feature_release_id is not null
    and feature_request_id is not null and feature_request_sha256 is not null and feature_receipt_json is not null)),
  check (feature_purpose is null or ((rule_set_version_id is null)=(feature_purpose='SIGNED_CHECK_ONLY'))),
  constraint weekly_source_group_rule_assignments_no_overlap exclude using gist (
    source_group_id with =,
    daterange(effective_from_week_ending,coalesce(effective_to_week_ending,'infinity'::date),'[]') with &&
  )
);
create index weekly_source_rule_set_versions_latest_idx on public.weekly_source_rule_set_versions(rule_set_id,version_number desc);
create index weekly_source_group_rule_assignments_version_idx on public.weekly_source_group_rule_assignments(rule_set_version_id);

alter table public.weekly_source_final_revisions add column rule_set_version_id uuid
  references public.weekly_source_rule_set_versions(id) on delete restrict;
comment on column public.weekly_source_final_revisions.rule_set_version_id is
  'WP-F1 insert-only rule witness. Disabled/unassigned/NULL assignments preserve legacy NULL. Never part of effective policy or NO_CHANGE.';

alter table public.weekly_source_rule_sets owner to postgres;
alter table public.weekly_source_rule_set_versions owner to postgres;
alter table public.weekly_source_group_rule_assignments owner to postgres;
do $security$
declare v_table text; v_owner text:=current_user;
begin
  foreach v_table in array array['weekly_source_rule_sets','weekly_source_rule_set_versions','weekly_source_group_rule_assignments'] loop
    execute format('alter table public.%I enable row level security',v_table);
    execute format('alter table public.%I force row level security',v_table);
    execute format('create policy cloudtms_miget_service_owner_all on public.%I for all to %I,service_role using (true) with check (true)',v_table,v_owner);
    execute format('revoke all on table public.%I from public,anon,authenticated,service_role',v_table);
    if not exists(select 1 from pg_catalog.pg_policy p join pg_catalog.pg_class c on c.oid=p.polrelid
      join pg_catalog.pg_namespace n on n.oid=c.relnamespace
      where n.nspname='public' and c.relname=v_table and c.relrowsecurity and c.relforcerowsecurity
        and p.polname='cloudtms_miget_service_owner_all' and p.polcmd='*'
        and (select oid from pg_catalog.pg_roles where rolname=v_owner)=any(p.polroles)
        and (select oid from pg_catalog.pg_roles where rolname='service_role')=any(p.polroles)
        and pg_catalog.pg_get_expr(p.polqual,p.polrelid)='true'
        and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid)='true') then
      raise exception 'WEEKLY_SOURCE_RULE_SET_RLS_POLICY_MISSING';
    end if;
  end loop;
end;
$security$;

-- Immutable version guards are installed by the later owning repeatable,
-- after the central guard exists on a blank NEW database. No direct API access
-- or rule-set RPC exists during this migration-only stage.


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



-- HealthRoster v2 F5/F6. No existing money is rewritten. NULL source columns
-- preserve the installed NHSP/ordinary/signed/legacy-HealthRoster path.



alter table public.weekly_source_upload_rows
  add column source_gross_pence bigint check (source_gross_pence between -999999999999 and 999999999999),
  add column source_vat_pence bigint check (source_vat_pence between -999999999999 and 999999999999),
  add column finalised_at_utc timestamptz check (finalised_at_utc is null or isfinite(finalised_at_utc)),
  add constraint weekly_source_upload_rows_invoice_money_shape check (
    (source_gross_pence is null and source_vat_pence is null)
    or (source_gross_pence is not null and source_vat_pence is not null
      and source_shift_charge_pence is not null
      and (row_finalisation_state='SOURCE_REPORT_LINE'
        or source_shift_charge_pence::numeric+source_vat_pence::numeric=source_gross_pence::numeric))
  );

alter table public.weekly_source_money_cell_evidence
  drop constraint weekly_source_money_cell_evidence_money_field_kind_check,
  add constraint weekly_source_money_cell_evidence_money_field_kind_check
  check (money_field_kind in ('COMMISSION','TOTAL_COST','FMC','BOTTOM_TOTAL_COST','SOURCE_NET','SOURCE_GROSS','SOURCE_VAT'));

-- Drop only the checks coupled to the closed comparison profile. The exact
-- legacy NHSP conjunction is reinstated below; independent IDs/FKs/uniques,
-- canonical-calculation fingerprint and money bounds remain in force.
do $block$
declare c record;
begin
  for c in select conname from pg_catalog.pg_constraint where conrelid='public.weekly_source_charge_checks'::regclass and contype='c'
    and (pg_catalog.pg_get_constraintdef(oid) like '%comparison_profile_version%'
      or pg_catalog.pg_get_constraintdef(oid) like '%comparison_result%'
      or pg_catalog.pg_get_constraintdef(oid) like '%row_sign_kind%')
  loop execute format('alter table public.weekly_source_charge_checks drop constraint %I',c.conname); end loop;
end;
$block$;
alter table public.weekly_source_charge_checks
  add constraint weekly_source_charge_checks_profile_result check (
    (comparison_profile_version='NHSP_TWO_COMPONENT_PENCE_V1'
      and row_sign_kind in ('POSITIVE','FULL_NEGATIVE')
      and comparison_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE')
      and (comparison_result='UNVERIFIABLE' or (source_commission_pence is not null and source_total_cost_pence is not null
        and source_shift_charge_pence is not null and calculated_segment_charge_pence is not null and source_charge_difference_pence is not null))
      and (comparison_result<>'EXACT' or source_charge_difference_pence=0)
      and (comparison_result<>'SOURCE_ROUNDING_EQUIVALENT' or (source_charge_difference_pence in (1,-1)
        and ((source_shift_charge_pence>0 and calculated_segment_charge_pence>0) or (source_shift_charge_pence<0 and calculated_segment_charge_pence<0))))
      and (comparison_result not in ('MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE') or phase_severity in ('PROVISIONAL_WARNING','FINALISATION_BLOCKER'))
      and (comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE') or blocker_code is null)
      and (comparison_result<>'ZERO_SOURCE_CHARGE' or (source_commission_pence=0 and source_total_cost_pence=0 and source_shift_charge_pence=0 and calculated_segment_charge_pence<>0 and blocker_code is null))
      and (row_sign_kind<>'POSITIVE' or comparison_result='UNVERIFIABLE' or (source_commission_pence>=0 and source_total_cost_pence>=0
        and ((source_shift_charge_pence>0 and calculated_segment_charge_pence>0) or (comparison_result='ZERO_SOURCE_CHARGE' and source_shift_charge_pence=0 and calculated_segment_charge_pence>0))))
      and (row_sign_kind<>'FULL_NEGATIVE' or comparison_result='UNVERIFIABLE' or (source_commission_pence<=0 and source_total_cost_pence<=0 and source_shift_charge_pence<0 and calculated_segment_charge_pence<0)))
    or (comparison_profile_version='SOURCE_INVOICE_MONEY_PENCE_V1' and row_sign_kind='POSITIVE'
      and comparison_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','SOURCE_AMOUNT_DIFFERS')
      and source_commission_pence is null and source_total_cost_pence is null
      and source_shift_charge_pence is not null and source_shift_charge_pence>=0
      and calculated_segment_charge_pence is not null and calculated_segment_charge_pence>0
      and source_charge_difference_pence is not null
      and source_charge_difference_pence=source_shift_charge_pence-calculated_segment_charge_pence
      and comparison_result=case when source_charge_difference_pence=0 then 'EXACT'
        when abs(source_charge_difference_pence)=1 then 'SOURCE_ROUNDING_EQUIVALENT' else 'SOURCE_AMOUNT_DIFFERS' end
      and phase_severity=case when comparison_result='SOURCE_AMOUNT_DIFFERS' then 'INFORMATION' else 'NONE' end and blocker_code is null)
  );

create table public.weekly_source_invoice_hold_releases (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  hold_kind text not null check (hold_kind in ('VAT_FIT','DUPLICATE_REQUEST_ID')),
  action text not null check (action in ('BILL_AT_NET_PLUS_20','BILL_AS_FILE')),
  net_pence bigint not null check (net_pence between -999999999999 and 999999999999),
  vat_pence bigint not null check (vat_pence between -999999999999 and 999999999999),
  gross_pence bigint not null check (gross_pence between -999999999999 and 999999999999),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  release_hash bytea not null check (pg_catalog.octet_length(release_hash)=32),
  check (net_pence::numeric+vat_pence::numeric=gross_pence::numeric),
  check (hold_kind<>'DUPLICATE_REQUEST_ID' or action='BILL_AS_FILE'),
  unique (upload_row_id,hold_kind), unique (release_hash)
);
alter table public.weekly_source_invoice_hold_releases owner to postgres;
alter table public.weekly_source_invoice_hold_releases enable row level security;
alter table public.weekly_source_invoice_hold_releases force row level security;
create policy cloudtms_miget_service_owner_all on public.weekly_source_invoice_hold_releases
  for all to CURRENT_USER,service_role using (true) with check (true);
revoke all on public.weekly_source_invoice_hold_releases from public,anon,authenticated,service_role;

alter table public.weekly_source_billing_movements
  add column self_bill_report_row_id uuid references public.weekly_source_upload_rows(id) on delete restrict,
  add column vat_rounding_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  alter column candidate_id drop not null, alter column contract_id drop not null,
  alter column work_event_id drop not null, alter column invoice_timesheet_id drop not null;
create unique index weekly_source_billing_movements_report_row_uq on public.weekly_source_billing_movements(self_bill_report_row_id) where self_bill_report_row_id is not null;
create unique index weekly_source_billing_movements_vat_rounding_client_uq on public.weekly_source_billing_movements(vat_rounding_revision_id,actual_client_id) where vat_rounding_revision_id is not null;
-- The origin/role/profile/price checks are replaced as one closed contract.
-- Retain every independent economic/range/hash/placement check and all FKs.
do $block$
declare c record;
begin
  for c in select conname from pg_catalog.pg_constraint where conrelid='public.weekly_source_billing_movements'::regclass and contype='c'
    and (pg_catalog.pg_get_constraintdef(oid) like '%transition_id%'
      or pg_catalog.pg_get_constraintdef(oid) like '%nhsp_upload_row_id%'
      or pg_catalog.pg_get_constraintdef(oid) like '%expense_authority_generation_id%'
      or pg_catalog.pg_get_constraintdef(oid) like '%source_profile_kind%'
      or pg_catalog.pg_get_constraintdef(oid) like '%source_line_kind%'
      or pg_catalog.pg_get_constraintdef(oid) like '%movement_role%'
      or pg_catalog.pg_get_constraintdef(oid) like '%price_check_result%')
  loop execute format('alter table public.weekly_source_billing_movements drop constraint %I',c.conname); end loop;
end;
$block$;
alter table public.weekly_source_billing_movements
  add constraint weekly_source_billing_movements_five_origins check (
    (transition_id is not null)::integer+(nhsp_upload_row_id is not null)::integer
    +(expense_authority_generation_id is not null)::integer+(self_bill_report_row_id is not null)::integer
    +(vat_rounding_revision_id is not null)::integer=1),
  add constraint weekly_source_billing_movements_invoice_contract check (
    source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS','SELF_BILL_REPORT')
    and movement_role in ('POSITIVE','REVERSAL','REPLACEMENT','NET_DIFFERENCE_PRESENTATION','EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT','STANDALONE_NEGATIVE','CORRECTION_UNWIND')
    and source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','GENERATED_HISTORICAL_REVERSAL','NON_NHSP_DIFFERENCE','NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE','SOURCE_FIXED_EXPENSE','SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')
    and ((source_profile_kind='NHSP_TRUST_BACKING_REPORT')=(nhsp_upload_row_id is not null))
    and ((self_bill_report_row_id is not null)=(source_line_kind='SELF_BILL_REPORT_LINE'))
    and (self_bill_report_row_id is null or source_profile_kind='SELF_BILL_REPORT')
    and (source_profile_kind<>'SELF_BILL_REPORT' or source_line_kind in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING'))
    and ((vat_rounding_revision_id is not null)=(source_line_kind='INVOICE_VAT_ROUNDING'))
    and (vat_rounding_revision_id is null or vat_rounding_revision_id=final_revision_id)
    and ((expense_authority_generation_id is not null)=(source_line_kind='SOURCE_FIXED_EXPENSE'))
    and ((expense_authority_generation_id is not null)=(movement_role in ('EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT')))
    and (movement_role<>'STANDALONE_NEGATIVE' or (source_line_kind='SELF_BILL_REPORT_LINE' and invoice_presentation_charge_pence<0))
    and (movement_role<>'CORRECTION_UNWIND' or (source_line_kind='SOURCE_INVOICE_CORRECTION' and transition_id is not null))
    and (source_line_kind<>'SOURCE_REBILL_LINE' or (transition_id is not null and movement_role='POSITIVE'))
    and (source_line_kind<>'SOURCE_INVOICE_CORRECTION' or (transition_id is not null and movement_role in ('REVERSAL','REPLACEMENT','CORRECTION_UNWIND')))
    and (source_line_kind<>'SELF_BILL_REPORT_LINE' or movement_role in ('POSITIVE','STANDALONE_NEGATIVE')
      or (movement_role='REVERSAL' and prior_movement_id is not null))
    and (source_line_kind<>'INVOICE_VAT_ROUNDING' or (movement_role='POSITIVE' and invoice_presentation_charge_pence=0 and calculated_comparison_charge_pence=0))
    and ((source_line_kind='INVOICE_VAT_ROUNDING' or (source_line_kind='SELF_BILL_REPORT_LINE' and movement_role='STANDALONE_NEGATIVE'))
      or (candidate_id is not null and contract_id is not null and work_event_id is not null and invoice_timesheet_id is not null))
    and price_check_result in ('NOT_APPLICABLE','EXACT','SOURCE_ROUNDING_EQUIVALENT','SOURCE_AMOUNT_DIFFERS','ACCEPTED_DISPARITY','ACCEPTED_ZERO')
    and ((price_check_result in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO'))=(charge_acceptance_id is not null))
    and (source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' or (source_validation_charge_pence is not null
      and invoice_presentation_charge_pence=source_validation_charge_pence and price_check_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO')))
    and (source_line_kind<>'SOURCE_FIXED_EXPENSE' or (source_validation_charge_pence is not null
      and invoice_presentation_charge_pence=source_validation_charge_pence and price_check_result='NOT_APPLICABLE'))
    and (source_profile_kind='NHSP_TRUST_BACKING_REPORT' or price_check_result in ('NOT_APPLICABLE','EXACT','SOURCE_ROUNDING_EQUIVALENT','SOURCE_AMOUNT_DIFFERS','ACCEPTED_DISPARITY'))
  ),
  add constraint weekly_source_billing_movements_invoice_only_zero_pay check (
    source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')
    or coalesce((total_pay_ex_vat=0 and canonical_pay_vector_json->>'total_pence'='0'
      and (canonical_pay_vector_json->>'paid_minutes')::numeric=0
      and (canonical_pay_vector_json->>'break_minutes')::numeric=0
      and canonical_pay_vector_json->'hours'='{"day":0,"night":0,"sat":0,"sun":0,"bh":0}'::jsonb),false)
  );

-- PostgreSQL auto-names the old 65-character enum constraint by shortening
-- the relation component. Resolve its exact one-column CHECK through catalogue
-- identity instead of guessing a truncated identifier or dropping coupled checks.
do $block$
declare constraint_name text; price_att smallint;
begin
  select attnum into strict price_att from pg_catalog.pg_attribute where attrelid='public.weekly_source_invoice_presentation_lines'::regclass and attname='price_check_result' and not attisdropped;
  select conname into strict constraint_name from pg_catalog.pg_constraint where conrelid='public.weekly_source_invoice_presentation_lines'::regclass and contype='c'
    and conkey=array[price_att]::smallint[] and pg_catalog.pg_get_constraintdef(oid) like '%NOT_APPLICABLE%';
  execute format('alter table public.weekly_source_invoice_presentation_lines drop constraint %I',constraint_name);
end;
$block$;

alter table public.weekly_source_invoice_presentation_lines
  add column invoice_hold_release_id uuid references public.weekly_source_invoice_hold_releases(id) on delete restrict,
  alter column candidate_id drop not null, alter column contract_id drop not null,
  alter column work_event_id drop not null, alter column source_shift_group_id drop not null,
  alter column work_date drop not null, alter column start_at_local drop not null,
  alter column end_at_local drop not null, alter column break_minutes drop not null,
  alter column candidate_display_snapshot drop not null, alter column description_snapshot drop not null,
  drop constraint weekly_source_invoice_presentation_lines_line_kind_check,
  drop constraint weekly_source_invoice_presentation_lines_origin_kind_check,
  add constraint weekly_source_invoice_presentation_lines_line_kind_check check (line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','GENERATED_HISTORICAL_REVERSAL','NON_NHSP_DIFFERENCE','SOURCE_FIXED_EXPENSE','SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')),
  add constraint weekly_source_invoice_presentation_lines_origin_kind_check check (origin_kind in ('GENERIC_TRANSITION','HEALTHROSTER_TRANSITION','NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE','SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SELF_BILL_REPORT_STANDALONE_CREDIT')),
  add constraint weekly_source_invoice_presentation_lines_price_result_chk check (price_check_result in ('NOT_APPLICABLE','EXACT','SOURCE_ROUNDING_EQUIVALENT','SOURCE_AMOUNT_DIFFERS','ACCEPTED_DISPARITY','ACCEPTED_ZERO')),
  add constraint weekly_source_invoice_presentation_lines_nullable_invoice_identity check (
    (line_kind='INVOICE_VAT_ROUNDING' and origin_kind='INVOICE_VAT_ROUNDING' and total_pay_ex_vat=0)
    or (line_kind='SELF_BILL_REPORT_LINE' and origin_kind='SELF_BILL_REPORT_STANDALONE_CREDIT' and total_pay_ex_vat=0 and total_charge_ex_vat<0)
    or (candidate_id is not null and contract_id is not null and work_event_id is not null
      and source_shift_group_id is not null and work_date is not null and start_at_local is not null and end_at_local is not null and break_minutes is not null
      and candidate_display_snapshot is not null and description_snapshot is not null)
  );
alter table public.weekly_source_invoice_placements alter column source_shift_group_id drop not null;


-- Exact selected once_only_schema component 1: 7afa77836974e011516d146be6778a5db32fc9e6:docs/healthroster-build/proposals/agency-current-shared-owner-union-forward-20261015/migration/01_same_agency_request_store.sqlinc
-- One planned preset-T0 change request authority, primary §§25.2/26.5.1/26.11.
-- Source construction only. No runtime/bootstrap assertion or business data.
create table private.weekly_source_agency_timesheet_change_requests (
  id uuid primary key default gen_random_uuid(),
  request_key text not null unique check(length(request_key) between 1 and 200),
  request_sha256 bytea not null check(octet_length(request_sha256)=32),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  reason text not null check(length(btrim(reason)) between 1 and 2000),
  environment text not null check(environment='TEST'),
  agency_id uuid not null,
  contract_week_id uuid not null references public.contract_weeks(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  week_ending_date date not null,
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  source_upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  source_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  rule_set_version_id uuid not null references public.weekly_source_rule_set_versions(id) on delete restrict,
  source_cas jsonb not null check(coalesce(jsonb_typeof(source_cas)='object'
    and source_cas ?& array['schema_version','source_cycle_id','client_ids','source_cas_sha256']
    and source_cas-array['schema_version','source_cycle_id','client_ids','source_cas_sha256']='{}'::jsonb
    and jsonb_typeof(source_cas->'schema_version')='string'
    and source_cas->>'schema_version'='WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'
    and jsonb_typeof(source_cas->'source_cycle_id')='string'
    and jsonb_typeof(source_cas->'client_ids')='array'
    and jsonb_typeof(source_cas->'source_cas_sha256')='string'
    and source_cas->>'source_cas_sha256'~'^[0-9a-f]{64}$',false)),
  -- Immutable native capture tokens and the current monotonic native footprint
  -- live on this SINGLE planned request. No parallel scope/claim ledger.
  source_dependency_tokens jsonb not null check(jsonb_typeof(source_dependency_tokens)='array'
    and octet_length(source_dependency_tokens::text)<=8388608),
  current_source_dependency_tokens jsonb not null check(jsonb_typeof(current_source_dependency_tokens)='array'
    and octet_length(current_source_dependency_tokens::text)<=8388608
    and current_source_dependency_tokens @> source_dependency_tokens),
  initial_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  initial_timesheet_version integer not null check(initial_timesheet_version>=1),
  initial_row_signature text not null check(length(initial_row_signature) between 1 and 512),
  canonical_family_booking_id text not null check(length(canonical_family_booking_id) between 1 and 200
    and canonical_family_booking_id=btrim(canonical_family_booking_id)),
  original_draft_invoice_id uuid references public.invoices(id) on delete restrict,
  original_draft_invoice_no text,
  state text not null default 'ACTIVE' check(state in('ACTIVE','COMPLETED')),
  phase text not null default 'CAPTURED' check(phase in('CAPTURED','DRAFT_REMOVED','UNAUTHORISED',
    'SCHEDULE_APPLIED','RECOMPUTE_PENDING','FINANCIAL_CURRENT','AUTHORISED','COMPLETED')),
  version bigint not null default 1 check(version>=1),
  current_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  current_timesheet_version integer not null check(current_timesheet_version>=1),
  current_row_signature text not null check(length(current_row_signature) between 1 and 512),
  return_hold boolean not null default false,
  outbox_id uuid,
  outbox_reason public.ts_fin_reason_enum,
  current_financial_id uuid references public.timesheets_financials(id) on delete restrict,
  native_receipts jsonb not null check(coalesce(jsonb_typeof(native_receipts)='object'
    and jsonb_typeof(native_receipts->'original_capture')='object'
    and native_receipts#>>'{original_capture,schema_version}'='WEEKLY_SOURCE_AGENCY_CHANGE_NATIVE_CAPTURE_V1'
    and octet_length(native_receipts::text)<=65536,false)),
  last_refusal_code text check(last_refusal_code is null or length(last_refusal_code) between 1 and 200),
  completed_receipt jsonb check(completed_receipt is null or (jsonb_typeof(completed_receipt)='object'
    and octet_length(completed_receipt::text)<=16384)),
  created_at_utc timestamptz not null default transaction_timestamp(),
  updated_at_utc timestamptz not null default transaction_timestamp(),
  completed_at_utc timestamptz,
  -- Original creator/click remains immutable. Each accepted phase is approved
  -- and audited by its genuine current scoped actor; no creator impersonation.
  updated_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  completed_by_user_id uuid references public.tms_users(id) on delete restrict,
  check((state='ACTIVE' and phase<>'COMPLETED' and completed_receipt is null
      and completed_at_utc is null and completed_by_user_id is null)
    or(state='COMPLETED' and phase='COMPLETED' and completed_receipt is not null
      and completed_at_utc is not null and completed_by_user_id is not null and not return_hold)),
  check(not return_hold or(state='ACTIVE' and original_draft_invoice_id is not null)),
  check(original_draft_invoice_id is not null or original_draft_invoice_no is null),
  check((outbox_id is null)=(outbox_reason is null)),
  check(coalesce(source_cas->>'source_cycle_id'=source_cycle_id::text,false)),
  check(coalesce(source_cas->'client_ids'=jsonb_build_array(client_id),false))
);
alter table private.weekly_source_agency_timesheet_change_requests owner to postgres;
alter table private.weekly_source_agency_timesheet_change_requests enable row level security;
alter table private.weekly_source_agency_timesheet_change_requests force row level security;
create policy cloudtms_miget_service_owner_all on private.weekly_source_agency_timesheet_change_requests
  for all to current_user using(true) with check(true);
revoke all on private.weekly_source_agency_timesheet_change_requests from public,anon,authenticated,service_role;
grant all on private.weekly_source_agency_timesheet_change_requests to postgres;
create unique index weekly_source_agency_change_active_family_v1
  on private.weekly_source_agency_timesheet_change_requests(agency_id,canonical_family_booking_id) where state='ACTIVE';
create unique index weekly_source_agency_change_active_week_v1
  on private.weekly_source_agency_timesheet_change_requests(contract_week_id) where state='ACTIVE';
create index weekly_source_agency_change_current_draft_hold_v1
  on private.weekly_source_agency_timesheet_change_requests(original_draft_invoice_id) where return_hold;
comment on table private.weekly_source_agency_timesheet_change_requests is
  'Single planned preset-T0 amendment/phase and original-D1 hold authority. No schedule, finance, pay, query, queue or P0 correction authority; history retained.';


-- Exact selected once_only_schema component 2: 38964e6acf7e0d10af9cfbc223a9bb251a4dadfd:docs/healthroster-build/proposals/agency-current-source-callers-lifecycle-module-20261015/migration/02_typed_existing_validation_columns.sqlinc
-- Additive proposal for the existing validation store. LEGACY includes all
-- existing import, manual and pre-validation paths; no row is reclassified.
-- last_source remains the real UUID/hr_imports FK. No fabricated import row.
alter table public.timesheet_validations
 add column validation_source_kind text not null default 'LEGACY',
 add column validation_source_publication_id uuid references public.weekly_source_projection_publications(id) on delete restrict,
 add column validation_source_facts_sha256 bytea,
 add column validation_source_actor_user_id uuid references public.tms_users(id) on delete restrict;
alter table public.timesheet_validations add constraint timesheet_validation_typed_source_v1 check (
 (validation_source_kind='LEGACY' and validation_source_publication_id is null
  and validation_source_facts_sha256 is null and validation_source_actor_user_id is null)
 or (validation_source_kind='AGENCY_INVOICE_WEEK' and last_source is null
  and validation_source_publication_id is not null
  and validation_source_actor_user_id is not null
  and validation_source_facts_sha256 is not null
  and octet_length(validation_source_facts_sha256)=32)
);
comment on column public.timesheet_validations.validation_source_kind is
 'Typed provenance of the one existing validation row. LEGACY preserves existing last_source semantics; AGENCY_INVOICE_WEEK binds genuine Source publication, facts and actual validating actor.';
comment on column public.timesheet_validations.last_source is
 'Existing real hr_imports UUID FK. Never stores a text source label or an Agency publication UUID.';
-- No grants, RLS relaxation, row update, new store or business bootstrap.

-- End exact selected once_only_schema components.

create or replace function private._weekly_source_immutable_record_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_IMMUTABLE_RECORD'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_IMMUTABLE_RECORD',
            'table_name',tg_table_name,
            'operation',tg_op
          )::text;
end;
$function$;
create or replace function private.weekly_source_final_rule_version_guard_v1()
returns trigger language plpgsql security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_version uuid;
begin
  if tg_op='UPDATE' then
    if new.rule_set_version_id is distinct from old.rule_set_version_id then
      raise exception 'WEEKLY_SOURCE_FINAL_RULE_VERSION_IMMUTABLE' using errcode='55000';
    end if;
    return new;
  end if;
  select a.rule_set_version_id into v_version from public.weekly_source_cycles c
    join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.feature_enabled and a.feature_purpose='RULE_SET'
      and c.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
    where c.id=new.source_cycle_id;
  if new.rule_set_version_id is not null and new.rule_set_version_id is distinct from v_version then
    raise exception 'WEEKLY_SOURCE_FINAL_RULE_VERSION_STALE' using errcode='40001';
  end if;
  new.rule_set_version_id:=v_version;
  return new;
end;
$function$;
create or replace function private.weekly_source_self_bill_binding_guard_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
begin
  if tg_op='DELETE' or tg_op='TRUNCATE' then raise exception 'WEEKLY_SOURCE_REPORT_BINDING_AUDIT_RETAINED' using errcode='55000'; end if;
  if tg_op='UPDATE' and (old.state<>'CURRENT' or new.state<>'SUPERSEDED'
    or new.superseded_at_utc is null or new.superseded_at_utc<old.bound_at_utc
    or (to_jsonb(old)-array['state','superseded_at_utc']) is distinct from (to_jsonb(new)-array['state','superseded_at_utc'])) then
    raise exception 'WEEKLY_SOURCE_REPORT_BINDING_TRANSITION_INVALID' using errcode='55000';
  end if;
  if tg_op='INSERT' and (new.state<>'CURRENT' or new.superseded_at_utc is not null
    or not exists(select 1 from public.weekly_source_upload_rows row_fact join public.weekly_source_uploads upload on upload.id=row_fact.upload_id
      join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
      join public.weekly_source_projection_publications report_publication on report_publication.id=scope.current_projection_publication_id
        and report_publication.upload_id=upload.id and report_publication.state='CURRENT' and report_publication.authority_scope_version=scope.version
      join public.weekly_source_upload_rows export_row on export_row.id=new.export_upload_row_id
        and export_row.external_source_key=row_fact.external_source_key
      join public.weekly_source_row_resolutions resolution on resolution.id=new.bound_row_resolution_id
        and resolution.upload_row_id=export_row.id and resolution.work_event_id=new.work_event_id
        and resolution.client_id=scope.client_id and resolution.mapping_state='RESOLVED'
      join public.weekly_source_projection_publications export_publication on export_publication.id=new.export_projection_publication_id
        and export_publication.upload_id=export_row.upload_id and export_publication.authority_scope_kind='CYCLE'
        and resolution.generation=coalesce(export_publication.projection_generation,export_publication.authority_scope_version)
      join public.weekly_source_cycles export_cycle on export_cycle.id=export_publication.source_cycle_id
        and export_cycle.source_group_id=scope.source_group_id
      where row_fact.id=new.report_row_id and row_fact.row_finalisation_state='SOURCE_REPORT_LINE'
        and scope.current_complete_upload_id=upload.id and upload.state='CURRENT'
        and scope.self_bill_report_id=new.self_bill_report_id
        and ((export_publication.state='CURRENT' and export_cycle.current_complete_upload_id=export_row.upload_id
          and export_cycle.current_projection_publication_id=export_publication.id)
          or (row_fact.source_shift_charge_pence<0 and private.weekly_source_self_bill_report_prior_binding_v1(row_fact.id)=jsonb_build_object(
            'prior_movement_id',private.weekly_source_self_bill_report_prior_binding_v1(row_fact.id)->'prior_movement_id',
            'row_id',new.export_upload_row_id,'resolution_id',new.bound_row_resolution_id,
            'publication_id',new.export_projection_publication_id,'work_event_id',new.work_event_id))))) then
    raise exception 'WEEKLY_SOURCE_REPORT_BINDING_OWNER_INVALID' using errcode='55000';
  end if;
  return new;
end;
$function$;
create or replace function private._weekly_source_rule_values_v1(p_input jsonb)
returns jsonb language plpgsql immutable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_values jsonb;
  v_choice record;
  v_choices jsonb:='{"checking_profile_kind":["HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1","HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1","ROSTER_WEEKLY_SUMMARY_ACTUAL_V1"],"self_bill_source":["SAME_FILE","SEPARATE_FINAL_REPORT"],"final_report_profile_kind":["NONE","ACACIUM_SELF_BILL_REPORT_V1"],"invoice_membership":["FINALISED_IN_CYCLE_WINDOW","FOLLOW_FINAL_REPORT_LINES"],"pay_membership":["ALL_FINALISED_IN_FILE","ON_ACCEPTED_REPORT_OR_PROTECTED"],"invoice_amount_basis":["CONTRACT_CALCULATION","SOURCE_AMOUNT"],"source_amount_basis":["NONE","EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY","REPORT_LINE_NET"],"line_vat_method":["ROUND_EACH_LINE_FROM_NET","LINE_GROSS_MINUS_NET_FROM_SOURCE","LINE_VAT_FROM_REPORT","ROUND_TOTAL_FROM_EXACT_NET"],"invoice_vat_method":["SUM_OF_LINES","ONCE_ON_TOTAL_TRUNCATE","ONCE_ON_TOTAL_HALF_UP"],"rate_difference_policy":["NOT_APPLICABLE","FLAG_ALLOW_IGNORE"],"negative_adjustment_policy":["NOT_ALLOWED","NEGATIVE_ADJUSTMENT_LINE"],"lookback_warning":["OFF","WARN_IF_AFTER_OLDEST_PROTECTED"],"identical_rekey":["COMPLETE_AUTOMATICALLY"],"authorised_late_addition":["COMPLETE_AUTOMATICALLY"],"equal_protected_hours":["CLEAR_AUTOMATICALLY"],"signed_comparison":["ON_EVERY_PUBLICATION"],"unfinalised_action":["ASK_CLIENT_TO_FINALISE"],"unfinalised_chase":["ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT"],"changed_query_clocks":["REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED"],"invoice_sender":["CLIENT_SELF_BILL","AGENCY_INVOICE"]}'::jsonb;
  v_separate boolean; v_unused boolean; v_agency boolean; v_summary boolean;
begin
  perform private._weekly_source_settings_assert_request_v1(p_input,array[
    'checking_profile_kind','self_bill_source','final_report_profile_kind','invoice_membership','pay_membership',
    'invoice_amount_basis','source_amount_basis','line_vat_method','invoice_vat_method','rate_difference_policy',
    'negative_adjustment_policy','lookback_warning','identical_rekey','authorised_late_addition','equal_protected_hours',
    'signed_comparison','unfinalised_action','unfinalised_chase','changed_query_clocks','invoice_sender',
    'earliest_final_weekday','earliest_final_local_time','attach_healthroster_section'
  ],'WEEKLY_SOURCE_RULE_VALUES_INVALID');
  v_values:='{"self_bill_source":"SAME_FILE","final_report_profile_kind":"NONE","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","pay_membership":"ALL_FINALISED_IN_FILE","invoice_amount_basis":"CONTRACT_CALCULATION","source_amount_basis":"NONE","line_vat_method":"ROUND_EACH_LINE_FROM_NET","invoice_vat_method":"SUM_OF_LINES","rate_difference_policy":"NOT_APPLICABLE","negative_adjustment_policy":"NEGATIVE_ADJUSTMENT_LINE","lookback_warning":"WARN_IF_AFTER_OLDEST_PROTECTED","identical_rekey":"COMPLETE_AUTOMATICALLY","authorised_late_addition":"COMPLETE_AUTOMATICALLY","equal_protected_hours":"CLEAR_AUTOMATICALLY","signed_comparison":"ON_EVERY_PUBLICATION","unfinalised_action":"ASK_CLIENT_TO_FINALISE","unfinalised_chase":"ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT","changed_query_clocks":"REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED","invoice_sender":"CLIENT_SELF_BILL","earliest_final_weekday":4,"earliest_final_local_time":"00:00","attach_healthroster_section":true}'::jsonb||p_input;
  v_agency:=v_values->>'invoice_sender'='AGENCY_INVOICE';
  if v_agency and not p_input ? 'earliest_final_weekday' then
    v_values:=jsonb_set(v_values,'{earliest_final_weekday}','null'::jsonb);
  end if;
  if v_agency and not p_input ? 'earliest_final_local_time' then
    v_values:=jsonb_set(v_values,'{earliest_final_local_time}','null'::jsonb);
  end if;
  for v_choice in select key,value from jsonb_each(v_choices) loop
    if jsonb_typeof(v_values->v_choice.key) is distinct from 'string'
      or not v_choice.value ? (v_values->>v_choice.key) then
      raise exception 'WEEKLY_SOURCE_RULE_VALUE_INVALID' using errcode='22023';
    end if;
  end loop;
  if jsonb_typeof(v_values->'attach_healthroster_section') is distinct from 'boolean' then
    raise exception 'WEEKLY_SOURCE_RULE_VALUE_INVALID' using errcode='22023';
  end if;
  v_unused:=v_values->'earliest_final_weekday'='null'::jsonb and v_values->'earliest_final_local_time'='null'::jsonb;
  v_summary:=v_values->>'checking_profile_kind'='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1';
  if not v_unused and (jsonb_typeof(v_values->'earliest_final_weekday') is distinct from 'number'
      or coalesce(v_values->>'earliest_final_weekday','') !~ '^[0-6]$'
      or jsonb_typeof(v_values->'earliest_final_local_time') is distinct from 'string'
      or coalesce(v_values->>'earliest_final_local_time','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$') then
    raise exception 'WEEKLY_SOURCE_RULE_FINAL_CLOCK_INVALID' using errcode='22023';
  end if;
  if (v_summary or v_agency) is distinct from v_unused then
    raise exception 'WEEKLY_SOURCE_RULE_R9' using errcode='22023';
  end if;
  v_separate:=v_values->>'self_bill_source'='SEPARATE_FINAL_REPORT';
  if not v_separate and (v_values->>'final_report_profile_kind'<>'NONE'
      or v_values->>'invoice_membership'='FOLLOW_FINAL_REPORT_LINES'
      or v_values->>'pay_membership'='ON_ACCEPTED_REPORT_OR_PROTECTED') then
    raise exception 'WEEKLY_SOURCE_RULE_R1' using errcode='22023';
  end if;
  if (v_values->>'invoice_membership'='FOLLOW_FINAL_REPORT_LINES' or v_values->>'pay_membership'='ON_ACCEPTED_REPORT_OR_PROTECTED')
    and (not v_separate or v_values->>'final_report_profile_kind'='NONE') then
    raise exception 'WEEKLY_SOURCE_RULE_R2' using errcode='22023';
  end if;
  if (v_values->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_values->>'source_amount_basis'='NONE')
    or (v_values->>'source_amount_basis'='REPORT_LINE_NET' and not v_separate)
    or (v_values->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' and v_summary) then
    raise exception 'WEEKLY_SOURCE_RULE_R3' using errcode='22023';
  end if;
  if (v_values->>'line_vat_method'='LINE_VAT_FROM_REPORT' and v_values->>'final_report_profile_kind'='NONE')
    or (v_values->>'line_vat_method'='LINE_GROSS_MINUS_NET_FROM_SOURCE'
      and v_values->>'source_amount_basis'<>'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY')
    or (v_values->>'line_vat_method'='ROUND_TOTAL_FROM_EXACT_NET' and v_values->>'invoice_amount_basis'='SOURCE_AMOUNT') then
    raise exception 'WEEKLY_SOURCE_RULE_R4' using errcode='22023';
  end if;
  if (v_values->>'rate_difference_policy'='FLAG_ALLOW_IGNORE')<>(v_values->>'invoice_amount_basis'='SOURCE_AMOUNT') then
    raise exception 'WEEKLY_SOURCE_RULE_R6' using errcode='22023';
  end if;
  if v_agency and v_values - 'attach_healthroster_section'<>'{"self_bill_source":"SAME_FILE","final_report_profile_kind":"NONE","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","pay_membership":"ALL_FINALISED_IN_FILE","invoice_amount_basis":"CONTRACT_CALCULATION","source_amount_basis":"NONE","line_vat_method":"ROUND_EACH_LINE_FROM_NET","invoice_vat_method":"SUM_OF_LINES","rate_difference_policy":"NOT_APPLICABLE","negative_adjustment_policy":"NEGATIVE_ADJUSTMENT_LINE","lookback_warning":"WARN_IF_AFTER_OLDEST_PROTECTED","identical_rekey":"COMPLETE_AUTOMATICALLY","authorised_late_addition":"COMPLETE_AUTOMATICALLY","equal_protected_hours":"CLEAR_AUTOMATICALLY","signed_comparison":"ON_EVERY_PUBLICATION","unfinalised_action":"ASK_CLIENT_TO_FINALISE","unfinalised_chase":"ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT","changed_query_clocks":"REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED","invoice_sender":"AGENCY_INVOICE","earliest_final_weekday":null,"earliest_final_local_time":null,"checking_profile_kind":"HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1"}'::jsonb then
    raise exception 'WEEKLY_SOURCE_RULE_R13' using errcode='22023';
  end if;
  return v_values;
end;
$function$;
create or replace function private._weekly_source_rule_group_context_v1(
  p_group_id uuid,p_from date,p_to date,p_values jsonb,p_require_live boolean default false
) returns void language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_group public.weekly_source_groups%rowtype;
  v_row record; v_policy jsonb; v_settings jsonb; v_scope_date date;
  v_contract_context record; v_contract_policy jsonb;
  v_agency boolean:=p_values->>'invoice_sender'='AGENCY_INVOICE';
  v_count integer:=0;
begin
  select * into v_group from public.weekly_source_groups where id=p_group_id and active;
  if not found or v_group.source_family<>'ROSTER' then
    raise exception 'WEEKLY_SOURCE_RULE_R10' using errcode='22023';
  end if;
  if v_agency and exists(select 1 from public.weekly_source_uploads u
    join public.weekly_source_cycles c on c.id=u.source_cycle_id
    join public.weekly_source_format_profiles f on f.id=u.source_format_profile_id
    where c.source_group_id=p_group_id and f.profile_code<>'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'
      and u.state in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY')) then
    raise exception 'WEEKLY_SOURCE_RULE_AGENCY_PROFILE_CONFLICT' using errcode='22023';
  end if;
  if p_values is not null and p_values->>'checking_profile_kind'<>'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' and not v_agency
    and (((p_values->>'earliest_final_weekday')::integer+6)%7*1440
      +extract(hour from (p_values->>'earliest_final_local_time')::time)::integer*60
      +extract(minute from (p_values->>'earliest_final_local_time')::time)::integer
      <(v_group.cutoff_weekday+6)%7*1440+extract(hour from v_group.cutoff_local_time)::integer*60
      +extract(minute from v_group.cutoff_local_time)::integer) then
    raise exception 'WEEKLY_SOURCE_RULE_R9' using errcode='22023';
  end if;
  -- Evaluate every effective policy boundary in the requested dated interval.
  -- Membership and policy tables already exclude overlap; missing coverage is
  -- refused rather than being filled with a guessed policy.
  for v_row in
    select gc.client_id,greatest(gc.valid_from,p_from) as scope_date
    from public.weekly_source_group_clients gc
    where gc.source_group_id=p_group_id and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]')
      && daterange(p_from,coalesce(p_to,'infinity'::date),'[]')
    union
    select gc.client_id,cp.effective_from
    from public.weekly_source_group_clients gc join public.weekly_source_client_policies cp
      on cp.client_id=gc.client_id and cp.source_group_id=gc.source_group_id
    where gc.source_group_id=p_group_id and cp.effective_from>=p_from
      and cp.effective_from<=coalesce(p_to,'infinity'::date)
      and cp.effective_from between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    union
    select gc.client_id,cp.effective_to+1
    from public.weekly_source_group_clients gc join public.weekly_source_client_policies cp
      on cp.client_id=gc.client_id and cp.source_group_id=gc.source_group_id
    where gc.source_group_id=p_group_id and cp.effective_to is not null and cp.effective_to+1>=p_from
      and cp.effective_to+1<=coalesce(p_to,'infinity'::date)
      and cp.effective_to+1 between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    order by client_id,scope_date
  loop
    v_count:=v_count+1; v_scope_date:=v_row.scope_date;
    if not exists(select 1 from public.weekly_source_client_policies cp where cp.source_group_id=p_group_id
      and cp.client_id=v_row.client_id and v_scope_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)) then
      raise exception 'WEEKLY_SOURCE_RULE_R8' using errcode='22023';
    end if;
    v_policy:=private._weekly_source_effective_policy_v1(v_row.client_id,null,v_scope_date);
    if p_values is null then
      if v_policy->>'authority_mode'<>'TIMESHEET_AUTHORITY' or v_policy->>'document_mode'<>'INVOICE_EVIDENCE_REQUIRED'
        or (v_policy->>'self_bill_enabled')::boolean then
        raise exception 'WEEKLY_SOURCE_RULE_SIGNED_GROUP_REQUIRED' using errcode='22023';
      end if;
      if p_require_live then
        v_settings:=private._contract_settings_effective_core_v1(v_row.client_id,null,v_scope_date,'DAILY',null);
        if not coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)
          or not coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
          or not coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false)
          or coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
          or coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
          or coalesce((v_settings->'values'->>'self_bill')::boolean,true) then
          raise exception 'WEEKLY_SOURCE_RULE_SIGNED_GROUP_REQUIRED' using errcode='22023'; end if;
      end if;
    elsif v_agency then
      if (v_policy->>'self_bill_enabled')::boolean
        or (p_require_live and (v_policy->>'authority_mode'<>'SOURCE_AUTHORITY' or v_policy->>'document_mode'<>'CHECK_ONLY'))
        or (not p_require_live and not ((v_policy->>'authority_mode'='SOURCE_AUTHORITY' and v_policy->>'document_mode'='CHECK_ONLY')
          or (v_policy->>'authority_mode'='TIMESHEET_AUTHORITY' and v_policy->>'document_mode'='INVOICE_EVIDENCE_REQUIRED'))) then
        raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
      end if;
      if exists(select 1 from public.weekly_source_group_rule_assignments a
        where a.source_group_id=p_group_id and a.feature_purpose='SIGNED_CHECK_ONLY'
          and daterange(a.effective_from_week_ending,coalesce(a.effective_to_week_ending,'infinity'::date),'[]')
            && daterange(p_from,coalesce(p_to,'infinity'::date),'[]')) then
        raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
      end if;
    elsif v_policy->>'authority_mode'<>'SOURCE_AUTHORITY' or v_policy->>'document_mode'<>'CHECK_ONLY'
      or not (v_policy->>'self_bill_enabled')::boolean then
      raise exception 'WEEKLY_SOURCE_RULE_R15' using errcode='22023';
    end if;
    -- SP-20 has no installed first-use witness in this source stage. Fail closed.
    if p_values is not null and (v_policy->>'source_fixed_expenses_enabled')::boolean then
      raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023';
    end if;
    perform private.weekly_source_magnit_contract_price_assert_v1(p_values,v_policy);
    if v_agency and coalesce((select cs.self_bill_no_invoices_sent from public.client_settings cs
      where cs.client_id=v_row.client_id and (cs.effective_from is null or cs.effective_from<=v_scope_date)
      order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false) then
      raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
    end if;
    if v_agency and exists(select 1 from public.contracts c where c.client_id=v_row.client_id
      and c.weekly_timesheet_source::text='HEALTHROSTER'
      and daterange(c.start_date,c.end_date,'[]') && daterange(v_scope_date,coalesce(p_to,'infinity'::date),'[]')
      and exists(select 1 from public.weekly_source_group_clients gc where gc.source_group_id=p_group_id
        and gc.client_id=c.client_id and greatest(c.start_date,v_scope_date) between gc.valid_from and coalesce(gc.valid_to,'infinity'::date))
      and (coalesce(c.self_bill,false) or coalesce(c.no_timesheet_required,false) or coalesce(c.is_nhsp,false))) then
      raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
    end if;
    -- Contract overrides are resolved by the established dated settings owner.
    for v_contract_context in select c.id,greatest(c.start_date,v_scope_date) as work_date
      from public.contracts c where c.client_id=v_row.client_id and c.weekly_timesheet_source::text='HEALTHROSTER'
        and daterange(c.start_date,c.end_date,'[]') && daterange(v_scope_date,coalesce(p_to,'infinity'::date),'[]')
        and exists(select 1 from public.weekly_source_group_clients gc where gc.source_group_id=p_group_id
          and gc.client_id=c.client_id and greatest(c.start_date,v_scope_date) between gc.valid_from and coalesce(gc.valid_to,'infinity'::date))
    loop
      v_settings:=private._contract_settings_effective_core_v1(v_row.client_id,v_contract_context.id,v_contract_context.work_date,'WEEKLY',null);
      v_contract_policy:=private._weekly_source_effective_policy_v1(v_row.client_id,v_contract_context.id,v_contract_context.work_date);
      if p_values is not null and coalesce((v_contract_policy->>'source_fixed_expenses_enabled')::boolean,false) then
        raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023'; end if;
      perform private.weekly_source_magnit_contract_price_assert_v1(p_values,v_contract_policy);
      if v_agency and (coalesce((v_settings->'values'->>'self_bill')::boolean,true)
        or coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
        or coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
        or not coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
        or not coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false)
        or not coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)) then
        raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
      end if;
    end loop;
  end loop;
  if p_require_live and v_count=0 then
    raise exception 'WEEKLY_SOURCE_RULE_GROUP_MEMBERS_REQUIRED' using errcode='22023';
  end if;
end;
$function$;
create or replace function private.weekly_source_is_import_authoritative_v1(p_client_id uuid,p_contract_id uuid,p_work_date date)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_policy jsonb;
begin
  if p_client_id is null or p_work_date is null or not exists(select 1
    from public.weekly_source_group_clients gc join public.weekly_source_groups g on g.id=gc.source_group_id
    where gc.client_id=p_client_id and g.active and g.source_family='ROSTER'
      and p_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)) then return false; end if;
  if p_contract_id is not null and not exists(select 1 from public.contracts c
    where c.id=p_contract_id and c.client_id=p_client_id) then return false; end if;
  if not exists(select 1 from public.weekly_source_client_policies cp join public.weekly_source_groups g on g.id=cp.source_group_id
    join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=cp.client_id
    where cp.client_id=p_client_id and g.active and g.source_family='ROSTER'
      and p_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
      and p_work_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)) then return false; end if;
  v_policy:=private._weekly_source_effective_policy_v1(p_client_id,p_contract_id,p_work_date);
  return v_policy->>'source_family'='ROSTER' and v_policy->>'authority_mode'='SOURCE_AUTHORITY'
    and v_policy->>'document_mode'='CHECK_ONLY';
end;
$function$;
create or replace function private.weekly_source_effective_rule_set_v1(p_client_id uuid,p_cycle_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare normal jsonb; auth jsonb; result jsonb;
begin
  normal:=private.weekly_source_operational_effective_rule_set_v1(p_client_id,p_cycle_id);
  if normal is not null then return normal; end if;
  auth:=private.weekly_source_rule_qualification_context_v1(p_client_id,p_cycle_id);
  if auth is null then return null; end if;
  select jsonb_build_object('contract','WEEKLY_SOURCE_EFFECTIVE_RULE_SET_V1','source_group_id',g.id,
    'assignment_id',a.id,'assignment_version',a.version,'rule_set_id',rules.id,'rule_set_version_id',v.id,
    'version_number',v.version_number,'values_sha256',v.values_sha256,'values',v.values_json) into strict result
  from public.weekly_source_cycles c join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
  join public.weekly_source_group_clients member on member.source_group_id=g.id and member.client_id=p_client_id
    and c.finalisation_week_ending between member.valid_from and coalesce(member.valid_to,'infinity'::date)
  join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.id=(auth->>'assignment_id')::uuid and not a.feature_enabled
    and c.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
  join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id and v.id=(auth->>'rule_set_version_id')::uuid
  join public.weekly_source_rule_sets rules on rules.id=v.rule_set_id and rules.agency_id=g.agency_id and rules.environment=g.environment
  where c.id=p_cycle_id;
  return result;
end;
$function$;
create or replace function private.weekly_source_has_self_bill_rules_v1(p_client_id uuid,p_cycle_id uuid)
returns boolean language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
  select private.weekly_source_effective_rule_set_v1(p_client_id,p_cycle_id) is not null
$function$;
create or replace function private.weekly_source_agency_invoice_check_only_v1(p_timesheet_id uuid)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ts public.timesheets%rowtype; v_contract public.contracts%rowtype;
  v_cycle uuid; v_rules jsonb; v_policy jsonb; v_settings jsonb;
begin
  select * into v_ts from public.timesheets t where t.timesheet_id=p_timesheet_id and t.is_current
    and t.sheet_scope::text='WEEKLY' and t.status::text<>'REVOKED' and t.revoked_at is null and t.archived_at_utc is null;
  if not found or v_ts.contract_id is null or v_ts.week_ending_date is null then return false; end if;
  select * into v_contract from public.contracts c where c.id=v_ts.contract_id
    and c.weekly_timesheet_source::text='HEALTHROSTER';
  if not found or v_contract.self_bill or coalesce(v_contract.no_timesheet_required,false)
    or coalesce(v_contract.is_nhsp,false) then return false; end if;
  if nullif(v_ts.settings_authority_json->>'client_id','') is not null
    and v_ts.settings_authority_json->>'client_id'<>v_contract.client_id::text then return false; end if;
  if v_contract.start_date>v_ts.week_ending_date or v_contract.end_date<v_ts.week_ending_date-6
    or exists(select 1 from public.timesheets_financials f where f.timesheet_id=p_timesheet_id
      and f.is_current and f.timesheet_version=v_ts.version and f.client_id is distinct from v_contract.client_id) then return false; end if;
  select c.id into v_cycle from public.weekly_source_cycles c
    join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
    join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=v_contract.client_id
      and v_ts.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    where c.finalisation_week_ending=v_ts.week_ending_date;
  if v_cycle is null then return false; end if;
  v_rules:=private.weekly_source_effective_rule_set_v1(v_contract.client_id,v_cycle);
  if v_rules is null or v_rules->'values'->>'invoice_sender'<>'AGENCY_INVOICE' then return false; end if;
  if not private.weekly_source_is_import_authoritative_v1(v_contract.client_id,v_contract.id,v_ts.week_ending_date) then return false; end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_ts.week_ending_date);
  if coalesce((v_policy->>'self_bill_enabled')::boolean,true) then return false; end if;
  if coalesce((select cs.self_bill_no_invoices_sent from public.client_settings cs
    where cs.client_id=v_contract.client_id and (cs.effective_from is null or cs.effective_from<=v_ts.week_ending_date)
    order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false) then return false; end if;
  v_settings:=private._contract_settings_effective_core_v1(v_contract.client_id,v_contract.id,v_ts.week_ending_date,'WEEKLY',null);
  return coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)
    and not coalesce((v_settings->'values'->>'self_bill')::boolean,true)
    and not coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
    and not coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
    and coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
    and coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false);
end;
$function$;
create or replace function private.weekly_source_signed_check_only_v1(p_timesheet_id uuid)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ts public.timesheets%rowtype; v_contract public.contracts%rowtype;
  v_client uuid; v_client_count integer; v_work_date date; v_policy jsonb; v_route record;
begin
  select * into v_ts from public.timesheets t where t.timesheet_id=p_timesheet_id and t.is_current
    and t.sheet_scope::text in ('WEEKLY','DAILY') and t.status::text not in ('REVOKED','ARCHIVED','CANCELLED') and t.revoked_at is null and t.archived_at_utc is null;
  if not found or v_ts.week_ending_date is null then return false; end if;
  if v_ts.contract_id is not null then
    select * into v_contract from public.contracts c where c.id=v_ts.contract_id;
    if not found or v_contract.self_bill or coalesce(v_contract.no_timesheet_required,false)
      or coalesce(v_contract.is_nhsp,false) then return false; end if;
    v_client:=v_contract.client_id;
    if nullif(v_ts.settings_authority_json->>'client_id','') is not null
      and v_ts.settings_authority_json->>'client_id'<>v_client::text then return false; end if;
    if exists(select 1 from public.timesheets_financials f where f.timesheet_id=p_timesheet_id
      and f.is_current and f.timesheet_version=v_ts.version and f.client_id is distinct from v_client) then return false; end if;
  elsif v_ts.sheet_scope::text='DAILY' then
    select count(distinct f.client_id),(array_agg(distinct f.client_id))[1] into v_client_count,v_client
      from public.timesheets_financials f where f.timesheet_id=p_timesheet_id and f.is_current
        and f.timesheet_version=v_ts.version and f.client_id is not null;
    if v_client_count<>1 then return false; end if;
    if nullif(v_ts.settings_authority_json->>'client_id','') is not null
      and v_ts.settings_authority_json->>'client_id'<>v_client::text then return false; end if;
  else return false;
  end if;
  v_work_date:=case when v_ts.sheet_scope::text='DAILY' then (v_ts.worked_start_iso at time zone 'Europe/London')::date
    else v_ts.week_ending_date end;
  if v_work_date is null or v_work_date>v_ts.week_ending_date or v_work_date<v_ts.week_ending_date-6 then return false; end if;
  if not exists(select 1 from public.weekly_source_group_clients gc
    join public.weekly_source_groups g on g.id=gc.source_group_id and g.active and g.source_family='ROSTER'
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.feature_enabled
      and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
      and v_ts.week_ending_date between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
    where gc.client_id=v_client and v_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
      and v_ts.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)) then return false; end if;
  if not exists(select 1 from public.weekly_source_client_policies cp
    join public.weekly_source_group_clients gc on gc.source_group_id=cp.source_group_id and gc.client_id=cp.client_id
    where cp.client_id=v_client and v_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    and v_work_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)) then return false; end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_client,v_ts.contract_id,v_work_date);
  if v_policy->>'source_family'<>'ROSTER' or v_policy->>'authority_mode'<>'TIMESHEET_AUTHORITY'
    or v_policy->>'document_mode'<>'INVOICE_EVIDENCE_REQUIRED' or coalesce((v_policy->>'self_bill_enabled')::boolean,true) then return false; end if;
  select * into v_route from public._import_review_effective_authority_core_v1(
    case when v_ts.sheet_scope::text='DAILY' then 'HR_DAILY' else 'HR_WEEKLY' end,v_ts.contract_id,v_client,v_work_date);
  return coalesce(v_route.route_eligible,false) and coalesce(v_route.validation_eligible,false)
    and not coalesce(v_route.import_authoritative,true) and v_route.authority_mode='VALIDATION_ONLY'
    and not coalesce(v_route.effective_is_nhsp,true) and not coalesce(v_route.effective_no_timesheet_required,true);
end;
$function$;
create or replace function private.weekly_source_configured_agency_invoice_v1(
  p_client_id uuid,p_group_id uuid,p_agency_id uuid,p_environment text,p_date date
) returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_settings jsonb; v_group_id uuid:=p_group_id;
begin
  if p_client_id is null or p_date is null then return false; end if;
  if v_group_id is null then
    select gc.source_group_id into v_group_id from public.weekly_source_group_clients gc
      join public.weekly_source_groups g on g.id=gc.source_group_id and g.active and g.source_family='ROSTER'
        and g.agency_id=p_agency_id and g.environment=p_environment
      where gc.client_id=p_client_id and p_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date);
  end if;
  if v_group_id is null or not exists(select 1 from public.weekly_source_groups g
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id
    join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
    join public.weekly_source_rule_sets r on r.id=v.rule_set_id and r.agency_id=g.agency_id and r.environment=g.environment
    where g.id=v_group_id and g.active and g.source_family='ROSTER' and g.agency_id=p_agency_id and g.environment=p_environment
      and a.feature_enabled and a.feature_purpose='RULE_SET'
      and p_date between a.effective_from_week_ending-6 and coalesce(a.effective_to_week_ending,'infinity'::date)
      and v.values_json->>'invoice_sender'='AGENCY_INVOICE') then return false; end if;
  if exists(select 1 from public.weekly_source_group_clients gc join public.weekly_source_group_rule_assignments a on a.source_group_id=gc.source_group_id
      where gc.client_id=p_client_id and a.feature_purpose='SIGNED_CHECK_ONLY'
        and p_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
        and p_date between a.effective_from_week_ending-6 and coalesce(a.effective_to_week_ending,'infinity'::date)) then return false; end if;
  if coalesce((select cs.self_bill_no_invoices_sent from public.client_settings cs
      where cs.client_id=p_client_id and (cs.effective_from is null or cs.effective_from<=p_date)
      order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false) then return false; end if;
  if not exists(select 1 from public.contracts c where c.client_id=p_client_id
      and c.weekly_timesheet_source::text='HEALTHROSTER' and c.end_date>=p_date)
    or exists(select 1 from public.contracts c where c.client_id=p_client_id
      and c.weekly_timesheet_source::text<>'NONE' and c.end_date>=p_date
      and (c.weekly_timesheet_source::text<>'HEALTHROSTER' or c.self_bill or coalesce(c.no_timesheet_required,false) or coalesce(c.is_nhsp,false))) then return false; end if;
  for v_settings in select private._contract_settings_effective_core_v1(c.client_id,c.id,greatest(c.start_date,p_date),'WEEKLY',null)
    from public.contracts c where c.client_id=p_client_id and c.weekly_timesheet_source::text='HEALTHROSTER' and c.end_date>=p_date
  loop
    if not coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)
      or not coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
      or not coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false)
      or coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
      or coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
      or coalesce((v_settings->'values'->>'self_bill')::boolean,true) then return false; end if;
  end loop;
  return true;
end;
$function$;
create or replace function private.bpay_next_initial_tsfin_bind_v1(p_root_authorisation_id uuid)
returns table(binding_version_id uuid,canonical_work_id uuid,canonical_component_key text,binding_revision bigint,qualification_state text)
language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_candidate uuid;v_work uuid;cert jsonb;origin jsonb;basis jsonb;component jsonb;
  l private.bpay_next_approved_line%rowtype;b private.bpay_next_source_component_binding%rowtype;
  v private.bpay_next_source_component_binding_version%rowtype;revision private.bpay_next_work_revision%rowtype;
  id_new uuid;digest bytea;n integer;
begin
  if p_root_authorisation_id is null then raise exception 'BPAY_NEXT_INITIAL_AUTH_ID_REQUIRED' using errcode='22023';end if;
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then raise exception 'BPAY_NEXT_BINDING_NEXT_REQUIRED' using errcode='55000';end if;
  select w.candidate_id,w.id into v_candidate,v_work
    from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id
    where r.source_event_id=p_root_authorisation_id;
  if not found then raise exception 'BPAY_NEXT_INITIAL_REVISION_REQUIRED' using errcode='55000';end if;
  select r.* into strict revision from private.bpay_next_work_revision r where r.source_event_id=p_root_authorisation_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  if not found then raise exception 'BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED' using errcode='55000';end if;
  perform 1 from private.bpay_next_work where id=v_work and candidate_id=v_candidate for update;
  perform 1 from private.bpay_next_work_revision where id=revision.id and work_id=v_work for share;
  -- Exact stored replay precedes current I-1 discovery and NEVER advances the pointer.
  select count(*) into n from private.bpay_next_source_component_binding_version x
    where x.initial_root_authorisation_id=p_root_authorisation_id;
  if n>0 then
    if n<>revision.expected_line_count then raise exception 'BPAY_NEXT_INITIAL_PARTIAL_REPLAY' using errcode='23514';end if;
    for v in select x.* from private.bpay_next_source_component_binding_version x
      where x.initial_root_authorisation_id=p_root_authorisation_id order by x.canonical_component_key collate "C"
    loop
      if v.origin_kind<>'INITIAL_AUTHORISED_TSFIN_V1' or v.canonical_work_id<>v_work or v.approval_revision_id<>revision.id then
        raise exception 'BPAY_NEXT_INITIAL_REPLAY_CONFLICT' using errcode='23514';end if;
      select x.* into strict b from private.bpay_next_source_component_binding x
        where x.canonical_work_id=v.canonical_work_id and x.canonical_component_key=v.canonical_component_key;
      binding_version_id:=v.id;canonical_work_id:=v.canonical_work_id;canonical_component_key:=v.canonical_component_key;
      binding_revision:=v.binding_revision;qualification_state:=case when b.current_binding_version_id=v.id then 'REGISTERED' else 'SUPERSEDED' end;
      return next;
    end loop;return;
  end if;
  cert:=private.bpay_next_initial_tsfin_certificate_v1(p_root_authorisation_id);
  origin:=cert->'origin';basis:=cert->'basis';
  -- Empty genuine I-1 inventory creates no invented component/binding/position.
  for component in select value from jsonb_array_elements(cert->'components') order by value->>'component_id'
  loop
    select x.* into strict l from private.bpay_next_approved_line x
      where x.revision_id=revision.id and x.source_component_id=(component->>'component_id')::uuid;
    if exists(select 1 from private.bpay_next_source_component_binding x where x.candidate_id=v_candidate and x.source_component_id=l.source_component_id) then
      raise exception 'BPAY_NEXT_INITIAL_COMPONENT_ALREADY_BOUND' using errcode='23514';end if;
    id_new:=gen_random_uuid();
    digest:=sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v_work,l.component_key,
      revision.id,l.id,p_root_authorisation_id,cert->>'certificate_digest',cert->>'command_id',component->>'component_sha256')::text,'UTF8'));
    insert into private.bpay_next_source_component_binding_version
      (id,canonical_work_id,canonical_component_key,candidate_id,source_component_id,binding_revision,
       approval_work_id,approval_revision_id,approval_component_key,approved_line_id,component_state,
       approval_source_channel,tax_treatment,classification_line_id,component_sha256,
       publication_command_id_1,publication_work_id_1,publication_revision_id_1,binding_digest,
       origin_kind,initial_root_authorisation_id,initial_financial_snapshot_id,initial_root_timesheet_id,
       initial_root_version,initial_authorisation_generation,initial_authorised_row_signature,
       initial_financial_snapshot_digest,initial_inventory_digest,initial_entitlement_digest,
       initial_detail_digest,initial_policy_digest,initial_certificate_digest)
    values(id_new,v_work,l.component_key,v_candidate,l.source_component_id,1,v_work,revision.id,l.component_key,l.id,'PRESENT',
      revision.source_pay_channel,l.tax_treatment,l.id,decode(component->>'component_sha256','hex'),
      (cert->>'command_id')::uuid,v_work,revision.id,digest,'INITIAL_AUTHORISED_TSFIN_V1',p_root_authorisation_id,
      (origin->>'financial_snapshot_id')::uuid,(origin->>'root_timesheet_id')::uuid,(origin->>'root_version')::integer,
      (origin->>'authorisation_generation')::integer,origin->>'authorised_row_signature',
      decode(origin->>'financial_snapshot_digest','hex'),decode(basis->>'inventory_digest','hex'),decode(basis->>'entitlement_digest','hex'),
      decode(basis->>'detail_digest','hex'),decode(basis->>'policy_fingerprint','hex'),decode(cert->>'certificate_digest','hex'));
    insert into private.bpay_next_source_component_binding
      (canonical_work_id,canonical_component_key,candidate_id,source_component_id,canonical_revision_id,canonical_line_id,current_binding_version_id,binding_revision)
    values(v_work,l.component_key,v_candidate,l.source_component_id,revision.id,l.id,id_new,1);
    binding_version_id:=id_new;canonical_work_id:=v_work;canonical_component_key:=l.component_key;binding_revision:=1;
    qualification_state:='REGISTERED';return next;
  end loop;
end
$function$;
create or replace function private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare t public.timesheets%rowtype; c public.contracts%rowtype; f public.timesheets_financials%rowtype;
  w public.candidate_submission_workflows%rowtype; h public.candidate_submission_components%rowtype;
  sig public.candidate_submission_components%rowtype; v_client uuid; v_candidate uuid;
  v_count integer; v_date date; v_input jsonb; v_patch jsonb; v_evidence jsonb; v_reason text;
  v_artifact_generation integer; v_final_receipt jsonb;
begin
  select * into t from public.timesheets where timesheet_id=p_timesheet_id;
  if t.timesheet_id is null or not t.is_current or t.archived_at_utc is not null or t.revoked_at is not null
    or t.status::text in('REVOKED','ARCHIVED','CANCELLED') or t.line_type::text<>'HOURS' or t.sheet_scope::text not in('WEEKLY','DAILY') then
    return jsonb_build_object('actual_submitted',false,'support_state','NOT_SUBMITTED','reason','NO_CURRENT_HOURS_TIMESHEET');
  end if;
  -- A waiting Office proposal is never a worker submission. Applied pair
  -- selection excludes displaced original and reversal before aggregation.
  if t.correction_kind='WHOLE_TIMESHEET_REVERSAL'
    or exists(select 1 from private.timesheet_correction_requests r where
      (r.state='WAITING' and r.replacement_timesheet_id=t.timesheet_id)
      or (r.state='APPLIED' and r.reversed_timesheet_id=t.timesheet_id)) then
    return jsonb_build_object('actual_submitted',false,'support_state','NOT_SUBMITTED','reason','CORRECTION_MEMBER_NOT_COUNTED');
  end if;
  select count(*),(array_agg(id order by id))[1] into v_count,f.id from public.timesheets_financials
    where timesheet_id=t.timesheet_id and is_current and timesheet_version=t.version;
  if v_count>1 then
    return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','CURRENT_FINANCIAL_IDENTITY_AMBIGUOUS');
  end if;
  if f.id is not null then select * into f from public.timesheets_financials where id=f.id; end if;
  if t.contract_id is not null then
    select * into c from public.contracts where id=t.contract_id;
    if c.id is null then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','ACTUAL_CONTRACT_UNAVAILABLE');
    end if;
    v_client:=c.client_id; v_candidate:=c.candidate_id;
    if f.id is not null and (f.client_id is distinct from v_client or f.candidate_id is distinct from v_candidate) then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','ACTUAL_FINANCIAL_OWNER_MISMATCH');
    end if;
  elsif t.sheet_scope::text='DAILY' and f.id is not null then
    v_client:=f.client_id; v_candidate:=f.candidate_id;
  else
    return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','NULL_CONTRACT_DAILY_OWNER_UNAVAILABLE');
  end if;
  if v_client is null or v_candidate is null or (nullif(t.settings_authority_json->>'client_id','') is not null
    and lower(t.settings_authority_json->>'client_id') is distinct from v_client::text) then
    return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','ACTUAL_OWNER_UNAVAILABLE');
  end if;
  -- Exact target binding can precede final TS workflow bookkeeping. Require
  -- one real submitted HOURS workflow for the actual Candidate and target.
  select count(*),(array_agg(id order by id))[1] into v_count,w.id
  from public.candidate_submission_workflows wf where (wf.target_timesheet_id=t.timesheet_id
    or (wf.state='FINALISED' and wf.workflow_kind='CONTRACT_COMBINED' and wf.id=t.candidate_workflow_id
      and exists(select 1 from public.candidate_submission_components actual_hours
        where actual_hours.workflow_id=wf.id and actual_hours.workflow_generation=wf.generation-1
          and actual_hours.component_kind='HOURS_TIMESHEET' and actual_hours.state='IMMUTABLE'
          and actual_hours.required and actual_hours.timesheet_id=t.timesheet_id)))
    and wf.candidate_id=v_candidate and wf.workflow_kind in('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
    and wf.state in('WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
      'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
      'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
    and wf.worker_submitted_at_utc is not null and wf.cancelled_at_utc is null
    and (t.candidate_workflow_id is null or wf.id=t.candidate_workflow_id);
  if v_count>1 then
    return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','SUBMISSION_WORKFLOW_AMBIGUOUS');
  end if;
  if w.id is not null then
    select * into w from public.candidate_submission_workflows where id=w.id;
    if jsonb_typeof(w.immutable_submission_json) is distinct from 'object'
      or octet_length(w.immutable_submission_sha256) is distinct from 32
      or private._candidate_sha256_jsonb_v1(w.immutable_submission_json) is distinct from w.immutable_submission_sha256
      or w.contract_id is distinct from t.contract_id
      or (t.sheet_scope::text='WEEKLY' and w.week_ending_date is distinct from t.week_ending_date)
      or (t.sheet_scope::text='DAILY' and (w.week_ending_date is not null or w.contract_week_id is not null))
      or w.scope is distinct from t.sheet_scope::text
      or (t.sheet_scope::text='DAILY') is distinct from (w.workflow_kind='DAILY') then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','IMMUTABLE_SUBMISSION_IDENTITY_UNPROVED');
    end if;
    -- Native WORKER_SUBMIT owns the signed artifact generation. Ordinary
    -- FINALISED bookkeeping advances the workflow exactly once without
    -- replacing those immutable components or the TS artifact binding.
    -- Accept that finite arm only with the actual retained native receipt.
    v_artifact_generation:=w.generation;
    if w.state='FINALISED' then
      select count(*),(jsonb_agg(a.after_json order by a.id))->0 into v_count,v_final_receipt
        from public.audit_events a where a.object_type='candidate_workflow_finalisation_completion'
          and a.object_id_text=w.id::text and a.action='CANDIDATE_WORKFLOW_FINALISATION_COMPLETED'
          and a.before_json->>'contract_version'='CANDIDATE_FINALISATION_COMPLETION_V1'
          and a.before_json->>'workflow_generation'=(w.generation-1)::text
          and a.after_json->'ok'='true'::jsonb and a.after_json->>'workflow_id'=w.id::text
          and a.after_json->>'generation'=w.generation::text and a.after_json->>'state'='FINALISED'
          and (a.after_json->>'timesheet_id'=t.timesheet_id::text
            or (w.workflow_kind='CONTRACT_COMBINED'
              and a.after_json#>>'{canonical_result,hours_timesheet_id}'=t.timesheet_id::text
              and a.after_json#>'{canonical_result,hours_result,ok}'='true'::jsonb))
          and a.after_json->>'canonical_financial_sha256_hex'=encode(w.canonical_financial_sha256,'hex')
          and a.before_json->>'finalisation_identity_sha256'=encode(private._candidate_sha256_jsonb_v1(
            a.before_json->'finalisation_identity'),'hex');
      if v_count<>1 or w.finalised_at_utc is null or octet_length(w.canonical_financial_sha256) is distinct from 32 then
        return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','NATIVE_FINALISATION_ARTIFACT_BINDING_UNPROVED');
      end if;
      v_artifact_generation:=w.generation-1;
    end if;
    -- A freshly re-submitted pending claim can precede native TS bookkeeping;
    -- its exact current target, immutable claim and signature own the new
    -- witness. A future/absent TS generation is never accepted, and FINALISED
    -- still requires its exact retained artifact generation/receipt binding.
    if t.candidate_workflow_id is not null and (t.candidate_workflow_generation is null
      or t.candidate_workflow_generation<1 or t.candidate_workflow_generation>v_artifact_generation
      or (w.state='FINALISED' and t.candidate_workflow_generation is distinct from v_artifact_generation)) then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','NATIVE_ARTIFACT_GENERATION_MISMATCH');
    end if;
    v_input:=coalesce(w.immutable_submission_json->'hours_submission',w.immutable_submission_json);
    if jsonb_typeof(v_input) is distinct from 'object' then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','IMMUTABLE_HOURS_SUBMISSION_UNAVAILABLE');
    end if;
    v_patch:=coalesce(v_input->'timesheet_patch_json',v_input);
    if jsonb_typeof(v_patch) is distinct from 'object' or (t.sheet_scope::text='WEEKLY'
      and jsonb_typeof(coalesce(v_patch->'actual_schedule_json',v_input#>'{canonical_tsfin_snapshot,actual_schedule_json}')) is distinct from 'array') then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','FROZEN_WORKER_HOURS_CLAIM_UNAVAILABLE');
    end if;
    select count(*),(array_agg(id order by id))[1] into v_count,h.id
    from public.candidate_submission_components component where component.workflow_id=w.id
      and component.workflow_generation=v_artifact_generation and component.component_kind='HOURS_TIMESHEET'
      and component.required and component.state='IMMUTABLE' and component.immutable_at_utc is not null
      and (component.timesheet_id is null or component.timesheet_id=t.timesheet_id);
    if v_count<>1 then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','IMMUTABLE_HOURS_COMPONENT_UNPROVED');
    end if;
    select * into h from public.candidate_submission_components where id=h.id;
    -- Electronic submission requires its native immutable Candidate signature.
    -- Paper workflows without that receipt remain a precise engineering gate;
    -- a later verified manual/QR source can qualify through the legacy arm.
    select * into sig from public.candidate_submission_components where id=w.candidate_signature_component_id
      and workflow_id=w.id and workflow_generation=v_artifact_generation and document_role='CANDIDATE_SIGNATURE'
      and state='IMMUTABLE' and immutable_at_utc is not null;
    if sig.id is null or w.candidate_signed_at_utc is null or nullif(btrim(sig.storage_key),'') is null
      or octet_length(sig.source_content_sha256) is distinct from 32
      or sig.source_content_sha256 is distinct from w.candidate_signature_sha256 then
      v_reason:='CANDIDATE_SUBMISSION_SIGNATURE_UNPROVED';
    else
      if t.sheet_scope::text='DAILY' then
        -- This unchanged native helper performs the actual submitted-hours
        -- patch projection; no guessed Daily JSON layout or booked fallback.
        v_patch:=private._candidate_daily_canonical_save_input_v1(w.id,w.generation)->'timesheet_patch_json';
        if not pg_input_is_valid(coalesce(v_patch->>'worked_start_iso',''),'timestamp with time zone')
          or not pg_input_is_valid(coalesce(v_patch->>'worked_end_iso',''),'timestamp with time zone') then
          return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','SUBMITTED_DAILY_WORK_DATE_UNAVAILABLE');
        end if;
        v_date:=( (v_patch->>'worked_start_iso')::timestamptz at time zone 'Europe/London')::date;
        if v_date is distinct from w.work_date or v_date<t.week_ending_date-6 or v_date>t.week_ending_date then
          return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','SUBMITTED_DAILY_WORK_DATE_MISMATCH');
        end if;
      else v_date:=t.week_ending_date; end if;
      return jsonb_build_object('actual_submitted',true,'support_state','PROVED','authority','CANDIDATE_IMMUTABLE_HOURS',
        'timesheet_id',t.timesheet_id,'timesheet_version',t.version,'client_id',v_client,'candidate_id',v_candidate,
        'contract_id',t.contract_id,'sheet_scope',t.sheet_scope,'week_ending_date',t.week_ending_date,'work_date',v_date,
        'workflow_id',w.id,'workflow_generation',v_artifact_generation,'submission_sha256',encode(w.immutable_submission_sha256,'hex'),
        'hours_component_id',h.id,'candidate_signature_component_id',sig.id,'submitted_at_utc',w.worker_submitted_at_utc,
        'manager_approved_at_utc',w.manager_approved_at_utc,'workflow_state',w.state);
    end if;
  end if;
  -- Reuse the normal read-only complete source/document contract. A manual
  -- key, planned clocks or bare schedule alone can never qualify this arm.
  v_evidence:=public._import_review_query_evidence_core_v1(t.timesheet_id);
  if coalesce((v_evidence->>'source_complete')::boolean,false)
    and coalesce((v_evidence->>'document_ready')::boolean,false) then
    v_date:=case when t.sheet_scope::text='DAILY' then (t.worked_start_iso at time zone 'Europe/London')::date else t.week_ending_date end;
    if v_date is null or v_date<t.week_ending_date-6 or v_date>t.week_ending_date then
      return jsonb_build_object('actual_submitted',false,'support_state','UNPROVED','reason','LEGACY_ACTUAL_WORK_DATE_UNAVAILABLE');
    end if;
    return jsonb_build_object('actual_submitted',true,'support_state','PROVED','authority','VERIFIED_LEGACY_SOURCE_DOCUMENT',
      'timesheet_id',t.timesheet_id,'timesheet_version',t.version,'client_id',v_client,'candidate_id',v_candidate,
      'contract_id',t.contract_id,'sheet_scope',t.sheet_scope,'week_ending_date',t.week_ending_date,'work_date',v_date,
      'document_version_id',v_evidence->'document_version_id','source_evidence_fingerprint',v_evidence->'evidence_fingerprint');
  end if;
  return jsonb_build_object('actual_submitted',false,'support_state',case when w.id is null
    and not coalesce((v_evidence->>'source_complete')::boolean,false) then 'NOT_SUBMITTED' else 'UNPROVED' end,
    'reason',coalesce(v_reason,'ACTUAL_SUBMISSION_SOURCE_WITNESS_UNAVAILABLE'));
end;
$function$;
create or replace function private.weekly_source_signed_submission_read_context_v1(
  p_timesheet_id uuid,p_source_group_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare t public.timesheets%rowtype; v_witness jsonb; v_client uuid; v_work_date date;
  v_policy jsonb; v_route record; v_native boolean;
begin
  v_witness:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
  if (v_witness->>'actual_submitted')::boolean is distinct from true then
    return jsonb_build_object('eligible',false,'submission_witness',v_witness,'read_only',true);
  end if;
  select * into t from public.timesheets where timesheet_id=p_timesheet_id;
  v_client:=(v_witness->>'client_id')::uuid; v_work_date:=(v_witness->>'work_date')::date;
  if not exists(select 1 from public.weekly_source_groups g
    join public.weekly_source_group_clients member on member.source_group_id=g.id and member.client_id=v_client
    join public.weekly_source_client_policies policy on policy.source_group_id=g.id and policy.client_id=v_client
    join public.weekly_source_group_rule_assignments assignment on assignment.source_group_id=g.id
      and assignment.feature_enabled and assignment.feature_purpose='SIGNED_CHECK_ONLY' and assignment.rule_set_version_id is null
    where g.id=p_source_group_id and g.active and g.source_family='ROSTER'
      and v_work_date between member.valid_from and coalesce(member.valid_to,'infinity'::date)
      and t.week_ending_date between member.valid_from and coalesce(member.valid_to,'infinity'::date)
      and v_work_date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)
      and t.week_ending_date between assignment.effective_from_week_ending and coalesce(assignment.effective_to_week_ending,'infinity'::date)
      and policy.authority_mode='TIMESHEET_AUTHORITY' and policy.document_mode='INVOICE_EVIDENCE_REQUIRED' and not policy.self_bill_enabled) then
    return jsonb_build_object('eligible',false,'submission_witness',v_witness,'read_only',true);
  end if;
  v_native:=private.weekly_source_signed_check_only_v1(p_timesheet_id);
  if v_native then
    return jsonb_build_object('eligible',true,'read_only',true,'submission_witness',v_witness,
      'route_state','NATIVE_ROUTE_AVAILABLE','query_route_available',true,'pending_materialisation',false);
  end if;
  -- This exception admits an actual submitted record to a read, never query,
  -- manager/correction/Send/Authorise or financial mutation. Client/Contract,
  -- Candidate, work date, workflow/generation and signature are already proved
  -- by the unchanged canonical submitted-hours projection in the witness.
  if t.sheet_scope::text<>'DAILY' or t.worked_start_iso is not null
    or v_witness->>'authority' is distinct from 'CANDIDATE_IMMUTABLE_HOURS' then
    return jsonb_build_object('eligible',false,'submission_witness',v_witness,'read_only',true);
  end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_client,t.contract_id,v_work_date);
  select * into v_route from public._import_review_effective_authority_core_v1('HR_DAILY',t.contract_id,v_client,v_work_date);
  if v_policy->>'source_family' is distinct from 'ROSTER' or v_policy->>'authority_mode' is distinct from 'TIMESHEET_AUTHORITY'
    or v_policy->>'document_mode' is distinct from 'INVOICE_EVIDENCE_REQUIRED'
    or coalesce((v_policy->>'self_bill_enabled')::boolean,true)
    or not coalesce(v_route.route_eligible,false) or not coalesce(v_route.validation_eligible,false)
    or coalesce(v_route.import_authoritative,true) or v_route.authority_mode is distinct from 'VALIDATION_ONLY'
    or coalesce(v_route.effective_is_nhsp,true) or coalesce(v_route.effective_no_timesheet_required,true) then
    return jsonb_build_object('eligible',false,'submission_witness',v_witness,'read_only',true);
  end if;
  return jsonb_build_object('eligible',true,'read_only',true,'submission_witness',v_witness,
    'route_state','SUBMITTED_PENDING_NATIVE_MATERIALISATION','query_route_available',false,
    'pending_materialisation',true,'actions_held',true,'action_hold_reason','NATIVE_DAILY_MATERIALISATION_PENDING');
end;
$function$;
create or replace function private.weekly_source_checklist_client_calendar_v1(
  p_client_id uuid,p_local_today date
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare authority jsonb; weekday integer; latest_week date;
begin
  if p_client_id is null or p_local_today is null then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_CLIENT_WEEK_AUTHORITY_UNAVAILABLE' using errcode='22023';
  end if;
  authority:=private._contract_settings_effective_core_v1(p_client_id,null,p_local_today,'OFFICE',null);
  if authority->>'authority_version' is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
    or authority->>'client_id' is distinct from p_client_id::text
    or not pg_input_is_valid(coalesce(authority#>>'{values,week_ending_weekday}',''),'integer')
    or coalesce(authority->>'authority_fingerprint','')!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_CLIENT_WEEK_AUTHORITY_UNAVAILABLE' using errcode='55000';
  end if;
  weekday:=(authority#>>'{values,week_ending_weekday}')::integer;
  if weekday not between 0 and 6 then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_CLIENT_WEEK_AUTHORITY_UNAVAILABLE' using errcode='55000';
  end if;
  -- A week ending today has not ended yet. Walk back from yesterday.
  latest_week:=p_local_today-1-((extract(dow from p_local_today-1)::integer-weekday+7)%7);
  return jsonb_build_object('contract','WEEKLY_SOURCE_CHECKLIST_CLIENT_CALENDAR_V1',
    'client_id',p_client_id,'local_today',p_local_today,'week_ending_weekday',weekday,
    'latest_ended_week',latest_week,'authority_fingerprint',authority->>'authority_fingerprint');
end;
$function$;
create or replace function private.weekly_source_check_only_query_rows_v1(
  p_source_cycle_id uuid,p_client_id uuid,p_filters jsonb
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare c public.weekly_source_cycles%rowtype; v_actor uuid; v_week date; v_limit integer:=50;
  v_cursor jsonb; v_binding text; v_revision text; v_rows jsonb; v_more boolean; v_last jsonb; v_admin boolean;
begin
  perform private._weekly_source_settings_assert_request_v1(p_filters,
    array['actor_user_id','source_group_id','week_ending','cursor','limit'], 'WEEKLY_SOURCE_CHECK_ONLY_FILTER_INVALID');
  v_actor:=nullif(p_filters->>'actor_user_id','')::uuid;
  if nullif(p_filters->>'week_ending','') is null or not pg_input_is_valid(p_filters->>'week_ending','date') then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_FILTER_INVALID' using errcode='22023';
  end if;
  v_week:=(p_filters->>'week_ending')::date;
  select * into c from public.weekly_source_cycles where id=p_source_cycle_id;
  if c.id is null or p_client_id is null or (c.scope_client_id is not null and c.scope_client_id<>p_client_id)
    or v_week not between c.finalisation_week_ending-6 and c.finalisation_week_ending
    or (nullif(p_filters->>'source_group_id','') is not null and (p_filters->>'source_group_id')::uuid<>c.source_group_id) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_UNAVAILABLE' using errcode='22023';
  end if;
  select lower(btrim(coalesce(role,'')))='admin' into v_admin from public.tms_users where id=v_actor and is_active;
  perform private.weekly_source_office_authority_v1(v_actor,case when v_admin then 'VIEW_SOURCE_PROGRESS' else 'VIEW_PAY_WORK' end,
    c.source_group_id,p_client_id,v_week);
  v_limit:=coalesce((p_filters->>'limit')::integer,50);
  if v_limit not between 1 and 100 then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_FILTER_INVALID' using errcode='22023'; end if;
  v_binding:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_BINDING_V1',
    jsonb_build_object('actor',v_actor,'view',case when v_admin then 'ADMIN' else 'PAY_READONLY' end,
      'cycle',c.id,'client',p_client_id,'filters',p_filters-'cursor'-'limit')),'hex');
  if nullif(p_filters->>'cursor','') is not null then
    begin
      if length(p_filters->>'cursor')>4096 then raise exception 'cursor'; end if;
      v_cursor:=convert_from(decode(p_filters->>'cursor','base64'),'UTF8')::jsonb;
      if v_cursor->>'contract' is distinct from 'WEEKLY_SOURCE_CHECK_ONLY_CURSOR_V1' or v_cursor->>'binding' is distinct from v_binding
        or jsonb_typeof(v_cursor->'after') is distinct from 'array' or jsonb_array_length(v_cursor->'after')<>3
        or coalesce(v_cursor->>'read_revision','')!~'^[0-9a-f]{64}$'
        or jsonb_typeof(v_cursor#>'{after,0}') is distinct from 'string'
        or jsonb_typeof(v_cursor#>'{after,1}') is distinct from 'string'
        or jsonb_typeof(v_cursor#>'{after,2}') is distinct from 'string'
        or not pg_input_is_valid(coalesce(v_cursor#>>'{after,1}',''),'date')
        or length(coalesce(v_cursor#>>'{after,2}','')) not between 1 and 512 then raise exception 'cursor'; end if;
      perform private._weekly_source_settings_assert_request_v1(v_cursor,array['contract','binding','read_revision','after'],'CURSOR');
    exception when others then raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end;
  end if;
  with actual as materialized (
    select t.*,s.witness,rctx.route_read,ca.id actual_candidate_id,cl.name client_name,coalesce(nullif(btrim(ca.first_name||' '||ca.last_name),''),ca.id::text) worker_name,
      private.weekly_source_query_ascii_fold_v1(coalesce(nullif(btrim(ca.first_name||' '||ca.last_name),''),ca.id::text)) collate "C" worker_sort,
      case when rctx.route_read->>'route_state'='NATIVE_AGENCY_INVOICE' then t.week_ending_date else (s.witness->>'work_date')::date end actual_work_date
    from public.timesheets t
    cross join lateral(select private.weekly_source_signed_submission_read_context_v1(t.timesheet_id,c.source_group_id) signed_read) signed_context
    cross join lateral(select case when signed_context.signed_read->'eligible'='true'::jsonb then signed_context.signed_read
      when private.weekly_source_agency_invoice_check_only_v1(t.timesheet_id) is true
        and private.weekly_source_effective_rule_set_v1(p_client_id,c.id)#>>'{values,invoice_sender}'='AGENCY_INVOICE' then
        jsonb_build_object('eligible',true,'read_only',true,'submission_witness',private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id),
          'route_state','NATIVE_AGENCY_INVOICE','query_route_available',true,'pending_materialisation',false)
      else signed_context.signed_read end route_read) rctx
    cross join lateral(select rctx.route_read->'submission_witness' witness) s
    left join public.contracts actual_contract on actual_contract.id=t.contract_id
    join public.clients cl on cl.id=p_client_id join public.candidates ca on ca.id=case
      when rctx.route_read->>'route_state'='NATIVE_AGENCY_INVOICE' then actual_contract.candidate_id else (s.witness->>'candidate_id')::uuid end
    where ((s.witness->>'actual_submitted')::boolean is true and s.witness->>'client_id'=p_client_id::text
      or (rctx.route_read->>'route_state'='NATIVE_AGENCY_INVOICE' and actual_contract.client_id=p_client_id))
      and t.week_ending_date between c.finalisation_week_ending-6 and c.finalisation_week_ending
      and t.week_ending_date=v_week
      and (rctx.route_read->>'eligible')::boolean is true
      and (rctx.route_read->>'route_state'='NATIVE_AGENCY_INVOICE' or exists(select 1 from public.weekly_source_groups g
        join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=p_client_id
        join public.weekly_source_client_policies cp on cp.source_group_id=g.id and cp.client_id=gc.client_id
        join public.weekly_source_group_rule_assignments ga on ga.source_group_id=g.id and ga.feature_enabled
          and ga.feature_purpose='SIGNED_CHECK_ONLY' and ga.rule_set_version_id is null
        where g.id=c.source_group_id and g.active and g.source_family='ROSTER'
          and (s.witness->>'work_date')::date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
          and t.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
          and (s.witness->>'work_date')::date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)
          and t.week_ending_date between ga.effective_from_week_ending and coalesce(ga.effective_to_week_ending,'infinity'::date)
          and cp.authority_mode='TIMESHEET_AUTHORITY' and cp.document_mode='INVOICE_EVIDENCE_REQUIRED' and not cp.self_bill_enabled))
  ), context as materialized (
    select a.*,tv.id validation_id,tv.status::text validation_status,tv.reason_code validation_reason,
      tv.last_source validation_import_id,tv.validated_at_utc,tf.id financial_id,tf.processing_status::text processing_status,
      r.id request_id,r.state request_state,r.state_revision request_state_revision,r.replacement_timesheet_id,
      pw.id replacement_workflow_id,pw.generation replacement_workflow_generation,pw.state replacement_workflow_state,
      pw.worker_submitted_at_utc replacement_submitted_at,pw.manager_approved_at_utc replacement_manager_approved_at,
      qi.id query_import_id,qi.uploaded_at_utc query_import_uploaded_at,qs.preview_generation,qs.state_version,
      qs.preview_fingerprint,qi.import_scope,qi.source_file_sha256,
      case when (a.route_read->>'query_route_available')::boolean is not true then null
        when qi.id is not null then private.weekly_source_issued_signed_query_evidence_v1(a.timesheet_id,qi.id)
        else public._import_review_query_evidence_core_v1(a.timesheet_id) end query_evidence
    from actual a
    left join lateral(select * from public.timesheet_validations v where v.timesheet_id=a.timesheet_id
      order by v.updated_at desc,v.created_at desc,v.id desc limit 1) tv on true
    left join public.timesheets_financials tf on tf.timesheet_id=a.timesheet_id and tf.is_current and tf.timesheet_version=a.version
    left join lateral(select * from private.timesheet_correction_requests request where request.state='WAITING'
      and request.reversed_timesheet_id=a.timesheet_id order by request.id limit 1) r on true
    left join public.timesheets pt on pt.timesheet_id=r.replacement_timesheet_id
    left join public.candidate_submission_workflows pw on pw.id=pt.candidate_workflow_id and pw.generation=pt.candidate_workflow_generation
    left join lateral(select hi.* from public.hr_imports hi
      where hi.pruned_at is null and hi.source_system::text in('HEALTHROSTER','HEALTHROSTER_DAILY')
        and (hi.client_id is null or hi.client_id=p_client_id)
        and (hi.id=tv.last_source or exists(select 1 from public.import_review_decisions d join public.import_review_states st on st.import_id=d.import_id
          and st.preview_generation=d.preview_generation and st.superseded_at_utc is null and st.abandoned_at_utc is null
          where d.import_id=hi.id and d.timesheet_id=a.timesheet_id and d.client_id=p_client_id and d.is_current))
      order by hi.uploaded_at_utc desc,hi.id desc limit 1) qi on true
    left join public.import_review_states qs on qs.import_id=qi.id
  ), rows as materialized (
    select x.worker_sort,x.actual_work_date,('timesheet:'||x.timesheet_id) collate "C" item_sort,
      jsonb_build_object('item_id','timesheet:'||x.timesheet_id,'timesheet_id',x.timesheet_id,'item_kind','ACTUAL_TIMESHEET',
      'source_group_id',c.source_group_id,'source_cycle_id',c.id,'client_id',p_client_id,'client',x.client_name,
      'candidate_id',x.actual_candidate_id,'worker',x.worker_name,'week_ending',x.week_ending_date,
      'work_date',x.actual_work_date,'sheet_scope',x.sheet_scope,'submission_witness',x.witness,
      'submission_read_context',x.route_read,
      'validation',jsonb_build_object('id',x.validation_id,'status',x.validation_status,'reason_code',x.validation_reason,
        'last_source',x.validation_import_id,'validated_at_utc',x.validated_at_utc),
      'ordinary_authorised',x.authorised_at_server is not null,'financial_id',x.financial_id,'processing_status',x.processing_status,
      'correction_owns_read_item',x.request_id is not null,'ordinary_query_reminder_suppressed',x.request_id is not null,
      'correction',case when x.request_id is not null then jsonb_build_object('request_id',x.request_id,'state',x.request_state,
        'state_revision',x.request_state_revision,'replacement_timesheet_id',x.replacement_timesheet_id,
        'workflow_id',x.replacement_workflow_id,'workflow_generation',x.replacement_workflow_generation,
        'workflow_state',x.replacement_workflow_state,'worker_submitted_at_utc',x.replacement_submitted_at,
        'manager_approved_at_utc',x.replacement_manager_approved_at) end,
      -- Pay work remains available for native subject admission, but invoice
      -- query evidence, recipients and delivery history are admin-only.
      'query_import_id',case when v_admin then x.query_import_id end,
      'query_preview',case when v_admin and x.query_import_id is not null then jsonb_build_object('import_id',x.query_import_id,
        'preview_generation',x.preview_generation,'state_version',x.state_version,'preview_fingerprint',x.preview_fingerprint) end,
      'query_evidence',case when v_admin then x.query_evidence end,
      'native_issues',case when v_admin and (x.route_read->>'query_route_available')::boolean is true
        then coalesce(issues.value,'[]'::jsonb) else '[]'::jsonb end,
      'unfinished_daily_review',review.unfinished_daily_review,
      -- One native open fact, before actor redaction. Retention and rendering
      -- consume this fact; neither owns another query/preview classifier.
      'open_read_item',x.request_id is not null
        or x.route_read->>'route_state' is not distinct from 'SUBMITTED_PENDING_NATIVE_MATERIALISATION'
        or x.authorised_at_server is null or review.unfinished_daily_review
        or ((x.route_read->>'query_route_available')::boolean is true and jsonb_array_length(coalesce(issues.value,'[]'::jsonb))>0)
        or x.validation_status is distinct from 'VALIDATION_OK' or x.processing_status is not distinct from 'READY_FOR_HR',
      'engineering_gate',case when x.validation_reason is not null and x.validation_status<>'VALIDATION_OK'
        and x.query_import_id is null then 'CURRENT_IMPORT_REVIEW_ISSUE_SNAPSHOT_UNAVAILABLE' end) value
    from context x
    cross join lateral(select case when x.sheet_scope::text='DAILY' and x.query_import_id is not null
      and upper(coalesce(x.import_scope,''))='HR_DAILY' then exists(select 1 from public.import_review_states ds
        where ds.import_id=x.query_import_id and ds.applied_at_utc is null and ds.superseded_at_utc is null and ds.abandoned_at_utc is null)
      else false end unfinished_daily_review) review
    left join lateral(select jsonb_agg(jsonb_build_object('action_id',d.action_id,'action_kind',d.action_kind,
      'import_id',d.import_id,'preview_generation',d.preview_generation,'timesheet_id',d.timesheet_id,'client_id',d.client_id,
      'evidence_fingerprint',d.evidence_fingerprint,'reason_code',d.summary_json->'reason_code','summary',d.summary_json,
      'issue_id',e.id,'issue_fingerprint',e.issue_fingerprint,'recipient_scope',e.recipient_scope,
      'sent_verified',e.delivery_history_status='SENT_VERIFIED' and delivery.status='SENT'
        and delivery.accepted_at_utc is not null and delivery.marked_at_utc is not null
        and item.marked_sent_at_utc is not null and (upper(mail.status::text)='SENT' or mail.sent_at is not null),
      'last_sent_at',e.last_sent_at,'sent_count',e.sent_count,'delivery_id',delivery.id,
      'recipient_email',delivery.recipient_email,'reminder_sequence',delivery.reminder_sequence,
      'mail_outbox_id',mail.id,'mail_status',mail.status,'accepted_at_utc',delivery.accepted_at_utc) order by d.action_id) value
      from public.import_review_decisions d
      left join public.hr_issue_emails e on e.issue_fingerprint=coalesce(nullif(d.summary_json->>'issue_fingerprint',''),d.source_identity)
        and e.timesheet_id=x.timesheet_id and e.client_id=p_client_id
      left join public.hr_issue_email_deliveries delivery on delivery.id=e.last_successful_delivery_id
      left join public.hr_issue_email_delivery_items item on item.delivery_id=delivery.id and item.issue_id=e.id
        and item.issue_fingerprint=e.issue_fingerprint and item.action_id=d.action_id
      left join public.mail_outbox mail on mail.id=delivery.mail_outbox_id and mail.deterministic_outbox_key=delivery.deterministic_outbox_key
      where d.import_id=x.query_import_id and d.preview_generation=x.preview_generation and d.is_current
        and d.timesheet_id=x.timesheet_id and d.client_id=p_client_id
        and d.action_kind in('EMAIL_ISSUE','EMAIL_REMINDER')) issues on true
  ), source_information as materialized (
    -- R-254 is informational inside an admitted actual-submission row. Native
    -- missing-source advisories never create a Checklist row or Timesheet count.
    -- They carry native action identity/reason, with no fabricated TS target.
    select private.weekly_source_query_ascii_fold_v1(coalesce(nullif(btrim(ca.first_name||' '||ca.last_name),''),
        d.summary_json->>'candidate_name',hr.staff_raw,hr.staff_norm)) collate "C" worker_sort,
      hr.date_local actual_work_date,('native-action:'||d.action_id) collate "C" item_sort,
      jsonb_build_object('item_id','native-action:'||d.action_id,'item_kind','SOURCE_INFORMATION',
        'timesheet_id',null,'counted_timesheet_id',null,'counts_for_row_admission',false,'open_read_item',false,
        'source_group_id',c.source_group_id,'source_cycle_id',c.id,'client_id',p_client_id,
        'client',cl.name,'candidate_id',d.candidate_id,
        'worker',coalesce(nullif(btrim(ca.first_name||' '||ca.last_name),''),d.summary_json->>'candidate_name',hr.staff_raw,hr.staff_norm),
        'work_date',hr.date_local,'week_ending',own_week.week_ending_date,
        'sheet_scope',case when upper(coalesce(d.summary_json->>'source_route','')) like '%DAILY%' then 'DAILY' else 'WEEKLY' end,
        'query_import_id',d.import_id,'hr_row_id',d.hr_row_id,'action_id',d.action_id,
        'query_preview',jsonb_build_object('import_id',d.import_id,'preview_generation',st.preview_generation,
          'state_version',st.state_version,'preview_fingerprint',st.preview_fingerprint),
        'action_kind',d.action_kind,'evidence_fingerprint',d.evidence_fingerprint,'summary',d.summary_json,
        'native_reason_code',d.summary_json->>'reason_code','writable_action',null) value
    from public.import_review_decisions d
    join public.import_review_states st on st.import_id=d.import_id and st.preview_generation=d.preview_generation
      and st.superseded_at_utc is null and st.abandoned_at_utc is null
    join public.hr_imports hi on hi.id=d.import_id and hi.pruned_at is null
      and hi.source_system::text in('HEALTHROSTER','HEALTHROSTER_DAILY') and (hi.client_id is null or hi.client_id=p_client_id)
    join public.hr_rows hr on hr.id=d.hr_row_id and hr.import_id=d.import_id
    join lateral(select min(admitted.week_ending_date) week_ending_date from actual admitted
      where hr.date_local between admitted.week_ending_date-6 and admitted.week_ending_date
      having count(distinct admitted.week_ending_date)=1) own_week on true
    join public.clients cl on cl.id=p_client_id left join public.candidates ca on ca.id=d.candidate_id
    where v_admin and d.client_id=p_client_id and d.timesheet_id is null and d.is_current
      and d.action_kind='ADVISORY' and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      and d.summary_json->>'reason_code' in('DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED',
        'WEEKLY_TIMESHEET_AWAITING_MANAGER_APPROVAL','WEEKLY_TIMESHEET_COMPLETING')
      and own_week.week_ending_date between c.finalisation_week_ending-6 and c.finalisation_week_ending
      and own_week.week_ending_date=v_week
      and exists(select 1 from actual admitted where admitted.week_ending_date=own_week.week_ending_date)
      and exists(select 1 from public.weekly_source_group_clients member
        join public.weekly_source_client_policies policy on policy.source_group_id=member.source_group_id and policy.client_id=member.client_id
        where member.source_group_id=c.source_group_id and member.client_id=p_client_id
          and hr.date_local between member.valid_from and coalesce(member.valid_to,'infinity'::date)
          and hr.date_local between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)
          and policy.authority_mode='TIMESHEET_AUTHORITY' and policy.document_mode='INVOICE_EVIDENCE_REQUIRED' and not policy.self_bill_enabled)
      and hi.id=(select latest.id from public.hr_imports latest
        join public.import_review_states latest_state on latest_state.import_id=latest.id
          and latest_state.superseded_at_utc is null and latest_state.abandoned_at_utc is null
        where latest.pruned_at is null and latest.source_system=hi.source_system
          and (latest.client_id is null or latest.client_id=p_client_id)
          and exists(select 1 from public.import_review_decisions latest_decision
            left join public.hr_rows latest_row on latest_row.id=latest_decision.hr_row_id and latest_row.import_id=latest.id
            where latest_decision.import_id=latest.id and latest_decision.preview_generation=latest_state.preview_generation
              and latest_decision.is_current and latest_decision.client_id=p_client_id
              and (latest_row.date_local between own_week.week_ending_date-6 and own_week.week_ending_date
                or exists(select 1 from actual latest_actual where latest_actual.timesheet_id=latest_decision.timesheet_id
                  and latest_actual.week_ending_date=own_week.week_ending_date)))
        order by latest.uploaded_at_utc desc,latest.id desc limit 1)
  ), all_rows as materialized (
    select * from rows union all select * from source_information
  ), version as (
    select encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_READ_REVISION_V1',
      coalesce(jsonb_agg(value order by worker_sort,actual_work_date,item_sort),'[]'::jsonb)),'hex') hash from all_rows
  ), page as (
    select * from all_rows where v_cursor is null or (worker_sort,actual_work_date,item_sort)>
      (v_cursor#>>'{after,0}',(v_cursor#>>'{after,1}')::date,v_cursor#>>'{after,2}')
    order by worker_sort,actual_work_date,item_sort limit v_limit+1
  ) select (select hash from version),coalesce(jsonb_agg(value order by worker_sort,actual_work_date,item_sort),'[]'::jsonb),count(*)>v_limit
    into v_revision,v_rows,v_more from page;
  if v_cursor is not null and v_cursor->>'read_revision' is distinct from v_revision then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
  end if;
  if v_more then v_rows:=v_rows-(jsonb_array_length(v_rows)-1); end if;
  v_last:=v_rows->(jsonb_array_length(v_rows)-1);
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_CHECK_ONLY_QUERY_ROWS_V1','rows',v_rows,
    'read_only',true,'invoice_subjects_hidden',not coalesce(v_admin,false),
    'read_revision',v_revision,'binding',v_binding,'has_more',v_more,'next_cursor',case when v_more then
      encode(convert_to(jsonb_build_object('contract','WEEKLY_SOURCE_CHECK_ONLY_CURSOR_V1','binding',v_binding,'read_revision',v_revision,
        'after',jsonb_build_array(private.weekly_source_query_ascii_fold_v1(v_last->>'worker'),v_last->'work_date',v_last->'item_id'))::text,'UTF8'),'base64') end);
end;
$function$;
create or replace function private.weekly_source_check_only_issue_filter_keys_v1(p_action_kind text,p_summary jsonb)
returns jsonb language plpgsql immutable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare keys jsonb:='[]'::jsonb; comparisons jsonb; evidence jsonb;
begin
  if p_action_kind is null or p_action_kind not in('EMAIL_ISSUE','EMAIL_REMINDER','ADVISORY') or jsonb_typeof(p_summary) is distinct from 'object' then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_NATIVE_FILTER_FACT_INVALID' using errcode='55000';
  end if;
  comparisons:=case when jsonb_typeof(p_summary->'comparisons')='array' then p_summary->'comparisons' else '[]'::jsonb end;
  evidence:=case when jsonb_typeof(p_summary->'evidence_rows')='array' then p_summary->'evidence_rows' else '[]'::jsonb end;
  if p_action_kind='ADVISORY' and p_summary->>'authority_mode'='VALIDATION_ONLY'
    and p_summary->>'reason_code' in('DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED') then
    keys:=keys||jsonb_build_array('TIMESHEET_MISSING');
  end if;
  if p_action_kind in('EMAIL_ISSUE','EMAIL_REMINDER') and (
    p_summary->>'reason_code' in('ACTUAL_HOURS_MISMATCH','START_END_MISMATCH','BREAK_MINUTES_MISMATCH')
    or exists(select 1 from jsonb_array_elements(comparisons) cx where cx->>'match_status'='MISMATCH')
    or exists(select 1 from jsonb_array_elements(evidence) e cross join lateral jsonb_array_elements_text(
      case when jsonb_typeof(e->'difference_codes')='array' then e->'difference_codes' else '[]'::jsonb end) code
      where code in('WORKED_HOURS','START_TIME','END_TIME','BREAK_MINUTES'))) then
    keys:=keys||jsonb_build_array('HOURS_DIFFER');
  end if;
  if p_action_kind in('EMAIL_ISSUE','EMAIL_REMINDER') and (p_summary->>'reason_code'='MISSING_FROM_IMPORT'
    or exists(select 1 from jsonb_array_elements(comparisons) cx where cx->>'match_status' in('UNMATCHED','HR_ONLY'))) then
    keys:=keys||jsonb_build_array('MISSING_SHIFT');
  end if;
  return keys;
end;
$function$;
create or replace function private.weekly_source_check_only_query_groups_v1(
  p_source_cycle_id uuid,p_client_id uuid,p_filters jsonb
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  actor public.tms_users%rowtype; cycle public.weekly_source_cycles%rowtype;
  scope jsonb; view text; authority jsonb; page jsonb; cursor text; raw_revision text;
  rows jsonb:='[]'::jsonb; raw jsonb; preview jsonb; native_issue jsonb;
  groups jsonb:='[]'::jsonb; targets jsonb:='[]'::jsonb; children jsonb; child jsonb;
  parent_identity jsonb; parent_key text; parent_proof text; binding text; revision text;
  action_bindings jsonb; child_binding jsonb; review_action jsonb; latest_decision public.import_review_decisions%rowtype;
  state public.import_review_states%rowtype; first_seen timestamptz; group_row jsonb;
  native_status jsonb; native_summary jsonb; actual_item_id text; native_actions text[]; child_filter_keys jsonb; group_filter_keys jsonb;
  reminder_facts jsonb; reminder_action jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_filters,
    array['actor_user_id','source_group_id','week_ending'],'WEEKLY_SOURCE_CHECK_ONLY_GROUP_FILTER_INVALID');
  select u.* into actor from public.tms_users u where u.id=(p_filters->>'actor_user_id')::uuid and u.is_active;
  if actor.id is null then raise exception 'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' using errcode='42501'; end if;
  view:=case when lower(btrim(coalesce(actor.role,'')))='admin' then 'ADMIN' else 'PAY_READONLY' end;
  if not pg_input_is_valid(coalesce(p_filters->>'week_ending',''),'date') then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_GROUP_FILTER_INVALID' using errcode='22023';
  end if;
  select c.* into cycle from public.weekly_source_cycles c where c.id=p_source_cycle_id;
  if cycle.id is null or cycle.source_group_id::text is distinct from p_filters->>'source_group_id'
    or p_client_id is null or (cycle.scope_client_id is not null and cycle.scope_client_id<>p_client_id)
    or (p_filters->>'week_ending')::date not between cycle.finalisation_week_ending-6 and cycle.finalisation_week_ending then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_UNAVAILABLE' using errcode='22023';
  end if;
  scope:=jsonb_build_object('source_group_id',cycle.source_group_id,'client_id',p_client_id,
    'source_cycle_id',cycle.id,'week_ending',p_filters->>'week_ending');
  authority:=private.weekly_source_office_authority_v1(actor.id,
    case when view='ADMIN' then 'VIEW_SOURCE_PROGRESS' else 'VIEW_PAY_WORK' end,
    cycle.source_group_id,p_client_id,(p_filters->>'week_ending')::date);
  binding:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUP_BINDING_V1',
    jsonb_build_object('actor',actor.id,'view',view,'authority',authority,'scope',scope)),'hex');
  cursor:=null;
  loop
    page:=private.weekly_source_check_only_query_rows_v1(cycle.id,p_client_id,
      jsonb_strip_nulls(p_filters||jsonb_build_object('limit',100,'cursor',cursor)));
    if page->'ok' is distinct from 'true'::jsonb then
      raise exception 'WEEKLY_SOURCE_CHECK_ONLY_NATIVE_GROUP_READ_UNAVAILABLE' using errcode='55000';
    end if;
    if raw_revision is not null and raw_revision is distinct from page->>'read_revision' then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
    end if;
    raw_revision:=page->>'read_revision'; rows:=rows||page->'rows';
    exit when page->'has_more'='false'::jsonb;
    if cursor is not distinct from page->>'next_cursor' then
      raise exception 'WEEKLY_SOURCE_CHECK_ONLY_NATIVE_GROUP_READ_UNAVAILABLE' using errcode='55000';
    end if;
    cursor:=page->>'next_cursor';
  end loop;
  -- HR comparison/questions are invoice subjects. Pay-only actors see no D10
  -- parents or private import/action frames; their authority still binds this
  -- empty census. Native Source pay subjects retain their separate existing path.
  if view='ADMIN' then
    for raw in select value from jsonb_array_elements(rows) loop
      preview:=raw->'query_preview';
      if preview is null or preview='null'::jsonb then continue; end if;
      select s.* into state from public.import_review_states s where s.import_id=(preview->>'import_id')::uuid
        and s.preview_generation=(preview->>'preview_generation')::integer and s.state_version=(preview->>'state_version')::bigint
        and s.preview_fingerprint=preview->>'preview_fingerprint' and s.superseded_at_utc is null and s.abandoned_at_utc is null;
      if state.import_id is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE' using errcode='40001'; end if;
      children:='[]'::jsonb; action_bindings:='[]'::jsonb; native_actions:='{}'::text[]; first_seen:=null; group_filter_keys:='[]'::jsonb;
      for native_issue in select value from jsonb_array_elements(case when raw->>'item_kind'='SOURCE_INFORMATION' then
        jsonb_build_array(jsonb_build_object('action_id',raw->'action_id','action_kind',raw->'action_kind',
          'evidence_fingerprint',raw->'evidence_fingerprint','reason_code',raw->'native_reason_code','summary',raw->'summary'))
        else coalesce(raw->'native_issues','[]'::jsonb) end) order by value->>'action_id'
      loop
        -- Action IDs are the native decision PK. Reprove current import/preview,
        -- actual subject and evidence; no lookup by a display name or guessed TS.
        select d.* into latest_decision from public.import_review_decisions d
          where d.action_id=native_issue->>'action_id' and d.import_id=state.import_id
            and d.preview_generation=state.preview_generation and d.client_id=p_client_id and d.is_current
            and d.evidence_fingerprint=native_issue->>'evidence_fingerprint'
            and d.action_kind=native_issue->>'action_kind'
            and ((raw->>'item_kind'='SOURCE_INFORMATION' and d.timesheet_id is null
              and d.hr_row_id=(raw->>'hr_row_id')::uuid and d.action_kind='ADVISORY'
              and d.summary_json->>'authority_mode'='VALIDATION_ONLY')
              or (raw->>'item_kind'='ACTUAL_TIMESHEET' and d.timesheet_id=(raw->>'timesheet_id')::uuid
                and d.action_kind in('EMAIL_ISSUE','EMAIL_REMINDER')));
        if latest_decision.action_id is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE' using errcode='40001'; end if;
        if latest_decision.action_id=any(native_actions) then
          raise exception 'WEEKLY_SOURCE_CHECK_ONLY_NATIVE_GROUP_ACTION_AMBIGUOUS' using errcode='55000';
        end if;
        native_actions:=array_append(native_actions,latest_decision.action_id);
        child_binding:=jsonb_build_object('owner','public.import_review_actions_page_v1','actor_user_id',actor.id,
          'scope',scope,'import_id',state.import_id,'preview_generation',state.preview_generation,'state_version',state.state_version,
          'preview_fingerprint',state.preview_fingerprint,'action_id',latest_decision.action_id,
          'action_kind',latest_decision.action_kind,'evidence_fingerprint',latest_decision.evidence_fingerprint);
        action_bindings:=action_bindings||jsonb_build_array(child_binding);
        native_summary:=latest_decision.summary_json;
        child_filter_keys:=private.weekly_source_check_only_issue_filter_keys_v1(latest_decision.action_kind,native_summary);
        select coalesce(jsonb_agg(key order by key),'[]'::jsonb) into group_filter_keys
          from(select distinct key from jsonb_array_elements_text(group_filter_keys||child_filter_keys) key) native_keys;
        native_status:=case when jsonb_typeof(native_summary->'status')='object' then native_summary->'status'
          else jsonb_build_object('text',coalesce(nullif(native_summary->>'outcome_label',''),
            nullif(native_summary->>'reason_code',''),latest_decision.action_kind),'tone','muted') end;
        first_seen:=least(first_seen,latest_decision.created_at_utc);
        child:=jsonb_build_object('row_key',latest_decision.action_id,'origin','CHECK_ONLY_D10',
          'day_date',to_char((raw->>'work_date')::date,'Dy FMDD Mon YYYY'),
          'job_role',coalesce(native_summary#>>'{imported_evidence,role}',''),
          'candidate_hours',coalesce(native_summary#>>'{current_evidence,worked_hours}',''),
          'system_hours',coalesce(native_summary#>>'{imported_evidence,worked_hours}',''),
          'issue',coalesce(native_summary->>'outcome_label',native_summary->>'reason_code',latest_decision.action_kind),
          'issue_filter_keys',child_filter_keys,
          'status',native_status,'incident_id',null,'accept_eligible',false,'actions','[]'::jsonb,
          'native_action_binding',child_binding,
          'delivery',case when raw->>'item_kind'='ACTUAL_TIMESHEET' then jsonb_build_object(
            'sent_verified',native_issue->'sent_verified','last_sent_at',native_issue->'last_sent_at',
            'sent_count',native_issue->'sent_count','issue_id',native_issue->'issue_id','issue_fingerprint',native_issue->'issue_fingerprint') end);
        children:=children||jsonb_build_array(child);
      end loop;
      if jsonb_array_length(children)=0 then continue; end if;
      actual_item_id:=raw->>'item_id';
      -- This owner creates the new native parent once from authentic identities.
      -- Consumers receive the literal key and proof; they never re-hash a child
      -- or infer a parent from candidate/client text. Preview/CAS changes make an
      -- old exact target stale, even when the physical Timesheet is unchanged.
      parent_identity:=jsonb_build_object('scope',scope,'item_id',actual_item_id,'preview',preview,'actions',action_bindings);
      -- Strip actor only from the ordered native action frames, preserving one
      -- stable parent across authorised Admin readers. Permission is in binding.
      parent_identity:=jsonb_set(parent_identity,'{actions}',(select jsonb_agg(a.value-'actor_user_id' order by a.ordinality)
        from jsonb_array_elements(action_bindings) with ordinality a(value,ordinality)),false);
      parent_key:='d10g_'||encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_QUERY_PARENT_V1',parent_identity),'hex');
      parent_proof:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_QUERY_PARENT_PROOF_V1',
        jsonb_build_object('owner_identity',parent_identity,'actor',actor.id,'authority',authority,'children',children)),'hex');
      -- Deliberate navigation delegates the complete existing native D10
      -- selection/APPLY/committed-email owner. A GET never invokes that owner.
      -- This descriptor grants no send, selection, Source outreach or Reminder.
      review_action:=jsonb_build_object('label','View details','kind','CHECK_ONLY_D10_REVIEW','enabled',true,
        'payload',jsonb_build_object('contract','WEEKLY_SOURCE_CHECK_ONLY_D10_REVIEW_ACTION_V1',
          'actor_user_id',actor.id,'permission_view',view,'scope',scope,'parent_group_key',parent_key,
          'item_id',actual_item_id,'proof_hash',parent_proof,'action_bindings',action_bindings),
        'context',jsonb_build_object('candidate',raw->'worker','client',raw->'client'));
      reminder_action:=null;
      if raw->>'item_kind'='SOURCE_INFORMATION' and raw->>'native_reason_code' in('DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED') then
        reminder_facts:=private.weekly_source_check_only_candidate_reminder_context_v1(actor.id,scope,raw->>'action_id');
        if reminder_facts->'available'='true'::jsonb then
          reminder_action:=jsonb_build_object('label','Remind candidate','kind','CHECK_ONLY_D10_CANDIDATE_REMINDER','enabled',true,
            'payload',jsonb_build_object('contract','WEEKLY_SOURCE_CHECK_ONLY_D10_CANDIDATE_REMINDER_ACTION_V1',
              'actor_user_id',actor.id,'permission_view',view,'scope',scope,'parent_group_key',parent_key,
              'parent_proof_hash',parent_proof,'action_id',raw->>'action_id','candidate_context_hash',reminder_facts->>'context_hash'),
            'context',jsonb_build_object('candidate',raw->'worker','client',raw->'client'));
        end if;
      end if;
      group_row:=jsonb_build_object('row_key',parent_key,'group_key',parent_key,'origin','CHECK_ONLY_D10',
        'client_id',p_client_id,'candidate_id',raw->'candidate_id','candidate_sort',private.weekly_source_query_ascii_fold_v1(raw->>'worker'),
        'candidate',raw->'worker','client',raw->'client','issues',jsonb_array_length(children),'issue_filter_keys',group_filter_keys,
        'candidate_asked',null,'manager_informed',null,'status',jsonb_build_object('text','HealthRoster comparison','tone','muted'),
        'age',case when first_seen is null then '—' when transaction_timestamp()-first_seen<interval '1 hour' then 'Less than 1 hour'
          when transaction_timestamp()-first_seen<interval '24 hours' then floor(extract(epoch from (transaction_timestamp()-first_seen))/3600)::integer||' hours'
          else floor(extract(epoch from (transaction_timestamp()-first_seen))/86400)::integer||' days' end,
        'first_seen_at_utc',first_seen,'children',children,'actions',jsonb_build_array(review_action),
        'outreach_eligible',false,'manager_eligible',false,'candidate_app_available',false,
        'source_outreach_eligible',false,'bulk_actions','{}'::jsonb,'proof_hash',parent_proof);
      if exists(select 1 from jsonb_array_elements(groups) g where g->>'group_key'=parent_key) then
        raise exception 'WEEKLY_SOURCE_CHECK_ONLY_NATIVE_GROUP_PARENT_AMBIGUOUS' using errcode='55000';
      end if;
      if reminder_action is not null then group_row:=jsonb_set(group_row,'{actions}',group_row->'actions'||jsonb_build_array(reminder_action),false); end if;
      groups:=groups||jsonb_build_array(group_row);
      targets:=targets||jsonb_build_array(jsonb_build_object('item_id',actual_item_id,'parent_group_key',parent_key,
        'owner','private.weekly_source_check_only_query_groups_v1','actor_user_id',actor.id,'permission_view',view,
        'import_id',state.import_id,'preview_generation',state.preview_generation,'state_version',state.state_version,
        'preview_fingerprint',state.preview_fingerprint,'proof_hash',parent_proof));
    end loop;
  end if;
  select coalesce(jsonb_agg(g.value order by g.value->>'candidate_sort' collate "C",g.value->>'group_key' collate "C"),'[]'::jsonb)
    into groups from jsonb_array_elements(groups) g(value);
  select coalesce(jsonb_agg(t.value order by t.value->>'item_id' collate "C"),'[]'::jsonb)
    into targets from jsonb_array_elements(targets) t(value);
  revision:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUP_READ_V1',
    jsonb_build_object('binding',binding,'source_read_revision',raw_revision,'groups',groups,'item_targets',targets)),'hex');
  return jsonb_build_object('contract','WEEKLY_SOURCE_CHECK_ONLY_QUERY_GROUPS_V1','actor_user_id',actor.id,
    'permission_view',view,'scope',scope,'binding',binding,'read_revision',revision,'source_read_revision',raw_revision,
    'groups',groups,'item_targets',targets,'source_outreach_eligible',false,'bulk_actions','{}'::jsonb);
end;
$function$;
create or replace function private.weekly_source_checklist_native_initial_certificate_core_v1(p_root_authorisation_id uuid,p_native_locks boolean)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  a public.weekly_source_root_authorisations%rowtype;
  t public.timesheets%rowtype;f public.timesheets_financials%rowtype;
  w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
  p private.bpay_next_publication%rowtype;c public.contracts%rowtype;
  inventory jsonb;basis jsonb;origin jsonb;details jsonb;n integer;epoch bigint;
begin
  if p_native_locks is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_ACCESS_MODE_REQUIRED' using errcode='22023'; end if;
  if p_root_authorisation_id is null then
    raise exception 'BPAY_NEXT_INITIAL_AUTH_ID_REQUIRED' using errcode='22023';end if;
  if p_native_locks then
  select m.owner_epoch into epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  else
  select m.owner_epoch into epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT';
  end if;
  if epoch is null then raise exception 'BPAY_NEXT_BINDING_NEXT_REQUIRED' using errcode='55000';end if;
  select x.* into strict a from public.weekly_source_root_authorisations x where x.id=p_root_authorisation_id;
  select x.* into strict t from public.timesheets x where x.timesheet_id=a.root_timesheet_id;
  select x.* into strict c from public.contracts x where x.id=t.contract_id;
  select x.* into strict r from private.bpay_next_work_revision x where x.source_event_id=a.id;
  select x.* into strict w from private.bpay_next_work x where x.id=r.work_id;
  select x.* into strict f from public.timesheets_financials x where x.id=r.financial_snapshot_id;
  select x.* into strict p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(a.id);
  if a.withdrawn_at_utc is not null or a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(t.booking_id))
     or t.is_current is distinct from true or t.authorised_at_server is null
     or t.revoked_at is not null or t.archived_at_utc is not null
     or (a.family_booking_id,a.timesheet_version) is distinct from (t.booking_id,t.version)
     or (w.work_kind,w.candidate_id,w.contract_id,w.original_timesheet_id,w.booking_id,w.week_ending_date,(case when w.current_revision_id is not null and exists(select 1 from private.bpay_next_work_revision cr where cr.id=w.current_revision_id and cr.sealed_at_utc is not null and cr.payment_treatment_approval_revision_id is not null) then private.bpay_next_treatment_approval_revision_v1(w.current_revision_id) else w.current_revision_id end))
       is distinct from ('SOURCE'::text,c.candidate_id,c.id,t.timesheet_id,t.booking_id,t.week_ending_date,r.id)
     or w.approval_state is distinct from 'APPROVED'
     or (r.source_kind,r.source_head_id,r.source_event_id,r.physical_timesheet_id,r.physical_timesheet_version,r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,a.id,t.timesheet_id,t.version,t.week_ending_date)
     or r.approved_at_utc is null or r.sealed_at_utc is null
     or f.is_current is distinct from true or f.is_stale is distinct from false or f.authorised_at_utc is null
     or (f.timesheet_id,f.timesheet_version,f.candidate_id,f.client_id)
       is distinct from (t.timesheet_id,t.version,w.candidate_id,c.client_id)
     or (p.work_id,p.revision_id,p.candidate_id,p.revision_no)
       is distinct from (w.id,r.id,w.candidate_id,r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=w.candidate_id
       where command.id=p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and p.status='QUEUED' and p.phase='NEW' and p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception 'BPAY_NEXT_INITIAL_AUTH_NOT_CURRENT' using errcode='55000';end if;
  inventory:=private.weekly_source_effective_inventory_v1(t.timesheet_id);
  basis:=inventory->'approval_basis';origin:=basis->'origin';
  details:=jsonb_build_object('segments',f.invoice_breakdown_json->'segments','additional_units',f.additional_units_json,
    'actual_schedule',f.actual_schedule_json,'rate_source_refs',f.rate_source_refs_json);
  if inventory->>'ok' is distinct from 'true' or inventory->>'authority' is distinct from 'TSFIN'
     or inventory->>'head_id' is not null or jsonb_typeof(inventory->'components') is distinct from 'array'
     or jsonb_typeof(basis) is distinct from 'object' or basis->'coverage_complete' is distinct from 'true'::jsonb
     or basis is distinct from private.weekly_source_inventory_approval_basis_v2(t.timesheet_id,inventory)
     or origin is distinct from jsonb_build_object('kind','INITIAL_AUTHORISED_TSFIN_V1',
       'root_authorisation_id',a.id,'financial_snapshot_id',f.id,'authorisation_generation',a.authorisation_generation,
       'root_timesheet_id',t.timesheet_id,'root_version',t.version::text,'authorised_row_signature',a.authorised_row_signature,
       'financial_snapshot_digest',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',
         jsonb_build_object('policy',f.policy_snapshot_json,'details',details,'components',inventory->'components')),'hex'),
       'inventory_digest',basis->>'inventory_digest','entitlement_digest',basis->>'entitlement_digest')
     or basis->>'detail_digest' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',details),'hex')
     or basis->>'policy_fingerprint' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',f.policy_snapshot_json),'hex')
     or r.source_inventory_digest is distinct from decode(basis->>'inventory_digest','hex')
     or r.approved_source_ex_vat is distinct from (basis->>'approved_pay_ex_vat')::numeric
     or r.expected_line_count is distinct from jsonb_array_length(inventory->'components')
  then raise exception 'BPAY_NEXT_INITIAL_I1_CERTIFICATE_REQUIRED' using errcode='55000';end if;
  select count(*) into n from private.bpay_next_approved_line l where l.revision_id=r.id;
  if n<>r.expected_line_count or exists(select 1 from jsonb_array_elements(inventory->'components') component
      left join private.bpay_next_approved_line l on l.revision_id=r.id
        and l.source_component_id=(component.value->>'component_id')::uuid
        and l.component_key='SOURCE:'||(component.value->>'component_id')
      where l.id is null or l.line_no is distinct from (component.value->>'component_ordinal')::integer
        or l.evidence_ref is distinct from 'source-component:'||(component.value->>'component_id')||'#'||(component.value->>'component_sha256')
        or l.source_pay_ex_vat is distinct from case when (component.value->>'exclude_from_pay')::boolean then 0 else (component.value->>'pay_ex_vat')::numeric end
        or l.component_kind is distinct from case component.value->>'component_kind' when 'WORKED_TIME' then 'WORK' when 'ADDITIONAL_UNIT' then 'ADDITIONAL' else 'EXPENSE' end
        or l.tax_treatment is distinct from case when component.value->>'component_kind' in ('WORKED_TIME','ADDITIONAL_UNIT') then 'TAXABLE'::text
          when component.value->>'component_kind' in ('EXPENSE','SOURCE_FIXED_EXPENSE') then 'NON_TAXABLE'::text else null::text end)
  then raise exception 'BPAY_NEXT_INITIAL_SEALED_COMPONENTS_MISMATCH' using errcode='23514';end if;
  return jsonb_build_object('candidate_id',w.candidate_id,'work_id',w.id,'revision_id',r.id,'command_id',p.command_id,
    'source_channel',r.source_pay_channel,'origin',origin,'basis',basis,'components',inventory->'components',
    'certificate_digest',encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',basis),'hex'));
end
$function$;
create or replace function private.weekly_source_checklist_native_initial_bound_assert_core_v1(p_binding_version_id uuid,p_native_locks boolean)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v private.bpay_next_source_component_binding_version%rowtype;l private.bpay_next_approved_line%rowtype;
  cert jsonb;origin jsonb;basis jsonb;component jsonb;
begin
  if p_native_locks is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_ACCESS_MODE_REQUIRED' using errcode='22023'; end if;
  select x.* into strict v from private.bpay_next_source_component_binding_version x where x.id=p_binding_version_id;
  if v.origin_kind is distinct from 'INITIAL_AUTHORISED_TSFIN_V1' then
    raise exception 'BPAY_NEXT_INITIAL_ORIGIN_REQUIRED' using errcode='23514';end if;
  cert:=private.weekly_source_checklist_native_initial_certificate_core_v1(v.initial_root_authorisation_id,p_native_locks);origin:=cert->'origin';basis:=cert->'basis';
  select value into strict component from jsonb_array_elements(cert->'components') x(value)
    where (value->>'component_id')::uuid=v.source_component_id;
  select x.* into strict l from private.bpay_next_approved_line x
    where x.revision_id=(cert->>'revision_id')::uuid and x.source_component_id=v.source_component_id;
  if (v.candidate_id,v.canonical_work_id,v.approval_work_id,v.approval_revision_id,v.publication_command_id_1,
      v.initial_financial_snapshot_id,v.initial_root_timesheet_id,v.initial_root_version,v.initial_authorisation_generation,
      v.initial_authorised_row_signature,v.initial_financial_snapshot_digest,v.initial_inventory_digest,v.initial_entitlement_digest,
      v.initial_detail_digest,v.initial_policy_digest,v.initial_certificate_digest,v.component_sha256)
    is distinct from ((cert->>'candidate_id')::uuid,(cert->>'work_id')::uuid,(cert->>'work_id')::uuid,(cert->>'revision_id')::uuid,
      (cert->>'command_id')::uuid,(origin->>'financial_snapshot_id')::uuid,(origin->>'root_timesheet_id')::uuid,
      (origin->>'root_version')::integer,(origin->>'authorisation_generation')::integer,origin->>'authorised_row_signature',
      decode(origin->>'financial_snapshot_digest','hex'),decode(basis->>'inventory_digest','hex'),decode(basis->>'entitlement_digest','hex'),
      decode(basis->>'detail_digest','hex'),decode(basis->>'policy_fingerprint','hex'),decode(cert->>'certificate_digest','hex'),
      decode(component->>'component_sha256','hex'))
     or (v.approved_line_id,v.classification_line_id,v.canonical_component_key,v.approval_component_key,
         v.approval_source_channel,v.tax_treatment,v.publication_work_id_1,v.publication_revision_id_1)
       is distinct from (l.id,l.id,l.component_key,l.component_key,cert->>'source_channel',l.tax_treatment,
         (cert->>'work_id')::uuid,(cert->>'revision_id')::uuid)
     or v.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v.canonical_work_id,
       v.canonical_component_key,v.approval_revision_id,v.approved_line_id,v.initial_root_authorisation_id,
       cert->>'certificate_digest',cert->>'command_id',component->>'component_sha256')::text,'UTF8'))
  then raise exception 'BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED' using errcode='23514';end if;
end
$function$;
create or replace function private.weekly_source_checklist_native_bound_snapshot_core_v1(p_binding_version_id uuid,p_expected_book_revision bigint,p_native_locks boolean)
returns table(qualification_state text,selection_state text,candidate_id uuid,
  canonical_work_id uuid,canonical_component_key text,binding_version_id uuid,source_book_revision bigint,
  approval_work_id uuid,approval_revision_id uuid,approved_line_id uuid,head_id uuid,source_receipt_id uuid,
  source_channel text,tax_treatment text,classification_line_id uuid,valuation_policy_id uuid,
  approved_source_ex_vat private.bpay_next_penny_amount,
  native_realised_paye private.bpay_next_penny_amount,native_realised_umbrella private.bpay_next_penny_amount,
  native_held_paye private.bpay_next_penny_amount,native_held_umbrella private.bpay_next_penny_amount)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare v_version private.bpay_next_source_component_binding_version%rowtype;
  v_binding private.bpay_next_source_component_binding%rowtype;
  v_position private.bpay_next_position%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_count integer;
begin
  if p_native_locks is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_ACCESS_MODE_REQUIRED' using errcode='22023'; end if;
  if p_binding_version_id is null or p_expected_book_revision is null or p_expected_book_revision<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_BOUND_SNAPSHOT_ID_REQUIRED';end if;
  if p_native_locks then
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT' for share;
  else
  perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT';
  end if;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_BINDING_NEXT_REQUIRED';end if;
  select v.* into strict v_version from private.bpay_next_source_component_binding_version v where v.id=p_binding_version_id;
  if p_native_locks then
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_version.candidate_id for share;
  else
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_version.candidate_id;
  end if;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED';end if;
  if p_native_locks then
  select b.* into strict v_binding from private.bpay_next_source_component_binding b
    where b.canonical_work_id=v_version.canonical_work_id and b.canonical_component_key=v_version.canonical_component_key for share;
  else
  select b.* into strict v_binding from private.bpay_next_source_component_binding b
    where b.canonical_work_id=v_version.canonical_work_id and b.canonical_component_key=v_version.canonical_component_key;
  end if;
  if v_binding.current_binding_version_id is distinct from p_binding_version_id then
    qualification_state:='SUPERSEDED';return next;return;end if;
  if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
    perform private.weekly_source_checklist_native_initial_bound_assert_core_v1(v_version.id,p_native_locks);
  else
  if not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_version.decision_bundle_id and b.bundle_revision=v_version.bundle_revision
      and b.state='COMMITTED') then
    qualification_state:='BUNDLE_PENDING';return next;return;end if;
  end if;
  if p_native_locks then
  select p.* into v_position from private.bpay_next_position p
    where p.work_id=v_version.canonical_work_id and p.component_key=v_version.canonical_component_key for share;
  else
  select p.* into v_position from private.bpay_next_position p
    where p.work_id=v_version.canonical_work_id and p.component_key=v_version.canonical_component_key;
  end if;
  if not found then qualification_state:='POSITION_PENDING';return next;return;end if;
  -- Retain actual Position applicability during the native apply interval:
  -- Work.applied may still be the predecessor while this Position is new.
  if not exists(select 1 from private.bpay_next_work w where w.id=v_position.work_id
    and (v_position.applied_revision_id=w.current_revision_id or v_position.applied_revision_id=w.applied_revision_id)) then
    qualification_state:='POSITION_PENDING';return next;return;end if;
  if v_version.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
    select pg_catalog.count(*) into v_count from private.bpay_next_publication p
      join private.bpay_next_work w on w.id=p.work_id
      join private.bpay_next_command c on c.id=p.command_id
      join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
      join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=w.candidate_id and j.job_kind='POSITION_APPLY'
      join private.bpay_next_worker_control ctl on ctl.candidate_id=w.candidate_id
      join private.bpay_next_module_control module on module.id=1 and module.active_owner='NEXT'
      where p.command_id=v_version.publication_command_id_1 and p.work_id=v_version.approval_work_id
        and p.revision_id=v_version.approval_revision_id and p.candidate_id=v_version.candidate_id
        and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
        and (case when w.current_revision_id is not null and exists(select 1 from private.bpay_next_work_revision cr where cr.id=w.current_revision_id and cr.sealed_at_utc is not null and cr.payment_treatment_approval_revision_id is not null) then private.bpay_next_treatment_approval_revision_v1(w.current_revision_id) else w.current_revision_id end)=p.revision_id and (case when w.applied_revision_id is not null and exists(select 1 from private.bpay_next_work_revision apr where apr.id=w.applied_revision_id and apr.sealed_at_utc is not null and apr.payment_treatment_approval_revision_id is not null) then private.bpay_next_treatment_approval_revision_v1(w.applied_revision_id) else w.applied_revision_id end)=p.revision_id
        and c.id=private.bpay_next_source_command_id_v1(v_version.initial_root_authorisation_id)
        and c.command_kind='POSITION_APPLY' and c.status='COMPLETE' and c.module_epoch=module.owner_epoch
        and c.expected_member_count=1 and c.enrolled_member_count=1 and c.sealed_at_utc is not null
        and j.status='DONE' and j.phase='DONE' and j.module_epoch=c.module_epoch
        and j.command_sequence=c.agency_sequence and j.owner_epoch>0 and j.owner_epoch<=ctl.active_owner_epoch
        and j.lease_nonce is null and j.lease_until_utc is null
        and (case when v_position.applied_revision_id is not null and exists(select 1 from private.bpay_next_work_revision pr where pr.id=v_position.applied_revision_id and pr.sealed_at_utc is not null and pr.payment_treatment_approval_revision_id is not null) then private.bpay_next_treatment_approval_revision_v1(v_position.applied_revision_id) else v_position.applied_revision_id end)=p.revision_id;
  else
  select pg_catalog.count(*) into v_count from private.bpay_next_source_binding_members_v1(v_version.source_receipt_id) m
    join private.bpay_next_publication p on p.command_id=m.publication_command_id
    join private.bpay_next_work w on w.id=m.work_id
    where p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
      and (case when w.current_revision_id is not null and exists(select 1 from private.bpay_next_work_revision cr where cr.id=w.current_revision_id and cr.sealed_at_utc is not null and cr.payment_treatment_approval_revision_id is not null) then private.bpay_next_treatment_approval_revision_v1(w.current_revision_id) else w.current_revision_id end)=m.revision_id and (case when w.applied_revision_id is not null and exists(select 1 from private.bpay_next_work_revision apr where apr.id=w.applied_revision_id and apr.sealed_at_utc is not null and apr.payment_treatment_approval_revision_id is not null) then private.bpay_next_treatment_approval_revision_v1(w.applied_revision_id) else w.applied_revision_id end)=m.revision_id
      and ((m.publication_command_id=v_version.publication_command_id_1
            and m.work_id=v_version.publication_work_id_1 and m.revision_id=v_version.publication_revision_id_1)
        or (m.publication_command_id=v_version.publication_command_id_2
            and m.work_id=v_version.publication_work_id_2 and m.revision_id=v_version.publication_revision_id_2));
  end if;
  if v_count is distinct from (case when v_version.publication_command_id_2 is null then 1 else 2 end) then
    qualification_state:='PUBLICATION_PENDING';return next;return;end if;
  if v_position.source_book_state is distinct from 'READY' then qualification_state:='BOOK_UNBOUND';return next;return;end if;
  if v_position.source_book_revision is distinct from p_expected_book_revision then
    qualification_state:='BOOK_REVISION_STALE';return next;return;end if;
  if v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
     or (v_position.source_book_seed_channel='PAYE' and
       (v_position.native_realised_paye is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_paye is distinct from v_position.held_source_ex_vat))
     or (v_position.source_book_seed_channel='UMBRELLA' and
       (v_position.native_realised_umbrella is distinct from v_position.realised_source_ex_vat
        or v_position.native_held_umbrella is distinct from v_position.held_source_ex_vat)) then
    qualification_state:='BOOK_POSTING_REQUIRED';return next;return;end if;
  if v_version.tax_treatment is null or v_version.classification_line_id is null then
    qualification_state:='CLASSIFICATION_UNBOUND';return next;return;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_version.approval_revision_id;
  if v_version.component_state='PRESENT' then
    select l.* into strict v_line from private.bpay_next_approved_line l where l.id=v_version.approved_line_id;
    approved_source_ex_vat:=v_line.source_pay_ex_vat;
  else
    if exists(select 1 from private.bpay_next_approved_line l
      where l.revision_id=v_version.approval_revision_id and l.component_key=v_version.approval_component_key) then
      raise exception using errcode='23514',message='BPAY_NEXT_BINDING_REMOVAL_LINE_CONFLICT';end if;
    approved_source_ex_vat:=0;
  end if;
  candidate_id:=v_version.candidate_id;canonical_work_id:=v_version.canonical_work_id;
  canonical_component_key:=v_version.canonical_component_key;binding_version_id:=v_version.id;
  source_book_revision:=v_position.source_book_revision;approval_work_id:=v_version.approval_work_id;
  approval_revision_id:=v_version.approval_revision_id;approved_line_id:=v_version.approved_line_id;
  head_id:=v_version.head_id;source_receipt_id:=v_version.source_receipt_id;source_channel:=v_version.approval_source_channel;
  tax_treatment:=v_version.tax_treatment;classification_line_id:=v_version.classification_line_id;
  valuation_policy_id:=v_revision.valuation_policy_id;
  native_realised_paye:=v_position.native_realised_paye;native_realised_umbrella:=v_position.native_realised_umbrella;
  native_held_paye:=v_position.native_held_paye;native_held_umbrella:=v_position.native_held_umbrella;
  selection_state:='UNBOUND';qualification_state:='READY';return next;
end
$function$;
create or replace function private.weekly_source_checklist_native_members_core_v1(p_work_id uuid,p_revision_id uuid,p_native_locks boolean)
returns table(component_key text,approved_line_id uuid,approved_segment_id uuid,segment_key text,
  component_kind text,tax_treatment text,work_date date,approved_quantity numeric,unit_label text,
  approved_amount numeric,realised_amount numeric,held_amount numeric,source_pay_channel text,
  parent_complete boolean,segment_attribution_complete boolean)
language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_revision private.bpay_next_work_revision%rowtype;v_approval uuid;v_event uuid;
begin
  if p_native_locks is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_ACCESS_MODE_REQUIRED' using errcode='22023'; end if;
  perform private.bpay_next_workbench_require_next_v1();
  select x.* into v_revision from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=p_work_id;
  v_approval:=p_revision_id;v_event:=v_revision.source_event_id;
  if v_revision.payment_treatment_approval_revision_id is not null and v_revision.sealed_at_utc is not null then
    v_approval:=private.bpay_next_treatment_approval_revision_v1(v_revision.id);
    select x.source_event_id into v_event from private.bpay_next_work_revision x where x.id=v_approval and x.work_id=p_work_id;
  end if;
  return query
  with source_books as materialized (
    select p.component_key,book.qualification_state,book.approval_revision_id,book.approved_line_id,
      book.source_channel,book.approved_source_ex_vat,book.native_realised_paye,book.native_realised_umbrella,
      book.native_held_paye,book.native_held_umbrella
    from private.bpay_next_work w join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=w.id
    join private.bpay_next_position p on p.work_id=w.id and p.source_book_state='READY' and p.source_book_revision>0
    left join private.bpay_next_approved_line a on a.revision_id=r.id and a.component_key=p.component_key
    join private.bpay_next_source_component_binding b on b.canonical_work_id=w.id and b.canonical_component_key=p.component_key
    join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      and v.approval_work_id=w.id and v.approval_revision_id=v_approval and v.approval_component_key=p.component_key
    left join private.bpay_next_approved_line original_class on original_class.id=coalesce(v.classification_line_id,b.canonical_line_id)
    cross join lateral private.weekly_source_checklist_native_bound_snapshot_core_v1(v.id,p.source_book_revision,p_native_locks) book
    where w.id=p_work_id and w.work_kind='SOURCE' and coalesce(a.component_kind,original_class.component_kind)='WORK' and (r.source_head_id is not null
      or (r.source_kind='SOURCE' and v_event is not null and r.financial_snapshot_id is not null))
  ), keys as (
    select a.component_key from private.bpay_next_approved_line a where a.revision_id=p_revision_id
    union select p.component_key from private.bpay_next_position p where p.work_id=p_work_id
  ), parents as (
    select k.component_key,a.id line_id,coalesce(a.component_kind,original_class.component_kind) component_kind,
      case when a.id is not null then a.tax_treatment else original_class.tax_treatment end tax_treatment,
      case when a.id is not null then a.work_date else original_class.work_date end work_date,a.approved_quantity,a.unit_label,
      p.approved_source_ex_vat a,p.realised_source_ex_vat c,p.held_source_ex_vat h,p.source_basis_channel,
      p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat,
      p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat,
      (p.work_id is not null and p.applied_revision_id=p_revision_id
        and p.approved_source_ex_vat=coalesce(a.source_pay_ex_vat,0)
        and (a.id is not null or p.approved_source_ex_vat=0)
        and (w.work_kind<>'SOURCE' or a.id is not null or original_class.id is not null)
        and (not (w.work_kind='SOURCE' and coalesce(a.component_kind,original_class.component_kind)='WORK' and (r.source_head_id is not null
          or (r.source_kind='SOURCE' and v_event is not null and r.financial_snapshot_id is not null)))
          or (book.qualification_state='READY' and book.approval_revision_id=v_approval
            and book.approved_line_id is not distinct from aa.id and book.source_channel=r.source_pay_channel
            and (book.approved_source_ex_vat,book.native_realised_paye,book.native_held_paye,book.native_realised_umbrella,book.native_held_umbrella)
              is not distinct from (p.approved_source_ex_vat,
                case when r.source_pay_channel='PAYE' then p.realised_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='PAYE' then p.held_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='UMBRELLA' then p.realised_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='UMBRELLA' then p.held_source_ex_vat else 0::numeric end)))) complete,
      (exists(select 1 from private.bpay_next_approved_segment s where s.revision_id=p_revision_id and s.component_key=k.component_key)
        or exists(select 1 from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=k.component_key)) segmented
    from keys k left join private.bpay_next_approved_line a on a.revision_id=p_revision_id and a.component_key=k.component_key
    left join private.bpay_next_approved_line aa on aa.revision_id=v_approval and aa.component_key=k.component_key
    left join private.bpay_next_position p on p.work_id=p_work_id and p.component_key=k.component_key
    join private.bpay_next_work w on w.id=p_work_id
    left join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=w.id
    left join private.bpay_next_source_component_binding binding on binding.canonical_work_id=w.id and binding.canonical_component_key=k.component_key
    left join private.bpay_next_source_component_binding_version version on version.id=binding.current_binding_version_id
    left join private.bpay_next_approved_line original_class on original_class.id=coalesce(version.classification_line_id,binding.canonical_line_id)
    left join source_books book on book.component_key=k.component_key
  ), segment_keys as (
    select s.component_key,s.segment_key from private.bpay_next_approved_segment s where s.revision_id=p_revision_id
    union select s.component_key,s.segment_key from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id
  ), members as (
    select p.*,s.segment_key,seg.id segment_id,seg.work_date segment_date,seg.approved_hours,
      coalesce(seg.approved_source_ex_vat,0) segment_a,b.realised_source_ex_vat segment_c,b.active_held_source_ex_vat segment_h
    from parents p join segment_keys s on s.component_key=p.component_key
    left join private.bpay_next_approved_segment seg on seg.revision_id=p_revision_id
      and seg.component_key=s.component_key and seg.segment_key=s.segment_key
    cross join lateral private.bpay_next_work_segment_gross_balance_v1(p_work_id,s.component_key,s.segment_key) b
  ), totals as (
    select m.component_key,sum(m.segment_a) a,sum(m.segment_c) c,sum(m.segment_h) h from members m group by m.component_key
  ), target_credit as (
    select e.component_key,sum(e.target_ex_vat) ex,sum(e.target_vat) vat,sum(e.target_inc_vat) inc
    from private.bpay_next_work_segment_effect_share e where e.work_id=p_work_id group by e.component_key
  ), target_hold as (
    select s.component_key,sum(s.target_reserved_ex_vat) ex,sum(s.target_reserved_vat) vat,sum(s.target_reserved_inc_vat) inc
    from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
    where s.work_id=p_work_id group by s.component_key
  )
  select m.component_key,m.line_id,m.segment_id,m.segment_key,m.component_kind,m.tax_treatment,
    m.segment_date,m.approved_hours,'hours'::text,m.segment_a,m.segment_c,m.segment_h,m.source_basis_channel,
    coalesce(m.complete,false),
    ((t.a,t.c,t.h,coalesce(c.ex,0),coalesce(c.vat,0),coalesce(c.inc,0),coalesce(h.ex,0),coalesce(h.vat,0),coalesce(h.inc,0))
      is not distinct from (m.a,m.c,m.h,m.realised_target_ex_vat,m.realised_target_vat,m.realised_target_inc_vat,
        m.held_target_ex_vat,m.held_target_vat,m.held_target_inc_vat))
    from members m join totals t on t.component_key=m.component_key
      left join target_credit c on c.component_key=m.component_key left join target_hold h on h.component_key=m.component_key
  union all
  select p.component_key,p.line_id,null::uuid,null::text,p.component_kind,p.tax_treatment,p.work_date,
    p.approved_quantity,p.unit_label,p.a,p.c,p.h,p.source_basis_channel,coalesce(p.complete,false),true
    from parents p where not p.segmented;
end
$function$;
create or replace function private.weekly_source_invoice_hold_release_context_core_v1(p_request jsonb,p_native_locks boolean)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $release_context$
declare actor uuid; row_id uuid; expected_hash bytea; expected_version bigint; kind text; choice text;
  s public.weekly_source_upload_rows%rowtype; u public.weekly_source_uploads%rowtype; c public.weekly_source_cycles%rowtype;
  g public.weekly_source_groups%rowtype; scope_row public.weekly_source_report_scopes%rowtype; rules jsonb; v_client_id uuid; existing public.weekly_source_invoice_hold_releases%rowtype;
  net bigint; vat bigint; gross bigint; h bytea; id uuid;
begin
  if p_native_locks is null then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_NATIVE_MODE_INVALID' using errcode='55000'; end if;
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','upload_row_id','source_cycle_id','report_scope_id','expected_source_row_hash','expected_authority_scope_version','hold_kind','action'],'WEEKLY_SOURCE_INVOICE_HOLD_REQUEST_INVALID');
  begin actor:=(p_request->>'actor_user_id')::uuid; row_id:=(p_request->>'upload_row_id')::uuid;
    expected_hash:=private.weekly_source_finalisation_hex32_v1(p_request->>'expected_source_row_hash','WEEKLY_SOURCE_INVOICE_HOLD_HASH_INVALID');
    expected_version:=(p_request->>'expected_authority_scope_version')::bigint;
    kind:=p_request->>'hold_kind'; choice:=coalesce(p_request->>'action',case when kind='VAT_FIT' then 'BILL_AT_NET_PLUS_20' else 'BILL_AS_FILE' end);
  exception when others then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_REQUEST_INVALID' using errcode='22023'; end;
  perform private._weekly_source_settings_require_admin_v1(actor);
  if kind is null or choice is null or kind not in ('VAT_FIT','DUPLICATE_REQUEST_ID') or choice not in ('BILL_AT_NET_PLUS_20','BILL_AS_FILE')
    or (kind='DUPLICATE_REQUEST_ID' and choice<>'BILL_AS_FILE') then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_CHOICE_INVALID' using errcode='22023'; end if;
  select * into strict s from public.weekly_source_upload_rows where weekly_source_upload_rows.id=row_id;
  select * into strict u from public.weekly_source_uploads where weekly_source_uploads.id=s.upload_id;
  if p_native_locks then
  select * into strict g from public.weekly_source_groups where weekly_source_groups.id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id) for update;
  else
  select * into strict g from public.weekly_source_groups where weekly_source_groups.id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id);
  end if;
  if p_native_locks then
  select * into strict c from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id for update;
  else
  select * into strict c from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id;
  end if;
  if u.report_scope_id is not null then
    if p_native_locks then
    select * into strict scope_row from public.weekly_source_report_scopes where weekly_source_report_scopes.id=u.report_scope_id for update;
    else
    select * into strict scope_row from public.weekly_source_report_scopes where weekly_source_report_scopes.id=u.report_scope_id;
    end if;
    if scope_row.source_cycle_id<>c.id or scope_row.source_group_id<>g.id or scope_row.current_complete_upload_id is distinct from u.id
      or s.row_finalisation_state<>'SOURCE_REPORT_LINE' then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_REPORT_SCOPE_INVALID' using errcode='55000'; end if;
  elsif c.current_complete_upload_id is distinct from u.id and u.state<>'CORRECTION_READY' then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE' using errcode='40001'; end if;
  if u.report_scope_id is distinct from nullif(p_request->>'report_scope_id','')::uuid or c.id is distinct from nullif(p_request->>'source_cycle_id','')::uuid or s.normalised_row_hash is distinct from expected_hash
    or coalesce(scope_row.version,c.version) is distinct from expected_version or u.state not in ('CURRENT','CORRECTION_READY') or not g.active or g.source_family<>'ROSTER' then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE' using errcode='40001'; end if;
  perform private._weekly_source_settings_deployment_v1(g.agency_id,g.environment);
  v_client_id:=coalesce(scope_row.client_id,c.scope_client_id,nullif(u.file_metadata_json->>'client_id','')::uuid);
  if v_client_id is null or (nullif(u.file_metadata_json->>'client_id','')::uuid is not null and nullif(u.file_metadata_json->>'client_id','')::uuid<>v_client_id) or (c.scope_client_id is not null and nullif(u.file_metadata_json->>'client_id','')::uuid is distinct from c.scope_client_id) then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_CLIENT_SCOPE_INVALID' using errcode='55000'; end if;
  perform private.weekly_source_office_authority_v1(actor,'FINALISE_WEEK',g.id,v_client_id,c.finalisation_week_ending);
  rules:=private.weekly_source_effective_rule_set_v1(v_client_id,c.id)->'values';
  if rules is null or rules->>'invoice_sender'<>'CLIENT_SELF_BILL' or rules->>'invoice_amount_basis'<>'SOURCE_AMOUNT'
    or s.source_money_parse_state<>'VALID' or s.source_shift_charge_pence is null or s.source_gross_pence is null or s.source_vat_pence is null then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_NOT_ELIGIBLE' using errcode='55000'; end if;
  if kind='VAT_FIT' and (rules->>'source_amount_basis'<>'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
    or abs(5::numeric*s.source_gross_pence-6::numeric*s.source_shift_charge_pence)<=5) then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_NOT_ELIGIBLE' using errcode='55000'; end if;
  if kind='DUPLICATE_REQUEST_ID' and (u.report_scope_id is null or s.row_finalisation_state<>'SOURCE_REPORT_LINE' or rules->>'self_bill_source' is distinct from 'SEPARATE_FINAL_REPORT' or rules->>'source_amount_basis' is distinct from 'REPORT_LINE_NET') then raise exception 'WEEKLY_SOURCE_INVOICE_DUPLICATE_HOLD_NOT_PROVED' using errcode='55000'; end if;
  if kind='DUPLICATE_REQUEST_ID' and private.weekly_source_self_bill_report_duplicate_prior_v1(s.id) is distinct from true then
    raise exception 'WEEKLY_SOURCE_INVOICE_DUPLICATE_HOLD_NOT_PROVED' using errcode='55000'; end if;
  return jsonb_build_object('actor_user_id',actor,'upload_row_id',row_id,'expected_source_row_hash',encode(expected_hash,'hex'),
    'expected_authority_scope_version',expected_version,'hold_kind',kind,'choice',choice,'row',to_jsonb(s),'upload',to_jsonb(u),
    'group',to_jsonb(g),'cycle',to_jsonb(c),'report_scope',to_jsonb(scope_row),'rules',rules,'client_id',v_client_id);
end;
$release_context$;
create or replace function private.weekly_source_invoice_hold_first_invoice_assert_v1(p_upload_row_id uuid)
returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $first_invoice$
begin
  if exists(select 1 from public.weekly_source_billing_movements m where
      (m.source_facts_json->>'upload_row_id'=p_upload_row_id::text or m.self_bill_report_row_id=p_upload_row_id)
      and m.movement_role in ('POSITIVE','REPLACEMENT','STANDALONE_NEGATIVE')
      and (m.placement_state<>'UNPLACED' or exists(select 1 from public.weekly_source_invoice_line_bindings b
        where b.billing_movement_id=m.id and b.state='CURRENT')
        or exists(select 1 from public.weekly_source_invoice_placements p join public.invoices i on i.id=p.invoice_id
          where p.billing_movement_id=m.id and i.status::text='ISSUED')))
    or exists(select 1 from public.weekly_source_final_snapshot_lines line join public.weekly_source_final_revisions r on r.id=line.final_revision_id
      where line.upload_row_id=p_upload_row_id and r.state='PREPARED') then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_ALREADY_PLACED' using errcode='55000'; end if;
end;
$first_invoice$;
create or replace function private.weekly_source_invoice_unreleased_vat_fit_movements_v1(p_client_manifest_id uuid)
returns setof public.weekly_source_billing_movements language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $native_blocker$
select m.* from public.weekly_source_manifest_movements mm join public.weekly_source_billing_movements m on m.id=mm.billing_movement_id
    where mm.client_manifest_id=p_client_manifest_id and m.source_facts_json->>'invoice_hold'='VAT_FIT'
      and not exists(select 1 from public.weekly_source_invoice_hold_releases receipt where receipt.upload_row_id::text=m.source_facts_json->>'upload_row_id' and receipt.hold_kind='VAT_FIT');
$native_blocker$;
create or replace function private.bpay_next_work_period_proof_v1(
 p_work_id uuid,p_revision_id uuid,p_business_date date
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $native_period$
declare w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 t public.timesheets%rowtype;c public.contracts%rowtype;s jsonb;weekday integer;
begin
 if p_work_id is null or p_business_date is null or not isfinite(p_business_date) then
  return jsonb_build_object('valid',false,'reason','NATIVE_WORK_PERIOD_UNPROVED');end if;
 select a.* into w from private.bpay_next_work a where a.id=p_work_id;
 if w.id is null or w.week_ending_date is null or not isfinite(w.week_ending_date) then
  return jsonb_build_object('valid',false,'reason','NATIVE_WORK_PERIOD_UNPROVED');end if;
 -- Preserve every pre-existing native Sunday admission and its later native
 -- approval/current/physical/member guards. This helper adds no new approval.
 if extract(isodow from w.week_ending_date)=7 then
  return jsonb_build_object('valid',true,'basis','NATIVE_SUNDAY_PERIOD',
   'work_id',w.id,'revision_id',p_revision_id,'week_ending',w.week_ending_date,
   'period_start',w.week_ending_date-6,'business_date',p_business_date,
   'future',w.week_ending_date-6>p_business_date);end if;
 -- Only a genuine contracted Weekly physical period extends the old native
 -- calendar domain. Daily, missing snapshots, Cases and invented dates do not.
 select a.* into r from private.bpay_next_work_revision a where a.id=p_revision_id and a.work_id=w.id;
 select a.* into t from public.timesheets a where a.timesheet_id=r.physical_timesheet_id;
 select a.* into c from public.contracts a where a.id=w.contract_id;
 s:=t.settings_authority_json;
 if r.id is null or c.id is null or c.client_id is null or c.candidate_id is null
  or t.timesheet_id is null or w.origin_kind is distinct from 'CONTRACT'
  or w.work_kind not in('ORDINARY','SOURCE','EXPENSE')
  or r.source_kind not in('ORDINARY','SOURCE','PROTECTED','EXPENSE')
  or r.week_ending_date is distinct from w.week_ending_date
  or t.week_ending_date is distinct from w.week_ending_date
  or t.sheet_scope::text is distinct from 'WEEKLY' or t.contract_id is distinct from c.id
  or t.booking_id is distinct from w.booking_id or t.version is distinct from r.physical_timesheet_version
  or t.is_current is distinct from true or t.revoked_at is not null or t.archived_at_utc is not null
  or c.candidate_id is distinct from w.candidate_id
  or t.settings_authority_version is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
  or jsonb_typeof(s) is distinct from 'object' or s->>'authority_version' is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
  or s->>'client_id' is distinct from c.client_id::text or s->>'contract_id' is distinct from c.id::text
  or s->>'relevant_date' is distinct from t.week_ending_date::text
  or s->>'workflow' is null or s->>'workflow' not in('WEEKLY','IMPORT')
  or s->>'authority_fingerprint' is distinct from t.settings_authority_fingerprint
  or t.settings_authority_fingerprint is null
  or encode(sha256(convert_to((s-'authority_fingerprint'-'resolved_at_utc')::text,'UTF8')),'hex')
    is distinct from t.settings_authority_fingerprint
  or pg_input_is_valid(s#>>'{values,week_ending_weekday}','integer') is distinct from true
  or (select count(*) from public.timesheets a where a.booking_id=w.booking_id and a.is_current)<>1 then
  return jsonb_build_object('valid',false,'reason','NATIVE_CAPTURED_WORK_PERIOD_UNPROVED');end if;
 weekday:=(s#>>'{values,week_ending_weekday}')::integer;
 if weekday not between 0 and 6 or extract(dow from w.week_ending_date)::integer<>weekday then
  return jsonb_build_object('valid',false,'reason','NATIVE_CAPTURED_WORK_PERIOD_UNPROVED');end if;
 return jsonb_build_object('valid',true,'basis','NATIVE_CAPTURED_CONTRACT_PERIOD',
  'work_id',w.id,'revision_id',r.id,'timesheet_id',t.timesheet_id,'timesheet_version',t.version,
  'contract_id',c.id,'candidate_id',c.candidate_id,'client_id',c.client_id,
  'settings_authority_fingerprint',t.settings_authority_fingerprint,'week_ending_weekday',weekday,
  'week_ending',w.week_ending_date,'period_start',w.week_ending_date-6,
  'business_date',p_business_date,'future',w.week_ending_date-6>p_business_date);
end;
$native_period$;
CREATE OR REPLACE FUNCTION private.weekly_source_checklist_legacy_pay_state_core_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql STABLE
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_out jsonb;
  v_has_tsfin boolean := false;
  v_today_uk date := (now() AT TIME ZONE 'Europe/London')::date;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'timesheet_pay_state: timesheet_id is required';
  END IF;

  WITH
  tf AS (
    SELECT
      tf.timesheet_id,
      tf.candidate_id,
      ts.booking_id,
      tf.pay_on_hold,
      tf.invoice_breakdown_json,
      ts.sheet_scope,
      ts.reference_number,
      ts.worked_start_iso AS ts_worked_start_iso,
      ts.worked_end_iso AS ts_worked_end_iso,
      ts.break_start_iso AS ts_break_start_iso,
      ts.break_end_iso AS ts_break_end_iso,
      ts.break_minutes AS ts_break_minutes,
      ts.actual_schedule_json AS ts_actual_schedule_json,
      tf.worked_start_iso AS tf_worked_start_iso,
      tf.worked_end_iso AS tf_worked_end_iso,
      tf.break_start_iso AS tf_break_start_iso,
      tf.break_end_iso AS tf_break_end_iso,
      tf.break_minutes AS tf_break_minutes,
      tf.actual_schedule_json AS tf_actual_schedule_json
    FROM public.timesheets_financials tf
    LEFT JOIN public.timesheets ts
      ON ts.timesheet_id = tf.timesheet_id
     AND ts.is_current = true
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
    LIMIT 1
  ),
  tf_norm AS (
    SELECT
      t.timesheet_id,
      t.candidate_id,
      t.booking_id,
      COALESCE(t.pay_on_hold, false) AS pay_on_hold,
      CASE
        WHEN t.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(t.invoice_breakdown_json) = 'object'
          THEN t.invoice_breakdown_json
        ELSE NULL
      END AS invoice_breakdown_json,
      upper(coalesce(t.sheet_scope::text, '')) AS sheet_scope,
      nullif(btrim(coalesce(t.reference_number, '')), '') AS reference_number,
      coalesce(t.tf_worked_start_iso, t.ts_worked_start_iso) AS effective_worked_start_iso,
      coalesce(t.tf_worked_end_iso, t.ts_worked_end_iso) AS effective_worked_end_iso,
      coalesce(t.tf_break_start_iso, t.ts_break_start_iso) AS effective_break_start_iso,
      coalesce(t.tf_break_end_iso, t.ts_break_end_iso) AS effective_break_end_iso,
      coalesce(t.tf_break_minutes, t.ts_break_minutes) AS effective_break_minutes,
      CASE
        WHEN jsonb_typeof(t.tf_actual_schedule_json) = 'object' THEN t.tf_actual_schedule_json
        WHEN jsonb_typeof(t.tf_actual_schedule_json) = 'array' THEN (
          SELECT tf_sched_item.value
          FROM jsonb_array_elements(t.tf_actual_schedule_json) AS tf_sched_item(value)
          WHERE tf_sched_item.value IS NOT NULL
            AND jsonb_typeof(tf_sched_item.value) = 'object'
          LIMIT 1
        )
        WHEN jsonb_typeof(t.ts_actual_schedule_json) = 'object' THEN t.ts_actual_schedule_json
        WHEN jsonb_typeof(t.ts_actual_schedule_json) = 'array' THEN (
          SELECT ts_sched_item.value
          FROM jsonb_array_elements(t.ts_actual_schedule_json) AS ts_sched_item(value)
          WHERE ts_sched_item.value IS NOT NULL
            AND jsonb_typeof(ts_sched_item.value) = 'object'
          LIMIT 1
        )
        ELSE NULL::jsonb
      END AS effective_daily_schedule_json
    FROM tf t
  ),
  mode_flags AS (
    SELECT
      tn.timesheet_id,
      tn.candidate_id,
      tn.booking_id,
      tn.pay_on_hold,
      tn.invoice_breakdown_json,
      tn.sheet_scope,
      tn.reference_number,
      tn.effective_worked_start_iso,
      tn.effective_worked_end_iso,
      tn.effective_break_start_iso,
      tn.effective_break_end_iso,
      tn.effective_break_minutes,
      tn.effective_daily_schedule_json,
      CASE
        WHEN tn.invoice_breakdown_json IS NOT NULL
         AND upper(coalesce(tn.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
         AND jsonb_typeof(tn.invoice_breakdown_json->'segments') = 'array'
          THEN true
        ELSE false
      END AS actual_segments_mode,
      CASE
        WHEN tn.sheet_scope = 'DAILY'
         AND coalesce(
           CASE
             WHEN tn.effective_worked_start_iso IS NOT NULL THEN ((tn.effective_worked_start_iso AT TIME ZONE 'Europe/London')::date)::text
             ELSE coalesce(
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date','')), ''),
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'work_date','')), ''),
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'ymd','')), ''),
               nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date_ymd','')), '')
             )
           END,
           ''
         ) <> ''
          THEN true
        ELSE false
      END AS synthetic_daily_mode,
      CASE
        WHEN tn.effective_worked_start_iso IS NOT NULL THEN ((tn.effective_worked_start_iso AT TIME ZONE 'Europe/London')::date)::text
        ELSE coalesce(
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date','')), ''),
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'work_date','')), ''),
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'ymd','')), ''),
          nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'date_ymd','')), '')
        )
      END AS synthetic_component_date,
      jsonb_strip_nulls(
        case
          when jsonb_typeof(tn.effective_daily_schedule_json->'breaks') = 'array' then tn.effective_daily_schedule_json->'breaks'
          when tn.effective_break_start_iso is not null and tn.effective_break_end_iso is not null then
            jsonb_build_array(
              jsonb_build_object(
                'start', to_char((tn.effective_break_start_iso AT TIME ZONE 'Europe/London'), 'HH24:MI'),
                'end', to_char((tn.effective_break_end_iso AT TIME ZONE 'Europe/London'), 'HH24:MI'),
                'break_mins', coalesce(
                  tn.effective_break_minutes::numeric,
                  case
                    when tn.effective_break_start_iso is not null and tn.effective_break_end_iso is not null
                      then greatest(
                        0::numeric,
                        round((extract(epoch from (tn.effective_break_end_iso - tn.effective_break_start_iso)) / 60.0)::numeric, 0)
                      )
                    else null::numeric
                  end
                )
              )
            )
          when nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_start','')), '') is not null
           and nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_end','')), '') is not null then
            jsonb_build_array(
              jsonb_build_object(
                'start', nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_start','')), ''),
                'end', nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_end','')), ''),
                'break_mins', coalesce(
                  tn.effective_break_minutes::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
                  nullif(btrim(coalesce(tn.effective_daily_schedule_json->>'breakMin','')), '')::numeric
                )
              )
            )
          else '[]'::jsonb
        end
      ) AS synthetic_component_breaks
    FROM tf_norm tn
  ),
  segment_components AS (
    SELECT
      nullif(btrim(COALESCE(seg.value->>'segment_id','')), '') AS component_id,
      nullif(btrim(COALESCE(seg.value->>'date','')), '') AS component_date,
      coalesce(
        nullif(btrim(COALESCE(seg.value->>'segment_stable_key','')), ''),
        nullif(btrim(COALESCE(seg.value->>'segment_id','')), ''),
        nullif(btrim(COALESCE(seg.value->>'segment_key','')), ''),
        nullif(btrim(COALESCE(seg.value->>'ref_num','')), ''),
        nullif(btrim(COALESCE(seg.value->>'date','')), '')
      ) AS component_stable_key,
      nullif(btrim(COALESCE(seg.value->>'ref_num','')), '') AS component_ref_num,
      COALESCE(NULLIF(seg.value->>'exclude_from_pay','')::boolean, false) AS is_on_hold,
      nullif(btrim(COALESCE(seg.value->>'start_utc','')), '') AS component_start_utc,
      nullif(btrim(COALESCE(seg.value->>'end_utc','')), '') AS component_end_utc,
      case
        when nullif(btrim(COALESCE(seg.value->>'start','')), '') is not null then nullif(btrim(COALESCE(seg.value->>'start','')), '')
        when nullif(btrim(COALESCE(seg.value->>'start_utc','')), '') is not null then to_char(((seg.value->>'start_utc')::timestamptz AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else null
      end AS component_start,
      case
        when nullif(btrim(COALESCE(seg.value->>'end','')), '') is not null then nullif(btrim(COALESCE(seg.value->>'end','')), '')
        when nullif(btrim(COALESCE(seg.value->>'end_utc','')), '') is not null then to_char(((seg.value->>'end_utc')::timestamptz AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else null
      end AS component_end,
      nullif(btrim(COALESCE(seg.value->>'break_start','')), '') AS component_break_start,
      nullif(btrim(COALESCE(seg.value->>'break_end','')), '') AS component_break_end,
      coalesce(
        nullif(seg.value->>'break_mins','')::numeric,
        nullif(seg.value->>'break_minutes','')::numeric
      ) AS component_break_mins,
      case
        when jsonb_typeof(seg.value->'breaks') = 'array' then seg.value->'breaks'
        else '[]'::jsonb
      end AS component_breaks
    FROM mode_flags mf
    JOIN LATERAL jsonb_array_elements(COALESCE(mf.invoice_breakdown_json->'segments','[]'::jsonb)) AS seg(value)
      ON true
    WHERE mf.actual_segments_mode = true
      AND seg.value IS NOT NULL
      AND jsonb_typeof(seg.value) = 'object'
      AND nullif(btrim(COALESCE(seg.value->>'segment_id','')), '') IS NOT NULL

    UNION ALL

    SELECT
      ('ts:' || mf.timesheet_id::text) AS component_id,
      mf.synthetic_component_date AS component_date,
      ('timesheet:' || COALESCE(mf.booking_id, mf.timesheet_id::text)) AS component_stable_key,
      coalesce(
        mf.reference_number,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'ref_num','')), '')
      ) AS component_ref_num,
      mf.pay_on_hold AS is_on_hold,
      CASE
        WHEN mf.effective_worked_start_iso IS NULL THEN NULL
        ELSE mf.effective_worked_start_iso::text
      END AS component_start_utc,
      CASE
        WHEN mf.effective_worked_end_iso IS NULL THEN NULL
        ELSE mf.effective_worked_end_iso::text
      END AS component_end_utc,
      case
        when mf.effective_worked_start_iso is not null then to_char((mf.effective_worked_start_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else coalesce(
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'start','')), ''),
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'worked_start','')), '')
        )
      end AS component_start,
      case
        when mf.effective_worked_end_iso is not null then to_char((mf.effective_worked_end_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else coalesce(
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'end','')), ''),
          nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'worked_end','')), '')
        )
      end AS component_end,
      case
        when mf.effective_break_start_iso is not null then to_char((mf.effective_break_start_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_start','')), '')
      end AS component_break_start,
      case
        when mf.effective_break_end_iso is not null then to_char((mf.effective_break_end_iso AT TIME ZONE 'Europe/London'), 'HH24:MI')
        else nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_end','')), '')
      end AS component_break_end,
      coalesce(
        mf.effective_break_minutes::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_minutes','')), '')::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'break_mins','')), '')::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'breakMinutes','')), '')::numeric,
        nullif(btrim(coalesce(mf.effective_daily_schedule_json->>'breakMin','')), '')::numeric,
        case
          when mf.effective_break_start_iso is not null and mf.effective_break_end_iso is not null
            then greatest(
              0::numeric,
              round((extract(epoch from (mf.effective_break_end_iso - mf.effective_break_start_iso)) / 60.0)::numeric, 0)
            )
          else null::numeric
        end
      ) AS component_break_mins,
      coalesce(mf.synthetic_component_breaks, '[]'::jsonb) AS component_breaks
    FROM mode_flags mf
    WHERE mf.actual_segments_mode = false
      AND mf.synthetic_daily_mode = true
  ),
  components AS (
    SELECT
      sc.component_id,
      sc.component_date,
      sc.component_stable_key,
      sc.component_ref_num,
      sc.is_on_hold,
      sc.component_start_utc,
      sc.component_end_utc,
      sc.component_start,
      sc.component_end,
      sc.component_break_start,
      sc.component_break_end,
      sc.component_break_mins,
      sc.component_breaks
    FROM segment_components sc

    UNION ALL

    SELECT
      'TOTAL'::text AS component_id,
      NULL::text AS component_date,
      ('timesheet:' || COALESCE(mf.booking_id, mf.timesheet_id::text))::text AS component_stable_key,
      NULL::text AS component_ref_num,
      mf.pay_on_hold AS is_on_hold,
      NULL::text AS component_start_utc,
      NULL::text AS component_end_utc,
      NULL::text AS component_start,
      NULL::text AS component_end,
      NULL::text AS component_break_start,
      NULL::text AS component_break_end,
      NULL::numeric AS component_break_mins,
      '[]'::jsonb AS component_breaks
    FROM mode_flags mf
    WHERE mf.actual_segments_mode = false
      AND mf.synthetic_daily_mode = false
  ),
  active_override AS (
    SELECT
      tpo.id AS override_id,
      tpo.timesheet_id,
      tpo.reason,
      tpo.created_at_utc,
      tpo.consumed_at_utc,
      CASE
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) <> 'CANCELLED'
          THEN pb.id
        ELSE NULL
      END AS consumed_by_batch_id,
      CASE
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) <> 'CANCELLED'
          THEN upper(COALESCE(pb.status::text,''))
        ELSE NULL
      END AS consumed_batch_status,
      CASE
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) <> 'CANCELLED'
          THEN false
        ELSE true
      END AS can_unadvance,
      CASE
        WHEN tpo.consumed_by_pay_batch_id IS NULL THEN 'ADVANCED_PENDING_BATCH'
        WHEN pb.id IS NOT NULL AND upper(COALESCE(pb.status::text,'')) = 'CANCELLED' THEN 'ADVANCED_BATCH_CANCELLED'
        WHEN pb.id IS NOT NULL THEN 'ADVANCED_IN_BATCH'
        ELSE 'ADVANCED_PENDING_BATCH'
      END AS advance_status
    FROM public.timesheet_payment_overrides tpo
    LEFT JOIN public.pay_batches pb
      ON pb.id = tpo.consumed_by_pay_batch_id
    WHERE tpo.timesheet_id = p_timesheet_id
      AND tpo.cleared_at_utc IS NULL
      AND upper(coalesce(tpo.override_type, '')) = 'ADVANCE_THIS_PAYMENT'
    ORDER BY tpo.created_at_utc DESC, tpo.id DESC
    LIMIT 1
  ),
  active_payment_snooze AS (
    SELECT
      s.id AS snooze_id,
      s.snooze_until_date,
      s.note
    FROM tf_norm tn
    JOIN public.pay_item_snoozes s
      ON s.candidate_id = tn.candidate_id
     AND s.source_ref IS NULL
     AND s.segment_id IS NULL
     AND s.segment_stable_key IS NULL
     AND s.cleared_at_utc IS NULL
     AND upper(COALESCE(s.snooze_kind,'')) = 'TIMESHEET_PAYMENT'
     AND (s.snooze_until_date IS NULL OR s.snooze_until_date >= v_today_uk)
     AND (
       (tn.booking_id IS NOT NULL AND s.booking_id IS NOT DISTINCT FROM tn.booking_id)
       OR
       (s.booking_id IS NULL AND s.timesheet_id = p_timesheet_id)
     )
    ORDER BY s.updated_at_utc DESC NULLS LAST, s.created_at_utc DESC, s.id DESC
    LIMIT 1
  ),
  active_segment_snoozes_source AS (
    SELECT
      s.id AS snooze_id,
      s.candidate_id,
      s.timesheet_id,
      s.booking_id,
      s.segment_id,
      coalesce(
        nullif(btrim(COALESCE(s.segment_stable_key,'')), ''),
        nullif(btrim(COALESCE(s.segment_id,'')), '')
      ) AS segment_stable_key,
      upper(COALESCE(s.snooze_kind,'')) AS snooze_kind,
      s.snooze_until_date,
      s.note,
      s.created_at_utc,
      s.updated_at_utc
    FROM tf_norm tn
    JOIN public.pay_item_snoozes s
      ON s.candidate_id = tn.candidate_id
     AND s.source_ref IS NULL
     AND s.cleared_at_utc IS NULL
     AND upper(COALESCE(s.snooze_kind,'')) IN ('DO_NOT_PAY', 'BLOCKED_TIMESHEET')
     AND (s.snooze_until_date IS NULL OR s.snooze_until_date >= v_today_uk)
     AND coalesce(
           nullif(btrim(COALESCE(s.segment_stable_key,'')), ''),
           nullif(btrim(COALESCE(s.segment_id,'')), '')
         ) IS NOT NULL
     AND (
       (tn.booking_id IS NOT NULL AND s.booking_id IS NOT DISTINCT FROM tn.booking_id)
       OR
       (s.booking_id IS NULL AND s.timesheet_id = p_timesheet_id)
     )
  ),
  active_segment_snooze_matches AS (
    SELECT
      sc.component_id,
      sc.component_date,
      sc.component_stable_key,
      sc.component_ref_num,
      sc.component_start_utc,
      sc.component_end_utc,
      sc.component_start,
      sc.component_end,
      sc.component_break_start,
      sc.component_break_end,
      sc.component_break_mins,
      sc.component_breaks,
      ass.snooze_id,
      ass.candidate_id,
      ass.timesheet_id,
      ass.booking_id,
      ass.segment_id,
      ass.segment_stable_key,
      ass.snooze_kind,
      ass.snooze_until_date,
      ass.note,
      ass.created_at_utc,
      ass.updated_at_utc,
      row_number() OVER (
        PARTITION BY sc.component_id
        ORDER BY ass.updated_at_utc DESC NULLS LAST, ass.created_at_utc DESC, ass.snooze_id DESC
      ) AS rn
    FROM segment_components sc
    JOIN active_segment_snoozes_source ass
      ON (
        (
          ass.booking_id IS NOT NULL
          AND (
            ass.segment_stable_key IS NOT DISTINCT FROM sc.component_stable_key
            OR ass.segment_id IS NOT DISTINCT FROM sc.component_id
          )
        )
        OR
        (
          ass.booking_id IS NULL
          AND ass.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
          AND (
            ass.segment_id IS NOT DISTINCT FROM sc.component_id
            OR ass.segment_stable_key IS NOT DISTINCT FROM sc.component_stable_key
          )
        )
      )
  ),
  active_segment_snoozes AS (
    SELECT
      asm.component_id,
      asm.component_date,
      asm.component_stable_key,
      asm.component_ref_num,
      asm.component_start_utc,
      asm.component_end_utc,
      asm.component_start,
      asm.component_end,
      asm.component_break_start,
      asm.component_break_end,
      asm.component_break_mins,
      asm.component_breaks,
      asm.snooze_id,
      asm.candidate_id,
      asm.timesheet_id,
      asm.booking_id,
      asm.segment_id,
      asm.segment_stable_key,
      asm.snooze_kind,
      asm.snooze_until_date,
      asm.note,
      asm.created_at_utc,
      asm.updated_at_utc
    FROM active_segment_snooze_matches asm
    WHERE asm.rn = 1
  ),
  pay_items AS (
    SELECT
      CASE
        WHEN nullif(btrim(COALESCE(pbi.segment_key,'')), '') IS NOT NULL
          THEN nullif(btrim(COALESCE(pbi.segment_key,'')), '')
        WHEN pbi.source_ref IS NOT NULL AND btrim(COALESCE(pbi.source_ref,'')) LIKE 'seg:%'
          THEN nullif(btrim(substring(btrim(COALESCE(pbi.source_ref,'')) from 5)), '')
        ELSE 'TOTAL'
      END AS component_id,
      pb.status AS batch_status,
      pb.completed_at_utc AS completed_at_utc,
      pbt.status AS transfer_status,
      pbt.rail_state AS transfer_rail_state,
      COALESCE(pbt_classifier.is_final_money_moved, false) AS transfer_final_money_moved,
      COALESCE(pbt_classifier.is_pending_non_final, false) AS transfer_pending_non_final,
      COALESCE(pbt_classifier.is_terminal_no_money, false) AS transfer_terminal_no_money
    FROM public.pay_batch_items pbi
    JOIN public.pay_batch_candidates pbc
      ON pbc.id = pbi.pay_batch_candidate_id
    JOIN public.pay_batches pb
      ON pb.id = pbc.pay_batch_id
    LEFT JOIN public.pay_bank_transfers pbt
      ON pbt.id = pbi.pay_bank_transfer_id
     AND pbt.pay_batch_id = pb.id
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      pbt.status,
      pbt.rail_state,
      COALESCE(pbt.rail_meta_json, '{}'::jsonb),
      COALESCE(pbt.rail_meta_json, '{}'::jsonb)
    ) AS pbt_classifier
      ON pbt.id IS NOT NULL
    WHERE pbi.timesheet_id = p_timesheet_id
      AND pbi.is_voided = false
      AND pbi.item_type IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
      AND pb.cancelled_at_utc IS NULL
  ),
  agg AS (
    SELECT
      pi.component_id,
      max(CASE WHEN upper(COALESCE(pi.batch_status::text,'')) = 'SETTLED' THEN 1 ELSE 0 END)::int AS has_settled_norm,
      max(CASE WHEN upper(COALESCE(pi.batch_status::text,'')) = 'SETTLED' THEN pi.completed_at_utc ELSE NULL END) AS settled_at_utc,
      max(CASE
            WHEN COALESCE(pi.transfer_final_money_moved, false) = false
             AND COALESCE(pi.transfer_terminal_no_money, false) = false
             AND (
               COALESCE(pi.transfer_pending_non_final, false) = true
               OR upper(COALESCE(pi.batch_status::text,'')) IN ('EXECUTING','WAITING_BANK_CONFIRM')
               OR upper(COALESCE(pi.transfer_status::text,'')) IN ('PENDING','PROCESSING','UNKNOWN')
             )
              THEN 1
            ELSE 0
          END)::int AS has_proc_any
    FROM pay_items pi
    WHERE pi.component_id IS NOT NULL
    GROUP BY pi.component_id
  ),
  comp_state AS (
    SELECT
      c.component_id,
      c.component_date,
      c.component_stable_key,
      c.component_ref_num,
      c.is_on_hold,
      c.component_start_utc,
      c.component_end_utc,
      c.component_start,
      c.component_end,
      c.component_break_start,
      c.component_break_end,
      c.component_break_mins,
      c.component_breaks,
      CASE
        WHEN c.is_on_hold = true THEN 'ON_HOLD'
        WHEN COALESCE(a.has_settled_norm,0) = 1 THEN 'PAID'
        WHEN ao.override_id IS NOT NULL AND COALESCE(a.has_proc_any,0) = 1 THEN 'ADVANCE_PROCESSING'
        WHEN COALESCE(a.has_proc_any,0) = 1 THEN 'PAY_PROCESSING'
        WHEN ao.override_id IS NOT NULL THEN 'ADVANCED'
        ELSE 'UNPAID'
      END AS stage,
      a.settled_at_utc,
      CASE
        WHEN c.component_id = 'TOTAL' THEN 'NONE'
        WHEN ass.snooze_id IS NULL THEN 'NONE'
        WHEN ass.snooze_until_date IS NULL THEN 'INDEFINITE_SNOOZED'
        ELSE 'DATED_SNOOZED'
      END AS snooze_state,
      ass.snooze_id,
      ass.snooze_until_date,
      ass.note AS snooze_note,
      ass.segment_id AS snooze_segment_id,
      ass.segment_stable_key AS snooze_segment_stable_key
    FROM components c
    LEFT JOIN agg a
      ON a.component_id = c.component_id
    LEFT JOIN active_override ao
      ON true
    LEFT JOIN active_segment_snoozes ass
      ON ass.component_id = c.component_id
  ),
  counts AS (
    SELECT
      count(*)::int AS total_components,
      count(*) FILTER (WHERE cs.is_on_hold = true)::int AS on_hold_components,
      count(*) FILTER (WHERE cs.is_on_hold = false)::int AS payable_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage = 'PAID')::int AS paid_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage = 'ADVANCED')::int AS advanced_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage IN ('PAY_PROCESSING','ADVANCE_PROCESSING'))::int AS processing_components,
      count(*) FILTER (WHERE cs.is_on_hold = false AND cs.stage = 'UNPAID')::int AS unpaid_components,
      max(CASE WHEN cs.is_on_hold = false AND cs.stage IN ('PAY_PROCESSING','ADVANCE_PROCESSING') THEN 1 ELSE 0 END)::int AS any_processing,
      max(CASE WHEN cs.is_on_hold = false AND cs.stage = 'ADVANCED' THEN 1 ELSE 0 END)::int AS any_advanced,
      max(CASE WHEN cs.is_on_hold = false AND cs.stage = 'PAID' THEN cs.settled_at_utc ELSE NULL END) AS paid_at_utc
    FROM comp_state cs
  ),
  pay_cache AS (
    SELECT
      cache_row.timesheet_id,
      CASE
        WHEN upper(coalesce(cache_row.summary_pay_status_code, '')) IN ('PAID','PARTIALLY_PAID','PROCESSING','ADVANCED')
          THEN upper(coalesce(cache_row.summary_pay_status_code, ''))
        WHEN cache_row.last_settled_at_utc IS NOT NULL
          THEN 'PAID'
        WHEN upper(coalesce(cache_row.summary_pay_status_code, '')) = 'UNPAID'
          THEN 'UNPAID'
        ELSE NULL::text
      END AS cached_pay_status_code,
      coalesce(cache_row.summary_pay_paid_at_utc, cache_row.last_settled_at_utc) AS cached_paid_at_utc,
      cache_row.summary_pay_icon_code,
      cache_row.summary_net_delta_ex_vat,
      cache_row.last_settled_pay_batch_id,
      cache_row.last_settled_at_utc
    FROM (SELECT 1) seed(seed_id)
    LEFT JOIN LATERAL (
      SELECT
        tps.timesheet_id,
        tps.summary_pay_status_code,
        tps.summary_pay_icon_code,
        tps.summary_pay_paid_at_utc,
        tps.summary_net_delta_ex_vat,
        tps.last_settled_pay_batch_id,
        tps.last_settled_at_utc
      FROM public.timesheet_pay_state tps
      WHERE tps.timesheet_id = p_timesheet_id
      LIMIT 1
    ) cache_row
      ON true
  ),
  unpaid_sample AS (
    SELECT
      coalesce(
        jsonb_agg(cs.component_date ORDER BY cs.component_date) FILTER (WHERE cs.component_date IS NOT NULL),
        '[]'::jsonb
      ) AS dates
    FROM (
      SELECT cs.component_date
      FROM comp_state cs
      WHERE cs.is_on_hold = false
        AND cs.stage = 'UNPAID'
        AND cs.component_date IS NOT NULL
      ORDER BY cs.component_date
      LIMIT 3
    ) cs
  ),
  delta AS (
    SELECT
      round(coalesce(sum(coalesce(oc.truth_ex_vat,0) - coalesce(oc.baseline_ex_vat,0)),0),2)::numeric AS net_delta_ex_vat,
      round(coalesce(sum(coalesce(oc.outstanding_ex_vat,0)),0),2)::numeric AS outstanding_ex_vat,
      round(coalesce(sum(coalesce(oc.reserved_ex_vat,0)),0),2)::numeric AS reserved_ex_vat
    FROM public._pay_outstanding_components(ARRAY[p_timesheet_id]) oc
  ),
  paid_totals AS (
    SELECT
      round(coalesce(sum(coalesce(paid_item.amount_ex_vat, 0)), 0), 2)::numeric AS paid_to_date_ex_vat,
      count(paid_item.id)::int AS paid_item_count,
      max(coalesce(paid_candidate.settled_at_utc, paid_transfer.completed_at_utc, paid_batch.completed_at_utc)) AS last_paid_at_utc
    FROM public.pay_batch_items AS paid_item
    JOIN public.pay_batch_candidates AS paid_candidate
      ON paid_candidate.id = paid_item.pay_batch_candidate_id
    JOIN public.pay_batches AS paid_batch
      ON paid_batch.id = paid_candidate.pay_batch_id
    LEFT JOIN public.pay_bank_transfers AS paid_transfer
      ON paid_transfer.id = paid_item.pay_bank_transfer_id
     AND paid_transfer.pay_batch_id = paid_batch.id
    LEFT JOIN LATERAL public._pay_rail_state_money_movement_classify(
      paid_transfer.status,
      paid_transfer.rail_state,
      coalesce(paid_transfer.rail_meta_json, '{}'::jsonb),
      coalesce(paid_transfer.rail_meta_json, '{}'::jsonb)
    ) AS paid_transfer_classifier
      ON paid_transfer.id IS NOT NULL
    WHERE paid_item.timesheet_id = p_timesheet_id
      AND coalesce(paid_item.is_voided, false) = false
      AND paid_item.item_type IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
      AND paid_batch.cancelled_at_utc IS NULL
      AND upper(btrim(coalesce(paid_batch.status, ''))) <> 'CANCELLED'
      AND (
        upper(btrim(coalesce(paid_candidate.settlement_status, ''))) IN ('SETTLED','PAID','CONFIRMED')
        OR upper(btrim(coalesce(paid_batch.status, ''))) = 'SETTLED'
        OR coalesce(paid_transfer_classifier.is_final_money_moved, false) = true
        OR upper(btrim(coalesce(paid_transfer.status, ''))) IN ('COMPLETED','COMPLETE','SETTLED','PAID','EXECUTED','COMMITTED','SUCCESS','SUCCESSFUL','SUCCEEDED')
        OR upper(btrim(coalesce(paid_transfer.rail_state, ''))) IN ('COMPLETED','COMPLETE','SETTLED','PAID','EXECUTED','COMMITTED','SUCCESS','SUCCESSFUL','SUCCEEDED')
        OR paid_transfer.completed_at_utc IS NOT NULL
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_settlement_scope AS paid_settlement_scope
          WHERE paid_settlement_scope.pay_batch_id = paid_batch.id
            AND paid_settlement_scope.pay_batch_candidate_id = paid_candidate.id
            AND paid_settlement_scope.status = 'SETTLED'
            AND (
              (
                paid_item.pay_bank_transfer_id IS NOT NULL
                AND paid_settlement_scope.payload_json #>> '{payment_scope_json,pay_bank_transfer_id}' = paid_item.pay_bank_transfer_id::text
              )
              OR coalesce(paid_settlement_scope.payload_json->'pay_batch_item_ids', '[]'::jsonb) ? paid_item.id::text
            )
        )
      )
  ),
  mode AS (
    SELECT
      COALESCE((SELECT (mf.actual_segments_mode OR mf.synthetic_daily_mode) FROM mode_flags mf LIMIT 1), false) AS is_segments_mode
  ),
  comp_json AS (
    SELECT
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'component_id', cs.component_id,
            'date', cs.component_date,
            'segment_stable_key', cs.component_stable_key,
            'ref_num', cs.component_ref_num,
            'is_on_hold', cs.is_on_hold,
            'stage', cs.stage,
            'paid_at_utc', cs.settled_at_utc,
            'paid_at_label_uk',
              CASE
                WHEN cs.settled_at_utc IS NULL THEN NULL
                ELSE to_char(cs.settled_at_utc AT TIME ZONE 'Europe/London', 'Dy DD/MM/YYYY HH24:MI') || 'hrs'
              END,
            'snooze_state', cs.snooze_state,
            'snooze_id', CASE WHEN cs.snooze_id IS NULL THEN NULL ELSE cs.snooze_id::text END,
            'snooze_until_date', CASE WHEN cs.snooze_until_date IS NULL THEN NULL ELSE cs.snooze_until_date::text END,
            'snooze_note', cs.snooze_note,
            'snooze_segment_id', cs.snooze_segment_id,
            'snooze_segment_stable_key', cs.snooze_segment_stable_key,
            'start_utc', cs.component_start_utc,
            'end_utc', cs.component_end_utc,
            'start', cs.component_start,
            'end', cs.component_end,
            'break_start', cs.component_break_start,
            'break_end', cs.component_break_end,
            'break_mins', cs.component_break_mins,
            'breaks', coalesce(cs.component_breaks, '[]'::jsonb)
          )
          ORDER BY
            CASE WHEN cs.component_id = 'TOTAL' THEN 0 ELSE 1 END,
            cs.component_date NULLS LAST,
            cs.component_id
        ),
        '[]'::jsonb
      ) AS components
    FROM comp_state cs
  ),
  segment_snoozes_json AS (
    SELECT
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'snooze_id', ass.snooze_id::text,
            'candidate_id', ass.candidate_id::text,
            'timesheet_id', CASE WHEN ass.timesheet_id IS NULL THEN NULL ELSE ass.timesheet_id::text END,
            'booking_id', ass.booking_id,
            'segment_id', ass.segment_id,
            'segment_stable_key', ass.segment_stable_key,
            'snooze_kind', ass.snooze_kind,
            'snooze_until_date', CASE WHEN ass.snooze_until_date IS NULL THEN NULL ELSE ass.snooze_until_date::text END,
            'note', ass.note,
            'snooze_note', ass.note,
            'date', ass.component_date,
            'work_date', ass.component_date,
            'ref_num', ass.component_ref_num,
            'start_utc', ass.component_start_utc,
            'end_utc', ass.component_end_utc,
            'start', ass.component_start,
            'end', ass.component_end,
            'break_start', ass.component_break_start,
            'break_end', ass.component_break_end,
            'break_mins', ass.component_break_mins,
            'breaks', coalesce(ass.component_breaks, '[]'::jsonb),
            'snooze_state',
              CASE
                WHEN ass.snooze_until_date IS NULL THEN 'INDEFINITE_SNOOZED'
                ELSE 'DATED_SNOOZED'
              END
          )
          ORDER BY ass.component_date NULLS LAST, ass.segment_stable_key NULLS LAST, ass.segment_id NULLS LAST
        ),
        '[]'::jsonb
      ) AS segment_snoozes
    FROM active_segment_snoozes ass
  ),
  paid_status AS (
    SELECT
      resolved.paid_status_code,
      resolved.paid_at_utc,
      CASE
        WHEN resolved.paid_at_utc IS NULL THEN NULL
        ELSE to_char(resolved.paid_at_utc AT TIME ZONE 'Europe/London', 'Dy DD/MM/YYYY HH24:MI') || 'hrs'
      END AS paid_at_label_uk
    FROM (
      SELECT
        CASE
          WHEN c.processing_components > 0 OR pc.cached_pay_status_code = 'PROCESSING' THEN 'PROCESSING'
          WHEN coalesce(pt.paid_to_date_ex_vat,0) > 0
               AND greatest(coalesce(d.reserved_ex_vat,0),0) <= 0
               AND greatest(coalesce(d.outstanding_ex_vat,0),0) <= 0
               AND coalesce(d.net_delta_ex_vat,0) <= 0 THEN 'PAID'
          WHEN coalesce(pt.paid_to_date_ex_vat,0) > 0 THEN 'PARTIALLY_PAID'
          ELSE 'UNPAID'
        END AS paid_status_code,
        coalesce(pt.last_paid_at_utc, pc.cached_paid_at_utc, c.paid_at_utc) AS paid_at_utc
      FROM counts c
      CROSS JOIN pay_cache pc
      CROSS JOIN paid_totals pt
      CROSS JOIN delta d
    ) resolved
  ),
  adjusted AS (
    SELECT
      coalesce(d.net_delta_ex_vat,0) AS net_delta_ex_vat,
      greatest(coalesce(d.outstanding_ex_vat,0),0) AS outstanding_ex_vat,
      greatest(coalesce(d.reserved_ex_vat,0),0) AS reserved_ex_vat,
      CASE
        WHEN greatest(coalesce(d.outstanding_ex_vat,0),0) > 0 THEN 'PAY_OUTSTANDING'
        WHEN coalesce(d.net_delta_ex_vat,0) < 0 THEN 'OVERPAID'
        ELSE 'NONE'
      END AS adjusted_pill,
      CASE
        WHEN greatest(coalesce(d.outstanding_ex_vat,0),0) > 0 THEN
          'Timesheet adjusted after payment. Additional pay outstanding: £' || to_char(greatest(coalesce(d.outstanding_ex_vat,0),0), 'FM999999990D00')
        WHEN coalesce(d.net_delta_ex_vat,0) > 0
             AND greatest(coalesce(d.reserved_ex_vat,0),0) >= coalesce(d.net_delta_ex_vat,0)
             AND greatest(coalesce(d.outstanding_ex_vat,0),0) <= 0 THEN
          'Timesheet adjusted after payment. Additional pay is reserved. No unreserved amount is currently owed.'
        WHEN coalesce(d.net_delta_ex_vat,0) < 0 THEN
          'Timesheet adjusted after payment. Current overpaid position: £' || to_char(abs(coalesce(d.net_delta_ex_vat,0)), 'FM999999990D00')
        ELSE NULL
      END AS adjusted_message
    FROM delta d
  ),
  hover AS (
    SELECT
      jsonb_build_object(
        'paid', c.paid_components,
        'processing', c.processing_components,
        'unpaid', c.unpaid_components,
        'on_hold', c.on_hold_components,
        'total', c.total_components,
        'payable', c.payable_components,
        'last_payment_utc', ps.paid_at_utc,
        'last_payment_label_uk', ps.paid_at_label_uk,
        'unpaid_sample_dates', us.dates,
        'net_delta_ex_vat', d.net_delta_ex_vat,
        'outstanding_ex_vat', d.outstanding_ex_vat,
        'reserved_ex_vat', d.reserved_ex_vat
      ) AS hover_summary
    FROM counts c
    CROSS JOIN unpaid_sample us
    CROSS JOIN delta d
    CROSS JOIN paid_status ps
  ),
  override_json AS (
    SELECT
      COALESCE((SELECT ao.override_id IS NOT NULL FROM active_override ao LIMIT 1), false) AS is_advanced,
      COALESCE((SELECT ao.can_unadvance FROM active_override ao LIMIT 1), false) AS can_unadvance,
      (SELECT ao.consumed_by_batch_id FROM active_override ao LIMIT 1) AS advanced_consumed_by_batch_id,
      (SELECT ao.consumed_at_utc FROM active_override ao LIMIT 1) AS advanced_consumed_at_utc,
      (SELECT ao.consumed_batch_status FROM active_override ao LIMIT 1) AS advanced_batch_status,
      COALESCE((SELECT ao.advance_status FROM active_override ao LIMIT 1), 'NOT_ADVANCED') AS advance_status
  ),
  snooze_json AS (
    SELECT
      COALESCE((SELECT aps.snooze_id IS NOT NULL FROM active_payment_snooze aps LIMIT 1), false) AS is_snoozed,
      (SELECT aps.snooze_until_date FROM active_payment_snooze aps LIMIT 1) AS snooze_until_date,
      COALESCE((SELECT aps.snooze_until_date IS NULL FROM active_payment_snooze aps LIMIT 1), false) AS snooze_is_indefinite,
      (SELECT aps.note FROM active_payment_snooze aps LIMIT 1) AS snooze_note
  )
  SELECT
    jsonb_build_object(
      'ok', true,
      'timesheet_id', p_timesheet_id::text,
      'is_segments_mode', m.is_segments_mode,
      'paid_status', ps.paid_status_code,
      'paid_at_utc', ps.paid_at_utc,
      'paid_at_label_uk', ps.paid_at_label_uk,
      'advanced_any', ((c.any_advanced = 1) OR ps.paid_status_code = 'ADVANCED'),
      'processing_any', ((c.any_processing = 1) OR ps.paid_status_code = 'PROCESSING'),
      'is_advanced', oj.is_advanced,
      'can_unadvance', oj.can_unadvance,
      'advanced_consumed_by_batch_id', CASE WHEN oj.advanced_consumed_by_batch_id IS NULL THEN NULL ELSE oj.advanced_consumed_by_batch_id::text END,
      'advanced_consumed_at_utc', oj.advanced_consumed_at_utc,
      'advanced_batch_status', oj.advanced_batch_status,
      'advance_status', oj.advance_status,
      'is_snoozed', sj.is_snoozed,
      'snooze_until_date', CASE WHEN sj.snooze_until_date IS NULL THEN NULL ELSE sj.snooze_until_date::text END,
      'snooze_is_indefinite', sj.snooze_is_indefinite,
      'snooze_note', sj.snooze_note,
      'counts', jsonb_build_object(
        'total_components', c.total_components,
        'payable_components', c.payable_components,
        'on_hold_components', c.on_hold_components,
        'paid_components', c.paid_components,
        'advanced_components', c.advanced_components,
        'processing_components', c.processing_components,
        'unpaid_components', c.unpaid_components
      ),
      'adjusted', jsonb_build_object(
        'net_delta_ex_vat', a.net_delta_ex_vat,
        'pill', a.adjusted_pill,
        'message', a.adjusted_message,
        'outstanding_ex_vat', a.outstanding_ex_vat,
        'reserved_ex_vat', a.reserved_ex_vat
      ),
      'paid_totals', jsonb_build_object(
        'paid_to_date_ex_vat', coalesce(pt.paid_to_date_ex_vat, 0),
        'paid_item_count', coalesce(pt.paid_item_count, 0),
        'last_paid_at_utc', pt.last_paid_at_utc
      ),
      'hover', h.hover_summary,
      'components', cj.components,
      'segment_snoozes', ssj.segment_snoozes
    )
  INTO v_out
  FROM counts c
  CROSS JOIN mode m
  CROSS JOIN paid_status ps
  CROSS JOIN adjusted a
  CROSS JOIN paid_totals pt
  CROSS JOIN hover h
  CROSS JOIN comp_json cj
  CROSS JOIN segment_snoozes_json ssj
  CROSS JOIN override_json oj
  CROSS JOIN snooze_json sj;

  v_has_tsfin := EXISTS (
    SELECT 1
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
      AND tf.is_current = true
  );

  IF v_out IS NULL THEN
    v_out := jsonb_build_object(
      'ok', false,
      'timesheet_id', p_timesheet_id::text,
      'error', 'NO_DATA'
    );
  END IF;

  -- Native diagnostic audit belongs to the unchanged public owner only.

  RETURN v_out;
END;
$function$;
create or replace function private.weekly_source_checklist_legacy_read_frame_v1(
 p_timesheet_id uuid,p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $legacy_frame$
declare actor public.tms_users%rowtype;t public.timesheets%rowtype;f public.timesheets_financials%rowtype;
 module private.bpay_next_module_control%rowtype;witness jsonb;permission jsonb;facts jsonb;
 financial_scope jsonb;financial_scope_after jsonb;state jsonb;state_after jsonb;
 frame jsonb;frame_after jsonb;n bigint;proof text;
begin
 perform private.weekly_source_query_require_service_v1();
 perform private._weekly_source_settings_assert_request_v1(p_scope,array['source_group_id','source_cycle_id','client_id','week_ending'],
  'WEEKLY_SOURCE_CHECKLIST_LEGACY_SCOPE_INVALID');
 if p_timesheet_id is null or p_actor_user_id is null or exists(select 1 from unnest(array['source_group_id','source_cycle_id','client_id']) k
  where jsonb_typeof(p_scope->k) is distinct from 'string' or pg_input_is_valid(p_scope->>k,'uuid') is distinct from true)
  or jsonb_typeof(p_scope->'week_ending') is distinct from 'string' or pg_input_is_valid(p_scope->>'week_ending','date') is distinct from true then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_LEGACY_SCOPE_INVALID' using errcode='22023';end if;
 select a.* into actor from public.tms_users a where a.id=p_actor_user_id and a.is_active;
 if actor.id is null or (lower(btrim(coalesce(actor.role,'')))<>'admin' and private.weekly_source_pay_actor_allowed_v1(actor.id) is distinct from true) then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501';end if;
 permission:=private.weekly_source_office_authority_v1(actor.id,'VIEW_PAY_WORK',(p_scope->>'source_group_id')::uuid,
  (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
 if not exists(select 1 from public.weekly_source_cycles c join public.weekly_source_groups g on g.id=c.source_group_id and g.active
  join public.weekly_source_group_clients m on m.source_group_id=g.id and m.client_id=(p_scope->>'client_id')::uuid
  where c.id=(p_scope->>'source_cycle_id')::uuid and g.id=(p_scope->>'source_group_id')::uuid
   and (c.scope_client_id is null or c.scope_client_id=m.client_id)
   and (p_scope->>'week_ending')::date between c.finalisation_week_ending-6 and c.finalisation_week_ending
   and (p_scope->>'week_ending')::date between m.valid_from and coalesce(m.valid_to,'infinity'::date)) then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_LEGACY_SCOPE_UNPROVED' using errcode='55000';end if;
 witness:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
 if witness->'actual_submitted' is distinct from 'true'::jsonb or witness->>'client_id' is distinct from p_scope->>'client_id'
  or witness->>'week_ending_date' is distinct from p_scope->>'week_ending' then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_LEGACY_SUBJECT_UNPROVED' using errcode='55000';end if;
 select m.* into module from private.bpay_next_module_control m where m.id=1;
 facts:=jsonb_build_object('scope',p_scope,'actor_user_id',actor.id,'permission',permission,'actual_submission',witness,'module',to_jsonb(module));
 if module.active_owner is distinct from 'LEGACY' then
  return jsonb_build_object('known',false,'reason','NATIVE_LEGACY_OWNER_UNAVAILABLE','native_facts',facts);end if;
 select r.* into t from public.timesheets r where r.timesheet_id=p_timesheet_id;
 select count(*),(array_agg(r.id order by r.id))[1] into n,f.id from public.timesheets_financials r where r.timesheet_id=t.timesheet_id and r.is_current;
 if n<>1 then return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_FINANCIAL_AMBIGUOUS','native_facts',facts);end if;
 select r.* into strict f from public.timesheets_financials r where r.id=f.id;
 facts:=facts||jsonb_build_object('root',to_jsonb(t),'current_financial',to_jsonb(f));
 if t.is_current is distinct from true or t.revoked_at is not null or t.archived_at_utc is not null or t.authorised_at_server is null
  or t.line_type::text is distinct from 'HOURS' or t.sheet_scope::text not in('WEEKLY','DAILY')
  or f.timesheet_version is distinct from t.version or f.is_stale is distinct from false or f.authorised_at_utc is null
  or f.client_id::text is distinct from witness->>'client_id' or f.candidate_id::text is distinct from witness->>'candidate_id'
  or private.weekly_source_pay_query_scope_v1(t.timesheet_id) is not null
  or (select count(*) from public.timesheets r where r.booking_id=t.booking_id and r.is_current)<>1 then
  return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_LEGACY_IDENTITY_UNAVAILABLE','native_facts',facts);end if;
 select coalesce(jsonb_agg(to_jsonb(s) order by s.family_timesheet_id),'[]'::jsonb) into financial_scope
  from private.bpay_entitlement_financial_scope_v1(array[t.timesheet_id]) s;
 if jsonb_array_length(financial_scope)=0 or exists(select 1 from jsonb_array_elements(financial_scope) s
  where s->>'requested_timesheet_id' is distinct from t.timesheet_id::text
   or s->>'canonical_timesheet_id' is distinct from t.timesheet_id::text
   or s->>'booking_id' is distinct from t.booking_id or s->'requested_is_canonical' is distinct from 'true'::jsonb) then
  return jsonb_build_object('known',false,'reason','NATIVE_LEGACY_FAMILY_SCOPE_UNAVAILABLE',
   'native_facts',facts||jsonb_build_object('financial_scope',financial_scope));end if;
 frame:=jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),'module',to_jsonb(module),'permission',permission,'submission',witness);
 -- This is the whole actual native read body with only its optional debug
 -- audit removed. It preserves every native component/settled/reserved/stage,
 -- treatment, advance, snooze and outstanding calculation. No fake actor.
 state:=private.weekly_source_checklist_legacy_pay_state_core_v1(t.timesheet_id);
 state_after:=private.weekly_source_checklist_legacy_pay_state_core_v1(t.timesheet_id);
 select coalesce(jsonb_agg(to_jsonb(s) order by s.family_timesheet_id),'[]'::jsonb) into financial_scope_after
  from private.bpay_entitlement_financial_scope_v1(array[t.timesheet_id]) s;
 frame_after:=jsonb_build_object('root',(select to_jsonb(r) from public.timesheets r where r.timesheet_id=t.timesheet_id),
  'financial',(select to_jsonb(r) from public.timesheets_financials r where r.id=f.id),
  'module',(select to_jsonb(m) from private.bpay_next_module_control m where m.id=1),
  'permission',private.weekly_source_office_authority_v1(actor.id,'VIEW_PAY_WORK',(p_scope->>'source_group_id')::uuid,
   (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date),'submission',private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id));
 if frame is distinct from frame_after or state is distinct from state_after or financial_scope is distinct from financial_scope_after
  or (select count(*) from public.timesheets_financials r where r.timesheet_id=t.timesheet_id and r.is_current)<>1
  or (select count(*) from public.timesheets r where r.booking_id=t.booking_id and r.is_current)<>1 then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';end if;
 facts:=facts||jsonb_build_object('financial_scope',financial_scope,'native_position',state,'frame',frame);
 proof:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_LEGACY_READ_FRAME_V1',facts),'hex');
 if state->'ok' is distinct from 'true'::jsonb or state->>'timesheet_id' is distinct from t.timesheet_id::text then
  return jsonb_build_object('known',false,'reason','NATIVE_LEGACY_POSITION_UNAVAILABLE','native_facts',facts);end if;
 return jsonb_build_object('known',true,'contract','WEEKLY_SOURCE_CHECKLIST_LEGACY_READ_FRAME_V1',
  'timesheet_id',t.timesheet_id,'financial_id',f.id,'scope',p_scope,'native_position',state,'read_proof_hash',proof,
  'withholding_known',false,'native_facts',facts);
end;
$legacy_frame$;
create or replace function private.weekly_source_checklist_timesheet_hold_facts_v1(
  p_timesheet_id uuid,p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  actor public.tms_users%rowtype; witness jsonb; native_scope jsonb;
  query_facts jsonb; first_auth jsonb; financial jsonb; facts jsonb; owner text;
  t public.timesheets%rowtype; f public.timesheets_financials%rowtype; count_rows integer;
  next_owner boolean; route text; hash text; reason text;legacy_frame jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  select u.* into actor from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if actor.id is null or (lower(btrim(coalesce(actor.role,'')))<>'admin' and not private.weekly_source_pay_actor_allowed_v1(actor.id)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501';
  end if;
  witness:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
  if witness->'actual_submitted' is distinct from 'true'::jsonb
    or witness->>'client_id' is distinct from p_scope->>'client_id'
    or witness->>'week_ending_date' is distinct from p_scope->>'week_ending' then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_HOLD_SUBJECT_UNPROVED' using errcode='55000';
  end if;
  if not exists(select 1 from public.weekly_source_cycles c
    join public.weekly_source_groups g on g.id=c.source_group_id and g.active
    join public.weekly_source_group_clients m on m.source_group_id=g.id and m.client_id=(p_scope->>'client_id')::uuid
    where c.id=(p_scope->>'source_cycle_id')::uuid and g.id=(p_scope->>'source_group_id')::uuid
      and ((p_scope->>'week_ending')::date) between c.finalisation_week_ending-6 and c.finalisation_week_ending
      and ((p_scope->>'week_ending')::date) between m.valid_from and coalesce(m.valid_to,'infinity'::date)
      and (c.scope_client_id is null or c.scope_client_id=m.client_id)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_HOLD_SCOPE_UNPROVED' using errcode='55000';
  end if;
  select ts.* into strict t from public.timesheets ts where ts.timesheet_id=p_timesheet_id;
  select count(*),(array_agg(tf.id order by tf.id))[1] into count_rows,f.id
    from public.timesheets_financials tf where tf.timesheet_id=t.timesheet_id and tf.is_current and tf.timesheet_version=t.version;
  if count_rows=1 then select tf.* into strict f from public.timesheets_financials tf where tf.id=f.id; end if;
  -- These are genuine ordinary first-authorisation facts, including Daily;
  -- they are not a Source authorisation record fabricated for ordinary work.
  financial:=jsonb_build_object('timesheet_id',t.timesheet_id,'timesheet_version',t.version,
    'timesheet_authorised_at_server',t.authorised_at_server,'current_financial_count',count_rows,
    'financial_id',case when count_rows=1 then f.id end,'financial_authorised_at_utc',case when count_rows=1 then f.authorised_at_utc end,
    'financial_is_stale',case when count_rows=1 then f.is_stale end,
    'financial_candidate_id',case when count_rows=1 then f.candidate_id end,
    'financial_client_id',case when count_rows=1 then f.client_id end,
    'financial_processing_status',case when count_rows=1 then f.processing_status::text end);
  select active_owner='NEXT' into next_owner from private.bpay_next_module_control where id=1;
  if next_owner is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_FINANCIAL_OWNER_UNPROVED' using errcode='55000'; end if;
  -- ScopeV2 executes the existing resolved Source classifier and returns null
  -- for ordinary/Daily work. No public Banking page, broader actor scope or
  -- mutable stage/publish/prepare method is called from this read adapter.
  native_scope:=private.weekly_source_pay_query_scope_v1(t.timesheet_id);
  if native_scope is not null then
    if native_scope->>'client_id' is distinct from witness->>'client_id'
      or native_scope->>'candidate_id' is distinct from witness->>'candidate_id'
      or native_scope->>'contract_id' is distinct from witness->>'contract_id'
      or native_scope->>'week_ending_date' is distinct from witness->>'week_ending_date'
      or native_scope->>'root_timesheet_id' is distinct from t.timesheet_id::text then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_FINANCIAL_SCOPE_CONFLICT' using errcode='55000';
    end if;
    query_facts:=private.weekly_source_pay_query_facts_v1(t.timesheet_id);
    first_auth:=private.weekly_source_first_authorisation_context_v1(t.timesheet_id);
    owner:='SOURCE';
    reason:=case when query_facts->'ok' is distinct from 'true'::jsonb then 'NATIVE_PAY_QUERY_FACTS_UNAVAILABLE'
      when first_auth->'ok' is distinct from 'true'::jsonb then 'NATIVE_FIRST_AUTHORISATION_UNAVAILABLE'
      else 'NATIVE_WITHHELD_QUANTITY_CERTIFICATE_REQUIRED' end;
  else
    owner:=case when next_owner then 'NEXT' else 'ORDINARY' end;
    if next_owner then route:=private.bpay_next_approval_route_v1(t.timesheet_id); end if;
    reason:=case when count_rows<>1 then 'NATIVE_CURRENT_FINANCIAL_IDENTITY_UNAVAILABLE'
      when f.candidate_id::text is distinct from witness->>'candidate_id' or f.client_id::text is distinct from witness->>'client_id'
        or f.is_stale is distinct from false then 'NATIVE_CURRENT_FINANCIAL_SCOPE_UNAVAILABLE'
      else 'NATIVE_WITHHELD_QUANTITY_CERTIFICATE_REQUIRED' end;
  end if;
  if not next_owner and native_scope is null then
    legacy_frame:=private.weekly_source_checklist_legacy_read_frame_v1(t.timesheet_id,p_scope,p_actor_user_id);end if;
  facts:=jsonb_build_object('legacy_native_position_frame',legacy_frame,'scope',p_scope,'timesheet_id',t.timesheet_id,'actual_submission',witness,
    'active_pay_owner',case when next_owner then 'NEXT' else 'LEGACY' end,'native_approval_route',route,
    'ordinary_authorisation',financial,'source_scope',native_scope,'pay_query_facts',query_facts,
    'source_first_authorisation',first_auth,'withheld_quantity_unavailable_reason',reason);
  hash:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_FINANCIAL_FACTS_V1',facts),'hex');
  return jsonb_build_object('native_facts',facts,'hold',jsonb_build_object('holds_pay',null,'holds_pay_scope',null,
    'withheld_amount_pence',null,'holds_invoice',null,'pending_decision',null,
    'authority',jsonb_build_object('owner',owner,'hash',hash,'withholding_known',false)));
end;
$function$;
create or replace function private.weekly_source_checklist_information_hold_facts_v1(
  p_raw jsonb,p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; decision public.import_review_decisions%rowtype;
  facts jsonb; hash text; count_rows integer;
begin
  perform private.weekly_source_query_require_service_v1();
  select u.* into actor from public.tms_users u where u.id=p_actor_user_id and u.is_active and lower(btrim(coalesce(u.role,'')))='admin';
  if actor.id is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501'; end if;
  if p_raw->>'item_kind' is distinct from 'SOURCE_INFORMATION'
    or p_raw->'counts_for_row_admission' is distinct from 'false'::jsonb
    or p_raw->'timesheet_id' is distinct from 'null'::jsonb
    or p_raw->>'client_id' is distinct from p_scope->>'client_id'
    or p_raw->>'week_ending' is distinct from p_scope->>'week_ending'
    or p_raw->>'source_group_id' is distinct from p_scope->>'source_group_id'
    or p_raw->>'source_cycle_id' is distinct from p_scope->>'source_cycle_id' then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_INFORMATION_FINANCIAL_SUBJECT_UNPROVED' using errcode='55000';
  end if;
  select count(*),(array_agg(d.action_id order by d.action_id))[1] into count_rows,decision.action_id
    from public.import_review_decisions d
    join public.import_review_states st on st.import_id=d.import_id and st.preview_generation=d.preview_generation
      and st.superseded_at_utc is null and st.abandoned_at_utc is null
    join public.hr_imports hi on hi.id=d.import_id and hi.pruned_at is null
      and hi.source_system::text in('HEALTHROSTER','HEALTHROSTER_DAILY')
    join public.hr_rows hr on hr.id=d.hr_row_id and hr.import_id=d.import_id
    where d.import_id=(p_raw->>'query_import_id')::uuid and d.hr_row_id=(p_raw->>'hr_row_id')::uuid
      and d.action_id=p_raw->>'action_id' and d.evidence_fingerprint=p_raw->>'evidence_fingerprint'
      and d.client_id=(p_scope->>'client_id')::uuid and d.timesheet_id is null and d.is_current and not d.blocking
      and d.action_kind='ADVISORY' and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      and d.summary_json->>'reason_code'=p_raw->>'native_reason_code'
      and d.summary_json->>'reason_code' in('DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED',
        'WEEKLY_TIMESHEET_AWAITING_MANAGER_APPROVAL','WEEKLY_TIMESHEET_COMPLETING')
      and hr.date_local=(p_raw->>'work_date')::date
      and hr.date_local between (p_scope->>'week_ending')::date-6 and (p_scope->>'week_ending')::date
      and exists(select 1 from public.weekly_source_cycles c
        join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
        join public.weekly_source_group_clients m on m.source_group_id=g.id and m.client_id=d.client_id
        join public.weekly_source_client_policies p on p.source_group_id=g.id and p.client_id=d.client_id
        join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id
          and a.feature_enabled and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
        where c.id=(p_scope->>'source_cycle_id')::uuid and g.id=(p_scope->>'source_group_id')::uuid
          and ((p_scope->>'week_ending')::date) between c.finalisation_week_ending-6 and c.finalisation_week_ending
          and (c.scope_client_id is null or c.scope_client_id=d.client_id)
          and hr.date_local between m.valid_from and coalesce(m.valid_to,'infinity'::date)
          and ((p_scope->>'week_ending')::date) between m.valid_from and coalesce(m.valid_to,'infinity'::date)
          and hr.date_local between p.effective_from and coalesce(p.effective_to,'infinity'::date)
          and ((p_scope->>'week_ending')::date) between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
          and p.authority_mode='TIMESHEET_AUTHORITY' and p.document_mode='INVOICE_EVIDENCE_REQUIRED' and not p.self_bill_enabled);
  if count_rows<>1 then raise exception 'WEEKLY_SOURCE_CHECKLIST_INFORMATION_FINANCIAL_SUBJECT_UNPROVED' using errcode='55000'; end if;
  select d.* into strict decision from public.import_review_decisions d where d.action_id=decision.action_id;
  facts:=jsonb_build_object('owner','private.weekly_source_check_only_query_rows_v1','scope',p_scope,
    'decision',to_jsonb(decision),'financial_subject','NONE','counts_for_row_admission',false);
  hash:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_INFORMATION_FINANCIAL_FACTS_V1',facts),'hex');
  return jsonb_build_object('native_facts',facts,'hold',jsonb_build_object('holds_pay',false,'holds_pay_scope',null,
    'withheld_amount_pence','0','holds_invoice',false,'pending_decision',false,
    'authority',jsonb_build_object('owner','SOURCE','hash',hash,'withholding_known',true)));
end;
$function$;
create or replace function private.weekly_source_checklist_ordinary_next_hold_money_v1(
  p_timesheet_id uuid,p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $ordinary_hold$
declare actor public.tms_users%rowtype; root public.timesheets%rowtype; financial public.timesheets_financials%rowtype;
  contract public.contracts%rowtype; work private.bpay_next_work%rowtype; revision private.bpay_next_work_revision%rowtype;
  group_row private.bpay_next_workbench_group%rowtype; metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  module_row private.bpay_next_module_control%rowtype; permission jsonb; witness jsonb; route text;
  facts jsonb; members jsonb; members_after jsonb; items jsonb; count_rows bigint; worker_revision bigint;
  native_sequence bigint; sequence_after bigint; native_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  frame jsonb; frame_after jsonb; source_amount numeric; pence numeric; positive_count bigint; proof text;
  native_daily boolean:=false;v_identity_candidate uuid;v_identity_client uuid;daily_date date;daily_evidence jsonb;v_native_period jsonb;
  approval_revision_id uuid;approval_revision_id_after uuid;approval_revision jsonb;native_publication jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_scope,
    array['source_group_id','source_cycle_id','client_id','week_ending'],'WEEKLY_SOURCE_CHECKLIST_ORDINARY_MONEY_SCOPE_INVALID');
  if p_timesheet_id is null or jsonb_typeof(p_scope->'source_group_id') is distinct from 'string'
    or not pg_input_is_valid(p_scope->>'source_group_id','uuid')
    or jsonb_typeof(p_scope->'source_cycle_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'source_cycle_id','uuid')
    or jsonb_typeof(p_scope->'client_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'client_id','uuid')
    or jsonb_typeof(p_scope->'week_ending') is distinct from 'string' or not pg_input_is_valid(p_scope->>'week_ending','date') then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ORDINARY_MONEY_SCOPE_INVALID' using errcode='22023'; end if;
  select u.* into actor from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if actor.id is null or (lower(btrim(coalesce(actor.role,'')))<>'admin' and not private.weekly_source_pay_actor_allowed_v1(actor.id)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501'; end if;
  permission:=private.weekly_source_office_authority_v1(actor.id,'VIEW_PAY_WORK',(p_scope->>'source_group_id')::uuid,
    (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
  witness:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
  if witness->'actual_submitted' is distinct from 'true'::jsonb or witness->>'client_id' is distinct from p_scope->>'client_id'
    or witness->>'week_ending_date' is distinct from p_scope->>'week_ending' then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ORDINARY_MONEY_SUBJECT_UNPROVED' using errcode='55000'; end if;
  if not exists(select 1 from public.weekly_source_cycles c join public.weekly_source_groups g on g.id=c.source_group_id
    join public.weekly_source_group_clients m on m.source_group_id=g.id and m.client_id=(p_scope->>'client_id')::uuid
    where c.id=(p_scope->>'source_cycle_id')::uuid and g.id=(p_scope->>'source_group_id')::uuid and g.active and g.source_family='ROSTER'
      and (p_scope->>'week_ending')::date between c.finalisation_week_ending-6 and c.finalisation_week_ending
      and (p_scope->>'week_ending')::date between m.valid_from and coalesce(m.valid_to,'infinity'::date)
      and (c.scope_client_id is null or c.scope_client_id=m.client_id)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ORDINARY_MONEY_SCOPE_UNPROVED' using errcode='55000'; end if;
  facts:=jsonb_build_object('scope',p_scope,'actor_user_id',actor.id,'permission',permission,'actual_submission',witness,
    'amount_basis','SOURCE_EX_VAT','grain','CURRENT_ORDINARY_WORK_AVAILABLE');
  select c.* into module_row from private.bpay_next_module_control c where c.id=1;
  facts:=facts||jsonb_build_object('module',to_jsonb(module_row));
  if module_row.active_owner is distinct from 'NEXT' then
    return jsonb_build_object('known',false,'reason','NATIVE_NEXT_OWNER_UNAVAILABLE','native_facts',facts); end if;
  select t.* into root from public.timesheets t where t.timesheet_id=p_timesheet_id;
  route:=private.bpay_next_approval_route_v1(p_timesheet_id);
  facts:=facts||jsonb_build_object('root',to_jsonb(root),'native_approval_route',route);
  native_daily:=root.sheet_scope::text='DAILY' and root.contract_id is null and route='DAILY_HOURS';
  -- The real Daily publisher owns its Sunday work week. No GET calendar
  -- conversion, rate lookup or native projection refresh is introduced.
  if (not native_daily and (root.sheet_scope::text is distinct from 'WEEKLY' or root.contract_id is null or route is distinct from 'ORDINARY_HOURS'))
    or root.line_type::text is distinct from 'HOURS'
    or root.is_current is distinct from true or root.authorised_at_server is null
    or root.revoked_at is not null or root.archived_at_utc is not null
    or private.weekly_source_pay_query_scope_v1(root.timesheet_id) is not null then
    return jsonb_build_object('known',false,'reason','NATIVE_ORDINARY_WEEKLY_CONTRACT_GRAIN_UNAVAILABLE','native_facts',facts); end if;
  if not native_daily then
  select c.* into contract from public.contracts c where c.id=root.contract_id;
  if contract.id is null or contract.client_id::text is distinct from witness->>'client_id'
    or contract.candidate_id::text is distinct from witness->>'candidate_id'
    or contract.id::text is distinct from witness->>'contract_id' then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_CONTRACT_IDENTITY_UNAVAILABLE','native_facts',facts); end if;
    v_identity_candidate:=contract.candidate_id;v_identity_client:=contract.client_id;
  else
    v_identity_candidate:=(witness->>'candidate_id')::uuid;v_identity_client:=(witness->>'client_id')::uuid;
    daily_date:=(root.worked_start_iso at time zone 'Europe/London')::date;
    if witness->'contract_id' is distinct from 'null'::jsonb or witness->>'sheet_scope' is distinct from 'DAILY'
      or witness->>'work_date' is distinct from daily_date::text or daily_date is null
      or root.week_ending_date is distinct from daily_date+(7-extract(isodow from daily_date)::integer)
      or root.is_adjustment or root.parent_timesheet_id is not null or root.correction_id is not null then
      return jsonb_build_object('known',false,'reason','NATIVE_DAILY_SUBMISSION_IDENTITY_UNAVAILABLE','native_facts',facts); end if;
  end if;
  select count(*) into count_rows from public.timesheets t where t.booking_id=root.booking_id and t.is_current;
  if count_rows<>1 then return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_BOOKING_AMBIGUOUS','native_facts',facts); end if;
  -- Match the native classifier's ALL current TF predicate, not a historical
  -- fallback or a narrower version-only query hiding another current TF.
  select count(*),(array_agg(f.id order by f.id))[1] into count_rows,financial.id from public.timesheets_financials f
    where f.timesheet_id=root.timesheet_id and f.is_current;
  if count_rows<>1 then return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_FINANCIAL_AMBIGUOUS','native_facts',facts); end if;
  select f.* into strict financial from public.timesheets_financials f where f.id=financial.id;
  facts:=facts||jsonb_build_object('contract',to_jsonb(contract),'current_financial',to_jsonb(financial));
  if financial.timesheet_version is distinct from root.version or financial.is_stale is distinct from false or financial.authorised_at_utc is null
    or financial.candidate_id is distinct from v_identity_candidate or financial.client_id is distinct from v_identity_client
    or financial.pay_on_hold is distinct from true
    or (native_daily and (financial.basis::text is distinct from 'SELF_REPORTED' or financial.processed_at_utc is null
      or (financial.worked_start_iso,financial.worked_end_iso,financial.break_start_iso,financial.break_end_iso,financial.break_minutes)
        is distinct from (root.worked_start_iso,root.worked_end_iso,root.break_start_iso,root.break_end_iso,root.break_minutes))) then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_FINANCIAL_HOLD_UNAVAILABLE','native_facts',facts); end if;
  select count(*),(array_agg(w.id order by w.id))[1] into count_rows,work.id from private.bpay_next_work w
    join private.bpay_next_work_revision r on r.id=w.current_revision_id and r.work_id=w.id
    where w.work_kind='ORDINARY'
      and ((not native_daily and w.origin_kind='CONTRACT' and w.origin_work_date is null)
        or (native_daily and w.origin_kind='DAILY' and w.origin_work_date=daily_date))
      and w.booking_id=root.booking_id and w.contract_id is not distinct from root.contract_id and w.candidate_id=v_identity_candidate
      and w.week_ending_date=root.week_ending_date and r.source_kind='ORDINARY'
      and r.physical_timesheet_id=root.timesheet_id and r.physical_timesheet_version=root.version and r.financial_snapshot_id=financial.id;
  if count_rows<>1 then return jsonb_build_object('known',false,'reason','NATIVE_ORDINARY_WORK_IDENTITY_UNAVAILABLE','native_facts',facts); end if;
  select w.* into strict work from private.bpay_next_work w where w.id=work.id;
  select r.* into strict revision from private.bpay_next_work_revision r where r.id=work.current_revision_id and r.work_id=work.id;
  begin
    approval_revision_id:=private.bpay_next_treatment_approval_revision_v1(revision.id);
  exception when sqlstate '23514' then
    return jsonb_build_object('known',false,'reason','NATIVE_TREATMENT_APPROVAL_UNAVAILABLE',
      'native_facts',facts||jsonb_build_object('native_qualification_sqlstate',sqlstate));end;
  select to_jsonb(a) into approval_revision from private.bpay_next_work_revision a where a.id=approval_revision_id and a.work_id=work.id;
  select to_jsonb(p) into native_publication from private.bpay_next_publication p where p.work_id=work.id and p.revision_id=revision.id;
  facts:=facts||jsonb_build_object('native_approval_revision_id',approval_revision_id,'native_approval_revision',approval_revision,
    'native_treatment_publication',native_publication);
  v_native_period:=private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today);
  facts:=facts||jsonb_build_object('native_work_period',v_native_period);
  if v_native_period->'valid' is distinct from 'true'::jsonb then
    return jsonb_build_object('known',false,'reason','NATIVE_WORK_PERIOD_UNAVAILABLE','native_facts',facts);end if;
  daily_evidence:=revision.daily_approval_evidence_json;
  facts:=facts||jsonb_build_object('work',to_jsonb(work),'revision',to_jsonb(revision),'native_daily',native_daily);
  if native_daily and (daily_evidence->>'contract_version' is distinct from 'BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    or daily_evidence->>'timesheet_id' is distinct from root.timesheet_id::text
    or daily_evidence->>'timesheet_version' is distinct from root.version::text
    or daily_evidence->>'financial_id' is distinct from financial.id::text
    or daily_evidence->>'booking_id' is distinct from root.booking_id
    or daily_evidence->>'candidate_id' is distinct from v_identity_candidate::text
    or daily_evidence->>'client_id' is distinct from v_identity_client::text
    or daily_evidence->>'work_date' is distinct from daily_date::text) then
    return jsonb_build_object('known',false,'reason','NATIVE_DAILY_APPROVAL_EVIDENCE_UNAVAILABLE','native_facts',facts); end if;
  if work.approval_state<>'APPROVED' or work.applied_revision_id is distinct from revision.id or revision.sealed_at_utc is null
    or revision.source_head_id is not null or revision.source_event_id is not null
    or revision.source_pay_channel is distinct from upper(financial.pay_method)
    or (select count(*) from private.bpay_next_approved_line l where l.revision_id=revision.id)<>revision.expected_line_count
    or work.pay_hold is distinct from true or work.pay_hold_control_version is null or work.pay_hold_control_version<=0 or work.pay_hold_changed_at_utc is null
    or work.pay_hold_physical_timesheet_id is distinct from root.timesheet_id or work.pay_hold_financial_id is distinct from financial.id
    or work.pay_hold_reason is distinct from financial.pay_on_hold_reason then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_SEALED_HOLD_CONTROL_UNAVAILABLE','native_facts',facts); end if;
  select g.* into group_row from private.bpay_next_workbench_group g where g.group_key='WORK:'||work.id::text and g.work_id=work.id;
  select m.* into metadata from private.bpay_next_workbench_candidate_metadata m where m.candidate_id=work.candidate_id;
  select c.financial_view_revision into worker_revision from private.bpay_next_worker_control c where c.candidate_id=work.candidate_id;
  facts:=facts||jsonb_build_object('group',to_jsonb(group_row),'metadata',to_jsonb(metadata),'worker_financial_view_revision',worker_revision);
  if group_row.group_key is null or metadata.candidate_id is null or worker_revision is null
    or group_row.group_kind<>'WORK' or group_row.complete is distinct from true or group_row.dirty is distinct from false or group_row.progress_state<>'CURRENT'
    or group_row.current_revision_id is distinct from revision.id or group_row.physical_timesheet_id is distinct from root.timesheet_id
    or group_row.candidate_id is distinct from v_identity_candidate or group_row.client_id is distinct from v_identity_client
    or group_row.contract_id is distinct from root.contract_id or group_row.week_ending_date is distinct from root.week_ending_date
    or group_row.financial_view_revision is distinct from worker_revision or metadata.continuation_pending is distinct from false
    or metadata.adopted_metadata_version is distinct from metadata.metadata_version or group_row.metadata_version is distinct from metadata.metadata_version
    or not exists(select 1 from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
      where c.id=work.candidate_id and c.active and c.pay_method::text in('PAYE','UMBRELLA') and group_row.target_pay_channel=c.pay_method::text
        and (c.pay_method::text<>'UMBRELLA' or (u.enabled and u.vat_chargeable is not null))) then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_PROJECTION_METADATA_UNAVAILABLE','native_facts',facts); end if;
  begin
    select coalesce(jsonb_agg(to_jsonb(m) order by m.component_key,m.segment_key nulls first),'[]'::jsonb) into members
      from private.weekly_source_checklist_native_members_core_v1(work.id,revision.id,false) m;
  exception when sqlstate '55000' or sqlstate '23514' or no_data_found or too_many_rows then
    return jsonb_build_object('known',false,'reason','NATIVE_MEMBER_CERTIFICATE_UNAVAILABLE',
      'native_facts',facts||jsonb_build_object('native_qualification_sqlstate',sqlstate)); end;
  select coalesce(jsonb_agg(to_jsonb(i) order by i.component_key,i.member_key,i.portion_kind),'[]'::jsonb) into items
    from private.bpay_next_workbench_item i where i.group_key=group_row.group_key;
  facts:=facts||jsonb_build_object('members',members,'items',items);
  if jsonb_array_length(members)=0 or exists(select 1 from jsonb_array_elements(members) m
      where m->'parent_complete' is distinct from 'true'::jsonb or m->'segment_attribution_complete' is distinct from 'true'::jsonb)
    or exists(select 1 from jsonb_array_elements(members) m group by m->>'component_key',m->>'segment_key' having count(*)<>1)
    or exists(select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key and
      (i.work_id is distinct from work.id or i.revision_id is distinct from revision.id or i.progress_state<>'CURRENT'
        or i.projection_revision is distinct from group_row.projection_revision or i.financial_view_revision is distinct from worker_revision
        or i.metadata_version is distinct from metadata.metadata_version or i.amount_known is distinct from true
        or i.candidate_id is distinct from v_identity_candidate or i.client_id is distinct from v_identity_client or i.contract_id is distinct from root.contract_id
        or i.physical_timesheet_id is distinct from root.timesheet_id or i.week_ending_date is distinct from root.week_ending_date
        or i.target_pay_channel is distinct from group_row.target_pay_channel or i.is_selected is null or i.selectable is null
        or i.query_qualified is null or i.query_state is null or i.reason_code is null or i.reason_priority is null or i.section is null)) then
    return jsonb_build_object('known',false,'reason','NATIVE_COMPLETE_MEMBER_PROJECTION_UNAVAILABLE','native_facts',facts); end if;
  -- Aggregate the native split ONCE for all members. This is a conservation
  -- comparison, not a new GREATEST/priority/quantity classification.
  if exists(with projected as (
      select i.component_key,i.member_kind,i.member_key,i.approved_line_id,i.approved_segment_id,
        count(*) n,sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
        sum(i.residual_source_ex_vat) residual,
        sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
      from private.bpay_next_workbench_item i where i.group_key=group_row.group_key
      group by i.component_key,i.member_kind,i.member_key,i.approved_line_id,i.approved_segment_id)
    select 1 from jsonb_array_elements(members) m left join projected p on p.component_key=m->>'component_key'
      and p.member_kind=case when m->>'segment_key' is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end
      and p.member_key=coalesce(m->>'segment_key',m->>'component_key')
      and p.approved_line_id is not distinct from (m->>'approved_line_id')::uuid
      and p.approved_segment_id is not distinct from (m->>'approved_segment_id')::uuid
    where p.n is null or (p.a,p.c,p.h,p.residual,p.split_residual) is distinct from
      ((m->>'approved_amount')::numeric,(m->>'realised_amount')::numeric,(m->>'held_amount')::numeric,
        (m->>'approved_amount')::numeric-(m->>'realised_amount')::numeric-(m->>'held_amount')::numeric,
        (m->>'approved_amount')::numeric-(m->>'realised_amount')::numeric-(m->>'held_amount')::numeric))
    or exists(select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key and not exists(
      select 1 from jsonb_array_elements(members) m where m->>'component_key'=i.component_key and coalesce(m->>'segment_key',m->>'component_key')=i.member_key
        and (m->>'approved_line_id')::uuid is not distinct from i.approved_line_id and (m->>'approved_segment_id')::uuid is not distinct from i.approved_segment_id
        and i.member_kind=case when m->>'segment_key' is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end
        and i.segment_key is not distinct from m->>'segment_key' and i.component_kind is not distinct from m->>'component_kind'
        and i.source_pay_channel is not distinct from m->>'source_pay_channel' and i.work_date is not distinct from (m->>'work_date')::date)) then
    return jsonb_build_object('known',false,'reason','NATIVE_FULL_MEMBER_CONSERVATION_UNAVAILABLE','native_facts',facts); end if;
  select c.last_sequence into native_sequence from private.bpay_next_command_clock c where c.id=1;
  if native_sequence is null or root.week_ending_date-6>native_today then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_CUT_UNAVAILABLE','native_facts',facts); end if;
  -- The native writer alone classified PAY_ON_HOLD. Recheck actual accepted
  -- and snooze owners; no cached null or higher-priority caption proves a cut.
  if exists(select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key
    and i.portion_kind='AVAILABLE' and not i.is_selected and i.included_source_ex_vat>0 and
      (exists(select 1 from private.bpay_next_workbench_accepted_work_v1(work.id,i.component_key,i.segment_key))
        or private.bpay_next_snooze_at_cut_v1(work.id,revision.id,i.segment_key,i.component_key,native_sequence,native_today) is not null
        or i.reason_code<>'PAY_ON_HOLD' or i.reason_priority<>40 or i.section<>'BLOCKED' or i.selectable
        or i.pay_hold is distinct from true or i.pay_hold_reason is distinct from financial.pay_on_hold_reason
        or i.query_qualified is distinct from false or i.query_state<>'NOT_APPLICABLE'
        or i.direction<>'PAYMENT' or i.accepted_run_id is not null or i.accepted_command_id is not null)) then
    return jsonb_build_object('known',false,'reason','NATIVE_POSITIVE_HOLD_CLASSIFICATION_UNAVAILABLE','native_facts',facts); end if;
  select count(*),sum(i.included_source_ex_vat) into positive_count,source_amount from private.bpay_next_workbench_item i
    where i.group_key=group_row.group_key and i.portion_kind='AVAILABLE' and not i.is_selected and i.included_source_ex_vat>0
      and i.reason_code='PAY_ON_HOLD' and i.reason_priority=40 and i.section='BLOCKED' and not i.selectable and i.direction='PAYMENT'
      and i.pay_hold and not i.query_qualified and i.query_state='NOT_APPLICABLE' and i.accepted_run_id is null and i.accepted_command_id is null;
  if positive_count=0 or source_amount is null or source_amount<=0 then
    return jsonb_build_object('known',false,'reason','NATIVE_POSITIVE_AVAILABLE_HOLD_AMOUNT_UNAVAILABLE','native_facts',facts); end if;
  frame:=jsonb_build_object('root',to_jsonb(root),'financial',to_jsonb(financial),'contract',case when root.contract_id is null then null else to_jsonb(contract) end,'work',to_jsonb(work),
    'revision',to_jsonb(revision),'group',to_jsonb(group_row),'metadata',to_jsonb(metadata),'worker_revision',worker_revision,'items',items,'module',to_jsonb(module_row));
  select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),'contract',case when t.contract_id is null then null else to_jsonb(c) end,'work',to_jsonb(w),'revision',to_jsonb(r),
    'group',to_jsonb(g),'metadata',to_jsonb(md),'worker_revision',ctl.financial_view_revision,'module',to_jsonb(module),
    'items',(select coalesce(jsonb_agg(to_jsonb(i) order by i.component_key,i.member_key,i.portion_kind),'[]'::jsonb)
      from private.bpay_next_workbench_item i where i.group_key=g.group_key)) into frame_after
    from public.timesheets t join public.timesheets_financials f on f.id=financial.id left join public.contracts c on c.id=t.contract_id
    join private.bpay_next_work w on w.id=work.id join private.bpay_next_work_revision r on r.id=w.current_revision_id and r.work_id=w.id
    join private.bpay_next_workbench_group g on g.group_key=group_row.group_key join private.bpay_next_workbench_candidate_metadata md on md.candidate_id=w.candidate_id
    join private.bpay_next_worker_control ctl on ctl.candidate_id=w.candidate_id join private.bpay_next_module_control module on module.id=1
    where t.timesheet_id=root.timesheet_id;
  begin
    select coalesce(jsonb_agg(to_jsonb(m) order by m.component_key,m.segment_key nulls first),'[]'::jsonb) into members_after
      from private.weekly_source_checklist_native_members_core_v1(work.id,revision.id,false) m;
  exception when sqlstate '55000' or sqlstate '23514' or no_data_found or too_many_rows then
    return jsonb_build_object('known',false,'reason','NATIVE_MEMBER_CERTIFICATE_CHANGED',
      'native_facts',facts||jsonb_build_object('native_qualification_sqlstate',sqlstate)); end;
  select c.last_sequence into sequence_after from private.bpay_next_command_clock c where c.id=1;
  permission:=private.weekly_source_office_authority_v1(actor.id,'VIEW_PAY_WORK',(p_scope->>'source_group_id')::uuid,
    (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
  begin
    approval_revision_id_after:=private.bpay_next_treatment_approval_revision_v1(revision.id);
  exception when sqlstate '23514' then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';end;
  if approval_revision_id is distinct from approval_revision_id_after
    or approval_revision is distinct from (select to_jsonb(a) from private.bpay_next_work_revision a where a.id=approval_revision_id and a.work_id=work.id)
    or native_publication is distinct from (select to_jsonb(p) from private.bpay_next_publication p where p.work_id=work.id and p.revision_id=revision.id)
    or v_native_period is distinct from private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today)
    or frame is distinct from frame_after or members is distinct from members_after or native_sequence is distinct from sequence_after
    or witness is distinct from private.weekly_source_actual_timesheet_submission_v1(root.timesheet_id)
    or (select count(*) from public.timesheets t where t.booking_id=root.booking_id and t.is_current)<>1
    or (select count(*) from public.timesheets_financials f where f.timesheet_id=root.timesheet_id and f.is_current)<>1 then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
  if not exists(select 1 from public.tms_users u where u.id=actor.id and u.is_active
    and (lower(btrim(coalesce(u.role,'')))='admin' or private.weekly_source_pay_actor_allowed_v1(u.id))) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501'; end if;
  pence:=source_amount*100;
  if pence<>trunc(pence) or pence<1 or pence>999999999999999999 then
    return jsonb_build_object('known',false,'reason','NATIVE_MINOR_UNIT_RANGE_UNAVAILABLE','native_facts',facts); end if;
  facts:=facts||jsonb_build_object('native_sequence',native_sequence,'native_business_date',native_today,
    'positive_available_item_count',positive_count,'withheld_available_source_ex_vat',source_amount);
  proof:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_ORDINARY_NEXT_AVAILABLE_HOLD_V1',facts),'hex');
  return jsonb_build_object('known',true,'native_facts',facts,
    'worker',coalesce(nullif(btrim(metadata.candidate_sort_name),''),work.candidate_id::text),
    'sheet_scope',root.sheet_scope::text,'work_date',case when native_daily then daily_date else root.week_ending_date end,
    'status',jsonb_build_object('text','Holds pay','tone','danger','subtext','Pay held: £'||to_char(source_amount,'FM9999999999999990.00')||
      case when native_daily then ' ex VAT for '||to_char(daily_date,'DD/MM/YYYY')
        else ' ex VAT for w/e '||to_char(root.week_ending_date,'DD/MM/YYYY') end||' · '||coalesce(financial.pay_on_hold_reason,'Pay on hold'), 'subtone','muted'),
    'native_ordinary_money_witness',jsonb_build_object('owner','private.bpay_next_workbench_refresh_work_core_v1',
      'actor_user_id',actor.id,'scope',p_scope,'timesheet_id',root.timesheet_id,'financial_id',financial.id,'work_id',work.id,
      'revision_id',revision.id,'amount_basis','SOURCE_EX_VAT','grain','CURRENT_ORDINARY_WORK_AVAILABLE',
      'daily_work_date',case when native_daily then daily_date end,'counts_for_row_admission',false,'read_proof_hash',proof),
    'hold',jsonb_build_object('holds_pay',true,'holds_pay_scope','TIMESHEET','withheld_amount_pence',pence::bigint::text,
      'holds_invoice',null,'pending_decision',null,
      'authority',jsonb_build_object('owner','ORDINARY_NEXT_AVAILABLE_EX_VAT','hash',proof,'withholding_known',true)));
end;
$ordinary_hold$;
create or replace function private.weekly_source_checklist_source_shift_query_money_v1(
  p_root_timesheet_id uuid,p_work_event_id uuid,p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; root public.timesheets%rowtype; financial public.timesheets_financials%rowtype;
  event public.weekly_work_events%rowtype; work private.bpay_next_work%rowtype; revision private.bpay_next_work_revision%rowtype;
  group_row private.bpay_next_workbench_group%rowtype; metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  gate jsonb; gate_after jsonb; native_scope jsonb; exclusions jsonb; members jsonb; members_after jsonb; member jsonb; items jsonb;
  count_rows bigint; worker_revision bigint; approval_revision_id uuid; source_event_id uuid; native_sequence bigint;
  native_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  totals record; actual_member_count bigint; matching_count bigint; positive_count bigint; source_amount numeric; pence numeric;
  facts jsonb; proof_hash text; permission jsonb; current_frame jsonb; current_frame_after jsonb;v_native_period jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_scope,
    array['source_group_id','source_cycle_id','client_id','week_ending'],'WEEKLY_SOURCE_CHECKLIST_SOURCE_MONEY_SCOPE_INVALID');
  if jsonb_typeof(p_scope->'source_group_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'source_group_id','uuid')
    or jsonb_typeof(p_scope->'source_cycle_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'source_cycle_id','uuid')
    or jsonb_typeof(p_scope->'client_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'client_id','uuid')
    or jsonb_typeof(p_scope->'week_ending') is distinct from 'string' or not pg_input_is_valid(p_scope->>'week_ending','date')
    or p_root_timesheet_id is null or p_work_event_id is null then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_SOURCE_MONEY_SCOPE_INVALID' using errcode='22023';
  end if;
  select u.* into actor from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if actor.id is null or (lower(btrim(coalesce(actor.role,'')))<>'admin' and not private.weekly_source_pay_actor_allowed_v1(actor.id)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501';
  end if;
  permission:=private.weekly_source_office_authority_v1(actor.id,'VIEW_PAY_WORK',(p_scope->>'source_group_id')::uuid,
    (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
  facts:=jsonb_build_object('scope',p_scope,'actor_user_id',actor.id,'permission',permission,
    'root_timesheet_id',p_root_timesheet_id,'work_event_id',p_work_event_id,'amount_basis','SOURCE_EX_VAT','grain','WORK_SEGMENT');
  if not exists(select 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner='NEXT') then
    return jsonb_build_object('known',false,'reason','NATIVE_NEXT_OWNER_UNAVAILABLE','native_facts',facts);
  end if;
  select t.* into root from public.timesheets t where t.timesheet_id=p_root_timesheet_id;
  native_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  select e.* into event from public.weekly_work_events e where e.id=p_work_event_id
    and e.client_id=(p_scope->>'client_id')::uuid and e.first_source_group_id=(p_scope->>'source_group_id')::uuid
    and e.work_date between (p_scope->>'week_ending')::date-6 and (p_scope->>'week_ending')::date;
  facts:=facts||jsonb_build_object('native_scope',native_scope,'root',to_jsonb(root),'work_event',to_jsonb(event));
  if native_scope is null or root.authorised_at_server is null or event.id is null
    or root.is_current is distinct from true or root.revoked_at is not null or root.archived_at_utc is not null
    or native_scope->>'root_timesheet_id' is distinct from root.timesheet_id::text
    or native_scope->>'client_id' is distinct from p_scope->>'client_id'
    or native_scope->>'candidate_id' is distinct from event.candidate_id::text
    or native_scope->>'week_ending_date' is distinct from p_scope->>'week_ending'
    or not exists(select 1 from public.weekly_source_row_timesheet_lineages l
      where l.work_event_id=event.id and l.timesheet_id=root.timesheet_id)
    or not exists(select 1 from public.weekly_source_cycles c join public.weekly_source_group_clients m on m.source_group_id=c.source_group_id
      where c.id=(p_scope->>'source_cycle_id')::uuid and c.source_group_id=(p_scope->>'source_group_id')::uuid
        and m.client_id=event.client_id and event.work_date between m.valid_from and coalesce(m.valid_to,'infinity'::date)
        and root.week_ending_date between c.finalisation_week_ending-6 and c.finalisation_week_ending
        and (c.scope_client_id is null or c.scope_client_id=event.client_id)) then
    return jsonb_build_object('known',false,'reason','ACTUAL_SOURCE_SHIFT_SCOPE_UNAVAILABLE','native_facts',facts);
  end if;
  select count(*),(array_agg(f.id order by f.id))[1] into count_rows,financial.id from public.timesheets_financials f
    where f.timesheet_id=root.timesheet_id and f.timesheet_version=root.version and f.is_current;
  if count_rows<>1 then return jsonb_build_object('known',false,'reason','CURRENT_FINANCIAL_IDENTITY_UNAVAILABLE','native_facts',facts); end if;
  select f.* into strict financial from public.timesheets_financials f where f.id=financial.id;
  facts:=facts||jsonb_build_object('current_financial',to_jsonb(financial));
  if financial.is_stale is distinct from false or financial.authorised_at_utc is null
    or financial.candidate_id is distinct from event.candidate_id or financial.client_id is distinct from event.client_id
    or coalesce(financial.pay_on_hold,false) then
    return jsonb_build_object('known',false,'reason','CURRENT_FINANCIAL_OR_OTHER_HOLD_UNAVAILABLE','native_facts',facts);
  end if;
  select count(*),(array_agg(w.id order by w.id))[1] into count_rows,work.id
    from private.bpay_next_work w join private.bpay_next_work_revision r on r.id=w.current_revision_id and r.work_id=w.id
    where w.work_kind='SOURCE' and w.candidate_id=event.candidate_id and w.contract_id=(native_scope->>'contract_id')::uuid
      and w.booking_id=root.booking_id and w.week_ending_date=root.week_ending_date
      and r.physical_timesheet_id=root.timesheet_id and r.physical_timesheet_version=root.version
      and r.source_kind in('SOURCE','PROTECTED');
  if count_rows<>1 then return jsonb_build_object('known',false,'reason','NATIVE_WORK_IDENTITY_UNAVAILABLE','native_facts',facts); end if;
  select w.* into strict work from private.bpay_next_work w where w.id=work.id;
  select r.* into strict revision from private.bpay_next_work_revision r where r.id=work.current_revision_id and r.work_id=work.id;
  v_native_period:=private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today);
  facts:=facts||jsonb_build_object('work',to_jsonb(work),'revision',to_jsonb(revision),'native_work_period',v_native_period);
  if v_native_period->'valid' is distinct from 'true'::jsonb then
    return jsonb_build_object('known',false,'reason','NATIVE_WORK_PERIOD_UNAVAILABLE','native_facts',facts);end if;
  if work.approval_state<>'APPROVED' or work.applied_revision_id is distinct from revision.id or revision.sealed_at_utc is null
    or (select count(*) from private.bpay_next_approved_line l where l.revision_id=revision.id)<>revision.expected_line_count then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_SEALED_WORK_UNAVAILABLE','native_facts',facts);
  end if;
  approval_revision_id:=revision.id;source_event_id:=revision.source_event_id;
  if revision.payment_treatment_approval_revision_id is not null then
    approval_revision_id:=private.bpay_next_treatment_approval_revision_v1(revision.id);
    select r.source_event_id into source_event_id from private.bpay_next_work_revision r where r.id=approval_revision_id and r.work_id=work.id;
  end if;
  if source_event_id is null then return jsonb_build_object('known',false,'reason','NATIVE_SOURCE_EVENT_UNAVAILABLE','native_facts',facts); end if;
  select g.* into group_row from private.bpay_next_workbench_group g where g.group_key='WORK:'||work.id::text and g.work_id=work.id;
  select m.* into metadata from private.bpay_next_workbench_candidate_metadata m where m.candidate_id=work.candidate_id;
  select c.financial_view_revision into worker_revision from private.bpay_next_worker_control c where c.candidate_id=work.candidate_id;
  facts:=facts||jsonb_build_object('group',to_jsonb(group_row),'metadata',to_jsonb(metadata),'worker_financial_view_revision',worker_revision);
  if group_row.group_key is null or metadata.candidate_id is null or worker_revision is null
    or group_row.group_kind<>'WORK' or group_row.complete is distinct from true or group_row.dirty is distinct from false or group_row.progress_state<>'CURRENT'
    or group_row.current_revision_id is distinct from revision.id or group_row.physical_timesheet_id is distinct from root.timesheet_id
    or group_row.candidate_id is distinct from event.candidate_id or group_row.client_id is distinct from event.client_id
    or group_row.contract_id is distinct from work.contract_id or group_row.week_ending_date is distinct from root.week_ending_date
    or group_row.financial_view_revision is distinct from worker_revision or metadata.continuation_pending is distinct from false
    or metadata.adopted_metadata_version is distinct from metadata.metadata_version or group_row.metadata_version is distinct from metadata.metadata_version
    or not exists(select 1 from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
      where c.id=work.candidate_id and c.active and c.pay_method::text in('PAYE','UMBRELLA')
        and group_row.target_pay_channel=c.pay_method::text
        and (c.pay_method::text<>'UMBRELLA' or (u.enabled and u.vat_chargeable is not null))) then
    return jsonb_build_object('known',false,'reason','NATIVE_PROJECTION_FRESHNESS_UNAVAILABLE','native_facts',facts);
  end if;
  gate:=private.weekly_source_pay_query_gate_v1(root.timesheet_id);
  facts:=facts||jsonb_build_object('query_gate',gate);
  if gate->'ok' is distinct from 'true'::jsonb or gate->>'code' is distinct from 'OK' or gate->'scope' is distinct from native_scope
    or gate->'blocked' is distinct from 'true'::jsonb then
    return jsonb_build_object('known',false,'reason','NATIVE_OPEN_QUERY_SCOPE_UNAVAILABLE','native_facts',facts);
  end if;
  exclusions:=private.bpay_next_source_query_segment_exclusions_v1(revision.id,gate);
  facts:=facts||jsonb_build_object('segment_query_exclusions',exclusions);
  if jsonb_typeof(exclusions) is distinct from 'array' or not exists(select 1 from jsonb_array_elements(exclusions) e where e->>'segment_key'=event.id::text) then
    return jsonb_build_object('known',false,'reason','NATIVE_SHIFT_QUERY_EXCLUSION_UNAVAILABLE','native_facts',facts);
  end if;
  -- This is the SAME complete native member/binding/publication/command/job/
  -- treatment/attribution derivation consumed by the native locked wrapper.
  -- No new positive amount is computed, and no browser can select its mode.
  begin
    select coalesce(jsonb_agg(to_jsonb(m) order by m.component_key,m.segment_key nulls first),'[]'::jsonb),count(*)
      into members,actual_member_count from private.weekly_source_checklist_native_members_core_v1(work.id,revision.id,false) m;
  exception when sqlstate '55000' or sqlstate '23514' or no_data_found or too_many_rows then
    return jsonb_build_object('known',false,'reason','NATIVE_BINDING_MEMBER_CERTIFICATE_UNAVAILABLE',
      'native_facts',facts||jsonb_build_object('native_qualification_sqlstate',sqlstate));
  end;
  select coalesce(jsonb_agg(to_jsonb(i) order by i.component_key,i.member_key,i.portion_kind),'[]'::jsonb)
    into items from private.bpay_next_workbench_item i where i.group_key=group_row.group_key;
  facts:=facts||jsonb_build_object('members',members,'items',items,'source_event_id',source_event_id);
  if actual_member_count=0 or exists(select 1 from jsonb_array_elements(members) m
    where m->'parent_complete' is distinct from 'true'::jsonb or m->'segment_attribution_complete' is distinct from 'true'::jsonb)
    or exists(select 1 from jsonb_array_elements(members) m group by m->>'component_key',m->>'segment_key' having count(*)<>1)
    or exists(select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key and
      (i.work_id is distinct from work.id or i.revision_id is distinct from revision.id or i.progress_state<>'CURRENT'
        or i.projection_revision is distinct from group_row.projection_revision or i.financial_view_revision is distinct from worker_revision
        or i.metadata_version is distinct from metadata.metadata_version or i.amount_known is distinct from true
        or i.candidate_id is distinct from event.candidate_id or i.client_id is distinct from event.client_id
        or i.contract_id is distinct from work.contract_id or i.physical_timesheet_id is distinct from root.timesheet_id
        or i.week_ending_date is distinct from root.week_ending_date
        or i.target_pay_channel is distinct from group_row.target_pay_channel)) then
    return jsonb_build_object('known',false,'reason','NATIVE_COMPLETE_MEMBER_PROJECTION_UNAVAILABLE','native_facts',facts);
  end if;
  -- Reprove full actual membership and the native conserved member quantities.
  -- Do not reproduce GREATEST(A-C-H), a priority ladder or a booking quantity.
  for member in select value from jsonb_array_elements(members) loop
    select count(*),sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
      sum(i.residual_source_ex_vat) residual,
      sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
      into totals from private.bpay_next_workbench_item i where i.group_key=group_row.group_key and i.component_key=member->>'component_key'
        and i.member_kind=case when member->>'segment_key' is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end
        and i.member_key=coalesce(member->>'segment_key',member->>'component_key')
        and i.approved_line_id is not distinct from (member->>'approved_line_id')::uuid
        and i.approved_segment_id is not distinct from (member->>'approved_segment_id')::uuid;
    if totals.count=0 or (totals.a,totals.c,totals.h,totals.residual,totals.split_residual) is distinct from
      ((member->>'approved_amount')::numeric,(member->>'realised_amount')::numeric,(member->>'held_amount')::numeric,
        (member->>'approved_amount')::numeric-(member->>'realised_amount')::numeric-(member->>'held_amount')::numeric,
        (member->>'approved_amount')::numeric-(member->>'realised_amount')::numeric-(member->>'held_amount')::numeric) then
      return jsonb_build_object('known',false,'reason','NATIVE_MEMBER_CONSERVATION_UNAVAILABLE','native_facts',facts);
    end if;
  end loop;
  if exists(select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key and not exists(
    select 1 from jsonb_array_elements(members) m where m->>'component_key'=i.component_key
      and coalesce(m->>'segment_key',m->>'component_key')=i.member_key
      and (m->>'approved_line_id')::uuid is not distinct from i.approved_line_id
      and (m->>'approved_segment_id')::uuid is not distinct from i.approved_segment_id
      and i.member_kind=case when m->>'segment_key' is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end
      and i.segment_key is not distinct from m->>'segment_key'
      and i.component_kind is not distinct from m->>'component_kind'
      and i.source_pay_channel is not distinct from m->>'source_pay_channel'
      and i.work_date is not distinct from (m->>'work_date')::date)) then
    return jsonb_build_object('known',false,'reason','NATIVE_FOREIGN_MEMBER_PROJECTION_UNAVAILABLE','native_facts',facts);
  end if;
  select count(*) into matching_count from jsonb_array_elements(members) m
    where m->>'segment_key'=event.id::text and m->>'component_kind' in('WORK','PROTECTED_WORK');
  select c.last_sequence into native_sequence from private.bpay_next_command_clock c where c.id=1;
  -- Reuse the exact current native accepted/snooze readers. Their genuine
  -- frozen selection/cut is never inferred from a cached null accepted ID.
  if native_sequence is null or root.week_ending_date-6>native_today
    or exists(select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key
      and i.segment_key=event.id::text and i.component_kind in('WORK','PROTECTED_WORK') and i.portion_kind='AVAILABLE'
      and (exists(select 1 from private.bpay_next_workbench_accepted_work_v1(work.id,i.component_key,i.segment_key))
        or private.bpay_next_snooze_at_cut_v1(work.id,revision.id,i.segment_key,i.component_key,native_sequence,native_today) is not null)) then
    return jsonb_build_object('known',false,'reason','NATIVE_CURRENT_ACCEPTED_OR_EXCLUDED_CUT_UNAVAILABLE',
      'native_facts',facts||jsonb_build_object('native_sequence',native_sequence,'native_business_date',native_today));
  end if;
  select count(*),sum(i.included_source_ex_vat) into positive_count,source_amount from private.bpay_next_workbench_item i
    where i.group_key=group_row.group_key and i.member_kind='WORK_SEGMENT' and i.segment_key=event.id::text
      and i.component_kind in('WORK','PROTECTED_WORK') and i.portion_kind='AVAILABLE' and i.direction='PAYMENT'
      and i.query_qualified and i.query_state='OPEN' and i.reason_code='PAY_QUERY_OPEN' and i.section='ACTION'
      and not i.selectable and not i.is_selected and i.amount_known and i.included_source_ex_vat>0
      and i.pay_hold is false and i.accepted_run_id is null and i.accepted_command_id is null;
  if matching_count=0 or positive_count=0 or source_amount is null or source_amount<=0 or exists(
    select 1 from private.bpay_next_workbench_item i where i.group_key=group_row.group_key and i.segment_key=event.id::text
      and i.component_kind in('WORK','PROTECTED_WORK') and i.portion_kind='AVAILABLE' and not i.is_selected and i.included_source_ex_vat>0
      and (i.query_qualified is distinct from true or i.query_state<>'OPEN' or i.reason_code<>'PAY_QUERY_OPEN' or i.section<>'ACTION')) then
    return jsonb_build_object('known',false,'reason','NATIVE_POSITIVE_QUERY_MONEY_UNAVAILABLE','native_facts',facts);
  end if;
  -- The false-mode native core is VOLATILE to preserve its original locked
  -- caller. Recheck its entire observation before certifying a GET, rather
  -- than treating a dirty flag or binding timestamp as a sealed snapshot.
  current_frame:=jsonb_build_object('root',to_jsonb(root),'financial',to_jsonb(financial),'event',to_jsonb(event),
    'work',to_jsonb(work),'revision',to_jsonb(revision),'group',to_jsonb(group_row),'metadata',to_jsonb(metadata),
    'worker_revision',worker_revision,'items',items);
  select jsonb_build_object('root',to_jsonb(t),'financial',to_jsonb(f),'event',to_jsonb(e),'work',to_jsonb(w),'revision',to_jsonb(r),
    'group',to_jsonb(g),'metadata',to_jsonb(md),'worker_revision',ctl.financial_view_revision,
    'items',(select coalesce(jsonb_agg(to_jsonb(i) order by i.component_key,i.member_key,i.portion_kind),'[]'::jsonb)
      from private.bpay_next_workbench_item i where i.group_key=g.group_key)) into current_frame_after
    from public.timesheets t join public.timesheets_financials f on f.id=financial.id
    join public.weekly_work_events e on e.id=event.id join private.bpay_next_work w on w.id=work.id
    join private.bpay_next_work_revision r on r.id=w.current_revision_id and r.work_id=w.id
    join private.bpay_next_workbench_group g on g.group_key=group_row.group_key
    join private.bpay_next_workbench_candidate_metadata md on md.candidate_id=w.candidate_id
    join private.bpay_next_worker_control ctl on ctl.candidate_id=w.candidate_id where t.timesheet_id=root.timesheet_id;
  begin
    select coalesce(jsonb_agg(to_jsonb(m) order by m.component_key,m.segment_key nulls first),'[]'::jsonb)
      into members_after from private.weekly_source_checklist_native_members_core_v1(work.id,revision.id,false) m;
  exception when sqlstate '55000' or sqlstate '23514' or no_data_found or too_many_rows then
    return jsonb_build_object('known',false,'reason','NATIVE_BINDING_MEMBER_CERTIFICATE_CHANGED',
      'native_facts',facts||jsonb_build_object('native_qualification_sqlstate',sqlstate));
  end;
  gate_after:=private.weekly_source_pay_query_gate_v1(root.timesheet_id);
  permission:=private.weekly_source_office_authority_v1(actor.id,'VIEW_PAY_WORK',(p_scope->>'source_group_id')::uuid,
    (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
  if v_native_period is distinct from private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today)
    or current_frame_after is distinct from current_frame or members_after is distinct from members or gate_after is distinct from gate
    or exclusions is distinct from private.bpay_next_source_query_segment_exclusions_v1(revision.id,gate_after) then
    return jsonb_build_object('known',false,'reason','NATIVE_COMPLETE_OBSERVATION_CHANGED','native_facts',facts);
  end if;
  if not exists(select 1 from public.tms_users u where u.id=actor.id and u.is_active
    and (lower(btrim(coalesce(u.role,'')))='admin' or private.weekly_source_pay_actor_allowed_v1(u.id))) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ACTOR_DENIED' using errcode='42501';
  end if;
  pence:=source_amount*100;
  if pence<>trunc(pence) or pence<1 or pence>999999999999999999 then
    return jsonb_build_object('known',false,'reason','NATIVE_MINOR_UNIT_RANGE_UNAVAILABLE','native_facts',facts);
  end if;
  facts:=facts||jsonb_build_object('native_scope',native_scope,'work',to_jsonb(work),'revision',to_jsonb(revision),'source_event_id',source_event_id,
    'group',to_jsonb(group_row),'metadata',to_jsonb(metadata),'worker_financial_view_revision',worker_revision,
    'members',members,'items',items,'query_gate',gate,'segment_query_exclusions',exclusions,
    'positive_query_item_count',positive_count,'withheld_source_ex_vat',source_amount);
  proof_hash:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_SOURCE_SHIFT_QUERY_MONEY_V1',facts),'hex');
  return jsonb_build_object('known',true,'native_facts',facts,'hold',jsonb_build_object('holds_pay',true,'holds_pay_scope','SHIFT',
    'withheld_amount_pence',pence::bigint::text,'holds_invoice',null,'pending_decision',null,
    'authority',jsonb_build_object('owner','SOURCE_QUERY_EX_VAT','hash',proof_hash,'withholding_known',true)));
end;
$function$;
create or replace function private.weekly_source_checklist_source_invoice_block_items_v1(p_scope jsonb,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; cycle public.weekly_source_cycles%rowtype;
  group_row public.weekly_source_groups%rowtype; revision public.weekly_source_final_revisions%rowtype;
  manifest public.weekly_source_client_manifests%rowtype; source_row public.weekly_source_upload_rows%rowtype;
  upload public.weekly_source_uploads%rowtype; member public.weekly_source_billing_movements%rowtype;
  worker public.candidates%rowtype; fact jsonb; facts jsonb:='[]'::jsonb; items jsonb:='[]'::jsonb;
  witness jsonb; hash text; seen uuid[]:='{}'::uuid[]; row_text text;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_scope,
    array['source_group_id','source_cycle_id','client_id','week_ending'],'WEEKLY_SOURCE_CHECKLIST_INVOICE_SCOPE_INVALID');
  if jsonb_typeof(p_scope->'source_group_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'source_group_id','uuid')
    or jsonb_typeof(p_scope->'source_cycle_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'source_cycle_id','uuid')
    or jsonb_typeof(p_scope->'client_id') is distinct from 'string' or not pg_input_is_valid(p_scope->>'client_id','uuid')
    or jsonb_typeof(p_scope->'week_ending') is distinct from 'string' or not pg_input_is_valid(p_scope->>'week_ending','date') then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_INVOICE_SCOPE_INVALID' using errcode='22023'; end if;
  select u.* into actor from public.tms_users u where u.id=p_actor_user_id and u.is_active
    and lower(btrim(coalesce(u.role,'')))='admin';
  if actor.id is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_INVOICE_ACTOR_DENIED' using errcode='42501'; end if;
  -- Finance and invoice-only population are never read under a reduced pay actor.
  perform private.weekly_source_office_authority_v1(actor.id,'VIEW_SOURCE_PROGRESS',
    (p_scope->>'source_group_id')::uuid,(p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
  select c.* into cycle from public.weekly_source_cycles c where c.id=(p_scope->>'source_cycle_id')::uuid
    and c.source_group_id=(p_scope->>'source_group_id')::uuid
    and (c.scope_client_id is null or c.scope_client_id=(p_scope->>'client_id')::uuid)
    and (p_scope->>'week_ending')::date between c.finalisation_week_ending-6 and c.finalisation_week_ending;
  select g.* into group_row from public.weekly_source_groups g where g.id=cycle.source_group_id and g.active and g.source_family='ROSTER';
  if group_row.id is null or not exists(select 1 from public.weekly_source_group_clients m
    where m.source_group_id=group_row.id and m.client_id=(p_scope->>'client_id')::uuid
      and (p_scope->>'week_ending')::date between m.valid_from and coalesce(m.valid_to,'infinity'::date)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_INVOICE_SCOPE_UNPROVED' using errcode='55000'; end if;
  -- Match the native CURRENT CYCLE completion/revision grain. A subsequent
  -- report publication does not erase the actual export invoice ledger.
  for manifest in select cm.* from public.weekly_source_client_manifests cm
    join public.weekly_source_final_revisions fr on fr.id=cm.final_revision_id and fr.state='CURRENT'
      and fr.source_cycle_id=cycle.id and fr.authority_scope_kind='CYCLE' and fr.report_scope_id is null
    where cm.source_cycle_id=cycle.id and cm.source_group_id=group_row.id and cm.client_id=(p_scope->>'client_id')::uuid
      and cm.invoice_state='READY'
    order by cm.id
  loop
    select fr.* into strict revision from public.weekly_source_final_revisions fr where fr.id=manifest.final_revision_id;
    -- Only conserved actual native manifest members can present a hold fact.
    -- Do not turn malformed native scope into a shorter/empty catalogue.
    if octet_length(manifest.manifest_hash) is distinct from 32 or octet_length(revision.manifest_hash) is distinct from 32
      or (select count(*) from public.weekly_source_manifest_movements mm where mm.client_manifest_id=manifest.id)<>manifest.movement_count
      or exists(select 1 from public.weekly_source_manifest_movements mm left join public.weekly_source_billing_movements m on m.id=mm.billing_movement_id
        where mm.client_manifest_id=manifest.id and (m.id is null or mm.movement_hash is distinct from m.movement_economic_hash
          or m.final_revision_id is distinct from revision.id or m.finalisation_cycle_id is distinct from cycle.id
          or m.actual_client_id is distinct from manifest.client_id)) then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_MANIFEST_UNAVAILABLE' using errcode='55000'; end if;
    fact:=jsonb_build_object('scope',p_scope,'actor',to_jsonb(actor),'cycle',to_jsonb(cycle),
      'manifest',to_jsonb(manifest),'revision',to_jsonb(revision));
    facts:=facts||jsonb_build_array(fact);
    for member in select m.* from private.weekly_source_invoice_unreleased_vat_fit_movements_v1(manifest.id) m order by m.id loop
      if not exists(select 1 from public.weekly_source_client_cycle_completions c where c.source_cycle_id=cycle.id
        and c.client_id=manifest.client_id and c.final_revision_id=revision.id and c.completion_kind='FINAL_SOURCE' and c.state='CURRENT') then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_COMPLETION_UNAVAILABLE' using errcode='55000'; end if;
      row_text:=member.source_facts_json->>'upload_row_id';
      if not pg_input_is_valid(row_text,'uuid') or row_text is null then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_ITEM_IDENTITY_UNAVAILABLE' using errcode='55000'; end if;
      select s.* into source_row from public.weekly_source_upload_rows s where s.id=row_text::uuid;
      select u.* into upload from public.weekly_source_uploads u where u.id=source_row.upload_id and u.source_cycle_id=cycle.id
        and u.report_scope_id is null;
      if source_row.id is null or upload.id is null or source_row.work_date is null or source_row.work_date='infinity'::date
        or source_row.work_date='-infinity'::date or member.candidate_id is null or octet_length(member.movement_economic_hash) is distinct from 32 then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_ITEM_IDENTITY_UNAVAILABLE' using errcode='55000'; end if;
      fact:=fact||jsonb_build_object('movement',to_jsonb(member),'source_row',to_jsonb(source_row),'upload',to_jsonb(upload));
      -- Observe all exact native blockers for revision freshness, including
      -- rows whose own Client week is outside this expansion's date interval.
      facts:=facts||jsonb_build_array(fact);
      if source_row.work_date not between (p_scope->>'week_ending')::date-6 and (p_scope->>'week_ending')::date then continue; end if;
      select c.* into worker from public.candidates c where c.id=member.candidate_id;
      if worker.id is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_ITEM_IDENTITY_UNAVAILABLE' using errcode='55000'; end if;
      if member.id=any(seen) then raise exception 'WEEKLY_SOURCE_CHECKLIST_INVOICE_ITEM_AMBIGUOUS' using errcode='55000'; end if;
      seen:=array_append(seen,member.id);
      -- An already admitted movement cannot be called a current admission
      -- hold; native admission refuses its placement before the hold guard.
      if member.placement_state<>'UNPLACED' or exists(select 1 from public.weekly_source_invoice_line_bindings b
        where b.billing_movement_id=member.id and b.state='CURRENT') then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_PLACEMENT_CONFLICT' using errcode='55000'; end if;
      hash:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_BLOCK_V1',fact),'hex');
      witness:=jsonb_build_object('owner','private.weekly_source_invoice_unreleased_vat_fit_movements_v1',
        'actor_user_id',actor.id,'scope',p_scope,'final_revision_id',revision.id,'client_manifest_id',manifest.id,
        'billing_movement_id',member.id,'upload_row_id',source_row.id,'work_event_id',member.work_event_id,
        'manifest_hash',encode(manifest.manifest_hash,'hex'),'movement_economic_hash',encode(member.movement_economic_hash,'hex'),'read_proof_hash',hash);
      items:=items||jsonb_build_array(jsonb_build_object('item_id','native-invoice:'||member.id::text,'worker',
        coalesce(nullif(btrim(worker.first_name||' '||worker.last_name),''),worker.id::text),
        'work_date',source_row.work_date,'native_invoice_witness',witness,
        'hold',jsonb_build_object('holds_pay',null,'holds_pay_scope',null,'withheld_amount_pence',null,'holds_invoice',true,'pending_decision',null,
          'authority',jsonb_build_object('owner','SOURCE_INVOICE_BLOCK','hash',hash,'withholding_known',false))));
    end loop;
  end loop;
  return jsonb_build_object('items',items,'native_facts',facts);
end;
$function$;
create or replace function private.weekly_source_checklist_source_invoice_decision_items_v1(
  p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $decision_items$
declare blocks jsonb; again jsonb; item jsonb; items jsonb:='[]'::jsonb;
  source_row public.weekly_source_upload_rows%rowtype; cycle public.weekly_source_cycles%rowtype;
  row_id uuid; request jsonb; context jsonb; context_again jsonb; fact jsonb;
  decisions jsonb:='[]'::jsonb; witness jsonb; proof text; known boolean; choices_known boolean;
  file_request jsonb; file_context jsonb; file_context_again jsonb;
begin
  -- One complete scope census precedes ALL row qualifications. It owns fresh
  -- service/Admin/VIEW_SOURCE_PROGRESS, dated membership and actual catalogue.
  blocks:=private.weekly_source_checklist_source_invoice_block_items_v1(p_scope,p_actor_user_id);
  select c.* into strict cycle from public.weekly_source_cycles c
    where c.id=(p_scope->>'source_cycle_id')::uuid;
  for item in select value from jsonb_array_elements(blocks->'items') loop
    row_id:=(item#>>'{native_invoice_witness,upload_row_id}')::uuid;
    select s.* into strict source_row from public.weekly_source_upload_rows s where s.id=row_id;
    known:=false;choices_known:=false;context:=null;context_again:=null;request:=null;
    file_request:=null;file_context:=null;file_context_again:=null;item:=item||jsonb_build_object('action',null);
    fact:=jsonb_build_object('native_invoice_witness',item->'native_invoice_witness');
    if cycle.version is null or cycle.version<0 or cycle.version>9007199254740991 then
      fact:=fact||jsonb_build_object('decision_known',false,'reason','NATIVE_RELEASE_VERSION_TRANSPORT_UNAVAILABLE');
    elsif octet_length(source_row.normalised_row_hash) is distinct from 32 then
      fact:=fact||jsonb_build_object('decision_known',false,'reason','NATIVE_RELEASE_ROW_HASH_UNAVAILABLE');
    else
      request:=jsonb_build_object('actor_user_id',p_actor_user_id,'upload_row_id',row_id,
        'source_cycle_id',cycle.id,'report_scope_id',null,
        'expected_source_row_hash',encode(source_row.normalised_row_hash,'hex'),
        'expected_authority_scope_version',cycle.version,'hold_kind','VAT_FIT','action','BILL_AT_NET_PLUS_20');
      fact:=fact||jsonb_build_object('native_release_request',request);
      begin
        -- Read mode is an owner-private literal. The complete current native
        -- qualification and first-invoice predicates remain their sole owners.
        context:=private.weekly_source_invoice_hold_release_context_core_v1(request,false);
        perform private.weekly_source_invoice_hold_first_invoice_assert_v1(row_id);
        context_again:=private.weekly_source_invoice_hold_release_context_core_v1(request,false);
        perform private.weekly_source_invoice_hold_first_invoice_assert_v1(row_id);
        if context is distinct from context_again then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
        known:=true;
      exception when sqlstate '55000' or sqlstate '40001' or sqlstate '22023' then
        -- Preserve the base item and actual invoice hold on unsupported native
        -- qualification. Permission/unexpected failures propagate to the owner.
        if sqlerrm='WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' then raise; end if;
        fact:=fact||jsonb_build_object('decision_known',false,'reason','NATIVE_RELEASE_QUALIFICATION_UNAVAILABLE',
          'qualification_sqlstate',sqlstate,'qualification_code',sqlerrm);
      end;
      fact:=fact||jsonb_build_object('native_release_context',context,'native_release_context_again',context_again);
      if known and (context->>'actor_user_id' is distinct from p_actor_user_id::text
        or context->>'upload_row_id' is distinct from row_id::text
        or context->>'client_id' is distinct from p_scope->>'client_id'
        or context#>>'{cycle,id}' is distinct from p_scope->>'source_cycle_id'
        or context#>>'{group,id}' is distinct from p_scope->>'source_group_id'
        or context->>'expected_source_row_hash' is distinct from request->>'expected_source_row_hash'
        or context->>'expected_authority_scope_version' is distinct from request->>'expected_authority_scope_version'
        or context#>>'{upload,report_scope_id}' is not null or context->>'hold_kind' is distinct from 'VAT_FIT'
        or context->>'choice' is distinct from 'BILL_AT_NET_PLUS_20'
        or context#>>'{rules,invoice_sender}' is distinct from 'CLIENT_SELF_BILL'
        or context#>>'{rules,invoice_amount_basis}' is distinct from 'SOURCE_AMOUNT'
        or context#>>'{rules,source_amount_basis}' is distinct from 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY') then
        known:=false;fact:=fact||jsonb_build_object('decision_known',false,'reason','NATIVE_RELEASE_DECISION_GRAIN_UNAVAILABLE');
      end if;
    end if;
    if known then
      if exists(select 1 from public.weekly_source_invoice_hold_releases r
        where r.upload_row_id=row_id and r.hold_kind='VAT_FIT') then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
      -- Choice eligibility is native-owned. Do not infer BILL_AS_FILE from the
      -- default qualification, calculate money or expose a public lock mode.
      file_request:=request||jsonb_build_object('action','BILL_AS_FILE');
      begin
        file_context:=private.weekly_source_invoice_hold_release_context_core_v1(file_request,false);
        perform private.weekly_source_invoice_hold_first_invoice_assert_v1(row_id);
        file_context_again:=private.weekly_source_invoice_hold_release_context_core_v1(file_request,false);
        perform private.weekly_source_invoice_hold_first_invoice_assert_v1(row_id);
        if file_context is distinct from file_context_again or (file_context-'choice') is distinct from (context-'choice')
          or file_context->>'choice' is distinct from 'BILL_AS_FILE' then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
        choices_known:=true;
      exception when sqlstate '55000' or sqlstate '40001' or sqlstate '22023' then
        if sqlerrm='WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' then raise; end if;
        fact:=fact||jsonb_build_object('choice_qualification_sqlstate',sqlstate,'choice_qualification_code',sqlerrm);
      end;
      fact:=fact||jsonb_build_object('decision_known',true,'pending_decision',true,'native_release_choices_known',choices_known,
        'native_file_release_request',file_request,'native_file_release_context',file_context,'native_file_release_context_again',file_context_again);
      proof:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_NATIVE_INVOICE_DECISION_V1',fact),'hex');
      witness:=jsonb_build_object('owner','private.weekly_source_invoice_hold_release_context_core_v1',
        'actor_user_id',p_actor_user_id,'scope',p_scope,
        'billing_movement_id',item#>>'{native_invoice_witness,billing_movement_id}',
        'client_manifest_id',item#>>'{native_invoice_witness,client_manifest_id}','upload_row_id',row_id,
        'invoice_block_read_proof_hash',item#>>'{native_invoice_witness,read_proof_hash}',
        'source_row_hash',encode(source_row.normalised_row_hash,'hex'),'authority_scope_version',cycle.version,
        'hold_kind','VAT_FIT','qualification_choice','BILL_AT_NET_PLUS_20','read_proof_hash',proof);
      item:=item||jsonb_build_object('action',case when choices_known then jsonb_build_object(
          'kind','INVOICE_HOLD_DECISION','label','Review invoice hold','native_allowed',true,
          'target',jsonb_build_object('contract','WEEKLY_SOURCE_CHECKLIST_INVOICE_HOLD_DECISION_ACTION_V1',
            'actor_user_id',p_actor_user_id,'permission_view','ADMIN','scope',p_scope,
            'billing_movement_id',witness->'billing_movement_id','client_manifest_id',witness->'client_manifest_id','upload_row_id',row_id,
            'expected_source_row_hash',witness->'source_row_hash','expected_authority_scope_version',cycle.version,
            'hold_kind','VAT_FIT','decision_read_proof_hash',proof,'choices',jsonb_build_array('BILL_AT_NET_PLUS_20','BILL_AS_FILE'))) end,
        'native_invoice_decision_witness',witness,
        'hold',jsonb_build_object('holds_pay',null,'holds_pay_scope',null,'withheld_amount_pence',null,
          'holds_invoice',true,'pending_decision',true,
          'authority',jsonb_build_object('owner','SOURCE_INVOICE_DECISION','hash',proof,'withholding_known',false)));
    end if;
    -- Both positive and unknown decision outcomes keep every base item.
    items:=items||jsonb_build_array(item);decisions:=decisions||jsonb_build_array(fact);
  end loop;
  -- One second complete census for the entire scope proves the same actor,
  -- catalogue and binding. Never re-census the whole scope for every movement.
  again:=private.weekly_source_checklist_source_invoice_block_items_v1(p_scope,p_actor_user_id);
  if blocks is distinct from again then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
  return jsonb_build_object('items',items,'native_facts',jsonb_build_object(
    'native_invoice_blocks',blocks->'native_facts','native_release_qualifications',decisions));
end;
$decision_items$;
create or replace function private.weekly_source_checklist_process_publication_v1(p_receipt_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $process_publication$
declare receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
 fr public.weekly_source_final_revisions%rowtype;root public.timesheets%rowtype;
 head public.weekly_source_entitlement_heads%rowtype;bundle public.weekly_source_entitlement_decision_bundles%rowtype;
 publication private.weekly_source_entitlement_publication_receipts%rowtype;
 generation public.weekly_exceptional_pay_generations%rowtype;approval public.weekly_exceptional_payment_approvals%rowtype;
 local_receipt private.weekly_source_local_protected_decision_receipts%rowtype;
 c1_request public.weekly_exceptional_c1_publication_requests%rowtype;
 inventory jsonb;basis jsonb;native_origin jsonb;witness jsonb;current_receipt jsonb;next_application jsonb;
 event record;current_root uuid;current_approval uuid;current_event uuid;member integer;n bigint;
 facts jsonb;events jsonb:='[]'::jsonb;event_fact jsonb;proved boolean:=false;event_proved boolean;
 source_units jsonb;source_expenses jsonb;expected_events jsonb;expected_event jsonb;event_matches bigint;
begin
 select r.* into receipt from public.weekly_source_ordinary_pay_projection_receipts r where r.id=p_receipt_id;
 select r.* into fr from public.weekly_source_final_revisions r where r.id=receipt.final_revision_id;
 select t.* into root from public.timesheets t where t.timesheet_id=receipt.root_timesheet_id;
 facts:=jsonb_build_object('receipt',to_jsonb(receipt),'final_revision',to_jsonb(fr),'root',to_jsonb(root));
 if receipt.id is null or fr.id is null or root.timesheet_id is null
  or private.weekly_source_report_projection_receipt_assert_v1(receipt) is distinct from true
  or receipt.source_cycle_id is distinct from fr.source_cycle_id
  or receipt.final_manifest_hash is distinct from fr.manifest_hash
  or receipt.final_policy_fingerprint is distinct from fr.policy_fingerprint
  or fr.state not in('CURRENT','SUPERSEDED')
  or not exists(select 1 from public.contracts c where c.id=root.contract_id and c.client_id=receipt.client_id)
  or receipt.outcome not in('PROPOSED','TARGET_MANAGED_SUPPRESSED') then
  return jsonb_build_object('proved',false,'native_facts',facts);end if;
 -- The caller proves its immutable task/run/context. This private leaf proves
 -- the current native publication only, at that SAME receipt revision.
 if receipt.outcome='PROPOSED' then
  inventory:=private.weekly_source_effective_inventory_v1(root.timesheet_id);basis:=inventory->'approval_basis';
  if basis->'coverage_complete'='true'::jsonb and inventory->>'authority'='HEAD' then
   select h.* into head from public.weekly_source_entitlement_heads h where h.id=(inventory->>'head_id')::uuid;
   select b.* into bundle from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=head.decision_bundle_id and b.bundle_revision=head.bundle_revision;
   select count(*) into n from private.weekly_source_entitlement_publication_receipts p
    where p.decision_bundle_id=head.decision_bundle_id and p.bundle_revision=head.bundle_revision and p.request_digest=bundle.request_digest;
   if n=1 then
    select p.* into strict publication from private.weekly_source_entitlement_publication_receipts p
     where p.decision_bundle_id=head.decision_bundle_id and p.bundle_revision=head.bundle_revision and p.request_digest=bundle.request_digest;
    member:=array_position(publication.head_ids,head.id);native_origin:=head.source_origin_json;
    if bundle.state='COMMITTED' and bundle.bundle_kind='SINGLE_ROOT' and cardinality(publication.head_ids)=1
     and member=1 and publication.member_root_ids[member]=root.timesheet_id
     and publication.member_family_booking_ids[member]=root.booking_id
     and publication.member_root_versions[member]=head.root_timesheet_version
     and publication.candidate_id=(select c.candidate_id from public.contracts c where c.id=root.contract_id)
     and head.authority_kind='LOCKED_FINAL_SOURCE'
     and native_origin->>'final_revision_id'=fr.id::text and native_origin->>'source_cycle_id'=fr.source_cycle_id::text
     and native_origin->>'revision_number'=fr.revision_number::text
     and native_origin->>'manifest_hash'=encode(fr.manifest_hash,'hex')
     and native_origin->>'policy_fingerprint'=encode(fr.policy_fingerprint,'hex')
     and head.source_generation_digest=private.weekly_source_publication_request_digest_v1(native_origin)
     and bundle.source_revision_digest=head.source_generation_digest
     and head.decision_bundle_id=private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_DECISION_BUNDLE_V1',btrim(root.booking_id)||'|'||fr.id::text) then proved:=true;end if;
   end if;
  end if;
  facts:=facts||jsonb_build_object('inventory',inventory,'head',to_jsonb(head),'bundle',to_jsonb(bundle),'publication',to_jsonb(publication));
 end if;
 if not proved then
  -- A task can contain multiple actual Source work events. Every corresponding
  -- snapshot/lineage member must carry its OWN current generation publication.
  source_units:=private.weekly_source_ordinary_projection_source_units_v1(fr.id,root.timesheet_id);
  source_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(root.timesheet_id,fr.id);
  facts:=facts||jsonb_build_object('native_source_units',source_units,'native_source_expenses',source_expenses);
  if source_units is distinct from receipt.source_unit_outcomes_json
   or source_expenses is distinct from receipt.source_expense_authorities_json
   or exists(select 1 from jsonb_array_elements(source_units||source_expenses) unit
    where jsonb_typeof(unit->'work_event_id') is distinct from 'string'
     or pg_input_is_valid(unit->>'work_event_id','uuid') is distinct from true) then
   return jsonb_build_object('proved',false,'native_facts',facts);end if;
  select coalesce(jsonb_agg(event_id order by event_id),'[]'::jsonb) into expected_events
   from (select distinct unit->>'work_event_id' event_id from jsonb_array_elements(source_units||source_expenses) unit) complete_events;
  facts:=facts||jsonb_build_object('expected_native_events',expected_events);
  n:=0;proved:=true;
  for expected_event in select value from jsonb_array_elements(expected_events)
  loop
   select count(*) into event_matches
   from public.weekly_source_final_snapshot_lines fs
   join public.weekly_source_row_resolutions rr on rr.id=fs.row_resolution_id and rr.upload_row_id=fs.upload_row_id
    and rr.work_event_id=fs.work_event_id and rr.candidate_id=fs.candidate_id and rr.client_id=fs.client_id
    and rr.contract_id=fs.contract_id and rr.mapping_state='RESOLVED'
   join public.weekly_source_row_timesheet_lineages l on l.row_resolution_id=rr.id
    and l.work_event_id=fs.work_event_id and l.candidate_id=fs.candidate_id and l.client_id=fs.client_id
    and l.contract_id=fs.contract_id and l.source_cycle_id=fr.source_cycle_id
    and l.family_booking_id=root.booking_id and l.week_ending_date=root.week_ending_date
    and l.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(root.timesheet_id))
   where fs.final_revision_id=fr.id and fs.work_event_id=(expected_event#>>'{}')::uuid
    and fs.client_id=receipt.client_id and fs.contract_id=root.contract_id
    and fs.candidate_id=(select c.candidate_id from public.contracts c where c.id=root.contract_id)
    and exists(select 1 from public.weekly_source_projection_publications p
     where p.upload_id=fr.upload_id and p.source_cycle_id=fr.source_cycle_id
      and p.authority_scope_kind=fr.authority_scope_kind and p.report_scope_id is not distinct from fr.report_scope_id
      and p.state in('CURRENT','SUPERSEDED') and rr.generation=coalesce(p.projection_generation,p.authority_scope_version));
   if event_matches<>1 then
    return jsonb_build_object('proved',false,'native_facts',facts||jsonb_build_object(
     'unproved_expected_event',expected_event,'exact_snapshot_lineage_matches',event_matches));end if;
   select fs.work_event_id,fs.candidate_id,fs.client_id,fs.contract_id into strict event
    from public.weekly_source_final_snapshot_lines fs where fs.final_revision_id=fr.id and fs.work_event_id=(expected_event#>>'{}')::uuid;
   n:=n+1;event_proved:=false;current_receipt:=null;current_approval:=null;current_event:=null;
   approval:=null;generation:=null;local_receipt:=null;c1_request:=null;inventory:=null;basis:=null;next_application:=null;
   witness:=private.weekly_source_protected_wait_witness_v1(event.work_event_id,fr.source_cycle_id);
   if witness is not null and exists(select 1 from public.timesheets t
    where t.timesheet_id=(witness->>'root_timesheet_id')::uuid and t.booking_id=root.booking_id
     and t.contract_id=root.contract_id and t.week_ending_date=root.week_ending_date
     and t.sheet_scope='WEEKLY' and t.line_type='HOURS' and t.is_adjustment is false) then
    current_root:=(witness->>'root_timesheet_id')::uuid;
    select g.* into strict generation from public.weekly_exceptional_pay_generations g where g.id=(witness->>'current_generation_id')::uuid;
    select count(*),(array_agg(t.approval_id))[1] into member,current_approval
     from public.weekly_exceptional_pay_target_events t where t.family_id=(witness->>'family_id')::uuid and t.financial_generation_id=generation.id;
    if member=1 then
     select a.* into strict approval from public.weekly_exceptional_payment_approvals a where a.id=current_approval;
     select count(*),(array_agg(e.id))[1] into member,current_event
      from public.weekly_exceptional_pay_family_events e where e.family_id=approval.pay_target_family_id
       and e.durable_work_event_id=approval.work_event_id and e.evidence_approval_id=approval.id and e.state='WAIT'
       and not exists(select 1 from public.weekly_exceptional_pay_family_events newer where newer.family_id=e.family_id
        and newer.durable_work_event_id=e.durable_work_event_id and newer.event_sequence>e.event_sequence);
     if member=1 then current_receipt:=private.weekly_source_protected_wait_receipt_v1(current_root,current_event)->'accepted_receipt';end if;
    end if;
    if current_receipt->>'receipt_state' in('COMPLETE','PUBLISHED') and current_receipt->>'generation_id'=generation.id::text
     and approval.candidate_id=event.candidate_id and approval.client_id=event.client_id and approval.contract_id=event.contract_id then
     if current_receipt->>'lane'='SOURCE_LOCAL' then
      select l.* into local_receipt from private.weekly_source_local_protected_decision_receipts l
       where l.generation_id=generation.id and l.publication_request_id=(current_receipt->>'publication_request_id')::uuid;
      inventory:=private.weekly_source_effective_inventory_v1(current_root);basis:=inventory->'approval_basis';
      if basis->'coverage_complete'='true'::jsonb and basis#>>'{scope,candidate_id}'=event.candidate_id::text
       and basis#>>'{scope,client_id}'=event.client_id::text and basis#>>'{scope,contract_id}'=event.contract_id::text then
       if local_receipt.result_json->>'authority'='COMMON_ENTITLEMENT_HEAD'
        and basis#>>'{origin,source_revision,generation_id}'=generation.id::text
        and basis#>>'{origin,source_revision,publication_request_id}'=local_receipt.publication_request_id::text then event_proved:=true;
       elsif local_receipt.result_json->>'authority'='UNAUTHORISED_TSFIN'
        and basis#>>'{origin,financial_snapshot_id}'=local_receipt.result_json->>'timesheet_financials_id' then event_proved:=true;end if;
      end if;
     elsif current_receipt->>'lane'='RETAINED_C1' then
      select c.* into c1_request from public.weekly_exceptional_c1_publication_requests c
       where c.id=(current_receipt->>'publication_request_id')::uuid and c.generation_id=generation.id;
      if c1_request.state='PUBLISHED' and c1_request.financial_row_id is not null then event_proved:=true;end if;
     elsif current_receipt->>'lane'='DIRECT_NEXT' then
      next_application:=private.bpay_next_source_origin_state_v1((current_receipt->>'work_id')::uuid,(current_receipt->>'revision_id')::uuid);
      event_proved:=true;
     end if;
    end if;
   end if;
   event_fact:=jsonb_build_object('work_event_id',event.work_event_id,'candidate_id',event.candidate_id,'client_id',event.client_id,
    'contract_id',event.contract_id,'witness',witness,'generation',to_jsonb(generation),'approval',to_jsonb(approval),
    'current_receipt',current_receipt,'local_receipt',to_jsonb(local_receipt),'c1_request',to_jsonb(c1_request),
    'inventory',inventory,'next_application',next_application,'proved',event_proved);
   events:=events||jsonb_build_array(event_fact);proved:=proved and event_proved;
  end loop;
  proved:=proved and n>0 and n=jsonb_array_length(expected_events);
  facts:=facts||jsonb_build_object('native_current_publications',events);
 end if;
 return jsonb_build_object('proved',coalesce(proved,false),'native_facts',facts);
end;
$process_publication$;
create or replace function private.weekly_source_checklist_process_v1(p_cycle uuid,p_client uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare revision record; task record; run public.weekly_source_finalisation_pay_runs%rowtype;
  receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype; contexts jsonb;
  facts jsonb:='[]'::jsonb; proof boolean; tasks_proved boolean; count_tasks integer;
  publication_proof jsonb;publication_proof_after jsonb;publication_facts jsonb;
begin
  for revision in
    -- History is independent per real scope. A report completion must never
    -- erase the export pay revision merely by superseding a client completion.
    select distinct on(fr.authority_scope_kind,fr.report_scope_id) fr.*,cm.id client_manifest_id,cm.manifest_hash client_manifest_hash
    from public.weekly_source_final_revisions fr
    join public.weekly_source_client_manifests cm on cm.final_revision_id=fr.id and cm.client_id=p_client
      and cm.source_cycle_id=fr.source_cycle_id
    where fr.source_cycle_id=p_cycle and fr.state in('CURRENT','SUPERSEDED')
    order by fr.authority_scope_kind,fr.report_scope_id,fr.revision_number desc,fr.id desc
  loop
    proof:=isfinite(revision.finalised_at_utc) and octet_length(revision.manifest_hash)=32
      and octet_length(revision.client_manifest_hash)=32
      and exists(select 1 from public.weekly_source_uploads u
        join public.weekly_source_upload_attempts a on a.logical_upload_id=u.id and a.result='ACCEPTED'
        join public.weekly_source_projection_publications publication on publication.upload_id=u.id
          and publication.source_cycle_id=p_cycle
          and publication.report_scope_id is not distinct from revision.report_scope_id
          and publication.authority_scope_kind=revision.authority_scope_kind
        where u.id=revision.upload_id and u.source_cycle_id=p_cycle
          and u.report_scope_id is not distinct from revision.report_scope_id);
    publication_facts:='[]'::jsonb;
    if revision.authority_scope_kind<>'SELF_BILL_REPORT_SCOPE' then
      select * into run from public.weekly_source_finalisation_pay_runs r where r.final_revision_id=revision.id;
      select coalesce(jsonb_agg(t.prepared_context_json||jsonb_build_object(
        'prepared_context_hash',encode(t.prepared_context_hash,'hex')) order by t.root_timesheet_id),'[]'::jsonb),count(*)::integer
        into contexts,count_tasks from public.weekly_source_finalisation_pay_tasks t where t.run_id=run.id;
      proof:=proof and run.id is not null and run.source_cycle_id=p_cycle and run.state='COMPLETE'
        and isfinite(run.completed_at_utc) and run.final_revision_manifest_hash=revision.manifest_hash
        and run.task_count=count_tasks and run.terminal_task_count=run.task_count and run.action_required_task_count=0
        and run.run_hash=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_FINALISATION_PAY_RUN_V1',jsonb_build_object(
          'final_revision_id',run.final_revision_id,'source_cycle_id',run.source_cycle_id,'requested_by_user_id',run.requested_by_user_id,
          'final_revision_manifest_hash',encode(run.final_revision_manifest_hash,'hex'),
          'task_manifest_hash',encode(run.task_manifest_hash,'hex'),'task_count',run.task_count))
        and run.task_manifest_hash=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_FINALISATION_PAY_TASK_MANIFEST_V1',contexts);
      tasks_proved:=true;
      for task in select t.*,m.client_id from public.weekly_source_finalisation_pay_tasks t
        join public.weekly_source_client_manifests m on m.id=t.client_manifest_id where t.run_id=run.id
      loop
        select * into receipt from public.weekly_source_ordinary_pay_projection_receipts r where r.id=task.projection_receipt_id;
        tasks_proved:=tasks_proved and task.state in('PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED')
          and isfinite(task.completed_at_utc) and receipt.id is not null
          and receipt.final_revision_id=revision.id and receipt.source_cycle_id=p_cycle
          and receipt.client_id=task.client_id and receipt.root_timesheet_id=task.root_timesheet_id
          and receipt.actor_user_id=run.requested_by_user_id and receipt.outcome=task.state
          and receipt.idempotency_key=task.projection_idempotency_key
          and receipt.final_manifest_hash=revision.manifest_hash and receipt.final_policy_fingerprint=revision.policy_fingerprint
          and private.weekly_source_report_projection_receipt_assert_v1(receipt) is true
          and task.prepared_context_hash=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_FINALISATION_PAY_ROOT_CONTEXT_V1',task.prepared_context_json)
          and task.prepared_context_json->>'schema_version'='WEEKLY_SOURCE_FINALISATION_PAY_ROOT_CONTEXT_V1'
          and task.prepared_context_json->>'final_revision_id'=revision.id::text
          and task.prepared_context_json->>'source_cycle_id'=p_cycle::text
          and task.prepared_context_json->>'client_id'=task.client_id::text
          and task.prepared_context_json->>'client_manifest_id'=task.client_manifest_id::text
          and task.prepared_context_json->>'root_timesheet_id'=task.root_timesheet_id::text;
        -- Orchestration alone still proves nothing. Require the SAME actual
        -- receipt revision's current native publication, for every actual event.
        if task.client_id=p_client and task.state in('PROPOSED','TARGET_MANAGED_SUPPRESSED') then
          publication_proof:=private.weekly_source_checklist_process_publication_v1(receipt.id);
          publication_proof_after:=private.weekly_source_checklist_process_publication_v1(receipt.id);
          if publication_proof is distinct from publication_proof_after then
            raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';end if;
          publication_facts:=publication_facts||jsonb_build_array(jsonb_build_object('task_id',task.id,'publication',publication_proof));
          tasks_proved:=tasks_proved and publication_proof->'proved'='true'::jsonb;
        end if;
        if task.state='PREPARED_FOR_AUTHORISATION' and not exists(select 1 from public.timesheets_financials f
          join public.timesheets root on root.timesheet_id=task.root_timesheet_id
          where f.id=receipt.published_timesheet_financial_id and f.timesheet_id=task.root_timesheet_id
            and f.client_id=task.client_id and f.timesheet_version=task.root_timesheet_version
            and root.booking_id=task.root_family_booking_id) then tasks_proved:=false; end if;
      end loop;
      proof:=proof and tasks_proved;
    end if;
    facts:=facts||jsonb_build_array(jsonb_build_object('final_revision_id',revision.id,
      'authority_scope_kind',revision.authority_scope_kind,'report_scope_id',revision.report_scope_id,
      'client_manifest_id',revision.client_manifest_id,'manifest_hash',encode(revision.manifest_hash,'hex'),
      'client_manifest_hash',encode(revision.client_manifest_hash,'hex'),'finalised_at_utc',revision.finalised_at_utc,
      'process_complete',coalesce(proof,false),'native_publication_facts',publication_facts,'pay_run_id',case when revision.authority_scope_kind<>'SELF_BILL_REPORT_SCOPE' then run.id end,
      'pay_run_hash',case when revision.authority_scope_kind<>'SELF_BILL_REPORT_SCOPE' then encode(run.run_hash,'hex') end));
  end loop;
  return jsonb_build_object('contract','WEEKLY_SOURCE_CHECKLIST_PROCESS_V1','source_cycle_id',p_cycle,'client_id',p_client,
    'scopes',facts,'financial_withholding_proved',false,
    'read_revision',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_PROCESS_V1',facts),'hex'));
end;
$function$;
create or replace function private.weekly_source_checklist_page_v1(
  p_records jsonb,p_request jsonb,p_actor uuid,p_view text,p_binding text,p_revision text
) returns jsonb language plpgsql immutable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  command text:=p_request->>'command'; direction text:=p_request->>'direction';
  page_limit integer:=(p_request->>'limit')::integer; token jsonb; anchor jsonb;
  selected jsonb; page jsonb; last_record jsonb; more boolean; after_key jsonb;
begin
  if jsonb_typeof(p_records) is distinct from 'array' or p_view not in('ADMIN','PAY_READONLY')
    or p_binding !~ '^[0-9a-f]{64}$' or p_revision !~ '^[0-9a-f]{64}$'
    or command not in('CHECKLIST','CHECKLIST_ISSUES') or direction not in('ASC','DESC')
    or page_limit not between 1 and 100 then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID' using errcode='55000';
  end if;
  if nullif(p_request->>'cursor','') is not null then
    begin
      token:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      perform private._weekly_source_settings_assert_request_v1(token,
        array['contract','binding','read_revision','after'],'CURSOR');
      if token->>'contract' is distinct from 'WEEKLY_SOURCE_CHECKLIST_CURSOR_V1'
        or token->>'binding' is distinct from p_binding or token->>'read_revision' is distinct from p_revision
        or jsonb_typeof(token->'after') is distinct from 'array'
        or jsonb_array_length(token->'after')<>6
        -- A cursor must name an actual admitted row in this exact revision.
        -- A changed/closed subject cannot be used to manufacture an empty end.
        or not exists(select 1 from jsonb_array_elements(p_records) r where r->'_key'=token->'after') then
        raise exception 'cursor';
      end if;
      after_key:=token->'after';
    exception when others then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
    end;
  end if;
  if nullif(p_request->>'anchor','') is not null then
    begin
      anchor:=convert_from(decode(p_request->>'anchor','base64'),'UTF8')::jsonb;
      perform private._weekly_source_settings_assert_request_v1(anchor,array['contract','binding','identity'],'ANCHOR');
      if anchor->>'contract' is distinct from 'WEEKLY_SOURCE_CHECKLIST_ANCHOR_V1'
        or anchor->>'binding' is distinct from p_binding
        or jsonb_typeof(anchor->'identity') is distinct from 'string'
        or length(anchor->>'identity') not between 1 and 512 then raise exception 'anchor'; end if;
      -- Anchored refresh starts at the origin. The Office consumes its previous
      -- prefix and finds the nearest surviving visible identity itself.
    exception when others then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
    end;
  end if;
  with candidates as (
    select r from jsonb_array_elements(p_records) r
    where after_key is null
      or (direction='ASC' and (r#>>'{_key,0}') collate "C">(after_key->>0) collate "C")
      or (direction='DESC' and (r#>>'{_key,0}') collate "C"<(after_key->>0) collate "C")
      or (r#>>'{_key,0}'=after_key->>0 and
        ((r#>>'{_key,1}') collate "C",(r#>>'{_key,2}') collate "C",(r#>>'{_key,3}') collate "C",(r#>>'{_key,4}') collate "C",(r#>>'{_key,5}') collate "C")>
        ((after_key->>1) collate "C",(after_key->>2) collate "C",(after_key->>3) collate "C",(after_key->>4) collate "C",(after_key->>5) collate "C"))
    order by case when direction='ASC' then r#>>'{_key,0}' end collate "C" asc,
      case when direction='DESC' then r#>>'{_key,0}' end collate "C" desc,
      r#>>'{_key,1}' collate "C",r#>>'{_key,2}' collate "C",r#>>'{_key,3}' collate "C",
      r#>>'{_key,4}' collate "C",r#>>'{_key,5}' collate "C"
    limit page_limit+1
  ) select coalesce(jsonb_agg(r order by
      case when direction='ASC' then r#>>'{_key,0}' end collate "C" asc,
      case when direction='DESC' then r#>>'{_key,0}' end collate "C" desc,
      r#>>'{_key,1}' collate "C",r#>>'{_key,2}' collate "C",r#>>'{_key,3}' collate "C",
      r#>>'{_key,4}' collate "C",r#>>'{_key,5}' collate "C"),'[]'::jsonb),count(*)>page_limit into selected,more from candidates;
  if more then selected:=selected-page_limit; end if;
  last_record:=selected->(jsonb_array_length(selected)-1);
  select coalesce(jsonb_agg(r-'_key' order by ordinal),'[]'::jsonb) into page from jsonb_array_elements(selected) with ordinality a(r,ordinal);
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_CHECKLIST_V1','command',command,
    'actor_user_id',p_actor,'permission_view',p_view,'binding',p_binding,'read_revision',p_revision,
    'rows',case when command='CHECKLIST' then page else '[]'::jsonb end,
    'items',case when command='CHECKLIST_ISSUES' then page else '[]'::jsonb end,
    'has_more',more,'next_cursor',case when more then replace(encode(convert_to(jsonb_build_object(
      'contract','WEEKLY_SOURCE_CHECKLIST_CURSOR_V1','binding',p_binding,'read_revision',p_revision,
      'after',last_record->'_key')::text,'UTF8'),'base64'),chr(10),'') end,
    'refresh_required',false,'anchor',case when last_record is not null then replace(encode(convert_to(jsonb_build_object(
      'contract','WEEKLY_SOURCE_CHECKLIST_ANCHOR_V1','binding',p_binding,
      'identity',coalesce(last_record->>'row_id',last_record->>'item_id'))::text,'UTF8'),'base64'),chr(10),'') end);
end;
$function$;
create or replace function private.weekly_source_office_tracker_v1(p_cycle_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_actor uuid:=p_actor_user_id; v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype; v_tracker jsonb;
begin
  select c.* into v_cycle from public.weekly_source_cycles c where c.id=p_cycle_id;
  select g.* into v_group from public.weekly_source_groups g where g.id=v_cycle.source_group_id and g.active;
  if v_cycle.id is null or v_group.id is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_UNAVAILABLE' using errcode='22023'; end if;
  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',v_group.id);
  select pg_catalog.jsonb_build_object(
    'title','Finalisation progress','cycle_label','Week ending '||to_char(v_cycle.finalisation_week_ending,'FMDD Mon YYYY'),
    'cycle_id',v_cycle.id,
    'cycle_options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'value',item.id,'label','Week ending '||to_char(item.finalisation_week_ending,'FMDD Mon YYYY'))
      order by item.finalisation_week_ending desc,item.id)
      from public.weekly_source_cycles item where item.source_group_id=v_group.id),'[]'::jsonb),
    'complete',not exists(
      select 1 from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and (v_cycle.scope_client_id is null or membership.client_id=v_cycle.scope_client_id)
        and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
        and not exists(select 1 from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id and completion.client_id=membership.client_id and completion.state='CURRENT')
    ),
    'rows',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','tracker-'||membership.client_id::text,'source',v_group.display_name,'client',client.name,
      'status',pg_catalog.jsonb_build_object(
        'text',case when completion.id is not null then case completion.completion_kind
          when 'FINAL_SOURCE' then 'Finalised' else 'No shifts this week' end else 'Not finalised' end,
        'tone',case when completion.id is not null then 'positive' else 'warning' end),
      'detail',case when completion.id is not null then to_char(completion.attested_at_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI') else '' end,
      'actions',case
        when completion.id is null
         and v_cycle.state not in ('FINALISING','FINALISED')
         and not exists(select 1 from public.weekly_source_final_revisions revision
           join public.weekly_source_final_snapshot_lines snapshot on snapshot.final_revision_id=revision.id
           where revision.source_cycle_id=v_cycle.id and revision.state in ('PREPARED','CURRENT')
             and snapshot.client_id=membership.client_id)
         and not exists(select 1 from public.weekly_source_uploads upload
           join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
           left join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
             where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution on true
           where upload.source_cycle_id=v_cycle.id
             and (upload.id=v_cycle.current_complete_upload_id or upload.state='CURRENT')
             and resolution.mapping_state is distinct from 'RESOLVED')
         and not exists(select 1 from public.weekly_source_report_scopes scope
           where scope.source_cycle_id=v_cycle.id and scope.client_id=membership.client_id
             and (scope.current_complete_upload_id is not null or scope.current_projection_publication_id is not null or scope.current_final_revision_id is not null))
         and not exists(select 1 from public.weekly_source_upload_rows source_row
           join public.weekly_source_uploads upload on upload.id=source_row.upload_id and upload.source_cycle_id=v_cycle.id and upload.state='CURRENT'
           join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
             where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution
             on resolution.mapping_state='RESOLVED' and resolution.client_id=membership.client_id)
         and not exists(select 1 from public.weekly_discrepancy_incidents incident
           where incident.source_cycle_id=v_cycle.id and incident.client_id=membership.client_id and incident.state='OPEN')
        then pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','No shifts to import','enabled',true,'kind','COMMAND','command','NO_SHIFTS_TO_IMPORT',
          'context',pg_catalog.jsonb_build_object(
            'trust',client.name,
            'cutoff',to_char(v_cycle.cutoff_at_utc at time zone 'Europe/London','Dy FMDD Mon YYYY HH24:MI')
          ),
          'payload',pg_catalog.jsonb_build_object(
            'actor_user_id',v_actor,
            'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
            'client_id',membership.client_id,'expected_cycle_version',v_cycle.version,
            'attestation_text','No shifts to import'
          )))
        else pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','View','enabled',true,'payload',pg_catalog.jsonb_build_object('client_id',membership.client_id)
        )) end
    ) order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
    from public.weekly_source_group_clients membership
    join public.clients client on client.id=membership.client_id
    left join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=v_cycle.id and completion.client_id=membership.client_id and completion.state='CURRENT'
    where membership.source_group_id=v_group.id
      and (v_cycle.scope_client_id is null or membership.client_id=v_cycle.scope_client_id)
      and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb)
  ) into v_tracker;
  return v_tracker;
end;
$function$;
create or replace function private.weekly_source_healthroster_checklist_leaf_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare c public.weekly_source_cycles%rowtype; g public.weekly_source_groups%rowtype;
  v_actor uuid; v_client uuid; v_week date; v_today date; v_admin boolean; v_rule jsonb;
  v_route text; v_actual jsonb; v_facts jsonb; v_unproved jsonb; v_gate jsonb:='[]'::jsonb;
  v_latest_week date; v_latest_timesheet_week date; v_calendar_authority jsonb;
  v_open integer; v_earlier_open integer; v_count integer; v_wait integer:=0; v_wait_fact jsonb; v_event record;
  v_read_scope record; v_page jsonb; v_cursor text; v_rows jsonb; v_revision text; v_query_revision text;
  v_scope_count integer; v_scope_open integer; v_retention_reads jsonb:='[]'::jsonb;
  v_money jsonb;v_legacy jsonb;v_money_row jsonb;v_money_scope jsonb;v_scope_money jsonb;v_scope_money_facts jsonb;
  v_financial_items jsonb:='[]'::jsonb;v_financial_facts jsonb:='[]'::jsonb;
begin
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_cycle_id','source_group_id','client_id','week_ending','command','cursor','limit'],
    'WEEKLY_SOURCE_CHECKLIST_LEAF_REQUEST_INVALID');
  v_actor:=(p_request->>'actor_user_id')::uuid; v_client:=(p_request->>'client_id')::uuid;
  if coalesce(p_request->>'command','CONTEXT') not in('CONTEXT','ISSUES') then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_LEAF_REQUEST_INVALID' using errcode='22023';
  end if;
  v_week:=(p_request->>'week_ending')::date;
  select * into c from public.weekly_source_cycles where id=(p_request->>'source_cycle_id')::uuid;
  select * into g from public.weekly_source_groups where id=c.source_group_id and active;
  if c.id is null or g.id is null or v_client is null or v_week is null
    or v_week not between c.finalisation_week_ending-6 and c.finalisation_week_ending
    or (c.scope_client_id is not null and c.scope_client_id<>v_client)
    or (nullif(p_request->>'source_group_id','') is not null and (p_request->>'source_group_id')::uuid<>g.id)
    or not exists(select 1 from public.weekly_source_group_clients membership where membership.source_group_id=g.id
      and membership.client_id=v_client and v_week between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_UNAVAILABLE' using errcode='22023';
  end if;
  select lower(btrim(coalesce(role,'')))='admin' into v_admin from public.tms_users where id=v_actor and is_active;
  perform private.weekly_source_office_authority_v1(v_actor,case when v_admin then 'VIEW_SOURCE_PROGRESS' else 'VIEW_PAY_WORK' end,g.id,v_client,v_week);
  v_today:=(statement_timestamp() at time zone g.timezone)::date;
  if g.source_family<>'ROSTER' then return jsonb_build_object('applies',false,'preserve_native',true); end if;
  v_rule:=private.weekly_source_effective_rule_set_v1(v_client,c.id);
  v_route:=case when v_rule is not null then case when v_rule#>>'{values,invoice_sender}'='AGENCY_INVOICE'
    then 'AGENCY_INVOICE' else 'SELF_BILL' end end;
  if v_route is null and exists(select 1 from public.weekly_source_group_rule_assignments a
    join public.weekly_source_client_policies p on p.source_group_id=a.source_group_id and p.client_id=v_client
    where a.source_group_id=g.id and a.feature_enabled and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
      and v_week between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
      and v_week between p.effective_from and coalesce(p.effective_to,'infinity'::date)
      and p.authority_mode='TIMESHEET_AUTHORITY' and p.document_mode='INVOICE_EVIDENCE_REQUIRED' and not p.self_bill_enabled) then
    v_route:='SIGNED_TIMESHEETS';
  end if;
  if v_route is null then return jsonb_build_object('applies',false,'preserve_native',true); end if;
  -- R-252 retains the client's latest ended calendar week, not its latest
  -- week containing a Timesheet. A completed old week must disappear even
  -- when no worker submitted a newer week. Reuse the native settings owner.
  v_calendar_authority:=private.weekly_source_checklist_client_calendar_v1(v_client,v_today);
  v_latest_week:=(v_calendar_authority->>'latest_ended_week')::date;
  -- Only actual Client authority narrows the witness scan. There is no booked
  -- Client/booking-name or live current-setting substitute for nullable Daily.
  with scoped as materialized (
    select t.*,s.witness,rctx.route_read,tv.status::text validation_status,tv.last_source validation_import_id,tf.processing_status::text processing_status,
      exists(select 1 from private.timesheet_correction_requests r where r.state='WAITING'
        and r.reversed_timesheet_id=t.timesheet_id) correction_waiting
    from public.timesheets t left join public.contracts con on con.id=t.contract_id
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current and tf.timesheet_version=t.version
    cross join lateral(select case when v_route='SIGNED_TIMESHEETS'
      then private.weekly_source_signed_submission_read_context_v1(t.timesheet_id,g.id)
      else jsonb_build_object('eligible',true,'submission_witness',private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id)) end route_read) rctx
    cross join lateral(select rctx.route_read->'submission_witness' witness) s
    left join lateral(select * from public.timesheet_validations v where v.timesheet_id=t.timesheet_id
      order by v.updated_at desc,v.created_at desc,v.id desc limit 1) tv on true
    where t.is_current and t.archived_at_utc is null and t.revoked_at is null
      and coalesce(con.client_id,case when t.contract_id is null and t.sheet_scope::text='DAILY' then tf.client_id end)=v_client
      and t.week_ending_date<v_today
      and exists(select 1 from public.weekly_source_group_clients m where m.source_group_id=g.id and m.client_id=v_client
        and t.week_ending_date between m.valid_from and coalesce(m.valid_to,'infinity'::date))
  ), counted as (
    select * from scoped where (witness->>'actual_submitted')::boolean is true and (route_read->>'eligible')::boolean is true
  ) select max(week_ending_date),count(*) filter(where week_ending_date=v_week)::integer,
    null::integer,
    coalesce(jsonb_agg(jsonb_build_object('timesheet_id',timesheet_id,'witness',witness,'submission_read_context',route_read,
      'ordinary_authorised',authorised_at_server is not null,'validation_status',validation_status,
      'processing_status',processing_status,'correction_waiting',correction_waiting)
      order by timesheet_id) filter(where week_ending_date=v_week),'[]'::jsonb),
    null::integer,
    coalesce((select jsonb_agg(jsonb_build_object('code',reason,'unproved_timesheet_count',number) order by reason)
      from(select witness->>'reason' reason,count(distinct timesheet_id) number from scoped
        where week_ending_date=v_week and witness->>'support_state'='UNPROVED'
        group by witness->>'reason') unproved),'[]'::jsonb)
    into v_latest_timesheet_week,v_count,v_open,v_actual,v_earlier_open,v_unproved from counted;
  v_gate:=v_gate||v_unproved;
  if v_route='SIGNED_TIMESHEETS' then
    -- P1-CHECKLIST-RETENTION1: obtain open work from the actual raw owner
    -- before old-week admission. Discover real submitted weeks/cycles only;
    -- no fixed lookback, fake cycle or R254 information-only admission.
    v_count:=0; v_open:=0; v_earlier_open:=0; v_actual:='[]'::jsonb; v_latest_timesheet_week:=null;
    for v_read_scope in
      with signed_weeks as materialized (
        select distinct t.week_ending_date from public.timesheets t
        cross join lateral(select private.weekly_source_signed_submission_read_context_v1(t.timesheet_id,g.id) context) signed_read
        where signed_read.context->'eligible'='true'::jsonb
          and signed_read.context#>'{submission_witness,actual_submitted}'='true'::jsonb
          and signed_read.context#>>'{submission_witness,client_id}'=v_client::text and t.week_ending_date<v_today
          and exists(select 1 from public.weekly_source_group_clients m where m.source_group_id=g.id and m.client_id=v_client
            and t.week_ending_date between m.valid_from and coalesce(m.valid_to,'infinity'::date))
      ) select week.week_ending_date,owner.cycle_id,owner.owner_count from signed_weeks week
      cross join lateral(select count(*) owner_count,(array_agg(cycle.id order by cycle.id))[1] cycle_id
        from public.weekly_source_cycles cycle where cycle.source_group_id=g.id
          and week.week_ending_date between cycle.finalisation_week_ending-6 and cycle.finalisation_week_ending
          and (cycle.scope_client_id=v_client or (cycle.scope_client_id is null and not exists(
            select 1 from public.weekly_source_cycles preferred where preferred.source_group_id=g.id
              and preferred.scope_client_id=v_client
              and week.week_ending_date between preferred.finalisation_week_ending-6 and preferred.finalisation_week_ending)))) owner
      order by week.week_ending_date
    loop
      if v_read_scope.owner_count<>1 then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_UNAVAILABLE' using errcode='55000'; end if;
      if v_read_scope.week_ending_date=v_week and v_read_scope.cycle_id<>c.id then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
      end if;
      v_cursor:=null; v_revision:=null; v_rows:='[]'::jsonb;
      loop
        v_page:=private.weekly_source_check_only_query_rows_v1(v_read_scope.cycle_id,v_client,
          jsonb_strip_nulls(jsonb_build_object('actor_user_id',v_actor,'source_group_id',g.id,
            'week_ending',v_read_scope.week_ending_date,'limit',100,'cursor',v_cursor)));
        if v_page->'ok' is distinct from 'true'::jsonb then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_READ_UNAVAILABLE' using errcode='55000'; end if;
        if v_revision is not null and v_revision is distinct from v_page->>'read_revision' then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
        end if;
        v_revision:=v_page->>'read_revision'; v_rows:=v_rows||v_page->'rows';
        exit when v_page->'has_more'='false'::jsonb;
        if v_cursor is not distinct from v_page->>'next_cursor' then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_READ_UNAVAILABLE' using errcode='55000'; end if;
        v_cursor:=v_page->>'next_cursor';
      end loop;
      if exists(select 1 from jsonb_array_elements(v_rows) r where r->>'item_kind'='ACTUAL_TIMESHEET'
        and jsonb_typeof(r->'open_read_item') is distinct from 'boolean') then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_READ_UNAVAILABLE' using errcode='55000';
      end if;
      select count(*)::integer,count(*) filter(where r->'open_read_item'='true'::jsonb)::integer
        into v_scope_count,v_scope_open from jsonb_array_elements(v_rows) r
        where r->>'item_kind'='ACTUAL_TIMESHEET' and r#>'{submission_witness,actual_submitted}'='true'::jsonb;
      v_money_scope:=jsonb_build_object('source_group_id',g.id,'source_cycle_id',v_read_scope.cycle_id,
        'client_id',v_client,'week_ending',v_read_scope.week_ending_date);
      v_scope_money:='[]'::jsonb;v_scope_money_facts:='[]'::jsonb;
      for v_money_row in select r from jsonb_array_elements(v_rows) r
        where r->>'item_kind'='ACTUAL_TIMESHEET' and r#>'{submission_witness,actual_submitted}'='true'::jsonb loop
        v_money:=private.weekly_source_checklist_ordinary_next_hold_money_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
        v_scope_money_facts:=v_scope_money_facts||jsonb_build_array(v_money->'native_facts');
        if v_money->'known'='true'::jsonb then
          v_scope_money:=v_scope_money||jsonb_build_array(v_money-'native_facts');
          -- Retain the week using the SAME native financial truth. No R254
          -- information row can enter this authentic counted-Timesheet arm.
          v_scope_open:=v_scope_open+1;
        end if;
        v_legacy:=private.weekly_source_checklist_legacy_hold_review_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
        v_scope_money_facts:=v_scope_money_facts||jsonb_build_array(v_legacy->'native_facts');
        if v_legacy->'review_required'='true'::jsonb then
          v_scope_money:=v_scope_money||jsonb_build_array((v_legacy-'native_facts')||jsonb_build_object('native_financial_kind','LEGACY_REVIEW'));
          v_scope_open:=v_scope_open+1;
        end if;
      end loop;
      if v_read_scope.week_ending_date=v_week then
        v_financial_items:=v_scope_money;v_financial_facts:=v_scope_money_facts;
      end if;
      if v_scope_count>0 then v_latest_timesheet_week:=v_read_scope.week_ending_date; end if;
      if v_scope_count>0 and v_scope_open>0 and v_read_scope.week_ending_date<v_latest_week then v_earlier_open:=v_earlier_open+1; end if;
      if v_read_scope.week_ending_date=v_week then
        v_count:=v_scope_count; v_open:=v_scope_open; v_query_revision:=v_revision;
        select coalesce(jsonb_agg(r order by r->>'timesheet_id'),'[]'::jsonb) into v_actual from jsonb_array_elements(v_rows) r
          where r->>'item_kind'='ACTUAL_TIMESHEET' and r#>'{submission_witness,actual_submitted}'='true'::jsonb;
      end if;
      v_retention_reads:=v_retention_reads||jsonb_build_array(jsonb_build_object('week_ending',v_read_scope.week_ending_date,
        'source_cycle_id',v_read_scope.cycle_id,'read_revision',v_revision,'actual_timesheet_count',v_scope_count,'open_read_items',v_scope_open,
        'native_financial_facts',v_scope_money_facts));
    end loop;
  end if;
  if v_route='AGENCY_INVOICE' then
    v_money_scope:=jsonb_build_object('source_group_id',g.id,'source_cycle_id',c.id,'client_id',v_client,'week_ending',v_week);
    for v_money_row in select value from jsonb_array_elements(v_actual) loop
      v_money:=private.weekly_source_checklist_ordinary_next_hold_money_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
      v_financial_facts:=v_financial_facts||jsonb_build_array(v_money->'native_facts');
      if v_money->'known'='true'::jsonb then v_financial_items:=v_financial_items||jsonb_build_array(v_money-'native_facts'); end if;
      v_legacy:=private.weekly_source_checklist_legacy_hold_review_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
      v_financial_facts:=v_financial_facts||jsonb_build_array(v_legacy->'native_facts');
      if v_legacy->'review_required'='true'::jsonb then
        v_financial_items:=v_financial_items||jsonb_build_array((v_legacy-'native_facts')||jsonb_build_object('native_financial_kind','LEGACY_REVIEW'));end if;
    end loop;
  end if;
  if v_route='SIGNED_TIMESHEETS' and (v_count=0 or (v_week<>v_latest_week and v_open=0)) then
    return jsonb_build_object('applies',true,'admitted',false,'route',v_route,'preserve_native',false,
      'actual_timesheet_count',v_count,'engineering_gates',v_gate);
  end if;
  -- This cell duty is process completion, not cash settlement. Keep every
  -- final scope/revision receipt; a CURRENT client completion is not enough
  -- for separate export pay plus invoice report history.
  select jsonb_build_object(
    'expected_contract_weeks',coalesce((select jsonb_agg(jsonb_build_object('contract_week_id',cw.id,
      'contract_id',cw.contract_id,'status',cw.status,'week_ending',cw.week_ending_date,'timesheet_id',cw.timesheet_id) order by cw.id)
      from public.contract_weeks cw join public.contracts con on con.id=cw.contract_id and con.client_id=v_client
      where cw.week_ending_date=v_week and cw.additional_seq=0 and not cw.is_adjustment
        and cw.status::text in('PLANNED','OPEN','SUBMITTED')),'[]'::jsonb),
    'accepted_checking_uploads',coalesce((select jsonb_agg(jsonb_build_object('upload_id',u.id,'uploaded_at_utc',u.uploaded_at_utc,
      'coverage_start',u.confirmed_coverage_start_local_date,'coverage_end',u.confirmed_coverage_end_local_date,
      'coverage_confirmed_at_utc',u.coverage_confirmed_at_utc,'coverage_confirmed_by',u.coverage_confirmed_by_user_id,
      'covers_whole_week',u.confirmed_coverage_start_local_date<=v_week-6 and u.confirmed_coverage_end_local_date>=v_week,
      'state',u.state,'accepted_attempt_id',attempt.id,'import_use',u.file_metadata_json->'import_use') order by u.uploaded_at_utc desc,u.id)
      from public.weekly_source_uploads u
      join lateral(select a.id from public.weekly_source_upload_attempts a where a.logical_upload_id=u.id and a.result='ACCEPTED'
        order by a.attempted_at_utc desc,a.id desc limit 1) attempt on true
      where u.source_cycle_id=c.id and u.report_scope_id is null and u.state in('CURRENT','SUPERSEDED')
        and upper(coalesce(u.file_metadata_json->>'import_use','CHECKING'))='CHECKING'
        and (c.scope_client_id=v_client or exists(select 1 from public.weekly_source_row_resolutions rr
          where rr.upload_row_id in(select ur.id from public.weekly_source_upload_rows ur where ur.upload_id=u.id)
            and rr.client_id=v_client and rr.mapping_state='RESOLVED'))),'[]'::jsonb),
    'completion_history',coalesce((select jsonb_agg(to_jsonb(completion) order by completion.completion_generation,completion.id)
      from public.weekly_source_client_cycle_completions completion where completion.source_cycle_id=c.id and completion.client_id=v_client),'[]'::jsonb),
    'final_revision_history',coalesce((select jsonb_agg(jsonb_build_object('final_revision_id',fr.id,'authority_scope_kind',fr.authority_scope_kind,
      'report_scope_id',fr.report_scope_id,'state',fr.state,'revision_number',fr.revision_number,'finalised_at_utc',fr.finalised_at_utc,
      'manifest_hash',encode(fr.manifest_hash,'hex'),'client_manifest_id',cm.id,'invoice_state',cm.invoice_state) order by fr.revision_number,fr.id)
      from public.weekly_source_final_revisions fr join public.weekly_source_client_manifests cm on cm.final_revision_id=fr.id and cm.client_id=v_client
      where fr.source_cycle_id=c.id),'[]'::jsonb),
    'report_scopes',coalesce((select jsonb_agg(jsonb_build_object('report_scope_id',rs.id,'self_bill_report_id',rs.self_bill_report_id,
      'version',rs.version,'current_complete_upload_id',rs.current_complete_upload_id,'current_projection_publication_id',rs.current_projection_publication_id,
      'current_final_revision_id',rs.current_final_revision_id) order by rs.id) from public.weekly_source_report_scopes rs
      where rs.source_cycle_id=c.id and rs.client_id=v_client),'[]'::jsonb)
  ) into v_facts;
  if v_week>=v_today then return jsonb_build_object('applies',true,'admitted',false,'route',v_route); end if;
  if v_route<>'SIGNED_TIMESHEETS' and jsonb_array_length(v_facts->'expected_contract_weeks')=0
    and jsonb_array_length(v_facts->'accepted_checking_uploads')=0 and jsonb_array_length(v_facts->'completion_history')=0
    and jsonb_array_length(v_facts->'report_scopes')=0 then
    return jsonb_build_object('applies',true,'admitted',false,'route',v_route);
  end if;
  if v_route='SIGNED_TIMESHEETS' and coalesce(p_request->>'command','CONTEXT')='ISSUES' then
    return private.weekly_source_check_only_query_rows_v1(c.id,v_client,jsonb_build_object('actor_user_id',v_actor,
      'source_group_id',g.id,'week_ending',v_week,'cursor',p_request->'cursor','limit',coalesce(p_request->'limit','50'::jsonb)));
  end if;
  -- A proved WAIT is an approved Source decision, never paid=true. Missing or
  -- partial proof stays unknown; it is not quietly reduced to zero waiting.
  if to_regprocedure('private.weekly_source_protected_wait_witness_v1(uuid,uuid)') is null then
    v_wait:=null; v_gate:=v_gate||jsonb_build_array('QUALIFIED_WAIT_READER_UNAVAILABLE');
  else
    for v_event in select e.id from public.weekly_work_events e where e.client_id=v_client
      and e.first_source_group_id=g.id and e.work_date between v_week-6 and v_week order by e.id loop
      begin
        execute 'select private.weekly_source_protected_wait_witness_v1($1,$2)' into v_wait_fact using v_event.id,c.id;
        if v_wait_fact is not null then
          if v_wait_fact->>'state' is distinct from 'QUALIFIED_WAIT' then raise exception 'WAIT'; end if;
          v_wait:=v_wait+1;
        end if;
      exception when sqlstate '55000' then
        v_wait:=null; v_gate:=v_gate||jsonb_build_array(jsonb_build_object('code','QUALIFIED_WAIT_WITNESS_UNAVAILABLE','work_event_id',v_event.id));
      end;
    end loop;
  end if;
  return jsonb_build_object('applies',true,'admitted',true,'contract','WEEKLY_SOURCE_HEALTHROSTER_CHECKLIST_CONTEXT_V1',
    'scope',jsonb_build_object('source_group_id',g.id,'source_cycle_id',c.id,'client_id',v_client,
      'week_ending',v_week,'cycle_week_ending',c.finalisation_week_ending),'route',v_route,
    'permission_view',case when v_admin then 'ADMIN' else 'PAY_READONLY' end,
    'source_cycle_version',c.version,'effective_rule',v_rule,'native_process_facts',v_facts,
    'checking_due_local_date',v_week+(8-extract(isodow from v_week)::integer),
    'checking_due',v_today>=v_week+(8-extract(isodow from v_week)::integer),
    'actual_timesheet_count',v_count,'actual_timesheet_authority_hash',
      encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_ACTUAL_SET_V1',v_actual),'hex'),
    'most_recent_ended_timesheet_week',v_latest_timesheet_week,
    'most_recent_ended_client_week',v_latest_week,
    'client_week_authority_fingerprint',v_calendar_authority->>'authority_fingerprint','open_timesheet_read_items',v_open,
    'earlier_open_weeks',v_earlier_open,'native_query_read_revision',v_query_revision,
    'qualified_waiting_count',v_wait,'engineering_gates',v_gate,
    'native_financial_items',v_financial_items,'native_financial_facts',v_financial_facts,
    'pay_financial_evidence_required',true,'root_checklist_union_required',true,
    'read_revision',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_HEALTHROSTER_CHECKLIST_CONTEXT_V1',
      jsonb_build_object('scope',p_request-'cursor'-'limit','cycle_version',c.version,'actual',v_actual,'facts',v_facts,
        'latest_week',v_latest_week,'latest_timesheet_week',v_latest_timesheet_week,
        'client_week_authority_fingerprint',v_calendar_authority->>'authority_fingerprint',
        'earlier_open_weeks',v_earlier_open,'actual_count',v_count,'open_read_items',v_open,'retention_reads',v_retention_reads,
        'wait_count',v_wait,'gates',v_gate,'native_financial_items',v_financial_items,'native_financial_facts',v_financial_facts)),'hex'));
end;
$function$;
create or replace function private.weekly_source_check_only_candidate_reminder_subject_v1(
  p_scope jsonb,p_action_id text,p_candidate_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare decision public.import_review_decisions%rowtype; hr public.hr_rows%rowtype;
  preview public.import_review_states%rowtype; cycle public.weekly_source_cycles%rowtype;
  witness jsonb; submitted jsonb:='[]'::jsonb; ts record;
begin
  perform private._weekly_source_settings_assert_request_v1(p_scope,
    array['source_group_id','source_cycle_id','client_id','week_ending'],'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SCOPE_INVALID');
  if jsonb_typeof(p_scope->'source_group_id') is distinct from 'string'
    or jsonb_typeof(p_scope->'source_cycle_id') is distinct from 'string'
    or jsonb_typeof(p_scope->'client_id') is distinct from 'string'
    or jsonb_typeof(p_scope->'week_ending') is distinct from 'string'
    or not pg_input_is_valid(p_scope->>'source_group_id','uuid')
    or not pg_input_is_valid(p_scope->>'source_cycle_id','uuid')
    or not pg_input_is_valid(p_scope->>'client_id','uuid')
    or not pg_input_is_valid(p_scope->>'week_ending','date')
    or p_action_id is null or p_action_id!~'^[0-9a-f]{64}$' or p_candidate_id is null then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SCOPE_INVALID' using errcode='22023';
  end if;
  select c.* into cycle from public.weekly_source_cycles c where c.id=(p_scope->>'source_cycle_id')::uuid
    and c.source_group_id=(p_scope->>'source_group_id')::uuid
    and (c.scope_client_id is null or c.scope_client_id=(p_scope->>'client_id')::uuid)
    and (p_scope->>'week_ending')::date between c.finalisation_week_ending-6 and c.finalisation_week_ending;
  if cycle.id is null then return jsonb_build_object('current_native_action',false,'reason','SCOPE_UNAVAILABLE'); end if;
  select d.* into decision from public.import_review_decisions d join public.hr_imports i on i.id=d.import_id
    and i.pruned_at is null and i.source_system::text in('HEALTHROSTER','HEALTHROSTER_DAILY')
    and (i.client_id is null or i.client_id=d.client_id)
    where d.action_id=p_action_id and d.is_current and d.client_id=(p_scope->>'client_id')::uuid
      and d.candidate_id=p_candidate_id and d.timesheet_id is null and d.action_kind='ADVISORY'
      and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      and d.summary_json->>'reason_code' in('DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED');
  if decision.action_id is null then return jsonb_build_object('current_native_action',false,'reason','NATIVE_ACTION_NO_LONGER_CURRENT'); end if;
  select h.* into hr from public.hr_rows h where h.id=decision.hr_row_id and h.import_id=decision.import_id
    and h.date_local between (p_scope->>'week_ending')::date-6 and (p_scope->>'week_ending')::date;
  select s.* into preview from public.import_review_states s where s.import_id=decision.import_id
    and s.preview_generation=decision.preview_generation and s.superseded_at_utc is null and s.abandoned_at_utc is null;
  if hr.id is null or preview.import_id is null then
    return jsonb_build_object('current_native_action',false,'reason','NATIVE_PREVIEW_NO_LONGER_CURRENT');
  end if;
  -- No planned clocks, workflow-state shortcut or new submission predicate.
  -- A genuine pending Daily immutable submission can precede physical clocks;
  -- that native witness already resolves the submit reminder, while the real
  -- D10 catalogue/ADVISORY remains unchanged for its existing review owner.
  for ts in select t.timesheet_id from public.timesheets t
    where t.week_ending_date=(p_scope->>'week_ending')::date
      and t.sheet_scope::text=case when decision.summary_json->>'reason_code'='DAILY_TIMESHEET_NOT_SUBMITTED' then 'DAILY' else 'WEEKLY' end
    order by t.timesheet_id
  loop
    witness:=private.weekly_source_actual_timesheet_submission_v1(ts.timesheet_id);
    if witness->'actual_submitted'='true'::jsonb and witness->>'candidate_id'=p_candidate_id::text
      and witness->>'client_id'=p_scope->>'client_id'
      and ((decision.summary_json->>'reason_code'='DAILY_TIMESHEET_NOT_SUBMITTED' and witness->>'work_date'=hr.date_local::text)
        or (decision.summary_json->>'reason_code'='WEEKLY_TIMESHEET_NOT_SUBMITTED'
          and decision.contract_id is not null and witness->>'contract_id'=decision.contract_id::text)) then
      submitted:=submitted||jsonb_build_array(witness);
    end if;
  end loop;
  return jsonb_build_object('current_native_action',true,'actual_submission_present',jsonb_array_length(submitted)>0,
    'native_action',jsonb_build_object('action_id',decision.action_id,'candidate_id',decision.candidate_id,
      'import_id',decision.import_id,'hr_row_id',hr.id,'work_date',hr.date_local,'contract_id',decision.contract_id,
      'preview_generation',preview.preview_generation,'state_version',preview.state_version,
      'preview_fingerprint',preview.preview_fingerprint,'evidence_fingerprint',decision.evidence_fingerprint,
      'native_reason_code',decision.summary_json->>'reason_code'),
    'actual_submission_witnesses',submitted);
end;
$function$;
create or replace function private.weekly_source_check_only_candidate_reminder_context_v1(
  p_actor_user_id uuid,p_scope jsonb,p_action_id text
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; cycle public.weekly_source_cycles%rowtype; source_group public.weekly_source_groups%rowtype;
  decision public.import_review_decisions%rowtype; account public.candidate_app_accounts%rowtype;
  link public.candidate_app_global_membership_links%rowtype; settings public.weekly_source_global_settings%rowtype;
  page jsonb; cursor text; revision text; raw jsonb; found_row jsonb; subject jsonb; matches integer:=0;
  links integer; target jsonb; contract_week_id uuid; context jsonb; last_reminder timestamptz; now_at timestamptz:=statement_timestamp();
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_scope,
    array['source_group_id','source_cycle_id','client_id','week_ending'],'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SCOPE_INVALID');
  if jsonb_typeof(p_scope->'source_group_id') is distinct from 'string'
    or jsonb_typeof(p_scope->'source_cycle_id') is distinct from 'string'
    or jsonb_typeof(p_scope->'client_id') is distinct from 'string'
    or jsonb_typeof(p_scope->'week_ending') is distinct from 'string'
    or not pg_input_is_valid(p_scope->>'source_group_id','uuid')
    or not pg_input_is_valid(p_scope->>'source_cycle_id','uuid')
    or not pg_input_is_valid(p_scope->>'client_id','uuid')
    or not pg_input_is_valid(p_scope->>'week_ending','date') then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SCOPE_INVALID' using errcode='22023';
  end if;
  select u.* into actor from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if actor.id is null or actor.role::text is distinct from 'admin' then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_ACTOR_FORBIDDEN' using errcode='42501';
  end if;
  select c.* into cycle from public.weekly_source_cycles c where c.id=(p_scope->>'source_cycle_id')::uuid;
  select g.* into source_group from public.weekly_source_groups g where g.id=cycle.source_group_id and g.active and g.source_family='ROSTER';
  if source_group.id is null or source_group.id::text is distinct from p_scope->>'source_group_id'
    or nullif(p_action_id,'') is null then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SCOPE_INVALID' using errcode='22023'; end if;
  perform private.weekly_source_office_authority_v1(actor.id,'VIEW_SOURCE_PROGRESS',source_group.id,
    (p_scope->>'client_id')::uuid,(p_scope->>'week_ending')::date);
  cursor:=null;
  loop
    page:=private.weekly_source_check_only_query_rows_v1(cycle.id,(p_scope->>'client_id')::uuid,
      jsonb_strip_nulls(jsonb_build_object('actor_user_id',actor.id,'source_group_id',source_group.id,
        'week_ending',p_scope->>'week_ending','limit',100,'cursor',cursor)));
    if page->'ok' is distinct from 'true'::jsonb or (revision is not null and revision is distinct from page->>'read_revision') then
      raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_STALE' using errcode='40001';
    end if;
    revision:=page->>'read_revision';
    for raw in select value from jsonb_array_elements(page->'rows') loop
      if raw->>'item_kind'='SOURCE_INFORMATION' and raw->>'action_id'=p_action_id then found_row:=raw; matches:=matches+1; end if;
    end loop;
    exit when page->'has_more'='false'::jsonb;
    if cursor is not distinct from page->>'next_cursor' then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_STALE' using errcode='40001'; end if;
    cursor:=page->>'next_cursor';
  end loop;
  if matches<>1 or found_row->'counts_for_row_admission' is distinct from 'false'::jsonb
    or found_row->'open_read_item' is distinct from 'false'::jsonb then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SUBJECT_UNAVAILABLE' using errcode='55000';
  end if;
  select d.* into decision from public.import_review_decisions d where d.action_id=p_action_id and d.is_current
    and d.import_id=(found_row->>'query_import_id')::uuid and d.hr_row_id=(found_row->>'hr_row_id')::uuid
    and d.client_id=(p_scope->>'client_id')::uuid and d.candidate_id=(found_row->>'candidate_id')::uuid
    and d.timesheet_id is null and d.action_kind='ADVISORY'
    and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and d.summary_json->>'reason_code' in('DAILY_TIMESHEET_NOT_SUBMITTED','WEEKLY_TIMESHEET_NOT_SUBMITTED')
    and d.evidence_fingerprint=found_row->>'evidence_fingerprint';
  if decision.action_id is null or not exists(select 1 from public.candidates c where c.id=decision.candidate_id and c.active) then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SUBJECT_UNAVAILABLE' using errcode='55000';
  end if;
  subject:=private.weekly_source_check_only_candidate_reminder_subject_v1(p_scope,p_action_id,decision.candidate_id);
  if subject->'current_native_action' is distinct from 'true'::jsonb then
    return jsonb_build_object('available',false,'reason','NATIVE_ACTION_NO_LONGER_CURRENT');
  end if;
  if subject->'actual_submission_present' is distinct from 'false'::jsonb then
    return jsonb_build_object('available',false,'reason','ACTUAL_SUBMISSION_ALREADY_PRESENT');
  end if;
  perform private._candidate_assert_environment(source_group.environment);
  if private._candidate_feature_enabled_v1(source_group.environment,'candidate_notifications') is not true then
    return jsonb_build_object('available',false,'reason','CANDIDATE_NOTIFICATIONS_DISABLED');
  end if;
  select count(*),(array_agg(l.membership_id order by l.membership_id))[1] into links,link.membership_id
    from public.candidate_app_global_membership_links l join public.candidate_app_accounts a on a.id=l.account_id
    where l.candidate_id=decision.candidate_id and l.state='ACTIVE' and a.status='ACTIVE' and a.environment=source_group.environment;
  if links<>1 then return jsonb_build_object('available',false,'reason','CANDIDATE_APP_UNAVAILABLE'); end if;
  select l.* into link from public.candidate_app_global_membership_links l where l.membership_id=link.membership_id;
  select a.* into account from public.candidate_app_accounts a where a.id=link.account_id;
  select s.* into strict settings from public.weekly_source_global_settings s where s.singleton;
  -- Existing target contract only: Daily hub, real unique Contract week, or
  -- native Home. Never fabricate a Timesheet/workflow/Source request identity.
  target:=case when decision.summary_json->>'reason_code'='DAILY_TIMESHEET_NOT_SUBMITTED'
    then jsonb_build_object('type','daily') else '{}'::jsonb end;
  if decision.summary_json->>'reason_code'='WEEKLY_TIMESHEET_NOT_SUBMITTED' and decision.contract_id is not null then
    select case when count(*)=1 then (array_agg(cw.id))[1] end into contract_week_id
      from public.contract_weeks cw join public.contracts c on c.id=cw.contract_id
      where cw.contract_id=decision.contract_id and c.client_id=decision.client_id and c.candidate_id=decision.candidate_id
        and cw.week_ending_date=(p_scope->>'week_ending')::date and cw.additional_seq=0 and not cw.is_adjustment;
    if contract_week_id is not null then target:=jsonb_build_object('contract_week_id',contract_week_id); end if;
  end if;
  select max(n.created_at_utc) into last_reminder from public.candidate_notifications n
    where n.candidate_id=decision.candidate_id and n.event_type='CHECK_ONLY_TIMESHEET_REMINDER'
      and n.template_params->>'client_id'=decision.client_id::text
      and n.template_params->>'week_ending'=p_scope->>'week_ending';
  context:=jsonb_build_object('actor_user_id',actor.id,'scope',p_scope,'item_id',found_row->'item_id',
    'action_id',decision.action_id,'import_id',decision.import_id,'preview_generation',decision.preview_generation,
    'state_version',found_row#>'{query_preview,state_version}','preview_fingerprint',found_row#>'{query_preview,preview_fingerprint}',
    'evidence_fingerprint',decision.evidence_fingerprint,'native_reason_code',decision.summary_json->>'reason_code',
    'native_action',subject->'native_action','candidate_id',decision.candidate_id,'account_id',account.id,'membership_id',link.membership_id,
    'membership_generation',link.membership_generation,'target',target,
    'candidate_manual_reminder_cooldown_seconds',extract(epoch from settings.candidate_manual_reminder_cooldown),
    'next_available_at_utc',last_reminder+settings.candidate_manual_reminder_cooldown);
  return jsonb_build_object('available',last_reminder is null or now_at>=last_reminder+settings.candidate_manual_reminder_cooldown,
    'reason',case when last_reminder is not null and now_at<last_reminder+settings.candidate_manual_reminder_cooldown then 'CANDIDATE_REMINDER_COOLDOWN' end,
    'context',context,'context_hash',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_CONTEXT_V1',
      jsonb_build_object('native_row',found_row,'source_revision',revision,'subject',subject,'actor',to_jsonb(actor),'account',to_jsonb(account),'link',to_jsonb(link),'context',context)),'hex'));
end;
$function$;
create or replace function public.weekly_source_check_only_candidate_reminder_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; actor_before jsonb; scope jsonb; context jsonb; facts jsonb;
  group_page jsonb; parent jsonb; parent_count integer; child_count integer; fresh_count integer;
  notification public.candidate_notifications%rowtype; native_result jsonb; settings public.weekly_source_global_settings%rowtype;
  dedupe text; request_hash text; idempotency_key uuid; now_at timestamptz:=transaction_timestamp(); next_at timestamptz;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','scope','parent_group_key','action_id','parent_proof_hash','candidate_context_hash','idempotency_key'],
    'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_REQUEST_INVALID');
  if jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or jsonb_typeof(p_request->'scope') is distinct from 'object'
    or jsonb_typeof(p_request->'parent_group_key') is distinct from 'string'
    or jsonb_typeof(p_request->'action_id') is distinct from 'string'
    or jsonb_typeof(p_request->'parent_proof_hash') is distinct from 'string'
    or jsonb_typeof(p_request->'candidate_context_hash') is distinct from 'string'
    or jsonb_typeof(p_request->'idempotency_key') is distinct from 'string'
    or p_request->>'parent_group_key'!~'^d10g_[0-9a-f]{64}$'
    or p_request->>'action_id'!~'^[0-9a-f]{64}$' or p_request->>'parent_proof_hash'!~'^[0-9a-f]{64}$'
    or p_request->>'candidate_context_hash'!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_REQUEST_INVALID' using errcode='22023';
  end if;
  scope:=p_request->'scope'; idempotency_key:=(p_request->>'idempotency_key')::uuid;
  perform private._weekly_source_settings_assert_request_v1(scope,
    array['source_group_id','source_cycle_id','client_id','week_ending'],'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SCOPE_INVALID');
  select u.* into actor from public.tms_users u where u.id=(p_request->>'actor_user_id')::uuid and u.is_active;
  if actor.id is null or actor.role::text is distinct from 'admin' then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_ACTOR_FORBIDDEN' using errcode='42501';
  end if;
  actor_before:=to_jsonb(actor);
  perform private.weekly_source_office_authority_v1(actor.id,'VIEW_SOURCE_PROGRESS',
    (scope->>'source_group_id')::uuid,(scope->>'client_id')::uuid,(scope->>'week_ending')::date);
  dedupe:='check-only-d10-candidate-reminder:'||actor.id::text||':'||idempotency_key::text;
  request_hash:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_REQUEST_V1',p_request),'hex');
  -- A committed exact replay returns its real first notification before mutable
  -- action/cooldown consultation. A reused key with changed input is refused.
  select n.* into notification from public.candidate_notifications n where n.dedupe_key=dedupe;
  if notification.id is not null then
    if notification.event_type is distinct from 'CHECK_ONLY_TIMESHEET_REMINDER'
      or notification.template_params->>'request_hash' is distinct from request_hash then
      raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_V1',
      'actor_user_id',actor.id,'scope',scope,'action_id',p_request->>'action_id','idempotency_key',idempotency_key,
      'notification_id',notification.id,'queued',true,'replay',true,
      'next_available_at_utc',notification.template_params->'next_available_at_utc');
  end if;
  facts:=private.weekly_source_check_only_candidate_reminder_context_v1(actor.id,scope,p_request->>'action_id');
  if facts->'available' is distinct from 'true'::jsonb or facts->>'context_hash' is distinct from p_request->>'candidate_context_hash' then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_UNAVAILABLE_OR_STALE' using errcode='40001';
  end if;
  context:=facts->'context';
  -- Native I1 admission before the finite Candidate cooldown/import/account
  -- locks. No TS/TF family or money is written by this purpose-bound intent.
  perform private.weekly_source_pay_query_admit_v2();
  perform pg_advisory_xact_lock(hashtextextended('check-only-d10-candidate-reminder:'||(context->>'candidate_id')||':'
    ||(scope->>'client_id')||':'||(scope->>'week_ending'),0));
  perform pg_advisory_xact_lock(hashtextextended('candidate-federated-membership:'||(context->>'membership_id'),0));
  perform 1 from public.hr_imports i where i.id=(context->>'import_id')::uuid for update;
  perform 1 from public.import_review_states s where s.import_id=(context->>'import_id')::uuid for update;
  perform 1 from public.candidate_app_global_membership_links l where l.membership_id=(context->>'membership_id')::uuid for update;
  perform 1 from public.candidate_app_accounts a where a.id=(context->>'account_id')::uuid for update;
  select n.* into notification from public.candidate_notifications n where n.dedupe_key=dedupe;
  if notification.id is not null then
    if notification.event_type is distinct from 'CHECK_ONLY_TIMESHEET_REMINDER'
      or notification.template_params->>'request_hash' is distinct from request_hash then
      raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_V1',
      'actor_user_id',actor.id,'scope',scope,'action_id',p_request->>'action_id','idempotency_key',idempotency_key,
      'notification_id',notification.id,'queued',true,'replay',true,
      'next_available_at_utc',notification.template_params->'next_available_at_utc');
  end if;
  facts:=private.weekly_source_check_only_candidate_reminder_context_v1(actor.id,scope,p_request->>'action_id');
  if facts->'available' is distinct from 'true'::jsonb or facts->>'context_hash' is distinct from p_request->>'candidate_context_hash' then
    raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_UNAVAILABLE_OR_STALE' using errcode='40001';
  end if;
  context:=facts->'context';
  group_page:=private.weekly_source_check_only_query_groups_v1((scope->>'source_cycle_id')::uuid,(scope->>'client_id')::uuid,
    jsonb_build_object('actor_user_id',actor.id,'source_group_id',scope->>'source_group_id','week_ending',scope->>'week_ending'));
  select count(*),(jsonb_agg(g))->0 into parent_count,parent from jsonb_array_elements(group_page->'groups') g
    where g->>'group_key'=p_request->>'parent_group_key' and g->>'proof_hash'=p_request->>'parent_proof_hash';
  if parent_count<>1 then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_PARENT_STALE' using errcode='40001'; end if;
  select count(*) into child_count from jsonb_array_elements(parent->'children') child
    where child->>'row_key'=p_request->>'action_id' and child#>>'{native_action_binding,action_kind}'='ADVISORY';
  if child_count<>1 then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_PARENT_STALE' using errcode='40001'; end if;
  -- This explicit action reproves through the actual current native D10
  -- catalogue. Its temp-table writes remain outside every GET/read graph.
  -- No second comparison of schedules or workflow submission is implemented.
  select count(*) into fresh_count from public._import_review_action_catalog_core_v1((context->>'import_id')::uuid,
    (context->>'preview_generation')::integer,5000) current_action
    where current_action.action_id=p_request->>'action_id' and current_action.action_kind='ADVISORY'
      and current_action.timesheet_id is null
      and current_action.client_id=(scope->>'client_id')::uuid and current_action.candidate_id=(context->>'candidate_id')::uuid
      and current_action.evidence_fingerprint=context->>'evidence_fingerprint'
      and current_action.summary_json->>'authority_mode'='VALIDATION_ONLY'
      and current_action.summary_json->>'reason_code'=context->>'native_reason_code';
  if fresh_count<>1 then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_SUBJECT_STALE' using errcode='40001'; end if;
  select s.* into strict settings from public.weekly_source_global_settings s where s.singleton;
  next_at:=now_at+settings.candidate_manual_reminder_cooldown;
  native_result:=private._candidate_notification_insert_v1((context->>'account_id')::uuid,(context->>'candidate_id')::uuid,
    null,null,'CHECK_ONLY_TIMESHEET_REMINDER','timesheet_expense_attention','check-only-submit-timesheet-v1',
    jsonb_build_object('request_hash',request_hash,'scope',scope,'client_id',scope->>'client_id','week_ending',scope->>'week_ending',
      'sheet_scope',case when context->>'native_reason_code'='DAILY_TIMESHEET_NOT_SUBMITTED' then 'DAILY' else 'WEEKLY' end,
      'work_date',(select r.date_local from public.hr_rows r where r.id=(select d.hr_row_id from public.import_review_decisions d where d.action_id=p_request->>'action_id')),
      'client_name',(select c.name from public.clients c where c.id=(scope->>'client_id')::uuid),
      'action_id',p_request->>'action_id','native_action',context->'native_action','membership_id',context->'membership_id',
      'next_available_at_utc',next_at),context->'target',dedupe,now_at);
  select u.* into actor from public.tms_users u where u.id=actor.id and u.is_active;
  if to_jsonb(actor) is distinct from actor_before then raise exception 'WEEKLY_SOURCE_CHECK_ONLY_REMINDER_ACTOR_FORBIDDEN' using errcode='42501'; end if;
  perform public._audit_insert('weekly_source_check_only_candidate_reminder',native_result->>'notification_id',
    'WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_STAGED',null,
    jsonb_build_object('scope',scope,'action_id',p_request->>'action_id','idempotency_key',idempotency_key),
    'Candidate check-only reminder staged',actor.id);
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_V1',
    'actor_user_id',actor.id,'scope',scope,'action_id',p_request->>'action_id','idempotency_key',idempotency_key,
    'notification_id',native_result->'notification_id','queued',true,'replay',false,'next_available_at_utc',next_at);
end;
$function$;
create or replace function private.weekly_source_checklist_submission_cycle_admit_v1(
  p_workflow_id uuid,p_environment text,p_candidate_id uuid
) returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  workflow public.candidate_submission_workflows%rowtype;
  target public.timesheets%rowtype; v_client_id uuid; groups integer; family_lock jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  select w.* into workflow from public.candidate_submission_workflows w
    where w.id=p_workflow_id and w.environment=p_environment and w.candidate_id=p_candidate_id;
  if workflow.id is null or workflow.workflow_kind not in('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
    or workflow.target_timesheet_id is null or workflow.cancelled_at_utc is not null then
    return jsonb_build_object('applies',false);
  end if;
  select t.* into target from public.timesheets t where t.timesheet_id=workflow.target_timesheet_id
    and t.is_current and t.archived_at_utc is null and t.revoked_at is null
    and t.status::text not in('REVOKED','ARCHIVED','CANCELLED') and t.line_type::text='HOURS'
    and t.sheet_scope::text in('WEEKLY','DAILY') and t.contract_id is not distinct from workflow.contract_id
    and ((workflow.scope='WEEKLY' and t.week_ending_date=workflow.week_ending_date)
      or (workflow.scope='DAILY' and workflow.week_ending_date is null and workflow.contract_week_id is null
        and workflow.work_date between t.week_ending_date-6 and t.week_ending_date));
  if target.timesheet_id is null or target.correction_kind='WHOLE_TIMESHEET_REVERSAL'
    or exists(select 1 from private.timesheet_correction_requests r where
      (r.state='WAITING' and r.replacement_timesheet_id=target.timesheet_id)
      or (r.state='APPLIED' and r.reversed_timesheet_id=target.timesheet_id)) then
    return jsonb_build_object('applies',false);
  end if;
  if target.contract_id is not null then
    select c.client_id into v_client_id from public.contracts c where c.id=target.contract_id and c.candidate_id=p_candidate_id;
  else
    -- Native Daily may have a null Contract. Require exactly one current TF
    -- owner, including the actual Candidate, rather than inventing a Contract.
    select case when count(*)=1 then (array_agg(f.client_id))[1] end into v_client_id
      from public.timesheets_financials f where f.timesheet_id=target.timesheet_id
        and f.is_current and f.timesheet_version=target.version and f.candidate_id=p_candidate_id;
  end if;
  if v_client_id is null then return jsonb_build_object('applies',false); end if;
  -- This is a potential signed scope only, used to acquire native locks early.
  -- No claim, financial verdict or cycle is manufactured from a draft.
  select count(distinct g.id) into groups from public.weekly_source_groups g
    join public.weekly_source_group_clients m on m.source_group_id=g.id and m.client_id=v_client_id
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id
      and a.feature_enabled and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
    join public.weekly_source_client_policies p on p.source_group_id=g.id and p.client_id=v_client_id
    where g.active and g.environment=p_environment and g.source_family='ROSTER'
      and target.week_ending_date between m.valid_from and coalesce(m.valid_to,'infinity'::date)
      and coalesce(workflow.work_date,target.week_ending_date) between m.valid_from and coalesce(m.valid_to,'infinity'::date)
      and target.week_ending_date between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
      and coalesce(workflow.work_date,target.week_ending_date) between p.effective_from and coalesce(p.effective_to,'infinity'::date)
      and p.authority_mode='TIMESHEET_AUTHORITY' and p.document_mode='INVOICE_EVIDENCE_REQUIRED' and not p.self_bill_enabled;
  if groups=0 then return jsonb_build_object('applies',false); end if;
  if groups<>1 then raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_SIGNED_SCOPE_AMBIGUOUS' using errcode='55000'; end if;
  -- The complete current transition calls here before its workflow/TS/TF row
  -- locks. Post-freeze prepare reacquires these same locks reentrantly, then
  -- takes the existing lifecycle advisory/G1 locks. Preserve I1 -> F1 -> G1.
  perform private.weekly_source_pay_query_admit_v2();
  family_lock:=private.weekly_source_lock_family_rows_v1(array[target.timesheet_id],null);
  if family_lock->'ok' is distinct from 'true'::jsonb then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_FAMILY_UNAVAILABLE' using errcode='55000';
  end if;
  if not exists(select 1 from public.candidate_submission_workflows w where w.id=workflow.id
    and w.environment=workflow.environment and w.candidate_id=workflow.candidate_id
    and w.target_timesheet_id=target.timesheet_id and w.generation=workflow.generation
    and w.updated_at_utc is not distinct from workflow.updated_at_utc) then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WORKFLOW_STALE' using errcode='40001';
  end if;
  return jsonb_build_object('applies',true,'timesheet_id',target.timesheet_id,'workflow_id',workflow.id,
    'workflow_generation',workflow.generation);
end;
$function$;
create or replace function private._weekly_source_settings_ensure_open_cycle_v1(
  p_source_group_id uuid,p_timesheet_id uuid,p_expected_witness_hash bytea,p_actor_user_id uuid default null
) returns uuid language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  group_row public.weekly_source_groups%rowtype; ts public.timesheets%rowtype;
  witnessed jsonb; route_read jsonb; family_lock jsonb; calendar jsonb; client_id uuid;
  own_week date; cycle_week date; cutoff_at timestamptz; cycle_id uuid; owners integer; expected_group jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_source_group_id is null or p_timesheet_id is null or octet_length(p_expected_witness_hash) is distinct from 32 then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_REQUEST_INVALID' using errcode='22023';
  end if;
  select t.* into ts from public.timesheets t where t.timesheet_id=p_timesheet_id;
  witnessed:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
  if witnessed->'actual_submitted' is distinct from 'true'::jsonb then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTUAL_SUBMISSION_REQUIRED' using errcode='55000';
  end if;
  if p_expected_witness_hash is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_V1',witnessed) then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_STALE' using errcode='40001';
  end if;
  route_read:=private.weekly_source_signed_submission_read_context_v1(p_timesheet_id,p_source_group_id);
  if route_read->'eligible' is distinct from 'true'::jsonb then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_SIGNED_SCOPE_REQUIRED' using errcode='55000';
  end if;
  client_id:=(witnessed->>'client_id')::uuid; own_week:=(witnessed->>'week_ending_date')::date;
  if p_actor_user_id is not null then
    perform private.weekly_source_office_authority_v1(p_actor_user_id,'VIEW_SOURCE_PROGRESS',p_source_group_id,client_id,own_week);
  end if;
  select g.* into group_row from public.weekly_source_groups g where g.id=p_source_group_id and g.active and g.source_family='ROSTER';
  if group_row.id is null then raise exception 'WEEKLY_SOURCE_CYCLE_GROUP_NOT_ACTIVE' using errcode='22023'; end if;
  expected_group:=to_jsonb(group_row);
  -- Native Source ordering is I1/F1 before G1. Reuse the actual family owner,
  -- then the existing cycle-lifecycle lock identity and native group lock.
  perform private.weekly_source_pay_query_admit_v2();
  family_lock:=private.weekly_source_lock_family_rows_v1(array[p_timesheet_id],null);
  if family_lock->'ok' is distinct from 'true'::jsonb then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_FAMILY_UNAVAILABLE' using errcode='55000';
  end if;
  perform pg_advisory_xact_lock(hashtextextended('weekly-source-cycle-lifecycle:'||p_source_group_id::text,0));
  select g.* into group_row from public.weekly_source_groups g where g.id=p_source_group_id for update;
  if to_jsonb(group_row) is distinct from expected_group then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_GROUP_STALE' using errcode='40001';
  end if;
  -- Reprove actual membership, effective policy, submitted immutable claim and
  -- current physical member after the same native family/group locks.
  route_read:=private.weekly_source_signed_submission_read_context_v1(p_timesheet_id,p_source_group_id);
  if route_read->'eligible' is distinct from 'true'::jsonb
    or private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_V1',route_read->'submission_witness') is distinct from p_expected_witness_hash then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_STALE' using errcode='40001';
  end if;
  if p_actor_user_id is not null then
    perform private.weekly_source_office_authority_v1(p_actor_user_id,'VIEW_SOURCE_PROGRESS',p_source_group_id,client_id,own_week);
  end if;
  calendar:=private.weekly_source_checklist_client_calendar_v1(client_id,own_week+1);
  if calendar->>'latest_ended_week' is distinct from own_week::text then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_CLIENT_WEEK_UNPROVED' using errcode='55000';
  end if;
  cycle_week:=own_week+((7-extract(dow from own_week)::integer)%7);
  -- Same historical cutoff expression as the original lifecycle owner; this
  -- does not supply a fake p_now_utc or alter created_at_utc's genuine default.
  cutoff_at:=((cycle_week+group_row.cutoff_weekday)+group_row.cutoff_local_time) at time zone group_row.timezone;
  select case when count(*) filter(where c.scope_client_id=client_id)>0
      then count(*) filter(where c.scope_client_id=client_id) else count(*) end,
    (array_agg(c.id order by (c.scope_client_id is not null) desc,c.id))[1] into owners,cycle_id from public.weekly_source_cycles c
    where c.source_group_id=p_source_group_id and c.finalisation_week_ending=cycle_week
      and (c.scope_client_id is null or c.scope_client_id=client_id);
  if owners>1 then raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_OWNER_AMBIGUOUS' using errcode='55000'; end if;
  if owners=1 then return cycle_id; end if;
  -- Metadata only, at the original native unique constraint and lock identity.
  -- No completion, comparison, Timesheet, finance, email, pay or provider work.
  -- Missing historical signed-client bookkeeping is Client-scoped, an existing
  -- native scope. The original owner counts only group-scoped open cycles;
  -- backfill must not introduce MULTIPLE_OPEN_CYCLES into that current view.
  insert into public.weekly_source_cycles(source_group_id,scope_client_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state)
    values(p_source_group_id,client_id,cycle_week,cutoff_at,'OPEN',0,'NONE')
    on conflict on constraint weekly_source_cycles_group_week_client_uq do nothing returning id into cycle_id;
  if cycle_id is null then
    select c.id into strict cycle_id from public.weekly_source_cycles c where c.source_group_id=p_source_group_id
      and c.finalisation_week_ending=cycle_week and c.scope_client_id=client_id;
  end if;
  return cycle_id;
end;
$function$;
create or replace function private.weekly_source_checklist_submission_cycle_prepare_v1(
  p_timesheet_id uuid,p_actor_user_id uuid default null
) returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare witness jsonb; group_id uuid; groups integer; v_client_id uuid; own_week date; cycle_id uuid;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_timesheet_id is null then return jsonb_build_object('applies',false,'reason','NO_TIMESHEET'); end if;
  witness:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
  if witness->'actual_submitted' is distinct from 'true'::jsonb then
    return jsonb_build_object('applies',false,'reason','NO_ACTUAL_SUBMISSION');
  end if;
  v_client_id:=(witness->>'client_id')::uuid; own_week:=(witness->>'week_ending_date')::date;
  select count(distinct g.id),(array_agg(distinct g.id order by g.id))[1] into groups,group_id
    from public.weekly_source_groups g
    join public.weekly_source_group_clients member on member.source_group_id=g.id and member.client_id=v_client_id
      and own_week between member.valid_from and coalesce(member.valid_to,'infinity'::date)
    cross join lateral(select private.weekly_source_signed_submission_read_context_v1(p_timesheet_id,g.id) context) read_context
    where g.active and g.source_family='ROSTER' and read_context.context->'eligible'='true'::jsonb;
  if groups=0 then return jsonb_build_object('applies',false,'reason','NO_SIGNED_GROUP'); end if;
  if groups<>1 then raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_SIGNED_SCOPE_AMBIGUOUS' using errcode='55000'; end if;
  cycle_id:=private._weekly_source_settings_ensure_open_cycle_v1(group_id,p_timesheet_id,
    private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_V1',witness),p_actor_user_id);
  return jsonb_build_object('applies',true,'source_group_id',group_id,'client_id',v_client_id,'week_ending',own_week,
    'source_cycle_id',cycle_id,'timesheet_id',p_timesheet_id,
    'submission_witness_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_V1',witness),'hex'));
end;
$function$;
create or replace function public.weekly_source_submitted_timesheet_prepare_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; timesheet_id uuid; result jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','timesheet_id'],'WEEKLY_SOURCE_SUBMISSION_CYCLE_REQUEST_INVALID');
  if jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or jsonb_typeof(p_request->'timesheet_id') is distinct from 'string' then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_REQUEST_INVALID' using errcode='22023';
  end if;
  timesheet_id:=(p_request->>'timesheet_id')::uuid;
  select u.* into actor from public.tms_users u where u.id=(p_request->>'actor_user_id')::uuid and u.is_active;
  if actor.id is null or actor.role::text is distinct from 'admin' then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTOR_FORBIDDEN' using errcode='42501';
  end if;
  -- The existing dispatcher resolves fresh actual submission, Client/date,
  -- dated signed membership/policy, and the same native I1/F1/lifecycle/G1.
  -- Ordinary, planned/draft/expense-only/P0-only and non-signed rows are noops.
  result:=private.weekly_source_checklist_submission_cycle_prepare_v1(timesheet_id,actor.id);
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_SUBMITTED_TIMESHEET_PREPARE_V1',
    'actor_user_id',actor.id,'timesheet_id',timesheet_id,'result',result);
end;
$function$;
create or replace function private.weekly_source_checklist_historical_submission_descriptor_v1(p_timesheet_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare witness jsonb; route_read jsonb; g public.weekly_source_groups%rowtype;
  v_client_id uuid; own_week date; work_date date; cycle_week date; group_id uuid; groups integer;
  calendar jsonb; dated_calendar jsonb; membership jsonb; policies jsonb; assignments jsonb;
  cycle_id uuid; owners integer; admission jsonb; witness_identity jsonb;
begin
  witness:=private.weekly_source_actual_timesheet_submission_v1(p_timesheet_id);
  if witness->'actual_submitted' is distinct from 'true'::jsonb then return null; end if;
  v_client_id:=(witness->>'client_id')::uuid; own_week:=(witness->>'week_ending_date')::date;
  work_date:=(witness->>'work_date')::date;
  select count(distinct source.id),(array_agg(distinct source.id order by source.id))[1] into groups,group_id
    from public.weekly_source_groups source
    cross join lateral(select private.weekly_source_signed_submission_read_context_v1(p_timesheet_id,source.id) context) signed_read
    where source.active and source.source_family='ROSTER' and signed_read.context->'eligible'='true'::jsonb;
  if groups=0 then return null; end if;
  if groups<>1 then raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_SIGNED_SCOPE_AMBIGUOUS' using errcode='55000'; end if;
  select source.* into g from public.weekly_source_groups source where source.id=group_id;
  calendar:=private.weekly_source_checklist_client_calendar_v1(v_client_id,(statement_timestamp() at time zone g.timezone)::date);
  if own_week>(calendar->>'latest_ended_week')::date then return null; end if;
  dated_calendar:=private.weekly_source_checklist_client_calendar_v1(v_client_id,own_week+1);
  if dated_calendar->>'latest_ended_week' is distinct from own_week::text then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_CLIENT_WEEK_UNPROVED' using errcode='55000';
  end if;
  route_read:=private.weekly_source_signed_submission_read_context_v1(p_timesheet_id,group_id);
  select coalesce(jsonb_agg(to_jsonb(m) order by to_jsonb(m)::text),'[]'::jsonb) into membership
    from public.weekly_source_group_clients m where m.source_group_id=group_id and m.client_id=v_client_id
      and own_week between m.valid_from and coalesce(m.valid_to,'infinity'::date)
      and work_date between m.valid_from and coalesce(m.valid_to,'infinity'::date);
  select coalesce(jsonb_agg(to_jsonb(p) order by to_jsonb(p)::text),'[]'::jsonb) into policies
    from public.weekly_source_client_policies p where p.source_group_id=group_id and p.client_id=v_client_id
      and work_date between p.effective_from and coalesce(p.effective_to,'infinity'::date)
      and p.authority_mode='TIMESHEET_AUTHORITY' and p.document_mode='INVOICE_EVIDENCE_REQUIRED' and not p.self_bill_enabled;
  select coalesce(jsonb_agg(to_jsonb(a) order by to_jsonb(a)::text),'[]'::jsonb) into assignments
    from public.weekly_source_group_rule_assignments a where a.source_group_id=group_id and a.feature_enabled
      and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
      and own_week between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date);
  if route_read->'eligible' is distinct from 'true'::jsonb or jsonb_array_length(membership)=0
    or jsonb_array_length(policies)=0 or jsonb_array_length(assignments)=0 then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_STALE' using errcode='40001';
  end if;
  -- Seal real dated admission independently of mutable cycle bookkeeping so
  -- separate Timesheets in the same week may safely reuse the first receipt.
  admission:=jsonb_build_object('submission_read',route_read,'source_group',to_jsonb(g),
    'membership',membership,'policies',policies,'assignments',assignments,
    'client_calendar',calendar,'dated_client_calendar',dated_calendar);
  cycle_week:=own_week+((7-extract(dow from own_week)::integer)%7);
  select case when count(*) filter(where c.scope_client_id=v_client_id)>0
      then count(*) filter(where c.scope_client_id=v_client_id) else count(*) end,
    (array_agg(c.id order by (c.scope_client_id is not null) desc,c.id))[1] into owners,cycle_id
    from public.weekly_source_cycles c where c.source_group_id=group_id and c.finalisation_week_ending=cycle_week
      and (c.scope_client_id is null or c.scope_client_id=v_client_id);
  if owners>1 then raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_OWNER_AMBIGUOUS' using errcode='55000'; end if;
  witness_identity:=jsonb_build_object('authority',witness->>'authority',
    'workflow_id',witness->'workflow_id','workflow_generation',witness->'workflow_generation',
    'hours_component_id',witness->'hours_component_id','candidate_signature_component_id',witness->'candidate_signature_component_id',
    'document_version_id',witness->'document_version_id');
  return jsonb_build_object('timesheet_id',p_timesheet_id,'timesheet_version',witness->'timesheet_version',
    'source_group_id',group_id,'client_id',v_client_id,'week_ending',own_week,'native_cycle_week_ending',cycle_week,
    'source_cycle_id',cycle_id,'witness_identity',witness_identity,
    'submission_witness_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_V1',witness),'hex'),
    'admission_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_HISTORICAL_SUBMISSION_ADMISSION_V1',admission),'hex'));
end;
$function$;
create or replace function public.weekly_source_checklist_submission_manifest_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; all_rows jsonb; missing jsonb; selected jsonb; entry jsonb;
  binding text; revision text; token jsonb; after_id uuid; last_id text; more boolean; page_limit integer:=100;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','cursor','limit'],'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REQUEST_INVALID');
  if jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or (p_request?'cursor' and jsonb_typeof(p_request->'cursor') is distinct from 'string')
    or (p_request?'limit' and (jsonb_typeof(p_request->'limit') is distinct from 'number'
      or not pg_input_is_valid(p_request->>'limit','integer'))) then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REQUEST_INVALID' using errcode='22023';
  end if;
  page_limit:=coalesce((p_request->>'limit')::integer,100);
  if page_limit not between 1 and 100 then raise exception 'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REQUEST_INVALID' using errcode='22023'; end if;
  if p_request?'cursor' and length(p_request->>'cursor') not between 1 and 4096 then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REQUEST_INVALID' using errcode='22023';
  end if;
  select u.* into actor from public.tms_users u where u.id=(p_request->>'actor_user_id')::uuid and u.is_active;
  if actor.id is null or actor.role::text is distinct from 'admin' then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTOR_FORBIDDEN' using errcode='42501';
  end if;
  with candidates as materialized (
    select private.weekly_source_checklist_historical_submission_descriptor_v1(t.timesheet_id) descriptor
      from public.timesheets t where t.is_current and t.archived_at_utc is null and t.revoked_at is null
        and t.line_type::text='HOURS' and t.sheet_scope::text in('DAILY','WEEKLY')
  ) select coalesce(jsonb_agg(descriptor order by descriptor->>'timesheet_id') filter(where descriptor is not null),'[]'::jsonb)
    into all_rows from candidates;
  for entry in select r from jsonb_array_elements(all_rows) r loop
    perform private.weekly_source_office_authority_v1(actor.id,'VIEW_SOURCE_PROGRESS',
      (entry->>'source_group_id')::uuid,(entry->>'client_id')::uuid,(entry->>'week_ending')::date);
  end loop;
  binding:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_MANIFEST_BINDING_V1',
    jsonb_build_object('actor',to_jsonb(actor),'order','ACTUAL_TIMESHEET_UUID_ASC')),'hex');
  revision:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_SUBMISSION_MANIFEST_REVISION_V1',all_rows),'hex');
  select coalesce(jsonb_agg(r order by r->>'timesheet_id'),'[]'::jsonb) into missing
    from jsonb_array_elements(all_rows) r where r->'source_cycle_id'='null'::jsonb;
  if p_request?'cursor' then
    begin
      token:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      perform private._weekly_source_settings_assert_request_v1(token,array['contract','binding','read_revision','after'],'CURSOR');
      if token->>'contract' is distinct from 'WEEKLY_SOURCE_SUBMISSION_MANIFEST_CURSOR_V1'
        or token->>'binding' is distinct from binding or token->>'read_revision' is distinct from revision
        or jsonb_typeof(token->'after') is distinct from 'string'
        or not exists(select 1 from jsonb_array_elements(missing) r where r->>'timesheet_id'=token->>'after') then raise exception 'cursor'; end if;
      after_id:=(token->>'after')::uuid;
    exception when others then raise exception 'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REFRESH_REQUIRED' using errcode='40001'; end;
  end if;
  select coalesce(jsonb_agg(r order by r->>'timesheet_id'),'[]'::jsonb),count(*)>page_limit into selected,more
    from(select r from jsonb_array_elements(missing) r where after_id is null or (r->>'timesheet_id')::uuid>after_id
      order by (r->>'timesheet_id')::uuid limit page_limit+1) page;
  if more then selected:=selected-page_limit; end if;
  last_id:=selected->(jsonb_array_length(selected)-1)->>'timesheet_id';
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_SUBMISSION_MANIFEST_V1','actor_user_id',actor.id,'permission_view','ADMIN',
    'binding',binding,'read_revision',revision,'eligible_count',jsonb_array_length(all_rows),'missing_count',jsonb_array_length(missing),
    'rows',selected,'has_more',more,'next_cursor',case when more then replace(encode(convert_to(jsonb_build_object(
      'contract','WEEKLY_SOURCE_SUBMISSION_MANIFEST_CURSOR_V1','binding',binding,'read_revision',revision,'after',last_id)::text,'UTF8'),'base64'),chr(10),'') end);
end;
$function$;
create or replace function public.weekly_source_checklist_historical_prepare_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare actor public.tms_users%rowtype; expected_actor jsonb; descriptor jsonb; after_descriptor jsonb; result jsonb;
  timesheet_id uuid; expected_witness text; expected_admission text;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','timesheet_id','submission_witness_sha256','admission_sha256'],'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REQUEST_INVALID');
  if jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
    or jsonb_typeof(p_request->'timesheet_id') is distinct from 'string'
    or jsonb_typeof(p_request->'submission_witness_sha256') is distinct from 'string'
    or jsonb_typeof(p_request->'admission_sha256') is distinct from 'string'
    or p_request->>'submission_witness_sha256'!~'^[0-9a-f]{64}$' or p_request->>'admission_sha256'!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_MANIFEST_REQUEST_INVALID' using errcode='22023';
  end if;
  timesheet_id:=(p_request->>'timesheet_id')::uuid;
  expected_witness:=p_request->>'submission_witness_sha256'; expected_admission:=p_request->>'admission_sha256';
  select u.* into actor from public.tms_users u where u.id=(p_request->>'actor_user_id')::uuid and u.is_active;
  if actor.id is null or actor.role::text is distinct from 'admin' then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTOR_FORBIDDEN' using errcode='42501';
  end if;
  expected_actor:=to_jsonb(actor);
  descriptor:=private.weekly_source_checklist_historical_submission_descriptor_v1(timesheet_id);
  if descriptor is null or descriptor->>'submission_witness_sha256' is distinct from expected_witness
    or descriptor->>'admission_sha256' is distinct from expected_admission then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_STALE' using errcode='40001';
  end if;
  -- Reuse the complete established lifecycle owner: genuine witness/dated
  -- membership/client calendar, I1 -> F1 -> lifecycle advisory/G1, native
  -- Sunday/cutoff/default created_at and unique/CAS. No business save here.
  result:=private.weekly_source_checklist_submission_cycle_prepare_v1(timesheet_id,actor.id);
  after_descriptor:=private.weekly_source_checklist_historical_submission_descriptor_v1(timesheet_id);
  if result->'applies' is distinct from 'true'::jsonb or after_descriptor is null
    or after_descriptor->>'submission_witness_sha256' is distinct from expected_witness
    or after_descriptor->>'admission_sha256' is distinct from expected_admission
    or after_descriptor->'source_cycle_id' is distinct from result->'source_cycle_id' then
    -- Exception rolls this metadata-only transaction back; earlier distinct
    -- Timesheet transactions remain accurately recorded by the setup caller.
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_STALE' using errcode='40001';
  end if;
  select u.* into actor from public.tms_users u where u.id=actor.id and u.is_active;
  if to_jsonb(actor) is distinct from expected_actor then
    raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTOR_FORBIDDEN' using errcode='42501';
  end if;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_HISTORICAL_SUBMISSION_PREPARE_V1',
    'actor_user_id',actor.id,'timesheet_id',timesheet_id,'metadata_only',true,'descriptor',after_descriptor);
end;
$function$;
create or replace function public.weekly_source_checklist_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  actor public.tms_users%rowtype; command text; view text; sort text; direction text; request jsonb;
  scope record; own_week date; local_today date; calendar jsonb; leaf jsonb; process jsonb; rule jsonb;
  route text; row_scope jsonb; raw_page jsonb; raw_cursor text; raw_rows jsonb; raw jsonb; witness jsonb;
  rows jsonb:='[]'::jsonb; items jsonb:='[]'::jsonb; row_items jsonb; fingerprints jsonb:='[]'::jsonb;
  row_id text; checked boolean; completed boolean; pay_complete boolean; checked_at timestamptz;
  checked_text text; complete_text text; latest_week date; earlier integer; actual_count integer;
  open_count integer; pay_count integer; checked_count integer; source_count integer; query_count integer;
  checking_upload uuid; completion public.weekly_source_client_cycle_completions%rowtype;
  action jsonb; reason text; item jsonb; correction jsonb; pending jsonb; signed jsonb; authority jsonb;
  subject text; pay_subject boolean; rank integer; status jsonb; sort_name text; native_issue jsonb;
  source_native record; source_witness jsonb; seen_items text[];
  native_workspace jsonb; tracker_action jsonb; finalise_link jsonb;
  agency_native record; agency_facts jsonb; comparison_accepted boolean; query_unsent boolean;
  report_bindings jsonb; needs_export boolean;
  hold_facts jsonb; source_money jsonb; item_hold jsonb; financial_facts jsonb; native_root uuid; native_roots integer;
  d10_groups jsonb; query_parent jsonb; query_targets jsonb; invoice_blocks jsonb;
  unknown_hold constant jsonb:='{"holds_pay":null,"holds_pay_scope":null,"withheld_amount_pence":null,"holds_invoice":null,"pending_decision":null,"authority":{"owner":"UNAVAILABLE","hash":null,"withholding_known":false}}';
  binding text; revision text; records jsonb; result jsonb; primary_key text; sort_key jsonb;
  empty_cell jsonb; admin_actions jsonb; no_work boolean; no_work_line text; limit_rows integer;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','command','source_group_id','client_id','source_cycle_id','week_ending','cursor','anchor','sort','direction','limit'],
    'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
  command:=coalesce(p_request->>'command','CHECKLIST');
  sort:=coalesce(p_request->>'sort',case when command='CHECKLIST' then 'week_ending' else 'worker' end);
  direction:=coalesce(p_request->>'direction',case when command='CHECKLIST' then 'DESC' else 'ASC' end);
  if command not in('CHECKLIST','CHECKLIST_ISSUES') or direction not in('ASC','DESC')
    or (command='CHECKLIST' and sort not in('week_ending','client','group','checked','completed'))
    or (command='CHECKLIST_ISSUES' and (sort<>'worker' or direction<>'ASC'))
    or coalesce(p_request->>'limit','50') !~ '^[1-9][0-9]{0,2}$'
    or (p_request ? 'cursor' and p_request ? 'anchor') then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID' using errcode='22023';
  end if;
  limit_rows:=coalesce((p_request->>'limit')::integer,50);
  if limit_rows not between 1 and 100 then raise exception 'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID' using errcode='22023'; end if;
  for reason in select key from jsonb_object_keys(p_request) key where key in('actor_user_id','source_group_id','client_id','source_cycle_id') loop
    if jsonb_typeof(p_request->reason) is distinct from 'string' or not pg_input_is_valid(p_request->>reason,'uuid') then
      raise exception 'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID' using errcode='22023';
    end if;
  end loop;
  if (p_request ? 'week_ending' and (coalesce(p_request->>'week_ending','') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      or not pg_input_is_valid(p_request->>'week_ending','date')))
    or (command='CHECKLIST_ISSUES' and not(p_request ?& array['source_group_id','client_id','source_cycle_id','week_ending'])) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID' using errcode='22023';
  end if;
  for reason in select key from jsonb_object_keys(p_request) key where key in('cursor','anchor') loop
    if jsonb_typeof(p_request->reason) is distinct from 'string' or length(p_request->>reason) not between 1 and 4096
      or p_request->>reason ~ '[[:cntrl:]]' then raise exception 'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID' using errcode='22023'; end if;
  end loop;
  select a.* into actor from public.tms_users a where a.id=(p_request->>'actor_user_id')::uuid;
  if actor.id is null or actor.is_active is distinct from true then
    raise exception 'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' using errcode='42501';
  end if;
  view:=case when lower(btrim(coalesce(actor.role,'')))='admin' then 'ADMIN' else 'PAY_READONLY' end;
  if view='PAY_READONLY' and private.weekly_source_pay_actor_allowed_v1(actor.id) is distinct from true then
    raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501';
  end if;
  request:=p_request||jsonb_build_object('command',command,'sort',sort,'direction',direction,'limit',limit_rows);
  binding:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_BINDING_V1',jsonb_build_object(
    'actor_user_id',actor.id,'permission_view',view,'role',actor.role,'payment_authoriser',actor.payment_authoriser,
    'payment_golden_key',actor.payment_golden_key,'scope',request-'cursor'-'anchor'-'limit')),'hex');
  -- An actual signed submission must not disappear because a real bookkeeping
  -- cycle is absent. Reads cannot create one; report that dependency explicitly.
  if exists(select 1 from public.timesheets t
    cross join lateral(select private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id) witness) submitted
    join public.weekly_source_group_clients m on m.client_id=(submitted.witness->>'client_id')::uuid
      and t.week_ending_date between m.valid_from and coalesce(m.valid_to,'infinity'::date)
    join public.weekly_source_groups g on g.id=m.source_group_id and g.active
    cross join lateral(select private.weekly_source_signed_submission_read_context_v1(t.timesheet_id,g.id) context) signed_read
    where (submitted.witness->>'actual_submitted')::boolean is true and (signed_read.context->>'eligible')::boolean is true
      and t.week_ending_date<(statement_timestamp() at time zone g.timezone)::date
      and not exists(select 1 from public.weekly_source_cycles c where c.source_group_id=g.id
        and t.week_ending_date between c.finalisation_week_ending-6 and c.finalisation_week_ending
        and (c.scope_client_id is null or c.scope_client_id=m.client_id))) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_UNAVAILABLE' using errcode='55000';
  end if;
  -- Discover every actual own week and every existing native cycle. There is
  -- no fixed historical window. Prefer the native Client-specific cycle, as
  -- the native scope owner does, but refuse ambiguous equal-priority authority.
  for scope in
    with source_members as materialized (
      select g.id group_id,g.display_name,g.source_family,g.timezone,m.client_id,cl.name client_name,m.valid_from,m.valid_to
      from public.weekly_source_groups g join public.weekly_source_group_clients m on m.source_group_id=g.id
      join public.clients cl on cl.id=m.client_id where g.active
    ), actual as materialized (
      select t.timesheet_id,t.week_ending_date,(s.witness->>'client_id')::uuid client_id
      from public.timesheets t cross join lateral(select private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id) witness) s
      where (s.witness->>'actual_submitted')::boolean is true
    ), candidates as (
      select m.*,c.id cycle_id,c.scope_client_id,c.finalisation_week_ending,c.version cycle_version,
        c.state cycle_state,calendar.value calendar,own.value week_ending
      from source_members m join public.weekly_source_cycles c on c.source_group_id=m.group_id
        and (c.scope_client_id is null or c.scope_client_id=m.client_id)
      cross join lateral(select private.weekly_source_checklist_client_calendar_v1(m.client_id,
        (statement_timestamp() at time zone m.timezone)::date) value) calendar
      cross join lateral(
        select distinct actual.week_ending_date value from actual where actual.client_id=m.client_id
          and actual.week_ending_date between c.finalisation_week_ending-6 and c.finalisation_week_ending
        union select c.finalisation_week_ending-((extract(dow from c.finalisation_week_ending)::integer-
          (calendar.value->>'week_ending_weekday')::integer+7)%7)
      ) own
      where own.value between m.valid_from and coalesce(m.valid_to,'infinity'::date)
        and own.value<(statement_timestamp() at time zone m.timezone)::date
        and exists(select 1 from public.weekly_source_client_policies policy where policy.source_group_id=m.group_id
          and policy.client_id=m.client_id and own.value between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)
          and (policy.authority_mode='SOURCE_AUTHORITY' or (policy.authority_mode='TIMESHEET_AUTHORITY'
            and policy.document_mode='INVOICE_EVIDENCE_REQUIRED' and not policy.self_bill_enabled)))
    ), priority as (
      select candidates.*,max(case when scope_client_id is not null then 1 else 0 end)
        over(partition by group_id,client_id,week_ending) best from candidates
    ), chosen as (
      select *,count(*) over(partition by group_id,client_id,week_ending) owner_count
      from priority where (case when scope_client_id is not null then 1 else 0 end)=best
    ) select * from chosen order by group_id,client_id,week_ending,cycle_id
  loop
    if scope.owner_count<>1 then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_SCOPE_AMBIGUOUS' using errcode='55000'; end if;
    own_week:=scope.week_ending; calendar:=scope.calendar; latest_week:=(calendar->>'latest_ended_week')::date;
    local_today:=(statement_timestamp() at time zone scope.timezone)::date;
    row_scope:=jsonb_build_object('source_group_id',scope.group_id,'client_id',scope.client_id,'source_cycle_id',scope.cycle_id,'week_ending',own_week);
    row_id:=scope.group_id::text||':'||scope.client_id::text||':'||own_week::text;
    perform private.weekly_source_office_authority_v1(actor.id,case when view='ADMIN' then 'VIEW_SOURCE_PROGRESS' else 'VIEW_PAY_WORK' end,
      scope.group_id,scope.client_id,own_week);
    leaf:=private.weekly_source_healthroster_checklist_leaf_v1(row_scope||jsonb_build_object('actor_user_id',actor.id));
    if leaf->'applies'='true'::jsonb and leaf->'admitted' is distinct from 'true'::jsonb then continue; end if;
    route:=case when leaf->'applies'='true'::jsonb then leaf->>'route' else 'NATIVE' end;
    if route='NATIVE' and scope.source_family<>'NHSP'
      and private.weekly_source_is_import_authoritative_v1(scope.client_id,null,own_week) is distinct from true then continue; end if;
    actual_count:=coalesce((leaf->>'actual_timesheet_count')::integer,0); earlier:=coalesce((leaf->>'earlier_open_weeks')::integer,0);
    checked_count:=0; open_count:=0; pay_count:=0; row_items:='[]'::jsonb; raw_rows:='[]'::jsonb; financial_facts:='[]'::jsonb;
    checked_at:=null; checking_upload:=null; admin_actions:='[]'::jsonb; finalise_link:=null; query_targets:='[]'::jsonb;
    if route in('SIGNED_TIMESHEETS','AGENCY_INVOICE') then
      d10_groups:=private.weekly_source_check_only_query_groups_v1(scope.cycle_id,scope.client_id,
        jsonb_build_object('actor_user_id',actor.id,'source_group_id',scope.group_id,'week_ending',own_week));
      query_targets:=d10_groups->'item_targets';
      if route='SIGNED_TIMESHEETS' and d10_groups->>'source_read_revision' is distinct from leaf->>'native_query_read_revision' then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
      end if;
      fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_d10_groups',d10_groups));
    end if;
    if route='SIGNED_TIMESHEETS' then
      raw_cursor:=null;
      loop
        raw_page:=private.weekly_source_check_only_query_rows_v1(scope.cycle_id,scope.client_id,
          jsonb_strip_nulls(jsonb_build_object('actor_user_id',actor.id,'source_group_id',scope.group_id,'week_ending',own_week,'limit',100,'cursor',raw_cursor)));
        if raw_page->'ok' is distinct from 'true'::jsonb then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_READ_UNAVAILABLE' using errcode='55000'; end if;
        if raw_page->>'read_revision' is distinct from d10_groups->>'source_read_revision' then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
        end if;
        raw_rows:=raw_rows||(raw_page->'rows');
        exit when raw_page->'has_more'='false'::jsonb;
        if raw_cursor is not distinct from raw_page->>'next_cursor' then raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_READ_UNAVAILABLE' using errcode='55000'; end if;
        raw_cursor:=raw_page->>'next_cursor';
      end loop;
      for raw in select value from jsonb_array_elements(raw_rows) loop
        action:=null; correction:=null; pending:=null; pay_subject:=false; signed:=null; item_hold:=unknown_hold; query_parent:=null;
        select value into query_parent from jsonb_array_elements(query_targets) where value->>'item_id'=raw->>'item_id';
        if raw->>'item_kind'='SOURCE_INFORMATION' then
          hold_facts:=private.weekly_source_checklist_information_hold_facts_v1(raw,row_scope,actor.id);
          item_hold:=hold_facts->'hold'; financial_facts:=financial_facts||jsonb_build_array(hold_facts->'native_facts');
          reason:=raw->>'native_reason_code'; subject:='INFORMATION'; rank:=90;
          status:=jsonb_build_object('text','No hold','tone','warning','subtext',coalesce(raw#>>'{summary,reason}',reason),'subtone','muted');
          item:=jsonb_build_object('source_information_witness',jsonb_build_object('native_action_id',raw->'action_id',
            'evidence_fingerprint',raw->'evidence_fingerprint','import_id',raw->'query_import_id','hr_row_id',raw->'hr_row_id',
            'existing_row_id',coalesce(query_parent->'parent_group_key',raw->'item_id'),'counts_for_row_admission',false),'counted_timesheet_id',null);
          if reason in('WEEKLY_TIMESHEET_NOT_SUBMITTED','DAILY_TIMESHEET_NOT_SUBMITTED') and query_parent is not null then
            action:=jsonb_build_object('kind','QUERIES','label','View missing Timesheet in Queries ›','native_allowed',true,
              'target',row_scope-'week_ending'||jsonb_build_object('row_key',query_parent->>'parent_group_key'));
          end if;
        else
          witness:=raw->'submission_witness';
          if jsonb_typeof(raw->'open_read_item') is distinct from 'boolean' then
            raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_READ_UNAVAILABLE' using errcode='55000';
          end if;
          hold_facts:=private.weekly_source_checklist_timesheet_hold_facts_v1((raw->>'timesheet_id')::uuid,row_scope,actor.id);
          item_hold:=hold_facts->'hold'; financial_facts:=financial_facts||jsonb_build_array(hold_facts->'native_facts');
          if raw#>>'{validation,last_source}' is not null and exists(select 1 from public.hr_imports hi
            join public.import_review_states st on st.import_id=hi.id and st.applied_at_utc is not null
            where hi.id=(raw#>>'{validation,last_source}')::uuid and hi.pruned_at is null
              and hi.source_system::text in('HEALTHROSTER','HEALTHROSTER_DAILY')) then
            checked_count:=checked_count+1;
          end if;
          if raw->'open_read_item'='false'::jsonb then continue; end if;
          if raw->'correction_owns_read_item'='true'::jsonb then
            select jsonb_build_object('request_id',r.id,'state',r.state,'state_revision',r.state_revision,
              'original_timesheet_id',r.reversed_timesheet_id,'replacement_timesheet_id',r.replacement_timesheet_id,
              'reversal_timesheet_id',r.reversal_timesheet_id) into correction from private.timesheet_correction_requests r
              where r.id=(raw#>>'{correction,request_id}')::uuid and r.state='WAITING' and r.reversed_timesheet_id=(raw->>'timesheet_id')::uuid;
            if correction is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001'; end if;
            begin signed:=private.timesheet_correction_signed_witness_v1((correction->>'replacement_timesheet_id')::uuid,false);
            exception when sqlstate '55000' then signed:=null; end;
            reason:=case when signed is not null then 'SIGNED_CORRECTION_RECEIVED' else 'CORRECTION_WAITING' end;
            pay_subject:=signed is not null; subject:=case when pay_subject then 'PAY_AND_INVOICE' else 'INVOICE_ONLY' end; rank:=10;
            status:=jsonb_build_object('text','Decision needed','tone','warning','subtext',case when signed is not null
              then 'Signed correction received' when raw#>>'{correction,worker_submitted_at_utc}' is not null
              then 'Correction with the manager' else 'Correction sent to the worker' end,'subtone','muted');
            action:=jsonb_build_object('kind','TIMESHEET','label','View correction in Timesheet ›','native_allowed',true,
              'target',jsonb_build_object('timesheet_id',correction->'replacement_timesheet_id'));
          elsif raw#>>'{submission_read_context,route_state}'='SUBMITTED_PENDING_NATIVE_MATERIALISATION' then
            reason:='NATIVE_DAILY_MATERIALISATION_PENDING'; subject:='PAY_AND_INVOICE'; pay_subject:=true; rank:=15;
            status:=jsonb_build_object('text','Decision needed','tone','warning','subtext','Submitted Timesheet · native materialisation pending','subtone','muted');
            pending:=jsonb_build_object('state','SUBMITTED_PENDING_NATIVE_MATERIALISATION','timesheet_id',raw->'timesheet_id',
              'client_id',witness->'client_id','candidate_id',witness->'candidate_id','work_date',raw->'work_date',
              'workflow_id',witness->'workflow_id','workflow_generation',witness->'workflow_generation',
              'submission_sha256',witness->'submission_sha256','actions_held',true);
          elsif raw->'ordinary_authorised' is distinct from 'true'::jsonb then
            reason:=case when witness->>'authority'='CANDIDATE_IMMUTABLE_HOURS' and witness->>'manager_approved_at_utc' is null
              then 'WAITING_FOR_MANAGER_SIGNATURE' else 'SIGNED_NOT_AUTHORISED' end;
            subject:='PAY_AND_INVOICE'; pay_subject:=true; rank:=20;
            status:=jsonb_build_object('text','Decision needed','tone','warning','subtext',case when reason='WAITING_FOR_MANAGER_SIGNATURE'
              then 'Waiting for the manager to sign' else 'Timesheet not authorised' end,'subtone','muted');
            action:=jsonb_build_object('kind','TIMESHEET','label','Open Timesheet ›','native_allowed',true,'target',jsonb_build_object('timesheet_id',raw->'timesheet_id'));
          elsif raw->'unfinished_daily_review'='true'::jsonb then
            reason:='DAILY_REVIEW_UNFINISHED'; subject:='INFORMATION'; rank:=30;
            status:=jsonb_build_object('text','No hold','tone','warning','subtext','Daily check not finished','subtone','muted');
            action:=jsonb_build_object('kind','DAILY_REVIEW','label','Continue daily check ›','native_allowed',true,'target',jsonb_build_object('import_id',raw->'query_import_id'));
          elsif jsonb_array_length(raw->'native_issues')>0 or raw#>>'{validation,status}' is distinct from 'VALIDATION_OK'
            or raw->>'processing_status'='READY_FOR_HR' then
            select value into native_issue from jsonb_array_elements(raw->'native_issues') order by value->>'action_id' limit 1;
            reason:=coalesce(native_issue->>'reason_code',raw#>>'{validation,reason_code}','NOT_YET_CHECKED'); subject:='INVOICE_ONLY'; rank:=40;
            status:=jsonb_build_object('text','Decision needed','tone','warning','subtext',case when native_issue is not null then
              case when native_issue->'sent_verified'='true'::jsonb then 'Query sent to Temp Staffing '||coalesce(native_issue->>'last_sent_at','')
              else 'Query not sent yet' end else 'Not yet checked against HealthRoster' end,'subtone','muted');
            action:=case when native_issue is not null and query_parent is not null then jsonb_build_object('kind','QUERIES','label','View query in Queries ›','native_allowed',true,
              'target',row_scope-'week_ending'||jsonb_build_object('row_key',query_parent->>'parent_group_key')) when native_issue is null then
              jsonb_build_object('kind','UPLOAD','label','Upload source file ›','native_allowed',true,'target',row_scope-'week_ending') end;
          else continue;
          end if;
          open_count:=open_count+1;
          if pay_subject then pay_count:=pay_count+1; end if;
          item:=jsonb_build_object('counted_timesheet_id',raw->'timesheet_id','source_information_witness',null);
        end if;
        if view='PAY_READONLY' and not pay_subject then continue; end if;
        item:=item||jsonb_build_object('item_id',raw->'item_id','item_kind',raw->'item_kind','scope',row_scope,'route',route,
          'worker',raw->'worker','work_date',raw->'work_date','sheet_scope',raw->'sheet_scope','reason_code',reason,
          'status',status,'hold',item_hold,'subject',subject,'permitted_pay_subject',pay_subject,
          'action',case when view='ADMIN' then action end,'queries_parent_witness',case when view='ADMIN' and action->>'kind'='QUERIES' then query_parent end,
          'correction_binding',correction,'submission_read',pending,
          '_rank',rank);
        row_items:=row_items||jsonb_build_array(item);
      end loop;
      checked:=checked_count=actual_count; completed:=open_count=0; pay_complete:=pay_count=0;
      checked_text:=case when checked then 'Checked' else (actual_count-checked_count)::text||' Timesheets not yet checked' end;
      complete_text:=case when completed and own_week=latest_week then case when earlier=0 then 'Nothing outstanding · earlier weeks all done'
        else 'Nothing outstanding this week · '||earlier::text||' earlier weeks open' end
        when completed then 'Nothing outstanding' else open_count::text||' items' end;
      fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'context',leaf,'native_query_rows',raw_rows));
    else
      process:=private.weekly_source_checklist_process_v1(scope.cycle_id,scope.client_id);
      select u.id,u.uploaded_at_utc into checking_upload,checked_at from public.weekly_source_uploads u
        join public.weekly_source_format_profiles profile on profile.id=u.source_format_profile_id
        join public.weekly_source_projection_publications publication on publication.upload_id=u.id and publication.state in('CURRENT','SUPERSEDED')
        where u.source_cycle_id=scope.cycle_id and u.report_scope_id is null and u.state in('CURRENT','SUPERSEDED')
          and exists(select 1 from public.weekly_source_upload_attempts a where a.logical_upload_id=u.id and a.result='ACCEPTED')
          and u.confirmed_coverage_start_local_date<=own_week-6 and u.confirmed_coverage_end_local_date>=own_week
          and (upper(coalesce(u.file_metadata_json->>'import_use','CHECKING'))='CHECKING' or profile.profile_code='NHSP_PREFINAL_RELEASED_V1')
          and exists(select 1 from public.weekly_source_upload_rows ur join public.weekly_source_row_resolutions rr on rr.upload_row_id=ur.id
            where ur.upload_id=u.id and rr.client_id=scope.client_id and rr.mapping_state='RESOLVED'
              and rr.generation=coalesce(publication.projection_generation,publication.authority_scope_version))
        order by u.uploaded_at_utc desc,u.id desc limit 1;
      select c.* into completion from public.weekly_source_client_cycle_completions c where c.source_cycle_id=scope.cycle_id
        and c.client_id=scope.client_id and c.state='CURRENT' and c.completion_kind='NO_SHIFTS_TO_IMPORT';
      select count(*)::integer into source_count from public.weekly_source_upload_rows ur
        join public.weekly_source_uploads u on u.id=ur.upload_id and u.source_cycle_id=scope.cycle_id and u.state='CURRENT'
        join public.weekly_source_row_resolutions rr on rr.upload_row_id=ur.id and rr.client_id=scope.client_id and rr.mapping_state='RESOLVED';
      no_work:=completion.id is not null and actual_count=0 and source_count=0
        and not exists(select 1 from public.contract_weeks cw join public.contracts con on con.id=cw.contract_id
          where con.client_id=scope.client_id and cw.week_ending_date=own_week and cw.status::text='SUBMITTED');
      checked:=checking_upload is not null or no_work;
      pay_complete:=exists(select 1 from jsonb_array_elements(process->'scopes') f where f->>'authority_scope_kind' in('CYCLE','NHSP_REPORT_SCOPE')
        and f->'process_complete'='true'::jsonb);
      rule:=leaf->'effective_rule';
      completed:=case when no_work then true when scope.source_family='NHSP' then
        exists(select 1 from public.weekly_source_report_scopes rs where rs.source_cycle_id=scope.cycle_id and rs.client_id=scope.client_id)
        and not exists(select 1 from public.weekly_source_report_scopes rs where rs.source_cycle_id=scope.cycle_id and rs.client_id=scope.client_id
          and not exists(select 1 from jsonb_array_elements(process->'scopes') f where f->>'report_scope_id'=rs.id::text and f->'process_complete'='true'::jsonb))
        when rule#>>'{values,invoice_membership}'='FOLLOW_FINAL_REPORT_LINES' then pay_complete
          and exists(select 1 from public.weekly_source_report_scopes rs where rs.source_cycle_id=scope.cycle_id and rs.client_id=scope.client_id and rs.self_bill_report_id is not null)
          and not exists(select 1 from public.weekly_source_report_scopes rs where rs.source_cycle_id=scope.cycle_id and rs.client_id=scope.client_id and rs.self_bill_report_id is not null
            and not exists(select 1 from jsonb_array_elements(process->'scopes') f where f->>'report_scope_id'=rs.id::text
              and f->>'authority_scope_kind'='SELF_BILL_REPORT_SCOPE' and f->'process_complete'='true'::jsonb))
        else pay_complete end;
      pay_complete:=pay_complete or no_work;
      if no_work then
        select 'No one worked · '||coalesce(nullif(a.display_name,''),a.id::text)||' · '||
          to_char(completion.attested_at_utc at time zone scope.timezone,'DD/MM/YYYY HH24:MI') into no_work_line
          from public.tms_users a where a.id=completion.attested_by_user_id;
      else no_work_line:=null; end if;
      checked_text:=case when no_work then no_work_line when checked then 'Checked '||to_char(checked_at at time zone scope.timezone,'DD/MM/YYYY HH24:MI:SS') else 'To do · upload the source file' end;
      if rule#>>'{values,invoice_membership}'='FOLLOW_FINAL_REPORT_LINES' then
        -- Q-NOPAY uses the native binding owner's exact finalised-export fact,
        -- independent of pay preparation/approval or report arrival time.
        select coalesce(jsonb_agg(jsonb_build_object('report_row_id',report_row.id,'binding',to_jsonb(b),
          'export_row_id',export_row.id,'export_row_state',export_row.row_finalisation_state,'finalised_by',export_row.finalised_by)
          order by report_row.id),'[]'::jsonb),coalesce(bool_or(b.id is null or export_row.row_finalisation_state is distinct from 'SOURCE_WORKED'
            or nullif(btrim(export_row.finalised_by),'') is null),false)
          into report_bindings,needs_export from public.weekly_source_report_scopes rs
          join public.weekly_source_projection_publications rp on rp.id=rs.current_projection_publication_id
            and rp.upload_id=rs.current_complete_upload_id and rp.report_scope_id=rs.id and rp.state='CURRENT'
            and rp.authority_scope_kind='SELF_BILL_REPORT_SCOPE' and rp.authority_scope_version=rs.version
          join public.weekly_source_uploads ru on ru.id=rp.upload_id and ru.state='CURRENT'
          join public.weekly_source_upload_rows report_row on report_row.upload_id=ru.id and report_row.row_finalisation_state='SOURCE_REPORT_LINE'
            and report_row.source_shift_charge_pence>=0 and report_row.work_date between own_week-6 and own_week
          left join public.weekly_source_self_bill_report_line_bindings b on b.report_row_id=report_row.id and b.state='CURRENT'
          left join public.weekly_source_upload_rows export_row on export_row.id=b.export_upload_row_id
          where rs.source_group_id=scope.group_id and rs.client_id=scope.client_id;
        if needs_export then checked:=false; checked_text:='Import a new export covering '||to_char(own_week-6,'DD/MM/YYYY')||' to '||to_char(own_week,'DD/MM/YYYY'); end if;
        fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'accepted_report_export_bindings',report_bindings));
      end if;
      complete_text:=case when no_work then no_work_line when completed then 'Processing complete' when pay_complete then 'Pay processing complete · report outstanding' else 'Processing outstanding' end;
      if view='ADMIN' then
        native_workspace:=private.weekly_source_office_tracker_v1(scope.cycle_id,actor.id);
        for tracker_action in select a.value from jsonb_array_elements(coalesce(native_workspace->'rows','[]'::jsonb)) r
          cross join lateral jsonb_array_elements(coalesce(r->'actions','[]'::jsonb)) a(value)
          where r->>'row_key'='tracker-'||scope.client_id::text and a.value->>'command'='NO_SHIFTS_TO_IMPORT' and a.value->'enabled'='true'::jsonb
        loop
          admin_actions:=admin_actions||jsonb_build_array(jsonb_build_object('kind','NO_ONE_WORKED','label','No one worked','native_allowed',true,
            'payload',(tracker_action->'payload'-'actor_user_id')||jsonb_build_object('attestation_text','No one worked')));
        end loop;
        if completion.id is not null and scope.cycle_state<>'FINALISING' then
          admin_actions:=jsonb_build_array(jsonb_build_object('kind','UNDO_NO_ONE_WORKED','label','Undo','native_allowed',true,
            'payload',row_scope-'week_ending'||jsonb_build_object('completion_id',completion.id,'expected_cycle_version',scope.cycle_version)));
        end if;
        if route in('SELF_BILL','NATIVE') then
          select jsonb_build_object('kind','FINALISE','label','View Finalise ›','native_allowed',true,
            'target',row_scope-'week_ending'||jsonb_build_object('report_scope_id',rs.id)) into finalise_link
            from public.weekly_source_report_scopes rs where rs.source_cycle_id=scope.cycle_id and rs.client_id=scope.client_id order by rs.id limit 1;
          finalise_link:=coalesce(finalise_link,jsonb_build_object('kind','FINALISE','label','View Finalise ›','native_allowed',true,'target',row_scope-'week_ending'));
        end if;
      end if;
      if route='NATIVE' and not exists(select 1 from public.contract_weeks cw join public.contracts con on con.id=cw.contract_id
        where con.client_id=scope.client_id and cw.week_ending_date=own_week and cw.additional_seq=0 and not cw.is_adjustment
          and cw.status::text in('PLANNED','OPEN','SUBMITTED')) and checking_upload is null and completion.id is null
        and jsonb_array_length(process->'scopes')=0 and not exists(select 1 from public.weekly_source_report_scopes rs
          where rs.source_cycle_id=scope.cycle_id and rs.client_id=scope.client_id) then continue; end if;
      fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'context',leaf,'process',process,
        'checking_upload',checking_upload,'checked_at',checked_at,'completion',to_jsonb(completion),'source_count',source_count));
      -- Reuse the native Queries owner for Source reason/status/population.
      -- Its work-event subject is distinct from a submitted Timesheet and from
      -- R-254 HR information. The witness retains actual catalogue identities.
      seen_items:='{}'::text[];
      for source_native in
        select publication.id publication_id,q.group_key parent_group_key,q.candidate_name,child.value native_row,
          event.id work_event_id,event.work_date,
          coalesce(comparison.source_row_id,review.upload_row_id,missing_row.id) upload_row_id,
          mapping.id row_resolution_id,incident.id incident_id,comparison.id comparison_revision_id,review.id manual_review_id,
          coalesce(comparison.issue_family,case when review.id is not null then 'MANUAL_REVIEW' else 'CANDIDATE_TIMESHEET_MISSING' end) native_reason
        from public.weekly_source_cycles cycle
        join public.weekly_source_projection_publications publication on publication.source_cycle_id=cycle.id and publication.state='CURRENT'
          and (publication.id=cycle.current_projection_publication_id or (cycle.current_projection_publication_id is null and
            exists(select 1 from public.weekly_source_report_scopes rs where rs.source_cycle_id=cycle.id and rs.client_id=scope.client_id
              and rs.current_projection_publication_id=publication.id)))
        cross join lateral private.weekly_source_office_query_groups_v1(cycle.id,publication.id,jsonb_build_object('client_id',scope.client_id)) q
        cross join lateral jsonb_array_elements(q.children) child(value)
        left join public.weekly_discrepancy_incidents incident on incident.id=nullif(child.value->>'incident_id','')::uuid
          and incident.client_id=scope.client_id and incident.source_group_id=scope.group_id and incident.state='OPEN'
        left join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
          and comparison.incident_id=incident.id
        left join private.weekly_source_manual_reviews review on review.id=nullif(child.value->>'manual_review_id','')::uuid
          and review.client_id=scope.client_id and review.source_group_id=scope.group_id and review.source_cycle_id=scope.cycle_id and review.state='OPEN'
        left join public.weekly_source_upload_rows missing_row on child.value->>'row_key'='missing-'||missing_row.id::text
          and missing_row.upload_id=publication.upload_id
        left join public.weekly_source_row_resolutions mapping on mapping.upload_row_id=missing_row.id
          and mapping.client_id=scope.client_id and mapping.mapping_state='RESOLVED'
          and mapping.generation=coalesce(publication.projection_generation,publication.authority_scope_version)
        join public.weekly_work_events event on event.id=coalesce(incident.work_event_id,review.work_event_id,mapping.work_event_id)
          and event.client_id=scope.client_id and event.first_source_group_id=scope.group_id
        where route<>'AGENCY_INVOICE' and cycle.id=scope.cycle_id and q.client_id=scope.client_id and event.work_date between own_week-6 and own_week
        order by publication.id,q.group_key,child.value->>'row_key'
      loop
        source_witness:=jsonb_build_object('owner','private.weekly_source_office_query_groups_v1',
          'projection_publication_id',source_native.publication_id,'work_event_id',source_native.work_event_id,
          'upload_row_id',source_native.upload_row_id,'row_resolution_id',source_native.row_resolution_id,
          'incident_id',source_native.incident_id,'comparison_revision_id',source_native.comparison_revision_id,
          'manual_review_id',source_native.manual_review_id,'native_row_key',source_native.native_row->>'row_key',
          'parent_group_key',source_native.parent_group_key,
          'evidence_fingerprint',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_NATIVE_QUERY_WITNESS_V1',
            jsonb_build_object('scope',row_scope,'publication_id',source_native.publication_id,'parent_group_key',source_native.parent_group_key,'work_event_id',source_native.work_event_id,
              'comparison_revision_id',source_native.comparison_revision_id,'row_resolution_id',source_native.row_resolution_id,
              'manual_review_id',source_native.manual_review_id,'native_row',source_native.native_row)),'hex'));
        if source_native.native_row->>'row_key'=any(seen_items) then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_ITEM_AMBIGUOUS' using errcode='55000';
        end if;
        seen_items:=array_append(seen_items,source_native.native_row->>'row_key');
        pay_subject:=source_native.native_row->'accept_eligible'='true'::jsonb
          or jsonb_typeof(source_native.native_row->'protected_pay_seed')='object';
        item_hold:=unknown_hold;
        -- Resolve only real existing native lineage roots. A Source work event
        -- does not acquire a counted Timesheet or invented family to read pay.
        select count(distinct t.timesheet_id),(array_agg(distinct t.timesheet_id order by t.timesheet_id))[1]
          into native_roots,native_root from public.weekly_source_row_timesheet_lineages l
          join public.timesheets t on t.timesheet_id=l.timesheet_id and t.is_current
          join public.contracts con on con.id=t.contract_id and con.client_id=scope.client_id
          where l.work_event_id=source_native.work_event_id and t.week_ending_date=own_week
            and t.line_type::text='HOURS' and t.archived_at_utc is null and t.revoked_at is null;
        if native_roots=1 and private.weekly_source_actual_timesheet_submission_v1(native_root)->'actual_submitted'='true'::jsonb then
          hold_facts:=private.weekly_source_checklist_timesheet_hold_facts_v1(native_root,row_scope,actor.id);
          item_hold:=hold_facts->'hold'; financial_facts:=financial_facts||jsonb_build_array(hold_facts->'native_facts');
        end if;
        if native_roots=1 then
          -- A genuine Source-owned authorised root may have no Candidate
          -- submission. The native money owner proves its real event/root,
          -- publication/command/job/member and current query qualification;
          -- it never creates a counted Timesheet or ordinary money subject.
          source_money:=private.weekly_source_checklist_source_shift_query_money_v1(
            native_root,source_native.work_event_id,row_scope,actor.id);
          financial_facts:=financial_facts||jsonb_build_array(source_money->'native_facts');
          if source_money->'known'='true'::jsonb then item_hold:=source_money->'hold';pay_subject:=true; end if;
        end if;
        if pay_subject then pay_count:=pay_count+1; end if;
        if view='PAY_READONLY' and pay_subject is distinct from true then continue; end if;
        row_items:=row_items||jsonb_build_array(jsonb_build_object('item_id','native-source:'||(source_native.native_row->>'row_key'),
          'item_kind','NATIVE_SOURCE','scope',row_scope,'route',route,'worker',source_native.candidate_name,
          'work_date',source_native.work_date,'sheet_scope','WEEKLY','reason_code',source_native.native_reason,
          'status',jsonb_build_object('text',coalesce(source_native.native_row#>>'{status,text}','Decision needed'),
            'tone',case when source_native.native_row#>>'{status,tone}' in('positive','warning','danger','muted')
              then source_native.native_row#>>'{status,tone}' else 'muted' end,
            'subtext',coalesce(source_native.native_row->>'issue',''),'subtone','muted'),'hold',item_hold,
          'subject',case when pay_subject then 'PAY' else 'INFORMATION' end,'permitted_pay_subject',coalesce(pay_subject,false),
          'action',case when view='ADMIN' then jsonb_build_object('kind','QUERIES','label','View item in Queries ›','native_allowed',true,
            'target',row_scope-'week_ending'||jsonb_build_object('row_key',source_native.parent_group_key)) end,
          'counted_timesheet_id',null,'correction_binding',null,'submission_read',null,'source_information_witness',null,
          'native_source_witness',source_witness,'_rank',50));
      end loop;
      if route='SELF_BILL' and view='ADMIN' then
        -- The same exact native admission blocker supplies invoice-only work.
        -- Missing amount/action qualification never suppresses its catalogue.
        -- Reduced pay readers do not execute this invoice population reader.
        invoice_blocks:=private.weekly_source_checklist_source_invoice_decision_items_v1(row_scope,actor.id);
        financial_facts:=financial_facts||jsonb_build_array(invoice_blocks->'native_facts');
        for raw in select value from jsonb_array_elements(invoice_blocks->'items') loop
          row_items:=row_items||jsonb_build_array(raw||jsonb_build_object('item_kind','NATIVE_INVOICE','scope',row_scope,'route',route,
            'sheet_scope','WEEKLY','reason_code','VAT_FIT','status',jsonb_build_object('text','Holds invoice','tone','danger',
              'subtext',case when raw#>>'{hold,pending_decision}'='true' then 'Office decision needed: native VAT_FIT release is qualified.'
              else 'Invoice held: source net and gross do not fit 20% VAT.' end,'subtone','muted'),
            'subject','INVOICE_ONLY','permitted_pay_subject',false,'action',raw->'action','counted_timesheet_id',null,
            'correction_binding',null,'submission_read',null,'source_information_witness',null,'native_source_witness',null,'_rank',55));
        end loop;
        if jsonb_array_length(invoice_blocks->'items')>0 then
          completed:=false;complete_text:=jsonb_array_length(row_items)::text||' items';
        end if;
      end if;
      if route='AGENCY_INVOICE' then
        -- This route is led by actual native ordinary Timesheets. Invoicing is
        -- not the Completed predicate, and a Source finalisation is not used
        -- as an ordinary Authorise/validation/query/correction substitute.
        agency_facts:='[]'::jsonb; actual_count:=0; checked_count:=0; open_count:=0; pay_count:=0;
        for agency_native in
          select t.timesheet_id,t.authorised_at_server,t.version,con.candidate_id,
            coalesce(nullif(btrim(ca.first_name||' '||ca.last_name),''),ca.id::text) worker,
            validation.id validation_id,validation.status::text validation_status,validation.last_source,
            fin.id financial_id,fin.processing_status::text processing_status,
            correction_request.id request_id,correction_request.state_revision,
            correction_request.reversed_timesheet_id,correction_request.replacement_timesheet_id,correction_request.reversal_timesheet_id,
            exists(select 1 from public.import_review_decisions d join public.import_review_states st on st.import_id=d.import_id
              and st.preview_generation=d.preview_generation and st.superseded_at_utc is null and st.abandoned_at_utc is null
              where d.timesheet_id=t.timesheet_id and d.client_id=scope.client_id and d.is_current
                and d.import_id=validation.last_source and d.action_kind in('EMAIL_ISSUE','EMAIL_REMINDER')
                and not exists(select 1 from public.hr_issue_emails e
                  join public.hr_issue_email_deliveries delivery on delivery.id=e.last_successful_delivery_id
                  join public.hr_issue_email_delivery_items delivery_item on delivery_item.delivery_id=delivery.id and delivery_item.issue_id=e.id
                    and delivery_item.issue_fingerprint=e.issue_fingerprint and delivery_item.action_id=d.action_id
                  join public.mail_outbox mail on mail.id=delivery.mail_outbox_id and mail.deterministic_outbox_key=delivery.deterministic_outbox_key
                  where e.timesheet_id=t.timesheet_id and e.client_id=scope.client_id
                    and e.issue_fingerprint=coalesce(nullif(d.summary_json->>'issue_fingerprint',''),d.source_identity)
                    and e.delivery_history_status='SENT_VERIFIED' and delivery.status='SENT'
                    and delivery.accepted_at_utc is not null and delivery.marked_at_utc is not null and delivery_item.marked_sent_at_utc is not null
                    and (upper(mail.status::text)='SENT' or mail.sent_at is not null))) query_unsent,
            exists(select 1 from public.hr_imports hi join public.import_review_states st on st.import_id=hi.id
              where hi.id=validation.last_source and hi.pruned_at is null and st.applied_at_utc is not null
                and hi.source_system::text in('HEALTHROSTER','HEALTHROSTER_DAILY')) comparison_accepted
          from public.timesheets t join public.contracts con on con.id=t.contract_id and con.client_id=scope.client_id
          join public.candidates ca on ca.id=con.candidate_id
          left join public.timesheets_financials fin on fin.timesheet_id=t.timesheet_id and fin.is_current and fin.timesheet_version=t.version
          left join lateral(select v.* from public.timesheet_validations v where v.timesheet_id=t.timesheet_id
            order by v.updated_at desc,v.created_at desc,v.id desc limit 1) validation on true
          left join private.timesheet_correction_requests correction_request on correction_request.state='WAITING'
            and correction_request.reversed_timesheet_id=t.timesheet_id
          where t.week_ending_date=own_week and t.is_current and t.line_type::text='HOURS'
            and t.archived_at_utc is null and t.revoked_at is null and t.correction_kind is distinct from 'WHOLE_TIMESHEET_REVERSAL'
            and private.weekly_source_agency_invoice_check_only_v1(t.timesheet_id) is true
            and not exists(select 1 from private.timesheet_correction_requests r where
              (r.state='WAITING' and r.replacement_timesheet_id=t.timesheet_id) or (r.state='APPLIED' and r.reversed_timesheet_id=t.timesheet_id))
          order by t.timesheet_id
        loop
          if exists(select 1 from jsonb_array_elements(agency_facts) f where f->>'timesheet_id'=agency_native.timesheet_id::text) then
            raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_ITEM_AMBIGUOUS' using errcode='55000';
          end if;
          agency_facts:=agency_facts||jsonb_build_array(to_jsonb(agency_native)); actual_count:=actual_count+1;
          if agency_native.comparison_accepted then checked_count:=checked_count+1; end if;
          correction:=null; action:=null; pay_subject:=false; signed:=null; item_hold:=unknown_hold; query_parent:=null;
          select value into query_parent from jsonb_array_elements(query_targets) where value->>'item_id'='timesheet:'||agency_native.timesheet_id::text;
          if private.weekly_source_actual_timesheet_submission_v1(agency_native.timesheet_id)->'actual_submitted'='true'::jsonb then
            hold_facts:=private.weekly_source_checklist_timesheet_hold_facts_v1(agency_native.timesheet_id,row_scope,actor.id);
            item_hold:=hold_facts->'hold'; financial_facts:=financial_facts||jsonb_build_array(hold_facts->'native_facts');
          end if;
          if agency_native.request_id is not null then
            correction:=jsonb_build_object('request_id',agency_native.request_id,'state','WAITING','state_revision',agency_native.state_revision,
              'original_timesheet_id',agency_native.reversed_timesheet_id,'replacement_timesheet_id',agency_native.replacement_timesheet_id,
              'reversal_timesheet_id',agency_native.reversal_timesheet_id);
            begin signed:=private.timesheet_correction_signed_witness_v1(agency_native.replacement_timesheet_id,false);
            exception when sqlstate '55000' then signed:=null; end;
            reason:=case when signed is null then 'CORRECTION_WAITING' else 'SIGNED_CORRECTION_RECEIVED' end;
            pay_subject:=signed is not null; subject:=case when pay_subject then 'PAY_AND_INVOICE' else 'INVOICE_ONLY' end; rank:=10;
            complete_text:=case when signed is null then 'Correction waiting' else 'Signed correction received' end;
            action:=jsonb_build_object('kind','TIMESHEET','label','View correction in Timesheet ›','native_allowed',true,
              'target',jsonb_build_object('timesheet_id',agency_native.replacement_timesheet_id));
          elsif agency_native.authorised_at_server is null then
            reason:='TIMESHEET_NOT_AUTHORISED'; pay_subject:=true; subject:='PAY_AND_INVOICE'; rank:=20;
            complete_text:='Timesheet not authorised';
            action:=jsonb_build_object('kind','TIMESHEET','label','Open Timesheet ›','native_allowed',true,'target',jsonb_build_object('timesheet_id',agency_native.timesheet_id));
          elsif agency_native.validation_status is distinct from 'VALIDATION_OK' or agency_native.processing_status='READY_FOR_HR'
            or agency_native.query_unsent then
            reason:=case when agency_native.query_unsent then 'QUERY_NOT_SENT' when not agency_native.comparison_accepted
              then 'NOT_YET_CHECKED' else 'HEALTHROSTER_VALIDATION_OUTSTANDING' end; subject:='INVOICE_ONLY'; rank:=40;
            complete_text:=case reason when 'QUERY_NOT_SENT' then 'Query not sent yet' when 'NOT_YET_CHECKED'
              then 'Not yet checked against HealthRoster' else 'HealthRoster validation outstanding' end;
            if query_parent is not null then
              action:=jsonb_build_object('kind','QUERIES','label','View item in Queries ›','native_allowed',true,
                'target',row_scope-'week_ending'||jsonb_build_object('row_key',query_parent->>'parent_group_key'));
            end if;
          else continue;
          end if;
          open_count:=open_count+1; if pay_subject then pay_count:=pay_count+1; end if;
          if view='PAY_READONLY' and not pay_subject then continue; end if;
          row_items:=row_items||jsonb_build_array(jsonb_build_object('item_id','timesheet:'||agency_native.timesheet_id::text,
            'item_kind','ACTUAL_TIMESHEET','scope',row_scope,'route',route,'worker',agency_native.worker,'work_date',own_week,
            'sheet_scope','WEEKLY','reason_code',reason,'status',jsonb_build_object('text','Decision needed','tone','warning','subtext',complete_text,'subtone','muted'),
            'hold',item_hold,'subject',subject,'permitted_pay_subject',pay_subject,'action',case when view='ADMIN' then action end,
            'queries_parent_witness',case when view='ADMIN' and action->>'kind'='QUERIES' then query_parent end,
            'counted_timesheet_id',agency_native.timesheet_id,'correction_binding',correction,'submission_read',null,'source_information_witness',null,'_rank',rank));
        end loop;
        checked:=no_work or (actual_count>0 and checked_count=actual_count);
        completed:=no_work or (actual_count>0 and open_count=0); pay_complete:=no_work or (actual_count>0 and pay_count=0);
        checked_text:=case when no_work then no_work_line when checked then 'Checked' else 'To do · upload the HealthRoster file' end;
        complete_text:=case when no_work then no_work_line when completed then 'Nothing outstanding' else open_count::text||' items' end;
        fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_agency_timesheets',agency_facts));
      end if;
      fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_source_items',row_items));
    end if;
    if route in('SIGNED_TIMESHEETS','AGENCY_INVOICE') then
      if jsonb_typeof(leaf->'native_financial_items') is distinct from 'array' or jsonb_typeof(leaf->'native_financial_facts') is distinct from 'array' then
        raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_FINANCIAL_FRAME_UNAVAILABLE' using errcode='55000'; end if;
      financial_facts:=financial_facts||leaf->'native_financial_facts';
      for raw in select value from jsonb_array_elements(leaf->'native_financial_items') loop
        if raw->>'native_financial_kind'='LEGACY_REVIEW' then
          if raw->'review_required' is distinct from 'true'::jsonb
            or raw#>'{native_legacy_review_witness,scope}' is distinct from row_scope then
            raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_FINANCIAL_FRAME_UNAVAILABLE' using errcode='55000';end if;
          row_items:=row_items||jsonb_build_array(jsonb_build_object(
            'item_id','native-legacy-review:'||(raw#>>'{native_legacy_review_witness,timesheet_id}'),
            'item_kind','NATIVE_ORDINARY_FINANCIAL_REVIEW','scope',row_scope,'route',route,'worker',raw->'worker',
            'work_date',raw->'work_date','sheet_scope',raw->'sheet_scope','reason_code','NATIVE_LEGACY_PAY_HOLD_REVIEW',
            'status',raw->'status','hold',raw->'hold','subject','PAY','permitted_pay_subject',true,
            'action',case when view='ADMIN' then jsonb_build_object('kind','TIMESHEET','label','Review native pay hold in Timesheet ›',
              'native_allowed',true,'target',jsonb_build_object('timesheet_id',raw#>>'{native_legacy_review_witness,timesheet_id}')) end,
            'counted_timesheet_id',raw#>>'{native_legacy_review_witness,timesheet_id}','correction_binding',null,'submission_read',null,
            'source_information_witness',null,'native_source_witness',null,'native_invoice_witness',null,'native_invoice_decision_witness',null,
            'queries_parent_witness',null,'native_ordinary_money_witness',null,'native_legacy_review_witness',raw->'native_legacy_review_witness','_rank',35));
          open_count:=open_count+1;pay_count:=pay_count+1;completed:=false;pay_complete:=false;complete_text:=open_count::text||' items';
          continue;
        end if;
        if raw->'known' is distinct from 'true'::jsonb or raw#>>'{native_ordinary_money_witness,scope,source_group_id}' is distinct from row_scope->>'source_group_id'
          or raw#>>'{native_ordinary_money_witness,scope,source_cycle_id}' is distinct from row_scope->>'source_cycle_id'
          or raw#>>'{native_ordinary_money_witness,scope,client_id}' is distinct from row_scope->>'client_id'
          or raw#>>'{native_ordinary_money_witness,scope,week_ending}' is distinct from row_scope->>'week_ending' then
          raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_FINANCIAL_FRAME_UNAVAILABLE' using errcode='55000'; end if;
        -- A separate native TSFIN hold is not a HealthRoster invoice query.
        -- The genuine Timesheet was already counted by its submission owner.
        row_items:=row_items||jsonb_build_array(jsonb_build_object('item_id','native-financial:'||(raw#>>'{native_ordinary_money_witness,work_id}'),
          'item_kind','NATIVE_ORDINARY_FINANCIAL','scope',row_scope,'route',route,'worker',raw->'worker','work_date',raw->'work_date',
          'sheet_scope',raw->>'sheet_scope','reason_code','PAY_ON_HOLD','status',raw->'status','hold',raw->'hold','subject','PAY','permitted_pay_subject',true,
          'action',case when view='ADMIN' then jsonb_build_object('kind','TIMESHEET','label','View pay hold in Timesheet ›','native_allowed',true,
            'target',jsonb_build_object('timesheet_id',raw#>>'{native_ordinary_money_witness,timesheet_id}')) end,
          'counted_timesheet_id',raw#>>'{native_ordinary_money_witness,timesheet_id}','correction_binding',null,'submission_read',null,
          'source_information_witness',null,'native_source_witness',null,'native_invoice_witness',null,'native_invoice_decision_witness',null,
          'queries_parent_witness',null,'native_ordinary_money_witness',raw->'native_ordinary_money_witness','_rank',35));
        open_count:=open_count+1;pay_count:=pay_count+1;completed:=false;pay_complete:=false;complete_text:=open_count::text||' items';
      end loop;
    end if;
    if route='SIGNED_TIMESHEETS' and own_week<>latest_week and completed then continue; end if;
    fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_financial_facts',financial_facts));
    if view='PAY_READONLY' and pay_count=0 then continue; end if;
    empty_cell:=jsonb_build_object('value',null,'status',jsonb_build_object('text','—','tone','muted','subtext','','subtone','muted'),'item_count',0,'link',null);
    rows:=rows||jsonb_build_array(jsonb_build_object('row_id',row_id,'scope',row_scope,'client',scope.client_name,
      'group',scope.display_name||case route when 'SIGNED_TIMESHEETS' then ' · signed Timesheets' when 'AGENCY_INVOICE' then ' · agency invoice'
        when 'SELF_BILL' then ' · self-bill' else '' end,'route',route,
      'retention',jsonb_build_object('actual_timesheet_count',actual_count,'most_recent_ended_week',latest_week,'has_open_items',not completed,'earlier_open_weeks',earlier),
      'checked',case when view='PAY_READONLY' then empty_cell else jsonb_build_object('value',checked,'item_count',actual_count-checked_count,
        'status',jsonb_build_object('text',checked_text,'tone','muted','subtext','','subtone','muted'),
        'link',case when checking_upload is not null then jsonb_build_object('kind','UPLOAD_DETAIL','label','View source file ›','native_allowed',true,'target',jsonb_build_object('upload_id',checking_upload)) end) end,
      'completed',jsonb_build_object('value',case when view='PAY_READONLY' then pay_complete else completed end,
        'item_count',case when view='PAY_READONLY' then pay_count else jsonb_array_length(row_items) end,
        'status',jsonb_build_object('text',case when view='PAY_READONLY' then case when pay_complete then 'Pay: nothing outstanding' else pay_count::text||' pay items' end else complete_text end,
          'tone','muted','subtext','','subtone','muted'),'link',case when view='ADMIN' then finalise_link end),
      'pay_completed',jsonb_build_object('value',pay_complete,'item_count',pay_count,'status',jsonb_build_object('text',case when pay_complete
        then 'Pay: nothing outstanding' else pay_count::text||' pay items' end,'tone','muted','subtext','','subtone','muted'),'link',null),
      'hold',unknown_hold,'permitted_pay_item_count',pay_count,'admin_actions',admin_actions));
    items:=items||row_items;
  end loop;
  -- One actor-wide revision is shared by main and independently paged item
  -- expansions. Filtering/sorting affect bindings, never erase change evidence.
  revision:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_READ_REVISION_V1',jsonb_build_object(
    'actor_user_id',actor.id,'permission_view',view,'native_facts',fingerprints,'rows',rows,'items',items)),'hex');
  records:='[]'::jsonb;
  for item in select value from jsonb_array_elements(case when command='CHECKLIST' then rows else items end) loop
    if exists(select 1 from unnest(array['source_group_id','client_id','source_cycle_id','week_ending']) k
      where request ? k and item#>>array['scope',k] is distinct from request->>k) then continue; end if;
    if command='CHECKLIST' then
      primary_key:=case sort when 'week_ending' then item#>>'{scope,week_ending}' when 'client' then private.weekly_source_query_ascii_fold_v1(item->>'client')
        when 'group' then private.weekly_source_query_ascii_fold_v1(item->>'group') when 'checked' then coalesce(item#>>'{checked,value}','false')
        when 'completed' then item#>>'{completed,value}' end;
      sort_key:=jsonb_build_array(primary_key,private.weekly_source_query_ascii_fold_v1(item->>'client'),item#>>'{scope,source_group_id}',
        item#>>'{scope,client_id}',item#>>'{scope,week_ending}',item->>'row_id');
    else
      sort_key:=jsonb_build_array(private.weekly_source_query_ascii_fold_v1(item->>'worker'),item->>'work_date',
        lpad(item->>'_rank',3,'0'),item->>'item_id',item#>>'{scope,client_id}',item#>>'{scope,week_ending}');
    end if;
    records:=records||jsonb_build_array((item-'_rank')||jsonb_build_object('_key',sort_key));
  end loop;
  if command='CHECKLIST_ISSUES' and not exists(select 1 from jsonb_array_elements(rows) r where r->'scope'=jsonb_build_object(
    'source_group_id',request->'source_group_id','client_id',request->'client_id','source_cycle_id',request->'source_cycle_id','week_ending',request->'week_ending')) then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';
  end if;
  result:=private.weekly_source_checklist_page_v1(records,request,actor.id,view,binding,revision);
  if octet_length(result::text)>200000 then raise exception 'WEEKLY_SOURCE_CHECKLIST_RESPONSE_TOO_LARGE' using errcode='54000'; end if;
  return result;
exception when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID' using errcode='22023';
end;
$function$;
create or replace function private.bpay_next_workbench_refresh_work_core_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_member record;v_accepted record;v_totals record;v_candidate uuid;v_client uuid;v_target text;v_financial bigint;v_projection bigint;
  v_group text:='WORK:'||p_work_id::text;v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  v_sunday date;v_sequence bigint;v_reason text;v_priority smallint;v_section text;v_complete boolean;
  v_pay_hold record;v_hold_canonical boolean;v_native_period jsonb;
  v_target_umbrella uuid;v_target_vat boolean;v_need_operation uuid;v_resolution_need jsonb;v_resolution_date date;v_resolution_date_context jsonb;
  v_source_event uuid;
  v_query_gate jsonb;v_query_scope jsonb;v_query_loaded boolean:=false;v_query_state text:='UNAVAILABLE';v_query_exclusions jsonb;v_segment_query_state text;
  v_wait_before boolean;v_wait_after boolean;v_case_id uuid;v_withdrawn_complete boolean;v_was_dirty boolean;
  v_manual private.bpay_next_work_collection%rowtype;v_manual_observation jsonb;v_manual_position private.bpay_next_position%rowtype;v_original_line private.bpay_next_approved_line%rowtype;
  v_positive numeric;v_recovery numeric;v_overhang numeric;v_snooze jsonb;v_count bigint:=0;v_portion text;v_zero boolean;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  -- Exact current booking authority; no historical/original-row fallback.
  select t.timesheet_id,tf.id financial_id,coalesce(tf.pay_on_hold,false) pay_on_hold,
    case when tf.pay_on_hold then tf.pay_on_hold_reason end pay_on_hold_reason into v_pay_hold
    from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.booking_id=v_work.booking_id and t.is_current;
  v_hold_canonical:=v_pay_hold.timesheet_id is not null and v_pay_hold.financial_id is not null;
  -- Read stored WAIT before replacing it; the withdrawal owner already changed work.
  select exists(select 1 from private.bpay_next_workbench_item i where i.group_key=v_group
    and i.reason_code='AWAITING_REAUTHORISATION') into v_wait_before;
  v_wait_after:=private.bpay_next_work_wait_state_v1(v_work.id);
  select exists(select 1 from private.bpay_next_workbench_group g where g.group_key=v_group and g.dirty) into v_was_dirty;
  -- Reauthorisation becomes effective only after final position application.
  -- Keep its stored WAIT marker during Processing for the final boundary hook.
  if v_wait_before and v_work.approval_state='APPROVED'
    and v_work.current_revision_id is distinct from v_work.applied_revision_id then
    update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
      dirty_group_count=dirty_group_count+case when v_was_dirty then 0 else 1 end,
      updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
    update private.bpay_next_workbench_group set complete=false,dirty=true,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    update private.bpay_next_workbench_item set selectable=false,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    return;
  end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text,case when c.pay_method='UMBRELLA' then c.umbrella_id end,
    case when c.pay_method='UMBRELLA' then u.vat_chargeable end into v_target,v_target_umbrella,v_target_vat
    from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=case when v_work.approval_state='WITHDRAWN' then v_work.applied_revision_id else v_work.current_revision_id end
      and r.work_id=v_work.id;
  -- Contractless DAILY authority is the existing publisher's sealed revision
  -- and its exact physical/current TF, never a name, original-row fallback or
  -- an invented Contract. Incomplete/changed evidence leaves Client unknown.
  if v_work.contract_id is null and v_work.work_kind='ORDINARY' and v_work.origin_kind='DAILY'
    and v_revision.source_kind='ORDINARY' and v_revision.sealed_at_utc is not null
    and v_revision.daily_approval_evidence_json->>'contract_version'='BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1'
    and v_revision.daily_approval_evidence_json->>'timesheet_id'=v_revision.physical_timesheet_id::text
    and v_revision.daily_approval_evidence_json->>'timesheet_version'=v_revision.physical_timesheet_version::text
    and v_revision.daily_approval_evidence_json->>'financial_id'=v_revision.financial_snapshot_id::text
    and v_revision.daily_approval_evidence_json->>'booking_id'=v_work.booking_id
    and v_revision.daily_approval_evidence_json->>'candidate_id'=v_work.candidate_id::text
    and v_revision.daily_approval_evidence_json->>'work_date'=v_work.origin_work_date::text then
    select f.client_id into v_client from public.timesheets t join public.timesheets_financials f
      on f.id=v_revision.financial_snapshot_id and f.timesheet_id=t.timesheet_id
      where t.timesheet_id=v_revision.physical_timesheet_id and t.booking_id=v_work.booking_id
        and t.version=v_revision.physical_timesheet_version and t.is_current and t.contract_id is null
        and t.sheet_scope::text='DAILY' and t.line_type::text='HOURS'
        and t.week_ending_date=v_work.week_ending_date and t.authorised_at_server is not null
        and t.revoked_at is null and t.archived_at_utc is null and not t.is_adjustment
        and t.parent_timesheet_id is null and t.correction_id is null
        and f.is_current and not f.is_stale and f.timesheet_version=t.version and f.authorised_at_utc is not null
        and f.processed_at_utc is not null and f.basis::text='SELF_REPORTED' and f.candidate_id=v_work.candidate_id
        and f.client_id::text=v_revision.daily_approval_evidence_json->>'client_id'
        and (t.worked_start_iso at time zone 'Europe/London')::date=v_work.origin_work_date
        and (f.worked_start_iso,f.worked_end_iso,f.break_start_iso,f.break_end_iso,f.break_minutes)
          is not distinct from (t.worked_start_iso,t.worked_end_iso,t.break_start_iso,t.break_end_iso,t.break_minutes)
        and t.settings_authority_json->>'authority_version'='CONTRACT_SETTINGS_AUTHORITY_V1'
        and t.settings_authority_json->>'workflow'='DAILY'
        and t.settings_authority_json->>'client_id'=f.client_id::text
        and t.settings_authority_json->'contract_id'='null'::jsonb
        and t.settings_authority_json->>'relevant_date'=v_work.origin_work_date::text
        and (select count(*) from public.timesheets current_ts where current_ts.booking_id=t.booking_id and current_ts.is_current)=1
        and (select count(*) from public.timesheets_financials current_tf where current_tf.timesheet_id=t.timesheet_id and current_tf.is_current)=1;
  end if;
  v_native_period:=private.bpay_next_work_period_proof_v1(v_work.id,v_revision.id,v_today);
  v_source_event:=v_revision.source_event_id;
  if v_revision.source_kind in ('SOURCE','PROTECTED') and v_revision.sealed_at_utc is not null
    and v_revision.payment_treatment_approval_revision_id is not null then
    select x.source_event_id into v_source_event from private.bpay_next_work_revision x
      where x.id=private.bpay_next_treatment_approval_revision_v1(v_revision.id) and x.work_id=v_work.id;
  end if;
  v_complete:=v_work.approval_state='APPROVED' and v_revision.id is not null and v_revision.sealed_at_utc is not null
    and v_work.applied_revision_id=v_work.current_revision_id
    and (select count(*) from private.bpay_next_approved_line a where a.revision_id=v_revision.id)=v_revision.expected_line_count;
  -- Only the latest native EX08 receipt and this exact Work; retained needs
  -- are joined inside this existing projection writer, never a read RPC.
  if v_complete then
    select r.operation_id into v_need_operation from private.bpay_next_pay_method_change_receipt r
      where r.candidate_id=v_candidate order by r.post_metadata_version desc limit 1;
    if v_need_operation is not null then
      perform private.bpay_next_component_resolution_need_publish_v1(v_need_operation,v_work.id);
      v_resolution_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
      if v_resolution_date_context->>'ok'='true' then
        v_resolution_date:=(v_resolution_date_context->>'current_official_pay_date')::date;
      end if;
    end if;
  end if;
  v_withdrawn_complete:=v_work.approval_state='WITHDRAWN' and v_revision.id is not null
    and v_revision.sealed_at_utc is not null and not exists(select 1 from private.bpay_next_position p
      where p.work_id=v_work.id and p.applied_revision_id is distinct from v_work.applied_revision_id
        and (p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
          or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
          or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0));
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_group_count=dirty_group_count-case when v_was_dirty then 1 else 0 end,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,v_work.current_revision_id,
    v_revision.source_pay_channel,v_target,v_work.week_ending_date,coalesce(v_complete,false),v_projection,v_financial,v_metadata.metadata_version,
    case when v_complete then 'CURRENT' when v_work.approval_state='APPROVED' then 'PROCESSING' else 'UNAVAILABLE' end)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,week_ending_date=excluded.week_ending_date,
    complete=excluded.complete,dirty=false,projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state=excluded.progress_state,error_code=null,updated_at_utc=transaction_timestamp();
  v_sunday:=v_today+(7-extract(isodow from v_today)::integer);
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  if v_work.approval_state='WITHDRAWN' then
    -- Native retained positions supply A/C/H directly. No zero revision, job,
    -- debt, current residual offer, segment repricing or Case mutation here.
    for v_member in select p.component_key,p.approved_source_ex_vat approved_amount,
      p.realised_source_ex_vat realised_amount,p.held_source_ex_vat held_amount,p.source_basis_channel,
      a.id approved_line_id,a.component_kind,a.tax_treatment,a.work_date,a.approved_quantity,a.unit_label
      from private.bpay_next_position p left join private.bpay_next_approved_line a
        on a.revision_id=v_revision.id and a.component_key=p.component_key
      where p.work_id=v_work.id and v_withdrawn_complete and (p.applied_revision_id=v_work.applied_revision_id
        or p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
        or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
        or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0) order by p.component_key loop
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
      v_item.component_key:=v_member.component_key;v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_member.component_key;
      v_item.portion_kind:='WITHDRAWN';v_item.direction:='INFORMATIONAL';v_item.payroll_stage:='GROSS_ADD';
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_member.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_member.component_kind;v_item.tax_treatment:=v_member.tax_treatment;
      v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
      v_item.source_pay_channel:=v_member.source_basis_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_member.approved_amount;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=v_member.held_amount;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.reason_priority:=case when v_wait_after then 70 else 20 end;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      insert into private.bpay_next_workbench_item select (v_item).*;
      select b.* into v_manual from private.bpay_next_work_collection b where b.work_id=v_work.id
        and b.component_key=v_member.component_key and b.collection_mode='MANUAL' and b.case_id is null
        and b.observed_revision_id=v_revision.id and b.manual_outstanding_source_ex_vat>0;
      if v_manual.id is not null and coalesce((private.bpay_next_work_collection_observation_v1(
          v_work.id,v_manual.component_key,v_revision.id)->>'complete')::boolean,false) then
        -- The native WITHDRAWN row above alone owns A/C/H and the WAIT marker.
        v_count:=v_count+1;v_item.id:=gen_random_uuid();v_item.portion_kind:='AVAILABLE';
        v_item.nominal_source_ex_vat:=v_manual.manual_outstanding_source_ex_vat;
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.active_reserved_source_ex_vat:=0;
        v_item.residual_source_ex_vat:=0;v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;
        v_item.excluded_source_ex_vat:=0;v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.written_off_source_ex_vat:=0;v_item.deduction_balance_source_ex_vat:=0;
        v_item.section:='ACTION';v_item.reason_code:='MANUAL_COLLECTION';v_item.reason_priority:=81;
        select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
        if v_item.component_kind is null then v_item.component_kind:=v_original_line.component_kind;end if;
        if v_item.tax_treatment is null then v_item.tax_treatment:=v_original_line.tax_treatment;end if;
        v_item.payroll_stage:=case v_item.tax_treatment when 'TAXABLE' then 'GROSS_DEDUCT' when 'NON_TAXABLE' then 'NET_DEDUCT' else null end;
        insert into private.bpay_next_workbench_item select (v_item).*;
      end if;
    end loop;
    update private.bpay_next_workbench_group set complete=v_count>0,certified_zero=false,
      progress_state=case when v_withdrawn_complete then 'CURRENT' when v_work.applied_revision_id is not null then 'PROCESSING' else 'UNAVAILABLE' end
      where group_key=v_group;
  else
  for v_member in select m.component_key,m.approved_line_id,m.approved_segment_id,m.segment_key,m.component_kind,m.tax_treatment,m.work_date,m.approved_quantity,m.unit_label,m.approved_amount,m.realised_amount,m.held_amount,m.source_pay_channel,m.parent_complete,m.segment_attribution_complete from private.bpay_next_workbench_work_members_v1(v_work.id,v_revision.id) m order by m.component_key,m.segment_key nulls first loop
    v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
    v_item.approved_segment_id:=v_member.approved_segment_id;v_item.segment_key:=v_member.segment_key;
    v_item.component_key:=v_member.component_key;v_item.member_kind:=case when v_member.segment_key is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end;
    v_item.member_key:=coalesce(v_member.segment_key,v_member.component_key);v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;
    v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;v_item.work_date:=v_member.work_date;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;
    v_item.candidate_reference:=v_metadata.tms_ref;v_item.client_display_name:=v_revision.client_display_name;
    v_item.timesheet_reference:=v_revision.timesheet_reference;v_item.component_kind:=v_member.component_kind;
    v_item.tax_treatment:=v_member.tax_treatment;v_item.source_pay_channel:=v_member.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:='PAYMENT';v_item.payroll_stage:='GROSS_ADD';v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
    v_item.amount_known:=coalesce(v_complete,false) and v_member.parent_complete and v_member.segment_attribution_complete;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_complete then 'CURRENT' else 'PROCESSING' end;v_item.updated_at_utc:=transaction_timestamp();
    v_reason:='READY';v_priority:=200;v_section:='READY';
    if v_revision.id is null or v_target is null or v_work.week_ending_date is null
      or v_native_period->'valid' is distinct from 'true'::jsonb then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif v_work.approval_state<>'APPROVED' then v_reason:='MISSING_APPROVAL';v_priority:=21;v_section:='BLOCKED';
    elsif not v_complete then v_reason:='APPLY_PENDING';v_priority:=30;v_section:='BLOCKED';
    elsif not v_member.parent_complete then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif not v_member.segment_attribution_complete then v_reason:='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE';v_priority:=12;v_section:='BLOCKED';end if;
    if v_item.amount_known then
      v_positive:=greatest(v_member.approved_amount-v_member.realised_amount-v_member.held_amount,0);
      v_recovery:=greatest(v_member.realised_amount-v_member.approved_amount,0);
      v_overhang:=greatest(v_member.held_amount-greatest(v_member.approved_amount-v_member.realised_amount,0),0);
      v_item.nominal_source_ex_vat:=v_positive;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=0;
      v_item.written_off_source_ex_vat:=0;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=v_positive;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=v_recovery;v_item.held_overhang_source_ex_vat:=v_overhang;
    else v_positive:=null;v_recovery:=null;v_overhang:=null;end if;
    select a.run_id,a.command_id,a.module_epoch,a.selected_source_ex_vat into v_accepted from private.bpay_next_workbench_accepted_work_v1(v_work.id,v_member.component_key,v_member.segment_key) a;
    if found then
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.selected_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.selected_source_ex_vat is not null;
      if v_priority>50 then v_reason:='IN_PREPARATION';v_priority:=50;v_section:='WAITING';end if;
    end if;
    v_snooze:=private.bpay_next_snooze_at_cut_v1(v_work.id,v_revision.id,v_member.segment_key,v_member.component_key,v_sequence,v_today);
    if v_snooze is not null then
      v_item.snooze_id:=(v_snooze->>'id')::uuid;v_item.snooze_version:=(v_snooze->>'version')::bigint;
      v_item.snooze_scope:=v_snooze->>'scope_kind';v_item.snooze_until_date:=(v_snooze->>'until_date')::date;
      if v_positive>0 then v_item.excluded_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work.id and s.active
      and (s.until_date is null or s.until_date>=v_today) and ((s.scope_kind='SEGMENT' and s.segment_key=v_member.segment_key)
        or s.scope_kind='WHOLE_TIMESHEET' or (s.scope_kind='EXPENSE_LINE' and s.component_key=v_member.component_key)
        or (s.scope_kind='EXPENSES_ALL' and v_member.component_kind in ('EXPENSE','MILEAGE')))) then
      v_item.stale_snooze_code:=case when v_member.segment_key is not null and v_member.approved_segment_id is null
        then 'SNOOZE_SEGMENT_STALE' else 'SNOOZE_BASIS_STALE' end;
    end if;
    if v_priority>100 and v_work.week_ending_date-6>v_today then
      v_reason:='FUTURE_WEEK';v_priority:=100;v_section:='WAITING';v_item.eligible_from_date:=v_work.week_ending_date-6;
    elsif v_priority>110 and v_snooze is not null and v_positive>0 then v_reason:='SNOOZED';v_priority:=110;v_section:='WAITING';end if;
    v_item.query_qualified:=coalesce(v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED')
      and v_member.component_kind in ('WORK','PROTECTED_WORK') and v_source_event is not null
      and v_revision.physical_timesheet_id is not null and v_item.amount_known and v_item.included_source_ex_vat>0,false);
    if v_item.query_qualified then v_item.query_state:='UNAVAILABLE';end if;
    v_item.section:=v_section;v_item.reason_code:=v_reason;v_item.reason_priority:=v_priority;
    v_item.pay_hold:=coalesce(v_pay_hold.pay_on_hold,false);v_item.pay_hold_reason:=v_pay_hold.pay_on_hold_reason;
    v_item.selectable:=v_section='READY' and v_item.amount_known;v_base:=v_item;
    if not v_item.amount_known or v_positive>0 or (v_member.approved_amount=0 and v_member.realised_amount=0
      and v_member.held_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null) then
      v_item.portion_kind:=case when not v_item.amount_known then 'EVIDENCE' when v_positive=0 then 'ZERO' else 'AVAILABLE' end;
      -- WB40: only a new positive offer is blocked. The saved v_base keeps
      -- native H/R and accepted Draft selection untouched (owner E1/E3).
      if not v_item.is_selected and not v_hold_canonical and v_item.reason_priority>15 then
        v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=15;v_item.section:='BLOCKED';
      elsif not v_item.is_selected and v_item.pay_hold and v_item.reason_priority>40 then
        v_item.reason_code:='PAY_ON_HOLD';v_item.reason_priority:=40;v_item.section:='BLOCKED';
      end if;
      v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      -- WB-07: query classification belongs only to this new positive portion.
      -- The original pre-query v_base/v_priority remain unchanged for H/R.
      if v_item.query_qualified and v_item.portion_kind='AVAILABLE' then
        if not v_query_loaded then
          v_query_gate:=null;v_query_state:='UNAVAILABLE';
          begin
            v_query_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
          exception when others then
            v_query_gate:=null;
          end;
          v_query_loaded:=true;
          -- Guard object types separately: malformed scalar replies cannot
          -- reach the object-minus-keys operators or escape the unavailable path.
          if jsonb_typeof(v_query_gate)='object' then
            if (v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
                and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
                and v_query_gate->'ok'='true'::jsonb and v_query_gate->>'code'='OK'
                and jsonb_typeof(v_query_gate->'code')='string'
                and jsonb_typeof(v_query_gate->'blocked')='boolean'
                and jsonb_typeof(v_query_gate->'query_state_sha256')='string'
                and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is true then
              v_query_scope:=v_query_gate->'scope';
              if jsonb_typeof(v_query_scope)='object' then
                if (v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
                    'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
                    and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
                      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
                    and v_query_scope->'root_timesheet_id'=to_jsonb(v_revision.physical_timesheet_id::text)
                    and v_query_scope->'family_booking_id'=to_jsonb(v_work.booking_id)
                    and v_query_scope->'candidate_id'=to_jsonb(v_work.candidate_id::text)
                    and v_query_scope->'client_id'=to_jsonb(v_client::text)
                    and v_query_scope->'contract_id'=to_jsonb(v_work.contract_id::text)
                    and v_query_scope->'week_ending_date'=to_jsonb(v_work.week_ending_date::text)
                    and v_query_scope->'root_version'=to_jsonb(v_revision.physical_timesheet_version::text)
                    and ((v_query_scope->'target_family_id'='null'::jsonb
                        and v_query_scope->'family_bound_version'='null'::jsonb)
                      or (jsonb_typeof(v_query_scope->'target_family_id')='string'
                        and v_query_scope->>'target_family_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        and jsonb_typeof(v_query_scope->'family_bound_version')='string'
                        and v_query_scope->>'family_bound_version' ~ '^[1-9][0-9]{0,18}$'
                        and (char_length(v_query_scope->>'family_bound_version')<19
                          or (v_query_scope->>'family_bound_version') collate "C"<='9223372036854775807' collate "C")))) is true then
                  v_query_state:=case when v_query_gate->'blocked'='true'::jsonb then 'OPEN' else 'CLEAR' end;
                  v_query_exclusions:=private.bpay_next_source_query_segment_exclusions_v1(v_revision.id,v_query_gate);
                end if;
              end if;
            end if;
          end if;
        end if;
        v_segment_query_state:=v_query_state;
        if v_query_state='OPEN' and v_query_exclusions is not null and v_member.segment_key is not null then
          v_segment_query_state:=case when exists(select 1 from jsonb_array_elements(v_query_exclusions) reason(value)
            where reason.value->>'segment_key'=v_member.segment_key) then 'OPEN' else 'CLEAR' end;
        end if;
        v_item.query_state:=v_segment_query_state;
        if v_segment_query_state='UNAVAILABLE' and v_item.reason_priority>60 then
          v_item.reason_code:='PAY_QUERY_UNAVAILABLE';v_item.reason_priority:=60;v_item.section:='BLOCKED';
        elsif v_segment_query_state='OPEN' and v_item.reason_priority>61 then
          v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION';
        end if;
        v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      end if;
      -- A future positive portion only. Numeric priorities alone cannot
      -- supersede Future/Snoozed WAITING or any accepted/reserved history.
      if not v_item.is_selected and v_item.portion_kind='AVAILABLE' then
        if v_item.tax_treatment='TAXABLE' and v_item.component_kind in ('WORK','PROTECTED_WORK','ADDITIONAL','ADJUSTMENT')
          and v_item.source_pay_channel in ('PAYE','UMBRELLA') and v_target in ('PAYE','UMBRELLA')
          and v_item.source_pay_channel<>v_target then
          v_item.channel_note_code:='CHANNEL_RESOLVER';
        end if;
        if v_item.section='READY' and v_item.amount_known and v_item.included_source_ex_vat>0
          and v_item.tax_treatment='TAXABLE' and v_need_operation is not null then
          v_resolution_need:=private.bpay_next_component_resolution_need_v1(v_work.id,v_revision.id,
            v_member.approved_line_id,v_member.approved_segment_id,v_target,v_target_umbrella,v_target_vat,v_resolution_date);
          if v_resolution_need is not null then
            v_item.reason_code:=v_resolution_need->>'reason_code';v_item.reason_priority:=75;
            v_item.channel_note_code:='CHANNEL_RESOLUTION_REQUIRED';v_item.section:='ACTION';v_item.selectable:=false;
          end if;
        end if;
      end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    elsif v_recovery>0 then
      v_item.portion_kind:='RECOVERY';v_item.direction:='INFORMATIONAL';v_item.selectable:=false;
      select b.* into v_manual from private.bpay_next_work_collection b where b.work_id=v_work.id
        and b.component_key=v_member.component_key and b.collection_mode='MANUAL'
        and b.observed_revision_id=v_revision.id;
      if v_manual.id is not null then
        v_item.section:='ACTION';v_item.reason_priority:=81;
        -- This informational liability never borrows a frozen accepted offer.
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
        v_item.accepted_run_id:=null;v_item.accepted_command_id:=null;v_item.accepted_module_epoch:=null;
        v_item.reason_code:=case when v_manual.case_id is not null then 'MANUAL_COLLECTION_REFERENCE'
          when v_manual.manual_outstanding_source_ex_vat is null then 'MANUAL_COLLECTION_UNAVAILABLE' else 'MANUAL_COLLECTION' end;
        v_item.nominal_source_ex_vat:=case when v_manual.case_id is not null then 0 else v_recovery end;
        if v_item.component_kind is null then
          select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
          v_item.component_kind:=v_original_line.component_kind;v_item.tax_treatment:=v_original_line.tax_treatment;
        end if;
        v_item.payroll_stage:=case v_item.tax_treatment when 'TAXABLE' then 'GROSS_DEDUCT' when 'NON_TAXABLE' then 'NET_DEDUCT' else null end;
      elsif v_priority>83 then v_item.reason_code:='RECOVERY_MANAGED';v_item.reason_priority:=83;v_item.section:='ACTION';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_item.amount_known and v_member.held_amount>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.active_reserved_source_ex_vat:=v_member.held_amount;v_item.nominal_source_ex_vat:=v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      if v_positive>0 or v_recovery>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_priority>111 then v_item.reason_code:='RESERVED';v_item.reason_priority:=111;v_item.section:='WAITING';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_base.amount_known and (v_positive>0 or v_recovery>0 or v_member.held_amount>0
      or (v_member.approved_amount=0 and v_member.realised_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null)) then
      select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
        sum(i.residual_source_ex_vat) residual,
        sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
        into v_totals from private.bpay_next_workbench_item i where i.group_key=v_group
          and i.component_key=v_member.component_key and i.member_kind=v_base.member_kind and i.member_key=v_base.member_key;
      if (v_totals.a,v_totals.c,v_totals.h,v_totals.residual,v_totals.split_residual)
        is distinct from (v_member.approved_amount,v_member.realised_amount,v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_NATIVE_MEMBER_NOT_CONSERVED';end if;
    end if;
  end loop;
  end if;
  -- A known retained manual liability is independent of unavailable positive
  -- Source classification. Keep that Evidence; never certify a new offer.
  update private.bpay_next_workbench_item i set reason_code='MANUAL_COLLECTION_UNAVAILABLE'
    from private.bpay_next_work_collection b where i.group_key=v_group and i.work_id=b.work_id
      and i.component_key=b.component_key and i.portion_kind='EVIDENCE' and not i.amount_known
      and not i.is_selected and b.collection_mode='MANUAL' and b.case_id is null
      and b.observed_revision_id=v_revision.id and b.manual_outstanding_source_ex_vat is null;
  if v_work.approval_state='APPROVED' and v_work.current_revision_id=v_work.applied_revision_id then
    for v_manual in select b.* from private.bpay_next_work_collection b where b.work_id=v_work.id
      and b.collection_mode='MANUAL' and b.case_id is null and b.observed_revision_id=v_revision.id
      and b.manual_outstanding_source_ex_vat>0 and not exists(select 1 from private.bpay_next_workbench_item i
        where i.group_key=v_group and i.component_key=b.component_key and i.amount_known) order by b.component_key loop
      v_manual_observation:=private.bpay_next_work_collection_observation_v1(v_work.id,v_manual.component_key,v_revision.id);
      if not coalesce((v_manual_observation->>'complete')::boolean,false) then continue;end if;
      select pos.* into strict v_manual_position from private.bpay_next_position pos where pos.work_id=v_work.id and pos.component_key=v_manual.component_key;
      select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.component_key:=v_manual.component_key;
      v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_manual.component_key;v_item.portion_kind:='RECOVERY';
      v_item.direction:='INFORMATIONAL';v_item.payroll_stage:=case v_original_line.tax_treatment when 'TAXABLE' then 'GROSS_DEDUCT' when 'NON_TAXABLE' then 'NET_DEDUCT' else null end;
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_original_line.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_original_line.component_kind;v_item.tax_treatment:=v_original_line.tax_treatment;
      v_item.source_pay_channel:=v_manual.original_source_pay_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_manual.manual_outstanding_source_ex_vat;
      v_item.approved_source_ex_vat:=v_manual_position.approved_source_ex_vat;v_item.realised_source_ex_vat:=v_manual_position.realised_source_ex_vat;
      v_item.active_reserved_source_ex_vat:=v_manual_position.held_source_ex_vat;
      v_item.residual_source_ex_vat:=v_manual_position.approved_source_ex_vat-v_manual_position.realised_source_ex_vat-v_manual_position.held_source_ex_vat;
      v_item.gross_due_source_ex_vat:=(v_manual_observation->>'positive_source_ex_vat')::numeric;
      v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=v_item.gross_due_source_ex_vat;
      v_item.recovery_source_ex_vat:=(v_manual_observation->>'negative_source_ex_vat')::numeric;
      v_item.held_overhang_source_ex_vat:=(v_manual_observation->>'held_overhang_source_ex_vat')::numeric;v_item.written_off_source_ex_vat:=0;
      v_item.section:='ACTION';v_item.reason_code:='MANUAL_COLLECTION';v_item.reason_priority:=81;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      if v_item.included_source_ex_vat+v_item.excluded_source_ex_vat-v_item.recovery_source_ex_vat-v_item.held_overhang_source_ex_vat
        is distinct from v_item.residual_source_ex_vat then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_MANUAL_DISPLAY_NOT_CONSERVED';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end loop;
  end if;
  if v_count=0 then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.work_id:=v_work.id;
    v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;v_item.selected_amount_known:=false;v_item.is_selected:=false;v_item.selectable:=false;
    v_item.section:='BLOCKED';v_item.reason_code:=case when v_work.approval_state<>'APPROVED' then 'MISSING_APPROVAL' else 'DATA_UNAVAILABLE' end;
    if v_work.approval_state='WITHDRAWN' then
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
    end if;
    v_item.reason_priority:=case when v_work.approval_state='WITHDRAWN' then case when v_wait_after then 70 else 20 end
      when v_work.approval_state<>'APPROVED' then 21 else 10 end;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_work.approval_state='WITHDRAWN' and v_work.applied_revision_id is not null
      and not v_withdrawn_complete then 'PROCESSING' else 'UNAVAILABLE' end;
    v_item.updated_at_utc:=transaction_timestamp();v_item.target_pay_channel:=v_target;
    v_zero:=coalesce(v_complete,false) and v_revision.certified_zero and v_revision.approved_source_ex_vat=0
      and v_revision.expected_line_count=0 and v_target is not null and v_work.week_ending_date is not null
      and v_native_period->'valid'='true'::jsonb
      and not exists(select 1 from private.bpay_next_position p where p.work_id=v_work.id);
    if v_zero then
      v_item.section:='READY';v_item.reason_code:='CERTIFIED_ZERO';v_item.reason_priority:=200;v_item.progress_state:='CURRENT';
      if v_work.week_ending_date-6>v_today then
        v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_work.week_ending_date-6;
      end if;
    end if;
    insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=g.complete and x.unknown=0,certified_zero=coalesce(v_complete,false) and coalesce(v_revision.certified_zero,false)
    from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,
      count(*) filter(where not amount_known and not (member_kind='GROUP_EVIDENCE' and coalesce(v_zero,false))) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  update private.bpay_next_workbench_group g set stale_snooze_count=x.n,
    stale_snooze_code=case when x.n>0 then 'SNOOZE_SEGMENT_STALE' end
    from(select count(*) n from private.bpay_next_snooze s where s.work_id=v_work.id and s.scope_kind='SEGMENT'
      and s.active and (s.until_date is null or s.until_date>=v_today) and not exists(select 1
        from private.bpay_next_approved_segment a where a.revision_id=v_revision.id and a.segment_key=s.segment_key)) x
    where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;
create or replace function private.bpay_next_append_work_skip_v1(p_run_id uuid,p_work_id uuid,p_revision_id uuid)
returns text language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_work private.bpay_next_work%rowtype;
 v_revision private.bpay_next_work_revision%rowtype;v_gate jsonb;v_skip text;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id;
 -- Accepted acknowledgements use the original per-Work skip, including a
 -- held Work with no work_choice. Never read newer hold to amend that offer.
 if v_run.accepted_choice_descriptor is not null then
  select skip_code into v_skip from private.bpay_next_run_choice_member
   where run_id=p_run_id and work_id=p_work_id and member_kind='WORK';
  if found then return v_skip;end if;
  if exists(select 1 from private.bpay_next_work_choice where run_id=p_run_id and work_id=p_work_id
    and expected_revision_id=p_revision_id) then return null;end if;
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_MISSING';
 end if;
 select src.* into strict v_work from private.bpay_next_work src where id=p_work_id for share;
 if p_revision_id is not null and not exists(select 1 from private.bpay_next_work_revision
   where work_id=p_work_id and id=p_revision_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_REVISION_CONFLICT';end if;
 select src.* into v_revision from private.bpay_next_work_revision src
   where work_id=p_work_id and id=p_revision_id;
 if v_work.approval_state is distinct from 'APPROVED' or v_revision.approved_at_utc is null
   or v_revision.sealed_at_utc is null then return 'BPAY_NEXT_SELECTION_WORK_NOT_APPROVED';end if;
 if v_work.current_revision_id is distinct from p_revision_id or v_work.applied_revision_id is distinct from p_revision_id then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if private.bpay_next_work_period_proof_v1(v_work.id,p_revision_id,v_run.snooze_business_date)->'valid' is distinct from 'true'::jsonb then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if v_work.week_ending_date-6>v_run.snooze_business_date then
  return 'BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';end if;
 if not exists(select 1 from public.timesheets t join public.timesheets_financials tf
   on tf.timesheet_id=t.timesheet_id and tf.is_current
   where t.booking_id=v_work.booking_id and t.is_current) then
  return 'BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';end if;
 if exists(select 1 from public.timesheets t join public.timesheets_financials tf
   on tf.timesheet_id=t.timesheet_id and tf.is_current=true
   where t.booking_id=v_work.booking_id and t.is_current=true and tf.pay_on_hold is true) then
  return 'BPAY_NEXT_SELECTION_ON_HOLD';end if;
 if not exists(select 1 from private.bpay_next_revision_worker_basis where revision_id=p_revision_id) then
  raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE';end if;
 if exists(select 1 from private.bpay_next_snooze sn
   join lateral(select ev.after_state from private.bpay_next_snooze_event ev
     where ev.snooze_id=sn.id and ev.control_sequence<=v_run.snooze_control_sequence
     order by ev.control_sequence desc limit 1) e on true
   join private.bpay_next_revision_worker_basis b on b.revision_id=p_revision_id
   where sn.work_id=p_work_id and sn.scope_kind='WHOLE_TIMESHEET' and (e.after_state->>'active')::boolean
     and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
     and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis) then
  return 'BPAY_NEXT_SELECTION_SNOOZED';end if;
 if v_revision.source_kind in ('SOURCE','PROTECTED') then
  begin
   perform private.weekly_source_pay_query_admit_v2();
   v_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
   if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'code' is distinct from 'OK'
     or jsonb_typeof(v_gate->'blocked') is distinct from 'boolean' then
    return 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end if;
   if v_gate->'blocked'='true'::jsonb and
     private.bpay_next_source_query_segment_exclusions_v1(p_revision_id,v_gate) is null then return 'SOURCE_PAY_QUERY_OPEN';end if;
  exception when others then return 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;
 end if;
 return null;
end
$function$;
create or replace function private.bpay_next_create_preparation_v1(
 p_run_id uuid,p_actor_user_id uuid,p_pay_date date,p_descriptor jsonb,p_command_id uuid,p_guard jsonb
) returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
 v_run private.bpay_next_pay_run%rowtype;v_result jsonb;v_ns jsonb;v_intent jsonb;
 v_row jsonb;v_array text;v_keys text[];v_field text;v_id uuid;v_header jsonb;
 v_query_admitted boolean:=true;v_policy text:='NONE';v_channel text:='ALL';
 v_reason text;v_proof uuid;v_guard jsonb;v_scope jsonb;v_policy_args jsonb;
 v_locked_candidates uuid[];v_locked_works uuid[];v_locked_revisions uuid[];v_locked_cases uuid[];
 v_candidate uuid;v_work uuid;v_revision uuid;v_sequence bigint;v_gate jsonb;v_query_exclusions jsonb;v_member record;
 v_memory boolean:=p_descriptor ? 'memory_baseline_run_id';v_baseline uuid;v_page_no bigint;v_lines uuid[];v_subset record;
 v_mode text;v_selected boolean;v_count bigint;v_code text;v_week_start date;v_work_count bigint:=0;
begin
 if p_guard is not null then
  if jsonb_typeof(p_guard) is distinct from 'object' or not(p_guard ?& array['decision','reason','reauth_proof_id'])
   or p_guard-array['decision','reason','reauth_proof_id']<>'{}'::jsonb
   or jsonb_typeof(p_guard->'decision') is distinct from 'string'
   or p_guard->>'decision' not in ('UMBRELLA_ONLY','VERIFY_BOTH','VERIFY_PAYE') then
   raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  v_policy:=p_guard->>'decision';v_channel:=case v_policy when 'UMBRELLA_ONLY' then 'UMBRELLA' when 'VERIFY_PAYE' then 'PAYE' else 'ALL' end;
  if v_policy='UMBRELLA_ONLY' then
   if p_guard->'reason'<>'null'::jsonb or p_guard->'reauth_proof_id'<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  else
   if jsonb_typeof(p_guard->'reason') is distinct from 'string' or btrim(p_guard->>'reason')=''
    or octet_length(p_guard->>'reason') not between 1 and 500
    or jsonb_typeof(p_guard->'reauth_proof_id') is distinct from 'string'
    or p_guard->>'reauth_proof_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
   v_reason:=p_guard->>'reason';v_proof:=(p_guard->>'reauth_proof_id')::uuid;
  end if;
 end if;
 if p_command_id is null or jsonb_typeof(p_descriptor) is distinct from 'object'
   or octet_length(p_descriptor::text)>32768
   or not(p_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision'])
   or p_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id']<>'{}'::jsonb then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';
 end if;
 if v_memory and p_descriptor->'memory_baseline_run_id'<>'null'::jsonb and
  (jsonb_typeof(p_descriptor->'memory_baseline_run_id')<>'string' or p_descriptor->>'memory_baseline_run_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 v_ns:=p_descriptor->'namespace';v_intent:=p_descriptor->'intent';
 if jsonb_typeof(v_ns) is distinct from 'object'
   or not(v_ns ?& array['agency_id','owner_epoch','actor_user_id','official_pay_date','scope'])
   or v_ns-array['agency_id','owner_epoch','actor_user_id','official_pay_date','scope']<>'{}'::jsonb
   or jsonb_typeof(v_ns->'scope') is distinct from 'object'
   or not(v_ns->'scope' ?& array['candidate_id','client_id','channel'])
   or (v_ns->'scope')-array['candidate_id','client_id','channel']<>'{}'::jsonb
   or jsonb_typeof(v_intent) is distinct from 'object'
   or not(v_intent ?& array['mode','candidates','works','components','cases'])
   or v_intent-array['mode','candidates','works','components','cases']<>'{}'::jsonb
   or jsonb_typeof(v_intent->'mode') is distinct from 'string'
   or v_intent->>'mode' not in ('ALL_MATCHING','NONE') then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';
 end if;
 foreach v_array in array array['candidates','works','components','cases'] loop
  if jsonb_typeof(v_intent->v_array) is distinct from 'array'
    or jsonb_array_length(v_intent->v_array)>1000 then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  v_keys:=case v_array when 'candidates' then array['candidate_id','selected']
    when 'works' then array['candidate_id','work_id','expected_revision_id','selected','mode']
    when 'components' then array['candidate_id','work_id','expected_revision_id','approved_line_id','selected']
    else array['candidate_id','case_component_id','selected'] end;
  for v_row in select value from jsonb_array_elements(v_intent->v_array) loop
   if jsonb_typeof(v_row) is distinct from 'object' or not(v_row ?& v_keys)
     or v_row-v_keys<>'{}'::jsonb or jsonb_typeof(v_row->'selected') is distinct from 'boolean' then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
   foreach v_field in array v_keys loop
    if v_field in ('selected','mode') then continue;end if;
    if jsonb_typeof(v_row->v_field) is distinct from 'string'
      or v_row->>v_field!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
     raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
   end loop;
   if v_array='works' and (jsonb_typeof(v_row->'mode') is distinct from 'string'
      or v_row->>'mode' not in ('ALL','SUBSET')) then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  end loop;
  if exists(select 1 from jsonb_array_elements(v_intent->v_array) a(value)
    group by case v_array when 'candidates' then a.value->>'candidate_id'
      when 'works' then a.value->>'work_id' when 'components' then a.value->>'approved_line_id'
      else a.value->>'case_component_id' end having count(*)>1) then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 end loop;
 if jsonb_typeof(v_ns->'agency_id') is distinct from 'string'
   or v_ns->>'agency_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_ns->'actor_user_id') is distinct from 'string'
   or v_ns->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_ns->'owner_epoch') is distinct from 'string'
   or v_ns->>'owner_epoch'!~'^[1-9][0-9]{0,18}$'
   or jsonb_typeof(v_ns->'official_pay_date') is distinct from 'string'
   or v_ns->>'official_pay_date'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   or jsonb_typeof(v_ns->'scope'->'channel') is distinct from 'string'
   or v_ns->'scope'->>'channel' not in ('ALL','PAYE','UMBRELLA') then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 foreach v_field in array array['candidate_id','client_id'] loop
  if v_ns->'scope'->v_field<>'null'::jsonb and (jsonb_typeof(v_ns->'scope'->v_field) is distinct from 'string'
    or v_ns->'scope'->>v_field!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 end loop;
 if jsonb_typeof(p_descriptor->'workbench_revision') is distinct from 'string'
   or p_descriptor->>'workbench_revision'!~'^(0|[1-9][0-9]{0,18})$'
   or jsonb_typeof(p_descriptor->'adoption_revision') is distinct from 'string'
   or p_descriptor->>'adoption_revision'!~'^(0|[1-9][0-9]{0,18})$' then
  raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
 -- An exact receipt replay acquires no new source/cut/FK-parent locks.
 select src.* into v_run from private.bpay_next_pay_run src where id=p_run_id;
 if found then
  if v_run.accepted_choice_descriptor is null then
   raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_RUN_NOT_EMPTY';end if;
  v_result:=private.bpay_next_create_preparation_v1(p_run_id,p_actor_user_id,p_pay_date);
  select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id;
  if v_run.accepted_choice_descriptor is distinct from p_descriptor
    or v_run.accepted_choice_command_id is distinct from p_command_id then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if (v_run.paye_guard_decision,v_run.channel_scope,v_run.paye_override_reason,v_run.reauth_proof_id,
    v_run.paye_override_actor) is distinct from (v_policy,v_channel,v_reason,v_proof,
      case when v_policy='NONE' then null::uuid else p_actor_user_id end) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if v_proof is not null then
   perform private.bpay_next_reauth_consume_v1(v_proof,p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_run.paye_guard_scope_sha256,p_command_id);
  end if;
  select agency_sequence into v_sequence from private.bpay_next_command where id=p_command_id;
  return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
    'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
    'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),
    'selected_candidate_count',v_run.selected_candidate_count::text,'empty',v_run.selected_candidate_count=0,'replay',true);
 end if;
 -- Source admission precedes the original module/run/snooze cut locks.
 begin perform private.weekly_source_pay_query_admit_v2();
 exception when lock_not_available then v_query_admitted:=false;end;
 perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 -- FK parent locks precede the inherited snooze command-clock SHARE.
 -- No revision-meta lock: Snooze writers already take clock before meta.
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_candidates from (select candidate.id from public.candidates candidate where exists(select 1 from private.bpay_next_workbench_item i
   where i.candidate_id=candidate.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from unnest(array['candidates','works','components','cases']) kind
     cross join lateral jsonb_array_elements(v_intent->kind) a(value) where a.value->>'candidate_id'=candidate.id::text)
   order by candidate.id for share) parent;
 if exists(select 1 from jsonb_array_elements(v_intent->'candidates') requested
   where not ((requested->>'candidate_id')::uuid=any(v_locked_candidates))) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 perform 1 from public.umbrellas umbrella where exists(select 1 from public.candidates candidate
   join private.bpay_next_workbench_item i on i.candidate_id=candidate.id and i.section='READY' and i.selectable
   where candidate.umbrella_id=umbrella.id
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   order by umbrella.id for share;
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_works from (select w.id from private.bpay_next_work w where exists(select 1 from private.bpay_next_workbench_item i
   where i.work_id=w.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from jsonb_array_elements(v_intent->'works') a(value) where a.value->>'work_id'=w.id::text)
   order by w.id for share) parent;
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_revisions from (select r.id from private.bpay_next_work_revision r where exists(select 1 from private.bpay_next_workbench_item i
   where i.revision_id=r.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from jsonb_array_elements(v_intent->'works') a(value) where a.value->>'expected_revision_id'=r.id::text)
   order by r.work_id,r.id for share) parent;
 select coalesce(array_agg(parent.id),'{}'::uuid[]) into v_locked_cases from (select component.id from private.bpay_next_case_component component where exists(select 1 from private.bpay_next_workbench_item i
   where i.case_component_id=component.id and i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel'))
   or exists(select 1 from jsonb_array_elements(v_intent->'cases') a(value) where a.value->>'case_component_id'=component.id::text)
   order by component.candidate_id,component.id for share) parent;
 if exists(select 1 from private.bpay_next_command c where c.status in ('RECEIVED','ENROLLING'))
   or exists(select 1 from private.bpay_next_job j join private.bpay_next_command c on c.id=j.command_id
   where j.status<>'DONE' and j.candidate_id=any(v_locked_candidates)) then
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPT_INPUT_EARLIER_ECONOMIC_COMMAND_PENDING';end if;
 -- Acquire UPDATE once before the inherited SHARE, avoiding concurrent upgrades.
 perform 1 from private.bpay_next_command_clock where id=1 for update;
 if exists(select 1 from private.bpay_next_command c where c.status in ('RECEIVED','ENROLLING'))
   or exists(select 1 from private.bpay_next_job j join private.bpay_next_command c on c.id=j.command_id
   where j.status<>'DONE' and j.candidate_id=any(v_locked_candidates)) then
  raise exception using errcode='55000',message='BPAY_NEXT_ACCEPT_INPUT_EARLIER_ECONOMIC_COMMAND_PENDING';end if;
 if exists(select 1 from private.bpay_next_command where id=p_command_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_COMMAND_REPLAY_CONFLICT';end if;
 -- A same-run concurrent legacy writer must never make clock->run wait.
 perform 1 from private.bpay_next_pay_run where id=p_run_id for update nowait;
 if found and exists(select 1 from private.bpay_next_pay_run where id=p_run_id and accepted_choice_descriptor is null) then
  raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_RUN_NOT_EMPTY';end if;
 -- Original UUID/actor/date/deadline owner is preserved, including replay.
 v_result:=private.bpay_next_create_preparation_v1(p_run_id,p_actor_user_id,p_pay_date);
 select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id for update;
 if v_run.accepted_choice_descriptor is not null then
  if v_run.accepted_choice_descriptor is distinct from p_descriptor
    or v_run.accepted_choice_command_id is distinct from p_command_id then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if (v_run.paye_guard_decision,v_run.channel_scope,v_run.paye_override_reason,v_run.reauth_proof_id,
    v_run.paye_override_actor) is distinct from (v_policy,v_channel,v_reason,v_proof,
      case when v_policy='NONE' then null::uuid else p_actor_user_id end) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  if v_proof is not null then
   perform private.bpay_next_reauth_consume_v1(v_proof,p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_run.paye_guard_scope_sha256,p_command_id);
  end if;
  select agency_sequence into v_sequence from private.bpay_next_command where id=p_command_id;
  return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
    'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
    'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),
    'selected_candidate_count',v_run.selected_candidate_count::text,'empty',v_run.selected_candidate_count=0,'replay',true);
 end if;
 if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' or v_run.selection_page_count<>0
   or v_run.selection_count<>0 or v_run.selected_candidate_count<>0
   or exists(select 1 from private.bpay_next_run_selection where run_id=p_run_id)
   or exists(select 1 from private.bpay_next_selection_candidate where run_id=p_run_id)
   or exists(select 1 from private.bpay_next_case_selection where run_id=p_run_id)
   or exists(select 1 from private.bpay_next_run_command where run_id=p_run_id)
   or v_run.preparation_expires_at_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_RUN_NOT_EMPTY';end if;
 -- Compare real authenticated identity/context; caller fields are never authority.
 v_header:=public.bpay_next_workbench_page_v1(jsonb_build_object('actor_user_id',p_actor_user_id,
   'kind','HEADER','candidate_id',v_ns->'scope'->'candidate_id','client_id',v_ns->'scope'->'client_id',
   'channel',v_ns->'scope'->>'channel','section','READY','search','','sort','NAME_ASC',
   'group_key',null,'item_id',null,'reason_code',null,'count_selection',null,'after',null,'limit',100));
 if v_ns->>'agency_id' is distinct from v_header->'context'->>'agency_id'
   or v_ns->>'actor_user_id' is distinct from p_actor_user_id::text
   or v_ns->>'owner_epoch' is distinct from v_header->'context'->>'owner_epoch'
   or v_ns->>'official_pay_date' is distinct from v_run.pay_date::text
   or p_descriptor->>'workbench_revision' is distinct from v_header->'context'->>'workbench_revision'
   or p_descriptor->>'adoption_revision' is distinct from v_header->'context'->>'adoption_revision'
   or jsonb_typeof(p_descriptor->'revision') is distinct from 'number'
   or p_descriptor->>'revision'!~'^(0|[1-9][0-9]{0,15})$' then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 -- Validate every deliberate subordinate identity before any accepted cut.
 for v_array,v_row in select k,value from unnest(array['works','components']) k
   cross join lateral jsonb_array_elements(v_intent->k) a(value) loop
  v_work:=(v_row->>'work_id')::uuid;v_candidate:=(v_row->>'candidate_id')::uuid;
  v_revision:=(v_row->>'expected_revision_id')::uuid;
  if not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r
    on r.work_id=w.id and r.id=v_revision where w.id=v_work and w.candidate_id=v_candidate)
    or (v_array='components' and not exists(select 1 from private.bpay_next_approved_line l
      where l.id=(v_row->>'approved_line_id')::uuid and l.revision_id=v_revision)) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 end loop;
 -- WB25: component-only memory overlays retain untouched native defaults.
 -- A supplied parent must match; an absent parent must be current, scoped and
 -- eligible under the original held Work/revision locks. Legacy stays SUBSET.
 if exists(select 1 from jsonb_array_elements(v_intent->'components') c(value)
   where case when exists(select 1 from jsonb_array_elements(v_intent->'works') w(value)
      where w.value->>'work_id'=c.value->>'work_id') then
     not exists(select 1 from jsonb_array_elements(v_intent->'works') w(value)
      where w.value->>'work_id'=c.value->>'work_id' and w.value->>'candidate_id'=c.value->>'candidate_id'
       and w.value->>'expected_revision_id'=c.value->>'expected_revision_id'
       and (v_memory or w.value->>'mode'='SUBSET' and w.value->'selected'='true'::jsonb))
    else not v_memory or not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r on r.work_id=w.id
       join private.bpay_next_workbench_item i on i.work_id=w.id and i.revision_id=r.id
       join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
       where w.id=(c.value->>'work_id')::uuid and w.candidate_id=(c.value->>'candidate_id')::uuid
        and r.id=(c.value->>'expected_revision_id')::uuid and w.current_revision_id=r.id and w.applied_revision_id=r.id
        and i.approved_line_id=(c.value->>'approved_line_id')::uuid and i.candidate_id=w.candidate_id
        and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_ns->'scope'->>'candidate_id' is null or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
        and (v_ns->'scope'->>'client_id' is null or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
        and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')) end) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 if exists(select 1 from jsonb_array_elements(v_intent->'cases') a(value) where not exists(
   select 1 from private.bpay_next_case_component c where c.id=(a.value->>'case_component_id')::uuid
     and c.candidate_id=(a.value->>'candidate_id')::uuid)) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
 -- DR-10: fresh serialized policy admission, before the original member cut.
 v_guard:=private.bpay_next_paye_guard_page_v1(p_actor_user_id,p_run_id,p_descriptor);
 if v_policy='NONE' and (v_guard->>'guard_applies')::boolean then
  raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNRESOLVED';end if;
 if v_policy<>'NONE' and not(v_guard->>'guard_applies')::boolean then
  raise exception using errcode='23514',message='BPAY_NEXT_PAYE_GUARD_CHANGED';end if;
 if v_policy in ('VERIFY_BOTH','VERIFY_PAYE') then
  if (v_guard->>'unexecuted')::boolean then
   raise exception using errcode='55000',message='BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNEXECUTED';end if;
  v_policy_args:=jsonb_build_object('run_id',p_run_id,'command_id',p_command_id,'decision',v_policy,'reason',v_reason,
    'context',p_descriptor-array['intent','revision','memory_baseline_run_id']);
  v_scope:=private.bpay_next_reauth_scope_v1(p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_policy_args,false);
  perform private.bpay_next_reauth_consume_v1(v_proof,p_actor_user_id,'PAYE_SAME_WEEK_OVERRIDE',v_scope->>'scope_sha256',p_command_id);
 end if;
 update private.bpay_next_pay_run set paye_guard_decision=v_policy,channel_scope=v_channel,
  paye_override_actor=case when v_policy='NONE' then null else p_actor_user_id end,
  paye_override_reason=v_reason,reauth_proof_id=v_proof,paye_guard_scope_sha256=v_scope->>'scope_sha256' where id=p_run_id;
 -- The original run boundary retained its snooze cut; no second clock or kind.
 insert into private.bpay_next_run_selection_exclusion(run_id,page_no,candidate_id,actor_user_id)
   select p_run_id,(row_number() over(order by a.value->>'candidate_id')+99)/100,
     (a.value->>'candidate_id')::uuid,p_actor_user_id
   from jsonb_array_elements(v_intent->'candidates') a(value) where a.value->'selected'='false'::jsonb;
 insert into private.bpay_next_run_selection_exclusion(run_id,page_no,work_id,actor_user_id)
   select p_run_id,((select count(*) from private.bpay_next_run_selection_exclusion where run_id=p_run_id)
     +row_number() over(order by a.value->>'work_id')+99)/100,(a.value->>'work_id')::uuid,p_actor_user_id
   from jsonb_array_elements(v_intent->'works') a(value) where a.value->'selected'='false'::jsonb;
 -- No membership may acquire an unheld FK parent after the command clock.
 if exists(select 1 from private.bpay_next_workbench_item i
   where i.section='READY' and i.selectable
     and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
     and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
     and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')
     and (not(i.candidate_id=any(v_locked_candidates))
       or (i.work_id is not null and (not(i.work_id=any(v_locked_works)) or not(i.revision_id=any(v_locked_revisions))))
       or (i.case_component_id is not null and not(i.case_component_id=any(v_locked_cases))))) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 -- The eligible leaf cut is universal; optional remembered-choice semantics remain original.
 perform private.bpay_next_selection_memory_capture_v1(p_run_id,p_descriptor);
 if v_memory then
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,jsonb_build_object('run_id',p_descriptor->'memory_baseline_run_id','scope',v_ns->'scope'));
  with choices as materialized(select * from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent)),
  works as (select l.work_id,l.candidate_id,
    coalesce((ov.value->>'expected_revision_id')::uuid,l.revision_id) expected_revision_id,
    case when bool_and(l.selected) then 'ALL' else 'SUBSET' end selection_mode
   from choices l left join lateral(select value from jsonb_array_elements(v_intent->'works') where value->>'work_id'=l.work_id::text) ov on true
   where l.work_id is not null and l.direction='PAYMENT'
    and (v_policy<>'VERIFY_PAYE' or exists(select 1 from public.candidates c where c.id=l.candidate_id and upper(c.pay_method)='PAYE'))
   group by l.work_id,l.candidate_id,coalesce((ov.value->>'expected_revision_id')::uuid,l.revision_id)
   having bool_or(l.selected)),
  work_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,work_id,expected_revision_id,selection_mode,selected,member_no)
   select p_run_id,'WORK',w.work_id,w.candidate_id,w.work_id,w.expected_revision_id,w.selection_mode,true,
    row_number() over(order by (w.selection_mode='SUBSET') desc,w.work_id) from works w returning candidate_id),
  case_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,case_component_id,selected,member_no)
   select p_run_id,'CASE_COMPONENT',l.case_component_id,l.candidate_id,l.case_component_id,l.selected,
    row_number() over(order by l.candidate_id,l.case_component_id) from choices l
   where l.case_component_id is not null
    and (v_channel='ALL' or exists(select 1 from public.candidates c where c.id=l.candidate_id and upper(c.pay_method)=v_channel)) and
    (exists(select 1 from work_members w where w.candidate_id=l.candidate_id)
     or exists(select 1 from choices payment where payment.candidate_id=l.candidate_id and payment.case_component_id is not null and payment.direction='PAYMENT' and payment.selected))
   returning candidate_id)
  select (select count(*) from work_members)+(select count(*) from case_members) into v_count;
 else
 -- Retain Work and Case membership in one statement snapshot.
 with work_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,
   work_id,expected_revision_id,selection_mode,selected,member_no)
 select p_run_id,'WORK',i.work_id,i.candidate_id,i.work_id,
   coalesce((ov.value->>'expected_revision_id')::uuid,i.revision_id),coalesce(ov.value->>'mode','ALL'),true,
   row_number() over(order by (coalesce(ov.value->>'mode','ALL')='SUBSET') desc,i.work_id)
 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g
   on g.group_key=i.group_key and g.candidate_id=i.candidate_id
 left join lateral(select a.value from jsonb_array_elements(v_intent->'works') a(value)
   where a.value->>'work_id'=i.work_id::text) ov on true
 where i.section='READY' and i.selectable and i.work_id is not null and i.amount_known
   and i.direction='PAYMENT' and i.portion_kind in ('AVAILABLE','ZERO')
   and (v_policy<>'VERIFY_PAYE' or exists(select 1 from public.candidates c where c.id=i.candidate_id and upper(c.pay_method)='PAYE'))
   and g.complete and not g.dirty and g.progress_state='CURRENT' and i.progress_state='CURRENT'
   and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
   and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
   and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
   and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')
   and not exists(select 1 from private.bpay_next_run_selection_exclusion e where e.run_id=p_run_id
     and (e.candidate_id=i.candidate_id or e.work_id=i.work_id))
   and (v_intent->>'mode'='ALL_MATCHING' or ov.value->'selected'='true'::jsonb or exists(
     select 1 from jsonb_array_elements(v_intent->'candidates') a(value)
       where a.value->>'candidate_id'=i.candidate_id::text and a.value->'selected'='true'::jsonb))
 group by i.work_id,i.candidate_id,coalesce((ov.value->>'expected_revision_id')::uuid,i.revision_id),coalesce(ov.value->>'mode','ALL') returning candidate_id),
 case_members as (insert into private.bpay_next_run_choice_member(run_id,member_kind,member_id,candidate_id,case_component_id,selected,member_no)
 select p_run_id,'CASE_COMPONENT',i.case_component_id,i.candidate_id,i.case_component_id,
   coalesce((ov.value->>'selected')::boolean,true),row_number() over(order by i.candidate_id,i.case_component_id)
 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g
   on g.group_key=i.group_key and g.candidate_id=i.candidate_id
 left join lateral(select a.value from jsonb_array_elements(v_intent->'cases') a(value)
   where a.value->>'case_component_id'=i.case_component_id::text) ov on true
 where i.section='READY' and i.selectable and i.case_component_id is not null
   and (v_channel='ALL' or exists(select 1 from public.candidates c where c.id=i.candidate_id and upper(c.pay_method)=v_channel))
   and g.complete and not g.dirty and g.progress_state='CURRENT' and i.progress_state='CURRENT'
   and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
   and (v_ns->'scope'->'candidate_id'='null'::jsonb or i.candidate_id=(v_ns->'scope'->>'candidate_id')::uuid)
   and (v_ns->'scope'->'client_id'='null'::jsonb or i.client_id=(v_ns->'scope'->>'client_id')::uuid)
   and (v_ns->'scope'->>'channel'='ALL' or i.target_pay_channel=v_ns->'scope'->>'channel')
   and not exists(select 1 from private.bpay_next_run_selection_exclusion e where e.run_id=p_run_id
     and e.candidate_id=i.candidate_id)
   and (exists(select 1 from work_members m where m.candidate_id=i.candidate_id)
     or exists(select 1 from private.bpay_next_workbench_item payment
       join private.bpay_next_workbench_group payment_group
         on payment_group.group_key=payment.group_key and payment_group.candidate_id=payment.candidate_id
       left join lateral(select a.value from jsonb_array_elements(v_intent->'cases') a(value)
         where a.value->>'case_component_id'=payment.case_component_id::text) chosen on true
       where payment.candidate_id=i.candidate_id and payment.case_component_id is not null
         and payment.section='READY' and payment.selectable and payment.direction='PAYMENT'
         and payment.amount_known and payment.portion_kind in ('AVAILABLE','ZERO')
         and payment_group.complete and not payment_group.dirty
         and payment_group.progress_state='CURRENT' and payment.progress_state='CURRENT'
         and payment.projection_revision=payment_group.projection_revision
         and payment.financial_view_revision=payment_group.financial_view_revision
         and coalesce((chosen.value->>'selected')::boolean,true)
         and (v_ns->'scope'->'client_id'='null'::jsonb or payment.client_id=(v_ns->'scope'->>'client_id')::uuid)
         and (v_ns->'scope'->>'channel'='ALL' or payment.target_pay_channel=v_ns->'scope'->>'channel')
         and (v_intent->>'mode'='ALL_MATCHING' or chosen.value->'selected'='true'::jsonb
           or exists(select 1 from jsonb_array_elements(v_intent->'candidates') a(value)
             where a.value->>'candidate_id'=i.candidate_id::text and a.value->'selected'='true'::jsonb))))
 group by i.case_component_id,i.candidate_id,coalesce((ov.value->>'selected')::boolean,true) returning candidate_id)
 select (select count(*) from work_members)+(select count(*) from case_members) into v_count;
 end if;
 v_week_start:=date_trunc('week',v_run.snooze_business_date::timestamp)::date;
 for v_member in select m.*,w.approval_state,w.current_revision_id,w.applied_revision_id,w.week_ending_date,
   r.physical_timesheet_id,r.source_kind,r.approved_at_utc,r.sealed_at_utc,upper(c.pay_method) as pay_channel
   from private.bpay_next_run_choice_member m join private.bpay_next_work w on w.id=m.work_id
   join private.bpay_next_work_revision r on r.work_id=w.id and r.id=m.expected_revision_id
   join public.candidates c on c.id=m.candidate_id
   where m.run_id=p_run_id and m.member_kind='WORK' order by m.member_no loop
  v_code:=null;
  if v_policy='UMBRELLA_ONLY' and v_member.pay_channel='PAYE' then
   v_code:='PAYE_EXCLUDED_BY_SAME_WEEK_GUARD';
  elsif v_member.approval_state<>'APPROVED' or v_member.approved_at_utc is null or v_member.sealed_at_utc is null then
   v_code:='BPAY_NEXT_SELECTION_WORK_NOT_APPROVED';
  elsif v_member.current_revision_id is distinct from v_member.expected_revision_id
    or v_member.applied_revision_id is distinct from v_member.expected_revision_id then
   v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif private.bpay_next_work_period_proof_v1(v_member.work_id,v_member.expected_revision_id,v_run.snooze_business_date)->'valid' is distinct from 'true'::jsonb then
   v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif v_member.week_ending_date-6>v_run.snooze_business_date then v_code:='BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE';
  elsif not exists(select 1 from private.bpay_next_work w
    join public.timesheets t on t.booking_id=w.booking_id and t.is_current
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where w.id=v_member.work_id) then v_code:='BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT';
  elsif exists(select 1 from private.bpay_next_work w
    join public.timesheets t on t.booking_id=w.booking_id and t.is_current=true
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where w.id=v_member.work_id and tf.pay_on_hold is true) then
   v_code:='BPAY_NEXT_SELECTION_ON_HOLD';
  elsif not exists(select 1 from private.bpay_next_revision_worker_basis where revision_id=v_member.expected_revision_id) then
   raise exception using errcode='55000',message='BPAY_NEXT_SELECTION_BASIS_UNAVAILABLE';
  elsif exists(select 1 from private.bpay_next_snooze sn
    join lateral(select ev.after_state from private.bpay_next_snooze_event ev
      where ev.snooze_id=sn.id and ev.control_sequence<=v_run.snooze_control_sequence
      order by ev.control_sequence desc limit 1) e on true
    join private.bpay_next_revision_worker_basis b on b.revision_id=v_member.expected_revision_id
    where sn.work_id=v_member.work_id and sn.scope_kind='WHOLE_TIMESHEET'
      and (e.after_state->>'active')::boolean
      and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=v_run.snooze_business_date)
      and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis) then
   v_code:='BPAY_NEXT_SELECTION_SNOOZED';
  end if;
  v_gate:=null;v_query_exclusions:=null;
  if v_code is null and v_member.source_kind in ('SOURCE','PROTECTED') then
   if not v_query_admitted then v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
   else begin v_gate:=private.weekly_source_pay_query_gate_v1(v_member.physical_timesheet_id);
    if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'code' is distinct from 'OK'
      or jsonb_typeof(v_gate->'blocked') is distinct from 'boolean' then
     v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
    else
     v_query_exclusions:=private.bpay_next_source_query_segment_exclusions_v1(v_member.expected_revision_id,v_gate);
     if v_gate->'blocked'='true'::jsonb and v_query_exclusions is null then v_code:='SOURCE_PAY_QUERY_OPEN';end if;
    end if;
   exception when others then v_code:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';end;end if;
  end if;
  if v_code is null and v_member.selection_mode='SUBSET' and v_memory then
   if not exists(select 1 from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent) l
     where l.work_id=v_member.work_id and l.revision_id=v_member.expected_revision_id and l.selected and l.direction='PAYMENT') then
    v_code:='BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE';end if;
  elsif not v_memory then
  if v_code is null and v_member.selection_mode='SUBSET' and (not exists(
    select 1 from private.bpay_next_workbench_item i
    join jsonb_array_elements(v_intent->'components') a(value)
      on a.value->>'approved_line_id'=i.approved_line_id::text and a.value->'selected'='true'::jsonb
    where i.work_id=v_member.work_id and i.revision_id=v_member.expected_revision_id
      and i.section='READY' and i.selectable and i.amount_known and i.direction='PAYMENT'
      and i.portion_kind in ('AVAILABLE','ZERO'))
    or exists(select 1 from jsonb_array_elements(v_intent->'components') chosen
      where chosen->>'work_id'=v_member.work_id::text and chosen->'selected'='true'::jsonb
        and not exists(select 1 from private.bpay_next_workbench_item i
          where i.work_id=v_member.work_id and i.revision_id=v_member.expected_revision_id
            and i.approved_line_id=(chosen->>'approved_line_id')::uuid and i.section='READY'
            and i.selectable and i.amount_known and i.direction='PAYMENT'
            and i.portion_kind in ('AVAILABLE','ZERO')))) then
   v_code:='BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE';end if;
  end if;
  if v_code is not null then
   update private.bpay_next_run_choice_member set skip_code=v_code
     where run_id=p_run_id and member_kind='WORK' and member_id=v_member.member_id;
  else
   -- Use the existing Rule B objects as the only captured financial authority.
   v_work_count:=v_work_count+1;
   insert into private.bpay_next_run_selection(run_id,selection_no,work_id,candidate_id)
     values(p_run_id,v_work_count,v_member.work_id,v_member.candidate_id);
   insert into private.bpay_next_work_choice(run_id,work_id,expected_revision_id,selection_mode,selection_state,accepted_resolution_refs)
     values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.selection_mode,
       case when v_member.selection_mode='ALL' then 'SEALED' else 'OPEN' end,
       private.bpay_next_accepted_resolution_refs_v1(p_run_id,v_member.work_id,v_member.expected_revision_id));
   if v_gate is not null then
    insert into private.bpay_next_run_work_query_snapshot(run_id,work_id,expected_revision_id,physical_timesheet_id,gate_result,captured_at_utc,segment_query_exclusions)
      values(p_run_id,v_member.work_id,v_member.expected_revision_id,v_member.physical_timesheet_id,v_gate,transaction_timestamp(),v_query_exclusions);
   end if;
  end if;
 end loop;
 -- Seal both original descriptor paths at the accepted cut with native pages.
 update private.bpay_next_pay_run set work_choice_count=v_work_count,
  sealed_work_choice_count=(select count(*) from private.bpay_next_work_choice where run_id=p_run_id and selection_state='SEALED') where id=p_run_id;
 for v_subset in select c.* from private.bpay_next_work_choice c where c.run_id=p_run_id and c.selection_mode='SUBSET' order by c.work_id loop
  v_page_no:=0;
  for v_lines in select array_agg(q.approved_line_id order by q.approved_line_id) from
   (select chosen.approved_line_id,(row_number() over(order by chosen.approved_line_id)-1)/100 page_no from (
     select l.approved_line_id from private.bpay_next_selection_memory_choices_v1(v_ns->'scope',v_baseline,v_intent) l
      where v_memory and l.work_id=v_subset.work_id and l.revision_id=v_subset.expected_revision_id and l.selected and l.direction='PAYMENT'
     union all select (l.value->>'approved_line_id')::uuid from jsonb_array_elements(v_intent->'components') l
      where not v_memory and l.value->>'work_id'=v_subset.work_id::text
       and l.value->>'expected_revision_id'=v_subset.expected_revision_id::text and l.value->'selected'='true'::jsonb
   ) chosen) q group by q.page_no order by q.page_no loop
   v_page_no:=v_page_no+1;
   perform private.bpay_next_append_component_selection_page_v1(p_run_id,v_subset.work_id,v_subset.expected_revision_id,v_page_no,v_lines);
  end loop;
  select c.* into strict v_subset from private.bpay_next_work_choice c where c.run_id=p_run_id and c.work_id=v_subset.work_id;
  perform private.bpay_next_seal_component_selection_v1(p_run_id,v_subset.work_id,v_subset.expected_revision_id,v_subset.page_count,v_subset.component_count);
 end loop;
 -- Capture the existing Rule B Candidate settings exactly once in this cut.
 insert into private.bpay_next_selection_candidate(run_id,candidate_id,member_no,
   target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,min_take_home_wtd)
 select p_run_id,c.id,row_number() over(order by c.id),upper(c.pay_method),c.umbrella_id,u.vat_chargeable,c.min_take_home_wtd
 from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
 where exists(select 1 from private.bpay_next_run_choice_member m where m.run_id=p_run_id
   and m.candidate_id=c.id and m.selected and m.skip_code is null
   and (m.member_kind='WORK' or exists(select 1 from private.bpay_next_workbench_item i
     where i.case_component_id=m.case_component_id and i.direction='PAYMENT' and i.section='READY')));
 get diagnostics v_count=row_count;
 -- Optimistic context check, with no meta row lock or downstream recalculation.
 if not exists(select 1 from private.bpay_next_workbench_revision r where r.id=1
   and r.workbench_revision::text=p_descriptor->>'workbench_revision'
   and r.adoption_revision::text=p_descriptor->>'adoption_revision' and r.adoption_state='CURRENT') then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 if v_count>0 then
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARE');
  insert into private.bpay_next_run_command(run_id,command_id) values(p_run_id,p_command_id);
 end if;
 update private.bpay_next_pay_run set accepted_choice_descriptor=p_descriptor,
   accepted_choice_command_id=p_command_id,selected_candidate_count=v_count,
   selection_count=v_work_count,work_choice_count=v_work_count,
   sealed_work_choice_count=(select count(*) from private.bpay_next_work_choice where run_id=p_run_id and selection_state='SEALED') where id=p_run_id;
 if v_memory then
  update private.bpay_next_selection_mode_memory set command_id=case when v_sequence is not null then p_command_id end,
   agency_sequence=v_sequence where run_id=p_run_id and row_kind='HEADER' and v_sequence is not null;
 end if;
 -- ACK can only acknowledge these already frozen segments; no later READY reread.
 for v_subset in select c.work_id from private.bpay_next_work_choice c where c.run_id=p_run_id and c.selection_state='SEALED' order by c.work_id loop
  perform private.bpay_next_materialise_choice_segments_v1(p_run_id,v_subset.work_id);
 end loop;
 return v_result||jsonb_build_object('command_id',p_command_id,'agency_sequence',v_sequence::text,
   'selection_count',v_work_count::text,
   'selected_candidate_count',v_count::text,'empty',v_count=0,
   'choice_work_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='WORK'),
   'choice_case_count',(select count(*)::text from private.bpay_next_run_choice_member where run_id=p_run_id and member_kind='CASE_COMPONENT'),'replay',false);
end
$function$;
create or replace function private.bpay_next_bound_source_snapshot_v1(p_binding_version_id uuid,p_expected_book_revision bigint)
returns table(qualification_state text,selection_state text,candidate_id uuid,
  canonical_work_id uuid,canonical_component_key text,binding_version_id uuid,source_book_revision bigint,
  approval_work_id uuid,approval_revision_id uuid,approved_line_id uuid,head_id uuid,source_receipt_id uuid,
  source_channel text,tax_treatment text,classification_line_id uuid,valuation_policy_id uuid,
  approved_source_ex_vat private.bpay_next_penny_amount,
  native_realised_paye private.bpay_next_penny_amount,native_realised_umbrella private.bpay_next_penny_amount,
  native_held_paye private.bpay_next_penny_amount,native_held_umbrella private.bpay_next_penny_amount)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  return query select * from private.weekly_source_checklist_native_bound_snapshot_core_v1(p_binding_version_id,p_expected_book_revision,true);
end
$function$;
create or replace function private.bpay_next_initial_tsfin_certificate_v1(p_root_authorisation_id uuid)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  return private.weekly_source_checklist_native_initial_certificate_core_v1(p_root_authorisation_id,true);
end
$function$;
create or replace function private.bpay_next_initial_tsfin_bound_assert_v1(p_binding_version_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
begin
  perform private.weekly_source_checklist_native_initial_bound_assert_core_v1(p_binding_version_id,true);
end
$function$;
create or replace function private.bpay_next_workbench_work_members_v1(p_work_id uuid,p_revision_id uuid)
returns table(component_key text,approved_line_id uuid,approved_segment_id uuid,segment_key text,
  component_kind text,tax_treatment text,work_date date,approved_quantity numeric,unit_label text,
  approved_amount numeric,realised_amount numeric,held_amount numeric,source_pay_channel text,
  parent_complete boolean,segment_attribution_complete boolean)
language plpgsql security definer set search_path=pg_catalog,private
as $function$
begin
  return query select * from private.weekly_source_checklist_native_members_core_v1(p_work_id,p_revision_id,true);
end
$function$;
alter function private._weekly_source_immutable_record_guard_v1() owner to postgres;
revoke all on function private._weekly_source_immutable_record_guard_v1()
  from public,anon,authenticated,service_role;
alter function private.weekly_source_final_rule_version_guard_v1() owner to postgres;
revoke all on function private._weekly_source_rule_values_v1(jsonb),private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean),
  private.weekly_source_is_import_authoritative_v1(uuid,uuid,date),private.weekly_source_effective_rule_set_v1(uuid,uuid),
  private.weekly_source_has_self_bill_rules_v1(uuid,uuid),private.weekly_source_agency_invoice_check_only_v1(uuid),
  private.weekly_source_signed_check_only_v1(uuid),
  private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date),
  private.weekly_source_final_rule_version_guard_v1() from public,anon,authenticated,service_role;
alter function private.weekly_source_self_bill_binding_guard_v1() owner to postgres;
revoke all on function private.weekly_source_self_bill_binding_guard_v1() from public,anon,authenticated,service_role;
alter function private.weekly_source_actual_timesheet_submission_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_actual_timesheet_submission_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_signed_submission_read_context_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_signed_submission_read_context_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_client_calendar_v1(uuid,date) owner to postgres;
revoke all on function private.weekly_source_checklist_client_calendar_v1(uuid,date) from public,anon,authenticated,service_role;
alter function private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb) owner to postgres;
revoke all on function private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_native_initial_certificate_core_v1(uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_checklist_native_initial_certificate_core_v1(uuid,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_native_initial_bound_assert_core_v1(uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_checklist_native_initial_bound_assert_core_v1(uuid,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_native_bound_snapshot_core_v1(uuid,bigint,boolean) owner to postgres;
revoke all on function private.weekly_source_checklist_native_bound_snapshot_core_v1(uuid,bigint,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_native_members_core_v1(uuid,uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_checklist_native_members_core_v1(uuid,uuid,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_invoice_hold_release_context_core_v1(jsonb,boolean) owner to postgres;
revoke all on function private.weekly_source_invoice_hold_release_context_core_v1(jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_invoice_hold_first_invoice_assert_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_invoice_hold_first_invoice_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_invoice_unreleased_vat_fit_movements_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_invoice_unreleased_vat_fit_movements_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_work_period_proof_v1(uuid,uuid,date) owner to postgres;
revoke all on function private.bpay_next_work_period_proof_v1(uuid,uuid,date) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_legacy_pay_state_core_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_legacy_pay_state_core_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_legacy_read_frame_v1(uuid,jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_legacy_read_frame_v1(uuid,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_timesheet_hold_facts_v1(uuid,jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_timesheet_hold_facts_v1(uuid,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_information_hold_facts_v1(jsonb,jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_information_hold_facts_v1(jsonb,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_ordinary_next_hold_money_v1(uuid,jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_ordinary_next_hold_money_v1(uuid,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_source_shift_query_money_v1(uuid,uuid,jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_source_shift_query_money_v1(uuid,uuid,jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_source_invoice_block_items_v1(jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_source_invoice_block_items_v1(jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_source_invoice_decision_items_v1(jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_source_invoice_decision_items_v1(jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_process_publication_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_process_publication_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_process_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_process_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_page_v1(jsonb,jsonb,uuid,text,text,text) owner to postgres;
revoke all on function private.weekly_source_checklist_page_v1(jsonb,jsonb,uuid,text,text,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_office_tracker_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_office_tracker_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_healthroster_checklist_leaf_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_healthroster_checklist_leaf_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid) owner to postgres;
revoke all on function private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text) owner to postgres;
revoke all on function private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text) from public,anon,authenticated,service_role;
alter function public.weekly_source_check_only_candidate_reminder_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_check_only_candidate_reminder_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_check_only_candidate_reminder_atomic_v1(jsonb) to service_role;
alter function private.weekly_source_checklist_submission_cycle_admit_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_submission_cycle_admit_v1(uuid,text,uuid) from public,anon,authenticated,service_role;
alter function private._weekly_source_settings_ensure_open_cycle_v1(uuid,uuid,bytea,uuid) owner to postgres;
revoke all on function private._weekly_source_settings_ensure_open_cycle_v1(uuid,uuid,bytea,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_submission_cycle_prepare_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_submission_cycle_prepare_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_submitted_timesheet_prepare_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_submitted_timesheet_prepare_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_submitted_timesheet_prepare_atomic_v1(jsonb) to service_role;
alter function private.weekly_source_checklist_historical_submission_descriptor_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_historical_submission_descriptor_v1(uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_checklist_submission_manifest_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_checklist_submission_manifest_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_checklist_submission_manifest_v1(jsonb) to service_role;
alter function public.weekly_source_checklist_historical_prepare_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_checklist_historical_prepare_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_checklist_historical_prepare_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_checklist_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_checklist_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_checklist_v1(jsonb) to service_role;
alter function private.bpay_next_workbench_refresh_work_core_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_work_core_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_append_work_skip_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_bound_source_snapshot_v1(uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_bound_source_snapshot_v1(uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_initial_tsfin_certificate_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_initial_tsfin_bound_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_v1(uuid),private.bpay_next_initial_tsfin_bind_v1(uuid),
  private.bpay_next_initial_tsfin_bound_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_work_members_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_work_members_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private._weekly_source_rule_values_v1(jsonb) owner to postgres;
alter function private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean) owner to postgres;
revoke all on function private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_is_import_authoritative_v1(uuid,uuid,date) owner to postgres;
alter function private.weekly_source_has_self_bill_rules_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_agency_invoice_check_only_v1(uuid) owner to postgres;
alter function private.weekly_source_signed_check_only_v1(uuid) owner to postgres;
alter function private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date) owner to postgres;
alter function private.bpay_next_initial_tsfin_bind_v1(uuid) owner to postgres;
drop trigger if exists weekly_source_immutable_record_guard on public.weekly_source_rule_set_versions;
create trigger weekly_source_immutable_record_guard before update or delete
  on public.weekly_source_rule_set_versions for each row execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_immutable_truncate_guard on public.weekly_source_rule_set_versions;
create trigger weekly_source_immutable_truncate_guard before truncate
  on public.weekly_source_rule_set_versions for each statement execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_final_rule_version_guard on public.weekly_source_final_revisions;
create trigger weekly_source_final_rule_version_guard before insert or update of rule_set_version_id
  on public.weekly_source_final_revisions for each row execute function private.weekly_source_final_rule_version_guard_v1();
drop trigger if exists weekly_source_immutable_record_guard on public.weekly_source_self_bill_reports;
create trigger weekly_source_immutable_record_guard before update or delete on public.weekly_source_self_bill_reports
  for each row execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_immutable_truncate_guard on public.weekly_source_self_bill_reports;
create trigger weekly_source_immutable_truncate_guard before truncate on public.weekly_source_self_bill_reports
  for each statement execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_self_bill_binding_guard_v1 on public.weekly_source_self_bill_report_line_bindings;
create trigger weekly_source_self_bill_binding_guard_v1 before insert or update or delete on public.weekly_source_self_bill_report_line_bindings
  for each row execute function private.weekly_source_self_bill_binding_guard_v1();
drop trigger if exists weekly_source_self_bill_binding_truncate_guard_v1 on public.weekly_source_self_bill_report_line_bindings;
create trigger weekly_source_self_bill_binding_truncate_guard_v1 before truncate on public.weekly_source_self_bill_report_line_bindings
  for each statement execute function private.weekly_source_self_bill_binding_guard_v1();
drop trigger if exists weekly_source_immutable_record_guard on public.weekly_source_invoice_hold_releases;
create trigger weekly_source_immutable_record_guard before update or delete on public.weekly_source_invoice_hold_releases
  for each row execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_immutable_truncate_guard on public.weekly_source_invoice_hold_releases;
create trigger weekly_source_immutable_truncate_guard before truncate on public.weekly_source_invoice_hold_releases
  for each statement execute function private._weekly_source_immutable_record_guard_v1();
do $finite_constructor$ declare r record;p pg_catalog.pg_proc%rowtype;begin
 if current_setting('check_function_bodies') is distinct from 'on' then raise exception 'CHECKLIST_CHECK_FUNCTION_BODIES_REQUIRED';end if;
 for r in select * from(values
('private._weekly_source_immutable_record_guard_v1()','v',true),
('private.weekly_source_final_rule_version_guard_v1()','v',true),
('private.weekly_source_self_bill_binding_guard_v1()','v',true),
('private._weekly_source_rule_values_v1(jsonb)','i',false),
('private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean)','s',true),
('private.weekly_source_is_import_authoritative_v1(uuid,uuid,date)','s',true),
('private.weekly_source_effective_rule_set_v1(uuid,uuid)','s',true),
('private.weekly_source_has_self_bill_rules_v1(uuid,uuid)','s',true),
('private.weekly_source_agency_invoice_check_only_v1(uuid)','s',true),
('private.weekly_source_signed_check_only_v1(uuid)','s',true),
('private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date)','s',true),
('private.bpay_next_initial_tsfin_bind_v1(uuid)','v',true),
('private.weekly_source_actual_timesheet_submission_v1(uuid)','s',true),
('private.weekly_source_signed_submission_read_context_v1(uuid,uuid)','s',true),
('private.weekly_source_checklist_client_calendar_v1(uuid,date)','s',true),
('private.weekly_source_check_only_query_rows_v1(uuid,uuid,jsonb)','s',true),
('private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb)','i',true),
('private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)','s',true),
('private.weekly_source_checklist_native_initial_certificate_core_v1(uuid,boolean)','v',true),
('private.weekly_source_checklist_native_initial_bound_assert_core_v1(uuid,boolean)','v',true),
('private.weekly_source_checklist_native_bound_snapshot_core_v1(uuid,bigint,boolean)','v',true),
('private.weekly_source_checklist_native_members_core_v1(uuid,uuid,boolean)','v',true),
('private.weekly_source_invoice_hold_release_context_core_v1(jsonb,boolean)','v',true),
('private.weekly_source_invoice_hold_first_invoice_assert_v1(uuid)','s',true),
('private.weekly_source_invoice_unreleased_vat_fit_movements_v1(uuid)','s',true),
('private.bpay_next_work_period_proof_v1(uuid,uuid,date)','s',true),
('private.weekly_source_checklist_legacy_pay_state_core_v1(uuid)','s',true),
('private.weekly_source_checklist_legacy_read_frame_v1(uuid,jsonb,uuid)','s',true),
('private.weekly_source_checklist_timesheet_hold_facts_v1(uuid,jsonb,uuid)','s',true),
('private.weekly_source_checklist_information_hold_facts_v1(jsonb,jsonb,uuid)','s',true),
('private.weekly_source_checklist_ordinary_next_hold_money_v1(uuid,jsonb,uuid)','s',true),
('private.weekly_source_checklist_source_shift_query_money_v1(uuid,uuid,jsonb,uuid)','s',true),
('private.weekly_source_checklist_source_invoice_block_items_v1(jsonb,uuid)','s',true),
('private.weekly_source_checklist_source_invoice_decision_items_v1(jsonb,uuid)','v',true),
('private.weekly_source_checklist_process_publication_v1(uuid)','s',true),
('private.weekly_source_checklist_process_v1(uuid,uuid)','s',true),
('private.weekly_source_checklist_page_v1(jsonb,jsonb,uuid,text,text,text)','i',true),
('private.weekly_source_office_tracker_v1(uuid,uuid)','s',true),
('private.weekly_source_healthroster_checklist_leaf_v1(jsonb)','s',true),
('private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid)','s',true),
('private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text)','s',true),
('public.weekly_source_check_only_candidate_reminder_atomic_v1(jsonb)','v',true),
('private.weekly_source_checklist_submission_cycle_admit_v1(uuid,text,uuid)','v',true),
('private._weekly_source_settings_ensure_open_cycle_v1(uuid,uuid,bytea,uuid)','v',true),
('private.weekly_source_checklist_submission_cycle_prepare_v1(uuid,uuid)','v',true),
('public.weekly_source_submitted_timesheet_prepare_atomic_v1(jsonb)','v',true),
('private.weekly_source_checklist_historical_submission_descriptor_v1(uuid)','s',true),
('public.weekly_source_checklist_submission_manifest_v1(jsonb)','s',true),
('public.weekly_source_checklist_historical_prepare_atomic_v1(jsonb)','v',true),
('public.weekly_source_checklist_v1(jsonb)','s',true),
('private.bpay_next_workbench_refresh_work_core_v1(uuid)','v',true),
('private.bpay_next_append_work_skip_v1(uuid,uuid,uuid)','v',false),
('private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)','v',true),
('private.bpay_next_bound_source_snapshot_v1(uuid,bigint)','v',true),
('private.bpay_next_initial_tsfin_certificate_v1(uuid)','v',true),
('private.bpay_next_initial_tsfin_bound_assert_v1(uuid)','v',true),
('private.bpay_next_workbench_work_members_v1(uuid,uuid)','v',true)
) expected(signature,volatility,security_definer) loop
 select q.* into p from pg_catalog.pg_proc q where q.oid=pg_catalog.to_regprocedure(r.signature);
 if not found or p.provolatile::text is distinct from r.volatility or p.prosecdef is distinct from r.security_definer then raise exception 'CHECKLIST_CONSTRUCTOR_CATALOG_MISMATCH: %',r.signature using errcode='55000';end if;end loop;end;$finite_constructor$;
