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
create or replace function public.weekly_source_invoice_hold_release_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare actor uuid; row_id uuid; expected_hash bytea; expected_version bigint; kind text; choice text;
  s public.weekly_source_upload_rows%rowtype; u public.weekly_source_uploads%rowtype; c public.weekly_source_cycles%rowtype;
  g public.weekly_source_groups%rowtype; scope_row public.weekly_source_report_scopes%rowtype; rules jsonb; v_client_id uuid; existing public.weekly_source_invoice_hold_releases%rowtype;
  net bigint; vat bigint; gross bigint; h bytea; id uuid; native_context jsonb;
begin
  native_context:=private.weekly_source_invoice_hold_release_context_core_v1(p_request,true);
  actor:=(native_context->>'actor_user_id')::uuid;row_id:=(native_context->>'upload_row_id')::uuid;
  expected_hash:=decode(native_context->>'expected_source_row_hash','hex');expected_version:=(native_context->>'expected_authority_scope_version')::bigint;
  kind:=native_context->>'hold_kind';choice:=native_context->>'choice';rules:=native_context->'rules';v_client_id:=(native_context->>'client_id')::uuid;
  s:=jsonb_populate_record(null::public.weekly_source_upload_rows,native_context->'row');
  u:=jsonb_populate_record(null::public.weekly_source_uploads,native_context->'upload');
  g:=jsonb_populate_record(null::public.weekly_source_groups,native_context->'group');
  c:=jsonb_populate_record(null::public.weekly_source_cycles,native_context->'cycle');
  scope_row:=jsonb_populate_record(null::public.weekly_source_report_scopes,native_context->'report_scope');
  net:=s.source_shift_charge_pence;
  vat:=case when choice='BILL_AT_NET_PLUS_20' then round(net::numeric*20/100)::bigint else s.source_vat_pence end;
  gross:=case when choice='BILL_AT_NET_PLUS_20' then net+vat else s.source_gross_pence end;
  select * into existing from public.weekly_source_invoice_hold_releases where upload_row_id=s.id and hold_kind=kind;
  if found then
    if existing.action<>choice or existing.net_pence<>net or existing.vat_pence<>vat or existing.gross_pence<>gross then
      raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_REPLAY_CONFLICT' using errcode='40001'; end if;
    return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1','status','RELEASED','idempotent',true,
      'release_id',existing.id,'upload_row_id',s.id,'hold_kind',kind,'action',choice,'net_pence',net,'vat_pence',vat,'gross_pence',gross);
  end if;
  -- First-invoice purpose authority: admission shares the cycle lock. Preserve
  -- every original movement, snapshot and manifest value and their hashes.
  perform private.weekly_source_invoice_hold_first_invoice_assert_v1(s.id);
  h:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1',jsonb_build_object('upload_row_id',s.id,
    'source_row_hash',encode(s.normalised_row_hash,'hex'),'hold_kind',kind,'action',choice,'net_pence',net,'vat_pence',vat,'gross_pence',gross,'actor_user_id',actor));
  insert into public.weekly_source_invoice_hold_releases(upload_row_id,hold_kind,action,net_pence,vat_pence,gross_pence,actor_user_id,release_hash)
    values(s.id,kind,choice,net,vat,gross,actor,h) returning public.weekly_source_invoice_hold_releases.id into id;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1','status','RELEASED','idempotent',false,
    'release_id',id,'upload_row_id',s.id,'hold_kind',kind,'action',choice,'net_pence',net,'vat_pence',vat,'gross_pence',gross);
end;
$function$;
create or replace function public.candidate_submission_finalize_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_expected_row_signature text default null,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now(),
  p_daily_materialisation_json jsonb default null
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
set lock_timeout = '5s'
set statement_timeout = '120s'
as $function$
declare
  v_environment text;
  v_context jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hours_component public.candidate_submission_components%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_manager_signature public.candidate_submission_components%rowtype;
  v_approved_request public.candidate_approval_requests%rowtype;
  v_paper_hours_return public.candidate_submission_components%rowtype;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
  v_daily_receipt_context jsonb;
  v_daily_receipt_only boolean:=false;
  v_completion_state text:='FINALISED';
  v_completion_generation integer;
  v_current_policy jsonb;
  v_system_actor uuid;
  v_input jsonb;
  v_electronic_patch jsonb:='{}'::jsonb;
  v_render_input jsonb;
  v_result jsonb;
  v_authorise_result jsonb;
  v_expense_authorise_result jsonb;
  v_response jsonb;
  v_target_timesheet_id uuid;
  v_hours_timesheet_id uuid;
  v_expense_timesheet_id uuid;
  v_evidence_component_ids uuid[];
  v_placement jsonb;
  v_hours_result jsonb;
  v_hours_input jsonb;
  v_expense_input jsonb;
  v_effective_separation boolean:=false;
  v_after_signature text;
  v_auto_requested boolean:=false;
  v_auto_blocked boolean:=false;
  v_auto_blockers jsonb:='[]'::jsonb;
  v_constraint_name text;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_daily_save_input jsonb;
  v_daily_patch jsonb;
  v_daily_signature jsonb;
  v_daily_save_receipt jsonb;
  v_canonical_financials_id uuid;
  v_canonical_financial_sha256 bytea;
  v_service_finalisation jsonb;
  v_is_office_service boolean:=false;
  v_replay_probe_only boolean:=false;
  v_key_replay_probe_only boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_request_hash text;
  v_mutation_receipt jsonb;
  v_prior_receipt_before jsonb;
  v_prior_receipt_after jsonb;
  v_finalisation_identity jsonb;
  v_finalisation_identity_hash text;
  v_completion_before jsonb;
  v_completion_after jsonb;
  v_worker_only_approval boolean:=false;
  v_worker_only_final_state jsonb;
  v_checklist_candidate_id uuid;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  v_service_finalisation:=coalesce(p_daily_materialisation_json->'service_finalisation','{}'::jsonb);
  v_is_office_service:=p_session_id is null
    and private._candidate_office_service_context_valid_v1(
      v_environment,nullif(v_service_finalisation->>'actor_user_id','')::uuid,'RETRY_FINALISATION'
    );
  v_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_probe_only')::boolean,false);
  v_key_replay_probe_only:=p_session_id is null
    and coalesce((v_service_finalisation->>'replay_key_probe_only')::boolean,false);
  if not v_is_office_service then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023'; end if;

  -- SP-12: plain existing session authentication, then early admission.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    if p_session_id is not null then
      v_context:=private._candidate_session_context_v1(
        p_session_id,v_environment,null,p_now_utc,false
      );
      v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
      if v_candidate_id is null then
        raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000';
      end if;
    end if;
    perform private.weekly_source_pay_query_admit_v2();
  end if;

  -- Resolve only the existing native workflow target. This acquires I1/F1
  -- before the unchanged finalizer's first workflow business lock; its later
  -- locked session/actor, generation, signature and receipt checks stay intact.
  select w.candidate_id into v_checklist_candidate_id from public.candidate_submission_workflows w
    where w.id=p_workflow_id and w.environment=v_environment;
  if p_session_id is not null then
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,false);
    v_checklist_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_checklist_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;
  perform private.weekly_source_checklist_submission_cycle_admit_v1(
    p_workflow_id,v_environment,v_checklist_candidate_id);

  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id for update;
  if not found or v_workflow.environment<>v_environment then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if p_session_id is null then
    v_candidate_id:=v_workflow.candidate_id;
  else
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;
  if v_workflow.candidate_id<>v_candidate_id then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
    raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
        'idempotency_key',btrim(p_idempotency_key),
        'reason','CREATION_KEY_REUSED_FOR_MUTATION'
      )::text;
  end if;
  v_mutation_channel:=case when v_is_office_service then 'OFFICE'
    when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end;
  v_mutation_actor_identity:=case when v_is_office_service
    then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end;
  if v_key_replay_probe_only then
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object(
      'ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation
    );
  end if;
  -- Candidate-session calls retain their established lifecycle errors before
  -- an immutable approval identity exists. Service replay/finalisation calls
  -- continue through the receipt path below before mutable lifecycle checks.
  if p_session_id is not null then
    if v_workflow.route='PAPER' and v_workflow.state<>'RECEIVED' then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    elsif v_workflow.route<>'PAPER' and v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
  end if;
  v_worker_only_approval:=v_workflow.scope='WEEKLY' and v_workflow.workflow_kind='CONTRACT_HOURS'
    and v_workflow.route='ELECTRONIC'
    and coalesce(v_workflow.policy_snapshot_json#>'{manager_approval_policy,approval}'='"NOT_REQUIRED"'::jsonb,false);
  v_finalisation_identity:=v_service_finalisation->'finalisation_identity';
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object' then
    if v_worker_only_approval then
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method','ELECTRONIC','approval_request_id',null,'approval_request_generation',null,
        'review_manifest_sha256_hex',encode(v_workflow.review_manifest_sha256,'hex'),
        'paper_return_manifest_sha256_hex',null,
        'candidate_signature_sha256_hex',encode(v_workflow.candidate_signature_sha256,'hex'),
        'policy_snapshot_sha256_hex',encode(v_workflow.policy_snapshot_sha256,'hex'));
    elsif v_workflow.route='PAPER' then
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method','PAPER','approval_request_id',null,
        'approval_request_generation',null,'review_manifest_sha256_hex',null,
        'paper_return_manifest_sha256_hex',case when v_workflow.paper_return_manifest_sha256 is null
          then null else encode(v_workflow.paper_return_manifest_sha256,'hex') end
      );
    else
      select approved.* into v_approved_request
      from public.candidate_approval_requests approved
      where approved.workflow_id=v_workflow.id
        and approved.workflow_generation=p_expected_generation
        and approved.state='APPROVED'
        and (nullif(v_service_finalisation->>'approval_request_id','') is null
          or approved.id=(v_service_finalisation->>'approval_request_id')::uuid)
      order by approved.approved_at_utc desc,approved.id desc
      limit 1;
      v_finalisation_identity:=jsonb_build_object(
        'contract_version','CANDIDATE_FINALISATION_IDENTITY_V1',
        'workflow_id',v_workflow.id,'workflow_generation',p_expected_generation,
        'approval_method',coalesce(v_approved_request.method,v_service_finalisation->>'approval_method'),
        'approval_request_id',coalesce(v_approved_request.id,
          nullif(v_service_finalisation->>'approval_request_id','')::uuid),
        'approval_request_generation',v_approved_request.request_generation,
        'review_manifest_sha256_hex',case when v_approved_request.review_manifest_sha256 is null
          then null else encode(v_approved_request.review_manifest_sha256,'hex') end,
        'paper_return_manifest_sha256_hex',null
      );
    end if;
    v_service_finalisation:=v_service_finalisation||jsonb_build_object(
      'contract_version','CANDIDATE_MANAGER_FINALISATION_V1',
      'workflow_generation',p_expected_generation,
      'approval_method',v_finalisation_identity->>'approval_method',
      'approval_request_id',v_finalisation_identity->'approval_request_id',
      'approval_request_generation',v_finalisation_identity->'approval_request_generation',
      'review_manifest_sha256_hex',coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''),
      'paper_return_manifest_sha256_hex',coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''),
      'finalisation_identity',v_finalisation_identity
    );
  end if;
  if jsonb_typeof(v_finalisation_identity) is distinct from 'object'
     or coalesce(v_finalisation_identity->>'contract_version','')
          <>'CANDIDATE_FINALISATION_IDENTITY_V1'
     or nullif(v_finalisation_identity->>'workflow_id','')::uuid is distinct from v_workflow.id
     or coalesce((v_finalisation_identity->>'workflow_generation')::integer,0)
          <>p_expected_generation
     or (upper(coalesce(v_finalisation_identity->>'approval_method','')) not in ('EMAIL','PHONE','PAPER')
       and not (v_worker_only_approval and v_finalisation_identity->>'approval_method'='ELECTRONIC')) then
    raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
      using errcode='28000',detail=jsonb_build_object('stage','IDENTITY')::text;
  end if;
  v_finalisation_identity_hash:=encode(extensions.digest(convert_to(
    v_finalisation_identity::text,'UTF8'
  ),'sha256'),'hex');
  v_mutation_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_FINALISATION_MUTATION_REQUEST_V3',
    'workflow_id',v_workflow.id,
    'action','RETRY_FINALISATION',
    'expected_generation',p_expected_generation,
    'service_finalisation',v_service_finalisation-'replay_probe_only',
    'channel',v_mutation_channel,
    'actor_identity',v_mutation_actor_identity
  )::text,'UTF8'),'sha256'),'hex');
  if v_replay_probe_only then
    if p_session_id is null and (
      coalesce(v_service_finalisation->>'contract_version','')
        <>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)
         <>p_expected_generation
    ) then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','REPLAY_ENVELOPE')::text;
    end if;
    select ae.before_json,ae.after_json
    into v_prior_receipt_before,v_prior_receipt_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_mutation_receipt'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=btrim(p_idempotency_key)
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_prior_receipt_before->>'request_sha256' is distinct from v_mutation_request_hash
         or upper(coalesce(v_prior_receipt_before->>'workflow_action',''))<>'RETRY_FINALISATION'
         or upper(coalesce(v_prior_receipt_before->>'channel',''))<>v_mutation_channel
         or coalesce(v_prior_receipt_before->>'actor_identity','')
              is distinct from coalesce(v_mutation_actor_identity,'')
         or nullif(v_prior_receipt_after->>'generation','')::integer
              is distinct from (p_expected_generation+case
                when v_workflow.workflow_kind='DAILY'
                  and v_prior_receipt_after->>'state'='RECEIVED'
                  and v_prior_receipt_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_IDEMPOTENCY_CONFLICT','workflow_id',v_workflow.id,
            'idempotency_key',btrim(p_idempotency_key)
          )::text;
      end if;
      return coalesce(v_prior_receipt_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    select ae.before_json,ae.after_json
    into v_completion_before,v_completion_after
    from public.audit_events ae
    where ae.object_type='candidate_workflow_finalisation_completion'
      and ae.object_id_text=v_workflow.id::text
      and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
    order by ae.ts_utc desc,ae.id desc
    limit 1;
    if found then
      if v_completion_before->>'finalisation_identity_sha256'
           is distinct from v_finalisation_identity_hash
         or nullif(v_completion_after->>'generation','')::integer
           is distinct from (p_expected_generation+case
             when v_workflow.workflow_kind='DAILY'
               and v_completion_after->>'state'='RECEIVED'
               and v_completion_after->>'office_resolution_pending'='true' then 0 else 1 end) then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
      end if;
      return coalesce(v_completion_after,'{}'::jsonb)
        ||jsonb_build_object('idempotent_replay',true);
    end if;
    return jsonb_build_object('ok',true,'replay_found',false,'workflow_id',v_workflow.id,
      'expected_generation',p_expected_generation);
  end if;
  v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    v_mutation_channel,v_mutation_actor_identity,
    null,p_now_utc
  );
  if coalesce((v_mutation_receipt->>'found')::boolean,false) then
    return coalesce(v_mutation_receipt->'response','{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;
  select ae.before_json,ae.after_json
  into v_completion_before,v_completion_after
  from public.audit_events ae
  where ae.object_type='candidate_workflow_finalisation_completion'
    and ae.object_id_text=v_workflow.id::text
    and ae.correlation_id=p_expected_generation::text||':'||v_finalisation_identity_hash
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    return coalesce(v_completion_after,'{}'::jsonb)
      ||jsonb_build_object('idempotent_replay',true);
  end if;
  if p_session_id is null then
    if coalesce(v_service_finalisation->>'contract_version','')<>'CANDIDATE_MANAGER_FINALISATION_V1'
       or coalesce((v_service_finalisation->>'workflow_generation')::integer,0)<>v_workflow.generation
       or upper(coalesce(v_service_finalisation->>'approval_method',''))<>v_workflow.route then
      raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
        using errcode='28000',detail=jsonb_build_object('stage','SERVICE_ENVELOPE')::text;
    end if;
    if v_worker_only_approval then
      if nullif(v_service_finalisation->>'approval_request_id','') is not null
         or nullif(v_finalisation_identity->>'approval_request_id','') is not null
         or v_finalisation_identity->'approval_request_generation' is distinct from 'null'::jsonb
         or v_finalisation_identity->>'approval_method' is distinct from 'ELECTRONIC'
         or v_finalisation_identity->>'review_manifest_sha256_hex' is distinct from encode(v_workflow.review_manifest_sha256,'hex')
         or v_finalisation_identity->>'candidate_signature_sha256_hex' is distinct from encode(v_workflow.candidate_signature_sha256,'hex')
         or v_finalisation_identity->>'policy_snapshot_sha256_hex' is distinct from encode(v_workflow.policy_snapshot_sha256,'hex') then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID' using errcode='28000',
          detail=jsonb_build_object('stage','WORKER_ONLY_IDENTITY')::text;
      end if;
    elsif v_workflow.route='PAPER' then
      if nullif(v_service_finalisation->>'approval_request_id','') is not null
         or nullif(v_finalisation_identity->>'approval_request_id','') is not null
         or upper(v_finalisation_identity->>'approval_method')<>'PAPER'
         or lower(coalesce(v_finalisation_identity->>'paper_return_manifest_sha256_hex',''))
              <>encode(v_workflow.paper_return_manifest_sha256,'hex') then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','PAPER_IDENTITY')::text;
      end if;
    else
      select * into v_approved_request
      from public.candidate_approval_requests a
      where a.id=nullif(v_service_finalisation->>'approval_request_id','')::uuid
        and a.workflow_id=v_workflow.id
        and a.workflow_generation=p_expected_generation
        and a.request_generation=coalesce(
          nullif(v_service_finalisation->>'approval_request_generation','')::integer,0
        )
        and a.method=upper(coalesce(v_service_finalisation->>'approval_method',''))
        and a.state='APPROVED'
        and encode(a.review_manifest_sha256,'hex')=lower(coalesce(v_service_finalisation->>'review_manifest_sha256_hex',''))
        and v_finalisation_identity->>'approval_request_id'=a.id::text
        and coalesce((v_finalisation_identity->>'approval_request_generation')::integer,0)=a.request_generation
        and upper(v_finalisation_identity->>'approval_method')=a.method
        and lower(coalesce(v_finalisation_identity->>'review_manifest_sha256_hex',''))
              =encode(a.review_manifest_sha256,'hex')
      for update;
      if not found then
        raise exception 'CANDIDATE_SERVICE_FINALISATION_INVALID'
          using errcode='28000',detail=jsonb_build_object('stage','APPROVAL_IDENTITY')::text;
      end if;
    end if;
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_VERSION_MISMATCH'
      using errcode='40001',detail=jsonb_build_object('code','WORKFLOW_VERSION_MISMATCH','current_generation',v_workflow.generation)::text;
  end if;
  if v_workflow.workflow_kind='DAILY' then
    if v_workflow.scope<>'DAILY' or v_workflow.route not in ('PHONE','EMAIL')
       or v_workflow.contract_week_id is not null or v_workflow.week_ending_date is not null
       or v_workflow.target_timesheet_id is null
       or v_workflow.anchor_timesheet_id is distinct from v_workflow.target_timesheet_id then
      raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
    end if;
    v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
      v_environment,v_candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
    v_daily_receipt_only:=coalesce((v_daily_receipt_context->>'candidate_first_receipt')::boolean,false)
      and coalesce((v_daily_receipt_context->>'office_resolution_pending')::boolean,false);
    select * into v_daily_timesheet
    from public.timesheets
    where timesheet_id=v_workflow.target_timesheet_id
      and is_current=true and archived_at_utc is null
      and sheet_scope='DAILY'::public.timesheet_scope_enum
      and nullif(btrim(coalesce(booking_id,'')),'') is not null
    for update;
    if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
    if not private._candidate_daily_entitled_v1(v_candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    select * into v_daily_fin
    from public.timesheets_financials
    where timesheet_id=v_daily_timesheet.timesheet_id
      and is_current=true and candidate_id=v_candidate_id
    order by computed_at_utc desc nulls last,updated_at desc,id desc
    limit 1
    for update;
    if (not found and not v_daily_receipt_only)
       or v_workflow.work_date is distinct from private._candidate_daily_work_date_v1(
         coalesce(v_daily_fin.worked_start_iso,v_daily_timesheet.worked_start_iso),
         v_daily_timesheet.scheduled_start_iso,
         v_daily_timesheet.week_ending_date
       ) then
      raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
    end if;
    if v_daily_fin.authorised_at_utc is not null
       or v_daily_fin.paid_at_utc is not null
       or v_daily_fin.locked_by_invoice_id is not null
       or v_daily_timesheet.archived_at_utc is not null then
      raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
    end if;
    if v_daily_timesheet.contract_id is not null then
      select * into v_contract
      from public.contracts
      where id=v_daily_timesheet.contract_id and candidate_id=v_candidate_id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    end if;
    if v_daily_receipt_only then
      v_current_policy:=v_daily_receipt_context->'policy';
    else
      if coalesce(v_daily_fin.client_id,v_contract.client_id) is null then
        raise exception 'CANDIDATE_DAILY_CLIENT_NOT_FOUND' using errcode='P0002';
      end if;
      v_current_policy:=private._candidate_policy_resolve_v1(
        coalesce(v_daily_fin.client_id,v_contract.client_id),v_contract.id,v_workflow.work_date
      );
    end if;
    if (v_workflow.route='PHONE' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_on_phone')::boolean,false))
       or (v_workflow.route='EMAIL' and not coalesce((v_current_policy->>'allow_daily_manager_authorise_by_email')::boolean,false)) then
      raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  else
    if v_workflow.workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
       or v_workflow.scope<>'WEEKLY' or v_workflow.contract_id is null
       or v_workflow.contract_week_id is null or v_workflow.week_ending_date is null then
      raise exception 'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_INVALID' using errcode='22023';
    end if;
    select * into v_contract
    from public.contracts
    where id=v_workflow.contract_id and candidate_id=v_candidate_id
    for update;
    if not found then raise exception 'CANDIDATE_WORKFLOW_OWNERSHIP_MISMATCH' using errcode='28000'; end if;
    select * into v_week
    from public.contract_weeks
    where id=v_workflow.contract_week_id
      and contract_id=v_contract.id
      and week_ending_date=v_workflow.week_ending_date
    for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_IDENTITY_MISMATCH' using errcode='40001'; end if;
    if v_workflow.anchor_timesheet_id is not null then
      select cw.* into v_anchor_week
      from public.contract_weeks cw
      join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      where cw.timesheet_id=v_workflow.anchor_timesheet_id
        and cw.contract_id=v_contract.id
        and cw.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      if v_workflow.anchor_timesheet_id is null
         or coalesce((private._candidate_record_capabilities_v1(
           v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
         )->>'hours_value')::numeric,0)<=0
            and coalesce((private._candidate_record_capabilities_v1(
              v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
            )->>'additional_units_value')::numeric,0)<=0 then
        -- Reuse the existing expense-admission authority for source weeks:
        -- immutable Candidate worked-hours evidence may precede source TSFIN.
        -- This neither writes nor unlocks the source-owned hours record.
        if v_workflow.anchor_timesheet_id is null
           or not coalesce((private._candidate_record_capabilities_v1(
             v_workflow.anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb
           )->>'import_authoritative')::boolean,false) then
          raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000';
        end if;
        v_placement:=public.expense_placement_resolve_v1(
          v_candidate_id,v_environment,v_workflow.anchor_timesheet_id,
          v_anchor_week.id,'{}'::jsonb,p_now_utc);
        if not coalesce((v_placement->>'ok')::boolean,false)
           or coalesce(v_placement->>'placement','') not in ('REUSE_CARRIER','CREATE_CARRIER') then
          raise exception 'CANDIDATE_EXPENSE_FINALISATION_ADMISSION_BLOCKED'
            using errcode='55000',detail=coalesce(v_placement->>'reason_code','NO_POSITIVE_WORKED_TIME');
        end if;
      end if;
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_workflow.target_timesheet_id is not null then
      raise exception 'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' using errcode='40001';
    elsif v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_workflow.target_timesheet_id is distinct from v_week.timesheet_id then
      raise exception 'CANDIDATE_WORKFLOW_TARGET_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
       and v_week.timesheet_id is not null then
      v_target_capabilities:=private._candidate_record_capabilities_v1(
        v_week.timesheet_id,v_week.id,'{}'::jsonb
      );
      if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
         or coalesce((v_target_capabilities->>'protected')::boolean,false)
         or not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false) then
        raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
      end if;
    end if;
    v_route_authority:=private._expense_approval_route_v1(
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_workflow.anchor_timesheet_id
        else v_week.timesheet_id end,
      case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.id else v_week.id end,v_workflow.workflow_kind
    );
    if v_route_authority->>'route_family'='MANUAL_NON_QR'
       or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
         and v_workflow.workflow_kind<>'CONTRACT_EXPENSE')
       or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
       or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family'='QR') then
      raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
    end if;
    v_current_policy:=private._expense_approval_policy_v1(
      v_contract.client_id,v_contract.id,v_workflow.week_ending_date,v_workflow.workflow_kind
    );
    -- Already-approved electronic claims retain their original policy comparison.
    -- Switching to PAPER always requires the current expense permission.
    if v_workflow.route<>'PAPER'
       and v_workflow.policy_snapshot_json->>'paper_submission_enabled_source'='IMPORT_DISABLED' then
      v_current_policy:=private._candidate_policy_resolve_v1(
        v_contract.client_id,v_contract.id,v_workflow.week_ending_date);
    end if;
    if v_workflow.route='PAPER'
       and not coalesce((v_current_policy->>'paper_submission_enabled')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
  end if;
  if v_workflow.route='PAPER' then
    if v_workflow.state<>'RECEIVED'
       or v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
         where (
           select count(*)
           from public.candidate_submission_components returned_page
           where returned_page.workflow_id=v_workflow.id
             and returned_page.workflow_generation=v_workflow.generation
             and returned_page.component_kind='SIGNED_RETURN'
             and returned_page.paper_return_page_key=expected_page->>'page_key'
             and returned_page.state='IMMUTABLE'
             and returned_page.source_content_sha256 is not null
         )<>1
       )
       or exists(
         select 1
         from public.candidate_submission_components returned_page
         where returned_page.workflow_id=v_workflow.id
           and returned_page.workflow_generation=v_workflow.generation
           and returned_page.component_kind='SIGNED_RETURN'
           and returned_page.state='IMMUTABLE'
           and not exists(
             select 1
             from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
             where expected_page->>'page_key'=returned_page.paper_return_page_key
           )
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000';
    end if;
  else
    if v_workflow.state<>'READY_TO_FINALISE' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    if not v_worker_only_approval then
      select * into v_approved_request
      from public.candidate_approval_requests a
        where a.workflow_id=v_workflow.id and a.workflow_generation=v_workflow.generation
          and a.state='APPROVED' and a.review_manifest_sha256=v_workflow.review_manifest_sha256
      for update;
      if not found then raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000'; end if;
    end if;

    if not exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
    ) or exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
        and (c.state<>'IMMUTABLE' or c.review_render_state<>'READY'
          or c.final_signed_render_state<>'READY'
          or c.review_render_input_sha256 is distinct from c.final_signed_render_input_sha256)
    ) then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select * into v_hours_component from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='HOURS_TIMESHEET' and c.required=true and c.state='IMMUTABLE'
    for update;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY') and not found then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='55000';
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' and found then
      raise exception 'CONTRACT_EXPENSE_HOURS_COMPONENT_FORBIDDEN' using errcode='55000';
    end if;
    if v_hours_component.id is not null and v_hours_component.review_render_input_sha256
       is distinct from v_hours_component.final_signed_render_input_sha256 then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
      select * into v_candidate_signature from public.candidate_submission_components c
      where c.id=v_workflow.candidate_signature_component_id and c.workflow_id=v_workflow.id
        and c.document_role='CANDIDATE_SIGNATURE' and c.state='IMMUTABLE' for update;
    elsif v_workflow.candidate_signature_component_id is not null
       or v_workflow.candidate_signature_sha256 is not null then
      raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='55000';
    end if;
    if not v_worker_only_approval then
      select * into v_manager_signature from public.candidate_submission_components c
      where c.id=v_workflow.manager_signature_component_id and c.workflow_id=v_workflow.id
        and c.document_role='MANAGER_SIGNATURE' and c.state='IMMUTABLE'
        and c.approval_request_id=v_approved_request.id for update;
    end if;
    if (v_workflow.workflow_kind<>'CONTRACT_EXPENSE' and (
          v_candidate_signature.id is null
          or v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256))
       or (not v_worker_only_approval and (v_manager_signature.id is null
         or v_manager_signature.source_content_sha256 is distinct from v_workflow.manager_signature_sha256
         or v_workflow.manager_approved_at_utc is null)) then
      raise exception 'ELECTRONIC_SIGNATURE_PAIR_INCOMPLETE' using errcode='55000';
    end if;
    if v_worker_only_approval and (
      v_hours_component.final_signed_renderer_receipt_json->>'form_variant' is distinct from 'ELECTRONIC_WORKER_SIGNED'
      or v_hours_component.final_signed_renderer_receipt_json->>'candidate_signature_sha256'
        is distinct from encode(v_candidate_signature.source_content_sha256,'hex')
      or v_hours_component.final_signed_renderer_receipt_json->'candidate_signature_embedded' is distinct from 'true'::jsonb
      or v_hours_component.final_signed_renderer_receipt_json->'manager_signature_embedded' is distinct from 'false'::jsonb
      or v_hours_component.final_signed_renderer_receipt_json->'manager_approval_date_embedded' is distinct from 'false'::jsonb
      or v_workflow.manager_signature_component_id is not null or v_workflow.manager_signature_sha256 is not null
      or v_workflow.manager_name is not null or v_workflow.manager_position is not null
      or v_workflow.manager_approved_at_utc is not null
      or exists(select 1 from public.candidate_approval_requests approval where approval.workflow_id=v_workflow.id
        and approval.workflow_generation=v_workflow.generation and approval.state in ('PENDING','APPROVED'))
    ) then raise exception 'CANDIDATE_WORKER_ONLY_DOCUMENT_INVALID' using errcode='55000';end if;
    v_render_input:=private._candidate_render_input_v1(v_workflow.id,v_workflow.generation);
    if v_hours_component.id is not null and decode(v_render_input->>'render_input_sha256','hex')
       is distinct from decode(
         private._candidate_component_render_input_v1(
           v_workflow.id,v_workflow.generation,v_hours_component.id
         )->>'workflow_render_input_sha256','hex'
       ) then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','ELECTRONIC',
      'auth_name',v_workflow.manager_name,
      'auth_job_title',v_workflow.manager_position,
      'r2_nurse_key',v_candidate_signature.storage_key,
      'r2_auth_key',v_manager_signature.storage_key,
      'img_sha256_nurse',case when v_candidate_signature.source_content_sha256 is null then null
        else encode(v_candidate_signature.source_content_sha256,'hex') end,
      'img_sha256_auth',encode(v_manager_signature.source_content_sha256,'hex'),
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',v_workflow.manager_approved_at_utc
    );
  end if;
  if v_workflow.route='PAPER' then
    v_electronic_patch:=jsonb_build_object(
      'submission_mode','MANUAL',
      'r2_nurse_key',null,
      'r2_auth_key',null,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'candidate_manager_approved_at_utc',null
    );
  end if;

  if coalesce(v_current_policy->>'policy_fingerprint','')
     is distinct from coalesce(v_workflow.policy_snapshot_json->>'policy_fingerprint','') then
    update public.candidate_approval_requests set state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';
    v_response:=jsonb_build_object('ok',false,'error_code','CANDIDATE_POLICY_CHANGED','workflow_id',v_workflow.id,
      'state','SUPERSEDED','generation',v_workflow.generation+1,'current_policy',v_current_policy);
    update public.candidate_submission_workflows set state='SUPERSEDED',generation=generation+1,
      policy_snapshot_json=v_current_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_current_policy),
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
      case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
      case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
      v_response,p_now_utc
    );
    return v_response;
  end if;

  select candidate_app_system_actor_user_id into v_system_actor from public.settings_defaults where id=1;
  if v_system_actor is null then raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED' using errcode='55000'; end if;
  v_input:=v_workflow.immutable_submission_json;
  if v_input is null or private._candidate_sha256_jsonb_v1(v_input)
     is distinct from v_workflow.immutable_submission_sha256 then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH' using errcode='40001';
  end if;
  if v_worker_only_approval then
    if v_current_policy#>'{manager_approval_policy,approval}' is distinct from '"NOT_REQUIRED"'::jsonb then
      raise exception 'CANDIDATE_POLICY_CHANGED' using errcode='40001';end if;
  end if;
  v_workflow.issue_codes:=private._candidate_finalisation_issue_codes_v1(
    v_workflow.issue_codes,
    private._candidate_submission_issue_codes_v1(
      v_workflow.id,v_input,v_current_policy
    )
  );
  v_effective_separation:=coalesce((v_current_policy->>'expenses_require_separate_timesheet')::boolean,false);
  if v_workflow.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
     and coalesce((v_current_policy->>'import_expense_separation_mandatory')::boolean,false)
     and not coalesce((v_current_policy->>'expense_invoice_email_ready')::boolean,false) then
    raise exception 'EXPENSE_INVOICE_EMAIL_REQUIRED' using errcode='55000';
  end if;

  if v_workflow.scope='WEEKLY' then
    select * into v_week from public.contract_weeks where id=v_workflow.contract_week_id for update;
    if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
    if v_workflow.workflow_kind='CONTRACT_COMBINED' then
      v_hours_input:=coalesce(v_input->'hours_submission',v_input);
      v_expense_input:=coalesce(v_input->'expense_submission',v_input);
      if jsonb_typeof(v_hours_input)<>'object' or jsonb_typeof(v_expense_input)<>'object' then
        raise exception 'CANDIDATE_COMBINED_SNAPSHOTS_REQUIRED' using errcode='22023';
      end if;
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      v_expense_input:=v_input;
    else
      v_hours_input:=v_input;
    end if;

    if v_hours_input is not null then
      if v_hours_input->'canonical_tsfin_snapshot' is null then
        raise exception 'CANDIDATE_CANONICAL_TSFIN_SNAPSHOT_REQUIRED' using errcode='22023';
      end if;
      perform set_config('cloudtms.candidate_electronic_finalise',v_workflow.id::text||':'||v_workflow.generation::text,true);
      if v_worker_only_approval then
        v_worker_only_final_state:=private._candidate_weekly_final_state_guard_v1(
          v_workflow.contract_week_id,v_workflow.target_timesheet_id,
          v_hours_input->'timesheet_create_json',coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb),
          v_hours_input->'canonical_tsfin_snapshot');
        if v_worker_only_final_state->>'expected_line_type' is distinct from 'HOURS'
           or v_worker_only_final_state->>'record_role'='IMPORT_HOURS'
           or coalesce((v_worker_only_final_state->>'expense_value')::numeric,-1)<>0 then
          raise exception 'CANDIDATE_FIXED_APPROVAL_HOURS_SCOPE_INVALID' using errcode='55000';end if;
      end if;
      v_hours_result:=public.contract_week_manual_upsert_atomic(
        p_week_id=>v_week.id,
        p_expected_timesheet_id=>v_workflow.target_timesheet_id,
        p_timesheet_create_json=>case when v_workflow.target_timesheet_id is null
          then coalesce(v_hours_input->'timesheet_create_json','{}'::jsonb)||v_electronic_patch else null end,
        p_timesheet_patch_json=>coalesce(v_hours_input->'timesheet_patch_json','{}'::jsonb)||v_electronic_patch,
        p_contract_week_patch_json=>coalesce(v_hours_input->'contract_week_patch_json','{}'::jsonb),
        p_tsfin_snapshot_json=>v_hours_input->'canonical_tsfin_snapshot',
        p_rotation_json=>null,p_actor_user_id=>v_system_actor,p_materialise_staged_evidence=>false,
        p_now_utc=>p_now_utc,p_expected_row_signature=>coalesce(p_expected_row_signature,v_workflow.expected_row_signature),
        p_queue_timesheet_materialisation_json=>jsonb_build_object('suppress_timesheet_evidence_materialisation',true)
      );
      if coalesce((v_hours_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED' using errcode='55000',detail=v_hours_result::text;
      end if;
      v_hours_timesheet_id:=coalesce(nullif(v_hours_result->>'timesheet_id','')::uuid,
        nullif(v_hours_result#>>'{timesheet,timesheet_id}','')::uuid);
      v_after_signature:=coalesce(v_hours_result->>'row_signature',v_hours_result->>'backend_row_signature',
        v_hours_result#>>'{timesheet,row_signature}');
    end if;

    if v_expense_input is not null then
      if v_workflow.workflow_kind='CONTRACT_EXPENSE' or v_effective_separation then
        v_placement:=public.expense_carrier_resolve_or_create_atomic_v1(
          v_candidate_id,v_environment,coalesce(v_hours_timesheet_id,v_workflow.anchor_timesheet_id),
          coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature),
          p_idempotency_key||':carrier',p_now_utc);
      else
        v_placement:=jsonb_build_object(
          'placement','SAME_RECORD','target_timesheet_id',coalesce(v_hours_timesheet_id,v_workflow.target_timesheet_id),
          'target_contract_week_id',v_week.id);
      end if;
      if v_workflow.route='PAPER' then
        select array_agg(c.id order by c.component_no,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.component_kind='SIGNED_RETURN' and c.state='IMMUTABLE'
          and c.paper_return_page_key<>'HOURS_TIMESHEET';
      else
        select array_agg(c.id order by c.review_ordinal,c.id) into v_evidence_component_ids
        from public.candidate_submission_components c
        where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
          and c.required=true and c.state<>'SUPERSEDED' and c.component_kind<>'HOURS_TIMESHEET';
      end if;
      update public.candidate_submission_workflows set
        -- A first combined weekly submission has no anchor until its hours row
        -- is materialised above. Bind that worked row before expense apply so
        -- SAME_RECORD stays a combined HOURS Timesheet. A genuinely separate
        -- expense carrier still has a different target and remains EXPENSES.
        anchor_timesheet_id=case when v_workflow.workflow_kind='CONTRACT_COMBINED'
          then coalesce(anchor_timesheet_id,v_hours_timesheet_id)
          else anchor_timesheet_id end,
        contract_week_id=nullif(v_placement->>'target_contract_week_id','')::uuid,
        target_timesheet_id=nullif(v_placement->>'target_timesheet_id','')::uuid,
        updated_at_utc=p_now_utc
      where id=v_workflow.id;
      perform set_config('cloudtms.candidate_finalize_workflow',v_workflow.id::text||':'||v_workflow.generation::text,true);
      v_result:=public.timesheet_expense_apply_atomic_v1(
        v_candidate_id,v_environment,nullif(v_placement->>'target_timesheet_id','')::uuid,
        v_workflow.id,v_workflow.generation,
        case when v_placement->>'placement'='SAME_RECORD' then coalesce(v_after_signature,p_expected_row_signature,v_workflow.expected_row_signature)
          else null end,
        v_expense_input,v_evidence_component_ids,p_idempotency_key||':expense',p_now_utc);
      v_expense_timesheet_id:=nullif(v_result->>'target_timesheet_id','')::uuid;
      v_target_timesheet_id:=coalesce(v_expense_timesheet_id,v_hours_timesheet_id);
      if v_hours_result is not null then
        v_result:=jsonb_build_object('ok',true,'hours_result',v_hours_result,'expense_result',v_result,
          'hours_timesheet_id',v_hours_timesheet_id,'expense_timesheet_id',v_expense_timesheet_id);
      end if;
    else
      v_result:=v_hours_result;
      v_target_timesheet_id:=v_hours_timesheet_id;
    end if;
  else
    if not v_is_office_service then
      perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
    end if;
    v_target_timesheet_id:=v_workflow.target_timesheet_id;
    if v_target_timesheet_id is null then
      raise exception 'CANDIDATE_DAILY_TIMESHEET_REQUIRED' using errcode='22023';
    end if;
    v_daily_save_input:=private._candidate_daily_canonical_save_input_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_patch:=v_daily_save_input->'timesheet_patch_json';
    if jsonb_typeof(p_daily_materialisation_json)<>'object' then
      raise exception 'CANDIDATE_DAILY_MATERIALISATION_REQUIRED' using errcode='22023';
    end if;
    if v_daily_receipt_only then
      if p_daily_materialisation_json->>'contract_version' is distinct from 'CANDIDATE_DAILY_FACTUAL_RECEIPT_V1'
         or p_daily_materialisation_json->>'workflow_id' is distinct from v_workflow.id::text
         or (p_daily_materialisation_json->>'workflow_generation')::integer is distinct from v_workflow.generation
         or p_daily_materialisation_json->>'timesheet_id' is distinct from v_target_timesheet_id::text then
        raise exception 'CANDIDATE_DAILY_RECEIPT_CONTEXT_CHANGED' using errcode='40001';
      end if;
      v_result:=private._candidate_daily_factual_receipt_v1(
        v_workflow.id,v_workflow.generation,
        p_daily_materialisation_json->>'canonical_save_input_sha256_hex',v_electronic_patch,p_now_utc);
      v_completion_state:='RECEIVED';
    else
    -- This private composition performs the pre-write row-signature check,
    -- factual save and bounded TSFIN write in this same finalisation transaction.
    -- Any later Process/Authorise error rolls the factual and financial write back.
    v_daily_save_receipt:=private._candidate_daily_save_recalculate_atomic_v1(
      v_workflow.id,v_workflow.generation,p_daily_materialisation_json,
      v_system_actor,p_now_utc
    );
    select * into v_daily_timesheet from public.timesheets
    where timesheet_id=v_target_timesheet_id and is_current=true for update;
    select * into v_daily_fin from public.timesheets_financials
    where id=nullif(v_daily_save_receipt->>'financials_id','')::uuid
      and timesheet_id=v_target_timesheet_id and is_current=true
    for update;
    if not found or v_daily_fin.processing_status<>'UNPROCESSED' then
      raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_NOT_READY' using errcode='55000';
    end if;
    v_after_signature:=nullif(v_daily_save_receipt->>'post_save_row_signature','');
    if v_after_signature is null then
      raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_RECEIPT_INVALID' using errcode='55000';
    end if;
    perform set_config('cloudtms.candidate_electronic_finalise','on',true);
    v_result:=public.timesheet_daily_manual_process_atomic(
      v_target_timesheet_id,v_target_timesheet_id,v_system_actor,
      v_electronic_patch,'{}'::jsonb,p_now_utc,v_after_signature
    );
    v_result:=jsonb_build_object(
      'ok',coalesce((v_result->>'ok')::boolean,false),
      'canonical_save_receipt',v_daily_save_receipt,
      'process_result',v_result,'timesheet_id',v_target_timesheet_id,
      'row_signature',coalesce(v_result->>'row_signature',v_result->>'backend_row_signature')
    );
    end if;
    v_hours_timesheet_id:=v_target_timesheet_id;
    v_after_signature:=coalesce(v_result->>'row_signature',v_result->>'backend_row_signature');
  end if;
  if coalesce((v_result->>'ok')::boolean,false)=false then
    raise exception 'CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'
      using errcode='55000',detail=jsonb_build_object(
        'code',coalesce(v_result->>'error_code','CANDIDATE_FINALISE_CANONICAL_APPLY_FAILED'),
        'canonical_result',v_result)::text;
  end if;
  if v_target_timesheet_id is null then raise exception 'CANDIDATE_FINALISE_TARGET_MISSING' using errcode='55000'; end if;

  if v_workflow.route<>'PAPER' and v_hours_component.id is not null then
    update public.candidate_submission_components set timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_hours_component.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Official electronically signed timesheet',
      v_hours_component.final_signed_storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_hours_component.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  elsif v_workflow.route='PAPER' and v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select * into v_paper_hours_return
    from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.component_kind='SIGNED_RETURN' and c.paper_return_page_key='HOURS_TIMESHEET'
      and c.state='IMMUTABLE' and c.source_content_sha256 is not null
    for update;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='55000'; end if;
    update public.candidate_submission_components set
      timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    where id=v_paper_hours_return.id;
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      document_role,candidate_component_id,processing_state
    ) values (
      coalesce(v_hours_timesheet_id,v_target_timesheet_id),'TIMESHEET','Returned signed paper timesheet',
      v_paper_hours_return.storage_key,p_now_utc,v_system_actor,
      'SIGNED_TIMESHEET',v_paper_hours_return.id,'READY'
    ) on conflict (candidate_component_id) where candidate_component_id is not null do nothing;
  end if;

  v_auto_requested:=coalesce((v_current_policy->>'candidate_electronic_auto_authorise')::boolean,false)
    and v_workflow.route<>'PAPER' and not v_worker_only_approval;
  if v_workflow.issue_codes ?| array[
    'UNEXPECTED_HOURS','DAILY_BREAK_UNEXPECTED','DUPLICATE_EXPENSE_REVIEW',
    'HEALTHROSTER_VALIDATION_REQUIRED','EVIDENCE_REVIEW_REQUIRED',
    'ADDITIONAL_UNITS_NEEDS_CHECKING','PLANNED_HOURS_UNRESOLVED'
  ] then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||v_workflow.issue_codes;
  end if;
  if v_workflow.route='PAPER' then
    v_auto_blocked:=true;v_auto_blockers:=v_auto_blockers||'"PAPER_NEVER_AUTO_AUTHORISES"'::jsonb;
  end if;
  if v_daily_receipt_only then
    v_auto_blocked:=true;
    v_auto_blockers:=v_auto_blockers||'"OFFICE_RESOLUTION_REQUIRED"'::jsonb;
  end if;
  if v_auto_requested and not v_auto_blocked then
    if v_hours_timesheet_id is not null then
      v_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_hours_timesheet_id,v_hours_timesheet_id,v_system_actor,p_now_utc,v_after_signature
      );
      if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_authorise_result::text;
      end if;
    end if;
    if v_expense_timesheet_id is not null and v_expense_timesheet_id is distinct from v_hours_timesheet_id then
      v_expense_authorise_result:=public.timesheet_authorise_generic_atomic(
        v_expense_timesheet_id,v_expense_timesheet_id,v_system_actor,p_now_utc,null);
      if coalesce((v_expense_authorise_result->>'ok')::boolean,false)=false then
        raise exception 'CANDIDATE_AUTO_AUTHORISE_FAILED' using errcode='55000',detail=v_expense_authorise_result::text;
      end if;
      v_authorise_result:=jsonb_build_object(
        'ok',true,'hours',v_authorise_result,'expenses',v_expense_authorise_result);
    end if;
    if coalesce((v_authorise_result->>'ok')::boolean,false)=false then
      v_auto_blocked:=true;
      v_auto_blockers:=v_auto_blockers||jsonb_build_array(coalesce(v_authorise_result->>'error_code','AUTHORISE_NOT_ADVANCED'));
    end if;
  end if;

  select financials.id into v_canonical_financials_id
  from public.timesheets_financials financials
  where financials.timesheet_id=coalesce(v_hours_timesheet_id,v_target_timesheet_id)
    and financials.is_current=true
  order by financials.computed_at_utc desc nulls last,financials.updated_at desc,financials.id desc
  limit 1;
  if not v_daily_receipt_only then
    if v_canonical_financials_id is null then
      raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='55000';
    end if;
    v_canonical_financial_sha256:=private._candidate_financial_content_sha256_v1(
      v_canonical_financials_id
    );
  end if;
  -- A received factual claim retains its artifact generation. It is not
  -- financial finalisation and never carries a fabricated financial hash.
  v_completion_generation:=v_workflow.generation+case when v_daily_receipt_only then 0 else 1 end;

  v_response:=jsonb_build_object(
    'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
    'state',v_completion_state,'generation',v_completion_generation,
    'office_resolution_pending',v_daily_receipt_only,
    'timesheet_id',v_target_timesheet_id,'canonical_result',v_result,
    'candidate_auto_authorise_effective',v_auto_requested,
    'auto_authorised',v_auto_requested and not v_auto_blocked,
    'canonical_financial_sha256_hex',encode(v_canonical_financial_sha256,'hex'),
    'auto_authorise_blockers',v_auto_blockers,
    'authorise_result',v_authorise_result
  );
  update public.candidate_submission_workflows set state=v_completion_state,generation=v_completion_generation,
    target_timesheet_id=v_target_timesheet_id,policy_snapshot_json=v_current_policy,
    canonical_financial_sha256=v_canonical_financial_sha256,
    issue_codes=v_workflow.issue_codes,
    finalised_at_utc=case when v_daily_receipt_only then null else p_now_utc end,
    last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,updated_at_utc=p_now_utc
  where id=v_workflow.id;
  perform private._candidate_notification_insert_v1(v_workflow.account_id,v_candidate_id,v_workflow.id,v_target_timesheet_id,
    case when v_auto_requested and not v_auto_blocked then 'AUTHORISED' else 'SUBMISSION_RECEIVED' end,
    'authorisation','candidate-submission-finalised-v1',jsonb_build_object('auto_authorised',v_auto_requested and not v_auto_blocked),
    jsonb_build_object('type','timesheet','timesheet_id',v_target_timesheet_id),
    'CANDIDATE_FINALISED_V1:'||v_workflow.id::text||':'||v_completion_generation::text,p_now_utc);
  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    case when v_daily_receipt_only then 'CANDIDATE_SUBMISSION_RECEIVED' else 'CANDIDATE_SUBMISSION_FINALISED' end,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object('state',v_completion_state,'generation',v_completion_generation,'timesheet_id',v_target_timesheet_id,
      'auto_authorised',v_auto_requested and not v_auto_blocked),null,v_system_actor,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    case when v_is_office_service then nullif(v_service_finalisation->>'actor_user_id','')::uuid
      else null end,
    'candidate_workflow_finalisation_completion',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_FINALISATION_COMPLETED',jsonb_build_object(
      'contract_version','CANDIDATE_FINALISATION_COMPLETION_V1',
      'workflow_generation',p_expected_generation,
      'finalisation_identity_sha256',v_finalisation_identity_hash,
      'finalisation_identity',v_finalisation_identity
    ),v_response,'Canonical finalisation completion receipt',
    p_expected_generation::text||':'||v_finalisation_identity_hash,p_now_utc
  );
  perform private._candidate_workflow_mutation_receipt_v1(
    v_workflow.id,p_idempotency_key,v_mutation_request_hash,'RETRY_FINALISATION',
    case when v_is_office_service then 'OFFICE' when p_session_id is null then 'SERVICE' else 'CANDIDATE_CLIENT' end,
    case when v_is_office_service then v_service_finalisation->>'actor_user_id' else coalesce(p_session_id::text,'SERVICE') end,
    v_response,p_now_utc
  );
  -- Retained native finalisation receipt now proves any artifact-generation
  -- bookkeeping advance. HOURS identity stays the real hours row, including
  -- a combined claim whose separate expense carrier is the final target.
  if v_hours_timesheet_id is not null then
    perform private.weekly_source_checklist_submission_cycle_prepare_v1(v_hours_timesheet_id,null);
  end if;
  return v_response;
exception
  when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name='timesheet_evidence_one_active_timesheet_uq' then
      raise exception 'TIMESHEET_EVIDENCE_ALREADY_ATTACHED' using errcode='23505';
    end if;
    raise;
end;
$function$;
create or replace function public.candidate_workflow_transition_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_action text,
  p_expected_generation integer,
  p_payload jsonb default '{}'::jsonb,
  p_idempotency_key text default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_context jsonb;
  v_account_id uuid;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_source_workflow public.candidate_submission_workflows%rowtype;
  v_existing_workflow public.candidate_submission_workflows%rowtype;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_anchor_week public.contract_weeks%rowtype;
  v_anchor_timesheet public.timesheets%rowtype;
  v_daily_timesheet public.timesheets%rowtype;
  v_daily_fin public.timesheets_financials%rowtype;
  v_daily_receipt_context jsonb;
  v_daily_first_source boolean:=false;
  v_policy jsonb;
  v_approval public.candidate_approval_requests%rowtype;
  v_component public.candidate_submission_components%rowtype;
  v_signature_component public.candidate_submission_components%rowtype;
  v_email_check jsonb;
  v_token_hash bytea;
  v_digest bytea;
  v_submission_hash bytea;
  v_render_input_hash bytea;
  v_manifest_hash bytea;
  v_mail_id uuid;
  v_response jsonb;
  v_manifest jsonb;
  v_render_contract jsonb;
  v_receipt jsonb;
  v_immutable_submission jsonb;
  v_expense_submission jsonb;
  v_component_ids uuid[];
  v_method text;
  v_next_generation integer;
  v_component_no integer;
  v_request_generation integer;
  v_reviewed_count integer;
  v_required_count integer;
  v_constraint_name text;
  v_is_service_action boolean;
  v_is_public_manager_action boolean;
  v_is_electronic boolean;
  v_worker_only_approval boolean:=false;
  v_component_kind text;
  v_document_role text;
  v_expense_category text;
  v_requested_media_type text;
  v_requested_byte_size bigint;
  v_manager_capture_method text;
  v_expected_source_digest bytea;
  v_verified_image_width integer;
  v_verified_image_height integer;
  v_review_ordinal integer;
  v_has_expenses boolean:=false;
  v_has_mileage boolean:=false;
  v_expense_value numeric:=0;
  v_required_categories text[]:=array[]::text[];
  v_required_category text;
  v_paper_manifest jsonb;
  v_paper_source_pages jsonb;
  v_paper_mileage_only boolean:=false;
  v_paper_page_key text;
  v_paper_timesheet_id uuid;
  v_paper_pack_result jsonb;
  v_paper_retirement_result jsonb;
  v_paper_mail public.mail_outbox%rowtype;
  v_paper_mail_id uuid;
  v_paper_manifest_sha256 text;
  v_paper_pack_storage_key text;
  v_paper_pack_sha256 text;
  v_paper_pack_media_type text;
  v_paper_pack_byte_size bigint;
  v_paper_pack_page_count integer;
  v_paper_pack_attachment jsonb;
  v_paper_notification_id uuid;
  v_paper_release_idempotent boolean:=false;
  v_paper_expense_update_active boolean:=false;
  v_paper_outbox_count integer:=0;
  v_paper_base_document_sha256 text;
  v_paper_branding_contract_sha256 text;
  v_paper_renderer_contract_version text;
  v_paper_expected_storage_key text;
  v_provider_lease_token text;
  v_provider_permit_expires_at timestamptz;
  v_manager_mail public.mail_outbox%rowtype;
  v_manager_route_receipt public.candidate_manager_email_route_receipts%rowtype;
  v_manager_mail_kind text;
  v_manager_provider_accepted_at timestamptz;
  v_manager_pending_mail_count integer:=0;
  v_manager_request_ids uuid[]:=array[]::uuid[];
  v_manager_withdrawal_request_id uuid;
  v_manager_withdrawal_count integer:=0;
  v_manager_retirement_result jsonb;
  v_submission_withdrawal_reset jsonb;
  v_cancel_reason text;
  v_cancel_reason_code text;
  v_audit_reason text;
  v_unlocked_workflow_updated_at timestamptz;
  v_paper_family_key text;
  v_source_component public.candidate_submission_components%rowtype;
  v_all_final_ready boolean:=false;
  v_server_issue_codes jsonb:='[]'::jsonb;
  v_duplicate_expense_review jsonb:='{}'::jsonb;
  v_duplicate_expense_category text;
  v_request_id uuid;
  v_workflow_kind text;
  v_scope text;
  v_route text;
  v_canonical_work_date date;
  v_canonical_week_ending_date date;
  v_client_id uuid;
  v_anchor_candidate_count integer:=0;
  v_anchor_week_id uuid;
  v_anchor_submitted_work boolean:=false;
  v_target_capabilities jsonb;
  v_route_authority jsonb;
  v_weekly_source_candidate_request_allowed boolean:=false;
  v_daily_input jsonb;
  v_daily_patch jsonb;
  v_expected_save_hash bytea;
  v_daily_context jsonb;
  v_daily_context_hash bytea;
  v_current_row_signature text;
  v_insert_workflow_id uuid;
  v_replacement_of_workflow_id uuid;
  v_is_rejected_resubmission boolean:=false;
  v_creation_request_identity jsonb;
  v_creation_identity jsonb;
  v_creation_request_sha256 bytea;
  v_initial_route text;
  v_source_anchor public.timesheets%rowtype;
  v_daily_booking_id text;
  v_paper_failure_code text;
  v_paper_failure_class text;
  v_paper_failure_retryable boolean:=false;
  v_paper_pack_attempt_token text;
  v_paper_pack_attempt_count integer:=0;
  v_paper_pack_attempt_expires_at timestamptz;
  v_paper_pack_next_retry_at timestamptz;
  v_paper_pack_operation_id text;
  v_office_actor_user_id uuid;
  v_is_office_service_action boolean:=false;
  v_is_internal_paper_service_action boolean:=false;
  v_mutation_channel text;
  v_mutation_actor_identity text;
  v_mutation_semantic_payload jsonb;
  v_mutation_request_sha256 text;
  v_mutation_receipt jsonb;
  v_mutation_replay_probe_only boolean:=false;
  v_expense_update_context jsonb;
  v_pending_expense_update public.candidate_pending_expense_updates%rowtype;
  v_is_pending_expense_update boolean:=false;
  v_replaced_manager_signature_ids uuid[]:=array[]::uuid[];
  v_checklist_cycle_admission jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_workflow_id is null or jsonb_typeof(v_payload)<>'object' then
    raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if v_payload ?| array['password','refresh_token','token'] then
    raise exception 'CANDIDATE_WORKFLOW_PLAINTEXT_SECRET_FORBIDDEN' using errcode='22023';
  end if;

  if v_action='SELECT_APPROVAL_METHOD' then
    v_method:=upper(coalesce(v_payload->>'method',''));
    v_action:=case v_method
      when 'PHONE' then 'SELECT_PHONE_APPROVAL'
      when 'EMAIL' then 'CREATE_EMAIL_APPROVAL_REQUEST'
      when 'PAPER' then 'PAPER_PREPARE'
      else v_action end;
  elsif v_action='EMAIL_REQUEST' then
    v_action:='CREATE_EMAIL_APPROVAL_REQUEST';
  elsif v_action='MANAGER_REVIEW' then
    v_action:='BEGIN_MANAGER_REVIEW';
  elsif v_action='REGISTER_MANAGER_REVIEW_DOCUMENT' then
    v_action:='REGISTER_REVIEW_COMPONENT';
  end if;
  v_mutation_replay_probe_only:=p_session_id is not null
    and coalesce((v_payload->>'mutation_replay_probe_only')::boolean,false);
  if v_mutation_replay_probe_only
     and jsonb_typeof(v_payload->'mutation_replay_semantic_payload') is distinct from 'object' then
    raise exception 'CANDIDATE_IDEMPOTENCY_RECEIPT_INVALID' using errcode='22023';
  end if;

  if coalesce(v_payload->>'actor_user_id','')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_office_actor_user_id:=(v_payload->>'actor_user_id')::uuid;
  end if;
  v_is_office_service_action:=p_session_id is null
    and coalesce((v_payload->>'service_office_action')::boolean,false)
    and private._candidate_office_service_context_valid_v1(
      v_environment,v_office_actor_user_id,v_action
    );
  v_is_internal_paper_service_action:=p_session_id is null and (
    (v_action='PAPER_PACK_RELEASE'
      and coalesce((v_payload->>'service_paper_pack_release')::boolean,false))
    or (v_action='PAPER_PROVIDER_SUBMIT_PERMIT'
      and coalesce((v_payload->>'service_paper_provider_submit_permit')::boolean,false))
    or (v_action='PAPER_PACK_MARK_FAILURE'
      and coalesce((v_payload->>'service_paper_pack_failure')::boolean,false))
    or (v_action='PAPER_PACK_ATTEMPT_CLAIM'
      and coalesce((v_payload->>'service_paper_pack_attempt')::boolean,false))
  );
  if not v_is_office_service_action and not v_is_internal_paper_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;

  if v_action in (
    'SELECT_PHONE_APPROVAL','CREATE_EMAIL_APPROVAL_REQUEST','BEGIN_MANAGER_REVIEW',
    'RECORD_REVIEW_PROGRESS','PHONE_APPROVE','EMAIL_APPROVE','MANAGER_REFUSE',
    'REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
    'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT',
    'MANAGER_PROVIDER_SUBMIT_PERMIT'
  ) or (p_session_id is null and v_action in ('COMPONENT_PREPARE','COMPONENT_COMPLETE')) then
    if not v_is_office_service_action and not v_is_internal_paper_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_manager_approval');
    end if;
  end if;
  if v_action='BEGIN_CANONICAL_DAILY_SAVE' then
    if not v_is_office_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_daily_finalisation');
    end if;
  end if;
  if v_action in (
    'PAPER_PREPARE','PAPER_RETURN','PAPER_PACK_RELEASE',
    'PAPER_PROVIDER_SUBMIT_PERMIT','PAPER_PACK_MARK_FAILURE','PAPER_PACK_ATTEMPT_CLAIM'
  ) then
    if not v_is_office_service_action and not v_is_internal_paper_service_action then
    perform private._candidate_require_feature_v1(v_environment,'candidate_paper_qr');
    end if;
  end if;
  if v_action='MARK_READ' then
    perform private._candidate_require_feature_v1(v_environment,'candidate_notifications');
  end if;

  v_is_service_action:=v_action in (
    'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT',
    'BEGIN_CANONICAL_DAILY_SAVE'
  ) or (p_session_id is null
    and v_action='PAPER_PACK_RELEASE'
    and coalesce((v_payload->>'service_paper_pack_release')::boolean,false)
  ) or (p_session_id is null
    and v_action='PAPER_PROVIDER_SUBMIT_PERMIT'
    and coalesce((v_payload->>'service_paper_provider_submit_permit')::boolean,false)
  ) or (p_session_id is null
    and v_action='PAPER_PACK_MARK_FAILURE'
    and coalesce((v_payload->>'service_paper_pack_failure')::boolean,false)
  ) or (p_session_id is null
    and v_action='PAPER_PACK_ATTEMPT_CLAIM'
    and coalesce((v_payload->>'service_paper_pack_attempt')::boolean,false)
  ) or (p_session_id is null
    and v_action='MANAGER_PROVIDER_SUBMIT_PERMIT'
    and coalesce((v_payload->>'service_manager_provider_submit_permit')::boolean,false)
  ) or (p_session_id is null
    and coalesce((v_payload->>'service_phone_approval')::boolean,false)
    and v_action in ('BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
      'COMPONENT_PREPARE','COMPONENT_COMPLETE'))
  or (v_is_office_service_action
    and v_action in ('CANCEL','REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
      'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
      'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT',
      'BEGIN_CANONICAL_DAILY_SAVE','PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM',
      'PAPER_PACK_MARK_FAILURE','WORKER_SUBMIT'));
  v_is_public_manager_action:=not v_is_service_action and p_session_id is null and v_action in (
    'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','EMAIL_APPROVE','MANAGER_REFUSE',
    'COMPONENT_PREPARE','COMPONENT_COMPLETE'
  );

  -- Checklist lifecycle admission precedes all workflow/TS/TF business locks.
  -- Plain native session authentication does not replace the locked recheck.
  if v_action='WORKER_SUBMIT' and (p_session_id is not null or v_is_office_service_action) then
    if p_session_id is not null then
      v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,false);
      v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
      if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
    else
      select candidate_id into v_candidate_id from public.candidate_submission_workflows
        where id=p_workflow_id and environment=v_environment;
    end if;
    v_checklist_cycle_admission:=private.weekly_source_checklist_submission_cycle_admit_v1(
      p_workflow_id,v_environment,v_candidate_id);
  end if;

  if v_is_service_action then
    if v_action='PAPER_PACK_RELEASE' then
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id and environment=v_environment;
      if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
      v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
      if v_paper_timesheet_id is null then
        raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
      end if;
      perform 1 from public.timesheets
      where timesheet_id=v_paper_timesheet_id and is_current=true and archived_at_utc is null
      for update;
      if not found then raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000'; end if;
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id and environment=v_environment
      for update;
      if not found
         or coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
              is distinct from v_paper_timesheet_id then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
      end if;
    else
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id and environment=v_environment
      for update;
    end if;
    if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
    if v_is_office_service_action
       and v_action in ('REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
         'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE') then
      if coalesce(v_payload->>'approval_request_id','')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
         or coalesce(v_payload->>'approval_request_generation','') !~ '^[1-9][0-9]{0,8}$' then
        raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
      end if;
      perform 1
      from public.candidate_approval_requests office_request
      where office_request.id=(v_payload->>'approval_request_id')::uuid
        and office_request.workflow_id=v_workflow.id
        and office_request.workflow_generation=v_workflow.generation
        and office_request.request_generation=(v_payload->>'approval_request_generation')::integer;
      if not found then
        raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
      end if;
    end if;
    v_account_id:=v_workflow.account_id;
    v_candidate_id:=v_workflow.candidate_id;
  elsif v_is_public_manager_action then
    if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
    select a.id,a.workflow_id into v_request_id,v_workflow.id
    from public.candidate_approval_requests a
    where a.token_hash=v_token_hash and a.workflow_id=p_workflow_id;
    if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    select * into v_workflow
    from public.candidate_submission_workflows
    where id=p_workflow_id and environment=v_environment
    for update;
    if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    select * into v_approval
    from public.candidate_approval_requests
    where id=v_request_id
      and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and method in ('EMAIL','PHONE')
      and state='PENDING'
      and expires_at_utc>p_now_utc
      and review_manifest_sha256=v_workflow.review_manifest_sha256
    for update;
    if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    v_account_id:=v_workflow.account_id;
    v_candidate_id:=v_workflow.candidate_id;
  else
    v_context:=private._candidate_session_context_v1(p_session_id,v_environment,null,p_now_utc,true);
    v_account_id:=nullif(v_context->>'account_id','')::uuid;
    v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
    if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  end if;

  if v_action='RESUBMIT_REJECTED' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null
       or btrim(p_idempotency_key) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if p_expected_generation is null or p_expected_generation<1 then
      raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='55000';
    end if;

    v_creation_request_identity:=jsonb_build_object(
      'version','CANDIDATE_WORKFLOW_CREATION_REQUEST_V1',
      'action','RESUBMIT_REJECTED',
      'environment',v_environment,
      'account_id',v_account_id,
      'candidate_id',v_candidate_id,
      'rejected_workflow_id',p_workflow_id,
      'rejected_workflow_generation',p_expected_generation
    );
    v_creation_request_sha256:=private._candidate_workflow_creation_request_sha256_v1(
      v_creation_request_identity
    );

    -- One account/key lock serialises exact retries. The source lock then
    -- serialises every key that attempts to replace the same rejected row.
    perform pg_advisory_xact_lock(hashtextextended(
      'candidate-workflow-idempotency|'||v_account_id::text||'|'||btrim(p_idempotency_key),0
    ));
    perform pg_advisory_xact_lock(hashtextextended(
      'candidate-rejected-source|'||p_workflow_id::text,0
    ));

    select * into v_source_workflow
    from public.candidate_submission_workflows
    where id=p_workflow_id
      and environment=v_environment
      and account_id=v_account_id
      and candidate_id=v_candidate_id
    for update;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
    end if;
    if v_source_workflow.generation<>p_expected_generation then
      raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='55000';
    end if;
    if v_source_workflow.state not in ('REJECTED','REFUSED') then
      raise exception 'CANDIDATE_REJECTED_WORKFLOW_NOT_RESUBMITTABLE' using errcode='55000';
    end if;

    select * into v_existing_workflow
    from public.candidate_submission_workflows
    where account_id=v_account_id
      and idempotency_key=btrim(p_idempotency_key)
    for update;
    if found then
      if v_existing_workflow.replacement_of_workflow_id is distinct from v_source_workflow.id
         or v_existing_workflow.candidate_id is distinct from v_source_workflow.candidate_id
         or v_existing_workflow.creation_request_sha256 is distinct from v_creation_request_sha256 then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='55000';
      end if;
      return jsonb_build_object(
        'ok',true,'idempotent_replay',true,
        'rejected_workflow_id',v_source_workflow.id,
        'replacement_workflow_id',v_existing_workflow.id,
        'replacement_created',false,
        'workflow_id',v_existing_workflow.id,
        'state',v_existing_workflow.state,
        'generation',v_existing_workflow.generation
      );
    end if;

    select * into v_existing_workflow
    from public.candidate_submission_workflows
    where replacement_of_workflow_id=v_source_workflow.id
    for update;
    if found or private._candidate_rejection_replaced_v1(v_source_workflow.id) then
      raise exception 'CANDIDATE_REJECTED_WORKFLOW_ALREADY_REPLACED'
        using errcode='55000',detail=case when v_existing_workflow.id is null then null
          else jsonb_build_object('replacement_workflow_id',v_existing_workflow.id)::text end;
    end if;

    v_is_rejected_resubmission:=true;
    v_replacement_of_workflow_id:=v_source_workflow.id;
    v_insert_workflow_id:=gen_random_uuid();
    if v_source_workflow.workflow_kind='DAILY' then
      v_daily_booking_id:=nullif(btrim(coalesce(
        v_source_workflow.creation_identity_json#>>'{derived,daily_booking_id}',
        ''
      )), '');
      if v_daily_booking_id is null then
        select nullif(btrim(coalesce(source_timesheet.booking_id,'')),'')
        into v_daily_booking_id
        from public.timesheets source_timesheet
        where source_timesheet.timesheet_id=coalesce(
          v_source_workflow.target_timesheet_id,v_source_workflow.anchor_timesheet_id
        );
      end if;
      select current_timesheet.* into v_daily_timesheet
      from public.timesheets current_timesheet
      where current_timesheet.booking_id=v_daily_booking_id
        and current_timesheet.is_current=true
        and current_timesheet.archived_at_utc is null
        and current_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum;
      if not found then
        raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
      end if;
      v_workflow_kind:='DAILY';
      v_scope:='DAILY';
      v_initial_route:=upper(coalesce(
        v_source_workflow.creation_identity_json#>>'{derived,initial_route}',
        v_source_workflow.route
      ));
      if v_initial_route not in ('EMAIL','PHONE') then
        raise exception 'CANDIDATE_REJECTED_REPLACEMENT_ROUTE_INVALID' using errcode='55000';
      end if;
      v_payload:=jsonb_build_object(
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'route',v_initial_route,
        'target_timesheet_id',v_daily_timesheet.timesheet_id
      );
    else
      v_workflow_kind:=case
        when v_source_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
          or v_source_workflow.workflow_kind='CONTRACT_EXPENSE'
          then 'CONTRACT_EXPENSE'
        when v_source_workflow.workflow_kind='CONTRACT_COMBINED'
          then 'CONTRACT_COMBINED'
        else 'CONTRACT_HOURS'
      end;
      v_scope:='WEEKLY';

      -- Refusal can return the exact Contract Week to an empty editable state:
      -- the refused Timesheet remains revoked history and timesheet_id becomes
      -- null.  The Contract Week is therefore the stable weekly authority.
      -- When it does own a current Timesheet, use only that exact row; never
      -- adopt another same-contract/date carrier.
      select current_week.* into v_week
      from public.contract_weeks current_week
      where current_week.id=v_source_workflow.contract_week_id
        and current_week.contract_id is not distinct from v_source_workflow.contract_id
        and current_week.week_ending_date is not distinct from v_source_workflow.week_ending_date
      for update;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002';
      end if;
      if v_week.timesheet_id is not null then
        select current_timesheet.* into v_anchor_timesheet
        from public.timesheets current_timesheet
        where current_timesheet.timesheet_id=v_week.timesheet_id
          and current_timesheet.is_current=true
          and current_timesheet.archived_at_utc is null
          and current_timesheet.sheet_scope='WEEKLY'::public.timesheet_scope_enum
        for update;
        if not found then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='55000';
        end if;
      end if;

      v_initial_route:=upper(coalesce(
        v_source_workflow.creation_identity_json#>>'{derived,initial_route}',
        case when v_source_workflow.route='PAPER' then 'PAPER' else 'ELECTRONIC' end
      ));
      v_route_authority:=private._candidate_route_family_v1(
        v_week.timesheet_id,v_week.id
      );
      if v_initial_route='PAPER'
         and (
           v_route_authority->>'route_family'='QR'
           or (
             v_route_authority->>'route_family'='ELECTRONIC'
             and coalesce(
               (v_route_authority->>'candidate_paper_submission_allowed')::boolean,
               false
             )
           )
         ) then
        v_initial_route:='PAPER';
      else
        v_initial_route:='ELECTRONIC';
      end if;
      v_payload:=jsonb_build_object(
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'route',v_initial_route,
        'contract_id',v_source_workflow.contract_id,
        'contract_week_id',v_week.id,
        'week_ending_date',v_week.week_ending_date,
        'anchor_timesheet_id',case when v_workflow_kind='CONTRACT_EXPENSE'
          then v_source_workflow.anchor_timesheet_id else v_week.timesheet_id end
      );
    end if;
    v_action:='CREATE';
  end if;

  if v_action='CREATE' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if not v_is_rejected_resubmission then
      perform pg_advisory_xact_lock(hashtextextended(
        'candidate-workflow-idempotency|'||v_account_id::text||'|'||btrim(p_idempotency_key),0
      ));
      v_insert_workflow_id:=p_workflow_id;
    end if;
    v_workflow_kind:=upper(coalesce(v_payload->>'workflow_kind',''));
    v_scope:=upper(coalesce(v_payload->>'scope',''));
    v_route:=upper(coalesce(v_payload->>'route',''));
    if v_workflow_kind not in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED','DAILY')
       or v_scope not in ('WEEKLY','DAILY')
       or v_route not in ('ELECTRONIC','PHONE','EMAIL','PAPER') then
      raise exception 'CANDIDATE_WORKFLOW_TYPE_INVALID' using errcode='22023';
    end if;
    v_daily_first_source:=v_workflow_kind='DAILY' and v_payload ? 'daily_source';
    if v_daily_first_source and (
      v_scope<>'DAILY' or v_route<>'PHONE'
      or jsonb_typeof(v_payload->'daily_source') is distinct from 'object'
      or v_payload->'submission_requested' is distinct from 'true'::jsonb
      or v_payload ?| array['target_timesheet_id','contract_id','contract_week_id','week_ending_date','anchor_timesheet_id']
    ) then
      raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
    end if;
    if not v_is_rejected_resubmission then
      if v_workflow_kind='DAILY' and not v_daily_first_source then
        select requested_timesheet.* into v_source_anchor
        from public.timesheets requested_timesheet
        where requested_timesheet.timesheet_id=nullif(v_payload->>'target_timesheet_id','')::uuid;
        if not found then
          raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002';
        end if;
      end if;
      v_creation_request_identity:=jsonb_strip_nulls(jsonb_build_object(
        'version','CANDIDATE_WORKFLOW_CREATION_REQUEST_V1',
        'action','CREATE',
        'environment',v_environment,
        'account_id',v_account_id,
        'candidate_id',v_candidate_id,
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'initial_route',v_route,
        'contract_id',nullif(v_payload->>'contract_id','')::uuid,
        'contract_week_id',nullif(v_payload->>'contract_week_id','')::uuid,
        'week_ending_date',nullif(v_payload->>'week_ending_date','')::date,
        'work_date',nullif(v_payload->>'work_date','')::date,
        'daily_source',case when v_daily_first_source then v_payload->'daily_source' else null end,
        'submission_requested',case when v_daily_first_source then true else null end,
        'daily_booking_id',case when v_workflow_kind='DAILY'
          then coalesce(nullif(btrim(coalesce(v_source_anchor.booking_id,'')),''),
            v_payload#>>'{daily_source,booking_id}') else null end,
        'target_timesheet_id',case when v_workflow_kind='DAILY'
          and nullif(btrim(coalesce(v_source_anchor.booking_id,'')),'') is null
          then v_source_anchor.timesheet_id else null end,
        'requested_anchor_timesheet_id',nullif(v_payload->>'anchor_timesheet_id','')::uuid,
        'input_snapshot',coalesce(v_payload->'input_snapshot','{}'::jsonb),
        'expected_row_signature',nullif(v_payload->>'expected_row_signature','')
      ));
      v_creation_request_sha256:=private._candidate_workflow_creation_request_sha256_v1(
        v_creation_request_identity
      );
      select * into v_existing_workflow
      from public.candidate_submission_workflows
      where account_id=v_account_id
        and idempotency_key=btrim(p_idempotency_key)
      for update;
      if found then
        if v_existing_workflow.candidate_id is distinct from v_candidate_id
           or v_existing_workflow.replacement_of_workflow_id is not null
           or v_existing_workflow.creation_request_sha256 is distinct from v_creation_request_sha256 then
          raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT' using errcode='40001';
        end if;
        return jsonb_build_object(
          'ok',true,'idempotent_replay',true,
          'workflow_id',v_existing_workflow.id,
          'state',v_existing_workflow.state,
          'generation',v_existing_workflow.generation
        );
      end if;
    end if;
    if v_workflow_kind='DAILY' then
      if v_daily_first_source then
        -- CREATE is invoked by the explicit submit action, not by editing a day.
        -- Its exact request receipt above is replayed before any source recheck.
        v_payload:=v_payload||jsonb_build_object('target_timesheet_id',
          private._candidate_daily_first_receipt_v1(v_environment,v_candidate_id,
            v_payload->'daily_source',v_insert_workflow_id,true,p_now_utc));
      end if;
      if v_scope<>'DAILY' or v_route<>'PHONE'
         or nullif(v_payload->>'target_timesheet_id','') is null
         or nullif(v_payload->>'contract_week_id','') is not null
         or nullif(v_payload->>'week_ending_date','') is not null then
        raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
      end if;
      select * into v_daily_timesheet
      from public.timesheets
      where timesheet_id=(v_payload->>'target_timesheet_id')::uuid
        and is_current=true
        and archived_at_utc is null
        and sheet_scope='DAILY'::public.timesheet_scope_enum
        and nullif(btrim(coalesce(booking_id,'')),'') is not null
      for update;
      if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
      v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
        v_environment,v_candidate_id,v_daily_timesheet.timesheet_id,true,p_now_utc);
      if not private._candidate_daily_entitled_v1(v_candidate_id) then
        raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
      end if;
      select * into v_daily_fin
      from public.timesheets_financials
      where timesheet_id=v_daily_timesheet.timesheet_id
        and is_current=true
        and candidate_id=v_candidate_id
      order by computed_at_utc desc nulls last,updated_at desc,id desc
      limit 1
      for update;
      -- The locked receipt context proves ownership even before Office has
      -- assigned financial context. Absence of TSFIN is not permission to write it.
      if v_daily_fin.authorised_at_utc is not null
         or v_daily_fin.paid_at_utc is not null
         or v_daily_fin.locked_by_invoice_id is not null
         or v_daily_timesheet.archived_at_utc is not null then
        raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
      end if;
      v_canonical_work_date:=private._candidate_daily_work_date_v1(
        coalesce(v_daily_fin.worked_start_iso,v_daily_timesheet.worked_start_iso),
        v_daily_timesheet.scheduled_start_iso,
        v_daily_timesheet.week_ending_date
      );
      if v_canonical_work_date is null
         or (nullif(v_payload->>'work_date','') is not null
             and (v_payload->>'work_date')::date<>v_canonical_work_date)
         or (nullif(v_payload->>'anchor_timesheet_id','') is not null
             and (v_payload->>'anchor_timesheet_id')::uuid<>v_daily_timesheet.timesheet_id) then
        raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='22023';
      end if;
      if v_daily_timesheet.contract_id is not null then
        select * into v_contract
        from public.contracts
        where id=v_daily_timesheet.contract_id and candidate_id=v_candidate_id
        for update;
        if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
      end if;
      v_client_id:=nullif(v_daily_receipt_context->>'client_id','')::uuid;
      v_policy:=v_daily_receipt_context->'policy';
      if (v_route='PHONE' and not coalesce((v_policy->>'allow_daily_manager_authorise_on_phone')::boolean,false))
         or (v_route='EMAIL' and not coalesce((v_policy->>'allow_daily_manager_authorise_by_email')::boolean,false)) then
        raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_NOT_ALLOWED' using errcode='55000';
      end if;
      v_canonical_week_ending_date:=null;
    else
      if v_scope<>'WEEKLY' or v_route not in ('ELECTRONIC','PAPER')
         or nullif(v_payload->>'contract_id','') is null
         or nullif(v_payload->>'contract_week_id','') is null then
        raise exception 'CANDIDATE_CONTRACT_WORKFLOW_IDENTITY_REQUIRED' using errcode='22023';
      end if;
      select * into v_contract
      from public.contracts
      where id=(v_payload->>'contract_id')::uuid and candidate_id=v_candidate_id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
      select * into v_week
      from public.contract_weeks
      where id=(v_payload->>'contract_week_id')::uuid and contract_id=v_contract.id
      for update;
      if not found then raise exception 'CANDIDATE_WORKFLOW_WEEK_NOT_FOUND' using errcode='P0002'; end if;
      v_canonical_week_ending_date:=v_week.week_ending_date;
      if nullif(v_payload->>'week_ending_date','') is not null
         and (v_payload->>'week_ending_date')::date<>v_canonical_week_ending_date then
        raise exception 'CANDIDATE_WORKFLOW_WEEK_MISMATCH' using errcode='22023';
      end if;
      v_policy:=private._expense_approval_policy_v1(v_contract.client_id,v_contract.id,v_canonical_week_ending_date,v_workflow_kind);
      -- Resolve and validate the worked anchor before route admission.  A
      -- server-created expense carrier deliberately has no worked Timesheet
      -- and uses MANUAL storage, so it cannot define the Candidate's manager
      -- approval route.
      if nullif(v_payload->>'anchor_timesheet_id','') is not null then
        select cw.* into v_anchor_week
        from public.contract_weeks cw
        join public.timesheets t on t.timesheet_id=cw.timesheet_id
          and t.is_current=true and t.archived_at_utc is null
          and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
        where cw.timesheet_id=(v_payload->>'anchor_timesheet_id')::uuid
          and cw.contract_id=v_contract.id
          and cw.week_ending_date=v_canonical_week_ending_date;
        if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023'; end if;
        if v_workflow_kind='CONTRACT_EXPENSE'
           and coalesce((private._candidate_record_capabilities_v1(v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb)->>'hours_value')::numeric,0)<=0
           and coalesce((private._candidate_record_capabilities_v1(v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb)->>'additional_units_value')::numeric,0)<=0 then
          -- Import-authoritative hours remain outside TSFIN until source
          -- finalisation.  The immutable Candidate hours submission for this
          -- exact anchor is positive worked-week evidence, while the expense
          -- itself still opens as a separate manager-approved workflow.
          select exists(
            select 1
            from public.candidate_submission_workflows submitted_workflow
            cross join lateral (
              select
                coalesce(
                  submitted_workflow.input_snapshot_json#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
                  submitted_workflow.input_snapshot_json#>'{timesheet_patch_json,actual_schedule_json}',
                  submitted_workflow.input_snapshot_json->'actual_schedule_json',
                  '[]'::jsonb
                ) as actual_schedule_json,
                coalesce(
                  submitted_workflow.input_snapshot_json#>'{hours_submission,timesheet_patch_json,additional_units_week}',
                  submitted_workflow.input_snapshot_json#>'{timesheet_patch_json,additional_units_week}',
                  submitted_workflow.input_snapshot_json->'additional_units_week',
                  '{}'::jsonb
                ) as additional_units_week,
                coalesce(
                  submitted_workflow.input_snapshot_json#>'{hours_submission,timesheet_patch_json,additional_units_per_day}',
                  submitted_workflow.input_snapshot_json#>'{timesheet_patch_json,additional_units_per_day}',
                  submitted_workflow.input_snapshot_json->'additional_units_per_day',
                  '{}'::jsonb
                ) as additional_units_per_day
            ) submitted
            where submitted_workflow.environment=v_environment
              and submitted_workflow.candidate_id=v_candidate_id
              and submitted_workflow.contract_id=v_contract.id
              and submitted_workflow.contract_week_id=v_anchor_week.id
              and submitted_workflow.week_ending_date=v_canonical_week_ending_date
              and submitted_workflow.anchor_timesheet_id=v_anchor_week.timesheet_id
              and submitted_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
              and submitted_workflow.state in (
                'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
                'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
                'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
                'READY_TO_FINALISE','RECEIVED','FINALISED'
              )
              and (
                (pg_catalog.jsonb_typeof(submitted.actual_schedule_json)='array'
                  and pg_catalog.jsonb_array_length(submitted.actual_schedule_json)>0)
                or private._candidate_json_numeric_sum(submitted.additional_units_week)>0
                or private._candidate_json_numeric_sum(submitted.additional_units_per_day)>0
              )
          ) into v_anchor_submitted_work;
          if not v_anchor_submitted_work then
            raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED' using errcode='22023';
          end if;
        end if;
      elsif v_workflow_kind='CONTRACT_EXPENSE' then
        -- A terminal workflow does not invalidate its immutable anchor receipt.
        -- Reuse one exact historical anchor for this carrier; contradictory
        -- history remains blocked. Only a carrier with no history uses the
        -- original same-week single-worked-row fallback.
        select count(distinct prior.anchor_timesheet_id)::integer,
          case when count(distinct prior.anchor_timesheet_id)=1
            then min(prior.anchor_timesheet_id::text)::uuid else null::uuid end
        into v_anchor_candidate_count,v_anchor_week_id
        from public.candidate_submission_workflows prior
        where prior.environment=v_environment
          and prior.candidate_id=v_candidate_id
          and prior.contract_id=v_contract.id
          and prior.contract_week_id=v_week.id
          and prior.week_ending_date=v_canonical_week_ending_date
          and prior.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
          and prior.anchor_timesheet_id is not null;

        if v_anchor_candidate_count>1 then
          raise exception 'EXPENSE_WORKED_ANCHOR_HISTORY_AMBIGUOUS' using errcode='55000';
        elsif v_anchor_candidate_count=1 then
          select cw.* into v_anchor_week
          from public.contract_weeks cw
          join public.timesheets t on t.timesheet_id=cw.timesheet_id
            and t.is_current=true and t.archived_at_utc is null
            and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          where cw.timesheet_id=v_anchor_week_id
            and cw.contract_id=v_contract.id
            and cw.week_ending_date=v_canonical_week_ending_date;
          if not found then
            raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023';
          end if;
          if coalesce((private._candidate_record_capabilities_v1(
               v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb
             )->>'hours_value')::numeric,0)<=0
             and coalesce((private._candidate_record_capabilities_v1(
               v_anchor_week.timesheet_id,v_anchor_week.id,'{}'::jsonb
             )->>'additional_units_value')::numeric,0)<=0 then
            raise exception 'CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED' using errcode='22023';
          end if;
        else
          select count(*)::integer,min(worked.id::text)::uuid
          into v_anchor_candidate_count,v_anchor_week_id
          from public.contract_weeks worked
          join public.timesheets t on t.timesheet_id=worked.timesheet_id
            and t.is_current=true and t.archived_at_utc is null
            and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
          where worked.contract_id=v_contract.id
            and worked.week_ending_date=v_canonical_week_ending_date
            and (
              coalesce(tf.total_hours,0)>0
              or private._candidate_json_numeric_sum(coalesce(tf.additional_units_json,'{}'::jsonb))>0
              or private._candidate_json_numeric_sum(coalesce(t.additional_units_week,'{}'::jsonb))
                +private._candidate_json_numeric_sum(coalesce(t.additional_units_per_day,'{}'::jsonb))>0
            );
          if v_anchor_candidate_count=0 then raise exception 'NO_POSITIVE_WORKED_TIME' using errcode='55000'; end if;
          if v_anchor_candidate_count>1 then raise exception 'EXPENSE_WORKED_ANCHOR_AMBIGUOUS' using errcode='55000'; end if;
          select * into v_anchor_week from public.contract_weeks where id=v_anchor_week_id;
        end if;
      elsif v_week.timesheet_id is not null then
        v_anchor_week:=v_week;
      end if;
      v_route_authority:=private._expense_approval_route_v1(
        case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.timesheet_id else v_week.timesheet_id end,
        case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week.id else v_week.id end,v_workflow_kind
      );
      -- Source-authority hours use either an exact live Office request or the
      -- candidate-initiated CHECK_ONLY week route.  This only admits a draft;
      -- the dedicated signed submit RPC rechecks all source and week guards.
      if v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
         and v_workflow_kind='CONTRACT_HOURS'
         and v_route='ELECTRONIC' then
        select exists(
          select 1
          from public.weekly_timesheet_submission_requests submission
          join public.weekly_candidate_outreach_generations generation
            on generation.candidate_cohort_id=submission.candidate_cohort_id
           and generation.source_cycle_id=submission.source_cycle_id
           and generation.candidate_id=submission.candidate_id
           and generation.generation_number=submission.request_generation
           and generation.request_kind='SUBMIT_TIMESHEET'
          join public.weekly_timesheet_submission_request_memberships membership
            on membership.submission_request_id=submission.id
          where v_week.timesheet_id is null
            and submission.candidate_id=v_candidate_id
            and submission.state in ('ACTIVE','PARTLY_SUBMITTED')
            and generation.state='ACTIVE'
            and generation.deadline_at_utc>=p_now_utc
            and membership.state='WAITING'
            and membership.contract_id=v_contract.id
            and membership.week_ending=v_canonical_week_ending_date
        ) or exists(
          select 1
          from public.weekly_candidate_outreach_generations generation
          join public.weekly_candidate_outreach_memberships membership
            on membership.candidate_generation_id=generation.id
          join public.weekly_discrepancy_incidents incident
            on incident.id=membership.incident_id
          join public.weekly_issue_comparison_revisions comparison
            on comparison.id=incident.current_comparison_revision_id
          where v_week.timesheet_id is not null
            and generation.candidate_id=v_candidate_id
            and generation.request_kind='CHECK_HOURS'
            and generation.state='ACTIVE'
            and generation.deadline_at_utc>=p_now_utc
            and membership.state='ACTIONABLE'
            and comparison.candidate_timesheet_id=v_week.timesheet_id
            and comparison.contract_id=v_contract.id
        ) into v_weekly_source_candidate_request_allowed;
        if not v_weekly_source_candidate_request_allowed then
          v_weekly_source_candidate_request_allowed:=
            v_canonical_week_ending_date-6 <= (p_now_utc at time zone 'Europe/London')::date
            and v_week.additional_seq=0 and not v_week.is_adjustment
            and v_week.status not in (
              'AUTHORISED'::public.contract_week_status_enum,
              'INVOICED'::public.contract_week_status_enum,
              'CANCELLED'::public.contract_week_status_enum
            )
            and (private._weekly_source_effective_policy_v1(
              v_contract.client_id,v_contract.id,v_canonical_week_ending_date
            )->>'authority_mode')='SOURCE_AUTHORITY'
            and (private._weekly_source_effective_policy_v1(
              v_contract.client_id,v_contract.id,v_canonical_week_ending_date
            )->>'document_mode')='CHECK_ONLY';
        end if;
      end if;
      -- A later separate expense starts on the neutral ELECTRONIC draft route.
      -- Its approval-method step may then select PHONE, EMAIL or PAPER even
      -- when the completed worked Timesheet used the QR/PAPER family.
      if v_route_authority->>'route_family'='MANUAL_NON_QR'
         or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE'
           and v_workflow_kind<>'CONTRACT_EXPENSE'
           and not v_weekly_source_candidate_request_allowed) then
        raise exception 'CANDIDATE_RECORD_VIEW_ONLY' using errcode='55000',detail=v_route_authority::text;
      end if;
      if (v_route_authority->>'route_family'='QR' and v_route<>'PAPER'
            and v_workflow_kind<>'CONTRACT_EXPENSE')
         or (v_route_authority->>'route_family'='ELECTRONIC' and v_route='PAPER'
           and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_route_authority->>'route_family'='IMPORT_AUTHORITATIVE' and v_route='PAPER'
           and (v_workflow_kind<>'CONTRACT_EXPENSE'
             or not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))) then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
      if v_week.timesheet_id is not null then
        select * into v_anchor_timesheet
        from public.timesheets
        where timesheet_id=v_week.timesheet_id
          and is_current=true
          and archived_at_utc is null;
        if not found then raise exception 'CANDIDATE_WORKFLOW_TARGET_NOT_CURRENT' using errcode='55000'; end if;
        if v_anchor_timesheet.sheet_scope is distinct from 'WEEKLY'::public.timesheet_scope_enum then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='22023';
        end if;
        if v_workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
          v_target_capabilities:=private._candidate_record_capabilities_v1(
            v_week.timesheet_id,v_week.id,'{}'::jsonb
          );
          if coalesce((v_target_capabilities->>'candidate_mutation_locked')::boolean,false)
             or coalesce((v_target_capabilities->>'protected')::boolean,false)
             or (
               not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false)
               and not v_weekly_source_candidate_request_allowed
             ) then
            raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
          end if;
        end if;
      end if;
      if nullif(v_payload->>'target_timesheet_id','') is not null then
        if v_workflow_kind='CONTRACT_EXPENSE' then
          raise exception 'CANDIDATE_EXPENSE_TARGET_SERVER_RESOLVED' using errcode='22023';
        end if;
        if v_week.timesheet_id is null
           or (v_payload->>'target_timesheet_id')::uuid<>v_week.timesheet_id then
          raise exception 'CANDIDATE_WORKFLOW_TARGET_MISMATCH' using errcode='22023';
        end if;
      end if;
    end if;

    -- Store the immutable request receipt together with the canonical identity
    -- that the server derived at creation time. Later workflow lifecycle changes
    -- may alter route, target, contract-week and generation fields, but must never
    -- alter this receipt or make an exact creation retry conflict.
    v_creation_identity:=jsonb_build_object(
      'version','CANDIDATE_WORKFLOW_CREATION_IDENTITY_V1',
      'request',v_creation_request_identity,
      'derived',jsonb_strip_nulls(jsonb_build_object(
        'workflow_kind',v_workflow_kind,
        'scope',v_scope,
        'initial_route',v_route,
        'contract_id',v_contract.id,
        'contract_week_id',case when v_workflow_kind='DAILY' then null else v_week.id end,
        'anchor_timesheet_id',case when v_workflow_kind='DAILY'
          then v_daily_timesheet.timesheet_id else v_anchor_week.timesheet_id end,
        'daily_booking_id',case when v_workflow_kind='DAILY'
          then nullif(btrim(coalesce(v_daily_timesheet.booking_id,'')),'') else null end,
        'work_date',v_canonical_work_date,
        'week_ending_date',v_canonical_week_ending_date,
        'replacement_of_workflow_id',v_replacement_of_workflow_id
      ))
    );

    select * into v_existing_workflow
    from public.candidate_submission_workflows
    where account_id=v_account_id
      and idempotency_key=btrim(p_idempotency_key)
    for update;
    if found then
      if v_existing_workflow.candidate_id is distinct from v_candidate_id
         or v_existing_workflow.replacement_of_workflow_id is distinct from v_replacement_of_workflow_id
         or v_existing_workflow.creation_request_sha256 is distinct from v_creation_request_sha256 then
        raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
          using errcode=case when v_is_rejected_resubmission then '55000' else '40001' end;
      end if;
      if v_is_rejected_resubmission then
        return jsonb_build_object(
          'ok',true,'idempotent_replay',true,
          'rejected_workflow_id',v_replacement_of_workflow_id,
          'replacement_workflow_id',v_existing_workflow.id,
          'replacement_created',false,
          'workflow_id',v_existing_workflow.id,
          'state',v_existing_workflow.state,
          'generation',v_existing_workflow.generation
        );
      end if;
      return jsonb_build_object('ok',true,'idempotent_replay',true,
        'workflow_id',v_existing_workflow.id,'state',v_existing_workflow.state,
        'generation',v_existing_workflow.generation);
    end if;

    if v_workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED') then
      perform pg_advisory_xact_lock(hashtext(
        v_candidate_id::text||'|'||v_contract.id::text||'|'||
        v_canonical_week_ending_date::text||
        '|CANDIDATE_EXPENSE_CLAIM'
      ));
      if exists(
        select 1
        from public.candidate_submission_workflows prior
        where prior.candidate_id=v_candidate_id
          and prior.contract_id=v_contract.id
          and prior.week_ending_date=v_canonical_week_ending_date
          and prior.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
          and prior.state not in (
            'CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED','FINALISED'
          )
      ) or exists(
        select 1
        from public.contract_weeks prior_week
        join public.timesheets prior_timesheet
          on prior_timesheet.timesheet_id=prior_week.timesheet_id
         and prior_timesheet.is_current=true
         and prior_timesheet.archived_at_utc is null
        join public.timesheets_financials prior_fin
          on prior_fin.timesheet_id=prior_timesheet.timesheet_id
         and prior_fin.is_current=true
        where prior_week.contract_id=v_contract.id
          and prior_week.week_ending_date=v_canonical_week_ending_date
          and prior_fin.authorised_at_utc is null
          and not exists(
            select 1
            from public.candidate_submission_workflows approved_claim
            where approved_claim.candidate_id=v_candidate_id
              and approved_claim.contract_id=v_contract.id
              and approved_claim.week_ending_date=v_canonical_week_ending_date
              and approved_claim.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
              and approved_claim.state='FINALISED'
              and approved_claim.target_timesheet_id=prior_timesheet.timesheet_id
          )
          and (
            abs(coalesce(prior_fin.expenses_pay_ex_vat,0))
            +abs(coalesce(prior_fin.expenses_charge_ex_vat,0))
            +abs(coalesce(prior_fin.mileage_units,0))
            +abs(coalesce(prior_fin.mileage_pay_ex_vat,0))
            +abs(coalesce(prior_fin.mileage_charge_ex_vat,0))
            +abs(coalesce(prior_fin.travel_pay_ex_vat,0))
            +abs(coalesce(prior_fin.travel_charge_ex_vat,0))
            +abs(coalesce(prior_fin.accommodation_pay_ex_vat,0))
            +abs(coalesce(prior_fin.accommodation_charge_ex_vat,0))
            +abs(coalesce(prior_fin.other_pay_ex_vat,0))
            +abs(coalesce(prior_fin.other_charge_ex_vat,0))
          )>0
      ) then
        raise exception 'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE' using errcode='55000';
      end if;
    end if;
    insert into public.candidate_submission_workflows(
      id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
      contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,work_date,week_ending_date,
      policy_snapshot_json,input_snapshot_json,issue_codes,expected_row_signature,idempotency_key,
      replacement_of_workflow_id,creation_request_sha256,creation_identity_json,
      last_mutation_idempotency_key,created_at_utc,updated_at_utc
    ) values (
      v_insert_workflow_id,v_environment,v_account_id,v_candidate_id,v_workflow_kind,
      v_scope,v_route,'WORKER_DRAFT',1,v_contract.id,
      case when v_workflow_kind='DAILY' then null else v_week.id end,
      case when v_workflow_kind='DAILY' then v_daily_timesheet.timesheet_id else v_anchor_week.timesheet_id end,
      case
        when v_workflow_kind='DAILY' then v_daily_timesheet.timesheet_id
        when v_workflow_kind='CONTRACT_EXPENSE' then null
        else v_week.timesheet_id
      end,
      v_canonical_work_date,v_canonical_week_ending_date,
      v_policy,coalesce(v_payload->'input_snapshot','{}'::jsonb),'[]'::jsonb,
      nullif(v_payload->>'expected_row_signature',''),btrim(p_idempotency_key),
      v_replacement_of_workflow_id,v_creation_request_sha256,v_creation_identity,
      btrim(p_idempotency_key),p_now_utc,p_now_utc
    ) returning * into v_workflow;
    perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_CREATED',null,
      jsonb_build_object('kind',v_workflow.workflow_kind,'scope',v_workflow.scope,'route',v_workflow.route),
      null,null,p_idempotency_key,p_now_utc);
    if v_is_rejected_resubmission then
      return jsonb_build_object(
        'ok',true,'idempotent_replay',false,
        'rejected_workflow_id',v_replacement_of_workflow_id,
        'replacement_workflow_id',v_workflow.id,
        'replacement_created',true,
        'workflow_id',v_workflow.id,'state',v_workflow.state,
        'generation',v_workflow.generation,'policy',v_policy
      );
    end if;
    return jsonb_build_object('ok',true,'idempotent_replay',false,
      'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'policy',v_policy);
  end if;

  if v_workflow.id is null then
    if v_action in ('PAPER_PREPARE','PAPER_RETURN','AMEND','CANCEL','SUPERSEDE') then
      -- Keep the canonical route/document lock order used by
      -- timesheet_qr_send_enqueue_v1: current timesheet, then workflow.
      -- The unlocked identity read is rechecked after both locks are held.
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id
        and environment=v_environment
        and candidate_id=v_candidate_id;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
      end if;
      v_unlocked_workflow_updated_at:=v_workflow.updated_at_utc;
      v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
      v_paper_family_key:='CANDIDATE_PAPER_FAMILY:'||v_workflow.environment||':'
        ||coalesce(v_workflow.contract_id::text,'-')||':'
        ||coalesce(v_workflow.week_ending_date::text,v_workflow.work_date::text,'-');
      perform pg_advisory_xact_lock(hashtextextended(v_paper_family_key,0));
      if v_action='PAPER_PREPARE' and v_paper_timesheet_id is null then
        raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
      end if;
      if v_paper_timesheet_id is not null
         and (v_action in ('PAPER_PREPARE','PAPER_RETURN')
           or (v_workflow.route='PAPER'
             and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED'))) then
        perform 1
        from public.timesheets
        where timesheet_id=v_paper_timesheet_id
          and is_current=true
          and archived_at_utc is null
        for update;
        if not found then
          raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
        end if;
      end if;
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id
        and environment=v_environment
        and candidate_id=v_candidate_id
      for update;
      if not found
         or coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
              is distinct from v_paper_timesheet_id then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
      end if;
      if v_action in ('PAPER_RETURN','AMEND','CANCEL','SUPERSEDE')
         and v_workflow.updated_at_utc is distinct from v_unlocked_workflow_updated_at then
        raise exception 'CANDIDATE_WORKFLOW_CONTEXT_CONFLICT' using errcode='40001';
      end if;
    else
      select * into v_workflow
      from public.candidate_submission_workflows
      where id=p_workflow_id
      for update;
    end if;
  end if;
  if not found or v_workflow.environment<>v_environment
     or (not v_is_service_action and not v_is_public_manager_action and v_workflow.candidate_id<>v_candidate_id) then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_action='WORKER_SUBMIT' and nullif(v_payload->>'update_id','') is not null then
    begin
      v_expense_update_context:=nullif(current_setting(
        'cloudtms.candidate_expense_update_submit_context',true
      ),'')::jsonb;
    exception when others then
      v_expense_update_context:=null;
    end;
    if coalesce(v_expense_update_context->>'contract_version','')
         <>'CANDIDATE_EXPENSE_UPDATE_SUBMIT_CONTEXT_V1'
       or v_expense_update_context->>'workflow_id'<>v_workflow.id::text
       or v_expense_update_context->>'workflow_generation'<>v_workflow.generation::text
       or v_expense_update_context->>'update_id'<>v_payload->>'update_id'
       or v_expense_update_context->>'idempotency_key'<>btrim(coalesce(p_idempotency_key,'')) then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_RECEIPT_INVALID' using errcode='28000';
    end if;
    select update_row.* into v_pending_expense_update
    from public.candidate_pending_expense_updates update_row
    where update_row.update_id=(v_payload->>'update_id')::uuid
      and update_row.workflow_id=v_workflow.id
      and update_row.current_workflow_generation=v_workflow.generation
      and update_row.state='EDITING'
      and update_row.actor_kind=v_expense_update_context->>'actor_kind'
      and update_row.actor_id is not distinct from nullif(
        v_expense_update_context->>'actor_id',''
      )::uuid
    for update;
    if not found then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_APPROVAL_CHANGED' using errcode='40001';
    end if;
    v_is_pending_expense_update:=true;
  end if;
  if nullif(btrim(coalesce(p_idempotency_key,'')),'') is not null then
    if nullif(btrim(coalesce(v_workflow.idempotency_key,'')),'')=btrim(p_idempotency_key) then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_IDEMPOTENCY_CONFLICT',
          'workflow_id',v_workflow.id,
          'idempotency_key',btrim(p_idempotency_key),
          'reason','CREATION_KEY_REUSED_FOR_MUTATION'
        )::text;
    end if;
    v_mutation_channel:=case
      when v_is_office_service_action then 'OFFICE'
      when v_is_public_manager_action then 'MANAGER_PUBLIC'
      when v_is_service_action then 'SERVICE'
      else 'CANDIDATE_CLIENT' end;
    v_mutation_actor_identity:=case
      when v_is_office_service_action then v_office_actor_user_id::text
      when v_is_public_manager_action then 'MANAGER_REQUEST:'||coalesce(v_request_id::text,'UNKNOWN')
      when v_is_service_action then 'SERVICE:'||v_action
      else 'ACCOUNT:'||coalesce(v_account_id::text,'UNKNOWN')
        ||':CANDIDATE:'||coalesce(v_candidate_id::text,'UNKNOWN') end;
    v_mutation_semantic_payload:=case
      when v_mutation_replay_probe_only then
        v_payload->'mutation_replay_semantic_payload'
      when v_action='COMPONENT_PREPARE' then
        v_payload-'service_office_action'-'actor_user_id'-'storage_key'
      when v_action='SELECT_PHONE_APPROVAL' then
        v_payload-'service_office_action'-'actor_user_id'-'expires_at_utc'
          -'approval_token_hash_hex'-'handoff_token_key_version'-'broker_handoff_key_version'
      when v_action='WORKER_SUBMIT' then
        case when jsonb_typeof(v_payload->'submission_request_identity')='object'
          then jsonb_build_object(
            'submission_request_identity',v_payload->'submission_request_identity'
          )
          else (v_payload-'service_office_action'-'actor_user_id'-'renderer_contract_version'-'immutable_submission')
            ||jsonb_build_object(
              'immutable_submission',coalesce(v_payload->'immutable_submission','{}'::jsonb)-'official_presentation'
            )
        end
      when v_action='CREATE_EMAIL_APPROVAL_REQUEST' then
        v_payload-'service_office_action'-'actor_user_id'-'mail'-'approval_token_hash_hex'
      when v_action in ('REMIND','RENEW') then
        v_payload-'service_office_action'-'actor_user_id'-'mail'-'approval_token_hash_hex'-'manager_email'
      else v_payload-'service_office_action'-'actor_user_id'
    end;
    v_mutation_request_sha256:=encode(extensions.digest(convert_to(jsonb_build_object(
      'contract_version','CANDIDATE_WORKFLOW_MUTATION_REQUEST_V1',
      'workflow_id',v_workflow.id,
      'action',v_action,
      'expected_generation',p_expected_generation,
      'payload',v_mutation_semantic_payload,
      'channel',v_mutation_channel,
      'actor_identity',v_mutation_actor_identity
    )::text,'UTF8'),'sha256'),'hex');
    v_mutation_receipt:=private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,btrim(p_idempotency_key),v_mutation_request_sha256,
      v_action,v_mutation_channel,v_mutation_actor_identity,null,p_now_utc
    );
    if coalesce((v_mutation_receipt->>'found')::boolean,false) then
      return v_mutation_receipt->'response';
    end if;
    if v_mutation_replay_probe_only then
      return jsonb_build_object(
        'ok',true,'replay_found',false,'workflow_id',v_workflow.id,
        'expected_generation',p_expected_generation
      );
    end if;
  end if;
  if p_expected_generation is not null and v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','WORKFLOW_GENERATION_CONFLICT','current_generation',v_workflow.generation)::text;
  end if;
  v_next_generation:=v_workflow.generation+1;
  if v_workflow.workflow_kind='DAILY' then
    v_daily_receipt_context:=private._candidate_daily_receipt_context_v1(
      v_environment,v_workflow.candidate_id,v_workflow.target_timesheet_id,true,p_now_utc);
    v_policy:=v_daily_receipt_context->'policy';
  elsif v_workflow.contract_id is not null then
    select * into v_contract from public.contracts where id=v_workflow.contract_id;
    v_policy:=private._expense_approval_policy_v1(v_contract.client_id,v_contract.id,
      coalesce(v_workflow.week_ending_date,v_workflow.work_date,(p_now_utc at time zone 'Europe/London')::date),v_workflow.workflow_kind);
  else
    v_policy:=v_workflow.policy_snapshot_json;
  end if;

  v_worker_only_approval:=v_workflow.scope='WEEKLY' and v_workflow.workflow_kind='CONTRACT_HOURS'
    and v_workflow.route='ELECTRONIC'
    and coalesce(v_policy#>'{manager_approval_policy,approval}'='"NOT_REQUIRED"'::jsonb,false)
    and (v_action='WORKER_SUBMIT' or (
      coalesce(v_workflow.policy_snapshot_json#>'{manager_approval_policy,approval}'='"NOT_REQUIRED"'::jsonb,false)
      and coalesce(v_policy->>'policy_fingerprint'=v_workflow.policy_snapshot_json->>'policy_fingerprint',false)));
  if v_workflow.scope='WEEKLY' and v_workflow.workflow_kind='CONTRACT_HOURS'
     and v_workflow.policy_snapshot_json#>'{manager_approval_policy,approval}'='"NOT_REQUIRED"'::jsonb
     and v_action in ('REGISTER_REVIEW_COMPONENT','SELECT_PHONE_APPROVAL','CREATE_EMAIL_APPROVAL_REQUEST')
     and v_worker_only_approval is distinct from true then
    raise exception 'CANDIDATE_POLICY_CHANGED' using errcode='40001';
  end if;

  if v_action='AMEND' then
    if v_workflow.state not in (
      'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',
      'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL',
      'AWAITING_PAPER_RETURN','REFUSED'
    ) then
      raise exception 'CANDIDATE_WORKFLOW_AMENDMENT_NOT_ALLOWED' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN' then
      v_paper_retirement_result:=private._candidate_paper_delivery_retire_v1(
        v_workflow.id,v_workflow.generation,'WORKFLOW_AMENDED',p_now_utc
      );
    end if;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state in ('PENDING','APPROVED');
    v_component_no:=0;
    for v_source_component in
      select canonical_source.*
      from (
        select distinct on (
          source_component.component_kind,source_component.expense_category,
          source_component.document_role,
          coalesce(source_component.source_component_id,source_component.id),
          source_component.source_content_sha256
        ) source_component.*
        from public.candidate_submission_components source_component
        where source_component.workflow_id=v_workflow.id
          and source_component.workflow_generation=v_workflow.generation
          and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
          and source_component.state='IMMUTABLE'
          and source_component.source_content_sha256 is not null
        order by source_component.component_kind,source_component.expense_category,
          source_component.document_role,
          coalesce(source_component.source_component_id,source_component.id),
          source_component.source_content_sha256,
          source_component.component_no,source_component.id
      ) canonical_source
      order by canonical_source.component_no,canonical_source.id
    loop
      v_component_no:=v_component_no+1;
      insert into public.candidate_submission_components(
        workflow_id,workflow_generation,component_no,timesheet_id,component_kind,expense_category,
        document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
        immutable_at_utc,required,review_render_state,final_signed_render_state,created_at_utc
      ) values (
        v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
        v_source_component.component_kind,v_source_component.expense_category,v_source_component.document_role,
        'IMMUTABLE',coalesce(v_source_component.source_component_id,v_source_component.id),
        v_source_component.storage_key,v_source_component.media_type,v_source_component.byte_size,
        v_source_component.source_content_sha256,p_now_utc,false,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
      );
    end loop;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and state<>'SUPERSEDED';
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'state','WORKER_DRAFT',
      'generation',v_next_generation,'preserved_source_component_count',v_component_no
    );
    update public.candidate_submission_workflows set
      state='WORKER_DRAFT',generation=v_next_generation,
      input_snapshot_json=coalesce(v_payload->'input_snapshot',input_snapshot_json),
      candidate_signature_component_id=null,candidate_signature_sha256=null,candidate_signed_at_utc=null,
      review_manifest_json=null,review_manifest_sha256=null,paper_return_manifest_json=null,
      paper_return_manifest_sha256=null,renderer_contract_version=null,
      manager_name=null,manager_position=null,manager_signature_component_id=null,
      manager_signature_sha256=null,manager_approved_at_utc=null,
      issue_codes='[]'::jsonb,worker_submitted_at_utc=null,
      daily_context_sha256=null,canonical_financial_sha256=null,
      canonical_save_input_sha256=null,canonical_save_row_signature=null,
      canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;
    perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
      'CANDIDATE_WORKFLOW_AMENDED',null,
      jsonb_build_object('generation',v_workflow.generation,'preserved_source_component_count',v_component_no),
      null,v_candidate_id,p_idempotency_key,p_now_utc);
    if v_mutation_request_sha256 is not null then
      perform private._candidate_workflow_mutation_receipt_v1(
        v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
        v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
      );
    end if;
    return v_response;
  elsif v_action='COMPONENT_PREPARE' then
    if v_workflow.state in ('FINALISED','CANCELLED','REJECTED','SUPERSEDED') then
      raise exception 'CANDIDATE_WORKFLOW_NOT_MUTABLE' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    v_component_kind:=upper(btrim(coalesce(v_payload->>'component_kind','')));
    v_document_role:=upper(btrim(coalesce(v_payload->>'document_role','')));
    v_expense_category:=nullif(upper(btrim(coalesce(v_payload->>'expense_category',''))),'');
    v_paper_page_key:=nullif(btrim(coalesce(v_payload->>'paper_return_page_key','')),'');
    v_requested_media_type:=nullif(lower(btrim(coalesce(v_payload->>'media_type',''))),'');
    begin
      v_requested_byte_size:=nullif(v_payload->>'byte_size','')::bigint;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'CANDIDATE_COMPONENT_SIZE_INVALID' using errcode='22023';
    end;
    v_manager_capture_method:=nullif(upper(btrim(coalesce(
      v_payload->>'manager_signature_capture_method',''
    ))),'');
    if nullif(v_payload->>'expected_source_content_sha256_hex','') is not null then
      if (v_payload->>'expected_source_content_sha256_hex') !~ '^[0-9a-fA-F]{64}$' then
        raise exception 'CANDIDATE_COMPONENT_DIGEST_INVALID' using errcode='22023';
      end if;
      v_expected_source_digest:=decode(v_payload->>'expected_source_content_sha256_hex','hex');
    end if;
    select * into v_component from public.candidate_submission_components
    where workflow_id=v_workflow.id and upload_idempotency_key=p_idempotency_key;
    if found then
      if v_component.workflow_generation is distinct from v_workflow.generation then
        raise exception 'CANDIDATE_COMPONENT_PREPARE_GENERATION_CONFLICT' using errcode='40001';
      end if;
      if v_component.state not in ('PENDING','IMMUTABLE') then
        raise exception 'CANDIDATE_COMPONENT_PREPARE_STATE_CONFLICT' using errcode='55000';
      end if;
      if v_component.component_kind is distinct from v_component_kind
         or v_component.document_role is distinct from v_document_role
         or v_component.expense_category is distinct from v_expense_category
         or lower(v_component.media_type) is distinct from v_requested_media_type
         or v_component.byte_size is distinct from v_requested_byte_size
         or v_component.manager_signature_capture_method is distinct from v_manager_capture_method
         or v_component.expected_source_content_sha256 is distinct from v_expected_source_digest
         or v_component.paper_return_page_key is distinct from v_paper_page_key then
        raise exception 'CANDIDATE_COMPONENT_PREPARE_IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
        'component_id',v_component.id,'component_no',v_component.component_no,
        'workflow_generation',v_component.workflow_generation,
        'storage_key',v_component.storage_key,'media_type',v_component.media_type,
        'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
        'document_role',v_component.document_role,'expense_category',v_component.expense_category,
        'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state)
        ||case when v_component.component_kind='MANAGER_SIGNATURE' then jsonb_build_object(
          'approval_request_id',v_component.approval_request_id,
          'approval_request_generation',v_approval.request_generation
        ) else '{}'::jsonb end;
      if v_mutation_request_sha256 is not null then
        perform private._candidate_workflow_mutation_receipt_v1(
          v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
          v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
        );
      end if;
      return v_response;
    end if;
    select coalesce(max(component_no),0)+1 into v_component_no
    from public.candidate_submission_components
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation;
    if v_is_public_manager_action then
      if v_component_kind<>'MANAGER_SIGNATURE' or v_document_role<>'MANAGER_SIGNATURE' then
        raise exception 'MANAGER_SIGNATURE_COMPONENT_REQUIRED' using errcode='28000';
      end if;
      if v_approval.id is null
         or v_approval.state<>'PENDING'
         or v_approval.expires_at_utc<=p_now_utc
         or v_approval.workflow_generation<>v_workflow.generation
         or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
        raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
      end if;
    elsif v_component_kind='MANAGER_SIGNATURE' then
      select * into v_approval
      from public.candidate_approval_requests
      where id=nullif(v_payload->>'approval_request_id','')::uuid
        and workflow_id=v_workflow.id
        and workflow_generation=v_workflow.generation
        and method='PHONE'
        and state='PENDING'
        and review_manifest_sha256=v_workflow.review_manifest_sha256
      for update;
      if not found then raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000'; end if;
    elsif not coalesce((
      (v_component_kind='CANDIDATE_SIGNATURE' and v_document_role='CANDIDATE_SIGNATURE' and v_expense_category is null)
      or (v_component_kind='MILEAGE_FORM' and v_document_role='MILEAGE_CLAIM_FORM' and v_expense_category='MILEAGE')
      or (v_component_kind='EXPENSE_EVIDENCE' and v_document_role='SOURCE_EVIDENCE'
          and v_expense_category in ('TRAVEL','ACCOMMODATION','OTHER','MILEAGE'))
      or (v_component_kind='SIGNED_RETURN' and v_document_role='SIGNED_RETURN' and v_expense_category is null)
      or (v_component_kind='MANAGER_SIGNATURE' and v_document_role='MANAGER_SIGNATURE'
          and v_expense_category is null
          and v_workflow.state='AWAITING_MANAGER_APPROVAL'
          and exists(
            select 1 from public.candidate_approval_requests approval
            where approval.workflow_id=v_workflow.id
              and approval.workflow_generation=v_workflow.generation
              and approval.method='PHONE' and approval.state='PENDING'
          ))
    ),false) then
      raise exception 'CANDIDATE_COMPONENT_TYPE_INVALID' using errcode='22023';
    end if;
    if v_component_kind='MANAGER_SIGNATURE' and v_approval.method='PHONE'
       and v_manager_capture_method is null then
      v_manager_capture_method:='DRAW';
    end if;
    if v_component_kind='MANAGER_SIGNATURE'
       and (coalesce(v_manager_capture_method,'') not in ('DRAW','UPLOAD')
         or (v_is_public_manager_action and v_approval.method='EMAIL'
           and v_expected_source_digest is null)) then
      raise exception 'MANAGER_SIGNATURE_CAPTURE_METHOD_INVALID' using errcode='22023';
    elsif v_component_kind<>'MANAGER_SIGNATURE'
       and (v_manager_capture_method is not null or v_expected_source_digest is not null) then
      raise exception 'CANDIDATE_COMPONENT_DIGEST_INVALID' using errcode='22023';
    end if;
    if v_component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE')
       and v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_COMPONENT_AMENDMENT_REQUIRED' using errcode='55000';
    end if;
    if v_component_kind='SIGNED_RETURN' then
      if v_workflow.route<>'PAPER' or v_workflow.state<>'AWAITING_PAPER_RETURN'
         or v_workflow.paper_return_manifest_sha256 is null
         or not exists(
           select 1
           from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') page
           where page->>'page_key'=v_paper_page_key
         ) then
        raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='22023';
      end if;
    elsif v_paper_page_key is not null then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_KEY_FORBIDDEN' using errcode='22023';
    end if;
    -- A manager may safely reopen the original approval link after a browser,
    -- network or dependency interruption.  The approval itself is still
    -- PENDING, so any earlier signature reservation is not an approval fact.
    -- Replace that unfinished/unused reservation atomically before inserting
    -- the newly drawn signature.  Exact retries already returned above by
    -- idempotency key, and an approved request can never enter this branch.
    if v_component_kind='MANAGER_SIGNATURE' and v_approval.id is not null then
      with replaced as (
        update public.candidate_submission_components
        set state='SUPERSEDED',superseded_at_utc=p_now_utc,
            review_render_state=case
              when review_render_state='NOT_REQUIRED' then review_render_state
              else 'SUPERSEDED'
            end,
            final_signed_render_state=case
              when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state
              else 'SUPERSEDED'
            end
        where approval_request_id=v_approval.id
          and component_kind='MANAGER_SIGNATURE'
          and state in ('PENDING','IMMUTABLE')
          and upload_idempotency_key is distinct from p_idempotency_key
        returning id
      )
      select coalesce(array_agg(id),array[]::uuid[])
      into v_replaced_manager_signature_ids
      from replaced;
      if cardinality(v_replaced_manager_signature_ids)>0 then
        perform private._candidate_audit_v1(
          'candidate_approval_request',v_approval.id::text,
          'MANAGER_SIGNATURE_REPLACED_BEFORE_DECISION',null,
          jsonb_build_object(
            'workflow_id',v_workflow.id,
            'workflow_generation',v_workflow.generation,
            'replaced_component_ids',to_jsonb(v_replaced_manager_signature_ids)
          ),null,null,p_idempotency_key,p_now_utc
        );
      end if;
    end if;
    if nullif(v_payload->>'source_component_id','') is not null then
      select source_component.* into v_source_component
      from public.candidate_submission_components source_component
      join public.candidate_submission_workflows source_workflow on source_workflow.id=source_component.workflow_id
      where source_component.id=(v_payload->>'source_component_id')::uuid
        and source_component.state in ('IMMUTABLE','SUPERSEDED','REJECTED')
        and source_component.immutable_at_utc is not null
        and source_component.source_content_sha256 is not null
        and source_component.source_component_id is null
        and source_workflow.environment=v_environment
        and source_workflow.account_id=v_account_id
        and source_workflow.candidate_id=v_candidate_id
        and (
          source_workflow.id=v_workflow.id
          or (
            source_workflow.contract_id is not distinct from v_workflow.contract_id
            and source_workflow.week_ending_date is not distinct from v_workflow.week_ending_date
            and source_workflow.state in ('CANCELLED','REJECTED','REFUSED','SUPERSEDED')
          )
        );
      if not found or v_source_component.component_kind<>v_component_kind
         or v_source_component.document_role<>v_document_role
         or v_source_component.expense_category is distinct from v_expense_category then
        raise exception 'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED' using errcode='28000';
      end if;
    end if;
    insert into public.candidate_submission_components(
      workflow_id,workflow_generation,component_no,approval_request_id,timesheet_id,component_kind,expense_category,
      document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
      upload_idempotency_key,immutable_at_utc,
      required,review_ordinal,review_render_state,final_signed_render_state,paper_return_page_key,
      manager_signature_capture_method,expected_source_content_sha256,created_at_utc
    ) values (
      v_workflow.id,v_workflow.generation,v_component_no,
      case when v_component_kind='MANAGER_SIGNATURE' then v_approval.id else null end,
      v_workflow.target_timesheet_id,
      v_component_kind,v_expense_category,v_document_role,
      case when v_source_component.id is null then 'PENDING' else 'IMMUTABLE' end,
      v_source_component.id,
      coalesce(v_source_component.storage_key,nullif(v_payload->>'storage_key','')),
      coalesce(v_source_component.media_type,v_requested_media_type),
      coalesce(v_source_component.byte_size,v_requested_byte_size),
      v_source_component.source_content_sha256,p_idempotency_key,
      case when v_source_component.id is null then null else p_now_utc end,
      false,null,'NOT_REQUIRED','NOT_REQUIRED',v_paper_page_key,
      v_manager_capture_method,v_expected_source_digest,
      p_now_utc
    ) returning * into v_component;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,'component_id',v_component.id,
      'component_no',v_component.component_no,'workflow_generation',v_component.workflow_generation,
      'storage_key',v_component.storage_key,'media_type',v_component.media_type,
      'byte_size',v_component.byte_size,'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,'expense_category',v_component.expense_category,
      'paper_return_page_key',v_component.paper_return_page_key,'state',v_component.state)
      ||case when v_component.component_kind='MANAGER_SIGNATURE' then jsonb_build_object(
        'approval_request_id',v_component.approval_request_id,
        'approval_request_generation',v_approval.request_generation
      ) else '{}'::jsonb end;
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
      v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
    );
    return v_response;
  elsif v_action='COMPONENT_COMPLETE' then
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation for update;
    if not found then raise exception 'CANDIDATE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
    if v_is_public_manager_action and v_component.component_kind<>'MANAGER_SIGNATURE' then
      raise exception 'MANAGER_SIGNATURE_COMPONENT_REQUIRED' using errcode='28000';
    end if;
    if v_component.component_kind='MANAGER_SIGNATURE' then
      if v_approval.id is null then
        select * into v_approval
        from public.candidate_approval_requests
        where id=v_component.approval_request_id
          and workflow_id=v_workflow.id
          and workflow_generation=v_workflow.generation
          and method='PHONE'
          and state='PENDING'
          and review_manifest_sha256=v_workflow.review_manifest_sha256
        for update;
      end if;
      if not found or v_component.approval_request_id is distinct from v_approval.id
         or v_approval.state<>'PENDING'
         or v_approval.workflow_generation<>v_workflow.generation
         or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256
         or v_approval.expires_at_utc<=p_now_utc then
        raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
      end if;
    elsif v_component.component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE')
          and v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_COMPONENT_AMENDMENT_REQUIRED' using errcode='55000';
    elsif v_component.component_kind='SIGNED_RETURN' and v_workflow.state<>'AWAITING_PAPER_RETURN' then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='55000';
    end if;
    if coalesce(v_payload->>'source_content_sha256_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_COMPONENT_DIGEST_INVALID' using errcode='22023';
    end if;
    v_digest:=decode(v_payload->>'source_content_sha256_hex','hex');
    if v_component.expected_source_content_sha256 is not null
       and v_component.expected_source_content_sha256<>v_digest then
      raise exception 'CANDIDATE_COMPONENT_DIGEST_MISMATCH' using errcode='22023';
    end if;
    if v_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
       and v_component.expected_source_content_sha256 is not null
       and (
         nullif(btrim(coalesce(v_component.storage_key,'')),'') is null
         or coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,
              v_component.byte_size) is distinct from v_component.byte_size
         or lower(coalesce(nullif(v_payload->>'verified_media_type',''),
              v_component.media_type)) is distinct from lower(v_component.media_type)
       ) then
      raise exception 'CANDIDATE_COMPONENT_MEDIA_INVALID' using errcode='22023';
    end if;
    v_manager_capture_method:=nullif(upper(btrim(coalesce(
      v_payload->>'manager_signature_capture_method',''
    ))),'');
    if v_component.component_kind='MANAGER_SIGNATURE' and v_approval.method='PHONE'
       and v_manager_capture_method is null then
      v_manager_capture_method:='DRAW';
    end if;
    begin
      v_verified_image_width:=nullif(v_payload->>'verified_image_width','')::integer;
      v_verified_image_height:=nullif(v_payload->>'verified_image_height','')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'CANDIDATE_COMPONENT_MEDIA_INVALID' using errcode='22023';
    end;
    if v_component.component_kind='MANAGER_SIGNATURE'
       and (v_manager_capture_method is distinct from v_component.manager_signature_capture_method
         or (v_approval.method='EMAIL' and (
           v_verified_image_width not between 1 and 10000
           or v_verified_image_height not between 1 and 10000
         ))) then
      raise exception 'MANAGER_SIGNATURE_CAPTURE_METHOD_INVALID' using errcode='22023';
    end if;
    if (v_component.component_kind in ('CANDIDATE_SIGNATURE','MANAGER_SIGNATURE')
          and lower(coalesce(v_payload->>'verified_media_type',v_component.media_type,''))
            not in ('image/jpeg','image/png','image/webp'))
       or (v_component.component_kind in ('MILEAGE_FORM','SIGNED_RETURN','EXPENSE_EVIDENCE')
          and lower(coalesce(v_payload->>'verified_media_type',v_component.media_type,''))
            not in ('application/pdf','image/jpeg','image/png','image/webp'))
       or coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,v_component.byte_size,0)
          not between 1 and 15728640 then
      raise exception 'CANDIDATE_COMPONENT_MEDIA_INVALID' using errcode='22023';
    end if;
    if v_component.state='IMMUTABLE' then
      if v_component.source_content_sha256=v_digest
         and v_component.byte_size=coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,v_component.byte_size)
         and lower(v_component.media_type)=lower(coalesce(nullif(v_payload->>'verified_media_type',''),v_component.media_type)) then
        v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
          'component_id',v_component.id,'state',v_component.state);
        if v_mutation_request_sha256 is not null then
          perform private._candidate_workflow_mutation_receipt_v1(
            v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
            v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
          );
        end if;
        return v_response;
      end if;
      raise exception 'CANDIDATE_COMPONENT_IMMUTABLE_CONFLICT' using errcode='40001';
    end if;
    if v_component.state<>'PENDING' then
      raise exception 'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      state='IMMUTABLE',source_content_sha256=v_digest,
      byte_size=coalesce(nullif(v_payload->>'verified_byte_size','')::bigint,byte_size),
      media_type=coalesce(nullif(lower(v_payload->>'verified_media_type'),''),media_type),
      validated_image_width=case when component_kind='MANAGER_SIGNATURE' then v_verified_image_width else validated_image_width end,
      validated_image_height=case when component_kind='MANAGER_SIGNATURE' then v_verified_image_height else validated_image_height end,
      immutable_at_utc=p_now_utc
    where id=v_component.id and state='PENDING' returning * into v_component;
    if not found then
      raise exception 'CANDIDATE_COMPONENT_COMPLETE_STATE_CONFLICT' using errcode='40001';
    end if;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,'component_id',v_component.id,
      'state',v_component.state,'immutable_at_utc',v_component.immutable_at_utc);
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
      v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
    );
    return v_response;
  elsif v_action='COMPONENT_SUPERSEDE' then
    if v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_COMPONENT_AMENDMENT_REQUIRED' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where id=nullif(v_payload->>'component_id','')::uuid and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation and state<>'SUPERSEDED'
      and component_kind in ('CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE')
    returning * into v_component;
    if not found then raise exception 'CANDIDATE_COMPONENT_NOT_FOUND' using errcode='P0002'; end if;
    v_response:=jsonb_build_object('ok',true,'component_id',v_component.id,'state',v_component.state);
    if v_mutation_request_sha256 is not null then
      perform private._candidate_workflow_mutation_receipt_v1(
        v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
        v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
      );
    end if;
    return v_response;
  end if;

  if v_action='WORKER_SUBMIT' then
    if v_workflow.state<>'WORKER_DRAFT' then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
    if v_workflow.workflow_kind='DAILY'
       and upper(coalesce(v_payload->>'approval_route',v_workflow.route))<>'PHONE' then
      raise exception 'CANDIDATE_DAILY_APPROVAL_ROUTE_INVALID' using errcode='22023';
    end if;
    if v_worker_only_approval and upper(coalesce(v_payload->>'approval_route',v_workflow.route))<>'ELECTRONIC' then
      raise exception 'CANDIDATE_FIXED_APPROVAL_ROUTE_INVALID' using errcode='22023';
    end if;
    v_is_electronic:=upper(coalesce(v_payload->>'approval_route',v_workflow.route))<>'PAPER';
    v_immutable_submission:=coalesce(v_payload->'immutable_submission',v_payload->'input_snapshot');
    if jsonb_typeof(v_immutable_submission)<>'object' then
      raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='22023';
    end if;
    if v_workflow.workflow_kind='DAILY' then
      if not private._candidate_daily_entitled_v1(v_candidate_id) then
        raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
      end if;
      select * into v_daily_timesheet
      from public.timesheets
      where timesheet_id=v_workflow.target_timesheet_id
        and is_current=true and archived_at_utc is null
        and sheet_scope='DAILY'::public.timesheet_scope_enum
      for update;
      if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
      select * into v_daily_fin
      from public.timesheets_financials
      where timesheet_id=v_daily_timesheet.timesheet_id and is_current=true and candidate_id=v_candidate_id
      order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1 for update;
      -- _candidate_daily_receipt_context_v1 already locked and verified this
      -- exact Candidate receipt and any existing protected financial state.
      v_canonical_work_date:=private._candidate_daily_work_date_v1(
        coalesce(
          nullif(v_immutable_submission#>>'{timesheet_patch_json,worked_start_iso}','')::timestamptz,
          nullif(v_immutable_submission->>'worked_start_iso','')::timestamptz,
          v_daily_fin.worked_start_iso,
          v_daily_timesheet.worked_start_iso
        ),
        v_daily_timesheet.scheduled_start_iso,
        v_daily_timesheet.week_ending_date
      );
      if v_canonical_work_date is distinct from v_workflow.work_date then
        raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='22023';
      end if;
    elsif v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
      select * into v_week
      from public.contract_weeks
      where id=v_workflow.contract_week_id
        and contract_id=v_workflow.contract_id
        and week_ending_date=v_workflow.week_ending_date
      for update;
      if not found then
        raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
      end if;
      if v_week.timesheet_id is null then
        -- A first electronic submission is reviewed before its Timesheet is
        -- materialised.  Its frozen canonical create/snapshot payload is
        -- applied only by the established manager-approved finalisation.
        if v_workflow.target_timesheet_id is not null then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
        end if;
      else
        select * into v_anchor_timesheet
        from public.timesheets
        where timesheet_id=v_workflow.target_timesheet_id
          and timesheet_id=v_week.timesheet_id
          and is_current=true
          and archived_at_utc is null
          and sheet_scope='WEEKLY'::public.timesheet_scope_enum
        for update;
        if not found then
          raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001';
        end if;
      end if;
      v_target_capabilities:=private._candidate_record_capabilities_v1(v_workflow.target_timesheet_id,v_week.id,'{}'::jsonb);
      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_week.id);
      if not coalesce((v_target_capabilities->>'can_edit_hours')::boolean,false)
         or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family'<>'ELECTRONIC') then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
    elsif v_workflow.workflow_kind='CONTRACT_EXPENSE' then
      select week_row.* into v_anchor_week
      from public.contract_weeks week_row
      join public.timesheets anchor_timesheet
        on anchor_timesheet.timesheet_id=week_row.timesheet_id
       and anchor_timesheet.is_current=true
       and anchor_timesheet.archived_at_utc is null
       and anchor_timesheet.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      where week_row.timesheet_id=v_workflow.anchor_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date
      for update of week_row,anchor_timesheet;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
      v_route_authority:=private._expense_approval_route_v1(v_workflow.anchor_timesheet_id,v_anchor_week.id,v_workflow.workflow_kind);
      if not coalesce((v_route_authority->>'candidate_expenses_allowed')::boolean,false)
         or (v_workflow.route='PAPER' and not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false))
         or (v_workflow.route<>'PAPER' and v_route_authority->>'route_family' not in ('ELECTRONIC','IMPORT_AUTHORITATIVE','QR')) then
        raise exception 'CANDIDATE_ROUTE_FAMILY_MISMATCH' using errcode='55000',detail=v_route_authority::text;
      end if;
    end if;
    if exists(
      select 1
      from (values
        (v_immutable_submission#>>'{canonical_tsfin_snapshot,candidate_id}'),
        (v_immutable_submission#>>'{hours_submission,canonical_tsfin_snapshot,candidate_id}'),
        (v_immutable_submission#>>'{expense_submission,canonical_tsfin_snapshot,candidate_id}')
      ) supplied(candidate_id_text)
      where nullif(supplied.candidate_id_text,'') is not null
        and supplied.candidate_id_text::uuid<>v_workflow.candidate_id
    ) or exists(
      select 1
      from (values
        (v_immutable_submission#>>'{canonical_tsfin_snapshot,client_id}'),
        (v_immutable_submission#>>'{hours_submission,canonical_tsfin_snapshot,client_id}'),
        (v_immutable_submission#>>'{expense_submission,canonical_tsfin_snapshot,client_id}')
      ) supplied(client_id_text)
      where nullif(supplied.client_id_text,'') is not null
        and supplied.client_id_text::uuid<>coalesce(v_contract.client_id,v_daily_fin.client_id)
    ) then
      raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_IDENTITY_MISMATCH' using errcode='22023';
    end if;
    if v_workflow.workflow_kind='DAILY' and exists(
      select 1
      from jsonb_array_elements(coalesce(
        v_immutable_submission#>'{timesheet_patch_json,actual_schedule_json}',
        v_immutable_submission#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
        '[]'::jsonb
      )) schedule_row
      where nullif(schedule_row->>'date','') is null
         or (schedule_row->>'date')::date<>v_workflow.work_date
    ) then
      raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='22023';
    elsif v_workflow.scope='WEEKLY' and exists(
      select 1
      from jsonb_array_elements(coalesce(
        v_immutable_submission#>'{timesheet_patch_json,actual_schedule_json}',
        v_immutable_submission#>'{hours_submission,timesheet_patch_json,actual_schedule_json}',
        '[]'::jsonb
      )) schedule_row
      where nullif(schedule_row->>'date','') is null
         or (schedule_row->>'date')::date not between v_workflow.week_ending_date-6 and v_workflow.week_ending_date
    ) then
      raise exception 'CANDIDATE_WEEK_DATE_OUT_OF_RANGE' using errcode='22023';
    end if;
    v_submission_hash:=private._candidate_sha256_jsonb_v1(v_immutable_submission);
    v_expense_submission:=case
      when v_workflow.workflow_kind='CONTRACT_COMBINED'
        and jsonb_typeof(v_immutable_submission->'expense_submission')='object'
        then v_immutable_submission->'expense_submission'
      else v_immutable_submission
    end;
    begin
      v_expense_value:=
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_units}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_charge_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric,0))+
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_charge_ex_vat}','')::numeric,0));
      v_has_mileage:=
        abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_units}','')::numeric,0))>0
        or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_pay_ex_vat}','')::numeric,0))>0
        or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,mileage_charge_ex_vat}','')::numeric,0))>0;
      v_required_categories:=array[]::text[];
      if abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_charge_ex_vat}','')::numeric,0))>0 then
        v_required_categories:=array_append(v_required_categories,'TRAVEL');
      end if;
      if abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_charge_ex_vat}','')::numeric,0))>0 then
        v_required_categories:=array_append(v_required_categories,'ACCOMMODATION');
      end if;
      if abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric,0))>0
         or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_charge_ex_vat}','')::numeric,0))>0
         or (
           (
             abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_pay_ex_vat}','')::numeric,0))>0
             or abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,expenses_charge_ex_vat}','')::numeric,0))>0
           )
           and (
             abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,travel_charge_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,accommodation_charge_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric,0))
             +abs(coalesce(nullif(v_expense_submission#>>'{canonical_tsfin_snapshot,other_charge_ex_vat}','')::numeric,0))
           )=0
         ) then
        v_required_categories:=array_append(v_required_categories,'OTHER');
      end if;
      if v_has_mileage then
        v_required_categories:=array_append(v_required_categories,'MILEAGE');
      end if;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'CANDIDATE_EXPENSE_VALUE_INVALID' using errcode='22023';
    end;
    v_has_expenses:=v_expense_value>0;
    if v_workflow.workflow_kind in ('CONTRACT_HOURS','DAILY') and v_has_expenses then
      raise exception 'CANDIDATE_WORKFLOW_KIND_ECONOMICS_MISMATCH' using errcode='22023';
    end if;
    if v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE') and not v_has_expenses then
      raise exception 'CANDIDATE_EXPENSE_CLAIM_REQUIRED' using errcode='22023';
    end if;
    v_server_issue_codes:=private._candidate_submission_issue_codes_v1(
      v_workflow.id,v_immutable_submission,v_policy
    );
    v_duplicate_expense_review:=case
      when v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE')
       and cardinality(v_required_categories)>0
      then private._expense_duplicate_review_v1(v_workflow.id,v_required_categories)
      else jsonb_build_object(
        'required',false,
        'categories','[]'::jsonb,
        'prior_claim_count',0,
        'confirmation_digest',null
      )
    end;
    if coalesce((v_duplicate_expense_review->>'required')::boolean,false) then
      if not v_is_pending_expense_update and (
         lower(coalesce(v_payload#>>'{duplicate_expense_confirmation,confirmed}','false'))
           not in ('true','t','1','yes')
         or nullif(v_payload#>>'{duplicate_expense_confirmation,confirmation_digest}','')
              is distinct from v_duplicate_expense_review->>'confirmation_digest') then
        raise exception 'CANDIDATE_DUPLICATE_EXPENSE_CONFIRMATION_REQUIRED'
          using errcode='PT409',detail=v_duplicate_expense_review::text;
      end if;
      v_server_issue_codes:=v_server_issue_codes||jsonb_build_array('DUPLICATE_EXPENSE_REVIEW');
      for v_duplicate_expense_category in
        select jsonb_array_elements_text(v_duplicate_expense_review->'categories')
      loop
        v_server_issue_codes:=v_server_issue_codes
          ||jsonb_build_array('DUPLICATE_EXPENSE_'||v_duplicate_expense_category);
      end loop;
      select coalesce(jsonb_agg(distinct issue_code order by issue_code),'[]'::jsonb)
      into v_server_issue_codes
      from jsonb_array_elements_text(v_server_issue_codes) issue_code;
    end if;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state in ('PENDING','APPROVED')
      and not exists (
        select 1 from public.candidate_pending_expense_updates pending_update
        where pending_update.workflow_id=v_workflow.id
          and pending_update.state in ('EDITING','RENDERING')
      );

    if v_is_electronic then
      v_component_no:=0;
      v_review_ordinal:=0;
      if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
        select * into v_source_component
        from public.candidate_submission_components
        where id=nullif(v_payload->>'candidate_signature_component_id','')::uuid
          and workflow_id=v_workflow.id and component_kind='CANDIDATE_SIGNATURE'
          and document_role='CANDIDATE_SIGNATURE' and state='IMMUTABLE'
          and source_content_sha256 is not null
          and (
            v_workflow.immutable_submission_sha256 is null
            or v_workflow.immutable_submission_sha256=v_submission_hash
            or (
              v_is_pending_expense_update
              and id=v_workflow.candidate_signature_component_id
              and exists(
                select 1
                from public.candidate_submission_components current_hours
                join public.candidate_submission_components prior_hours
                  on prior_hours.workflow_id=current_hours.workflow_id
                  and prior_hours.workflow_generation=
                    v_pending_expense_update.from_workflow_generation
                  and prior_hours.component_kind='HOURS_TIMESHEET'
                  and prior_hours.state='IMMUTABLE'
                  and prior_hours.source_content_sha256=current_hours.source_content_sha256
                where current_hours.workflow_id=v_workflow.id
                  and current_hours.workflow_generation=v_workflow.generation
                  and current_hours.component_kind='HOURS_TIMESHEET'
                  and current_hours.state='IMMUTABLE'
              )
            )
            or (
              source_component_id is null
              and created_at_utc>=coalesce(v_workflow.worker_submitted_at_utc,'-infinity'::timestamptz)
            )
          )
        for update;
        if not found then
          if v_workflow.immutable_submission_sha256 is not null
             and v_workflow.immutable_submission_sha256<>v_submission_hash then
            raise exception 'CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT' using errcode='55000';
          end if;
          raise exception 'CANDIDATE_SIGNATURE_REQUIRED' using errcode='55000';
        end if;
        v_component_no:=v_component_no+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          source_component_id,storage_key,media_type,byte_size,source_content_sha256,immutable_at_utc,
          required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
          'CANDIDATE_SIGNATURE','CANDIDATE_SIGNATURE','IMMUTABLE',
          coalesce(v_source_component.source_component_id,v_source_component.id),v_source_component.storage_key,
          v_source_component.media_type,v_source_component.byte_size,v_source_component.source_content_sha256,p_now_utc,
          false,null,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
        ) returning * into v_signature_component;
        v_component_no:=v_component_no+1;
        v_review_ordinal:=v_review_ordinal+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,'HOURS_TIMESHEET',
          'ELECTRONIC_TIMESHEET_MANAGER_REVIEW','IMMUTABLE',p_now_utc,true,v_review_ordinal,
          'PENDING','PENDING',p_now_utc
        ) returning * into v_component;
      elsif nullif(v_payload->>'candidate_signature_component_id','') is not null then
        raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='22023';
      end if;

      if v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE') then
        foreach v_required_category in array v_required_categories loop
          if not exists(
            select 1 from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('EXPENSE_EVIDENCE','MILEAGE_FORM')
              and source_component.expense_category=v_required_category
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
          ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
            using errcode='22023',detail=jsonb_build_object('category',v_required_category)::text; end if;
        end loop;
        if v_has_mileage and not exists(
          select 1 from public.candidate_submission_components source_component
          where source_component.workflow_id=v_workflow.id
            and source_component.workflow_generation=v_workflow.generation
            and source_component.component_kind='MILEAGE_FORM'
            and source_component.state='IMMUTABLE'
            and source_component.source_content_sha256 is not null
        ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
          using errcode='22023',detail=jsonb_build_object('category','MILEAGE')::text; end if;

        v_component_no:=v_component_no+1;
        insert into public.candidate_submission_components(
          workflow_id,workflow_generation,component_no,timesheet_id,component_kind,document_role,state,
          immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
        ) values (
          v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,'EXPENSE_SUMMARY',
          'EXPENSE_MILEAGE_APPROVAL_SUMMARY','IMMUTABLE',p_now_utc,false,null,
          'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
        );

        for v_source_component in
          select canonical_source.*
          from (
            select distinct on (
              source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256
            ) source_component.*
            from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
            order by source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256,
              source_component.component_no,source_component.id
          ) canonical_source
          order by case canonical_source.component_kind when 'MILEAGE_FORM' then 0 else 1 end,
            case canonical_source.expense_category
              when 'ACCOMMODATION' then 1 when 'TRAVEL' then 2 when 'MILEAGE' then 3
              when 'OTHER' then 4 else 5 end,
            canonical_source.component_no,canonical_source.id
        loop
          v_component_no:=v_component_no+1;
          v_review_ordinal:=v_review_ordinal+1;
          insert into public.candidate_submission_components(
            workflow_id,workflow_generation,component_no,timesheet_id,component_kind,expense_category,
            document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
            immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
          ) values (
            v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
            v_source_component.component_kind,v_source_component.expense_category,v_source_component.document_role,
            'IMMUTABLE',coalesce(v_source_component.source_component_id,v_source_component.id),
            v_source_component.storage_key,v_source_component.media_type,v_source_component.byte_size,
            v_source_component.source_content_sha256,p_now_utc,true,v_review_ordinal,'PENDING','PENDING',p_now_utc
          );
        end loop;
      end if;

      update public.candidate_submission_components set
        state='SUPERSEDED',superseded_at_utc=p_now_utc,
        review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
        final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and (required=true or document_role='MANAGER_SIGNATURE') and state<>'SUPERSEDED'
        and not exists(
          select 1 from public.candidate_pending_expense_updates pending_update
          where pending_update.workflow_id=v_workflow.id
            and pending_update.state in ('EDITING','RENDERING')
        );

      update public.candidate_submission_workflows set
        state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT',generation=v_next_generation,
        route=case when upper(coalesce(v_payload->>'approval_route',route)) in ('PHONE','EMAIL')
          then upper(coalesce(v_payload->>'approval_route',route)) else 'ELECTRONIC' end,
        input_snapshot_json=v_immutable_submission,immutable_submission_json=v_immutable_submission,
        immutable_submission_sha256=v_submission_hash,
        policy_snapshot_json=v_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
        candidate_signature_component_id=case when workflow_kind='CONTRACT_EXPENSE' then null else v_signature_component.id end,
        candidate_signature_sha256=case when workflow_kind='CONTRACT_EXPENSE' then null else v_signature_component.source_content_sha256 end,
        candidate_signed_at_utc=case when workflow_kind='CONTRACT_EXPENSE' then null
          when v_is_pending_expense_update then nullif(
            v_pending_expense_update.prior_workflow_snapshot_json->>'candidate_signed_at_utc',''
          )::timestamptz
          else coalesce(nullif(v_payload->>'candidate_signed_at_utc','')::timestamptz,p_now_utc) end,
        renderer_contract_version=coalesce(nullif(btrim(v_payload->>'renderer_contract_version'),''),'TIMESHEET_OFFICIAL_PDF_V1'),
        review_manifest_json=null,review_manifest_sha256=null,
        manager_name=null,manager_position=null,manager_signature_component_id=null,
        manager_signature_sha256=null,manager_approved_at_utc=null,
        issue_codes=v_server_issue_codes,worker_submitted_at_utc=p_now_utc,
        daily_context_sha256=null,canonical_financial_sha256=null,
        canonical_save_input_sha256=null,canonical_save_row_signature=null,
        canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
        last_mutation_idempotency_key=p_idempotency_key,updated_at_utc=p_now_utc
      where id=v_workflow.id returning * into v_workflow;
      v_render_contract:=private._candidate_render_contract_v1(
        v_workflow.id,v_workflow.generation,'ELECTRONIC_MANAGER_REVIEW');
      v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
        'state',v_workflow.state,'generation',v_workflow.generation,
        'review_document_component_id',case when v_workflow.workflow_kind='CONTRACT_EXPENSE' then null else v_component.id end,
        'render_contract',v_render_contract,'idempotent_replay',false);
    else
      v_component_no:=0;
      if v_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE') then
        foreach v_required_category in array v_required_categories loop
          if not exists(
            select 1 from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('EXPENSE_EVIDENCE','MILEAGE_FORM')
              and source_component.expense_category=v_required_category
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
          ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
            using errcode='22023',detail=jsonb_build_object('category',v_required_category)::text; end if;
        end loop;
        if v_has_mileage and not exists(
          select 1 from public.candidate_submission_components source_component
          where source_component.workflow_id=v_workflow.id
            and source_component.workflow_generation=v_workflow.generation
            and source_component.component_kind='MILEAGE_FORM'
            and source_component.expense_category='MILEAGE'
            and source_component.state='IMMUTABLE'
            and source_component.source_content_sha256 is not null
        ) then raise exception 'EXPENSE_EVIDENCE_REQUIRED'
          using errcode='22023',detail=jsonb_build_object('category','MILEAGE')::text; end if;

        for v_source_component in
          select canonical_source.*
          from (
            select distinct on (
              source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256
            ) source_component.*
            from public.candidate_submission_components source_component
            where source_component.workflow_id=v_workflow.id
              and source_component.workflow_generation=v_workflow.generation
              and source_component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
              and source_component.state='IMMUTABLE'
              and source_component.source_content_sha256 is not null
            order by source_component.component_kind,source_component.expense_category,
              source_component.document_role,
              coalesce(source_component.source_component_id,source_component.id),
              source_component.source_content_sha256,
              source_component.component_no,source_component.id
          ) canonical_source
          order by case canonical_source.component_kind when 'MILEAGE_FORM' then 0 else 1 end,
            canonical_source.component_no,canonical_source.id
        loop
          v_component_no:=v_component_no+1;
          insert into public.candidate_submission_components(
            workflow_id,workflow_generation,component_no,timesheet_id,component_kind,expense_category,
            document_role,state,source_component_id,storage_key,media_type,byte_size,source_content_sha256,
            immutable_at_utc,required,review_ordinal,review_render_state,final_signed_render_state,created_at_utc
          ) values (
            v_workflow.id,v_next_generation,v_component_no,v_workflow.target_timesheet_id,
            v_source_component.component_kind,v_source_component.expense_category,v_source_component.document_role,
            'IMMUTABLE',coalesce(v_source_component.source_component_id,v_source_component.id),
            v_source_component.storage_key,v_source_component.media_type,v_source_component.byte_size,
            v_source_component.source_content_sha256,p_now_utc,false,null,'NOT_REQUIRED','NOT_REQUIRED',p_now_utc
          );
        end loop;
      end if;
      update public.candidate_submission_components set
        state='SUPERSEDED',superseded_at_utc=p_now_utc,
        review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
        final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and (required=true or document_role='MANAGER_SIGNATURE') and state<>'SUPERSEDED'
        and not exists(
          select 1 from public.candidate_pending_expense_updates pending_update
          where pending_update.workflow_id=v_workflow.id
            and pending_update.state in ('EDITING','RENDERING')
        );
      update public.candidate_submission_workflows set
        state='WORKER_SUBMITTED',generation=v_next_generation,route='PAPER',
        input_snapshot_json=v_immutable_submission,immutable_submission_json=v_immutable_submission,
        immutable_submission_sha256=private._candidate_sha256_jsonb_v1(v_immutable_submission),
        policy_snapshot_json=v_policy,policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
        paper_return_manifest_json=null,paper_return_manifest_sha256=null,
        issue_codes=v_server_issue_codes,worker_submitted_at_utc=p_now_utc,
        daily_context_sha256=null,canonical_financial_sha256=null,
        canonical_save_input_sha256=null,canonical_save_row_signature=null,
        canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
        last_mutation_idempotency_key=p_idempotency_key,updated_at_utc=p_now_utc
      where id=v_workflow.id returning * into v_workflow;
      v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
        'state',v_workflow.state,'generation',v_workflow.generation,'idempotent_replay',false);
    end if;
    update public.candidate_submission_workflows
    set last_mutation_response_json=v_response where id=v_workflow.id;

    -- The unchanged native owner has now stored the immutable HOURS claim,
    -- its real Candidate signature and artifact generation. No Weekly upload
    -- or publication is required to prepare genuine signed-client metadata.
    if v_checklist_cycle_admission->'applies'='true'::jsonb then
      if v_workflow.target_timesheet_id is distinct from (v_checklist_cycle_admission->>'timesheet_id')::uuid then
        raise exception 'WEEKLY_SOURCE_SUBMISSION_CYCLE_WORKFLOW_STALE' using errcode='40001';
      end if;
      perform private.weekly_source_checklist_submission_cycle_prepare_v1(v_workflow.target_timesheet_id,null);
    end if;

  elsif v_action='REGISTER_REVIEW_COMPONENT' then
    if v_workflow.state not in ('WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL') then
      raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000';
    end if;
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and required=true and state<>'SUPERSEDED' for update;
    if not found then raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000'; end if;
    if coalesce(v_payload->>'content_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'render_input_sha256_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'MANAGER_REVIEW_RENDER_FAILED' using errcode='22023',
        detail=jsonb_build_object(
          'component_id',v_component.id,
          'expected_contract',v_render_contract,
          'received_receipt',v_receipt,
          'received_render_input_sha256',lower(coalesce(v_payload->>'render_input_sha256_hex',''))
        )::text;
    end if;
    v_digest:=decode(v_payload->>'content_sha256_hex','hex');
    v_render_input_hash:=decode(v_payload->>'render_input_sha256_hex','hex');
    v_receipt:=coalesce(v_payload->'renderer_receipt','{}'::jsonb);
    v_render_contract:=private._candidate_component_render_contract_v1(
      v_workflow.id,v_workflow.generation,v_component.id,'REVIEW');
    if v_render_input_hash<>decode(v_render_contract->>'render_input_sha256','hex')
       or nullif(btrim(v_payload->>'storage_key'),'') is null
       or lower(coalesce(v_payload->>'media_type','')) not in ('application/pdf','image/jpeg','image/png','image/webp')
       or coalesce((v_payload->>'page_count')::integer,0)<>1
       or coalesce((v_payload->>'byte_size')::bigint,0)<=0
       or upper(coalesce(v_receipt->>'form_variant',''))<>v_render_contract->>'form_variant'
       or nullif(v_receipt->>'workflow_id','')::uuid is distinct from v_workflow.id
       or coalesce((v_receipt->>'workflow_generation')::integer,0)<>v_workflow.generation
       or nullif(v_receipt->>'component_id','')::uuid is distinct from v_component.id
       or upper(coalesce(v_receipt->>'component_kind',''))<>v_component.component_kind
       or upper(coalesce(v_receipt->>'document_role',''))<>v_component.document_role
       or coalesce((v_receipt->>'review_ordinal')::integer,0)<>v_component.review_ordinal
       or upper(coalesce(v_receipt->>'scope',''))<>v_workflow.scope
       or coalesce((v_receipt->>'candidate_signature_embedded')::boolean,false)
          is distinct from (v_component.component_kind='HOURS_TIMESHEET')
       or coalesce((v_receipt->>'manager_signature_embedded')::boolean,false)=true
       or coalesce((v_receipt->>'manager_approval_date_embedded')::boolean,false)=true
       or coalesce((v_receipt->>'page_count')::integer,0)<>1
       or lower(coalesce(v_receipt->>'render_input_sha256',''))<>encode(v_render_input_hash,'hex') then
      raise exception 'MANAGER_REVIEW_RENDER_FAILED' using errcode='22023',
        detail=jsonb_build_object(
          'component_id',v_component.id,
          'expected_contract',v_render_contract,
          'received_receipt',v_receipt,
          'received_render_input_sha256',lower(coalesce(v_payload->>'render_input_sha256_hex',''))
        )::text;
    end if;
    if v_component.review_render_state='READY' then
      if v_component.review_storage_key=v_payload->>'storage_key'
         and v_component.review_content_sha256=v_digest
         and v_component.review_render_input_sha256=v_render_input_hash then
        v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'component_id',v_component.id,'state',v_workflow.state);
        if v_mutation_request_sha256 is not null then
          perform private._candidate_workflow_mutation_receipt_v1(
            v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
            v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
          );
        end if;
        return v_response;
      end if;
      raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      review_storage_key=v_payload->>'storage_key',review_content_sha256=v_digest,
      review_media_type=lower(v_payload->>'media_type'),review_byte_size=(v_payload->>'byte_size')::bigint,
      review_page_count=1,review_render_input_sha256=v_render_input_hash,
      review_renderer_contract_version=coalesce(nullif(v_payload->>'renderer_contract_version',''),v_workflow.renderer_contract_version),
      review_renderer_receipt_json=v_receipt,review_generated_at_utc=p_now_utc,
      review_render_state='READY'
    where id=v_component.id returning * into v_component;
    if v_worker_only_approval then
      if v_component.component_kind<>'HOURS_TIMESHEET'
         or v_render_contract->>'form_variant'<>'ELECTRONIC_WORKER_SIGNED'
         or v_receipt->>'form_variant'<>'ELECTRONIC_WORKER_SIGNED'
         or v_receipt->>'candidate_signature_sha256' is distinct from encode(v_workflow.candidate_signature_sha256,'hex')
         or coalesce((v_receipt->>'manager_signature_embedded')::boolean,false)
         or coalesce((v_receipt->>'manager_approval_date_embedded')::boolean,false) then
        raise exception 'CANDIDATE_WORKER_ONLY_DOCUMENT_INVALID' using errcode='55000';
      end if;
      update public.candidate_submission_components set
        final_signed_storage_key=review_storage_key,final_signed_content_sha256=review_content_sha256,
        final_signed_media_type=review_media_type,final_signed_byte_size=review_byte_size,
        final_signed_page_count=review_page_count,final_signed_render_input_sha256=review_render_input_sha256,
        final_signed_renderer_contract_version=review_renderer_contract_version,
        final_signed_renderer_receipt_json=review_renderer_receipt_json,
        final_signed_generated_at_utc=review_generated_at_utc,final_signed_render_state='READY'
      where id=v_component.id returning * into v_component;
    end if;
    v_manifest:=private._candidate_review_manifest_v1(v_workflow.id,v_workflow.generation);
    if coalesce((v_manifest->>'all_ready')::boolean,false) then
      update public.candidate_submission_workflows set
        state=case when v_worker_only_approval then 'READY_TO_FINALISE' else 'READY_FOR_MANAGER_APPROVAL' end,review_manifest_json=v_manifest,
        review_manifest_sha256=decode(v_manifest->>'manifest_sha256','hex'),
        last_mutation_idempotency_key=p_idempotency_key,updated_at_utc=p_now_utc
      where id=v_workflow.id returning * into v_workflow;
    end if;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,
      'workflow_id',v_workflow.id,'state',v_workflow.state,'generation',v_workflow.generation,
      'component_id',v_component.id,'review_document_ready',v_component.review_render_state='READY',
      'review_manifest',v_manifest)||case when v_worker_only_approval
        then jsonb_build_object('worker_only_approval',true) else '{}'::jsonb end;
    update public.candidate_submission_workflows set last_mutation_response_json=v_response where id=v_workflow.id;

  elsif v_action in ('SELECT_PHONE_APPROVAL','CREATE_EMAIL_APPROVAL_REQUEST') then
    if v_worker_only_approval then
      raise exception 'CANDIDATE_MANAGER_APPROVAL_NOT_REQUIRED' using errcode='55000';
    end if;
    if v_workflow.state not in ('READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL') then
      raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    v_manifest:=private._candidate_review_manifest_v1(v_workflow.id,v_workflow.generation);
    if coalesce((v_manifest->>'all_ready')::boolean,false)=false
       or decode(v_manifest->>'manifest_sha256','hex') is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
    end if;
    select array_agg(c.id order by c.review_ordinal,c.id) into v_component_ids
    from public.candidate_submission_components c
    where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
      and c.required=true and c.state<>'SUPERSEDED';
    select coalesce(max(request_generation),0)+1 into v_request_generation
    from public.candidate_approval_requests where workflow_id=v_workflow.id;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';
    if v_action='CREATE_EMAIL_APPROVAL_REQUEST' then
      if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
        raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
      end if;
      v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
      v_email_check:=private._candidate_manager_email_allowed_v1(
        v_policy->'manager_approval_policy',v_payload->>'manager_email',
        v_policy->'barred_manager_email_domains');
      if coalesce((v_email_check->>'allowed')::boolean,false)=false then
        raise exception 'MANAGER_EMAIL_NOT_ALLOWED' using errcode='22023',detail=v_email_check::text;
      end if;
      insert into public.candidate_approval_requests(
        workflow_id,workflow_generation,request_generation,method,state,manager_email_normalized,
        token_hash,expires_at_utc,initial_sent_at_utc,last_sent_at_utc,next_reminder_at_utc,
        review_manifest_sha256,required_component_ids,required_component_manifest_json,
        manager_review_timesheet_component_id,manager_review_timesheet_sha256,
        idempotency_key,created_at_utc,updated_at_utc
      ) values (
        v_workflow.id,v_workflow.generation,v_request_generation,'EMAIL','PENDING',
        v_email_check->>'email_normalized',v_token_hash,p_now_utc+interval '7 days',
        null,null,null,
        decode(v_manifest->>'manifest_sha256','hex'),v_component_ids,v_manifest->'required_components',
        nullif(v_manifest->>'manager_review_timesheet_component_id','')::uuid,
        case when nullif(v_manifest->>'manager_review_timesheet_sha256','') is null then null
          else decode(v_manifest->>'manager_review_timesheet_sha256','hex') end,
        p_idempotency_key,p_now_utc,p_now_utc
      ) returning * into v_approval;
      v_mail_id:=private._candidate_queue_mail_v1(
        coalesce(v_payload->'mail','{}'::jsonb)||jsonb_build_object(
          'payment_scope_json',jsonb_build_object(
            'candidate_mail_authority','MANAGER_APPROVAL_V1',
            'candidate_manager_mail_kind','INITIAL',
            'candidate_manager_workflow_id',v_workflow.id,
            'candidate_manager_workflow_generation',v_workflow.generation,
            'candidate_approval_request_id',v_approval.id,
            'candidate_approval_request_generation',v_approval.request_generation,
            'candidate_manager_mail_retired',false
          )
        ),v_approval.manager_email_normalized,
        'CANDIDATE_MANAGER_APPROVAL_V1:'||v_approval.id::text||':0',
        'candidate-manager-approval:'||v_approval.id::text,v_workflow.id,p_now_utc);
    else
      if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$'
         or nullif(v_payload->>'expires_at_utc','')::timestamptz<=p_now_utc
         or nullif(v_payload->>'expires_at_utc','')::timestamptz>p_now_utc+interval '2 hours' then
        raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
      end if;
      v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
      insert into public.candidate_approval_requests(
        workflow_id,workflow_generation,request_generation,method,state,token_hash,expires_at_utc,
        review_manifest_sha256,required_component_ids,required_component_manifest_json,
        manager_review_timesheet_component_id,manager_review_timesheet_sha256,
        idempotency_key,created_at_utc,updated_at_utc
      ) values (
        v_workflow.id,v_workflow.generation,v_request_generation,'PHONE','PENDING',v_token_hash,
        nullif(v_payload->>'expires_at_utc','')::timestamptz,
        decode(v_manifest->>'manifest_sha256','hex'),v_component_ids,v_manifest->'required_components',
        nullif(v_manifest->>'manager_review_timesheet_component_id','')::uuid,
        case when nullif(v_manifest->>'manager_review_timesheet_sha256','') is null then null
          else decode(v_manifest->>'manager_review_timesheet_sha256','hex') end,
        p_idempotency_key,p_now_utc,p_now_utc
      ) returning * into v_approval;
    end if;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'approval_request_generation',v_approval.request_generation,
      'method',v_approval.method,
      'review_manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
      'issued_at_utc',v_approval.created_at_utc,
      'expires_at_utc',v_approval.expires_at_utc,'mail_outbox_id',v_mail_id);
    if v_action='SELECT_PHONE_APPROVAL' then
      if coalesce(v_payload->>'handoff_token_key_version','') !~ '^[1-9][0-9]{0,2}$'
         or (v_payload->>'handoff_token_key_version')::integer>32 then
        raise exception 'CANDIDATE_REPLAY_KEY_VERSION_INVALID' using errcode='22023';
      end if;
      if jsonb_typeof(v_payload->'public_broker_binding')<>'object'
         or coalesce(v_payload#>>'{public_broker_binding,contract_version}','')
              <>'CANDIDATE_PUBLIC_PHONE_BINDING_V1'
         or coalesce(v_payload#>>'{public_broker_binding,public_session_binding_sha256}','')
              !~ '^[0-9a-f]{64}$'
         or (v_payload#>'{public_broker_binding,device_binding_sha256}') is not null
            and jsonb_typeof(v_payload#>'{public_broker_binding,device_binding_sha256}')<>'null'
            and coalesce(v_payload#>>'{public_broker_binding,device_binding_sha256}','')
                  !~ '^[0-9a-f]{64}$'
         or coalesce(v_payload->>'broker_handoff_key_version','') !~ '^[1-9][0-9]{0,4}$'
         or (v_payload->>'broker_handoff_key_version')::integer>65535 then
        raise exception 'CANDIDATE_PHONE_HANDOFF_BINDING_INVALID' using errcode='22023';
      end if;
      v_response:=v_response||jsonb_build_object(
        'approval_token_hash_hex',encode(v_approval.token_hash,'hex'),
        'handoff_token_key_version',(v_payload->>'handoff_token_key_version')::integer,
        'public_broker_binding',v_payload->'public_broker_binding',
        'broker_handoff_key_version',(v_payload->>'broker_handoff_key_version')::integer
      );
    end if;
    update public.candidate_submission_workflows set
      state='AWAITING_MANAGER_APPROVAL',route=v_approval.method,policy_snapshot_json=v_policy,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='BEGIN_MANAGER_REVIEW' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if v_approval.workflow_generation<>v_workflow.generation
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_APPROVAL_REQUEST_SUPERSEDED' using errcode='40001';
    end if;
    update public.candidate_approval_requests set
      review_started_at_utc=coalesce(review_started_at_utc,p_now_utc),updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    select count(*) into v_reviewed_count from unnest(v_approval.required_component_ids) u(id)
    where v_approval.review_progress_json ? u.id::text;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'approval_request_generation',v_approval.request_generation,
      'workflow_kind',v_workflow.workflow_kind,
      'method',v_approval.method,'expires_at_utc',v_approval.expires_at_utc,
      'manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
      'page_count',coalesce((select sum(coalesce(c.review_page_count,1))
        from public.candidate_submission_components c where c.id=any(v_approval.required_component_ids)),0),
      'ordered_components',(select coalesce(jsonb_agg(
        component.value||jsonb_build_object(
          'viewed',v_approval.review_progress_json ? (component.value->>'component_id')
        ) order by (component.value->>'ordinal')::integer
      ),'[]'::jsonb) from jsonb_array_elements(v_approval.required_component_manifest_json) component),
      'reviewed_count',v_reviewed_count,
      'all_pages_viewed',v_reviewed_count=cardinality(v_approval.required_component_ids),
      'manager_identity_requirements',jsonb_build_object('name_required',true,'position_required',true,'signature_required',true),
      'can_approve',true,'can_refuse',true);
    if v_mutation_request_sha256 is not null then
      perform private._candidate_workflow_mutation_receipt_v1(
        v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
        v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
      );
    end if;
    return v_response;

  elsif v_action='RECORD_REVIEW_PROGRESS' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if lower(coalesce(v_payload->>'manifest_sha256_hex',''))<>encode(v_approval.review_manifest_sha256,'hex') then
      raise exception 'MANAGER_REVIEW_MANIFEST_MISMATCH' using errcode='40001';
    end if;
    select count(*) into v_reviewed_count from unnest(v_approval.required_component_ids) u(id)
    where v_approval.review_progress_json ? u.id::text;
    if (v_approval.method='EMAIL' or nullif(v_payload->>'progress_version','') is not null)
       and coalesce(nullif(v_payload->>'progress_version','')::integer,-1)<>v_reviewed_count then
      raise exception 'MANAGER_REVIEW_PROGRESS_CONFLICT' using errcode='40001';
    end if;
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid
      and id=any(v_approval.required_component_ids)
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and review_render_state='READY' for update;
    if not found or lower(coalesce(v_payload->>'component_sha256_hex',''))<>encode(v_component.review_content_sha256,'hex') then
      raise exception 'MANAGER_REVIEW_MANIFEST_MISMATCH' using errcode='40001';
    end if;
    update public.candidate_approval_requests set
      review_progress_json=review_progress_json||jsonb_build_object(v_component.id::text,jsonb_build_object(
        'component_id',v_component.id,'component_sha256',encode(v_component.review_content_sha256,'hex'),
        'manifest_sha256',encode(v_approval.review_manifest_sha256,'hex'),
        'viewed_receipt',coalesce(v_payload->'viewed_receipt','{}'::jsonb),
        'reviewed_at_utc',p_now_utc)),
      review_started_at_utc=coalesce(review_started_at_utc,p_now_utc),updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    update public.candidate_submission_components set manager_reviewed_at_utc=p_now_utc
    where id=v_component.id;
    select count(*) into v_reviewed_count from unnest(v_approval.required_component_ids) u(id)
    where v_approval.review_progress_json ? u.id::text;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'approval_request_generation',v_approval.request_generation,
      'component_id',v_component.id,'reviewed_count',v_reviewed_count,
      'required_count',cardinality(v_approval.required_component_ids),
      'progress_version',v_reviewed_count,
      'all_pages_viewed',v_reviewed_count=cardinality(v_approval.required_component_ids));
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action in ('PHONE_APPROVE','EMAIL_APPROVE') then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and method='PHONE' and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if (v_action='EMAIL_APPROVE' and v_approval.method<>'EMAIL')
       or (v_action='PHONE_APPROVE' and v_approval.method<>'PHONE') then
      raise exception 'MANAGER_APPROVAL_METHOD_MISMATCH' using errcode='28000';
    end if;
    if lower(coalesce(v_payload->>'manifest_sha256_hex',''))<>encode(v_approval.review_manifest_sha256,'hex')
       or v_approval.workflow_generation<>v_workflow.generation
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REVIEW_MANIFEST_MISMATCH' using errcode='40001';
    end if;
    if exists(
      select 1 from unnest(v_approval.required_component_ids) u(id)
      join public.candidate_submission_components c on c.id=u.id
      where not (v_approval.review_progress_json ? u.id::text)
         or v_approval.review_progress_json#>>array[u.id::text,'component_sha256']
            is distinct from encode(c.review_content_sha256,'hex')
    ) then
      raise exception 'MANAGER_REVIEW_COMPONENT_NOT_REVIEWED' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(v_payload->>'manager_name','')),'') is null
       or nullif(btrim(coalesce(v_payload->>'manager_position','')),'') is null
       or pg_catalog.length(btrim(v_payload->>'manager_name'))>200
       or pg_catalog.length(btrim(v_payload->>'manager_position'))>200
       or (v_approval.method='EMAIL' and (
         coalesce(v_payload->>'attestation_version','')<>'MANAGER_APPROVAL_ATTESTATION_V1'
         or coalesce((v_payload->>'attestation_accepted')::boolean,false)<>true
       )) then
      raise exception 'MANAGER_SIGNATURE_REQUIRED' using errcode='22023';
    end if;
    select * into v_signature_component from public.candidate_submission_components
    where id=nullif(v_payload->>'signature_component_id','')::uuid
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and document_role='MANAGER_SIGNATURE' and state='IMMUTABLE'
      and approval_request_id=v_approval.id
      and source_content_sha256 is not null for update;
    if not found then raise exception 'MANAGER_SIGNATURE_REQUIRED' using errcode='22023'; end if;
    update public.candidate_approval_requests set
      state='APPROVED',manager_name=btrim(v_payload->>'manager_name'),
      manager_position=btrim(v_payload->>'manager_position'),signature_component_id=v_signature_component.id,
      approved_at_utc=p_now_utc,review_completed_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and id<>v_approval.id and state='PENDING';
    perform public.candidate_manager_email_route_receipt_retire_v1(
      v_workflow.id,v_approval.id,'MANAGER_APPROVED',p_now_utc
    );
    update public.candidate_submission_components set manager_approved_at_utc=p_now_utc
    where id=any(v_approval.required_component_ids);
    update public.candidate_submission_workflows set
      state='MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',route=v_approval.method,
      manager_name=v_approval.manager_name,manager_position=v_approval.manager_position,
      manager_signature_component_id=v_signature_component.id,
      manager_signature_sha256=v_signature_component.source_content_sha256,
      manager_approved_at_utc=p_now_utc,last_mutation_idempotency_key=p_idempotency_key,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;
    v_render_contract:=private._candidate_render_contract_v1(
      v_workflow.id,v_workflow.generation,'FINAL_SIGNED');
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state',v_workflow.state,'generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'approval_request_generation',v_approval.request_generation,
      'approved_at_utc',v_approval.approved_at_utc,
      'final_render_contract',v_render_contract);
    update public.candidate_submission_workflows set last_mutation_response_json=v_response
    where id=v_workflow.id;
    perform private._candidate_notification_insert_v1(v_account_id,v_candidate_id,v_workflow.id,
      v_workflow.target_timesheet_id,'MANAGER_APPROVED','manager_approval',
      'candidate-manager-approved-v1','{}'::jsonb,
      jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      'CANDIDATE_MANAGER_APPROVED_V1:'||v_workflow.id::text||':'||v_workflow.generation::text,p_now_utc);

  elsif v_action='REGISTER_FINAL_SIGNED_DOCUMENT' then
    if v_workflow.state not in ('MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE') then
      raise exception 'FINAL_SIGNED_DOCUMENT_STALE' using errcode='55000';
    end if;
    select * into v_component from public.candidate_submission_components
    where id=nullif(v_payload->>'component_id','')::uuid
      and workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and required=true and state='IMMUTABLE'
      and review_render_state='READY' for update;
    if not found then raise exception 'FINAL_SIGNED_DOCUMENT_STALE' using errcode='55000'; end if;
    if coalesce(v_payload->>'content_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'render_input_sha256_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'FINAL_SIGNED_DOCUMENT_NOT_READY' using errcode='22023';
    end if;
    v_digest:=decode(v_payload->>'content_sha256_hex','hex');
    v_render_input_hash:=decode(v_payload->>'render_input_sha256_hex','hex');
    v_receipt:=coalesce(v_payload->'renderer_receipt','{}'::jsonb);
    v_render_contract:=private._candidate_component_render_contract_v1(
      v_workflow.id,v_workflow.generation,v_component.id,'FINAL');
    if v_render_input_hash<>decode(v_render_contract->>'render_input_sha256','hex')
       or v_render_input_hash is distinct from v_component.review_render_input_sha256
       or nullif(btrim(v_payload->>'storage_key'),'') is null
       or lower(coalesce(v_payload->>'media_type',''))<>'application/pdf'
       or coalesce((v_payload->>'page_count')::integer,0)<>1
       or coalesce((v_payload->>'byte_size')::bigint,0)<=0
       or upper(coalesce(v_receipt->>'form_variant',''))<>v_render_contract->>'form_variant'
       or nullif(v_receipt->>'workflow_id','')::uuid is distinct from v_workflow.id
       or coalesce((v_receipt->>'workflow_generation')::integer,0)<>v_workflow.generation
       or nullif(v_receipt->>'component_id','')::uuid is distinct from v_component.id
       or upper(coalesce(v_receipt->>'component_kind',''))<>v_component.component_kind
       or upper(coalesce(v_receipt->>'document_role',''))<>v_component.document_role
       or coalesce((v_receipt->>'review_ordinal')::integer,0)<>v_component.review_ordinal
       or upper(coalesce(v_receipt->>'scope',''))<>v_workflow.scope
       or coalesce((v_receipt->>'candidate_signature_embedded')::boolean,false)
          is distinct from (v_component.component_kind='HOURS_TIMESHEET')
       or coalesce((v_receipt->>'manager_signature_embedded')::boolean,false)=false
       or coalesce((v_receipt->>'manager_approval_date_embedded')::boolean,false)=false
       or coalesce((v_receipt->>'page_count')::integer,0)<>1
       or lower(coalesce(v_receipt->>'render_input_sha256',''))<>encode(v_render_input_hash,'hex')
       or (v_component.component_kind='HOURS_TIMESHEET' and
          lower(coalesce(v_receipt->>'candidate_signature_sha256',''))<>encode(v_workflow.candidate_signature_sha256,'hex'))
       or lower(coalesce(v_receipt->>'manager_signature_sha256',''))<>encode(v_workflow.manager_signature_sha256,'hex')
       or btrim(coalesce(v_receipt->>'manager_name',''))<>v_workflow.manager_name
       or btrim(coalesce(v_receipt->>'manager_position',''))<>v_workflow.manager_position
       or nullif(v_receipt->>'manager_approved_at_utc','')::timestamptz is distinct from v_workflow.manager_approved_at_utc then
      raise exception 'FINAL_RENDER_INPUT_MISMATCH' using errcode='40001';
    end if;
    if v_component.final_signed_render_state='READY' then
      if v_component.final_signed_storage_key=v_payload->>'storage_key'
         and v_component.final_signed_content_sha256=v_digest
         and v_component.final_signed_render_input_sha256=v_render_input_hash then
        v_response:=jsonb_build_object('ok',true,'idempotent_replay',true,
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'component_id',v_component.id,'state',v_workflow.state);
        if v_mutation_request_sha256 is not null then
          perform private._candidate_workflow_mutation_receipt_v1(
            v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
            v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
          );
        end if;
        return v_response;
      end if;
      raise exception 'FINAL_SIGNED_DOCUMENT_STALE' using errcode='55000';
    end if;
    update public.candidate_submission_components set
      final_signed_storage_key=v_payload->>'storage_key',final_signed_content_sha256=v_digest,
      final_signed_media_type='application/pdf',final_signed_byte_size=(v_payload->>'byte_size')::bigint,
      final_signed_page_count=1,final_signed_render_input_sha256=v_render_input_hash,
      final_signed_renderer_contract_version=coalesce(nullif(v_payload->>'renderer_contract_version',''),v_workflow.renderer_contract_version),
      final_signed_renderer_receipt_json=v_receipt,final_signed_generated_at_utc=p_now_utc,
      final_signed_render_state='READY'
    where id=v_component.id returning * into v_component;
    select exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
    ) and not exists(
      select 1 from public.candidate_submission_components c
      where c.workflow_id=v_workflow.id and c.workflow_generation=v_workflow.generation
        and c.required=true and c.state<>'SUPERSEDED'
        and c.final_signed_render_state<>'READY'
    ) into v_all_final_ready;
    v_response:=jsonb_build_object('ok',true,'idempotent_replay',false,
      'workflow_id',v_workflow.id,
      'state',case when v_all_final_ready then 'READY_TO_FINALISE' else 'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' end,
      'generation',v_workflow.generation,'component_id',v_component.id,
      'component_final_signed_document_ready',true,'all_final_signed_documents_ready',v_all_final_ready);
    update public.candidate_submission_workflows set
      state=case when v_all_final_ready then 'READY_TO_FINALISE' else 'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' end,
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='BEGIN_CANONICAL_DAILY_SAVE' then
    if v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
       or v_workflow.state<>'READY_TO_FINALISE' or v_workflow.route not in ('PHONE','EMAIL') then
      raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_NOT_READY' using errcode='55000';
    end if;
    if not private._candidate_daily_entitled_v1(v_workflow.candidate_id) then
      raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
    end if;
    v_daily_input:=private._candidate_daily_canonical_save_input_v1(v_workflow.id,v_workflow.generation);
    v_expected_save_hash:=private._candidate_sha256_jsonb_v1(v_daily_input);
    -- An unresolved new Daily receipt must not enter the financial save/process
    -- path. Its finaliser still verifies the same manager, documents and input.
    if coalesce((v_daily_receipt_context->>'candidate_first_receipt')::boolean,false)
       and coalesce((v_daily_receipt_context->>'office_resolution_pending')::boolean,false) then
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'state',v_workflow.state,'receipt_mode','DAILY_FACTUAL',
        'timesheet_id',v_workflow.target_timesheet_id,
        'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'));
      if v_mutation_request_sha256 is not null then
        perform private._candidate_workflow_mutation_receipt_v1(
          v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
          v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc);
      end if;
      return v_response;
    end if;
    v_daily_context:=private._candidate_daily_context_contract_v1(
      v_workflow.id,v_workflow.generation
    );
    v_daily_context_hash:=private._candidate_sha256_jsonb_v1(v_daily_context);
    if v_workflow.canonical_saved_at_utc is not null
       and v_workflow.canonical_save_input_sha256=v_expected_save_hash
       and v_workflow.canonical_save_financials_id is not null
       and nullif(btrim(coalesce(v_workflow.canonical_save_row_signature,'')),'') is not null then
      v_response:=jsonb_build_object(
        'ok',true,'idempotent_replay',true,'workflow_id',v_workflow.id,
        'generation',v_workflow.generation,'state',v_workflow.state,
        'canonical_save_registered',true,
        'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'),
        'canonical_context_sha256_hex',encode(v_workflow.daily_context_sha256,'hex'),
        'canonical_save_row_signature',v_workflow.canonical_save_row_signature,
        'canonical_save_financials_id',v_workflow.canonical_save_financials_id
      );
      if v_mutation_request_sha256 is not null then
        perform private._candidate_workflow_mutation_receipt_v1(
          v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
          v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
        );
      end if;
      return v_response;
    end if;
    v_current_row_signature:=nullif(btrim(v_daily_context->>'pre_save_row_signature'),'');
    if v_current_row_signature is null then
      raise exception 'CANDIDATE_DAILY_ROW_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'idempotent_replay',false,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'state',v_workflow.state,
      'canonical_save_registered',false,'canonical_save_input',v_daily_input,
      'canonical_save_input_sha256_hex',encode(v_expected_save_hash,'hex'),
      'canonical_context',v_daily_context,
      'canonical_context_sha256_hex',encode(v_daily_context_hash,'hex'),
      'expected_row_signature',v_current_row_signature
    );
    update public.candidate_submission_workflows set
      daily_context_sha256=v_daily_context_hash,canonical_financial_sha256=null,
      canonical_save_input_sha256=null,canonical_save_row_signature=null,
      canonical_save_financials_id=null,canonical_save_receipt_json=null,canonical_saved_at_utc=null,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='MANAGER_REFUSE' then
    if v_is_public_manager_action then
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and token_hash=v_token_hash for update;
    else
      select * into v_approval from public.candidate_approval_requests
      where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
        and state='PENDING'
        and (not coalesce((v_payload->>'service_office_action')::boolean,false)
          or (id=(v_payload->>'approval_request_id')::uuid
            and request_generation=(v_payload->>'approval_request_generation')::integer))
      for update;
    end if;
    if not found or v_approval.state<>'PENDING' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_READY' using errcode='28000';
    end if;
    if v_approval.expires_at_utc<=p_now_utc then
      raise exception 'MANAGER_APPROVAL_REQUEST_EXPIRED' using errcode='28000';
    end if;
    if nullif(btrim(coalesce(v_payload->>'reason','')),'') is null
       or pg_catalog.length(btrim(v_payload->>'reason'))>1000 then
      raise exception 'MANAGER_REFUSAL_REASON_REQUIRED' using errcode='22023';
    end if;
    update public.candidate_approval_requests set
      state='REFUSED',refusal_reason=btrim(v_payload->>'reason'),refused_at_utc=p_now_utc,
      updated_at_utc=p_now_utc where id=v_approval.id;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and id<>v_approval.id and state='PENDING';
    perform public.candidate_manager_email_route_receipt_retire_v1(
      v_workflow.id,v_approval.id,'MANAGER_REFUSED',p_now_utc
    );
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'state','REFUSED',
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'approval_request_generation',v_approval.request_generation,
      'refused_at_utc',p_now_utc,'rejection_scope','COMPLETE_ELECTRONIC_TRANSACTION');
    update public.candidate_submission_workflows set
      state='REFUSED',rejection_reason=btrim(v_payload->>'reason'),
      rejection_scope='COMPLETE_ELECTRONIC_TRANSACTION',last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc where id=v_workflow.id;
    perform private._candidate_notification_insert_v1(v_account_id,v_candidate_id,v_workflow.id,
      v_workflow.target_timesheet_id,'MANAGER_REFUSED','manager_refusal','candidate-manager-refused-v1',
      jsonb_build_object('reason',btrim(v_payload->>'reason')),
      jsonb_build_object('type','workflow','workflow_id',v_workflow.id),
      'CANDIDATE_MANAGER_REFUSED_V1:'||v_workflow.id::text||':'||v_workflow.generation::text,p_now_utc);

  elsif v_action='REMIND' then
    if nullif(v_payload->>'approval_request_id','') is null
       or nullif(v_payload->>'approval_request_generation','') is null then
      raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and method='EMAIL' and state='PENDING'
      and id=(v_payload->>'approval_request_id')::uuid
      and request_generation=(v_payload->>'approval_request_generation')::integer
    for update skip locked;
    if not found or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_REMINDER_NOT_ELIGIBLE' using errcode='55000';
    end if;
    if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
    end if;
    select max(manager_mail.sent_at),count(*) filter (
      where manager_mail.status='QUEUED' and manager_mail.sent_at is null
        and lower(coalesce(manager_mail.payment_scope_json->>'candidate_manager_mail_retired','false'))
              in ('false','f','0','no')
    )::integer
    into v_manager_provider_accepted_at,v_manager_pending_mail_count
    from public.mail_outbox manager_mail
    where upper(coalesce(manager_mail.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
      and manager_mail.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
      and manager_mail.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
      and manager_mail.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
      and manager_mail.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text
      and upper(coalesce(manager_mail.payment_scope_json->>'candidate_manager_mail_kind',''))
            in ('INITIAL','REMINDER','RENEWAL')
      and (
        manager_mail.status='QUEUED'
        or (manager_mail.status='SENT' and manager_mail.sent_at is not null
          and upper(coalesce(manager_mail.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK'))
      );
    if v_approval.expires_at_utc<=p_now_utc or v_approval.resend_count>=5
       or v_manager_provider_accepted_at is null
       or v_manager_provider_accepted_at+interval '24 hours'>p_now_utc
       or v_manager_pending_mail_count>0 then
      raise exception 'MANAGER_REMINDER_NOT_ELIGIBLE' using errcode='55000';
    end if;
    v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
    update public.candidate_approval_requests set
      token_hash=v_token_hash,resend_count=resend_count+1,
      initial_sent_at_utc=null,last_sent_at_utc=null,next_reminder_at_utc=null,
      updated_at_utc=p_now_utc
    where id=v_approval.id returning * into v_approval;
    v_mail_id:=private._candidate_queue_mail_v1(
      coalesce(v_payload->'mail','{}'::jsonb)||jsonb_build_object(
        'payment_scope_json',jsonb_build_object(
          'candidate_mail_authority','MANAGER_APPROVAL_V1',
          'candidate_manager_mail_kind','REMINDER',
          'candidate_manager_workflow_id',v_workflow.id,
          'candidate_manager_workflow_generation',v_workflow.generation,
          'candidate_approval_request_id',v_approval.id,
          'candidate_approval_request_generation',v_approval.request_generation,
          'candidate_manager_mail_retired',false
        )
      ),v_approval.manager_email_normalized,
      'CANDIDATE_MANAGER_REMINDER_V1:'||v_approval.id::text||':'||v_approval.resend_count::text,
      'candidate-manager-reminder:'||v_approval.id::text||':'||v_approval.resend_count::text,
      v_workflow.id,p_now_utc);
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'approval_request_id',v_approval.id,
      'resend_count',v_approval.resend_count,
      'previous_provider_accepted_at_utc',v_manager_provider_accepted_at,
      'reminder_delivery_pending',true,
      'mail_outbox_id',v_mail_id);
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='RENEW' then
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL'
       or v_workflow.review_manifest_sha256 is null then
      raise exception 'MANAGER_APPROVAL_NOT_RENEWABLE' using errcode='55000';
    end if;
    if nullif(v_payload->>'approval_request_id','') is null
       or nullif(v_payload->>'approval_request_generation','') is null then
      raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and method='EMAIL'
      and id=(v_payload->>'approval_request_id')::uuid
      and request_generation=(v_payload->>'approval_request_generation')::integer
    for update;
    if found and v_approval.state='PENDING' and v_approval.expires_at_utc<=p_now_utc then
      update public.candidate_approval_requests set
        state='EXPIRED',updated_at_utc=p_now_utc
      where id=v_approval.id returning * into v_approval;
    end if;
    if not found or v_approval.state<>'EXPIRED'
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'MANAGER_APPROVAL_NOT_RENEWABLE' using errcode='55000';
    end if;
    if coalesce(v_payload->>'approval_token_hash_hex','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_APPROVAL_TOKEN_INVALID' using errcode='22023';
    end if;
    v_token_hash:=decode(v_payload->>'approval_token_hash_hex','hex');
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc where id=v_approval.id;
    insert into public.candidate_approval_requests(
      workflow_id,workflow_generation,request_generation,method,state,manager_email_normalized,
      token_hash,expires_at_utc,initial_sent_at_utc,last_sent_at_utc,next_reminder_at_utc,
      renewal_count,review_manifest_sha256,required_component_ids,required_component_manifest_json,
      manager_review_timesheet_component_id,manager_review_timesheet_sha256,
      idempotency_key,created_at_utc,updated_at_utc
    ) values (
      v_workflow.id,v_workflow.generation,v_approval.request_generation+1,'EMAIL','PENDING',
      v_approval.manager_email_normalized,v_token_hash,p_now_utc+interval '7 days',null,null,
      null,v_approval.renewal_count+1,v_approval.review_manifest_sha256,
      v_approval.required_component_ids,v_approval.required_component_manifest_json,
      v_approval.manager_review_timesheet_component_id,v_approval.manager_review_timesheet_sha256,
      p_idempotency_key,p_now_utc,p_now_utc
    ) returning * into v_approval;
    v_mail_id:=private._candidate_queue_mail_v1(
      coalesce(v_payload->'mail','{}'::jsonb)||jsonb_build_object(
        'payment_scope_json',jsonb_build_object(
          'candidate_mail_authority','MANAGER_APPROVAL_V1',
          'candidate_manager_mail_kind','RENEWAL',
          'candidate_manager_workflow_id',v_workflow.id,
          'candidate_manager_workflow_generation',v_workflow.generation,
          'candidate_approval_request_id',v_approval.id,
          'candidate_approval_request_generation',v_approval.request_generation,
          'candidate_manager_mail_retired',false
        )
      ),v_approval.manager_email_normalized,
      'CANDIDATE_MANAGER_RENEW_V1:'||v_approval.id::text,
      'candidate-manager-renew:'||v_approval.id::text,v_workflow.id,p_now_utc);
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'expires_at_utc',v_approval.expires_at_utc,
      'renewal_count',v_approval.renewal_count,'mail_outbox_id',v_mail_id);
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='MANAGER_REQUEST_CANCEL' then
    if not v_is_service_action then
      raise exception 'CANDIDATE_MANAGER_REQUEST_CANCEL_SERVICE_REQUIRED' using errcode='42501';
    end if;
    v_cancel_reason:=nullif(btrim(coalesce(v_payload->>'reason_note',v_payload->>'reason','')), '');
    v_cancel_reason_code:=coalesce(
      nullif(upper(btrim(coalesce(v_payload->>'reason_code',''))),''),
      'OFFICE_MANAGER_REQUEST_CANCELLED'
    );
    if v_cancel_reason is null then
      raise exception 'CANDIDATE_CANCELLATION_REASON_REQUIRED' using errcode='22023';
    end if;
    if length(v_cancel_reason)>1000 then
      raise exception 'CANDIDATE_CANCELLATION_REASON_INVALID' using errcode='22023';
    end if;
    v_audit_reason:=v_cancel_reason;
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL' then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_CANCELLABLE' using errcode='55000';
    end if;
    select request_row.* into v_approval
    from public.candidate_approval_requests request_row
    where request_row.id=(v_payload->>'approval_request_id')::uuid
      and request_row.workflow_id=v_workflow.id
      and request_row.workflow_generation=v_workflow.generation
      and request_row.request_generation=(v_payload->>'approval_request_generation')::integer
      and request_row.method='EMAIL'
      and request_row.state='PENDING'
    for update;
    if not found then
      raise exception 'MANAGER_APPROVAL_REQUEST_NOT_CANCELLABLE' using errcode='55000';
    end if;
    v_manager_retirement_result:=private._candidate_manager_mail_retire_v1(
      v_workflow.id,v_workflow.generation,array[v_approval.id],
      'APPROVAL_REQUEST_CANCELLED',p_now_utc
    );
    perform public.candidate_manager_email_route_receipt_retire_v1(
      v_workflow.id,v_approval.id,'APPROVAL_REQUEST_CANCELLED',p_now_utc
    );
    update public.candidate_approval_requests set
      state='CANCELLED',cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_approval.id;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and approval_request_id=v_approval.id
      and component_kind='MANAGER_SIGNATURE'
      and state<>'SUPERSEDED';
    if coalesce((v_manager_retirement_result->>'withdrawal_required')::boolean,false) then
      perform private._candidate_queue_mail_v1(
        private._candidate_manager_terminal_mail_payload_v1(
          v_payload->'manager_terminal_mail','WITHDRAWAL'
        )||jsonb_build_object(
          'payment_scope_json',jsonb_build_object(
            'candidate_mail_authority','MANAGER_APPROVAL_V1',
            'candidate_manager_mail_kind','WITHDRAWAL',
            'candidate_manager_workflow_id',v_workflow.id,
            'candidate_manager_workflow_generation',v_workflow.generation,
            'candidate_approval_request_id',v_approval.id,
            'candidate_approval_request_generation',v_approval.request_generation,
            'candidate_manager_template_version',(v_payload->'manager_terminal_mail'->>'manager_template_version')::bigint,
            'candidate_manager_template_sha256',v_payload->'manager_terminal_mail'->>'manager_template_sha256',
            'candidate_manager_submission_type',v_payload->'manager_terminal_mail'->>'manager_submission_type',
            'candidate_manager_mail_retired',false
          )
        ),v_approval.manager_email_normalized,
        'CANDIDATE_MANAGER_WITHDRAWAL_V1:'||v_approval.id::text||':'||v_workflow.generation::text,
        'candidate-manager-withdrawal:'||v_approval.id::text,v_workflow.id,p_now_utc
      );
      v_manager_withdrawal_count:=1;
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,
      'state','READY_FOR_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'manager_request_cancelled',true,
      'cancellation_reason',v_cancel_reason,
      'cancellation_reason_code',v_cancel_reason_code,
      'manager_withdrawal_count',v_manager_withdrawal_count,
      'manager_mail_retirement',v_manager_retirement_result,
      'claim_cancelled',false
    );
    update public.candidate_submission_workflows set
      state='READY_FOR_MANAGER_APPROVAL',route='ELECTRONIC',
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='CANCEL_MANAGER_HANDOFF' then
    if v_workflow.state<>'AWAITING_MANAGER_APPROVAL' or v_workflow.route<>'PHONE' then
      raise exception 'MANAGER_PHONE_HANDOFF_NOT_CANCELLABLE' using errcode='55000';
    end if;
    select * into v_approval from public.candidate_approval_requests
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and method='PHONE' and state='PENDING'
      and (not coalesce((v_payload->>'service_office_action')::boolean,false)
        or (id=(v_payload->>'approval_request_id')::uuid
          and request_generation=(v_payload->>'approval_request_generation')::integer))
    order by request_generation desc limit 1 for update;
    if not found then raise exception 'MANAGER_PHONE_HANDOFF_NOT_CANCELLABLE' using errcode='55000'; end if;
    update public.candidate_approval_requests set
      state='CANCELLED',cancelled_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where id=v_approval.id;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id and workflow_generation=v_workflow.generation
      and approval_request_id=v_approval.id and component_kind='MANAGER_SIGNATURE'
      and state<>'SUPERSEDED';
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','READY_FOR_MANAGER_APPROVAL','generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'handoff_cancelled',true);
    update public.candidate_submission_workflows set
      state='READY_FOR_MANAGER_APPROVAL',
      route=case when v_workflow.workflow_kind='DAILY' then 'PHONE' else 'ELECTRONIC' end,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id returning * into v_workflow;

  elsif v_action='MANAGER_PROVIDER_SUBMIT_PERMIT' then
    if not v_is_service_action then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_SUBMIT_PERMIT_SERVICE_REQUIRED'
        using errcode='42501';
    end if;
    v_provider_lease_token:=nullif(btrim(coalesce(v_payload->>'attempt_lease_token','')),'');
    if v_provider_lease_token is null
       or coalesce(v_payload->>'mail_outbox_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_PERMIT_INVALID' using errcode='22023';
    end if;
    select candidate_mail.* into v_manager_mail
    from public.mail_outbox candidate_mail
    where candidate_mail.id=(v_payload->>'mail_outbox_id')::uuid
      and candidate_mail.type='TIMESHEET_GENERAL'
      and candidate_mail.context_kind='CANDIDATE_WORKFLOW'
      and candidate_mail.context_id=v_workflow.id
      and candidate_mail.status='QUEUED'
      and candidate_mail.sent_at is null
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and upper(coalesce(candidate_mail.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no')
    for update;
    if not found then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_manager_mail_kind:=upper(coalesce(
      v_manager_mail.payment_scope_json->>'candidate_manager_mail_kind',''
    ));
    if v_manager_mail_kind not in ('INITIAL','REMINDER','RENEWAL','WITHDRAWAL','CANCELLATION')
       or coalesce(v_manager_mail.payment_scope_json->>'candidate_approval_request_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_manager_mail.payment_scope_json->>'candidate_approval_request_generation','')
          !~ '^[1-9][0-9]{0,8}$'
       or coalesce(v_manager_mail.payment_scope_json->>'candidate_manager_workflow_generation','')
          !~ '^[1-9][0-9]{0,8}$' then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    select request_row.* into v_approval
    from public.candidate_approval_requests request_row
    where request_row.id=(v_manager_mail.payment_scope_json->>'candidate_approval_request_id')::uuid
      and request_row.workflow_id=v_workflow.id
      and request_row.workflow_generation=
            (v_manager_mail.payment_scope_json->>'candidate_manager_workflow_generation')::integer
      and request_row.request_generation=
            (v_manager_mail.payment_scope_json->>'candidate_approval_request_generation')::integer
      and request_row.method='EMAIL'
      and request_row.manager_email_normalized=v_manager_mail."to"
    for update;
    if not found then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    if v_manager_mail_kind in ('INITIAL','REMINDER','RENEWAL') then
      select route_receipt.* into v_manager_route_receipt
      from public.candidate_manager_email_route_receipts route_receipt
      where route_receipt.route_receipt_id=v_approval.current_manager_route_receipt_id
        and route_receipt.workflow_id=v_workflow.id
        and route_receipt.approval_request_id=v_approval.id
        and route_receipt.request_generation=v_approval.request_generation
        and route_receipt.manager_token_hash_snapshot=v_approval.token_hash
        and route_receipt.state='CURRENT'
        and route_receipt.route_receipt_id::text=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_receipt_id'
        and route_receipt.manager_route_ticket_id::text=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_ticket_id'
        and route_receipt.route_revision::text=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_revision'
        and pg_catalog.encode(route_receipt.registration_receipt_sha256,'hex')=
              v_manager_mail.payment_scope_json->>'candidate_manager_route_registration_sha256'
      for update;
      if not found then
        raise exception 'CANDIDATE_MANAGER_ROUTE_RECEIPT_NOT_CURRENT' using errcode='40001';
      end if;
    end if;
    if v_manager_mail_kind in ('WITHDRAWAL','CANCELLATION') then
      if v_approval.state not in ('CANCELLED','SUPERSEDED','EXPIRED','REFUSED') then
        raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
      end if;
    elsif v_approval.state<>'PENDING' or v_approval.expires_at_utc<=p_now_utc
       or v_workflow.route<>'EMAIL' or v_workflow.state<>'AWAITING_MANAGER_APPROVAL'
       or v_workflow.generation<>v_approval.workflow_generation
       or v_approval.review_manifest_sha256 is distinct from v_workflow.review_manifest_sha256 then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_provider_permit_expires_at:=greatest(
      v_manager_mail.attempt_lease_expires_at_utc,p_now_utc+interval '15 minutes'
    );
    update public.mail_outbox candidate_mail
    set attempt_lease_expires_at_utc=v_provider_permit_expires_at
    where candidate_mail.id=v_manager_mail.id
      and candidate_mail.status='QUEUED' and candidate_mail.sent_at is null
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no');
    if not found then
      raise exception 'CANDIDATE_MANAGER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'approval_request_id',v_approval.id,'mail_outbox_id',v_manager_mail.id,
      'approval_request_generation',v_approval.request_generation,
      'approval_workflow_generation',v_approval.workflow_generation,
      'manager_mail_kind',v_manager_mail_kind,'provider_submit_permit',true,
      'provider_submit_permit_expires_at_utc',v_provider_permit_expires_at
    );

  elsif v_action='PAPER_PROVIDER_SUBMIT_PERMIT' then
    if not v_is_service_action or p_session_id is not null then
      raise exception 'CANDIDATE_PAPER_PROVIDER_SUBMIT_PERMIT_SERVICE_REQUIRED'
        using errcode='28000';
    end if;
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_provider_lease_token:=nullif(btrim(coalesce(v_payload->>'attempt_lease_token','')),'');
    v_paper_manifest_sha256:=lower(btrim(coalesce(
      v_payload->>'paper_return_manifest_sha256',''
    )));
    if v_paper_mail_id is null or v_provider_lease_token is null
       or v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$' then
      raise exception 'CANDIDATE_PAPER_PROVIDER_BINDING_INVALID' using errcode='22023';
    end if;
    if v_workflow.route<>'PAPER'
       or v_workflow.state<>'AWAITING_PAPER_RETURN'
       or v_workflow.paper_return_manifest_sha256 is null
       or encode(v_workflow.paper_return_manifest_sha256,'hex')<>v_paper_manifest_sha256
       or jsonb_typeof(v_workflow.paper_return_manifest_json->'pages') is distinct from 'array'
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') manifest_page
         where upper(coalesce(manifest_page->>'component_kind',''))='EXPENSE_SUMMARY'
            or upper(coalesce(manifest_page->>'page_key',''))='EXPENSE_SUMMARY'
       ) then
      raise exception 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if exists(
      select 1 from public.candidate_pending_expense_updates update_row
      where update_row.workflow_id=v_workflow.id
        and update_row.state in ('EDITING','RENDERING')
    ) then
      raise exception 'CANDIDATE_EXPENSE_UPDATE_IN_PROGRESS' using errcode='40001';
    end if;
    select candidate_mail.* into v_paper_mail
    from public.mail_outbox candidate_mail
    where candidate_mail.id=v_paper_mail_id
      and candidate_mail.type='TIMESHEET_QR'
      and candidate_mail.status='QUEUED'
      and candidate_mail.sent_at is null
      and candidate_mail.context_kind='timesheets'
      and candidate_mail.context_id=coalesce(
        v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id
      )
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and candidate_mail.payment_scope_json->>'candidate_mail_authority' in (
        'CANDIDATE_PAPER_V1','CANDIDATE_PAPER_PACK_EMAIL_V1'
      )
      and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in ('false','f','0','no')
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('true','t','1','yes')
      and lower(coalesce(candidate_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            in ('false','f','0','no')
      and jsonb_typeof(candidate_mail.attachments)='array'
      and jsonb_array_length(candidate_mail.attachments)>0
      and candidate_mail.attachments->0->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.attachments->0->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.attachments->0->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_provider_permit_expires_at:=greatest(
      v_paper_mail.attempt_lease_expires_at_utc,
      p_now_utc+interval '15 minutes'
    );
    update public.mail_outbox candidate_mail
    set attempt_lease_expires_at_utc=v_provider_permit_expires_at
    where candidate_mail.id=v_paper_mail.id
      and candidate_mail.status='QUEUED'
      and candidate_mail.sent_at is null
      and candidate_mail.attempt_lease_token=v_provider_lease_token
      and candidate_mail.attempt_lease_expires_at_utc>p_now_utc
      and lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in ('false','f','0','no');
    if not found then
      raise exception 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE' using errcode='40001';
    end if;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'state',v_workflow.state,
      'generation',v_workflow.generation,'mail_outbox_id',v_paper_mail.id,
      'provider_submit_permit',true,
      'provider_submit_permit_expires_at_utc',v_provider_permit_expires_at
    );

  elsif v_action in ('CANCEL','SUPERSEDE') then
    if v_workflow.state in ('CANCELLED','REJECTED','SUPERSEDED')
       or (v_action='SUPERSEDE' and v_workflow.state='FINALISED') then
      raise exception 'CANDIDATE_WORKFLOW_NOT_CANCELLABLE' using errcode='55000';
    end if;
    if v_action='CANCEL' then
      v_cancel_reason:=nullif(btrim(coalesce(v_payload->>'reason_note',v_payload->>'reason','')), '');
      v_cancel_reason_code:=nullif(upper(btrim(coalesce(v_payload->>'reason_code',''))),'');
      if v_cancel_reason is null then
        raise exception 'CANDIDATE_CANCELLATION_REASON_REQUIRED' using errcode='22023';
      end if;
      if length(v_cancel_reason)>1000 then
        raise exception 'CANDIDATE_CANCELLATION_REASON_INVALID' using errcode='22023';
      end if;
      v_audit_reason:=v_cancel_reason;
    end if;
    if v_workflow.route='PAPER'
       and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED') then
      v_paper_retirement_result:=private._candidate_paper_delivery_retire_set_v1(
        array[v_workflow.id],array[v_workflow.generation],
        case when v_action='CANCEL' then 'WORKFLOW_CANCELLED' else 'WORKFLOW_SUPERSEDED' end,
        p_now_utc
      );
      if not coalesce((v_paper_retirement_result->>'retired')::boolean,false)
         or not coalesce(
           (v_paper_retirement_result->>'qr_invalidation_proven')::boolean,false
         ) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=v_paper_retirement_result::text;
      end if;
    end if;
    select coalesce(array_agg(locked_request.id order by locked_request.id),array[]::uuid[])
    into v_manager_request_ids
    from (
      select request_row.id
      from public.candidate_approval_requests request_row
      where request_row.workflow_id=v_workflow.id
        and request_row.workflow_generation=v_workflow.generation
        and request_row.method='EMAIL'
        and request_row.state in ('PENDING','APPROVED')
      order by request_row.id
      for update
    ) locked_request;
    if cardinality(v_manager_request_ids)>0 then
      v_manager_retirement_result:=private._candidate_manager_mail_retire_v1(
        v_workflow.id,v_workflow.generation,v_manager_request_ids,
        case when v_action='CANCEL' then 'WORKFLOW_CANCELLED' else 'WORKFLOW_SUPERSEDED' end,
        p_now_utc
      );
      foreach v_manager_withdrawal_request_id in array v_manager_request_ids loop
        perform public.candidate_manager_email_route_receipt_retire_v1(
          v_workflow.id,v_manager_withdrawal_request_id,
          case when v_action='CANCEL' then 'WORKFLOW_CANCELLED' else 'WORKFLOW_SUPERSEDED' end,
          p_now_utc
        );
      end loop;
    end if;
    update public.candidate_approval_requests set
      state=case when v_action='CANCEL' then 'CANCELLED' else 'SUPERSEDED' end,
      cancelled_at_utc=case when v_action='CANCEL' then p_now_utc else cancelled_at_utc end,
      superseded_at_utc=case when v_action='SUPERSEDE' then p_now_utc else superseded_at_utc end,
      updated_at_utc=p_now_utc where workflow_id=v_workflow.id and state in ('PENDING','APPROVED');
    if v_action='CANCEL' and coalesce((v_manager_retirement_result->>'withdrawal_required')::boolean,false) then
      for v_manager_withdrawal_request_id in
        select value::uuid
        from jsonb_array_elements_text(coalesce(
          v_manager_retirement_result->'withdrawal_request_ids','[]'::jsonb
        )) value
      loop
        select request_row.* into v_approval
        from public.candidate_approval_requests request_row
        where request_row.id=v_manager_withdrawal_request_id
          and request_row.workflow_id=v_workflow.id
          and request_row.workflow_generation=v_workflow.generation
          and request_row.method='EMAIL'
          and request_row.state='CANCELLED';
        if found then
          perform private._candidate_queue_mail_v1(
            private._candidate_manager_terminal_mail_payload_v1(
              v_payload->'manager_terminal_mail','CANCELLATION'
            )||jsonb_build_object(
              'payment_scope_json',jsonb_build_object(
                'candidate_mail_authority','MANAGER_APPROVAL_V1',
                'candidate_manager_mail_kind','CANCELLATION',
                'candidate_manager_workflow_id',v_workflow.id,
                'candidate_manager_workflow_generation',v_workflow.generation,
                'candidate_approval_request_id',v_approval.id,
                'candidate_approval_request_generation',v_approval.request_generation,
                'candidate_manager_template_version',(v_payload->'manager_terminal_mail'->>'manager_template_version')::bigint,
                'candidate_manager_template_sha256',v_payload->'manager_terminal_mail'->>'manager_template_sha256',
                'candidate_manager_submission_type',v_payload->'manager_terminal_mail'->>'manager_submission_type',
                'candidate_manager_mail_retired',false
              )
            ),v_approval.manager_email_normalized,
            'CANDIDATE_MANAGER_CANCELLATION_V1:'||v_approval.id::text||':'||v_workflow.generation::text,
            'candidate-manager-cancellation:'||v_approval.id::text,v_workflow.id,p_now_utc
          );
          v_manager_withdrawal_count:=v_manager_withdrawal_count+1;
        end if;
      end loop;
    end if;
    update public.candidate_submission_components set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,
      review_render_state=case when review_render_state='NOT_REQUIRED' then review_render_state else 'SUPERSEDED' end,
      final_signed_render_state=case when final_signed_render_state='NOT_REQUIRED' then final_signed_render_state else 'SUPERSEDED' end
    where workflow_id=v_workflow.id and state not in ('SUPERSEDED','REJECTED');
    if v_action='CANCEL'
       and v_workflow.scope='WEEKLY'
       and v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED') then
      v_submission_withdrawal_reset:=private._candidate_weekly_withdrawal_reset_v1(
        v_workflow.id,v_cancel_reason,p_now_utc
      );
    elsif v_action='CANCEL'
       and v_workflow.scope='DAILY'
       and v_workflow.workflow_kind='DAILY' then
      v_submission_withdrawal_reset:=private._candidate_daily_receipt_reset_v1(
        v_environment,v_workflow.candidate_id,
        coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id),
        coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id),
        v_cancel_reason,v_workflow.candidate_id,'CANDIDATE_WITHDRAWN',p_now_utc
      );
    end if;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state',v_action||case when v_action='CANCEL' then 'LED' else 'D' end,
      'generation',v_next_generation,
      'cancellation_reason',case when v_action='CANCEL' then v_cancel_reason else null end,
      'cancellation_reason_code',case when v_action='CANCEL' then v_cancel_reason_code else null end,
      'submission_withdrawal_reset',v_submission_withdrawal_reset,
      'manager_withdrawal_count',v_manager_withdrawal_count,
      'manager_mail_retirement',v_manager_retirement_result);
    update public.candidate_submission_workflows set
      state=case when v_action='CANCEL' then 'CANCELLED' else 'SUPERSEDED' end,
      generation=v_next_generation,cancelled_at_utc=case when v_action='CANCEL' then p_now_utc else cancelled_at_utc end,
      last_mutation_idempotency_key=p_idempotency_key,last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc where id=v_workflow.id;

  elsif v_action='PAPER_PREPARE' then
    if nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
      raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023';
    end if;
    if v_workflow.state not in ('WORKER_SUBMITTED','AWAITING_PAPER_RETURN') then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
    if v_workflow.scope='DAILY' then
      raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
    end if;
    if v_workflow.workflow_kind='CONTRACT_EXPENSE'
       and v_workflow.target_timesheet_id is null then
      select week_row.* into v_anchor_week
      from public.contract_weeks week_row
      where week_row.timesheet_id=v_workflow.anchor_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date;
      if not found then raise exception 'CANDIDATE_WORKFLOW_ANCHOR_MISMATCH' using errcode='40001'; end if;
      if not coalesce((v_policy->>'paper_submission_enabled')::boolean,false) then
        raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000';
      end if;
    else
      v_route_authority:=private._candidate_route_family_v1(v_workflow.target_timesheet_id,v_workflow.contract_week_id);
      if not coalesce((v_route_authority->>'candidate_paper_submission_allowed')::boolean,false) then
        raise exception 'CANDIDATE_PAPER_ROUTE_NOT_ALLOWED' using errcode='55000',detail=v_route_authority::text;
      end if;
    end if;
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id and state='PENDING';

    select coalesce(jsonb_agg(jsonb_build_object(
      'page_key',case source_component.component_kind
        when 'MILEAGE_FORM' then 'MILEAGE_FORM:' else 'EXPENSE_EVIDENCE:' end
        ||coalesce(source_component.source_component_id,source_component.id)::text,
      'component_kind',source_component.component_kind,
      'expense_category',source_component.expense_category,
      'source_component_id',coalesce(source_component.source_component_id,source_component.id),
      'source_content_sha256',encode(source_component.source_content_sha256,'hex')
    ) order by source_component.component_no,source_component.id),'[]'::jsonb)
    into v_paper_source_pages
    from (
      select distinct on (
        component.component_kind,component.expense_category,component.document_role,
        coalesce(component.source_component_id,component.id),component.source_content_sha256
      ) component.*
      from public.candidate_submission_components component
      where component.workflow_id=v_workflow.id
        and component.workflow_generation=v_workflow.generation
        and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
        and component.state='IMMUTABLE'
        and component.source_content_sha256 is not null
      order by component.component_kind,component.expense_category,component.document_role,
        coalesce(component.source_component_id,component.id),component.source_content_sha256,
        component.component_no,component.id
    ) source_component;

    v_paper_mileage_only:=jsonb_array_length(v_paper_source_pages)>0
      and not exists(
        select 1
        from jsonb_array_elements(v_paper_source_pages) source_page
        where source_page->>'component_kind'<>'MILEAGE_FORM'
           or source_page->>'expense_category'<>'MILEAGE'
      );

    v_paper_manifest:=jsonb_build_object(
      'workflow_id',v_workflow.id,
      'workflow_generation',v_workflow.generation,
      'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
      'pages',
        case when v_workflow.workflow_kind<>'CONTRACT_EXPENSE'
          then jsonb_build_array(jsonb_build_object(
            'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
          else '[]'::jsonb end
        -- The expense summary is an internal, automatically regenerated aid;
        -- it is never a manager-decision or signed-return page.
        || '[]'::jsonb
        || v_paper_source_pages
    );
    update public.candidate_submission_workflows set
      state='AWAITING_PAPER_RETURN',route='PAPER',policy_snapshot_json=v_policy,
      policy_snapshot_sha256=private._candidate_sha256_jsonb_v1(v_policy),
      paper_return_manifest_json=v_paper_manifest,
      paper_return_manifest_sha256=private._candidate_sha256_jsonb_v1(v_paper_manifest),
      updated_at_utc=p_now_utc where id=v_workflow.id returning * into v_workflow;

    -- The existing QR/document/email authority is composed inside this
    -- transaction. A PAPER workflow is not accepted unless its exact held
    -- email operation exists and is bound to this frozen manifest.
    if v_workflow.workflow_kind='CONTRACT_EXPENSE'
       and v_workflow.target_timesheet_id is null then
      execute
        'select public.candidate_targetless_expense_paper_pack_enqueue_v1($1,$2,$3,$4,$5)'
        into v_paper_pack_result
        using v_environment,v_workflow.id,v_workflow.generation,
          p_idempotency_key||':paper-pack',p_now_utc;
    else
      execute
        'select public.timesheet_qr_send_enqueue_v1($1,$2,$3,$4,$5)'
        into v_paper_pack_result
        using v_paper_timesheet_id,v_paper_timesheet_id,null::uuid,
          p_idempotency_key||':paper-pack',p_now_utc;
    end if;

    if not coalesce((v_paper_pack_result->>'ok')::boolean,false)
       or not coalesce((v_paper_pack_result->>'queued')::boolean,false)
       or not coalesce((v_paper_pack_result->>'recipient_available')::boolean,false) then
      if coalesce(v_paper_pack_result->>'error_code','') in (
        'CANDIDATE_EMAIL_MISSING','CANDIDATE_EMAIL_OPTED_OUT','CANDIDATE_NOT_FOUND'
      ) then
        raise exception 'CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE'
          using errcode='55000',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_EMAIL_NOT_AVAILABLE',
            'cause',v_paper_pack_result->>'error_code'
          )::text;
      end if;
      raise exception 'CANDIDATE_PAPER_PACK_QUEUE_FAILED'
        using errcode='55000',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_PACK_QUEUE_FAILED',
          'cause',coalesce(v_paper_pack_result->>'error_code','UNKNOWN')
        )::text;
    end if;

    v_mail_id:=nullif(v_paper_pack_result->>'mail_outbox_id','')::uuid;
    if v_mail_id is null then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='55000';
    end if;
    perform 1
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.context_id=v_paper_timesheet_id
      and candidate_paper_mail.status='QUEUED'
      and candidate_paper_mail.attempt_lease_token is null
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =encode(v_workflow.paper_return_manifest_sha256,'hex')
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('false','f','0','no')
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            in ('true','t','1','yes')
      and candidate_paper_mail.payment_scope_json->>'mail_hold_reason'='CANDIDATE_PAPER_PACK_PENDING'
      and jsonb_typeof(candidate_paper_mail.attachments)='array'
      and jsonb_array_length(candidate_paper_mail.attachments)=0
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='55000';
    end if;

    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','AWAITING_PAPER_RETURN','generation',v_workflow.generation,
      'paper_return_manifest_sha256',encode(v_workflow.paper_return_manifest_sha256,'hex'),
      'paper_return_page_count',jsonb_array_length(v_paper_manifest->'pages'),
      'paper_pack',jsonb_build_object(
        'queued',true,
        'recipient_available',true,
        'mail_outbox_id',v_mail_id,
        'send_state',v_paper_pack_result->>'send_state',
        'document_operation_id',v_paper_pack_result->>'document_operation_id',
        'document_version_id',v_paper_pack_result->>'document_version_id',
        'document_version_status',v_paper_pack_result->>'document_version_status',
        'current_timesheet_id',v_paper_pack_result->>'current_timesheet_id',
        'current_version',v_paper_pack_result->'current_version'
      ));
    update public.candidate_submission_workflows set
      last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,
      updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='PAPER_PACK_ATTEMPT_CLAIM' then
    if not v_is_service_action or p_session_id is not null
       or not coalesce((v_payload->>'service_paper_pack_attempt')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER'
       or v_workflow.paper_return_manifest_sha256 is null then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_pack_attempt_token:=lower(btrim(coalesce(v_payload->>'paper_pack_attempt_token','')));
    v_paper_pack_operation_id:=nullif(btrim(coalesce(
      v_payload->>'paper_pack_operation_id',p_idempotency_key,''
    )), '');
    if v_paper_mail_id is null
       or v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_manifest_sha256<>encode(v_workflow.paper_return_manifest_sha256,'hex')
       or v_paper_pack_attempt_token !~ '^[0-9a-f]{64}$'
       or v_paper_pack_operation_id is null
       or length(v_paper_pack_operation_id)>200 then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_RECEIPT_INVALID' using errcode='22023';
    end if;
    select * into v_paper_mail
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_paper_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
         in ('true','t','1','yes') then
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'mail_outbox_id',v_paper_mail.id,'paper_pack_attempt_state','READY',
        'paper_pack_operation_id',v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id',
        'claim_acquired_new',false,
        'paper_pack_attempt_count',coalesce(
          nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_count','')::integer,0
        )
      );
    else
      v_paper_failure_class:=upper(coalesce(
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_failure_class',''
      ));
      if v_paper_failure_class='TERMINAL' then
        raise exception 'CANDIDATE_PAPER_PACK_FAILED_TERMINAL' using errcode='55000';
      end if;
      v_paper_pack_next_retry_at:=nullif(
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_next_retry_at_utc',''
      )::timestamptz;
      if v_paper_failure_class='RETRYABLE'
         and v_paper_pack_next_retry_at is not null and v_paper_pack_next_retry_at>p_now_utc then
        raise exception 'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE'
          using errcode='55000',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE',
            'next_retry_at_utc',v_paper_pack_next_retry_at
          )::text;
      end if;
      if nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token','') is not null
         and nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_expires_at_utc','')::timestamptz
               >p_now_utc then
        raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_IN_PROGRESS' using errcode='40001';
      end if;
      v_paper_pack_attempt_count:=coalesce(
        nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_count','')::integer,0
      )+1;
      v_paper_pack_attempt_expires_at:=p_now_utc+interval '10 minutes';
      update public.mail_outbox candidate_paper_mail
      set payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
        'candidate_paper_pack_attempt_token',v_paper_pack_attempt_token,
        'candidate_paper_pack_attempt_expires_at_utc',v_paper_pack_attempt_expires_at,
        'candidate_paper_pack_attempt_count',v_paper_pack_attempt_count,
        'candidate_paper_pack_last_attempted_at_utc',p_now_utc,
        'candidate_paper_pack_operation_id',v_paper_pack_operation_id,
        'candidate_paper_pack_operation_state','CLAIMED'
      )
      where candidate_paper_mail.id=v_paper_mail.id;
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'mail_outbox_id',v_paper_mail.id,'paper_pack_attempt_state','CLAIMED',
        'paper_pack_operation_id',v_paper_pack_operation_id,
        'claim_acquired_new',true,
        'paper_pack_attempt_token',v_paper_pack_attempt_token,
        'paper_pack_attempt_count',v_paper_pack_attempt_count,
        'paper_pack_attempt_expires_at_utc',v_paper_pack_attempt_expires_at
      );
    end if;

  elsif v_action='PAPER_PACK_MARK_FAILURE' then
    if not v_is_service_action or p_session_id is not null
       or not coalesce((v_payload->>'service_paper_pack_failure')::boolean,false) then
      raise exception 'CANDIDATE_PAPER_PACK_FAILURE_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER'
       or v_workflow.paper_return_manifest_sha256 is null then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_failure_code:=upper(btrim(coalesce(v_payload->>'error_code','')));
    v_paper_pack_operation_id:=nullif(btrim(coalesce(
      v_payload->>'paper_pack_operation_id',''
    )), '');
    if v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_manifest_sha256<>encode(v_workflow.paper_return_manifest_sha256,'hex')
       or v_paper_failure_code='' then
      raise exception 'CANDIDATE_PAPER_PACK_FAILURE_RECEIPT_INVALID' using errcode='22023';
    end if;
    if v_paper_failure_code not in (
      'CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT',
      'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
      'CANDIDATE_PAPER_R2_WRITE_TRANSIENT',
      'CANDIDATE_PAPER_DOCUMENT_FAILED',
      'CANDIDATE_PAPER_OUTBOX_NOT_READY',
      'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
      'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID',
      'CANDIDATE_PAPER_PACK_COMPONENT_MISSING',
      'CANDIDATE_PAPER_PACK_INCOMPLETE',
      'CANDIDATE_PAPER_PACK_IDENTITY_INVALID',
      'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT',
      'CANDIDATE_PAPER_OUTBOX_CONFLICT',
      'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
    ) then
      v_paper_failure_code:='CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED';
    end if;
    v_paper_failure_class:=case
      when v_paper_failure_code in (
        'CANDIDATE_PAPER_PACK_ASSEMBLY_TRANSIENT',
        'CANDIDATE_PAPER_SOURCE_READ_TRANSIENT',
        'CANDIDATE_PAPER_R2_WRITE_TRANSIENT'
      ) then 'RETRYABLE'
      when v_paper_failure_code in (
        'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
        'CANDIDATE_PAPER_DOCUMENT_FAILED',
        'CANDIDATE_PAPER_OUTBOX_NOT_READY',
        'CANDIDATE_PAPER_PACK_MEDIA_TYPE_INVALID',
        'CANDIDATE_PAPER_PACK_COMPONENT_MISSING',
        'CANDIDATE_PAPER_PACK_INCOMPLETE',
        'CANDIDATE_PAPER_PACK_IDENTITY_INVALID',
        'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT',
        'CANDIDATE_PAPER_OUTBOX_CONFLICT',
        'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
      ) then 'TERMINAL'
      else 'TERMINAL' end;
    v_paper_failure_retryable:=v_paper_failure_class='RETRYABLE';
    v_paper_pack_attempt_token:=lower(btrim(coalesce(v_payload->>'paper_pack_attempt_token','')));
    if v_paper_mail_id is null then
      v_response:=jsonb_build_object(
        'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
        'mail_outbox_id',null,'paper_pack_state','FAILED_TERMINAL',
        'failure_scope','WORKFLOW','failure_class','TERMINAL',
        'failure_code',v_paper_failure_code,'retryable',false,
        'paper_pack_operation_id',v_paper_pack_operation_id,
        'failure_contract_version','CANDIDATE_PAPER_PACK_FAILURE_V2'
      );
    else
    select * into v_paper_mail
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_paper_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    end if;
    if nullif(btrim(coalesce(v_paper_mail.attempt_lease_token,'')),'') is not null
       and v_paper_mail.attempt_lease_expires_at_utc>p_now_utc then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token','') is not null
       and lower(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token')
             is distinct from nullif(v_paper_pack_attempt_token,'') then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    if v_paper_pack_operation_id is not null
       and nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id','')
             is distinct from v_paper_pack_operation_id then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    v_paper_pack_attempt_count:=coalesce(
      nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_count','')::integer,0
    );
    v_paper_pack_next_retry_at:=case when v_paper_failure_retryable then p_now_utc+case
      when v_paper_pack_attempt_count<=1 then interval '1 minute'
      when v_paper_pack_attempt_count=2 then interval '5 minutes'
      when v_paper_pack_attempt_count=3 then interval '15 minutes'
      else interval '30 minutes' end else null end;
    update public.mail_outbox candidate_paper_mail
    set payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
      'candidate_paper_pack_ready',false,
      'candidate_paper_pack_retryable',v_paper_failure_retryable,
      'candidate_paper_pack_failure_class',v_paper_failure_class,
      'candidate_paper_pack_failure_code',v_paper_failure_code,
      'candidate_paper_pack_failure_contract_version','CANDIDATE_PAPER_PACK_FAILURE_V2',
      'candidate_paper_pack_failed_at_utc',p_now_utc,
      'candidate_paper_pack_next_retry_at_utc',v_paper_pack_next_retry_at,
      'candidate_paper_pack_attempt_token',null,
      'candidate_paper_pack_attempt_expires_at_utc',null,
      'candidate_paper_pack_operation_id',coalesce(
        v_paper_pack_operation_id,
        candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
      ),
      'candidate_paper_pack_operation_state',case when v_paper_failure_retryable
        then 'FAILED_RETRYABLE' else 'FAILED_TERMINAL' end
    )
    where candidate_paper_mail.id=v_paper_mail.id;
    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'mail_outbox_id',v_paper_mail.id,'paper_pack_state',
        case when v_paper_failure_retryable then 'FAILED_RETRYABLE' else 'FAILED_TERMINAL' end,
      'failure_class',v_paper_failure_class,'failure_code',v_paper_failure_code,
      'paper_pack_operation_id',coalesce(
        v_paper_pack_operation_id,
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
      ),
      'retryable',v_paper_failure_retryable,
      'failure_contract_version','CANDIDATE_PAPER_PACK_FAILURE_V2',
      'paper_pack_attempt_count',v_paper_pack_attempt_count,
      'next_retry_at_utc',v_paper_pack_next_retry_at
    );
    end if;
    update public.candidate_submission_workflows
    set last_mutation_idempotency_key=p_idempotency_key,
        last_mutation_response_json=v_response,updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation;

  elsif v_action='PAPER_PACK_RELEASE' then
    if not v_is_service_action or p_session_id is not null then
      raise exception 'CANDIDATE_PAPER_PACK_RELEASE_SERVICE_REQUIRED' using errcode='28000';
    end if;
    if v_workflow.state<>'AWAITING_PAPER_RETURN' or v_workflow.route<>'PAPER' then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256
       or jsonb_typeof(v_workflow.paper_return_manifest_json->'pages') is distinct from 'array'
       or exists(
         select 1
         from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') manifest_page
         where upper(coalesce(manifest_page->>'component_kind',''))='EXPENSE_SUMMARY'
            or upper(coalesce(manifest_page->>'page_key',''))='EXPENSE_SUMMARY'
       ) then
      raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000';
    end if;
    -- RELEASE is a separate transaction from ATTEMPT_CLAIM. Recompute the
    -- update hold while the workflow row is locked; rebind/abort take the
    -- same lock, so this truth cannot change before the outbox update.
    select exists(
      select 1 from public.candidate_pending_expense_updates update_row
      where update_row.workflow_id=v_workflow.id
        and update_row.update_mode='PAPER_REPLACEMENT'
        and update_row.current_workflow_generation=v_workflow.generation
        and update_row.state in ('EDITING','RENDERING')
    ) into v_paper_expense_update_active;

    v_paper_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
    v_paper_mail_id:=nullif(btrim(coalesce(v_payload->>'mail_outbox_id','')),'')::uuid;
    v_paper_manifest_sha256:=lower(btrim(coalesce(v_payload->>'paper_return_manifest_sha256','')));
    v_paper_pack_storage_key:=nullif(btrim(coalesce(v_payload->>'complete_pack_storage_key','')),'');
    v_paper_pack_sha256:=lower(btrim(coalesce(v_payload->>'complete_pack_sha256','')));
    v_paper_pack_media_type:=lower(btrim(coalesce(v_payload->>'complete_pack_media_type','')));
    v_paper_base_document_sha256:=lower(btrim(coalesce(v_payload->>'base_document_sha256','')));
    v_paper_branding_contract_sha256:=lower(btrim(coalesce(v_payload->>'branding_contract_sha256','')));
    v_paper_renderer_contract_version:=btrim(coalesce(v_payload->>'renderer_contract_version',''));
    v_paper_pack_attempt_token:=lower(btrim(coalesce(v_payload->>'paper_pack_attempt_token','')));
    v_paper_pack_operation_id:=nullif(btrim(coalesce(
      v_payload->>'paper_pack_operation_id',''
    )), '');
    begin
      v_paper_pack_byte_size:=(v_payload->>'complete_pack_byte_size')::bigint;
      v_paper_pack_page_count:=(v_payload->>'complete_pack_page_count')::integer;
    exception when others then
      raise exception 'CANDIDATE_PAPER_PACK_RECEIPT_INVALID' using errcode='22023';
    end;
    if v_paper_mail_id is null
       or v_paper_manifest_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_pack_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_base_document_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_branding_contract_sha256 !~ '^[0-9a-f]{64}$'
       or v_paper_pack_media_type<>'application/pdf'
       or coalesce(v_paper_pack_byte_size,0)<1
       or coalesce(v_paper_pack_page_count,0)<1
       or v_paper_renderer_contract_version=''
       or (v_paper_pack_attempt_token<>'' and v_paper_pack_operation_id is null)
       or v_paper_manifest_sha256<>encode(v_workflow.paper_return_manifest_sha256,'hex')
       or v_paper_renderer_contract_version<>coalesce(
         v_workflow.renderer_contract_version,
         v_workflow.immutable_submission_json#>>'{official_presentation,renderer_contract_version}'
       )
       or v_paper_branding_contract_sha256<>lower(coalesce(
         v_workflow.immutable_submission_json#>>'{official_presentation,branding,branding_contract_sha256}',''
       )) then
      raise exception 'CANDIDATE_PAPER_PACK_RECEIPT_INVALID' using errcode='22023';
    end if;
    v_paper_expected_storage_key:='candidate-app/'||lower(v_environment)||'/'
      ||v_workflow.id::text||'/'||v_workflow.generation::text||'/paper-pack/'
      ||v_paper_manifest_sha256||'-'||v_paper_base_document_sha256||'-'
      ||v_paper_branding_contract_sha256||'-'||v_paper_renderer_contract_version||'.pdf';
    if v_paper_pack_storage_key is distinct from v_paper_expected_storage_key then
      raise exception 'CANDIDATE_PAPER_PACK_IDENTITY_CONFLICT' using errcode='40001';
    end if;
    if jsonb_array_length(coalesce(v_workflow.paper_return_manifest_json->'pages','[]'::jsonb))
         <>v_paper_pack_page_count then
      raise exception 'CANDIDATE_PAPER_PACK_PAGE_COUNT_MISMATCH' using errcode='22023';
    end if;

    select count(*)::integer into v_paper_outbox_count
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.context_id=v_paper_timesheet_id
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256;
    if v_paper_outbox_count<>1 then
      raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
    end if;

    select * into v_paper_mail
    from public.mail_outbox candidate_paper_mail
    where candidate_paper_mail.id=v_paper_mail_id
      and candidate_paper_mail.type='TIMESHEET_QR'
      and candidate_paper_mail.context_kind='timesheets'
      and candidate_paper_mail.context_id=v_paper_timesheet_id
      and candidate_paper_mail.payment_scope_json->>'candidate_mail_authority'='CANDIDATE_PAPER_V1'
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_paper_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_paper_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =v_paper_manifest_sha256
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    end if;
    if v_paper_mail.status='FAILED' then
      raise exception 'CANDIDATE_PAPER_OUTBOX_FAILED' using errcode='55000';
    end if;
    if nullif(btrim(coalesce(v_paper_mail.attempt_lease_token,'')),'') is not null then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS' using errcode='40001';
    end if;
    if nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token','') is not null
       and lower(v_paper_mail.payment_scope_json->>'candidate_paper_pack_attempt_token')
             is distinct from nullif(v_paper_pack_attempt_token,'') then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    if v_paper_pack_operation_id is not null
       and nullif(v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id','')
             is distinct from v_paper_pack_operation_id then
      raise exception 'CANDIDATE_PAPER_PACK_ATTEMPT_STALE' using errcode='40001';
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;

    v_paper_pack_attachment:=jsonb_build_array(jsonb_build_object(
      'r2_key',v_paper_pack_storage_key,
      'filename',case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        then 'Expense_' else 'Timesheet_' end
        ||coalesce(v_workflow.week_ending_date::text,v_paper_timesheet_id::text)||'.pdf',
      'content_type','application/pdf','sha256',v_paper_pack_sha256,
      'size_bytes',v_paper_pack_byte_size,'page_count',v_paper_pack_page_count,
      'candidate_workflow_id',v_workflow.id,
      'candidate_workflow_generation',v_workflow.generation,
      'paper_return_manifest_sha256',v_paper_manifest_sha256
    ));

    if v_paper_mail.status in ('QUEUED','SENT')
       and lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in ('true','t','1','yes')
       and v_paper_mail.attachments=v_paper_pack_attachment
       and v_paper_mail.payment_scope_json->>'candidate_complete_pack_storage_key'=v_paper_pack_storage_key
       and lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_complete_pack_sha256',''))=v_paper_pack_sha256
       and v_paper_mail.payment_scope_json->>'candidate_complete_pack_size_bytes'=v_paper_pack_byte_size::text
       and v_paper_mail.payment_scope_json->>'candidate_complete_pack_page_count'=v_paper_pack_page_count::text then
      v_paper_release_idempotent:=true;
    elsif v_paper_mail.status='SENT' then
      raise exception 'CANDIDATE_PAPER_OUTBOX_ALREADY_SENT' using errcode='55000';
    elsif v_paper_mail.status<>'QUEUED'
       or lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
            not in ('false','f','0','no')
       or lower(coalesce(v_paper_mail.payment_scope_json->>'mail_held_until_pdf_rendered','false'))
            not in ('true','t','1','yes')
       or v_paper_mail.payment_scope_json->>'mail_hold_reason'<>'CANDIDATE_PAPER_PACK_PENDING'
       or jsonb_typeof(v_paper_mail.attachments)<>'array'
       or jsonb_array_length(v_paper_mail.attachments)<>0 then
      raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
    else
      update public.mail_outbox candidate_paper_mail
      set attachments=v_paper_pack_attachment,
          scheduled_for_utc=case when v_paper_expense_update_active
            then 'infinity'::timestamptz else p_now_utc end,
          next_attempt_at_utc=case when v_paper_expense_update_active
            then 'infinity'::timestamptz else p_now_utc end,
          payment_scope_json=candidate_paper_mail.payment_scope_json||jsonb_build_object(
            'candidate_paper_pack_ready',true,
            'candidate_paper_pack_retryable',false,
            'candidate_paper_pack_failure_class',null,
            'candidate_paper_pack_failure_code',null,
            'candidate_paper_pack_failure_contract_version',null,
            'candidate_paper_pack_failed_at_utc',null,
            'candidate_paper_pack_next_retry_at_utc',null,
            'candidate_paper_pack_attempt_token',null,
            'candidate_paper_pack_attempt_expires_at_utc',null,
            'candidate_paper_pack_operation_id',coalesce(
              v_paper_pack_operation_id,
              candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
            ),
            'candidate_paper_pack_operation_state','READY',
            'mail_held_until_pdf_rendered',v_paper_expense_update_active,
            'mail_delayed_for_pdf_render',v_paper_expense_update_active,
            'mail_hold_reason',case when v_paper_expense_update_active
              then 'CANDIDATE_EXPENSE_UPDATE_PENDING' end,
            'candidate_expense_update_hold',v_paper_expense_update_active,
            'candidate_complete_pack_storage_key',v_paper_pack_storage_key,
            'candidate_complete_pack_sha256',v_paper_pack_sha256,
            'candidate_complete_pack_size_bytes',v_paper_pack_byte_size,
            'candidate_complete_pack_page_count',v_paper_pack_page_count,
            'candidate_complete_pack_media_type','application/pdf',
            'candidate_complete_pack_ready_at_utc',p_now_utc,
            'candidate_complete_pack_base_document_sha256',v_paper_base_document_sha256,
            'candidate_complete_pack_branding_contract_sha256',v_paper_branding_contract_sha256,
            'candidate_complete_pack_renderer_contract_version',v_paper_renderer_contract_version
          )
      where candidate_paper_mail.id=v_paper_mail.id
        and candidate_paper_mail.status='QUEUED'
        and candidate_paper_mail.attempt_lease_token is null
        and lower(coalesce(candidate_paper_mail.payment_scope_json->>'candidate_paper_pack_ready','false'))
              in ('false','f','0','no')
        and lower(coalesce(candidate_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
              in ('false','f','0','no');
      if not found then
        raise exception 'CANDIDATE_PAPER_OUTBOX_NOT_READY' using errcode='40001';
      end if;
    end if;

    if not v_paper_expense_update_active then
      insert into public.candidate_notifications(
        account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
        template_key,template_params,deep_link_json,state,push_state,dedupe_key,created_at_utc
      ) values (
        v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,v_paper_timesheet_id,
        'PAPER_PACK_READY','resubmission_required','candidate-paper-pack-ready-v1',
        jsonb_build_object('page_count',v_paper_pack_page_count,'workflow_generation',v_workflow.generation),
        jsonb_build_object('type','paper_pack','timesheet_id',v_paper_timesheet_id,
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation),
        'UNREAD','PENDING',
        'CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'
          ||v_workflow.generation::text||':'||v_paper_manifest_sha256,
        p_now_utc
      ) on conflict(dedupe_key) do nothing
      returning id into v_paper_notification_id;
      if v_paper_notification_id is null then
        select notification.id into v_paper_notification_id
        from public.candidate_notifications notification
        where notification.dedupe_key='CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'
          ||v_workflow.generation::text||':'||v_paper_manifest_sha256;
      end if;
    end if;

    update public.candidate_submission_workflows
    set updated_at_utc=p_now_utc
    where id=v_workflow.id
      and generation=v_workflow.generation
      and state='AWAITING_PAPER_RETURN'
      and route='PAPER';
    if not found then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;

    v_response:=jsonb_build_object(
      'ok',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'state',v_workflow.state,'timesheet_id',v_paper_timesheet_id,
      'mail_outbox_id',v_paper_mail.id,'notification_id',v_paper_notification_id,
      'paper_return_manifest_sha256',v_paper_manifest_sha256,
      'paper_pack_operation_id',coalesce(
        v_paper_pack_operation_id,
        v_paper_mail.payment_scope_json->>'candidate_paper_pack_operation_id'
      ),
      'complete_pack_storage_key',v_paper_pack_storage_key,
      'complete_pack_sha256',v_paper_pack_sha256,
      'complete_pack_byte_size',v_paper_pack_byte_size,
      'complete_pack_page_count',v_paper_pack_page_count,
      'paper_pack_held_for_expense_update',v_paper_expense_update_active,
      'idempotent_replay',v_paper_release_idempotent
    );

  elsif v_action='PAPER_RETURN' then
    if v_workflow.state<>'AWAITING_PAPER_RETURN' then
      raise exception 'CANDIDATE_WORKFLOW_TRANSITION_INVALID' using errcode='55000';
    end if;
    if v_workflow.paper_return_manifest_sha256 is null
       or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
          is distinct from v_workflow.paper_return_manifest_sha256 then
      raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000';
    end if;
    -- The claimed mail lease is the provider-submit permit. PAPER return and
    -- every authority-changing transition lock the same exact delivery row,
    -- so provider submission cannot be authorised and then invalidated before
    -- its external call. An active permit is a controlled retryable conflict.
    select count(*)::integer into v_paper_outbox_count
    from public.mail_outbox candidate_mail
    where candidate_mail.type='TIMESHEET_QR'
      and candidate_mail.context_kind='timesheets'
      and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =encode(v_workflow.paper_return_manifest_sha256,'hex');
    if v_paper_outbox_count<>1 then
      raise exception 'CANDIDATE_PAPER_OUTBOX_CONFLICT' using errcode='40001';
    end if;
    select candidate_mail.* into v_paper_mail
    from public.mail_outbox candidate_mail
    where candidate_mail.type='TIMESHEET_QR'
      and candidate_mail.context_kind='timesheets'
      and candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
            =encode(v_workflow.paper_return_manifest_sha256,'hex')
    for update;
    if nullif(btrim(coalesce(v_paper_mail.attempt_lease_token,'')),'') is not null
       and (v_paper_mail.attempt_lease_expires_at_utc is null
         or v_paper_mail.attempt_lease_expires_at_utc>p_now_utc) then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'mail_outbox_id',v_paper_mail.id
        )::text;
    end if;
    if lower(coalesce(v_paper_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
         in ('true','t','1','yes') then
      raise exception 'CANDIDATE_PAPER_WORKFLOW_STALE' using errcode='40001';
    end if;
    if exists(
      select 1
      from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
      where (
        select count(*)
        from public.candidate_submission_components returned_page
        where returned_page.workflow_id=v_workflow.id
          and returned_page.workflow_generation=v_workflow.generation
          and returned_page.component_kind='SIGNED_RETURN'
          and returned_page.paper_return_page_key=expected_page->>'page_key'
          and returned_page.state='IMMUTABLE'
          and returned_page.source_content_sha256 is not null
      )<>1
    ) or exists(
      select 1
      from public.candidate_submission_components returned_page
      where returned_page.workflow_id=v_workflow.id
        and returned_page.workflow_generation=v_workflow.generation
        and returned_page.component_kind='SIGNED_RETURN'
        and returned_page.state='IMMUTABLE'
        and not exists(
          select 1
          from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
          where expected_page->>'page_key'=returned_page.paper_return_page_key
        )
    ) then raise exception 'CANDIDATE_PAPER_RETURN_INCOMPLETE' using errcode='22023'; end if;
    v_response:=jsonb_build_object('ok',true,'workflow_id',v_workflow.id,
      'state','RECEIVED','generation',v_workflow.generation);
    update public.candidate_submission_workflows set
      state='RECEIVED',last_mutation_idempotency_key=p_idempotency_key,
      last_mutation_response_json=v_response,updated_at_utc=p_now_utc
    where id=v_workflow.id returning * into v_workflow;

  elsif v_action='MARK_READ' then
    update public.candidate_notifications set state='READ',read_at_utc=p_now_utc
    where id=nullif(v_payload->>'notification_id','')::uuid
      and account_id=v_account_id and state='UNREAD';
    v_response:=jsonb_build_object('ok',true,
      'notification_id',nullif(v_payload->>'notification_id','')::uuid,'state','READ');
  else
    raise exception 'CANDIDATE_WORKFLOW_ACTION_INVALID'
      using errcode='22023',detail=jsonb_build_object(
        'code','CANDIDATE_WORKFLOW_ACTION_INVALID','action',v_action)::text;
  end if;

  perform private._candidate_audit_v1('candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_WORKFLOW_'||v_action,
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    v_response,v_audit_reason,null,p_idempotency_key,p_now_utc);
  if v_mutation_request_sha256 is not null then
    perform private._candidate_workflow_mutation_receipt_v1(
      v_workflow.id,p_idempotency_key,v_mutation_request_sha256,v_action,
      v_mutation_channel,v_mutation_actor_identity,v_response,p_now_utc
    );
  end if;
  return v_response;
exception
  when unique_violation then
    get stacked diagnostics v_constraint_name=constraint_name;
    if v_constraint_name='candidate_submission_components_source_sha256_uq' then
      raise exception 'CANDIDATE_EVIDENCE_BYTES_ALREADY_USED' using errcode='23505';
    elsif v_constraint_name in (
      'candidate_submission_components_hours_review_uq',
      'candidate_submission_components_required_ordinal_uq'
    ) then
      raise exception 'MANAGER_REVIEW_DOCUMENT_STALE' using errcode='23505';
    elsif v_constraint_name='candidate_submission_workflows_one_active_expense_uq' then
      raise exception 'CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE' using errcode='23505';
    elsif v_constraint_name='candidate_submission_workflows_account_idempotency_uq' then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode=case when v_is_rejected_resubmission then '55000' else '40001' end;
    elsif v_constraint_name='candidate_submission_workflows_replacement_source_uq' then
      raise exception 'CANDIDATE_REJECTED_WORKFLOW_ALREADY_REPLACED' using errcode='55000';
    elsif v_constraint_name='candidate_submission_components_paper_return_page_uq' then
      raise exception 'CANDIDATE_PAPER_RETURN_PAGE_DUPLICATE' using errcode='23505';
    end if;
    raise;
end;
$function$;
create or replace function private.candidate_notification_visible_v1(
  p_notification public.candidate_notifications, p_now_utc timestamptz,
  p_restored_state text default null
) returns boolean language plpgsql stable security definer set search_path = ''
as $function$
declare
  v_state text:=coalesce(p_restored_state,p_notification.state);
  v_generation uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_component_generation integer;
begin
  if v_state not in ('READ','UNREAD') then return false; end if;
  -- Apply retention and explicit retirement even when evaluating a deleted
  -- notification's prior state for Undo. Lifecycle branches cannot bypass them.
  if p_notification.created_at_utc <= p_now_utc
    - (case when v_state='READ' then interval '30 days' else interval '90 days' end)
    or p_notification.deep_link_json->'obsolete'='true'::jsonb then return false; end if;
  if p_notification.event_type='CHECK_ONLY_TIMESHEET_REMINDER' then
    -- Purpose-bound in-app reminder: preserve common retention/Undo semantics
    -- above, then delegate the same pure native-action/submission observation.
    if pg_catalog.jsonb_typeof(p_notification.template_params->'scope') is distinct from 'object'
      or pg_catalog.jsonb_typeof(p_notification.template_params->'native_action') is distinct from 'object'
      or p_notification.candidate_id is null then return false; end if;
    if not exists(select 1 from public.candidate_app_global_membership_links link
      join public.candidate_app_accounts account on account.id=link.account_id
      join public.weekly_source_groups source_group on source_group.environment=account.environment and source_group.active
      where link.membership_id::text=p_notification.template_params->>'membership_id'
        and link.account_id=p_notification.account_id and link.candidate_id=p_notification.candidate_id
        and link.state='ACTIVE' and account.status='ACTIVE'
        and source_group.id::text=p_notification.template_params#>>'{scope,source_group_id}') then return false; end if;
    begin
      return exists(select 1 from private.weekly_source_check_only_candidate_reminder_subject_v1(
        p_notification.template_params->'scope',p_notification.template_params->>'action_id',p_notification.candidate_id) subject
        where subject->'current_native_action'='true'::jsonb and subject->'actual_submission_present'='false'::jsonb
          and subject->'native_action'=p_notification.template_params->'native_action');
    exception when invalid_text_representation or invalid_parameter_value then return false; end;
  end if;
  if p_notification.deep_link_json->>'destination'='WEEKLY_SOURCE_REQUEST' then
    begin v_generation:=(p_notification.deep_link_json->>'request_id')::uuid;
    exception when invalid_text_representation then return false; end;
    return exists(
      select 1 from public.weekly_candidate_outreach_generations generation
      where generation.id=v_generation and generation.candidate_id=p_notification.candidate_id
        and generation.state='ACTIVE'
        and private.weekly_source_candidate_generation_current_v1(generation.id)
        and (
          (generation.request_kind='SUBMIT_TIMESHEET' and exists(
            select 1 from public.weekly_timesheet_submission_requests request
            join public.weekly_timesheet_submission_request_memberships membership on membership.submission_request_id=request.id
            where request.candidate_cohort_id=generation.candidate_cohort_id
              and request.source_cycle_id=generation.source_cycle_id
              and request.candidate_id=generation.candidate_id
              and request.request_generation=generation.generation_number
              and request.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED') and membership.state='WAITING'
          )) or
          (generation.request_kind='CHECK_HOURS' and p_now_utc<=generation.deadline_at_utc and exists(
            select 1 from public.weekly_candidate_outreach_memberships membership
            join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
            where membership.candidate_generation_id=generation.id and membership.state='ACTIONABLE'
              and incident.state='OPEN' and not private.weekly_source_covered_hours_incident_v1(incident.id)
          ))
        )
    );
  end if;
  -- Category rejection remains actionable even when its now-empty parent was
  -- finalised/cancelled. Exact component lineage takes precedence over parent state.
  if p_notification.event_type='OFFICE_REJECTED'
     and p_notification.template_params->>'resubmission_scope'='EXPENSE_CATEGORY' then
    begin
      select * into v_component from public.candidate_expense_components component
      where component.expense_component_id=(p_notification.template_params->>'expense_component_id')::uuid
        and component.workflow_id=p_notification.workflow_id;
    exception when invalid_text_representation then return false; end;
    if not found then return false; end if;
    select event.component_generation into v_component_generation
    from public.candidate_expense_component_events event
    where event.expense_component_id=v_component.expense_component_id
      and event.event_type='OFFICE_REJECTED' and event.occurred_at_utc=p_notification.created_at_utc
    order by event.component_generation desc limit 1;
    -- Missing historical metadata is not permission to discard an unresolved rejection.
    if v_component_generation is not null and v_component.component_generation<>v_component_generation then return false; end if;
    if v_component.lifecycle_state<>'OFFICE_REJECTED' then return false; end if;
    if exists(select 1 from public.candidate_expense_operations operation
      where operation.action_code='RESUBMIT_EXPENSE_CATEGORY' and operation.state='COMMITTED'
        and operation.candidate_id=p_notification.candidate_id
        and operation.result_json->>'source_expense_component_id'=v_component.expense_component_id::text
        and operation.result_json->>'source_component_generation'=v_component_generation::text
    ) then return false; end if;
    return true;
  end if;
  if p_notification.workflow_id is not null then
    select * into v_workflow from public.candidate_submission_workflows workflow
    where workflow.id=p_notification.workflow_id and workflow.account_id=p_notification.account_id
      and workflow.candidate_id=p_notification.candidate_id;
    if not found then return false; end if;
    if p_notification.event_type='PAPER_PACK_READY' then
      return v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN'
        and p_notification.template_params->>'workflow_generation'=v_workflow.generation::text
        and v_workflow.paper_return_manifest_sha256 is not null
        and p_notification.dedupe_key like '%:'||pg_catalog.encode(v_workflow.paper_return_manifest_sha256,'hex');
    end if;
    if p_notification.event_type in ('OFFICE_REJECTED','MANAGER_REFUSED') then
      return v_workflow.state in ('REJECTED','REFUSED')
        and not private._candidate_rejection_replaced_v1(v_workflow.id);
    end if;
    if v_workflow.state in ('CANCELLED','EXPIRED','SUPERSEDED')
      and p_notification.event_type not in ('CLAIM_CANCELLED','EXPENSE_CLAIM_CANCELLED','CLAIM_WITHDRAWN','EXPENSE_WITHDRAWN')
      then return false; end if;
  end if;
  return true;
end;
$function$;
create or replace function private.weekly_source_checklist_legacy_hold_review_v1(
 p_timesheet_id uuid,p_scope jsonb,p_actor_user_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $legacy_review$
declare frame jsonb;frame_after jsonb;facts jsonb;witness jsonb;proof text;worker text;work_date date;
begin
 frame:=private.weekly_source_checklist_legacy_read_frame_v1(p_timesheet_id,p_scope,p_actor_user_id);
 facts:=jsonb_build_object('legacy_read_frame',frame);
 if frame->'known' is distinct from 'true'::jsonb then
  return jsonb_build_object('review_required',null,'native_facts',facts);end if;
 -- The exact native frame already requires actual submission, the LEGACY
 -- owner, one coherent current booking/TF, authorisation, dated permission,
 -- ordinary ownership and the genuine canonical native financial family.
 if frame#>'{native_facts,current_financial,pay_on_hold}' is distinct from 'true'::jsonb then
  return jsonb_build_object('review_required',false,'native_facts',facts);end if;
 work_date:=case when frame#>>'{native_facts,root,sheet_scope}'='DAILY'
  then (frame#>>'{native_facts,actual_submission,work_date}')::date
  else (p_scope->>'week_ending')::date end;
 if work_date is null or work_date not between (p_scope->>'week_ending')::date-6 and (p_scope->>'week_ending')::date then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_LEGACY_REVIEW_DATE_UNPROVED' using errcode='55000';end if;
 select coalesce(nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),'Worker') into worker
  from public.candidates c where c.id=(frame#>>'{native_facts,actual_submission,candidate_id}')::uuid;
 if worker is null then raise exception 'WEEKLY_SOURCE_CHECKLIST_LEGACY_REVIEW_SUBJECT_UNPROVED' using errcode='55000';end if;
 frame_after:=private.weekly_source_checklist_legacy_read_frame_v1(p_timesheet_id,p_scope,p_actor_user_id);
 if frame is distinct from frame_after then
  raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';end if;
 facts:=facts||jsonb_build_object('legacy_read_frame_again',frame_after,'worker',worker,'work_date',work_date,
  'native_basis','NATIVE_CURRENT_TSFIN_PAY_ON_HOLD','amount_basis','UNAVAILABLE','withholding_known',false);
 proof:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CHECKLIST_LEGACY_HOLD_REVIEW_V1',facts),'hex');
 witness:=jsonb_build_object('owner','private.weekly_source_checklist_legacy_read_frame_v1','actor_user_id',p_actor_user_id,
  'scope',p_scope,'timesheet_id',p_timesheet_id,'financial_id',frame->'financial_id',
  'native_basis','NATIVE_CURRENT_TSFIN_PAY_ON_HOLD','amount_basis','UNAVAILABLE','counts_for_row_admission',false,
  'read_proof_hash',proof);
 return jsonb_build_object('review_required',true,'worker',worker,'work_date',work_date,
  'sheet_scope',frame#>'{native_facts,root,sheet_scope}','native_legacy_review_witness',witness,
  'status',jsonb_build_object('text','Decision needed','tone','warning','subtext','Native pay hold needs review','subtone','warning'),
  'hold',jsonb_build_object('holds_pay',null,'holds_pay_scope',null,'withheld_amount_pence',null,
   'holds_invoice',null,'pending_decision',null,'authority',jsonb_build_object('owner','ORDINARY','hash',proof,'withholding_known',false)),
  'native_facts',facts);
end;
$legacy_review$;
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
alter function private.bpay_next_initial_tsfin_certificate_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_initial_tsfin_bound_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_v1(uuid),private.bpay_next_initial_tsfin_bind_v1(uuid),
  private.bpay_next_initial_tsfin_bound_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_invoice_hold_release_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_invoice_hold_release_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_invoice_hold_release_atomic_v1(jsonb) to service_role;
alter function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) owner to postgres;
revoke all on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) from public,anon,authenticated;
grant execute on function public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamptz,jsonb) to service_role;
alter function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz) owner to postgres;
revoke all on function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamptz) to service_role;
alter function private.candidate_notification_visible_v1(public.candidate_notifications,timestamptz,text) owner to postgres;
revoke all on function private.candidate_notification_visible_v1(public.candidate_notifications,timestamptz,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_checklist_legacy_hold_review_v1(uuid,jsonb,uuid) owner to postgres;
revoke all on function private.weekly_source_checklist_legacy_hold_review_v1(uuid,jsonb,uuid) from public,anon,authenticated,service_role;
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
('public.weekly_source_invoice_hold_release_atomic_v1(jsonb)','v',true),
('public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)','v',true),
('public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)','v',true),
('private.candidate_notification_visible_v1(public.candidate_notifications,timestamp with time zone,text)','s',true),
('private.weekly_source_checklist_legacy_hold_review_v1(uuid,jsonb,uuid)','s',true),
('private.bpay_next_initial_tsfin_certificate_v1(uuid)','v',true),
('private.bpay_next_initial_tsfin_bound_assert_v1(uuid)','v',true)
) expected(signature,volatility,security_definer) loop
 select q.* into p from pg_catalog.pg_proc q where q.oid=pg_catalog.to_regprocedure(r.signature);
 if not found or p.provolatile::text is distinct from r.volatility or p.prosecdef is distinct from r.security_definer then raise exception 'CHECKLIST_CONSTRUCTOR_CATALOG_MISMATCH: %',r.signature using errcode='55000';end if;end loop;end;$finite_constructor$;
