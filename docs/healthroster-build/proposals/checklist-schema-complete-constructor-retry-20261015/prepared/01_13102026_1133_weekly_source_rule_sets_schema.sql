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

