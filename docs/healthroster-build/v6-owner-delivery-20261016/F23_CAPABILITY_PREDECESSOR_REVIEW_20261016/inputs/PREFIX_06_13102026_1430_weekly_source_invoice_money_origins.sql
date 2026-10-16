-- HealthRoster v2 F5/F6. No existing money is rewritten. NULL source columns
-- preserve the installed NHSP/ordinary/signed/legacy-HealthRoster path.
\set ON_ERROR_STOP on
begin;

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
commit;
