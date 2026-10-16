begin;
do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
-- REVIEW PROPOSAL ONLY. Not installed, not executed, native qualification NOT_RUN.
-- Attach only before the exact immutable 13102026_1430 body in one existing atomic unit.
-- Keep the existing writer/identity admission, ledger and outer transaction.
lock table public.weekly_source_charge_checks, public.weekly_source_billing_movements,
  public.weekly_source_invoice_presentation_lines in access exclusive mode;
do $health_f23_money_predecessor$
declare previous_path text;
begin
  previous_path:=pg_catalog.current_setting('search_path');
  perform pg_catalog.set_config('search_path','pg_catalog, public',true);
  if exists (
    with expected(name, definition, is_validated, is_deferrable, is_deferred) as (values
      ('weekly_source_charge_checks_check2', 'CHECK (comparison_result = ''UNVERIFIABLE''::text OR source_commission_pence IS NOT NULL AND source_total_cost_pence IS NOT NULL AND source_shift_charge_pence IS NOT NULL AND calculated_segment_charge_pence IS NOT NULL AND source_charge_difference_pence IS NOT NULL)', true, false, false),
      ('weekly_source_charge_checks_check3', 'CHECK (comparison_result <> ''EXACT''::text OR source_charge_difference_pence = 0)', true, false, false),
      ('weekly_source_charge_checks_check4', 'CHECK (comparison_result <> ''SOURCE_ROUNDING_EQUIVALENT''::text OR (source_charge_difference_pence = ANY (ARRAY[1::bigint, ''-1''::integer::bigint])) AND (source_shift_charge_pence > 0 AND calculated_segment_charge_pence > 0 OR source_shift_charge_pence < 0 AND calculated_segment_charge_pence < 0))', true, false, false),
      ('weekly_source_charge_checks_check5', 'CHECK ((comparison_result <> ALL (ARRAY[''MISMATCH''::text, ''ZERO_SOURCE_CHARGE''::text, ''UNVERIFIABLE''::text])) OR (phase_severity = ANY (ARRAY[''PROVISIONAL_WARNING''::text, ''FINALISATION_BLOCKER''::text])))', true, false, false),
      ('weekly_source_charge_checks_check6', 'CHECK ((comparison_result = ANY (ARRAY[''MISMATCH''::text, ''ZERO_SOURCE_CHARGE''::text, ''UNVERIFIABLE''::text])) OR blocker_code IS NULL)', true, false, false),
      ('weekly_source_charge_checks_check7', 'CHECK (comparison_result <> ''ZERO_SOURCE_CHARGE''::text OR source_commission_pence = 0 AND source_total_cost_pence = 0 AND source_shift_charge_pence = 0 AND calculated_segment_charge_pence <> 0 AND blocker_code IS NULL)', true, false, false),
      ('weekly_source_charge_checks_check8', 'CHECK (row_sign_kind <> ''POSITIVE''::text OR comparison_result = ''UNVERIFIABLE''::text OR source_commission_pence >= 0 AND source_total_cost_pence >= 0 AND (source_shift_charge_pence > 0 AND calculated_segment_charge_pence > 0 OR comparison_result = ''ZERO_SOURCE_CHARGE''::text AND source_shift_charge_pence = 0 AND calculated_segment_charge_pence > 0))', true, false, false),
      ('weekly_source_charge_checks_check9', 'CHECK (row_sign_kind <> ''FULL_NEGATIVE''::text OR comparison_result = ''UNVERIFIABLE''::text OR source_commission_pence <= 0 AND source_total_cost_pence <= 0 AND source_shift_charge_pence < 0 AND calculated_segment_charge_pence < 0)', true, false, false),
      ('weekly_source_charge_checks_comparison_profile_version_check', 'CHECK (comparison_profile_version = ''NHSP_TWO_COMPONENT_PENCE_V1''::text)', true, false, false),
      ('weekly_source_charge_checks_comparison_result_check', 'CHECK (comparison_result = ANY (ARRAY[''EXACT''::text, ''SOURCE_ROUNDING_EQUIVALENT''::text, ''MISMATCH''::text, ''ZERO_SOURCE_CHARGE''::text, ''UNVERIFIABLE''::text]))', true, false, false),
      ('weekly_source_charge_checks_row_sign_kind_check', 'CHECK (row_sign_kind = ANY (ARRAY[''POSITIVE''::text, ''FULL_NEGATIVE''::text]))', true, false, false)
    ), actual as (
      select c.conname::text name, pg_catalog.pg_get_constraintdef(c.oid,true) definition,
        c.convalidated is_validated,c.condeferrable is_deferrable,c.condeferred is_deferred
      from pg_catalog.pg_constraint c
      where c.conrelid='public.weekly_source_charge_checks'::pg_catalog.regclass and c.contype='c'
        and (pg_catalog.pg_get_constraintdef(c.oid) like '%comparison_profile_version%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%comparison_result%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%row_sign_kind%')
    )
    select 1 from expected e full join actual a using (name)
    where (e.definition,e.is_validated,e.is_deferrable,e.is_deferred)
      is distinct from (a.definition,a.is_validated,a.is_deferrable,a.is_deferred)
  ) then raise exception 'HEALTH_F23_WEEKLY_SOURCE_CHARGE_CHECKS_PREDECESSOR_CHANGED' using errcode='55000'; end if;
  if exists (
    with expected(name, definition, is_validated, is_deferrable, is_deferred) as (values
      ('weekly_source_billing_movements_check', 'CHECK (((transition_id IS NOT NULL)::integer + (nhsp_upload_row_id IS NOT NULL)::integer + (expense_authority_generation_id IS NOT NULL)::integer) = 1)', true, false, false),
      ('weekly_source_billing_movements_check1', 'CHECK ((source_profile_kind = ''NHSP_TRUST_BACKING_REPORT''::text) = (nhsp_upload_row_id IS NOT NULL))', true, false, false),
      ('weekly_source_billing_movements_check2', 'CHECK ((expense_authority_generation_id IS NOT NULL) = (source_line_kind = ''SOURCE_FIXED_EXPENSE''::text))', true, false, false),
      ('weekly_source_billing_movements_check3', 'CHECK ((expense_authority_generation_id IS NOT NULL) = (movement_role = ANY (ARRAY[''EXPENSE_POSITIVE''::text, ''EXPENSE_REVERSAL''::text, ''EXPENSE_REPLACEMENT''::text])))', true, false, false),
      ('weekly_source_billing_movements_check4', 'CHECK (source_profile_kind <> ''NHSP_TRUST_BACKING_REPORT''::text OR source_validation_charge_pence IS NOT NULL AND invoice_presentation_charge_pence = source_validation_charge_pence AND (price_check_result = ANY (ARRAY[''EXACT''::text, ''SOURCE_ROUNDING_EQUIVALENT''::text, ''ACCEPTED_DISPARITY''::text, ''ACCEPTED_ZERO''::text])))', true, false, false),
      ('weekly_source_billing_movements_check5', 'CHECK ((price_check_result = ANY (ARRAY[''ACCEPTED_DISPARITY''::text, ''ACCEPTED_ZERO''::text])) = (charge_acceptance_id IS NOT NULL))', true, false, false),
      ('weekly_source_billing_movements_check6', 'CHECK (source_line_kind <> ''SOURCE_FIXED_EXPENSE''::text OR source_validation_charge_pence IS NOT NULL AND invoice_presentation_charge_pence = source_validation_charge_pence AND price_check_result = ''NOT_APPLICABLE''::text)', true, false, false),
      ('weekly_source_billing_movements_check7', 'CHECK (source_profile_kind = ''NHSP_TRUST_BACKING_REPORT''::text OR (price_check_result = ANY (ARRAY[''NOT_APPLICABLE''::text, ''EXACT''::text, ''SOURCE_ROUNDING_EQUIVALENT''::text])) AND charge_acceptance_id IS NULL)', true, false, false),
      ('weekly_source_billing_movements_movement_role_check', 'CHECK (movement_role = ANY (ARRAY[''POSITIVE''::text, ''REVERSAL''::text, ''REPLACEMENT''::text, ''NET_DIFFERENCE_PRESENTATION''::text, ''EXPENSE_POSITIVE''::text, ''EXPENSE_REVERSAL''::text, ''EXPENSE_REPLACEMENT''::text]))', true, false, false),
      ('weekly_source_billing_movements_price_check_result_check', 'CHECK (price_check_result = ANY (ARRAY[''NOT_APPLICABLE''::text, ''EXACT''::text, ''SOURCE_ROUNDING_EQUIVALENT''::text, ''ACCEPTED_DISPARITY''::text, ''ACCEPTED_ZERO''::text]))', true, false, false),
      ('weekly_source_billing_movements_source_line_kind_check', 'CHECK (source_line_kind = ANY (ARRAY[''SOURCE_ORDINARY''::text, ''SOURCE_REPLACEMENT''::text, ''GENERATED_HISTORICAL_REVERSAL''::text, ''NON_NHSP_DIFFERENCE''::text, ''NHSP_PHYSICAL_POSITIVE''::text, ''NHSP_PHYSICAL_FULL_NEGATIVE''::text, ''SOURCE_FIXED_EXPENSE''::text]))', true, false, false),
      ('weekly_source_billing_movements_source_profile_kind_check', 'CHECK (source_profile_kind = ANY (ARRAY[''GENERIC_COMPLETE_SNAPSHOT''::text, ''NHSP_TRUST_BACKING_REPORT''::text, ''HEALTHROSTER_ACTUAL_ROWS''::text]))', true, false, false)
    ), actual as (
      select c.conname::text name, pg_catalog.pg_get_constraintdef(c.oid,true) definition,
        c.convalidated is_validated,c.condeferrable is_deferrable,c.condeferred is_deferred
      from pg_catalog.pg_constraint c
      where c.conrelid='public.weekly_source_billing_movements'::pg_catalog.regclass and c.contype='c'
        and (pg_catalog.pg_get_constraintdef(c.oid) like '%transition_id%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%nhsp_upload_row_id%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%expense_authority_generation_id%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%source_profile_kind%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%source_line_kind%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%movement_role%'
        or pg_catalog.pg_get_constraintdef(c.oid) like '%price_check_result%')
    )
    select 1 from expected e full join actual a using (name)
    where (e.definition,e.is_validated,e.is_deferrable,e.is_deferred)
      is distinct from (a.definition,a.is_validated,a.is_deferrable,a.is_deferred)
  ) then raise exception 'HEALTH_F23_WEEKLY_SOURCE_BILLING_MOVEMENTS_PREDECESSOR_CHANGED' using errcode='55000'; end if;
  if (select count(*) from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass
      and c.contype='c'
      and c.conkey=array[(select a.attnum from pg_catalog.pg_attribute a
        where a.attrelid=c.conrelid and a.attname='price_check_result' and not a.attisdropped)]::smallint[]
      and pg_catalog.pg_get_constraintdef(c.oid) like '%NOT_APPLICABLE%')<>1
    or not exists (select 1 from pg_catalog.pg_constraint c
      where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass
        and c.contype='c' and c.conname='weekly_source_invoice_presentation_lin_price_check_result_check'
        and c.conkey=array[(select a.attnum from pg_catalog.pg_attribute a
          where a.attrelid=c.conrelid and a.attname='price_check_result' and not a.attisdropped)]::smallint[]
        and pg_catalog.pg_get_constraintdef(c.oid,true)='CHECK (price_check_result = ANY (ARRAY[''NOT_APPLICABLE''::text, ''EXACT''::text, ''SOURCE_ROUNDING_EQUIVALENT''::text, ''ACCEPTED_DISPARITY''::text, ''ACCEPTED_ZERO''::text]))'
        and c.convalidated and not c.condeferrable and not c.condeferred) then
    raise exception 'HEALTH_F23_PRICE_ENUM_PREDECESSOR_CHANGED' using errcode='55000';
  end if;
  perform pg_catalog.set_config('search_path',previous_path,true);
end;
$health_f23_money_predecessor$;

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
alter table public.weekly_source_invoice_hold_releases OWNER TO CURRENT_USER;
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


do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id)
    values('supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql','931765daf31387fd5e5de7c9a9f6944ab1bf1c030a6ea7a4b9f2c3f0cc79a40c','HEALTHROSTER_F23_PREBODY_SOURCE_ONLY_20261016');
commit;