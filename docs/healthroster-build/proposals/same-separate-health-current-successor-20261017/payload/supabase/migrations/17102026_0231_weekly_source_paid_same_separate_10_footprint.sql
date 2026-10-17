\set ON_ERROR_STOP on
begin;
-- Actual first footprint only. Existing data is retained with NORMAL defaults.
-- No old paid-choice proposal or audit namespace is silently adopted.
do $footprint$
begin
  if exists(select 1 from (values
      ('private.weekly_source_office_row_choices','pay_effect'),
      ('private.weekly_source_office_row_choices','paid_overlap_target_work_event_id'),
      ('private.weekly_source_office_row_choices','paid_overlap_inputs_json'),
      ('private.weekly_source_office_row_choices','paid_overlap_inputs_sha256'),
      ('public.weekly_source_row_resolutions','pay_effect'),
      ('public.weekly_source_row_resolutions','paid_overlap_choice_id'),
      ('public.weekly_source_final_snapshot_lines','pay_effect'),
      ('public.weekly_source_final_snapshot_lines','paid_overlap_choice_id'),
      ('public.weekly_source_billing_movements','pay_effect'),
      ('public.weekly_source_billing_movements','invoice_only_snapshot_line_id'),
      ('public.weekly_source_final_revisions','paid_overlap_choice_census'),
      ('public.weekly_source_state_transitions','pay_basis_kind'),
      ('public.weekly_source_state_transitions','paid_match_source_rows')) expected(relation_name,column_name)
    join pg_catalog.pg_attribute a on a.attrelid=expected.relation_name::regclass
      and a.attname=expected.column_name and not a.attisdropped)
    or exists(select 1 from public.audit_events where action='WEEKLY_SOURCE_PAID_SHIFT_MATCH_CONFIRMED')
    or exists(select 1 from pg_catalog.pg_trigger where tgname in (
      'weekly_source_paid_choice_revision_update_guard','weekly_source_paid_choice_revision_delete_guard',
      'weekly_source_paid_choice_revision_truncate_guard') and not tgisinternal)
    or pg_catalog.to_regclass('public.weekly_source_payable_snapshot_event_uq') is not null
    or pg_catalog.to_regclass('public.weekly_source_invoice_only_snapshot_row_uq') is not null
    or pg_catalog.to_regclass('public.weekly_source_invoice_only_snapshot_movement_uq') is not null then
    raise exception 'WEEKLY_SOURCE_PAID_MATCH_RESERVED_FOOTPRINT_NOT_EMPTY' using errcode='55000';
  end if;
end;
$footprint$;

-- One-time SOURCE proposal only. Existing immutable per-row choices remain
-- the single decision store; no cash/pay/settlement record is rewritten.
alter table private.weekly_source_office_row_choices
  add column pay_effect text not null default 'NORMAL',
  add column paid_overlap_target_work_event_id uuid references public.weekly_work_events(id) on delete restrict,
  add column paid_overlap_inputs_json jsonb,
  add column paid_overlap_inputs_sha256 bytea,
  add constraint weekly_source_office_paid_overlap_effect_check check (
    pay_effect in ('NORMAL','NONE')
    and ((paid_overlap_target_work_event_id is null and paid_overlap_inputs_json is null
      and paid_overlap_inputs_sha256 is null and pay_effect='NORMAL')
      or (paid_overlap_target_work_event_id is not null
        and jsonb_typeof(paid_overlap_inputs_json) is not distinct from 'object'
        and paid_overlap_inputs_sha256 is not null and octet_length(paid_overlap_inputs_sha256)=32
        and candidate_id is not null and client_id is not null and contract_id is not null
        and match_reason is not null and char_length(btrim(match_reason)) between 1 and 1000
        and ((pay_effect='NONE' and not separate_shift and work_event_id is not null and work_event_id=paid_overlap_target_work_event_id)
          or (pay_effect='NORMAL' and separate_shift and work_event_id is null))))
  );

-- Projection stores the effect rederived from that exact immutable choice,
-- never a Worker-supplied paid/no-pay Boolean.
alter table public.weekly_source_row_resolutions
  add column pay_effect text not null default 'NORMAL',
  add column paid_overlap_choice_id bigint references private.weekly_source_office_row_choices(id) on delete restrict,
  add constraint weekly_source_resolution_pay_effect_check check (
    pay_effect in ('NORMAL','NONE') and (pay_effect='NORMAL' or paid_overlap_choice_id is not null));

-- Invoice-only companion snapshots retain the true Source row and economic
-- result. Only NORMAL is a payable snapshot; the pay basis stays independent.
alter table public.weekly_source_final_snapshot_lines
  add column pay_effect text not null default 'NORMAL',
  add column paid_overlap_choice_id bigint references private.weekly_source_office_row_choices(id) on delete restrict,
  add constraint weekly_source_snapshot_pay_effect_check check (
    pay_effect in ('NORMAL','NONE') and (pay_effect='NORMAL' or paid_overlap_choice_id is not null));
alter table public.weekly_source_billing_movements
  add column pay_effect text not null default 'NORMAL',
  add column invoice_only_snapshot_line_id uuid references public.weekly_source_final_snapshot_lines(id) on delete restrict,
  add constraint weekly_source_movement_pay_effect_check check (
    pay_effect in ('NORMAL','NONE') and (pay_effect='NORMAL' or
      (total_pay_ex_vat=0 and (invoice_only_snapshot_line_id is not null
        or (nhsp_upload_row_id is not null and transition_id is null and invoice_only_snapshot_line_id is null
          and expense_authority_generation_id is null and self_bill_report_row_id is null and vat_rounding_revision_id is null
          and source_profile_kind='NHSP_TRUST_BACKING_REPORT' and source_line_kind='NHSP_PHYSICAL_POSITIVE'
          and movement_role='POSITIVE' and correction_unit_id is null and prior_movement_id is null
          and coalesce(canonical_pay_vector_json->>'total_pence'='0'
            and (canonical_pay_vector_json->>'paid_minutes')::numeric=0
            and (canonical_pay_vector_json->>'break_minutes')::numeric=0
            and canonical_pay_vector_json->'hours'='{"day":0,"night":0,"sat":0,"sun":0,"bh":0}'::jsonb,false))))));

alter table public.weekly_source_final_revisions
  add column paid_overlap_choice_census jsonb not null default '{}'::jsonb,
  add constraint weekly_source_paid_choice_census_object check (jsonb_typeof(paid_overlap_choice_census)='object');
alter table public.weekly_source_state_transitions
  add column pay_basis_kind text not null default 'CURRENT_SOURCE_SNAPSHOT',
  add column paid_match_source_rows jsonb,
  add constraint weekly_source_paid_match_basis_check check (
    (pay_basis_kind='CURRENT_SOURCE_SNAPSHOT' and paid_match_source_rows is null)
    or (pay_basis_kind='RETAINED_PAID_MATCH' and outcome='NO_CHANGE' and new_present and previous_present
      and new_snapshot_line_id=previous_snapshot_line_id and jsonb_typeof(paid_match_source_rows)='array'
      and jsonb_array_length(paid_match_source_rows)>0));

-- The original snapshot unique is discovered by its exact ordered attributes,
-- not a guessed provider/truncated name. NONE companions may share the same
-- real work event with the one payable NORMAL snapshot. No old row changes.
do $migration$
declare v_name text;v_count integer;v_keys smallint[];
begin
  select array_agg(attnum::smallint order by ord) into v_keys
  from unnest(array['final_revision_id','work_event_id']) with ordinality key(name,ord)
  join pg_catalog.pg_attribute a on a.attrelid='public.weekly_source_final_snapshot_lines'::regclass
    and a.attname=key.name and not a.attisdropped;
  select count(*),min(conname::text) into v_count,v_name from pg_catalog.pg_constraint
    where conrelid='public.weekly_source_final_snapshot_lines'::regclass and contype='u' and conkey=v_keys;
  if v_count<>1 or cardinality(v_keys)<>2 then
    raise exception 'WEEKLY_SOURCE_PAYABLE_SNAPSHOT_UNIQUE_UNBOUND' using errcode='55000';end if;
  execute format('alter table public.weekly_source_final_snapshot_lines drop constraint %I',v_name);
end;
$migration$;
create unique index weekly_source_payable_snapshot_event_uq
  on public.weekly_source_final_snapshot_lines(final_revision_id,work_event_id) where pay_effect='NORMAL';
create unique index weekly_source_invoice_only_snapshot_row_uq
  on public.weekly_source_final_snapshot_lines(final_revision_id,upload_row_id) where pay_effect='NONE';

-- One-time Source proposal; exact original two coupled CHECK owners retained.
alter table public.weekly_source_billing_movements
  drop constraint weekly_source_billing_movements_five_origins,
  drop constraint weekly_source_billing_movements_invoice_contract,
  drop constraint weekly_source_billing_movements_invoice_only_zero_pay;
alter table public.weekly_source_billing_movements
  add constraint weekly_source_billing_movements_five_origins check (
    (transition_id is not null)::integer+(nhsp_upload_row_id is not null)::integer
    +(expense_authority_generation_id is not null)::integer+(self_bill_report_row_id is not null)::integer
    +(vat_rounding_revision_id is not null)::integer+(invoice_only_snapshot_line_id is not null)::integer=1),
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
    and ((expense_authority_generation_id is not null or (invoice_only_snapshot_line_id is not null and pay_effect='NONE'
      and source_line_kind='SOURCE_FIXED_EXPENSE'))=(source_line_kind='SOURCE_FIXED_EXPENSE'))
    and ((expense_authority_generation_id is not null or (invoice_only_snapshot_line_id is not null and pay_effect='NONE'
      and source_line_kind='SOURCE_FIXED_EXPENSE'))=(movement_role in ('EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT')))
    and (movement_role<>'STANDALONE_NEGATIVE' or (source_line_kind='SELF_BILL_REPORT_LINE' and invoice_presentation_charge_pence<0))
    and (movement_role<>'CORRECTION_UNWIND' or (source_line_kind='SOURCE_INVOICE_CORRECTION' and transition_id is not null))
    and (source_line_kind<>'SOURCE_REBILL_LINE' or (movement_role='POSITIVE' and (transition_id is not null
      or (invoice_only_snapshot_line_id is not null and pay_effect='NONE'))))
    and (invoice_only_snapshot_line_id is null or (pay_effect='NONE'
      and ((source_line_kind='SOURCE_REBILL_LINE' and movement_role='POSITIVE')
        or (source_line_kind='SOURCE_FIXED_EXPENSE' and movement_role='EXPENSE_POSITIVE'))
      and source_profile_kind in ('HEALTHROSTER_ACTUAL_ROWS','GENERIC_COMPLETE_SNAPSHOT')))
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
    (pay_effect<>'NONE' and source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION'))
    or coalesce((pay_effect='NONE' and source_line_kind='SOURCE_FIXED_EXPENSE' and total_pay_ex_vat=0
      and canonical_pay_vector_json->>'schema_version'='WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1'
      and canonical_pay_vector_json->>'kind'='PAY' and (canonical_pay_vector_json->>'source_expense_pence')::bigint=0),false)
    or coalesce((total_pay_ex_vat=0 and canonical_pay_vector_json->>'total_pence'='0'
      and (canonical_pay_vector_json->>'paid_minutes')::numeric=0
      and (canonical_pay_vector_json->>'break_minutes')::numeric=0
      and canonical_pay_vector_json->'hours'='{"day":0,"night":0,"sat":0,"sun":0,"bh":0}'::jsonb),false)
  );
create unique index weekly_source_invoice_only_snapshot_movement_uq on public.weekly_source_billing_movements(invoice_only_snapshot_line_id,source_line_kind) where invoice_only_snapshot_line_id is not null;
alter table public.weekly_source_invoice_presentation_lines drop constraint weekly_source_invoice_presentation_lines_origin_kind_check,
  add constraint weekly_source_invoice_presentation_lines_origin_kind_check check (origin_kind in ('GENERIC_TRANSITION','HEALTHROSTER_TRANSITION','NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE','SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SELF_BILL_REPORT_STANDALONE_CREDIT','SOURCE_INVOICE_ONLY_SNAPSHOT'));

commit;
