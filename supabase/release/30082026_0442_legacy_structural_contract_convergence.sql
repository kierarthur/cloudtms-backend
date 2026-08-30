-- LEGACY_UPGRADE-only structural convergence from the sealed LIVE lineage to
-- the exact current managed-TEST relation, trigger, RLS and provider-default
-- contract. All changes are catalog/schema changes; application rows are never
-- deleted or rewritten. Numeric narrowing is allowed only after a lossless
-- preflight against every existing value.

begin;
set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_structural_prestate$
declare
  v_count integer;
  v_schema_owner text;
begin
  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname in ('public','private') and c.relkind in ('r','p','v','m','S');
  -- The contract owns 229 application relations. The four private release-
  -- control tables are deliberately outside the application contract.
  if v_count<>233 then
    raise exception 'LEGACY_STRUCTURAL_RELATION_COUNT_DRIFT:%',v_count;
  end if;

  select pg_catalog.pg_get_userbyid(n.nspowner) into v_schema_owner
  from pg_catalog.pg_namespace n where n.nspname='public';
  if v_schema_owner not in (current_user,'pg_database_owner') then
    raise exception 'LEGACY_PUBLIC_SCHEMA_OWNER_DRIFT:%',v_schema_owner;
  end if;

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_attribute a
  where a.attrelid='public.banking_pay_workbench_sessions'::pg_catalog.regclass
    and a.attname in ('replacement_idempotency_key','replacement_session_id')
    and a.attnum>0 and not a.attisdropped;
  if v_count<>0 then
    raise exception 'LEGACY_REPLACEMENT_SESSION_COLUMNS_ALREADY_PRESENT:%',v_count;
  end if;

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_attribute a
  where a.attrelid='public.banking_pay_workbench_candidate_delta_projection_runs'::pg_catalog.regclass
    and a.attname in (
      'candidate_state_updated','projected_row_count','projection_fingerprint',
      'shadow_compare_enforced','shadow_compare_required','shadow_compare_status',
      'write_cursor_json','write_phase','written_line_work_count',
      'written_preview_count','written_source_count'
    ) and a.attnum>0 and not a.attisdropped;
  if v_count<>0 then
    raise exception 'LEGACY_DELTA_PROJECTION_COLUMNS_ALREADY_PRESENT:%',v_count;
  end if;

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_attribute a
  where a.attrelid='public.settings_defaults'::pg_catalog.regclass
    and a.attname in (
      'banking_pay_auto_unwind_terminal_no_money',
      'banking_pay_workbench_clone_rebase_budget_ms',
      'banking_pay_workbench_clone_rebase_units_per_job',
      'banking_pay_workbench_cron_clone_rebase_units_per_job',
      'banking_pay_workbench_nudge_clone_rebase_units_per_job',
      'default_workbench_settings'
    ) and a.attnum>0 and not a.attisdropped;
  if v_count<>0 then
    raise exception 'LEGACY_SETTINGS_COLUMNS_ALREADY_PRESENT:%',v_count;
  end if;

  if exists (
    select 1 from public.pay_batch_items
    where frozen_target_amount_ex_vat is not null
      and (frozen_target_amount_ex_vat<>pg_catalog.round(frozen_target_amount_ex_vat,2)
        or pg_catalog.abs(frozen_target_amount_ex_vat)>=10000000000)
  ) or exists (
    select 1 from public.pay_batch_items
    where frozen_target_amount_inc_vat is not null
      and (frozen_target_amount_inc_vat<>pg_catalog.round(frozen_target_amount_inc_vat,2)
        or pg_catalog.abs(frozen_target_amount_inc_vat)>=10000000000)
  ) or exists (
    select 1 from public.pay_finance_case_components
    where source_amount<>pg_catalog.round(source_amount,2)
       or pg_catalog.abs(source_amount)>=10000000000
       or remaining_source_amount<>pg_catalog.round(remaining_source_amount,2)
       or pg_catalog.abs(remaining_source_amount)>=10000000000
  ) then
    raise exception 'LEGACY_NUMERIC_12_2_CONVERSION_WOULD_CHANGE_DATA';
  end if;
end
$legacy_structural_prestate$;

alter table public.banking_pay_workbench_sessions
  add column replacement_idempotency_key text,
  add column replacement_session_id uuid;
alter table public.banking_pay_workbench_sessions
  add constraint bpay_workbench_sessions_replacement_session_id_fkey
  foreign key (replacement_session_id)
  references public.banking_pay_workbench_sessions(id) on delete set null;
create index idx_bpay_workbench_sessions_replacement_session_id
  on public.banking_pay_workbench_sessions (replacement_session_id)
  where replacement_session_id is not null;
create unique index ux_bpay_workbench_sessions_replacement_idempotency_key
  on public.banking_pay_workbench_sessions (replacement_idempotency_key)
  where replacement_idempotency_key is not null;

alter table public.banking_pay_workbench_candidate_delta_projection_runs
  add column candidate_state_updated boolean not null default false,
  add column projected_row_count integer not null default 0,
  add column projection_fingerprint text,
  add column shadow_compare_enforced boolean not null default false,
  add column shadow_compare_required boolean not null default false,
  add column shadow_compare_status text,
  add column write_cursor_json jsonb not null default '{}'::jsonb,
  add column write_phase text,
  add column written_line_work_count integer not null default 0,
  add column written_preview_count integer not null default 0,
  add column written_source_count integer not null default 0;
alter table public.banking_pay_workbench_candidate_delta_projection_runs
  add constraint chk_bpay_delta_runs_non_negative_counts
    check (projected_row_count>=0 and written_source_count>=0
      and written_line_work_count>=0 and written_preview_count>=0),
  add constraint chk_bpay_delta_runs_write_cursor_object
    check (pg_catalog.jsonb_typeof(write_cursor_json)='object'::text);

alter table public.settings_defaults
  add column banking_pay_auto_unwind_terminal_no_money boolean not null default false,
  add column banking_pay_workbench_clone_rebase_budget_ms integer not null default 3000,
  add column banking_pay_workbench_clone_rebase_units_per_job integer not null default 100,
  add column banking_pay_workbench_cron_clone_rebase_units_per_job integer not null default 250,
  add column banking_pay_workbench_nudge_clone_rebase_units_per_job integer not null default 100,
  add column default_workbench_settings jsonb;
alter table public.settings_defaults
  add constraint settings_defaults_bpw_clone_rebase_budget_ms_chk
    check (banking_pay_workbench_clone_rebase_budget_ms>=500
      and banking_pay_workbench_clone_rebase_budget_ms<=30000),
  add constraint settings_defaults_bpw_clone_rebase_units_chk
    check (banking_pay_workbench_clone_rebase_units_per_job>=1
      and banking_pay_workbench_clone_rebase_units_per_job<=1000),
  add constraint settings_defaults_bpw_cron_clone_rebase_units_chk
    check (banking_pay_workbench_cron_clone_rebase_units_per_job>=1
      and banking_pay_workbench_cron_clone_rebase_units_per_job<=2000),
  add constraint settings_defaults_bpw_nudge_clone_rebase_units_chk
    check (banking_pay_workbench_nudge_clone_rebase_units_per_job>=1
      and banking_pay_workbench_nudge_clone_rebase_units_per_job<=1000);

alter table public.pay_batch_items
  alter column frozen_target_amount_ex_vat type numeric(12,2)
    using frozen_target_amount_ex_vat::numeric(12,2),
  alter column frozen_target_amount_inc_vat type numeric(12,2)
    using frozen_target_amount_inc_vat::numeric(12,2);
drop view public.v_finance_cases_register;

alter table public.pay_finance_case_components
  alter column source_amount type numeric(12,2) using source_amount::numeric(12,2),
  alter column remaining_source_amount type numeric(12,2)
    using remaining_source_amount::numeric(12,2);

create view public.v_finance_cases_register
  with (security_invoker=true)
as
WITH reservation_rollup AS (
         SELECT r.finance_case_id,
            round(COALESCE(sum(
                CASE
                    WHEN r.status = ANY (ARRAY['RESERVED'::text, 'COMMITTED'::text]) THEN r.reserved_amount
                    ELSE 0::numeric
                END), 0::numeric), 2) AS active_reserved_amount,
            round(COALESCE(sum(
                CASE
                    WHEN r.status = 'RESERVED'::text THEN r.reserved_amount
                    ELSE 0::numeric
                END), 0::numeric), 2) AS reserved_amount,
            round(COALESCE(sum(
                CASE
                    WHEN r.status = 'COMMITTED'::text THEN r.reserved_amount
                    ELSE 0::numeric
                END), 0::numeric), 2) AS committed_amount,
            round(COALESCE(sum(
                CASE
                    WHEN r.status = 'SETTLED'::text THEN r.reserved_amount
                    ELSE 0::numeric
                END), 0::numeric), 2) AS settled_amount,
            round(COALESCE(sum(
                CASE
                    WHEN r.status = 'RELEASED'::text THEN r.reserved_amount
                    ELSE 0::numeric
                END), 0::numeric), 2) AS released_amount,
            count(*) FILTER (WHERE r.status = ANY (ARRAY['RESERVED'::text, 'COMMITTED'::text]))::integer AS active_reservation_count,
            max(r.created_at_utc) AS latest_reservation_created_at_utc,
            max(r.committed_at_utc) AS latest_committed_at_utc,
            max(r.settled_at_utc) AS latest_settled_at_utc,
            max(r.released_at_utc) AS latest_released_at_utc
           FROM pay_advance_reservations r
          GROUP BY r.finance_case_id
        ), latest_remittance AS (
         SELECT DISTINCT ON (x.finance_case_id) x.finance_case_id,
            x.pay_batch_id,
            x.pay_date,
            x.remittance_sent_at_utc,
            x.remittance_trigger_status,
            x.last_remittance_error
           FROM ( SELECT COALESCE(pbi.finance_case_id, pa_fallback.id) AS finance_case_id,
                    pbc.pay_batch_id,
                    pb.pay_date,
                    pbc.remittance_sent_at_utc,
                    pbc.remittance_trigger_status,
                    pbc.last_remittance_error
                   FROM pay_batch_items pbi
                     JOIN pay_batch_candidates pbc ON pbc.id = pbi.pay_batch_candidate_id
                     JOIN pay_batches pb ON pb.id = pbc.pay_batch_id
                     LEFT JOIN pay_advances pa_fallback ON pbi.finance_case_id IS NULL AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
                  WHERE pbi.finance_case_id IS NOT NULL OR pbi.source_ref IS NOT NULL AND pbi.source_ref ~~ 'advance:%'::text) x
          WHERE x.finance_case_id IS NOT NULL
          ORDER BY x.finance_case_id, x.remittance_sent_at_utc DESC NULLS LAST, x.pay_date DESC NULLS LAST, x.pay_batch_id DESC
        ), latest_recovery_batch AS (
         SELECT DISTINCT ON (x.finance_case_id) x.finance_case_id,
            x.pay_batch_id AS latest_recovery_pay_batch_id,
            x.pay_date AS latest_recovery_pay_date
           FROM ( SELECT COALESCE(pbi.finance_case_id, pa_fallback.id) AS finance_case_id,
                    pbc.pay_batch_id,
                    pb.pay_date
                   FROM pay_batch_items pbi
                     JOIN pay_batch_candidates pbc ON pbc.id = pbi.pay_batch_candidate_id
                     JOIN pay_batches pb ON pb.id = pbc.pay_batch_id
                     LEFT JOIN pay_advances pa_fallback ON pbi.finance_case_id IS NULL AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
                  WHERE (pbi.item_type = ANY (ARRAY['OVERPAYMENT_RECOVERY'::text, 'LOAN_REPAYMENT'::text, 'MANUAL_DEBT_RECOVERY'::text])) AND (pbi.finance_case_id IS NOT NULL OR pbi.source_ref IS NOT NULL AND pbi.source_ref ~~ 'advance:%'::text)) x
          WHERE x.finance_case_id IS NOT NULL
          ORDER BY x.finance_case_id, x.pay_date DESC NULLS LAST, x.pay_batch_id DESC
        ), latest_finance_batch AS (
         SELECT DISTINCT ON (x.finance_case_id) x.finance_case_id,
            x.pay_batch_id AS latest_finance_pay_batch_id,
            x.pay_date AS latest_finance_pay_date,
            x.batch_status AS latest_finance_batch_status,
            x.batch_created_at_utc AS latest_finance_batch_created_at_utc,
            x.batch_completed_at_utc AS latest_finance_batch_completed_at_utc,
            x.batch_cancelled_at_utc AS latest_finance_batch_cancelled_at_utc,
            x.authoritative_payment_date AS latest_finance_authoritative_payment_date
           FROM ( SELECT COALESCE(pbi.finance_case_id, pa_fallback.id) AS finance_case_id,
                    pb.id AS pay_batch_id,
                    pb.pay_date,
                    pb.status AS batch_status,
                    pb.created_at_utc AS batch_created_at_utc,
                    pb.completed_at_utc AS batch_completed_at_utc,
                    pb.cancelled_at_utc AS batch_cancelled_at_utc,
                    pb.authoritative_payment_date
                   FROM pay_batch_items pbi
                     JOIN pay_batch_candidates pbc ON pbc.id = pbi.pay_batch_candidate_id
                     JOIN pay_batches pb ON pb.id = pbc.pay_batch_id
                     LEFT JOIN pay_advances pa_fallback ON pbi.finance_case_id IS NULL AND pbi.source_ref = ('advance:'::text || pa_fallback.id::text)
                  WHERE pbi.finance_case_id IS NOT NULL OR pbi.source_ref IS NOT NULL AND pbi.source_ref ~~ 'advance:%'::text) x
          WHERE x.finance_case_id IS NOT NULL
          ORDER BY x.finance_case_id, x.batch_created_at_utc DESC NULLS LAST, x.pay_date DESC NULLS LAST, x.pay_batch_id DESC
        ), active_snooze AS (
         SELECT DISTINCT ON (pa_1.id) pa_1.id AS finance_case_id,
            s_1.id AS snooze_id,
            s_1.snooze_kind,
            s_1.snooze_until_date,
            s_1.note,
            s_1.created_at_utc,
            s_1.updated_at_utc,
            s_1.created_by_user_id,
            s_1.updated_by_user_id
           FROM pay_advances pa_1
             JOIN pay_item_snoozes s_1 ON s_1.source_ref = ('advance:'::text || pa_1.id::text) AND s_1.cleared_at_utc IS NULL
          ORDER BY pa_1.id, s_1.updated_at_utc DESC NULLS LAST, s_1.created_at_utc DESC, s_1.id DESC
        ), component_rollup AS (
         SELECT pfc.finance_case_id,
            count(*) FILTER (WHERE pfc.closed_at_utc IS NULL AND pfc.remaining_source_amount > 0::numeric AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::pay_finance_component_classification_enum)::integer AS open_taxable_count,
            count(*) FILTER (WHERE pfc.closed_at_utc IS NULL AND pfc.remaining_source_amount > 0::numeric AND pfc.classification = 'REIMBURSEMENT_GROSS_FIXED'::pay_finance_component_classification_enum)::integer AS open_reimbursement_count,
            count(*) FILTER (WHERE pfc.closed_at_utc IS NULL AND pfc.remaining_source_amount > 0::numeric AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::pay_finance_component_classification_enum AND upper(COALESCE(pfc.source_pay_method, ''::text)) IS DISTINCT FROM upper(COALESCE(component_candidate.pay_method, ''::text)) AND (component_case.case_type = 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum OR
                CASE
                    WHEN NULLIF(pfc.source_basis_json ->> 'source_units'::text, ''::text) ~ '^-?\d+(\.\d+)?$'::text THEN NULLIF(pfc.source_basis_json ->> 'source_units'::text, ''::text)::numeric <> 0::numeric
                    ELSE false
                END AND NULLIF(pfc.source_basis_json ->> 'source_rate'::text, ''::text) ~ '^-?\d+(\.\d+)?$'::text AND NULLIF(pfc.source_basis_json ->> 'source_charge_rate'::text, ''::text) ~ '^-?\d+(\.\d+)?$'::text AND COALESCE(pfc.component_key_type, ''::text) <> 'ADJUSTMENT_CODE'::text) AND (pfc.is_resolution_stale OR pfc.saved_target_pay_method IS NULL OR upper(COALESCE(pfc.saved_target_pay_method, ''::text)) IS DISTINCT FROM upper(COALESCE(component_candidate.pay_method, ''::text)) OR pfc.saved_resolution_mode IS NULL))::integer AS unresolved_taxable_count,
            count(*) FILTER (WHERE pfc.closed_at_utc IS NULL AND pfc.remaining_source_amount > 0::numeric AND pfc.classification = 'TAXABLE_CHANNEL_SENSITIVE'::pay_finance_component_classification_enum AND upper(COALESCE(pfc.source_pay_method, ''::text)) IS DISTINCT FROM upper(COALESCE(component_candidate.pay_method, ''::text)) AND (component_case.case_type = 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum OR
                CASE
                    WHEN NULLIF(pfc.source_basis_json ->> 'source_units'::text, ''::text) ~ '^-?\d+(\.\d+)?$'::text THEN NULLIF(pfc.source_basis_json ->> 'source_units'::text, ''::text)::numeric <> 0::numeric
                    ELSE false
                END AND NULLIF(pfc.source_basis_json ->> 'source_rate'::text, ''::text) ~ '^-?\d+(\.\d+)?$'::text AND NULLIF(pfc.source_basis_json ->> 'source_charge_rate'::text, ''::text) ~ '^-?\d+(\.\d+)?$'::text AND COALESCE(pfc.component_key_type, ''::text) <> 'ADJUSTMENT_CODE'::text) AND pfc.is_resolution_stale)::integer AS stale_count
           FROM pay_finance_case_components pfc
             JOIN pay_advances component_case ON component_case.id = pfc.finance_case_id
             JOIN candidates component_candidate ON component_candidate.id = pfc.candidate_id
          GROUP BY pfc.finance_case_id
        ), oneoff_bank_details AS (
         SELECT d.finance_case_id,
            d.candidate_id,
            d.beneficiary_name,
            d.sort_code,
            d.account_number,
            d.bank_details_hash,
            d.note,
            d.created_at_utc,
            d.created_by_user_id,
            d.updated_at_utc,
            d.updated_by_user_id
           FROM pay_finance_case_oneoff_payout_bank_details d
        ), base_rows AS (
         SELECT pa.id AS finance_case_id,
            pa.case_type,
            pa.advance_kind,
            pa.reason,
            pa.candidate_id,
            c.tms_ref AS candidate_tms_ref,
            c.display_name AS candidate_display_name,
            c.first_name AS candidate_first_name,
            c.last_name AS candidate_last_name,
            c.pay_method,
            pa.client_id,
            cli.name AS client_name,
            pa.linked_timesheet_id,
            pa.linked_shift_date,
            pa.created_at,
            pa.created_by,
            pa.updated_at,
            pa.status,
            pa.payout_status,
            pa.payout_pay_batch_id,
            pa.payout_transfer_id,
            pa.original_amount,
            pa.outstanding_amount,
            pa.weekly_due,
            pa.weeks_total,
            pa.start_week_start,
            pa.next_due_week_start,
            pa.schedule_json,
            pa.adjustment_comment,
            pa.source_original_paid_amount,
            pa.source_corrected_paid_amount,
            pa.minimum_earnings_threshold,
            pa.take_home_floor_override,
            pa.baseline_signature,
            pa.best_guess_hours,
            pa.notes,
            pa.written_off_at_utc,
            pa.written_off_by_user_id,
            pa.write_off_reason,
            pa.cleared_at_utc,
            pa.cleared_by_user_id,
            rr.active_reserved_amount,
            rr.reserved_amount,
            rr.committed_amount,
            rr.settled_amount,
            rr.released_amount,
            rr.active_reservation_count,
            rr.latest_reservation_created_at_utc,
            rr.latest_committed_at_utc,
            rr.latest_settled_at_utc,
            rr.latest_released_at_utc,
            lrb.latest_recovery_pay_batch_id,
            lrb.latest_recovery_pay_date,
            lr.remittance_sent_at_utc AS latest_remittance_sent_at_utc,
            lr.remittance_trigger_status AS latest_remittance_trigger_status,
            lr.last_remittance_error,
            s.snooze_id AS active_snooze_id,
            s.snooze_kind AS active_snooze_kind,
            s.snooze_until_date AS active_snooze_until_date,
            s.note AS active_snooze_note,
            s.created_at_utc AS active_snooze_created_at_utc,
            s.updated_at_utc AS active_snooze_updated_at_utc,
            COALESCE(cr.open_taxable_count, 0) > 0 AND COALESCE(cr.open_reimbursement_count, 0) > 0 AS is_mixed_case,
            COALESCE(cr.open_taxable_count, 0) AS open_taxable_count,
            COALESCE(cr.open_reimbursement_count, 0) AS open_reimbursement_count,
            COALESCE(cr.unresolved_taxable_count, 0) AS unresolved_taxable_count,
            COALESCE(cr.stale_count, 0) AS stale_count,
            jsonb_build_object('open_taxable_count', COALESCE(cr.open_taxable_count, 0), 'open_reimbursement_count', COALESCE(cr.open_reimbursement_count, 0), 'unresolved_taxable_count', COALESCE(cr.unresolved_taxable_count, 0), 'stale_count', COALESCE(cr.stale_count, 0), 'is_mixed_case', COALESCE(cr.open_taxable_count, 0) > 0 AND COALESCE(cr.open_reimbursement_count, 0) > 0) AS component_resolution_summary_json,
            pa.taxability,
            pa.routing_kind,
            pa.oneoff_bank_details_required,
            lfb.latest_finance_pay_batch_id,
            lfb.latest_finance_pay_date,
            lfb.latest_finance_batch_status,
            lfb.latest_finance_batch_created_at_utc,
            lfb.latest_finance_batch_completed_at_utc,
            lfb.latest_finance_batch_cancelled_at_utc,
            lfb.latest_finance_authoritative_payment_date,
            obd.finance_case_id AS oneoff_bank_finance_case_id,
            obd.candidate_id AS oneoff_bank_candidate_id,
            obd.beneficiary_name AS oneoff_bank_beneficiary_name,
            obd.sort_code AS oneoff_bank_sort_code,
            obd.account_number AS oneoff_bank_account_number,
            obd.bank_details_hash AS oneoff_bank_details_hash,
            obd.note AS oneoff_bank_note,
            obd.created_at_utc AS oneoff_bank_created_at_utc,
            obd.created_by_user_id AS oneoff_bank_created_by_user_id,
            obd.updated_at_utc AS oneoff_bank_updated_at_utc,
            obd.updated_by_user_id AS oneoff_bank_updated_by_user_id
           FROM pay_advances pa
             JOIN candidates c ON c.id = pa.candidate_id
             LEFT JOIN clients cli ON cli.id = pa.client_id
             LEFT JOIN reservation_rollup rr ON rr.finance_case_id = pa.id
             LEFT JOIN latest_remittance lr ON lr.finance_case_id = pa.id
             LEFT JOIN latest_recovery_batch lrb ON lrb.finance_case_id = pa.id
             LEFT JOIN latest_finance_batch lfb ON lfb.finance_case_id = pa.id
             LEFT JOIN active_snooze s ON s.finance_case_id = pa.id
             LEFT JOIN component_rollup cr ON cr.finance_case_id = pa.id
             LEFT JOIN oneoff_bank_details obd ON obd.finance_case_id = pa.id
        ), derived_rows AS (
         SELECT br.finance_case_id,
            br.case_type,
            br.advance_kind,
            br.reason,
            br.candidate_id,
            br.candidate_tms_ref,
            br.candidate_display_name,
            br.candidate_first_name,
            br.candidate_last_name,
            br.pay_method,
            br.client_id,
            br.client_name,
            br.linked_timesheet_id,
            br.linked_shift_date,
            br.created_at,
            br.created_by,
            br.updated_at,
            br.status,
            br.payout_status,
            br.payout_pay_batch_id,
            br.payout_transfer_id,
            br.original_amount,
            br.outstanding_amount,
            br.weekly_due,
            br.weeks_total,
            br.start_week_start,
            br.next_due_week_start,
            br.schedule_json,
            br.adjustment_comment,
            br.source_original_paid_amount,
            br.source_corrected_paid_amount,
            br.minimum_earnings_threshold,
            br.take_home_floor_override,
            br.baseline_signature,
            br.best_guess_hours,
            br.notes,
            br.written_off_at_utc,
            br.written_off_by_user_id,
            br.write_off_reason,
            br.cleared_at_utc,
            br.cleared_by_user_id,
            br.active_reserved_amount,
            br.reserved_amount,
            br.committed_amount,
            br.settled_amount,
            br.released_amount,
            br.active_reservation_count,
            br.latest_reservation_created_at_utc,
            br.latest_committed_at_utc,
            br.latest_settled_at_utc,
            br.latest_released_at_utc,
            br.latest_recovery_pay_batch_id,
            br.latest_recovery_pay_date,
            br.latest_remittance_sent_at_utc,
            br.latest_remittance_trigger_status,
            br.last_remittance_error,
            br.active_snooze_id,
            br.active_snooze_kind,
            br.active_snooze_until_date,
            br.active_snooze_note,
            br.active_snooze_created_at_utc,
            br.active_snooze_updated_at_utc,
            br.is_mixed_case,
            br.open_taxable_count,
            br.open_reimbursement_count,
            br.unresolved_taxable_count,
            br.stale_count,
            br.component_resolution_summary_json,
            br.taxability,
            br.routing_kind,
            br.oneoff_bank_details_required,
            br.latest_finance_pay_batch_id,
            br.latest_finance_pay_date,
            br.latest_finance_batch_status,
            br.latest_finance_batch_created_at_utc,
            br.latest_finance_batch_completed_at_utc,
            br.latest_finance_batch_cancelled_at_utc,
            br.latest_finance_authoritative_payment_date,
            br.oneoff_bank_finance_case_id,
            br.oneoff_bank_candidate_id,
            br.oneoff_bank_beneficiary_name,
            br.oneoff_bank_sort_code,
            br.oneoff_bank_account_number,
            br.oneoff_bank_details_hash,
            br.oneoff_bank_note,
            br.oneoff_bank_created_at_utc,
            br.oneoff_bank_created_by_user_id,
            br.oneoff_bank_updated_at_utc,
            br.oneoff_bank_updated_by_user_id,
                CASE
                    WHEN br.case_type = 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum THEN 'CREDIT_ADJUSTMENTS'::text
                    WHEN br.case_type = 'UNDERPAYMENT'::pay_finance_case_type_enum THEN 'CREDIT_ADJUSTMENTS'::text
                    WHEN br.case_type = 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum THEN 'DEBIT_ADJUSTMENTS'::text
                    WHEN br.case_type = 'OVERPAYMENT'::pay_finance_case_type_enum THEN 'DEBIT_ADJUSTMENTS'::text
                    ELSE 'LOANS_PAYMENT_ADVANCES'::text
                END AS management_group,
                CASE
                    WHEN br.latest_finance_batch_cancelled_at_utc IS NOT NULL OR upper(COALESCE(br.latest_finance_batch_status, ''::text)) = 'CANCELLED'::text OR upper(COALESCE(br.payout_status::text, ''::text)) = 'CANCELLED'::text THEN 'Cancelled'::text
                    WHEN upper(COALESCE(br.latest_finance_batch_status, ''::text)) = ANY (ARRAY['FAILED'::text, 'ERROR'::text]) THEN 'Failed'::text
                    WHEN (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum, 'UNDERPAYMENT'::pay_finance_case_type_enum])) AND upper(COALESCE(br.payout_status::text, ''::text)) = 'PAID'::text OR (br.case_type = ANY (ARRAY['OVERPAYMENT'::pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.written_off_at_utc IS NOT NULL OR br.cleared_at_utc IS NOT NULL OR (upper(COALESCE(br.status::text, ''::text)) = ANY (ARRAY['PAID_OFF'::text, 'CLEARED'::text])) OR COALESCE(br.outstanding_amount, 0::numeric) <= 0::numeric) THEN 'Paid'::text
                    WHEN br.latest_finance_pay_batch_id IS NOT NULL THEN 'Drafted awaiting authorisation'::text
                    ELSE 'Not processed yet'::text
                END AS lifecycle_status_display,
            upper(COALESCE(br.pay_method, ''::text)) = 'UMBRELLA'::text AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::pay_finance_routing_kind_enum AND (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum OR br.taxability = 'NON_TAXABLE'::pay_finance_taxability_enum) AS is_candidate_directed_oneoff_payout,
            br.routing_kind = 'UMBRELLA_COMPANY'::pay_finance_routing_kind_enum AS appears_on_umbrella_remittance,
            upper(COALESCE(br.pay_method, ''::text)) = 'UMBRELLA'::text AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::pay_finance_routing_kind_enum AND (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum OR br.taxability = 'NON_TAXABLE'::pay_finance_taxability_enum) AS generates_candidate_payment_advice,
            br.oneoff_bank_finance_case_id IS NOT NULL AS oneoff_bank_details_present,
            upper(COALESCE(br.pay_method, ''::text)) = 'UMBRELLA'::text AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::pay_finance_routing_kind_enum AND (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum OR br.taxability = 'NON_TAXABLE'::pay_finance_taxability_enum) AND
                CASE
                    WHEN br.latest_finance_batch_cancelled_at_utc IS NOT NULL OR upper(COALESCE(br.latest_finance_batch_status, ''::text)) = 'CANCELLED'::text OR upper(COALESCE(br.payout_status::text, ''::text)) = 'CANCELLED'::text THEN 'Cancelled'::text
                    WHEN upper(COALESCE(br.latest_finance_batch_status, ''::text)) = ANY (ARRAY['FAILED'::text, 'ERROR'::text]) THEN 'Failed'::text
                    WHEN (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum, 'UNDERPAYMENT'::pay_finance_case_type_enum])) AND upper(COALESCE(br.payout_status::text, ''::text)) = 'PAID'::text OR (br.case_type = ANY (ARRAY['OVERPAYMENT'::pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.written_off_at_utc IS NOT NULL OR br.cleared_at_utc IS NOT NULL OR (upper(COALESCE(br.status::text, ''::text)) = ANY (ARRAY['PAID_OFF'::text, 'CLEARED'::text])) OR COALESCE(br.outstanding_amount, 0::numeric) <= 0::numeric) THEN 'Paid'::text
                    WHEN br.latest_finance_pay_batch_id IS NOT NULL THEN 'Drafted awaiting authorisation'::text
                    ELSE 'Not processed yet'::text
                END = 'Not processed yet'::text AS oneoff_bank_details_editable,
            (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum])) AND br.written_off_at_utc IS NULL AND br.cleared_at_utc IS NULL AND (upper(COALESCE(br.status::text, ''::text)) <> ALL (ARRAY['PAID_OFF'::text, 'CLEARED'::text])) AND COALESCE(br.outstanding_amount, 0::numeric) > 0::numeric AS snooze_allowed,
            upper(COALESCE(br.pay_method, ''::text)) = 'UMBRELLA'::text AND br.routing_kind = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'::pay_finance_routing_kind_enum AND (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.case_type <> 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum OR br.taxability = 'NON_TAXABLE'::pay_finance_taxability_enum) AND
                CASE
                    WHEN br.latest_finance_batch_cancelled_at_utc IS NOT NULL OR upper(COALESCE(br.latest_finance_batch_status, ''::text)) = 'CANCELLED'::text OR upper(COALESCE(br.payout_status::text, ''::text)) = 'CANCELLED'::text THEN 'Cancelled'::text
                    WHEN upper(COALESCE(br.latest_finance_batch_status, ''::text)) = ANY (ARRAY['FAILED'::text, 'ERROR'::text]) THEN 'Failed'::text
                    WHEN (br.case_type = ANY (ARRAY['PAYMENT_ADVANCE'::pay_finance_case_type_enum, 'MANUAL_CREDIT_ADJUSTMENT'::pay_finance_case_type_enum, 'UNDERPAYMENT'::pay_finance_case_type_enum])) AND upper(COALESCE(br.payout_status::text, ''::text)) = 'PAID'::text OR (br.case_type = ANY (ARRAY['OVERPAYMENT'::pay_finance_case_type_enum, 'MANUAL_DEBT_ADJUSTMENT'::pay_finance_case_type_enum])) AND (br.written_off_at_utc IS NOT NULL OR br.cleared_at_utc IS NOT NULL OR (upper(COALESCE(br.status::text, ''::text)) = ANY (ARRAY['PAID_OFF'::text, 'CLEARED'::text])) OR COALESCE(br.outstanding_amount, 0::numeric) <= 0::numeric) THEN 'Paid'::text
                    WHEN br.latest_finance_pay_batch_id IS NOT NULL THEN 'Drafted awaiting authorisation'::text
                    ELSE 'Not processed yet'::text
                END = 'Not processed yet'::text AS edit_bank_details_allowed
           FROM base_rows br
        )
 SELECT finance_case_id,
    case_type,
    advance_kind,
    reason,
    candidate_id,
    candidate_tms_ref,
    candidate_display_name,
    candidate_first_name,
    candidate_last_name,
    pay_method,
    client_id,
    client_name,
    linked_timesheet_id,
    linked_shift_date,
    created_at,
    created_by,
    updated_at,
    status,
    payout_status,
    payout_pay_batch_id,
    payout_transfer_id,
    original_amount,
    outstanding_amount,
    weekly_due,
    weeks_total,
    start_week_start,
    next_due_week_start,
    schedule_json,
    adjustment_comment,
    source_original_paid_amount,
    source_corrected_paid_amount,
    minimum_earnings_threshold,
    take_home_floor_override,
    baseline_signature,
    best_guess_hours,
    notes,
    written_off_at_utc,
    written_off_by_user_id,
    write_off_reason,
    cleared_at_utc,
    cleared_by_user_id,
    active_reserved_amount,
    reserved_amount,
    committed_amount,
    settled_amount,
    released_amount,
    active_reservation_count,
    latest_reservation_created_at_utc,
    latest_committed_at_utc,
    latest_settled_at_utc,
    latest_released_at_utc,
    latest_recovery_pay_batch_id,
    latest_recovery_pay_date,
    latest_remittance_sent_at_utc,
    latest_remittance_trigger_status,
    last_remittance_error,
    active_snooze_id,
    active_snooze_kind,
    active_snooze_until_date,
    active_snooze_note,
    active_snooze_created_at_utc,
    active_snooze_updated_at_utc,
    is_mixed_case,
    open_taxable_count,
    open_reimbursement_count,
    unresolved_taxable_count,
    stale_count,
    component_resolution_summary_json,
    taxability,
    routing_kind,
    oneoff_bank_details_present,
    oneoff_bank_details_editable,
    management_group,
    lifecycle_status_display,
    is_candidate_directed_oneoff_payout,
    appears_on_umbrella_remittance,
    generates_candidate_payment_advice,
    snooze_allowed,
    edit_bank_details_allowed,
    oneoff_bank_details_required,
    oneoff_bank_finance_case_id,
    oneoff_bank_candidate_id,
    oneoff_bank_beneficiary_name,
    oneoff_bank_sort_code,
    oneoff_bank_account_number,
    oneoff_bank_details_hash,
    oneoff_bank_note,
    oneoff_bank_created_at_utc,
    oneoff_bank_created_by_user_id,
    oneoff_bank_updated_at_utc,
    oneoff_bank_updated_by_user_id,
    latest_finance_pay_batch_id,
    latest_finance_pay_date,
    latest_finance_batch_status,
    latest_finance_batch_created_at_utc,
    latest_finance_batch_completed_at_utc,
    latest_finance_batch_cancelled_at_utc,
    latest_finance_authoritative_payment_date
   FROM derived_rows dr;;

revoke all privileges on table public.v_finance_cases_register
  from PUBLIC, current_user, service_role, anon, authenticated;
grant all privileges on table public.v_finance_cases_register
  to current_user, service_role;

drop index public.candidates_tms_ref_num_idx;
create unique index uq_hr_rows_import_external_row_key_nonblank
  on public.hr_rows (import_id,external_row_key)
  where external_row_key is not null and pg_catalog.btrim(external_row_key)<>''::text;
create index idx_manual_timesheet_queue_staged_contract_week
  on public.manual_timesheet_queue ((meta_json->>'contract_week_id'),uploaded_at_utc,id)
  where status='STAGED'::text
    and nullif(pg_catalog.btrim(coalesce(meta_json->>'contract_week_id',''::text)),''::text) is not null;
create index idx_timesheet_evidence_ts_kind_created
  on public.timesheet_evidence (timesheet_id,kind,created_at,id);

do $legacy_candidate_workbench_rls$
declare
  v_table text;
begin
  foreach v_table in array array[
    'banking_pay_operation_candidate_allocation_rows',
    'banking_pay_operation_candidate_scope',
    'banking_pay_workbench_candidate_delta_projection_runs',
    'banking_pay_workbench_candidate_line_work',
    'banking_pay_workbench_candidate_source_lines'
  ]::text[]
  loop
    execute pg_catalog.format('alter table public.%I enable row level security',v_table);
    execute pg_catalog.format(
      'grant all privileges on table public.%I to current_user, service_role, anon, authenticated',
      v_table
    );
    execute pg_catalog.format(
      'create policy cloudtms_miget_service_owner_all on public.%I as permissive for all to current_user, service_role using (true) with check (true)',
      v_table
    );
  end loop;
end
$legacy_candidate_workbench_rls$;

revoke all privileges on sequence public.candidate_tmsref_seq
  from PUBLIC, current_user, service_role, anon, authenticated;
grant all privileges on sequence public.candidate_tmsref_seq
  to current_user, service_role, anon, authenticated;

revoke all privileges on schema public
  from PUBLIC, current_user, service_role, anon, authenticated;
grant usage on schema public
  to PUBLIC, current_user, service_role, anon, authenticated;
alter schema public owner to pg_database_owner;
grant create,usage on schema public to pg_database_owner;
grant usage on schema public to current_user;

alter default privileges for role current_user in schema public
  revoke all privileges on tables from PUBLIC, current_user, service_role, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all privileges on tables to current_user, service_role;
alter default privileges for role current_user in schema public
  revoke all privileges on functions from PUBLIC, current_user, service_role, anon, authenticated;
alter default privileges for role current_user in schema public
  grant execute on functions to current_user, service_role;
alter default privileges for role current_user in schema public
  revoke all privileges on sequences from PUBLIC, current_user, service_role, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all privileges on sequences to current_user, service_role;

do $legacy_release_mode_constraint$
declare
  v_definition text;
  v_unexpected_rows bigint;
begin
  select pg_catalog.pg_get_constraintdef(c.oid)
  into v_definition
  from pg_catalog.pg_constraint c
  where c.conrelid='private.cloudtms_database_releases'::pg_catalog.regclass
    and c.conname='cloudtms_database_releases_install_mode_check';

  if v_definition is null or pg_catalog.md5(v_definition)<>'ea7fe362b8f3ce4930fb69f99af13860' then
    raise exception 'LEGACY_RELEASE_MODE_CONSTRAINT_PRESTATE_DRIFT:%',v_definition;
  end if;

  select pg_catalog.count(*)::bigint
  into v_unexpected_rows
  from private.cloudtms_database_releases
  where install_mode not in ('NEW','UPGRADE','ADOPT');
  if v_unexpected_rows<>0 then
    raise exception 'LEGACY_RELEASE_MODE_UNEXPECTED_PRESTATE_ROWS:%',v_unexpected_rows;
  end if;

  alter table private.cloudtms_database_releases
    drop constraint cloudtms_database_releases_install_mode_check;
  alter table private.cloudtms_database_releases
    add constraint cloudtms_database_releases_install_mode_check
    check (install_mode in ('NEW','UPGRADE','ADOPT','LEGACY_UPGRADE'));
end
$legacy_release_mode_constraint$;

do $legacy_structural_poststate$
declare
  v_count integer;
  v_definition text;
begin
  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_policies
  where schemaname='public' and policyname='cloudtms_miget_service_owner_all'
    and tablename in (
      'banking_pay_operation_candidate_allocation_rows',
      'banking_pay_operation_candidate_scope',
      'banking_pay_workbench_candidate_delta_projection_runs',
      'banking_pay_workbench_candidate_line_work',
      'banking_pay_workbench_candidate_source_lines'
    ) and roles @> array[current_user::name,'service_role'::name]
    and cardinality(roles)=2 and cmd='ALL' and qual='true' and with_check='true';
  if v_count<>5 then
    raise exception 'LEGACY_STRUCTURAL_RLS_POLICY_CONVERGENCE_FAILED:%',v_count;
  end if;

  if pg_catalog.pg_get_userbyid((select nspowner from pg_catalog.pg_namespace where nspname='public'))
       <>'pg_database_owner' then
    raise exception 'LEGACY_PUBLIC_SCHEMA_OWNER_CONVERGENCE_FAILED';
  end if;

  select pg_catalog.pg_get_constraintdef(c.oid)
  into v_definition
  from pg_catalog.pg_constraint c
  where c.conrelid='private.cloudtms_database_releases'::pg_catalog.regclass
    and c.conname='cloudtms_database_releases_install_mode_check';
  if v_definition is null or pg_catalog.md5(v_definition)<>'8f5bc83b3f8158a1dde3adaed4ab0c0d' then
    raise exception 'LEGACY_RELEASE_MODE_CONSTRAINT_CONVERGENCE_FAILED:%',v_definition;
  end if;
end
$legacy_structural_poststate$;

commit;
