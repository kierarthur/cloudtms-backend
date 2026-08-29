-- Immutable CloudTMS TEST view and trigger snapshot.
-- Generated from pg_get_viewdef / pg_get_triggerdef; definitions only.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- public.candidate_activity_rollup
create or replace view public.candidate_activity_rollup with (security_invoker=true) as
WITH anchor AS (
         SELECT (now() AT TIME ZONE 'Europe/London'::text)::date AS anchor_ymd
        ), contract_active AS (
         SELECT con.candidate_id,
            bool_or(con.start_date <= (( SELECT a.anchor_ymd
                   FROM anchor a)) AND COALESCE(con.end_date, ( SELECT a.anchor_ymd
                   FROM anchor a)) >= (( SELECT a.anchor_ymd
                   FROM anchor a))) AS is_currently_working
           FROM contracts con
          WHERE con.candidate_id IS NOT NULL
          GROUP BY con.candidate_id
        ), last_ts AS (
         SELECT tf.candidate_id,
            max(ts.week_ending_date) AS last_timesheet_week_ending
           FROM timesheets_financials tf
             JOIN timesheets ts ON ts.timesheet_id = tf.timesheet_id AND ts.is_current = true
          WHERE tf.is_current = true AND tf.candidate_id IS NOT NULL
          GROUP BY tf.candidate_id
        )
 SELECT cand.id AS candidate_id,
    COALESCE(ca.is_currently_working, false) AS is_currently_working,
    lt.last_timesheet_week_ending
   FROM candidates cand
     LEFT JOIN contract_active ca ON ca.candidate_id = cand.id
     LEFT JOIN last_ts lt ON lt.candidate_id = cand.id;;
alter view public.candidate_activity_rollup owner to postgres;

-- public.candidates_summary
create or replace view public.candidates_summary with (security_invoker=true) as
SELECT c.id,
    c.tms_ref,
    c.first_name,
    c.last_name,
    c.display_name,
    c.email,
    c.phone,
    c.pay_method,
    c.umbrella_id,
    u.name AS umbrella_name,
    c.active,
    c.created_at,
    c.updated_at,
    c.key_norm,
    c.mileage_pay_rate,
    c.account_holder,
    c.bank_name,
    c.sort_code,
    c.account_number,
    c.roles,
    c.notes,
    c.rev,
    c.job_title_id,
    c.prof_reg_number,
    c.prof_reg_type,
    c.ni_number,
    c.date_of_birth,
    c.gender,
    c.address_line1,
    c.address_line2,
    c.address_line3,
    c.town_city,
    c.county,
    c.postcode,
    c.country,
    COALESCE(jt_primary.label, jt_legacy.label) AS primary_job_title,
    string_agg(jt_all.label, '; '::text ORDER BY (
        CASE
            WHEN cjt_all.is_primary THEN 0
            ELSE 1
        END), (lower(jt_all.label))) AS job_titles_display,
    c.tms_ref_num,
    COALESCE(array_agg(DISTINCT cjt_all.job_title_id ORDER BY cjt_all.job_title_id) FILTER (WHERE cjt_all.job_title_id IS NOT NULL),
        CASE
            WHEN c.job_title_id IS NOT NULL THEN ARRAY[c.job_title_id]
            ELSE '{}'::uuid[]
        END) AS job_title_ids,
    COALESCE((array_agg(DISTINCT cjt_primary.job_title_id ORDER BY cjt_primary.job_title_id) FILTER (WHERE cjt_primary.job_title_id IS NOT NULL))[1], c.job_title_id) AS primary_job_title_id
   FROM candidates c
     LEFT JOIN umbrellas u ON u.id = c.umbrella_id
     LEFT JOIN candidate_job_titles cjt_all ON cjt_all.candidate_id = c.id
     LEFT JOIN default_job_titles jt_all ON jt_all.id = cjt_all.job_title_id
     LEFT JOIN candidate_job_titles cjt_primary ON cjt_primary.candidate_id = c.id AND cjt_primary.is_primary = true
     LEFT JOIN default_job_titles jt_primary ON jt_primary.id = cjt_primary.job_title_id
     LEFT JOIN default_job_titles jt_legacy ON jt_legacy.id = c.job_title_id
  GROUP BY c.id, c.tms_ref, c.first_name, c.last_name, c.display_name, c.email, c.phone, c.pay_method, c.umbrella_id, u.name, c.active, c.created_at, c.updated_at, c.key_norm, c.mileage_pay_rate, c.account_holder, c.bank_name, c.sort_code, c.account_number, c.roles, c.notes, c.rev, c.job_title_id, c.prof_reg_number, c.prof_reg_type, c.ni_number, c.date_of_birth, c.gender, c.address_line1, c.address_line2, c.address_line3, c.town_city, c.county, c.postcode, c.country, jt_primary.label, jt_legacy.label, c.tms_ref_num;;
alter view public.candidates_summary owner to postgres;

-- public.candidates_summary_activity
create or replace view public.candidates_summary_activity with (security_invoker=true) as
SELECT cs.id,
    cs.tms_ref,
    cs.first_name,
    cs.last_name,
    cs.display_name,
    cs.email,
    cs.phone,
    cs.pay_method,
    cs.umbrella_id,
    cs.umbrella_name,
    cs.active,
    cs.created_at,
    cs.updated_at,
    cs.key_norm,
    cs.mileage_pay_rate,
    cs.account_holder,
    cs.bank_name,
    cs.sort_code,
    cs.account_number,
    cs.roles,
    cs.notes,
    cs.rev,
    cs.job_title_id,
    cs.prof_reg_number,
    cs.prof_reg_type,
    cs.ni_number,
    cs.date_of_birth,
    cs.gender,
    cs.address_line1,
    cs.address_line2,
    cs.address_line3,
    cs.town_city,
    cs.county,
    cs.postcode,
    cs.country,
    cs.primary_job_title,
    cs.job_titles_display,
    car.is_currently_working,
    car.last_timesheet_week_ending,
    cs.tms_ref_num,
    cs.job_title_ids,
    cs.primary_job_title_id
   FROM candidates_summary cs
     LEFT JOIN candidate_activity_rollup car ON car.candidate_id = cs.id;;
alter view public.candidates_summary_activity owner to postgres;

-- public.timesheets_hr_view
create or replace view public.timesheets_hr_view with (security_invoker=true) as
SELECT t.timesheet_id AS id,
    c.id AS candidate_id,
    t.occupant_key_norm,
    COALESCE(t.worked_start_iso, t.scheduled_start_iso) AS start_utc,
    COALESCE(t.worked_end_iso, t.scheduled_end_iso) AS end_utc,
    COALESCE(NULLIF(t.ward_norm, ''::text), NULLIF(t.hospital_norm, ''::text)) AS unit,
        CASE
            WHEN t.job_title_norm ~~* '%hca%'::text OR t.job_title_norm ~~* '%health care%'::text THEN 'HCA'::text
            WHEN t.job_title_norm ~~* '%rmn%'::text OR t.job_title_norm ~~* '%mental%'::text THEN 'RMN'::text
            WHEN t.job_title_norm ~~* '%registered nurse%'::text OR t.job_title_norm ~~* '%rgn%'::text THEN 'RGN'::text
            ELSE NULL::text
        END AS role_code,
    v.status = 'VALIDATION_OK'::validation_status_enum AS authorised,
    (COALESCE(t.worked_start_iso, t.scheduled_start_iso) AT TIME ZONE 'Europe/London'::text)::date AS date_ymd
   FROM timesheets t
     LEFT JOIN candidates c ON c.key_norm = t.occupant_key_norm
     LEFT JOIN LATERAL ( SELECT tv.status
           FROM timesheet_validations tv
          WHERE tv.timesheet_id = t.timesheet_id
          ORDER BY tv.updated_at DESC
         LIMIT 1) v ON true
  WHERE t.is_current IS TRUE;;
alter view public.timesheets_hr_view owner to postgres;

-- public.v_contract_weeks_enriched
create or replace view public.v_contract_weeks_enriched with (security_invoker=true) as
SELECT cw.id,
    cw.contract_id,
    cw.week_ending_date,
    cw.additional_seq,
    cw.status,
    cw.submission_mode_snapshot,
    cw.timesheet_id,
    cw.uploaded_pdf_r2_key,
    cw.day_entries_json,
    cw.totals_json,
    cw.created_at,
    cw.updated_at,
    c.candidate_id,
    c.client_id,
    c.require_reference_to_pay,
    c.require_reference_to_invoice,
    cw.planned_schedule_json,
    cw.enforce_day_partition,
    cw.allowed_days_mask,
    cw.split_boundary_date,
    cw.worker_note,
    cw.split_group_key,
    c.is_ad_hoc,
    cw.is_adjustment
   FROM contract_weeks cw
     JOIN contracts c ON c.id = cw.contract_id;;
alter view public.v_contract_weeks_enriched owner to postgres;

-- public.v_finance_cases_register
create or replace view public.v_finance_cases_register with (security_invoker=true) as
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
alter view public.v_finance_cases_register owner to postgres;

-- public.v_legacy_candidate_contract_summary
create or replace view public.v_legacy_candidate_contract_summary with (security_invoker=true) as
SELECT candidate_id,
    max(COALESCE(end_date, start_date)) AS last_worked_date,
    count(*) AS contract_count,
    count(DISTINCT client_id) AS distinct_client_count
   FROM legacy_contracts lc
  GROUP BY candidate_id;;
alter view public.v_legacy_candidate_contract_summary owner to postgres;

-- public.v_legacy_client_candidates
create or replace view public.v_legacy_client_candidates with (security_invoker=true) as
SELECT lc.client_id,
    cli.name AS client_name,
    lc.candidate_id,
    cand.display_name AS candidate_name,
    max(COALESCE(lc.end_date, lc.start_date)) AS last_worked_date,
    min(lc.start_date) AS first_worked_date,
    count(*) AS contract_count
   FROM legacy_contracts lc
     JOIN clients cli ON cli.id = lc.client_id
     JOIN candidates cand ON cand.id = lc.candidate_id
  GROUP BY lc.client_id, cli.name, lc.candidate_id, cand.display_name;;
alter view public.v_legacy_client_candidates owner to postgres;

-- public.v_legacy_contract_rate_lines_flat
create or replace view public.v_legacy_contract_rate_lines_flat with (security_invoker=true) as
SELECT lc.candidate_id,
    cand.display_name AS candidate_name,
    lc.client_id,
    cli.name AS client_name,
    lc.start_date,
    lc.end_date,
    lc.job_title,
    lc.pay_method,
    lc.raw_payment_method,
    lrl.line_no,
    lrl.label,
    lrl.pay_rate,
    lrl.margin,
    lrl.charge_rate
   FROM legacy_contracts lc
     JOIN clients cli ON cli.id = lc.client_id
     JOIN candidates cand ON cand.id = lc.candidate_id
     JOIN legacy_contract_rate_lines lrl ON lrl.legacy_contract_id = lc.id;;
alter view public.v_legacy_contract_rate_lines_flat owner to postgres;

-- public.v_legacy_contracts_by_candidate
create or replace view public.v_legacy_contracts_by_candidate with (security_invoker=true) as
SELECT lc.candidate_id,
    lc.id AS legacy_contract_id,
    lc.client_id,
    cli.name AS client_name,
    lc.start_date,
    lc.end_date,
    lc.job_title,
    lc.pay_method,
    lc.raw_payment_method,
    COALESCE(jsonb_agg(jsonb_build_object('line_no', lrl.line_no, 'label', lrl.label, 'pay_rate', lrl.pay_rate, 'margin', lrl.margin, 'charge_rate', lrl.charge_rate) ORDER BY lrl.line_no) FILTER (WHERE lrl.id IS NOT NULL), '[]'::jsonb) AS rate_lines_json
   FROM legacy_contracts lc
     JOIN clients cli ON cli.id = lc.client_id
     LEFT JOIN legacy_contract_rate_lines lrl ON lrl.legacy_contract_id = lc.id
  GROUP BY lc.candidate_id, lc.id, lc.client_id, cli.name, lc.start_date, lc.end_date, lc.job_title, lc.pay_method, lc.raw_payment_method;;
alter view public.v_legacy_contracts_by_candidate owner to postgres;

-- public.v_mailshot_resolution_graph
create or replace view public.v_mailshot_resolution_graph with (security_invoker=true) as
SELECT root_entity_type,
    source_family,
    source_view_name
   FROM ( VALUES ('candidate'::text,'candidate'::text,'v_mailshot_src_candidate'::text), ('candidate'::text,'umbrella'::text,'v_mailshot_src_umbrella'::text), ('candidate'::text,'system'::text,'v_mailshot_src_system'::text), ('client'::text,'client'::text,'v_mailshot_src_client'::text), ('client'::text,'system'::text,'v_mailshot_src_system'::text), ('contract'::text,'contract'::text,'v_mailshot_src_contract'::text), ('contract'::text,'candidate'::text,'v_mailshot_src_candidate'::text), ('contract'::text,'client'::text,'v_mailshot_src_client'::text), ('contract'::text,'umbrella'::text,'v_mailshot_src_umbrella'::text), ('contract'::text,'system'::text,'v_mailshot_src_system'::text), ('timesheet'::text,'timesheet'::text,'v_mailshot_src_timesheet'::text), ('timesheet'::text,'contract'::text,'v_mailshot_src_contract'::text), ('timesheet'::text,'candidate'::text,'v_mailshot_src_candidate'::text), ('timesheet'::text,'client'::text,'v_mailshot_src_client'::text), ('timesheet'::text,'umbrella'::text,'v_mailshot_src_umbrella'::text), ('timesheet'::text,'system'::text,'v_mailshot_src_system'::text), ('invoice'::text,'invoice'::text,'v_mailshot_src_invoice'::text), ('invoice'::text,'client'::text,'v_mailshot_src_client'::text), ('invoice'::text,'system'::text,'v_mailshot_src_system'::text), ('umbrella'::text,'umbrella'::text,'v_mailshot_src_umbrella'::text), ('umbrella'::text,'system'::text,'v_mailshot_src_system'::text)) g(root_entity_type, source_family, source_view_name);;
alter view public.v_mailshot_resolution_graph owner to postgres;

-- public.v_mailshot_src_candidate
create or replace view public.v_mailshot_src_candidate with (security_invoker=true) as
SELECT c.id AS _context_id,
    c.tms_ref,
    c.title,
    c.first_name,
    c.last_name,
    c.display_name,
    c.email,
    c.phone,
    c.pay_method,
    c.active,
    c.band,
    u.name AS umbrella_name
   FROM candidates c
     LEFT JOIN umbrellas u ON u.id = c.umbrella_id;;
alter view public.v_mailshot_src_candidate owner to postgres;

-- public.v_mailshot_src_client
create or replace view public.v_mailshot_src_client with (security_invoker=true) as
SELECT c.id AS _context_id,
    c.cli_ref,
    c.name,
    c.invoice_address,
    c.primary_invoice_email,
    c.ap_phone,
    c.ts_queries_email,
    c.contact_title,
    c.contact_known_as,
    c.contact_forename,
    c.contact_surname,
    c.contact_job_title,
    c.contact_tel,
    c.contact_mobile,
    c.contact_email,
    c.website,
    c.vat_chargeable,
    c.payment_terms_days,
    cs.default_submission_mode,
    cs.week_ending_weekday,
    cs.pay_reference_required,
    cs.invoice_reference_required,
    cs.auto_invoice_default
   FROM clients c
     LEFT JOIN LATERAL ( SELECT cs_1.id,
            cs_1.client_id,
            cs_1.timezone_id,
            cs_1.day_start,
            cs_1.day_end,
            cs_1.night_start,
            cs_1.night_end,
            cs_1.bh_source,
            cs_1.bh_list,
            cs_1.bh_feed_url,
            cs_1.vat_rate_pct,
            cs_1.holiday_pay_pct,
            cs_1.erni_pct,
            cs_1.apply_holiday_to,
            cs_1.apply_erni_to,
            cs_1.margin_includes,
            cs_1.effective_from,
            cs_1.created_at,
            cs_1.updated_at,
            cs_1.hr_validation_required,
            cs_1.ts_reference_required,
            cs_1.week_ending_weekday,
            cs_1.autoprocess_hr,
            cs_1.pay_reference_required,
            cs_1.invoice_reference_required,
            cs_1.default_submission_mode,
            cs_1.sat_start,
            cs_1.sat_end,
            cs_1.sun_start,
            cs_1.sun_end,
            cs_1.is_nhsp,
            cs_1.self_bill_no_invoices_sent,
            cs_1.daily_calc_of_invoices,
            cs_1.no_timesheet_required,
            cs_1.group_nightsat_sunbh,
            cs_1.requires_hr,
            cs_1.hr_attach_to_invoice,
            cs_1.ts_attach_to_invoice,
            cs_1.bh_start,
            cs_1.bh_end,
            cs_1.auto_invoice_default,
            cs_1.send_manual_invoices_to_different_email,
            cs_1.manual_invoices_alt_email_address,
            cs_1.invoice_consolidation_mode,
            cs_1.reference_number_required_to_issue_invoice,
            cs_1.opt_in_email,
            cs_1.opt_in_sms,
            cs_1.opt_in_whatsapp
           FROM client_settings cs_1
          WHERE cs_1.client_id = c.id
          ORDER BY cs_1.effective_from DESC NULLS LAST, cs_1.updated_at DESC NULLS LAST, cs_1.created_at DESC NULLS LAST, cs_1.id DESC
         LIMIT 1) cs ON true;;
alter view public.v_mailshot_src_client owner to postgres;

-- public.v_mailshot_src_contract
create or replace view public.v_mailshot_src_contract with (security_invoker=true) as
SELECT id AS _context_id,
    role,
    band,
    display_site,
    ward_hint,
    start_date,
    end_date,
    pay_method_snapshot,
    default_submission_mode,
    week_ending_weekday_snapshot,
    auto_invoice,
    require_reference_to_pay,
    require_reference_to_invoice,
    self_bill,
    weekly_timesheet_source,
    no_timesheet_required,
    daily_calc_of_invoices,
    group_nightsat_sunbh,
    is_nhsp,
    autoprocess_hr,
    requires_hr,
    hr_attach_to_invoice,
    ts_attach_to_invoice,
    reference_number_required_to_issue_invoice,
    send_manual_invoices_to_different_email,
    manual_invoices_alt_email_address,
    is_ad_hoc
   FROM contracts c;;
alter view public.v_mailshot_src_contract owner to postgres;

-- public.v_mailshot_src_invoice
create or replace view public.v_mailshot_src_invoice with (security_invoker=true) as
WITH line_agg AS (
         SELECT il.invoice_id,
            count(*) AS line_count,
            count(DISTINCT il.timesheet_id) AS timesheet_count,
            COALESCE(sum(il.hours_day), 0::numeric) AS hours_day,
            COALESCE(sum(il.hours_night), 0::numeric) AS hours_night,
            COALESCE(sum(il.hours_sat), 0::numeric) AS hours_sat,
            COALESCE(sum(il.hours_sun), 0::numeric) AS hours_sun,
            COALESCE(sum(il.hours_bh), 0::numeric) AS hours_bh,
            COALESCE(sum(il.hours_day), 0::numeric) + COALESCE(sum(il.hours_night), 0::numeric) + COALESCE(sum(il.hours_sat), 0::numeric) + COALESCE(sum(il.hours_sun), 0::numeric) + COALESCE(sum(il.hours_bh), 0::numeric) AS total_hours,
            COALESCE(sum(il.total_pay_ex_vat), 0::numeric) AS total_pay_ex_vat_lines,
            COALESCE(sum(il.total_charge_ex_vat), 0::numeric) AS total_charge_ex_vat_lines,
            COALESCE(sum(il.margin_ex_vat), 0::numeric) AS margin_ex_vat_lines,
            COALESCE(sum(il.vat_amount), 0::numeric) AS vat_amount_lines,
            COALESCE(sum(il.total_inc_vat), 0::numeric) AS total_inc_vat_lines
           FROM invoice_lines il
          GROUP BY il.invoice_id
        )
 SELECT i.id AS _context_id,
    i.invoice_no,
    i.type,
    i.status,
    i.status_date_utc,
    i.issued_at_utc,
    i.due_at_utc,
    i.paid_at_utc,
    i.notes,
    i.invoice_pdf_generated_at_utc,
    i.subtotal_ex_vat,
    i.vat_amount,
    i.total_inc_vat,
    COALESCE(la.line_count, 0::bigint) AS line_count,
    COALESCE(la.timesheet_count, 0::bigint) AS timesheet_count,
    COALESCE(la.hours_day, 0::numeric) AS hours_day,
    COALESCE(la.hours_night, 0::numeric) AS hours_night,
    COALESCE(la.hours_sat, 0::numeric) AS hours_sat,
    COALESCE(la.hours_sun, 0::numeric) AS hours_sun,
    COALESCE(la.hours_bh, 0::numeric) AS hours_bh,
    COALESCE(la.total_hours, 0::numeric) AS total_hours,
    COALESCE(la.total_pay_ex_vat_lines, 0::numeric) AS total_pay_ex_vat_lines,
    COALESCE(la.total_charge_ex_vat_lines, 0::numeric) AS total_charge_ex_vat_lines,
    COALESCE(la.margin_ex_vat_lines, 0::numeric) AS margin_ex_vat_lines,
    COALESCE(la.vat_amount_lines, 0::numeric) AS vat_amount_lines,
    COALESCE(la.total_inc_vat_lines, 0::numeric) AS total_inc_vat_lines
   FROM invoices i
     LEFT JOIN line_agg la ON la.invoice_id = i.id;;
alter view public.v_mailshot_src_invoice owner to postgres;

-- public.v_mailshot_src_system
create or replace view public.v_mailshot_src_system with (security_invoker=true) as
SELECT 'system'::text AS _context_key,
    (now() AT TIME ZONE 'Europe/London'::text)::date AS today_uk,
    to_char((now() AT TIME ZONE 'Europe/London'::text)::date::timestamp with time zone, 'YYYY-MM-DD'::text) AS today_ymd,
    to_char((now() AT TIME ZONE 'Europe/London'::text)::date::timestamp with time zone, 'DD/MM/YYYY'::text) AS today_ddmmyyyy,
    now() AS now_utc,
    (now() AT TIME ZONE 'Europe/London'::text) AS now_uk;;
alter view public.v_mailshot_src_system owner to postgres;

-- public.v_mailshot_src_timesheet
create or replace view public.v_mailshot_src_timesheet with (security_invoker=true) as
SELECT t.timesheet_id AS _context_id,
    t.booking_id,
    t.week_ending_date,
    t.sheet_scope,
    t.submission_mode,
    t.status,
    t.reference_number,
    t.reference_set_at,
    t.auth_name,
    t.auth_job_title,
    t.authorised_at_server,
    t.scheduled_start_iso,
    t.scheduled_end_iso,
    t.worked_start_iso,
    t.worked_end_iso,
    t.break_start_iso,
    t.break_end_iso,
    t.break_minutes,
    t.worked_minutes,
    t.hospital_norm,
    t.ward_norm,
    t.job_title_norm,
    t.shift_label_norm,
    t.line_type,
    t.generated_pdf_at_utc,
    tf.basis,
    tf.processing_status,
    tf.pay_method,
    tf.role,
    tf.band,
    tf.candidate_assignment,
    tf.hours_day,
    tf.hours_night,
    tf.hours_sat,
    tf.hours_sun,
    tf.hours_bh,
    tf.total_hours,
    tf.pay_day,
    tf.pay_night,
    tf.pay_sat,
    tf.pay_sun,
    tf.pay_bh,
    tf.charge_day,
    tf.charge_night,
    tf.charge_sat,
    tf.charge_sun,
    tf.charge_bh,
    tf.total_pay_ex_vat,
    tf.total_charge_ex_vat,
    tf.margin_ex_vat,
    tf.expenses_pay_ex_vat,
    tf.expenses_charge_ex_vat,
    tf.mileage_units,
    tf.mileage_pay_rate,
    tf.mileage_charge_rate,
    tf.mileage_pay_ex_vat,
    tf.mileage_charge_ex_vat,
    tf.travel_pay_ex_vat,
    tf.travel_charge_ex_vat,
    tf.accommodation_pay_ex_vat,
    tf.accommodation_charge_ex_vat,
    tf.other_pay_ex_vat,
    tf.other_charge_ex_vat,
    tf.additional_pay_ex_vat,
    tf.additional_charge_ex_vat,
    tf.additional_margin_ex_vat,
    tf.pay_on_hold,
    tf.pay_on_hold_reason,
    tf.paid_at_utc,
    tf.payment_reference,
    tf.locked_by_invoice_id IS NOT NULL AS invoice_locked,
    tf.locked_by_invoice_id
   FROM timesheets t
     LEFT JOIN LATERAL ( SELECT tf_1.id,
            tf_1.timesheet_id,
            tf_1.timesheet_version,
            tf_1.basis,
            tf_1.is_current,
            tf_1.is_stale,
            tf_1.stale_reason,
            tf_1.worked_start_iso,
            tf_1.worked_end_iso,
            tf_1.break_start_iso,
            tf_1.break_end_iso,
            tf_1.break_minutes,
            tf_1.candidate_id,
            tf_1.client_id,
            tf_1.role,
            tf_1.band,
            tf_1.pay_method,
            tf_1.policy_snapshot_json,
            tf_1.rate_source_refs_json,
            tf_1.hours_day,
            tf_1.hours_night,
            tf_1.hours_sat,
            tf_1.hours_sun,
            tf_1.hours_bh,
            tf_1.pay_day,
            tf_1.pay_night,
            tf_1.pay_sat,
            tf_1.pay_sun,
            tf_1.pay_bh,
            tf_1.charge_day,
            tf_1.charge_night,
            tf_1.charge_sat,
            tf_1.charge_sun,
            tf_1.charge_bh,
            tf_1.total_hours,
            tf_1.total_pay_ex_vat,
            tf_1.total_charge_ex_vat,
            tf_1.margin_ex_vat,
            tf_1.computed_at_utc,
            tf_1.locked_by_invoice_id,
            tf_1.locked_at_utc,
            tf_1.unlocked_by_credit_note_id,
            tf_1.created_at,
            tf_1.updated_at,
            tf_1.occupant_key_norm,
            tf_1.candidate_assignment,
            tf_1.processing_status,
            tf_1.expenses_pay_ex_vat,
            tf_1.expenses_charge_ex_vat,
            tf_1.expenses_description,
            tf_1.expenses_evidence_r2_key,
            tf_1.mileage_pay_ex_vat,
            tf_1.mileage_charge_ex_vat,
            tf_1.mileage_evidence_r2_key,
            tf_1.mileage_pay_rate,
            tf_1.mileage_charge_rate,
            tf_1.po_number,
            tf_1.pay_on_hold,
            tf_1.pay_on_hold_reason,
            tf_1.pay_on_hold_since_utc,
            tf_1.paid_at_utc,
            tf_1.paid_by_user_id,
            tf_1.payment_reference,
            tf_1.remittance_last_sent_at_utc,
            tf_1.remittance_send_count,
            tf_1.pay_wtr_rate_pct_snapshot,
            tf_1.pay_vat_rate_pct_snapshot,
            tf_1.pay_vat_amount_snapshot,
            tf_1.pay_total_inc_vat_snapshot,
            tf_1.processed_by_user_id,
            tf_1.processed_at_utc,
            tf_1.authorised_by_user_id,
            tf_1.authorised_at_utc,
            tf_1.expenses_evidence_manifest,
            tf_1.mileage_evidence_manifest,
            tf_1.actual_schedule_json,
            tf_1.actual_minutes_by_day_json,
            tf_1.additional_units_json,
            tf_1.additional_pay_ex_vat,
            tf_1.additional_charge_ex_vat,
            tf_1.additional_margin_ex_vat,
            tf_1.invoice_breakdown_json,
            tf_1.nhsp_import_id,
            tf_1.has_rate_issue,
            tf_1.has_pay_channel_issue,
            tf_1.hr_crosscheck_status,
            tf_1.hr_crosscheck_issues,
            tf_1.external_source_rows_json,
            tf_1.mileage_units,
            tf_1.travel_pay_ex_vat,
            tf_1.travel_charge_ex_vat,
            tf_1.accommodation_pay_ex_vat,
            tf_1.accommodation_charge_ex_vat,
            tf_1.other_pay_ex_vat,
            tf_1.other_charge_ex_vat
           FROM timesheets_financials tf_1
          WHERE tf_1.timesheet_id = t.timesheet_id AND tf_1.is_current = true
          ORDER BY tf_1.updated_at DESC NULLS LAST, tf_1.created_at DESC NULLS LAST, tf_1.id DESC
         LIMIT 1) tf ON true;;
alter view public.v_mailshot_src_timesheet owner to postgres;

-- public.v_mailshot_src_umbrella
create or replace view public.v_mailshot_src_umbrella with (security_invoker=true) as
SELECT id AS _context_id,
    name,
    remittance_email,
    enabled,
    vat_chargeable,
    company_number,
    address_line1,
    address_line2,
    address_line3,
    town_city,
    county,
    postcode,
    country
   FROM umbrellas u;;
alter view public.v_mailshot_src_umbrella owner to postgres;

-- public.v_outbox_unified
create or replace view public.v_outbox_unified with (security_invoker=true) as
SELECT 'EMAIL'::text AS channel,
    o.id AS outbox_id,
    o.type AS outbox_type,
    o.status::text AS status,
    o.provider_status AS delivery_status,
    o.created_at_utc,
    o.sent_at,
    o.delivered_at,
    o.read_at,
    o.failed_at,
    o."to" AS to_address,
    o.cc,
    o.bcc,
    o.reply_to,
    o.importance,
    o.email_type,
    o.subject,
    o.body_text,
    o.body_html,
    o.attachments,
    o.reference,
    o.provider_message_id,
    o.last_error,
    o.created_by,
    o.recipient_kind,
    o.recipient_id,
    o.context_kind,
    o.context_id,
    o.mailshot_run_id,
    o.document_template_id,
    o.scheduled_for_utc,
    o.next_attempt_at_utc
   FROM mail_outbox o
UNION ALL
 SELECT c.channel,
    c.id AS outbox_id,
    NULL::text AS outbox_type,
    c.status,
        CASE
            WHEN c.read_at IS NOT NULL THEN 'READ'::text
            WHEN c.delivered_at IS NOT NULL THEN 'DELIVERED'::text
            WHEN c.sent_at IS NOT NULL THEN 'SENT'::text
            WHEN c.failed_at IS NOT NULL THEN 'FAILED'::text
            ELSE NULL::text
        END AS delivery_status,
    c.created_at_utc,
    c.sent_at,
    c.delivered_at,
    c.read_at,
    c.failed_at,
    c.to_address,
    NULL::text AS cc,
    NULL::text AS bcc,
    NULL::text AS reply_to,
    NULL::text AS importance,
    NULL::text AS email_type,
    NULL::text AS subject,
    c.message_text AS body_text,
    NULL::text AS body_html,
    NULL::jsonb AS attachments,
    NULL::text AS reference,
    c.provider_message_id,
    c.last_error,
    c.created_by,
    c.recipient_kind,
    c.recipient_id,
    c.context_kind,
    c.context_id,
    c.mailshot_run_id,
    c.document_template_id,
    c.scheduled_for_utc,
    c.next_attempt_at_utc
   FROM comms_outbox c;;
alter view public.v_outbox_unified owner to postgres;

-- public.v_rates_client_defaults_enabled
create or replace view public.v_rates_client_defaults_enabled with (security_invoker=true) as
SELECT id,
    client_id,
    role,
    band,
    charge_day,
    charge_night,
    charge_sat,
    charge_sun,
    charge_bh,
    date_from,
    date_to,
    created_at,
    updated_at,
    paye_day,
    paye_night,
    paye_sat,
    paye_sun,
    paye_bh,
    umb_day,
    umb_night,
    umb_sat,
    umb_sun,
    umb_bh,
    disabled_at_utc,
    disabled_by
   FROM rates_client_defaults
  WHERE disabled_at_utc IS NULL;;
alter view public.v_rates_client_defaults_enabled owner to postgres;

-- public.v_timesheets_daily_match
create or replace view public.v_timesheets_daily_match with (security_invoker=true) as
SELECT tf.timesheet_id,
    tf.timesheet_version,
    tf.candidate_id,
    tf.client_id,
    tf.worked_start_iso,
    tf.worked_end_iso,
    tf.break_minutes,
    t.worked_minutes,
    tf.role AS tsfin_role,
    tf.band AS tsfin_band,
    tf.processing_status,
    tf.locked_by_invoice_id,
    tf.paid_at_utc,
    t.booking_id,
    t.version AS timesheet_row_version,
    t.status AS timesheet_status,
    t.sheet_scope,
    t.reference_number,
    t.occupant_key_norm,
    t.hospital_norm,
    t.ward_norm,
    t.band AS timesheet_band,
    t.job_title_norm,
    t.shift_label_norm
   FROM timesheets_financials tf
     JOIN timesheets t ON t.timesheet_id = tf.timesheet_id AND t.version = tf.timesheet_version
  WHERE tf.is_current = true AND t.is_current = true AND t.sheet_scope = 'DAILY'::timesheet_scope_enum;;
alter view public.v_timesheets_daily_match owner to postgres;

-- public.v_timesheets_funnel
create or replace view public.v_timesheets_funnel with (security_invoker=true) as
SELECT 'WEEK'::text AS kind,
    cw.id AS key_id,
    cw.contract_id,
    c.candidate_id,
    c.client_id,
    cw.week_ending_date,
    cw.additional_seq,
    cw.status,
    COALESCE(t.submission_mode, cw.submission_mode_snapshot) AS submission_mode,
    t.sheet_scope,
    t.qr_status,
    t.timesheet_id,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_status,
    t.authorised_at_server
   FROM contract_weeks cw
     JOIN contracts c ON c.id = cw.contract_id
     LEFT JOIN timesheets t ON t.timesheet_id = cw.timesheet_id AND t.is_current = true
     LEFT JOIN timesheets_financials tf ON tf.timesheet_id = t.timesheet_id AND tf.is_current = true;;
alter view public.v_timesheets_funnel owner to postgres;

-- public.v_ts_invoice_precheck
create or replace view public.v_ts_invoice_precheck with (security_invoker=true) as
WITH anchor AS (
         SELECT (now() AT TIME ZONE 'Europe/London'::text)::date AS anchor_ymd
        )
 SELECT ts.timesheet_id,
    ts.week_ending_date,
    ts.submission_mode,
    ts.manual_pdf_r2_key,
    ts.reference_number,
    COALESCE(
        CASE
            WHEN c.overrideclientsettings THEN c.require_reference_to_invoice
            ELSE NULL::boolean
        END, cs.invoice_reference_required, false) AS require_reference_to_invoice,
        CASE
            WHEN ts.qr_status = 'PENDING'::timesheet_qr_status_enum AND (ts.qr_token IS NOT NULL AND ts.qr_generated_at IS NOT NULL OR ts.qr_last_sent_hash IS NOT NULL) AND ts.qr_scanned_at IS NULL THEN 'BLOCK_QR_UNSIGNED'::text
            WHEN COALESCE(
            CASE
                WHEN c.overrideclientsettings THEN c.ts_attach_to_invoice
                ELSE NULL::boolean
            END, cs.ts_attach_to_invoice, sd.ts_attach_to_invoice, true) = true AND (ts.submission_mode = 'MANUAL'::submission_mode_enum AND ts.manual_pdf_r2_key IS NULL AND COALESCE(tepdf.has_timesheet_evidence_pdf, false) = false OR ts.submission_mode IS DISTINCT FROM 'MANUAL'::submission_mode_enum AND ts.generated_pdf_at_utc IS NULL AND COALESCE(tepdf.has_timesheet_evidence_pdf, false) = false) AND NOT (COALESCE(tf.total_hours, 0::numeric) = 0::numeric AND (COALESCE(tf.travel_charge_ex_vat, 0::numeric) + COALESCE(tf.accommodation_charge_ex_vat, 0::numeric) + COALESCE(tf.other_charge_ex_vat, 0::numeric) + COALESCE(tf.mileage_charge_ex_vat, 0::numeric)) > 0::numeric) THEN 'BLOCK_NO_PDF'::text
            WHEN COALESCE(
            CASE
                WHEN c.overrideclientsettings THEN c.require_reference_to_invoice
                ELSE NULL::boolean
            END, cs.invoice_reference_required, false) = true AND COALESCE(tf.total_hours, 0::numeric) > 0::numeric AND COALESCE(refchk.missing_raw, false) = true THEN 'BLOCK_NO_REFERENCE'::text
            WHEN (COALESCE(tf.mileage_units, 0::numeric) > 0::numeric OR COALESCE(tf.mileage_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(tf.mileage_pay_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
               FROM timesheet_evidence te
              WHERE te.timesheet_id = ts.timesheet_id AND upper(te.kind) = 'MILEAGE'::text)) THEN 'BLOCK_NO_MILEAGE_EVIDENCE'::text
            WHEN (COALESCE(tf.travel_pay_ex_vat, 0::numeric) > 0::numeric OR COALESCE(tf.travel_charge_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
               FROM timesheet_evidence te
              WHERE te.timesheet_id = ts.timesheet_id AND upper(te.kind) = 'TRAVEL'::text)) OR (COALESCE(tf.accommodation_pay_ex_vat, 0::numeric) > 0::numeric OR COALESCE(tf.accommodation_charge_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
               FROM timesheet_evidence te
              WHERE te.timesheet_id = ts.timesheet_id AND upper(te.kind) = 'ACCOMMODATION'::text)) OR (COALESCE(tf.other_pay_ex_vat, 0::numeric) > 0::numeric OR COALESCE(tf.other_charge_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
               FROM timesheet_evidence te
              WHERE te.timesheet_id = ts.timesheet_id AND upper(te.kind) = 'OTHER'::text)) THEN 'BLOCK_NO_EXPENSES_EVIDENCE'::text
            ELSE 'OK'::text
        END AS precheck_status,
    COALESCE(
        CASE
            WHEN c.overrideclientsettings THEN c.ts_attach_to_invoice
            ELSE NULL::boolean
        END, cs.ts_attach_to_invoice, sd.ts_attach_to_invoice, true) AS effective_ts_attach_to_invoice,
    COALESCE(
        CASE
            WHEN c.overrideclientsettings THEN c.hr_attach_to_invoice
            ELSE NULL::boolean
        END, cs.hr_attach_to_invoice, sd.hr_attach_to_invoice, true) AS effective_hr_attach_to_invoice,
    COALESCE(cs.auto_invoice_default, false) AS effective_auto_invoice_default,
    COALESCE(tepdf.has_timesheet_evidence_pdf, false) AS has_timesheet_evidence_pdf,
    COALESCE(
        CASE
            WHEN c.overrideclientsettings THEN c.reference_number_required_to_issue_invoice
            ELSE NULL::boolean
        END, cs.reference_number_required_to_issue_invoice, false) AS reference_number_required_to_issue_invoice,
    COALESCE(
        CASE
            WHEN c.overrideclientsettings THEN c.reference_number_required_to_issue_invoice
            ELSE NULL::boolean
        END, cs.reference_number_required_to_issue_invoice, false) = true AND COALESCE(tf.total_hours, 0::numeric) > 0::numeric AND COALESCE(refchk.issue_missing_raw, false) = true AS issue_missing_reference,
        CASE
            WHEN COALESCE(tf.total_hours, 0::numeric) <= 0::numeric THEN 0
            WHEN COALESCE(
            CASE
                WHEN c.overrideclientsettings THEN c.reference_number_required_to_issue_invoice
                ELSE NULL::boolean
            END, cs.reference_number_required_to_issue_invoice, false) = true THEN COALESCE(refchk.issue_missing_count, 0)
            ELSE 0
        END AS issue_missing_reference_count
   FROM timesheets ts
     LEFT JOIN contract_weeks cw ON cw.timesheet_id = ts.timesheet_id
     LEFT JOIN contracts c ON c.id = COALESCE(ts.contract_id, cw.contract_id)
     LEFT JOIN settings_defaults sd ON sd.id = 1
     LEFT JOIN LATERAL ( SELECT tf0.client_id,
            tf0.total_hours,
            tf0.invoice_breakdown_json,
            tf0.additional_units_json,
            tf0.additional_pay_ex_vat,
            tf0.additional_charge_ex_vat,
            tf0.mileage_units,
            tf0.mileage_pay_ex_vat,
            tf0.mileage_charge_ex_vat,
            tf0.travel_pay_ex_vat,
            tf0.travel_charge_ex_vat,
            tf0.accommodation_pay_ex_vat,
            tf0.accommodation_charge_ex_vat,
            tf0.other_pay_ex_vat,
            tf0.other_charge_ex_vat
           FROM timesheets_financials tf0
          WHERE tf0.timesheet_id = ts.timesheet_id AND tf0.is_current = true
          ORDER BY tf0.updated_at DESC NULLS LAST
         LIMIT 1) tf ON true
     LEFT JOIN LATERAL ( SELECT (EXISTS ( SELECT 1
                   FROM timesheet_evidence te
                  WHERE te.timesheet_id = ts.timesheet_id AND upper(te.kind) = 'TIMESHEET'::text)) AS has_timesheet_evidence_pdf) tepdf ON true
     LEFT JOIN LATERAL ( SELECT cs0.client_id,
            cs0.auto_invoice_default,
            cs0.hr_attach_to_invoice,
            cs0.ts_attach_to_invoice,
            cs0.reference_number_required_to_issue_invoice,
            cs0.invoice_reference_required
           FROM client_settings cs0
             CROSS JOIN anchor a
          WHERE cs0.client_id = tf.client_id AND (cs0.effective_from <= a.anchor_ymd OR cs0.effective_from IS NULL)
          ORDER BY cs0.effective_from DESC NULLS LAST
         LIMIT 1) cs ON true
     LEFT JOIN LATERAL ( WITH segs AS (
                 SELECT s.seg,
                    NULLIF(btrim(COALESCE(s.seg ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) AS locked_by,
                    NULLIF(btrim(COALESCE(s.seg ->> 'ref_num'::text, ''::text)), ''::text) AS ref_num,
                    COALESCE(NULLIF(s.seg ->> 'hours_day'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_night'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_sat'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_sun'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_bh'::text, ''::text)::numeric, 0::numeric) AS hours_sum,
                    COALESCE(NULLIF(s.seg ->> 'charge_amount'::text, ''::text)::numeric, 0::numeric) AS charge_ex
                   FROM ( SELECT value.value AS seg
                           FROM jsonb_array_elements(
                                CASE
                                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments'::text) = 'array'::text THEN tf.invoice_breakdown_json -> 'segments'::text
                                    ELSE '[]'::jsonb
                                END) value(value)) s
                  WHERE jsonb_typeof(s.seg) = 'object'::text
                ), segs_pos_unlocked AS (
                 SELECT segs.seg,
                    segs.locked_by,
                    segs.ref_num,
                    segs.hours_sum,
                    segs.charge_ex
                   FROM segs
                  WHERE segs.locked_by IS NULL AND (COALESCE(segs.hours_sum, 0::numeric) > 0::numeric OR COALESCE(segs.charge_ex, 0::numeric) > 0::numeric)
                ), segs_pos_locked AS (
                 SELECT segs.seg,
                    segs.locked_by,
                    segs.ref_num,
                    segs.hours_sum,
                    segs.charge_ex
                   FROM segs
                  WHERE segs.locked_by IS NOT NULL AND (COALESCE(segs.hours_sum, 0::numeric) > 0::numeric OR COALESCE(segs.charge_ex, 0::numeric) > 0::numeric)
                ), segs_missing_unlocked AS (
                 SELECT count(*)::integer AS cnt
                   FROM segs_pos_unlocked
                  WHERE segs_pos_unlocked.ref_num IS NULL
                ), segs_missing_locked AS (
                 SELECT count(*)::integer AS cnt
                   FROM segs_pos_locked
                  WHERE segs_pos_locked.ref_num IS NULL
                ), weekly_manual_missing AS (
                 SELECT
                        CASE
                            WHEN ts.actual_schedule_json IS NULL OR jsonb_typeof(ts.actual_schedule_json) <> 'array'::text THEN 1
                            WHEN jsonb_array_length(ts.actual_schedule_json) = 0 THEN 1
                            ELSE ( SELECT count(*)::integer AS count
                               FROM jsonb_array_elements(ts.actual_schedule_json) seg(value)
                              WHERE COALESCE(btrim(seg.value ->> 'start'::text), ''::text) <> ''::text AND COALESCE(btrim(seg.value ->> 'end'::text), ''::text) <> ''::text AND COALESCE(btrim(seg.value ->> 'ref_num'::text), ''::text) = ''::text)
                        END AS cnt
                ), weekly_nonmanual_has_any AS (
                 SELECT (EXISTS ( SELECT 1
                           FROM jsonb_array_elements_text(
                                CASE
                                    WHEN ts.day_references_json IS NOT NULL AND jsonb_typeof(ts.day_references_json) = 'object'::text AND jsonb_typeof(ts.day_references_json -> '__freeform_refs'::text) = 'array'::text THEN ts.day_references_json -> '__freeform_refs'::text
                                    WHEN ts.day_references_json IS NOT NULL AND jsonb_typeof(ts.day_references_json) = 'object'::text AND jsonb_typeof(ts.day_references_json -> '__freeform'::text) = 'array'::text THEN ts.day_references_json -> '__freeform'::text
                                    WHEN ts.day_references_json IS NOT NULL AND jsonb_typeof(ts.day_references_json) = 'object'::text AND jsonb_typeof(ts.day_references_json -> '__freeform_lines'::text) = 'array'::text THEN ts.day_references_json -> '__freeform_lines'::text
                                    WHEN ts.day_references_json IS NOT NULL AND jsonb_typeof(ts.day_references_json) = 'array'::text THEN ts.day_references_json
                                    ELSE '[]'::jsonb
                                END) t(x)
                          WHERE NULLIF(btrim(COALESCE(t.x, ''::text)), ''::text) IS NOT NULL)) OR (EXISTS ( SELECT 1
                           FROM jsonb_each_text(
                                CASE
                                    WHEN ts.day_references_json IS NOT NULL AND jsonb_typeof(ts.day_references_json) = 'object'::text THEN ts.day_references_json
                                    ELSE '{}'::jsonb
                                END) j(k, v)
                          WHERE NULLIF(btrim(COALESCE(j.v, ''::text)), ''::text) IS NOT NULL AND "left"(COALESCE(j.k, ''::text), 2) <> '__'::text)) AS has_any
                )
         SELECT
                CASE
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND COALESCE(( SELECT segs_missing_unlocked.cnt
                       FROM segs_missing_unlocked), 0) > 0 THEN true
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND COALESCE(( SELECT segs_missing_unlocked.cnt
                       FROM segs_missing_unlocked), 0) = 0 THEN false
                    WHEN ts.sheet_scope = 'DAILY'::timesheet_scope_enum THEN ts.reference_number IS NULL OR length(btrim(ts.reference_number)) = 0
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode = 'MANUAL'::submission_mode_enum THEN COALESCE(( SELECT weekly_manual_missing.cnt
                       FROM weekly_manual_missing), 0) > 0
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode <> 'MANUAL'::submission_mode_enum THEN COALESCE(( SELECT weekly_nonmanual_has_any.has_any
                       FROM weekly_nonmanual_has_any), false) = false
                    ELSE false
                END AS missing_raw,
                CASE
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text THEN COALESCE(( SELECT segs_missing_unlocked.cnt
                       FROM segs_missing_unlocked), 0)
                    WHEN ts.sheet_scope = 'DAILY'::timesheet_scope_enum THEN
                    CASE
                        WHEN ts.reference_number IS NULL OR length(btrim(ts.reference_number)) = 0 THEN 1
                        ELSE 0
                    END
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode = 'MANUAL'::submission_mode_enum THEN COALESCE(( SELECT weekly_manual_missing.cnt
                       FROM weekly_manual_missing), 0)
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode <> 'MANUAL'::submission_mode_enum THEN
                    CASE
                        WHEN COALESCE(( SELECT weekly_nonmanual_has_any.has_any
                           FROM weekly_nonmanual_has_any), false) = false THEN 1
                        ELSE 0
                    END
                    ELSE 0
                END AS missing_count,
                CASE
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND COALESCE(( SELECT segs_missing_locked.cnt
                       FROM segs_missing_locked), 0) > 0 THEN true
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND COALESCE(( SELECT segs_missing_locked.cnt
                       FROM segs_missing_locked), 0) = 0 THEN false
                    WHEN ts.sheet_scope = 'DAILY'::timesheet_scope_enum THEN ts.reference_number IS NULL OR length(btrim(ts.reference_number)) = 0
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode = 'MANUAL'::submission_mode_enum THEN COALESCE(( SELECT weekly_manual_missing.cnt
                       FROM weekly_manual_missing), 0) > 0
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode <> 'MANUAL'::submission_mode_enum THEN COALESCE(( SELECT weekly_nonmanual_has_any.has_any
                       FROM weekly_nonmanual_has_any), false) = false
                    ELSE false
                END AS issue_missing_raw,
                CASE
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text THEN COALESCE(( SELECT segs_missing_locked.cnt
                       FROM segs_missing_locked), 0)
                    WHEN ts.sheet_scope = 'DAILY'::timesheet_scope_enum THEN
                    CASE
                        WHEN ts.reference_number IS NULL OR length(btrim(ts.reference_number)) = 0 THEN 1
                        ELSE 0
                    END
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode = 'MANUAL'::submission_mode_enum THEN COALESCE(( SELECT weekly_manual_missing.cnt
                       FROM weekly_manual_missing), 0)
                    WHEN ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND ts.submission_mode <> 'MANUAL'::submission_mode_enum THEN
                    CASE
                        WHEN COALESCE(( SELECT weekly_nonmanual_has_any.has_any
                           FROM weekly_nonmanual_has_any), false) = false THEN 1
                        ELSE 0
                    END
                    ELSE 0
                END AS issue_missing_count) refchk ON true;;
alter view public.v_ts_invoice_precheck owner to postgres;

-- public.v_timesheets_details
create or replace view public.v_timesheets_details with (security_invoker=true) as
WITH nhsp_agg AS (
         SELECT s.timesheet_id,
            count(*) AS nhsp_shift_count,
            count(*) FILTER (WHERE s.invoice_status = 'INCLUDED'::text) AS nhsp_shift_included_count,
            count(*) FILTER (WHERE s.invoice_status = 'DEFERRED'::text) AS nhsp_shift_deferred_count
           FROM nhsp_shifts s
          WHERE s.timesheet_id IS NOT NULL
          GROUP BY s.timesheet_id
        )
 SELECT t.timesheet_id,
    t.booking_id,
    t.contract_id,
    tf.candidate_id,
    tf.client_id,
    t.week_ending_date,
    t.sheet_scope,
    t.submission_mode,
    t.status AS timesheet_status,
    t.reference_number,
    t.occupant_key_norm,
    t.hospital_norm,
    t.ward_norm,
    t.job_title_norm,
    t.shift_label_norm,
    t.authorised_at_server,
    tf.id AS tsfin_id,
    tf.basis AS tsfin_basis,
    tf.processing_status,
    tf.pay_method,
    tf.total_hours,
    tf.hours_day,
    tf.hours_night,
    tf.hours_sat,
    tf.hours_sun,
    tf.hours_bh,
    tf.total_pay_ex_vat,
    tf.total_charge_ex_vat,
    tf.margin_ex_vat,
    tf.expenses_pay_ex_vat,
    tf.expenses_charge_ex_vat,
    tf.mileage_pay_ex_vat,
    tf.mileage_charge_ex_vat,
    tf.invoice_breakdown_json,
    tf.locked_by_invoice_id,
    tf.paid_at_utc,
    tv.status AS validation_status,
    tv.reason_code AS validation_reason_code,
    tv.hr_request_id,
    tv.validated_at_utc,
    tv.last_source AS validation_last_source_import_id,
    n.nhsp_shift_count,
    n.nhsp_shift_included_count,
    n.nhsp_shift_deferred_count,
    tf.expenses_description,
    tf.expenses_evidence_r2_key,
    tf.expenses_evidence_manifest,
    tf.mileage_units,
    tf.mileage_pay_rate,
    tf.mileage_charge_rate,
    tf.mileage_evidence_r2_key,
    tf.mileage_evidence_manifest,
    tf.travel_pay_ex_vat,
    tf.travel_charge_ex_vat,
    tf.accommodation_pay_ex_vat,
    tf.accommodation_charge_ex_vat,
    tf.other_pay_ex_vat,
    tf.other_charge_ex_vat,
        CASE
            WHEN t.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE'::text THEN true
            ELSE false
        END AS refs_block_invoicing,
        CASE
            WHEN t.timesheet_id IS NOT NULL AND COALESCE(pc.issue_missing_reference, false) = true THEN true
            ELSE false
        END AS refs_block_issuing_invoices,
        CASE
            WHEN t.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE'::text AND COALESCE(pc.issue_missing_reference, false) = true THEN true
            ELSE false
        END AS refs_block_invoice_and_issuing,
    refsdet.refs_missing_items_count,
    refsdet.refs_missing_items_json
   FROM timesheets t
     LEFT JOIN timesheets_financials tf ON tf.timesheet_id = t.timesheet_id AND tf.is_current = true
     LEFT JOIN timesheet_validations tv ON tv.timesheet_id = t.timesheet_id
     LEFT JOIN nhsp_agg n ON n.timesheet_id = t.timesheet_id
     LEFT JOIN v_ts_invoice_precheck pc ON pc.timesheet_id = t.timesheet_id
     LEFT JOIN LATERAL ( SELECT x.items AS refs_missing_items_json,
            COALESCE(jsonb_array_length(x.items), 0) AS refs_missing_items_count
           FROM ( SELECT
                        CASE
                            WHEN t.timesheet_id IS NOT NULL AND (pc.precheck_status = 'BLOCK_NO_REFERENCE'::text OR COALESCE(pc.issue_missing_reference, false) = true) THEN COALESCE(( SELECT jsonb_agg(q.item ORDER BY (q.item ->> 'day_ymd'::text), (q.item ->> 'kind'::text), (COALESCE(NULLIF(q.item ->> 'segment_index'::text, ''::text)::integer, 0))) AS jsonb_agg
                               FROM ( SELECT jsonb_build_object('kind', 'TIMESHEET', 'day_ymd', to_char((COALESCE(t.worked_start_iso, t.scheduled_start_iso) AT TIME ZONE 'Europe/London'::text)::date::timestamp with time zone, 'YYYY-MM-DD'::text), 'start_utc', COALESCE(t.worked_start_iso, t.scheduled_start_iso), 'end_utc', COALESCE(t.worked_end_iso, t.scheduled_end_iso), 'current_reference', NULLIF(btrim(COALESCE(t.reference_number, ''::text)), ''::text)) AS item
                                      WHERE t.sheet_scope = 'DAILY'::timesheet_scope_enum AND (t.reference_number IS NULL OR btrim(t.reference_number) = ''::text)
                                    UNION ALL
                                     SELECT jsonb_build_object('kind', 'FREEFORM', 'day_ymd', j.k, 'current_reference', NULLIF(btrim(COALESCE(j.v, ''::text)), ''::text)) AS item
                                       FROM jsonb_each_text(
 CASE
  WHEN t.day_references_json IS NOT NULL AND jsonb_typeof(t.day_references_json) = 'object'::text THEN t.day_references_json
  ELSE '{}'::jsonb
 END) j(k, v)
                                      WHERE "left"(COALESCE(j.k, ''::text), 2) <> '__'::text AND (j.v IS NULL OR btrim(j.v) = ''::text OR lower(btrim(j.v)) = 'null'::text)
                                    UNION ALL
                                     SELECT jsonb_build_object('kind', 'SEGMENT', 'segment_index', s.ord - 1, 'segment_id', NULLIF(btrim(COALESCE(s.seg ->> 'segment_id'::text, ''::text)), ''::text), 'day_ymd', NULLIF(btrim(COALESCE(s.seg ->> 'date'::text, ''::text)), ''::text), 'start_utc', NULLIF(btrim(COALESCE(s.seg ->> 'start_utc'::text, ''::text)), ''::text), 'end_utc', NULLIF(btrim(COALESCE(s.seg ->> 'end_utc'::text, ''::text)), ''::text), 'start', NULLIF(btrim(COALESCE(s.seg ->> 'start_utc'::text, ''::text)), ''::text), 'end', NULLIF(btrim(COALESCE(s.seg ->> 'end_utc'::text, ''::text)), ''::text), 'current_reference', NULLIF(btrim(COALESCE(s.seg ->> 'ref_num'::text, ''::text)), ''::text), 'locked_by_invoice_id', NULLIF(btrim(COALESCE(s.seg ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text), 'scope',
 CASE
  WHEN NULLIF(btrim(COALESCE(s.seg ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) IS NULL THEN 'INVOICING'::text
  ELSE 'ISSUING'::text
 END) AS item
                                       FROM jsonb_array_elements(
 CASE
  WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments'::text) = 'array'::text THEN tf.invoice_breakdown_json -> 'segments'::text
  ELSE '[]'::jsonb
 END) WITH ORDINALITY s(seg, ord)
                                      WHERE jsonb_typeof(s.seg) = 'object'::text AND ((COALESCE(NULLIF(s.seg ->> 'hours_day'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_night'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_sat'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_sun'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_bh'::text, ''::text)::numeric, 0::numeric)) > 0::numeric OR COALESCE(NULLIF(s.seg ->> 'charge_amount'::text, ''::text)::numeric, 0::numeric) > 0::numeric) AND COALESCE(btrim(COALESCE(s.seg ->> 'ref_num'::text, ''::text)), ''::text) = ''::text AND (pc.precheck_status = 'BLOCK_NO_REFERENCE'::text AND NULLIF(btrim(COALESCE(s.seg ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) IS NULL OR COALESCE(pc.issue_missing_reference, false) = true AND NULLIF(btrim(COALESCE(s.seg ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) IS NOT NULL)
                                    UNION ALL
                                     SELECT jsonb_build_object('kind', 'SEGMENT', 'segment_index', s.ord - 1, 'day_ymd',
 CASE
  WHEN s.seg ? 'date_ymd'::text THEN NULLIF(btrim(COALESCE(s.seg ->> 'date_ymd'::text, ''::text)), ''::text)
  WHEN SUBSTRING(COALESCE(s.seg ->> 'start'::text, ''::text) FROM 1 FOR 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'::text THEN SUBSTRING(COALESCE(s.seg ->> 'start'::text, ''::text) FROM 1 FOR 10)
  ELSE NULL::text
 END, 'start', NULLIF(btrim(COALESCE(s.seg ->> 'start'::text, ''::text)), ''::text), 'end', NULLIF(btrim(COALESCE(s.seg ->> 'end'::text, ''::text)), ''::text), 'current_reference', NULLIF(btrim(COALESCE(s.seg ->> 'ref_num'::text, ''::text)), ''::text)) AS item
                                       FROM jsonb_array_elements(
 CASE
  WHEN t.actual_schedule_json IS NOT NULL AND jsonb_typeof(t.actual_schedule_json) = 'array'::text THEN t.actual_schedule_json
  ELSE '[]'::jsonb
 END) WITH ORDINALITY s(seg, ord)
                                      WHERE COALESCE(btrim(COALESCE(s.seg ->> 'start'::text, ''::text)), ''::text) <> ''::text AND COALESCE(btrim(COALESCE(s.seg ->> 'end'::text, ''::text)), ''::text) <> ''::text AND COALESCE(btrim(COALESCE(s.seg ->> 'ref_num'::text, ''::text)), ''::text) = ''::text AND (NOT (s.seg ? 'hours_day'::text OR s.seg ? 'hours_night'::text OR s.seg ? 'hours_sat'::text OR s.seg ? 'hours_sun'::text OR s.seg ? 'hours_bh'::text) OR (COALESCE(NULLIF(s.seg ->> 'hours_day'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_night'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_sat'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_sun'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.seg ->> 'hours_bh'::text, ''::text)::numeric, 0::numeric)) > 0::numeric)) q), '[]'::jsonb)
                            ELSE '[]'::jsonb
                        END AS items) x) refsdet ON true;;
alter view public.v_timesheets_details owner to postgres;

-- public.v_timesheets_summary_base
create or replace view public.v_timesheets_summary_base with (security_invoker=true) as
WITH latest_tsfin AS (
         SELECT DISTINCT ON (tf.timesheet_id) tf.id,
            tf.timesheet_id,
            tf.candidate_id,
            tf.client_id,
            tf.pay_method,
            tf.processing_status,
            tf.basis,
            tf.total_hours,
            tf.total_pay_ex_vat,
            tf.total_charge_ex_vat,
            tf.margin_ex_vat,
            tf.paid_at_utc,
            tf.pay_on_hold,
            tf.locked_by_invoice_id,
            tf.has_rate_issue,
            tf.has_pay_channel_issue,
            tf.hr_crosscheck_status,
            tf.hr_crosscheck_issues,
            tf.external_source_rows_json,
            tf.invoice_breakdown_json,
            tf.expenses_charge_ex_vat,
            tf.expenses_evidence_r2_key,
            tf.expenses_evidence_manifest,
            tf.mileage_charge_ex_vat,
            tf.mileage_evidence_r2_key,
            tf.mileage_evidence_manifest,
            tf.expenses_pay_ex_vat,
            tf.expenses_description,
            tf.mileage_units,
            tf.mileage_pay_rate,
            tf.mileage_charge_rate,
            tf.mileage_pay_ex_vat,
            tf.travel_pay_ex_vat,
            tf.travel_charge_ex_vat,
            tf.accommodation_pay_ex_vat,
            tf.accommodation_charge_ex_vat,
            tf.other_pay_ex_vat,
            tf.other_charge_ex_vat,
            tf.computed_at_utc,
            tf.created_at
           FROM timesheets_financials tf
          WHERE tf.is_current = true
          ORDER BY tf.timesheet_id, tf.created_at DESC
        ), validations_latest AS (
         SELECT DISTINCT ON (tv.timesheet_id) tv.timesheet_id,
            tv.status,
            tv.reason_code
           FROM timesheet_validations tv
          ORDER BY tv.timesheet_id, tv.created_at DESC
        ), nhsp_agg AS (
         SELECT ns.timesheet_id,
            count(*)::integer AS nhsp_shift_count,
            count(*) FILTER (WHERE ns.invoice_status = 'INCLUDED'::text)::integer AS nhsp_shift_included_count,
            count(*) FILTER (WHERE ns.invoice_status = 'DEFERRED'::text)::integer AS nhsp_shift_deferred_count
           FROM nhsp_shifts ns
          GROUP BY ns.timesheet_id
        ), pay_adj AS (
         SELECT pa.timesheet_id,
            count(*)::integer AS pay_adjustment_count
           FROM ts_pay_adjustments pa
          GROUP BY pa.timesheet_id
        ), evidence_agg AS (
         SELECT te.timesheet_id,
            count(*)::integer AS evidence_count
           FROM timesheet_evidence te
          GROUP BY te.timesheet_id
        ), client_hr AS (
         SELECT cs.client_id,
            bool_or(cs.autoprocess_hr) AS autoprocess_hr,
            bool_or(cs.requires_hr) AS requires_hr,
            bool_or(cs.no_timesheet_required) AS no_timesheet_required,
            bool_or(cs.pay_reference_required) AS pay_reference_required,
            bool_or(cs.invoice_reference_required) AS invoice_reference_required,
            bool_or(cs.reference_number_required_to_issue_invoice) AS reference_number_required_to_issue_invoice,
            bool_or(cs.hr_validation_required) AS hr_validation_required,
            bool_or(cs.ts_reference_required) AS ts_reference_required,
            bool_or(cs.is_nhsp) AS is_nhsp
           FROM client_settings cs
          GROUP BY cs.client_id
        ), ts_base AS (
         SELECT ts.timesheet_id,
            ts.status AS timesheet_status,
            ts.week_ending_date,
            ts.booking_id,
            ts.occupant_key_norm,
            ts.hospital_norm,
            ts.sheet_scope,
            ts.submission_mode,
            ts.authorised_at_server,
            COALESCE(tf.candidate_id, ct.candidate_id) AS candidate_id,
            COALESCE(tf.client_id, ct.client_id) AS client_id,
            tf.pay_method,
            tf.processing_status,
            tf.basis,
            tf.total_hours,
            tf.total_pay_ex_vat,
            tf.total_charge_ex_vat,
            tf.margin_ex_vat,
            tf.paid_at_utc,
            tf.pay_on_hold,
            tf.locked_by_invoice_id,
                CASE
                    WHEN COALESCE(tf.candidate_id, ct.candidate_id) IS NULL AND ts.candidate_hint_text IS NOT NULL AND jsonb_typeof(ts.candidate_hint_text) = 'object'::text AND (NULLIF(btrim(concat_ws(' '::text, NULLIF(btrim(ts.candidate_hint_text ->> 'first_name'::text), ''::text), NULLIF(btrim(ts.candidate_hint_text ->> 'surname'::text), ''::text))), ''::text) IS NOT NULL OR NULLIF(btrim(ts.candidate_hint_text ->> 'display_name'::text), ''::text) IS NOT NULL OR NULLIF(btrim(ts.candidate_hint_text ->> 'email'::text), ''::text) IS NOT NULL) THEN ('Unresolved Timesheet - '::text || COALESCE(NULLIF(btrim(concat_ws(' '::text, NULLIF(btrim(ts.candidate_hint_text ->> 'first_name'::text), ''::text), NULLIF(btrim(ts.candidate_hint_text ->> 'surname'::text), ''::text))), ''::text), NULLIF(btrim(ts.candidate_hint_text ->> 'display_name'::text), ''::text), 'Candidate'::text)) ||
                    CASE
                        WHEN NULLIF(btrim(ts.candidate_hint_text ->> 'email'::text), ''::text) IS NOT NULL THEN ', Email - '::text || btrim(ts.candidate_hint_text ->> 'email'::text)
                        ELSE ''::text
                    END
                    ELSE COALESCE(c.display_name, ts.occupant_key_norm)
                END AS candidate_name,
            cli.name AS client_name,
            COALESCE(na.nhsp_shift_count, 0) AS nhsp_shift_count,
            COALESCE(na.nhsp_shift_included_count, 0) AS nhsp_shift_included_count,
            COALESCE(na.nhsp_shift_deferred_count, 0) AS nhsp_shift_deferred_count,
            vl.status AS validation_status,
            cw.id AS contract_week_id,
            cw.week_ending_date AS contract_week_ending_date,
            cw.status AS contract_week_status,
            cw.additional_seq,
            COALESCE(ts.is_adjustment, false) = true OR COALESCE(cw.is_adjustment, false) = true OR COALESCE(cw.additional_seq, 0) > 0 OR ts.parent_timesheet_id IS NOT NULL OR ts.correction_id IS NOT NULL OR ts.correction_kind IS NOT NULL AS is_adjustment,
            ts.qr_status,
            ts.qr_token,
            ts.qr_generated_at,
            ts.qr_scanned_at,
            COALESCE(pa.pay_adjustment_count, 0) AS pay_adjustment_count,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.autoprocess_hr
                    ELSE NULL::boolean
                END, ch.autoprocess_hr, false) AS client_autoprocess_hr,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.requires_hr
                    ELSE NULL::boolean
                END, ch.requires_hr, false) AS client_requires_hr,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.no_timesheet_required
                    ELSE NULL::boolean
                END, ch.no_timesheet_required, false) AS client_no_timesheet_required,
            COALESCE(ch.pay_reference_required, false) AS client_pay_reference_required,
            COALESCE(ch.invoice_reference_required, false) AS client_invoice_reference_required,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.requires_hr
                    ELSE NULL::boolean
                END, ch.hr_validation_required, false) AS client_hr_validation_required,
            COALESCE(ch.ts_reference_required, false) AS client_ts_reference_required,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.is_nhsp
                    ELSE NULL::boolean
                END, ch.is_nhsp, false) AS client_is_nhsp,
            tf.has_rate_issue,
            tf.has_pay_channel_issue,
            tf.hr_crosscheck_status,
            tf.hr_crosscheck_issues,
            tf.external_source_rows_json,
            tf.invoice_breakdown_json,
            ts.reference_number,
            ts.day_references_json,
            ts.actual_schedule_json,
            ts.r2_nurse_key,
            ts.r2_auth_key,
            ts.manual_pdf_r2_key,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.require_reference_to_pay
                    ELSE NULL::boolean
                END, ch.pay_reference_required, false) AS require_reference_to_pay,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.require_reference_to_invoice
                    ELSE NULL::boolean
                END, ch.invoice_reference_required, false) AS require_reference_to_invoice,
            COALESCE(ea.evidence_count, 0) AS evidence_count,
            tf.expenses_charge_ex_vat,
            tf.expenses_evidence_r2_key,
            tf.expenses_evidence_manifest,
            tf.mileage_charge_ex_vat,
            tf.mileage_evidence_r2_key,
            tf.mileage_evidence_manifest,
            cand.pay_method AS cand_pay_method,
            cand.account_holder AS cand_account_holder,
            cand.sort_code AS cand_sort_code,
            cand.account_number AS cand_account_number,
            cand.umbrella_id AS cand_umbrella_id,
            umb.enabled AS umb_enabled,
            umb.name AS umb_name,
            umb.sort_code AS umb_sort_code,
            umb.account_number AS umb_account_number,
            ts.candidate_hint_text,
            tf.expenses_pay_ex_vat,
            tf.expenses_description,
            tf.mileage_units,
            tf.mileage_pay_rate,
            tf.mileage_charge_rate,
            tf.mileage_pay_ex_vat,
            tf.travel_pay_ex_vat,
            tf.travel_charge_ex_vat,
            tf.accommodation_pay_ex_vat,
            tf.accommodation_charge_ex_vat,
            tf.other_pay_ex_vat,
            tf.other_charge_ex_vat,
            ts.generated_pdf_at_utc,
            ts.generated_pdf_refs_sig,
            ts.qr_sent_refs_sig,
            ts.qr_last_sent_hash
           FROM timesheets ts
             LEFT JOIN contract_weeks cw ON cw.timesheet_id = ts.timesheet_id
             LEFT JOIN contracts ct ON ct.id = COALESCE(ts.contract_id, cw.contract_id)
             LEFT JOIN latest_tsfin tf ON tf.timesheet_id = ts.timesheet_id
             LEFT JOIN candidates c ON c.id = COALESCE(tf.candidate_id, ct.candidate_id)
             LEFT JOIN clients cli ON cli.id = COALESCE(tf.client_id, ct.client_id)
             LEFT JOIN client_hr ch ON ch.client_id = COALESCE(tf.client_id, ct.client_id)
             LEFT JOIN nhsp_agg na ON na.timesheet_id = ts.timesheet_id
             LEFT JOIN pay_adj pa ON pa.timesheet_id = ts.timesheet_id
             LEFT JOIN validations_latest vl ON vl.timesheet_id = ts.timesheet_id
             LEFT JOIN evidence_agg ea ON ea.timesheet_id = ts.timesheet_id
             LEFT JOIN candidates cand ON cand.id = COALESCE(tf.candidate_id, ct.candidate_id)
             LEFT JOIN umbrellas umb ON umb.id = cand.umbrella_id
          WHERE ts.is_current = true
        ), planned_weeks AS (
         SELECT NULL::uuid AS timesheet_id,
            NULL::timesheet_status_enum AS timesheet_status,
            cw.week_ending_date,
            NULL::text AS booking_id,
            NULL::text AS occupant_key_norm,
            NULL::text AS hospital_norm,
            'WEEKLY'::timesheet_scope_enum AS sheet_scope,
            cw.submission_mode_snapshot AS submission_mode,
            NULL::timestamp with time zone AS authorised_at_server,
            ct.candidate_id,
            ct.client_id,
            NULL::text AS pay_method,
            NULL::ts_fin_processing_status_enum AS processing_status,
            NULL::timesheet_fin_basis_enum AS basis,
            round(COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'day'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'night'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'sat'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'sun'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'bh'::text, ''::text)::numeric, 0::numeric), 2) AS total_hours,
            NULL::numeric AS total_pay_ex_vat,
            NULL::numeric AS total_charge_ex_vat,
            NULL::numeric AS margin_ex_vat,
            NULL::timestamp with time zone AS paid_at_utc,
            false AS pay_on_hold,
            NULL::uuid AS locked_by_invoice_id,
            cand.display_name AS candidate_name,
            cli.name AS client_name,
            0 AS nhsp_shift_count,
            0 AS nhsp_shift_included_count,
            0 AS nhsp_shift_deferred_count,
            NULL::validation_status_enum AS validation_status,
            cw.id AS contract_week_id,
            cw.week_ending_date AS contract_week_ending_date,
            cw.status AS contract_week_status,
            cw.additional_seq,
            COALESCE(cw.is_adjustment, false) = true OR COALESCE(cw.additional_seq, 0) > 0 AS is_adjustment,
            NULL::timesheet_qr_status_enum AS qr_status,
            NULL::text AS qr_token,
            NULL::timestamp with time zone AS qr_generated_at,
            NULL::timestamp with time zone AS qr_scanned_at,
            0 AS pay_adjustment_count,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.autoprocess_hr
                    ELSE NULL::boolean
                END, ch.autoprocess_hr, false) AS client_autoprocess_hr,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.requires_hr
                    ELSE NULL::boolean
                END, ch.requires_hr, false) AS client_requires_hr,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.no_timesheet_required
                    ELSE NULL::boolean
                END, ch.no_timesheet_required, false) AS client_no_timesheet_required,
            COALESCE(ch.pay_reference_required, false) AS client_pay_reference_required,
            COALESCE(ch.invoice_reference_required, false) AS client_invoice_reference_required,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.requires_hr
                    ELSE NULL::boolean
                END, ch.hr_validation_required, false) AS client_hr_validation_required,
            COALESCE(ch.ts_reference_required, false) AS client_ts_reference_required,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.is_nhsp
                    ELSE NULL::boolean
                END, ch.is_nhsp, false) AS client_is_nhsp,
            false AS has_rate_issue,
            false AS has_pay_channel_issue,
            NULL::text AS hr_crosscheck_status,
            NULL::text[] AS hr_crosscheck_issues,
            NULL::jsonb AS external_source_rows_json,
            NULL::jsonb AS invoice_breakdown_json,
            NULL::text AS reference_number,
            NULL::jsonb AS day_references_json,
            NULL::jsonb AS actual_schedule_json,
            NULL::text AS r2_nurse_key,
            NULL::text AS r2_auth_key,
            NULL::text AS manual_pdf_r2_key,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.require_reference_to_pay
                    ELSE NULL::boolean
                END, ch.pay_reference_required, false) AS require_reference_to_pay,
            COALESCE(
                CASE
                    WHEN ct.overrideclientsettings THEN ct.require_reference_to_invoice
                    ELSE NULL::boolean
                END, ch.invoice_reference_required, false) AS require_reference_to_invoice,
            0 AS evidence_count,
            NULL::numeric AS expenses_charge_ex_vat,
            NULL::text AS expenses_evidence_r2_key,
            NULL::jsonb AS expenses_evidence_manifest,
            NULL::numeric AS mileage_charge_ex_vat,
            NULL::text AS mileage_evidence_r2_key,
            NULL::jsonb AS mileage_evidence_manifest,
            NULL::text AS cand_pay_method,
            NULL::text AS cand_account_holder,
            NULL::text AS cand_sort_code,
            NULL::text AS cand_account_number,
            NULL::uuid AS cand_umbrella_id,
            NULL::boolean AS umb_enabled,
            NULL::text AS umb_name,
            NULL::text AS umb_sort_code,
            NULL::text AS umb_account_number,
            NULL::jsonb AS candidate_hint_text,
            NULL::numeric AS expenses_pay_ex_vat,
            NULL::text AS expenses_description,
            NULL::numeric AS mileage_units,
            NULL::numeric AS mileage_pay_rate,
            NULL::numeric AS mileage_charge_rate,
            NULL::numeric AS mileage_pay_ex_vat,
            NULL::numeric AS travel_pay_ex_vat,
            NULL::numeric AS travel_charge_ex_vat,
            NULL::numeric AS accommodation_pay_ex_vat,
            NULL::numeric AS accommodation_charge_ex_vat,
            NULL::numeric AS other_pay_ex_vat,
            NULL::numeric AS other_charge_ex_vat,
            NULL::timestamp with time zone AS generated_pdf_at_utc,
            NULL::text AS generated_pdf_refs_sig,
            NULL::text AS qr_sent_refs_sig,
            NULL::text AS qr_last_sent_hash
           FROM contract_weeks cw
             JOIN contracts ct ON ct.id = cw.contract_id
             LEFT JOIN candidates cand ON cand.id = ct.candidate_id
             LEFT JOIN clients cli ON cli.id = ct.client_id
             LEFT JOIN client_hr ch ON ch.client_id = ct.client_id
          WHERE cw.timesheet_id IS NULL
        ), all_rows AS (
         SELECT ts_base.timesheet_id,
            ts_base.timesheet_status,
            ts_base.week_ending_date,
            ts_base.booking_id,
            ts_base.occupant_key_norm,
            ts_base.hospital_norm,
            ts_base.sheet_scope,
            ts_base.submission_mode,
            ts_base.authorised_at_server,
            ts_base.candidate_id,
            ts_base.client_id,
            ts_base.pay_method,
            ts_base.processing_status,
            ts_base.basis,
            ts_base.total_hours,
            ts_base.total_pay_ex_vat,
            ts_base.total_charge_ex_vat,
            ts_base.margin_ex_vat,
            ts_base.paid_at_utc,
            ts_base.pay_on_hold,
            ts_base.locked_by_invoice_id,
            ts_base.candidate_name,
            ts_base.client_name,
            ts_base.nhsp_shift_count,
            ts_base.nhsp_shift_included_count,
            ts_base.nhsp_shift_deferred_count,
            ts_base.validation_status,
            ts_base.contract_week_id,
            ts_base.contract_week_ending_date,
            ts_base.contract_week_status,
            ts_base.additional_seq,
            ts_base.is_adjustment,
            ts_base.qr_status,
            ts_base.qr_token,
            ts_base.qr_generated_at,
            ts_base.qr_scanned_at,
            ts_base.pay_adjustment_count,
            ts_base.client_autoprocess_hr,
            ts_base.client_requires_hr,
            ts_base.client_no_timesheet_required,
            ts_base.client_pay_reference_required,
            ts_base.client_invoice_reference_required,
            ts_base.client_hr_validation_required,
            ts_base.client_ts_reference_required,
            ts_base.client_is_nhsp,
            ts_base.has_rate_issue,
            ts_base.has_pay_channel_issue,
            ts_base.hr_crosscheck_status,
            ts_base.hr_crosscheck_issues,
            ts_base.external_source_rows_json,
            ts_base.invoice_breakdown_json,
            ts_base.reference_number,
            ts_base.day_references_json,
            ts_base.actual_schedule_json,
            ts_base.r2_nurse_key,
            ts_base.r2_auth_key,
            ts_base.manual_pdf_r2_key,
            ts_base.require_reference_to_pay,
            ts_base.require_reference_to_invoice,
            ts_base.evidence_count,
            ts_base.expenses_charge_ex_vat,
            ts_base.expenses_evidence_r2_key,
            ts_base.expenses_evidence_manifest,
            ts_base.mileage_charge_ex_vat,
            ts_base.mileage_evidence_r2_key,
            ts_base.mileage_evidence_manifest,
            ts_base.cand_pay_method,
            ts_base.cand_account_holder,
            ts_base.cand_sort_code,
            ts_base.cand_account_number,
            ts_base.cand_umbrella_id,
            ts_base.umb_enabled,
            ts_base.umb_name,
            ts_base.umb_sort_code,
            ts_base.umb_account_number,
            ts_base.candidate_hint_text,
            ts_base.expenses_pay_ex_vat,
            ts_base.expenses_description,
            ts_base.mileage_units,
            ts_base.mileage_pay_rate,
            ts_base.mileage_charge_rate,
            ts_base.mileage_pay_ex_vat,
            ts_base.travel_pay_ex_vat,
            ts_base.travel_charge_ex_vat,
            ts_base.accommodation_pay_ex_vat,
            ts_base.accommodation_charge_ex_vat,
            ts_base.other_pay_ex_vat,
            ts_base.other_charge_ex_vat,
            ts_base.generated_pdf_at_utc,
            ts_base.generated_pdf_refs_sig,
            ts_base.qr_sent_refs_sig,
            ts_base.qr_last_sent_hash
           FROM ts_base
        UNION ALL
         SELECT planned_weeks.timesheet_id,
            planned_weeks.timesheet_status,
            planned_weeks.week_ending_date,
            planned_weeks.booking_id,
            planned_weeks.occupant_key_norm,
            planned_weeks.hospital_norm,
            planned_weeks.sheet_scope,
            planned_weeks.submission_mode,
            planned_weeks.authorised_at_server,
            planned_weeks.candidate_id,
            planned_weeks.client_id,
            planned_weeks.pay_method,
            planned_weeks.processing_status,
            planned_weeks.basis,
            planned_weeks.total_hours,
            planned_weeks.total_pay_ex_vat,
            planned_weeks.total_charge_ex_vat,
            planned_weeks.margin_ex_vat,
            planned_weeks.paid_at_utc,
            planned_weeks.pay_on_hold,
            planned_weeks.locked_by_invoice_id,
            planned_weeks.candidate_name,
            planned_weeks.client_name,
            planned_weeks.nhsp_shift_count,
            planned_weeks.nhsp_shift_included_count,
            planned_weeks.nhsp_shift_deferred_count,
            planned_weeks.validation_status,
            planned_weeks.contract_week_id,
            planned_weeks.contract_week_ending_date,
            planned_weeks.contract_week_status,
            planned_weeks.additional_seq,
            planned_weeks.is_adjustment,
            planned_weeks.qr_status,
            planned_weeks.qr_token,
            planned_weeks.qr_generated_at,
            planned_weeks.qr_scanned_at,
            planned_weeks.pay_adjustment_count,
            planned_weeks.client_autoprocess_hr,
            planned_weeks.client_requires_hr,
            planned_weeks.client_no_timesheet_required,
            planned_weeks.client_pay_reference_required,
            planned_weeks.client_invoice_reference_required,
            planned_weeks.client_hr_validation_required,
            planned_weeks.client_ts_reference_required,
            planned_weeks.client_is_nhsp,
            planned_weeks.has_rate_issue,
            planned_weeks.has_pay_channel_issue,
            planned_weeks.hr_crosscheck_status,
            planned_weeks.hr_crosscheck_issues,
            planned_weeks.external_source_rows_json,
            planned_weeks.invoice_breakdown_json,
            planned_weeks.reference_number,
            planned_weeks.day_references_json,
            planned_weeks.actual_schedule_json,
            planned_weeks.r2_nurse_key,
            planned_weeks.r2_auth_key,
            planned_weeks.manual_pdf_r2_key,
            planned_weeks.require_reference_to_pay,
            planned_weeks.require_reference_to_invoice,
            planned_weeks.evidence_count,
            planned_weeks.expenses_charge_ex_vat,
            planned_weeks.expenses_evidence_r2_key,
            planned_weeks.expenses_evidence_manifest,
            planned_weeks.mileage_charge_ex_vat,
            planned_weeks.mileage_evidence_r2_key,
            planned_weeks.mileage_evidence_manifest,
            planned_weeks.cand_pay_method,
            planned_weeks.cand_account_holder,
            planned_weeks.cand_sort_code,
            planned_weeks.cand_account_number,
            planned_weeks.cand_umbrella_id,
            planned_weeks.umb_enabled,
            planned_weeks.umb_name,
            planned_weeks.umb_sort_code,
            planned_weeks.umb_account_number,
            planned_weeks.candidate_hint_text,
            planned_weeks.expenses_pay_ex_vat,
            planned_weeks.expenses_description,
            planned_weeks.mileage_units,
            planned_weeks.mileage_pay_rate,
            planned_weeks.mileage_charge_rate,
            planned_weeks.mileage_pay_ex_vat,
            planned_weeks.travel_pay_ex_vat,
            planned_weeks.travel_charge_ex_vat,
            planned_weeks.accommodation_pay_ex_vat,
            planned_weeks.accommodation_charge_ex_vat,
            planned_weeks.other_pay_ex_vat,
            planned_weeks.other_charge_ex_vat,
            planned_weeks.generated_pdf_at_utc,
            planned_weeks.generated_pdf_refs_sig,
            planned_weeks.qr_sent_refs_sig,
            planned_weeks.qr_last_sent_hash
           FROM planned_weeks
        ), with_issues AS (
         SELECT ar.timesheet_id,
            ar.timesheet_status,
            ar.week_ending_date,
            ar.booking_id,
            ar.occupant_key_norm,
            ar.hospital_norm,
            ar.sheet_scope,
            ar.submission_mode,
            ar.authorised_at_server,
            ar.candidate_id,
            ar.client_id,
            ar.pay_method,
            ar.processing_status,
            ar.basis,
            ar.total_hours,
            ar.total_pay_ex_vat,
            ar.total_charge_ex_vat,
            ar.margin_ex_vat,
            ar.paid_at_utc,
            ar.pay_on_hold,
            ar.locked_by_invoice_id,
            ar.candidate_name,
            ar.client_name,
            ar.nhsp_shift_count,
            ar.nhsp_shift_included_count,
            ar.nhsp_shift_deferred_count,
            ar.validation_status,
            ar.contract_week_id,
            ar.contract_week_ending_date,
            ar.contract_week_status,
            ar.additional_seq,
            ar.is_adjustment,
            ar.qr_status,
            ar.qr_token,
            ar.qr_generated_at,
            ar.qr_scanned_at,
            ar.pay_adjustment_count,
            ar.client_autoprocess_hr,
            ar.client_requires_hr,
            ar.client_no_timesheet_required,
            ar.client_pay_reference_required,
            ar.client_invoice_reference_required,
            ar.client_hr_validation_required,
            ar.client_ts_reference_required,
            ar.client_is_nhsp,
            ar.has_rate_issue,
            ar.has_pay_channel_issue,
            ar.hr_crosscheck_status,
            ar.hr_crosscheck_issues,
            ar.external_source_rows_json,
            ar.invoice_breakdown_json,
            ar.reference_number,
            ar.day_references_json,
            ar.actual_schedule_json,
            ar.r2_nurse_key,
            ar.r2_auth_key,
            ar.manual_pdf_r2_key,
            ar.require_reference_to_pay,
            ar.require_reference_to_invoice,
            ar.evidence_count,
            ar.expenses_charge_ex_vat,
            ar.expenses_evidence_r2_key,
            ar.expenses_evidence_manifest,
            ar.mileage_charge_ex_vat,
            ar.mileage_evidence_r2_key,
            ar.mileage_evidence_manifest,
            ar.cand_pay_method,
            ar.cand_account_holder,
            ar.cand_sort_code,
            ar.cand_account_number,
            ar.cand_umbrella_id,
            ar.umb_enabled,
            ar.umb_name,
            ar.umb_sort_code,
            ar.umb_account_number,
            ar.candidate_hint_text,
            ar.expenses_pay_ex_vat,
            ar.expenses_description,
            ar.mileage_units,
            ar.mileage_pay_rate,
            ar.mileage_charge_rate,
            ar.mileage_pay_ex_vat,
            ar.travel_pay_ex_vat,
            ar.travel_charge_ex_vat,
            ar.accommodation_pay_ex_vat,
            ar.accommodation_charge_ex_vat,
            ar.other_pay_ex_vat,
            ar.other_charge_ex_vat,
            ar.generated_pdf_at_utc,
            ar.generated_pdf_refs_sig,
            ar.qr_sent_refs_sig,
            ar.qr_last_sent_hash,
            ((((((((((((((ARRAY[]::text[] ||
                CASE
                    WHEN ar.has_rate_issue OR ar.processing_status = 'RATE_MISSING'::ts_fin_processing_status_enum THEN ARRAY['Rate'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.has_pay_channel_issue OR ar.processing_status = 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum THEN ARRAY['Pay channel'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.processing_status = 'UNASSIGNED'::ts_fin_processing_status_enum THEN ARRAY['Candidate ID'::text]
                    WHEN ar.processing_status = 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum THEN ARRAY['Client ID'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.pay_on_hold THEN ARRAY['On hold'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN NOT (ar.timesheet_id IS NOT NULL AND COALESCE(ar.client_hr_validation_required, false) = true AND COALESCE(ar.client_no_timesheet_required, false) = false AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric) AND (ar.hr_crosscheck_status = 'HOURS_MISMATCH_HR'::text OR ar.hr_crosscheck_issues && ARRAY['HOURS_MISMATCH_HR'::text]) THEN ARRAY['Hours mismatch HR'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN NOT (ar.timesheet_id IS NOT NULL AND COALESCE(ar.client_hr_validation_required, false) = true AND COALESCE(ar.client_no_timesheet_required, false) = false AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric) AND ar.hr_crosscheck_issues && ARRAY['HR_HOURS_MISSING'::text] THEN ARRAY['HR hours missing'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'::text] THEN ARRAY['Duplicate contracts'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND ar.client_requires_hr AND NOT ar.client_no_timesheet_required AND ar.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric AND NOT (COALESCE(ar.evidence_count, 0) > 0 OR ar.submission_mode = 'ELECTRONIC'::submission_mode_enum AND ar.r2_nurse_key IS NOT NULL AND ar.r2_auth_key IS NOT NULL OR ar.submission_mode = 'MANUAL'::submission_mode_enum AND ar.manual_pdf_r2_key IS NOT NULL) THEN ARRAY['Timesheet evidence'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND (COALESCE(ar.travel_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.travel_pay_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.accommodation_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.accommodation_pay_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.other_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.other_pay_ex_vat, 0::numeric) > 0::numeric) AND ((COALESCE(ar.travel_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.travel_pay_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'TRAVEL'::text)) OR (COALESCE(ar.accommodation_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.accommodation_pay_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'ACCOMMODATION'::text)) OR (COALESCE(ar.other_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.other_pay_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'OTHER'::text))) THEN ARRAY['Expenses evidence'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND (COALESCE(ar.mileage_units, 0::numeric) > 0::numeric OR COALESCE(ar.mileage_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(ar.mileage_pay_ex_vat, 0::numeric) > 0::numeric) AND NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'MILEAGE'::text)) THEN ARRAY['Mileage evidence'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND ar.sheet_scope = 'DAILY'::timesheet_scope_enum AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric AND (COALESCE(ar.require_reference_to_pay, false) OR COALESCE(ar.require_reference_to_invoice, false) OR ar.client_ts_reference_required OR ar.client_pay_reference_required OR ar.client_invoice_reference_required) AND (ar.reference_number IS NULL OR length(btrim(ar.reference_number)) = 0) THEN ARRAY['Reference'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND ar.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric AND (COALESCE(ar.require_reference_to_pay, false) OR COALESCE(ar.require_reference_to_invoice, false) OR NOT COALESCE(ar.require_reference_to_pay, false) AND NOT COALESCE(ar.require_reference_to_invoice, false) AND (ar.client_pay_reference_required OR ar.client_invoice_reference_required OR ar.client_ts_reference_required)) AND (ar.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(ar.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(ar.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND jsonb_typeof(ar.invoice_breakdown_json -> 'segments'::text) = 'array'::text AND (EXISTS ( SELECT 1
                       FROM jsonb_array_elements(ar.invoice_breakdown_json -> 'segments'::text) s(value)
                      WHERE NULLIF(btrim(COALESCE(s.value ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) IS NULL AND (COALESCE(NULLIF(s.value ->> 'hours_day'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_night'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_sat'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_sun'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_bh'::text, ''::text)::numeric, 0::numeric)) > 0::numeric AND COALESCE(btrim(s.value ->> 'ref_num'::text), ''::text) = ''::text)) OR ar.submission_mode = 'MANUAL'::submission_mode_enum AND (ar.actual_schedule_json IS NULL OR jsonb_typeof(ar.actual_schedule_json) <> 'array'::text OR jsonb_typeof(ar.actual_schedule_json) = 'array'::text AND (jsonb_array_length(ar.actual_schedule_json) = 0 OR (EXISTS ( SELECT 1
                       FROM jsonb_array_elements(ar.actual_schedule_json) seg_1(value)
                      WHERE COALESCE(btrim(seg_1.value ->> 'start'::text), ''::text) <> ''::text AND COALESCE(btrim(seg_1.value ->> 'end'::text), ''::text) <> ''::text AND COALESCE(btrim(seg_1.value ->> 'ref_num'::text), ''::text) = ''::text)))) OR ar.submission_mode <> 'MANUAL'::submission_mode_enum AND NOT ((EXISTS ( SELECT 1
                       FROM jsonb_array_elements_text(
                            CASE
                                WHEN ar.day_references_json IS NOT NULL AND jsonb_typeof(ar.day_references_json) = 'object'::text AND jsonb_typeof(ar.day_references_json -> '__freeform_refs'::text) = 'array'::text THEN ar.day_references_json -> '__freeform_refs'::text
                                WHEN ar.day_references_json IS NOT NULL AND jsonb_typeof(ar.day_references_json) = 'object'::text AND jsonb_typeof(ar.day_references_json -> '__freeform'::text) = 'array'::text THEN ar.day_references_json -> '__freeform'::text
                                WHEN ar.day_references_json IS NOT NULL AND jsonb_typeof(ar.day_references_json) = 'object'::text AND jsonb_typeof(ar.day_references_json -> '__freeform_lines'::text) = 'array'::text THEN ar.day_references_json -> '__freeform_lines'::text
                                WHEN ar.day_references_json IS NOT NULL AND jsonb_typeof(ar.day_references_json) = 'array'::text THEN ar.day_references_json
                                ELSE '[]'::jsonb
                            END) t(x)
                      WHERE NULLIF(btrim(COALESCE(t.x, ''::text)), ''::text) IS NOT NULL)) OR (EXISTS ( SELECT 1
                       FROM jsonb_each_text(
                            CASE
                                WHEN ar.day_references_json IS NOT NULL AND jsonb_typeof(ar.day_references_json) = 'object'::text THEN ar.day_references_json
                                ELSE '{}'::jsonb
                            END) j(k, v)
                      WHERE NULLIF(btrim(COALESCE(j.v, ''::text)), ''::text) IS NOT NULL AND "left"(COALESCE(j.k, ''::text), 2) <> '__'::text)))) THEN ARRAY['Reference'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND COALESCE(ar.client_hr_validation_required, false) = true AND COALESCE(ar.client_no_timesheet_required, false) = false AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric AND (ar.validation_status IS NULL OR ar.validation_status = 'PENDING'::validation_status_enum) THEN ARRAY['Awaiting validation'::text]
                    WHEN ar.timesheet_id IS NOT NULL AND COALESCE(ar.client_hr_validation_required, false) = true AND COALESCE(ar.client_no_timesheet_required, false) = false AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric AND ar.validation_status IS NOT NULL AND (ar.validation_status <> ALL (ARRAY['VALIDATION_OK'::validation_status_enum, 'OVERRIDDEN'::validation_status_enum, 'PENDING'::validation_status_enum])) THEN ARRAY['Validation failed'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND ar.client_requires_hr AND NOT ar.client_autoprocess_hr AND ar.authorised_at_server IS NULL THEN ARRAY['Authorisation'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.timesheet_id IS NOT NULL AND ar.qr_status = 'PENDING'::timesheet_qr_status_enum AND (ar.qr_token IS NOT NULL AND length(btrim(ar.qr_token)) > 0 AND ar.qr_generated_at IS NOT NULL OR ar.qr_last_sent_hash IS NOT NULL) AND ar.qr_scanned_at IS NULL AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric THEN ARRAY['Awaiting signed QR timesheet'::text]
                    ELSE ARRAY[]::text[]
                END AS issue_codes
           FROM all_rows ar
        ), pay_ts AS (
         SELECT DISTINCT wi_1.timesheet_id,
            COALESCE(wi_1.pay_on_hold, false) AS pay_on_hold,
            wi_1.invoice_breakdown_json
           FROM with_issues wi_1
          WHERE wi_1.timesheet_id IS NOT NULL
        ), pay_is_seg AS (
         SELECT pt.timesheet_id,
            pt.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(pt.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(pt.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND jsonb_typeof(pt.invoice_breakdown_json -> 'segments'::text) = 'array'::text AS is_segments_mode
           FROM pay_ts pt
        ), pay_components AS (
         SELECT pt.timesheet_id,
            NULLIF(btrim(COALESCE(seg_1.value ->> 'segment_id'::text, ''::text)), ''::text) AS component_id,
            COALESCE(NULLIF(seg_1.value ->> 'exclude_from_pay'::text, ''::text)::boolean, false) AS is_on_hold
           FROM pay_ts pt
             JOIN pay_is_seg ps ON ps.timesheet_id = pt.timesheet_id
             JOIN LATERAL jsonb_array_elements(COALESCE(pt.invoice_breakdown_json -> 'segments'::text, '[]'::jsonb)) seg_1(value) ON true
          WHERE ps.is_segments_mode = true AND seg_1.value IS NOT NULL AND jsonb_typeof(seg_1.value) = 'object'::text AND NULLIF(btrim(COALESCE(seg_1.value ->> 'segment_id'::text, ''::text)), ''::text) IS NOT NULL
        UNION ALL
         SELECT pt.timesheet_id,
            'TOTAL'::text AS component_id,
            pt.pay_on_hold AS is_on_hold
           FROM pay_ts pt
             JOIN pay_is_seg ps ON ps.timesheet_id = pt.timesheet_id
          WHERE ps.is_segments_mode = false
        ), pay_items AS (
         SELECT pbi.timesheet_id,
                CASE
                    WHEN NULLIF(btrim(COALESCE(pbi.segment_key, ''::text)), ''::text) IS NOT NULL THEN NULLIF(btrim(COALESCE(pbi.segment_key, ''::text)), ''::text)
                    WHEN pbi.source_ref IS NOT NULL AND btrim(COALESCE(pbi.source_ref, ''::text)) ~~ 'seg:%'::text THEN NULLIF(btrim(split_part(btrim(pbi.source_ref), ':'::text, 2)), ''::text)
                    ELSE 'TOTAL'::text
                END AS component_id,
            upper(COALESCE(pb.status, ''::text)) AS batch_status,
            pb.completed_at_utc
           FROM pay_batch_items pbi
             JOIN pay_batch_candidates pbc ON pbc.id = pbi.pay_batch_candidate_id
             JOIN pay_batches pb ON pb.id = pbc.pay_batch_id
             JOIN pay_ts pt ON pt.timesheet_id = pbi.timesheet_id
          WHERE pbi.is_voided = false AND pb.cancelled_at_utc IS NULL AND (pbi.item_type = ANY (ARRAY['SEGMENT_DELTA'::text, 'EXPENSE_DELTA'::text, 'ADJUSTMENT_DELTA'::text, 'MILEAGE_DELTA'::text]))
        ), pay_items_agg AS (
         SELECT pi.timesheet_id,
            pi.component_id,
            max(
                CASE
                    WHEN pi.batch_status = 'SETTLED'::text THEN 1
                    ELSE 0
                END) AS has_settled,
            max(
                CASE
                    WHEN pi.batch_status = ANY (ARRAY['DRAFT'::text, 'DRAFT_CREATED'::text, 'READY'::text, 'WAITING_BANK_CONFIRM'::text, 'PARTIAL'::text, 'FAILED'::text, 'BLOCKED_FUNDS'::text, 'SCHEDULED'::text, 'EXECUTING'::text, 'AWAITING_AUTHORISATION'::text, 'AUTHORISED_FOR_PAYMENT'::text]) THEN 1
                    ELSE 0
                END) AS has_processing,
            max(
                CASE
                    WHEN pi.batch_status = 'SETTLED'::text THEN pi.completed_at_utc
                    ELSE NULL::timestamp with time zone
                END) AS settled_at_utc
           FROM pay_items pi
          WHERE pi.component_id IS NOT NULL
          GROUP BY pi.timesheet_id, pi.component_id
        ), pay_component_state AS (
         SELECT pc_1.timesheet_id,
            pc_1.component_id,
            pc_1.is_on_hold,
                CASE
                    WHEN pc_1.is_on_hold = true THEN 'ON_HOLD'::text
                    WHEN COALESCE(pia.has_settled, 0) = 1 THEN 'PAID'::text
                    WHEN COALESCE(pia.has_processing, 0) = 1 THEN 'PROCESSING'::text
                    ELSE 'UNPAID'::text
                END AS component_stage,
            pia.settled_at_utc
           FROM pay_components pc_1
             LEFT JOIN pay_items_agg pia ON pia.timesheet_id = pc_1.timesheet_id AND pia.component_id = pc_1.component_id
        ), pay_counts AS (
         SELECT pcs.timesheet_id,
            count(*)::integer AS total_components,
            count(*) FILTER (WHERE pcs.is_on_hold = true)::integer AS on_hold_components,
            count(*) FILTER (WHERE pcs.is_on_hold = false)::integer AS payable_components,
            count(*) FILTER (WHERE pcs.is_on_hold = false AND pcs.component_stage = 'PAID'::text)::integer AS paid_components,
            max(
                CASE
                    WHEN pcs.is_on_hold = false AND pcs.component_stage = 'PROCESSING'::text THEN 1
                    ELSE 0
                END) AS any_processing,
            max(
                CASE
                    WHEN pcs.is_on_hold = false AND pcs.component_stage = 'PAID'::text THEN pcs.settled_at_utc
                    ELSE NULL::timestamp with time zone
                END) AS pay_paid_at_utc
           FROM pay_component_state pcs
          GROUP BY pcs.timesheet_id
        ), pay_delta AS (
         SELECT pt.timesheet_id,
            round(COALESCE(sum(COALESCE(oc.truth_ex_vat, 0::numeric) - COALESCE(oc.baseline_ex_vat, 0::numeric)), 0::numeric), 2) AS net_delta_ex_vat
           FROM pay_ts pt
             LEFT JOIN LATERAL _pay_outstanding_components(ARRAY[pt.timesheet_id]) oc(timesheet_id, key_type, key_value, truth_ex_vat, baseline_ex_vat, reserved_ex_vat, outstanding_ex_vat, truth_inc_vat, baseline_inc_vat, reserved_inc_vat, outstanding_inc_vat, reservation_overrun_detected) ON true
          GROUP BY pt.timesheet_id
        ), pay_rollup AS (
         SELECT pc_1.timesheet_id,
                CASE
                    WHEN pc_1.payable_components IS NULL OR pc_1.payable_components = 0 THEN 'UNPAID'::text
                    WHEN pc_1.any_processing = 1 THEN 'PROCESSING'::text
                    WHEN pc_1.paid_components = pc_1.payable_components THEN 'PAID'::text
                    WHEN pc_1.paid_components > 0 THEN 'PARTIALLY_PAID'::text
                    ELSE 'UNPAID'::text
                END AS pay_status_code,
            pc_1.pay_paid_at_utc,
            pd.net_delta_ex_vat,
                CASE
                    WHEN pc_1.any_processing = 1 THEN 'CLOCK'::text
                    WHEN pd.net_delta_ex_vat < 0::numeric THEN 'RED_COIN'::text
                    WHEN pd.net_delta_ex_vat > 0::numeric THEN 'HALF_COIN'::text
                    WHEN pc_1.payable_components > 0 AND pc_1.paid_components = pc_1.payable_components THEN 'COIN'::text
                    ELSE 'NONE'::text
                END AS pay_icon_code
           FROM pay_counts pc_1
             LEFT JOIN pay_delta pd ON pd.timesheet_id = pc_1.timesheet_id
        )
 SELECT wi.timesheet_id,
    wi.timesheet_status,
    wi.week_ending_date,
    wi.booking_id,
    wi.occupant_key_norm,
    wi.hospital_norm,
    wi.sheet_scope,
    wi.submission_mode,
    wi.authorised_at_server,
    wi.candidate_id,
    wi.client_id,
    wi.pay_method,
    wi.processing_status,
    wi.basis,
    wi.total_hours,
    wi.total_pay_ex_vat,
    wi.total_charge_ex_vat,
    wi.margin_ex_vat,
    wi.paid_at_utc,
    wi.pay_on_hold,
    wi.timesheet_id IS NOT NULL AND wi.paid_at_utc IS NULL AND COALESCE(wi.pay_on_hold, false) = false AND wi.authorised_at_server IS NOT NULL AND wi.processing_status IS NOT NULL AND (wi.processing_status <> ALL (ARRAY['UNASSIGNED'::ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum, 'RATE_MISSING'::ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum])) AND COALESCE(wi.has_rate_issue, false) = false AND COALESCE(wi.has_pay_channel_issue, false) = false AND (COALESCE(wi.require_reference_to_pay, false) = false OR COALESCE(wi.total_hours, 0::numeric) <= 0::numeric OR
        CASE
            WHEN wi.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(wi.invoice_breakdown_json) = 'object'::text AND upper(COALESCE(wi.invoice_breakdown_json ->> 'mode'::text, ''::text)) = 'SEGMENTS'::text AND jsonb_typeof(wi.invoice_breakdown_json -> 'segments'::text) = 'array'::text THEN NOT (EXISTS ( SELECT 1
               FROM jsonb_array_elements(wi.invoice_breakdown_json -> 'segments'::text) s(value)
              WHERE (COALESCE(NULLIF(s.value ->> 'hours_day'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_night'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_sat'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_sun'::text, ''::text)::numeric, 0::numeric) + COALESCE(NULLIF(s.value ->> 'hours_bh'::text, ''::text)::numeric, 0::numeric)) > 0::numeric AND COALESCE(btrim(s.value ->> 'ref_num'::text), ''::text) = ''::text))
            ELSE
            CASE
                WHEN wi.sheet_scope = 'DAILY'::timesheet_scope_enum THEN wi.reference_number IS NOT NULL AND length(btrim(wi.reference_number)) > 0
                WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum THEN
                CASE
                    WHEN wi.submission_mode = 'MANUAL'::submission_mode_enum THEN wi.actual_schedule_json IS NOT NULL AND jsonb_typeof(wi.actual_schedule_json) = 'array'::text AND jsonb_array_length(wi.actual_schedule_json) > 0 AND NOT (EXISTS ( SELECT 1
                       FROM jsonb_array_elements(wi.actual_schedule_json) seg_1(value)
                      WHERE COALESCE(btrim(seg_1.value ->> 'start'::text), ''::text) <> ''::text AND COALESCE(btrim(seg_1.value ->> 'end'::text), ''::text) <> ''::text AND COALESCE(btrim(seg_1.value ->> 'ref_num'::text), ''::text) = ''::text))
                    ELSE (EXISTS ( SELECT 1
                       FROM jsonb_array_elements_text(
                            CASE
                                WHEN wi.day_references_json IS NOT NULL AND jsonb_typeof(wi.day_references_json) = 'object'::text AND jsonb_typeof(wi.day_references_json -> '__freeform_refs'::text) = 'array'::text THEN wi.day_references_json -> '__freeform_refs'::text
                                WHEN wi.day_references_json IS NOT NULL AND jsonb_typeof(wi.day_references_json) = 'object'::text AND jsonb_typeof(wi.day_references_json -> '__freeform'::text) = 'array'::text THEN wi.day_references_json -> '__freeform'::text
                                WHEN wi.day_references_json IS NOT NULL AND jsonb_typeof(wi.day_references_json) = 'object'::text AND jsonb_typeof(wi.day_references_json -> '__freeform_lines'::text) = 'array'::text THEN wi.day_references_json -> '__freeform_lines'::text
                                WHEN wi.day_references_json IS NOT NULL AND jsonb_typeof(wi.day_references_json) = 'array'::text THEN wi.day_references_json
                                ELSE '[]'::jsonb
                            END) t(x)
                      WHERE NULLIF(btrim(COALESCE(t.x, ''::text)), ''::text) IS NOT NULL)) OR wi.day_references_json IS NOT NULL AND jsonb_typeof(wi.day_references_json) = 'object'::text AND (EXISTS ( SELECT 1
                       FROM jsonb_each_text(wi.day_references_json) j(k, v)
                      WHERE NULLIF(btrim(COALESCE(j.v, ''::text)), ''::text) IS NOT NULL AND "left"(COALESCE(j.k, ''::text), 2) <> '__'::text))
                END
                ELSE wi.reference_number IS NOT NULL AND length(btrim(wi.reference_number)) > 0
            END
        END) AS ready_to_pay,
    wi.locked_by_invoice_id,
    wi.candidate_name,
    wi.client_name,
    wi.nhsp_shift_count,
    wi.nhsp_shift_included_count,
    wi.nhsp_shift_deferred_count,
    wi.validation_status,
        CASE
            WHEN wi.timesheet_id IS NULL THEN
            CASE wi.contract_week_status
                WHEN 'PLANNED'::contract_week_status_enum THEN 'PLANNED'::text
                WHEN 'OPEN'::contract_week_status_enum THEN 'PLANNED'::text
                WHEN 'SUBMITTED'::contract_week_status_enum THEN 'PENDING_AUTH'::text
                WHEN 'AUTHORISED'::contract_week_status_enum THEN 'READY_FOR_INVOICE'::text
                WHEN 'INVOICED'::contract_week_status_enum THEN 'INVOICED'::text
                WHEN 'CANCELLED'::contract_week_status_enum THEN 'NEEDS_ATTENTION'::text
                ELSE 'UNKNOWN'::text
            END
            WHEN wi.paid_at_utc IS NOT NULL THEN 'PAID'::text
            WHEN wi.locked_by_invoice_id IS NOT NULL OR seg.seg_total IS NOT NULL AND seg.seg_total > 0 AND COALESCE(seg.seg_locked, 0) >= seg.seg_total THEN 'INVOICED'::text
            WHEN wi.timesheet_id IS NOT NULL AND wi.qr_status = 'PENDING'::timesheet_qr_status_enum AND (wi.qr_token IS NULL OR length(btrim(wi.qr_token)) = 0) AND wi.qr_generated_at IS NULL THEN 'QR_NOT_ISSUED'::text
            WHEN wi.timesheet_id IS NOT NULL AND wi.qr_status = 'PENDING'::timesheet_qr_status_enum AND wi.qr_token IS NOT NULL AND length(btrim(wi.qr_token)) > 0 AND wi.qr_generated_at IS NOT NULL AND wi.qr_scanned_at IS NULL THEN 'QR_ISSUED_AWAITING_SIGNATURE'::text
            WHEN wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum THEN 'READY_FOR_INVOICE'::text
            WHEN wi.processing_status = 'READY_FOR_HR'::ts_fin_processing_status_enum THEN 'READY_FOR_HR'::text
            WHEN wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum THEN 'PENDING_AUTH'::text
            WHEN wi.processing_status = ANY (ARRAY['UNASSIGNED'::ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum, 'RATE_MISSING'::ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum]) THEN 'NEEDS_ATTENTION'::text
            ELSE 'UNKNOWN'::text
        END AS summary_stage,
        CASE
            WHEN wi.sheet_scope = 'DAILY'::timesheet_scope_enum AND wi.submission_mode = 'ELECTRONIC'::submission_mode_enum THEN 'DAILY_ELECTRONIC'::text
            WHEN wi.sheet_scope = 'DAILY'::timesheet_scope_enum AND wi.submission_mode = 'MANUAL'::submission_mode_enum THEN 'DAILY_MANUAL'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND (COALESCE(wi.is_adjustment, false) = true OR COALESCE(wi.additional_seq, 0) > 0) AND (wi.basis = 'NHSP_ADJUSTMENT'::timesheet_fin_basis_enum OR wi.basis = 'NHSP'::timesheet_fin_basis_enum OR COALESCE(wi.client_is_nhsp, false) = true) THEN 'WEEKLY_NHSP_ADJUSTMENT'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND (COALESCE(wi.is_adjustment, false) = true OR COALESCE(wi.additional_seq, 0) > 0) AND (wi.basis = 'HEALTHROSTER_ADJUSTMENT'::timesheet_fin_basis_enum OR wi.basis = 'HEALTHROSTER_SELF_BILL'::timesheet_fin_basis_enum OR COALESCE(wi.client_autoprocess_hr, false) = true) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND (COALESCE(wi.is_adjustment, false) = true OR COALESCE(wi.additional_seq, 0) > 0) THEN 'WEEKLY_MANUAL_ADJUSTMENT'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.basis = 'NHSP_ADJUSTMENT'::timesheet_fin_basis_enum THEN 'WEEKLY_NHSP_ADJUSTMENT'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.basis = 'HEALTHROSTER_ADJUSTMENT'::timesheet_fin_basis_enum THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.basis = 'NHSP'::timesheet_fin_basis_enum THEN 'WEEKLY_NHSP'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.client_is_nhsp IS TRUE THEN 'WEEKLY_NHSP'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND (wi.client_autoprocess_hr IS TRUE OR wi.basis = 'HEALTHROSTER_SELF_BILL'::timesheet_fin_basis_enum) THEN 'WEEKLY_HEALTHROSTER'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.submission_mode = 'ELECTRONIC'::submission_mode_enum THEN 'WEEKLY_ELECTRONIC'::text
            WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.submission_mode = 'MANUAL'::submission_mode_enum THEN 'WEEKLY_MANUAL'::text
            ELSE 'UNKNOWN'::text
        END AS route_type,
    wi.contract_week_id,
    wi.contract_week_ending_date,
    wi.contract_week_status,
    wi.additional_seq,
    wi.is_adjustment,
    wi.qr_status,
    wi.pay_adjustment_count,
    wi.pay_adjustment_count > 0 AS has_pay_adjustments,
    COALESCE(wi.is_adjustment, false) OR wi.pay_adjustment_count > 0 AS is_adjusted,
    wi.qr_status IS NOT NULL AS is_qr,
    (wi.processing_status = ANY (ARRAY['UNASSIGNED'::ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum, 'RATE_MISSING'::ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum])) OR NOT (wi.timesheet_id IS NOT NULL AND COALESCE(wi.client_hr_validation_required, false) = true AND COALESCE(wi.client_no_timesheet_required, false) = false AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric) AND wi.hr_crosscheck_status IS NOT NULL AND wi.hr_crosscheck_status <> 'OK'::text OR NOT (wi.timesheet_id IS NOT NULL AND COALESCE(wi.client_hr_validation_required, false) = true AND COALESCE(wi.client_no_timesheet_required, false) = false AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric) AND wi.hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'::text] OR ic.issue_codes_final IS NOT NULL AND array_length(ic.issue_codes_final, 1) > 0 AS needs_attention,
    wi.client_autoprocess_hr,
    wi.has_rate_issue,
    wi.has_pay_channel_issue,
    wi.hr_crosscheck_status,
    wi.hr_crosscheck_issues,
    wi.external_source_rows_json,
    ic.issue_codes_final AS issue_codes,
    wi.client_requires_hr,
    wi.client_no_timesheet_required,
    wi.client_is_nhsp,
    wi.client_pay_reference_required,
    wi.client_invoice_reference_required,
    wi.client_hr_validation_required,
    wi.client_ts_reference_required,
    wi.require_reference_to_pay,
    wi.require_reference_to_invoice,
    wi.qr_token,
    wi.qr_generated_at,
    wi.qr_scanned_at,
    wi.candidate_hint_text,
    wi.expenses_pay_ex_vat,
    wi.expenses_description,
    wi.mileage_units,
    wi.mileage_pay_rate,
    wi.mileage_charge_rate,
    wi.mileage_pay_ex_vat,
    wi.travel_pay_ex_vat,
    wi.travel_charge_ex_vat,
    wi.accommodation_pay_ex_vat,
    wi.accommodation_charge_ex_vat,
    wi.other_pay_ex_vat,
    wi.other_charge_ex_vat,
    wi.timesheet_id IS NOT NULL AND COALESCE(wi.client_hr_validation_required, false) = true AND COALESCE(wi.client_no_timesheet_required, false) = false AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric AS hr_validation_required_for_invoice,
    seg.seg_total AS invoice_segments_total,
    seg.seg_locked AS invoice_segments_locked,
        CASE
            WHEN seg.seg_total IS NULL THEN NULL::integer
            ELSE GREATEST(seg.seg_total - COALESCE(seg.seg_locked, 0), 0)
        END AS invoice_segments_unlocked,
        CASE
            WHEN seg.seg_total IS NULL THEN NULL::text
            WHEN COALESCE(seg.seg_locked, 0) = 0 THEN 'NOT_INVOICED'::text
            WHEN COALESCE(seg.seg_locked, 0) >= seg.seg_total THEN 'FULLY_INVOICED'::text
            ELSE 'PARTIALLY_INVOICED'::text
        END AS invoice_segment_stage,
        CASE
            WHEN wi.timesheet_id IS NULL THEN 'UNPROCESSED'::text
            WHEN wi.locked_by_invoice_id IS NOT NULL OR COALESCE(seg.seg_locked, 0) > 0 OR seg.seg_total IS NOT NULL AND COALESCE(seg.seg_locked, 0) > 0 THEN 'INVOICED'::text
            WHEN wi.timesheet_id IS NOT NULL AND wi.authorised_at_server IS NULL AND (wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum OR COALESCE(wi.client_requires_hr, false) = true AND COALESCE(wi.client_autoprocess_hr, false) = false AND array_length(ic.issue_codes_final, 1) = 1 AND ic.issue_codes_final @> ARRAY['Authorisation'::text]) THEN 'AWAITING_AUTHORISATION'::text
            WHEN wi.timesheet_id IS NOT NULL AND wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum THEN 'AUTHORISED_FOR_INVOICING'::text
            ELSE 'PROCESSING_DELAYED'::text
        END AS tools_stage,
        CASE
            WHEN wi.timesheet_id IS NULL THEN 'Unprocessed'::text
            WHEN wi.locked_by_invoice_id IS NOT NULL OR COALESCE(seg.seg_locked, 0) > 0 OR seg.seg_total IS NOT NULL AND COALESCE(seg.seg_locked, 0) > 0 THEN
            CASE
                WHEN seg.seg_total IS NOT NULL AND COALESCE(seg.seg_locked, 0) > 0 AND COALESCE(seg.seg_locked, 0) < seg.seg_total THEN 'Partially Invoiced'::text
                ELSE 'Invoiced'::text
            END
            WHEN wi.timesheet_id IS NOT NULL AND ic.issue_codes_final @> ARRAY['Awaiting signed QR timesheet'::text] THEN 'Awaiting signed QR timesheet'::text
            WHEN wi.timesheet_id IS NOT NULL AND wi.authorised_at_server IS NULL AND (wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum OR COALESCE(wi.client_requires_hr, false) = true AND COALESCE(wi.client_autoprocess_hr, false) = false AND array_length(ic.issue_codes_final, 1) = 1 AND ic.issue_codes_final @> ARRAY['Authorisation'::text]) THEN 'Awaiting Authorisation'::text
            WHEN wi.timesheet_id IS NOT NULL AND wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum THEN 'Authorised for Invoicing'::text
            ELSE 'Processing Delayed'::text
        END AS processing_status_display,
    COALESCE(seg.invoice_paid_any, false) AS invoice_is_paid,
        CASE
            WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE'::text THEN true
            ELSE false
        END AS refs_block_invoicing,
        CASE
            WHEN wi.timesheet_id IS NOT NULL AND COALESCE(pc.issue_missing_reference, false) = true THEN true
            ELSE false
        END AS refs_block_issuing_invoices,
        CASE
            WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE'::text AND COALESCE(pc.issue_missing_reference, false) = true THEN true
            ELSE false
        END AS refs_block_invoice_and_issuing,
        CASE
            WHEN wi.timesheet_id IS NULL THEN 'NONE'::text
            ELSE COALESCE(payr.pay_icon_code, 'NONE'::text)
        END AS pay_icon_code,
        CASE
            WHEN wi.timesheet_id IS NULL THEN NULL::text
            ELSE payr.pay_status_code
        END AS pay_status_code,
        CASE
            WHEN wi.timesheet_id IS NULL THEN NULL::timestamp with time zone
            ELSE payr.pay_paid_at_utc
        END AS pay_paid_at_utc,
        CASE
            WHEN wi.timesheet_id IS NULL THEN NULL::numeric
            ELSE payr.net_delta_ex_vat
        END AS net_delta_ex_vat
   FROM with_issues wi
     LEFT JOIN pay_rollup payr ON payr.timesheet_id = wi.timesheet_id
     LEFT JOIN LATERAL ( SELECT timesheet_pdf_reference_sig(wi.timesheet_id) AS current_refs_sig) rs ON true
     LEFT JOIN LATERAL ( SELECT pc0.precheck_status,
            pc0.issue_missing_reference,
            pc0.issue_missing_reference_count
           FROM v_ts_invoice_precheck pc0
          WHERE pc0.timesheet_id = wi.timesheet_id
         LIMIT 1) pc ON true
     LEFT JOIN LATERAL ( SELECT ((
                CASE
                    WHEN wi.timesheet_id IS NOT NULL AND wi.timesheet_id IS NOT NULL AND COALESCE(wi.client_hr_validation_required, false) = true AND COALESCE(wi.client_no_timesheet_required, false) = false AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric AND NOT (wi.validation_status = 'VALIDATION_OK'::validation_status_enum OR wi.validation_status = 'OVERRIDDEN'::validation_status_enum) THEN ARRAY[]::text[]
                    WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE'::text THEN ARRAY['Refs - Can''t invoice'::text]
                    WHEN wi.timesheet_id IS NOT NULL AND COALESCE(pc.issue_missing_reference, false) = true THEN ARRAY['Refs - Send Invoice will be blocked'::text]
                    ELSE ARRAY[]::text[]
                END ||
                CASE
                    WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_PDF'::text THEN ARRAY['Timesheet evidence missing'::text]
                    ELSE ARRAY[]::text[]
                END) || array_remove(array_remove(COALESCE(wi.issue_codes, ARRAY[]::text[]), 'Reference'::text), 'Timesheet evidence'::text)) ||
                CASE
                    WHEN wi.timesheet_id IS NOT NULL AND COALESCE(wi.client_no_timesheet_required, false) = false AND COALESCE(wi.client_is_nhsp, false) = false AND (wi.submission_mode = 'ELECTRONIC'::submission_mode_enum AND wi.manual_pdf_r2_key IS NULL AND wi.generated_pdf_at_utc IS NOT NULL AND (wi.generated_pdf_refs_sig IS NULL OR rs.current_refs_sig IS NOT NULL AND wi.generated_pdf_refs_sig <> rs.current_refs_sig) OR (wi.qr_token IS NOT NULL AND wi.qr_generated_at IS NOT NULL OR wi.qr_last_sent_hash IS NOT NULL) AND wi.qr_sent_refs_sig IS NOT NULL AND rs.current_refs_sig IS NOT NULL AND wi.qr_sent_refs_sig <> rs.current_refs_sig) THEN ARRAY['Refs - Timesheet PDF invalid'::text]
                    ELSE ARRAY[]::text[]
                END AS issue_codes_final) ic ON true
     LEFT JOIN LATERAL ( SELECT
                CASE
                    WHEN wi.timesheet_id IS NULL THEN NULL::integer
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text) = 'SEGMENTS'::text AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments'::text) = 'array'::text THEN jsonb_array_length(tf.invoice_breakdown_json -> 'segments'::text)
                    ELSE 1
                END AS seg_total,
                CASE
                    WHEN wi.timesheet_id IS NULL THEN NULL::integer
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text) = 'SEGMENTS'::text AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments'::text) = 'array'::text THEN ( SELECT count(*)::integer AS count
                       FROM jsonb_array_elements(tf.invoice_breakdown_json -> 'segments'::text) s(value)
                      WHERE NULLIF(btrim(COALESCE(s.value ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) IS NOT NULL)
                    ELSE
                    CASE
                        WHEN wi.locked_by_invoice_id IS NULL THEN 0
                        ELSE 1
                    END
                END AS seg_locked,
                CASE
                    WHEN wi.timesheet_id IS NULL THEN NULL::boolean
                    WHEN tf.invoice_breakdown_json IS NOT NULL AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text AND COALESCE(tf.invoice_breakdown_json ->> 'mode'::text, ''::text) = 'SEGMENTS'::text AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments'::text) = 'array'::text THEN (EXISTS ( SELECT 1
                       FROM jsonb_array_elements(tf.invoice_breakdown_json -> 'segments'::text) s(value)
                         JOIN invoices inv2 ON inv2.id =
                            CASE
                                WHEN NULLIF(btrim(COALESCE(s.value ->> 'invoice_locked_invoice_id'::text, ''::text)), ''::text) IS NOT NULL AND (s.value ->> 'invoice_locked_invoice_id'::text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'::text THEN (s.value ->> 'invoice_locked_invoice_id'::text)::uuid
                                ELSE NULL::uuid
                            END
                      WHERE inv2.status = 'PAID'::invoice_status_enum))
                    ELSE (EXISTS ( SELECT 1
                       FROM invoices inv2
                      WHERE inv2.id = wi.locked_by_invoice_id AND inv2.status = 'PAID'::invoice_status_enum))
                END AS invoice_paid_any
           FROM timesheets_financials tf
          WHERE tf.is_current = true AND tf.timesheet_id = wi.timesheet_id
          ORDER BY tf.created_at DESC
         LIMIT 1) seg ON true;;
alter view public.v_timesheets_summary_base owner to postgres;

-- public.v_timesheets_summary
create or replace view public.v_timesheets_summary with (security_invoker=true) as
SELECT v.timesheet_id,
    v.timesheet_status,
    v.week_ending_date,
    v.booking_id,
    v.occupant_key_norm,
    v.hospital_norm,
    v.sheet_scope,
    v.submission_mode,
    v.authorised_at_server,
    v.candidate_id,
    v.client_id,
    v.pay_method,
    v.processing_status,
    v.basis,
    v.total_hours,
    v.total_pay_ex_vat,
    v.total_charge_ex_vat,
    v.margin_ex_vat,
    v.paid_at_utc,
    v.pay_on_hold,
    v.ready_to_pay,
    v.locked_by_invoice_id,
    v.candidate_name,
    v.client_name,
    v.nhsp_shift_count,
    v.nhsp_shift_included_count,
    v.nhsp_shift_deferred_count,
    v.validation_status,
    v.summary_stage,
    v.route_type,
    v.contract_week_id,
    v.contract_week_ending_date,
    v.contract_week_status,
    v.additional_seq,
    COALESCE(ts2.is_adjustment, false) = true OR COALESCE(cw.is_adjustment, false) = true OR COALESCE(cw.additional_seq, v.additional_seq, 0) > 0 OR COALESCE(v.is_adjustment, false) = true OR ts2.parent_timesheet_id IS NOT NULL OR ts2.correction_id IS NOT NULL OR ts2.correction_kind IS NOT NULL AS is_adjustment,
    v.qr_status,
    v.pay_adjustment_count,
    v.has_pay_adjustments,
    COALESCE(ts2.is_adjustment, false) = true OR COALESCE(cw.is_adjustment, false) = true OR COALESCE(cw.additional_seq, v.additional_seq, 0) > 0 OR COALESCE(v.is_adjustment, false) = true OR ts2.parent_timesheet_id IS NOT NULL OR ts2.correction_id IS NOT NULL OR ts2.correction_kind IS NOT NULL OR COALESCE(v.has_pay_adjustments, false) = true AS is_adjusted,
    v.is_qr,
    v.needs_attention,
    v.client_autoprocess_hr,
    v.has_rate_issue,
    v.has_pay_channel_issue,
    v.hr_crosscheck_status,
    v.hr_crosscheck_issues,
    v.external_source_rows_json,
    v.issue_codes,
    COALESCE(ts2.contract_id, cw.contract_id) AS contract_id,
    v.client_requires_hr,
    v.client_no_timesheet_required,
    v.client_is_nhsp,
    v.client_pay_reference_required,
    v.client_invoice_reference_required,
    v.client_hr_validation_required,
    v.client_ts_reference_required,
    v.require_reference_to_pay,
    v.require_reference_to_invoice,
    v.candidate_hint_text,
    v.expenses_pay_ex_vat,
    v.expenses_description,
    v.mileage_units,
    v.mileage_pay_rate,
    v.mileage_charge_rate,
    v.mileage_pay_ex_vat,
    v.travel_pay_ex_vat,
    v.travel_charge_ex_vat,
    v.accommodation_pay_ex_vat,
    v.accommodation_charge_ex_vat,
    v.other_pay_ex_vat,
    v.other_charge_ex_vat,
        CASE
            WHEN v.locked_by_invoice_id IS NULL THEN NULL::text
            WHEN inv.id IS NULL THEN 'INVOICED_NOT_ISSUED'::text
            WHEN inv.status = ANY (ARRAY['ISSUED'::invoice_status_enum, 'PAID'::invoice_status_enum]) THEN 'INVOICED_ISSUED'::text
            ELSE 'INVOICED_NOT_ISSUED'::text
        END AS invoice_issue_stage,
    v.invoice_segments_total,
    v.invoice_segments_locked,
    v.invoice_segments_unlocked,
    v.invoice_segment_stage,
    v.tools_stage,
    v.processing_status_display,
    v.invoice_is_paid,
    v.refs_block_invoicing,
    v.refs_block_issuing_invoices,
    v.refs_block_invoice_and_issuing,
        CASE
            WHEN COALESCE(ts2.is_adjustment, false) = true OR COALESCE(cw.is_adjustment, false) = true OR COALESCE(cw.additional_seq, v.additional_seq, 0) > 0 OR COALESCE(v.is_adjustment, false) = true OR ts2.parent_timesheet_id IS NOT NULL OR ts2.correction_id IS NOT NULL OR ts2.correction_kind IS NOT NULL THEN
            CASE
                WHEN upper(COALESCE(ts2.adjustment_origin, ''::text)) = 'MANUAL_ADJUSTMENT'::text THEN 'Manual Adjustment'::text
                WHEN upper(COALESCE(ts2.adjustment_origin, ''::text)) ~~ 'IMPORT_%'::text OR ts2.correction_kind IS NOT NULL OR ts2.correction_id IS NOT NULL THEN 'NHSP Adjustment'::text
                ELSE 'Manual Adjustment'::text
            END
            ELSE
            CASE
                WHEN v.route_type = 'DAILY_ELECTRONIC'::text THEN 'Daily Electronic'::text
                WHEN v.route_type = 'DAILY_MANUAL'::text THEN 'Daily Manual'::text
                WHEN v.route_type = 'WEEKLY_ELECTRONIC'::text THEN 'Weekly Electronic'::text
                WHEN v.route_type = 'WEEKLY_MANUAL'::text THEN 'Weekly Manual'::text
                WHEN v.route_type = 'WEEKLY_NHSP'::text THEN 'Weekly NHSP'::text
                WHEN v.route_type = 'WEEKLY_NHSP_ADJUSTMENT'::text THEN 'Weekly NHSP'::text
                WHEN v.route_type = 'WEEKLY_HEALTHROSTER'::text THEN 'Weekly HealthRoster'::text
                ELSE 'Unknown'::text
            END
        END AS route_display,
    v.pay_icon_code,
    v.pay_status_code,
    v.pay_paid_at_utc,
    v.net_delta_ex_vat
   FROM v_timesheets_summary_base v
     LEFT JOIN contract_weeks cw ON cw.id = v.contract_week_id
     LEFT JOIN timesheets ts2 ON ts2.timesheet_id = v.timesheet_id
     LEFT JOIN invoices inv ON inv.id = v.locked_by_invoice_id;;
alter view public.v_timesheets_summary owner to postgres;

-- public.vw_picker_candidates
create or replace view public.vw_picker_candidates with (security_invoker=true) as
SELECT id,
    first_name,
    last_name,
    display_name,
    email,
    active,
    rev,
    updated_at,
    roles
   FROM candidates c;;
alter view public.vw_picker_candidates owner to postgres;

-- public.vw_picker_clients
create or replace view public.vw_picker_clients with (security_invoker=true) as
SELECT c.id,
    c.name,
    c.primary_invoice_email,
    c.rev,
    c.updated_at,
    COALESCE(bool_or(cs.is_nhsp), false) AS is_nhsp,
    COALESCE(bool_or(cs.autoprocess_hr), false) AS autoprocess_hr
   FROM clients c
     LEFT JOIN client_settings cs ON cs.client_id = c.id
  GROUP BY c.id, c.name, c.primary_invoice_email, c.rev, c.updated_at;;
alter view public.vw_picker_clients owner to postgres;

-- private.candidate_daily_authority_transitions.candidate_daily_authority_transitions_immutable
CREATE TRIGGER candidate_daily_authority_transitions_immutable BEFORE DELETE OR UPDATE ON private.candidate_daily_authority_transitions FOR EACH ROW EXECUTE FUNCTION private._candidate_daily_transition_immutable_v1();

-- private.candidate_daily_source_links.candidate_daily_source_link_identity_history_guard_v1
CREATE TRIGGER candidate_daily_source_link_identity_history_guard_v1 BEFORE INSERT OR UPDATE OF environment, source_system, hmac_key_version, identifier_hmac, candidate_id ON private.candidate_daily_source_links FOR EACH ROW EXECUTE FUNCTION private._candidate_daily_source_link_identity_history_guard_v1();

-- public.app_change_counters.trg_pay_workbench_scope_counter_stage_v1
CREATE TRIGGER trg_pay_workbench_scope_counter_stage_v1 BEFORE INSERT OR UPDATE ON app_change_counters FOR EACH ROW EXECUTE FUNCTION pay_workbench_scope_counter_stage_trg_v1();

-- public.bank_name_checks.trg_pay_workbench_mark_candidate_dirty__bank_name_checks
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__bank_name_checks AFTER INSERT OR DELETE OR UPDATE ON bank_name_checks FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.bank_payee_map.trg_pay_workbench_mark_candidate_dirty__bank_payee_map
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__bank_payee_map AFTER INSERT OR DELETE OR UPDATE ON bank_payee_map FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.banking_alert_display_summary.trg_banking_alert_display_summary_touch_updated_at_utc
CREATE TRIGGER trg_banking_alert_display_summary_touch_updated_at_utc BEFORE UPDATE ON banking_alert_display_summary FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_operation_candidate_allocation_rows.trg_banking_pay_operation_candidate_allocation_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_candidate_allocation_touch_updated_at BEFORE UPDATE ON banking_pay_operation_candidate_allocation_rows FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_candidate_scope.trg_banking_pay_operation_candidate_scope_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_candidate_scope_touch_updated_at BEFORE UPDATE ON banking_pay_operation_candidate_scope FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_chunks.trg_banking_pay_operation_chunks_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_chunks_touch_updated_at BEFORE UPDATE ON banking_pay_operation_chunks FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_config.trg_banking_pay_operation_config_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_config_touch_updated_at BEFORE UPDATE ON banking_pay_operation_config FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_remittance_scope.trg_banking_pay_operation_remittance_scope_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_remittance_scope_touch_updated_at BEFORE UPDATE ON banking_pay_operation_remittance_scope FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_scope_units.trg_bpay_op_scope_units_touch_updated_at_utc
CREATE TRIGGER trg_bpay_op_scope_units_touch_updated_at_utc BEFORE UPDATE ON banking_pay_operation_scope_units FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_operation_settlement_scope.trg_banking_pay_operation_settlement_scope_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_settlement_scope_touch_updated_at BEFORE UPDATE ON banking_pay_operation_settlement_scope FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_transfer_scope.trg_banking_pay_operation_transfer_scope_touch_updated_at
CREATE TRIGGER trg_banking_pay_operation_transfer_scope_touch_updated_at BEFORE UPDATE ON banking_pay_operation_transfer_scope FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_operation_transfer_scope_items.trg_bpay_op_transfer_items_touch_updated_at_utc
CREATE TRIGGER trg_bpay_op_transfer_items_touch_updated_at_utc BEFORE UPDATE ON banking_pay_operation_transfer_scope_items FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_operations.trg_banking_pay_operations_touch_updated_at
CREATE TRIGGER trg_banking_pay_operations_touch_updated_at BEFORE UPDATE ON banking_pay_operations FOR EACH ROW EXECUTE FUNCTION _banking_pay_operation_touch_updated_at();

-- public.banking_pay_scope_change_transactions.trg_pay_workbench_scope_change_finalize_v1
CREATE CONSTRAINT TRIGGER trg_pay_workbench_scope_change_finalize_v1 AFTER INSERT ON banking_pay_scope_change_transactions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pay_workbench_scope_change_finalize_trg_v1();

-- public.banking_pay_workbench_candidate_line_work.trg_bpay_wb_line_work_touch_updated_at_utc
CREATE TRIGGER trg_bpay_wb_line_work_touch_updated_at_utc BEFORE UPDATE ON banking_pay_workbench_candidate_line_work FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_workbench_candidate_source_lines.trg_bpay_wb_source_lines_touch_updated_at_utc
CREATE TRIGGER trg_bpay_wb_source_lines_touch_updated_at_utc BEFORE UPDATE ON banking_pay_workbench_candidate_source_lines FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_workbench_jobs.trg_pay_workbench_scope_job_stage_v1
CREATE TRIGGER trg_pay_workbench_scope_job_stage_v1 BEFORE INSERT OR UPDATE ON banking_pay_workbench_jobs FOR EACH ROW EXECUTE FUNCTION pay_workbench_scope_job_stage_trg_v1();

-- public.banking_pay_workbench_preview_rows.trg_banking_pay_preview_selection_carry_apply
CREATE TRIGGER trg_banking_pay_preview_selection_carry_apply AFTER INSERT OR UPDATE OF status, selected, selection_state, row_json, key_type, key_value ON banking_pay_workbench_preview_rows FOR EACH ROW EXECUTE FUNCTION trg_banking_pay_preview_selection_carry_apply();

-- public.banking_pay_workbench_preview_rows.trg_bpay_wb_preview_touch_updated_at_utc
CREATE TRIGGER trg_bpay_wb_preview_touch_updated_at_utc BEFORE UPDATE ON banking_pay_workbench_preview_rows FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_workbench_session_case_resolutions.trg_bpay_wb_case_resolution_origin_guard
CREATE TRIGGER trg_bpay_wb_case_resolution_origin_guard BEFORE INSERT OR UPDATE OF session_id, source_basis_fingerprint, payload_json, resolution_origin_session_id, resolution_origin_pay_date, resolution_origin_source_basis_fingerprint ON banking_pay_workbench_session_case_resolutions FOR EACH ROW EXECUTE FUNCTION pay_workbench_case_resolution_origin_guard_v1();

-- public.banking_pay_workbench_session_scope.trg_bpay_wb_scope_touch_updated_at_utc
CREATE TRIGGER trg_bpay_wb_scope_touch_updated_at_utc BEFORE UPDATE ON banking_pay_workbench_session_scope FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.banking_pay_workbench_sessions.trg_bpay_workbench_sessions_change_bump
CREATE TRIGGER trg_bpay_workbench_sessions_change_bump AFTER INSERT OR UPDATE OF progress_counter_version, progress_state, status, version, discarded_at_utc, replacement_session_id, scope_seed_complete, scope_total_count, scope_seeded_count, scope_ready_count, scope_pending_count, scope_failed_count, line_units_total, line_units_ready, line_units_pending, line_units_failed, preview_row_count, selected_row_count, section_counts_json, candidate_sample_rows_json, progress_json, server_selected_preview_row_ids, server_selected_preview_row_ids_provided ON banking_pay_workbench_sessions FOR EACH ROW EXECUTE FUNCTION pay_workbench_session_change_bump();

-- public.candidate_approval_requests.candidate_manager_request_mail_guard_v1
CREATE TRIGGER candidate_manager_request_mail_guard_v1 BEFORE UPDATE OF state ON candidate_approval_requests FOR EACH ROW EXECUTE FUNCTION private._candidate_manager_request_mail_guard_v1();

-- public.candidate_job_titles.trg_candidate_job_titles_set_updated_at
CREATE TRIGGER trg_candidate_job_titles_set_updated_at BEFORE UPDATE ON candidate_job_titles FOR EACH ROW EXECUTE FUNCTION set_candidate_job_titles_updated_at();

-- public.candidate_job_titles.trg_cc_candidate_job_titles
CREATE TRIGGER trg_cc_candidate_job_titles AFTER INSERT OR DELETE OR UPDATE ON candidate_job_titles FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('candidates');

-- public.candidate_submission_components.candidate_submission_components_immutability_guard
CREATE TRIGGER candidate_submission_components_immutability_guard BEFORE DELETE OR UPDATE ON candidate_submission_components FOR EACH ROW EXECUTE FUNCTION private._candidate_component_immutability_guard_v1();

-- public.candidate_submission_workflows.candidate_workflow_creation_identity_guard_v1
CREATE TRIGGER candidate_workflow_creation_identity_guard_v1 BEFORE INSERT OR UPDATE ON candidate_submission_workflows FOR EACH ROW EXECUTE FUNCTION private._candidate_workflow_creation_identity_guard_v1();

-- public.candidates.set_tms_ref
CREATE TRIGGER set_tms_ref BEFORE INSERT OR UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION trg_set_tms_ref();

-- public.candidates.trg_candidates_set_bank_hash
CREATE TRIGGER trg_candidates_set_bank_hash BEFORE INSERT OR UPDATE OF sort_code, account_number, account_holder ON candidates FOR EACH ROW EXECUTE FUNCTION _trg_candidates_set_bank_hash();

-- public.candidates.trg_candidates_set_rev
CREATE TRIGGER trg_candidates_set_rev BEFORE INSERT OR UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION trg_set_rev();

-- public.candidates.trg_candidates_set_updated_at
CREATE TRIGGER trg_candidates_set_updated_at BEFORE UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.candidates.trg_candidates_tombstone_ad
CREATE TRIGGER trg_candidates_tombstone_ad AFTER DELETE ON candidates FOR EACH ROW EXECUTE FUNCTION trg_candidates_tombstone();

-- public.candidates.trg_cc_candidates
CREATE TRIGGER trg_cc_candidates AFTER INSERT OR DELETE OR UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('candidates');

-- public.candidates.trg_invoice_candidate_revision_v2_candidates_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_candidates_d AFTER DELETE ON candidates REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'first_name', 'last_name', 'display_name', 'active', 'key_norm');

-- public.candidates.trg_invoice_candidate_revision_v2_candidates_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_candidates_i AFTER INSERT ON candidates REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'first_name', 'last_name', 'display_name', 'active', 'key_norm');

-- public.candidates.trg_invoice_candidate_revision_v2_candidates_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_candidates_u AFTER UPDATE ON candidates REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'first_name', 'last_name', 'display_name', 'active', 'key_norm');

-- public.candidates.trg_pay_workbench_mark_candidate_dirty__candidates
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__candidates AFTER INSERT OR UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.candidates.trg_tsfin_candidates_wakeup_aiu
CREATE TRIGGER trg_tsfin_candidates_wakeup_aiu AFTER INSERT OR UPDATE OF key_norm, nhsp_hr_name_aliases, pay_method, umbrella_id, active ON candidates FOR EACH ROW EXECUTE FUNCTION trg_tsfin_candidates_wakeup();

-- public.candidates_tombstones.trg_cc_candidates_tombstones
CREATE TRIGGER trg_cc_candidates_tombstones AFTER INSERT OR DELETE OR UPDATE ON candidates_tombstones FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('candidates');

-- public.client_hospitals.trg_tsfin_client_hospitals_wakeup_aiu
CREATE TRIGGER trg_tsfin_client_hospitals_wakeup_aiu AFTER INSERT OR UPDATE OF hospital_name_norm, client_id ON client_hospitals FOR EACH ROW EXECUTE FUNCTION trg_tsfin_client_hospitals_wakeup();

-- public.client_settings.trg_invoice_candidate_revision_v2_client_settings_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_client_settings_d AFTER DELETE ON client_settings REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'client_id', 'vat_rate_pct', 'effective_from', 'hr_validation_required', 'ts_reference_required', 'week_ending_weekday', 'autoprocess_hr', 'invoice_reference_required', 'default_submission_mode', 'is_nhsp', 'self_bill_no_invoices_sent', 'daily_calc_of_invoices', 'no_timesheet_required', 'group_nightsat_sunbh', 'requires_hr', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'auto_invoice_default', 'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address', 'invoice_consolidation_mode', 'reference_number_required_to_issue_invoice', 'reversal_complete_financials_date', 'reversal_replacement_financials_date');

-- public.client_settings.trg_invoice_candidate_revision_v2_client_settings_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_client_settings_i AFTER INSERT ON client_settings REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'client_id', 'vat_rate_pct', 'effective_from', 'hr_validation_required', 'ts_reference_required', 'week_ending_weekday', 'autoprocess_hr', 'invoice_reference_required', 'default_submission_mode', 'is_nhsp', 'self_bill_no_invoices_sent', 'daily_calc_of_invoices', 'no_timesheet_required', 'group_nightsat_sunbh', 'requires_hr', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'auto_invoice_default', 'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address', 'invoice_consolidation_mode', 'reference_number_required_to_issue_invoice', 'reversal_complete_financials_date', 'reversal_replacement_financials_date');

-- public.client_settings.trg_invoice_candidate_revision_v2_client_settings_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_client_settings_u AFTER UPDATE ON client_settings REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'client_id', 'vat_rate_pct', 'effective_from', 'hr_validation_required', 'ts_reference_required', 'week_ending_weekday', 'autoprocess_hr', 'invoice_reference_required', 'default_submission_mode', 'is_nhsp', 'self_bill_no_invoices_sent', 'daily_calc_of_invoices', 'no_timesheet_required', 'group_nightsat_sunbh', 'requires_hr', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'auto_invoice_default', 'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address', 'invoice_consolidation_mode', 'reference_number_required_to_issue_invoice', 'reversal_complete_financials_date', 'reversal_replacement_financials_date');

-- public.client_settings.trg_pay_workbench_mark_contract_client_dirty__client_settings
CREATE TRIGGER trg_pay_workbench_mark_contract_client_dirty__client_settings AFTER INSERT OR DELETE OR UPDATE ON client_settings FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_contract_client_dirty();

-- public.client_settings.trg_touch_client_from_client_settings
CREATE TRIGGER trg_touch_client_from_client_settings AFTER INSERT OR DELETE OR UPDATE ON client_settings FOR EACH ROW EXECUTE FUNCTION _trg_touch_client_from_client_settings();

-- public.clients.set_cli_ref
CREATE TRIGGER set_cli_ref BEFORE INSERT OR UPDATE ON clients FOR EACH ROW EXECUTE FUNCTION trg_set_cli_ref();

-- public.clients.trg_cc_clients
CREATE TRIGGER trg_cc_clients AFTER INSERT OR DELETE OR UPDATE ON clients FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('clients');

-- public.clients.trg_clients_set_rev
CREATE TRIGGER trg_clients_set_rev BEFORE INSERT OR UPDATE ON clients FOR EACH ROW EXECUTE FUNCTION trg_set_rev();

-- public.clients.trg_clients_set_updated_at
CREATE TRIGGER trg_clients_set_updated_at BEFORE UPDATE ON clients FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.clients.trg_clients_tombstone_ad
CREATE TRIGGER trg_clients_tombstone_ad AFTER DELETE ON clients FOR EACH ROW EXECUTE FUNCTION trg_clients_tombstone();

-- public.clients.trg_invoice_candidate_revision_v2_clients_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_clients_d AFTER DELETE ON clients REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'name', 'invoice_address', 'primary_invoice_email', 'vat_chargeable', 'payment_terms_days', 'ts_queries_email', 'client_address', 'contact_email');

-- public.clients.trg_invoice_candidate_revision_v2_clients_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_clients_i AFTER INSERT ON clients REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'name', 'invoice_address', 'primary_invoice_email', 'vat_chargeable', 'payment_terms_days', 'ts_queries_email', 'client_address', 'contact_email');

-- public.clients.trg_invoice_candidate_revision_v2_clients_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_clients_u AFTER UPDATE ON clients REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'name', 'invoice_address', 'primary_invoice_email', 'vat_chargeable', 'payment_terms_days', 'ts_queries_email', 'client_address', 'contact_email');

-- public.clients_tombstones.trg_cc_clients_tombstones
CREATE TRIGGER trg_cc_clients_tombstones AFTER INSERT OR DELETE OR UPDATE ON clients_tombstones FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('clients');

-- public.contract_weeks.trg_cc_contract_weeks
CREATE TRIGGER trg_cc_contract_weeks AFTER INSERT OR DELETE OR UPDATE ON contract_weeks FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('contracts');

-- public.contract_weeks.trg_contract_weeks_set_updated_at
CREATE TRIGGER trg_contract_weeks_set_updated_at BEFORE UPDATE ON contract_weeks FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.contract_weeks.trg_invoice_candidate_revision_v2_contract_weeks_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_contract_weeks_d AFTER DELETE ON contract_weeks REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'contract_id', 'week_ending_date', 'additional_seq', 'status', 'submission_mode_snapshot', 'timesheet_id', 'day_entries_json', 'totals_json', 'planned_schedule_json', 'is_adjustment', 'enforce_day_partition', 'allowed_days_mask', 'split_boundary_date', 'split_group_key');

-- public.contract_weeks.trg_invoice_candidate_revision_v2_contract_weeks_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_contract_weeks_i AFTER INSERT ON contract_weeks REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'contract_id', 'week_ending_date', 'additional_seq', 'status', 'submission_mode_snapshot', 'timesheet_id', 'day_entries_json', 'totals_json', 'planned_schedule_json', 'is_adjustment', 'enforce_day_partition', 'allowed_days_mask', 'split_boundary_date', 'split_group_key');

-- public.contract_weeks.trg_invoice_candidate_revision_v2_contract_weeks_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_contract_weeks_u AFTER UPDATE ON contract_weeks REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'contract_id', 'week_ending_date', 'additional_seq', 'status', 'submission_mode_snapshot', 'timesheet_id', 'day_entries_json', 'totals_json', 'planned_schedule_json', 'is_adjustment', 'enforce_day_partition', 'allowed_days_mask', 'split_boundary_date', 'split_group_key');

-- public.contracts.trg_cc_contracts
CREATE TRIGGER trg_cc_contracts AFTER INSERT OR DELETE OR UPDATE ON contracts FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('contracts');

-- public.contracts.trg_contracts_enforce_overrideclientsettings
CREATE TRIGGER trg_contracts_enforce_overrideclientsettings BEFORE INSERT OR UPDATE ON contracts FOR EACH ROW EXECUTE FUNCTION contracts_enforce_overrideclientsettings();

-- public.contracts.trg_contracts_set_updated_at
CREATE TRIGGER trg_contracts_set_updated_at BEFORE UPDATE ON contracts FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.contracts.trg_invoice_candidate_revision_v2_contracts_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_contracts_d AFTER DELETE ON contracts REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'candidate_id', 'client_id', 'start_date', 'end_date', 'pay_method_snapshot', 'rates_json', 'std_hours_json', 'default_submission_mode', 'week_ending_weekday_snapshot', 'auto_invoice', 'require_reference_to_invoice', 'mileage_charge_rate', 'additional_rates_json', 'self_bill', 'weekly_timesheet_source', 'no_timesheet_required', 'daily_calc_of_invoices', 'group_nightsat_sunbh', 'is_nhsp', 'autoprocess_hr', 'requires_hr', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'overrideclientsettings', 'reference_number_required_to_issue_invoice', 'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address', 'is_ad_hoc', 'healthroster_import_auto_authorise_override', 'nhsp_import_auto_authorise_override');

-- public.contracts.trg_invoice_candidate_revision_v2_contracts_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_contracts_i AFTER INSERT ON contracts REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'candidate_id', 'client_id', 'start_date', 'end_date', 'pay_method_snapshot', 'rates_json', 'std_hours_json', 'default_submission_mode', 'week_ending_weekday_snapshot', 'auto_invoice', 'require_reference_to_invoice', 'mileage_charge_rate', 'additional_rates_json', 'self_bill', 'weekly_timesheet_source', 'no_timesheet_required', 'daily_calc_of_invoices', 'group_nightsat_sunbh', 'is_nhsp', 'autoprocess_hr', 'requires_hr', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'overrideclientsettings', 'reference_number_required_to_issue_invoice', 'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address', 'is_ad_hoc', 'healthroster_import_auto_authorise_override', 'nhsp_import_auto_authorise_override');

-- public.contracts.trg_invoice_candidate_revision_v2_contracts_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_contracts_u AFTER UPDATE ON contracts REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'candidate_id', 'client_id', 'start_date', 'end_date', 'pay_method_snapshot', 'rates_json', 'std_hours_json', 'default_submission_mode', 'week_ending_weekday_snapshot', 'auto_invoice', 'require_reference_to_invoice', 'mileage_charge_rate', 'additional_rates_json', 'self_bill', 'weekly_timesheet_source', 'no_timesheet_required', 'daily_calc_of_invoices', 'group_nightsat_sunbh', 'is_nhsp', 'autoprocess_hr', 'requires_hr', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'overrideclientsettings', 'reference_number_required_to_issue_invoice', 'send_manual_invoices_to_different_email', 'manual_invoices_alt_email_address', 'is_ad_hoc', 'healthroster_import_auto_authorise_override', 'nhsp_import_auto_authorise_override');

-- public.contracts.trg_pay_workbench_mark_contract_client_dirty__contracts
CREATE TRIGGER trg_pay_workbench_mark_contract_client_dirty__contracts AFTER INSERT OR DELETE OR UPDATE ON contracts FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_contract_client_dirty();

-- public.hr_imports.trg_hr_imports_review_immutable
CREATE TRIGGER trg_hr_imports_review_immutable BEFORE UPDATE ON hr_imports FOR EACH ROW EXECUTE FUNCTION _import_review_immutable_guard_v1();

-- public.hr_imports.trg_import_review_prune_guard
CREATE TRIGGER trg_import_review_prune_guard BEFORE UPDATE OF pruned_at ON hr_imports FOR EACH ROW EXECUTE FUNCTION import_review_prune_guard_v1();

-- public.import_review_action_outcomes.trg_import_review_action_outcomes_immutable
CREATE TRIGGER trg_import_review_action_outcomes_immutable BEFORE DELETE OR UPDATE ON import_review_action_outcomes FOR EACH ROW EXECUTE FUNCTION _import_review_action_outcomes_immutable_guard_v1();

-- public.import_review_daily_timesheet_resolutions.trg_import_review_daily_resolution_guard
CREATE TRIGGER trg_import_review_daily_resolution_guard BEFORE DELETE OR UPDATE ON import_review_daily_timesheet_resolutions FOR EACH ROW EXECUTE FUNCTION _import_review_daily_resolution_guard_v1();

-- public.import_review_events.trg_import_review_events_immutable
CREATE TRIGGER trg_import_review_events_immutable BEFORE DELETE OR UPDATE ON import_review_events FOR EACH ROW EXECUTE FUNCTION _import_review_events_immutable_guard_v1();

-- public.import_review_scope_candidates.trg_import_review_scope_candidates_immutable
CREATE TRIGGER trg_import_review_scope_candidates_immutable BEFORE INSERT OR DELETE OR UPDATE ON import_review_scope_candidates FOR EACH ROW EXECUTE FUNCTION _import_review_immutable_guard_v1();

-- public.import_review_scope_clients.trg_import_review_scope_clients_immutable
CREATE TRIGGER trg_import_review_scope_clients_immutable BEFORE INSERT OR DELETE OR UPDATE ON import_review_scope_clients FOR EACH ROW EXECUTE FUNCTION _import_review_immutable_guard_v1();

-- public.import_review_states.trg_import_review_states_guard
CREATE TRIGGER trg_import_review_states_guard BEFORE DELETE OR UPDATE ON import_review_states FOR EACH ROW EXECUTE FUNCTION _import_review_state_guard_v1();

-- public.import_review_weekly_validation_resolutions.trg_import_review_weekly_validation_resolution_guard
CREATE TRIGGER trg_import_review_weekly_validation_resolution_guard BEFORE DELETE OR UPDATE ON import_review_weekly_validation_resolutions FOR EACH ROW EXECUTE FUNCTION _import_review_daily_resolution_guard_v1();

-- public.invoice_document_assets.trg_invoice_candidate_revision_v2_invoice_document_assets_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_document_assets_d AFTER DELETE ON invoice_document_assets REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'source_kind', 'source_id', 'source_revision', 'declared_media_type', 'detected_media_type', 'original_sha256', 'original_size_bytes', 'status', 'normalised_manifest_hash', 'normalised_r2_key', 'normalised_sha256', 'normalised_size_bytes', 'normalised_page_count', 'operation_id', 'error_json', 'ready_at_utc');

-- public.invoice_document_assets.trg_invoice_candidate_revision_v2_invoice_document_assets_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_document_assets_i AFTER INSERT ON invoice_document_assets REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'source_kind', 'source_id', 'source_revision', 'declared_media_type', 'detected_media_type', 'original_sha256', 'original_size_bytes', 'status', 'normalised_manifest_hash', 'normalised_r2_key', 'normalised_sha256', 'normalised_size_bytes', 'normalised_page_count', 'operation_id', 'error_json', 'ready_at_utc');

-- public.invoice_document_assets.trg_invoice_candidate_revision_v2_invoice_document_assets_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_document_assets_u AFTER UPDATE ON invoice_document_assets REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'source_kind', 'source_id', 'source_revision', 'declared_media_type', 'detected_media_type', 'original_sha256', 'original_size_bytes', 'status', 'normalised_manifest_hash', 'normalised_r2_key', 'normalised_sha256', 'normalised_size_bytes', 'normalised_page_count', 'operation_id', 'error_json', 'ready_at_utc');

-- public.invoice_document_assets.trg_invoice_document_assets_immutability
CREATE TRIGGER trg_invoice_document_assets_immutability BEFORE DELETE OR UPDATE ON invoice_document_assets FOR EACH ROW EXECUTE FUNCTION trg_invoice_document_immutability_guard();

-- public.invoice_document_versions.trg_invoice_candidate_revision_v2_invoice_document_version_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_document_version_d AFTER DELETE ON invoice_document_versions REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'entity_type', 'entity_id', 'purpose', 'operation_id', 'source_revision', 'template_version', 'status', 'r2_key', 'sha256', 'size_bytes', 'page_count', 'ready_at_utc', 'verified_at_utc', 'superseded_at_utc', 'error_json');

-- public.invoice_document_versions.trg_invoice_candidate_revision_v2_invoice_document_version_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_document_version_i AFTER INSERT ON invoice_document_versions REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'entity_type', 'entity_id', 'purpose', 'operation_id', 'source_revision', 'template_version', 'status', 'r2_key', 'sha256', 'size_bytes', 'page_count', 'ready_at_utc', 'verified_at_utc', 'superseded_at_utc', 'error_json');

-- public.invoice_document_versions.trg_invoice_candidate_revision_v2_invoice_document_version_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_document_version_u AFTER UPDATE ON invoice_document_versions REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'entity_type', 'entity_id', 'purpose', 'operation_id', 'source_revision', 'template_version', 'status', 'r2_key', 'sha256', 'size_bytes', 'page_count', 'ready_at_utc', 'verified_at_utc', 'superseded_at_utc', 'error_json');

-- public.invoice_document_versions.trg_invoice_document_versions_immutability
CREATE TRIGGER trg_invoice_document_versions_immutability BEFORE DELETE OR UPDATE ON invoice_document_versions FOR EACH ROW EXECUTE FUNCTION trg_invoice_document_immutability_guard();

-- public.invoice_hr_source_rows.trg_invoice_candidate_revision_v2_invoice_hr_source_rows_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_hr_source_rows_d AFTER DELETE ON invoice_hr_source_rows REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('false', 'true', 'invoice_id', 'source_system', 'import_id', 'header_rows', 'header_columns', 'rows_json');

-- public.invoice_hr_source_rows.trg_invoice_candidate_revision_v2_invoice_hr_source_rows_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_hr_source_rows_i AFTER INSERT ON invoice_hr_source_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('false', 'true', 'invoice_id', 'source_system', 'import_id', 'header_rows', 'header_columns', 'rows_json');

-- public.invoice_hr_source_rows.trg_invoice_candidate_revision_v2_invoice_hr_source_rows_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_hr_source_rows_u AFTER UPDATE ON invoice_hr_source_rows REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('false', 'true', 'invoice_id', 'source_system', 'import_id', 'header_rows', 'header_columns', 'rows_json');

-- public.invoice_hr_source_rows.trg_invoice_hr_source_document_invalidate_d
CREATE TRIGGER trg_invoice_hr_source_document_invalidate_d AFTER DELETE ON invoice_hr_source_rows REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoice_hr_source_rows.trg_invoice_hr_source_document_invalidate_i
CREATE TRIGGER trg_invoice_hr_source_document_invalidate_i AFTER INSERT ON invoice_hr_source_rows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoice_hr_source_rows.trg_invoice_hr_source_document_invalidate_u
CREATE TRIGGER trg_invoice_hr_source_document_invalidate_u AFTER UPDATE ON invoice_hr_source_rows REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoice_lines.trg_cc_invoice_lines
CREATE TRIGGER trg_cc_invoice_lines AFTER INSERT OR DELETE OR UPDATE ON invoice_lines FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('invoices');

-- public.invoice_lines.trg_id_invoice_lines_ad
CREATE TRIGGER trg_id_invoice_lines_ad AFTER DELETE ON invoice_lines REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_id_invoice_lines_ad_stmt();

-- public.invoice_lines.trg_id_invoice_lines_ai
CREATE TRIGGER trg_id_invoice_lines_ai AFTER INSERT ON invoice_lines REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_id_invoice_lines_ai_stmt();

-- public.invoice_lines.trg_id_invoice_lines_au
CREATE TRIGGER trg_id_invoice_lines_au AFTER UPDATE ON invoice_lines REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_id_invoice_lines_au_stmt();

-- public.invoice_lines.trg_invoice_candidate_revision_v2_invoice_lines_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_lines_d AFTER DELETE ON invoice_lines REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'invoice_id', 'timesheet_id', 'booking_id', 'source_key', 'description', 'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh', 'total_pay_ex_vat', 'total_charge_ex_vat', 'vat_rate_pct', 'vat_amount', 'total_inc_vat', 'margin_ex_vat', 'meta_json');

-- public.invoice_lines.trg_invoice_candidate_revision_v2_invoice_lines_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_lines_i AFTER INSERT ON invoice_lines REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'invoice_id', 'timesheet_id', 'booking_id', 'source_key', 'description', 'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh', 'total_pay_ex_vat', 'total_charge_ex_vat', 'vat_rate_pct', 'vat_amount', 'total_inc_vat', 'margin_ex_vat', 'meta_json');

-- public.invoice_lines.trg_invoice_candidate_revision_v2_invoice_lines_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_lines_u AFTER UPDATE ON invoice_lines REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'invoice_id', 'timesheet_id', 'booking_id', 'source_key', 'description', 'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh', 'total_pay_ex_vat', 'total_charge_ex_vat', 'vat_rate_pct', 'vat_amount', 'total_inc_vat', 'margin_ex_vat', 'meta_json');

-- public.invoice_lines.trg_invoice_line_archived_timesheet_guard_v1
CREATE TRIGGER trg_invoice_line_archived_timesheet_guard_v1 BEFORE INSERT OR UPDATE ON invoice_lines FOR EACH ROW EXECUTE FUNCTION invoice_line_archived_timesheet_guard_v1();

-- public.invoice_lines.trg_invoice_lines_document_invalidate_d
CREATE TRIGGER trg_invoice_lines_document_invalidate_d AFTER DELETE ON invoice_lines REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoice_lines.trg_invoice_lines_document_invalidate_i
CREATE TRIGGER trg_invoice_lines_document_invalidate_i AFTER INSERT ON invoice_lines REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoice_lines.trg_invoice_lines_document_invalidate_u
CREATE TRIGGER trg_invoice_lines_document_invalidate_u AFTER UPDATE ON invoice_lines REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoice_operation_chunks.trg_invoice_candidate_revision_v2_invoice_operation_chunks_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_operation_chunks_d AFTER DELETE ON invoice_operation_chunks REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'operation_id', 'chunk_type', 'entity_type', 'entity_id', 'document_version_id', 'document_asset_id', 'input_document_version_id', 'status', 'phase', 'replaced_by_chunk_id', 'manifest_generation', 'is_manifest_member', 'manifest_committed', 'result_visible', 'selection_key', 'result_category');

-- public.invoice_operation_chunks.trg_invoice_candidate_revision_v2_invoice_operation_chunks_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_operation_chunks_i AFTER INSERT ON invoice_operation_chunks REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'operation_id', 'chunk_type', 'entity_type', 'entity_id', 'document_version_id', 'document_asset_id', 'input_document_version_id', 'status', 'phase', 'replaced_by_chunk_id', 'manifest_generation', 'is_manifest_member', 'manifest_committed', 'result_visible', 'selection_key', 'result_category');

-- public.invoice_operation_chunks.trg_invoice_candidate_revision_v2_invoice_operation_chunks_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_operation_chunks_u AFTER UPDATE ON invoice_operation_chunks REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'operation_id', 'chunk_type', 'entity_type', 'entity_id', 'document_version_id', 'document_asset_id', 'input_document_version_id', 'status', 'phase', 'replaced_by_chunk_id', 'manifest_generation', 'is_manifest_member', 'manifest_committed', 'result_visible', 'selection_key', 'result_category');

-- public.invoice_operation_chunks.trg_invoice_result_page_revision_v2_d
CREATE TRIGGER trg_invoice_result_page_revision_v2_d AFTER DELETE ON invoice_operation_chunks REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_result_page_revision_trigger_v2();

-- public.invoice_operation_chunks.trg_invoice_result_page_revision_v2_i
CREATE TRIGGER trg_invoice_result_page_revision_v2_i AFTER INSERT ON invoice_operation_chunks REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_result_page_revision_trigger_v2();

-- public.invoice_operation_chunks.trg_invoice_result_page_revision_v2_u
CREATE TRIGGER trg_invoice_result_page_revision_v2_u AFTER UPDATE ON invoice_operation_chunks REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_result_page_revision_trigger_v2();

-- public.invoice_operations.trg_invoice_candidate_revision_v2_invoice_operations_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_operations_d AFTER DELETE ON invoice_operations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'parent_operation_id', 'operation_type', 'entity_type', 'entity_id', 'status', 'phase', 'source_revision', 'template_version', 'control_version', 'manifest_generation', 'manifest_committed', 'release_complete');

-- public.invoice_operations.trg_invoice_candidate_revision_v2_invoice_operations_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_operations_i AFTER INSERT ON invoice_operations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'parent_operation_id', 'operation_type', 'entity_type', 'entity_id', 'status', 'phase', 'source_revision', 'template_version', 'control_version', 'manifest_generation', 'manifest_committed', 'release_complete');

-- public.invoice_operations.trg_invoice_candidate_revision_v2_invoice_operations_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoice_operations_u AFTER UPDATE ON invoice_operations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'parent_operation_id', 'operation_type', 'entity_type', 'entity_id', 'status', 'phase', 'source_revision', 'template_version', 'control_version', 'manifest_generation', 'manifest_committed', 'release_complete');

-- public.invoices.trg_cc_invoices
CREATE TRIGGER trg_cc_invoices AFTER INSERT OR DELETE OR UPDATE ON invoices FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('invoices');

-- public.invoices.trg_id_invoices_ad
CREATE TRIGGER trg_id_invoices_ad AFTER DELETE ON invoices FOR EACH ROW EXECUTE FUNCTION trg_id_invoices_after_delete();

-- public.invoices.trg_id_invoices_meta_aiu
CREATE TRIGGER trg_id_invoices_meta_aiu AFTER INSERT OR UPDATE ON invoices FOR EACH ROW EXECUTE FUNCTION trg_id_invoices_meta_aiu();

-- public.invoices.trg_invoice_candidate_revision_v2_invoices_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoices_d AFTER DELETE ON invoices REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'client_id', 'invoice_no', 'type', 'original_invoice_id', 'subtotal_ex_vat', 'vat_amount', 'total_inc_vat', 'due_at_utc', 'notes', 'do_not_send', 'header_snapshot_json', 'status', 'issued_at_utc', 'paid_at_utc', 'on_hold_reason', 'document_revision', 'document_state', 'preview_document_version_id', 'issued_document_version_id', 'active_document_operation_id', 'issue_state', 'active_issue_operation_id', 'last_document_error_json');

-- public.invoices.trg_invoice_candidate_revision_v2_invoices_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoices_i AFTER INSERT ON invoices REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'client_id', 'invoice_no', 'type', 'original_invoice_id', 'subtotal_ex_vat', 'vat_amount', 'total_inc_vat', 'due_at_utc', 'notes', 'do_not_send', 'header_snapshot_json', 'status', 'issued_at_utc', 'paid_at_utc', 'on_hold_reason', 'document_revision', 'document_state', 'preview_document_version_id', 'issued_document_version_id', 'active_document_operation_id', 'issue_state', 'active_issue_operation_id', 'last_document_error_json');

-- public.invoices.trg_invoice_candidate_revision_v2_invoices_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_invoices_u AFTER UPDATE ON invoices REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'client_id', 'invoice_no', 'type', 'original_invoice_id', 'subtotal_ex_vat', 'vat_amount', 'total_inc_vat', 'due_at_utc', 'notes', 'do_not_send', 'header_snapshot_json', 'status', 'issued_at_utc', 'paid_at_utc', 'on_hold_reason', 'document_revision', 'document_state', 'preview_document_version_id', 'issued_document_version_id', 'active_document_operation_id', 'issue_state', 'active_issue_operation_id', 'last_document_error_json');

-- public.invoices.trg_invoice_document_invalidate_u
CREATE TRIGGER trg_invoice_document_invalidate_u AFTER UPDATE ON invoices REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_invoice_document_invalidate();

-- public.invoices.trg_invoices_set_invoice_no_bi
CREATE TRIGGER trg_invoices_set_invoice_no_bi BEFORE INSERT ON invoices FOR EACH ROW EXECUTE FUNCTION trg_invoices_set_invoice_no();

-- public.nhsp_shifts.trg_invoice_candidate_revision_v2_nhsp_shifts_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_nhsp_shifts_d AFTER DELETE ON nhsp_shifts REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'false', 'external_row_key', 'latest_import_id', 'candidate_id', 'client_id', 'contract_id', 'timesheet_id', 'work_date', 'ward', 'start_utc', 'end_utc', 'break_mins', 'pay_minutes', 'pay_amount_snapshot', 'charge_amount_snapshot', 'invoice_status', 'defer_until_run_after', 'invoice_id', 'source_system', 'hr_request_id', 'held_back_reason', 'assignment_code', 'ref_num', 'week_ending_date', 'cancelled_at_utc', 'cancelled_by_import_id', 'cancelled_reason');

-- public.nhsp_shifts.trg_invoice_candidate_revision_v2_nhsp_shifts_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_nhsp_shifts_i AFTER INSERT ON nhsp_shifts REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'false', 'external_row_key', 'latest_import_id', 'candidate_id', 'client_id', 'contract_id', 'timesheet_id', 'work_date', 'ward', 'start_utc', 'end_utc', 'break_mins', 'pay_minutes', 'pay_amount_snapshot', 'charge_amount_snapshot', 'invoice_status', 'defer_until_run_after', 'invoice_id', 'source_system', 'hr_request_id', 'held_back_reason', 'assignment_code', 'ref_num', 'week_ending_date', 'cancelled_at_utc', 'cancelled_by_import_id', 'cancelled_reason');

-- public.nhsp_shifts.trg_invoice_candidate_revision_v2_nhsp_shifts_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_nhsp_shifts_u AFTER UPDATE ON nhsp_shifts REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'false', 'external_row_key', 'latest_import_id', 'candidate_id', 'client_id', 'contract_id', 'timesheet_id', 'work_date', 'ward', 'start_utc', 'end_utc', 'break_mins', 'pay_minutes', 'pay_amount_snapshot', 'charge_amount_snapshot', 'invoice_status', 'defer_until_run_after', 'invoice_id', 'source_system', 'hr_request_id', 'held_back_reason', 'assignment_code', 'ref_num', 'week_ending_date', 'cancelled_at_utc', 'cancelled_by_import_id', 'cancelled_reason');

-- public.pay_advance_reservations.trg_bpay_wb_reservations_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_reservations_delete_dirty_v1 AFTER DELETE ON pay_advance_reservations REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_advance_reservations.trg_bpay_wb_reservations_insert_dirty_v1
CREATE TRIGGER trg_bpay_wb_reservations_insert_dirty_v1 AFTER INSERT ON pay_advance_reservations REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_advance_reservations.trg_bpay_wb_reservations_update_dirty_v1
CREATE TRIGGER trg_bpay_wb_reservations_update_dirty_v1 AFTER UPDATE ON pay_advance_reservations REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_advances.trg_bpay_wb_observe_advances_delete
CREATE TRIGGER trg_bpay_wb_observe_advances_delete AFTER DELETE ON pay_advances REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_advances.trg_bpay_wb_observe_advances_insert
CREATE TRIGGER trg_bpay_wb_observe_advances_insert AFTER INSERT ON pay_advances REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_advances.trg_bpay_wb_observe_advances_update
CREATE TRIGGER trg_bpay_wb_observe_advances_update AFTER UPDATE ON pay_advances REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_advances.trg_pay_advances_set_next_due
CREATE TRIGGER trg_pay_advances_set_next_due BEFORE INSERT OR UPDATE OF schedule_json ON pay_advances FOR EACH ROW EXECUTE FUNCTION trg_pay_advances_set_next_due();

-- public.pay_advances.trg_pay_workbench_mark_candidate_dirty__pay_advances
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__pay_advances BEFORE INSERT OR DELETE OR UPDATE ON pay_advances FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.pay_advances.trg_retention_capture_pay_advances_insert
CREATE TRIGGER trg_retention_capture_pay_advances_insert AFTER INSERT ON pay_advances REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_advances.trg_retention_capture_pay_advances_update
CREATE TRIGGER trg_retention_capture_pay_advances_update AFTER UPDATE ON pay_advances REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_advances.trg_ts_summary_pay_cache_advances_au
CREATE TRIGGER trg_ts_summary_pay_cache_advances_au AFTER UPDATE ON pay_advances REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_bank_transfer_events.trg_bpay_wb_transfer_events_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_transfer_events_delete_dirty_v1 AFTER DELETE ON pay_bank_transfer_events REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_bank_transfer_events.trg_bpay_wb_transfer_events_insert_dirty_v1
CREATE TRIGGER trg_bpay_wb_transfer_events_insert_dirty_v1 AFTER INSERT ON pay_bank_transfer_events REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_bank_transfer_events.trg_bpay_wb_transfer_events_update_dirty_v1
CREATE TRIGGER trg_bpay_wb_transfer_events_update_dirty_v1 AFTER UPDATE ON pay_bank_transfer_events REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_bank_transfers.trg_bpay_wb_transfers_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_transfers_delete_dirty_v1 AFTER DELETE ON pay_bank_transfers REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_bank_transfers.trg_pay_bank_transfers_normalise_status_biu
CREATE TRIGGER trg_pay_bank_transfers_normalise_status_biu BEFORE INSERT OR UPDATE OF status ON pay_bank_transfers FOR EACH ROW EXECUTE FUNCTION _pay_bank_transfers_normalise_status_biu();

-- public.pay_bank_transfers.trg_ts_summary_pay_cache_transfers_au
CREATE TRIGGER trg_ts_summary_pay_cache_transfers_au AFTER UPDATE ON pay_bank_transfers REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_batch_candidates.trg_bpay_wb_batch_candidates_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_batch_candidates_delete_dirty_v1 AFTER DELETE ON pay_batch_candidates REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_candidates.trg_ts_summary_pay_cache_candidates_au
CREATE TRIGGER trg_ts_summary_pay_cache_candidates_au AFTER UPDATE ON pay_batch_candidates REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_batch_display_summary.trg_pay_batch_display_summary_touch_updated_at_utc
CREATE TRIGGER trg_pay_batch_display_summary_touch_updated_at_utc BEFORE UPDATE ON pay_batch_display_summary FOR EACH ROW EXECUTE FUNCTION _cloudtms_touch_updated_at_utc();

-- public.pay_batch_item_breakdowns.pay_batch_item_breakdown_kind_guard_v1
CREATE TRIGGER pay_batch_item_breakdown_kind_guard_v1 BEFORE INSERT OR UPDATE OF pay_batch_item_id, line_kind ON pay_batch_item_breakdowns FOR EACH ROW EXECUTE FUNCTION _pay_batch_item_breakdown_kind_guard_v1();

-- public.pay_batch_item_breakdowns.trg_bpay_wb_breakdowns_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_breakdowns_delete_dirty_v1 AFTER DELETE ON pay_batch_item_breakdowns REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_item_breakdowns.trg_bpay_wb_breakdowns_insert_dirty_v1
CREATE TRIGGER trg_bpay_wb_breakdowns_insert_dirty_v1 AFTER INSERT ON pay_batch_item_breakdowns REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_item_breakdowns.trg_bpay_wb_breakdowns_update_dirty_v1
CREATE TRIGGER trg_bpay_wb_breakdowns_update_dirty_v1 AFTER UPDATE ON pay_batch_item_breakdowns REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_items.trg_bpay_wb_batch_items_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_batch_items_delete_dirty_v1 AFTER DELETE ON pay_batch_items REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_items.trg_bpay_wb_batch_items_insert_dirty_v1
CREATE TRIGGER trg_bpay_wb_batch_items_insert_dirty_v1 AFTER INSERT ON pay_batch_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_items.trg_bpay_wb_batch_items_update_dirty_v1
CREATE TRIGGER trg_bpay_wb_batch_items_update_dirty_v1 AFTER UPDATE ON pay_batch_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_items.trg_retention_capture_pay_batch_items_insert
CREATE TRIGGER trg_retention_capture_pay_batch_items_insert AFTER INSERT ON pay_batch_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_batch_items.trg_retention_capture_pay_batch_items_update
CREATE TRIGGER trg_retention_capture_pay_batch_items_update AFTER UPDATE ON pay_batch_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_batch_items.trg_ts_summary_pay_cache_items_ad
CREATE TRIGGER trg_ts_summary_pay_cache_items_ad AFTER DELETE ON pay_batch_items REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_batch_items.trg_ts_summary_pay_cache_items_au
CREATE TRIGGER trg_ts_summary_pay_cache_items_au AFTER UPDATE ON pay_batch_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_batch_timesheet_snapshots.trg_bpay_wb_snapshots_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_snapshots_delete_dirty_v1 AFTER DELETE ON pay_batch_timesheet_snapshots REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_timesheet_snapshots.trg_bpay_wb_snapshots_insert_dirty_v1
CREATE TRIGGER trg_bpay_wb_snapshots_insert_dirty_v1 AFTER INSERT ON pay_batch_timesheet_snapshots REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_timesheet_snapshots.trg_bpay_wb_snapshots_update_dirty_v1
CREATE TRIGGER trg_bpay_wb_snapshots_update_dirty_v1 AFTER UPDATE ON pay_batch_timesheet_snapshots REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batch_timesheet_snapshots.trg_retention_capture_pay_batch_timesheet_snapshots_insert
CREATE TRIGGER trg_retention_capture_pay_batch_timesheet_snapshots_insert AFTER INSERT ON pay_batch_timesheet_snapshots REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_batch_timesheet_snapshots.trg_retention_capture_pay_batch_timesheet_snapshots_update
CREATE TRIGGER trg_retention_capture_pay_batch_timesheet_snapshots_update AFTER UPDATE ON pay_batch_timesheet_snapshots REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_batches.trg_banking_alert_success_events_pay_batches_insert
CREATE TRIGGER trg_banking_alert_success_events_pay_batches_insert AFTER INSERT ON pay_batches FOR EACH ROW EXECUTE FUNCTION banking_alert_success_event_capture_pay_batch();

-- public.pay_batches.trg_banking_alert_success_events_pay_batches_update
CREATE TRIGGER trg_banking_alert_success_events_pay_batches_update AFTER UPDATE OF status, schedule_kind, scheduled_at_utc, completed_at_utc, execution_intent_json, bank_csv_export_json, settlement_confirmation_json, total_bank_out ON pay_batches FOR EACH ROW EXECUTE FUNCTION banking_alert_success_event_capture_pay_batch();

-- public.pay_batches.trg_bpay_wb_batches_delete_dirty_v1
CREATE TRIGGER trg_bpay_wb_batches_delete_dirty_v1 AFTER DELETE ON pay_batches REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_batches.trg_retention_capture_pay_batches_insert
CREATE TRIGGER trg_retention_capture_pay_batches_insert AFTER INSERT ON pay_batches REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_batches.trg_retention_capture_pay_batches_update
CREATE TRIGGER trg_retention_capture_pay_batches_update AFTER UPDATE ON pay_batches REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_batches.trg_ts_summary_pay_cache_batches_au
CREATE TRIGGER trg_ts_summary_pay_cache_batches_au AFTER UPDATE ON pay_batches REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_finance_case_components.trg_bpay_wb_observe_components_delete
CREATE TRIGGER trg_bpay_wb_observe_components_delete AFTER DELETE ON pay_finance_case_components REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_finance_case_components.trg_bpay_wb_observe_components_insert
CREATE TRIGGER trg_bpay_wb_observe_components_insert AFTER INSERT ON pay_finance_case_components REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_finance_case_components.trg_bpay_wb_observe_components_update
CREATE TRIGGER trg_bpay_wb_observe_components_update AFTER UPDATE ON pay_finance_case_components REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_finance_case_components.trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com
CREATE TRIGGER trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com BEFORE INSERT OR DELETE OR UPDATE ON pay_finance_case_components FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_finance_case_dirty();

-- public.pay_finance_case_components.trg_retention_capture_pay_finance_case_components_insert
CREATE TRIGGER trg_retention_capture_pay_finance_case_components_insert AFTER INSERT ON pay_finance_case_components REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_finance_case_components.trg_retention_capture_pay_finance_case_components_update
CREATE TRIGGER trg_retention_capture_pay_finance_case_components_update AFTER UPDATE ON pay_finance_case_components REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_finance_case_components.trg_ts_summary_pay_cache_finance_components_ad
CREATE TRIGGER trg_ts_summary_pay_cache_finance_components_ad AFTER DELETE ON pay_finance_case_components REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_finance_case_components.trg_ts_summary_pay_cache_finance_components_ai
CREATE TRIGGER trg_ts_summary_pay_cache_finance_components_ai AFTER INSERT ON pay_finance_case_components REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_finance_case_components.trg_ts_summary_pay_cache_finance_components_au
CREATE TRIGGER trg_ts_summary_pay_cache_finance_components_au AFTER UPDATE ON pay_finance_case_components REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_finance_case_events.trg_bpay_wb_observe_events_delete
CREATE TRIGGER trg_bpay_wb_observe_events_delete AFTER DELETE ON pay_finance_case_events REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_finance_case_events.trg_bpay_wb_observe_events_insert
CREATE TRIGGER trg_bpay_wb_observe_events_insert AFTER INSERT ON pay_finance_case_events REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_finance_case_events.trg_bpay_wb_observe_events_update
CREATE TRIGGER trg_bpay_wb_observe_events_update AFTER UPDATE ON pay_finance_case_events REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

-- public.pay_finance_case_events.trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve
CREATE TRIGGER trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve BEFORE INSERT OR DELETE OR UPDATE ON pay_finance_case_events FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_finance_case_dirty();

-- public.pay_item_snoozes.trg_pay_item_snoozes_sync_identity_fields_biu
CREATE TRIGGER trg_pay_item_snoozes_sync_identity_fields_biu BEFORE INSERT OR UPDATE OF candidate_id, timesheet_id, booking_id, segment_id, segment_stable_key, source_ref, snooze_kind, snooze_until_date, note ON pay_item_snoozes FOR EACH ROW EXECUTE FUNCTION pay_item_snoozes_sync_identity_fields();

-- public.pay_item_snoozes.trg_pay_workbench_mark_candidate_dirty__pay_item_snoozes
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__pay_item_snoozes AFTER INSERT OR DELETE OR UPDATE ON pay_item_snoozes FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.pay_manual_adjustment_carry_forwards.trg_retention_capture_pay_manual_adjustment_carry_forwards_inse
CREATE TRIGGER trg_retention_capture_pay_manual_adjustment_carry_forwards_inse AFTER INSERT ON pay_manual_adjustment_carry_forwards REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_manual_adjustment_carry_forwards.trg_retention_capture_pay_manual_adjustment_carry_forwards_upda
CREATE TRIGGER trg_retention_capture_pay_manual_adjustment_carry_forwards_upda AFTER UPDATE ON pay_manual_adjustment_carry_forwards REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_payment_correction_items.trg_retention_capture_pay_payment_correction_items_insert
CREATE TRIGGER trg_retention_capture_pay_payment_correction_items_insert AFTER INSERT ON pay_payment_correction_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_payment_correction_items.trg_retention_capture_pay_payment_correction_items_update
CREATE TRIGGER trg_retention_capture_pay_payment_correction_items_update AFTER UPDATE ON pay_payment_correction_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.pay_payment_correction_items.trg_ts_summary_pay_cache_correction_items_ad
CREATE TRIGGER trg_ts_summary_pay_cache_correction_items_ad AFTER DELETE ON pay_payment_correction_items REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_payment_correction_items.trg_ts_summary_pay_cache_correction_items_ai
CREATE TRIGGER trg_ts_summary_pay_cache_correction_items_ai AFTER INSERT ON pay_payment_correction_items REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_payment_correction_items.trg_ts_summary_pay_cache_correction_items_au
CREATE TRIGGER trg_ts_summary_pay_cache_correction_items_au AFTER UPDATE ON pay_payment_correction_items REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_payment_correction_requests.trg_ts_summary_pay_cache_correction_requests_ad
CREATE TRIGGER trg_ts_summary_pay_cache_correction_requests_ad AFTER DELETE ON pay_payment_correction_requests REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_payment_correction_requests.trg_ts_summary_pay_cache_correction_requests_ai
CREATE TRIGGER trg_ts_summary_pay_cache_correction_requests_ai AFTER INSERT ON pay_payment_correction_requests REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.pay_payment_correction_requests.trg_ts_summary_pay_cache_correction_requests_au
CREATE TRIGGER trg_ts_summary_pay_cache_correction_requests_au AFTER UPDATE ON pay_payment_correction_requests REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.rates_presets.trg_rates_presets_set_updated_at
CREATE TRIGGER trg_rates_presets_set_updated_at BEFORE UPDATE ON rates_presets FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.report_presets.trg_report_presets_set_updated_at
CREATE TRIGGER trg_report_presets_set_updated_at BEFORE UPDATE ON report_presets FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.settings_defaults.trg_invoice_candidate_revision_v2_settings_defaults_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_settings_defaults_d AFTER DELETE ON settings_defaults REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'vat_registration_number', 'agency_name', 'agency_logo', 'registered_address', 'company_reg_number', 'bank_name', 'bank_sort_code', 'bank_account_number', 'finance_email', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'invoice_document_presentation_json', 'timesheet_header_json', 'timesheet_footer_json', 'temporary_worker_declaration_json', 'client_declaration_json');

-- public.settings_defaults.trg_invoice_candidate_revision_v2_settings_defaults_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_settings_defaults_i AFTER INSERT ON settings_defaults REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'vat_registration_number', 'agency_name', 'agency_logo', 'registered_address', 'company_reg_number', 'bank_name', 'bank_sort_code', 'bank_account_number', 'finance_email', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'invoice_document_presentation_json', 'timesheet_header_json', 'timesheet_footer_json', 'temporary_worker_declaration_json', 'client_declaration_json');

-- public.settings_defaults.trg_invoice_candidate_revision_v2_settings_defaults_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_settings_defaults_u AFTER UPDATE ON settings_defaults REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'vat_registration_number', 'agency_name', 'agency_logo', 'registered_address', 'company_reg_number', 'bank_name', 'bank_sort_code', 'bank_account_number', 'finance_email', 'hr_attach_to_invoice', 'ts_attach_to_invoice', 'invoice_document_presentation_json', 'timesheet_header_json', 'timesheet_footer_json', 'temporary_worker_declaration_json', 'client_declaration_json');

-- public.settings_finance_windows.set_updated_at_settings_finance_windows
CREATE TRIGGER set_updated_at_settings_finance_windows BEFORE UPDATE ON settings_finance_windows FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at_settings_finance_windows();

-- public.settings_finance_windows.trg_invoice_candidate_revision_v2_settings_finance_windows_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_settings_finance_windows_d AFTER DELETE ON settings_finance_windows REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'false', 'date_from', 'date_to', 'vat_rate_pct', 'mileage_charge_defaults');

-- public.settings_finance_windows.trg_invoice_candidate_revision_v2_settings_finance_windows_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_settings_finance_windows_i AFTER INSERT ON settings_finance_windows REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'false', 'date_from', 'date_to', 'vat_rate_pct', 'mileage_charge_defaults');

-- public.settings_finance_windows.trg_invoice_candidate_revision_v2_settings_finance_windows_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_settings_finance_windows_u AFTER UPDATE ON settings_finance_windows REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'false', 'date_from', 'date_to', 'vat_rate_pct', 'mileage_charge_defaults');

-- public.settings_finance_windows.trg_tsfin_finance_windows_erni_wakeup_all_aiu
CREATE TRIGGER trg_tsfin_finance_windows_erni_wakeup_all_aiu AFTER INSERT OR UPDATE OF erni_pct, apply_erni_to, mileage_pay_defaults, mileage_charge_defaults, date_from, date_to ON settings_finance_windows FOR EACH ROW EXECUTE FUNCTION trg_tsfin_finance_windows_erni_wakeup_all();

-- public.sheets_outbox.trg_sheets_outbox_updated_at
CREATE TRIGGER trg_sheets_outbox_updated_at BEFORE UPDATE ON sheets_outbox FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.timesheet_evidence.timesheet_evidence_candidate_lineage_guard
CREATE TRIGGER timesheet_evidence_candidate_lineage_guard BEFORE INSERT OR UPDATE OF candidate_component_id, document_role, timesheet_id, storage_key ON timesheet_evidence FOR EACH ROW EXECUTE FUNCTION private._candidate_evidence_lineage_guard_v1();

-- public.timesheet_evidence.trg_invoice_candidate_revision_v2_timesheet_evidence_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheet_evidence_d AFTER DELETE ON timesheet_evidence REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'timesheet_id', 'kind', 'storage_key', 'source_revision', 'display_name', 'document_asset_id', 'processing_state', 'processing_error_json');

-- public.timesheet_evidence.trg_invoice_candidate_revision_v2_timesheet_evidence_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheet_evidence_i AFTER INSERT ON timesheet_evidence REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'timesheet_id', 'kind', 'storage_key', 'source_revision', 'display_name', 'document_asset_id', 'processing_state', 'processing_error_json');

-- public.timesheet_evidence.trg_invoice_candidate_revision_v2_timesheet_evidence_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheet_evidence_u AFTER UPDATE ON timesheet_evidence REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'timesheet_id', 'kind', 'storage_key', 'source_revision', 'display_name', 'document_asset_id', 'processing_state', 'processing_error_json');

-- public.timesheet_evidence.trg_timesheet_archived_evidence_guard_v1
CREATE TRIGGER trg_timesheet_archived_evidence_guard_v1 BEFORE INSERT OR DELETE OR UPDATE ON timesheet_evidence FOR EACH ROW EXECUTE FUNCTION timesheet_archived_evidence_guard_v1();

-- public.timesheet_evidence.trg_timesheet_evidence_document_invalidate_d
CREATE TRIGGER trg_timesheet_evidence_document_invalidate_d AFTER DELETE ON timesheet_evidence REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.timesheet_evidence.trg_timesheet_evidence_document_invalidate_i
CREATE TRIGGER trg_timesheet_evidence_document_invalidate_i AFTER INSERT ON timesheet_evidence REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.timesheet_evidence.trg_timesheet_evidence_document_invalidate_u
CREATE TRIGGER trg_timesheet_evidence_document_invalidate_u AFTER UPDATE ON timesheet_evidence REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.timesheet_pay_state.trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state AFTER INSERT OR DELETE OR UPDATE ON timesheet_pay_state FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.timesheet_pay_state.trg_retention_capture_timesheet_pay_state_insert
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_insert AFTER INSERT ON timesheet_pay_state REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheet_pay_state.trg_retention_capture_timesheet_pay_state_update
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_update AFTER UPDATE ON timesheet_pay_state REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheet_pay_state.trg_ts_summary_pay_cache_state_ad
CREATE TRIGGER trg_ts_summary_pay_cache_state_ad AFTER DELETE ON timesheet_pay_state REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_pay_state.trg_ts_summary_pay_cache_state_ai
CREATE TRIGGER trg_ts_summary_pay_cache_state_ai AFTER INSERT ON timesheet_pay_state REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_pay_state.trg_ts_summary_pay_cache_state_au
CREATE TRIGGER trg_ts_summary_pay_cache_state_au AFTER UPDATE ON timesheet_pay_state REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_pay_state_history.trg_retention_capture_timesheet_pay_state_history_insert
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_history_insert AFTER INSERT ON timesheet_pay_state_history REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheet_pay_state_history.trg_retention_capture_timesheet_pay_state_history_update
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_history_update AFTER UPDATE ON timesheet_pay_state_history REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheet_pay_state_history.trg_ts_summary_pay_cache_history_ad
CREATE TRIGGER trg_ts_summary_pay_cache_history_ad AFTER DELETE ON timesheet_pay_state_history REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_pay_state_history.trg_ts_summary_pay_cache_history_ai
CREATE TRIGGER trg_ts_summary_pay_cache_history_ai AFTER INSERT ON timesheet_pay_state_history REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_pay_state_history.trg_ts_summary_pay_cache_history_au
CREATE TRIGGER trg_ts_summary_pay_cache_history_au AFTER UPDATE ON timesheet_pay_state_history REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_payment_overrides.trg_pay_workbench_mark_candidate_dirty__timesheet_payment_overr
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__timesheet_payment_overr AFTER INSERT OR DELETE OR UPDATE ON timesheet_payment_overrides FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.timesheet_payment_overrides.trg_retention_capture_timesheet_payment_overrides_insert
CREATE TRIGGER trg_retention_capture_timesheet_payment_overrides_insert AFTER INSERT ON timesheet_payment_overrides REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheet_payment_overrides.trg_retention_capture_timesheet_payment_overrides_update
CREATE TRIGGER trg_retention_capture_timesheet_payment_overrides_update AFTER UPDATE ON timesheet_payment_overrides REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheet_payment_overrides.trg_ts_summary_pay_cache_override_ad
CREATE TRIGGER trg_ts_summary_pay_cache_override_ad AFTER DELETE ON timesheet_payment_overrides REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_payment_overrides.trg_ts_summary_pay_cache_override_ai
CREATE TRIGGER trg_ts_summary_pay_cache_override_ai AFTER INSERT ON timesheet_payment_overrides REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_payment_overrides.trg_ts_summary_pay_cache_override_au
CREATE TRIGGER trg_ts_summary_pay_cache_override_au AFTER UPDATE ON timesheet_payment_overrides REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheet_validations.trg_tsfin_timesheet_validations_wakeup
CREATE TRIGGER trg_tsfin_timesheet_validations_wakeup AFTER INSERT OR UPDATE ON timesheet_validations FOR EACH ROW EXECUTE FUNCTION trg_tsfin_timesheet_validations_wakeup();

-- public.timesheets.timesheets_candidate_qr_pack_ready_notification_trg
CREATE TRIGGER timesheets_candidate_qr_pack_ready_notification_trg BEFORE UPDATE OF document_state, current_document_version_id ON timesheets FOR EACH ROW EXECUTE FUNCTION private._candidate_qr_pack_ready_notification_v1();

-- public.timesheets.trg_cc_timesheets
CREATE TRIGGER trg_cc_timesheets AFTER INSERT OR DELETE OR UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('timesheets');

-- public.timesheets.trg_invoice_candidate_revision_v2_timesheets_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheets_d AFTER DELETE ON timesheets REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'booking_id', 'occupant_key_norm', 'hospital_norm', 'ward_norm', 'job_title_norm', 'shift_label_norm', 'scheduled_start_iso', 'scheduled_end_iso', 'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso', 'break_minutes', 'worked_minutes', 'week_ending_date', 'auth_name', 'auth_job_title', 'authorised_at_server', 'r2_nurse_key', 'r2_auth_key', 'img_sha256_nurse', 'img_sha256_auth', 'reference_number', 'status', 'version', 'is_current', 'revoked_at', 'contract_id', 'submission_mode', 'line_type', 'sheet_scope', 'actual_schedule_json', 'additional_units_week', 'additional_units_per_day', 'day_references_json', 'qr_signed_hash', 'qr_signed_at_utc', 'qr_status', 'qr_r2_key', 'candidate_hint_text', 'band', 'is_adjustment', 'parent_timesheet_id', 'correction_id', 'correction_kind', 'adjustment_origin', 'archived_at_utc', 'archived_by_user_id', 'archived_reason_code', 'document_revision', 'document_state', 'current_document_version_id', 'active_document_operation_id', 'manual_document_asset_id', 'last_document_error_json');

-- public.timesheets.trg_invoice_candidate_revision_v2_timesheets_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheets_i AFTER INSERT ON timesheets REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'booking_id', 'occupant_key_norm', 'hospital_norm', 'ward_norm', 'job_title_norm', 'shift_label_norm', 'scheduled_start_iso', 'scheduled_end_iso', 'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso', 'break_minutes', 'worked_minutes', 'week_ending_date', 'auth_name', 'auth_job_title', 'authorised_at_server', 'r2_nurse_key', 'r2_auth_key', 'img_sha256_nurse', 'img_sha256_auth', 'reference_number', 'status', 'version', 'is_current', 'revoked_at', 'contract_id', 'submission_mode', 'line_type', 'sheet_scope', 'actual_schedule_json', 'additional_units_week', 'additional_units_per_day', 'day_references_json', 'qr_signed_hash', 'qr_signed_at_utc', 'qr_status', 'qr_r2_key', 'candidate_hint_text', 'band', 'is_adjustment', 'parent_timesheet_id', 'correction_id', 'correction_kind', 'adjustment_origin', 'archived_at_utc', 'archived_by_user_id', 'archived_reason_code', 'document_revision', 'document_state', 'current_document_version_id', 'active_document_operation_id', 'manual_document_asset_id', 'last_document_error_json');

-- public.timesheets.trg_invoice_candidate_revision_v2_timesheets_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheets_u AFTER UPDATE ON timesheets REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'booking_id', 'occupant_key_norm', 'hospital_norm', 'ward_norm', 'job_title_norm', 'shift_label_norm', 'scheduled_start_iso', 'scheduled_end_iso', 'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso', 'break_minutes', 'worked_minutes', 'week_ending_date', 'auth_name', 'auth_job_title', 'authorised_at_server', 'r2_nurse_key', 'r2_auth_key', 'img_sha256_nurse', 'img_sha256_auth', 'reference_number', 'status', 'version', 'is_current', 'revoked_at', 'contract_id', 'submission_mode', 'line_type', 'sheet_scope', 'actual_schedule_json', 'additional_units_week', 'additional_units_per_day', 'day_references_json', 'qr_signed_hash', 'qr_signed_at_utc', 'qr_status', 'qr_r2_key', 'candidate_hint_text', 'band', 'is_adjustment', 'parent_timesheet_id', 'correction_id', 'correction_kind', 'adjustment_origin', 'archived_at_utc', 'archived_by_user_id', 'archived_reason_code', 'document_revision', 'document_state', 'current_document_version_id', 'active_document_operation_id', 'manual_document_asset_id', 'last_document_error_json');

-- public.timesheets.trg_pay_workbench_mark_candidate_dirty__timesheets
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__timesheets AFTER INSERT OR DELETE OR UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.timesheets.trg_timesheet_archive_row_guard_v1
CREATE TRIGGER trg_timesheet_archive_row_guard_v1 BEFORE INSERT OR DELETE OR UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION timesheet_archive_row_guard_v1();

-- public.timesheets.trg_timesheets_document_invalidate_u
CREATE TRIGGER trg_timesheets_document_invalidate_u AFTER UPDATE ON timesheets REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.timesheets.trg_timesheets_invalidate_prevalidation_on_change
CREATE TRIGGER trg_timesheets_invalidate_prevalidation_on_change AFTER UPDATE OF worked_start_iso, worked_end_iso, break_start_iso, break_end_iso, break_minutes, reference_number, day_references_json, actual_schedule_json, additional_units_week, additional_units_per_day ON timesheets FOR EACH ROW WHEN (COALESCE(new.is_current, false) = true AND COALESCE(old.is_current, false) = true) EXECUTE FUNCTION timesheets_invalidate_prevalidation_on_change();

-- public.timesheets.trg_timesheets_updated_at
CREATE TRIGGER trg_timesheets_updated_at BEFORE UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- public.timesheets.trg_ts_summary_pay_cache_timesheets_au
CREATE TRIGGER trg_ts_summary_pay_cache_timesheets_au AFTER UPDATE ON timesheets REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheets.trg_tsfin_ai
CREATE TRIGGER trg_tsfin_ai AFTER INSERT ON timesheets FOR EACH ROW EXECUTE FUNCTION trg_tsfin_after_insert();

-- public.timesheets.trg_tsfin_au
CREATE TRIGGER trg_tsfin_au AFTER UPDATE OF is_current, authorised_at_server, revoked_at ON timesheets FOR EACH ROW EXECUTE FUNCTION trg_tsfin_after_update();

-- public.timesheets_financials.trg_cc_timesheets_financials
CREATE TRIGGER trg_cc_timesheets_financials AFTER INSERT OR DELETE OR UPDATE ON timesheets_financials FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('timesheets');

-- public.timesheets_financials.trg_invoice_candidate_revision_v2_timesheets_financials_d
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheets_financials_d AFTER DELETE ON timesheets_financials REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'timesheet_id', 'timesheet_version', 'basis', 'is_current', 'is_stale', 'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso', 'break_minutes', 'candidate_id', 'client_id', 'role', 'band', 'policy_snapshot_json', 'rate_source_refs_json', 'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh', 'total_hours', 'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat', 'processing_status', 'expenses_pay_ex_vat', 'expenses_charge_ex_vat', 'expenses_description', 'expenses_evidence_manifest', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat', 'mileage_pay_rate', 'mileage_charge_rate', 'mileage_evidence_manifest', 'actual_schedule_json', 'actual_minutes_by_day_json', 'additional_units_json', 'additional_pay_ex_vat', 'additional_charge_ex_vat', 'additional_margin_ex_vat', 'invoice_breakdown_json', 'nhsp_import_id', 'has_rate_issue', 'hr_crosscheck_status', 'hr_crosscheck_issues', 'external_source_rows_json', 'mileage_units', 'travel_pay_ex_vat', 'travel_charge_ex_vat', 'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat', 'other_pay_ex_vat', 'other_charge_ex_vat', 'stale_reason', 'pay_method', 'locked_by_invoice_id', 'unlocked_by_credit_note_id', 'po_number', 'pay_on_hold', 'pay_on_hold_reason', 'has_pay_channel_issue');

-- public.timesheets_financials.trg_invoice_candidate_revision_v2_timesheets_financials_i
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheets_financials_i AFTER INSERT ON timesheets_financials REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'timesheet_id', 'timesheet_version', 'basis', 'is_current', 'is_stale', 'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso', 'break_minutes', 'candidate_id', 'client_id', 'role', 'band', 'policy_snapshot_json', 'rate_source_refs_json', 'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh', 'total_hours', 'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat', 'processing_status', 'expenses_pay_ex_vat', 'expenses_charge_ex_vat', 'expenses_description', 'expenses_evidence_manifest', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat', 'mileage_pay_rate', 'mileage_charge_rate', 'mileage_evidence_manifest', 'actual_schedule_json', 'actual_minutes_by_day_json', 'additional_units_json', 'additional_pay_ex_vat', 'additional_charge_ex_vat', 'additional_margin_ex_vat', 'invoice_breakdown_json', 'nhsp_import_id', 'has_rate_issue', 'hr_crosscheck_status', 'hr_crosscheck_issues', 'external_source_rows_json', 'mileage_units', 'travel_pay_ex_vat', 'travel_charge_ex_vat', 'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat', 'other_pay_ex_vat', 'other_charge_ex_vat', 'stale_reason', 'pay_method', 'locked_by_invoice_id', 'unlocked_by_credit_note_id', 'po_number', 'pay_on_hold', 'pay_on_hold_reason', 'has_pay_channel_issue');

-- public.timesheets_financials.trg_invoice_candidate_revision_v2_timesheets_financials_u
CREATE TRIGGER trg_invoice_candidate_revision_v2_timesheets_financials_u AFTER UPDATE ON timesheets_financials REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private._invoice_candidate_revision_trigger_v2('true', 'true', 'timesheet_id', 'timesheet_version', 'basis', 'is_current', 'is_stale', 'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso', 'break_minutes', 'candidate_id', 'client_id', 'role', 'band', 'policy_snapshot_json', 'rate_source_refs_json', 'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh', 'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh', 'total_hours', 'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat', 'processing_status', 'expenses_pay_ex_vat', 'expenses_charge_ex_vat', 'expenses_description', 'expenses_evidence_manifest', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat', 'mileage_pay_rate', 'mileage_charge_rate', 'mileage_evidence_manifest', 'actual_schedule_json', 'actual_minutes_by_day_json', 'additional_units_json', 'additional_pay_ex_vat', 'additional_charge_ex_vat', 'additional_margin_ex_vat', 'invoice_breakdown_json', 'nhsp_import_id', 'has_rate_issue', 'hr_crosscheck_status', 'hr_crosscheck_issues', 'external_source_rows_json', 'mileage_units', 'travel_pay_ex_vat', 'travel_charge_ex_vat', 'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat', 'other_pay_ex_vat', 'other_charge_ex_vat', 'stale_reason', 'pay_method', 'locked_by_invoice_id', 'unlocked_by_credit_note_id', 'po_number', 'pay_on_hold', 'pay_on_hold_reason', 'has_pay_channel_issue');

-- public.timesheets_financials.trg_pay_workbench_mark_candidate_dirty__timesheets_financials
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__timesheets_financials AFTER INSERT OR DELETE OR UPDATE ON timesheets_financials FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.timesheets_financials.trg_retention_capture_timesheets_financials_insert
CREATE TRIGGER trg_retention_capture_timesheets_financials_insert AFTER INSERT ON timesheets_financials REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheets_financials.trg_retention_capture_timesheets_financials_update
CREATE TRIGGER trg_retention_capture_timesheets_financials_update AFTER UPDATE ON timesheets_financials REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.timesheets_financials.trg_set_updated_at_tsfin
CREATE TRIGGER trg_set_updated_at_tsfin BEFORE UPDATE ON timesheets_financials FOR EACH ROW EXECUTE FUNCTION set_updated_at_tsfin();

-- public.timesheets_financials.trg_ts_summary_pay_cache_tsfin_ad
CREATE TRIGGER trg_ts_summary_pay_cache_tsfin_ad AFTER DELETE ON timesheets_financials REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheets_financials.trg_ts_summary_pay_cache_tsfin_ai
CREATE TRIGGER trg_ts_summary_pay_cache_tsfin_ai AFTER INSERT ON timesheets_financials REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheets_financials.trg_ts_summary_pay_cache_tsfin_au
CREATE TRIGGER trg_ts_summary_pay_cache_tsfin_au AFTER UPDATE ON timesheets_financials REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.timesheets_financials.trg_tsfin_document_invalidate_d
CREATE TRIGGER trg_tsfin_document_invalidate_d AFTER DELETE ON timesheets_financials REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.timesheets_financials.trg_tsfin_document_invalidate_i
CREATE TRIGGER trg_tsfin_document_invalidate_i AFTER INSERT ON timesheets_financials REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.timesheets_financials.trg_tsfin_document_invalidate_u
CREATE TRIGGER trg_tsfin_document_invalidate_u AFTER UPDATE ON timesheets_financials REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION trg_timesheet_document_invalidate();

-- public.tms_users.tms_users_touch
CREATE TRIGGER tms_users_touch BEFORE UPDATE ON tms_users FOR EACH ROW EXECUTE FUNCTION tms_touch_updated_at();

-- public.ts_pay_adjustments.trg_pay_workbench_mark_candidate_dirty__ts_pay_adjustments
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__ts_pay_adjustments AFTER INSERT OR DELETE OR UPDATE ON ts_pay_adjustments FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.ts_pay_adjustments.trg_retention_capture_ts_pay_adjustments_insert
CREATE TRIGGER trg_retention_capture_ts_pay_adjustments_insert AFTER INSERT ON ts_pay_adjustments REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.ts_pay_adjustments.trg_retention_capture_ts_pay_adjustments_update
CREATE TRIGGER trg_retention_capture_ts_pay_adjustments_update AFTER UPDATE ON ts_pay_adjustments REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION timesheet_financial_retention_capture_trigger_v1();

-- public.ts_pay_adjustments.trg_ts_summary_pay_cache_adjustments_ad
CREATE TRIGGER trg_ts_summary_pay_cache_adjustments_ad AFTER DELETE ON ts_pay_adjustments REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.ts_pay_adjustments.trg_ts_summary_pay_cache_adjustments_ai
CREATE TRIGGER trg_ts_summary_pay_cache_adjustments_ai AFTER INSERT ON ts_pay_adjustments REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.ts_pay_adjustments.trg_ts_summary_pay_cache_adjustments_au
CREATE TRIGGER trg_ts_summary_pay_cache_adjustments_au AFTER UPDATE ON ts_pay_adjustments REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION pay_timesheet_summary_pay_state_refresh_trigger();

-- public.umbrellas.trg_cc_umbrellas
CREATE TRIGGER trg_cc_umbrellas AFTER INSERT OR DELETE OR UPDATE ON umbrellas FOR EACH ROW EXECUTE FUNCTION _trg_change_bump('umbrellas');

-- public.umbrellas.trg_pay_workbench_mark_candidate_dirty__umbrellas
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__umbrellas AFTER INSERT OR DELETE OR UPDATE ON umbrellas FOR EACH ROW EXECUTE FUNCTION pay_workbench_mark_candidate_dirty();

-- public.umbrellas.trg_umbrellas_set_bank_hash
CREATE TRIGGER trg_umbrellas_set_bank_hash BEFORE INSERT OR UPDATE OF sort_code, account_number, name ON umbrellas FOR EACH ROW EXECUTE FUNCTION _trg_umbrellas_set_bank_hash();

