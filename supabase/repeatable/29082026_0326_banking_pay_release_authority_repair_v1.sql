-- Exact provider-neutral authority repair for Banking Pay modal v2.
--
-- The immutable historical compatibility replay was accidentally changed by
-- the first capability-off release. Its partial hosted replay replaced eight
-- current routine bodies before failing. This closure restores only those
-- eight certified definitions and their service-only ACLs. It deliberately
-- contains no broad include, provider setting, economic rewrite or alternate
-- Draft/selection owner.
--
-- Generated from the named current authoritative source files by:
--   node scripts/generate-banking-pay-release-authority-repair.mjs

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.bulk_authorise_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_classification text := NULL;
  v_show_daily boolean := TRUE;
  v_show_weekly boolean := TRUE;
  v_show_manual boolean := TRUE;
  v_show_qr boolean := TRUE;
  v_show_electronic boolean := TRUE;
  v_validation_already boolean := TRUE;
  v_validation_awaiting boolean := TRUE;
  v_show_authorised_invoiced_unissued boolean := FALSE;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_classification := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'classification', v_filters->>'classificationRaw', '')), ''));
  IF v_classification NOT IN ('TIMESHEETS', 'NHSP', 'HR') THEN
    v_classification := NULL;
  END IF;

  v_show_daily := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_weekly := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_manual', v_filters->>'showManual', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_qr := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_qr', v_filters->>'showQr', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_electronic := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_electronic', v_filters->>'showElectronic', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_already := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_already', v_filters->>'validationAlready', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_awaiting := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_awaiting', v_filters->>'validationAwaiting', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_authorised_invoiced_unissued := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_authorised_invoiced_unissued', v_filters->>'showAuthorisedInvoicedUnissued', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  IF EXISTS(
    SELECT 1
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters||jsonb_build_object('disable_paging',true,'apply_paging',false,'profile','list','include_evidence',false,'include_compare',false,'include_import_source_rows',false)
    ) s
    JOIN public.timesheets AS correction_timesheet
      ON correction_timesheet.timesheet_id = s.timesheet_id
    WHERE correction_timesheet.is_current=true and correction_timesheet.archived_at_utc is null
      and coalesce(correction_timesheet.is_adjustment,false) and correction_timesheet.parent_timesheet_id is not null
      and correction_timesheet.correction_id is not null and upper(coalesce(correction_timesheet.adjustment_origin,''))='IMPORT_CORRECTION'
      and correction_timesheet.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and ((upper(coalesce(s.route_type,'')) like '%NHSP%' or upper(coalesce(s.basis,'')) like 'NHSP%'
          or upper(coalesce(correction_timesheet.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
        = (upper(coalesce(s.route_type,'')) like '%HEALTHROSTER%' or upper(coalesce(s.basis,'')) like 'HEALTHROSTER%'
          or upper(coalesce(correction_timesheet.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER'))
  ) THEN
    RAISE EXCEPTION 'BULK_AUTHORISE_CORRECTION_SOURCE_CONFLICT' USING ERRCODE='22023';
  END IF;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT
      summary_row.*,
      correction_timesheet.is_current AS correction_is_current,
      correction_timesheet.archived_at_utc AS correction_archived_at_utc,
      correction_timesheet.is_adjustment AS correction_is_adjustment,
      correction_timesheet.parent_timesheet_id AS correction_parent_timesheet_id,
      correction_timesheet.correction_id,
      correction_timesheet.adjustment_origin,
      correction_timesheet.correction_kind,
      correction_timesheet.candidate_hint_text AS correction_candidate_hint_text
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    LEFT JOIN public.timesheets AS correction_timesheet
      ON correction_timesheet.timesheet_id = summary_row.timesheet_id
    WHERE (summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL)
      AND UPPER(COALESCE(summary_row.tools_stage, '')) <> 'ARCHIVED'
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      CASE
        WHEN lightweight_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || lightweight_rows.timesheet_id::text
        WHEN lightweight_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || lightweight_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and lightweight_rows.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and (upper(coalesce(lightweight_rows.route_type,'')) like '%HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.basis,'')) like 'HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.correction_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER')
        THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
        THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.sheet_scope, lightweight_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN lightweight_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(lightweight_rows.processing_status, lightweight_rows.tools_stage, lightweight_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(lightweight_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
        OR EXISTS (
          SELECT 1
          FROM public.invoice_lines AS issued_invoice_line
          JOIN public.invoices AS issued_invoice
            ON issued_invoice.id = issued_invoice_line.invoice_id
          WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
            AND (
              issued_invoice.issued_at_utc IS NOT NULL
              OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
            )
        )
      ) AS locked_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS draft_invoice_line
        JOIN public.invoices AS draft_invoice
          ON draft_invoice.id = draft_invoice_line.invoice_id
        WHERE draft_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND draft_invoice.issued_at_utc IS NULL
          AND UPPER(COALESCE(draft_invoice.status::text, '')) = 'DRAFT'
      ) AS has_unissued_invoice_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS issued_invoice_line
        JOIN public.invoices AS issued_invoice
          ON issued_invoice.id = issued_invoice_line.invoice_id
        WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND (
            issued_invoice.issued_at_utc IS NOT NULL
            OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
          )
      ) AS has_issued_invoice_calc,
      COALESCE(lightweight_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(lightweight_rows.qr_status, '')) = 'PENDING'
        AND lightweight_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(lightweight_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(lightweight_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and (upper(coalesce(lightweight_rows.route_type,'')) like '%HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.basis,'')) like 'HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.correction_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER') then 'HR'
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION' then 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HR'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(lightweight_rows.timesheet_id::text, ''),
        COALESCE(lightweight_rows.contract_week_id::text, ''),
        COALESCE(lightweight_rows.processing_status, ''),
        COALESCE(lightweight_rows.summary_stage, ''),
        COALESCE(lightweight_rows.tools_stage, ''),
        COALESCE(lightweight_rows.authorised_at_utc::text, ''),
        COALESCE(lightweight_rows.authorised_at_server::text, ''),
        COALESCE(lightweight_rows.processed_at_utc::text, ''),
        COALESCE(lightweight_rows.paid_at_utc::text, ''),
        COALESCE(lightweight_rows.invoice_segments_locked::text, ''),
        COALESCE(lightweight_rows.route_family, ''),
        COALESCE(lightweight_rows.route_subfamily, ''),
        COALESCE(lightweight_rows.validation_status, ''),
        COALESCE(lightweight_rows.attached_evidence_count::text, ''),
        COALESCE(lightweight_rows.primary_artifact_storage_key, '')
      )) AS row_signature_calc
    FROM lightweight_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.requires_authorisation_calc = TRUE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
        AND NOT (
          'DUPLICATE_EXPENSE_REVIEW'=ANY(COALESCE(classified_rows.issue_codes,ARRAY[]::text[]))
        )
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_unauthorise_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS can_unprocess_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR (classified_rows.contract_week_id IS NOT NULL AND classified_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND COALESCE(classified_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        classified_rows.locked_calc = TRUE
        OR classified_rows.authorised_calc = TRUE
        OR classified_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc
    FROM classified_rows
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status,
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'correction_id',decision_rows.correction_id,
        'correction_kind',decision_rows.correction_kind,
        'adjustment_origin',decision_rows.adjustment_origin,
        'correction_source_system',case when decision_rows.correction_is_current=true and decision_rows.correction_archived_at_utc is null
          and coalesce(decision_rows.correction_is_adjustment,false) and decision_rows.correction_parent_timesheet_id is not null
          and decision_rows.correction_id is not null and upper(coalesce(decision_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          then case when decision_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
        'correction_display_label',case when decision_rows.correction_is_current=true and decision_rows.correction_archived_at_utc is null
          and coalesce(decision_rows.correction_is_adjustment,false) and decision_rows.correction_parent_timesheet_id is not null
          and decision_rows.correction_id is not null and upper(coalesce(decision_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          then case
            when decision_rows.bulk_authorise_classification_calc='HR' and decision_rows.correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
            when decision_rows.bulk_authorise_classification_calc='HR' and decision_rows.correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
            when decision_rows.bulk_authorise_classification_calc='NHSP' and decision_rows.correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
            when decision_rows.bulk_authorise_classification_calc='NHSP' and decision_rows.correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
        'bulk_authorise_section',
        CASE
          WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible'
          WHEN decision_rows.timesheet_id IS NOT NULL
            AND decision_rows.requires_authorisation_calc = TRUE
            AND decision_rows.authorised_calc = FALSE
            AND 'DUPLICATE_EXPENSE_REVIEW'=ANY(COALESCE(decision_rows.issue_codes,ARRAY[]::text[]))
            THEN 'processed_review_required'
          WHEN decision_rows.timesheet_id IS NOT NULL
            AND decision_rows.authorised_calc = TRUE
            AND decision_rows.locked_calc = FALSE
            AND decision_rows.has_issued_invoice_calc = FALSE
            AND (decision_rows.has_unissued_invoice_calc = FALSE OR v_show_authorised_invoiced_unissued = TRUE)
            THEN 'authorised_eligible'
          ELSE NULL::text
        END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        CASE
          WHEN decision_rows.has_issued_invoice_calc THEN 'ISSUED'
          WHEN decision_rows.has_unissued_invoice_calc THEN 'DRAFT'
          ELSE NULL::text
        END,
        'has_unissued_invoice',
        decision_rows.has_unissued_invoice_calc,
        'has_issued_invoice',
        decision_rows.has_issued_invoice_calc,
        'is_invoiced',
        decision_rows.has_unissued_invoice_calc OR decision_rows.has_issued_invoice_calc,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        decision_rows.is_adjusted,
        'is_adjustment',
        decision_rows.is_adjusted,
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        decision_rows.has_any_evidence,
        'attached_evidence_count',
        decision_rows.attached_evidence_count,
        'evidence_count',
        decision_rows.attached_evidence_count,
        'primary_artifact_id',
        NULL::text,
        'primary_artifact_kind',
        CASE WHEN decision_rows.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET' ELSE NULL::text END,
        'primary_artifact_storage_key',
        decision_rows.primary_artifact_storage_key,
        'primary_artifact_display_name',
        decision_rows.primary_artifact_display_name,
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'invoice_locked' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'bulk_authorise_block_code',
        CASE WHEN 'DUPLICATE_EXPENSE_REVIEW'=ANY(COALESCE(decision_rows.issue_codes,ARRAY[]::text[]))
          THEN 'DUPLICATE_EXPENSE_REVIEW_REQUIRED' ELSE NULL::text END,
        'bulk_authorise_block_message',
        CASE WHEN 'DUPLICATE_EXPENSE_REVIEW'=ANY(COALESCE(decision_rows.issue_codes,ARRAY[]::text[]))
          THEN 'Not included in bulk authorisation because an expense category was already submitted for this Client and week ending.'
          ELSE NULL::text END,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        decision_rows.row_signature_calc
      )
      || JSONB_BUILD_OBJECT(
        'backend_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc),
        'mutation_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc)
      )
      || JSONB_BUILD_OBJECT(
        'evidence_badges',
        JSONB_BUILD_ARRAY(
          JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(decision_rows.has_any_evidence, FALSE), 'has_evidence', COALESCE(decision_rows.has_any_evidence, FALSE)),
          JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', decision_rows.has_any_evidence,
          'attached_evidence_count', decision_rows.attached_evidence_count,
          'primary_artifact_storage_key', decision_rows.primary_artifact_storage_key,
          'primary_artifact_display_name', decision_rows.primary_artifact_display_name,
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(decision_rows.timesheet_id, decision_rows.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  canonical_authorise_signature_rows AS MATERIALIZED (
    SELECT canonical_patch.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      JSONB_BUILD_OBJECT(
        'dataset_mode', 'authorise',
        'projection', 'dataset_row',
        'row_keys', COALESCE((
          SELECT JSONB_AGG(payload_rows.row_json->>'row_key' ORDER BY payload_rows.row_json->>'row_key')
          FROM payload_rows
          WHERE NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'row_key', '')), '') IS NOT NULL
        ), '[]'::jsonb)
      )
    ) AS canonical_patch(row_json)
  ),
  canonical_payload_rows AS MATERIALIZED (
    SELECT
      (
        payload_rows.row_json
        || jsonb_strip_nulls(jsonb_build_object(
          'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'mutation_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'mutation_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'summary_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'summary_stage', '')), ''),
          'tools_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', '')), ''),
          'processing_status', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'processing_status', '')), '')
        ))
        || jsonb_build_object(
          'has_retained_financial_history', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'can_unprocess', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_action_visible', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
          'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
          'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'can_archive', FALSE,
          'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
        )
        || jsonb_build_object(
          'action_flags',
            COALESCE(payload_rows.row_json->'action_flags', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'action_flags', '{}'::jsonb)
            || jsonb_build_object(
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
              'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'can_archive', FALSE,
              'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
            ),
          'row_patch',
            COALESCE(payload_rows.row_json->'row_patch', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'row_patch', '{}'::jsonb)
            || jsonb_strip_nulls(jsonb_build_object(
              'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), '')
            ))
        )
      ) AS row_json
    FROM payload_rows
    LEFT JOIN canonical_authorise_signature_rows
      ON canonical_authorise_signature_rows.row_json->>'row_key' = payload_rows.row_json->>'row_key'
  ),
  eligible_rows_before_classification AS MATERIALIZED (
    SELECT canonical_payload_rows.row_json
    FROM canonical_payload_rows
    WHERE NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(canonical_payload_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
  ),
  classification_filtered_rows AS MATERIALIZED (
    SELECT eligible_rows_before_classification.row_json
    FROM eligible_rows_before_classification
    WHERE v_classification IS NULL
       OR UPPER(COALESCE(eligible_rows_before_classification.row_json->>'bulk_authorise_classification', '')) = v_classification
  ),
  visible_rows AS MATERIALIZED (
    SELECT classification_filtered_rows.row_json
    FROM classification_filtered_rows
    WHERE (
        UPPER(COALESCE(classification_filtered_rows.row_json->>'bulk_authorise_classification', '')) <> 'TIMESHEETS'
        OR (
          CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'DAILY' THEN v_show_daily
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' THEN v_show_weekly
            ELSE TRUE
          END
          AND CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'MANUAL_NON_QR' THEN v_show_manual
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'QR' THEN v_show_qr
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'ELECTRONIC' THEN v_show_electronic
            ELSE v_show_manual AND v_show_qr AND v_show_electronic
          END
          AND CASE
            WHEN COALESCE((classification_filtered_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE THEN v_validation_awaiting
            ELSE v_validation_already
          END
        )
      )
  ),
  paged_visible_rows AS MATERIALIZED (
    SELECT visible_rows.row_json
    FROM visible_rows
    ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  visible_counts AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'processed_eligible')::integer AS processed_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'authorised_eligible')::integer AS authorised_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'MANUAL_NON_QR')::integer AS manual_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'QR')::integer AS qr_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'ELECTRONIC')::integer AS electronic_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = FALSE)::integer AS already_validated_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE)::integer AS awaiting_validation_count
    FROM visible_rows
  ),
  eligible_counts AS (
    SELECT
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'TIMESHEETS')::integer AS timesheets_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'NHSP')::integer AS nhsp_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'HR')::integer AS hr_count
    FROM eligible_rows_before_classification
  ),
  rows_payload AS (
    SELECT COALESCE(
      JSONB_AGG(
        paged_visible_rows.row_json
        ORDER BY paged_visible_rows.row_json->>'week_ending_date', paged_visible_rows.row_json->>'client_name', paged_visible_rows.row_json->>'candidate_name', paged_visible_rows.row_json->>'row_key'
      ),
      '[]'::jsonb
    ) AS rows_json
    FROM paged_visible_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', COALESCE(visible_counts.total_count, 0),
      'processed_eligible', COALESCE(visible_counts.processed_eligible_count, 0),
      'authorised_eligible', COALESCE(visible_counts.authorised_eligible_count, 0),
      'by_classification', JSONB_BUILD_OBJECT(
        'TIMESHEETS', COALESCE(eligible_counts.timesheets_count, 0),
        'NHSP', COALESCE(eligible_counts.nhsp_count, 0),
        'HR', COALESCE(eligible_counts.hr_count, 0)
      ),
      'timesheets_by_type', JSONB_BUILD_OBJECT(
        'manual', COALESCE(visible_counts.manual_count, 0),
        'qr', COALESCE(visible_counts.qr_count, 0),
        'electronic', COALESCE(visible_counts.electronic_count, 0)
      ),
      'validation', JSONB_BUILD_OBJECT(
        'already_validated', COALESCE(visible_counts.already_validated_count, 0),
        'awaiting_validation', COALESCE(visible_counts.awaiting_validation_count, 0),
        'scope', 'visible_rows_after_classification_and_toggle_filters'
      ),
      'scope', JSONB_BUILD_OBJECT(
        'total', 'visible_rows_after_classification_and_toggle_filters',
        'by_classification', 'eligible_rows_before_classification_filter',
        'timesheets_by_type', 'visible_rows_after_classification_and_toggle_filters',
        'validation', 'visible_rows_after_classification_and_toggle_filters'
      )
    ),
    'rows', COALESCE(rows_payload.rows_json, '[]'::jsonb)
  )
  INTO v_out
  FROM visible_counts
  CROSS JOIN eligible_counts
  CROSS JOIN rows_payload;

  RETURN private._candidate_dataset_overlay_v1(COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', 0,
      'processed_eligible', 0,
      'authorised_eligible', 0,
      'by_classification', JSONB_BUILD_OBJECT('TIMESHEETS', 0, 'NHSP', 0, 'HR', 0),
      'timesheets_by_type', JSONB_BUILD_OBJECT('manual', 0, 'qr', 0, 'electronic', 0),
      'validation', JSONB_BUILD_OBJECT('already_validated', 0, 'awaiting_validation', 0)
    ),
    'rows', '[]'::jsonb
  )));
END;
$function$;

ALTER FUNCTION public.bulk_authorise_dataset_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bulk_authorise_dataset_v1(jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bulk_authorise_dataset_v1(jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.bulk_process_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_show_weekly_manual boolean := TRUE;
  v_show_daily_manual boolean := TRUE;
  v_bucket text := NULL;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_show_weekly_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly_manual', v_filters->>'showWeeklyManual', v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_daily_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily_manual', v_filters->>'showDailyManual', v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_bucket := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'bucket', v_filters->>'bulk_process_bucket', v_filters->>'bulkProcessBucket', '')), ''));
  IF v_bucket NOT IN ('UNPROCESSED', 'PROCESSED') THEN
    v_bucket := NULL;
  END IF;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT summary_row.*
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    WHERE (summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL)
      AND UPPER(COALESCE(summary_row.tools_stage, '')) <> 'ARCHIVED'
  ),
  retention_unit_members AS MATERIALIZED (
    SELECT
      lightweight_rows.timesheet_id AS row_timesheet_id,
      lightweight_rows.timesheet_id AS member_timesheet_id
    FROM lightweight_rows
    WHERE lightweight_rows.timesheet_id IS NOT NULL

    UNION

    SELECT
      lightweight_rows.timesheet_id AS row_timesheet_id,
      unit_timesheet.timesheet_id AS member_timesheet_id
    FROM lightweight_rows
    JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = lightweight_rows.timesheet_id
     AND current_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
    JOIN public.timesheets AS unit_timesheet
      ON unit_timesheet.booking_id = current_timesheet.booking_id
    WHERE lightweight_rows.timesheet_id IS NOT NULL
  ),
  retention_by_row AS MATERIALIZED (
    SELECT
      retention_unit_members.row_timesheet_id,
      BOOL_OR(retention.timesheet_id IS NOT NULL) AS has_retained_financial_history
    FROM retention_unit_members
    LEFT JOIN public.timesheet_financial_retention AS retention
      ON retention.timesheet_id = retention_unit_members.member_timesheet_id
    GROUP BY retention_unit_members.row_timesheet_id
  ),
  source_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      current_timesheet.is_adjustment AS current_timesheet_is_adjustment,
      current_timesheet.parent_timesheet_id AS current_timesheet_parent_timesheet_id,
      current_timesheet.adjustment_origin AS current_timesheet_adjustment_origin,
      current_timesheet.correction_id AS current_timesheet_correction_id,
      current_timesheet.correction_kind AS current_timesheet_correction_kind,
      (
        COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
        AND (
          LEFT(UPPER(COALESCE(NULLIF(BTRIM(current_timesheet.adjustment_origin::text), ''), '')), 7) = 'IMPORT_'
          OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_id::text, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_kind::text, '')), '') IS NOT NULL
        )
      ) AS is_import_derived_adjustment_calc,
      (
        (
          COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE
          OR COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
          OR current_timesheet.parent_timesheet_id IS NOT NULL
        )
        AND (
          UPPER(COALESCE(lightweight_rows.route_family, '')) IN ('MANUAL', 'MANUAL_NON_QR', 'QR')
          OR UPPER(COALESCE(lightweight_rows.submission_mode, lightweight_rows.submission_mode_snapshot, '')) = 'MANUAL'
          OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE
          OR UPPER(COALESCE(lightweight_rows.qr_status, '')) IN ('PENDING', 'SIGNED', 'SCANNED', 'USED')
        )
        AND NOT (
          COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
          AND (
            LEFT(UPPER(COALESCE(NULLIF(BTRIM(current_timesheet.adjustment_origin::text), ''), '')), 7) = 'IMPORT_'
            OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_id::text, '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_kind::text, '')), '') IS NOT NULL
          )
        )
      ) AS is_user_created_manual_qr_adjustment_calc,
      COALESCE(retention_by_row.has_retained_financial_history, FALSE) AS has_retained_financial_history_calc
    FROM lightweight_rows
    LEFT JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = lightweight_rows.timesheet_id
    LEFT JOIN retention_by_row
      ON retention_by_row.row_timesheet_id = lightweight_rows.timesheet_id
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      CASE
        WHEN source_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || source_rows.timesheet_id::text
        WHEN source_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || source_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') THEN 'NHSP'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.underlying_channel_family, source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.underlying_channel_family, source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope, source_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN source_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(source_rows.processing_status, source_rows.tools_stage, source_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(source_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(source_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(source_rows.invoice_segments_locked, 0) > 0
      ) AS locked_calc,
      COALESCE(source_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(source_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(source_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(source_rows.qr_status, '')) = 'PENDING'
        AND source_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(source_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(source_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HR'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HR'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(source_rows.timesheet_id::text, ''),
        COALESCE(source_rows.contract_week_id::text, ''),
        COALESCE(source_rows.processing_status, ''),
        COALESCE(source_rows.summary_stage, ''),
        COALESCE(source_rows.tools_stage, ''),
        COALESCE(source_rows.authorised_at_utc::text, ''),
        COALESCE(source_rows.authorised_at_server::text, ''),
        COALESCE(source_rows.processed_at_utc::text, ''),
        COALESCE(source_rows.paid_at_utc::text, ''),
        COALESCE(source_rows.invoice_segments_locked::text, ''),
        COALESCE(source_rows.route_family, ''),
        COALESCE(source_rows.route_subfamily, ''),
        COALESCE(source_rows.validation_status, ''),
        COALESCE(source_rows.attached_evidence_count::text, ''),
        COALESCE(source_rows.primary_artifact_storage_key, ''),
        COALESCE(source_rows.is_import_derived_adjustment_calc::text, ''),
        COALESCE(source_rows.is_user_created_manual_qr_adjustment_calc::text, ''),
        COALESCE(source_rows.current_timesheet_adjustment_origin::text, ''),
        COALESCE(source_rows.current_timesheet_correction_id::text, ''),
        COALESCE(source_rows.current_timesheet_correction_kind::text, ''),
        COALESCE(source_rows.has_retained_financial_history_calc::text, 'false')
      )) AS row_signature_calc
    FROM source_rows
    WHERE (v_week_ending_from IS NULL OR source_rows.week_ending_date >= v_week_ending_from)
      AND (v_week_ending_to IS NULL OR source_rows.week_ending_date <= v_week_ending_to)
  ),
  eligibility_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        (
          classified_rows.route_family_calc = 'MANUAL_NON_QR'
          OR COALESCE(classified_rows.is_user_created_manual_qr_adjustment_calc, FALSE) = TRUE
        )
        AND COALESCE(classified_rows.is_import_derived_adjustment_calc, FALSE) = FALSE
        AND classified_rows.route_family_calc NOT IN ('IMPORT_AUTHORITATIVE', 'ELECTRONIC')
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
      ) AS bulk_process_user_adjustment_eligible_calc
    FROM classified_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      eligibility_rows.*,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_save_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_edit_timesheet_data_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS unprocess_action_visible_calc,
      (
        (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      )
        AND COALESCE(eligibility_rows.has_retained_financial_history_calc, FALSE) = FALSE
      ) AS can_unprocess_calc,
      CASE
        WHEN (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      )
         AND COALESCE(eligibility_rows.has_retained_financial_history_calc, FALSE) = TRUE
        THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'::text
        ELSE NULL::text
      END AS unprocess_block_reason_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.requires_authorisation_calc = TRUE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.qr_unsigned_blocked_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = TRUE
      ) AS can_bulk_unauthorise_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_manage_evidence_calc,
      (
        eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND COALESCE(eligibility_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        eligibility_rows.locked_calc = TRUE
        OR eligibility_rows.authorised_calc = TRUE
        OR eligibility_rows.bulk_process_user_adjustment_eligible_calc = FALSE
      ) AS review_only_calc
    FROM eligibility_rows
  ),
  evidence_target_rows AS MATERIALIZED (
    SELECT DISTINCT
      decision_rows.row_key_calc AS row_key_calc,
      decision_rows.timesheet_id AS timesheet_id,
      decision_rows.contract_week_id AS contract_week_id
    FROM decision_rows
    WHERE decision_rows.row_key_calc IS NOT NULL
      AND decision_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      AND (
        (UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) NOT IN ('WEEKLY', 'DAILY')
      )
      AND (
        (
          decision_rows.bulk_process_bucket_calc = 'UNPROCESSED'
          AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
          AND decision_rows.can_process_calc = TRUE
        )
        OR (
          decision_rows.bulk_process_bucket_calc = 'PROCESSED'
          AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
          AND decision_rows.timesheet_id IS NOT NULL
          AND decision_rows.unprocess_action_visible_calc = TRUE
        )
      )
  ),
  evidence_kind_rows AS MATERIALIZED (
    SELECT
      evidence_target_rows.row_key_calc AS row_key_calc,
      evidence_target_rows.timesheet_id AS timesheet_id,
      evidence_target_rows.contract_week_id AS contract_week_id,
      timesheet_evidence.id AS evidence_id,
      CASE
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('TIMESHEET', 'TS') THEN 'TIMESHEET'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('MILEAGE', 'MILES', 'MILE') THEN 'MILEAGE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) = 'TRAVEL' THEN 'TRAVEL'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('ACCOMMODATION', 'ACCOM') THEN 'ACCOMMODATION'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) = 'OTHER' THEN 'OTHER'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('EXPENSE', 'EXPENSES') THEN 'EXPENSE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IS NOT NULL THEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), ''))
        ELSE 'OTHER'
      END AS evidence_kind_norm,
      NULLIF(BTRIM(COALESCE(timesheet_evidence.display_name, '')), '') AS evidence_display_name,
      NULLIF(BTRIM(COALESCE(timesheet_evidence.storage_key, '')), '') AS evidence_storage_key,
      timesheet_evidence.created_at AS evidence_created_at
    FROM evidence_target_rows AS evidence_target_rows
    JOIN public.timesheet_evidence AS timesheet_evidence
      ON timesheet_evidence.timesheet_id = evidence_target_rows.timesheet_id
    WHERE evidence_target_rows.timesheet_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(timesheet_evidence.storage_key, '')), '') IS NOT NULL

    UNION ALL

    SELECT
      evidence_target_rows.row_key_calc AS row_key_calc,
      NULL::uuid AS timesheet_id,
      evidence_target_rows.contract_week_id AS contract_week_id,
      manual_timesheet_queue.id AS evidence_id,
      CASE
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('TIMESHEET', 'TS') THEN 'TIMESHEET'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('MILEAGE', 'MILES', 'MILE') THEN 'MILEAGE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) = 'TRAVEL' THEN 'TRAVEL'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('ACCOMMODATION', 'ACCOM') THEN 'ACCOMMODATION'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) = 'OTHER' THEN 'OTHER'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('EXPENSE', 'EXPENSES') THEN 'EXPENSE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IS NOT NULL THEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), ''))
        ELSE 'OTHER'
      END AS evidence_kind_norm,
      NULLIF(BTRIM(COALESCE(manual_timesheet_queue.original_filename, '')), '') AS evidence_display_name,
      NULLIF(BTRIM(COALESCE(manual_timesheet_queue.r2_key, '')), '') AS evidence_storage_key,
      manual_timesheet_queue.uploaded_at_utc AS evidence_created_at
    FROM evidence_target_rows AS evidence_target_rows
    JOIN public.manual_timesheet_queue AS manual_timesheet_queue
      ON evidence_target_rows.timesheet_id IS NULL
     AND evidence_target_rows.contract_week_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'contract_week_id', '')), '') = evidence_target_rows.contract_week_id::text
    WHERE UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.status, '')), '')) = 'STAGED'
      AND manual_timesheet_queue.timesheet_id IS NULL
      AND NULLIF(BTRIM(COALESCE(manual_timesheet_queue.r2_key, '')), '') IS NOT NULL
  ),
  evidence_kind_ranked AS MATERIALIZED (
    SELECT
      evidence_kind_rows.*,
      ROW_NUMBER() OVER (
        PARTITION BY evidence_kind_rows.row_key_calc
        ORDER BY
          CASE evidence_kind_rows.evidence_kind_norm
            WHEN 'TIMESHEET' THEN 1
            WHEN 'MILEAGE' THEN 2
            WHEN 'TRAVEL' THEN 3
            WHEN 'ACCOMMODATION' THEN 4
            WHEN 'OTHER' THEN 5
            ELSE 6
          END,
          evidence_kind_rows.evidence_kind_norm ASC,
          evidence_kind_rows.evidence_created_at ASC NULLS LAST,
          evidence_kind_rows.evidence_id ASC
      ) AS evidence_rank
    FROM evidence_kind_rows
  ),
  evidence_kind_counts AS MATERIALIZED (
    SELECT
      evidence_kind_ranked.row_key_calc AS row_key_calc,
      evidence_kind_ranked.evidence_kind_norm AS evidence_kind_norm,
      COUNT(*)::integer AS evidence_kind_count,
      CASE evidence_kind_ranked.evidence_kind_norm
        WHEN 'TIMESHEET' THEN 1
        WHEN 'MILEAGE' THEN 2
        WHEN 'TRAVEL' THEN 3
        WHEN 'ACCOMMODATION' THEN 4
        WHEN 'OTHER' THEN 5
        ELSE 6
      END AS evidence_kind_sort
    FROM evidence_kind_ranked
    GROUP BY evidence_kind_ranked.row_key_calc, evidence_kind_ranked.evidence_kind_norm
  ),
  evidence_kind_extra_badges AS MATERIALIZED (
    SELECT
      evidence_kind_counts.row_key_calc AS row_key_calc,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'kind', evidence_kind_counts.evidence_kind_norm,
          'present', evidence_kind_counts.evidence_kind_count > 0,
          'has_evidence', evidence_kind_counts.evidence_kind_count > 0,
          'count', evidence_kind_counts.evidence_kind_count
        )
        ORDER BY evidence_kind_counts.evidence_kind_sort, evidence_kind_counts.evidence_kind_norm
      ) AS extra_evidence_badges
    FROM evidence_kind_counts
    WHERE evidence_kind_counts.evidence_kind_norm NOT IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
    GROUP BY evidence_kind_counts.row_key_calc
  ),
  evidence_kind_summary AS MATERIALIZED (
    SELECT
      evidence_kind_ranked.row_key_calc AS row_key_calc,
      NULLIF(MAX(evidence_kind_ranked.timesheet_id::text), '')::uuid AS timesheet_id,
      NULLIF(MAX(evidence_kind_ranked.contract_week_id::text), '')::uuid AS contract_week_id,
      COUNT(*)::integer AS total_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'TIMESHEET')::integer AS timesheet_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'MILEAGE')::integer AS mileage_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'TRAVEL')::integer AS travel_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'ACCOMMODATION')::integer AS accommodation_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'OTHER')::integer AS other_evidence_count,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_id::text ELSE NULL::text END) AS primary_artifact_id,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_kind_norm ELSE NULL::text END) AS primary_artifact_kind,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_storage_key ELSE NULL::text END) AS primary_artifact_storage_key,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN COALESCE(evidence_kind_ranked.evidence_display_name, REGEXP_REPLACE(evidence_kind_ranked.evidence_storage_key, '^.*/', '')) ELSE NULL::text END) AS primary_artifact_display_name
    FROM evidence_kind_ranked
    GROUP BY evidence_kind_ranked.row_key_calc
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'bulk_process_user_adjustment_eligible',
        decision_rows.bulk_process_user_adjustment_eligible_calc,
        'is_import_authoritative',
        decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'is_import_derived_adjustment',
        COALESCE(decision_rows.is_import_derived_adjustment_calc, FALSE),
        'is_user_created_manual_qr_adjustment',
        COALESCE(decision_rows.is_user_created_manual_qr_adjustment_calc, FALSE),
        'adjustment_source',
        CASE
          WHEN decision_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_DERIVED'
          WHEN decision_rows.is_user_created_manual_qr_adjustment_calc = TRUE THEN 'USER_CREATED_MANUAL_QR'
          ELSE NULL::text
        END,
        'bulk_process_excluded_reason',
        CASE
          WHEN decision_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE_ADJUSTED_HOURS'
          ELSE NULL::text
        END,
        'current_timesheet_is_adjustment',
        decision_rows.current_timesheet_is_adjustment,
        'parent_timesheet_id',
        decision_rows.current_timesheet_parent_timesheet_id,
        'adjustment_origin',
        decision_rows.current_timesheet_adjustment_origin,
        'correction_id',
        decision_rows.current_timesheet_correction_id,
        'correction_kind',
        decision_rows.current_timesheet_correction_kind,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section',
        CASE WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible' WHEN decision_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible' ELSE NULL::text END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        decision_rows.invoice_issue_stage,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        (COALESCE(decision_rows.current_timesheet_is_adjustment, FALSE) OR COALESCE(decision_rows.is_adjusted, FALSE)),
        'is_adjustment',
        (COALESCE(decision_rows.current_timesheet_is_adjustment, FALSE) OR COALESCE(decision_rows.is_adjusted, FALSE)),
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        COALESCE(evidence_kind_summary.total_evidence_count > 0, decision_rows.has_any_evidence),
        'attached_evidence_count',
        COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
        'evidence_count',
        COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
        'primary_artifact_id',
        evidence_kind_summary.primary_artifact_id,
        'primary_artifact_kind',
        evidence_kind_summary.primary_artifact_kind,
        'primary_artifact_storage_key',
        COALESCE(evidence_kind_summary.primary_artifact_storage_key, decision_rows.primary_artifact_storage_key),
        'primary_artifact_display_name',
        COALESCE(evidence_kind_summary.primary_artifact_display_name, decision_rows.primary_artifact_display_name),
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'invoice_locked' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'has_retained_financial_history',
        COALESCE(decision_rows.has_retained_financial_history_calc, FALSE),
        'unprocess_block_reason',
        decision_rows.unprocess_block_reason_calc,
        'unprocess_action_visible',
        decision_rows.unprocess_action_visible_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        ))
      )
      || JSONB_BUILD_OBJECT(
        'backend_row_signature',
        COALESCE(lifecycle_signature.signature_text, MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        ))),
        'mutation_row_signature',
        COALESCE(lifecycle_signature.signature_text, MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        )))
      )
      || JSONB_BUILD_OBJECT(
        'evidence_badges',
        (
          JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.mileage_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.travel_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.other_evidence_count, 0))
          )
          || COALESCE(evidence_kind_extra_badges.extra_evidence_badges, '[]'::jsonb)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE(evidence_kind_summary.total_evidence_count > 0, decision_rows.has_any_evidence),
          'attached_evidence_count', COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
          'primary_artifact_id', evidence_kind_summary.primary_artifact_id,
          'primary_artifact_kind', evidence_kind_summary.primary_artifact_kind,
          'primary_artifact_storage_key', COALESCE(evidence_kind_summary.primary_artifact_storage_key, decision_rows.primary_artifact_storage_key),
          'primary_artifact_display_name', COALESCE(evidence_kind_summary.primary_artifact_display_name, decision_rows.primary_artifact_display_name),
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode,
          'evidence_badges', (
            JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.mileage_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.travel_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.other_evidence_count, 0))
            )
            || COALESCE(evidence_kind_extra_badges.extra_evidence_badges, '[]'::jsonb)
          )
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'has_retained_financial_history', COALESCE(decision_rows.has_retained_financial_history_calc, FALSE),
          'unprocess_block_reason', decision_rows.unprocess_block_reason_calc,
          'unprocess_action_visible', decision_rows.unprocess_action_visible_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(
          'timesheet_id', decision_rows.timesheet_id,
          'row_key', decision_rows.row_key_calc,
          'has_retained_financial_history', COALESCE(decision_rows.has_retained_financial_history_calc, FALSE),
          'can_unprocess', decision_rows.can_unprocess_calc,
          'unprocess_block_reason', decision_rows.unprocess_block_reason_calc,
          'unprocess_action_visible', decision_rows.unprocess_action_visible_calc
        ),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(decision_rows.timesheet_id, decision_rows.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
    LEFT JOIN evidence_kind_summary AS evidence_kind_summary
      ON evidence_kind_summary.row_key_calc = decision_rows.row_key_calc
    LEFT JOIN evidence_kind_extra_badges AS evidence_kind_extra_badges
      ON evidence_kind_extra_badges.row_key_calc = decision_rows.row_key_calc
  ),
  manual_rows AS MATERIALIZED (
    SELECT payload_rows.row_json
    FROM payload_rows
    WHERE COALESCE((payload_rows.row_json->>'bulk_process_user_adjustment_eligible')::boolean, FALSE) = TRUE
      AND COALESCE((payload_rows.row_json->>'is_import_authoritative')::boolean, FALSE) = FALSE
      AND COALESCE(payload_rows.row_json->>'adjustment_source', '') <> 'IMPORT_DERIVED'
      AND (
        (UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) NOT IN ('WEEKLY', 'DAILY')
      )
  ),
  unprocessed_rows AS MATERIALIZED (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'UNPROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
      AND COALESCE((manual_rows.row_json->>'can_process')::boolean, FALSE) = TRUE
  ),
  processed_rows AS MATERIALIZED (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'PROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
      AND NULLIF(BTRIM(COALESCE(manual_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND COALESCE((manual_rows.row_json->>'unprocess_action_visible')::boolean, FALSE) = TRUE
  ),
  paged_unprocessed_rows AS MATERIALIZED (
    SELECT unprocessed_rows.row_json
    FROM unprocessed_rows
    ORDER BY unprocessed_rows.row_json->>'week_ending_date', unprocessed_rows.row_json->>'client_name', unprocessed_rows.row_json->>'candidate_name', unprocessed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  paged_processed_rows AS MATERIALIZED (
    SELECT processed_rows.row_json
    FROM processed_rows
    ORDER BY processed_rows.row_json->>'week_ending_date', processed_rows.row_json->>'client_name', processed_rows.row_json->>'candidate_name', processed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  counts AS (
    SELECT
      (SELECT COUNT(*)::integer FROM unprocessed_rows) AS unprocessed_count,
      (SELECT COUNT(*)::integer FROM processed_rows) AS processed_count
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'week_ending_from', v_week_ending_from,
      'week_ending_to', v_week_ending_to,
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'unprocessed', counts.unprocessed_count,
      'processed', counts.processed_count,
      'total', counts.unprocessed_count + counts.processed_count
    ),
    'unprocessed_rows', COALESCE((
      SELECT JSONB_AGG(paged_unprocessed_rows.row_json ORDER BY paged_unprocessed_rows.row_json->>'week_ending_date', paged_unprocessed_rows.row_json->>'client_name', paged_unprocessed_rows.row_json->>'candidate_name', paged_unprocessed_rows.row_json->>'row_key')
      FROM paged_unprocessed_rows
    ), '[]'::jsonb),
    'processed_rows', COALESCE((
      SELECT JSONB_AGG(paged_processed_rows.row_json ORDER BY paged_processed_rows.row_json->>'week_ending_date', paged_processed_rows.row_json->>'client_name', paged_processed_rows.row_json->>'candidate_name', paged_processed_rows.row_json->>'row_key')
      FROM paged_processed_rows
    ), '[]'::jsonb)
  )
  INTO v_out
  FROM counts;

  RETURN private._candidate_dataset_overlay_v1(COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'week_ending_from', v_week_ending_from,
      'week_ending_to', v_week_ending_to,
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT('unprocessed', 0, 'processed', 0, 'total', 0),
    'unprocessed_rows', '[]'::jsonb,
    'processed_rows', '[]'::jsonb
  )));
END;
$function$;

ALTER FUNCTION public.bulk_process_dataset_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bulk_process_dataset_v1(jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bulk_process_dataset_v1(jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_patch_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_dataset_mode text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  v_projection text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'projection', v_filters->>'profile', 'status_patch')), ''));

  v_row_keys text[] := NULL;
  v_previous_row_key text := NULL;
  v_actor_user_id uuid := NULL;

  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;
  v_has_row_key_filter boolean := FALSE;

  v_changed_domains text[] := ARRAY[]::text[];
  v_status_only_hint boolean := FALSE;
  v_manual_changed_hint boolean := FALSE;
  v_evidence_changed_hint boolean := FALSE;
  v_storage_changed_hint boolean := FALSE;
  v_identity_changed_hint boolean := FALSE;
BEGIN
  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'mode', '')), ''));
  END IF;

  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := NULL;
  END IF;

  IF v_projection NOT IN ('status_patch', 'dataset_row', 'summary_row', 'active_row_header') THEN
    v_projection := 'status_patch';
  END IF;

  v_previous_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'previous_row_key', v_filters->>'previousRowKey', '')), '');

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId', '')), '') IS NOT NULL
       AND COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId') ~* v_uuid_re THEN
      v_actor_user_id := COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId')::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_actor_user_id := NULL;
  END;

  v_has_row_key_filter :=
    (v_filters ? 'row_key')
    OR (v_filters ? 'rowKey')
    OR (v_filters ? 'row_keys')
    OR (v_filters ? 'rowKeys');

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey')), '')];
  END IF;

  IF v_has_row_key_filter AND COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('row_keys', to_jsonb(v_row_keys));
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids));
  END IF;

  IF v_filters ? 'changed_domains' AND jsonb_typeof(v_filters->'changed_domains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changed_domains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF v_filters ? 'changedDomains' AND jsonb_typeof(v_filters->'changedDomains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changedDomains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains', '')), '') IS NOT NULL THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(split_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM unnest(regexp_split_to_array(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains'), '\s*,\s*')) AS split_values(value)
    WHERE NULLIF(BTRIM(split_values.value), '') IS NOT NULL;
  END IF;

  v_identity_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['identity','adoption','row_identity','contract_week_to_timesheet','contract-week-to-timesheet','created_timesheet']::text[], FALSE);

  v_evidence_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['evidence','attached_evidence','queue_evidence','document','documents']::text[], FALSE);

  v_storage_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['storage','storage_key','storage-key','preview','artifact','primary_artifact']::text[], FALSE);

  v_manual_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['manual','schedule','editor','save','process','unprocess','financials','tsfin','additional_units','expenses']::text[], FALSE);

  -- Process-mode mutation callers may request a lightweight status_patch without
  -- explicit changed_domains. Treat those as manual/editor-affecting so the
  -- frontend fetches one fresh active_row_visible context instead of preserving
  -- stale schedule/expense/editor state as if this were a pure status patch.
  IF NOT v_manual_changed_hint
     AND COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
     AND v_dataset_mode = 'process'
     AND v_projection = 'status_patch' THEN
    v_manual_changed_hint := TRUE;
  END IF;

  v_status_only_hint :=
    (
      COALESCE(v_changed_domains && ARRAY['status','authorise','authorize','unauthorise','unauthorize','processing_status','authorisation','authorization']::text[], FALSE)
      OR (
        COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
        AND v_dataset_mode = 'authorise'
        AND v_projection = 'status_patch'
      )
    )
    AND NOT v_identity_changed_hint
    AND NOT v_evidence_changed_hint
    AND NOT v_storage_changed_hint
    AND NOT v_manual_changed_hint;

  IF EXISTS(
    SELECT 1
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) source_row
    JOIN public.timesheets t ON t.timesheet_id=source_row.timesheet_id
    LEFT JOIN public.timesheets_financials tf ON tf.timesheet_id=t.timesheet_id AND tf.is_current
    WHERE t.is_current AND t.archived_at_utc IS NULL AND coalesce(t.is_adjustment,false)
      AND t.parent_timesheet_id IS NOT NULL AND t.correction_id IS NOT NULL
      AND upper(coalesce(t.adjustment_origin,''))='IMPORT_CORRECTION'
      AND t.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      AND ((upper(coalesce(source_row.route_type,'')) like '%NHSP%' or upper(coalesce(tf.basis::text,'')) like 'NHSP%'
          or upper(coalesce(t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
       = (upper(coalesce(source_row.route_type,'')) like '%HEALTHROSTER%' or upper(coalesce(tf.basis::text,'')) like 'HEALTHROSTER%'
          or upper(coalesce(t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER'))
  ) THEN
    RAISE EXCEPTION 'BULK_AUTHORISE_CORRECTION_SOURCE_CONFLICT' USING ERRCODE='22023';
  END IF;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  retention_unit_members AS MATERIALIZED (
    SELECT
      source_rows.timesheet_id AS row_timesheet_id,
      source_rows.timesheet_id AS member_timesheet_id
    FROM source_rows
    WHERE source_rows.timesheet_id IS NOT NULL

    UNION

    SELECT
      source_rows.timesheet_id AS row_timesheet_id,
      unit_timesheet.timesheet_id AS member_timesheet_id
    FROM source_rows
    JOIN public.timesheets AS anchor_timesheet
      ON anchor_timesheet.timesheet_id = source_rows.timesheet_id
     AND anchor_timesheet.is_current = TRUE
     AND anchor_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
    JOIN public.timesheets AS unit_timesheet
      ON unit_timesheet.booking_id = anchor_timesheet.booking_id
  ),
  retention_by_row AS MATERIALIZED (
    SELECT
      retention_unit_members.row_timesheet_id,
      COALESCE(BOOL_OR(marker.timesheet_id IS NOT NULL), FALSE) AS has_retained_financial_history
    FROM retention_unit_members
    LEFT JOIN public.timesheet_financial_retention AS marker
      ON marker.timesheet_id = retention_unit_members.member_timesheet_id
    GROUP BY retention_unit_members.row_timesheet_id
  ),
  enriched_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      timesheet_row.version AS timesheet_version,
      timesheet_row.updated_at AS timesheet_updated_at,
      timesheet_row.is_current AS timesheet_is_current,
      timesheet_row.archived_at_utc AS timesheet_archived_at_utc,
      timesheet_row.manual_pdf_r2_key,
      timesheet_row.qr_r2_key,
      timesheet_row.generated_pdf_at_utc,
      timesheet_row.manual_pdf_rotation_degrees,
      timesheet_row.qr_token AS timesheet_qr_token,
      timesheet_row.qr_generated_at AS timesheet_qr_generated_at,
      timesheet_row.qr_scanned_at AS timesheet_qr_scanned_at,
      COALESCE(retention_unit.has_retained_financial_history, FALSE) AS has_retained_financial_history,
      contract_week_row.updated_at AS contract_week_updated_at,
      contract_week_row.uploaded_pdf_r2_key,
      contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
      contract_week_row.totals_json AS contract_week_totals_json,
      contract_week_row.is_adjustment AS contract_week_is_adjustment,
      contract_week_row.additional_seq AS contract_week_additional_seq,
      timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
      timesheet_row.is_adjustment AS timesheet_is_adjustment,
      timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
      timesheet_row.correction_id AS timesheet_correction_id,
      timesheet_row.correction_kind AS timesheet_correction_kind,
      timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
      timesheet_row.candidate_hint_text AS timesheet_candidate_hint_text,
      financial_row.basis::text AS tsfin_basis_text,
      financial_row.updated_at AS tsfin_updated_at,
      financial_row.actual_schedule_json AS tsfin_actual_schedule_json,
      financial_row.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
      financial_row.total_hours AS tsfin_total_hours,
      financial_row.expenses_pay_ex_vat AS tsfin_expenses_pay_ex_vat,
      financial_row.expenses_charge_ex_vat AS tsfin_expenses_charge_ex_vat,
      financial_row.mileage_units AS tsfin_mileage_units,
      financial_row.mileage_pay_ex_vat AS tsfin_mileage_pay_ex_vat,
      financial_row.mileage_charge_ex_vat AS tsfin_mileage_charge_ex_vat,
      financial_row.travel_pay_ex_vat AS tsfin_travel_pay_ex_vat,
      financial_row.travel_charge_ex_vat AS tsfin_travel_charge_ex_vat,
      financial_row.accommodation_pay_ex_vat AS tsfin_accommodation_pay_ex_vat,
      financial_row.accommodation_charge_ex_vat AS tsfin_accommodation_charge_ex_vat,
      financial_row.other_pay_ex_vat AS tsfin_other_pay_ex_vat,
      financial_row.other_charge_ex_vat AS tsfin_other_charge_ex_vat,
      financial_row.authorised_at_utc AS tsfin_authorised_at_utc,
      financial_row.processed_at_utc AS tsfin_processed_at_utc,
      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS evidence_attached_count,
      evidence_summary.primary_storage_key AS evidence_primary_storage_key,
      evidence_summary.primary_display_name AS evidence_primary_display_name,
      CASE
        WHEN evidence_summary.evidence_updated_at IS NOT NULL
         AND staged_evidence_summary.staged_evidence_updated_at IS NOT NULL
          THEN GREATEST(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
        ELSE COALESCE(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
      END AS evidence_updated_at,
      COALESCE(evidence_summary.has_attached_timesheet_evidence, FALSE) AS evidence_has_attached_timesheet,
      COALESCE(evidence_summary.has_attached_mileage_evidence, FALSE) AS evidence_has_attached_mileage,
      COALESCE(evidence_summary.has_attached_travel_evidence, FALSE) AS evidence_has_attached_travel,
      COALESCE(evidence_summary.has_attached_accommodation_evidence, FALSE) AS evidence_has_attached_accommodation,
      COALESCE(evidence_summary.has_attached_other_evidence, FALSE) AS evidence_has_attached_other,
      COALESCE(staged_evidence_summary.staged_evidence_count, 0)::integer AS evidence_staged_count,
      staged_evidence_summary.primary_staged_storage_key AS evidence_primary_staged_storage_key,
      staged_evidence_summary.primary_staged_display_name AS evidence_primary_staged_display_name,
      staged_evidence_summary.primary_staged_kind AS evidence_primary_staged_kind,
      COALESCE(staged_evidence_summary.has_staged_timesheet_evidence, FALSE) AS evidence_has_staged_timesheet,
      COALESCE(staged_evidence_summary.has_staged_mileage_evidence, FALSE) AS evidence_has_staged_mileage,
      COALESCE(staged_evidence_summary.has_staged_travel_evidence, FALSE) AS evidence_has_staged_travel,
      COALESCE(staged_evidence_summary.has_staged_accommodation_evidence, FALSE) AS evidence_has_staged_accommodation,
      COALESCE(staged_evidence_summary.has_staged_other_evidence, FALSE) AS evidence_has_staged_other,
      UPPER(COALESCE(source_rows.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(source_rows.submission_mode::text, '')) AS submission_mode_upper,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope::text, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(source_rows.sheet_scope::text, ''))
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type,
      (
        COALESCE(source_rows.is_qr, FALSE)
        OR source_rows.qr_status IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(timesheet_row.qr_token, '')), '') IS NOT NULL
        OR timesheet_row.qr_generated_at IS NOT NULL
      ) AS is_qr_route,
      (
        UPPER(COALESCE(source_rows.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'MANUAL'
        AND (
          COALESCE(timesheet_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.additional_seq, source_rows.additional_seq, 0) > 0
          OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
          OR timesheet_row.parent_timesheet_id IS NOT NULL
        )
        AND NOT (
          UPPER(COALESCE(timesheet_row.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
          OR NULLIF(BTRIM(COALESCE(timesheet_row.correction_kind, '')), '') IS NOT NULL
          OR timesheet_row.correction_id IS NOT NULL
        )
      ) AS is_manual_additional_adjustment_calc
    FROM source_rows
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN retention_by_row AS retention_unit
      ON retention_unit.row_timesheet_id = source_rows.timesheet_id
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id)::integer AS attached_evidence_count,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET'), FALSE) AS has_attached_timesheet_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'MILEAGE'), FALSE) AS has_attached_mileage_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TRAVEL'), FALSE) AS has_attached_travel_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_attached_accommodation_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'OTHER'), FALSE) AS has_attached_other_evidence,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_display_name,
        MAX(timesheet_evidence_row.created_at) AS evidence_updated_at
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(staged_queue_row.id)::integer AS staged_evidence_count,
        MAX(staged_queue_row.uploaded_at_utc) AS staged_evidence_updated_at,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence,
        (ARRAY_AGG(
          staged_queue_row.r2_key
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_storage_key,
        (ARRAY_AGG(
          staged_queue_row.display_name
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_display_name,
        (ARRAY_AGG(
          staged_queue_row.staged_kind_upper
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_kind
      FROM (
        SELECT
          manual_queue_row.id,
          manual_queue_row.r2_key,
          manual_queue_row.uploaded_at_utc,
          COALESCE(
            NULLIF(BTRIM(COALESCE(manual_queue_row.original_filename, '')), ''),
            NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'display_name', manual_queue_row.meta_json->>'displayName', manual_queue_row.meta_json->>'filename', manual_queue_row.meta_json->>'original_filename', manual_queue_row.meta_json->>'originalFilename', '')), '')
          ) AS display_name,
          CASE
            WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
              manual_queue_row.meta_json->>'staged_kind',
              manual_queue_row.meta_json->>'stagedKind',
              manual_queue_row.meta_json->>'evidence_kind',
              manual_queue_row.meta_json->>'evidenceKind',
              manual_queue_row.meta_json->>'kind',
              manual_queue_row.meta_json->>'type',
              manual_queue_row.meta_json->>'evidence_type',
              manual_queue_row.meta_json->>'evidenceType',
              ''
            )), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
              THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
                manual_queue_row.meta_json->>'staged_kind',
                manual_queue_row.meta_json->>'stagedKind',
                manual_queue_row.meta_json->>'evidence_kind',
                manual_queue_row.meta_json->>'evidenceKind',
                manual_queue_row.meta_json->>'kind',
                manual_queue_row.meta_json->>'type',
                manual_queue_row.meta_json->>'evidence_type',
                manual_queue_row.meta_json->>'evidenceType',
                ''
              )), ''), 'TIMESHEET'))
            ELSE 'OTHER'
          END AS staged_kind_upper
        FROM public.manual_timesheet_queue AS manual_queue_row
        WHERE source_rows.timesheet_id IS NULL
          AND source_rows.contract_week_id IS NOT NULL
          AND UPPER(COALESCE(manual_queue_row.status, '')) = 'STAGED'
          AND NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'contract_week_id', '')), '') = source_rows.contract_week_id::text
      ) AS staged_queue_row
    ) AS staged_evidence_summary ON TRUE
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      enriched_rows.*,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
        ) THEN 'NHSP'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
         AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN NULL::text
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      (
        enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required_calc,
      (
        enriched_rows.timesheet_id IS NULL
        AND enriched_rows.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.contract_week_status::text, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed_calc,
      (
        enriched_rows.timesheet_id IS NOT NULL
        AND UPPER(COALESCE(enriched_rows.tools_stage, '')) <> 'ARCHIVED'
        AND (
          UPPER(COALESCE(enriched_rows.processing_status::text, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed_calc,
      (
        UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'ARCHIVED'
        OR COALESCE(enriched_rows.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(enriched_rows.invoice_segments_locked, 0) > 0
        OR COALESCE(enriched_rows.invoice_is_paid, FALSE) = TRUE
      ) AS locked_calc,
      (
        enriched_rows.authorised_at_server IS NOT NULL
        OR enriched_rows.tsfin_authorised_at_utc IS NOT NULL
      ) AS is_authorised_calc,
      (
        UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(enriched_rows.client_requires_hr, FALSE) = TRUE
          AND COALESCE(enriched_rows.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'PENDING'
        AND (
          NULLIF(BTRIM(COALESCE(enriched_rows.timesheet_qr_token, '')), '') IS NOT NULL
          OR enriched_rows.timesheet_qr_generated_at IS NOT NULL
        )
        AND enriched_rows.timesheet_qr_scanned_at IS NULL
      ) AS qr_pending_awaiting_signature_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'USED'
        AND enriched_rows.timesheet_qr_scanned_at IS NOT NULL
      ) AS qr_signed_returned_calc,
      COALESCE(
        enriched_rows.evidence_primary_storage_key,
        enriched_rows.evidence_primary_staged_storage_key,
        NULLIF(enriched_rows.manual_pdf_r2_key, ''),
        NULLIF(enriched_rows.qr_r2_key, ''),
        NULLIF(enriched_rows.uploaded_pdf_r2_key, '')
      ) AS primary_artifact_storage_key_calc,
      COALESCE(
        enriched_rows.evidence_primary_display_name,
        enriched_rows.evidence_primary_staged_display_name,
        CASE WHEN NULLIF(enriched_rows.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(enriched_rows.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN NULLIF(enriched_rows.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name_calc
    FROM enriched_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      CASE
        WHEN classified_rows.timesheet_is_current=true
         AND classified_rows.timesheet_archived_at_utc is null
         AND coalesce(classified_rows.timesheet_is_adjustment,false)
         AND classified_rows.timesheet_parent_timesheet_id is not null
         AND classified_rows.timesheet_correction_id is not null
         AND upper(coalesce(classified_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
         AND classified_rows.timesheet_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
         THEN case
          when (classified_rows.route_type_upper like '%HEALTHROSTER%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'HEALTHROSTER%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER')
            and not (classified_rows.route_type_upper like '%NHSP%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'NHSP%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP') then 'HR'
          when (classified_rows.route_type_upper like '%NHSP%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'NHSP%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
            and not (classified_rows.route_type_upper like '%HEALTHROSTER%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'HEALTHROSTER%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER') then 'NHSP'
          else 'TIMESHEETS' end
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
         AND classified_rows.route_subfamily_calc = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) AS is_unprocessed_calc,
      CASE
        WHEN (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(classified_rows.client_hr_validation_required, FALSE) = TRUE
        AND UPPER(COALESCE(classified_rows.validation_status::text, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_awaiting_calc,
      (
        classified_rows.qr_pending_awaiting_signature_calc = TRUE
        OR UPPER(COALESCE(classified_rows.processing_status::text, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(COALESCE(classified_rows.issue_codes, ARRAY[]::text[]))
      ) AS qr_unsigned_blocked_calc,
      CASE
        WHEN classified_rows.primary_artifact_storage_key_calc IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode_calc
    FROM classified_rows
  ),
  final_rows AS MATERIALIZED (
    SELECT
      decision_rows.*,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || decision_rows.contract_week_id::text
        ELSE ''::text
      END AS row_key_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN decision_rows.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.requires_authorisation_calc = TRUE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.qr_unsigned_blocked_calc = FALSE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_authorise_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = TRUE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_unauthorise_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
      ) AS unprocess_action_visible_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
        AND COALESCE(decision_rows.has_retained_financial_history, FALSE) = FALSE
      ) AS can_unprocess_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL
         AND decision_rows.locked_calc = FALSE
         AND decision_rows.is_authorised_calc = FALSE
         AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
         AND decision_rows.is_unprocessed_calc = FALSE
         AND COALESCE(decision_rows.has_retained_financial_history, FALSE) = TRUE
        THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'::text
        ELSE NULL::text
      END AS unprocess_block_reason,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = TRUE
      ) AS can_process_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR (decision_rows.contract_week_id IS NOT NULL AND decision_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND UPPER(COALESCE(decision_rows.tools_stage, '')) <> 'ARCHIVED'
        AND (decision_rows.locked_by_invoice_id IS NOT NULL OR COALESCE(decision_rows.invoice_segments_locked, 0) > 0) = FALSE
        AND decision_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        decision_rows.locked_calc = TRUE
        OR decision_rows.is_authorised_calc = TRUE
        OR decision_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc,
      (
        decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND COALESCE(decision_rows.is_adjusted, FALSE) = FALSE
        AND (
          (decision_rows.period_type = 'WEEKLY' AND decision_rows.contract_week_id IS NOT NULL)
          OR (decision_rows.period_type = 'DAILY' AND decision_rows.timesheet_id IS NOT NULL)
        )
        AND (
          decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
          OR (decision_rows.route_family_calc = 'MANUAL_NON_QR' AND decision_rows.submission_mode_upper = 'MANUAL' AND decision_rows.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual_calc
    FROM decision_rows
  ),
  signed_rows_base AS MATERIALIZED (
    SELECT
      final_rows.*,
      md5(concat_ws('|',
        COALESCE(final_rows.timesheet_id::text, ''),
        COALESCE(final_rows.contract_week_id::text, ''),
        COALESCE(final_rows.timesheet_version::text, final_rows.timesheet_version::text, ''),
        COALESCE(final_rows.timesheet_updated_at::text, ''),
        COALESCE(final_rows.contract_week_updated_at::text, ''),
        COALESCE(final_rows.tsfin_updated_at::text, ''),
        COALESCE(final_rows.evidence_updated_at::text, ''),
        COALESCE(final_rows.evidence_staged_count::text, ''),
        COALESCE(final_rows.evidence_has_staged_timesheet::text, ''),
        COALESCE(final_rows.evidence_has_staged_mileage::text, ''),
        COALESCE(final_rows.evidence_has_staged_travel::text, ''),
        COALESCE(final_rows.evidence_has_staged_accommodation::text, ''),
        COALESCE(final_rows.evidence_has_staged_other::text, ''),
        COALESCE(final_rows.processing_status::text, ''),
        COALESCE(final_rows.summary_stage, ''),
        COALESCE(final_rows.tools_stage, ''),
        COALESCE(final_rows.authorised_at_server::text, ''),
        COALESCE(final_rows.tsfin_authorised_at_utc::text, ''),
        COALESCE(final_rows.is_authorised_calc::text, ''),
        COALESCE(final_rows.locked_calc::text, ''),
        COALESCE(final_rows.has_retained_financial_history::text, ''),
        COALESCE(final_rows.unprocess_action_visible_calc::text, ''),
        COALESCE(final_rows.can_unprocess_calc::text, ''),
        COALESCE(final_rows.unprocess_block_reason, ''),
        COALESCE(final_rows.route_family_calc, ''),
        COALESCE(final_rows.route_subfamily_calc, ''),
        COALESCE(final_rows.bulk_process_bucket_calc, ''),
        COALESCE(final_rows.bulk_authorise_classification_calc, ''),
        COALESCE(final_rows.primary_artifact_storage_key_calc, ''),
        COALESCE(final_rows.total_hours::text, ''),
        COALESCE(final_rows.total_pay_ex_vat::text, ''),
        COALESCE(final_rows.total_charge_ex_vat::text, ''),
        COALESCE(final_rows.margin_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_units::text, ''),
        COALESCE(final_rows.tsfin_mileage_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_charge_ex_vat::text, ''),
        COALESCE(final_rows.issue_codes::text, '')
      )) AS row_signature_calc
    FROM final_rows
  ),
  signed_rows AS MATERIALIZED (
    SELECT
      signed_rows_base.*,
      COALESCE(lifecycle_signature.signature_text, signed_rows_base.row_signature_calc) AS backend_row_signature_calc,
      COALESCE(lifecycle_signature.signature_text, signed_rows_base.row_signature_calc) AS mutation_row_signature_calc
    FROM signed_rows_base
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(signed_rows_base.timesheet_id, signed_rows_base.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      signed_rows.*,
      CASE
        WHEN signed_rows.can_bulk_authorise_calc THEN 'processed_eligible'
        WHEN signed_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible'
        ELSE NULL::text
      END AS bulk_authorise_section_calc,
      (
        v_identity_changed_hint
        OR (
          v_previous_row_key IS NOT NULL
          AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
        )
      ) AS identity_changed_calc,
      (
        v_status_only_hint
        AND NOT (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        )
      ) AS status_only_calc,
      jsonb_build_object(
        'status_only', (
          v_status_only_hint
          AND NOT (
            v_identity_changed_hint
            OR (
              v_previous_row_key IS NOT NULL
              AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            )
          )
        ),
        'manual_changed', v_manual_changed_hint,
        'evidence_changed', v_evidence_changed_hint,
        'storage_changed', v_storage_changed_hint,
        'identity_changed', (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        ),
        'invalidate_context', false,
        'invalidate_row_context', false,
        'invalidate_preview', CASE WHEN v_storage_changed_hint OR v_evidence_changed_hint THEN true ELSE false END,
        'invalidate_evidence', CASE WHEN v_evidence_changed_hint OR v_storage_changed_hint THEN true ELSE false END,
        'invalidate_editor_context', CASE WHEN v_manual_changed_hint THEN true ELSE false END,
        'row_keys', CASE
          WHEN v_previous_row_key IS NOT NULL
           AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            THEN jsonb_build_array(v_previous_row_key, signed_rows.row_key_calc)
          ELSE jsonb_build_array(signed_rows.row_key_calc)
        END,
        'timesheet_ids', CASE WHEN signed_rows.timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.timesheet_id) END,
        'contract_week_ids', CASE WHEN signed_rows.contract_week_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.contract_week_id) END,
        'storage_keys', CASE WHEN signed_rows.primary_artifact_storage_key_calc IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.primary_artifact_storage_key_calc) END,
        'row_signature', signed_rows.row_signature_calc,
        'backend_row_signature', signed_rows.backend_row_signature_calc,
        'mutation_row_signature', signed_rows.mutation_row_signature_calc,
        'datasets', CASE
          WHEN v_dataset_mode = 'authorise' THEN jsonb_build_array('bulk_authorise')
          WHEN v_dataset_mode = 'process' THEN jsonb_build_array('bulk_process')
          ELSE jsonb_build_array('bulk_process', 'bulk_authorise')
        END
      ) AS cache_hints_json,
      jsonb_build_object(
        'processed_eligible', 0,
        'authorised_eligible', 0,
        'unprocessed', 0,
        'processed', 0,
        'total', 0
      ) AS count_deltas_json,
      (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) AS keep_additional_manual_adjustment_schedule_empty_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json, '[]'::jsonb)
      END AS actual_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.contract_week_planned_schedule_json, '[]'::jsonb)
      END AS planned_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN 0::numeric
        ELSE COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric)
      END AS total_hours_calc,
      CASE
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
           OR COALESCE(signed_rows.client_is_nhsp, FALSE) = TRUE
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
           OR COALESCE(signed_rows.client_autoprocess_hr, FALSE) = TRUE
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE signed_rows.route_type
      END AS effective_route_type_calc,
      (
        COALESCE(signed_rows.timesheet_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_additional_seq, signed_rows.additional_seq, 0) > 0
        OR COALESCE(signed_rows.is_adjustment, FALSE) = TRUE
        OR signed_rows.timesheet_parent_timesheet_id IS NOT NULL
        OR signed_rows.timesheet_correction_id IS NOT NULL
        OR signed_rows.timesheet_correction_kind IS NOT NULL
      ) AS effective_is_adjustment_calc
    FROM signed_rows
  )
  SELECT
    private._candidate_office_context_overlay_v1((
      jsonb_build_object(
        'id', COALESCE(payload_rows.timesheet_id::text, payload_rows.contract_week_id::text),
        'row_key', payload_rows.row_key_calc,
        'previous_row_key', v_previous_row_key,
        'new_row_key', payload_rows.row_key_calc,
        'stable_row_id', payload_rows.stable_row_id_calc,
        'timesheet_id', payload_rows.timesheet_id,
        'current_timesheet_id', payload_rows.timesheet_id,
        'requested_timesheet_id', payload_rows.timesheet_id,
        'expected_timesheet_id', payload_rows.timesheet_id,
        'contract_week_id', payload_rows.contract_week_id,
        'contract_id', COALESCE((SELECT contract_lookup.contract_id FROM public.timesheets AS contract_lookup WHERE contract_lookup.timesheet_id = payload_rows.timesheet_id AND contract_lookup.is_current = TRUE LIMIT 1), (SELECT contract_week_lookup.contract_id FROM public.contract_weeks AS contract_week_lookup WHERE contract_week_lookup.id = payload_rows.contract_week_id LIMIT 1)),
        'row_signature', payload_rows.row_signature_calc,
        'backend_row_signature', payload_rows.backend_row_signature_calc,
        'mutation_row_signature', payload_rows.mutation_row_signature_calc,
        'render_signature', payload_rows.row_signature_calc,
        'previous_row_signature', NULL::text,
        'timesheet_version', payload_rows.timesheet_version,
        'current_version', payload_rows.timesheet_version,
        'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
        'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'actor_user_id', v_actor_user_id,
        'projection', v_projection,
        'dataset_mode', v_dataset_mode
      )
      || jsonb_build_object(
        'candidate_id', payload_rows.candidate_id,
        'candidate_name', payload_rows.candidate_name,
        'candidate_display_name', payload_rows.candidate_name,
        'client_id', payload_rows.client_id,
        'client_name', payload_rows.client_name,
        'client_display_name', payload_rows.client_name,
        'booking_id', payload_rows.booking_id,
        'booking_ref', payload_rows.booking_id,
        'occupant_key_norm', payload_rows.occupant_key_norm,
        'hospital_name', payload_rows.hospital_norm,
        'hospital_norm', payload_rows.hospital_norm,
        'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
        'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
        'period_type', payload_rows.period_type,
        'sheet_scope', payload_rows.sheet_scope,
        'timesheet_scope', payload_rows.sheet_scope,
        'submission_mode', payload_rows.submission_mode,
        'submission_mode_snapshot', payload_rows.submission_mode,
        'basis', payload_rows.basis,
        'route_type', payload_rows.effective_route_type_calc,
        'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
        'route_family', payload_rows.route_family_calc,
        'route_subfamily', payload_rows.route_subfamily_calc,
        'underlying_channel_family', payload_rows.underlying_channel_family_calc,
        'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', payload_rows.compare_block_required_calc,
        'is_adjustment', payload_rows.effective_is_adjustment_calc,
        'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
        'actual_schedule_json', payload_rows.actual_schedule_json_calc,
        'planned_schedule_json', payload_rows.planned_schedule_json_calc,
        'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc
      )
      || jsonb_build_object(
        'processing_status', payload_rows.processing_status::text,
        'processing_status_display', payload_rows.processing_status_display,
        'summary_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.summary_stage
        END,
        'tools_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.tools_stage
        END,
        'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
        'correction_id',payload_rows.timesheet_correction_id,
        'correction_kind',payload_rows.timesheet_correction_kind,
        'adjustment_origin',payload_rows.timesheet_adjustment_origin,
        'correction_source_system',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
          and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
          and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
          and payload_rows.timesheet_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          then case when payload_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
        'correction_display_label',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
          and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
          and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
          then case
            when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
            when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
            when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
            when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
        'is_authorised', payload_rows.is_authorised_calc,
        'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
        'authorised_at_server', payload_rows.authorised_at_server,
        'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
        'requires_authorisation', payload_rows.requires_authorisation_calc,
        'locked', payload_rows.locked_calc,
        'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
        'paid_at_utc', payload_rows.paid_at_utc,
        'review_only', payload_rows.review_only_calc,
        'can_save', payload_rows.can_save_calc,
        'can_process', payload_rows.can_process_calc,
        'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
        'can_unprocess', payload_rows.can_unprocess_calc,
        'unprocess_block_reason', payload_rows.unprocess_block_reason,
        'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
        'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
        'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence', payload_rows.can_manage_evidence_calc,
        'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
      )
      || jsonb_build_object(
        'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
        'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
        'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
        'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
        'expenses_pay_ex_vat', payload_rows.tsfin_expenses_pay_ex_vat,
        'expenses_charge_ex_vat', payload_rows.tsfin_expenses_charge_ex_vat,
        'mileage_units', payload_rows.tsfin_mileage_units,
        'mileage_pay_ex_vat', payload_rows.tsfin_mileage_pay_ex_vat,
        'mileage_charge_ex_vat', payload_rows.tsfin_mileage_charge_ex_vat,
        'travel_pay_ex_vat', payload_rows.tsfin_travel_pay_ex_vat,
        'travel_charge_ex_vat', payload_rows.tsfin_travel_charge_ex_vat,
        'accommodation_pay_ex_vat', payload_rows.tsfin_accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', payload_rows.tsfin_accommodation_charge_ex_vat,
        'other_pay_ex_vat', payload_rows.tsfin_other_pay_ex_vat,
        'other_charge_ex_vat', payload_rows.tsfin_other_charge_ex_vat,
        'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
        'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
        'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
        'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
        'invoice_segment_stage', payload_rows.invoice_segment_stage,
        'pay_icon_code', payload_rows.pay_icon_code,
        'pay_status_code', payload_rows.pay_status_code,
        'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
        'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
        'validation_status', payload_rows.validation_status::text,
        'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
        'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
        'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
        'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
        'qr_signed_returned', payload_rows.qr_signed_returned_calc
      )
      || jsonb_build_object(
        'is_qr', payload_rows.is_qr_route,
        'qr_status', payload_rows.qr_status::text,
        'qr_generated_at', payload_rows.timesheet_qr_generated_at,
        'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
        'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
        'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
        'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
        'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE),
        'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
        'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
        'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
        'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
        'has_any_evidence', (
          COALESCE(payload_rows.evidence_attached_count, 0) > 0
          OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
          OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
        ),
        'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
        'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
        'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
        'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
        'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
        'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
        'evidence_badges', jsonb_build_array(
          jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
          jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
          jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
          jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
          jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
        )
      )
      || jsonb_build_object(
        'count_deltas', payload_rows.count_deltas_json,
        'cache_invalidation_hints', payload_rows.cache_hints_json,
        'action_flags', jsonb_build_object(
          'can_save', payload_rows.can_save_calc,
          'can_process', payload_rows.can_process_calc,
          'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
          'can_unprocess', payload_rows.can_unprocess_calc,
          'unprocess_block_reason', payload_rows.unprocess_block_reason,
          'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
          'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
          'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', payload_rows.can_manage_evidence_calc,
          'can_add_additional_manual', payload_rows.can_add_additional_manual_calc,
          'review_only', payload_rows.review_only_calc,
          'is_adjustment', payload_rows.effective_is_adjustment_calc,
          'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
          'supportsUnprocessedExpenseDraft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'supports_unprocessed_expense_draft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'expense_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_DRAFT'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END
        ),
        'artifact_hints', jsonb_build_object(
          'route_family', payload_rows.route_family_calc,
          'route_subfamily', payload_rows.route_subfamily_calc,
          'underlying_channel_family', payload_rows.underlying_channel_family_calc,
          'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
          'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
          'has_any_evidence', (COALESCE(payload_rows.evidence_attached_count, 0) > 0 OR COALESCE(payload_rows.evidence_staged_count, 0) > 0 OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL),
          'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
          'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0)
        ),
        'row_patch', (
          jsonb_build_object(
            'previous_row_key', v_previous_row_key,
            'row_key', payload_rows.row_key_calc,
            'new_row_key', payload_rows.row_key_calc,
            'stable_row_id', payload_rows.stable_row_id_calc,
            'timesheet_id', payload_rows.timesheet_id,
            'current_timesheet_id', payload_rows.timesheet_id,
            'requested_timesheet_id', payload_rows.timesheet_id,
            'expected_timesheet_id', payload_rows.timesheet_id,
            'contract_week_id', payload_rows.contract_week_id,
            'row_signature', payload_rows.row_signature_calc,
            'backend_row_signature', payload_rows.backend_row_signature_calc,
            'mutation_row_signature', payload_rows.mutation_row_signature_calc,
            'render_signature', payload_rows.row_signature_calc,
            'previous_row_signature', NULL::text,
            'timesheet_version', payload_rows.timesheet_version,
            'current_version', payload_rows.timesheet_version,
            'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
            'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
            'was_stale', FALSE,
            'candidate_id', payload_rows.candidate_id,
            'candidate_name', payload_rows.candidate_name,
            'candidate_display_name', payload_rows.candidate_name,
            'client_id', payload_rows.client_id,
            'client_name', payload_rows.client_name,
            'client_display_name', payload_rows.client_name,
            'booking_id', payload_rows.booking_id,
            'booking_ref', payload_rows.booking_id,
            'occupant_key_norm', payload_rows.occupant_key_norm,
            'hospital_name', payload_rows.hospital_norm,
            'hospital_norm', payload_rows.hospital_norm,
            'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
            'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
            'period_type', payload_rows.period_type,
            'sheet_scope', payload_rows.sheet_scope,
            'timesheet_scope', payload_rows.sheet_scope,
            'submission_mode', payload_rows.submission_mode,
            'submission_mode_snapshot', payload_rows.submission_mode,
            'basis', payload_rows.basis,
            'route_type', payload_rows.effective_route_type_calc,
            'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
            'route_family', payload_rows.route_family_calc,
            'route_subfamily', payload_rows.route_subfamily_calc
          )
          || jsonb_build_object(
            'underlying_channel_family', payload_rows.underlying_channel_family_calc,
            'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
            'compare_block_required', payload_rows.compare_block_required_calc,
            'is_adjustment', payload_rows.effective_is_adjustment_calc,
            'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
            'actual_schedule_json', payload_rows.actual_schedule_json_calc,
            'planned_schedule_json', payload_rows.planned_schedule_json_calc,
            'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
            'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'processing_status', payload_rows.processing_status::text,
            'processing_status_display', payload_rows.processing_status_display,
            'summary_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.summary_stage
        END,
            'tools_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.tools_stage
        END,
            'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
            'previous_bulk_process_bucket', NULL::text,
            'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
            'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
            'correction_id',payload_rows.timesheet_correction_id,
            'correction_kind',payload_rows.timesheet_correction_kind,
            'adjustment_origin',payload_rows.timesheet_adjustment_origin,
            'correction_source_system',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
              and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
              and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
              and payload_rows.timesheet_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
              then case when payload_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
            'correction_display_label',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
              and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
              and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
              then case
                when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
                when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
                when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
                when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
            'previous_bulk_authorise_section', NULL::text,
            'is_authorised', payload_rows.is_authorised_calc,
            'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
            'authorised_at_server', payload_rows.authorised_at_server,
            'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
            'requires_authorisation', payload_rows.requires_authorisation_calc,
            'locked', payload_rows.locked_calc,
            'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
            'paid_at_utc', payload_rows.paid_at_utc,
            'review_only', payload_rows.review_only_calc,
            'can_save', payload_rows.can_save_calc,
            'can_process', payload_rows.can_process_calc,
            'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
            'can_unprocess', payload_rows.can_unprocess_calc,
            'unprocess_block_reason', payload_rows.unprocess_block_reason,
            'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
            'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
            'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
            'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
            'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
            'can_manage_evidence', payload_rows.can_manage_evidence_calc,
            'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
          )
          || jsonb_build_object(
            'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
            'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
            'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
            'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
            'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
            'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
            'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
            'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
            'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
            'invoice_segment_stage', payload_rows.invoice_segment_stage,
            'pay_icon_code', payload_rows.pay_icon_code,
            'pay_status_code', payload_rows.pay_status_code,
            'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
            'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
            'validation_status', payload_rows.validation_status::text,
            'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
            'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
            'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
            'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
            'qr_signed_returned', payload_rows.qr_signed_returned_calc,
            'is_qr', payload_rows.is_qr_route,
            'qr_status', payload_rows.qr_status::text,
            'qr_generated_at', payload_rows.timesheet_qr_generated_at,
            'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
            'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
            'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
            'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
            'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE)
          )
          || jsonb_build_object(
            'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
            'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
            'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
            'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
            'has_any_evidence', (
              COALESCE(payload_rows.evidence_attached_count, 0) > 0
              OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
              OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
            ),
            'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
            'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
            'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
            'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
            'previous_primary_artifact_storage_key', NULL::text,
            'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
            'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
            'evidence_badges', jsonb_build_array(
              jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
              jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
              jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
              jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
              jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
            ),
            'count_deltas', payload_rows.count_deltas_json,
            'cache_invalidation_hints', payload_rows.cache_hints_json
          )
        )
      )
    )) AS row_json
  FROM payload_rows
  ORDER BY
    COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date) ASC NULLS LAST,
    payload_rows.client_name ASC NULLS LAST,
    payload_rows.candidate_name ASC NULLS LAST,
    payload_rows.row_key_calc ASC;
END;
$function$;

ALTER FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_create_json jsonb := CASE WHEN p_timesheet_create_json IS NULL THEN NULL WHEN jsonb_typeof(p_timesheet_create_json) = 'object' THEN p_timesheet_create_json ELSE NULL END;
  v_patch_json jsonb := CASE WHEN p_timesheet_patch_json IS NULL THEN '{}'::jsonb WHEN jsonb_typeof(p_timesheet_patch_json) = 'object' THEN p_timesheet_patch_json ELSE NULL END;
  v_week_patch_json jsonb := CASE WHEN p_contract_week_patch_json IS NULL THEN '{}'::jsonb WHEN jsonb_typeof(p_contract_week_patch_json) = 'object' THEN p_contract_week_patch_json ELSE NULL END;
  v_tsfin_snapshot_json jsonb := CASE WHEN p_tsfin_snapshot_json IS NULL THEN NULL WHEN jsonb_typeof(p_tsfin_snapshot_json) = 'object' THEN p_tsfin_snapshot_json ELSE NULL END;
  v_rotation_json jsonb := CASE WHEN p_rotation_json IS NULL THEN NULL WHEN jsonb_typeof(p_rotation_json) = 'object' THEN p_rotation_json ELSE NULL END;
  v_queue_timesheet_materialisation_json jsonb := CASE WHEN p_queue_timesheet_materialisation_json IS NULL THEN NULL WHEN jsonb_typeof(p_queue_timesheet_materialisation_json) = 'object' THEN p_queue_timesheet_materialisation_json ELSE NULL END;
  v_suppress_timesheet_evidence_materialisation boolean := false;
  v_has_selected_queue_timesheet_materialisation boolean := false;
  v_create_rec public.timesheets%ROWTYPE;
  v_patch_rec public.timesheets%ROWTYPE;
  v_week_patch_rec public.contract_weeks%ROWTYPE;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_created_now boolean := false;
  v_was_stale boolean := false;
  v_previous_contract_week_status text := NULL;
  v_previous_processing_status text := NULL;
  v_segment_invoice_lock boolean := false;
  v_rotation_action text := NULL;
  v_rotation_new_timesheet_id uuid := NULL;
  v_rotation_pending_qr boolean := false;
  v_rotation_revoke_reason text := NULL;
  v_next_version integer := NULL;
  v_rotated_ts public.timesheets%ROWTYPE;
  v_original_booking_id text := NULL;
  v_existing_same_booking public.timesheets%ROWTYPE;
  v_queue_item public.manual_timesheet_queue%ROWTYPE;
  v_queue_kind text := NULL;
  v_queue_storage_key text := NULL;
  v_primary_timesheet_storage_key text := NULL;
  v_primary_timesheet_rotation_raw integer := 0;
  v_primary_timesheet_rotation_deg integer := 0;
  v_primary_timesheet_queue_id uuid := NULL;
  v_timesheet_stage_key_count integer := 0;
  v_attached_evidence_count integer := 0;
  v_attached_queue_count integer := 0;
  v_duplicate_queue_count integer := 0;
  v_selected_queue_id_text text := NULL;
  v_selected_queue_id uuid := NULL;
  v_selected_queue_storage_key text := NULL;
  v_selected_queue_contract_week_text text := NULL;
  v_existing_timesheet_evidence_conflict_count integer := 0;
  v_tsfin_result jsonb := '{}'::jsonb;
  v_summary_refresh_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_constraint text := NULL;
  v_temp_log_enabled boolean := false;
  v_signature_diag_json jsonb := '{}'::jsonb;
  v_diag_started_at timestamp with time zone := clock_timestamp();
  v_diag_stage_started_at timestamp with time zone := clock_timestamp();
  v_diag_elapsed_ms numeric := 0;
  v_diag_duration_ms numeric := 0;
  v_diag_step_index integer := 0;
  v_staged_evidence_loop_count integer := 0;
  v_candidate_final_state_guard jsonb := '{}'::jsonb;
  v_candidate_route_guard jsonb := '{}'::jsonb;
  v_candidate_workflow_id uuid := NULL;
  v_candidate_workflow_kind text := NULL;
  v_candidate_workflow_route text := NULL;
  v_candidate_electronic_context boolean :=
    COALESCE(current_setting('cloudtms.candidate_electronic_finalise', true), '') <> ''
    AND private._candidate_feature_enabled_current_v1('candidate_app_writes');
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);
  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'manual_timesheet_save', true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);
  PERFORM set_config('cloudtms.summary_refresh_mode', 'ordinary_manual_save_lightweight', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', COALESCE(p_expected_timesheet_id::text, ''), true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;
  IF p_timesheet_patch_json IS NOT NULL AND v_patch_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_patch_json')::text;
  END IF;
  IF p_contract_week_patch_json IS NOT NULL AND v_week_patch_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_contract_week_patch_json')::text;
  END IF;
  IF p_timesheet_create_json IS NOT NULL AND v_create_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_create_json')::text;
  END IF;
  IF NOT v_candidate_electronic_context AND (
       COALESCE(v_create_json,'{}'::jsonb) ?| ARRAY[
         'candidate_workflow_id','candidate_workflow_generation','candidate_manager_approved_at_utc']
       OR COALESCE(v_patch_json,'{}'::jsonb) ?| ARRAY[
         'candidate_workflow_id','candidate_workflow_generation','candidate_manager_approved_at_utc']
     ) THEN
    RAISE EXCEPTION USING MESSAGE='CANDIDATE_FINALISE_CONTEXT_REQUIRED';
  END IF;
  IF p_tsfin_snapshot_json IS NOT NULL AND v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_tsfin_snapshot_json')::text;
  END IF;
  IF p_rotation_json IS NOT NULL AND v_rotation_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json')::text;
  END IF;
  IF p_queue_timesheet_materialisation_json IS NOT NULL AND v_queue_timesheet_materialisation_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_queue_timesheet_materialisation_json')::text;
  END IF;

  IF v_queue_timesheet_materialisation_json IS NOT NULL THEN
    v_suppress_timesheet_evidence_materialisation := LOWER(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'suppress_timesheet_evidence_materialisation', v_queue_timesheet_materialisation_json ->> 'suppressTimesheetEvidenceMaterialisation', ''))) IN ('true','1','yes','y','on');
    v_has_selected_queue_timesheet_materialisation := NOT v_suppress_timesheet_evidence_materialisation
      AND NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'queue_id', v_queue_timesheet_materialisation_json ->> 'queueId', '')), '') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storageKey', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'r2_key', '')), ''), ''), '^/+', ''), '') IS NOT NULL;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'entry_payload_validated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'has_create_json', v_create_json IS NOT NULL,
          'has_patch_json', v_patch_json <> '{}'::jsonb,
          'has_week_patch_json', v_week_patch_json <> '{}'::jsonb,
          'has_tsfin_snapshot_json', v_tsfin_snapshot_json IS NOT NULL,
          'has_rotation_json', v_rotation_json IS NOT NULL,
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'has_queue_timesheet_materialisation_json', v_queue_timesheet_materialisation_json IS NOT NULL,
          'suppress_timesheet_evidence_materialisation', v_suppress_timesheet_evidence_materialisation,
          'has_selected_queue_timesheet_materialisation', v_has_selected_queue_timesheet_materialisation,
          'expected_row_signature_present', NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NOT NULL
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;
  v_previous_contract_week_status := v_week.status::text;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_locked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_id', v_week.contract_id,
          'week_status', v_week.status::text,
          'contract_week_timesheet_id', v_week.timesheet_id,
          'week_ending_date', v_week.week_ending_date,
          'previous_contract_week_status', v_previous_contract_week_status
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'staged_timesheet_advisory_lock_acquired',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'advisory_lock_scope', 'contract_week_staged_timesheet',
          'contract_id', v_week.contract_id,
          'week_status', v_week.status::text
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT c.*
    INTO v_contract
  FROM public.contracts AS c
  WHERE c.id = v_week.contract_id
  FOR UPDATE;

  IF v_contract.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_id', v_week.contract_id)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_locked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_id', v_contract.id,
          'candidate_id', v_contract.candidate_id,
          'client_id', v_contract.client_id,
          'pay_method_snapshot', v_contract.pay_method_snapshot,
          'default_submission_mode', v_contract.default_submission_mode
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_week.status IN ('AUTHORISED'::public.contract_week_status_enum, 'INVOICED'::public.contract_week_status_enum, 'CANCELLED'::public.contract_week_status_enum) THEN
    RAISE EXCEPTION USING
      MESSAGE = CASE WHEN v_week.status = 'INVOICED'::public.contract_week_status_enum THEN 'INVOICED_OR_LOCKED' ELSE 'TIMESHEET_LOCKED_OR_PAID' END,
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'contract_week_status', v_week.status::text)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_status_gate_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_status', v_week.status::text,
          'locked_status_gate_passed', true
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_pointer_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    FOR UPDATE;

    IF v_pointer_ts.timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_week.timesheet_id)::text;
    END IF;

    IF COALESCE(v_pointer_ts.is_current, false) THEN
      v_current_ts := v_pointer_ts;
    ELSE
      SELECT ts.*
        INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_pointer_ts.booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;
      IF v_current_ts.timesheet_id IS NULL THEN
        v_current_ts := v_pointer_ts;
      ELSE
        v_was_stale := true;
      END IF;
    END IF;

    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
    END IF;
    IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
      RAISE EXCEPTION USING
        MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH',
        DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_timesheet_resolved',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_pointer_timesheet_id', v_week.timesheet_id,
          'pointer_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id END,
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'booking_id', CASE WHEN v_current_ts.booking_id IS NULL THEN NULL ELSE v_current_ts.booking_id END,
          'timesheet_version', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.version END,
          'timesheet_is_current', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.is_current END,
          'was_stale', v_was_stale
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    IF v_current_ts.archived_at_utc IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;

    SELECT tf.*
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = v_current_ts.timesheet_id
      AND tf.is_current = true
    ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status::text;
      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
      END IF;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      IF COALESCE(v_segment_invoice_lock, false) THEN
        RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'lock_scope', 'segment')::text;
      END IF;
    END IF;
  END IF;

  IF v_candidate_electronic_context THEN
    BEGIN
      v_candidate_workflow_id := split_part(
        current_setting('cloudtms.candidate_electronic_finalise', true),
        ':',
        1
      )::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
      RAISE EXCEPTION 'CANDIDATE_FINALISE_CONTEXT_INVALID' USING ERRCODE='42501';
    END;

    SELECT upper(w.workflow_kind),upper(w.route)
      INTO v_candidate_workflow_kind,v_candidate_workflow_route
    FROM public.candidate_submission_workflows AS w
    WHERE w.id=v_candidate_workflow_id
      AND w.contract_week_id=v_week.id
      AND w.state IN ('READY_TO_FINALISE','RECEIVED')
    FOR SHARE;

    IF NOT FOUND OR v_candidate_workflow_kind NOT IN ('CONTRACT_HOURS','CONTRACT_COMBINED') THEN
      RAISE EXCEPTION 'CANDIDATE_FINALISE_WORKFLOW_INVALID' USING ERRCODE='42501';
    END IF;

    v_candidate_route_guard := private._candidate_route_family_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      v_week.id
    );
    IF v_candidate_route_guard->>'route_family' IN ('IMPORT_AUTHORITATIVE','MANUAL_NON_QR')
       OR (v_candidate_workflow_route='ELECTRONIC'
           AND v_candidate_route_guard->>'route_family'<>'ELECTRONIC')
       OR (v_candidate_workflow_route='PAPER'
           AND NOT COALESCE(
             (v_candidate_route_guard->>'candidate_paper_submission_allowed')::boolean,
             false
           )) THEN
      RAISE EXCEPTION 'CANDIDATE_ROUTE_NOT_ALLOWED'
        USING ERRCODE='42501',
              DETAIL=jsonb_build_object(
                'workflow_id',v_candidate_workflow_id,
                'workflow_route',v_candidate_workflow_route,
                'route_family',v_candidate_route_guard->>'route_family'
              )::text;
    END IF;
  END IF;

  v_candidate_final_state_guard := private._candidate_weekly_final_state_guard_v1(
    v_week.id,
    CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
    v_create_json,
    v_patch_json,
    v_tsfin_snapshot_json
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_tsfin_and_lock_gate_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'current_tsfin_id', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.id END,
          'previous_processing_status', v_previous_processing_status,
          'processing_status', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.processing_status::text END,
          'paid_at_utc', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.paid_at_utc END,
          'locked_by_invoice_id', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.locked_by_invoice_id END,
          'segment_invoice_lock', v_segment_invoice_lock
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'before_signature_generation_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'expected_row_signature_input', NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), ''),
          'patch_backend_row_signature', NULLIF(BTRIM(COALESCE(v_patch_json ->> 'backend_row_signature', '')), ''),
          'patch_row_signature', NULLIF(BTRIM(COALESCE(v_patch_json ->> 'row_signature', '')), ''),
          'week_patch_backend_row_signature', NULLIF(BTRIM(COALESCE(v_week_patch_json ->> 'backend_row_signature', '')), ''),
          'week_patch_row_signature', NULLIF(BTRIM(COALESCE(v_week_patch_json ->> 'row_signature', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END, v_week.id, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, v_patch_json ->> 'backend_row_signature', v_patch_json ->> 'row_signature', v_week_patch_json ->> 'backend_row_signature', v_week_patch_json ->> 'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, v_week.id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'row_signature_mismatch_before_manual_upsert',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3),
          'duration_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3),
          'contract_week_id', v_week.id,
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json
        ))
      );
    END IF;
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'contract_week_id', v_week.id, 'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'row_signature_guard_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'current_row_signature', v_current_row_signature,
          'expected_row_signature', v_expected_row_signature,
          'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
          'signature_match', CASE WHEN v_expected_row_signature IS NULL THEN true ELSE COALESCE(v_current_row_signature, '') IS NOT DISTINCT FROM v_expected_row_signature END
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    IF v_create_json IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_create_json', 'reason', 'required_when_no_current_timesheet')::text;
    END IF;

    v_create_rec := jsonb_populate_record(NULL::public.timesheets, v_create_json);
    IF NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '') IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'booking_id')::text;
    END IF;
    IF COALESCE(v_create_rec.week_ending_date, v_week.week_ending_date) IS DISTINCT FROM v_week.week_ending_date THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'week_ending_date', 'expected_value', v_week.week_ending_date)::text;
    END IF;
    IF COALESCE(v_create_rec.contract_id, v_week.contract_id) IS DISTINCT FROM v_week.contract_id THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'contract_id', 'expected_value', v_week.contract_id)::text;
    END IF;

    v_original_booking_id := NULLIF(BTRIM(v_create_rec.booking_id), '');
    PERFORM pg_advisory_xact_lock(hashtext(v_original_booking_id));

    SELECT ts.*
      INTO v_existing_same_booking
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_original_booking_id
      AND ts.contract_id = v_week.contract_id
      AND ts.week_ending_date = v_week.week_ending_date
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_existing_same_booking.timesheet_id IS NOT NULL THEN
      v_current_ts := v_existing_same_booking;
      v_was_stale := true;
      v_patch_json := (v_create_json - 'timesheet_id' - 'booking_id' - 'version' - 'is_current' - 'contract_id' - 'week_ending_date' - 'created_at') || COALESCE(v_patch_json, '{}'::jsonb);
    ELSE
      SELECT COALESCE(MAX(ts.version), 0) + 1
        INTO v_create_rec.version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_original_booking_id;

      INSERT INTO public.timesheets (
        timesheet_id,
        booking_id,
        occupant_key_norm,
        hospital_norm,
        ward_norm,
        job_title_norm,
        shift_label_norm,
        week_ending_date,
        authorised_at_server,
        auth_name,
        auth_job_title,
        r2_nurse_key,
        r2_auth_key,
        img_sha256_nurse,
        img_sha256_auth,
        candidate_workflow_id,
        candidate_workflow_generation,
        candidate_manager_approved_at_utc,
        status,
        created_at,
        updated_at,
        version,
        is_current,
        contract_id,
        submission_mode,
        manual_pdf_r2_key,
        line_type,
        sheet_scope,
        actual_schedule_json,
        additional_units_week,
        additional_units_per_day,
        qr_token,
        qr_status,
        qr_payload_json,
        qr_generated_at,
        qr_scanned_at,
        qr_scan_info_json,
        qr_r2_key,
        day_references_json,
        manual_pdf_rotation_degrees,
        qr_last_sent_hash,
        qr_last_sent_at_utc,
        qr_signed_hash,
        qr_signed_at_utc,
        candidate_hint_text,
        band,
        is_adjustment
      )
      VALUES (
        COALESCE(v_create_rec.timesheet_id, gen_random_uuid()),
        v_original_booking_id,
        COALESCE(v_create_rec.occupant_key_norm, ''),
        COALESCE(v_create_rec.hospital_norm, ''),
        COALESCE(v_create_rec.ward_norm, ''),
        COALESCE(v_create_rec.job_title_norm, ''),
        COALESCE(v_create_rec.shift_label_norm, 'weekly'),
        v_week.week_ending_date,
        v_create_rec.authorised_at_server,
        v_create_rec.auth_name,
        v_create_rec.auth_job_title,
        v_create_rec.r2_nurse_key,
        v_create_rec.r2_auth_key,
        v_create_rec.img_sha256_nurse,
        v_create_rec.img_sha256_auth,
        v_create_rec.candidate_workflow_id,
        v_create_rec.candidate_workflow_generation,
        v_create_rec.candidate_manager_approved_at_utc,
        COALESCE(v_create_rec.status, 'RECEIVED'::public.timesheet_status_enum),
        COALESCE(v_create_rec.created_at, v_now),
        v_now,
        COALESCE(v_create_rec.version, 1),
        true,
        v_week.contract_id,
        COALESCE(v_create_rec.submission_mode, 'MANUAL'::public.submission_mode_enum),
        v_create_rec.manual_pdf_r2_key,
        COALESCE(v_create_rec.line_type, 'HOURS'::public.timesheet_line_type_enum),
        COALESCE(v_create_rec.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        COALESCE(v_create_rec.actual_schedule_json, '[]'::jsonb),
        COALESCE(v_create_rec.additional_units_week, '{}'::jsonb),
        COALESCE(v_create_rec.additional_units_per_day, '{}'::jsonb),
        v_create_rec.qr_token,
        v_create_rec.qr_status,
        COALESCE(v_create_rec.qr_payload_json, '{}'::jsonb),
        v_create_rec.qr_generated_at,
        v_create_rec.qr_scanned_at,
        v_create_rec.qr_scan_info_json,
        v_create_rec.qr_r2_key,
        v_create_rec.day_references_json,
        COALESCE(v_create_rec.manual_pdf_rotation_degrees, 0),
        v_create_rec.qr_last_sent_hash,
        v_create_rec.qr_last_sent_at_utc,
        v_create_rec.qr_signed_hash,
        v_create_rec.qr_signed_at_utc,
        v_create_rec.candidate_hint_text,
        v_create_rec.band,
        COALESCE(v_create_rec.is_adjustment, COALESCE(v_week.is_adjustment, false))
      )
      RETURNING * INTO v_current_ts;
      v_created_now := true;
    END IF;
  ELSE
    IF v_rotation_json IS NOT NULL THEN
      IF p_actor_user_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id', 'reason', 'required_for_rotation')::text;
      END IF;
      v_rotation_action := UPPER(NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'qr_action', '')), ''));
      IF v_rotation_action NOT IN ('INVALIDATE', 'REISSUE', 'REVOKE_TO_MANUAL') THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json.qr_action')::text;
      END IF;
      v_rotation_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'new_timesheet_id', '')), '')::uuid;
      IF v_rotation_new_timesheet_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json.new_timesheet_id')::text;
      END IF;
      v_rotation_pending_qr := v_rotation_action IN ('INVALIDATE', 'REISSUE');
      v_rotation_revoke_reason := NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'revoke_reason', '')), '');
      PERFORM pg_advisory_xact_lock(hashtext(v_current_ts.booking_id));
      SELECT ts.version
        INTO v_next_version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_current_ts.booking_id
      ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;
      v_next_version := COALESCE(v_next_version, COALESCE(v_current_ts.version, 1)) + 1;

      UPDATE public.timesheets AS ts
         SET is_current = false,
             status = 'REVOKED'::public.timesheet_status_enum,
             revoked_reason = v_rotation_revoke_reason,
             revoked_by = p_actor_user_id::text,
             updated_at = v_now
       WHERE ts.timesheet_id = v_current_ts.timesheet_id
         AND ts.is_current = true;

      v_rotated_ts := v_current_ts;
      v_rotated_ts.timesheet_id := v_rotation_new_timesheet_id;
      v_rotated_ts.version := v_next_version;
      v_rotated_ts.is_current := true;
      v_rotated_ts.status := 'RECEIVED'::public.timesheet_status_enum;
      v_rotated_ts.revoked_at := NULL;
      v_rotated_ts.revoked_reason := NULL;
      v_rotated_ts.revoked_by := NULL;
      v_rotated_ts.authorised_at_server := NULL;
      v_rotated_ts.auth_name := NULL;
      v_rotated_ts.auth_job_title := NULL;
      v_rotated_ts.r2_nurse_key := NULL;
      v_rotated_ts.r2_auth_key := NULL;
      v_rotated_ts.img_sha256_nurse := NULL;
      v_rotated_ts.img_sha256_auth := NULL;
      v_rotated_ts.candidate_workflow_id := NULL;
      v_rotated_ts.candidate_workflow_generation := NULL;
      v_rotated_ts.candidate_manager_approved_at_utc := NULL;
      v_rotated_ts.qr_token := NULL;
      v_rotated_ts.qr_status := CASE WHEN v_rotation_pending_qr THEN 'PENDING'::public.timesheet_qr_status_enum ELSE NULL END;
      v_rotated_ts.qr_payload_json := '{}'::jsonb;
      v_rotated_ts.qr_generated_at := NULL;
      v_rotated_ts.qr_scanned_at := NULL;
      v_rotated_ts.qr_scan_info_json := NULL;
      v_rotated_ts.qr_r2_key := NULL;
      v_rotated_ts.qr_last_sent_hash := NULL;
      v_rotated_ts.qr_last_sent_at_utc := NULL;
      v_rotated_ts.qr_signed_hash := NULL;
      v_rotated_ts.qr_signed_at_utc := NULL;
      v_rotated_ts.manual_pdf_r2_key := NULL;
      v_rotated_ts.created_at := v_now;
      v_rotated_ts.updated_at := v_now;
      INSERT INTO public.timesheets SELECT (v_rotated_ts).* RETURNING * INTO v_current_ts;
    END IF;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'timesheet_identity_ready_for_patch',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'booking_id', v_current_ts.booking_id,
          'timesheet_version', v_current_ts.version,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'rotation_action', v_rotation_action,
          'rotation_new_timesheet_id', v_rotation_new_timesheet_id,
          'rotation_pending_qr', v_rotation_pending_qr,
          'has_patch_json', v_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  END IF;

  IF v_patch_json <> '{}'::jsonb THEN
    v_patch_rec := jsonb_populate_record(v_current_ts, v_patch_json);
    UPDATE public.timesheets AS ts
       SET occupant_key_norm = COALESCE(v_patch_rec.occupant_key_norm, ts.occupant_key_norm),
           hospital_norm = COALESCE(v_patch_rec.hospital_norm, ts.hospital_norm),
           ward_norm = COALESCE(v_patch_rec.ward_norm, ts.ward_norm),
           job_title_norm = COALESCE(v_patch_rec.job_title_norm, ts.job_title_norm),
           shift_label_norm = v_patch_rec.shift_label_norm,
           authorised_at_server = v_patch_rec.authorised_at_server,
           auth_name = v_patch_rec.auth_name,
           auth_job_title = v_patch_rec.auth_job_title,
           r2_nurse_key = v_patch_rec.r2_nurse_key,
           r2_auth_key = v_patch_rec.r2_auth_key,
           img_sha256_nurse = v_patch_rec.img_sha256_nurse,
           img_sha256_auth = v_patch_rec.img_sha256_auth,
           candidate_workflow_id = v_patch_rec.candidate_workflow_id,
           candidate_workflow_generation = v_patch_rec.candidate_workflow_generation,
           candidate_manager_approved_at_utc = v_patch_rec.candidate_manager_approved_at_utc,
           status = COALESCE(v_patch_rec.status, ts.status),
           submission_mode = COALESCE(v_patch_rec.submission_mode, ts.submission_mode),
           manual_pdf_r2_key = v_patch_rec.manual_pdf_r2_key,
           line_type = COALESCE(v_patch_rec.line_type, ts.line_type),
           sheet_scope = COALESCE(v_patch_rec.sheet_scope, ts.sheet_scope),
           actual_schedule_json = COALESCE(v_patch_rec.actual_schedule_json, ts.actual_schedule_json, '[]'::jsonb),
           additional_units_week = COALESCE(v_patch_rec.additional_units_week, ts.additional_units_week, '{}'::jsonb),
           additional_units_per_day = COALESCE(v_patch_rec.additional_units_per_day, ts.additional_units_per_day, '{}'::jsonb),
           qr_token = v_patch_rec.qr_token,
           qr_status = v_patch_rec.qr_status,
           qr_payload_json = COALESCE(v_patch_rec.qr_payload_json, '{}'::jsonb),
           qr_generated_at = v_patch_rec.qr_generated_at,
           qr_scanned_at = v_patch_rec.qr_scanned_at,
           qr_scan_info_json = v_patch_rec.qr_scan_info_json,
           qr_r2_key = v_patch_rec.qr_r2_key,
           day_references_json = v_patch_rec.day_references_json,
           manual_pdf_rotation_degrees = COALESCE(v_patch_rec.manual_pdf_rotation_degrees, ts.manual_pdf_rotation_degrees, 0),
           qr_last_sent_hash = v_patch_rec.qr_last_sent_hash,
           qr_last_sent_at_utc = v_patch_rec.qr_last_sent_at_utc,
           qr_signed_hash = v_patch_rec.qr_signed_hash,
           qr_signed_at_utc = v_patch_rec.qr_signed_at_utc,
           candidate_hint_text = v_patch_rec.candidate_hint_text,
           band = COALESCE(v_patch_rec.band, ts.band),
           is_adjustment = COALESCE(v_patch_rec.is_adjustment, ts.is_adjustment),
           updated_at = v_now
     WHERE ts.timesheet_id = v_current_ts.timesheet_id
       AND ts.is_current = true
     RETURNING * INTO v_current_ts;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'timesheet_patch_applied',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'booking_id', v_current_ts.booking_id,
          'timesheet_version', v_current_ts.version,
          'timesheet_status', v_current_ts.status::text,
          'submission_mode', v_current_ts.submission_mode::text,
          'sheet_scope', v_current_ts.sheet_scope::text,
          'line_type', v_current_ts.line_type::text,
          'actual_schedule_count', CASE WHEN jsonb_typeof(v_current_ts.actual_schedule_json) = 'array' THEN jsonb_array_length(v_current_ts.actual_schedule_json) ELSE NULL END,
          'has_patch_json', v_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF p_materialise_staged_evidence AND v_has_selected_queue_timesheet_materialisation THEN
    v_selected_queue_id_text := NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'queue_id', v_queue_timesheet_materialisation_json ->> 'queueId', '')), '');
    v_selected_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storageKey', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'r2_key', '')), ''), ''), '^/+', ''), '');
    v_selected_queue_contract_week_text := NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'contract_week_id', v_queue_timesheet_materialisation_json ->> 'contractWeekId', '')), '');

    IF v_selected_queue_id_text IS NULL OR v_selected_queue_storage_key IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id_text, 'storage_key', v_selected_queue_storage_key, 'reason', 'missing_queue_identity')::text;
    END IF;

    BEGIN
      v_selected_queue_id := v_selected_queue_id_text::uuid;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id_text, 'storage_key', v_selected_queue_storage_key, 'reason', 'invalid_queue_id')::text;
    END;

    IF v_selected_queue_contract_week_text IS NOT NULL AND v_selected_queue_contract_week_text <> v_week.id::text THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISMATCH', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'supplied_contract_week_id', v_selected_queue_contract_week_text, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'active_contract_week_mismatch')::text;
    END IF;

    SELECT mq.*
      INTO v_queue_item
      FROM public.manual_timesheet_queue AS mq
     WHERE mq.id = v_selected_queue_id
     FOR UPDATE;

    IF v_queue_item.id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'queue_row_not_found')::text;
    END IF;

    v_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'r2_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'file_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'canonical_key', '')), ''), ''), '^/+', ''), '');
    IF v_queue_storage_key IS NULL OR v_queue_storage_key IS DISTINCT FROM v_selected_queue_storage_key THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_STORAGE_MISMATCH', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'expected_storage_key', v_selected_queue_storage_key, 'actual_storage_key', v_queue_storage_key)::text;
    END IF;

    IF UPPER(COALESCE(v_queue_item.status, '')) <> 'QUEUED' OR v_queue_item.timesheet_id IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'status', v_queue_item.status, 'timesheet_id', v_queue_item.timesheet_id)::text;
    END IF;

    v_queue_kind := 'TIMESHEET';

    IF v_primary_timesheet_storage_key IS NULL THEN
      v_primary_timesheet_storage_key := v_queue_storage_key;
      v_primary_timesheet_queue_id := v_queue_item.id;
      v_primary_timesheet_rotation_raw := ((COALESCE(v_queue_item.last_rotation_deg, 0)::integer % 360) + 360) % 360;
      v_primary_timesheet_rotation_deg := CASE WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0 WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90 WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180 ELSE 270 END;
    ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
      RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_primary_timesheet_storage_key, 'conflicting_storage_key', v_queue_storage_key, 'queue_id', v_queue_item.id)::text;
    END IF;
    v_timesheet_stage_key_count := v_timesheet_stage_key_count + 1;

    SELECT COUNT(*)
      INTO v_existing_timesheet_evidence_conflict_count
      FROM public.timesheet_evidence AS te
     WHERE te.timesheet_id = v_current_ts.timesheet_id
       AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
       AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') IS DISTINCT FROM v_queue_storage_key;
    IF COALESCE(v_existing_timesheet_evidence_conflict_count, 0) > 0 THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_EVIDENCE_ALREADY_EXISTS', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_current_ts.timesheet_id, 'queue_id', v_queue_item.id, 'storage_key', v_queue_storage_key)::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1
        FROM public.timesheet_evidence AS te
       WHERE te.timesheet_id = v_current_ts.timesheet_id
         AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
         AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') = v_queue_storage_key
    ) THEN
      INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
      VALUES (v_current_ts.timesheet_id, 'TIMESHEET', v_queue_item.original_filename, v_queue_storage_key, COALESCE(v_queue_item.uploaded_at_utc, v_now), COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id));
      v_attached_evidence_count := v_attached_evidence_count + 1;
    ELSE
      v_duplicate_queue_count := v_duplicate_queue_count + 1;
    END IF;

    UPDATE public.manual_timesheet_queue AS mq
       SET status = 'ATTACHED',
           timesheet_id = v_current_ts.timesheet_id,
           r2_key = v_queue_storage_key,
           meta_json = (COALESCE(mq.meta_json, '{}'::jsonb) - 'deferred_target_timesheet_id' - 'materialisation_deferred_at_utc' - 'deferred_rotation_degrees' - 'dematerialised_from_timesheet_id' - 'dematerialised_from_booking_id' - 'dematerialised_at_utc')
             || jsonb_build_object(
               'contract_week_id', v_week.id::text,
               'staged_kind', 'TIMESHEET',
               'selected_queue_timesheet_materialisation', true,
               'materialisation_deferred_to_backend', false,
               'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
               'materialised_at_utc', v_now,
               'materialised_storage_key', v_queue_storage_key,
               'materialised_from_process_preview', true,
               'preview_selection_key', COALESCE(v_queue_timesheet_materialisation_json ->> 'preview_selection_key', v_queue_timesheet_materialisation_json ->> 'previewSelectionKey'),
               'preview_identity', COALESCE(v_queue_timesheet_materialisation_json ->> 'preview_identity', v_queue_timesheet_materialisation_json ->> 'previewIdentity'),
               'active_identity', COALESCE(v_queue_timesheet_materialisation_json ->> 'active_identity', v_queue_timesheet_materialisation_json ->> 'activeIdentity')
             )
     WHERE mq.id = v_queue_item.id
       AND mq.status = 'QUEUED'
       AND mq.timesheet_id IS NULL;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'conditional_attach_failed')::text;
    END IF;
    v_attached_queue_count := v_attached_queue_count + 1;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'selected_queue_materialisation_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'has_selected_queue_timesheet_materialisation', v_has_selected_queue_timesheet_materialisation,
          'selected_queue_id', v_selected_queue_id,
          'selected_queue_storage_key', v_selected_queue_storage_key,
          'primary_timesheet_queue_id', v_primary_timesheet_queue_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count,
          'timesheet_stage_key_count', v_timesheet_stage_key_count
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF p_materialise_staged_evidence THEN
    FOR v_queue_item IN
      SELECT mq.*
      FROM public.manual_timesheet_queue AS mq
      WHERE mq.status = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      ORDER BY mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
      FOR UPDATE
    LOOP
      v_staged_evidence_loop_count := v_staged_evidence_loop_count + 1;
      v_queue_kind := UPPER(COALESCE(NULLIF(BTRIM(v_queue_item.meta_json ->> 'staged_kind'), ''), NULLIF(BTRIM(v_queue_item.meta_json ->> 'kind'), ''), NULLIF(BTRIM(v_queue_item.meta_json ->> 'attached_kind'), ''), 'TIMESHEET'));
      IF v_queue_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
        v_queue_kind := 'OTHER';
      END IF;
      IF v_queue_kind = 'TIMESHEET' AND (v_suppress_timesheet_evidence_materialisation OR v_has_selected_queue_timesheet_materialisation) THEN
        CONTINUE;
      END IF;
      v_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'r2_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'file_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'canonical_key', '')), ''), ''), '^/+', ''), '');
      IF v_queue_storage_key IS NULL THEN
        IF v_queue_kind = 'TIMESHEET' THEN
          RAISE EXCEPTION USING MESSAGE = 'INVALID_TIMESHEET_EVIDENCE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_queue_item.id, 'reason', 'missing_storage_key')::text;
        END IF;
        CONTINUE;
      END IF;

      IF v_queue_kind = 'TIMESHEET' THEN
        IF v_primary_timesheet_storage_key IS NULL THEN
          v_primary_timesheet_storage_key := v_queue_storage_key;
          v_primary_timesheet_queue_id := v_queue_item.id;
          v_primary_timesheet_rotation_raw := ((COALESCE(v_queue_item.last_rotation_deg, 0)::integer % 360) + 360) % 360;
          v_primary_timesheet_rotation_deg := CASE WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0 WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90 WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180 ELSE 270 END;
        ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
          RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_primary_timesheet_storage_key, 'conflicting_storage_key', v_queue_storage_key, 'queue_id', v_queue_item.id)::text;
        END IF;
        v_timesheet_stage_key_count := v_timesheet_stage_key_count + 1;
      END IF;

      IF NOT EXISTS (
        SELECT 1
        FROM public.timesheet_evidence AS te
        WHERE te.timesheet_id = v_current_ts.timesheet_id
          AND te.kind = v_queue_kind
          AND te.storage_key = v_queue_storage_key
      ) THEN
        INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
        VALUES (v_current_ts.timesheet_id, v_queue_kind, v_queue_item.original_filename, v_queue_storage_key, COALESCE(v_queue_item.uploaded_at_utc, v_now), COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id));
        v_attached_evidence_count := v_attached_evidence_count + 1;
      ELSE
        v_duplicate_queue_count := v_duplicate_queue_count + 1;
      END IF;

      UPDATE public.manual_timesheet_queue AS mq
         SET status = 'ATTACHED',
             timesheet_id = v_current_ts.timesheet_id,
             r2_key = v_queue_storage_key,
             meta_json = (COALESCE(mq.meta_json, '{}'::jsonb) - 'deferred_target_timesheet_id' - 'materialisation_deferred_at_utc' - 'deferred_rotation_degrees' - 'dematerialised_from_timesheet_id' - 'dematerialised_from_booking_id' - 'dematerialised_at_utc')
               || jsonb_build_object(
                 'contract_week_id', v_week.id::text,
                 'staged_kind', v_queue_kind,
                 'materialisation_deferred_to_backend', false,
                 'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
                 'materialised_at_utc', v_now,
                 'materialised_storage_key', v_queue_storage_key,
                 'duplicate_timesheet_evidence_identity', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN true ELSE false END,
                 'duplicate_of_queue_item_id', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN v_primary_timesheet_queue_id::text ELSE NULL END,
                 'materialisation_noop_reason', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN 'same_storage_key_duplicate' ELSE NULL END
               )
       WHERE mq.id = v_queue_item.id;
      v_attached_queue_count := v_attached_queue_count + 1;
    END LOOP;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'staged_evidence_materialisation_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'staged_evidence_loop_count', v_staged_evidence_loop_count,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count,
          'timesheet_stage_key_count', v_timesheet_stage_key_count,
          'primary_timesheet_queue_id', v_primary_timesheet_queue_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_primary_timesheet_storage_key IS NOT NULL THEN
    UPDATE public.timesheets AS ts
       SET manual_pdf_r2_key = v_primary_timesheet_storage_key,
           manual_pdf_rotation_degrees = v_primary_timesheet_rotation_deg,
           updated_at = v_now
     WHERE ts.timesheet_id = v_current_ts.timesheet_id
       AND ts.is_current = true
     RETURNING * INTO v_current_ts;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'primary_timesheet_storage_applied',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg,
          'manual_pdf_r2_key', v_current_ts.manual_pdf_r2_key,
          'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_week_patch_rec := jsonb_populate_record(v_week, v_week_patch_json);
  UPDATE public.contract_weeks AS cw
     SET status = COALESCE(v_week_patch_rec.status, 'SUBMITTED'::public.contract_week_status_enum),
         submission_mode_snapshot = COALESCE(v_week_patch_rec.submission_mode_snapshot, cw.submission_mode_snapshot, 'MANUAL'::public.submission_mode_enum),
         timesheet_id = v_current_ts.timesheet_id,
         uploaded_pdf_r2_key = COALESCE(v_week_patch_rec.uploaded_pdf_r2_key, v_primary_timesheet_storage_key, cw.uploaded_pdf_r2_key),
         day_entries_json = COALESCE(v_week_patch_rec.day_entries_json, cw.day_entries_json),
         totals_json = COALESCE(v_week_patch_rec.totals_json, cw.totals_json),
         planned_schedule_json = COALESCE(v_week_patch_rec.planned_schedule_json, cw.planned_schedule_json),
         is_adjustment = COALESCE(v_week_patch_rec.is_adjustment, cw.is_adjustment),
         enforce_day_partition = COALESCE(v_week_patch_rec.enforce_day_partition, cw.enforce_day_partition),
         allowed_days_mask = COALESCE(v_week_patch_rec.allowed_days_mask, cw.allowed_days_mask),
         split_boundary_date = COALESCE(v_week_patch_rec.split_boundary_date, cw.split_boundary_date),
         worker_note = COALESCE(v_week_patch_rec.worker_note, cw.worker_note),
         split_group_key = COALESCE(v_week_patch_rec.split_group_key, cw.split_group_key),
         updated_at = v_now
   WHERE cw.id = v_week.id
   RETURNING * INTO v_week;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_update_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_id', v_week.id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'new_contract_week_status', v_week.status::text,
          'submission_mode_snapshot', v_week.submission_mode_snapshot::text,
          'uploaded_pdf_r2_key', v_week.uploaded_pdf_r2_key,
          'has_week_patch_json', v_week_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_tsfin_snapshot_json')::text;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), '')::uuid IS DISTINCT FROM v_contract.client_id THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'client_id', 'expected_value', v_contract.client_id, 'supplied_value', v_tsfin_snapshot_json ->> 'client_id')::text;
  END IF;
  IF v_contract.candidate_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), '')::uuid IS DISTINCT FROM v_contract.candidate_id THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'candidate_id', 'expected_value', v_contract.candidate_id, 'supplied_value', v_tsfin_snapshot_json ->> 'candidate_id')::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_snapshot_validated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'contract_client_id', v_contract.client_id,
          'contract_candidate_id', v_contract.candidate_id,
          'snapshot_client_id', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), ''),
          'snapshot_candidate_id', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), ''),
          'snapshot_processing_status', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''),
          'snapshot_total_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_pay_ex_vat', '')), ''),
          'snapshot_total_charge_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_charge_ex_vat', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_tsfin_snapshot_json := v_tsfin_snapshot_json || jsonb_build_object(
    'timesheet_id', v_current_ts.timesheet_id::text,
    'timesheet_version', v_current_ts.version,
    'processing_status', CASE
      WHEN COALESCE(NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''), 'PENDING_AUTH') = 'AWAITING_MANUAL_SIGNATURE'
       AND v_current_ts.submission_mode = 'MANUAL'::public.submission_mode_enum
       AND v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
       AND v_current_ts.qr_status IS NULL
      THEN 'PENDING_AUTH'
      ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''), 'PENDING_AUTH')
    END
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'snapshot_processing_status', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''),
          'snapshot_basis', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'basis', '')), ''),
          'snapshot_total_hours', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_hours', '')), ''),
          'snapshot_total_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_pay_ex_vat', '')), ''),
          'snapshot_total_charge_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_charge_ex_vat', '')), ''),
          'snapshot_expenses_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'expenses_pay_ex_vat', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_tsfin_result := public.tsfin_write_current_snapshot_single_bounded(
    p_timesheet_id => v_current_ts.timesheet_id,
    p_timesheet_version => v_current_ts.version,
    p_snapshot_json => v_tsfin_snapshot_json,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => v_now
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_completed',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'tsfin_result_ok', COALESCE((v_tsfin_result ->> 'ok')::boolean, false),
          'tsfin_result_code', NULLIF(BTRIM(COALESCE(v_tsfin_result ->> 'code', v_tsfin_result ->> 'error_code', '')), ''),
          'tsfin_result_message', NULLIF(BTRIM(COALESCE(v_tsfin_result ->> 'message', v_tsfin_result ->> 'error', '')), ''),
          'tsfin_result_keys', (SELECT jsonb_agg(result_key ORDER BY result_key) FROM jsonb_object_keys(COALESCE(v_tsfin_result, '{}'::jsonb)) AS result_keys(result_key))
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE((v_tsfin_result ->> 'ok')::boolean, false) IS DISTINCT FROM true THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_UPDATE_FAILED', DETAIL = COALESCE(v_tsfin_result, '{}'::jsonb)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_result_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'tsfin_result_ok', COALESCE((v_tsfin_result ->> 'ok')::boolean, false)
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_tsfin_reloaded_after_write',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'current_tsfin_id', v_current_tsfin.id,
          'processing_status', v_current_tsfin.processing_status::text,
          'total_hours', v_current_tsfin.total_hours,
          'total_pay_ex_vat', v_current_tsfin.total_pay_ex_vat,
          'total_charge_ex_vat', v_current_tsfin.total_charge_ex_vat,
          'margin_ex_vat', v_current_tsfin.margin_ex_vat,
          'computed_at_utc', v_current_tsfin.computed_at_utc
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_summary_refresh_result := public.pay_timesheet_summary_pay_state_refresh(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    p_actor_user_id
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'summary_pay_state_lightweight_patch_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'summary_refresh_result', v_summary_refresh_result
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'after_signature_generation_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'previous_row_signature', v_current_row_signature
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, v_week.id, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  IF COALESCE(v_temp_log_enabled, false) THEN
    PERFORM public._temp_diag_log(
      'TIMESHEET_SAVE_SIGNATURE_DIAG',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_strip_nulls(jsonb_build_object(
        'tag', 'TIMESHEET_SAVE_SIGNATURE_DIAG',
        'function_name', 'contract_week_manual_upsert_atomic',
        'stage', 'after_manual_upsert_signature_generated',
        'action', 'manual_upsert',
        'route_family', 'contract_week_manual_upsert',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3),
        'duration_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3),
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id,
        'previous_row_signature', v_current_row_signature,
        'new_row_signature', v_after_row_signature,
        'signature_payload', v_after_signature_json
      ))
    );
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'after_signature_generated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'previous_row_signature', v_current_row_signature,
          'new_row_signature', v_after_row_signature,
          'signature_payload_present', v_after_signature_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    CASE WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED' ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED' END,
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id END,
      'previous_contract_week_status', v_previous_contract_week_status,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'new_contract_week_status', v_week.status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_row_signature', v_after_row_signature,
      'created_now', v_created_now,
      'attached_evidence_count', v_attached_evidence_count,
      'attached_queue_count', v_attached_queue_count,
      'duplicate_queue_count', v_duplicate_queue_count,
      'primary_timesheet_storage_key', v_primary_timesheet_storage_key
    ),
    'WEEKLY_MANUAL_PROCESS',
    p_actor_user_id
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'audit_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'audit_action', CASE WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED' ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED' END,
          'new_processing_status', v_current_tsfin.processing_status::text,
          'new_row_signature', v_after_row_signature
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'return',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'operation', CASE WHEN v_created_now THEN 'weekly_manual_process_create' ELSE 'weekly_manual_process_update' END,
          'new_contract_week_status', v_week.status::text,
          'new_processing_status', v_current_tsfin.processing_status::text,
          'new_row_signature', v_after_row_signature,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', CASE WHEN v_created_now THEN 'weekly_manual_process_create' ELSE 'weekly_manual_process_update' END,
    'contract_week_id', v_week.id,
    'contract_id', v_week.contract_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_version', v_current_ts.version,
    'was_stale', v_was_stale,
    'created_now', v_created_now,
    'previous_contract_week_status', v_previous_contract_week_status,
    'new_contract_week_status', v_week.status::text,
    'previous_processing_status', v_previous_processing_status,
    'processing_status', v_current_tsfin.processing_status::text,
    'new_processing_status', v_current_tsfin.processing_status::text,
    'timesheet_financials_id', v_current_tsfin.id,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'expected_row_signature', v_after_row_signature,
    'lifecycle_signature_stable', v_after_row_signature IS NOT NULL,
    'lifecycle_signature_pending_reason', CASE WHEN v_after_row_signature IS NULL THEN 'POST_SAVE_ROW_SIGNATURE_UNAVAILABLE' ELSE NULL::text END,
    'requires_authorise_preflight', v_after_row_signature IS NULL,
    'requires_affected_row_refresh', v_after_row_signature IS NULL,
    'refresh_required', v_after_row_signature IS NULL,
    'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
    'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
    'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
    'lifecycle_patch', jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'timesheet_status', v_current_ts.status::text,
      'status', v_current_ts.status::text,
      'contract_week_status', v_week.status::text,
      'processing_status', v_current_tsfin.processing_status::text,
      'tsfin_processing_status', v_current_tsfin.processing_status::text,
      'timesheet_financials_id', v_current_tsfin.id,
      'is_current', v_current_ts.is_current,
      'tsfin_is_current', v_current_tsfin.is_current,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature,
      'expected_row_signature', v_after_row_signature,
      'lifecycle_signature_stable', v_after_row_signature IS NOT NULL,
      'lifecycle_signature_pending_reason', CASE WHEN v_after_row_signature IS NULL THEN 'POST_SAVE_ROW_SIGNATURE_UNAVAILABLE' ELSE NULL::text END,
      'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
      'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
      'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
      'requires_network_before_authorise', v_after_row_signature IS NULL,
      'refresh_required', v_after_row_signature IS NULL,
      'row_stale', v_after_row_signature IS NULL,
      'lifecycle_refresh_failed', v_after_row_signature IS NULL
    )),
    'timesheet', jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'status', v_current_ts.status::text,
      'is_current', v_current_ts.is_current,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature
    )),
    'tsfin', jsonb_strip_nulls(jsonb_build_object(
      'id', v_current_tsfin.id,
      'timesheet_id', v_current_tsfin.timesheet_id,
      'is_current', v_current_tsfin.is_current,
      'processing_status', v_current_tsfin.processing_status::text,
      'total_hours', v_current_tsfin.total_hours,
      'total_pay_ex_vat', v_current_tsfin.total_pay_ex_vat,
      'total_charge_ex_vat', v_current_tsfin.total_charge_ex_vat,
      'margin_ex_vat', v_current_tsfin.margin_ex_vat,
      'computed_at_utc', v_current_tsfin.computed_at_utc,
      'updated_at', v_current_tsfin.updated_at
    )),
    'summary_pay_state_refresh', COALESCE(v_summary_refresh_result, '{}'::jsonb),
    'evidence_summary', jsonb_build_object(
      'attached_evidence_count', v_attached_evidence_count,
      'attached_queue_count', v_attached_queue_count,
      'duplicate_queue_count', v_duplicate_queue_count,
      'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
      'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg,
      'selected_queue_timesheet_queue_id', v_selected_queue_id,
      'selected_queue_timesheet_storage_key', v_selected_queue_storage_key
    ),
    'affected_rows', jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'timesheet_status', v_current_ts.status::text,
      'contract_week_status', v_week.status::text,
      'processing_status', v_current_tsfin.processing_status::text,
      'tsfin_processing_status', v_current_tsfin.processing_status::text,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature,
      'expected_row_signature', v_after_row_signature,
      'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
      'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
      'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
      'requires_network_before_authorise', v_after_row_signature IS NULL,
      'row_key', 'timesheet:' || v_current_ts.timesheet_id::text
    ))),
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'timesheet_summary_pay_state_cache', 'timesheet_pay_state', 'contract_weeks', 'timesheet_evidence', 'manual_timesheet_queue'),
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_current_ts.booking_id
    )
  ) || CASE
    WHEN private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') THEN
      jsonb_build_object(
        'candidate_record_role',v_candidate_final_state_guard->>'record_role',
        'candidate_expected_line_type',v_candidate_final_state_guard->>'expected_line_type',
        'candidate_final_state_guard',v_candidate_final_state_guard)
    ELSE '{}'::jsonb END;
EXCEPTION
  WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_error_constraint = CONSTRAINT_NAME;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_diag_step_index := v_diag_step_index + 1;
      v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
      v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
      PERFORM public._temp_diag_log(
        'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'exception_unique_violation',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'constraint_name', v_error_constraint
        ))
      );
      v_diag_stage_started_at := clock_timestamp();
    END IF;
    IF v_error_constraint = 'uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr' THEN
      RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'constraint_name', v_error_constraint)::text;
    END IF;
    RAISE;
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_diag_step_index := v_diag_step_index + 1;
      v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
      v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
      PERFORM public._temp_diag_log(
        'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'exception_others',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'error_state', v_error_state,
          'error_message', v_error_message
        ))
      );
      v_diag_stage_started_at := clock_timestamp();
    END IF;
    IF v_error_state = '55P03' THEN
      RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'error_state', v_error_state)::text;
    END IF;
    RAISE;
END;
$function$;

ALTER FUNCTION public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_mark_finance_case_dirty()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := clock_timestamp();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_old_finance_case_id uuid := NULL::uuid;
  v_new_finance_case_id uuid := NULL::uuid;
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_id uuid;
  v_candidate_id uuid;
  v_scope_id text;
  v_reason text;
  v_payload_json jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_jobs_queued integer := 0;
  v_internal_build_token uuid := NULL::uuid;
  v_internal_candidate_id uuid := NULL::uuid;
  v_internal_source_id uuid := NULL::uuid;
  v_internal_timesheet_id uuid := NULL::uuid;
  v_internal_logical_source_id uuid := NULL::uuid;
  v_internal_finance_case_id uuid := NULL::uuid;
  v_internal_finance_component_id uuid := NULL::uuid;
  v_internal_before_digest text := NULL::text;
  v_internal_after_digest text := NULL::text;
  v_expected_match_count integer := 0;
  v_internal_economic_key_type text := NULL::text;
  v_internal_economic_key_value text := NULL::text;
  v_effect_capture_mode boolean := lower(COALESCE(current_setting('cloudtms.pay_workbench_effect_capture_mode',true),''))='capture';
BEGIN
  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_mark_finance_case_dirty', 'stage', 'entry', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'queue_class', 'DIRTY_TRIGGER_PRIORITY'));

  IF TG_OP <> 'DELETE' THEN v_new_row := to_jsonb(NEW); END IF;
  IF TG_OP <> 'INSERT' THEN v_old_row := to_jsonb(OLD); END IF;

  -- These three retained Workbench triggers are recreated as BEFORE ROW
  -- triggers by the V1.2.4 trigger artifact.  The exact transition identity and
  -- full before/after digests are therefore declared before the finance DML is
  -- applied.  The current build token and durable RPC-2 context are required;
  -- an effect outside the sealed build scope fails the reconciliation.
  IF to_regclass('pg_temp._bpay_wb_sync_context_v1') IS NOT NULL
     AND to_regclass('pg_temp._bpay_wb_expected_effects') IS NOT NULL
     AND COALESCE(current_setting('cloudtms.pay_workbench_overpayment_sync_token',true),'')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_internal_build_token:=current_setting('cloudtms.pay_workbench_overpayment_sync_token',true)::uuid;
    SELECT build_row.candidate_id INTO v_internal_candidate_id
    FROM private.banking_pay_workbench_economic_builds build_row
    JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
      ON sync_context.build_id=build_row.id AND sync_context.build_token=build_row.build_token
    JOIN private.banking_pay_workbench_stage_attempts attempt
      ON attempt.id=sync_context.attempt_id AND attempt.attempt_nonce=sync_context.attempt_nonce
      AND attempt.build_id=build_row.id AND attempt.attempt_status='STARTED'
    WHERE build_row.build_token=v_internal_build_token
      AND build_row.status='RECONCILING' AND build_row.private_stage='RECONCILE_EXECUTE';
    IF v_internal_candidate_id IS NOT NULL THEN
      IF TG_WHEN<>'BEFORE' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_internal_source_id:=COALESCE(NULLIF(v_new_row->>'id','')::uuid,
        NULLIF(v_old_row->>'id','')::uuid,NULLIF(v_new_row->>'finance_case_id','')::uuid,
        NULLIF(v_old_row->>'finance_case_id','')::uuid);
      v_internal_timesheet_id:=COALESCE(NULLIF(v_new_row->>'linked_timesheet_id','')::uuid,
        NULLIF(v_old_row->>'linked_timesheet_id','')::uuid,NULLIF(v_new_row->>'timesheet_id','')::uuid,
        NULLIF(v_old_row->>'timesheet_id','')::uuid);
      IF v_internal_source_id IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_internal_finance_case_id:=COALESCE(NULLIF(v_new_row->>'finance_case_id','')::uuid,
        NULLIF(v_old_row->>'finance_case_id','')::uuid,
        CASE WHEN v_trigger_table='pay_advances' THEN v_internal_source_id END);
      v_internal_finance_component_id:=COALESCE(
        NULLIF(v_new_row->>'finance_component_id','')::uuid,
        NULLIF(v_old_row->>'finance_component_id','')::uuid,
        CASE WHEN v_trigger_table='pay_finance_case_components' THEN v_internal_source_id END);
      -- Finance-case event rows do not carry their owner timesheet. Resolve it
      -- from the same stable parent identities used by the AFTER-statement
      -- observer so capture and execution cannot disagree merely because one
      -- side saw a nullable event payload.
      IF v_trigger_table='pay_finance_case_events' THEN
        SELECT COALESCE(component.linked_timesheet_id,finance_case.linked_timesheet_id)
        INTO v_internal_timesheet_id
        FROM (SELECT 1) anchor
        LEFT JOIN public.pay_finance_case_components component
          ON component.id=v_internal_finance_component_id
        LEFT JOIN public.pay_advances finance_case
          ON finance_case.id=COALESCE(v_internal_finance_case_id,component.finance_case_id);
      END IF;
      v_internal_economic_key_type:=COALESCE(NULLIF(btrim(v_new_row->>'component_key_type'),''),
        NULLIF(btrim(v_old_row->>'component_key_type'),''));
      v_internal_economic_key_value:=COALESCE(NULLIF(btrim(v_new_row->>'component_key_value'),''),
        NULLIF(btrim(v_old_row->>'component_key_value'),''));
      v_internal_before_digest:=CASE WHEN TG_OP='INSERT' THEN NULL ELSE md5(
        private.pay_workbench_finance_effect_normalise_row_v1(
          v_trigger_table,TG_OP,v_old_row,v_old_row)::text) END;
      v_internal_after_digest:=CASE WHEN TG_OP='DELETE' THEN NULL ELSE md5(
        private.pay_workbench_finance_effect_normalise_row_v1(
          v_trigger_table,TG_OP,v_new_row,v_old_row)::text) END;

      IF COALESCE(NULLIF(v_new_row->>'candidate_id','')::uuid,
           NULLIF(v_old_row->>'candidate_id','')::uuid,
           (SELECT finance_case.candidate_id FROM public.pay_advances finance_case
             WHERE finance_case.id=v_internal_finance_case_id))
           IS DISTINCT FROM v_internal_candidate_id
         OR (v_internal_timesheet_id IS NOT NULL AND NOT EXISTS(
           SELECT 1 FROM private.banking_pay_workbench_economic_build_scope scope_row
           JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
             ON sync_context.build_id=scope_row.build_id
           WHERE scope_row.timesheet_id=v_internal_timesheet_id
         ))
         OR (v_trigger_table='pay_finance_case_components'
           AND v_internal_economic_key_type IS NOT NULL
           AND NOT EXISTS(
             SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
             JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
               ON sync_context.build_id=fact.build_id
             WHERE fact.economic_key_type=v_internal_economic_key_type
               AND fact.economic_key_value=v_internal_economic_key_value
           )
           AND NOT EXISTS(
             SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
             JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
               ON sync_context.build_id=fact.build_id
             WHERE fact.finance_component_id=v_internal_finance_component_id
           )) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
      END IF;
      IF v_effect_capture_mode THEN
        INSERT INTO pg_temp._bpay_wb_expected_effects(
          build_token,candidate_id,timesheet_id,relation_name,operation,source_id,
          finance_case_id,finance_component_id,economic_key_type,economic_key_value,
          expected_before_digest,expected_after_digest,proposed,observed
        ) VALUES(
          v_internal_build_token,v_internal_candidate_id,v_internal_timesheet_id,v_trigger_table,TG_OP,
          v_internal_source_id,v_internal_finance_case_id,v_internal_finance_component_id,
          v_internal_economic_key_type,v_internal_economic_key_value,
          v_internal_before_digest,v_internal_after_digest,true,false
        );
      ELSIF TG_OP='INSERT' THEN
        UPDATE pg_temp._bpay_wb_expected_effects expected
        SET actual_source_id=v_internal_source_id,proposed=true
        WHERE expected.ctid=(SELECT candidate.ctid
          FROM pg_temp._bpay_wb_expected_effects candidate
          WHERE candidate.build_token=v_internal_build_token
            AND candidate.candidate_id=v_internal_candidate_id
            AND candidate.timesheet_id IS NOT DISTINCT FROM v_internal_timesheet_id
            AND candidate.relation_name=v_trigger_table AND candidate.operation=TG_OP
            AND candidate.proposed IS NOT TRUE AND candidate.observed IS NOT TRUE
            AND candidate.economic_key_type IS NOT DISTINCT FROM v_internal_economic_key_type
            AND candidate.economic_key_value IS NOT DISTINCT FROM v_internal_economic_key_value
            AND candidate.expected_before_digest IS NULL
            AND candidate.expected_after_digest IS NOT DISTINCT FROM v_internal_after_digest
            AND (v_trigger_table<>'pay_finance_case_components' OR
              COALESCE((SELECT identity_map.actual_source_id
                FROM pg_temp._bpay_wb_effect_identity_map_v1 identity_map
                WHERE identity_map.relation_name='pay_advances'
                  AND identity_map.logical_source_id=candidate.finance_case_id),
                candidate.finance_case_id) IS NOT DISTINCT FROM v_internal_finance_case_id)
            AND (v_trigger_table<>'pay_finance_case_events' OR (
              COALESCE((SELECT identity_map.actual_source_id
                FROM pg_temp._bpay_wb_effect_identity_map_v1 identity_map
                WHERE identity_map.relation_name='pay_advances'
                  AND identity_map.logical_source_id=candidate.finance_case_id),
                candidate.finance_case_id) IS NOT DISTINCT FROM v_internal_finance_case_id
              AND COALESCE((SELECT identity_map.actual_source_id
                FROM pg_temp._bpay_wb_effect_identity_map_v1 identity_map
                WHERE identity_map.relation_name='pay_finance_case_components'
                  AND identity_map.logical_source_id=candidate.finance_component_id),
                candidate.finance_component_id) IS NOT DISTINCT FROM v_internal_finance_component_id))
          ORDER BY candidate.source_id LIMIT 1)
        RETURNING expected.source_id INTO v_internal_logical_source_id;
        GET DIAGNOSTICS v_expected_match_count=ROW_COUNT;
        IF v_expected_match_count<>1 OR v_internal_logical_source_id IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
        END IF;
        INSERT INTO pg_temp._bpay_wb_effect_identity_map_v1(
          relation_name,logical_source_id,actual_source_id)
        VALUES(v_trigger_table,v_internal_logical_source_id,v_internal_source_id);
      ELSE
        UPDATE pg_temp._bpay_wb_expected_effects expected SET proposed=true
        WHERE expected.build_token=v_internal_build_token
          AND expected.candidate_id=v_internal_candidate_id
          AND expected.timesheet_id IS NOT DISTINCT FROM v_internal_timesheet_id
          AND expected.relation_name=v_trigger_table AND expected.operation=TG_OP
          AND expected.source_id=v_internal_source_id
          AND expected.finance_case_id IS NOT DISTINCT FROM v_internal_finance_case_id
          AND expected.finance_component_id IS NOT DISTINCT FROM v_internal_finance_component_id
          AND expected.economic_key_type IS NOT DISTINCT FROM v_internal_economic_key_type
          AND expected.economic_key_value IS NOT DISTINCT FROM v_internal_economic_key_value
          AND expected.proposed IS NOT TRUE AND expected.observed IS NOT TRUE
          AND expected.expected_before_digest IS NOT DISTINCT FROM v_internal_before_digest
          AND expected.expected_after_digest IS NOT DISTINCT FROM v_internal_after_digest;
        GET DIAGNOSTICS v_expected_match_count=ROW_COUNT;
        IF v_expected_match_count<>1 THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
        END IF;
      END IF;
      IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
  END IF;

  -- A protected preview synchronisation deliberately records SYNC_SKIPPED as
  -- audit evidence.  That informational event must not dirty Banking Pay and
  -- enqueue the same protected synchronisation again.
  IF v_trigger_table = 'pay_finance_case_events'
     AND TG_OP <> 'DELETE'
     AND UPPER(BTRIM(COALESCE(v_new_row->>'event_type', ''))) = 'SYNC_SKIPPED' THEN
    RETURN NEW;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_old_row->>'finance_case_id', v_old_row->>'id', '')), '') ~* v_uuid_re THEN
    v_old_finance_case_id := NULLIF(BTRIM(COALESCE(v_old_row->>'finance_case_id', v_old_row->>'id', '')), '')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_new_row->>'finance_case_id', v_new_row->>'id', '')), '') ~* v_uuid_re THEN
    v_new_finance_case_id := NULLIF(BTRIM(COALESCE(v_new_row->>'finance_case_id', v_new_row->>'id', '')), '')::uuid;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT finance_cases.finance_case_id ORDER BY finance_cases.finance_case_id), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM (
    SELECT v_old_finance_case_id AS finance_case_id
    UNION ALL
    SELECT v_new_finance_case_id AS finance_case_id
  ) AS finance_cases
  WHERE finance_cases.finance_case_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT candidates.candidate_id ORDER BY candidates.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM (
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid ELSE NULL::uuid END AS candidate_id
    UNION ALL
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid ELSE NULL::uuid END AS candidate_id
    UNION ALL
    SELECT component_old.candidate_id FROM public.pay_finance_case_components AS component_old WHERE component_old.finance_case_id = v_old_finance_case_id
    UNION ALL
    SELECT component_new.candidate_id FROM public.pay_finance_case_components AS component_new WHERE component_new.finance_case_id = v_new_finance_case_id
    UNION ALL
    SELECT advance_old.candidate_id FROM public.pay_advances AS advance_old WHERE advance_old.id = v_old_finance_case_id
    UNION ALL
    SELECT advance_new.candidate_id FROM public.pay_advances AS advance_new WHERE advance_new.id = v_new_finance_case_id
  ) AS candidates
  WHERE candidates.candidate_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT timesheets.timesheet_id ORDER BY timesheets.timesheet_id), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM (
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', v_old_row->>'timesheet_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', v_old_row->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id
    UNION ALL
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', v_new_row->>'timesheet_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', v_new_row->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id
  ) AS timesheets
  WHERE timesheets.timesheet_id IS NOT NULL;

  v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;

  IF COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    FOREACH v_finance_case_id IN ARRAY v_finance_case_ids
    LOOP
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'FINANCE_CASE',
        'scope_id', v_finance_case_id::text,
        'finance_case_id', v_finance_case_id::text,
        'finance_case_ids', to_jsonb(v_finance_case_ids),
        'candidate_ids', to_jsonb(v_candidate_ids),
        'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
        'reason', v_reason,
        'dirty_reason', v_reason,
        'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
        'force_legacy', true,
        'projection_class', 'FINANCE_CASE',
        'fallback_reason', 'FINANCE_CASE_DIRTY_TRIGGER',
        'source_build_required', true,
        'line_work_required', true,
        'row_backed_scope_required', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );

      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
        p_scope_kind => 'FINANCE_CASE',
        p_scope_id => v_finance_case_id::text,
        p_candidate_id => NULL::uuid,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
      v_jobs_queued := v_jobs_queued + 1;
    END LOOP;
  ELSE
    FOREACH v_candidate_id IN ARRAY v_candidate_ids
    LOOP
      v_scope_id := v_candidate_id::text;
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'CANDIDATE',
        'scope_id', v_scope_id,
        'candidate_id', v_scope_id,
        'candidate_ids', to_jsonb(ARRAY[v_candidate_id]),
        'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
        'reason', v_reason,
        'dirty_reason', v_reason,
        'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
        'projection_class', 'FINANCE_CASE',
        'fallback_reason', 'FINANCE_CASE_DIRTY_TRIGGER_NO_CASE_ID',
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );

      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        p_scope_kind => 'CANDIDATE',
        p_scope_id => v_scope_id,
        p_candidate_id => v_candidate_id,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
      v_jobs_queued := v_jobs_queued + 1;
    END LOOP;
  END IF;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_mark_finance_case_dirty', 'stage', 'return_enqueued_dirty_priority', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'finance_case_count', COALESCE(array_length(v_finance_case_ids, 1), 0), 'candidate_count', COALESCE(array_length(v_candidate_ids, 1), 0), 'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0), 'jobs_queued', v_jobs_queued, 'queue_class', 'DIRTY_TRIGGER_PRIORITY', 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));

  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$function$;

ALTER FUNCTION public.pay_workbench_mark_finance_case_dirty() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_mark_finance_case_dirty() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_mark_finance_case_dirty() TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_session_id uuid := NULL::uuid;
  v_session_id_text text := NULL::text;
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_actor_user_id uuid := p_actor_user_id;
  v_live_change_seq bigint := 0;
  v_payload_source_change_seq bigint := NULL::bigint;
  v_source_change_seq bigint := 0;
  v_job_type text := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_pay_channel_scope text := 'ALL';
  v_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_queue_identity_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_queue_identity_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_source_build_run_id_text text := NULL::text;
  v_source_build_run_id uuid := NULL::uuid;
  v_source_build_hash text := NULL::text;
  v_source_build_seed_text text := NULL::text;
  v_initial_cursor_json jsonb := '{}'::jsonb;
  v_cursor_token text := 'none';
  v_session_signature_token text := 'none';
  v_stage_limit integer := 100;
  v_dedupe_key text;
  v_job_id uuid;
  v_job_status text;
  v_job_was_inserted boolean := false;
  v_insert_row_count integer := 0;
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'WORKBENCH_CANDIDATE_SOURCE_BUILD');
  v_payload_out_json jsonb := '{}'::jsonb;
  v_classifier_result jsonb := '{}'::jsonb;
  v_force_legacy boolean := false;
  v_force_broad_legacy boolean := false;
  v_resolved_job_type text := NULL::text;
  v_projection_run_id uuid := NULL::uuid;
  v_projection_run_id_text text := NULL::text;
  v_projection_mode text := 'LEGACY';
  v_projection_class text := 'UNKNOWN';
  v_resolved_mode text := 'LEGACY';
  v_no_job_reason text := NULL::text;
  v_delta_jobs_superseded integer := 0;
  v_delta_ids_hash text := NULL::text;
  v_delta_coalescing_key text := NULL::text;
  v_delta_coalescing_hash text := NULL::text;
  v_delta_active_running_job_id uuid := NULL::uuid;
  v_existing_delta_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_existing_delta_source_change_seq bigint := 0;
  v_existing_delta_event_count integer := 0;
  v_merged_delta_event_count integer := 1;
  v_existing_delta_projection_run_id_text text := NULL::text;
  v_delta_merge_reused_existing boolean := false;
  v_payload_shadow_compare_required boolean := false;
  v_payload_shadow_compare_enforced boolean := false;
  v_rotation_scope_json jsonb := '{}'::jsonb;
  v_early_preflight_result jsonb := '{}'::jsonb;
  v_early_preflight_action text := 'PROCEED';
  v_early_preflight_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_early_preflight_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_shadow_compare_required boolean := false;
  v_shadow_compare_enforced boolean := false;
  v_bounded_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_scope_change_tx_token uuid := NULL::uuid;
  v_scope_invalidation_result jsonb := '{}'::jsonb;
  v_scope_state_precedes_job boolean := false;
  v_payload_scope_change_generation bigint := NULL::bigint;
  v_finalized_scope_tx_state text := NULL::text;
  v_finalized_scope_tx_generation bigint := NULL::bigint;
  v_live_scope_change_generation bigint := 0;
  v_registry_dirty_generation bigint := NULL::bigint;
  v_registry_source_change_seq_before bigint := 0;
  v_registry_source_change_seq_after bigint := 0;
  v_registry_sequence_synchronised boolean := false;
  v_scope_state_generation_match_count integer := 0;
  v_stale_preinvalidated_absorb_only boolean := false;
  v_requested_source_build_run_id uuid := NULL::uuid;
  v_authority_fingerprint_text text := NULL::text;
  v_authority_fingerprint text := NULL::text;
  v_owner_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_owner_root_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_owner_active_job_id uuid := NULL::uuid;
  v_owner_refresh_scope_kind text := NULL::text;
  v_owner_pay_channel_scope text := NULL::text;
  v_owner_targeted_timesheet_ids_json jsonb := '[]'::jsonb;
  v_owner_linked_timesheet_ids_json jsonb := '[]'::jsonb;
  v_owner_covers_request boolean := false;
  v_owner_resolution text := 'NO_CURRENT_OWNER';
  v_owner_reasons_json jsonb := '[]'::jsonb;
  v_owner_trigger_sources_json jsonb := '[]'::jsonb;
  v_owner_provenance_json jsonb := '{}'::jsonb;
  v_owner_request_count bigint := 0;
  v_owner_scope_status text := NULL::text;
  v_reversion_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_reversion_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_reversion_attestation jsonb := '{}'::jsonb;
  v_reversion_source_count integer := 0;
  v_reversion_state_exact boolean := false;
  v_semantic_ready_publication_enabled boolean := false;
  v_source_publication_identity_enforced boolean := false;
  v_source_publication_baseline_required boolean := false;
  v_required_physical_publication_contract_version smallint := 0;
  v_authority_fingerprint_version smallint := 2;
  v_physical_currentness jsonb := '{}'::jsonb;
  v_candidate_currentness jsonb := '{}'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'snapshot_run_id is required';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  PERFORM 1
  FROM public.banking_pay_snapshot_runs AS snapshot_run
  WHERE snapshot_run.id = p_snapshot_run_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_snapshot_runs row % not found', p_snapshot_run_id;
  END IF;

  PERFORM 1
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidates row % not found', p_candidate_id;
  END IF;

  SELECT COALESCE(
           settings_row.banking_pay_workbench_semantic_ready_publication_v3_enabled,
           false
         ),
         COALESCE(
           settings_row.banking_pay_source_publication_identity_enforce_v1_enabled,
           false
         )
  INTO v_semantic_ready_publication_enabled,
       v_source_publication_identity_enforced
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1;

  v_source_publication_baseline_required :=
    v_source_publication_identity_enforced
    OR lower(BTRIM(COALESCE(
      v_payload_json->>'source_publication_baseline_required',
      v_payload_json#>>'{source_publication,baseline_required}',
      'false'
    ))) IN ('true','t','1','yes','y','on');
  v_required_physical_publication_contract_version :=
    CASE WHEN v_source_publication_baseline_required THEN 1 ELSE 0 END;
  -- Fingerprint V3 distinguishes the semantic publication contract from the
  -- legacy structural owner even while physical-publication enforcement is
  -- still being rolled out.  The physical contract remains an independent
  -- 0/1 capability inside V3, so enabling semantic V3 does not silently make
  -- legacy Drafts fast-reversion eligible.
  v_authority_fingerprint_version := CASE
    WHEN v_semantic_ready_publication_enabled
      OR v_source_publication_baseline_required THEN 3
    ELSE 2
  END;

  -- One candidate may be requested through several independent refresh routes
  -- in the same lifecycle. Elect/reuse its economic owner under the common
  -- candidate serial authority before taking any session or scope row lock.
  -- This is re-entrant for callers which already own the transaction lock.
  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(p_candidate_id),
      24062027
    )
  );

  v_session_id_text := NULLIF(BTRIM(COALESCE(
    v_payload_json->>'session_id',
    v_payload_json->>'source_session_id',
    v_payload_json->>'workbench_session_id',
    v_payload_json#>>'{workbench,session_id}',
    ''
  )), '');

  IF v_session_id_text IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'job_id', NULL::text,
      'job_type', NULL::text,
      'canonical_job_type', NULL::text,
      'snapshot_run_id', p_snapshot_run_id::text,
      'session_id', NULL::text,
      'candidate_id', p_candidate_id::text,
      'source_change_seq', 0,
      'source_build_required', true,
      'line_work_required', true,
      'delta_refresh_required', false,
      'full_snapshot_job', false,
      'no_op', true,
      'deferred', true,
      'requires_workbench_session', true,
      'reason', v_reason,
      'message', 'Banking Pay candidate refresh now requires a row-backed workbench session.'
    );
  END IF;

  IF v_session_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SESSION_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SESSION_ID_INVALID',
              'session_id', v_session_id_text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  v_session_id := v_session_id_text::uuid;

  SELECT COALESCE(array_agg(DISTINCT parsed_target_ids.timesheet_id_value ORDER BY parsed_target_ids.timesheet_id_value), ARRAY[]::uuid[])
  INTO v_early_preflight_targeted_timesheet_ids
  FROM (
    SELECT NULLIF(BTRIM(targeted_values.value), '')::uuid AS timesheet_id_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_payload_json->'targeted_timesheet_ids') = 'array' THEN v_payload_json->'targeted_timesheet_ids'
        WHEN jsonb_typeof(v_payload_json#>'{source_build,targeted_timesheet_ids}') = 'array' THEN v_payload_json#>'{source_build,targeted_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}') = 'array' THEN v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_json->>'targeted_timesheet_ids')
        WHEN jsonb_typeof(v_payload_json#>'{source_build,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{source_build,targeted_timesheet_ids}')
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{preview_decisions_json,targeted_timesheet_ids}')
        ELSE '[]'::jsonb
      END
    ) AS targeted_values(value)
    WHERE NULLIF(BTRIM(targeted_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_target_ids;

  SELECT COALESCE(array_agg(DISTINCT parsed_linked_ids.timesheet_id_value ORDER BY parsed_linked_ids.timesheet_id_value), ARRAY[]::uuid[])
  INTO v_early_preflight_linked_timesheet_ids
  FROM (
    SELECT NULLIF(BTRIM(linked_values.value), '')::uuid AS timesheet_id_value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_payload_json->'linked_timesheet_ids') = 'array' THEN v_payload_json->'linked_timesheet_ids'
        WHEN jsonb_typeof(v_payload_json#>'{source_build,linked_timesheet_ids}') = 'array' THEN v_payload_json#>'{source_build,linked_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}') = 'array' THEN v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_json->>'linked_timesheet_ids')
        WHEN jsonb_typeof(v_payload_json#>'{source_build,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{source_build,linked_timesheet_ids}')
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{preview_decisions_json,linked_timesheet_ids}')
        ELSE '[]'::jsonb
      END
    ) AS linked_values(value)
    WHERE NULLIF(BTRIM(linked_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_linked_ids;

  -- The hotkey is only advisory after the canonical session and scope
  -- authorities have been locked.  It must never create/reuse work for a
  -- candidate which has not first entered the session through the scope owner.
  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id=v_session_id
  FOR UPDATE;
  IF NOT FOUND OR UPPER(BTRIM(COALESCE(v_session_row.status,'')))<>'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not open',v_session_id;
  END IF;
  IF v_session_row.source_snapshot_run_id IS DISTINCT FROM p_snapshot_run_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SNAPSHOT_MISMATCH' USING ERRCODE='P0001';
  END IF;
  PERFORM 1 FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id=v_session_id AND scope_row.candidate_id=p_candidate_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate % is not in workbench session scope %',p_candidate_id,v_session_id;
  END IF;

  IF COALESCE(array_length(v_early_preflight_targeted_timesheet_ids, 1), 0) > 0 THEN
    SELECT public.pay_workbench_authorise_delta_hotkey_preflight(
      p_session_id => v_session_id,
      p_candidate_id => p_candidate_id,
      p_targeted_timesheet_ids => v_early_preflight_targeted_timesheet_ids,
      p_linked_timesheet_ids => v_early_preflight_linked_timesheet_ids,
      p_payload_json => v_payload_json || jsonb_build_object(
        'session_id', v_session_id::text,
        'source_session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'snapshot_run_id', p_snapshot_run_id::text,
        'source_snapshot_run_id', p_snapshot_run_id::text,
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'projection_mode', 'DELTA',
        'projection_class', CASE
          WHEN lower(BTRIM(COALESCE(v_payload_json->>'authorise_boundary_changed','false'))) IN ('true','t','1','yes','y','on')
            OR lower(BTRIM(COALESCE(v_payload_json->>'unauthorise_boundary_changed','false'))) IN ('true','t','1','yes','y','on')
            OR lower(BTRIM(COALESCE(v_payload_json->>'lifecycle_mutation_context',v_payload_json->>'mutation_context',''))) IN ('timesheet_authorise','authorise_timesheet','timesheet_unauthorise','unauthorise_timesheet')
          THEN 'TIMESHEET_LIFECYCLE' ELSE 'NORMAL_TIMESHEET' END,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ),
      p_reason => v_reason,
      p_actor_user_id => v_actor_user_id,
      p_source_change_seq => NULL::bigint
    ) INTO v_early_preflight_result;

    v_early_preflight_action := COALESCE(v_early_preflight_result->>'action', 'PROCEED');

    IF v_early_preflight_action IN ('REUSED_QUEUED_SAME_FAMILY_JOB', 'UPDATED_WAITING_AFTER_RUNNING_JOB') THEN
      RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok', true,
        'job_id', NULLIF(BTRIM(COALESCE(v_early_preflight_result->>'job_id', '')), ''),
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'resolved_mode', 'DELTA',
        'snapshot_run_id', p_snapshot_run_id::text,
        'session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', CASE WHEN COALESCE(v_early_preflight_result->>'latest_source_change_seq', '') ~ '^[0-9]{1,18}$' THEN (v_early_preflight_result->>'latest_source_change_seq')::bigint ELSE NULL::bigint END,
        'dedupe_key', NULLIF(BTRIM(COALESCE(v_early_preflight_result->>'normalised_delta_family_key', '')), ''),
        'reason', v_reason,
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'targeted_timesheet_ids', COALESCE(v_early_preflight_result->'targeted_timesheet_ids', '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_early_preflight_result->'linked_timesheet_ids', '[]'::jsonb),
        'queue_identity_targeted_timesheet_ids', COALESCE(v_early_preflight_result->'queue_identity_targeted_timesheet_ids', v_early_preflight_result->'resolved_family_timesheet_ids', v_early_preflight_result->'targeted_timesheet_ids', '[]'::jsonb),
        'queue_identity_linked_timesheet_ids', COALESCE(v_early_preflight_result->'queue_identity_linked_timesheet_ids', '[]'::jsonb),
        'source_build_required', false,
        'line_work_required', false,
        'line_work_only', false,
        'delta_refresh_required', true,
        'scope_status', 'DELTA_REFRESH_PENDING',
        'projection_mode', 'DELTA',
        'projection_class', 'NORMAL_TIMESHEET',
        'full_snapshot_job', false,
        'reused', true,
        'early_preflight_action', v_early_preflight_action,
        'early_preflight_result', COALESCE(v_early_preflight_result, '{}'::jsonb),
        'early_preflight_returned_before_session_lock', true,
        'session_lock_skipped', true,
        'scope_lock_skipped', true,
        'classifier_work_skipped', true,
        'dirty_marking_skipped', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ));
    END IF;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = v_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', v_session_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not open', v_session_id;
  END IF;

  IF v_session_row.source_snapshot_run_id IS DISTINCT FROM p_snapshot_run_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SNAPSHOT_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SNAPSHOT_MISMATCH',
              'session_id', v_session_id::text,
              'payload_snapshot_run_id', p_snapshot_run_id::text,
              'session_snapshot_run_id', v_session_row.source_snapshot_run_id::text
            )::text;
  END IF;

  PERFORM 1
  FROM public.banking_pay_workbench_session_scope AS session_scope
  WHERE session_scope.session_id = v_session_id
    AND session_scope.candidate_id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidate % is not in workbench session scope %', p_candidate_id, v_session_id;
  END IF;

  v_actor_user_id := COALESCE(v_actor_user_id, v_session_row.actor_user_id);

  SELECT COALESCE(change_counter.seq, 0),
         COALESCE(change_counter.scope_change_generation, 0)
  INTO v_live_change_seq,
       v_live_scope_change_generation
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text
  FOR UPDATE;

  IF COALESCE(v_payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json->>'source_change_seq')::bigint;
  ELSIF COALESCE(v_payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json->>'source_change_sequence')::bigint;
  ELSIF COALESCE(v_payload_json->>'latest_source_change_seq', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json->>'latest_source_change_seq')::bigint;
  ELSIF COALESCE(v_payload_json#>>'{source_build,source_change_seq}', '') ~ '^[0-9]{1,18}$' THEN
    v_payload_source_change_seq := (v_payload_json#>>'{source_build,source_change_seq}')::bigint;
  END IF;

  v_source_change_seq := GREATEST(COALESCE(v_payload_source_change_seq, 0), COALESCE(v_live_change_seq, 0));

  SELECT COALESCE(jsonb_agg(parsed_target_ids.timesheet_id_text ORDER BY parsed_target_ids.timesheet_id_text), '[]'::jsonb)
  INTO v_targeted_timesheet_ids_json
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(targeted_values.value), '') AS timesheet_id_text
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_payload_json->'targeted_timesheet_ids') = 'array' THEN v_payload_json->'targeted_timesheet_ids'
        WHEN jsonb_typeof(v_payload_json#>'{source_build,targeted_timesheet_ids}') = 'array' THEN v_payload_json#>'{source_build,targeted_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}') = 'array' THEN v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_json->>'targeted_timesheet_ids')
        WHEN jsonb_typeof(v_payload_json#>'{source_build,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{source_build,targeted_timesheet_ids}')
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,targeted_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{preview_decisions_json,targeted_timesheet_ids}')
        ELSE '[]'::jsonb
      END
    ) AS targeted_values(value)
    WHERE NULLIF(BTRIM(targeted_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_target_ids;

  SELECT COALESCE(jsonb_agg(parsed_linked_ids.timesheet_id_text ORDER BY parsed_linked_ids.timesheet_id_text), '[]'::jsonb)
  INTO v_linked_timesheet_ids_json
  FROM (
    SELECT DISTINCT NULLIF(BTRIM(linked_values.value), '') AS timesheet_id_text
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(v_payload_json->'linked_timesheet_ids') = 'array' THEN v_payload_json->'linked_timesheet_ids'
        WHEN jsonb_typeof(v_payload_json#>'{source_build,linked_timesheet_ids}') = 'array' THEN v_payload_json#>'{source_build,linked_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}') = 'array' THEN v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}'
        WHEN jsonb_typeof(v_payload_json->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_json->>'linked_timesheet_ids')
        WHEN jsonb_typeof(v_payload_json#>'{source_build,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{source_build,linked_timesheet_ids}')
        WHEN jsonb_typeof(v_payload_json#>'{preview_decisions_json,linked_timesheet_ids}') = 'string' THEN jsonb_build_array(v_payload_json#>>'{preview_decisions_json,linked_timesheet_ids}')
        ELSE '[]'::jsonb
      END
    ) AS linked_values(value)
    WHERE NULLIF(BTRIM(linked_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS parsed_linked_ids;

  v_refresh_scope_kind := NULLIF(UPPER(BTRIM(COALESCE(
    v_payload_json->>'refresh_scope_kind',
    v_payload_json#>>'{source_build,refresh_scope_kind}',
    v_payload_json#>>'{preview_decisions_json,refresh_scope_kind}',
    ''
  ))), '');

  IF v_refresh_scope_kind IS NULL OR v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
    v_refresh_scope_kind := CASE
      WHEN jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
        OR jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) > 0
      THEN 'TARGETED_TIMESHEETS'
      ELSE 'CANDIDATE_FULL_LIVE'
    END;
  END IF;


  v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);
  v_queue_identity_linked_timesheet_ids_json := COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb);

  IF v_refresh_scope_kind = 'TARGETED_TIMESHEETS'
     AND jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) > 0
     AND to_regprocedure('public._pay_workbench_normalise_timesheet_rotation_scope_payload(uuid[],uuid[])') IS NOT NULL THEN
    SELECT public._pay_workbench_normalise_timesheet_rotation_scope_payload(
      COALESCE((
        SELECT array_agg(DISTINCT targeted_value.value::uuid ORDER BY targeted_value.value::uuid)
        FROM jsonb_array_elements_text(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)) AS targeted_value(value)
        WHERE targeted_value.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ), ARRAY[]::uuid[]),
      COALESCE((
        SELECT array_agg(DISTINCT linked_value.value::uuid ORDER BY linked_value.value::uuid)
        FROM jsonb_array_elements_text(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)) AS linked_value(value)
        WHERE linked_value.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ), ARRAY[]::uuid[])
    )
    INTO v_rotation_scope_json;

    v_payload_json := public._pay_workbench_merge_targeted_scope_payload(
      v_payload_json,
      jsonb_build_object(
        'targeted_timesheet_ids_requested', COALESCE(v_rotation_scope_json->'requested_targeted_timesheet_ids', v_targeted_timesheet_ids_json),
        'linked_timesheet_ids_requested', COALESCE(v_rotation_scope_json->'requested_linked_timesheet_ids', v_linked_timesheet_ids_json),
        'requested_timesheet_ids', COALESCE(v_rotation_scope_json->'requested_timesheet_ids', v_targeted_timesheet_ids_json),
        'family_timesheet_ids', COALESCE(v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json),
        'targeted_timesheet_ids', COALESCE(v_rotation_scope_json->'targeted_timesheet_ids', v_targeted_timesheet_ids_json),
        'linked_timesheet_ids', COALESCE(v_rotation_scope_json->'linked_timesheet_ids', v_linked_timesheet_ids_json, '[]'::jsonb),
        'queue_identity_targeted_timesheet_ids', COALESCE(v_rotation_scope_json->'queue_identity_targeted_timesheet_ids', v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json),
        'queue_identity_linked_timesheet_ids', COALESCE(v_rotation_scope_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb),
        'queue_identity_timesheet_ids', COALESCE(v_rotation_scope_json->'queue_identity_timesheet_ids', v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json),
        'rotation_family_scope_for_queue_identity', true,
        'queue_identity_preserves_targeted_linked_semantics', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      )
    );
    v_targeted_timesheet_ids_json := COALESCE(v_rotation_scope_json->'targeted_timesheet_ids', v_targeted_timesheet_ids_json);
    v_linked_timesheet_ids_json := COALESCE(v_rotation_scope_json->'linked_timesheet_ids', v_linked_timesheet_ids_json, '[]'::jsonb);
    v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_rotation_scope_json->'queue_identity_targeted_timesheet_ids', v_rotation_scope_json->'family_timesheet_ids', v_targeted_timesheet_ids_json, '[]'::jsonb);
    v_queue_identity_linked_timesheet_ids_json := COALESCE(v_rotation_scope_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb);
  END IF;

  v_pay_channel_scope := NULLIF(UPPER(BTRIM(COALESCE(
    v_payload_json->>'pay_channel_scope',
    v_payload_json#>>'{source_build,pay_channel_scope}',
    v_payload_json#>>'{preview_decisions_json,pay_channel_scope}',
    v_session_row.filters_json->>'pay_channel_scope',
    v_session_row.filters_json#>>'{filters,pay_channel_scope}',
    'ALL'
  ))), '');

  IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
    v_pay_channel_scope := 'ALL';
  END IF;

  v_force_legacy := lower(BTRIM(COALESCE(v_payload_json->>'force_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_force_broad_legacy := lower(BTRIM(COALESCE(v_payload_json->>'force_broad_legacy', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF jsonb_typeof(v_payload_json->'classifier_result') = 'object' THEN
    v_classifier_result := v_payload_json->'classifier_result';
  ELSIF v_payload_json ? 'fast_path_allowed' OR v_payload_json ? 'resolved_job_type' THEN
    v_classifier_result := v_payload_json;
  ELSIF to_regprocedure('public.pay_workbench_delta_refresh_classify_v1(uuid,uuid,jsonb)') IS NOT NULL THEN
    v_classifier_result := public.pay_workbench_delta_refresh_classify_v1(
      v_session_id,
      p_candidate_id,
      v_payload_json
      || jsonb_build_object(
        'session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'pay_channel_scope', v_pay_channel_scope,
        'force_legacy', v_force_legacy,
        'force_broad_legacy', v_force_broad_legacy
      )
    );
  ELSE
    v_classifier_result := jsonb_build_object(
      'ok', true,
      'fast_path_allowed', false,
      'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'projection_mode', 'LEGACY',
      'projection_class', COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_payload_json->>'projection_class', ''))), ''), 'UNKNOWN_TRIGGER'),
      'scope_status', 'SOURCE_BUILD_PENDING',
      'fallback_required', true,
      'fallback_reason', 'DELTA_CLASSIFIER_NOT_INSTALLED'
    );
  END IF;

  v_classifier_result := COALESCE(v_classifier_result, '{}'::jsonb)
    - 'old_row_json'
    - 'new_row_json'
    - 'old_row'
    - 'new_row'
    - 'source_row_json'
    - 'work_payload_json'
    - 'result_row_json'
    - 'preview_row_json'
    - 'row_payload_json'
    - 'line_payload_json'
    - 'projection_rows'
    - 'projected_rows';

  v_resolved_mode := UPPER(BTRIM(COALESCE(v_classifier_result->>'resolved_mode', v_classifier_result->>'routing_decision', v_classifier_result->>'projection_mode', 'LEGACY')));
  IF v_resolved_mode NOT IN ('DELTA', 'PATCH_ONLY', 'CLONE_REBASE', 'LEGACY', 'BLOCKED') THEN
    v_resolved_mode := 'LEGACY';
  END IF;

  v_resolved_job_type := UPPER(BTRIM(COALESCE(v_classifier_result->>'resolved_job_type', CASE WHEN v_resolved_mode = 'DELTA' THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH' WHEN v_resolved_mode = 'LEGACY' THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD' ELSE 'NOOP' END)));
  v_projection_mode := UPPER(BTRIM(COALESCE(v_classifier_result->>'projection_mode', v_payload_json->>'projection_mode', 'LEGACY')));
  v_projection_class := UPPER(BTRIM(COALESCE(v_classifier_result->>'projection_class', v_payload_json->>'projection_class', 'UNKNOWN')));

  IF jsonb_typeof(v_classifier_result->'targeted_timesheet_ids') = 'array' THEN
    v_targeted_timesheet_ids_json := v_classifier_result->'targeted_timesheet_ids';
  END IF;
  IF jsonb_typeof(v_classifier_result->'linked_timesheet_ids') = 'array' THEN
    v_linked_timesheet_ids_json := v_classifier_result->'linked_timesheet_ids';
  END IF;

  IF jsonb_typeof(v_payload_json->'queue_identity_targeted_timesheet_ids') = 'array' THEN
    v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_payload_json->'queue_identity_targeted_timesheet_ids', v_queue_identity_targeted_timesheet_ids_json, '[]'::jsonb);
  ELSIF jsonb_array_length(COALESCE(v_queue_identity_targeted_timesheet_ids_json, '[]'::jsonb)) = 0 THEN
    v_queue_identity_targeted_timesheet_ids_json := COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb);
  END IF;

  IF jsonb_typeof(v_payload_json->'queue_identity_linked_timesheet_ids') = 'array' THEN
    v_queue_identity_linked_timesheet_ids_json := COALESCE(v_payload_json->'queue_identity_linked_timesheet_ids', '[]'::jsonb);
  ELSIF jsonb_array_length(COALESCE(v_queue_identity_linked_timesheet_ids_json, '[]'::jsonb)) = 0 THEN
    v_queue_identity_linked_timesheet_ids_json := COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb);
  END IF;

  IF v_force_legacy IS TRUE OR v_force_broad_legacy IS TRUE THEN
    v_resolved_mode := 'LEGACY';
    v_resolved_job_type := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
  INTO v_bounded_timesheet_ids
  FROM jsonb_array_elements_text(
    COALESCE(v_targeted_timesheet_ids_json,'[]'::jsonb)
    || COALESCE(v_linked_timesheet_ids_json,'[]'::jsonb)
  ) value
  WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  IF COALESCE(v_payload_json->>'scope_change_tx_token','')
     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_scope_change_tx_token:=(v_payload_json->>'scope_change_tx_token')::uuid;
  END IF;
  v_scope_state_precedes_job := lower(BTRIM(COALESCE(
    v_payload_json->>'bounded_scope_state_precedes_job','false'
  ))) IN ('true','t','1','yes','y','on');
  IF COALESCE(v_payload_json->>'scope_change_generation','') ~ '^\d+$' THEN
    v_payload_scope_change_generation :=
      (v_payload_json->>'scope_change_generation')::bigint;
  END IF;

  -- Elect an already-current economic owner before scope invalidation.  A
  -- reason such as USER_REQUESTED_FULL_REFRESH or force_legacy is provenance,
  -- not proof that current certified output is stale.  This early fence is
  -- what prevents a same-sequence refresh from first destroying the evidence
  -- needed by COMPLETE_CURRENT_AUTHORITY.
  IF NOT v_scope_state_precedes_job THEN
    SELECT registry.dirty_generation,
           registry.current_source_change_seq
    INTO v_registry_dirty_generation,
         v_registry_source_change_seq_before
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id=p_candidate_id
    FOR UPDATE;
  END IF;

  v_session_signature_token:=md5(COALESCE(v_session_row.session_signature,''));

  v_authority_fingerprint_text := concat_ws('|',
    CASE WHEN v_authority_fingerprint_version=3
      THEN 'WORKBENCH_SOURCE_OWNER_V3' ELSE 'WORKBENCH_SOURCE_OWNER_V2' END,
    v_session_id::text,
    COALESCE(v_session_row.version,0)::text,
    v_session_row.source_snapshot_run_id::text,
    v_session_signature_token,
    p_candidate_id::text,
    COALESCE(v_source_change_seq,0)::text,
    COALESCE(v_registry_dirty_generation,v_live_scope_change_generation,0)::text,
    UPPER(BTRIM(COALESCE(v_pay_channel_scope,'ALL'))),
    'FULL_CANDIDATE',
    CASE WHEN v_authority_fingerprint_version=3
      THEN 'READY_TO_PAY_SEMANTIC_V2' ELSE NULL END,
    CASE WHEN v_authority_fingerprint_version=3
      THEN v_required_physical_publication_contract_version::text ELSE NULL END
  );
  v_authority_fingerprint := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_authority_fingerprint_text,'UTF8'),'sha256'),
    'hex'
  );

  SELECT build_row.*
  INTO v_owner_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  JOIN public.banking_pay_workbench_session_scope AS current_scope
    ON current_scope.session_id=build_row.session_id
   AND current_scope.candidate_id=build_row.candidate_id
  JOIN public.banking_pay_workbench_session_candidate_state AS current_state
    ON current_state.session_id=build_row.session_id
   AND current_state.candidate_id=build_row.candidate_id
  WHERE build_row.candidate_id=p_candidate_id
    AND build_row.session_id=v_session_id
    AND build_row.session_version=COALESCE(v_session_row.version,0)
    AND build_row.source_snapshot_run_id=v_session_row.source_snapshot_run_id
    AND build_row.source_change_seq=COALESCE(v_source_change_seq,0)
    AND build_row.captured_candidate_generation=
          COALESCE(v_registry_dirty_generation,v_live_scope_change_generation,0)
    AND UPPER(BTRIM(COALESCE(build_row.status,'')))='COMPLETE'
    AND UPPER(BTRIM(COALESCE(build_row.private_stage,'')))='COMPLETE'
    AND current_scope.dirty IS FALSE
    AND current_scope.pending_job_id IS NULL
    AND current_scope.certified_preview_publication_required IS TRUE
    AND current_scope.certified_preview_publication_parity_ok IS TRUE
    AND current_scope.certified_preview_publication_session_version=build_row.session_version
    AND current_scope.certified_preview_publication_source_change_seq=build_row.source_change_seq
    AND current_scope.certified_preview_publication_source_build_run_id=build_row.source_build_run_id
    AND (
      NOT v_source_publication_baseline_required
      OR (
        current_scope.certified_preview_publication_source_publication_id IS NOT NULL
        AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
              =current_scope.certified_preview_publication_source_publication_id::text
      )
    )
    AND (
      NOT v_semantic_ready_publication_enabled
      OR (
        current_scope.certified_preview_publication_attestation_json->>'attestation_version'
              ='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
        AND current_scope.certified_preview_publication_attestation_json->>'contract_version'='3'
        AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
              ='READY_TO_PAY_SEMANTIC_V2'
        AND COALESCE(
          (current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,
          false
        )
        AND COALESCE(
          (current_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean,
          false
        )
      )
    )
    AND UPPER(BTRIM(COALESCE(current_state.status,'')))='READY'
    AND current_state.pending_job_id IS NULL
    AND current_state.session_version=build_row.session_version
    AND current_state.source_change_seq=build_row.source_change_seq
  ORDER BY build_row.completed_at_utc DESC NULLS LAST,build_row.id DESC
  LIMIT 1
  FOR UPDATE OF build_row,current_scope,current_state;

  IF FOUND THEN
    SELECT source_job.*
    INTO v_owner_root_job
    FROM public.banking_pay_workbench_jobs AS source_job
    WHERE source_job.economic_build_id=v_owner_build.id
      AND UPPER(BTRIM(COALESCE(source_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND COALESCE(
        source_job.payload_json->>'source_build_run_id',
        source_job.payload_json#>>'{source_build,source_build_run_id}',
        ''
      )=v_owner_build.source_build_run_id::text
    ORDER BY source_job.created_at_utc,source_job.id
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      v_owner_pay_channel_scope:=UPPER(BTRIM(COALESCE(
        v_owner_root_job.payload_json->>'pay_channel_scope',
        v_owner_root_job.payload_json#>>'{source_build,pay_channel_scope}',
        'ALL'
      )));
      IF v_owner_pay_channel_scope=UPPER(BTRIM(COALESCE(v_pay_channel_scope,'ALL'))) THEN
        EXECUTE 'SELECT private.pay_workbench_candidate_physical_currentness_page_v1($1,$2,$3,$4)'
          INTO v_physical_currentness
          USING v_session_id,ARRAY[p_candidate_id],'TERMINAL_CURRENT',
            jsonb_build_object('contract_version',1,'allow_active_owner',false);
        v_candidate_currentness:=COALESCE(v_physical_currentness->'candidate_results'->0,'{}'::jsonb);
        IF COALESCE((v_candidate_currentness->>'terminal_current')::boolean,false) IS NOT TRUE THEN
          v_owner_build:=NULL;
          v_owner_root_job:=NULL;
        ELSE
        UPDATE public.banking_pay_workbench_jobs AS owner_job
        SET payload_json=jsonb_strip_nulls(
              COALESCE(owner_job.payload_json,'{}'::jsonb)
              || jsonb_build_object('reason_latest',v_reason)
            ),
            updated_at_utc=v_now
        WHERE owner_job.id=v_owner_root_job.id;

        RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok',true,
        'job_id',v_owner_root_job.id::text,
        'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'session_id',v_session_id::text,
        'candidate_id',p_candidate_id::text,
        'session_version',COALESCE(v_session_row.version,0),
        'source_change_seq',COALESCE(v_source_change_seq,0),
        'registry_source_change_seq',COALESCE(v_registry_source_change_seq_before,v_source_change_seq,0),
        'source_build_run_id',v_owner_build.source_build_run_id::text,
        'source_publication_id',(
          SELECT current_scope.certified_preview_publication_source_publication_id::text
          FROM public.banking_pay_workbench_session_scope AS current_scope
          WHERE current_scope.session_id=v_session_id
            AND current_scope.candidate_id=p_candidate_id
        ),
        'authority_fingerprint',COALESCE(v_owner_build.authority_fingerprint,v_authority_fingerprint),
        'authority_fingerprint_version',COALESCE(v_owner_build.authority_fingerprint_version,v_authority_fingerprint_version),
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'owner_resolution','COMPLETE_CURRENT_AUTHORITY',
        'owner_build_id',v_owner_build.id::text,
        'owner_root_job_id',v_owner_root_job.id::text,
        'owner_source_build_run_id',v_owner_build.source_build_run_id::text,
        'requested_coverage','FULL_CANDIDATE',
        'owner_coverage','FULL_CANDIDATE',
        'scope_status','MATERIALISED',
        'source_build_required',false,
        'delta_refresh_required',false,
        'coalesced',true,
        'reused',true,
        'new_owner_created',false,
        'no_op',true,
        'pre_invalidation_owner_election',true,
        'diagnostic_provenance_merged',true,
        'reason',v_reason,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
        ));
        END IF;
      END IF;
    END IF;
  END IF;

  v_owner_build:=NULL;
  v_owner_root_job:=NULL;
  v_owner_active_job_id:=NULL::uuid;
  SELECT build_row.*
  INTO v_owner_build
  FROM private.banking_pay_workbench_economic_builds AS build_row
  WHERE build_row.candidate_id=p_candidate_id
    AND build_row.session_id=v_session_id
    AND build_row.session_version=COALESCE(v_session_row.version,0)
    AND build_row.source_snapshot_run_id=v_session_row.source_snapshot_run_id
    AND build_row.source_change_seq=COALESCE(v_source_change_seq,0)
    AND build_row.captured_candidate_generation=
          COALESCE(v_registry_dirty_generation,v_live_scope_change_generation,0)
    AND build_row.authority_fingerprint_version=v_authority_fingerprint_version
    AND build_row.authority_fingerprint=v_authority_fingerprint
    AND UPPER(BTRIM(COALESCE(build_row.status,''))) IN (
      'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
      'PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
    )
    AND EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.economic_build_id=build_row.id
        AND active_job.status IN ('QUEUED','RUNNING')
    )
  ORDER BY build_row.created_at_utc DESC,build_row.id DESC
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    SELECT source_job.*
    INTO v_owner_root_job
    FROM public.banking_pay_workbench_jobs AS source_job
    WHERE source_job.economic_build_id=v_owner_build.id
      AND UPPER(BTRIM(COALESCE(source_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
    ORDER BY source_job.created_at_utc,source_job.id
    LIMIT 1
    FOR UPDATE;
    SELECT active_job.id
    INTO v_owner_active_job_id
    FROM public.banking_pay_workbench_jobs AS active_job
    WHERE active_job.economic_build_id=v_owner_build.id
      AND active_job.status IN ('QUEUED','RUNNING')
    ORDER BY CASE WHEN active_job.status='RUNNING' THEN 0 ELSE 1 END,
             active_job.run_at_utc,active_job.created_at_utc,active_job.id
    LIMIT 1;

    IF v_owner_root_job.id IS NOT NULL AND v_owner_active_job_id IS NOT NULL THEN
      RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok',true,
        'job_id',v_owner_root_job.id::text,
        'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'session_id',v_session_id::text,
        'candidate_id',p_candidate_id::text,
        'session_version',COALESCE(v_session_row.version,0),
        'source_change_seq',COALESCE(v_source_change_seq,0),
        'source_build_run_id',v_owner_build.source_build_run_id::text,
        'authority_fingerprint',v_authority_fingerprint,
        'authority_fingerprint_version',v_authority_fingerprint_version,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'owner_resolution','ACTIVE_CURRENT_OWNER_COVERS_REQUEST',
        'owner_build_id',v_owner_build.id::text,
        'owner_root_job_id',v_owner_root_job.id::text,
        'owner_active_job_id',v_owner_active_job_id::text,
        'owner_source_build_run_id',v_owner_build.source_build_run_id::text,
        'requested_coverage','FULL_CANDIDATE',
        'owner_coverage','FULL_CANDIDATE',
        'scope_status','SOURCE_BUILD_PENDING',
        'source_build_required',true,
        'delta_refresh_required',false,
        'coalesced',true,
        'reused',true,
        'new_owner_created',false,
        'no_op',false,
        'pre_invalidation_owner_election',true,
        'reason',v_reason,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
      ));
    END IF;
  END IF;

  v_owner_build:=NULL;
  v_owner_root_job:=NULL;
  v_owner_active_job_id:=NULL::uuid;

  IF v_scope_state_precedes_job THEN
    SELECT registry.dirty_generation,
           registry.current_source_change_seq
    INTO v_registry_dirty_generation,
         v_registry_source_change_seq_before
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id=p_candidate_id
    FOR UPDATE;

    -- A delayed pre-invalidated job may be older than a newer authority which
    -- has already adopted the live sequence and generation.  Such a request
    -- may only be absorbed by an active/current owner below; it must never
    -- create work from its stale transaction proof.
    v_stale_preinvalidated_absorb_only :=
      v_payload_scope_change_generation IS NOT NULL
      AND v_payload_scope_change_generation < COALESCE(v_live_scope_change_generation,0)
      AND COALESCE(v_registry_dirty_generation,0)=COALESCE(v_live_scope_change_generation,0)
      AND COALESCE(v_registry_source_change_seq_before,0)=COALESCE(v_source_change_seq,0);

    IF v_stale_preinvalidated_absorb_only THEN
      v_registry_source_change_seq_after:=v_registry_source_change_seq_before;
      v_scope_invalidation_result:=jsonb_build_object(
        'ok',true,
        'stale_preinvalidated_absorb_only',true,
        'payload_scope_change_generation',v_payload_scope_change_generation,
        'current_scope_change_generation',v_live_scope_change_generation,
        'accepted_source_change_seq',v_source_change_seq,
        'reason',COALESCE(v_reason,'CANDIDATE_REFRESH_ENQUEUED')
      );
    ELSE
      SELECT scope_tx.state,
             scope_tx.allocated_generation
      INTO v_finalized_scope_tx_state,
           v_finalized_scope_tx_generation
      FROM public.banking_pay_scope_change_transactions AS scope_tx
      WHERE scope_tx.tx_token=v_scope_change_tx_token
      FOR UPDATE;

      SELECT count(*)::integer
      INTO v_scope_state_generation_match_count
      FROM unnest(v_bounded_timesheet_ids) AS requested(timesheet_id)
       JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
        ON scope_state.timesheet_id=requested.timesheet_id
       AND scope_state.candidate_id=p_candidate_id
       AND scope_state.dirty_generation=v_payload_scope_change_generation;

      IF v_scope_change_tx_token IS NULL
         OR v_payload_scope_change_generation IS NULL
         OR v_payload_scope_change_generation < 1
         OR v_finalized_scope_tx_state IS DISTINCT FROM 'FINALIZED'
         OR v_finalized_scope_tx_generation IS DISTINCT FROM
            v_payload_scope_change_generation
         OR v_live_scope_change_generation IS DISTINCT FROM
            v_payload_scope_change_generation
         OR COALESCE(v_registry_dirty_generation,0) IS DISTINCT FROM
            v_payload_scope_change_generation
         OR v_scope_state_generation_match_count IS DISTINCT FROM
            cardinality(v_bounded_timesheet_ids) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED'
          USING ERRCODE='22023', DETAIL=jsonb_build_object(
            'code','PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED',
            'candidate_id',p_candidate_id,
            'scope_change_tx_token',v_scope_change_tx_token,
            'payload_scope_change_generation',v_payload_scope_change_generation,
            'transaction_state',v_finalized_scope_tx_state,
            'transaction_generation',v_finalized_scope_tx_generation,
            'live_scope_change_generation',v_live_scope_change_generation,
            'registry_dirty_generation',v_registry_dirty_generation,
            'requested_timesheet_count',cardinality(v_bounded_timesheet_ids),
            'matched_scope_state_count',v_scope_state_generation_match_count
          )::text;
      END IF;

      UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry
      SET current_source_change_seq=GREATEST(
            COALESCE(registry.current_source_change_seq,0),
            COALESCE(v_source_change_seq,0)
          ),
          updated_at_utc=CASE
            WHEN COALESCE(registry.current_source_change_seq,0)<COALESCE(v_source_change_seq,0)
              THEN clock_timestamp()
            ELSE registry.updated_at_utc
          END
      WHERE registry.candidate_id=p_candidate_id
        AND registry.dirty_generation=v_payload_scope_change_generation
      RETURNING registry.current_source_change_seq
      INTO v_registry_source_change_seq_after;

      IF NOT FOUND
         OR COALESCE(v_registry_source_change_seq_after,0)<COALESCE(v_source_change_seq,0) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED'
          USING ERRCODE='40001', DETAIL=jsonb_build_object(
            'code','PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED',
            'candidate_id',p_candidate_id,
            'accepted_source_change_seq',v_source_change_seq,
            'registry_source_change_seq_before',v_registry_source_change_seq_before,
            'registry_source_change_seq_after',v_registry_source_change_seq_after,
            'scope_change_generation',v_payload_scope_change_generation
          )::text;
      END IF;
      v_registry_sequence_synchronised :=
        COALESCE(v_registry_source_change_seq_after,0)>
        COALESCE(v_registry_source_change_seq_before,0);

      v_scope_invalidation_result := jsonb_build_object(
        'ok',true,
        'already_finalized',true,
        'scope_change_tx_token',v_scope_change_tx_token,
        'scope_change_generation',v_finalized_scope_tx_generation,
        'candidate_count',1,
        'timesheet_count',cardinality(v_bounded_timesheet_ids),
        'accepted_source_change_seq',v_source_change_seq,
        'registry_source_change_seq_before',v_registry_source_change_seq_before,
        'registry_source_change_seq_after',v_registry_source_change_seq_after,
        'registry_sequence_synchronised',v_registry_sequence_synchronised,
        'reason',COALESCE(v_reason,'CANDIDATE_REFRESH_ENQUEUED')
      );
    END IF;
  ELSE
    v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
      CASE WHEN cardinality(v_bounded_timesheet_ids)=0 THEN ARRAY[p_candidate_id]
        ELSE array_fill(p_candidate_id,ARRAY[cardinality(v_bounded_timesheet_ids)]) END,
      CASE WHEN cardinality(v_bounded_timesheet_ids)=0 THEN ARRAY[NULL::uuid]
        ELSE v_bounded_timesheet_ids END,
      COALESCE(v_reason,'CANDIDATE_REFRESH_ENQUEUED'),
      v_scope_change_tx_token,
      v_payload_json||jsonb_build_object(
        'skip_candidate_job_enqueue',true,
        'source_change_seq',v_source_change_seq,
        'source_change_sequence',v_source_change_seq,
        'latest_source_change_seq',v_source_change_seq
      )
    );

    SELECT registry.dirty_generation,
           registry.current_source_change_seq
    INTO v_registry_dirty_generation,
         v_registry_source_change_seq_after
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id=p_candidate_id
    FOR UPDATE;

    IF NOT FOUND
       OR COALESCE(v_registry_source_change_seq_after,0)<COALESCE(v_source_change_seq,0) THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED'
        USING ERRCODE='40001', DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_ACCEPTED_SOURCE_SEQUENCE_NOT_SYNCHRONISED',
          'candidate_id',p_candidate_id,
          'accepted_source_change_seq',v_source_change_seq,
          'registry_source_change_seq_after',v_registry_source_change_seq_after,
          'registry_dirty_generation',v_registry_dirty_generation
        )::text;
    END IF;
    v_registry_sequence_synchronised := true;
  END IF;

  v_payload_shadow_compare_required := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_compare_required',
    v_payload_json->>'shadow_compare',
    v_payload_json->>'shadow_mode',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_payload_shadow_compare_enforced := lower(BTRIM(COALESCE(
    v_payload_json->>'shadow_compare_enforced',
    v_payload_json->>'enforce_shadow_compare',
    v_payload_json->>'shadow_enforced',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  v_shadow_compare_required := (
    lower(BTRIM(COALESCE(v_classifier_result#>>'{complexity_flags,delta_shadow_mode}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_classifier_result#>>'{complexity_flags,payload_shadow_mode}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_classifier_result->>'shadow_compare_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_payload_shadow_compare_required
  );

  v_shadow_compare_enforced := COALESCE(v_shadow_compare_required, false) AND (
    COALESCE(v_payload_shadow_compare_enforced, false)
    OR lower(BTRIM(COALESCE(v_classifier_result->>'shadow_compare_enforced', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
  );

  IF v_resolved_mode IN ('PATCH_ONLY', 'CLONE_REBASE', 'BLOCKED') THEN
    v_no_job_reason := CASE
      WHEN v_resolved_mode = 'PATCH_ONLY' THEN COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'PATCH_ONLY_NO_SOURCE_BUILD_JOB')
      WHEN v_resolved_mode = 'CLONE_REBASE' THEN COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'CLONE_REBASE_NOT_A_CANDIDATE_SOURCE_BUILD_JOB')
      ELSE COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'CLASSIFIER_BLOCKED_REFRESH')
    END;

    UPDATE public.banking_pay_workbench_session_scope AS no_job_scope
    SET status = CASE
          WHEN v_resolved_mode = 'PATCH_ONLY' THEN COALESCE(NULLIF(v_classifier_result->>'scope_status', ''), 'PATCH_ONLY_PENDING')
          WHEN v_resolved_mode = 'CLONE_REBASE' THEN COALESCE(NULLIF(v_classifier_result->>'scope_status', ''), 'CLONE_REBASE_PENDING')
          ELSE COALESCE(NULLIF(v_classifier_result->>'scope_status', ''), no_job_scope.status)
        END,
        pending_job_id = NULL,
        dirty = false,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    WHERE no_job_scope.session_id = v_session_id
      AND no_job_scope.candidate_id = p_candidate_id
      AND v_resolved_mode IN ('PATCH_ONLY', 'CLONE_REBASE');

    RETURN jsonb_strip_nulls(jsonb_build_object(
      'ok', true,
      'job_id', NULL::text,
      'job_type', NULL::text,
      'canonical_job_type', NULL::text,
      'snapshot_run_id', p_snapshot_run_id::text,
      'session_id', v_session_id::text,
      'candidate_id', p_candidate_id::text,
      'session_version', COALESCE(v_session_row.version, 0),
      'source_change_seq', COALESCE(v_source_change_seq, 0),
      'reason', v_reason,
      'resolved_mode', v_resolved_mode,
      'projection_mode', v_projection_mode,
      'projection_class', v_projection_class,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
      'pay_channel_scope', v_pay_channel_scope,
      'source_build_required', false,
      'line_work_required', false,
      'line_work_only', false,
      'delta_refresh_required', false,
      'patch_only', v_resolved_mode = 'PATCH_ONLY',
      'clone_rebase_required', v_resolved_mode = 'CLONE_REBASE',
      'blocked', v_resolved_mode = 'BLOCKED',
      'no_op', v_resolved_mode = 'BLOCKED',
      'deferred', v_resolved_mode = 'CLONE_REBASE',
      'fallback_required', lower(BTRIM(COALESCE(v_classifier_result->>'fallback_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'fallback_reason', v_no_job_reason,
      'classifier_result', v_classifier_result
    ));
  END IF;

  IF v_resolved_mode = 'DELTA'
     AND v_resolved_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
     AND lower(BTRIM(COALESCE(v_classifier_result->>'fast_path_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
     AND v_force_legacy IS NOT TRUE
     AND v_force_broad_legacy IS NOT TRUE THEN
    v_job_type := 'WORKBENCH_CANDIDATE_DELTA_REFRESH';
  ELSE
    v_resolved_mode := 'LEGACY';
    v_job_type := 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
  END IF;

  IF COALESCE(v_payload_json->>'source_build_limit', '') ~ '^[0-9]{1,9}$' THEN
    v_stage_limit := LEAST(GREATEST((v_payload_json->>'source_build_limit')::integer, 1), 100);
  ELSIF COALESCE(v_payload_json#>>'{stage_limits,source_build}', '') ~ '^[0-9]{1,9}$' THEN
    v_stage_limit := LEAST(GREATEST((v_payload_json#>>'{stage_limits,source_build}')::integer, 1), 100);
  ELSIF COALESCE(v_payload_json->>'limit', '') ~ '^[0-9]{1,9}$' THEN
    v_stage_limit := LEAST(GREATEST((v_payload_json->>'limit')::integer, 1), 100);
  ELSE
    SELECT LEAST(GREATEST(COALESCE(
      CASE
        WHEN COALESCE(to_jsonb(settings_row)->>'banking_pay_workbench_source_build_units_per_job', '') ~ '^[0-9]{1,9}$'
          THEN (to_jsonb(settings_row)->>'banking_pay_workbench_source_build_units_per_job')::integer
        ELSE NULL::integer
      END,
      CASE
        WHEN COALESCE(to_jsonb(settings_row)->>'banking_pay_workbench_stage_work_units_per_job', '') ~ '^[0-9]{1,9}$'
          THEN (to_jsonb(settings_row)->>'banking_pay_workbench_stage_work_units_per_job')::integer
        ELSE NULL::integer
      END,
      25
    ), 1), 100)
    INTO v_stage_limit
    FROM public.settings_defaults AS settings_row
    WHERE settings_row.id = 1
    LIMIT 1;

    v_stage_limit := COALESCE(v_stage_limit, 25);
  END IF;

  v_initial_cursor_json := CASE
    WHEN jsonb_typeof(v_payload_json->'cursor_json') = 'object' THEN v_payload_json->'cursor_json'
    WHEN jsonb_typeof(v_payload_json->'cursor') = 'object' THEN v_payload_json->'cursor'
    WHEN jsonb_typeof(v_payload_json->'source_cursor') = 'object' THEN v_payload_json->'source_cursor'
    WHEN jsonb_typeof(v_payload_json#>'{source_build,cursor}') = 'object' THEN v_payload_json#>'{source_build,cursor}'
    ELSE '{}'::jsonb
  END;

  v_cursor_token := md5(COALESCE(v_initial_cursor_json, '{}'::jsonb)::text);
  v_session_signature_token := md5(COALESCE(v_session_row.session_signature, ''));

  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    v_projection_run_id_text := NULLIF(BTRIM(COALESCE(v_payload_json->>'projection_run_id', v_classifier_result->>'projection_run_id', '')), '');
    IF v_projection_run_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_projection_run_id := v_projection_run_id_text::uuid;
    ELSE
      v_projection_run_id := gen_random_uuid();
    END IF;

    v_delta_ids_hash := md5(
      COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb)::text
      || ':'
      || COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb)::text
    );

    v_delta_coalescing_key := 'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
    || ':session:' || COALESCE(v_session_id::text, 'none')
    || ':version:' || COALESCE(COALESCE(v_session_row.version, 0), 0)::text
    || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''), 'DELTA'))), ''), 'DELTA')
    || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(v_projection_class, ''), 'UNKNOWN'))), ''), 'UNKNOWN')
    || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''), 'TARGETED_TIMESHEETS'))), ''), 'TARGETED_TIMESHEETS')
    || ':candidate:' || COALESCE(p_candidate_id::text, 'none')
    || ':timesheets:' || COALESCE(v_delta_ids_hash, md5('[]:[]'));
    v_delta_coalescing_hash := md5(v_delta_coalescing_key);

    SELECT running_delta_job.id
    INTO v_delta_active_running_job_id
    FROM public.banking_pay_workbench_jobs AS running_delta_job
    WHERE running_delta_job.session_id = v_session_id
      AND running_delta_job.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
      AND UPPER(BTRIM(COALESCE(running_delta_job.status, ''))) = 'RUNNING'
      AND (
        COALESCE(running_delta_job.payload_json->>'normalised_delta_family_key', running_delta_job.payload_json->>'delta_family_key', running_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
        OR (
            SELECT
              'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
              || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.session_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                ''
              )), ''), 'none')
              || ':version:' || COALESCE(
                CASE
                  WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                    THEN delta_family_values.session_version_text::bigint
                  ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                END,
                0
              )::text
              || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_mode_text, ''),
                NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                'DELTA'
              ))), ''), 'DELTA')
              || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_class_text, ''),
                NULLIF(v_projection_class, ''),
                'UNKNOWN'
              ))), ''), 'UNKNOWN')
              || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                CASE
                  WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                    OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                    THEN 'TARGETED_TIMESHEETS'
                  ELSE 'CANDIDATE_FULL_LIVE'
                END
              ))), ''), 'CANDIDATE_FULL_LIVE')
              || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.candidate_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                ''
              )), ''), 'none')
              || ':timesheets:' || md5(
                COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                || ':'
                || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
              )
            FROM (
              SELECT
                NULLIF(BTRIM(COALESCE(
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                  CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                  '0'
                )), '') AS session_version_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))) AS projection_mode_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))) AS projection_class_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                ))) AS refresh_scope_kind_text
            ) AS delta_family_values
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                    WHEN NULLIF(BTRIM(COALESCE(
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                           ''
                         )), '') IS NOT NULL
                      THEN jsonb_build_array(COALESCE(
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                      ))
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_targeted_raw(value)
                WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_targeted_sorted
            ) AS delta_family_targeted
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_linked_raw(value)
                WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_linked_sorted
            ) AS delta_family_linked
          ) = v_delta_coalescing_key
      )
    ORDER BY running_delta_job.started_at_utc ASC NULLS LAST, running_delta_job.created_at_utc ASC, running_delta_job.id ASC
    LIMIT 1;

    v_dedupe_key := CASE
      WHEN v_delta_active_running_job_id IS NOT NULL THEN
        v_delta_coalescing_key || ':waiting_after_running:' || v_delta_active_running_job_id::text
      ELSE
        v_delta_coalescing_key
    END;

    v_payload_out_json := jsonb_strip_nulls(
      v_payload_json
      || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'resolved_mode', 'DELTA',
        'projection_run_id', v_projection_run_id::text,
        'projection_mode', COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'),
        'projection_class', v_projection_class,
        'phase', 'INIT_PREFLIGHT',
        'cursor_json', '{}'::jsonb,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'queue_identity_targeted_timesheet_ids', COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb),
        'queue_identity_linked_timesheet_ids', COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb),
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'source_change_sequence', COALESCE(v_source_change_seq, 0),
        'latest_source_change_seq', COALESCE(v_source_change_seq, 0),
        'delta_coalescing_key', v_delta_coalescing_key,
        'delta_family_key', v_delta_coalescing_key,
        'normalised_delta_family_key', v_delta_coalescing_key,
        'delta_coalescing_hash', v_delta_coalescing_hash,
        'coalesced_source_change_seqs', jsonb_build_array(COALESCE(v_source_change_seq, 0)),
        'coalesced_event_count', 1,
        'latest_event_at_utc', v_now::text,
        'source_build_required', false,
        'line_work_required', false,
        'delta_refresh_required', true,
        'legacy_fallback_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      )
      || jsonb_build_object(
        'reason', v_reason,
        'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'source_snapshot_run_id', p_snapshot_run_id::text,
        'session_id', v_session_id::text,
        'source_session_id', v_session_id::text,
        'workbench_session_id', v_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'pay_channel_scope', v_pay_channel_scope,
        'refresh_scope_kind', COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'),
        'candidate_id', p_candidate_id::text,
        'dedupe_key', v_dedupe_key,
        'shadow_compare_required', COALESCE(v_shadow_compare_required, false),
        'shadow_compare_enforced', COALESCE(v_shadow_compare_enforced, false),
        'classifier_result', v_classifier_result,
        'fallback_required', false,
        'fallback_reason', NULL::text,
        'force_legacy', false,
        'force_broad_legacy', false
      )
      || jsonb_build_object(
        'mutation_context', NULLIF(BTRIM(COALESCE(v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'lifecycle_context', '')), ''),
        'lifecycle_mutation_context', NULLIF(BTRIM(COALESCE(v_payload_json->>'lifecycle_mutation_context', v_payload_json->>'mutation_context', v_payload_json->>'lifecycle_context', '')), ''),
        'trigger_table', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_table', v_payload_json#>>'{trigger,table}', '')), ''),
        'trigger_operation', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_operation', v_payload_json->>'trigger_op', v_payload_json#>>'{trigger,operation}', '')), ''),
        'trigger_op', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_op', v_payload_json->>'trigger_operation', v_payload_json#>>'{trigger,operation}', '')), ''),
        'authorise_boundary_changed', lower(BTRIM(COALESCE(v_payload_json->>'authorise_boundary_changed', v_classifier_result#>>'{complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'unauthorise_boundary_changed', lower(BTRIM(COALESCE(v_payload_json->>'unauthorise_boundary_changed', v_classifier_result#>>'{complexity_flags,unauthorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'explicit_banking_pay_action', lower(BTRIM(COALESCE(v_payload_json->>'explicit_banking_pay_action', v_classifier_result#>>'{complexity_flags,explicit_banking_pay_action}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'banking_pay_dirty_required', lower(BTRIM(COALESCE(v_payload_json->>'banking_pay_dirty_required', v_classifier_result->>'banking_pay_dirty_required', v_classifier_result#>>'{complexity_flags,banking_pay_dirty_required}', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
        'ordinary_timesheet_edit_save_no_dirty', lower(BTRIM(COALESCE(v_payload_json->>'ordinary_timesheet_edit_save_no_dirty', v_classifier_result#>>'{complexity_flags,ordinary_timesheet_edit_save_no_dirty}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      )
    );
  ELSE
    v_source_build_run_id_text := NULLIF(BTRIM(COALESCE(
      v_payload_json->>'source_build_run_id',
      v_payload_json#>>'{source_build,source_build_run_id}',
      v_payload_json#>>'{source_build,run_id}',
      v_initial_cursor_json->>'source_build_run_id',
      ''
    )), '');

    IF v_source_build_run_id_text IS NOT NULL THEN
      IF v_source_build_run_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SOURCE_BUILD_RUN_ID_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_SOURCE_BUILD_RUN_ID_INVALID',
                  'source_build_run_id', v_source_build_run_id_text,
                  'session_id', v_session_id::text,
                  'candidate_id', p_candidate_id::text
                )::text;
      END IF;
      v_requested_source_build_run_id := v_source_build_run_id_text::uuid;
    END IF;

    v_authority_fingerprint_text := concat_ws('|',
      CASE WHEN v_authority_fingerprint_version=3
        THEN 'WORKBENCH_SOURCE_OWNER_V3' ELSE 'WORKBENCH_SOURCE_OWNER_V2' END,
      v_session_id::text,
      COALESCE(v_session_row.version, 0)::text,
      v_session_row.source_snapshot_run_id::text,
      v_session_signature_token,
      p_candidate_id::text,
      COALESCE(v_source_change_seq, 0)::text,
      COALESCE(v_registry_dirty_generation, 0)::text,
      UPPER(BTRIM(COALESCE(v_pay_channel_scope, 'ALL'))),
      'FULL_CANDIDATE',
      CASE WHEN v_authority_fingerprint_version=3
        THEN 'READY_TO_PAY_SEMANTIC_V2' ELSE NULL END,
      CASE WHEN v_authority_fingerprint_version=3
        THEN v_required_physical_publication_contract_version::text ELSE NULL END
    );
    v_authority_fingerprint := pg_catalog.encode(
      extensions.digest(pg_catalog.convert_to(v_authority_fingerprint_text, 'UTF8'), 'sha256'),
      'hex'
    );
    v_source_build_hash := substr(v_authority_fingerprint, 1, 32);
    v_source_build_run_id := (
      substr(v_source_build_hash, 1, 8) || '-' ||
      substr(v_source_build_hash, 9, 4) || '-' ||
      substr(v_source_build_hash, 13, 4) || '-' ||
      substr(v_source_build_hash, 17, 4) || '-' ||
      substr(v_source_build_hash, 21, 12)
    )::uuid;

    v_dedupe_key := CASE WHEN v_authority_fingerprint_version=3
        THEN 'WORKBENCH_SOURCE_OWNER_V3:' ELSE 'WORKBENCH_SOURCE_OWNER_V2:' END
      || v_authority_fingerprint
      || ':cursor:' || v_cursor_token;

    v_payload_out_json := jsonb_strip_nulls(
      v_payload_json
      || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'resolved_mode', 'LEGACY',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'reason', v_reason,
        'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'source_snapshot_run_id', p_snapshot_run_id::text,
        'session_id', v_session_id::text,
        'source_session_id', v_session_id::text,
        'workbench_session_id', v_session_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'session_signature', v_session_row.session_signature,
        'session_signature_token', v_session_signature_token
      )
      || jsonb_build_object(
        'candidate_id', p_candidate_id::text,
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'source_change_sequence', COALESCE(v_source_change_seq, 0),
        'source_build_run_id', v_source_build_run_id::text,
        'refresh_scope_kind', v_refresh_scope_kind,
        'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
        'targeted_timesheet_ids_requested', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
        'linked_timesheet_ids_requested', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)
      )
      || jsonb_build_object(
        'pay_channel_scope', v_pay_channel_scope,
        'source_build_required', true,
        'line_work_required', true,
        'line_work_only', false,
        'delta_refresh_required', false,
        'source_build_action', 'BUILD_SOURCE',
        'line_work_action', 'SOURCE_BUILD_THEN_SEED',
        'source_build_limit', v_stage_limit,
        'limit', v_stage_limit,
        'source_cursor', COALESCE(v_initial_cursor_json, '{}'::jsonb),
        'cursor_json', COALESCE(v_initial_cursor_json, '{}'::jsonb),
        'cursor_token', v_cursor_token
      )
      || jsonb_build_object(
        'trigger_table', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_table', v_payload_json#>>'{trigger,table}', '')), ''),
        'trigger_operation', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_operation', v_payload_json->>'trigger_op', v_payload_json#>>'{trigger,operation}', '')), ''),
        'trigger_op', NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_op', v_payload_json->>'trigger_operation', v_payload_json#>>'{trigger,operation}', '')), ''),
        'dedupe_key', v_dedupe_key,
        'authority_fingerprint_version', v_authority_fingerprint_version,
        'authority_fingerprint', v_authority_fingerprint,
        'source_publication_baseline_required',v_source_publication_baseline_required,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'requested_source_build_run_id', CASE WHEN v_requested_source_build_run_id IS NULL THEN NULL ELSE v_requested_source_build_run_id::text END,
        'created_by_helper', 'pay_workbench_enqueue_candidate_refresh',
        'created_at_utc', v_now::text,
        'classifier_result', v_classifier_result
      )
      || jsonb_build_object(
        'source_build', jsonb_strip_nulls(jsonb_build_object(
          'required', true,
          'run_id', v_source_build_run_id::text,
          'source_build_run_id', v_source_build_run_id::text,
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'session_version', COALESCE(v_session_row.version, 0),
          'refresh_scope_kind', v_refresh_scope_kind,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'pay_channel_scope', v_pay_channel_scope,
          'limit', v_stage_limit,
          'cursor', COALESCE(v_initial_cursor_json, '{}'::jsonb),
          'reason', v_reason
          ,'source_publication_baseline_required',v_source_publication_baseline_required
          ,'required_physical_publication_contract_version',v_required_physical_publication_contract_version
        ))
      )
    );
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    -- An active economic build, rather than the lifecycle reason or the root
    -- job's current status, is the durable refresh owner.  A succeeded root
    -- with queued/running stage continuations therefore remains active.
    SELECT build_row.*
    INTO v_owner_build
    FROM private.banking_pay_workbench_economic_builds AS build_row
    WHERE build_row.candidate_id = p_candidate_id
      AND build_row.session_id = v_session_id
      AND build_row.session_version = COALESCE(v_session_row.version, 0)
      AND build_row.source_snapshot_run_id = v_session_row.source_snapshot_run_id
      AND build_row.source_change_seq = COALESCE(v_source_change_seq, 0)
      AND build_row.captured_candidate_generation = COALESCE(v_registry_dirty_generation, 0)
      AND (
        v_source_publication_baseline_required IS NOT TRUE
        OR (
          build_row.authority_fingerprint_version=3
          AND COALESCE((build_row.attestation_json->>'required_physical_publication_contract_version')::integer,0)>=1
        )
      )
      AND UPPER(BTRIM(COALESCE(build_row.status, ''))) IN (
        'COLLECTING',
        'READY_FOR_RECONCILIATION',
        'RECONCILING',
        'RECONCILED',
        'PUBLISHING',
        'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
      )
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS owner_active_job
        WHERE owner_active_job.economic_build_id = build_row.id
          AND UPPER(BTRIM(COALESCE(owner_active_job.status, ''))) IN ('QUEUED', 'RUNNING')
      )
    ORDER BY
      CASE
        WHEN build_row.id = (
          SELECT registry.current_build_id
          FROM private.banking_pay_workbench_candidate_scope_registry AS registry
          WHERE registry.candidate_id = p_candidate_id
        ) THEN 0
        ELSE 1
      END,
      build_row.created_at_utc DESC,
      build_row.id DESC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      SELECT source_job.*
      INTO v_owner_root_job
      FROM public.banking_pay_workbench_jobs AS source_job
      WHERE source_job.economic_build_id = v_owner_build.id
        AND UPPER(BTRIM(COALESCE(source_job.job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND COALESCE(
          source_job.payload_json->>'source_build_run_id',
          source_job.payload_json#>>'{source_build,source_build_run_id}',
          ''
        ) = v_owner_build.source_build_run_id::text
      ORDER BY source_job.created_at_utc, source_job.id
      LIMIT 1
      FOR UPDATE;

      IF FOUND THEN
        v_owner_refresh_scope_kind := UPPER(BTRIM(COALESCE(
          v_owner_root_job.payload_json->>'refresh_scope_kind',
          v_owner_root_job.payload_json#>>'{source_build,refresh_scope_kind}',
          ''
        )));
        v_owner_pay_channel_scope := UPPER(BTRIM(COALESCE(
          v_owner_root_job.payload_json->>'pay_channel_scope',
          v_owner_root_job.payload_json#>>'{source_build,pay_channel_scope}',
          'ALL'
        )));
        v_owner_targeted_timesheet_ids_json := CASE
          WHEN jsonb_typeof(v_owner_root_job.payload_json->'targeted_timesheet_ids') = 'array'
            THEN v_owner_root_job.payload_json->'targeted_timesheet_ids'
          WHEN jsonb_typeof(v_owner_root_job.payload_json#>'{source_build,targeted_timesheet_ids}') = 'array'
            THEN v_owner_root_job.payload_json#>'{source_build,targeted_timesheet_ids}'
          ELSE '[]'::jsonb
        END;
        v_owner_linked_timesheet_ids_json := CASE
          WHEN jsonb_typeof(v_owner_root_job.payload_json->'linked_timesheet_ids') = 'array'
            THEN v_owner_root_job.payload_json->'linked_timesheet_ids'
          WHEN jsonb_typeof(v_owner_root_job.payload_json#>'{source_build,linked_timesheet_ids}') = 'array'
            THEN v_owner_root_job.payload_json#>'{source_build,linked_timesheet_ids}'
          ELSE '[]'::jsonb
        END;
        -- Every WORKBENCH_CANDIDATE_SOURCE_BUILD owns complete candidate truth.
        -- Diagnostic refresh scope is retained as provenance, not identity.
        v_owner_covers_request :=
          v_owner_pay_channel_scope = UPPER(BTRIM(COALESCE(v_pay_channel_scope, 'ALL')));
        IF v_owner_covers_request THEN
          v_owner_resolution := 'ACTIVE_CURRENT_OWNER_COVERS_REQUEST';
        END IF;
      END IF;
    END IF;

    -- A cancellation reversion deliberately retains the immutable original
    -- economic-build lineage while publishing a new current source run at the
    -- cancellation sequence/generation.  It is therefore a complete current
    -- authority even though the historical build row itself has an older
    -- source sequence.  Recognise only the exact V3 terminal attestation; an
    -- ordinary V1/V2 or structurally incomplete scope cannot enter this path.
    IF v_owner_resolution='NO_CURRENT_OWNER'
       AND v_scope_state_precedes_job IS TRUE THEN
      SELECT current_scope.*
      INTO v_reversion_scope
      FROM public.banking_pay_workbench_session_scope AS current_scope
      WHERE current_scope.session_id=v_session_id
        AND current_scope.candidate_id=p_candidate_id
        AND current_scope.dirty IS FALSE
        AND current_scope.pending_job_id IS NULL
        AND current_scope.certified_preview_publication_required IS TRUE
        AND current_scope.certified_preview_publication_parity_ok IS TRUE
        AND current_scope.certified_preview_publication_session_version=COALESCE(v_session_row.version,0)
        AND current_scope.certified_preview_publication_source_change_seq=COALESCE(v_source_change_seq,0)
        AND current_scope.certified_preview_publication_attestation_json->>'attestation_version'
              ='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
        AND current_scope.certified_preview_publication_attestation_json->>'contract_version'='3'
        AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
              ='READY_TO_PAY_SEMANTIC_V2'
        AND current_scope.certified_preview_publication_attestation_json->>'authority_kind'
              ='CERTIFIED_CANCELLATION_REVERSION'
        AND COALESCE((current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,false)
        AND COALESCE((current_scope.certified_preview_publication_attestation_json->>'parity_complete')::boolean,false)
        AND (
          NOT v_source_publication_baseline_required
          OR (
            current_scope.certified_preview_publication_source_publication_id IS NOT NULL
            AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
                  =current_scope.certified_preview_publication_source_publication_id::text
          )
        )
      FOR UPDATE;

      IF FOUND THEN
        v_reversion_attestation:=COALESCE(
          v_reversion_scope.certified_preview_publication_attestation_json,'{}'::jsonb
        );

        SELECT current_state.*
        INTO v_reversion_state
        FROM public.banking_pay_workbench_session_candidate_state AS current_state
        WHERE current_state.session_id=v_session_id
          AND current_state.candidate_id=p_candidate_id
          AND UPPER(BTRIM(COALESCE(current_state.status,'')))='READY'
          AND current_state.pending_job_id IS NULL
          AND current_state.session_version=COALESCE(v_session_row.version,0)
          AND current_state.source_change_seq=COALESCE(v_source_change_seq,0)
        FOR UPDATE;
        v_reversion_state_exact:=FOUND;

        SELECT COUNT(*)::integer
        INTO v_reversion_source_count
        FROM public.banking_pay_workbench_candidate_source_lines AS reversion_source
        WHERE reversion_source.session_id=v_session_id
          AND reversion_source.candidate_id=p_candidate_id
          AND reversion_source.session_version=COALESCE(v_session_row.version,0)
          AND reversion_source.source_change_seq=COALESCE(v_source_change_seq,0)
          AND reversion_source.source_publication_id
                =v_reversion_scope.certified_preview_publication_source_publication_id
          AND reversion_source.status='CURRENT';

        IF v_reversion_state_exact
           AND COALESCE(v_registry_source_change_seq_after,v_source_change_seq,0)
                =COALESCE(v_source_change_seq,0)
           AND COALESCE(v_registry_dirty_generation,0)=COALESCE(v_live_scope_change_generation,0)
           AND COALESCE(v_reversion_attestation->>'source_row_count','') ~ '^[0-9]{1,9}$'
           AND v_reversion_source_count=(v_reversion_attestation->>'source_row_count')::integer
           AND NOT EXISTS (
             SELECT 1
             FROM public.banking_pay_workbench_candidate_source_lines AS foreign_current_source
             WHERE foreign_current_source.session_id=v_session_id
               AND foreign_current_source.candidate_id=p_candidate_id
               AND foreign_current_source.status='CURRENT'
               AND (
                 foreign_current_source.session_version IS DISTINCT FROM COALESCE(v_session_row.version,0)
                 OR foreign_current_source.source_change_seq IS DISTINCT FROM COALESCE(v_source_change_seq,0)
                 OR foreign_current_source.source_publication_id IS DISTINCT FROM
                      v_reversion_scope.certified_preview_publication_source_publication_id
               )
            ) THEN
          EXECUTE 'SELECT private.pay_workbench_candidate_physical_currentness_page_v1($1,$2,$3,$4)'
            INTO v_physical_currentness
            USING v_session_id,ARRAY[p_candidate_id],'TERMINAL_CURRENT',
              jsonb_build_object('contract_version',1,'allow_active_owner',false);
          v_candidate_currentness:=COALESCE(v_physical_currentness->'candidate_results'->0,'{}'::jsonb);
          IF COALESCE((v_candidate_currentness->>'terminal_current')::boolean,false) THEN
          RETURN jsonb_strip_nulls(jsonb_build_object(
            'ok',true,
            'job_id',NULL,
            'job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'canonical_job_type','WORKBENCH_CANDIDATE_SOURCE_BUILD',
            'session_id',v_session_id::text,
            'candidate_id',p_candidate_id::text,
            'session_version',COALESCE(v_session_row.version,0),
            'source_change_seq',COALESCE(v_source_change_seq,0),
            'registry_source_change_seq',COALESCE(v_registry_source_change_seq_after,v_source_change_seq,0),
            'source_build_run_id',v_reversion_scope.certified_preview_publication_source_build_run_id::text,
            'source_publication_id',v_reversion_scope.certified_preview_publication_source_publication_id::text,
            'authority_fingerprint',v_authority_fingerprint,
            'authority_fingerprint_version',v_authority_fingerprint_version,
            'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
            'owner_resolution','COMPLETE_CURRENT_AUTHORITY',
            'owner_build_id',v_reversion_attestation->>'economic_build_id',
            'owner_root_job_id',NULL,
            'owner_source_build_run_id',v_reversion_scope.certified_preview_publication_source_build_run_id::text,
            'requested_coverage','FULL_CANDIDATE',
            'owner_coverage','FULL_CANDIDATE',
            'scope_status',v_reversion_scope.status,
            'source_build_required',false,
            'delta_refresh_required',false,
            'coalesced',true,
            'reused',true,
            'new_owner_created',false,
            'no_op',true,
            'stale_preinvalidated_absorb_only',v_stale_preinvalidated_absorb_only,
            'diagnostic_provenance_merged',false,
            'reason',v_reason,
            'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
          ));
          END IF;
        END IF;
      END IF;
    END IF;

    -- A delayed dirty event which revalidates to an already published current
    -- authority is a true no-op.  This branch is deliberately limited to the
    -- caller's proven, already-finalised scope generation.
    IF v_owner_resolution = 'NO_CURRENT_OWNER'
       AND v_scope_state_precedes_job IS TRUE THEN
      SELECT build_row.*
      INTO v_owner_build
      FROM private.banking_pay_workbench_economic_builds AS build_row
      JOIN public.banking_pay_workbench_session_scope AS current_scope
        ON current_scope.session_id = build_row.session_id
       AND current_scope.candidate_id = build_row.candidate_id
      JOIN public.banking_pay_workbench_session_candidate_state AS current_state
        ON current_state.session_id = build_row.session_id
       AND current_state.candidate_id = build_row.candidate_id
      WHERE build_row.candidate_id = p_candidate_id
        AND build_row.session_id = v_session_id
        AND build_row.session_version = COALESCE(v_session_row.version, 0)
        AND build_row.source_snapshot_run_id = v_session_row.source_snapshot_run_id
        AND build_row.source_change_seq = COALESCE(v_source_change_seq, 0)
        AND build_row.captured_candidate_generation = COALESCE(v_registry_dirty_generation, 0)
        AND UPPER(BTRIM(COALESCE(build_row.status, ''))) = 'COMPLETE'
        AND UPPER(BTRIM(COALESCE(build_row.private_stage, ''))) = 'COMPLETE'
        AND current_scope.dirty IS FALSE
        AND current_scope.pending_job_id IS NULL
        AND current_scope.certified_preview_publication_required IS TRUE
        AND current_scope.certified_preview_publication_parity_ok IS TRUE
        AND current_scope.certified_preview_publication_session_version = build_row.session_version
        AND current_scope.certified_preview_publication_source_change_seq = build_row.source_change_seq
        AND current_scope.certified_preview_publication_source_build_run_id = build_row.source_build_run_id
        AND (
          NOT v_source_publication_baseline_required
          OR (
            current_scope.certified_preview_publication_source_publication_id IS NOT NULL
            AND current_scope.certified_preview_publication_attestation_json->>'source_publication_id'
                  =current_scope.certified_preview_publication_source_publication_id::text
          )
        )
        AND (
          v_semantic_ready_publication_enabled IS NOT TRUE
          OR (
            current_scope.certified_preview_publication_attestation_json->>'attestation_version'
              = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
            AND current_scope.certified_preview_publication_attestation_json->>'contract_version' = '3'
            AND current_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
              = 'READY_TO_PAY_SEMANTIC_V2'
            AND COALESCE(
              (current_scope.certified_preview_publication_attestation_json->>'semantic_ready')::boolean,
              false
            )
          )
        )
        AND UPPER(BTRIM(COALESCE(current_state.status, ''))) = 'READY'
        AND current_state.pending_job_id IS NULL
        AND current_state.session_version = build_row.session_version
        AND current_state.source_change_seq = build_row.source_change_seq
      ORDER BY build_row.completed_at_utc DESC NULLS LAST, build_row.id DESC
      LIMIT 1
      FOR UPDATE OF build_row, current_scope, current_state;

      IF FOUND THEN
        SELECT source_job.*
        INTO v_owner_root_job
        FROM public.banking_pay_workbench_jobs AS source_job
        WHERE source_job.economic_build_id = v_owner_build.id
          AND UPPER(BTRIM(COALESCE(source_job.job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND COALESCE(
            source_job.payload_json->>'source_build_run_id',
            source_job.payload_json#>>'{source_build,source_build_run_id}',
            ''
          ) = v_owner_build.source_build_run_id::text
        ORDER BY source_job.created_at_utc, source_job.id
        LIMIT 1
        FOR UPDATE;

        IF FOUND THEN
          v_owner_refresh_scope_kind := UPPER(BTRIM(COALESCE(
            v_owner_root_job.payload_json->>'refresh_scope_kind',
            v_owner_root_job.payload_json#>>'{source_build,refresh_scope_kind}',
            ''
          )));
          v_owner_pay_channel_scope := UPPER(BTRIM(COALESCE(
            v_owner_root_job.payload_json->>'pay_channel_scope',
            v_owner_root_job.payload_json#>>'{source_build,pay_channel_scope}',
            'ALL'
          )));
          v_owner_covers_request :=
            v_owner_pay_channel_scope = UPPER(BTRIM(COALESCE(v_pay_channel_scope, 'ALL')));
          IF v_owner_covers_request THEN
            EXECUTE 'SELECT private.pay_workbench_candidate_physical_currentness_page_v1($1,$2,$3,$4)'
              INTO v_physical_currentness
              USING v_session_id,ARRAY[p_candidate_id],'TERMINAL_CURRENT',
                jsonb_build_object('contract_version',1,'allow_active_owner',false);
            v_candidate_currentness:=COALESCE(v_physical_currentness->'candidate_results'->0,'{}'::jsonb);
            IF COALESCE((v_candidate_currentness->>'terminal_current')::boolean,false) THEN
              v_owner_resolution := 'COMPLETE_CURRENT_AUTHORITY';
            END IF;
          END IF;
        END IF;
      END IF;
    END IF;

    IF v_owner_resolution IN (
      'ACTIVE_CURRENT_OWNER_COVERS_REQUEST',
      'COMPLETE_CURRENT_AUTHORITY'
    ) THEN
      SELECT COALESCE(jsonb_agg(bounded_reason.reason ORDER BY bounded_reason.reason), '[]'::jsonb)
      INTO v_owner_reasons_json
      FROM (
        SELECT DISTINCT reason_value.reason
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_owner_root_job.payload_json->'reasons') = 'array'
              THEN v_owner_root_job.payload_json->'reasons'
            WHEN NULLIF(BTRIM(COALESCE(v_owner_root_job.payload_json->>'reason', '')), '') IS NOT NULL
              THEN jsonb_build_array(v_owner_root_job.payload_json->>'reason')
            ELSE '[]'::jsonb
          END || jsonb_build_array(v_reason)
        ) AS reason_value(reason)
        WHERE NULLIF(BTRIM(reason_value.reason), '') IS NOT NULL
        ORDER BY reason_value.reason
        LIMIT 16
      ) AS bounded_reason;

      SELECT COALESCE(jsonb_agg(bounded_source.source ORDER BY bounded_source.source), '[]'::jsonb)
      INTO v_owner_trigger_sources_json
      FROM (
        SELECT DISTINCT source_value.source
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_owner_root_job.payload_json->'trigger_sources') = 'array'
              THEN v_owner_root_job.payload_json->'trigger_sources'
            ELSE '[]'::jsonb
          END || jsonb_build_array(NULLIF(BTRIM(COALESCE(
            v_payload_json->>'trigger_source',
            v_payload_json->>'trigger_table',
            v_payload_json->>'enqueue_origin',
            ''
          )), ''))
        ) AS source_value(source)
        WHERE NULLIF(BTRIM(source_value.source), '') IS NOT NULL
        ORDER BY source_value.source
        LIMIT 16
      ) AS bounded_source;

      v_owner_request_count := GREATEST(
        CASE WHEN COALESCE(v_owner_root_job.payload_json->>'reason_count', '') ~ '^\d+$'
          THEN (v_owner_root_job.payload_json->>'reason_count')::bigint ELSE 1 END,
        CASE WHEN COALESCE(v_owner_root_job.payload_json#>>'{orchestration_provenance,coalesced_request_count}', '') ~ '^\d+$'
          THEN (v_owner_root_job.payload_json#>>'{orchestration_provenance,coalesced_request_count}')::bigint ELSE 1 END
      ) + 1;
      v_owner_provenance_json := jsonb_strip_nulls(
        CASE WHEN jsonb_typeof(v_owner_root_job.payload_json->'orchestration_provenance') = 'object'
          THEN v_owner_root_job.payload_json->'orchestration_provenance' ELSE '{}'::jsonb END
        || jsonb_build_object(
          'primary_reason', COALESCE(v_owner_root_job.payload_json#>>'{orchestration_provenance,primary_reason}', v_owner_root_job.payload_json->>'reason', v_reason),
          'reason_latest', v_reason,
          'reasons', v_owner_reasons_json,
          'trigger_sources', v_owner_trigger_sources_json,
          'coalesced_request_count', v_owner_request_count,
          'last_requested_at_utc', v_now::text,
          'authority_fingerprint_version', v_authority_fingerprint_version,
          'authority_fingerprint', v_authority_fingerprint
        )
      );

      UPDATE public.banking_pay_workbench_jobs AS owner_job
      SET payload_json = jsonb_strip_nulls(
            COALESCE(owner_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'reason_latest', v_reason,
              'reason_count', v_owner_request_count,
              'reasons', v_owner_reasons_json,
              'trigger_sources', v_owner_trigger_sources_json,
              'orchestration_provenance', v_owner_provenance_json
            )
          ),
          updated_at_utc = v_now
      WHERE owner_job.id = v_owner_root_job.id;

      SELECT active_job.id
      INTO v_owner_active_job_id
      FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.economic_build_id = v_owner_build.id
        AND UPPER(BTRIM(COALESCE(active_job.status, ''))) IN ('QUEUED', 'RUNNING')
      ORDER BY CASE WHEN UPPER(BTRIM(active_job.status)) = 'RUNNING' THEN 0 ELSE 1 END,
               active_job.run_at_utc,
               active_job.created_at_utc,
               active_job.id
      LIMIT 1;

      SELECT scope_row.status
      INTO v_owner_scope_status
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id = v_session_id
        AND scope_row.candidate_id = p_candidate_id;

      RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok', true,
        'job_id', v_owner_root_job.id::text,
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'session_id', v_session_id::text,
        'candidate_id', p_candidate_id::text,
        'session_version', COALESCE(v_session_row.version, 0),
        'source_change_seq', COALESCE(v_source_change_seq, 0),
        'registry_source_change_seq', COALESCE(v_registry_source_change_seq_after, v_source_change_seq, 0),
        'source_build_run_id', v_owner_build.source_build_run_id::text,
        'requested_source_build_run_id', CASE WHEN v_requested_source_build_run_id IS NULL THEN NULL ELSE v_requested_source_build_run_id::text END,
        'authority_fingerprint', v_authority_fingerprint,
        'authority_fingerprint_version', v_authority_fingerprint_version,
        'required_physical_publication_contract_version',v_required_physical_publication_contract_version,
        'owner_resolution', v_owner_resolution,
        'owner_build_id', v_owner_build.id::text,
        'owner_root_job_id', v_owner_root_job.id::text,
        'owner_active_job_id', CASE WHEN v_owner_active_job_id IS NULL THEN NULL ELSE v_owner_active_job_id::text END,
        'owner_source_build_run_id', v_owner_build.source_build_run_id::text,
        'requested_coverage', 'FULL_CANDIDATE',
        'owner_coverage', 'FULL_CANDIDATE',
        'scope_status', COALESCE(v_owner_scope_status, CASE WHEN v_owner_resolution = 'COMPLETE_CURRENT_AUTHORITY' THEN 'MATERIALISED' ELSE 'SOURCE_BUILD_PENDING' END),
        'source_build_required', v_owner_resolution <> 'COMPLETE_CURRENT_AUTHORITY',
        'delta_refresh_required', false,
        'coalesced', true,
        'reused', true,
        'new_owner_created', false,
        'no_op', v_owner_resolution = 'COMPLETE_CURRENT_AUTHORITY',
        'stale_preinvalidated_absorb_only', v_stale_preinvalidated_absorb_only,
        'diagnostic_provenance_merged', true,
        'reason', v_reason,
        'reason_count', v_owner_request_count,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      ));
    END IF;
  END IF;

  IF v_stale_preinvalidated_absorb_only THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT'
      USING ERRCODE='40001', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT',
        'candidate_id',p_candidate_id,
        'payload_scope_change_generation',v_payload_scope_change_generation,
        'live_scope_change_generation',v_live_scope_change_generation,
        'registry_dirty_generation',v_registry_dirty_generation,
        'source_change_seq',v_source_change_seq,
        'registry_source_change_seq',v_registry_source_change_seq_after,
        'owner_resolution',v_owner_resolution
      )::text;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    SELECT existing_delta_job.*
    INTO v_existing_delta_job
    FROM public.banking_pay_workbench_jobs AS existing_delta_job
    WHERE existing_delta_job.session_id = v_session_id
      AND existing_delta_job.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(existing_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
      AND UPPER(BTRIM(COALESCE(existing_delta_job.status, ''))) = 'QUEUED'
      AND (
        COALESCE(existing_delta_job.payload_json->>'normalised_delta_family_key', existing_delta_job.payload_json->>'delta_family_key', existing_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
        OR (
            SELECT
              'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
              || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                existing_delta_job.session_id::text,
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                ''
              )), ''), 'none')
              || ':version:' || COALESCE(
                CASE
                  WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                    THEN delta_family_values.session_version_text::bigint
                  ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                END,
                0
              )::text
              || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_mode_text, ''),
                NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                'DELTA'
              ))), ''), 'DELTA')
              || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_class_text, ''),
                NULLIF(v_projection_class, ''),
                'UNKNOWN'
              ))), ''), 'UNKNOWN')
              || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                CASE
                  WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                    OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                    THEN 'TARGETED_TIMESHEETS'
                  ELSE 'CANDIDATE_FULL_LIVE'
                END
              ))), ''), 'CANDIDATE_FULL_LIVE')
              || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                existing_delta_job.candidate_id::text,
                (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                ''
              )), ''), 'none')
              || ':timesheets:' || md5(
                COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                || ':'
                || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
              )
            FROM (
              SELECT
                NULLIF(BTRIM(COALESCE(
                  (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                  (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                  (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'version',
                  CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                  '0'
                )), '') AS session_version_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))) AS projection_mode_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                  NULLIF(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))) AS projection_class_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                  NULLIF((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                  NULLIF(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                  NULLIF(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                ))) AS refresh_scope_kind_text
            ) AS delta_family_values
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                      THEN (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                    WHEN NULLIF(BTRIM(COALESCE(
                           (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                           (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                           ''
                         )), '') IS NOT NULL
                      THEN jsonb_build_array(COALESCE(
                        (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                        (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                      ))
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_targeted_raw(value)
                WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_targeted_sorted
            ) AS delta_family_targeted
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                      THEN (COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(existing_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_linked_raw(value)
                WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_linked_sorted
            ) AS delta_family_linked
          ) = v_delta_coalescing_key
      )
      AND lower(BTRIM(COALESCE(existing_delta_job.payload_json->>'force_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      AND lower(BTRIM(COALESCE(existing_delta_job.payload_json->>'force_broad_legacy', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    ORDER BY
      CASE
        WHEN COALESCE(existing_delta_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (existing_delta_job.payload_json->>'source_change_seq')::bigint
        ELSE 0::bigint
      END DESC,
      existing_delta_job.priority ASC,
      existing_delta_job.run_at_utc ASC,
      existing_delta_job.created_at_utc ASC,
      existing_delta_job.id ASC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      -- Reused queued latest-state heads must not inherit an old projection/cursor identity.
      v_existing_delta_projection_run_id_text := NULLIF(BTRIM(COALESCE(v_existing_delta_job.payload_json->>'projection_run_id', '')), '');
      v_projection_run_id := gen_random_uuid();

      v_existing_delta_source_change_seq := GREATEST(
        CASE WHEN COALESCE(v_existing_delta_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_existing_delta_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_existing_delta_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_existing_delta_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_existing_delta_job.payload_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_existing_delta_job.payload_json->>'source_change_sequence')::bigint ELSE 0::bigint END
      );
      v_existing_delta_event_count := CASE
        WHEN COALESCE(v_existing_delta_job.payload_json->>'coalesced_event_count', '') ~ '^[0-9]{1,9}$'
          THEN GREATEST((v_existing_delta_job.payload_json->>'coalesced_event_count')::integer, 1)
        ELSE 1
      END;
      v_merged_delta_event_count := v_existing_delta_event_count + 1;

      v_payload_out_json := public._pay_workbench_merge_targeted_scope_payload(
        COALESCE(v_existing_delta_job.payload_json, '{}'::jsonb),
        COALESCE(v_payload_out_json, '{}'::jsonb)
      ) || jsonb_build_object(
        'job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'projection_run_id', v_projection_run_id::text,
        'projection_mode', COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'),
        'projection_class', v_projection_class,
        'phase', 'INIT_PREFLIGHT',
        'source_build_required', false,
        'line_work_required', false,
        'delta_refresh_required', true,
        'legacy_fallback_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      );

      v_source_change_seq := GREATEST(
        COALESCE(v_live_change_seq, 0),
        COALESCE(v_source_change_seq, 0),
        COALESCE(v_existing_delta_source_change_seq, 0),
        CASE WHEN COALESCE(v_payload_out_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_payload_out_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_payload_out_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_payload_out_json->>'source_change_seq')::bigint ELSE 0::bigint END,
        CASE WHEN COALESCE(v_payload_out_json->>'source_change_sequence', '') ~ '^-?[0-9]{1,18}$'
          THEN (v_payload_out_json->>'source_change_sequence')::bigint ELSE 0::bigint END
      );

      SELECT COALESCE(jsonb_agg(merged_target_ids.timesheet_id_text ORDER BY merged_target_ids.timesheet_id_text), '[]'::jsonb)
      INTO v_targeted_timesheet_ids_json
      FROM (
        SELECT DISTINCT NULLIF(BTRIM(targeted_values.value), '') AS timesheet_id_text
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_payload_out_json->'targeted_timesheet_ids') = 'array' THEN v_payload_out_json->'targeted_timesheet_ids'
            WHEN jsonb_typeof(v_payload_out_json->'targeted_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_out_json->>'targeted_timesheet_ids')
            ELSE '[]'::jsonb
          END
        ) AS targeted_values(value)
        WHERE NULLIF(BTRIM(targeted_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS merged_target_ids;

      SELECT COALESCE(jsonb_agg(merged_linked_ids.timesheet_id_text ORDER BY merged_linked_ids.timesheet_id_text), '[]'::jsonb)
      INTO v_linked_timesheet_ids_json
      FROM (
        SELECT DISTINCT NULLIF(BTRIM(linked_values.value), '') AS timesheet_id_text
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_payload_out_json->'linked_timesheet_ids') = 'array' THEN v_payload_out_json->'linked_timesheet_ids'
            WHEN jsonb_typeof(v_payload_out_json->'linked_timesheet_ids') = 'string' THEN jsonb_build_array(v_payload_out_json->>'linked_timesheet_ids')
            ELSE '[]'::jsonb
          END
        ) AS linked_values(value)
        WHERE NULLIF(BTRIM(linked_values.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS merged_linked_ids;

      v_delta_ids_hash := md5(
        COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb)::text
        || ':'
        || COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb)::text
      );

      v_delta_coalescing_key := 'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
    || ':session:' || COALESCE(v_session_id::text, 'none')
    || ':version:' || COALESCE(COALESCE(v_session_row.version, 0), 0)::text
    || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''), 'DELTA'))), ''), 'DELTA')
    || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(v_projection_class, ''), 'UNKNOWN'))), ''), 'UNKNOWN')
    || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''), 'TARGETED_TIMESHEETS'))), ''), 'TARGETED_TIMESHEETS')
    || ':candidate:' || COALESCE(p_candidate_id::text, 'none')
    || ':timesheets:' || COALESCE(v_delta_ids_hash, md5('[]:[]'));
      v_delta_coalescing_hash := md5(v_delta_coalescing_key);

      v_payload_out_json := jsonb_strip_nulls(
        (COALESCE(v_payload_out_json, '{}'::jsonb) - ARRAY[
          'cursor',
          'cursor_json',
          'next_cursor',
          'next_cursor_json',
          'source_cursor',
          'write_cursor_json',
          'candidate_cursor',
          'cursor_token',
          'has_cursor',
          'continuation_reason',
          'source_job_id',
          'continuation_source_job_id',
          'bounded_continuation_source_job_id',
          'parent_job_id',
          'next_phase',
          'write_phase',
          'source_result_summary',
          'source_result_has_more',
          'source_result_next_cursor_present'
        ]::text[])
        || jsonb_build_object(
          'cursor_json', '{}'::jsonb,
          'continuation', false,
          'phase', 'INIT_PREFLIGHT',
          'run_mode', CASE WHEN v_delta_active_running_job_id IS NULL THEN 'LATEST_STATE_HEAD' ELSE 'LATEST_RERUN_AFTER_RUNNING' END,
          'normalised_delta_family_key', v_delta_coalescing_key,
          'delta_family_key', v_delta_coalescing_key,
          'delta_coalescing_key', v_delta_coalescing_key,
          'delta_coalescing_hash', v_delta_coalescing_hash
        )
      );

      SELECT running_delta_job.id
      INTO v_delta_active_running_job_id
      FROM public.banking_pay_workbench_jobs AS running_delta_job
      WHERE running_delta_job.session_id = v_session_id
        AND running_delta_job.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        AND UPPER(BTRIM(COALESCE(running_delta_job.status, ''))) = 'RUNNING'
        AND running_delta_job.id IS DISTINCT FROM v_existing_delta_job.id
        AND (
          COALESCE(running_delta_job.payload_json->>'normalised_delta_family_key', running_delta_job.payload_json->>'delta_family_key', running_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
          OR (
              SELECT
                'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
                || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                  running_delta_job.session_id::text,
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                  ''
                )), ''), 'none')
                || ':version:' || COALESCE(
                  CASE
                    WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                      THEN delta_family_values.session_version_text::bigint
                    ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                  END,
                  0
                )::text
                || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.projection_mode_text, ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))), ''), 'DELTA')
                || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.projection_class_text, ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))), ''), 'UNKNOWN')
                || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                  NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                  CASE
                    WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                      OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                      THEN 'TARGETED_TIMESHEETS'
                    ELSE 'CANDIDATE_FULL_LIVE'
                  END
                ))), ''), 'CANDIDATE_FULL_LIVE')
                || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                  running_delta_job.candidate_id::text,
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                  ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                  ''
                )), ''), 'none')
                || ':timesheets:' || md5(
                  COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                  || ':'
                  || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
                )
              FROM (
                SELECT
                  NULLIF(BTRIM(COALESCE(
                    (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                    (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                    (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                    CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                    '0'
                  )), '') AS session_version_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                    NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                    'DELTA'
                  ))) AS projection_mode_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                    NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                    NULLIF(v_projection_class, ''),
                    'UNKNOWN'
                  ))) AS projection_class_text,
                  UPPER(BTRIM(COALESCE(
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                    NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                    NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                    NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                    NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                  ))) AS refresh_scope_kind_text
              ) AS delta_family_values
              CROSS JOIN (
                SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
                FROM (
                  SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                  FROM jsonb_array_elements_text(
                    CASE
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                        THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                        THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                      WHEN NULLIF(BTRIM(COALESCE(
                             (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                             (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                             ''
                           )), '') IS NOT NULL
                        THEN jsonb_build_array(COALESCE(
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                          (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                        ))
                      ELSE '[]'::jsonb
                    END
                  ) AS delta_family_targeted_raw(value)
                  WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) AS delta_family_targeted_sorted
              ) AS delta_family_targeted
              CROSS JOIN (
                SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
                FROM (
                  SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                  FROM jsonb_array_elements_text(
                    CASE
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                        THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                      WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                        THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                      WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                        THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                      ELSE '[]'::jsonb
                    END
                  ) AS delta_family_linked_raw(value)
                  WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                ) AS delta_family_linked_sorted
              ) AS delta_family_linked
            ) = v_delta_coalescing_key
        )
      ORDER BY running_delta_job.started_at_utc ASC NULLS LAST, running_delta_job.created_at_utc ASC, running_delta_job.id ASC
      LIMIT 1;

      v_dedupe_key := CASE
        WHEN v_delta_active_running_job_id IS NOT NULL THEN
          v_delta_coalescing_key || ':waiting_after_running:' || v_delta_active_running_job_id::text
        ELSE
          v_delta_coalescing_key
      END;

      v_payload_out_json := jsonb_strip_nulls(
        v_payload_out_json
        || jsonb_build_object(
          'dedupe_key', v_dedupe_key,
          'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
          'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
          'queue_identity_targeted_timesheet_ids', COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb),
          'queue_identity_linked_timesheet_ids', COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb),
          'projection_run_id', v_projection_run_id::text,
          'source_change_seq', COALESCE(v_source_change_seq, 0),
          'source_change_sequence', COALESCE(v_source_change_seq, 0),
          'latest_source_change_seq', COALESCE(v_source_change_seq, 0),
          'delta_coalescing_key', v_delta_coalescing_key,
          'delta_family_key', v_delta_coalescing_key,
          'normalised_delta_family_key', v_delta_coalescing_key,
          'delta_coalescing_hash', v_delta_coalescing_hash,
          'coalesced_event_count', GREATEST(COALESCE(v_merged_delta_event_count, 1), 1),
          'coalesced_source_change_seqs', jsonb_build_array(COALESCE(v_existing_delta_source_change_seq, 0), COALESCE(v_source_change_seq, 0)),
          'latest_event_at_utc', v_now::text,
          'scope_merge_applied', true,
          'scope_merge_at_utc', v_now::text,
          'cursor_json', '{}'::jsonb,
          'continuation', false,
          'phase', 'INIT_PREFLIGHT',
          'run_mode', CASE WHEN v_delta_active_running_job_id IS NULL THEN 'LATEST_STATE_HEAD' ELSE 'LATEST_RERUN_AFTER_RUNNING' END
        )
      );

      UPDATE public.banking_pay_workbench_jobs AS existing_delta_update
      SET dedupe_key = v_dedupe_key,
          priority = LEAST(existing_delta_update.priority, 43),
          run_at_utc = LEAST(existing_delta_update.run_at_utc, v_now),
          payload_json = v_payload_out_json,
          updated_at_utc = v_now
      WHERE existing_delta_update.id = v_existing_delta_job.id
      RETURNING existing_delta_update.id,
                existing_delta_update.status,
                false
      INTO v_job_id,
           v_job_status,
           v_job_was_inserted;

      v_delta_merge_reused_existing := true;
    END IF;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
    v_payload_out_json := jsonb_strip_nulls(
      (COALESCE(v_payload_out_json, '{}'::jsonb) - ARRAY[
        'old_row_json',
        'new_row_json',
        'old_row',
        'new_row',
        'source_row_json',
        'work_payload_json',
        'result_row_json',
        'preview_row_json',
        'row_payload_json',
        'line_payload_json',
        'projection_rows',
        'projected_rows',
        'cursor',
        'cursor_json',
        'next_cursor',
        'next_cursor_json',
        'source_cursor',
        'write_cursor_json',
        'candidate_cursor',
        'cursor_token',
        'has_cursor',
        'continuation_reason',
        'source_job_id',
        'continuation_source_job_id',
        'bounded_continuation_source_job_id',
        'parent_job_id',
        'next_phase',
        'write_phase',
        'source_result_summary',
        'source_result_has_more',
        'source_result_next_cursor_present'
      ]::text[])
      || jsonb_build_object(
        'continuation', false,
        'cursor_json', '{}'::jsonb,
        'run_mode', CASE WHEN v_delta_active_running_job_id IS NULL THEN 'LATEST_STATE_HEAD' ELSE 'LATEST_RERUN_AFTER_RUNNING' END,
        'phase', 'INIT_PREFLIGHT'
      )
    );
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    UPDATE public.banking_pay_workbench_jobs AS queued_delta_job
    SET status = 'SUCCEEDED',
        completed_at_utc = v_now,
        updated_at_utc = v_now,
        payload_json = jsonb_strip_nulls(
          COALESCE(queued_delta_job.payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'superseded_by_legacy', true,
            'superseded_by_legacy_at_utc', v_now::text,
            'superseded_reason', COALESCE(NULLIF(v_classifier_result->>'fallback_reason', ''), 'LEGACY_REFRESH_WINS'),
            'legacy_source_change_seq', COALESCE(v_source_change_seq, 0)
          )
        )
    WHERE queued_delta_job.session_id = v_session_id
      AND queued_delta_job.candidate_id = p_candidate_id
      AND UPPER(BTRIM(COALESCE(queued_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
      AND UPPER(BTRIM(COALESCE(queued_delta_job.status, ''))) = 'QUEUED';
    GET DIAGNOSTICS v_delta_jobs_superseded = ROW_COUNT;
  END IF;

  IF v_job_id IS NULL THEN
    INSERT INTO public.banking_pay_workbench_jobs AS enqueue_job (
      job_type,
      status,
      priority,
      run_at_utc,
      attempt_count,
      max_attempts,
      dedupe_key,
      snapshot_run_id,
      session_id,
      candidate_id,
      payload_json,
      economic_build_id,
      private_stage,
      private_cursor_kind,
      private_cursor_json,
      private_stage_version,
      created_at_utc,
      updated_at_utc,
      started_at_utc,
      completed_at_utc,
      failed_at_utc,
      last_error_json
    )
    VALUES (
      v_job_type,
      'QUEUED',
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN 43 ELSE 44 END,
      v_now,
      0,
      8,
      v_dedupe_key,
      p_snapshot_run_id,
      v_session_id,
      p_candidate_id,
      v_payload_out_json,
      NULL::uuid,
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
      '{}'::jsonb,
      CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 1 ELSE NULL::integer END,
      v_now,
      v_now,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::jsonb
    )
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(enqueue_job.priority, EXCLUDED.priority),
        run_at_utc = LEAST(enqueue_job.run_at_utc, EXCLUDED.run_at_utc),
        payload_json = CASE
          WHEN EXCLUDED.job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
            jsonb_strip_nulls(
              (public._pay_workbench_merge_targeted_scope_payload(
                COALESCE(enqueue_job.payload_json, '{}'::jsonb),
                COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
              ) - ARRAY[
                'cursor',
                'cursor_json',
                'next_cursor',
                'next_cursor_json',
                'source_cursor',
                'write_cursor_json',
                'candidate_cursor',
                'cursor_token',
                'has_cursor',
                'continuation_reason',
                'source_job_id',
                'continuation_source_job_id',
                'bounded_continuation_source_job_id',
                'parent_job_id',
                'next_phase',
                'write_phase',
                'source_result_summary',
                'source_result_has_more',
                'source_result_next_cursor_present'
              ]::text[])
              || jsonb_build_object(
                'cursor_json', '{}'::jsonb,
                'continuation', false,
                'phase', 'INIT_PREFLIGHT',
                'run_mode', CASE
                  WHEN lower(BTRIM(COALESCE(EXCLUDED.payload_json->>'waiting_after_running_delta_job', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'LATEST_RERUN_AFTER_RUNNING'
                  ELSE 'LATEST_STATE_HEAD'
                END,
                'latest_source_change_seq', GREATEST(
                  CASE WHEN COALESCE(enqueue_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (enqueue_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(enqueue_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (enqueue_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END
                )
              )
            )
          ELSE public._pay_workbench_merge_targeted_scope_payload(
            COALESCE(enqueue_job.payload_json, '{}'::jsonb),
            COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
          )
        END,
        updated_at_utc = v_now
    WHERE NOT (
      UPPER(BTRIM(COALESCE(enqueue_job.status, ''))) = 'RUNNING'
      AND EXCLUDED.job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
    )
    RETURNING enqueue_job.id,
              enqueue_job.status,
              (xmax = 0)
    INTO v_job_id, v_job_status, v_job_was_inserted;

    GET DIAGNOSTICS v_insert_row_count = ROW_COUNT;

    IF COALESCE(v_insert_row_count, 0) = 0
       AND v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN
      SELECT running_delta_job.id
      INTO v_delta_active_running_job_id
      FROM public.banking_pay_workbench_jobs AS running_delta_job
      WHERE running_delta_job.session_id = v_session_id
        AND running_delta_job.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(running_delta_job.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
        AND UPPER(BTRIM(COALESCE(running_delta_job.status, ''))) = 'RUNNING'
        AND (
          COALESCE(running_delta_job.payload_json->>'normalised_delta_family_key', running_delta_job.payload_json->>'delta_family_key', running_delta_job.payload_json->>'delta_coalescing_key', '') = v_delta_coalescing_key
          OR (
            SELECT
              'WORKBENCH_CANDIDATE_DELTA_REFRESH_FAMILY'
              || ':session:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.session_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'source_session_id',
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'target_session_id',
                ''
              )), ''), 'none')
              || ':version:' || COALESCE(
                CASE
                  WHEN delta_family_values.session_version_text ~ '^-?[0-9]{1,18}$'
                    THEN delta_family_values.session_version_text::bigint
                  ELSE COALESCE(COALESCE(v_session_row.version, 0), 0)::bigint
                END,
                0
              )::text
              || ':projection_mode:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_mode_text, ''),
                NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                'DELTA'
              ))), ''), 'DELTA')
              || ':projection_class:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.projection_class_text, ''),
                NULLIF(v_projection_class, ''),
                'UNKNOWN'
              ))), ''), 'UNKNOWN')
              || ':refresh_scope:' || COALESCE(NULLIF(UPPER(BTRIM(COALESCE(
                NULLIF(delta_family_values.refresh_scope_kind_text, ''),
                NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), ''),
                CASE
                  WHEN jsonb_array_length(COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)) > 0
                    OR jsonb_array_length(COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)) > 0
                    THEN 'TARGETED_TIMESHEETS'
                  ELSE 'CANDIDATE_FULL_LIVE'
                END
              ))), ''), 'CANDIDATE_FULL_LIVE')
              || ':candidate:' || COALESCE(NULLIF(BTRIM(COALESCE(
                running_delta_job.candidate_id::text,
                (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'candidate_id',
                ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{candidate,id}'),
                ''
              )), ''), 'none')
              || ':timesheets:' || md5(
                COALESCE(delta_family_targeted.targeted_timesheet_ids_json, '[]'::jsonb)::text
                || ':'
                || COALESCE(delta_family_linked.linked_timesheet_ids_json, '[]'::jsonb)::text
              )
            FROM (
              SELECT
                NULLIF(BTRIM(COALESCE(
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'workbench_session_version',
                  (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'version',
                  CASE WHEN COALESCE(v_session_row.version, 0) IS NULL THEN NULL ELSE (COALESCE(v_session_row.version, 0))::text END,
                  '0'
                )), '') AS session_version_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'resolved_mode', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'mode', ''),
                  NULLIF(COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA'), ''),
                  'DELTA'
                ))) AS projection_mode_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'projection_class', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'classifier_projection_class', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{classifier_result,projection_class}'), ''),
                  NULLIF(v_projection_class, ''),
                  'UNKNOWN'
                ))) AS projection_class_text,
                UPPER(BTRIM(COALESCE(
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'refresh_scope_kind', ''),
                  NULLIF((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'scope_kind', ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{source_build,refresh_scope_kind}'), ''),
                  NULLIF(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>>'{preview_decisions_json,refresh_scope_kind}'), ''),
                  NULLIF(COALESCE(NULLIF(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE'), 'TARGETED_TIMESHEETS'), '')
                ))) AS refresh_scope_kind_text
            ) AS delta_family_values
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_targeted_sorted.timesheet_id_text ORDER BY delta_family_targeted_sorted.timesheet_id_text), '[]'::jsonb) AS targeted_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_targeted_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'targeted_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,targeted_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,targeted_timesheet_ids}')
                    WHEN NULLIF(BTRIM(COALESCE(
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                           (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id',
                           ''
                         )), '') IS NOT NULL
                      THEN jsonb_build_array(COALESCE(
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'targeted_timesheet_id',
                        (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'timesheet_id'
                      ))
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_targeted_raw(value)
                WHERE NULLIF(BTRIM(delta_family_targeted_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_targeted_sorted
            ) AS delta_family_targeted
            CROSS JOIN (
              SELECT COALESCE(jsonb_agg(delta_family_linked_sorted.timesheet_id_text ORDER BY delta_family_linked_sorted.timesheet_id_text), '[]'::jsonb) AS linked_timesheet_ids_json
              FROM (
                SELECT DISTINCT NULLIF(BTRIM(delta_family_linked_raw.value), '') AS timesheet_id_text
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'array'
                      THEN (COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids'
                    WHEN jsonb_typeof((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->'linked_timesheet_ids') = 'string'
                      THEN jsonb_build_array((COALESCE(running_delta_job.payload_json, '{}'::jsonb))->>'linked_timesheet_ids')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{source_build,linked_timesheet_ids}')
                    WHEN jsonb_typeof(((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')) = 'array'
                      THEN ((COALESCE(running_delta_job.payload_json, '{}'::jsonb))#>'{preview_decisions_json,linked_timesheet_ids}')
                    ELSE '[]'::jsonb
                  END
                ) AS delta_family_linked_raw(value)
                WHERE NULLIF(BTRIM(delta_family_linked_raw.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              ) AS delta_family_linked_sorted
            ) AS delta_family_linked
          ) = v_delta_coalescing_key
          OR running_delta_job.dedupe_key = v_dedupe_key
        )
      ORDER BY running_delta_job.started_at_utc ASC NULLS LAST, running_delta_job.created_at_utc ASC, running_delta_job.id ASC
      LIMIT 1;

      IF v_delta_active_running_job_id IS NOT NULL THEN
        v_dedupe_key := v_delta_coalescing_key || ':waiting_after_running:' || v_delta_active_running_job_id::text;

        v_payload_out_json := jsonb_strip_nulls(
          COALESCE(v_payload_out_json, '{}'::jsonb)
          || jsonb_build_object(
            'dedupe_key', v_dedupe_key,
            'delta_active_running_job_id', v_delta_active_running_job_id::text,
            'waiting_after_running_delta_job', true,
            'waiting_after_running_enqueued_at_utc', v_now::text,
            'delta_running_conflict_fail_closed', true
          )
        );

        INSERT INTO public.banking_pay_workbench_jobs AS waiting_enqueue_job (
          job_type,
          status,
          priority,
          run_at_utc,
          attempt_count,
          max_attempts,
          dedupe_key,
          snapshot_run_id,
          session_id,
          candidate_id,
          payload_json,
          economic_build_id,
          private_stage,
          private_cursor_kind,
          private_cursor_json,
          private_stage_version,
          created_at_utc,
          updated_at_utc,
          started_at_utc,
          completed_at_utc,
          failed_at_utc,
          last_error_json
        )
        VALUES (
          v_job_type,
          'QUEUED',
          43,
          v_now,
          0,
          8,
          v_dedupe_key,
          p_snapshot_run_id,
          v_session_id,
          p_candidate_id,
          v_payload_out_json,
          NULL::uuid,
          CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
          CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'BUILD_INITIALISE' ELSE NULL::text END,
          '{}'::jsonb,
          CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 1 ELSE NULL::integer END,
          v_now,
          v_now,
          NULL::timestamptz,
          NULL::timestamptz,
          NULL::timestamptz,
          NULL::jsonb
        )
        ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
        DO UPDATE
        SET priority = LEAST(waiting_enqueue_job.priority, EXCLUDED.priority),
            run_at_utc = LEAST(waiting_enqueue_job.run_at_utc, EXCLUDED.run_at_utc),
            payload_json = jsonb_strip_nulls(
              (public._pay_workbench_merge_targeted_scope_payload(
                COALESCE(waiting_enqueue_job.payload_json, '{}'::jsonb),
                COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
              ) - ARRAY[
                'cursor',
                'cursor_json',
                'next_cursor',
                'next_cursor_json',
                'source_cursor',
                'write_cursor_json',
                'candidate_cursor',
                'cursor_token',
                'has_cursor',
                'continuation_reason',
                'source_job_id',
                'continuation_source_job_id',
                'bounded_continuation_source_job_id',
                'parent_job_id',
                'next_phase',
                'write_phase',
                'source_result_summary',
                'source_result_has_more',
                'source_result_next_cursor_present'
              ]::text[])
              || jsonb_build_object(
                'cursor_json', '{}'::jsonb,
                'continuation', false,
                'phase', 'INIT_PREFLIGHT',
                'run_mode', 'LATEST_RERUN_AFTER_RUNNING',
                'waiting_after_running_delta_job', true,
                'latest_source_change_seq', GREATEST(
                  CASE WHEN COALESCE(waiting_enqueue_job.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (waiting_enqueue_job.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(waiting_enqueue_job.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (waiting_enqueue_job.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'latest_source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'latest_source_change_seq')::bigint ELSE 0::bigint END,
                  CASE WHEN COALESCE(EXCLUDED.payload_json->>'source_change_seq', '') ~ '^-?[0-9]{1,18}$' THEN (EXCLUDED.payload_json->>'source_change_seq')::bigint ELSE 0::bigint END
                )
              )
            ),
            updated_at_utc = v_now
        WHERE UPPER(BTRIM(COALESCE(waiting_enqueue_job.status, ''))) <> 'RUNNING'
        RETURNING waiting_enqueue_job.id,
                  waiting_enqueue_job.status,
                  (xmax = 0)
        INTO v_job_id, v_job_status, v_job_was_inserted;

        GET DIAGNOSTICS v_insert_row_count = ROW_COUNT;
      END IF;
    END IF;

    IF v_job_id IS NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_ACTIVE_DELTA_HEAD_CONFLICT'
        USING ERRCODE = '55000',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_ENQUEUE_CANDIDATE_REFRESH_ACTIVE_DELTA_HEAD_CONFLICT',
                'session_id', v_session_id::text,
                'candidate_id', p_candidate_id::text,
                'job_type', v_job_type,
                'delta_coalescing_key', v_delta_coalescing_key,
                'dedupe_key', v_dedupe_key,
                'active_running_job_id', CASE WHEN v_delta_active_running_job_id IS NULL THEN NULL ELSE v_delta_active_running_job_id::text END,
                'message', 'A running delta refresh head was detected during enqueue conflict handling and a safe waiting-head could not be created.'
              )::text;
    END IF;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    v_owner_resolution := CASE
      WHEN v_job_was_inserted THEN 'NEW_OWNER_CREATED'
      ELSE 'ACTIVE_ROOT_JOB_REUSED'
    END;
  END IF;

  UPDATE public.banking_pay_workbench_session_scope AS session_scope
  SET status = CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN 'DELTA_REFRESH_PENDING' ELSE 'SOURCE_BUILD_PENDING' END,
      pending_job_id = v_job_id,
      dirty = true,
      error_json = NULL::jsonb,
      updated_at_utc = v_now
  WHERE session_scope.session_id = v_session_id
    AND session_scope.candidate_id = p_candidate_id;

  /*
    The certified publisher requires one session-candidate state row carrying
    the exact session version and source sequence owned by this refresh.  A
    freshly seeded or force-reseeded scope can legitimately have no state row
    yet.  Establish that pending owner here, alongside the canonical job/scope
    enqueue, rather than allowing the completed build to reach publication
    with no state authority to adopt.

    Existing fragments are deliberately retained on conflict for display
    continuity, but they are no longer READY authority while this job owns the
    candidate.  A state row from a newer source sequence or session version is
    never overwritten by an older enqueue.
  */
  INSERT INTO public.banking_pay_workbench_session_candidate_state AS candidate_state (
    session_id,
    candidate_id,
    status,
    source_change_seq,
    session_version,
    pending_job_id,
    created_at_utc,
    updated_at_utc,
    last_error_json
  )
  VALUES (
    v_session_id,
    p_candidate_id,
    'PENDING',
    COALESCE(v_source_change_seq, 0),
    COALESCE(v_session_row.version, 1),
    v_job_id,
    v_now,
    v_now,
    NULL::jsonb
  )
  ON CONFLICT (session_id, candidate_id)
  DO UPDATE
  SET status = 'PENDING',
      source_change_seq = EXCLUDED.source_change_seq,
      session_version = EXCLUDED.session_version,
      pending_job_id = EXCLUDED.pending_job_id,
      updated_at_utc = v_now,
      last_error_json = NULL::jsonb
  WHERE candidate_state.source_change_seq <= EXCLUDED.source_change_seq
    AND candidate_state.session_version <= EXCLUDED.session_version;

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    v_job_id::text,
    CASE WHEN v_job_was_inserted THEN 'QUEUED' ELSE 'REUSED' END,
    NULL::jsonb,
    jsonb_build_object(
      'id', v_job_id::text,
      'job_type', v_job_type,
    'resolved_mode', v_resolved_mode,
      'status', v_job_status,
      'snapshot_run_id', p_snapshot_run_id::text,
      'session_id', v_session_id::text,
      'candidate_id', p_candidate_id::text,
      'dedupe_key', v_dedupe_key,
      'source_change_seq', COALESCE(v_source_change_seq, 0),
      'source_build_run_id', CASE WHEN v_source_build_run_id IS NULL THEN NULL ELSE v_source_build_run_id::text END,
      'projection_run_id', CASE WHEN v_projection_run_id IS NULL THEN NULL ELSE v_projection_run_id::text END,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_count', jsonb_array_length(COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb)),
      'linked_timesheet_count', jsonb_array_length(COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb)),
      'queue_identity_targeted_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
      'queue_identity_linked_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
      'source_build_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'line_work_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'delta_refresh_required', v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
      'delta_scope_merge_reused_existing', COALESCE(v_delta_merge_reused_existing, false),
      'delta_coalescing_key', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_key ELSE NULL::text END,
      'delta_coalescing_hash', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_hash ELSE NULL::text END,
      'delta_active_running_job_id', CASE WHEN v_delta_active_running_job_id IS NULL THEN NULL ELSE v_delta_active_running_job_id::text END,
      'delta_jobs_superseded_by_legacy', COALESCE(v_delta_jobs_superseded, 0)
    ),
    'WORKBENCH_CANDIDATE_REFRESH_JOB_ENQUEUE',
    v_actor_user_id
  );

  RETURN jsonb_strip_nulls(jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', v_job_type,
    'canonical_job_type', v_job_type,
    'resolved_mode', v_resolved_mode,
    'snapshot_run_id', p_snapshot_run_id::text,
    'session_id', v_session_id::text,
    'candidate_id', p_candidate_id::text,
    'session_version', COALESCE(v_session_row.version, 0),
    'source_change_seq', COALESCE(v_source_change_seq, 0),
    'registry_source_change_seq', COALESCE(v_registry_source_change_seq_after, v_source_change_seq, 0),
    'registry_sequence_synchronised', COALESCE(v_registry_sequence_synchronised, false),
    'source_build_run_id', CASE WHEN v_source_build_run_id IS NULL THEN NULL ELSE v_source_build_run_id::text END,
    'projection_run_id', CASE WHEN v_projection_run_id IS NULL THEN NULL ELSE v_projection_run_id::text END,
    'dedupe_key', v_dedupe_key,
    'reason', v_reason,
    'refresh_scope_kind', v_refresh_scope_kind,
    'targeted_timesheet_ids', COALESCE(v_targeted_timesheet_ids_json, '[]'::jsonb),
    'linked_timesheet_ids', COALESCE(v_linked_timesheet_ids_json, '[]'::jsonb),
    'queue_identity_targeted_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_targeted_timesheet_ids_json, v_targeted_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
    'queue_identity_linked_timesheet_ids', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(v_queue_identity_linked_timesheet_ids_json, v_linked_timesheet_ids_json, '[]'::jsonb) ELSE NULL::jsonb END,
    'pay_channel_scope', v_pay_channel_scope,
    'source_build_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'line_work_required', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
    'line_work_only', false,
    'delta_refresh_required', v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH',
    'scope_status', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN 'DELTA_REFRESH_PENDING' ELSE 'SOURCE_BUILD_PENDING' END,
    'projection_mode', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN COALESCE(NULLIF(v_projection_mode, 'LEGACY'), 'DELTA') ELSE NULL::text END,
    'projection_class', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_projection_class ELSE NULL::text END,
    'full_snapshot_job', false,
    'reused', NOT v_job_was_inserted,
    'coalesced', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' AND NOT v_job_was_inserted,
    'new_owner_created', v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' AND v_job_was_inserted,
    'owner_resolution', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_owner_resolution ELSE NULL::text END,
    'authority_fingerprint_version', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_authority_fingerprint_version ELSE NULL::integer END,
    'required_physical_publication_contract_version', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_required_physical_publication_contract_version ELSE NULL::integer END,
    'authority_fingerprint', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_authority_fingerprint ELSE NULL::text END,
    'owner_root_job_id', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_job_id::text ELSE NULL::text END,
    'requested_coverage', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'FULL_CANDIDATE' ELSE NULL::text END,
    'owner_coverage', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'FULL_CANDIDATE' ELSE NULL::text END,
    'owner_source_build_run_id', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_source_build_run_id::text ELSE NULL::text END,
    'delta_scope_merge_reused_existing', COALESCE(v_delta_merge_reused_existing, false),
    'delta_coalescing_key', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_key ELSE NULL::text END,
    'delta_coalescing_hash', CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' THEN v_delta_coalescing_hash ELSE NULL::text END,
    'delta_active_running_job_id', CASE WHEN v_delta_active_running_job_id IS NULL THEN NULL ELSE v_delta_active_running_job_id::text END,
    'delta_jobs_superseded_by_legacy', COALESCE(v_delta_jobs_superseded, 0),
    'classifier_result', v_classifier_result
  ));
END;
$function$;

ALTER FUNCTION public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_process_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_patch_json jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_previous_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_timesheet_patch jsonb := COALESCE(p_timesheet_patch_json, '{}'::jsonb);
  v_tsfin_patch jsonb := COALESCE(p_tsfin_patch_json, '{}'::jsonb);
  v_forbidden_tsfin_patch_keys text[] := ARRAY[
    'candidate_id','client_id','pay_method','candidate_assignment','basis','policy_snapshot_json','rate_source_refs_json',
    'normal_hours','unsocial_hours','saturday_hours','sunday_hours','bank_holiday_hours','sleep_in_units','on_call_units','mileage_units','expenses_units',
    'total_hours','total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','net_delta_ex_vat','normal_pay_rate','unsocial_pay_rate','saturday_pay_rate','sunday_pay_rate','bank_holiday_pay_rate',
    'normal_charge_rate','unsocial_charge_rate','saturday_charge_rate','sunday_charge_rate','bank_holiday_charge_rate','mileage_pay_rate','mileage_charge_rate','expenses_pay','expenses_charge',
    'has_rate_issue','has_pay_channel_issue','hours_day','hours_night','hours_sat','hours_sun','hours_bh','pay_day','pay_night','pay_sat','pay_sun','pay_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh',
    'expenses_pay_ex_vat','expenses_charge_ex_vat','mileage_pay_ex_vat','mileage_charge_ex_vat','travel_pay_ex_vat','travel_charge_ex_vat','accommodation_pay_ex_vat','accommodation_charge_ex_vat','other_pay_ex_vat','other_charge_ex_vat','additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat'
  ];
  v_financial_affecting_timesheet_patch_keys text[] := ARRAY[
    'worked_start_iso','worked_end_iso','break_start_iso','break_end_iso','break_minutes','worked_minutes','actual_schedule_json','additional_units_week','additional_units_per_day','scheduled_start_iso','scheduled_end_iso','week_ending_date','worked_date','work_date'
  ];
  v_patch_key text := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_candidate_electronic_context boolean :=
    COALESCE(current_setting('cloudtms.candidate_electronic_finalise', true), '') <> ''
    AND private._candidate_feature_enabled_current_v1('candidate_daily_finalisation');
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;
  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'message', 'p_actor_user_id is required.');
  END IF;
  IF jsonb_typeof(v_timesheet_patch) <> 'object' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_MUST_BE_OBJECT', 'message', 'p_timesheet_patch_json must be a JSON object.');
  END IF;
  IF jsonb_typeof(v_tsfin_patch) <> 'object' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_MUST_BE_OBJECT', 'message', 'p_tsfin_patch_json must be a JSON object.');
  END IF;

  IF NOT v_candidate_electronic_context
     AND v_timesheet_patch ?| ARRAY[
       'submission_mode','auth_name','auth_job_title','r2_nurse_key','r2_auth_key',
       'img_sha256_nurse','img_sha256_auth','candidate_workflow_id',
       'candidate_workflow_generation','candidate_manager_approved_at_utc'
     ] THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process',
      'error_code', 'CANDIDATE_ELECTRONIC_FIELDS_FORBIDDEN',
      'message', 'Candidate electronic document fields require the bounded Candidate App finalisation authority.');
  END IF;

  FOREACH v_patch_key IN ARRAY v_forbidden_tsfin_patch_keys LOOP
    IF v_tsfin_patch ? v_patch_key THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_FORBIDDEN_FIELD', 'message', 'Cannot process: TSFIN patch contains an authoritative or financial field.', 'field', v_patch_key, 'timesheet_id', p_timesheet_id);
    END IF;
  END LOOP;

  FOREACH v_patch_key IN ARRAY v_financial_affecting_timesheet_patch_keys LOOP
    IF v_timesheet_patch ? v_patch_key THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_REQUIRES_RECALCULATION', 'message', 'Cannot process while changing worked time, schedule, break, work date, or additional units. Save and recalculate the row before processing.', 'field', v_patch_key, 'timesheet_id', p_timesheet_id);
    END IF;
  END LOOP;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_requested_ts.booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    v_current_ts := v_requested_ts;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_MOVED', 'message', 'Timesheet has moved to a newer current row.', 'requested_timesheet_id', p_timesheet_id, 'expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id, 'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.sheet_scope <> 'DAILY'::public.timesheet_scope_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual process only applies to DAILY sheets.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_ts.submission_mode <> 'MANUAL'::public.submission_mode_enum
     AND NOT (v_candidate_electronic_context
       AND v_current_ts.submission_mode = 'ELECTRONIC'::public.submission_mode_enum) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before processing.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ALREADY_AUTHORISED', 'message', 'This timesheet is already authorised.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_previous_status := v_current_tsfin.processing_status;
  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_LOCKED_BY_INVOICE', 'message', 'Timesheet is invoice-locked; cannot process.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_previous_status <> 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_UNPROCESSED', 'message', 'Timesheet is not in UNPROCESSED state.', 'current_timesheet_id', v_current_ts.timesheet_id, 'previous_status', v_previous_status::text);
  END IF;
  IF v_current_tsfin.candidate_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_MISSING', 'message', 'Cannot process: candidate is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_tsfin.client_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CLIENT_MISSING', 'message', 'Cannot process: client is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF COALESCE(v_current_tsfin.has_rate_issue, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'RATE_ISSUE', 'message', 'Cannot process: TSFIN has a rate issue.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF COALESCE(v_current_tsfin.has_pay_channel_issue, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'PAY_CHANNEL_ISSUE', 'message', 'Cannot process: TSFIN has a pay channel issue.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_current_tsfin.pay_method, '')), '') IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'PAY_METHOD_MISSING', 'message', 'Cannot process: pay method is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_tsfin.candidate_assignment = 'UNASSIGNED'::public.candidate_assignment_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_ASSIGNMENT_UNRESOLVED', 'message', 'Cannot process: candidate assignment is unresolved in TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, v_timesheet_patch ->> 'backend_row_signature', v_timesheet_patch ->> 'row_signature', v_tsfin_patch ->> 'backend_row_signature', v_tsfin_patch ->> 'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'ROW_SIGNATURE_MISMATCH', 'message', 'Timesheet changed after it was loaded. Refresh the row and try again.', 'current_timesheet_id', v_current_ts.timesheet_id, 'expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature);
  END IF;

  UPDATE public.timesheets AS ts
     SET reference_number = CASE WHEN v_timesheet_patch ? 'reference_number' THEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') ELSE ts.reference_number END,
         reference_set_at = CASE
           WHEN v_timesheet_patch ? 'reference_number' THEN
             CASE WHEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') IS NULL THEN NULL::timestamp with time zone
                  WHEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(ts.reference_number, '')), '') THEN v_now
                  ELSE COALESCE(ts.reference_set_at, v_now)
             END
           ELSE ts.reference_set_at
         END,
         submission_mode = CASE WHEN v_candidate_electronic_context
           THEN 'ELECTRONIC'::public.submission_mode_enum ELSE ts.submission_mode END,
         auth_name = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'auth_name'),'') ELSE ts.auth_name END,
         auth_job_title = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'auth_job_title'),'') ELSE ts.auth_job_title END,
         r2_nurse_key = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'r2_nurse_key'),'') ELSE ts.r2_nurse_key END,
         r2_auth_key = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'r2_auth_key'),'') ELSE ts.r2_auth_key END,
         img_sha256_nurse = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'img_sha256_nurse'),'') ELSE ts.img_sha256_nurse END,
         img_sha256_auth = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'img_sha256_auth'),'') ELSE ts.img_sha256_auth END,
         candidate_workflow_id = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_workflow_id','')::uuid ELSE ts.candidate_workflow_id END,
         candidate_workflow_generation = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_workflow_generation','')::integer ELSE ts.candidate_workflow_generation END,
         candidate_manager_approved_at_utc = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_manager_approved_at_utc','')::timestamptz
           ELSE ts.candidate_manager_approved_at_utc END,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         processed_by_user_id = p_actor_user_id,
         processed_at_utc = v_now,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_PROCESSED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'previous_processing_status', v_previous_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'new_processing_status', v_new_status::text, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'DAILY_MANUAL_PROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'daily_manual_process',
    'processed', true,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'timesheet_financials_id', v_current_tsfin.id,
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_status', v_previous_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'status_transition', jsonb_build_object('from', v_previous_status::text, 'to', v_new_status::text, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id),
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'count_deltas', jsonb_build_object('unprocessed', -1, 'processed', 1),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials'), 'timesheet_id', v_current_ts.timesheet_id)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'LOCK_TIMEOUT', 'message', 'The timesheet is currently locked by another operation.', 'timesheet_id', p_timesheet_id);
  END IF;
  RAISE;
END;
$function$;

ALTER FUNCTION public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text) TO service_role;

CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  RETURN public.timesheet_daily_manual_unprocess_atomic(
    p_timesheet_id => p_timesheet_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => p_now_utc,
    p_expected_row_signature => NULL::text
  );
END;
$function$;

ALTER FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  TO service_role;

-- Upgrade-history convergence: interrupted installs can leave an
-- earlier functional repeatable newer than the unchanged final browser ACL
-- closure. Reassert the exact final service-only ACL for the four proved
-- pre-existing authorities and every additive Banking v2 RPC.
REVOKE ALL ON FUNCTION
  public.pay_timesheet_summary_pay_state_refresh_trigger(),
  public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer),
  public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb),
  public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer),
  public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid,jsonb),
  public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer),
  public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text),
  public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer),
  public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text),
  public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer),
  public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer),
  public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text),
  public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb),
  public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text),
  public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb),
  public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)
FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
  public.pay_timesheet_summary_pay_state_refresh_trigger(),
  public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer),
  public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb),
  public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer),
  public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid,jsonb),
  public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer),
  public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text),
  public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer),
  public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text),
  public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer),
  public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer),
  public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text),
  public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb),
  public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text),
  public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb),
  public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)
TO service_role;

NOTIFY pgrst, 'reload schema';

commit;
