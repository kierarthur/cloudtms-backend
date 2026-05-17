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
    WHERE summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL
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
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
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
        COALESCE(lightweight_rows.paid_at_utc, lightweight_rows.pay_paid_at_utc) IS NOT NULL
        OR COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
      ) AS locked_calc,
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
    WHERE (v_week_ending_from IS NULL OR lightweight_rows.week_ending_date >= v_week_ending_from)
      AND (v_week_ending_to IS NULL OR lightweight_rows.week_ending_date <= v_week_ending_to)
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
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
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.requires_authorisation_calc = TRUE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.qr_unsigned_blocked_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
      ) AS can_bulk_unauthorise_calc,
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
        CASE WHEN decision_rows.locked_calc THEN 'locked_or_paid' ELSE NULL::text END,
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
        decision_rows.row_signature_calc,
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
  ),
  manual_rows AS MATERIALIZED (
    SELECT payload_rows.row_json
    FROM payload_rows
    WHERE UPPER(COALESCE(payload_rows.row_json->>'route_family', '')) = 'MANUAL_NON_QR'
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
      AND COALESCE((manual_rows.row_json->>'can_unprocess')::boolean, FALSE) = TRUE
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

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
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
  ));
END;
$function$;














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

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

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
    WHERE summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL
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
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
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
        COALESCE(lightweight_rows.paid_at_utc, lightweight_rows.pay_paid_at_utc) IS NOT NULL
        OR COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
      ) AS locked_calc,
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
        AND classified_rows.qr_unsigned_blocked_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
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
        CASE WHEN decision_rows.locked_calc THEN 'locked_or_paid' ELSE NULL::text END,
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
        decision_rows.row_signature_calc,
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
  ),
  eligible_rows_before_classification AS MATERIALIZED (
    SELECT payload_rows.row_json
    FROM payload_rows
    WHERE NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(payload_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
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

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
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
  ));
END;
$function$;










CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_decision_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_q text := NULL;
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_q_like text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
BEGIN
  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  BEGIN
    IF v_candidate_id_text IS NOT NULL THEN
      v_candidate_id := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id := NULL;
  END;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  BEGIN
    IF v_client_id_text IS NOT NULL THEN
      v_client_id := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id := NULL;
  END;

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  END IF;

  v_dataset_mode := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  IF v_dataset_mode NOT IN ('PROCESS', 'AUTHORISE', 'ROW_CONTEXT') THEN
    v_dataset_mode := NULL;
  END IF;

  v_period_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'period_type', v_filters->>'periodType', v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_period_filter NOT IN ('DAILY', 'WEEKLY') THEN
    v_period_filter := NULL;
  END IF;

  v_date_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), '');
  v_date_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), '');
  v_week_ending_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), '');

  BEGIN
    IF v_date_from_text IS NOT NULL THEN
      v_date_from := v_date_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_from := NULL;
  END;

  BEGIN
    IF v_date_to_text IS NOT NULL THEN
      v_date_to := v_date_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_to := NULL;
  END;

  BEGIN
    IF v_week_ending_text IS NOT NULL THEN
      v_week_ending_date := v_week_ending_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_date := NULL;
  END;

  RETURN QUERY
  WITH base_scope AS (
    SELECT vb0.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_filters) AS vb0
  ),
  base_ids AS (
    SELECT DISTINCT
      base_scope.timesheet_id,
      base_scope.contract_week_id
    FROM base_scope
  ),
  tf_ranked AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.processing_status,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.invoice_breakdown_json,
      tf0.external_source_rows_json,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.actual_schedule_json,
      tf0.additional_units_json,
      tf0.updated_at,
      tf0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tf
        WHERE bs_tf.timesheet_id = tf0.timesheet_id
      )
  ),
  tf_latest AS (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.timesheet_version,
      tf1.basis,
      tf1.is_stale,
      tf1.stale_reason,
      tf1.worked_start_iso,
      tf1.worked_end_iso,
      tf1.break_start_iso,
      tf1.break_end_iso,
      tf1.break_minutes,
      tf1.candidate_id,
      tf1.client_id,
      tf1.role,
      tf1.band,
      tf1.pay_method,
      tf1.policy_snapshot_json,
      tf1.processing_status,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.computed_at_utc,
      tf1.locked_by_invoice_id,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.pay_on_hold_reason,
      tf1.processed_by_user_id,
      tf1.processed_at_utc,
      tf1.authorised_by_user_id,
      tf1.authorised_at_utc,
      tf1.invoice_breakdown_json,
      tf1.external_source_rows_json,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_charge_ex_vat,
      tf1.expenses_description,
      tf1.expenses_evidence_r2_key,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.mileage_evidence_r2_key,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat,
      tf1.actual_schedule_json,
      tf1.additional_units_json,
      tf1.updated_at,
      tf1.created_at
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.pre_validated,
      tv0.validated_at_utc,
      tv0.updated_at,
      tv0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tv
        WHERE bs_tv.timesheet_id = tv0.timesheet_id
      )
  ),
  tv_latest AS (
    SELECT
      tv1.id,
      tv1.timesheet_id,
      tv1.status,
      tv1.reason_code,
      tv1.pre_validated,
      tv1.validated_at_utc,
      tv1.updated_at,
      tv1.created_at
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  te_ranked AS (
    SELECT
      te0.id,
      te0.timesheet_id,
      te0.kind,
      te0.display_name,
      te0.storage_key,
      te0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY te0.timesheet_id
        ORDER BY
          CASE
            WHEN UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET' THEN 0
            ELSE 1
          END ASC,
          te0.created_at ASC NULLS LAST,
          te0.id ASC
      ) AS rn
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te
        WHERE bs_te.timesheet_id = te0.timesheet_id
      )
  ),
  te_primary AS (
    SELECT
      te1.id,
      te1.timesheet_id,
      te1.kind,
      te1.display_name,
      te1.storage_key
    FROM te_ranked AS te1
    WHERE te1.rn = 1
  ),
  te_agg AS (
    SELECT
      te2.timesheet_id,
      COUNT(te2.id)::integer AS attached_evidence_count,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TIMESHEET'), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'MILEAGE'), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TRAVEL'), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence,
      MAX(te2.created_at) AS evidence_updated_at
    FROM public.timesheet_evidence AS te2
    WHERE te2.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te2
        WHERE bs_te2.timesheet_id = te2.timesheet_id
      )
    GROUP BY te2.timesheet_id
  ),
  mq_ranked AS (
    SELECT
      mq0.id,
      mq0.r2_key,
      mq0.original_filename,
      mq0.mime_type,
      mq0.uploaded_at_utc,
      mq0.last_rotation_deg,
      mq0.meta_json,
      NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') AS contract_week_id_text,
      CASE
        WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
          THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET'))
        ELSE 'OTHER'
      END AS staged_kind_upper,
      ROW_NUMBER() OVER (
        PARTITION BY NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
        ORDER BY mq0.uploaded_at_utc ASC NULLS LAST, mq0.id ASC
      ) AS rn
    FROM public.manual_timesheet_queue AS mq0
    WHERE UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_mq
        WHERE bs_mq.contract_week_id::text = NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
      )
  ),
  mq_primary AS (
    SELECT
      mq1.id,
      mq1.r2_key,
      mq1.original_filename,
      mq1.mime_type,
      mq1.last_rotation_deg,
      mq1.contract_week_id_text,
      mq1.staged_kind_upper
    FROM mq_ranked AS mq1
    WHERE mq1.rn = 1
  ),
  mq_agg AS (
    SELECT
      mq2.contract_week_id_text,
      COUNT(mq2.id)::integer AS queue_staged_count,
      MAX(mq2.uploaded_at_utc) AS queue_updated_at,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence
    FROM mq_ranked AS mq2
    GROUP BY mq2.contract_week_id_text
  ),
  raw_rows AS (
    SELECT
      vb.timesheet_id AS timesheet_id,
      vb.timesheet_status::text AS timesheet_status,
      vb.week_ending_date AS week_ending_date,
      vb.booking_id AS booking_id,
      vb.occupant_key_norm AS occupant_key_norm,
      vb.hospital_norm AS hospital_norm,
      vb.sheet_scope::text AS sheet_scope,
      vb.submission_mode::text AS submission_mode,
      vb.authorised_at_server AS authorised_at_server,
      COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) AS candidate_id,
      COALESCE(vb.client_id, tf.client_id, ct.client_id) AS client_id,
      COALESCE(vb.pay_method, tf.pay_method, cand.pay_method) AS pay_method,
      COALESCE(tf.processing_status::text, vb.processing_status::text) AS processing_status,
      COALESCE(tf.basis::text, vb.basis::text) AS basis,
      COALESCE(tf.total_hours, vb.total_hours) AS total_hours,
      COALESCE(tf.total_pay_ex_vat, vb.total_pay_ex_vat) AS total_pay_ex_vat,
      COALESCE(tf.total_charge_ex_vat, vb.total_charge_ex_vat) AS total_charge_ex_vat,
      COALESCE(tf.margin_ex_vat, vb.margin_ex_vat) AS margin_ex_vat,
      COALESCE(tf.paid_at_utc, vb.paid_at_utc) AS paid_at_utc,
      COALESCE(tf.pay_on_hold, vb.pay_on_hold, FALSE) AS pay_on_hold,
      vb.ready_to_pay AS ready_to_pay,
      COALESCE(tf.locked_by_invoice_id, vb.locked_by_invoice_id) AS locked_by_invoice_id,
      COALESCE(NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_name,
      COALESCE(NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_display_name,
      cand.first_name AS candidate_first_name,
      cand.last_name AS candidate_surname,
      cand.email AS candidate_email,
      COALESCE(cand.opt_in_email, TRUE) AS candidate_opt_in_email,
      COALESCE(NULLIF(BTRIM(vb.client_name), ''), NULLIF(BTRIM(cli.name), ''), vb.hospital_norm) AS client_name,
      COALESCE(NULLIF(BTRIM(cli.name), ''), NULLIF(BTRIM(vb.client_name), ''), vb.hospital_norm) AS client_display_name,
      vb.nhsp_shift_count AS nhsp_shift_count,
      vb.nhsp_shift_included_count AS nhsp_shift_included_count,
      vb.nhsp_shift_deferred_count AS nhsp_shift_deferred_count,
      COALESCE(tv.status::text, vb.validation_status::text) AS validation_status,
      vb.summary_stage AS summary_stage,
      CASE
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        )
        AND (
          UPPER(COALESCE(vb.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT')
          OR COALESCE(vb.client_is_nhsp, FALSE) = TRUE
          OR COALESCE(ct.is_nhsp, FALSE) = TRUE
          OR UPPER(COALESCE(tf.basis::text, vb.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
        ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        )
        AND (
          UPPER(COALESCE(vb.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT')
          OR COALESCE(vb.client_autoprocess_hr, FALSE) = TRUE
          OR COALESCE(ct.autoprocess_hr, FALSE) = TRUE
          OR UPPER(COALESCE(tf.basis::text, vb.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
        ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE vb.route_type
      END AS route_type,
      vb.contract_week_id AS contract_week_id,
      vb.contract_week_ending_date AS contract_week_ending_date,
      COALESCE(vb.contract_week_status::text, cw.status::text) AS contract_week_status,
      COALESCE(vb.additional_seq, cw.additional_seq, 0) AS additional_seq,
      (
        COALESCE(ts.is_adjustment, FALSE) = TRUE
        OR COALESCE(vb.is_adjustment, FALSE) = TRUE
        OR COALESCE(cw.is_adjustment, FALSE) = TRUE
        OR COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
      ) AS is_adjustment,
      vb.qr_status::text AS vb_qr_status,
      ts.qr_status::text AS ts_qr_status,
      ts.qr_token AS qr_token,
      ts.qr_generated_at AS qr_generated_at,
      ts.qr_scanned_at AS qr_scanned_at,
      ts.qr_last_sent_hash AS qr_last_sent_hash,
      ts.qr_signed_hash AS qr_signed_hash,
      ts.qr_signed_at_utc AS qr_signed_at_utc,
      vb.qr_token AS vb_qr_token,
      vb.qr_generated_at AS vb_qr_generated_at,
      vb.qr_scanned_at AS vb_qr_scanned_at,
      COALESCE(vb.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(vb.has_pay_adjustments, FALSE) AS has_pay_adjustments,
      COALESCE(vb.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(vb.client_requires_hr, FALSE) AS client_requires_hr,
      COALESCE(vb.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(vb.client_pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(vb.client_invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(vb.client_hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(vb.client_ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(vb.client_is_nhsp, FALSE) AS client_is_nhsp,
      COALESCE(vb.hr_validation_required_for_invoice, FALSE) AS hr_validation_required_for_invoice,
      COALESCE(tf.has_rate_issue, vb.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf.has_pay_channel_issue, vb.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      COALESCE(tf.hr_crosscheck_status, vb.hr_crosscheck_status) AS hr_crosscheck_status,
      COALESCE(tf.hr_crosscheck_issues, vb.hr_crosscheck_issues) AS hr_crosscheck_issues,
      COALESCE(tf.external_source_rows_json, vb.external_source_rows_json) AS external_source_rows_json,
      COALESCE(vb.issue_codes, ARRAY[]::text[]) AS issue_codes,
      tf.worked_start_iso AS worked_start_iso,
      COALESCE(ts.contract_id, cw.contract_id, ct.id) AS contract_id,
      ct.role AS contract_role,
      ct.band AS contract_band,
      ct.display_site AS contract_display_site,
      ct.ward_hint AS contract_ward_hint,
      ct.std_schedule_json AS contract_std_schedule_json,
      ct.additional_rates_json AS contract_additional_rates_json,
      ct.mileage_pay_rate AS contract_mileage_pay_rate,
      ct.mileage_charge_rate AS contract_mileage_charge_rate,
      ct.default_submission_mode::text AS contract_default_submission_mode,
      ct.weekly_timesheet_source AS contract_weekly_timesheet_source,
      COALESCE(ct.is_nhsp, FALSE) AS contract_is_nhsp,
      COALESCE(ct.autoprocess_hr, FALSE) AS contract_autoprocess_hr,
      COALESCE(ct.requires_hr, FALSE) AS contract_requires_hr,
      COALESCE(ct.no_timesheet_required, FALSE) AS contract_no_timesheet_required,
      COALESCE(ct.daily_calc_of_invoices, FALSE) AS contract_daily_calc_of_invoices,
      COALESCE(ct.group_nightsat_sunbh, FALSE) AS contract_group_nightsat_sunbh,
      COALESCE(ct.is_ad_hoc, FALSE) AS contract_is_ad_hoc,
      vb.candidate_hint_text AS candidate_hint_text,
      COALESCE(tf.expenses_pay_ex_vat, vb.expenses_pay_ex_vat) AS expenses_pay_ex_vat,
      COALESCE(tf.expenses_description, vb.expenses_description) AS expenses_description,
      COALESCE(tf.mileage_units, vb.mileage_units) AS mileage_units,
      COALESCE(tf.mileage_pay_rate, vb.mileage_pay_rate) AS mileage_pay_rate,
      COALESCE(tf.mileage_charge_rate, vb.mileage_charge_rate) AS mileage_charge_rate,
      COALESCE(tf.mileage_pay_ex_vat, vb.mileage_pay_ex_vat) AS mileage_pay_ex_vat,
      COALESCE(tf.travel_pay_ex_vat, vb.travel_pay_ex_vat) AS travel_pay_ex_vat,
      COALESCE(tf.travel_charge_ex_vat, vb.travel_charge_ex_vat) AS travel_charge_ex_vat,
      COALESCE(tf.accommodation_pay_ex_vat, vb.accommodation_pay_ex_vat) AS accommodation_pay_ex_vat,
      COALESCE(tf.accommodation_charge_ex_vat, vb.accommodation_charge_ex_vat) AS accommodation_charge_ex_vat,
      COALESCE(tf.other_pay_ex_vat, vb.other_pay_ex_vat) AS other_pay_ex_vat,
      COALESCE(tf.other_charge_ex_vat, vb.other_charge_ex_vat) AS other_charge_ex_vat,
      vb.invoice_segments_total AS invoice_segments_total,
      vb.invoice_segments_locked AS invoice_segments_locked,
      vb.invoice_segments_unlocked AS invoice_segments_unlocked,
      vb.invoice_segment_stage AS invoice_segment_stage,
      vb.tools_stage AS tools_stage,
      vb.processing_status_display AS processing_status_display,
      vb.invoice_is_paid AS invoice_is_paid,
      vb.refs_block_invoicing AS refs_block_invoicing,
      vb.refs_block_issuing_invoices AS refs_block_issuing_invoices,
      vb.refs_block_invoice_and_issuing AS refs_block_invoice_and_issuing,
      COALESCE(vb.pay_icon_code, 'NONE') AS pay_icon_code,
      vb.pay_status_code AS pay_status_code,
      vb.pay_paid_at_utc AS pay_paid_at_utc,
      COALESCE(vb.net_delta_ex_vat, 0::numeric) AS net_delta_ex_vat,
      ts.version AS timesheet_version,
      ts.is_current AS timesheet_is_current,
      ts.created_at AS timesheet_created_at,
      ts.updated_at AS timesheet_updated_at,
      ts.worked_start_iso AS timesheet_worked_start_iso,
      ts.worked_end_iso AS timesheet_worked_end_iso,
      ts.scheduled_start_iso AS timesheet_scheduled_start_iso,
      ts.scheduled_end_iso AS timesheet_scheduled_end_iso,
      ts.break_start_iso AS timesheet_break_start_iso,
      ts.break_end_iso AS timesheet_break_end_iso,
      ts.break_minutes AS timesheet_break_minutes,
      ts.worked_minutes AS timesheet_worked_minutes,
      ts.reference_number AS reference_number,
      ts.r2_nurse_key AS r2_nurse_key,
      ts.r2_auth_key AS r2_auth_key,
      ts.manual_pdf_r2_key AS manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
      ts.generated_pdf_at_utc AS generated_pdf_at_utc,
      ts.actual_schedule_json AS timesheet_actual_schedule_json,
      tf.actual_schedule_json AS tsfin_actual_schedule_json,
      ts.additional_units_week AS additional_units_week,
      ts.additional_units_per_day AS additional_units_per_day,
      ts.day_references_json AS day_references_json,
      ts.parent_timesheet_id AS parent_timesheet_id,
      ts.adjustment_origin AS adjustment_origin,
      ts.correction_id AS correction_id,
      ts.correction_kind AS correction_kind,
      cw.id AS cw_id,
      cw.status::text AS cw_status,
      cw.submission_mode_snapshot::text AS cw_submission_mode_snapshot,
      cw.timesheet_id AS cw_timesheet_id,
      cw.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
      cw.day_entries_json AS contract_week_day_entries_json,
      cw.totals_json AS contract_week_totals_json,
      cw.planned_schedule_json AS contract_week_planned_schedule_json,
      cw.updated_at AS contract_week_updated_at,
      cw.created_at AS contract_week_created_at,
      tf.id AS tsfin_id,
      tf.timesheet_version AS tsfin_timesheet_version,
      tf.is_stale AS tsfin_is_stale,
      tf.stale_reason AS tsfin_stale_reason,
      tf.computed_at_utc AS tsfin_computed_at_utc,
      tf.processed_by_user_id AS processed_by_user_id,
      tf.processed_at_utc AS processed_at_utc,
      pu.display_name AS processed_by_display,
      tf.authorised_by_user_id AS authorised_by_user_id,
      tf.authorised_at_utc AS authorised_at_utc,
      au.display_name AS authorised_by_display,
      tf.invoice_breakdown_json AS invoice_breakdown_json,
      tf.policy_snapshot_json AS policy_snapshot_json,
      tf.updated_at AS tsfin_updated_at,
      tv.updated_at AS validation_updated_at,
      tv.pre_validated AS validation_pre_validated,
      tv.reason_code AS validation_reason_code,
      tv.validated_at_utc AS validation_validated_at_utc,
      te.evidence_updated_at AS evidence_updated_at,
      te.attached_evidence_count AS attached_evidence_count,
      COALESCE(te.has_timesheet_evidence, FALSE) AS ev_timesheet,
      COALESCE(te.has_mileage_evidence, FALSE) AS ev_mileage,
      COALESCE(te.has_travel_evidence, FALSE) AS ev_travel,
      COALESCE(te.has_accommodation_evidence, FALSE) AS ev_accommodation,
      COALESCE(te.has_other_evidence, FALSE) AS ev_other,
      tep.id AS primary_evidence_id,
      tep.kind AS primary_evidence_kind,
      tep.display_name AS primary_evidence_display_name,
      tep.storage_key AS primary_evidence_storage_key,
      COALESCE(mqa.queue_staged_count, 0) AS queue_staged_count,
      mqa.queue_updated_at AS queue_updated_at,
      COALESCE(mqa.has_staged_timesheet_evidence, FALSE) AS staged_ev_timesheet,
      COALESCE(mqa.has_staged_mileage_evidence, FALSE) AS staged_ev_mileage,
      COALESCE(mqa.has_staged_travel_evidence, FALSE) AS staged_ev_travel,
      COALESCE(mqa.has_staged_accommodation_evidence, FALSE) AS staged_ev_accommodation,
      COALESCE(mqa.has_staged_other_evidence, FALSE) AS staged_ev_other,
      mqp.id AS primary_queue_id,
      mqp.r2_key AS primary_queue_r2_key,
      mqp.original_filename AS primary_queue_original_filename,
      mqp.mime_type AS primary_queue_mime_type,
      mqp.last_rotation_deg AS primary_queue_rotation_degrees,
      mqp.staged_kind_upper AS primary_queue_staged_kind,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_electronic
        WHERE hist_electronic.booking_id = vb.booking_id
          AND hist_electronic.is_current = FALSE
          AND UPPER(COALESCE(hist_electronic.submission_mode::text, '')) = 'ELECTRONIC'
      ) AS has_historical_electronic,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_pending
        WHERE hist_qr_pending.booking_id = vb.booking_id
          AND hist_qr_pending.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_pending.qr_status::text, '')) = 'PENDING'
          AND hist_qr_pending.qr_generated_at IS NOT NULL
          AND hist_qr_pending.qr_scanned_at IS NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_pending.qr_token, '')), '') IS NOT NULL
      ) AS has_historical_qr_pending,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_signed
        WHERE hist_qr_signed.booking_id = vb.booking_id
          AND hist_qr_signed.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_signed.qr_status::text, '')) = 'USED'
          AND hist_qr_signed.qr_scanned_at IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_signed.manual_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_historical_qr_signed,
      EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array' THEN tf.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_value)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) AS has_segment_invoice_lock
    FROM base_scope AS vb
    LEFT JOIN public.timesheets AS ts
      ON ts.timesheet_id = vb.timesheet_id
     AND ts.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw
      ON cw.id = vb.contract_week_id
    LEFT JOIN public.contracts AS ct
      ON ct.id = COALESCE(ts.contract_id, cw.contract_id)
    LEFT JOIN tf_latest AS tf
      ON tf.timesheet_id = vb.timesheet_id
    LEFT JOIN tv_latest AS tv
      ON tv.timesheet_id = vb.timesheet_id
    LEFT JOIN te_agg AS te
      ON te.timesheet_id = vb.timesheet_id
    LEFT JOIN te_primary AS tep
      ON tep.timesheet_id = vb.timesheet_id
    LEFT JOIN mq_agg AS mqa
      ON mqa.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN mq_primary AS mqp
      ON mqp.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN public.candidates AS cand
      ON cand.id = COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id)
    LEFT JOIN public.clients AS cli
      ON cli.id = COALESCE(vb.client_id, tf.client_id, ct.client_id)
    LEFT JOIN public.tms_users AS pu
      ON pu.id = tf.processed_by_user_id
    LEFT JOIN public.tms_users AS au
      ON au.id = tf.authorised_by_user_id
    WHERE (v_candidate_id IS NULL OR COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) = v_candidate_id)
      AND (v_client_id IS NULL OR COALESCE(vb.client_id, tf.client_id, ct.client_id) = v_client_id)
      AND (v_timesheet_ids IS NULL OR vb.timesheet_id = ANY(v_timesheet_ids))
      AND (v_contract_week_ids IS NULL OR vb.contract_week_id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR CASE
             WHEN vb.timesheet_id IS NOT NULL THEN 'timesheet:' || vb.timesheet_id::text
             WHEN vb.contract_week_id IS NOT NULL THEN 'contract_week:' || vb.contract_week_id::text
             ELSE ''
           END = ANY(v_row_keys)
      )
      AND (
        v_q_like IS NULL
        OR CONCAT_WS(
             ' ',
             vb.candidate_name,
             cand.display_name,
             cand.first_name,
             cand.last_name,
             cand.email,
             vb.client_name,
             cli.name,
             vb.booking_id,
             vb.occupant_key_norm,
             vb.hospital_norm,
             ts.ward_norm,
             ct.display_site,
             ct.ward_hint,
             ts.reference_number,
             vb.route_type,
             vb.summary_stage,
             vb.tools_stage,
             vb.processing_status_display,
             vb.validation_status::text,
             vb.timesheet_id::text,
             vb.contract_week_id::text,
             COALESCE(ts.contract_id, cw.contract_id, ct.id)::text,
             vb.issue_codes::text
           ) ILIKE v_q_like ESCAPE E'\\'
      )
  ),
  classified AS (
    SELECT
      rr.*,
      UPPER(COALESCE(rr.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(rr.sheet_scope, '')) AS sheet_scope_upper,
      UPPER(COALESCE(rr.submission_mode, rr.cw_submission_mode_snapshot, '')) AS submission_mode_upper,
      UPPER(COALESCE(rr.vb_qr_status, rr.ts_qr_status, '')) AS qr_status_upper,
      (
        COALESCE(rr.is_adjustment, FALSE)
        OR COALESCE(rr.additional_seq, 0) > 0
        OR rr.parent_timesheet_id IS NOT NULL
        OR rr.correction_id IS NOT NULL
        OR rr.correction_kind IS NOT NULL
      ) AS is_adjustment_or_additional,
      (
        COALESCE(rr.vb_qr_status, rr.ts_qr_status) IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_token, rr.vb_qr_token, '')), '') IS NOT NULL
        OR rr.qr_generated_at IS NOT NULL
        OR rr.vb_qr_generated_at IS NOT NULL
        OR rr.qr_scanned_at IS NOT NULL
        OR rr.vb_qr_scanned_at IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_last_sent_hash, '')), '') IS NOT NULL
        OR COALESCE(rr.vb_qr_status, rr.ts_qr_status) IN ('PENDING', 'USED', 'EXPIRED', 'CANCELLED')
      ) AS is_qr_route,
      CASE
        WHEN UPPER(COALESCE(rr.sheet_scope, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(rr.sheet_scope, ''))
        WHEN UPPER(COALESCE(rr.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type
    FROM raw_rows AS rr
  ),
  routed AS (
    SELECT
      cl.*,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
        ) THEN 'NHSP'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN NULL::text
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family,
      (
        cl.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(cl.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required
    FROM classified AS cl
  ),
  decisions AS (
    SELECT
      rt.*,
      CASE
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' AND rt.route_subfamily = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification,
      CASE
        WHEN rt.route_family = 'MANUAL_NON_QR' THEN 1
        WHEN rt.route_family = 'QR' THEN 2
        WHEN rt.route_family = 'ELECTRONIC' THEN 3
        ELSE 99
      END AS timesheet_type_sort_key,
      (
        rt.timesheet_id IS NULL
        AND rt.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.contract_week_status, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed,
      (
        rt.timesheet_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.processing_status, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed,
      (
        COALESCE(rt.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(rt.paid_at_utc, NULL) IS NOT NULL
        OR COALESCE(rt.has_segment_invoice_lock, FALSE) = TRUE
        OR COALESCE(rt.invoice_segments_locked, 0) > 0
        OR COALESCE(rt.invoice_is_paid, FALSE) = TRUE
      ) AS locked,
      (
        rt.authorised_at_server IS NOT NULL
        OR rt.authorised_at_utc IS NOT NULL
      ) AS is_authorised,
      (
        UPPER(COALESCE(rt.processing_status, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(rt.client_requires_hr, FALSE) = TRUE
          AND COALESCE(rt.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(rt.processing_status, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation,
      (
        UPPER(COALESCE(rt.validation_status, '')) IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_satisfied,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'PENDING'
        AND (
          (
            NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NOT NULL
            AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NOT NULL
          )
          OR NULLIF(BTRIM(COALESCE(rt.qr_last_sent_hash, '')), '') IS NOT NULL
        )
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS qr_pending_awaiting_signature,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'USED'
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NOT NULL
      ) AS qr_signed_returned,
      (
        UPPER(COALESCE(rt.submission_mode, rt.cw_submission_mode_snapshot, '')) = 'MANUAL'
        AND NULLIF(BTRIM(COALESCE(rt.qr_status_upper, '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NULL
        AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NULL
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS is_manual_only,
      (
        COALESCE(rt.contract_default_submission_mode, '') = 'ELECTRONIC'
        OR UPPER(COALESCE(rt.policy_snapshot_json->>'default_submission_mode', '')) = 'ELECTRONIC'
      ) AS supports_electronic_submission,
      CASE
        WHEN COALESCE(rt.is_adjustment_or_additional, FALSE) = TRUE THEN
          CASE
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) = 'MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) LIKE 'IMPORT\_%' ESCAPE '\' OR rt.correction_kind IS NOT NULL OR rt.correction_id IS NOT NULL THEN 'NHSP Adjustment'
            ELSE 'Manual Adjustment'
          END
        WHEN rt.route_type_upper = 'DAILY_ELECTRONIC' THEN 'Daily Electronic'
        WHEN rt.route_type_upper = 'DAILY_MANUAL' THEN 'Daily Manual'
        WHEN rt.route_type_upper = 'WEEKLY_ELECTRONIC' THEN 'Weekly Electronic'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL' THEN 'Weekly Manual'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP' THEN 'Weekly NHSP'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'Weekly NHSP Adjustment'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'Weekly HealthRoster'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'Weekly HealthRoster Adjustment'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Weekly Manual Adjustment'
        ELSE 'Unknown'
      END AS route_display,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(rt.client_is_nhsp, FALSE) = TRUE OR COALESCE(rt.contract_is_nhsp, FALSE) = TRUE THEN 'NHSP'
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') THEN 'HEALTHROSTER'
        WHEN rt.period_type = 'WEEKLY' THEN 'NONE'
        ELSE NULL::text
      END AS contract_weekly_mode,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') AND COALESCE(rt.client_no_timesheet_required, FALSE) = TRUE THEN 'CREATE'
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') THEN 'VERIFY'
        ELSE ''
      END AS contract_hr_weekly_behaviour
    FROM routed AS rt
  ),
  final_rows AS (
    SELECT
      dc.*,
      (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) AS is_unprocessed,
      (
        dc.is_adjustment_or_additional = TRUE
        AND dc.contract_week_id IS NOT NULL
        AND dc.route_family = 'MANUAL_NON_QR'
        AND COALESCE(dc.locked, FALSE) = FALSE
        AND COALESCE(dc.is_authorised, FALSE) = FALSE
        AND (dc.timesheet_id IS NULL OR dc.is_real_row_unprocessed = TRUE)
      ) AS supports_unprocessed_expense_draft,
      (
        dc.is_adjustment_or_additional = TRUE
        AND COALESCE(dc.total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          dc.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(dc.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(dc.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
      ) AS keep_additional_manual_adjustment_schedule_empty,
      CASE WHEN (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) THEN 'UNPROCESSED' ELSE 'PROCESSED' END AS bulk_process_bucket,
      (dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE) AS hr_validation_awaiting,
      (
        dc.qr_pending_awaiting_signature = TRUE
        OR UPPER(COALESCE(dc.processing_status, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(dc.issue_codes)
      ) AS qr_unsigned_blocked,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.requires_authorisation = TRUE
        AND dc.is_authorised = FALSE
        AND (
          dc.qr_pending_awaiting_signature = FALSE
          AND UPPER(COALESCE(dc.processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE'
          AND NOT ('Awaiting signed QR timesheet' = ANY(dc.issue_codes))
        )
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_authorise,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = TRUE
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_unauthorise,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND NOT (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_unprocess,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_process,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_save,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = TRUE
      ) AS evidence_document_locked,
      CASE
        WHEN NOT (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR')) THEN 'INVALID_TIMESHEET_CONTEXT'
        WHEN dc.locked_by_invoice_id IS NOT NULL THEN 'INVOICE_LOCKED'
        WHEN dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0 THEN 'INVOICE_SEGMENT_LOCKED'
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_AUTHORITATIVE_ROUTE'
        ELSE NULL::text
      END AS evidence_lock_reason,
      (
        dc.locked = TRUE
        OR dc.is_authorised = TRUE
        OR dc.route_family <> 'MANUAL_NON_QR'
      ) AS review_only,
      (
        dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND (
          (dc.period_type = 'WEEKLY' AND dc.contract_week_id IS NOT NULL)
          OR (dc.period_type = 'DAILY' AND dc.timesheet_id IS NOT NULL)
        )
        AND (
          dc.route_family = 'IMPORT_AUTHORITATIVE'
          OR (dc.route_family = 'MANUAL_NON_QR' AND dc.submission_mode_upper = 'MANUAL' AND dc.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND (
          (dc.period_type = 'WEEKLY' AND dc.submission_mode_upper = 'ELECTRONIC' AND (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL))
          OR (dc.period_type = 'DAILY' AND dc.submission_mode_upper = 'ELECTRONIC' AND dc.timesheet_id IS NOT NULL)
        )
      ) AS can_switch_to_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.period_type = 'WEEKLY'
        AND dc.timesheet_id IS NULL
        AND dc.contract_week_id IS NOT NULL
        AND dc.submission_mode_upper = 'MANUAL'
      ) AS can_switch_planned_week_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.has_historical_electronic = TRUE
      ) AS can_revert_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
      ) AS can_allow_qr_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
        AND dc.supports_electronic_submission = TRUE
      ) AS can_allow_electronic_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.is_qr_route = TRUE
        AND NULLIF(BTRIM(COALESCE(dc.qr_status_upper, '')), '') IS NOT NULL
      ) AS can_convert_qr_to_manual_only,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_pending = TRUE
      ) AS can_restore_qr_pending,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_signed = TRUE
      ) AS can_restore_qr_signed,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_SOURCE_ONLY'
        WHEN dc.route_family = 'ELECTRONIC' AND dc.generated_pdf_at_utc IS NULL AND (NULLIF(BTRIM(COALESCE(dc.r2_nurse_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(dc.r2_auth_key, '')), '') IS NOT NULL) THEN 'SIGNATURES_ONLY'
        WHEN dc.timesheet_id IS NOT NULL AND (dc.manual_pdf_r2_key IS NOT NULL OR dc.generated_pdf_at_utc IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL AND (dc.uploaded_pdf_r2_key IS NOT NULL OR dc.primary_queue_r2_key IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        ELSE 'NONE'
      END AS primary_left_pane_mode,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR'
          AND COALESCE(
            NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'ELECTRONIC'
          AND COALESCE(
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
            NULLIF(BTRIM(dc.manual_pdf_r2_key), '')
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'MANUAL_NON_QR'
          AND dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.manual_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_evidence_storage_key), '') IS NOT NULL THEN 'EVIDENCE'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.uploaded_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_queue_r2_key), '') IS NOT NULL THEN 'QUEUE'
        ELSE NULL::text
      END AS primary_artifact_source,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR' THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'ELECTRONIC' THEN COALESCE(
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'MANUAL_NON_QR' AND dc.timesheet_id IS NOT NULL THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.timesheet_id IS NOT NULL THEN NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL THEN COALESCE(NULLIF(BTRIM(dc.uploaded_pdf_r2_key), ''), NULLIF(BTRIM(dc.primary_queue_r2_key), ''))
        ELSE NULL::text
      END AS primary_artifact_storage_key,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN dc.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_text,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN 'timesheet:' || dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN 'contract_week:' || dc.contract_week_id::text
        ELSE ''
      END AS row_key,
      MD5(CONCAT_WS('|',
        COALESCE(dc.timesheet_id::text, ''),
        COALESCE(dc.contract_week_id::text, ''),
        COALESCE(dc.timesheet_version::text, ''),
        COALESCE(dc.timesheet_updated_at::text, ''),
        COALESCE(dc.contract_week_updated_at::text, ''),
        COALESCE(dc.tsfin_updated_at::text, ''),
        COALESCE(dc.validation_updated_at::text, ''),
        COALESCE(dc.evidence_updated_at::text, ''),
        COALESCE(dc.queue_updated_at::text, ''),
        COALESCE(dc.processing_status, ''),
        COALESCE(dc.summary_stage, ''),
        COALESCE(dc.tools_stage, ''),
        COALESCE(dc.authorised_at_server::text, ''),
        COALESCE(dc.authorised_at_utc::text, ''),
        COALESCE(dc.is_authorised::text, ''),
        COALESCE(dc.locked::text, ''),
        COALESCE(dc.locked_by_invoice_id::text, ''),
        COALESCE(dc.has_segment_invoice_lock::text, ''),
        COALESCE(dc.invoice_segments_locked::text, ''),
        COALESCE(dc.invoice_is_paid::text, ''),
        COALESCE(dc.paid_at_utc::text, ''),
        COALESCE(dc.route_family, ''),
        COALESCE(dc.route_subfamily, ''),
        COALESCE(dc.submission_mode, ''),
        COALESCE(dc.cw_submission_mode_snapshot, ''),
        COALESCE(dc.qr_status_upper, ''),
        COALESCE(dc.qr_generated_at::text, ''),
        COALESCE(dc.qr_scanned_at::text, ''),
        COALESCE(dc.qr_signed_at_utc::text, ''),
        COALESCE(dc.validation_status, ''),
        COALESCE(dc.validation_pre_validated::text, ''),
        COALESCE(dc.validation_reason_code, ''),
        COALESCE(dc.validation_validated_at_utc::text, ''),
        COALESCE((dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE)::text, ''),
        COALESCE(dc.issue_codes::text, ''),
        COALESCE(dc.attached_evidence_count::text, ''),
        COALESCE(dc.ev_timesheet::text, ''),
        COALESCE(dc.ev_mileage::text, ''),
        COALESCE(dc.ev_travel::text, ''),
        COALESCE(dc.ev_accommodation::text, ''),
        COALESCE(dc.ev_other::text, ''),
        COALESCE(dc.primary_evidence_id::text, ''),
        COALESCE(dc.primary_evidence_kind, ''),
        COALESCE(dc.queue_staged_count::text, ''),
        COALESCE(dc.staged_ev_timesheet::text, ''),
        COALESCE(dc.staged_ev_mileage::text, ''),
        COALESCE(dc.staged_ev_travel::text, ''),
        COALESCE(dc.staged_ev_accommodation::text, ''),
        COALESCE(dc.staged_ev_other::text, ''),
        COALESCE(dc.primary_queue_staged_kind, ''),
        COALESCE(dc.primary_evidence_storage_key, ''),
        COALESCE(dc.manual_pdf_r2_key, ''),
        COALESCE(dc.uploaded_pdf_r2_key, ''),
        COALESCE(dc.generated_pdf_at_utc::text, ''),
        COALESCE(dc.primary_queue_r2_key, ''),
        COALESCE(dc.client_requires_hr::text, ''),
        COALESCE(dc.client_no_timesheet_required::text, ''),
        COALESCE(dc.client_autoprocess_hr::text, ''),
        COALESCE(dc.client_is_nhsp::text, ''),
        COALESCE(dc.total_hours::text, ''),
        COALESCE(dc.total_pay_ex_vat::text, ''),
        COALESCE(dc.total_charge_ex_vat::text, ''),
        COALESCE(dc.margin_ex_vat::text, '')
      )) AS row_signature
    FROM decisions AS dc
  ),
  artifact_rows AS (
    SELECT
      fr.*,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_artifact_source = 'EVIDENCE' AND fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_artifact_source = 'QUEUE' AND fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        ELSE NULL::text
      END AS primary_artifact_id,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'TIMESHEET'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_evidence_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_source = 'QUEUE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_queue_staged_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET'
        WHEN fr.primary_evidence_kind IS NOT NULL THEN UPPER(fr.primary_evidence_kind)
        WHEN fr.primary_queue_id IS NOT NULL THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_queue_staged_kind), ''), 'TIMESHEET'))
        ELSE NULL::text
      END AS primary_artifact_kind,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'Timesheet PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN COALESCE(NULLIF(BTRIM(fr.primary_evidence_display_name), ''), 'Evidence')
        WHEN fr.primary_artifact_source = 'QUEUE' THEN COALESCE(NULLIF(BTRIM(fr.primary_queue_original_filename), ''), 'Staged evidence')
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'Timesheet PDF'
        WHEN fr.primary_evidence_display_name IS NOT NULL THEN fr.primary_evidence_display_name
        WHEN fr.primary_queue_original_filename IS NOT NULL THEN fr.primary_queue_original_filename
        ELSE NULL::text
      END AS primary_artifact_display_name,
      CASE
        WHEN fr.primary_left_pane_mode = 'IMPORT_SOURCE_ONLY' THEN 'IMPORT_TABLE'
        WHEN fr.primary_left_pane_mode = 'SIGNATURES_ONLY' THEN 'SIGNATURES'
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN 'FILE'
        WHEN fr.primary_artifact_source = 'QUEUE' THEN 'PDF'
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'PDF'
        WHEN fr.primary_evidence_storage_key IS NOT NULL THEN 'FILE'
        ELSE NULL::text
      END AS primary_artifact_preview_mode,
      (
        COALESCE(fr.ev_timesheet, FALSE)
        OR COALESCE(fr.ev_mileage, FALSE)
        OR COALESCE(fr.ev_travel, FALSE)
        OR COALESCE(fr.ev_accommodation, FALSE)
        OR COALESCE(fr.ev_other, FALSE)
        OR COALESCE(fr.staged_ev_timesheet, FALSE)
        OR COALESCE(fr.staged_ev_mileage, FALSE)
        OR COALESCE(fr.staged_ev_travel, FALSE)
        OR COALESCE(fr.staged_ev_accommodation, FALSE)
        OR COALESCE(fr.staged_ev_other, FALSE)
        OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_any_evidence,
      JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', (COALESCE(fr.ev_timesheet, FALSE) OR COALESCE(fr.staged_ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(fr.ev_timesheet, FALSE) OR COALESCE(fr.staged_ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
        JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', (COALESCE(fr.ev_mileage, FALSE) OR COALESCE(fr.staged_ev_mileage, FALSE)), 'has_evidence', (COALESCE(fr.ev_mileage, FALSE) OR COALESCE(fr.staged_ev_mileage, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', (COALESCE(fr.ev_travel, FALSE) OR COALESCE(fr.staged_ev_travel, FALSE)), 'has_evidence', (COALESCE(fr.ev_travel, FALSE) OR COALESCE(fr.staged_ev_travel, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', (COALESCE(fr.ev_accommodation, FALSE) OR COALESCE(fr.staged_ev_accommodation, FALSE)), 'has_evidence', (COALESCE(fr.ev_accommodation, FALSE) OR COALESCE(fr.staged_ev_accommodation, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', (COALESCE(fr.ev_other, FALSE) OR COALESCE(fr.staged_ev_other, FALSE)), 'has_evidence', (COALESCE(fr.ev_other, FALSE) OR COALESCE(fr.staged_ev_other, FALSE)))
      ) AS evidence_badges
    FROM final_rows AS fr
  )
  SELECT
    (
      JSONB_BUILD_OBJECT(
        'id', COALESCE(ar.timesheet_id::text, ar.contract_week_id::text),
        'row_key', ar.row_key,
        'stable_row_id', ar.stable_row_id_text,
        'timesheet_id', ar.timesheet_id,
        'current_timesheet_id', ar.timesheet_id,
        'requested_timesheet_id', ar.timesheet_id,
        'expected_timesheet_id', ar.timesheet_id,
        'contract_week_id', ar.contract_week_id,
        'contract_id', ar.contract_id,
        'booking_id', ar.booking_id,
        'timesheet_version', ar.timesheet_version,
        'row_signature', ar.row_signature,
        'updated_at', COALESCE(ar.timesheet_updated_at, ar.contract_week_updated_at, ar.tsfin_updated_at),
        'is_current', COALESCE(ar.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'candidate_id', ar.candidate_id,
        'candidate_name', ar.candidate_name,
        'candidate_display_name', ar.candidate_display_name,
        'candidate_first_name', ar.candidate_first_name,
        'candidate_surname', ar.candidate_surname,
        'occupant_key_norm', ar.occupant_key_norm,
        'client_id', ar.client_id,
        'client_name', ar.client_name,
        'client_display_name', ar.client_display_name,
        'hospital_name', ar.hospital_norm,
        'hospital_norm', ar.hospital_norm,
        'site_name', ar.contract_display_site,
        'ward_name', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'ward_norm', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'booking_ref', ar.booking_id,
        'external_ref', ar.reference_number
      )
      || JSONB_BUILD_OBJECT(
        'week_ending_date', ar.week_ending_date,
        'contract_week_ending_date', ar.contract_week_ending_date,
        'work_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE ar.week_ending_date END,
        'shift_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'period_type', ar.period_type,
        'timesheet_type_sort_key', ar.timesheet_type_sort_key,
        'summary_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.summary_stage END,
        'tools_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.tools_stage END,
        'processing_status', ar.processing_status,
        'processing_status_display', CASE WHEN ar.is_unprocessed THEN 'Unprocessed' ELSE ar.processing_status_display END,
        'processed_at_utc', ar.processed_at_utc,
        'processed_by_user_id', ar.processed_by_user_id,
        'processed_by_display', ar.processed_by_display,
        'authorised_at_server', ar.authorised_at_server,
        'authorised_at_utc', ar.authorised_at_utc,
        'authorised_by_user_id', ar.authorised_by_user_id,
        'authorised_by_display', ar.authorised_by_display,
        'locked_by_invoice_id', ar.locked_by_invoice_id,
        'paid_at_utc', ar.paid_at_utc,
        'invoice_segments_total', ar.invoice_segments_total,
        'invoice_segments_locked', ar.invoice_segments_locked,
        'invoice_segments_unlocked', ar.invoice_segments_unlocked,
        'invoice_segment_stage', ar.invoice_segment_stage,
        'invoice_is_paid', ar.invoice_is_paid,
        'ready_to_pay', ar.ready_to_pay,
        'pay_icon_code', ar.pay_icon_code,
        'pay_status_code', ar.pay_status_code,
        'pay_paid_at_utc', ar.pay_paid_at_utc,
        'pay_on_hold', ar.pay_on_hold
      )
      || JSONB_BUILD_OBJECT(
        'total_hours', ar.total_hours,
        'total_pay_ex_vat', ar.total_pay_ex_vat,
        'total_charge_ex_vat', ar.total_charge_ex_vat,
        'margin_ex_vat', ar.margin_ex_vat,
        'net_delta_ex_vat', ar.net_delta_ex_vat,
        'route_type', ar.route_type,
        'route_display', ar.route_display,
        'submission_mode', ar.submission_mode,
        'submission_mode_snapshot', ar.cw_submission_mode_snapshot,
        'sheet_scope', ar.sheet_scope,
        'timesheet_scope', ar.sheet_scope,
        'route_family', ar.route_family,
        'route_subfamily', ar.route_subfamily,
        'underlying_channel_family', ar.underlying_channel_family,
        'is_adjustment', ar.is_adjustment_or_additional,
        'additional_seq', ar.additional_seq,
        'actual_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.timesheet_actual_schedule_json, ar.tsfin_actual_schedule_json, '[]'::jsonb) END,
        'planned_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.contract_week_planned_schedule_json, '[]'::jsonb) END,
        'contract_week_totals_json', COALESCE(ar.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', ar.keep_additional_manual_adjustment_schedule_empty,
        'keep_additional_manual_adjustment_schedule_empty', ar.keep_additional_manual_adjustment_schedule_empty,
        '__suppressStandardScheduleFallback', ar.keep_additional_manual_adjustment_schedule_empty,
        '__keepAdditionalManualAdjustmentScheduleEmpty', ar.keep_additional_manual_adjustment_schedule_empty,
        'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', ar.compare_block_required,
        'bulk_process_bucket', ar.bulk_process_bucket,
        'bulk_authorise_classification', ar.bulk_authorise_classification,
        'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
        'has_timesheet', ar.timesheet_id IS NOT NULL,
        'is_contract_week_only', ar.timesheet_id IS NULL AND ar.contract_week_id IS NOT NULL,
        'is_real_timesheet_row', ar.timesheet_id IS NOT NULL,
        'requires_authorisation', ar.requires_authorisation,
        'is_authorised', ar.is_authorised,
        'locked', ar.locked,
        'review_only', ar.review_only,
        'can_save', ar.can_save,
        'can_process', ar.can_process,
        'can_unprocess', ar.can_unprocess,
        'can_edit_timesheet_data', ar.can_edit_timesheet_data,
        'can_manage_evidence', ar.can_manage_evidence,
        'can_bulk_authorise', ar.can_bulk_authorise,
        'can_bulk_unauthorise', ar.can_bulk_unauthorise,
        'can_add_additional_manual', ar.can_add_additional_manual,
        'supportsUnprocessedExpenseDraft', ar.supports_unprocessed_expense_draft,
        'supports_unprocessed_expense_draft', ar.supports_unprocessed_expense_draft,
        'expense_storage_target', CASE
          WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_DRAFT'
          WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TSFIN'
          ELSE NULL::text
        END,
        'expense_evidence_storage_target', CASE
          WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
          WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
          ELSE NULL::text
        END
      )
      || JSONB_BUILD_OBJECT(
        'is_qr', ar.is_qr_route,
        'qr_status', NULLIF(ar.qr_status_upper, ''),
        'qr_generated_at', COALESCE(ar.qr_generated_at, ar.vb_qr_generated_at),
        'qr_scanned_at', COALESCE(ar.qr_scanned_at, ar.vb_qr_scanned_at),
        'qr_signed_at_utc', ar.qr_signed_at_utc,
        'qr_unsigned_blocked', ar.qr_unsigned_blocked,
        'qr_signed_returned', ar.qr_signed_returned,
        'can_allow_qr_again', ar.can_allow_qr_again,
        'can_allow_electronic_again', ar.can_allow_electronic_again,
        'can_switch_to_manual', ar.can_switch_to_manual,
        'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
        'can_revert_to_electronic', ar.can_revert_to_electronic,
        'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
        'can_restore_qr_pending', ar.can_restore_qr_pending,
        'can_restore_qr_signed', ar.can_restore_qr_signed,
        'qr_email_can_send_now', (ar.can_allow_qr_again OR NULLIF(ar.qr_status_upper, '') = 'PENDING') AND NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL AND COALESCE(ar.candidate_opt_in_email, TRUE) = TRUE,
        'qr_email_recipient_available', NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL,
        'client_requires_hr', ar.client_requires_hr,
        'client_autoprocess_hr', ar.client_autoprocess_hr,
        'client_no_timesheet_required', ar.client_no_timesheet_required,
        'client_is_nhsp', ar.client_is_nhsp,
        'hr_validation_required_for_invoice', ar.hr_validation_required_for_invoice,
        'validation_status', ar.validation_status,
        'validation_pre_validated', COALESCE(ar.validation_pre_validated, FALSE),
        'hr_validation_satisfied', ar.hr_validation_satisfied,
        'hr_validation_awaiting', ar.hr_validation_awaiting,
        'issue_codes', ar.issue_codes,
        'has_deviation_marker', (
          COALESCE(ar.has_rate_issue, FALSE)
          OR COALESCE(ar.has_pay_channel_issue, FALSE)
          OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
          OR COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0
          OR (
            NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
            AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
          )
        ),
        'deviation_marker_reason', CASE
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          WHEN COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0 THEN 'HR_CROSSCHECK_ISSUE'
          ELSE NULL::text
        END,
        'nhsp_highlight_red', (
          (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE)
          AND (
            COALESCE(ar.has_rate_issue, FALSE)
            OR COALESCE(ar.has_pay_channel_issue, FALSE)
            OR COALESCE(ar.nhsp_shift_deferred_count, 0) > 0
            OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
            OR (
              NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
              AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
            )
          )
        ),
        'nhsp_highlight_reason', CASE
          WHEN NOT (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE) THEN NULL::text
          WHEN COALESCE(ar.nhsp_shift_deferred_count, 0) > 0 THEN 'DEFERRED_NHSP_SHIFTS'
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          ELSE NULL::text
        END,
        'nhsp_deviation_pct', CASE
          WHEN COALESCE(ar.nhsp_shift_count, 0) > 0 THEN ROUND((COALESCE(ar.nhsp_shift_deferred_count, 0)::numeric / NULLIF(ar.nhsp_shift_count::numeric, 0)) * 100, 2)
          ELSE NULL::numeric
        END,
        'nhsp_is_ad_hoc', ar.contract_is_ad_hoc
      )
      || JSONB_BUILD_OBJECT(
        'has_any_evidence', ar.has_any_evidence,
        'evidence_badges', ar.evidence_badges,
        'primary_artifact_id', ar.primary_artifact_id,
        'primary_artifact_kind', ar.primary_artifact_kind,
        'primary_artifact_display_name', ar.primary_artifact_display_name,
        'primary_artifact_storage_key', ar.primary_artifact_storage_key,
        'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
        'primary_artifact_source', ar.primary_artifact_source,
        'primary_left_pane_mode', ar.primary_left_pane_mode,
        'manual_pdf_r2_key', ar.manual_pdf_r2_key,
        'uploaded_pdf_r2_key', ar.uploaded_pdf_r2_key,
        'generated_pdf_at_utc', ar.generated_pdf_at_utc,
        'manual_pdf_rotation_degrees', ar.manual_pdf_rotation_degrees,
        'attached_evidence_count', COALESCE(ar.attached_evidence_count, 0),
        'queue_staged_count', COALESCE(ar.queue_staged_count, 0),
        'evidence_document_locked', ar.evidence_document_locked,
        'evidence_lock_reason', ar.evidence_lock_reason,
        'contract_additional_rates_json', ar.contract_additional_rates_json,
        'contract_std_schedule_json', ar.contract_std_schedule_json,
        'contract_mileage_pay_rate', ar.contract_mileage_pay_rate,
        'contract_mileage_charge_rate', ar.contract_mileage_charge_rate,
        'contract_weekly_mode', ar.contract_weekly_mode,
        'contract_hr_weekly_behaviour', ar.contract_hr_weekly_behaviour,
        'contract_role', ar.contract_role,
        'contract_band', ar.contract_band,
        'reference_number', ar.reference_number,
        'has_pay_adjustments', ar.has_pay_adjustments,
        'pay_adjustment_count', ar.pay_adjustment_count
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', JSONB_BUILD_OBJECT(
          'can_restore_qr_pending', ar.can_restore_qr_pending,
          'can_restore_qr_signed', ar.can_restore_qr_signed,
          'can_revert_to_electronic', ar.can_revert_to_electronic,
          'can_allow_qr_again', ar.can_allow_qr_again,
          'can_allow_electronic_again', ar.can_allow_electronic_again,
          'can_switch_to_manual', ar.can_switch_to_manual,
          'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
          'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
          'supports_electronic_submission', ar.supports_electronic_submission,
          'is_manual_only', ar.is_manual_only,
          'is_adjustment', ar.is_adjustment_or_additional,
          'locked_by_invoice', ar.locked_by_invoice_id IS NOT NULL,
          'paid', ar.paid_at_utc IS NOT NULL,
          'is_unprocessed', ar.is_unprocessed,
          'route_family', ar.route_family,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'can_save', ar.can_save,
          'can_process', ar.can_process,
          'can_edit_timesheet_data', ar.can_edit_timesheet_data,
          'can_manage_evidence', ar.can_manage_evidence,
          'supportsUnprocessedExpenseDraft', ar.supports_unprocessed_expense_draft,
          'supports_unprocessed_expense_draft', ar.supports_unprocessed_expense_draft,
          'expense_storage_target', CASE
            WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_DRAFT'
            WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END,
          'compare_block_required', ar.compare_block_required,
          'has_primary_artifact', ar.primary_artifact_storage_key IS NOT NULL,
          'primary_left_pane_mode', ar.primary_left_pane_mode,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'electronic_artifact_exists', ar.route_family = 'ELECTRONIC' AND ar.primary_artifact_storage_key IS NOT NULL,
          'healthroster_compare_required', ar.compare_block_required
        ),
        'bulk_authorise', JSONB_BUILD_OBJECT(
          'classification', ar.bulk_authorise_classification,
          'section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'can_authorise', ar.can_bulk_authorise,
          'can_unauthorise', ar.can_bulk_unauthorise,
          'requires_authorisation', ar.requires_authorisation,
          'is_authorised', ar.is_authorised,
          'hr_validation_awaiting', ar.hr_validation_awaiting,
          'qr_unsigned_blocked', ar.qr_unsigned_blocked,
          'qr_signed_returned', ar.qr_signed_returned
        ),
        'artifact_hints', JSONB_BUILD_OBJECT(
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'has_any_evidence', ar.has_any_evidence,
          'evidence_badges', ar.evidence_badges
        ),
        'row_patch', JSONB_BUILD_OBJECT(
          'previous_row_key', NULL::text,
          'row_key', ar.row_key,
          'new_row_key', ar.row_key,
          'stable_row_id', ar.stable_row_id_text,
          'timesheet_id', ar.timesheet_id,
          'current_timesheet_id', ar.timesheet_id,
          'expected_timesheet_id', ar.timesheet_id,
          'contract_week_id', ar.contract_week_id,
          'is_adjustment', ar.is_adjustment_or_additional,
          'additional_seq', ar.additional_seq,
          'route_type', ar.route_type,
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'actual_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.timesheet_actual_schedule_json, ar.tsfin_actual_schedule_json, '[]'::jsonb) END,
          'planned_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.contract_week_planned_schedule_json, '[]'::jsonb) END,
          'total_hours', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN 0::numeric ELSE ar.total_hours END,
          'suppress_standard_schedule_fallback', ar.keep_additional_manual_adjustment_schedule_empty,
          'keep_additional_manual_adjustment_schedule_empty', ar.keep_additional_manual_adjustment_schedule_empty,
          '__suppressStandardScheduleFallback', ar.keep_additional_manual_adjustment_schedule_empty,
          '__keepAdditionalManualAdjustmentScheduleEmpty', ar.keep_additional_manual_adjustment_schedule_empty,
          'row_signature', ar.row_signature,
          'previous_row_signature', NULL::text,
          'bulk_process_bucket', ar.bulk_process_bucket,
          'previous_bulk_process_bucket', NULL::text,
          'bulk_authorise_classification', ar.bulk_authorise_classification,
          'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'previous_bulk_authorise_section', NULL::text,
          'processing_status', ar.processing_status,
          'summary_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.summary_stage END,
          'tools_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.tools_stage END,
          'is_authorised', ar.is_authorised,
          'locked', ar.locked,
          'can_process', ar.can_process,
          'can_unprocess', ar.can_unprocess,
          'can_bulk_authorise', ar.can_bulk_authorise,
          'can_bulk_unauthorise', ar.can_bulk_unauthorise,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'previous_primary_artifact_storage_key', NULL::text,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'evidence_badges', ar.evidence_badges,
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(
            'row_keys', JSONB_BUILD_ARRAY(ar.row_key),
            'timesheet_ids', CASE WHEN ar.timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.timesheet_id) END,
            'contract_week_ids', CASE WHEN ar.contract_week_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.contract_week_id) END,
            'storage_keys', CASE WHEN ar.primary_artifact_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.primary_artifact_storage_key) END,
            'row_signature', ar.row_signature,
            'invalidate_context', TRUE,
            'invalidate_preview', FALSE
          )
        )
      )
    ) AS row_json
  FROM artifact_rows AS ar
  ORDER BY
    ar.week_ending_date ASC NULLS LAST,
    ar.client_name ASC NULLS LAST,
    ar.candidate_name ASC NULLS LAST,
    ar.row_key ASC;
END;
$function$;





CREATE OR REPLACE FUNCTION public.bulk_process_row_context_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_decision_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_has_identity boolean := FALSE;
  v_row_key text := NULL;
  v_id_text text := NULL;
  v_timesheet_id_text text := NULL;
  v_contract_week_id_text text := NULL;
  v_include_evidence boolean := FALSE;
  v_include_compare boolean := FALSE;
  v_include_import_source_rows boolean := FALSE;
  v_profile text := NULL;
  v_base_only boolean := FALSE;
  v_header_row_json jsonb := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_out jsonb;
  v_editor_layer jsonb := NULL;
  v_evidence_layer jsonb := NULL;
  v_compare_layer jsonb := NULL;
  v_layer_errors jsonb := '[]'::jsonb;
  v_layer_names jsonb := '[]'::jsonb;
  v_effective_has_any_evidence boolean := FALSE;
  v_effective_badge_timesheet boolean := FALSE;
  v_effective_badge_mileage boolean := FALSE;
  v_effective_badge_travel boolean := FALSE;
  v_effective_badge_accommodation boolean := FALSE;
  v_effective_badge_other boolean := FALSE;
  v_effective_evidence_badges jsonb := '[]'::jsonb;
  v_effective_evidence_meta jsonb := '{}'::jsonb;
  v_effective_row_patch jsonb := '{}'::jsonb;
  v_effective_row jsonb := '{}'::jsonb;
  v_effective_data_row jsonb := '{}'::jsonb;
  v_effective_artifact_hints jsonb := '{}'::jsonb;
  v_effective_action_flags jsonb := '{}'::jsonb;
  v_effective_details jsonb := '{}'::jsonb;
  v_effective_left_pane jsonb := '{}'::jsonb;
  v_effective_attached_evidence_count integer := 0;
  v_canonical_row_json jsonb := NULL;
  v_canonical_row_signature text := NULL;
  v_canonical_row_key text := NULL;
  v_canonical_timesheet_id text := NULL;
  v_canonical_contract_week_id text := NULL;
BEGIN
  v_has_identity := (
    NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'week_id', v_filters->>'weekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '') IS NOT NULL
    OR (v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' AND jsonb_array_length(v_filters->'row_keys') > 0)
    OR (v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' AND jsonb_array_length(v_filters->'timesheet_ids') > 0)
    OR (v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' AND jsonb_array_length(v_filters->'contract_week_ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
    OR (v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' AND jsonb_array_length(v_filters->'rowKeys') > 0)
    OR (v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' AND jsonb_array_length(v_filters->'timesheetIds') > 0)
    OR (v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' AND jsonb_array_length(v_filters->'contractWeekIds') > 0)
    OR (v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' AND jsonb_array_length(v_filters->'ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
  );

  IF v_has_identity = FALSE THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'context_kind', 'bulk_process_row_context',
      'context_profile', 'status_header',
      'profile', 'status_header',
      'soft_failure', TRUE,
      'context_degraded', TRUE,
      'degraded_reason', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'header_loaded', FALSE,
      'header_only', FALSE,
      'editor_loaded', FALSE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', TRUE,
      'schedule_authoritative', FALSE,
      'loaded_layers', '[]'::jsonb,
      'error', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'message', 'bulk_process_row_context_v1 requires row_key, timesheet_id, or contract_week_id.',
      'filters', v_filters
    );
  END IF;

  v_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '');
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '');
  v_contract_week_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', v_filters->>'week_id', v_filters->>'weekId', '')), '');
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_row_key IS NULL AND v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'row_keys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'rowKeys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL THEN
    v_row_key := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys'), ',', 1)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheet_ids') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheetIds') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_timesheet_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '');
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contract_week_ids') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contractWeekIds') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_contract_week_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '');
  END IF;

  IF v_id_text IS NULL AND v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT NULLIF(BTRIM(id_values.value), '')
      INTO v_id_text
    FROM jsonb_array_elements_text(v_filters->'ids') WITH ORDINALITY AS id_values(value, ordinal_position)
    WHERE id_values.value ~* v_uuid_re
    ORDER BY id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '') ~* v_uuid_re THEN
    v_id_text := NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_timesheet_id_text IS NULL AND v_row_key LIKE 'timesheet:%' THEN
    v_timesheet_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 11)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_contract_week_id_text IS NULL AND v_row_key LIKE 'contract_week:%' THEN
    v_contract_week_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 15)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_contract_week_id_text IS NULL AND v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    IF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('contract_week', 'contract-week', 'week', 'contractweek')
       OR LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_contract_week_only', v_filters->>'isContractWeekOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN
      v_contract_week_id_text := v_id_text;
    ELSIF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('timesheet', 'timesheet_id', 'timesheet-id', 'ts') THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.timesheets AS identity_ts
      WHERE identity_ts.timesheet_id = v_id_text::uuid
        AND identity_ts.is_current = TRUE
    ) THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.contract_weeks AS identity_cw
      WHERE identity_cw.id = v_id_text::uuid
    ) THEN
      v_contract_week_id_text := v_id_text;
    ELSE
      v_timesheet_id_text := v_id_text;
    END IF;
  END IF;

  IF v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('timesheet_id', v_timesheet_id_text);
  END IF;

  IF v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('contract_week_id', v_contract_week_id_text);
  END IF;

  IF v_row_key IS NOT NULL THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('row_key', v_row_key);
  END IF;

  v_base_only := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'base_only', v_filters->>'baseOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on');

  v_profile := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'profile', v_filters->>'context_profile', v_filters->>'contextProfile', '')), ''));

  IF v_profile IS NULL THEN
    v_profile := 'status_header';
  END IF;

  IF v_profile NOT IN ('active_row_visible', 'status_header', 'editor', 'evidence', 'compare_import', 'full') THEN
    v_profile := 'status_header';
  END IF;

  v_include_evidence := CASE
    WHEN v_profile = 'evidence' THEN TRUE
    WHEN v_profile = 'full' THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_evidence', v_filters->>'includeEvidence', v_filters->>'load_evidence', v_filters->>'loadEvidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_compare := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_compare', v_filters->>'includeCompare', v_filters->>'load_compare', v_filters->>'loadCompare', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_import_source_rows := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_import_source_rows', v_filters->>'includeImportSourceRows', v_filters->>'load_import_source_rows', v_filters->>'loadImportSourceRows', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;


  IF v_profile = 'active_row_visible' AND (
    COALESCE(v_include_evidence, FALSE) = TRUE
    OR COALESCE(v_include_compare, FALSE) = TRUE
    OR COALESCE(v_include_import_source_rows, FALSE) = TRUE
  ) THEN
    v_layer_errors := '[]'::jsonb;
    v_layer_names := JSONB_BUILD_ARRAY('header', 'editor');

    v_editor_layer := public.bulk_process_row_context_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'profile', 'editor',
           'context_profile', 'editor',
           'include_evidence', FALSE,
           'include_compare', FALSE,
           'include_import_source_rows', FALSE
         )
    );

    IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = FALSE THEN
      RETURN COALESCE(v_editor_layer, JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'active_row_visible',
        'profile', 'active_row_visible',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', COALESCE(v_editor_layer->>'degraded_reason', v_editor_layer->>'error', 'EDITOR_LAYER_FAILED'),
        'header_loaded', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'header_loaded', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'loaded_layers', COALESCE(v_editor_layer->'loaded_layers', '[]'::jsonb),
        'filters', v_filters
      );
    END IF;

    v_out := v_editor_layer || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_process_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_process',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', v_layer_names
    );

    IF COALESCE(v_include_evidence, FALSE) = TRUE THEN
      v_evidence_layer := public.bulk_process_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'evidence',
             'context_profile', 'evidence',
             'include_evidence', TRUE,
             'include_compare', FALSE,
             'include_import_source_rows', FALSE
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('evidence');
        v_out := v_out
          || JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
            'attached_evidence', COALESCE(v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'attachedRows', COALESCE(v_evidence_layer->'attachedRows', v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'primary_artifact', COALESCE(v_evidence_layer->'primary_artifact', v_out->'primary_artifact', v_out#>'{details,primary_artifact}', JSONB_BUILD_OBJECT()),
            'preview_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'preview_storage_key', '')), '')),
            'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'primary_artifact_storage_key', '')), '')),
            'primary_artifact_preview_mode', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'primary_artifact_preview_mode', '')), '')),
            'has_any_evidence', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'attached_evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'attached_evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_badges', COALESCE(v_evidence_layer->'evidence_badges', v_evidence_layer#>'{evidence_meta,evidence_badges}', '[]'::jsonb),
            'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE),
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'data_row', v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row_patch', JSONB_BUILD_OBJECT()),
            'action_flags', COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'action_flags', JSONB_BUILD_OBJECT()),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE)
            ),
            'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE)
            )
          );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_evidence_layer->>'degraded_reason', v_evidence_layer->>'error', 'EVIDENCE_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'evidence_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', 'EVIDENCE_LAYER_FAILED',
          'evidence_layer_failure', COALESCE(v_evidence_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    IF COALESCE(v_include_compare, FALSE) = TRUE OR COALESCE(v_include_import_source_rows, FALSE) = TRUE THEN
      v_compare_layer := public.bulk_process_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'compare_import',
             'context_profile', 'compare_import',
             'include_evidence', FALSE,
             'include_compare', COALESCE(v_include_compare, FALSE),
             'include_import_source_rows', COALESCE(v_include_import_source_rows, FALSE)
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_compare_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('compare_import');
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', TRUE,
          'compare', COALESCE(v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'compare_payload', COALESCE(v_compare_layer->'compare_payload', v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_rows', COALESCE(v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb),
            'external_source_rows_json', COALESCE(v_compare_layer#>'{details,external_source_rows_json}', v_compare_layer#>'{compare,external_source_rows_json}', '[]'::jsonb)
          ),
          'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_items', COALESCE(v_compare_layer#>'{left_pane,source_items}', v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb)
          )
        );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_compare_layer->>'degraded_reason', v_compare_layer->>'error', 'COMPARE_IMPORT_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', CASE WHEN JSONB_ARRAY_LENGTH(v_layer_errors) > 0 THEN 'LAYER_FAILURE' ELSE 'COMPARE_IMPORT_LAYER_FAILED' END,
          'compare_import_layer_failure', COALESCE(v_compare_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    v_out := v_out || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_process_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_process',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'loaded_layers', v_layer_names,
      'layer_errors', v_layer_errors,
      'filters', v_filters
    );


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN v_out;
  END IF;


  IF v_profile IN ('status_header', 'editor') OR (v_profile = 'active_row_visible' AND COALESCE(v_include_evidence, FALSE) = FALSE) THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (
          cw_ids.input_contract_week_id IS NOT NULL
          AND cw0.id = cw_ids.input_contract_week_id
        )
        OR (
          cw_ids.input_timesheet_id IS NOT NULL
          AND cw0.timesheet_id = cw_ids.input_timesheet_id
        )
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    contract_row AS (
      SELECT ct0.*
      FROM public.contracts AS ct0
      LEFT JOIN timesheet_row AS ts_for_ct ON TRUE
      LEFT JOIN contract_week_row AS cw_for_ct ON TRUE
      WHERE ct0.id = COALESCE(ts_for_ct.contract_id, cw_for_ct.contract_id)
      ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
      LIMIT 1
    ),
    candidate_row AS (
      SELECT cand0.*
      FROM public.candidates AS cand0
      LEFT JOIN contract_row AS ct_for_cand ON TRUE
      WHERE cand0.id = ct_for_cand.candidate_id
      LIMIT 1
    ),
    client_row AS (
      SELECT cli0.*
      FROM public.clients AS cli0
      LEFT JOIN contract_row AS ct_for_cli ON TRUE
      WHERE cli0.id = ct_for_cli.client_id
      LIMIT 1
    ),
    validation_rows AS (
      SELECT tv0.*
      FROM public.timesheet_validations AS tv0
      LEFT JOIN timesheet_row AS ts_for_tv ON TRUE
      WHERE ts_for_tv.timesheet_id IS NOT NULL
        AND tv0.timesheet_id = ts_for_tv.timesheet_id
      ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
    ),
    validation_payload AS (
      SELECT
        COALESCE(JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'id', validation_rows.id,
            'timesheet_id', validation_rows.timesheet_id,
            'booking_id', validation_rows.booking_id,
            'status', validation_rows.status,
            'reason_code', validation_rows.reason_code,
            'hr_request_id', validation_rows.hr_request_id,
            'validated_at_utc', validation_rows.validated_at_utc,
            'override_requested_at_utc', validation_rows.override_requested_at_utc,
            'override_requested_by', validation_rows.override_requested_by,
            'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
            'override_confirmed_by', validation_rows.override_confirmed_by,
            'override_reason', validation_rows.override_reason,
            'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
            'last_source', validation_rows.last_source,
            'created_at', validation_rows.created_at,
            'updated_at', validation_rows.updated_at,
            'hr_request_source', validation_rows.hr_request_source,
            'hr_request_set_by', validation_rows.hr_request_set_by,
            'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
            'pre_validated', validation_rows.pre_validated
          ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
        ), '[]'::jsonb) AS validations_json,
        (
          SELECT JSONB_BUILD_OBJECT(
            'id', validation_latest.id,
            'status', validation_latest.status,
            'reason_code', validation_latest.reason_code,
            'hr_request_id', validation_latest.hr_request_id,
            'validated_at_utc', validation_latest.validated_at_utc,
            'pre_validated', COALESCE(validation_latest.pre_validated, FALSE),
            'updated_at', validation_latest.updated_at
          )
          FROM validation_rows AS validation_latest
          ORDER BY validation_latest.validated_at_utc DESC NULLS LAST, validation_latest.created_at DESC NULLS LAST, validation_latest.id DESC
          LIMIT 1
        ) AS latest_validation_json
      FROM validation_rows
    ),
    core_row AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id, contract_row.id) AS resolved_contract_id,
        COALESCE(summary_row.candidate_id, contract_row.candidate_id, candidate_row.id) AS resolved_candidate_id,
        COALESCE(summary_row.client_id, contract_row.client_id, client_row.id) AS resolved_client_id,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_name,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_display_name,
        COALESCE(NULLIF(BTRIM(summary_row.client_name), ''), NULLIF(BTRIM(client_row.name), '')) AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.work_date, timesheet_row.worked_start_iso::date, contract_week_row.week_ending_date) AS work_date,
        COALESCE(summary_row.booking_id, timesheet_row.booking_id) AS booking_id,
        COALESCE(summary_row.occupant_key_norm, timesheet_row.occupant_key_norm, tsfin_row.occupant_key_norm) AS occupant_key_norm,
        COALESCE(summary_row.hospital_norm, timesheet_row.hospital_norm) AS hospital_norm,
        COALESCE(summary_row.candidate_hint_text, timesheet_row.candidate_hint_text, JSONB_BUILD_OBJECT()) AS candidate_hint_text,
        COALESCE(summary_row.sheet_scope, timesheet_row.sheet_scope::text) AS sheet_scope,
        COALESCE(summary_row.submission_mode, timesheet_row.submission_mode::text) AS submission_mode,
        COALESCE(summary_row.submission_mode_snapshot, contract_week_row.submission_mode_snapshot::text) AS submission_mode_snapshot,
        COALESCE(summary_row.basis, tsfin_row.basis::text) AS basis,
        summary_row.route_type AS route_type,
        summary_row.route_display AS route_display,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        summary_row.route_subfamily AS route_subfamily,
        summary_row.underlying_channel_family AS underlying_channel_family,
        COALESCE(summary_row.summary_stage, CASE WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED' ELSE 'PROCESSED' END) AS summary_stage,
        COALESCE(summary_row.tools_stage, CASE WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED' ELSE COALESCE(tsfin_row.processing_status::text, timesheet_row.status::text) END) AS tools_stage,
        COALESCE(summary_row.processing_status, tsfin_row.processing_status::text) AS processing_status,
        summary_row.processing_status_display AS processing_status_display,
        COALESCE(summary_row.authorised_at_utc, tsfin_row.authorised_at_utc) AS authorised_at_utc,
        COALESCE(summary_row.authorised_at_server, timesheet_row.authorised_at_server) AS authorised_at_server,
        COALESCE(summary_row.processed_at_utc, tsfin_row.processed_at_utc) AS processed_at_utc,
        COALESCE(summary_row.is_authorised, tsfin_row.authorised_at_utc IS NOT NULL, timesheet_row.authorised_at_server IS NOT NULL, FALSE) AS is_authorised,
        COALESCE(summary_row.total_hours, tsfin_row.total_hours) AS total_hours,
        COALESCE(summary_row.total_pay_ex_vat, tsfin_row.total_pay_ex_vat) AS total_pay_ex_vat,
        COALESCE(summary_row.total_charge_ex_vat, tsfin_row.total_charge_ex_vat) AS total_charge_ex_vat,
        COALESCE(summary_row.margin_ex_vat, tsfin_row.margin_ex_vat) AS margin_ex_vat,
        summary_row.net_delta_ex_vat AS net_delta_ex_vat,
        COALESCE(summary_row.paid_at_utc, tsfin_row.paid_at_utc) AS paid_at_utc,
        summary_row.pay_icon_code AS pay_icon_code,
        summary_row.pay_status_code AS pay_status_code,
        summary_row.pay_paid_at_utc AS pay_paid_at_utc,
        COALESCE(summary_row.invoice_is_paid, FALSE) AS invoice_is_paid,
        summary_row.invoice_issue_stage AS invoice_issue_stage,
        summary_row.invoice_segment_stage AS invoice_segment_stage,
        COALESCE(summary_row.invoice_segments_total, 0) AS invoice_segments_total,
        COALESCE(summary_row.invoice_segments_locked, 0) AS invoice_segments_locked,
        COALESCE(summary_row.invoice_segments_unlocked, 0) AS invoice_segments_unlocked,
        COALESCE(TO_JSONB(summary_row.issue_codes), '[]'::jsonb) AS issue_codes_json,
        COALESCE(summary_row.validation_status, validation_payload.latest_validation_json->>'status') AS validation_status,
        summary_row.validation_summary AS validation_summary,
        COALESCE(summary_row.hr_crosscheck_status, tsfin_row.hr_crosscheck_status) AS hr_crosscheck_status,
        COALESCE(TO_JSONB(summary_row.hr_crosscheck_issues), TO_JSONB(tsfin_row.hr_crosscheck_issues), '[]'::jsonb) AS hr_crosscheck_issues_json,
        COALESCE(summary_row.qr_status, timesheet_row.qr_status::text) AS qr_status,
        COALESCE(summary_row.is_qr, timesheet_row.qr_status IS NOT NULL, FALSE) AS is_qr,
        COALESCE(summary_row.is_adjusted, timesheet_row.is_adjustment, contract_week_row.is_adjustment, FALSE) AS is_adjusted,
        COALESCE(summary_row.needs_attention, FALSE) AS needs_attention,
        COALESCE(summary_row.has_rate_issue, tsfin_row.has_rate_issue, FALSE) AS has_rate_issue,
        COALESCE(summary_row.has_pay_channel_issue, tsfin_row.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
        COALESCE(summary_row.client_no_timesheet_required, contract_row.no_timesheet_required, FALSE) AS client_no_timesheet_required,
        COALESCE(summary_row.client_autoprocess_hr, contract_row.autoprocess_hr, FALSE) AS client_autoprocess_hr,
        COALESCE(summary_row.client_is_nhsp, contract_row.is_nhsp, FALSE) AS client_is_nhsp,
        COALESCE(summary_row.has_any_evidence, FALSE) AS has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS attached_evidence_count,
        summary_row.primary_artifact_storage_key AS primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS primary_artifact_preview_mode,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.version AS timesheet_version,
        timesheet_row.status AS timesheet_status,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.generated_pdf_at_utc AS generated_pdf_at_utc,
        timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
        timesheet_row.additional_units_week AS timesheet_additional_units_week,
        timesheet_row.additional_units_per_day AS timesheet_additional_units_per_day,
        timesheet_row.sheet_scope AS timesheet_sheet_scope,
        timesheet_row.worked_start_iso AS timesheet_worked_start_iso,
        timesheet_row.worked_end_iso AS timesheet_worked_end_iso,
        timesheet_row.break_start_iso AS timesheet_break_start_iso,
        timesheet_row.break_end_iso AS timesheet_break_end_iso,
        timesheet_row.break_minutes AS timesheet_break_minutes,
        timesheet_row.worked_minutes AS timesheet_worked_minutes,
        timesheet_row.auth_name AS timesheet_auth_name,
        timesheet_row.auth_job_title AS timesheet_auth_job_title,
        timesheet_row.reference_number AS timesheet_reference_number,
        timesheet_row.reference_set_at AS timesheet_reference_set_at,
        timesheet_row.created_at AS timesheet_created_at,
        timesheet_row.is_adjustment AS timesheet_is_adjustment,
        timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
        timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
        contract_week_row.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
        contract_week_row.day_entries_json AS contract_week_day_entries_json,
        contract_week_row.totals_json AS contract_week_totals_json,
        contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
        contract_week_row.additional_seq AS contract_week_additional_seq,
        contract_week_row.status AS contract_week_status,
        contract_week_row.created_at AS contract_week_created_at,
        contract_week_row.is_adjustment AS contract_week_is_adjustment,
        contract_week_row.enforce_day_partition AS contract_week_enforce_day_partition,
        contract_week_row.allowed_days_mask AS contract_week_allowed_days_mask,
        contract_week_row.split_boundary_date AS contract_week_split_boundary_date,
        contract_week_row.worker_note AS contract_week_worker_note,
        contract_week_row.split_group_key AS contract_week_split_group_key,
        contract_row.id AS contract_id,
        contract_row.role AS contract_role,
        contract_row.band AS contract_band,
        contract_row.display_site AS contract_display_site,
        contract_row.ward_hint AS contract_ward_hint,
        contract_row.default_submission_mode AS contract_default_submission_mode,
        contract_row.std_schedule_json AS contract_std_schedule_json,
        contract_row.additional_rates_json AS contract_additional_rates_json,
        contract_row.weekly_timesheet_source AS contract_weekly_timesheet_source,
        contract_row.no_timesheet_required AS contract_no_timesheet_required,
        contract_row.autoprocess_hr AS contract_autoprocess_hr,
        contract_row.requires_hr AS contract_requires_hr,
        contract_row.hr_attach_to_invoice AS contract_hr_attach_to_invoice,
        contract_row.ts_attach_to_invoice AS contract_ts_attach_to_invoice,
        contract_row.is_nhsp AS contract_is_nhsp,
        candidate_row.id AS candidate_id,
        candidate_row.tms_ref AS candidate_tms_ref,
        candidate_row.first_name AS candidate_first_name,
        candidate_row.last_name AS candidate_last_name,
        candidate_row.display_name AS candidate_display_name_raw,
        candidate_row.email AS candidate_email,
        candidate_row.phone AS candidate_phone,
        candidate_row.key_norm AS candidate_key_norm,
        candidate_row.band AS candidate_band,
        client_row.id AS client_id,
        client_row.cli_ref AS client_cli_ref,
        client_row.name AS client_name_raw,
        client_row.vat_chargeable AS client_vat_chargeable,
        client_row.ts_queries_email AS client_ts_queries_email,
        tsfin_row.id AS tsfin_id,
        tsfin_row.timesheet_version AS tsfin_timesheet_version,
        tsfin_row.basis AS tsfin_basis,
        tsfin_row.is_current AS tsfin_is_current,
        tsfin_row.is_stale AS tsfin_is_stale,
        tsfin_row.stale_reason AS tsfin_stale_reason,
        tsfin_row.actual_schedule_json AS tsfin_actual_schedule_json,
        tsfin_row.actual_minutes_by_day_json AS tsfin_actual_minutes_by_day_json,
        tsfin_row.additional_units_json AS tsfin_additional_units_json,
        tsfin_row.processing_status AS tsfin_processing_status,
        tsfin_row.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
        tsfin_row.locked_at_utc AS tsfin_locked_at_utc,
        tsfin_row.paid_at_utc AS tsfin_paid_at_utc,
        tsfin_row.processed_at_utc AS tsfin_processed_at_utc,
        tsfin_row.authorised_at_utc AS tsfin_authorised_at_utc,
        tsfin_row.external_source_rows_json AS tsfin_external_source_rows_json,
        validation_payload.validations_json AS validations_json,
        validation_payload.latest_validation_json AS latest_validation_json
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN contract_row ON TRUE
      LEFT JOIN candidate_row ON TRUE
      LEFT JOIN client_row ON TRUE
      LEFT JOIN validation_payload ON TRUE
    ),
    flags AS (
      SELECT
        core_row.*,
        (core_row.resolved_timesheet_id IS NOT NULL OR core_row.resolved_contract_week_id IS NOT NULL) AS row_found,
        (
          core_row.tsfin_locked_by_invoice_id IS NOT NULL
          OR core_row.tsfin_paid_at_utc IS NOT NULL
          OR core_row.paid_at_utc IS NOT NULL
          OR COALESCE(core_row.invoice_segments_locked, 0) > 0
          OR COALESCE(core_row.invoice_is_paid, FALSE) = TRUE
        ) AS locked_bool,
        COALESCE(core_row.is_authorised, FALSE) AS authorised_bool,
        (
          core_row.resolved_timesheet_id IS NULL
          OR UPPER(COALESCE(core_row.processing_status, core_row.tools_stage, core_row.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
        ) AS unprocessed_bool,
        (
          UPPER(COALESCE(core_row.processing_status, '')) = 'PENDING_AUTH'
          OR (
            COALESCE(core_row.contract_requires_hr, FALSE) = TRUE
            AND COALESCE(core_row.contract_autoprocess_hr, FALSE) = FALSE
            AND UPPER(COALESCE(core_row.processing_status, '')) = 'READY_FOR_HR'
          )
        ) AS requires_authorisation_bool,
        (
          UPPER(COALESCE(core_row.qr_status, '')) = 'PENDING'
          AND core_row.resolved_timesheet_id IS NOT NULL
        ) AS qr_unsigned_blocked_bool,
        (
          COALESCE(core_row.timesheet_is_adjustment, core_row.contract_week_is_adjustment, FALSE) = TRUE
          AND COALESCE(core_row.contract_week_additional_seq, 0) > 0
          AND (
            core_row.timesheet_actual_schedule_json IS NULL
            OR core_row.timesheet_actual_schedule_json = '[]'::jsonb
            OR (jsonb_typeof(core_row.timesheet_actual_schedule_json) = 'array' AND jsonb_array_length(core_row.timesheet_actual_schedule_json) = 0)
          )
        ) AS keep_blank_additional_schedule_bool
      FROM core_row
    ),
    row_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_process_row_context',
          'context_profile', v_profile,
          'profile', v_profile,
          'context_type', CASE WHEN 'bulk_process_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', TRUE,
          'header_only', (v_profile = 'status_header'),
          'editor_loaded', (v_profile IN ('editor', 'active_row_visible')),
          'evidence_loaded', FALSE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', NOT (v_profile IN ('editor', 'active_row_visible')),
          'schedule_authoritative', (v_profile IN ('editor', 'active_row_visible')),
          'loaded_layers', CASE WHEN v_profile = 'status_header' THEN JSONB_BUILD_ARRAY('header') ELSE JSONB_BUILD_ARRAY('header', 'editor') END,
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
          'timesheet_id', flags.resolved_timesheet_id,
          'current_timesheet_id', flags.resolved_timesheet_id,
          'requested_timesheet_id', flags.resolved_timesheet_id,
          'expected_timesheet_id', flags.resolved_timesheet_id,
          'contract_week_id', flags.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(flags.resolved_timesheet_id::text, ''), COALESCE(flags.resolved_contract_week_id::text, ''), COALESCE(flags.timesheet_version::text, ''), COALESCE(flags.timesheet_updated_at::text, ''), COALESCE(flags.contract_week_updated_at::text, ''), COALESCE(flags.tsfin_updated_at::text, ''), v_profile)),
          'filters', v_filters
        )
        || JSONB_BUILD_OBJECT(
          'row', (
            JSONB_BUILD_OBJECT(
              'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
              'timesheet_id', flags.resolved_timesheet_id,
              'current_timesheet_id', flags.resolved_timesheet_id,
              'requested_timesheet_id', flags.resolved_timesheet_id,
              'expected_timesheet_id', flags.resolved_timesheet_id,
              'contract_week_id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'candidate_id', flags.resolved_candidate_id,
              'candidate_name', flags.candidate_name,
              'candidate_display_name', flags.candidate_display_name,
              'client_id', flags.resolved_client_id,
              'client_name', flags.client_name,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'candidate_hint_text', flags.candidate_hint_text,
              'week_ending_date', flags.week_ending_date,
              'work_date', flags.work_date,
              'sheet_scope', flags.sheet_scope,
              'submission_mode', flags.submission_mode,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'basis', flags.basis,
              'status', flags.timesheet_status,
              'contract_week_status', flags.contract_week_status,
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'summary_stage', flags.summary_stage,
              'tools_stage', flags.tools_stage,
              'processing_status', flags.processing_status,
              'processing_status_display', flags.processing_status_display,
              'authorised_at_utc', flags.authorised_at_utc,
              'authorised_at_server', flags.authorised_at_server,
              'processed_at_utc', flags.processed_at_utc,
              'is_authorised', flags.authorised_bool,
              'locked', flags.locked_bool,
              'has_timesheet', flags.resolved_timesheet_id IS NOT NULL
            )
            || JSONB_BUILD_OBJECT(
              'total_hours', flags.total_hours,
              'total_pay_ex_vat', flags.total_pay_ex_vat,
              'total_charge_ex_vat', flags.total_charge_ex_vat,
              'margin_ex_vat', flags.margin_ex_vat,
              'net_delta_ex_vat', flags.net_delta_ex_vat,
              'paid_at_utc', flags.paid_at_utc,
              'pay_icon_code', flags.pay_icon_code,
              'pay_status_code', flags.pay_status_code,
              'pay_paid_at_utc', flags.pay_paid_at_utc,
              'invoice_is_paid', flags.invoice_is_paid,
              'invoice_issue_stage', flags.invoice_issue_stage,
              'invoice_segment_stage', flags.invoice_segment_stage,
              'invoice_segments_total', flags.invoice_segments_total,
              'invoice_segments_locked', flags.invoice_segments_locked,
              'invoice_segments_unlocked', flags.invoice_segments_unlocked,
              'issue_codes', flags.issue_codes_json,
              'validation_status', flags.validation_status,
              'validation_summary', flags.validation_summary,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json,
              'qr_status', flags.qr_status,
              'is_qr', flags.is_qr,
              'is_adjusted', flags.is_adjusted,
              'needs_attention', flags.needs_attention,
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'client_no_timesheet_required', flags.client_no_timesheet_required,
              'client_autoprocess_hr', flags.client_autoprocess_hr,
              'client_is_nhsp', flags.client_is_nhsp
            )
            || JSONB_BUILD_OBJECT(
              'has_any_evidence', flags.has_any_evidence,
              'attached_evidence_count', flags.attached_evidence_count,
              'evidence_count', flags.attached_evidence_count,
              'primary_artifact_storage_key', flags.primary_artifact_storage_key,
              'primary_artifact_display_name', flags.primary_artifact_display_name,
              'primary_artifact_preview_mode', flags.primary_artifact_preview_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'qr_r2_key', flags.qr_r2_key,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'period_type', CASE WHEN flags.resolved_contract_week_id IS NOT NULL THEN 'WEEKLY' ELSE 'DAILY' END,
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              '__suppressStandardScheduleFallback', flags.keep_blank_additional_schedule_bool,
              '__keepAdditionalManualAdjustmentScheduleEmpty', flags.keep_blank_additional_schedule_bool
            )
            || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'contract_week_totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
            ) ELSE JSONB_BUILD_OBJECT() END
            || JSONB_BUILD_OBJECT(
              'action_flags', JSONB_BUILD_OBJECT(
                'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
                'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
                'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
                'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
                'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
                'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
                'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
                'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
                'additional_seq', flags.contract_week_additional_seq,
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count
              ),
              'row_patch', JSONB_BUILD_OBJECT(),
              'artifact_hints', JSONB_BUILD_OBJECT(
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count,
                'primary_artifact_storage_key', flags.primary_artifact_storage_key,
                'primary_artifact_display_name', flags.primary_artifact_display_name,
                'primary_artifact_preview_mode', flags.primary_artifact_preview_mode
              ),
              'evidence_badges', JSONB_BUILD_ARRAY(
                JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(flags.has_any_evidence, FALSE), 'has_evidence', COALESCE(flags.has_any_evidence, FALSE)),
                JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
              )
            )
          ),
          'evidence', '[]'::jsonb
        )
        || JSONB_BUILD_OBJECT(
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT(
            'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
            'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
            'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
            'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
            'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
            'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
            'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
            'has_any_evidence', flags.has_any_evidence,
            'attached_evidence_count', flags.attached_evidence_count
          ),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT()
        )
        || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(
            'requested_timesheet_id', flags.resolved_timesheet_id,
            'current_timesheet_id', flags.resolved_timesheet_id,
            'expected_timesheet_id', flags.resolved_timesheet_id,
            'current_version', flags.timesheet_version,
            'was_stale', COALESCE(flags.tsfin_is_stale, FALSE),
            'booking_id', flags.booking_id,
            'timesheet', JSONB_BUILD_OBJECT(
              'timesheet_id', flags.resolved_timesheet_id,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'worked_start_iso', flags.timesheet_worked_start_iso,
              'worked_end_iso', flags.timesheet_worked_end_iso,
              'break_start_iso', flags.timesheet_break_start_iso,
              'break_end_iso', flags.timesheet_break_end_iso,
              'break_minutes', flags.timesheet_break_minutes,
              'worked_minutes', flags.timesheet_worked_minutes,
              'week_ending_date', flags.week_ending_date,
              'auth_name', flags.timesheet_auth_name,
              'auth_job_title', flags.timesheet_auth_job_title,
              'authorised_at_server', flags.authorised_at_server,
              'reference_number', flags.timesheet_reference_number,
              'reference_set_at', flags.timesheet_reference_set_at,
              'status', flags.timesheet_status,
              'created_at', flags.timesheet_created_at,
              'updated_at', flags.timesheet_updated_at,
              'version', flags.timesheet_version,
              'is_current', flags.resolved_timesheet_id IS NOT NULL,
              'contract_id', flags.resolved_contract_id,
              'submission_mode', flags.submission_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'sheet_scope', flags.sheet_scope,
              'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb),
              'qr_status', flags.qr_status,
              'qr_r2_key', flags.qr_r2_key,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'candidate_hint_text', flags.candidate_hint_text,
              'is_adjustment', flags.timesheet_is_adjustment,
              'parent_timesheet_id', flags.timesheet_parent_timesheet_id,
              'adjustment_origin', flags.timesheet_adjustment_origin
            ),
            'tsfin', JSONB_BUILD_OBJECT(
              'id', flags.tsfin_id,
              'timesheet_id', flags.resolved_timesheet_id,
              'timesheet_version', flags.tsfin_timesheet_version,
              'basis', flags.tsfin_basis,
              'is_current', flags.tsfin_is_current,
              'is_stale', flags.tsfin_is_stale,
              'stale_reason', flags.tsfin_stale_reason,
              'processing_status', flags.tsfin_processing_status,
              'total_hours', flags.total_hours,
              'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
              'locked_at_utc', flags.tsfin_locked_at_utc,
              'paid_at_utc', flags.tsfin_paid_at_utc,
              'processed_at_utc', flags.tsfin_processed_at_utc,
              'authorised_at_utc', flags.tsfin_authorised_at_utc,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json
            ),
            'validations', COALESCE(flags.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', flags.validation_status,
              'pre_validated', COALESCE((flags.latest_validation_json->>'pre_validated')::boolean, FALSE),
              'latest', flags.latest_validation_json
            ),
            'contract_week_id', flags.resolved_contract_week_id,
            'contract_week', JSONB_BUILD_OBJECT(
              'id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'week_ending_date', flags.week_ending_date,
              'additional_seq', flags.contract_week_additional_seq,
              'status', flags.contract_week_status,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'timesheet_id', flags.resolved_timesheet_id,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
              'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'created_at', flags.contract_week_created_at,
              'updated_at', flags.contract_week_updated_at,
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'is_adjustment', flags.contract_week_is_adjustment,
              'enforce_day_partition', flags.contract_week_enforce_day_partition,
              'allowed_days_mask', flags.contract_week_allowed_days_mask,
              'split_boundary_date', flags.contract_week_split_boundary_date,
              'worker_note', flags.contract_week_worker_note,
              'split_group_key', flags.contract_week_split_group_key
            ),
            'related', JSONB_BUILD_OBJECT(
              'contract', JSONB_BUILD_OBJECT(
                'id', flags.resolved_contract_id,
                'candidate_id', flags.resolved_candidate_id,
                'client_id', flags.resolved_client_id,
                'role', flags.contract_role,
                'band', flags.contract_band,
                'display_site', flags.contract_display_site,
                'ward_hint', flags.contract_ward_hint,
                'default_submission_mode', flags.contract_default_submission_mode,
                'std_schedule_json', COALESCE(flags.contract_std_schedule_json, '[]'::jsonb),
                'additional_rates_json', COALESCE(flags.contract_additional_rates_json, '{}'::jsonb),
                'weekly_timesheet_source', flags.contract_weekly_timesheet_source,
                'no_timesheet_required', flags.contract_no_timesheet_required,
                'autoprocess_hr', flags.contract_autoprocess_hr,
                'requires_hr', flags.contract_requires_hr,
                'hr_attach_to_invoice', flags.contract_hr_attach_to_invoice,
                'ts_attach_to_invoice', flags.contract_ts_attach_to_invoice,
                'is_nhsp', flags.contract_is_nhsp
              ),
              'candidate', JSONB_BUILD_OBJECT(
                'id', flags.resolved_candidate_id,
                'tms_ref', flags.candidate_tms_ref,
                'first_name', flags.candidate_first_name,
                'last_name', flags.candidate_last_name,
                'display_name', flags.candidate_display_name_raw,
                'email', flags.candidate_email,
                'phone', flags.candidate_phone,
                'key_norm', flags.candidate_key_norm,
                'band', flags.candidate_band
              ),
              'client', JSONB_BUILD_OBJECT(
                'id', flags.resolved_client_id,
                'cli_ref', flags.client_cli_ref,
                'name', flags.client_name_raw,
                'vat_chargeable', flags.client_vat_chargeable,
                'ts_queries_email', flags.client_ts_queries_email
              )
            ),
            'evidence', '[]'::jsonb,
            'policy', JSONB_BUILD_OBJECT(
              'weekly_mode', flags.contract_weekly_timesheet_source,
              'requires_hr', flags.contract_requires_hr,
              'autoprocess_hr', flags.contract_autoprocess_hr,
              'no_timesheet_required', flags.contract_no_timesheet_required,
              'is_nhsp', flags.contract_is_nhsp
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              'summary_stage', flags.summary_stage,
              'client_requires_hr', flags.contract_requires_hr,
              'client_autoprocess_hr', flags.contract_autoprocess_hr,
              'client_no_timesheet_required', flags.contract_no_timesheet_required,
              'client_is_nhsp', flags.contract_is_nhsp,
              'contract_id', flags.resolved_contract_id,
              'issue_codes', flags.issue_codes_json
            )
          ),
          'timesheet', JSONB_BUILD_OBJECT(
            'timesheet_id', flags.resolved_timesheet_id,
            'booking_id', flags.booking_id,
            'worked_start_iso', flags.timesheet_worked_start_iso,
            'worked_end_iso', flags.timesheet_worked_end_iso,
            'break_start_iso', flags.timesheet_break_start_iso,
            'break_end_iso', flags.timesheet_break_end_iso,
            'break_minutes', flags.timesheet_break_minutes,
            'worked_minutes', flags.timesheet_worked_minutes,
            'week_ending_date', flags.week_ending_date,
            'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
            'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
          ),
          'tsfin', JSONB_BUILD_OBJECT(
            'id', flags.tsfin_id,
            'timesheet_id', flags.resolved_timesheet_id,
            'basis', flags.tsfin_basis,
            'processing_status', flags.tsfin_processing_status,
            'total_hours', flags.total_hours,
            'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
            'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
            'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
            'paid_at_utc', flags.tsfin_paid_at_utc,
            'processed_at_utc', flags.tsfin_processed_at_utc,
            'authorised_at_utc', flags.tsfin_authorised_at_utc
          ),
          'contract_week', JSONB_BUILD_OBJECT(
            'id', flags.resolved_contract_week_id,
            'contract_id', flags.resolved_contract_id,
            'week_ending_date', flags.week_ending_date,
            'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
            'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
            'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
            'additional_seq', flags.contract_week_additional_seq,
            'is_adjustment', flags.contract_week_is_adjustment
          ),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) ELSE JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) END AS payload_json
      FROM flags
      WHERE flags.row_found = TRUE
    )
    SELECT row_payload.payload_json || JSONB_BUILD_OBJECT(
        'data_row', row_payload.payload_json->'row',
        'row_patch', COALESCE(row_payload.payload_json->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_key', row_payload.payload_json->>'row_key')
      )
      INTO v_out
    FROM row_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk process row context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN v_out;
  END IF;

  IF v_profile = 'evidence' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.candidate_display_name AS candidate_display_name,
        summary_row.client_name AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        COALESCE(summary_row.has_any_evidence, FALSE) AS summary_has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS summary_attached_evidence_count,
        summary_row.primary_artifact_storage_key AS summary_primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS summary_primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS summary_primary_artifact_preview_mode,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
    ),
    evidence_items AS (
      SELECT
        0::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:manual_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Timesheet PDF',
          'filename', 'Timesheet PDF',
          'storage_key', resolved.manual_pdf_r2_key,
          'r2_key', resolved.manual_pdf_r2_key,
          'file_key', resolved.manual_pdf_r2_key,
          'download_storage_key', resolved.manual_pdf_r2_key,
          'original_filename', 'Timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'last_rotation_deg', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.manual_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_manual_pdf_duplicate
          WHERE te_manual_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_manual_pdf_duplicate.storage_key = resolved.manual_pdf_r2_key
        )
      UNION ALL
      SELECT
        1::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:qr_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Signed QR Timesheet',
          'filename', 'Signed QR Timesheet',
          'storage_key', resolved.qr_r2_key,
          'r2_key', resolved.qr_r2_key,
          'file_key', resolved.qr_r2_key,
          'download_storage_key', resolved.qr_r2_key,
          'original_filename', 'Signed QR Timesheet',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.qr_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_qr_pdf_duplicate
          WHERE te_qr_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_qr_pdf_duplicate.storage_key = resolved.qr_r2_key
        )
      UNION ALL
      SELECT
        2::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:contract_week_pdf:' || resolved.resolved_contract_week_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Uploaded timesheet PDF',
          'filename', 'Uploaded timesheet PDF',
          'storage_key', resolved.uploaded_pdf_r2_key,
          'r2_key', resolved.uploaded_pdf_r2_key,
          'file_key', resolved.uploaded_pdf_r2_key,
          'download_storage_key', resolved.uploaded_pdf_r2_key,
          'original_filename', 'Uploaded timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.contract_week_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_contract_week_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.uploaded_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_contract_week_pdf_duplicate
          WHERE te_contract_week_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_contract_week_pdf_duplicate.storage_key = resolved.uploaded_pdf_r2_key
        )
      UNION ALL
      SELECT
        10::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', te0.id,
          'evidence_id', te0.id,
          'queue_id', NULL::uuid,
          'timesheet_id', te0.timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'storage_key', te0.storage_key,
          'r2_key', te0.storage_key,
          'file_key', te0.storage_key,
          'download_storage_key', te0.storage_key,
          'original_filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'mime_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'content_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'uploaded_at_utc', te0.created_at,
          'created_at', te0.created_at,
          'created_by', te0.created_by,
          'rotation', 0,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
            ELSE 'FILE'
          END,
          'source_label', 'Attached',
          'source_badge', 'Attached'
        ) AS item_json
      FROM public.timesheet_evidence AS te0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND te0.timesheet_id = resolved.resolved_timesheet_id
      UNION ALL
      SELECT
        20::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', mq0.id,
          'evidence_id', NULL::uuid,
          'queue_id', mq0.id,
          'timesheet_id', NULL::uuid,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'original_filename', mq0.original_filename,
          'storage_key', mq0.r2_key,
          'r2_key', mq0.r2_key,
          'file_key', mq0.r2_key,
          'download_storage_key', mq0.r2_key,
          'mime_type', mq0.mime_type,
          'content_type', mq0.mime_type,
          'content_hash', mq0.content_hash,
          'uploaded_at_utc', mq0.uploaded_at_utc,
          'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
          'staged_by_user_id', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'staged_by_user_id'), ''), mq0.uploaded_by_user_id::text),
          'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
          'last_rotation_deg', COALESCE(mq0.last_rotation_deg::integer, 0),
          'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
          'pages', '[]'::jsonb,
          'status', mq0.status,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'is_staged_context', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(mq0.mime_type, '')) LIKE 'image/%' THEN 'IMAGE'
            WHEN LOWER(COALESCE(mq0.mime_type, '')) = 'application/pdf' THEN 'PDF'
            ELSE 'FILE'
          END,
          'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
          'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
        ) AS item_json
      FROM public.manual_timesheet_queue AS mq0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NULL
        AND resolved.resolved_contract_week_id IS NOT NULL
        AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = resolved.resolved_contract_week_id::text
    ),
    evidence_ranked AS (
      SELECT evidence_items.sort_order, evidence_items.item_json
      FROM evidence_items
      WHERE NULLIF(BTRIM(COALESCE(evidence_items.item_json->>'storage_key', evidence_items.item_json->>'r2_key', '')), '') IS NOT NULL
    ),
    evidence_payload AS (
      SELECT
        COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
        COUNT(*)::integer AS evidence_count,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TIMESHEET') AS has_timesheet,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'MILEAGE') AS has_mileage,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TRAVEL') AS has_travel,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'ACCOMMODATION') AS has_accommodation,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'OTHER') AS has_other,
        (SELECT er1.item_json FROM evidence_ranked AS er1 ORDER BY er1.sort_order ASC, er1.item_json->>'id' ASC LIMIT 1) AS primary_evidence_json
      FROM evidence_ranked
    ),
    final_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_process_row_context',
          'context_profile', 'evidence',
          'profile', 'evidence',
          'context_type', CASE WHEN 'bulk_process_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', FALSE,
          'header_only', FALSE,
          'editor_loaded', FALSE,
          'evidence_loaded', TRUE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', TRUE,
          'schedule_authoritative', FALSE,
          'loaded_layers', JSONB_BUILD_ARRAY('evidence'),
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), 'evidence')),
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attached_evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attachedRows', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
          'preview_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
          'primary_artifact_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
          'primary_artifact_preview_mode', COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', resolved.summary_primary_artifact_preview_mode),
          'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
          'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_badges', JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
          ),
          'evidence_meta', JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row', JSONB_BUILD_OBJECT(
            'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
            'timesheet_id', resolved.resolved_timesheet_id,
            'current_timesheet_id', resolved.resolved_timesheet_id,
            'requested_timesheet_id', resolved.resolved_timesheet_id,
            'expected_timesheet_id', resolved.resolved_timesheet_id,
            'contract_week_id', resolved.resolved_contract_week_id,
            'contract_id', resolved.resolved_contract_id,
            'candidate_id', resolved.candidate_id,
            'candidate_name', resolved.candidate_name,
            'candidate_display_name', resolved.candidate_display_name,
            'client_id', resolved.client_id,
            'client_name', resolved.client_name,
            'week_ending_date', resolved.week_ending_date,
            'route_family', resolved.route_family,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
            'primary_artifact_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
            'primary_artifact_preview_mode', COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', resolved.summary_primary_artifact_preview_mode),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT('has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0)),
          'details', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'left_pane', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT(),
          'filters', v_filters
        ) AS payload_json
      FROM resolved
      LEFT JOIN evidence_payload ON TRUE
      WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL
    )
    SELECT final_payload.payload_json || JSONB_BUILD_OBJECT('data_row', final_payload.payload_json->'row')
      INTO v_out
    FROM final_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'evidence',
        'profile', 'evidence',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk process evidence context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN v_out;
  END IF;

  IF v_profile = 'compare_import' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM public.timesheet_summary_lightweight_rows_v1(v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)) AS summary_source
      CROSS JOIN input_ids AS summary_ids
      WHERE (summary_ids.input_timesheet_id IS NULL OR summary_source.timesheet_id = summary_ids.input_timesheet_id)
        AND (summary_ids.input_contract_week_id IS NULL OR summary_source.contract_week_id = summary_ids.input_contract_week_id)
      ORDER BY summary_source.timesheet_id NULLS LAST, summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.client_name AS client_name,
        summary_row.week_ending_date AS week_ending_date,
        summary_row.route_family AS route_family,
        tsfin_row.external_source_rows_json AS external_source_rows_json,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
    )
    SELECT JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'context_type', CASE WHEN 'bulk_process_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
        'slim_context', TRUE,
        'header_loaded', TRUE,
        'header_only', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', TRUE,
        'full_loaded', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', JSONB_BUILD_ARRAY('header', 'compare_import'),
        'soft_failure', FALSE,
        'context_degraded', FALSE,
        'degraded_reason', NULL::text,
        'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
        'timesheet_id', resolved.resolved_timesheet_id,
        'current_timesheet_id', resolved.resolved_timesheet_id,
        'requested_timesheet_id', resolved.resolved_timesheet_id,
        'expected_timesheet_id', resolved.resolved_timesheet_id,
        'contract_week_id', resolved.resolved_contract_week_id,
        'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), COALESCE(resolved.tsfin_updated_at::text, ''), 'compare_import')),
        'row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'data_row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'row_patch', JSONB_BUILD_OBJECT(),
        'action_flags', JSONB_BUILD_OBJECT(),
        'compare', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'include_import_source_rows', v_include_import_source_rows
        ),
        'details', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'evidence', '[]'::jsonb
        ),
        'left_pane', JSONB_BUILD_OBJECT('source_items', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END, 'evidence', '[]'::jsonb),
        'evidence', '[]'::jsonb,
        'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE),
        'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
        'count_deltas', JSONB_BUILD_OBJECT(),
        'filters', v_filters
      )
      INTO v_out
    FROM resolved
    WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk process compare/import context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN v_out;
  END IF;

  WITH decision_row AS (
    SELECT patch_result.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'dataset_mode', 'process',
           'projection', 'active_row_header',
           'profile', v_profile
         )
    ) AS patch_result(row_json)
    ORDER BY patch_result.row_json->>'row_key'
    LIMIT 1
  ),
  row_ids AS (
    SELECT
      decision_row.row_json,
      CASE
        WHEN COALESCE(decision_row.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_week_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_week_id')::uuid
        ELSE NULL::uuid
      END AS contract_week_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS contract_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'candidate_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'candidate_id')::uuid
        ELSE NULL::uuid
      END AS candidate_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'client_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS client_id
    FROM decision_row
  ),
  timesheet_row AS (
    SELECT
      ts0.timesheet_id,
      ts0.booking_id,
      ts0.occupant_key_norm,
      ts0.hospital_norm,
      ts0.ward_norm,
      ts0.job_title_norm,
      ts0.shift_label_norm,
      ts0.scheduled_start_iso,
      ts0.scheduled_end_iso,
      ts0.worked_start_iso,
      ts0.worked_end_iso,
      ts0.break_start_iso,
      ts0.break_end_iso,
      ts0.break_minutes,
      ts0.worked_minutes,
      ts0.week_ending_date,
      ts0.auth_name,
      ts0.auth_job_title,
      ts0.authorised_at_server,
      ts0.r2_nurse_key,
      ts0.r2_auth_key,
      ts0.reference_number,
      ts0.reference_set_at,
      ts0.status,
      ts0.created_at,
      ts0.updated_at,
      ts0.version,
      ts0.is_current,
      ts0.contract_id,
      ts0.submission_mode,
      ts0.manual_pdf_r2_key,
      ts0.line_type,
      ts0.sheet_scope,
      ts0.actual_schedule_json,
      ts0.additional_units_week,
      ts0.additional_units_per_day,
      ts0.qr_status,
      ts0.qr_payload_json,
      ts0.qr_generated_at,
      ts0.qr_scanned_at,
      ts0.qr_scan_info_json,
      ts0.qr_r2_key,
      ts0.day_references_json,
      ts0.manual_pdf_rotation_degrees,
      ts0.qr_last_sent_hash,
      ts0.qr_last_sent_at_utc,
      ts0.qr_signed_hash,
      ts0.qr_signed_at_utc,
      ts0.candidate_hint_text,
      ts0.band,
      ts0.generated_pdf_at_utc,
      ts0.generated_pdf_refs_sig,
      ts0.generated_pdf_refs_snapshot_json,
      ts0.generated_pdf_refs_captured_at_utc,
      ts0.qr_sent_refs_sig,
      ts0.qr_sent_refs_snapshot_json,
      ts0.qr_sent_refs_captured_at_utc,
      ts0.is_adjustment,
      ts0.parent_timesheet_id,
      ts0.correction_id,
      ts0.correction_kind,
      ts0.adjustment_origin
    FROM public.timesheets AS ts0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND ts0.timesheet_id = ids.timesheet_id
      AND ts0.is_current = TRUE
    ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
    LIMIT 1
  ),
  tsfin_row AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.rate_source_refs_json,
      tf0.hours_day,
      tf0.hours_night,
      tf0.hours_sat,
      tf0.hours_sun,
      tf0.hours_bh,
      tf0.pay_day,
      tf0.pay_night,
      tf0.pay_sat,
      tf0.pay_sun,
      tf0.pay_bh,
      tf0.charge_day,
      tf0.charge_night,
      tf0.charge_sat,
      tf0.charge_sun,
      tf0.charge_bh,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.locked_at_utc,
      tf0.unlocked_by_credit_note_id,
      tf0.created_at,
      tf0.updated_at,
      tf0.occupant_key_norm,
      tf0.candidate_assignment,
      tf0.processing_status,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.po_number,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.pay_on_hold_since_utc,
      tf0.paid_at_utc,
      tf0.paid_by_user_id,
      tf0.payment_reference,
      tf0.remittance_last_sent_at_utc,
      tf0.remittance_send_count,
      tf0.pay_wtr_rate_pct_snapshot,
      tf0.pay_vat_rate_pct_snapshot,
      tf0.pay_vat_amount_snapshot,
      tf0.pay_total_inc_vat_snapshot,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.expenses_evidence_manifest,
      tf0.mileage_evidence_manifest,
      tf0.actual_schedule_json,
      tf0.actual_minutes_by_day_json,
      tf0.additional_units_json,
      tf0.additional_pay_ex_vat,
      tf0.additional_charge_ex_vat,
      tf0.additional_margin_ex_vat,
      tf0.invoice_breakdown_json,
      tf0.nhsp_import_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.mileage_units,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat
    FROM public.timesheets_financials AS tf0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tf0.timesheet_id = ids.timesheet_id
      AND tf0.is_current = TRUE
    ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
    LIMIT 1
  ),
  contract_week_row AS (
    SELECT
      cw0.id,
      cw0.contract_id,
      cw0.week_ending_date,
      cw0.additional_seq,
      cw0.status,
      cw0.submission_mode_snapshot,
      cw0.timesheet_id,
      cw0.uploaded_pdf_r2_key,
      cw0.day_entries_json,
      cw0.totals_json,
      cw0.created_at,
      cw0.updated_at,
      cw0.planned_schedule_json,
      cw0.is_adjustment,
      cw0.enforce_day_partition,
      cw0.allowed_days_mask,
      cw0.split_boundary_date,
      cw0.worker_note,
      cw0.split_group_key
    FROM public.contract_weeks AS cw0
    CROSS JOIN row_ids AS ids
    WHERE (ids.contract_week_id IS NOT NULL AND cw0.id = ids.contract_week_id)
       OR (ids.contract_week_id IS NULL AND ids.timesheet_id IS NOT NULL AND cw0.timesheet_id = ids.timesheet_id)
    ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
    LIMIT 1
  ),
  contract_row AS (
    SELECT
      ct0.id,
      ct0.candidate_id,
      ct0.client_id,
      ct0.role,
      ct0.band,
      ct0.display_site,
      ct0.ward_hint,
      ct0.start_date,
      ct0.end_date,
      ct0.pay_method_snapshot,
      ct0.rates_json,
      ct0.std_hours_json,
      ct0.default_submission_mode,
      ct0.week_ending_weekday_snapshot,
      ct0.auto_invoice,
      ct0.require_reference_to_pay,
      ct0.require_reference_to_invoice,
      ct0.created_at,
      ct0.updated_at,
      ct0.bucket_labels_json,
      ct0.std_schedule_json,
      ct0.mileage_pay_rate,
      ct0.mileage_charge_rate,
      ct0.additional_rates_json,
      ct0.self_bill,
      ct0.weekly_timesheet_source,
      ct0.no_timesheet_required,
      ct0.daily_calc_of_invoices,
      ct0.group_nightsat_sunbh,
      ct0.is_nhsp,
      ct0.autoprocess_hr,
      ct0.requires_hr,
      ct0.hr_attach_to_invoice,
      ct0.ts_attach_to_invoice,
      ct0.overrideclientsettings,
      ct0.reference_number_required_to_issue_invoice,
      ct0.send_manual_invoices_to_different_email,
      ct0.manual_invoices_alt_email_address,
      ct0.is_ad_hoc
    FROM public.contracts AS ct0
    CROSS JOIN row_ids AS ids
    LEFT JOIN timesheet_row AS ts1 ON TRUE
    LEFT JOIN contract_week_row AS cw1 ON TRUE
    WHERE ct0.id = COALESCE(ids.contract_id, ts1.contract_id, cw1.contract_id)
    ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
    LIMIT 1
  ),
  candidate_row AS (
    SELECT
      cand0.id,
      cand0.tms_ref,
      cand0.first_name,
      cand0.last_name,
      cand0.display_name,
      cand0.email,
      cand0.phone,
      cand0.pay_method,
      cand0.umbrella_id,
      cand0.active,
      cand0.key_norm,
      cand0.mileage_pay_rate,
      cand0.job_title_id,
      cand0.prof_reg_number,
      cand0.prof_reg_type,
      cand0.ni_number,
      cand0.date_of_birth,
      cand0.gender,
      cand0.title,
      cand0.opt_in_email,
      cand0.opt_in_sms,
      cand0.opt_in_whatsapp,
      cand0.band,
      cand0.tms_ref_num
    FROM public.candidates AS cand0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cand0.id = COALESCE(ids.candidate_id, ct1.candidate_id)
    LIMIT 1
  ),
  client_row AS (
    SELECT
      cli0.id,
      cli0.cli_ref,
      cli0.name,
      cli0.invoice_address,
      cli0.primary_invoice_email,
      cli0.ap_phone,
      cli0.vat_chargeable,
      cli0.payment_terms_days,
      cli0.mileage_charge_rate,
      cli0.ts_queries_email,
      cli0.client_address,
      cli0.contact_title,
      cli0.contact_known_as,
      cli0.contact_forename,
      cli0.contact_surname,
      cli0.contact_job_title,
      cli0.contact_tel,
      cli0.contact_mobile,
      cli0.contact_email,
      cli0.website
    FROM public.clients AS cli0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cli0.id = COALESCE(ids.client_id, ct1.client_id)
    LIMIT 1
  ),
  validation_rows AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.booking_id,
      tv0.status,
      tv0.reason_code,
      tv0.hr_request_id,
      tv0.validated_at_utc,
      tv0.override_requested_at_utc,
      tv0.override_requested_by,
      tv0.override_confirmed_at_utc,
      tv0.override_confirmed_by,
      tv0.override_reason,
      tv0.override_cleared_at_utc,
      tv0.last_source,
      tv0.created_at,
      tv0.updated_at,
      tv0.hr_request_source,
      tv0.hr_request_set_by,
      tv0.hr_request_set_at_utc,
      tv0.pre_validated
    FROM public.timesheet_validations AS tv0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tv0.timesheet_id = ids.timesheet_id
    ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
  ),
  validation_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', validation_rows.id,
          'timesheet_id', validation_rows.timesheet_id,
          'booking_id', validation_rows.booking_id,
          'status', validation_rows.status,
          'reason_code', validation_rows.reason_code,
          'hr_request_id', validation_rows.hr_request_id,
          'validated_at_utc', validation_rows.validated_at_utc,
          'override_requested_at_utc', validation_rows.override_requested_at_utc,
          'override_requested_by', validation_rows.override_requested_by,
          'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
          'override_confirmed_by', validation_rows.override_confirmed_by,
          'override_reason', validation_rows.override_reason,
          'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
          'last_source', validation_rows.last_source,
          'created_at', validation_rows.created_at,
          'updated_at', validation_rows.updated_at,
          'hr_request_source', validation_rows.hr_request_source,
          'hr_request_set_by', validation_rows.hr_request_set_by,
          'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
          'pre_validated', validation_rows.pre_validated
        ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
      ), '[]'::jsonb) AS validations_json,
      (
        SELECT JSONB_BUILD_OBJECT(
          'id', vr1.id,
          'status', vr1.status,
          'reason_code', vr1.reason_code,
          'hr_request_id', vr1.hr_request_id,
          'validated_at_utc', vr1.validated_at_utc,
          'pre_validated', COALESCE(vr1.pre_validated, FALSE),
          'updated_at', vr1.updated_at
        )
        FROM validation_rows AS vr1
        ORDER BY vr1.validated_at_utc DESC NULLS LAST, vr1.created_at DESC NULLS LAST, vr1.id DESC
        LIMIT 1
      ) AS latest_validation_json
    FROM validation_rows
  ),
  evidence_items AS (
    SELECT
      0::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_id'), ''), 'sys:timesheet:' || COALESCE(row_ids.timesheet_id::text, row_ids.contract_week_id::text)),
        'evidence_id', NULL::uuid,
        'queue_id', NULL::uuid,
        'timesheet_id', row_ids.timesheet_id,
        'contract_week_id', row_ids.contract_week_id,
        'kind', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET'),
        'display_name', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'r2_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'file_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'download_storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'original_filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'mime_type', CASE
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          ELSE NULL::text
        END,
        'content_type', CASE
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          ELSE NULL::text
        END,
        'uploaded_at_utc', row_ids.row_json->>'updated_at',
        'rotation_degrees', CASE WHEN COALESCE(row_ids.row_json->>'manual_pdf_rotation_degrees', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'manual_pdf_rotation_degrees')::integer ELSE 0 END,
        'last_rotation_deg', CASE WHEN COALESCE(row_ids.row_json->>'manual_pdf_rotation_degrees', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'manual_pdf_rotation_degrees')::integer ELSE 0 END,
        'page_count', NULL::integer,
        'pages', '[]'::jsonb,
        'system', TRUE,
        'is_view_only', TRUE,
        'can_delete', FALSE,
        'can_reclassify', FALSE,
        'can_edit_kind', FALSE,
        'can_edit_type', FALSE,
        'can_return_to_queue', FALSE,
        'preview_mode', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_preview_mode'), ''), 'PDF'),
        'source_label', 'System',
        'source_badge', 'System'
      ) AS item_json
    FROM row_ids
    WHERE v_include_evidence = TRUE
      AND NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '') IS NOT NULL
      AND NOT (
        NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') LIKE 'evidence:%'
        OR (
          row_ids.timesheet_id IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te_primary_artifact
            WHERE te_primary_artifact.timesheet_id = row_ids.timesheet_id
              AND te_primary_artifact.storage_key = NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '')
          )
        )
      )
    UNION ALL
    SELECT
      10::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', te0.id,
        'evidence_id', te0.id,
        'queue_id', NULL::uuid,
        'timesheet_id', te0.timesheet_id,
        'contract_week_id', NULL::uuid,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
        'filename', COALESCE(
          NULLIF(
            BTRIM(
              CASE
                WHEN NULLIF(BTRIM(COALESCE(te0.display_name, '')), '') IS NOT NULL
                  AND LOWER(NULLIF(BTRIM(COALESCE(te0.display_name, '')), '')) ~ '\.(pdf|png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)$'
                  THEN te0.display_name
                WHEN NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
                  THEN REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')
                ELSE te0.display_name
              END
            ),
            ''
          ),
          'Evidence'
        ),
        'storage_key', te0.storage_key,
        'r2_key', te0.storage_key,
        'file_key', te0.storage_key,
        'download_storage_key', te0.storage_key,
        'original_filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
        'mime_type', CASE
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END,
        'content_type', CASE
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END,
        'uploaded_at_utc', te0.created_at,
        'created_at', te0.created_at,
        'created_by', te0.created_by,
        'rotation', 0,
        'rotation_degrees', 0,
        'last_rotation_deg', 0,
        'page_count', NULL::integer,
        'pages', '[]'::jsonb,
        'system', FALSE,
        'is_view_only', NOT COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_delete', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_reclassify', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_kind', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_type', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_return_to_queue', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'preview_mode', CASE
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
          ELSE 'FILE'
        END,
        'source_label', 'Attached',
        'source_badge', 'Attached'
      ) AS item_json
    FROM public.timesheet_evidence AS te0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NOT NULL
      AND te0.timesheet_id = ids.timesheet_id
    UNION ALL
    SELECT
      20::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', mq0.id,
        'evidence_id', NULL::uuid,
        'queue_id', mq0.id,
        'timesheet_id', NULL::uuid,
        'contract_week_id', ids.contract_week_id,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'original_filename', mq0.original_filename,
        'storage_key', mq0.r2_key,
        'r2_key', mq0.r2_key,
        'file_key', mq0.r2_key,
        'download_storage_key', mq0.r2_key,
        'mime_type', COALESCE(NULLIF(BTRIM(mq0.mime_type), ''), CASE
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END),
        'content_type', COALESCE(NULLIF(BTRIM(mq0.mime_type), ''), CASE
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END),
        'content_hash', mq0.content_hash,
        'uploaded_at_utc', mq0.uploaded_at_utc,
        'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
        'staged_by_user_id', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'staged_by_user_id'), ''), mq0.uploaded_by_user_id::text),
        'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
        'last_rotation_deg', COALESCE(mq0.last_rotation_deg::integer, 0),
        'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
        'pages', '[]'::jsonb,
        'status', mq0.status,
        'system', FALSE,
        'is_view_only', NOT COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_delete', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_reclassify', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_kind', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_type', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_return_to_queue', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'is_staged_context', TRUE,
        'preview_mode', CASE
          WHEN LOWER(COALESCE(mq0.mime_type, '')) LIKE 'image/%'
            OR LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
          WHEN LOWER(COALESCE(mq0.mime_type, '')) = 'application/pdf'
            OR LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.pdf($|[?#])' THEN 'PDF'
          ELSE 'FILE'
        END,
        'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
        'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
      ) AS item_json
    FROM public.manual_timesheet_queue AS mq0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NULL
      AND ids.contract_week_id IS NOT NULL
      AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = ids.contract_week_id::text
  ),
  evidence_ranked AS (
    SELECT
      evidence_items.sort_order,
      evidence_items.item_json,
      ROW_NUMBER() OVER (ORDER BY evidence_items.sort_order ASC, evidence_items.item_json->>'id' ASC) AS rn
    FROM evidence_items
  ),
  evidence_payload AS (
    SELECT
      COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
      (
        SELECT er1.item_json
        FROM evidence_ranked AS er1
        WHERE er1.rn = 1
      ) AS primary_evidence_json
    FROM evidence_ranked
  ),
  timesheet_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'timesheet_id', timesheet_row.timesheet_id,
        'booking_id', timesheet_row.booking_id,
        'occupant_key_norm', timesheet_row.occupant_key_norm,
        'hospital_norm', timesheet_row.hospital_norm,
        'ward_norm', timesheet_row.ward_norm,
        'job_title_norm', timesheet_row.job_title_norm,
        'shift_label_norm', timesheet_row.shift_label_norm,
        'scheduled_start_iso', timesheet_row.scheduled_start_iso,
        'scheduled_end_iso', timesheet_row.scheduled_end_iso,
        'worked_start_iso', timesheet_row.worked_start_iso,
        'worked_end_iso', timesheet_row.worked_end_iso,
        'break_start_iso', timesheet_row.break_start_iso,
        'break_end_iso', timesheet_row.break_end_iso,
        'break_minutes', timesheet_row.break_minutes,
        'worked_minutes', timesheet_row.worked_minutes,
        'week_ending_date', timesheet_row.week_ending_date,
        'auth_name', timesheet_row.auth_name,
        'auth_job_title', timesheet_row.auth_job_title,
        'authorised_at_server', timesheet_row.authorised_at_server,
        'r2_nurse_key', timesheet_row.r2_nurse_key,
        'r2_auth_key', timesheet_row.r2_auth_key
      )
      || JSONB_BUILD_OBJECT(
        'reference_number', timesheet_row.reference_number,
        'reference_set_at', timesheet_row.reference_set_at,
        'status', timesheet_row.status,
        'created_at', timesheet_row.created_at,
        'updated_at', timesheet_row.updated_at,
        'version', timesheet_row.version,
        'is_current', timesheet_row.is_current,
        'contract_id', timesheet_row.contract_id,
        'submission_mode', timesheet_row.submission_mode,
        'manual_pdf_r2_key', timesheet_row.manual_pdf_r2_key,
        'line_type', timesheet_row.line_type,
        'sheet_scope', timesheet_row.sheet_scope,
        'actual_schedule_json', timesheet_row.actual_schedule_json,
        'additional_units_week', timesheet_row.additional_units_week,
        'additional_units_per_day', timesheet_row.additional_units_per_day,
        'qr_status', timesheet_row.qr_status,
        'qr_payload_json', timesheet_row.qr_payload_json,
        'qr_generated_at', timesheet_row.qr_generated_at,
        'qr_scanned_at', timesheet_row.qr_scanned_at,
        'qr_scan_info_json', timesheet_row.qr_scan_info_json,
        'qr_r2_key', timesheet_row.qr_r2_key,
        'day_references_json', timesheet_row.day_references_json,
        'manual_pdf_rotation_degrees', timesheet_row.manual_pdf_rotation_degrees
      )
      || JSONB_BUILD_OBJECT(
        'qr_last_sent_hash', timesheet_row.qr_last_sent_hash,
        'qr_last_sent_at_utc', timesheet_row.qr_last_sent_at_utc,
        'qr_signed_hash', timesheet_row.qr_signed_hash,
        'qr_signed_at_utc', timesheet_row.qr_signed_at_utc,
        'candidate_hint_text', timesheet_row.candidate_hint_text,
        'band', timesheet_row.band,
        'generated_pdf_at_utc', timesheet_row.generated_pdf_at_utc,
        'generated_pdf_refs_sig', timesheet_row.generated_pdf_refs_sig,
        'generated_pdf_refs_snapshot_json', timesheet_row.generated_pdf_refs_snapshot_json,
        'generated_pdf_refs_captured_at_utc', timesheet_row.generated_pdf_refs_captured_at_utc,
        'qr_sent_refs_sig', timesheet_row.qr_sent_refs_sig,
        'qr_sent_refs_snapshot_json', timesheet_row.qr_sent_refs_snapshot_json,
        'qr_sent_refs_captured_at_utc', timesheet_row.qr_sent_refs_captured_at_utc,
        'is_adjustment', timesheet_row.is_adjustment,
        'parent_timesheet_id', timesheet_row.parent_timesheet_id,
        'correction_id', timesheet_row.correction_id,
        'correction_kind', timesheet_row.correction_kind,
        'adjustment_origin', timesheet_row.adjustment_origin
      ) AS timesheet_json
    FROM timesheet_row
  ),
  tsfin_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', tsfin_row.id,
        'timesheet_id', tsfin_row.timesheet_id,
        'timesheet_version', tsfin_row.timesheet_version,
        'basis', tsfin_row.basis,
        'is_current', tsfin_row.is_current,
        'is_stale', tsfin_row.is_stale,
        'stale_reason', tsfin_row.stale_reason,
        'worked_start_iso', tsfin_row.worked_start_iso,
        'worked_end_iso', tsfin_row.worked_end_iso,
        'break_start_iso', tsfin_row.break_start_iso,
        'break_end_iso', tsfin_row.break_end_iso,
        'break_minutes', tsfin_row.break_minutes,
        'candidate_id', tsfin_row.candidate_id,
        'client_id', tsfin_row.client_id,
        'role', tsfin_row.role,
        'band', tsfin_row.band,
        'pay_method', tsfin_row.pay_method,
        'policy_snapshot_json', tsfin_row.policy_snapshot_json,
        'rate_source_refs_json', tsfin_row.rate_source_refs_json,
        'hours_day', tsfin_row.hours_day,
        'hours_night', tsfin_row.hours_night,
        'hours_sat', tsfin_row.hours_sat,
        'hours_sun', tsfin_row.hours_sun,
        'hours_bh', tsfin_row.hours_bh
      )
      || JSONB_BUILD_OBJECT(
        'pay_day', tsfin_row.pay_day,
        'pay_night', tsfin_row.pay_night,
        'pay_sat', tsfin_row.pay_sat,
        'pay_sun', tsfin_row.pay_sun,
        'pay_bh', tsfin_row.pay_bh,
        'charge_day', tsfin_row.charge_day,
        'charge_night', tsfin_row.charge_night,
        'charge_sat', tsfin_row.charge_sat,
        'charge_sun', tsfin_row.charge_sun,
        'charge_bh', tsfin_row.charge_bh,
        'total_hours', tsfin_row.total_hours,
        'total_pay_ex_vat', tsfin_row.total_pay_ex_vat,
        'total_charge_ex_vat', tsfin_row.total_charge_ex_vat,
        'margin_ex_vat', tsfin_row.margin_ex_vat,
        'computed_at_utc', tsfin_row.computed_at_utc,
        'locked_by_invoice_id', tsfin_row.locked_by_invoice_id,
        'locked_at_utc', tsfin_row.locked_at_utc,
        'unlocked_by_credit_note_id', tsfin_row.unlocked_by_credit_note_id,
        'created_at', tsfin_row.created_at,
        'updated_at', tsfin_row.updated_at,
        'occupant_key_norm', tsfin_row.occupant_key_norm,
        'candidate_assignment', tsfin_row.candidate_assignment,
        'processing_status', tsfin_row.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'expenses_pay_ex_vat', tsfin_row.expenses_pay_ex_vat,
        'expenses_charge_ex_vat', tsfin_row.expenses_charge_ex_vat,
        'expenses_description', tsfin_row.expenses_description,
        'expenses_evidence_r2_key', tsfin_row.expenses_evidence_r2_key,
        'mileage_pay_ex_vat', tsfin_row.mileage_pay_ex_vat,
        'mileage_charge_ex_vat', tsfin_row.mileage_charge_ex_vat,
        'mileage_evidence_r2_key', tsfin_row.mileage_evidence_r2_key,
        'mileage_pay_rate', tsfin_row.mileage_pay_rate,
        'mileage_charge_rate', tsfin_row.mileage_charge_rate,
        'mileage_units', tsfin_row.mileage_units,
        'travel_pay_ex_vat', tsfin_row.travel_pay_ex_vat,
        'travel_charge_ex_vat', tsfin_row.travel_charge_ex_vat,
        'accommodation_pay_ex_vat', tsfin_row.accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', tsfin_row.accommodation_charge_ex_vat,
        'other_pay_ex_vat', tsfin_row.other_pay_ex_vat,
        'other_charge_ex_vat', tsfin_row.other_charge_ex_vat,
        'po_number', tsfin_row.po_number,
        'pay_on_hold', tsfin_row.pay_on_hold,
        'pay_on_hold_reason', tsfin_row.pay_on_hold_reason,
        'pay_on_hold_since_utc', tsfin_row.pay_on_hold_since_utc,
        'paid_at_utc', tsfin_row.paid_at_utc,
        'paid_by_user_id', tsfin_row.paid_by_user_id,
        'payment_reference', tsfin_row.payment_reference
      )
      || JSONB_BUILD_OBJECT(
        'remittance_last_sent_at_utc', tsfin_row.remittance_last_sent_at_utc,
        'remittance_send_count', tsfin_row.remittance_send_count,
        'pay_wtr_rate_pct_snapshot', tsfin_row.pay_wtr_rate_pct_snapshot,
        'pay_vat_rate_pct_snapshot', tsfin_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot', tsfin_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot', tsfin_row.pay_total_inc_vat_snapshot,
        'processed_by_user_id', tsfin_row.processed_by_user_id,
        'processed_at_utc', tsfin_row.processed_at_utc,
        'authorised_by_user_id', tsfin_row.authorised_by_user_id,
        'authorised_at_utc', tsfin_row.authorised_at_utc,
        'expenses_evidence_manifest', tsfin_row.expenses_evidence_manifest,
        'mileage_evidence_manifest', tsfin_row.mileage_evidence_manifest,
        'actual_schedule_json', tsfin_row.actual_schedule_json,
        'actual_minutes_by_day_json', tsfin_row.actual_minutes_by_day_json,
        'additional_units_json', tsfin_row.additional_units_json,
        'additional_pay_ex_vat', tsfin_row.additional_pay_ex_vat,
        'additional_charge_ex_vat', tsfin_row.additional_charge_ex_vat,
        'additional_margin_ex_vat', tsfin_row.additional_margin_ex_vat,
        'invoice_breakdown_json', tsfin_row.invoice_breakdown_json,
        'nhsp_import_id', tsfin_row.nhsp_import_id,
        'has_rate_issue', tsfin_row.has_rate_issue,
        'has_pay_channel_issue', tsfin_row.has_pay_channel_issue,
        'hr_crosscheck_status', tsfin_row.hr_crosscheck_status,
        'hr_crosscheck_issues', tsfin_row.hr_crosscheck_issues,
        'external_source_rows_json', tsfin_row.external_source_rows_json
      ) AS tsfin_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN NULL::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN JSONB_BUILD_OBJECT('mode', 'SEGMENTS', 'segments', tsfin_row.invoice_breakdown_json)
        ELSE NULL::jsonb
      END AS invoice_breakdown_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND UPPER(COALESCE(tsfin_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS' THEN TRUE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN TRUE
        ELSE FALSE
      END AS is_segments_mode,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json->'segments') = 'array' THEN tsfin_row.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END AS segments_json
    FROM tsfin_row
  ),
  contract_week_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_week_row.id,
        'contract_id', contract_week_row.contract_id,
        'week_ending_date', contract_week_row.week_ending_date,
        'additional_seq', contract_week_row.additional_seq,
        'status', contract_week_row.status,
        'submission_mode_snapshot', contract_week_row.submission_mode_snapshot,
        'timesheet_id', contract_week_row.timesheet_id,
        'uploaded_pdf_r2_key', contract_week_row.uploaded_pdf_r2_key,
        'day_entries_json', contract_week_row.day_entries_json,
        'totals_json', contract_week_row.totals_json,
        'created_at', contract_week_row.created_at,
        'updated_at', contract_week_row.updated_at,
        'planned_schedule_json', contract_week_row.planned_schedule_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'is_adjustment', contract_week_row.is_adjustment,
        'enforce_day_partition', contract_week_row.enforce_day_partition,
        'allowed_days_mask', contract_week_row.allowed_days_mask,
        'split_boundary_date', contract_week_row.split_boundary_date,
        'worker_note', contract_week_row.worker_note,
        'split_group_key', contract_week_row.split_group_key,
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display'
      ) AS contract_week_json
    FROM contract_week_row
    CROSS JOIN row_ids
    LEFT JOIN contract_row ON TRUE
  ),
  contract_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_row.id,
        'candidate_id', contract_row.candidate_id,
        'client_id', contract_row.client_id,
        'role', contract_row.role,
        'band', contract_row.band,
        'display_site', contract_row.display_site,
        'ward_hint', contract_row.ward_hint,
        'start_date', contract_row.start_date,
        'end_date', contract_row.end_date,
        'pay_method_snapshot', contract_row.pay_method_snapshot,
        'rates_json', contract_row.rates_json,
        'std_hours_json', contract_row.std_hours_json,
        'default_submission_mode', contract_row.default_submission_mode,
        'week_ending_weekday_snapshot', contract_row.week_ending_weekday_snapshot,
        'auto_invoice', contract_row.auto_invoice,
        'require_reference_to_pay', contract_row.require_reference_to_pay,
        'require_reference_to_invoice', contract_row.require_reference_to_invoice,
        'bucket_labels_json', contract_row.bucket_labels_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'mileage_pay_rate', contract_row.mileage_pay_rate,
        'mileage_charge_rate', contract_row.mileage_charge_rate,
        'additional_rates_json', contract_row.additional_rates_json,
        'self_bill', contract_row.self_bill,
        'weekly_timesheet_source', contract_row.weekly_timesheet_source,
        'no_timesheet_required', contract_row.no_timesheet_required,
        'daily_calc_of_invoices', contract_row.daily_calc_of_invoices,
        'group_nightsat_sunbh', contract_row.group_nightsat_sunbh,
        'is_nhsp', contract_row.is_nhsp,
        'autoprocess_hr', contract_row.autoprocess_hr,
        'requires_hr', contract_row.requires_hr,
        'hr_attach_to_invoice', contract_row.hr_attach_to_invoice,
        'ts_attach_to_invoice', contract_row.ts_attach_to_invoice,
        'overrideclientsettings', contract_row.overrideclientsettings,
        'reference_number_required_to_issue_invoice', contract_row.reference_number_required_to_issue_invoice,
        'send_manual_invoices_to_different_email', contract_row.send_manual_invoices_to_different_email,
        'manual_invoices_alt_email_address', contract_row.manual_invoices_alt_email_address,
        'is_ad_hoc', contract_row.is_ad_hoc,
        'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
        'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour'
      ) AS contract_json
    FROM contract_row
    CROSS JOIN row_ids
  ),
  related_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'counts', JSONB_BUILD_OBJECT(),
        'candidate', CASE WHEN candidate_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', candidate_row.id,
          'tms_ref', candidate_row.tms_ref,
          'first_name', candidate_row.first_name,
          'last_name', candidate_row.last_name,
          'display_name', candidate_row.display_name,
          'email', candidate_row.email,
          'phone', candidate_row.phone,
          'pay_method', candidate_row.pay_method,
          'umbrella_id', candidate_row.umbrella_id,
          'active', candidate_row.active,
          'key_norm', candidate_row.key_norm,
          'mileage_pay_rate', candidate_row.mileage_pay_rate,
          'job_title_id', candidate_row.job_title_id,
          'prof_reg_number', candidate_row.prof_reg_number,
          'prof_reg_type', candidate_row.prof_reg_type,
          'ni_number', candidate_row.ni_number,
          'date_of_birth', candidate_row.date_of_birth,
          'gender', candidate_row.gender,
          'title', candidate_row.title,
          'opt_in_email', candidate_row.opt_in_email,
          'opt_in_sms', candidate_row.opt_in_sms,
          'opt_in_whatsapp', candidate_row.opt_in_whatsapp,
          'band', candidate_row.band,
          'tms_ref_num', candidate_row.tms_ref_num
        ) END,
        'client', CASE WHEN client_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', client_row.id,
          'cli_ref', client_row.cli_ref,
          'name', client_row.name,
          'invoice_address', client_row.invoice_address,
          'primary_invoice_email', client_row.primary_invoice_email,
          'ap_phone', client_row.ap_phone,
          'vat_chargeable', client_row.vat_chargeable,
          'payment_terms_days', client_row.payment_terms_days,
          'mileage_charge_rate', client_row.mileage_charge_rate,
          'ts_queries_email', client_row.ts_queries_email,
          'client_address', client_row.client_address,
          'contact_title', client_row.contact_title,
          'contact_known_as', client_row.contact_known_as,
          'contact_forename', client_row.contact_forename,
          'contact_surname', client_row.contact_surname,
          'contact_job_title', client_row.contact_job_title,
          'contact_tel', client_row.contact_tel,
          'contact_mobile', client_row.contact_mobile,
          'contact_email', client_row.contact_email,
          'website', client_row.website
        ) END,
        'contract', COALESCE(contract_payload.contract_json, NULL::jsonb),
        'invoices', '[]'::jsonb,
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'invoice', NULL::jsonb,
        'umbrella', NULL::jsonb,
        'series', '[]'::jsonb
      ) AS related_json
    FROM row_ids
    LEFT JOIN candidate_row ON TRUE
    LEFT JOIN client_row ON TRUE
    LEFT JOIN contract_payload ON TRUE
  )
,
  base_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'context_type', 'bulk_process',
        'slim_context', TRUE,
        'evidence_loaded', v_include_evidence,
        'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
        'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
        'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
        'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'timesheet_version')::integer ELSE NULL::integer END,
        'contract_week_id', row_ids.row_json->>'contract_week_id',
        'row_key', row_ids.row_json->>'row_key',
        'row_signature', row_ids.row_json->>'row_signature',
        'was_stale', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'was_stale', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'has_timesheet', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'has_timesheet', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'locked', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'locked', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'bulk_process_bucket', row_ids.row_json->>'bulk_process_bucket',
        'bulk_authorise_classification', row_ids.row_json->>'bulk_authorise_classification',
        'bulk_authorise_section', row_ids.row_json->>'bulk_authorise_section',
        'route_family', row_ids.row_json->>'route_family',
        'route_subfamily', row_ids.row_json->>'route_subfamily',
        'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
        'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'compare_block_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'compare_block_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'is_adjustment', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_adjustment', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'additional_seq', CASE WHEN COALESCE(row_ids.row_json->>'additional_seq', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'additional_seq')::integer ELSE NULL::integer END,
        'actual_schedule_json', COALESCE(row_ids.row_json->'actual_schedule_json', '[]'::jsonb),
        'planned_schedule_json', COALESCE(row_ids.row_json->'planned_schedule_json', '[]'::jsonb),
        'contract_week_totals_json', COALESCE(row_ids.row_json->'contract_week_totals_json', '{}'::jsonb),
        'total_hours', CASE WHEN COALESCE(row_ids.row_json->>'total_hours', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (row_ids.row_json->>'total_hours')::numeric ELSE NULL::numeric END,
        'suppress_standard_schedule_fallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'keep_additional_manual_adjustment_schedule_empty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        '__suppressStandardScheduleFallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        '__keepAdditionalManualAdjustmentScheduleEmpty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'period_type', row_ids.row_json->>'period_type',
        'timesheet_type_sort_key', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_type_sort_key', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'timesheet_type_sort_key')::integer ELSE NULL::integer END,
        'can_save', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_save', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_process', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_process', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_unprocess', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_unprocess', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_timesheet_data', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_edit_timesheet_data', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_manage_evidence', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_add_additional_manual', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_add_additional_manual', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'review_only', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'review_only', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'hr_validation_required_for_invoice', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_required_for_invoice', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'validation_status', row_ids.row_json->>'validation_status',
        'validation_pre_validated', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'validation_pre_validated', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'has_deviation_marker', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'has_deviation_marker', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'deviation_marker_reason', row_ids.row_json->>'deviation_marker_reason'
      )
      || JSONB_BUILD_OBJECT(
        'row', row_ids.row_json,
        'row_patch', COALESCE(row_ids.row_json->'row_patch', JSONB_BUILD_OBJECT()),
        'details', (
          JSONB_BUILD_OBJECT(
            'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
            'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
            'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
            'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'timesheet_version')::integer ELSE NULL::integer END,
            'was_stale', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'was_stale', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'booking_id', row_ids.row_json->>'booking_id',
            'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
            'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
            'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
            'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
            'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
          )
          || JSONB_BUILD_OBJECT(
            'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', row_ids.row_json->>'validation_status',
              'pre_validated', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'validation_pre_validated', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'hr_validation_satisfied', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_satisfied', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'hr_validation_awaiting', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_awaiting', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
            ),
            'shifts', '[]'::jsonb,
            'contract_week_id', row_ids.row_json->>'contract_week_id',
            'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
            'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
            'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
          )
          || JSONB_BUILD_OBJECT(
            'policy', (
              CASE
                WHEN tsfin_payload.tsfin_json IS NOT NULL
                 AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
                ELSE JSONB_BUILD_OBJECT()
              END
              || JSONB_BUILD_OBJECT(
                'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
                'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
                'requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
                'autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
                'no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
                'is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
              )
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', row_ids.row_json->>'route_type',
              'route_display', row_ids.row_json->>'route_display',
              'route_family', row_ids.row_json->>'route_family',
              'route_subfamily', row_ids.row_json->>'route_subfamily',
              'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
              'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'is_adjustment', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_adjustment', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'additional_seq', CASE WHEN COALESCE(row_ids.row_json->>'additional_seq', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'additional_seq')::integer ELSE NULL::integer END,
              'actual_schedule_json', COALESCE(row_ids.row_json->'actual_schedule_json', '[]'::jsonb),
              'planned_schedule_json', COALESCE(row_ids.row_json->'planned_schedule_json', '[]'::jsonb),
              'suppress_standard_schedule_fallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'keep_additional_manual_adjustment_schedule_empty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'summary_stage', row_ids.row_json->>'summary_stage',
              'client_requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'client_autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'client_no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'client_is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'contract_id', row_ids.row_json->>'contract_id',
              'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
            ),
            'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'summary_stage', row_ids.row_json->>'summary_stage',
            'route_type', row_ids.row_json->>'route_type',
            'route_display', row_ids.row_json->>'route_display',
            'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
            'sheet_scope', row_ids.row_json->>'sheet_scope',
            'period_type', row_ids.row_json->>'period_type'
          )
          || JSONB_BUILD_OBJECT(
            'qr_status', row_ids.row_json->>'qr_status',
            'qr_generated_at', row_ids.row_json->>'qr_generated_at',
            'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
            'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
            'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
            'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
            'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees',
            'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
            'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
            'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
            'primary_artifact', CASE
              WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
                'id', row_ids.row_json->>'primary_artifact_id',
                'kind', row_ids.row_json->>'primary_artifact_kind',
                'display_name', row_ids.row_json->>'primary_artifact_display_name',
                'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
                'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
              )
              ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
            END,
            'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
            'pay_state', NULL::jsonb,
            'segment_snoozes', '[]'::jsonb
          )
        ),
        'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
        'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
        'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
        'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
        'validation_summary', JSONB_BUILD_OBJECT(
          'status', row_ids.row_json->>'validation_status',
          'pre_validated', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'validation_pre_validated', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'hr_validation_satisfied', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_satisfied', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'hr_validation_awaiting', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_awaiting', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
        ),
        'shifts', '[]'::jsonb
      )
      || JSONB_BUILD_OBJECT(
        'effective', JSONB_BUILD_OBJECT(
          'route_type', row_ids.row_json->>'route_type',
          'route_display', row_ids.row_json->>'route_display',
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'is_adjustment', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_adjustment', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'additional_seq', CASE WHEN COALESCE(row_ids.row_json->>'additional_seq', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'additional_seq')::integer ELSE NULL::integer END,
          'actual_schedule_json', COALESCE(row_ids.row_json->'actual_schedule_json', '[]'::jsonb),
          'planned_schedule_json', COALESCE(row_ids.row_json->'planned_schedule_json', '[]'::jsonb),
          'suppress_standard_schedule_fallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'keep_additional_manual_adjustment_schedule_empty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'summary_stage', row_ids.row_json->>'summary_stage',
          'client_requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'client_autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'client_no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'client_is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'contract_id', row_ids.row_json->>'contract_id',
          'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
        ),
        'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'summary_stage', row_ids.row_json->>'summary_stage',
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display',
        'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
        'sheet_scope', row_ids.row_json->>'sheet_scope',
        'period_type', row_ids.row_json->>'period_type',
        'qr_status', row_ids.row_json->>'qr_status',
        'qr_generated_at', row_ids.row_json->>'qr_generated_at',
        'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
        'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
        'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
        'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
        'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees'
      )
      || JSONB_BUILD_OBJECT(
        'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
        'policy', (
          CASE
            WHEN tsfin_payload.tsfin_json IS NOT NULL
             AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
            ELSE JSONB_BUILD_OBJECT()
          END
          || JSONB_BUILD_OBJECT(
            'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
            'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
            'requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
          )
        ),
        'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
        'evidence_meta', JSONB_BUILD_OBJECT(
          'has_any_evidence', (
            COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
            OR jsonb_array_length(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) > 0
          ),
          'evidence_badges', JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TIMESHEET' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TIMESHEET' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'MILEAGE' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'MILEAGE' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TRAVEL' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TRAVEL' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'ACCOMMODATION' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'ACCOMMODATION' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'OTHER' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'OTHER' LIMIT 1), FALSE))
          ),
          'attached_evidence_count', GREATEST(
            CASE WHEN COALESCE(row_ids.row_json->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'attached_evidence_count')::integer ELSE 0 END,
            COALESCE(jsonb_array_length(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)), 0)
          ),
          'queue_staged_count', CASE WHEN COALESCE(row_ids.row_json->>'queue_staged_count', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'queue_staged_count')::integer ELSE 0 END,
          'evidence_document_locked', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'evidence_document_locked', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'evidence_lock_reason', row_ids.row_json->>'evidence_lock_reason'
        ),
        'primary_artifact', CASE
          WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'id', row_ids.row_json->>'primary_artifact_id',
            'kind', row_ids.row_json->>'primary_artifact_kind',
            'display_name', row_ids.row_json->>'primary_artifact_display_name',
            'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
          )
          ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
        END,
        'primary_artifact_id', row_ids.row_json->>'primary_artifact_id',
        'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_artifact_preview_mode', row_ids.row_json->>'primary_artifact_preview_mode',
        'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'compare_block_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'compare_block_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'primary_artifact', CASE
            WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
              'id', row_ids.row_json->>'primary_artifact_id',
              'kind', row_ids.row_json->>'primary_artifact_kind',
              'display_name', row_ids.row_json->>'primary_artifact_display_name',
              'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
              'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
            )
            ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
          END,
          'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
          'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
        )
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
        'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
        'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
        'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
        'pay_state', NULL::jsonb,
        'segment_snoozes', '[]'::jsonb,
        'is_advanced', FALSE,
        'can_unadvance', FALSE,
        'advanced_consumed_by_batch_id', NULL::text,
        'is_snoozed', FALSE,
        'snooze_until_date', NULL::text,
        'snooze_is_indefinite', FALSE,
        'snooze_note', NULL::text
      ) AS payload_json
    FROM decision_row
    CROSS JOIN row_ids
    LEFT JOIN timesheet_payload ON TRUE
    LEFT JOIN tsfin_payload ON TRUE
    LEFT JOIN contract_week_payload ON TRUE
    LEFT JOIN validation_payload ON TRUE
    LEFT JOIN evidence_payload ON TRUE
    LEFT JOIN related_payload ON TRUE
  ),
  effective_evidence_payload AS (
    SELECT
      base_payload.payload_json,
      COALESCE(LOWER(NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{evidence_meta,has_any_evidence}', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) AS effective_has_any_evidence,
      COALESCE(base_payload.payload_json#>'{evidence_meta,evidence_badges}', '[]'::jsonb) AS effective_evidence_badges,
      COALESCE(base_payload.payload_json->'primary_artifact', base_payload.payload_json#>'{details,primary_artifact}', NULL::jsonb) AS effective_primary_artifact,
      COALESCE(
        NULLIF(BTRIM(COALESCE(base_payload.payload_json->>'primary_artifact_storage_key', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{primary_artifact,storage_key}', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{details,primary_artifact,storage_key}', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json->>'preview_storage_key', '')), '')
      ) AS effective_primary_artifact_storage_key,
      COALESCE(
        NULLIF(BTRIM(COALESCE(base_payload.payload_json->>'primary_artifact_preview_mode', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{primary_artifact,preview_mode}', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{details,primary_artifact,preview_mode}', '')), '')
      ) AS effective_primary_artifact_preview_mode
    FROM base_payload
  ),
  final_payload AS (
    SELECT
      effective_evidence_payload.payload_json
      || JSONB_BUILD_OBJECT(
        'row',
          COALESCE(effective_evidence_payload.payload_json->'row', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'data_row',
          COALESCE(effective_evidence_payload.payload_json->'row', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'row_patch',
          COALESCE(effective_evidence_payload.payload_json->'row_patch', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'artifact_hints',
          COALESCE(effective_evidence_payload.payload_json->'artifact_hints', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'details',
          COALESCE(effective_evidence_payload.payload_json->'details', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'preview_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode,
            'evidence_meta', COALESCE(effective_evidence_payload.payload_json->'evidence_meta', JSONB_BUILD_OBJECT()),
            'artifact_hints',
              COALESCE(effective_evidence_payload.payload_json#>'{details,artifact_hints}', JSONB_BUILD_OBJECT())
              || JSONB_BUILD_OBJECT(
                'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
                'evidence_badges', effective_evidence_payload.effective_evidence_badges,
                'primary_artifact', effective_evidence_payload.effective_primary_artifact,
                'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
                'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
              )
          ),
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', effective_evidence_payload.payload_json->>'route_family',
          'route_subfamily', effective_evidence_payload.payload_json->>'route_subfamily',
          'underlying_channel_family', effective_evidence_payload.payload_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(effective_evidence_payload.payload_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'compare_block_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(effective_evidence_payload.payload_json->>'compare_block_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'primary_artifact', effective_evidence_payload.effective_primary_artifact,
          'source_items', '[]'::jsonb,
          'evidence', COALESCE(effective_evidence_payload.payload_json->'evidence', '[]'::jsonb),
          'evidence_meta', COALESCE(effective_evidence_payload.payload_json->'evidence_meta', JSONB_BUILD_OBJECT()),
          'primary_left_pane_mode', effective_evidence_payload.payload_json->>'primary_left_pane_mode'
        )
      ) AS payload_json
    FROM effective_evidence_payload
  )
  SELECT final_payload.payload_json
    INTO v_out
  FROM final_payload;

  IF v_out IS NOT NULL THEN
    v_out := v_out || JSONB_BUILD_OBJECT(
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', COALESCE(v_include_evidence, FALSE),
      'compare_loaded', COALESCE(v_include_compare, FALSE),
      'full_loaded', (v_profile = 'full'),
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', CASE
        WHEN v_profile = 'full' THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import', 'full')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE AND COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence')
        WHEN COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'compare_import')
        ELSE JSONB_BUILD_ARRAY('header', 'editor')
      END
    );
  END IF;

  IF v_out IS NOT NULL AND COALESCE(v_include_evidence, FALSE) = TRUE THEN
    SELECT
      (
        COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->'row'->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
        OR COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->'data_row'->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
        OR COALESCE(jsonb_array_length(COALESCE(v_out->'evidence', '[]'::jsonb)), 0) > 0
      ),
      (
        COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
          WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'TIMESHEET'
          LIMIT 1
        ), FALSE)
        OR COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'row'->'evidence_badges', '[]'::jsonb)) AS row_badge(badge_json)
          WHERE UPPER(COALESCE(row_badge.badge_json->>'kind', '')) = 'TIMESHEET'
            AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_badge.badge_json->>'present', row_badge.badge_json->>'has_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE
          LIMIT 1
        ), FALSE)
      ),
      (
        COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
          WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'MILEAGE'
          LIMIT 1
        ), FALSE)
        OR COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'row'->'evidence_badges', '[]'::jsonb)) AS row_badge(badge_json)
          WHERE UPPER(COALESCE(row_badge.badge_json->>'kind', '')) = 'MILEAGE'
            AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_badge.badge_json->>'present', row_badge.badge_json->>'has_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE
          LIMIT 1
        ), FALSE)
      ),
      (
        COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
          WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'TRAVEL'
          LIMIT 1
        ), FALSE)
        OR COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'row'->'evidence_badges', '[]'::jsonb)) AS row_badge(badge_json)
          WHERE UPPER(COALESCE(row_badge.badge_json->>'kind', '')) = 'TRAVEL'
            AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_badge.badge_json->>'present', row_badge.badge_json->>'has_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE
          LIMIT 1
        ), FALSE)
      ),
      (
        COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
          WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'ACCOMMODATION'
          LIMIT 1
        ), FALSE)
        OR COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'row'->'evidence_badges', '[]'::jsonb)) AS row_badge(badge_json)
          WHERE UPPER(COALESCE(row_badge.badge_json->>'kind', '')) = 'ACCOMMODATION'
            AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_badge.badge_json->>'present', row_badge.badge_json->>'has_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE
          LIMIT 1
        ), FALSE)
      ),
      (
        COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
          WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'OTHER'
          LIMIT 1
        ), FALSE)
        OR COALESCE((
          SELECT TRUE
          FROM jsonb_array_elements(COALESCE(v_out->'row'->'evidence_badges', '[]'::jsonb)) AS row_badge(badge_json)
          WHERE UPPER(COALESCE(row_badge.badge_json->>'kind', '')) = 'OTHER'
            AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_badge.badge_json->>'present', row_badge.badge_json->>'has_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE
          LIMIT 1
        ), FALSE)
      )
    INTO
      v_effective_has_any_evidence,
      v_effective_badge_timesheet,
      v_effective_badge_mileage,
      v_effective_badge_travel,
      v_effective_badge_accommodation,
      v_effective_badge_other;

    v_effective_attached_evidence_count := COALESCE(jsonb_array_length(COALESCE(v_out->'evidence', '[]'::jsonb)), 0);

    v_effective_evidence_badges := JSONB_BUILD_ARRAY(
      JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(v_effective_badge_timesheet, FALSE), 'has_evidence', COALESCE(v_effective_badge_timesheet, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(v_effective_badge_mileage, FALSE), 'has_evidence', COALESCE(v_effective_badge_mileage, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(v_effective_badge_travel, FALSE), 'has_evidence', COALESCE(v_effective_badge_travel, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(v_effective_badge_accommodation, FALSE), 'has_evidence', COALESCE(v_effective_badge_accommodation, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(v_effective_badge_other, FALSE), 'has_evidence', COALESCE(v_effective_badge_other, FALSE))
    );

    v_effective_evidence_meta := COALESCE(v_out->'evidence_meta', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0)
    );

    v_effective_artifact_hints := COALESCE(v_out->'artifact_hints', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0)
    );

    v_effective_action_flags := COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0)
    );

    v_effective_row_patch := COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'artifact_hints', v_effective_artifact_hints
    );

    v_effective_row := COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', COALESCE(v_out->'row'->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
        'evidence_badges', v_effective_evidence_badges
      ),
      'row_patch', v_effective_row_patch
    );

    v_effective_data_row := COALESCE(v_out->'data_row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', COALESCE(v_out->'data_row'->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
        'evidence_badges', v_effective_evidence_badges
      ),
      'row_patch', v_effective_row_patch
    );

    v_effective_details := COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'evidence', COALESCE(v_out->'evidence', '[]'::jsonb),
      'evidence_meta', v_effective_evidence_meta,
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', COALESCE(v_out->'details'->'action_flags', v_effective_action_flags) || JSONB_BUILD_OBJECT(
        'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
        'evidence_badges', v_effective_evidence_badges
      )
    );

    v_effective_left_pane := COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'evidence', COALESCE(v_out->'evidence', '[]'::jsonb),
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'artifact_hints', v_effective_artifact_hints
    );

    v_out := v_out || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'evidence_meta', v_effective_evidence_meta,
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', v_effective_action_flags,
      'row_patch', v_effective_row_patch,
      'row', v_effective_row,
      'data_row', v_effective_data_row,
      'details', v_effective_details,
      'left_pane', v_effective_left_pane
    );
  END IF;


  IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
    v_canonical_row_json := NULL;
    v_canonical_row_signature := NULL;
    v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
    v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
    v_canonical_contract_week_id := CASE
      WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
      ELSE NULL
    END;

    IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
      SELECT canonical_result.row_json
      INTO v_canonical_row_json
      FROM public.bulk_timesheet_row_patch_v1(
        JSONB_STRIP_NULLS(
          JSONB_BUILD_OBJECT(
            'dataset_mode', 'process',
            'projection', 'active_row_header',
            'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
            'row_key', v_canonical_row_key,
            'timesheet_id', v_canonical_timesheet_id,
            'current_timesheet_id', v_canonical_timesheet_id,
            'requested_timesheet_id', v_canonical_timesheet_id,
            'expected_timesheet_id', v_canonical_timesheet_id,
            'contract_week_id', v_canonical_contract_week_id
          )
        )
      ) AS canonical_result(row_json)
      WHERE canonical_result.row_json IS NOT NULL
      ORDER BY canonical_result.row_json->>'row_key'
      LIMIT 1;

      v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
      IF v_canonical_row_signature IS NOT NULL THEN
        v_out := v_out || JSONB_BUILD_OBJECT(
          'row_signature', v_canonical_row_signature,
          'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
          'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
          'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
          'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
        );
      END IF;
    END IF;
  END IF;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'context_kind', 'bulk_process_row_context',
    'context_profile', v_profile,
    'profile', v_profile,
    'soft_failure', TRUE,
    'context_degraded', TRUE,
    'degraded_reason', 'ROW_NOT_FOUND',
    'header_loaded', FALSE,
    'header_only', FALSE,
    'editor_loaded', FALSE,
    'evidence_loaded', FALSE,
    'compare_loaded', FALSE,
    'full_loaded', FALSE,
    'schedule_pending', TRUE,
    'schedule_authoritative', FALSE,
    'loaded_layers', '[]'::jsonb,
    'error', 'ROW_NOT_FOUND',
    'message', 'No bulk process row context was found for the supplied identity',
    'filters', v_filters
  ));
END;
$function$;










CREATE OR REPLACE FUNCTION public.bulk_authorise_row_context_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_decision_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_has_identity boolean := FALSE;
  v_row_key text := NULL;
  v_id_text text := NULL;
  v_timesheet_id_text text := NULL;
  v_contract_week_id_text text := NULL;
  v_include_evidence boolean := FALSE;
  v_include_compare boolean := FALSE;
  v_include_import_source_rows boolean := FALSE;
  v_profile text := NULL;
  v_base_only boolean := FALSE;
  v_header_row_json jsonb := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_out jsonb;
  v_editor_layer jsonb := NULL;
  v_evidence_layer jsonb := NULL;
  v_compare_layer jsonb := NULL;
  v_layer_errors jsonb := '[]'::jsonb;
  v_layer_names jsonb := '[]'::jsonb;
BEGIN
  v_has_identity := (
    NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'week_id', v_filters->>'weekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '') IS NOT NULL
    OR (v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' AND jsonb_array_length(v_filters->'row_keys') > 0)
    OR (v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' AND jsonb_array_length(v_filters->'timesheet_ids') > 0)
    OR (v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' AND jsonb_array_length(v_filters->'contract_week_ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
    OR (v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' AND jsonb_array_length(v_filters->'rowKeys') > 0)
    OR (v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' AND jsonb_array_length(v_filters->'timesheetIds') > 0)
    OR (v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' AND jsonb_array_length(v_filters->'contractWeekIds') > 0)
    OR (v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' AND jsonb_array_length(v_filters->'ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
  );

  IF v_has_identity = FALSE THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'context_kind', 'bulk_authorise_row_context',
      'context_profile', 'status_header',
      'profile', 'status_header',
      'soft_failure', TRUE,
      'context_degraded', TRUE,
      'degraded_reason', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'header_loaded', FALSE,
      'header_only', FALSE,
      'editor_loaded', FALSE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', TRUE,
      'schedule_authoritative', FALSE,
      'loaded_layers', '[]'::jsonb,
      'error', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'message', 'bulk_authorise_row_context_v1 requires row_key, timesheet_id, or contract_week_id.',
      'filters', v_filters
    );
  END IF;

  v_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '');
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '');
  v_contract_week_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', v_filters->>'week_id', v_filters->>'weekId', '')), '');
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_row_key IS NULL AND v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'row_keys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'rowKeys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL THEN
    v_row_key := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys'), ',', 1)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheet_ids') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheetIds') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_timesheet_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '');
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contract_week_ids') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contractWeekIds') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_contract_week_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '');
  END IF;

  IF v_id_text IS NULL AND v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT NULLIF(BTRIM(id_values.value), '')
      INTO v_id_text
    FROM jsonb_array_elements_text(v_filters->'ids') WITH ORDINALITY AS id_values(value, ordinal_position)
    WHERE id_values.value ~* v_uuid_re
    ORDER BY id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '') ~* v_uuid_re THEN
    v_id_text := NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_timesheet_id_text IS NULL AND v_row_key LIKE 'timesheet:%' THEN
    v_timesheet_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 11)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_contract_week_id_text IS NULL AND v_row_key LIKE 'contract_week:%' THEN
    v_contract_week_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 15)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_contract_week_id_text IS NULL AND v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    IF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('contract_week', 'contract-week', 'week', 'contractweek')
       OR LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_contract_week_only', v_filters->>'isContractWeekOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN
      v_contract_week_id_text := v_id_text;
    ELSIF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('timesheet', 'timesheet_id', 'timesheet-id', 'ts') THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.timesheets AS identity_ts
      WHERE identity_ts.timesheet_id = v_id_text::uuid
        AND identity_ts.is_current = TRUE
    ) THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.contract_weeks AS identity_cw
      WHERE identity_cw.id = v_id_text::uuid
    ) THEN
      v_contract_week_id_text := v_id_text;
    ELSE
      v_timesheet_id_text := v_id_text;
    END IF;
  END IF;

  IF v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('timesheet_id', v_timesheet_id_text);
  END IF;

  IF v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('contract_week_id', v_contract_week_id_text);
  END IF;

  IF v_row_key IS NOT NULL THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('row_key', v_row_key);
  END IF;

  v_base_only := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'base_only', v_filters->>'baseOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on');

  v_profile := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'profile', v_filters->>'context_profile', v_filters->>'contextProfile', '')), ''));

  IF v_profile IS NULL THEN
    v_profile := 'status_header';
  END IF;

  IF v_profile NOT IN ('active_row_visible', 'status_header', 'editor', 'evidence', 'compare_import', 'full') THEN
    v_profile := 'status_header';
  END IF;

  v_include_evidence := CASE
    WHEN v_profile = 'evidence' THEN TRUE
    WHEN v_profile = 'full' THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_evidence', v_filters->>'includeEvidence', v_filters->>'load_evidence', v_filters->>'loadEvidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_compare := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_compare', v_filters->>'includeCompare', v_filters->>'load_compare', v_filters->>'loadCompare', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_import_source_rows := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_import_source_rows', v_filters->>'includeImportSourceRows', v_filters->>'load_import_source_rows', v_filters->>'loadImportSourceRows', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;


  IF v_profile = 'active_row_visible' AND (
    COALESCE(v_include_evidence, FALSE) = TRUE
    OR COALESCE(v_include_compare, FALSE) = TRUE
    OR COALESCE(v_include_import_source_rows, FALSE) = TRUE
  ) THEN
    v_layer_errors := '[]'::jsonb;
    v_layer_names := JSONB_BUILD_ARRAY('header', 'editor');

    v_editor_layer := public.bulk_authorise_row_context_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'profile', 'editor',
           'context_profile', 'editor',
           'include_evidence', FALSE,
           'include_compare', FALSE,
           'include_import_source_rows', FALSE
         )
    );

    IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = FALSE THEN
      RETURN COALESCE(v_editor_layer, JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'active_row_visible',
        'profile', 'active_row_visible',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', COALESCE(v_editor_layer->>'degraded_reason', v_editor_layer->>'error', 'EDITOR_LAYER_FAILED'),
        'header_loaded', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'header_loaded', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'loaded_layers', COALESCE(v_editor_layer->'loaded_layers', '[]'::jsonb),
        'filters', v_filters
      );
    END IF;

    v_out := v_editor_layer || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_authorise_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_authorise',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', v_layer_names
    );

    IF COALESCE(v_include_evidence, FALSE) = TRUE THEN
      v_evidence_layer := public.bulk_authorise_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'evidence',
             'context_profile', 'evidence',
             'include_evidence', TRUE,
             'include_compare', FALSE,
             'include_import_source_rows', FALSE
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('evidence');
        v_out := v_out
          || JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
            'attached_evidence', COALESCE(v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'attachedRows', COALESCE(v_evidence_layer->'attachedRows', v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'primary_artifact', COALESCE(v_evidence_layer->'primary_artifact', v_out->'primary_artifact', v_out#>'{details,primary_artifact}', JSONB_BUILD_OBJECT()),
            'preview_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'preview_storage_key', '')), '')),
            'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'primary_artifact_storage_key', '')), '')),
            'primary_artifact_preview_mode', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'primary_artifact_preview_mode', '')), '')),
            'has_any_evidence', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'attached_evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'attached_evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_badges', COALESCE(v_evidence_layer->'evidence_badges', v_evidence_layer#>'{evidence_meta,evidence_badges}', '[]'::jsonb),
            'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE),
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'data_row', v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row_patch', JSONB_BUILD_OBJECT()),
            'action_flags', COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'action_flags', JSONB_BUILD_OBJECT()),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE)
            ),
            'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE)
            )
          );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_evidence_layer->>'degraded_reason', v_evidence_layer->>'error', 'EVIDENCE_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'evidence_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', 'EVIDENCE_LAYER_FAILED',
          'evidence_layer_failure', COALESCE(v_evidence_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    IF COALESCE(v_include_compare, FALSE) = TRUE OR COALESCE(v_include_import_source_rows, FALSE) = TRUE THEN
      v_compare_layer := public.bulk_authorise_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'compare_import',
             'context_profile', 'compare_import',
             'include_evidence', FALSE,
             'include_compare', COALESCE(v_include_compare, FALSE),
             'include_import_source_rows', COALESCE(v_include_import_source_rows, FALSE)
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_compare_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('compare_import');
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', TRUE,
          'compare', COALESCE(v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'compare_payload', COALESCE(v_compare_layer->'compare_payload', v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_rows', COALESCE(v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb),
            'external_source_rows_json', COALESCE(v_compare_layer#>'{details,external_source_rows_json}', v_compare_layer#>'{compare,external_source_rows_json}', '[]'::jsonb)
          ),
          'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_items', COALESCE(v_compare_layer#>'{left_pane,source_items}', v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb)
          )
        );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_compare_layer->>'degraded_reason', v_compare_layer->>'error', 'COMPARE_IMPORT_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', CASE WHEN JSONB_ARRAY_LENGTH(v_layer_errors) > 0 THEN 'LAYER_FAILURE' ELSE 'COMPARE_IMPORT_LAYER_FAILED' END,
          'compare_import_layer_failure', COALESCE(v_compare_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    v_out := v_out || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_authorise_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_authorise',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'loaded_layers', v_layer_names,
      'layer_errors', v_layer_errors,
      'filters', v_filters
    );

    RETURN v_out;
  END IF;


  IF v_profile IN ('status_header', 'editor') OR (v_profile = 'active_row_visible' AND COALESCE(v_include_evidence, FALSE) = FALSE) THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (
          cw_ids.input_contract_week_id IS NOT NULL
          AND cw0.id = cw_ids.input_contract_week_id
        )
        OR (
          cw_ids.input_timesheet_id IS NOT NULL
          AND cw0.timesheet_id = cw_ids.input_timesheet_id
        )
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    contract_row AS (
      SELECT ct0.*
      FROM public.contracts AS ct0
      LEFT JOIN timesheet_row AS ts_for_ct ON TRUE
      LEFT JOIN contract_week_row AS cw_for_ct ON TRUE
      WHERE ct0.id = COALESCE(ts_for_ct.contract_id, cw_for_ct.contract_id)
      ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
      LIMIT 1
    ),
    candidate_row AS (
      SELECT cand0.*
      FROM public.candidates AS cand0
      LEFT JOIN contract_row AS ct_for_cand ON TRUE
      WHERE cand0.id = ct_for_cand.candidate_id
      LIMIT 1
    ),
    client_row AS (
      SELECT cli0.*
      FROM public.clients AS cli0
      LEFT JOIN contract_row AS ct_for_cli ON TRUE
      WHERE cli0.id = ct_for_cli.client_id
      LIMIT 1
    ),
    validation_rows AS (
      SELECT tv0.*
      FROM public.timesheet_validations AS tv0
      LEFT JOIN timesheet_row AS ts_for_tv ON TRUE
      WHERE ts_for_tv.timesheet_id IS NOT NULL
        AND tv0.timesheet_id = ts_for_tv.timesheet_id
      ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
    ),
    validation_payload AS (
      SELECT
        COALESCE(JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'id', validation_rows.id,
            'timesheet_id', validation_rows.timesheet_id,
            'booking_id', validation_rows.booking_id,
            'status', validation_rows.status,
            'reason_code', validation_rows.reason_code,
            'hr_request_id', validation_rows.hr_request_id,
            'validated_at_utc', validation_rows.validated_at_utc,
            'override_requested_at_utc', validation_rows.override_requested_at_utc,
            'override_requested_by', validation_rows.override_requested_by,
            'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
            'override_confirmed_by', validation_rows.override_confirmed_by,
            'override_reason', validation_rows.override_reason,
            'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
            'last_source', validation_rows.last_source,
            'created_at', validation_rows.created_at,
            'updated_at', validation_rows.updated_at,
            'hr_request_source', validation_rows.hr_request_source,
            'hr_request_set_by', validation_rows.hr_request_set_by,
            'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
            'pre_validated', validation_rows.pre_validated
          ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
        ), '[]'::jsonb) AS validations_json,
        (
          SELECT JSONB_BUILD_OBJECT(
            'id', validation_latest.id,
            'status', validation_latest.status,
            'reason_code', validation_latest.reason_code,
            'hr_request_id', validation_latest.hr_request_id,
            'validated_at_utc', validation_latest.validated_at_utc,
            'pre_validated', COALESCE(validation_latest.pre_validated, FALSE),
            'updated_at', validation_latest.updated_at
          )
          FROM validation_rows AS validation_latest
          ORDER BY validation_latest.validated_at_utc DESC NULLS LAST, validation_latest.created_at DESC NULLS LAST, validation_latest.id DESC
          LIMIT 1
        ) AS latest_validation_json
      FROM validation_rows
    ),
    core_row AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id, contract_row.id) AS resolved_contract_id,
        COALESCE(summary_row.candidate_id, contract_row.candidate_id, candidate_row.id) AS resolved_candidate_id,
        COALESCE(summary_row.client_id, contract_row.client_id, client_row.id) AS resolved_client_id,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_name,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_display_name,
        COALESCE(NULLIF(BTRIM(summary_row.client_name), ''), NULLIF(BTRIM(client_row.name), '')) AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.work_date, timesheet_row.worked_start_iso::date, contract_week_row.week_ending_date) AS work_date,
        COALESCE(summary_row.booking_id, timesheet_row.booking_id) AS booking_id,
        COALESCE(summary_row.occupant_key_norm, timesheet_row.occupant_key_norm, tsfin_row.occupant_key_norm) AS occupant_key_norm,
        COALESCE(summary_row.hospital_norm, timesheet_row.hospital_norm) AS hospital_norm,
        COALESCE(summary_row.candidate_hint_text, timesheet_row.candidate_hint_text, JSONB_BUILD_OBJECT()) AS candidate_hint_text,
        COALESCE(summary_row.sheet_scope, timesheet_row.sheet_scope::text) AS sheet_scope,
        COALESCE(summary_row.submission_mode, timesheet_row.submission_mode::text) AS submission_mode,
        COALESCE(summary_row.submission_mode_snapshot, contract_week_row.submission_mode_snapshot::text) AS submission_mode_snapshot,
        COALESCE(summary_row.basis, tsfin_row.basis::text) AS basis,
        summary_row.route_type AS route_type,
        summary_row.route_display AS route_display,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        summary_row.route_subfamily AS route_subfamily,
        summary_row.underlying_channel_family AS underlying_channel_family,
        COALESCE(summary_row.summary_stage, CASE WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED' ELSE 'PROCESSED' END) AS summary_stage,
        COALESCE(summary_row.tools_stage, CASE WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED' ELSE COALESCE(tsfin_row.processing_status::text, timesheet_row.status::text) END) AS tools_stage,
        COALESCE(summary_row.processing_status, tsfin_row.processing_status::text) AS processing_status,
        summary_row.processing_status_display AS processing_status_display,
        COALESCE(summary_row.authorised_at_utc, tsfin_row.authorised_at_utc) AS authorised_at_utc,
        COALESCE(summary_row.authorised_at_server, timesheet_row.authorised_at_server) AS authorised_at_server,
        COALESCE(summary_row.processed_at_utc, tsfin_row.processed_at_utc) AS processed_at_utc,
        COALESCE(summary_row.is_authorised, tsfin_row.authorised_at_utc IS NOT NULL, timesheet_row.authorised_at_server IS NOT NULL, FALSE) AS is_authorised,
        COALESCE(summary_row.total_hours, tsfin_row.total_hours) AS total_hours,
        COALESCE(summary_row.total_pay_ex_vat, tsfin_row.total_pay_ex_vat) AS total_pay_ex_vat,
        COALESCE(summary_row.total_charge_ex_vat, tsfin_row.total_charge_ex_vat) AS total_charge_ex_vat,
        COALESCE(summary_row.margin_ex_vat, tsfin_row.margin_ex_vat) AS margin_ex_vat,
        summary_row.net_delta_ex_vat AS net_delta_ex_vat,
        COALESCE(summary_row.paid_at_utc, tsfin_row.paid_at_utc) AS paid_at_utc,
        summary_row.pay_icon_code AS pay_icon_code,
        summary_row.pay_status_code AS pay_status_code,
        summary_row.pay_paid_at_utc AS pay_paid_at_utc,
        COALESCE(summary_row.invoice_is_paid, FALSE) AS invoice_is_paid,
        summary_row.invoice_issue_stage AS invoice_issue_stage,
        summary_row.invoice_segment_stage AS invoice_segment_stage,
        COALESCE(summary_row.invoice_segments_total, 0) AS invoice_segments_total,
        COALESCE(summary_row.invoice_segments_locked, 0) AS invoice_segments_locked,
        COALESCE(summary_row.invoice_segments_unlocked, 0) AS invoice_segments_unlocked,
        COALESCE(TO_JSONB(summary_row.issue_codes), '[]'::jsonb) AS issue_codes_json,
        COALESCE(summary_row.validation_status, validation_payload.latest_validation_json->>'status') AS validation_status,
        summary_row.validation_summary AS validation_summary,
        COALESCE(summary_row.hr_crosscheck_status, tsfin_row.hr_crosscheck_status) AS hr_crosscheck_status,
        COALESCE(TO_JSONB(summary_row.hr_crosscheck_issues), TO_JSONB(tsfin_row.hr_crosscheck_issues), '[]'::jsonb) AS hr_crosscheck_issues_json,
        COALESCE(summary_row.qr_status, timesheet_row.qr_status::text) AS qr_status,
        COALESCE(summary_row.is_qr, timesheet_row.qr_status IS NOT NULL, FALSE) AS is_qr,
        COALESCE(summary_row.is_adjusted, timesheet_row.is_adjustment, contract_week_row.is_adjustment, FALSE) AS is_adjusted,
        COALESCE(summary_row.needs_attention, FALSE) AS needs_attention,
        COALESCE(summary_row.has_rate_issue, tsfin_row.has_rate_issue, FALSE) AS has_rate_issue,
        COALESCE(summary_row.has_pay_channel_issue, tsfin_row.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
        COALESCE(summary_row.client_no_timesheet_required, contract_row.no_timesheet_required, FALSE) AS client_no_timesheet_required,
        COALESCE(summary_row.client_autoprocess_hr, contract_row.autoprocess_hr, FALSE) AS client_autoprocess_hr,
        COALESCE(summary_row.client_is_nhsp, contract_row.is_nhsp, FALSE) AS client_is_nhsp,
        COALESCE(summary_row.has_any_evidence, FALSE) AS has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS attached_evidence_count,
        summary_row.primary_artifact_storage_key AS primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS primary_artifact_preview_mode,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.version AS timesheet_version,
        timesheet_row.status AS timesheet_status,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.generated_pdf_at_utc AS generated_pdf_at_utc,
        timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
        timesheet_row.additional_units_week AS timesheet_additional_units_week,
        timesheet_row.additional_units_per_day AS timesheet_additional_units_per_day,
        timesheet_row.sheet_scope AS timesheet_sheet_scope,
        timesheet_row.worked_start_iso AS timesheet_worked_start_iso,
        timesheet_row.worked_end_iso AS timesheet_worked_end_iso,
        timesheet_row.break_start_iso AS timesheet_break_start_iso,
        timesheet_row.break_end_iso AS timesheet_break_end_iso,
        timesheet_row.break_minutes AS timesheet_break_minutes,
        timesheet_row.worked_minutes AS timesheet_worked_minutes,
        timesheet_row.auth_name AS timesheet_auth_name,
        timesheet_row.auth_job_title AS timesheet_auth_job_title,
        timesheet_row.reference_number AS timesheet_reference_number,
        timesheet_row.reference_set_at AS timesheet_reference_set_at,
        timesheet_row.created_at AS timesheet_created_at,
        timesheet_row.is_adjustment AS timesheet_is_adjustment,
        timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
        timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
        contract_week_row.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
        contract_week_row.day_entries_json AS contract_week_day_entries_json,
        contract_week_row.totals_json AS contract_week_totals_json,
        contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
        contract_week_row.additional_seq AS contract_week_additional_seq,
        contract_week_row.status AS contract_week_status,
        contract_week_row.created_at AS contract_week_created_at,
        contract_week_row.is_adjustment AS contract_week_is_adjustment,
        contract_week_row.enforce_day_partition AS contract_week_enforce_day_partition,
        contract_week_row.allowed_days_mask AS contract_week_allowed_days_mask,
        contract_week_row.split_boundary_date AS contract_week_split_boundary_date,
        contract_week_row.worker_note AS contract_week_worker_note,
        contract_week_row.split_group_key AS contract_week_split_group_key,
        contract_row.id AS contract_id,
        contract_row.role AS contract_role,
        contract_row.band AS contract_band,
        contract_row.display_site AS contract_display_site,
        contract_row.ward_hint AS contract_ward_hint,
        contract_row.default_submission_mode AS contract_default_submission_mode,
        contract_row.std_schedule_json AS contract_std_schedule_json,
        contract_row.additional_rates_json AS contract_additional_rates_json,
        contract_row.weekly_timesheet_source AS contract_weekly_timesheet_source,
        contract_row.no_timesheet_required AS contract_no_timesheet_required,
        contract_row.autoprocess_hr AS contract_autoprocess_hr,
        contract_row.requires_hr AS contract_requires_hr,
        contract_row.hr_attach_to_invoice AS contract_hr_attach_to_invoice,
        contract_row.ts_attach_to_invoice AS contract_ts_attach_to_invoice,
        contract_row.is_nhsp AS contract_is_nhsp,
        candidate_row.id AS candidate_id,
        candidate_row.tms_ref AS candidate_tms_ref,
        candidate_row.first_name AS candidate_first_name,
        candidate_row.last_name AS candidate_last_name,
        candidate_row.display_name AS candidate_display_name_raw,
        candidate_row.email AS candidate_email,
        candidate_row.phone AS candidate_phone,
        candidate_row.key_norm AS candidate_key_norm,
        candidate_row.band AS candidate_band,
        client_row.id AS client_id,
        client_row.cli_ref AS client_cli_ref,
        client_row.name AS client_name_raw,
        client_row.vat_chargeable AS client_vat_chargeable,
        client_row.ts_queries_email AS client_ts_queries_email,
        tsfin_row.id AS tsfin_id,
        tsfin_row.timesheet_version AS tsfin_timesheet_version,
        tsfin_row.basis AS tsfin_basis,
        tsfin_row.is_current AS tsfin_is_current,
        tsfin_row.is_stale AS tsfin_is_stale,
        tsfin_row.stale_reason AS tsfin_stale_reason,
        tsfin_row.actual_schedule_json AS tsfin_actual_schedule_json,
        tsfin_row.actual_minutes_by_day_json AS tsfin_actual_minutes_by_day_json,
        tsfin_row.additional_units_json AS tsfin_additional_units_json,
        tsfin_row.processing_status AS tsfin_processing_status,
        tsfin_row.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
        tsfin_row.locked_at_utc AS tsfin_locked_at_utc,
        tsfin_row.paid_at_utc AS tsfin_paid_at_utc,
        tsfin_row.processed_at_utc AS tsfin_processed_at_utc,
        tsfin_row.authorised_at_utc AS tsfin_authorised_at_utc,
        tsfin_row.external_source_rows_json AS tsfin_external_source_rows_json,
        validation_payload.validations_json AS validations_json,
        validation_payload.latest_validation_json AS latest_validation_json
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN contract_row ON TRUE
      LEFT JOIN candidate_row ON TRUE
      LEFT JOIN client_row ON TRUE
      LEFT JOIN validation_payload ON TRUE
    ),
    flags AS (
      SELECT
        core_row.*,
        (core_row.resolved_timesheet_id IS NOT NULL OR core_row.resolved_contract_week_id IS NOT NULL) AS row_found,
        (
          core_row.tsfin_locked_by_invoice_id IS NOT NULL
          OR core_row.tsfin_paid_at_utc IS NOT NULL
          OR core_row.paid_at_utc IS NOT NULL
          OR COALESCE(core_row.invoice_segments_locked, 0) > 0
          OR COALESCE(core_row.invoice_is_paid, FALSE) = TRUE
        ) AS locked_bool,
        COALESCE(core_row.is_authorised, FALSE) AS authorised_bool,
        (
          core_row.resolved_timesheet_id IS NULL
          OR UPPER(COALESCE(core_row.processing_status, core_row.tools_stage, core_row.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
        ) AS unprocessed_bool,
        (
          UPPER(COALESCE(core_row.processing_status, '')) = 'PENDING_AUTH'
          OR (
            COALESCE(core_row.contract_requires_hr, FALSE) = TRUE
            AND COALESCE(core_row.contract_autoprocess_hr, FALSE) = FALSE
            AND UPPER(COALESCE(core_row.processing_status, '')) = 'READY_FOR_HR'
          )
        ) AS requires_authorisation_bool,
        (
          UPPER(COALESCE(core_row.qr_status, '')) = 'PENDING'
          AND core_row.resolved_timesheet_id IS NOT NULL
        ) AS qr_unsigned_blocked_bool,
        (
          COALESCE(core_row.timesheet_is_adjustment, core_row.contract_week_is_adjustment, FALSE) = TRUE
          AND COALESCE(core_row.contract_week_additional_seq, 0) > 0
          AND (
            core_row.timesheet_actual_schedule_json IS NULL
            OR core_row.timesheet_actual_schedule_json = '[]'::jsonb
            OR (jsonb_typeof(core_row.timesheet_actual_schedule_json) = 'array' AND jsonb_array_length(core_row.timesheet_actual_schedule_json) = 0)
          )
        ) AS keep_blank_additional_schedule_bool
      FROM core_row
    ),
    row_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_authorise_row_context',
          'context_profile', v_profile,
          'profile', v_profile,
          'context_type', CASE WHEN 'bulk_authorise_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', TRUE,
          'header_only', (v_profile = 'status_header'),
          'editor_loaded', (v_profile IN ('editor', 'active_row_visible')),
          'evidence_loaded', FALSE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', NOT (v_profile IN ('editor', 'active_row_visible')),
          'schedule_authoritative', (v_profile IN ('editor', 'active_row_visible')),
          'loaded_layers', CASE WHEN v_profile = 'status_header' THEN JSONB_BUILD_ARRAY('header') ELSE JSONB_BUILD_ARRAY('header', 'editor') END,
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
          'timesheet_id', flags.resolved_timesheet_id,
          'current_timesheet_id', flags.resolved_timesheet_id,
          'requested_timesheet_id', flags.resolved_timesheet_id,
          'expected_timesheet_id', flags.resolved_timesheet_id,
          'contract_week_id', flags.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(flags.resolved_timesheet_id::text, ''), COALESCE(flags.resolved_contract_week_id::text, ''), COALESCE(flags.timesheet_version::text, ''), COALESCE(flags.timesheet_updated_at::text, ''), COALESCE(flags.contract_week_updated_at::text, ''), COALESCE(flags.tsfin_updated_at::text, ''), v_profile)),
          'filters', v_filters
        )
        || JSONB_BUILD_OBJECT(
          'row', (
            JSONB_BUILD_OBJECT(
              'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
              'timesheet_id', flags.resolved_timesheet_id,
              'current_timesheet_id', flags.resolved_timesheet_id,
              'requested_timesheet_id', flags.resolved_timesheet_id,
              'expected_timesheet_id', flags.resolved_timesheet_id,
              'contract_week_id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'candidate_id', flags.resolved_candidate_id,
              'candidate_name', flags.candidate_name,
              'candidate_display_name', flags.candidate_display_name,
              'client_id', flags.resolved_client_id,
              'client_name', flags.client_name,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'candidate_hint_text', flags.candidate_hint_text,
              'week_ending_date', flags.week_ending_date,
              'work_date', flags.work_date,
              'sheet_scope', flags.sheet_scope,
              'submission_mode', flags.submission_mode,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'basis', flags.basis,
              'status', flags.timesheet_status,
              'contract_week_status', flags.contract_week_status,
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'summary_stage', flags.summary_stage,
              'tools_stage', flags.tools_stage,
              'processing_status', flags.processing_status,
              'processing_status_display', flags.processing_status_display,
              'authorised_at_utc', flags.authorised_at_utc,
              'authorised_at_server', flags.authorised_at_server,
              'processed_at_utc', flags.processed_at_utc,
              'is_authorised', flags.authorised_bool,
              'locked', flags.locked_bool,
              'has_timesheet', flags.resolved_timesheet_id IS NOT NULL
            )
            || JSONB_BUILD_OBJECT(
              'total_hours', flags.total_hours,
              'total_pay_ex_vat', flags.total_pay_ex_vat,
              'total_charge_ex_vat', flags.total_charge_ex_vat,
              'margin_ex_vat', flags.margin_ex_vat,
              'net_delta_ex_vat', flags.net_delta_ex_vat,
              'paid_at_utc', flags.paid_at_utc,
              'pay_icon_code', flags.pay_icon_code,
              'pay_status_code', flags.pay_status_code,
              'pay_paid_at_utc', flags.pay_paid_at_utc,
              'invoice_is_paid', flags.invoice_is_paid,
              'invoice_issue_stage', flags.invoice_issue_stage,
              'invoice_segment_stage', flags.invoice_segment_stage,
              'invoice_segments_total', flags.invoice_segments_total,
              'invoice_segments_locked', flags.invoice_segments_locked,
              'invoice_segments_unlocked', flags.invoice_segments_unlocked,
              'issue_codes', flags.issue_codes_json,
              'validation_status', flags.validation_status,
              'validation_summary', flags.validation_summary,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json,
              'qr_status', flags.qr_status,
              'is_qr', flags.is_qr,
              'is_adjusted', flags.is_adjusted,
              'needs_attention', flags.needs_attention,
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'client_no_timesheet_required', flags.client_no_timesheet_required,
              'client_autoprocess_hr', flags.client_autoprocess_hr,
              'client_is_nhsp', flags.client_is_nhsp
            )
            || JSONB_BUILD_OBJECT(
              'has_any_evidence', flags.has_any_evidence,
              'attached_evidence_count', flags.attached_evidence_count,
              'evidence_count', flags.attached_evidence_count,
              'primary_artifact_storage_key', flags.primary_artifact_storage_key,
              'primary_artifact_display_name', flags.primary_artifact_display_name,
              'primary_artifact_preview_mode', flags.primary_artifact_preview_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'qr_r2_key', flags.qr_r2_key,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'period_type', CASE WHEN flags.resolved_contract_week_id IS NOT NULL THEN 'WEEKLY' ELSE 'DAILY' END,
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              '__suppressStandardScheduleFallback', flags.keep_blank_additional_schedule_bool,
              '__keepAdditionalManualAdjustmentScheduleEmpty', flags.keep_blank_additional_schedule_bool
            )
            || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'contract_week_totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
            ) ELSE JSONB_BUILD_OBJECT() END
            || JSONB_BUILD_OBJECT(
              'action_flags', JSONB_BUILD_OBJECT(
                'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
                'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
                'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
                'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
                'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
                'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
                'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
                'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
                'additional_seq', flags.contract_week_additional_seq,
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count
              ),
              'row_patch', JSONB_BUILD_OBJECT(),
              'artifact_hints', JSONB_BUILD_OBJECT(
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count,
                'primary_artifact_storage_key', flags.primary_artifact_storage_key,
                'primary_artifact_display_name', flags.primary_artifact_display_name,
                'primary_artifact_preview_mode', flags.primary_artifact_preview_mode
              ),
              'evidence_badges', JSONB_BUILD_ARRAY(
                JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(flags.has_any_evidence, FALSE), 'has_evidence', COALESCE(flags.has_any_evidence, FALSE)),
                JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
              )
            )
          ),
          'evidence', '[]'::jsonb
        )
        || JSONB_BUILD_OBJECT(
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT(
            'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
            'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
            'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
            'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
            'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
            'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
            'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
            'has_any_evidence', flags.has_any_evidence,
            'attached_evidence_count', flags.attached_evidence_count
          ),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT()
        )
        || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(
            'requested_timesheet_id', flags.resolved_timesheet_id,
            'current_timesheet_id', flags.resolved_timesheet_id,
            'expected_timesheet_id', flags.resolved_timesheet_id,
            'current_version', flags.timesheet_version,
            'was_stale', COALESCE(flags.tsfin_is_stale, FALSE),
            'booking_id', flags.booking_id,
            'timesheet', JSONB_BUILD_OBJECT(
              'timesheet_id', flags.resolved_timesheet_id,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'worked_start_iso', flags.timesheet_worked_start_iso,
              'worked_end_iso', flags.timesheet_worked_end_iso,
              'break_start_iso', flags.timesheet_break_start_iso,
              'break_end_iso', flags.timesheet_break_end_iso,
              'break_minutes', flags.timesheet_break_minutes,
              'worked_minutes', flags.timesheet_worked_minutes,
              'week_ending_date', flags.week_ending_date,
              'auth_name', flags.timesheet_auth_name,
              'auth_job_title', flags.timesheet_auth_job_title,
              'authorised_at_server', flags.authorised_at_server,
              'reference_number', flags.timesheet_reference_number,
              'reference_set_at', flags.timesheet_reference_set_at,
              'status', flags.timesheet_status,
              'created_at', flags.timesheet_created_at,
              'updated_at', flags.timesheet_updated_at,
              'version', flags.timesheet_version,
              'is_current', flags.resolved_timesheet_id IS NOT NULL,
              'contract_id', flags.resolved_contract_id,
              'submission_mode', flags.submission_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'sheet_scope', flags.sheet_scope,
              'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb),
              'qr_status', flags.qr_status,
              'qr_r2_key', flags.qr_r2_key,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'candidate_hint_text', flags.candidate_hint_text,
              'is_adjustment', flags.timesheet_is_adjustment,
              'parent_timesheet_id', flags.timesheet_parent_timesheet_id,
              'adjustment_origin', flags.timesheet_adjustment_origin
            ),
            'tsfin', JSONB_BUILD_OBJECT(
              'id', flags.tsfin_id,
              'timesheet_id', flags.resolved_timesheet_id,
              'timesheet_version', flags.tsfin_timesheet_version,
              'basis', flags.tsfin_basis,
              'is_current', flags.tsfin_is_current,
              'is_stale', flags.tsfin_is_stale,
              'stale_reason', flags.tsfin_stale_reason,
              'processing_status', flags.tsfin_processing_status,
              'total_hours', flags.total_hours,
              'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
              'locked_at_utc', flags.tsfin_locked_at_utc,
              'paid_at_utc', flags.tsfin_paid_at_utc,
              'processed_at_utc', flags.tsfin_processed_at_utc,
              'authorised_at_utc', flags.tsfin_authorised_at_utc,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json
            ),
            'validations', COALESCE(flags.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', flags.validation_status,
              'pre_validated', COALESCE((flags.latest_validation_json->>'pre_validated')::boolean, FALSE),
              'latest', flags.latest_validation_json
            ),
            'contract_week_id', flags.resolved_contract_week_id,
            'contract_week', JSONB_BUILD_OBJECT(
              'id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'week_ending_date', flags.week_ending_date,
              'additional_seq', flags.contract_week_additional_seq,
              'status', flags.contract_week_status,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'timesheet_id', flags.resolved_timesheet_id,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
              'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'created_at', flags.contract_week_created_at,
              'updated_at', flags.contract_week_updated_at,
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'is_adjustment', flags.contract_week_is_adjustment,
              'enforce_day_partition', flags.contract_week_enforce_day_partition,
              'allowed_days_mask', flags.contract_week_allowed_days_mask,
              'split_boundary_date', flags.contract_week_split_boundary_date,
              'worker_note', flags.contract_week_worker_note,
              'split_group_key', flags.contract_week_split_group_key
            ),
            'related', JSONB_BUILD_OBJECT(
              'contract', JSONB_BUILD_OBJECT(
                'id', flags.resolved_contract_id,
                'candidate_id', flags.resolved_candidate_id,
                'client_id', flags.resolved_client_id,
                'role', flags.contract_role,
                'band', flags.contract_band,
                'display_site', flags.contract_display_site,
                'ward_hint', flags.contract_ward_hint,
                'default_submission_mode', flags.contract_default_submission_mode,
                'std_schedule_json', COALESCE(flags.contract_std_schedule_json, '[]'::jsonb),
                'additional_rates_json', COALESCE(flags.contract_additional_rates_json, '{}'::jsonb),
                'weekly_timesheet_source', flags.contract_weekly_timesheet_source,
                'no_timesheet_required', flags.contract_no_timesheet_required,
                'autoprocess_hr', flags.contract_autoprocess_hr,
                'requires_hr', flags.contract_requires_hr,
                'hr_attach_to_invoice', flags.contract_hr_attach_to_invoice,
                'ts_attach_to_invoice', flags.contract_ts_attach_to_invoice,
                'is_nhsp', flags.contract_is_nhsp
              ),
              'candidate', JSONB_BUILD_OBJECT(
                'id', flags.resolved_candidate_id,
                'tms_ref', flags.candidate_tms_ref,
                'first_name', flags.candidate_first_name,
                'last_name', flags.candidate_last_name,
                'display_name', flags.candidate_display_name_raw,
                'email', flags.candidate_email,
                'phone', flags.candidate_phone,
                'key_norm', flags.candidate_key_norm,
                'band', flags.candidate_band
              ),
              'client', JSONB_BUILD_OBJECT(
                'id', flags.resolved_client_id,
                'cli_ref', flags.client_cli_ref,
                'name', flags.client_name_raw,
                'vat_chargeable', flags.client_vat_chargeable,
                'ts_queries_email', flags.client_ts_queries_email
              )
            ),
            'evidence', '[]'::jsonb,
            'policy', JSONB_BUILD_OBJECT(
              'weekly_mode', flags.contract_weekly_timesheet_source,
              'requires_hr', flags.contract_requires_hr,
              'autoprocess_hr', flags.contract_autoprocess_hr,
              'no_timesheet_required', flags.contract_no_timesheet_required,
              'is_nhsp', flags.contract_is_nhsp
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              'summary_stage', flags.summary_stage,
              'client_requires_hr', flags.contract_requires_hr,
              'client_autoprocess_hr', flags.contract_autoprocess_hr,
              'client_no_timesheet_required', flags.contract_no_timesheet_required,
              'client_is_nhsp', flags.contract_is_nhsp,
              'contract_id', flags.resolved_contract_id,
              'issue_codes', flags.issue_codes_json
            )
          ),
          'timesheet', JSONB_BUILD_OBJECT(
            'timesheet_id', flags.resolved_timesheet_id,
            'booking_id', flags.booking_id,
            'worked_start_iso', flags.timesheet_worked_start_iso,
            'worked_end_iso', flags.timesheet_worked_end_iso,
            'break_start_iso', flags.timesheet_break_start_iso,
            'break_end_iso', flags.timesheet_break_end_iso,
            'break_minutes', flags.timesheet_break_minutes,
            'worked_minutes', flags.timesheet_worked_minutes,
            'week_ending_date', flags.week_ending_date,
            'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
            'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
          ),
          'tsfin', JSONB_BUILD_OBJECT(
            'id', flags.tsfin_id,
            'timesheet_id', flags.resolved_timesheet_id,
            'basis', flags.tsfin_basis,
            'processing_status', flags.tsfin_processing_status,
            'total_hours', flags.total_hours,
            'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
            'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
            'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
            'paid_at_utc', flags.tsfin_paid_at_utc,
            'processed_at_utc', flags.tsfin_processed_at_utc,
            'authorised_at_utc', flags.tsfin_authorised_at_utc
          ),
          'contract_week', JSONB_BUILD_OBJECT(
            'id', flags.resolved_contract_week_id,
            'contract_id', flags.resolved_contract_id,
            'week_ending_date', flags.week_ending_date,
            'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
            'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
            'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
            'additional_seq', flags.contract_week_additional_seq,
            'is_adjustment', flags.contract_week_is_adjustment
          ),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) ELSE JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) END AS payload_json
      FROM flags
      WHERE flags.row_found = TRUE
    )
    SELECT row_payload.payload_json || JSONB_BUILD_OBJECT(
        'data_row', row_payload.payload_json->'row',
        'row_patch', COALESCE(row_payload.payload_json->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_key', row_payload.payload_json->>'row_key')
      )
      INTO v_out
    FROM row_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk authorise row context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;

    RETURN v_out;
  END IF;

  IF v_profile = 'evidence' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.candidate_display_name AS candidate_display_name,
        summary_row.client_name AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        COALESCE(summary_row.has_any_evidence, FALSE) AS summary_has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS summary_attached_evidence_count,
        summary_row.primary_artifact_storage_key AS summary_primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS summary_primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS summary_primary_artifact_preview_mode,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
    ),
    evidence_items AS (
      SELECT
        0::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:manual_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Timesheet PDF',
          'filename', 'Timesheet PDF',
          'storage_key', resolved.manual_pdf_r2_key,
          'r2_key', resolved.manual_pdf_r2_key,
          'file_key', resolved.manual_pdf_r2_key,
          'download_storage_key', resolved.manual_pdf_r2_key,
          'original_filename', 'Timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'last_rotation_deg', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.manual_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_manual_pdf_duplicate
          WHERE te_manual_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_manual_pdf_duplicate.storage_key = resolved.manual_pdf_r2_key
        )
      UNION ALL
      SELECT
        1::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:qr_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Signed QR Timesheet',
          'filename', 'Signed QR Timesheet',
          'storage_key', resolved.qr_r2_key,
          'r2_key', resolved.qr_r2_key,
          'file_key', resolved.qr_r2_key,
          'download_storage_key', resolved.qr_r2_key,
          'original_filename', 'Signed QR Timesheet',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.qr_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_qr_pdf_duplicate
          WHERE te_qr_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_qr_pdf_duplicate.storage_key = resolved.qr_r2_key
        )
      UNION ALL
      SELECT
        2::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:contract_week_pdf:' || resolved.resolved_contract_week_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Uploaded timesheet PDF',
          'filename', 'Uploaded timesheet PDF',
          'storage_key', resolved.uploaded_pdf_r2_key,
          'r2_key', resolved.uploaded_pdf_r2_key,
          'file_key', resolved.uploaded_pdf_r2_key,
          'download_storage_key', resolved.uploaded_pdf_r2_key,
          'original_filename', 'Uploaded timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.contract_week_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_contract_week_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.uploaded_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_contract_week_pdf_duplicate
          WHERE te_contract_week_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_contract_week_pdf_duplicate.storage_key = resolved.uploaded_pdf_r2_key
        )
      UNION ALL
      SELECT
        10::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', te0.id,
          'evidence_id', te0.id,
          'queue_id', NULL::uuid,
          'timesheet_id', te0.timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'storage_key', te0.storage_key,
          'r2_key', te0.storage_key,
          'file_key', te0.storage_key,
          'download_storage_key', te0.storage_key,
          'original_filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'mime_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'content_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'uploaded_at_utc', te0.created_at,
          'created_at', te0.created_at,
          'created_by', te0.created_by,
          'rotation', 0,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
            ELSE 'FILE'
          END,
          'source_label', 'Attached',
          'source_badge', 'Attached'
        ) AS item_json
      FROM public.timesheet_evidence AS te0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND te0.timesheet_id = resolved.resolved_timesheet_id
      UNION ALL
      SELECT
        20::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', mq0.id,
          'evidence_id', NULL::uuid,
          'queue_id', mq0.id,
          'timesheet_id', NULL::uuid,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'original_filename', mq0.original_filename,
          'storage_key', mq0.r2_key,
          'r2_key', mq0.r2_key,
          'file_key', mq0.r2_key,
          'download_storage_key', mq0.r2_key,
          'mime_type', mq0.mime_type,
          'content_type', mq0.mime_type,
          'content_hash', mq0.content_hash,
          'uploaded_at_utc', mq0.uploaded_at_utc,
          'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
          'staged_by_user_id', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'staged_by_user_id'), ''), mq0.uploaded_by_user_id::text),
          'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
          'last_rotation_deg', COALESCE(mq0.last_rotation_deg::integer, 0),
          'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
          'pages', '[]'::jsonb,
          'status', mq0.status,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'is_staged_context', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(mq0.mime_type, '')) LIKE 'image/%' THEN 'IMAGE'
            WHEN LOWER(COALESCE(mq0.mime_type, '')) = 'application/pdf' THEN 'PDF'
            ELSE 'FILE'
          END,
          'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
          'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
        ) AS item_json
      FROM public.manual_timesheet_queue AS mq0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NULL
        AND resolved.resolved_contract_week_id IS NOT NULL
        AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = resolved.resolved_contract_week_id::text
    ),
    evidence_ranked AS (
      SELECT evidence_items.sort_order, evidence_items.item_json
      FROM evidence_items
      WHERE NULLIF(BTRIM(COALESCE(evidence_items.item_json->>'storage_key', evidence_items.item_json->>'r2_key', '')), '') IS NOT NULL
    ),
    evidence_payload AS (
      SELECT
        COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
        COUNT(*)::integer AS evidence_count,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TIMESHEET') AS has_timesheet,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'MILEAGE') AS has_mileage,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TRAVEL') AS has_travel,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'ACCOMMODATION') AS has_accommodation,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'OTHER') AS has_other,
        (SELECT er1.item_json FROM evidence_ranked AS er1 ORDER BY er1.sort_order ASC, er1.item_json->>'id' ASC LIMIT 1) AS primary_evidence_json
      FROM evidence_ranked
    ),
    final_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_authorise_row_context',
          'context_profile', 'evidence',
          'profile', 'evidence',
          'context_type', CASE WHEN 'bulk_authorise_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', FALSE,
          'header_only', FALSE,
          'editor_loaded', FALSE,
          'evidence_loaded', TRUE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', TRUE,
          'schedule_authoritative', FALSE,
          'loaded_layers', JSONB_BUILD_ARRAY('evidence'),
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), 'evidence')),
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attached_evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attachedRows', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
          'preview_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
          'primary_artifact_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
          'primary_artifact_preview_mode', COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', resolved.summary_primary_artifact_preview_mode),
          'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
          'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_badges', JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
          ),
          'evidence_meta', JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row', JSONB_BUILD_OBJECT(
            'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
            'timesheet_id', resolved.resolved_timesheet_id,
            'current_timesheet_id', resolved.resolved_timesheet_id,
            'requested_timesheet_id', resolved.resolved_timesheet_id,
            'expected_timesheet_id', resolved.resolved_timesheet_id,
            'contract_week_id', resolved.resolved_contract_week_id,
            'contract_id', resolved.resolved_contract_id,
            'candidate_id', resolved.candidate_id,
            'candidate_name', resolved.candidate_name,
            'candidate_display_name', resolved.candidate_display_name,
            'client_id', resolved.client_id,
            'client_name', resolved.client_name,
            'week_ending_date', resolved.week_ending_date,
            'route_family', resolved.route_family,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
            'primary_artifact_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
            'primary_artifact_preview_mode', COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', resolved.summary_primary_artifact_preview_mode),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT('has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0)),
          'details', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'left_pane', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT(),
          'filters', v_filters
        ) AS payload_json
      FROM resolved
      LEFT JOIN evidence_payload ON TRUE
      WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL
    )
    SELECT final_payload.payload_json || JSONB_BUILD_OBJECT('data_row', final_payload.payload_json->'row')
      INTO v_out
    FROM final_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'evidence',
        'profile', 'evidence',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk authorise evidence context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;

    RETURN v_out;
  END IF;

  IF v_profile = 'compare_import' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM public.timesheet_summary_lightweight_rows_v1(v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)) AS summary_source
      CROSS JOIN input_ids AS summary_ids
      WHERE (summary_ids.input_timesheet_id IS NULL OR summary_source.timesheet_id = summary_ids.input_timesheet_id)
        AND (summary_ids.input_contract_week_id IS NULL OR summary_source.contract_week_id = summary_ids.input_contract_week_id)
      ORDER BY summary_source.timesheet_id NULLS LAST, summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.client_name AS client_name,
        summary_row.week_ending_date AS week_ending_date,
        summary_row.route_family AS route_family,
        tsfin_row.external_source_rows_json AS external_source_rows_json,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
    )
    SELECT JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'context_type', CASE WHEN 'bulk_authorise_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
        'slim_context', TRUE,
        'header_loaded', TRUE,
        'header_only', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', TRUE,
        'full_loaded', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', JSONB_BUILD_ARRAY('header', 'compare_import'),
        'soft_failure', FALSE,
        'context_degraded', FALSE,
        'degraded_reason', NULL::text,
        'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
        'timesheet_id', resolved.resolved_timesheet_id,
        'current_timesheet_id', resolved.resolved_timesheet_id,
        'requested_timesheet_id', resolved.resolved_timesheet_id,
        'expected_timesheet_id', resolved.resolved_timesheet_id,
        'contract_week_id', resolved.resolved_contract_week_id,
        'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), COALESCE(resolved.tsfin_updated_at::text, ''), 'compare_import')),
        'row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'data_row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'row_patch', JSONB_BUILD_OBJECT(),
        'action_flags', JSONB_BUILD_OBJECT(),
        'compare', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'include_import_source_rows', v_include_import_source_rows
        ),
        'details', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'evidence', '[]'::jsonb
        ),
        'left_pane', JSONB_BUILD_OBJECT('source_items', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END, 'evidence', '[]'::jsonb),
        'evidence', '[]'::jsonb,
        'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE),
        'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
        'count_deltas', JSONB_BUILD_OBJECT(),
        'filters', v_filters
      )
      INTO v_out
    FROM resolved
    WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk authorise compare/import context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;

    RETURN v_out;
  END IF;

  WITH decision_row AS (
    SELECT patch_result.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'dataset_mode', 'authorise',
           'projection', 'active_row_header',
           'profile', v_profile
         )
    ) AS patch_result(row_json)
    ORDER BY patch_result.row_json->>'row_key'
    LIMIT 1
  ),
  row_ids AS (
    SELECT
      decision_row.row_json,
      CASE
        WHEN COALESCE(decision_row.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_week_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_week_id')::uuid
        ELSE NULL::uuid
      END AS contract_week_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS contract_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'candidate_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'candidate_id')::uuid
        ELSE NULL::uuid
      END AS candidate_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'client_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS client_id
    FROM decision_row
  ),
  timesheet_row AS (
    SELECT
      ts0.timesheet_id,
      ts0.booking_id,
      ts0.occupant_key_norm,
      ts0.hospital_norm,
      ts0.ward_norm,
      ts0.job_title_norm,
      ts0.shift_label_norm,
      ts0.scheduled_start_iso,
      ts0.scheduled_end_iso,
      ts0.worked_start_iso,
      ts0.worked_end_iso,
      ts0.break_start_iso,
      ts0.break_end_iso,
      ts0.break_minutes,
      ts0.worked_minutes,
      ts0.week_ending_date,
      ts0.auth_name,
      ts0.auth_job_title,
      ts0.authorised_at_server,
      ts0.r2_nurse_key,
      ts0.r2_auth_key,
      ts0.reference_number,
      ts0.reference_set_at,
      ts0.status,
      ts0.created_at,
      ts0.updated_at,
      ts0.version,
      ts0.is_current,
      ts0.contract_id,
      ts0.submission_mode,
      ts0.manual_pdf_r2_key,
      ts0.line_type,
      ts0.sheet_scope,
      ts0.actual_schedule_json,
      ts0.additional_units_week,
      ts0.additional_units_per_day,
      ts0.qr_status,
      ts0.qr_payload_json,
      ts0.qr_generated_at,
      ts0.qr_scanned_at,
      ts0.qr_scan_info_json,
      ts0.qr_r2_key,
      ts0.day_references_json,
      ts0.manual_pdf_rotation_degrees,
      ts0.qr_last_sent_hash,
      ts0.qr_last_sent_at_utc,
      ts0.qr_signed_hash,
      ts0.qr_signed_at_utc,
      ts0.candidate_hint_text,
      ts0.band,
      ts0.generated_pdf_at_utc,
      ts0.generated_pdf_refs_sig,
      ts0.generated_pdf_refs_snapshot_json,
      ts0.generated_pdf_refs_captured_at_utc,
      ts0.qr_sent_refs_sig,
      ts0.qr_sent_refs_snapshot_json,
      ts0.qr_sent_refs_captured_at_utc,
      ts0.is_adjustment,
      ts0.parent_timesheet_id,
      ts0.correction_id,
      ts0.correction_kind,
      ts0.adjustment_origin
    FROM public.timesheets AS ts0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND ts0.timesheet_id = ids.timesheet_id
      AND ts0.is_current = TRUE
    ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
    LIMIT 1
  ),
  tsfin_row AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.rate_source_refs_json,
      tf0.hours_day,
      tf0.hours_night,
      tf0.hours_sat,
      tf0.hours_sun,
      tf0.hours_bh,
      tf0.pay_day,
      tf0.pay_night,
      tf0.pay_sat,
      tf0.pay_sun,
      tf0.pay_bh,
      tf0.charge_day,
      tf0.charge_night,
      tf0.charge_sat,
      tf0.charge_sun,
      tf0.charge_bh,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.locked_at_utc,
      tf0.unlocked_by_credit_note_id,
      tf0.created_at,
      tf0.updated_at,
      tf0.occupant_key_norm,
      tf0.candidate_assignment,
      tf0.processing_status,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.po_number,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.pay_on_hold_since_utc,
      tf0.paid_at_utc,
      tf0.paid_by_user_id,
      tf0.payment_reference,
      tf0.remittance_last_sent_at_utc,
      tf0.remittance_send_count,
      tf0.pay_wtr_rate_pct_snapshot,
      tf0.pay_vat_rate_pct_snapshot,
      tf0.pay_vat_amount_snapshot,
      tf0.pay_total_inc_vat_snapshot,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.expenses_evidence_manifest,
      tf0.mileage_evidence_manifest,
      tf0.actual_schedule_json,
      tf0.actual_minutes_by_day_json,
      tf0.additional_units_json,
      tf0.additional_pay_ex_vat,
      tf0.additional_charge_ex_vat,
      tf0.additional_margin_ex_vat,
      tf0.invoice_breakdown_json,
      tf0.nhsp_import_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.mileage_units,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat
    FROM public.timesheets_financials AS tf0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tf0.timesheet_id = ids.timesheet_id
      AND tf0.is_current = TRUE
    ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
    LIMIT 1
  ),
  contract_week_row AS (
    SELECT
      cw0.id,
      cw0.contract_id,
      cw0.week_ending_date,
      cw0.additional_seq,
      cw0.status,
      cw0.submission_mode_snapshot,
      cw0.timesheet_id,
      cw0.uploaded_pdf_r2_key,
      cw0.day_entries_json,
      cw0.totals_json,
      cw0.created_at,
      cw0.updated_at,
      cw0.planned_schedule_json,
      cw0.is_adjustment,
      cw0.enforce_day_partition,
      cw0.allowed_days_mask,
      cw0.split_boundary_date,
      cw0.worker_note,
      cw0.split_group_key
    FROM public.contract_weeks AS cw0
    CROSS JOIN row_ids AS ids
    WHERE (ids.contract_week_id IS NOT NULL AND cw0.id = ids.contract_week_id)
       OR (ids.contract_week_id IS NULL AND ids.timesheet_id IS NOT NULL AND cw0.timesheet_id = ids.timesheet_id)
    ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
    LIMIT 1
  ),
  contract_row AS (
    SELECT
      ct0.id,
      ct0.candidate_id,
      ct0.client_id,
      ct0.role,
      ct0.band,
      ct0.display_site,
      ct0.ward_hint,
      ct0.start_date,
      ct0.end_date,
      ct0.pay_method_snapshot,
      ct0.rates_json,
      ct0.std_hours_json,
      ct0.default_submission_mode,
      ct0.week_ending_weekday_snapshot,
      ct0.auto_invoice,
      ct0.require_reference_to_pay,
      ct0.require_reference_to_invoice,
      ct0.created_at,
      ct0.updated_at,
      ct0.bucket_labels_json,
      ct0.std_schedule_json,
      ct0.mileage_pay_rate,
      ct0.mileage_charge_rate,
      ct0.additional_rates_json,
      ct0.self_bill,
      ct0.weekly_timesheet_source,
      ct0.no_timesheet_required,
      ct0.daily_calc_of_invoices,
      ct0.group_nightsat_sunbh,
      ct0.is_nhsp,
      ct0.autoprocess_hr,
      ct0.requires_hr,
      ct0.hr_attach_to_invoice,
      ct0.ts_attach_to_invoice,
      ct0.overrideclientsettings,
      ct0.reference_number_required_to_issue_invoice,
      ct0.send_manual_invoices_to_different_email,
      ct0.manual_invoices_alt_email_address,
      ct0.is_ad_hoc
    FROM public.contracts AS ct0
    CROSS JOIN row_ids AS ids
    LEFT JOIN timesheet_row AS ts1 ON TRUE
    LEFT JOIN contract_week_row AS cw1 ON TRUE
    WHERE ct0.id = COALESCE(ids.contract_id, ts1.contract_id, cw1.contract_id)
    ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
    LIMIT 1
  ),
  candidate_row AS (
    SELECT
      cand0.id,
      cand0.tms_ref,
      cand0.first_name,
      cand0.last_name,
      cand0.display_name,
      cand0.email,
      cand0.phone,
      cand0.pay_method,
      cand0.umbrella_id,
      cand0.active,
      cand0.key_norm,
      cand0.mileage_pay_rate,
      cand0.job_title_id,
      cand0.prof_reg_number,
      cand0.prof_reg_type,
      cand0.ni_number,
      cand0.date_of_birth,
      cand0.gender,
      cand0.title,
      cand0.opt_in_email,
      cand0.opt_in_sms,
      cand0.opt_in_whatsapp,
      cand0.band,
      cand0.tms_ref_num
    FROM public.candidates AS cand0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cand0.id = COALESCE(ids.candidate_id, ct1.candidate_id)
    LIMIT 1
  ),
  client_row AS (
    SELECT
      cli0.id,
      cli0.cli_ref,
      cli0.name,
      cli0.invoice_address,
      cli0.primary_invoice_email,
      cli0.ap_phone,
      cli0.vat_chargeable,
      cli0.payment_terms_days,
      cli0.mileage_charge_rate,
      cli0.ts_queries_email,
      cli0.client_address,
      cli0.contact_title,
      cli0.contact_known_as,
      cli0.contact_forename,
      cli0.contact_surname,
      cli0.contact_job_title,
      cli0.contact_tel,
      cli0.contact_mobile,
      cli0.contact_email,
      cli0.website
    FROM public.clients AS cli0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cli0.id = COALESCE(ids.client_id, ct1.client_id)
    LIMIT 1
  ),
  validation_rows AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.booking_id,
      tv0.status,
      tv0.reason_code,
      tv0.hr_request_id,
      tv0.validated_at_utc,
      tv0.override_requested_at_utc,
      tv0.override_requested_by,
      tv0.override_confirmed_at_utc,
      tv0.override_confirmed_by,
      tv0.override_reason,
      tv0.override_cleared_at_utc,
      tv0.last_source,
      tv0.created_at,
      tv0.updated_at,
      tv0.hr_request_source,
      tv0.hr_request_set_by,
      tv0.hr_request_set_at_utc,
      tv0.pre_validated
    FROM public.timesheet_validations AS tv0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tv0.timesheet_id = ids.timesheet_id
    ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
  ),
  validation_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', validation_rows.id,
          'timesheet_id', validation_rows.timesheet_id,
          'booking_id', validation_rows.booking_id,
          'status', validation_rows.status,
          'reason_code', validation_rows.reason_code,
          'hr_request_id', validation_rows.hr_request_id,
          'validated_at_utc', validation_rows.validated_at_utc,
          'override_requested_at_utc', validation_rows.override_requested_at_utc,
          'override_requested_by', validation_rows.override_requested_by,
          'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
          'override_confirmed_by', validation_rows.override_confirmed_by,
          'override_reason', validation_rows.override_reason,
          'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
          'last_source', validation_rows.last_source,
          'created_at', validation_rows.created_at,
          'updated_at', validation_rows.updated_at,
          'hr_request_source', validation_rows.hr_request_source,
          'hr_request_set_by', validation_rows.hr_request_set_by,
          'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
          'pre_validated', validation_rows.pre_validated
        ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
      ), '[]'::jsonb) AS validations_json,
      (
        SELECT JSONB_BUILD_OBJECT(
          'id', vr1.id,
          'status', vr1.status,
          'reason_code', vr1.reason_code,
          'hr_request_id', vr1.hr_request_id,
          'validated_at_utc', vr1.validated_at_utc,
          'pre_validated', COALESCE(vr1.pre_validated, FALSE),
          'updated_at', vr1.updated_at
        )
        FROM validation_rows AS vr1
        ORDER BY vr1.validated_at_utc DESC NULLS LAST, vr1.created_at DESC NULLS LAST, vr1.id DESC
        LIMIT 1
      ) AS latest_validation_json
    FROM validation_rows
  ),
  evidence_items AS (
    SELECT
      0::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_id'), ''), 'sys:timesheet:' || COALESCE(row_ids.timesheet_id::text, row_ids.contract_week_id::text)),
        'timesheet_id', row_ids.timesheet_id,
        'contract_week_id', row_ids.contract_week_id,
        'kind', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET'),
        'display_name', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'r2_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'uploaded_at_utc', row_ids.row_json->>'updated_at',
        'rotation_degrees', COALESCE(NULLIF(row_ids.row_json->>'manual_pdf_rotation_degrees', '')::integer, 0),
        'system', TRUE,
        'is_view_only', TRUE,
        'can_delete', FALSE,
        'can_reclassify', FALSE,
        'can_edit_kind', FALSE,
        'can_edit_type', FALSE,
        'can_return_to_queue', FALSE,
        'preview_mode', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_preview_mode'), ''), 'PDF'),
        'source_label', 'System',
        'source_badge', 'System'
      ) AS item_json
    FROM row_ids
    WHERE v_include_evidence = TRUE
      AND NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '') IS NOT NULL
      AND NOT (
        NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') LIKE 'evidence:%'
        OR (
          row_ids.timesheet_id IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te_primary_artifact
            WHERE te_primary_artifact.timesheet_id = row_ids.timesheet_id
              AND te_primary_artifact.storage_key = NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '')
          )
        )
      )
    UNION ALL
    SELECT
      10::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', te0.id,
        'evidence_id', te0.id,
        'timesheet_id', te0.timesheet_id,
        'contract_week_id', NULL::uuid,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'storage_key', te0.storage_key,
        'r2_key', te0.storage_key,
        'mime_type', NULL::text,
        'uploaded_at_utc', te0.created_at,
        'created_at', te0.created_at,
        'created_by', te0.created_by,
        'rotation', 0,
        'rotation_degrees', 0,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'preview_mode', 'FILE',
        'source_label', 'Attached',
        'source_badge', 'Attached'
      ) AS item_json
    FROM public.timesheet_evidence AS te0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NOT NULL
      AND te0.timesheet_id = ids.timesheet_id
    UNION ALL
    SELECT
      20::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', mq0.id,
        'queue_id', mq0.id,
        'timesheet_id', NULL::uuid,
        'contract_week_id', ids.contract_week_id,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'original_filename', mq0.original_filename,
        'storage_key', mq0.r2_key,
        'r2_key', mq0.r2_key,
        'mime_type', mq0.mime_type,
        'content_hash', mq0.content_hash,
        'uploaded_at_utc', mq0.uploaded_at_utc,
        'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
        'staged_by_user_id', mq0.uploaded_by_user_id,
        'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
        'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
        'status', mq0.status,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'is_staged_context', TRUE,
        'preview_mode', 'PDF',
        'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
        'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
      ) AS item_json
    FROM public.manual_timesheet_queue AS mq0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NULL
      AND ids.contract_week_id IS NOT NULL
      AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = ids.contract_week_id::text
  ),
  evidence_ranked AS (
    SELECT
      evidence_items.sort_order,
      evidence_items.item_json,
      ROW_NUMBER() OVER (ORDER BY evidence_items.sort_order ASC, evidence_items.item_json->>'id' ASC) AS rn
    FROM evidence_items
  ),
  evidence_payload AS (
    SELECT
      COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
      (
        SELECT er1.item_json
        FROM evidence_ranked AS er1
        WHERE er1.rn = 1
      ) AS primary_evidence_json
    FROM evidence_ranked
  ),
  timesheet_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'timesheet_id', timesheet_row.timesheet_id,
        'booking_id', timesheet_row.booking_id,
        'occupant_key_norm', timesheet_row.occupant_key_norm,
        'hospital_norm', timesheet_row.hospital_norm,
        'ward_norm', timesheet_row.ward_norm,
        'job_title_norm', timesheet_row.job_title_norm,
        'shift_label_norm', timesheet_row.shift_label_norm,
        'scheduled_start_iso', timesheet_row.scheduled_start_iso,
        'scheduled_end_iso', timesheet_row.scheduled_end_iso,
        'worked_start_iso', timesheet_row.worked_start_iso,
        'worked_end_iso', timesheet_row.worked_end_iso,
        'break_start_iso', timesheet_row.break_start_iso,
        'break_end_iso', timesheet_row.break_end_iso,
        'break_minutes', timesheet_row.break_minutes,
        'worked_minutes', timesheet_row.worked_minutes,
        'week_ending_date', timesheet_row.week_ending_date,
        'auth_name', timesheet_row.auth_name,
        'auth_job_title', timesheet_row.auth_job_title,
        'authorised_at_server', timesheet_row.authorised_at_server,
        'r2_nurse_key', timesheet_row.r2_nurse_key,
        'r2_auth_key', timesheet_row.r2_auth_key
      )
      || JSONB_BUILD_OBJECT(
        'reference_number', timesheet_row.reference_number,
        'reference_set_at', timesheet_row.reference_set_at,
        'status', timesheet_row.status,
        'created_at', timesheet_row.created_at,
        'updated_at', timesheet_row.updated_at,
        'version', timesheet_row.version,
        'is_current', timesheet_row.is_current,
        'contract_id', timesheet_row.contract_id,
        'submission_mode', timesheet_row.submission_mode,
        'manual_pdf_r2_key', timesheet_row.manual_pdf_r2_key,
        'line_type', timesheet_row.line_type,
        'sheet_scope', timesheet_row.sheet_scope,
        'actual_schedule_json', timesheet_row.actual_schedule_json,
        'additional_units_week', timesheet_row.additional_units_week,
        'additional_units_per_day', timesheet_row.additional_units_per_day,
        'qr_status', timesheet_row.qr_status,
        'qr_payload_json', timesheet_row.qr_payload_json,
        'qr_generated_at', timesheet_row.qr_generated_at,
        'qr_scanned_at', timesheet_row.qr_scanned_at,
        'qr_scan_info_json', timesheet_row.qr_scan_info_json,
        'qr_r2_key', timesheet_row.qr_r2_key,
        'day_references_json', timesheet_row.day_references_json,
        'manual_pdf_rotation_degrees', timesheet_row.manual_pdf_rotation_degrees
      )
      || JSONB_BUILD_OBJECT(
        'qr_last_sent_hash', timesheet_row.qr_last_sent_hash,
        'qr_last_sent_at_utc', timesheet_row.qr_last_sent_at_utc,
        'qr_signed_hash', timesheet_row.qr_signed_hash,
        'qr_signed_at_utc', timesheet_row.qr_signed_at_utc,
        'candidate_hint_text', timesheet_row.candidate_hint_text,
        'band', timesheet_row.band,
        'generated_pdf_at_utc', timesheet_row.generated_pdf_at_utc,
        'generated_pdf_refs_sig', timesheet_row.generated_pdf_refs_sig,
        'generated_pdf_refs_snapshot_json', timesheet_row.generated_pdf_refs_snapshot_json,
        'generated_pdf_refs_captured_at_utc', timesheet_row.generated_pdf_refs_captured_at_utc,
        'qr_sent_refs_sig', timesheet_row.qr_sent_refs_sig,
        'qr_sent_refs_snapshot_json', timesheet_row.qr_sent_refs_snapshot_json,
        'qr_sent_refs_captured_at_utc', timesheet_row.qr_sent_refs_captured_at_utc,
        'is_adjustment', timesheet_row.is_adjustment,
        'parent_timesheet_id', timesheet_row.parent_timesheet_id,
        'correction_id', timesheet_row.correction_id,
        'correction_kind', timesheet_row.correction_kind,
        'adjustment_origin', timesheet_row.adjustment_origin
      ) AS timesheet_json
    FROM timesheet_row
  ),
  tsfin_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', tsfin_row.id,
        'timesheet_id', tsfin_row.timesheet_id,
        'timesheet_version', tsfin_row.timesheet_version,
        'basis', tsfin_row.basis,
        'is_current', tsfin_row.is_current,
        'is_stale', tsfin_row.is_stale,
        'stale_reason', tsfin_row.stale_reason,
        'worked_start_iso', tsfin_row.worked_start_iso,
        'worked_end_iso', tsfin_row.worked_end_iso,
        'break_start_iso', tsfin_row.break_start_iso,
        'break_end_iso', tsfin_row.break_end_iso,
        'break_minutes', tsfin_row.break_minutes,
        'candidate_id', tsfin_row.candidate_id,
        'client_id', tsfin_row.client_id,
        'role', tsfin_row.role,
        'band', tsfin_row.band,
        'pay_method', tsfin_row.pay_method,
        'policy_snapshot_json', tsfin_row.policy_snapshot_json,
        'rate_source_refs_json', tsfin_row.rate_source_refs_json,
        'hours_day', tsfin_row.hours_day,
        'hours_night', tsfin_row.hours_night,
        'hours_sat', tsfin_row.hours_sat,
        'hours_sun', tsfin_row.hours_sun,
        'hours_bh', tsfin_row.hours_bh
      )
      || JSONB_BUILD_OBJECT(
        'pay_day', tsfin_row.pay_day,
        'pay_night', tsfin_row.pay_night,
        'pay_sat', tsfin_row.pay_sat,
        'pay_sun', tsfin_row.pay_sun,
        'pay_bh', tsfin_row.pay_bh,
        'charge_day', tsfin_row.charge_day,
        'charge_night', tsfin_row.charge_night,
        'charge_sat', tsfin_row.charge_sat,
        'charge_sun', tsfin_row.charge_sun,
        'charge_bh', tsfin_row.charge_bh,
        'total_hours', tsfin_row.total_hours,
        'total_pay_ex_vat', tsfin_row.total_pay_ex_vat,
        'total_charge_ex_vat', tsfin_row.total_charge_ex_vat,
        'margin_ex_vat', tsfin_row.margin_ex_vat,
        'computed_at_utc', tsfin_row.computed_at_utc,
        'locked_by_invoice_id', tsfin_row.locked_by_invoice_id,
        'locked_at_utc', tsfin_row.locked_at_utc,
        'unlocked_by_credit_note_id', tsfin_row.unlocked_by_credit_note_id,
        'created_at', tsfin_row.created_at,
        'updated_at', tsfin_row.updated_at,
        'occupant_key_norm', tsfin_row.occupant_key_norm,
        'candidate_assignment', tsfin_row.candidate_assignment,
        'processing_status', tsfin_row.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'expenses_pay_ex_vat', tsfin_row.expenses_pay_ex_vat,
        'expenses_charge_ex_vat', tsfin_row.expenses_charge_ex_vat,
        'expenses_description', tsfin_row.expenses_description,
        'expenses_evidence_r2_key', tsfin_row.expenses_evidence_r2_key,
        'mileage_pay_ex_vat', tsfin_row.mileage_pay_ex_vat,
        'mileage_charge_ex_vat', tsfin_row.mileage_charge_ex_vat,
        'mileage_evidence_r2_key', tsfin_row.mileage_evidence_r2_key,
        'mileage_pay_rate', tsfin_row.mileage_pay_rate,
        'mileage_charge_rate', tsfin_row.mileage_charge_rate,
        'mileage_units', tsfin_row.mileage_units,
        'travel_pay_ex_vat', tsfin_row.travel_pay_ex_vat,
        'travel_charge_ex_vat', tsfin_row.travel_charge_ex_vat,
        'accommodation_pay_ex_vat', tsfin_row.accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', tsfin_row.accommodation_charge_ex_vat,
        'other_pay_ex_vat', tsfin_row.other_pay_ex_vat,
        'other_charge_ex_vat', tsfin_row.other_charge_ex_vat,
        'po_number', tsfin_row.po_number,
        'pay_on_hold', tsfin_row.pay_on_hold,
        'pay_on_hold_reason', tsfin_row.pay_on_hold_reason,
        'pay_on_hold_since_utc', tsfin_row.pay_on_hold_since_utc,
        'paid_at_utc', tsfin_row.paid_at_utc,
        'paid_by_user_id', tsfin_row.paid_by_user_id,
        'payment_reference', tsfin_row.payment_reference
      )
      || JSONB_BUILD_OBJECT(
        'remittance_last_sent_at_utc', tsfin_row.remittance_last_sent_at_utc,
        'remittance_send_count', tsfin_row.remittance_send_count,
        'pay_wtr_rate_pct_snapshot', tsfin_row.pay_wtr_rate_pct_snapshot,
        'pay_vat_rate_pct_snapshot', tsfin_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot', tsfin_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot', tsfin_row.pay_total_inc_vat_snapshot,
        'processed_by_user_id', tsfin_row.processed_by_user_id,
        'processed_at_utc', tsfin_row.processed_at_utc,
        'authorised_by_user_id', tsfin_row.authorised_by_user_id,
        'authorised_at_utc', tsfin_row.authorised_at_utc,
        'expenses_evidence_manifest', tsfin_row.expenses_evidence_manifest,
        'mileage_evidence_manifest', tsfin_row.mileage_evidence_manifest,
        'actual_schedule_json', tsfin_row.actual_schedule_json,
        'actual_minutes_by_day_json', tsfin_row.actual_minutes_by_day_json,
        'additional_units_json', tsfin_row.additional_units_json,
        'additional_pay_ex_vat', tsfin_row.additional_pay_ex_vat,
        'additional_charge_ex_vat', tsfin_row.additional_charge_ex_vat,
        'additional_margin_ex_vat', tsfin_row.additional_margin_ex_vat,
        'invoice_breakdown_json', tsfin_row.invoice_breakdown_json,
        'nhsp_import_id', tsfin_row.nhsp_import_id,
        'has_rate_issue', tsfin_row.has_rate_issue,
        'has_pay_channel_issue', tsfin_row.has_pay_channel_issue,
        'hr_crosscheck_status', tsfin_row.hr_crosscheck_status,
        'hr_crosscheck_issues', tsfin_row.hr_crosscheck_issues,
        'external_source_rows_json', tsfin_row.external_source_rows_json
      ) AS tsfin_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN NULL::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN JSONB_BUILD_OBJECT('mode', 'SEGMENTS', 'segments', tsfin_row.invoice_breakdown_json)
        ELSE NULL::jsonb
      END AS invoice_breakdown_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND UPPER(COALESCE(tsfin_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS' THEN TRUE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN TRUE
        ELSE FALSE
      END AS is_segments_mode,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json->'segments') = 'array' THEN tsfin_row.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END AS segments_json
    FROM tsfin_row
  ),
  contract_week_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_week_row.id,
        'contract_id', contract_week_row.contract_id,
        'week_ending_date', contract_week_row.week_ending_date,
        'additional_seq', contract_week_row.additional_seq,
        'status', contract_week_row.status,
        'submission_mode_snapshot', contract_week_row.submission_mode_snapshot,
        'timesheet_id', contract_week_row.timesheet_id,
        'uploaded_pdf_r2_key', contract_week_row.uploaded_pdf_r2_key,
        'day_entries_json', contract_week_row.day_entries_json,
        'totals_json', contract_week_row.totals_json,
        'created_at', contract_week_row.created_at,
        'updated_at', contract_week_row.updated_at,
        'planned_schedule_json', contract_week_row.planned_schedule_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'is_adjustment', contract_week_row.is_adjustment,
        'enforce_day_partition', contract_week_row.enforce_day_partition,
        'allowed_days_mask', contract_week_row.allowed_days_mask,
        'split_boundary_date', contract_week_row.split_boundary_date,
        'worker_note', contract_week_row.worker_note,
        'split_group_key', contract_week_row.split_group_key,
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display'
      ) AS contract_week_json
    FROM contract_week_row
    CROSS JOIN row_ids
    LEFT JOIN contract_row ON TRUE
  ),
  contract_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_row.id,
        'candidate_id', contract_row.candidate_id,
        'client_id', contract_row.client_id,
        'role', contract_row.role,
        'band', contract_row.band,
        'display_site', contract_row.display_site,
        'ward_hint', contract_row.ward_hint,
        'start_date', contract_row.start_date,
        'end_date', contract_row.end_date,
        'pay_method_snapshot', contract_row.pay_method_snapshot,
        'rates_json', contract_row.rates_json,
        'std_hours_json', contract_row.std_hours_json,
        'default_submission_mode', contract_row.default_submission_mode,
        'week_ending_weekday_snapshot', contract_row.week_ending_weekday_snapshot,
        'auto_invoice', contract_row.auto_invoice,
        'require_reference_to_pay', contract_row.require_reference_to_pay,
        'require_reference_to_invoice', contract_row.require_reference_to_invoice,
        'bucket_labels_json', contract_row.bucket_labels_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'mileage_pay_rate', contract_row.mileage_pay_rate,
        'mileage_charge_rate', contract_row.mileage_charge_rate,
        'additional_rates_json', contract_row.additional_rates_json,
        'self_bill', contract_row.self_bill,
        'weekly_timesheet_source', contract_row.weekly_timesheet_source,
        'no_timesheet_required', contract_row.no_timesheet_required,
        'daily_calc_of_invoices', contract_row.daily_calc_of_invoices,
        'group_nightsat_sunbh', contract_row.group_nightsat_sunbh,
        'is_nhsp', contract_row.is_nhsp,
        'autoprocess_hr', contract_row.autoprocess_hr,
        'requires_hr', contract_row.requires_hr,
        'hr_attach_to_invoice', contract_row.hr_attach_to_invoice,
        'ts_attach_to_invoice', contract_row.ts_attach_to_invoice,
        'overrideclientsettings', contract_row.overrideclientsettings,
        'reference_number_required_to_issue_invoice', contract_row.reference_number_required_to_issue_invoice,
        'send_manual_invoices_to_different_email', contract_row.send_manual_invoices_to_different_email,
        'manual_invoices_alt_email_address', contract_row.manual_invoices_alt_email_address,
        'is_ad_hoc', contract_row.is_ad_hoc,
        'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
        'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour'
      ) AS contract_json
    FROM contract_row
    CROSS JOIN row_ids
  ),
  related_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'counts', JSONB_BUILD_OBJECT(),
        'candidate', CASE WHEN candidate_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', candidate_row.id,
          'tms_ref', candidate_row.tms_ref,
          'first_name', candidate_row.first_name,
          'last_name', candidate_row.last_name,
          'display_name', candidate_row.display_name,
          'email', candidate_row.email,
          'phone', candidate_row.phone,
          'pay_method', candidate_row.pay_method,
          'umbrella_id', candidate_row.umbrella_id,
          'active', candidate_row.active,
          'key_norm', candidate_row.key_norm,
          'mileage_pay_rate', candidate_row.mileage_pay_rate,
          'job_title_id', candidate_row.job_title_id,
          'prof_reg_number', candidate_row.prof_reg_number,
          'prof_reg_type', candidate_row.prof_reg_type,
          'ni_number', candidate_row.ni_number,
          'date_of_birth', candidate_row.date_of_birth,
          'gender', candidate_row.gender,
          'title', candidate_row.title,
          'opt_in_email', candidate_row.opt_in_email,
          'opt_in_sms', candidate_row.opt_in_sms,
          'opt_in_whatsapp', candidate_row.opt_in_whatsapp,
          'band', candidate_row.band,
          'tms_ref_num', candidate_row.tms_ref_num
        ) END,
        'client', CASE WHEN client_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', client_row.id,
          'cli_ref', client_row.cli_ref,
          'name', client_row.name,
          'invoice_address', client_row.invoice_address,
          'primary_invoice_email', client_row.primary_invoice_email,
          'ap_phone', client_row.ap_phone,
          'vat_chargeable', client_row.vat_chargeable,
          'payment_terms_days', client_row.payment_terms_days,
          'mileage_charge_rate', client_row.mileage_charge_rate,
          'ts_queries_email', client_row.ts_queries_email,
          'client_address', client_row.client_address,
          'contact_title', client_row.contact_title,
          'contact_known_as', client_row.contact_known_as,
          'contact_forename', client_row.contact_forename,
          'contact_surname', client_row.contact_surname,
          'contact_job_title', client_row.contact_job_title,
          'contact_tel', client_row.contact_tel,
          'contact_mobile', client_row.contact_mobile,
          'contact_email', client_row.contact_email,
          'website', client_row.website
        ) END,
        'contract', COALESCE(contract_payload.contract_json, NULL::jsonb),
        'invoices', '[]'::jsonb,
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'invoice', NULL::jsonb,
        'umbrella', NULL::jsonb,
        'series', '[]'::jsonb
      ) AS related_json
    FROM row_ids
    LEFT JOIN candidate_row ON TRUE
    LEFT JOIN client_row ON TRUE
    LEFT JOIN contract_payload ON TRUE
  )
,
  shift_rows AS (
    SELECT
      ns0.id,
      ns0.external_row_key,
      ns0.latest_import_id,
      ns0.candidate_id,
      ns0.client_id,
      ns0.contract_id,
      ns0.timesheet_id,
      ns0.work_date,
      ns0.ward,
      ns0.start_utc,
      ns0.end_utc,
      ns0.break_mins,
      ns0.pay_minutes,
      ns0.pay_amount_snapshot,
      ns0.charge_amount_snapshot,
      ns0.invoice_status,
      ns0.defer_until_run_after,
      ns0.invoice_id,
      ns0.created_at,
      ns0.updated_at,
      ns0.source_system,
      ns0.hr_request_id,
      ns0.held_back_reason,
      ns0.staff_name,
      ns0.staff_norm,
      ns0.ward_norm,
      ns0.assignment_code,
      ns0.ref_num,
      ns0.week_ending_date,
      ns0.cancelled_at_utc,
      ns0.cancelled_by_import_id,
      ns0.cancelled_reason
    FROM public.nhsp_shifts AS ns0
    CROSS JOIN row_ids AS ids
    WHERE (v_include_compare = TRUE OR v_include_import_source_rows = TRUE)
      AND (
        (v_include_compare = TRUE AND COALESCE((ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE)
        OR (v_include_import_source_rows = TRUE AND UPPER(COALESCE(ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE')
      )
      AND (
        (ids.timesheet_id IS NOT NULL AND ns0.timesheet_id = ids.timesheet_id)
        OR (
          ids.timesheet_id IS NULL
          AND ids.contract_id IS NOT NULL
          AND ns0.contract_id = ids.contract_id
          AND ns0.week_ending_date = CASE
            WHEN COALESCE(ids.row_json->>'week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (ids.row_json->>'week_ending_date')::date
            WHEN COALESCE(ids.row_json->>'contract_week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (ids.row_json->>'contract_week_ending_date')::date
            ELSE NULL::date
          END
        )
      )
    ORDER BY ns0.work_date ASC NULLS LAST, ns0.start_utc ASC NULLS LAST, ns0.id ASC
  ),
  shifts_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', shift_rows.id,
          'external_row_key', shift_rows.external_row_key,
          'latest_import_id', shift_rows.latest_import_id,
          'candidate_id', shift_rows.candidate_id,
          'client_id', shift_rows.client_id,
          'contract_id', shift_rows.contract_id,
          'timesheet_id', shift_rows.timesheet_id,
          'work_date', shift_rows.work_date,
          'ward', shift_rows.ward,
          'start_utc', shift_rows.start_utc,
          'end_utc', shift_rows.end_utc,
          'break_mins', shift_rows.break_mins,
          'pay_minutes', shift_rows.pay_minutes,
          'pay_amount_snapshot', shift_rows.pay_amount_snapshot,
          'charge_amount_snapshot', shift_rows.charge_amount_snapshot,
          'invoice_status', shift_rows.invoice_status,
          'defer_until_run_after', shift_rows.defer_until_run_after,
          'invoice_id', shift_rows.invoice_id,
          'created_at', shift_rows.created_at,
          'updated_at', shift_rows.updated_at,
          'source_system', shift_rows.source_system,
          'hr_request_id', shift_rows.hr_request_id,
          'held_back_reason', shift_rows.held_back_reason,
          'staff_name', shift_rows.staff_name,
          'staff_norm', shift_rows.staff_norm,
          'ward_norm', shift_rows.ward_norm,
          'assignment_code', shift_rows.assignment_code,
          'ref_num', shift_rows.ref_num,
          'week_ending_date', shift_rows.week_ending_date,
          'cancelled_at_utc', shift_rows.cancelled_at_utc,
          'cancelled_by_import_id', shift_rows.cancelled_by_import_id,
          'cancelled_reason', shift_rows.cancelled_reason
        ) ORDER BY shift_rows.work_date ASC NULLS LAST, shift_rows.start_utc ASC NULLS LAST, shift_rows.id ASC
      ), '[]'::jsonb) AS shifts_json,
      JSONB_BUILD_OBJECT(
        'import_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr1.latest_import_id::text)) FROM shift_rows AS sr1 WHERE sr1.latest_import_id IS NOT NULL), '[]'::jsonb),
        'source_systems', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr2.source_system::text)) FROM shift_rows AS sr2 WHERE sr2.source_system IS NOT NULL), '[]'::jsonb),
        'shift_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr3.id::text)) FROM shift_rows AS sr3 WHERE sr3.id IS NOT NULL), '[]'::jsonb),
        'external_row_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr4.external_row_key)) FROM shift_rows AS sr4 WHERE NULLIF(BTRIM(COALESCE(sr4.external_row_key, '')), '') IS NOT NULL), '[]'::jsonb),
        'hr_request_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr5.hr_request_id)) FROM shift_rows AS sr5 WHERE NULLIF(BTRIM(COALESCE(sr5.hr_request_id, '')), '') IS NOT NULL), '[]'::jsonb),
        'work_dates', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr6.work_date::text)) FROM shift_rows AS sr6 WHERE sr6.work_date IS NOT NULL), '[]'::jsonb),
        'ref_nums', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr7.ref_num)) FROM shift_rows AS sr7 WHERE NULLIF(BTRIM(COALESCE(sr7.ref_num, '')), '') IS NOT NULL), '[]'::jsonb)
      ) AS imported_detail_refs_json
    FROM shift_rows
  ),
  base_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'context_type', 'bulk_authorise',
        'slim_context', TRUE,
        'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
        'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
        'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
        'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version', '') ~ '^[0-9]+$' THEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version')::integer ELSE NULL::integer END,
        'contract_week_id', row_ids.row_json->>'contract_week_id',
        'row_key', row_ids.row_json->>'row_key',
        'row_signature', row_ids.row_json->>'row_signature',
        'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
        'has_timesheet', COALESCE((row_ids.row_json->>'has_timesheet')::boolean, FALSE),
        'locked', COALESCE((row_ids.row_json->>'locked')::boolean, FALSE),
        'bulk_process_bucket', row_ids.row_json->>'bulk_process_bucket',
        'bulk_authorise_classification', row_ids.row_json->>'bulk_authorise_classification',
        'bulk_authorise_section', row_ids.row_json->>'bulk_authorise_section',
        'route_family', row_ids.row_json->>'route_family',
        'route_subfamily', row_ids.row_json->>'route_subfamily',
        'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
        'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
        'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
        'period_type', row_ids.row_json->>'period_type',
        'timesheet_type_sort_key', CASE WHEN NULLIF(row_ids.row_json->>'timesheet_type_sort_key', '') IS NULL THEN NULL::integer ELSE (row_ids.row_json->>'timesheet_type_sort_key')::integer END,
        'can_save', COALESCE((row_ids.row_json->>'can_save')::boolean, FALSE),
        'can_process', COALESCE((row_ids.row_json->>'can_process')::boolean, FALSE),
        'can_unprocess', COALESCE((row_ids.row_json->>'can_unprocess')::boolean, FALSE),
        'can_edit_timesheet_data', COALESCE((row_ids.row_json->>'can_edit_timesheet_data')::boolean, FALSE),
        'can_manage_evidence', COALESCE((row_ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_add_additional_manual', COALESCE((row_ids.row_json->>'can_add_additional_manual')::boolean, FALSE),
        'review_only', COALESCE((row_ids.row_json->>'review_only')::boolean, FALSE),
        'hr_validation_required_for_invoice', COALESCE((row_ids.row_json->>'hr_validation_required_for_invoice')::boolean, FALSE),
        'validation_status', row_ids.row_json->>'validation_status',
        'validation_pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
        'has_deviation_marker', COALESCE((row_ids.row_json->>'has_deviation_marker')::boolean, FALSE),
        'deviation_marker_reason', row_ids.row_json->>'deviation_marker_reason'
      )
      || JSONB_BUILD_OBJECT(
        'can_bulk_authorise', COALESCE((row_ids.row_json->>'can_bulk_authorise')::boolean, FALSE),
        'can_bulk_unauthorise', COALESCE((row_ids.row_json->>'can_bulk_unauthorise')::boolean, FALSE),
        'requires_authorisation', COALESCE((row_ids.row_json->>'requires_authorisation')::boolean, FALSE),
        'is_authorised', COALESCE((row_ids.row_json->>'is_authorised')::boolean, FALSE),
        'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
        'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
        'qr_unsigned_blocked', COALESCE((row_ids.row_json->>'qr_unsigned_blocked')::boolean, FALSE),
        'qr_signed_returned', COALESCE((row_ids.row_json->>'qr_signed_returned')::boolean, FALSE),
        'row', row_ids.row_json,
        'row_patch', COALESCE(row_ids.row_json->'row_patch', JSONB_BUILD_OBJECT()),
        'details', (
          JSONB_BUILD_OBJECT(
            'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
            'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
            'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
            'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version', '') ~ '^[0-9]+$' THEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version')::integer ELSE NULL::integer END,
            'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
            'booking_id', row_ids.row_json->>'booking_id',
            'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
            'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
            'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
            'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
            'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
          )
          || JSONB_BUILD_OBJECT(
            'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', row_ids.row_json->>'validation_status',
              'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
              'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
              'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
              'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
            ),
            'shifts', CASE
              WHEN v_include_import_source_rows = TRUE
               AND (COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE OR UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE') THEN COALESCE(shifts_payload.shifts_json, '[]'::jsonb)
              ELSE '[]'::jsonb
            END,
            'contract_week_id', row_ids.row_json->>'contract_week_id',
            'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
            'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
            'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
          )
          || JSONB_BUILD_OBJECT(
            'policy', (
              CASE
                WHEN tsfin_payload.tsfin_json IS NOT NULL
                 AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
                ELSE JSONB_BUILD_OBJECT()
              END
              || JSONB_BUILD_OBJECT(
                'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
                'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
                'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
                'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
                'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
                'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
              )
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', row_ids.row_json->>'route_type',
              'route_display', row_ids.row_json->>'route_display',
              'summary_stage', row_ids.row_json->>'summary_stage',
              'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
              'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
              'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
              'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
              'contract_id', row_ids.row_json->>'contract_id',
              'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
              'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
            ),
            'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
            'summary_stage', row_ids.row_json->>'summary_stage',
            'route_type', row_ids.row_json->>'route_type',
            'route_display', row_ids.row_json->>'route_display',
            'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
            'sheet_scope', row_ids.row_json->>'sheet_scope',
            'period_type', row_ids.row_json->>'period_type'
          )
          || JSONB_BUILD_OBJECT(
            'qr_status', row_ids.row_json->>'qr_status',
            'qr_generated_at', row_ids.row_json->>'qr_generated_at',
            'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
            'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
            'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
            'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
            'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees',
            'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
            'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
            'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
            'healthroster_compare', CASE
              WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
                'required', TRUE,
                'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
                'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
              )
              ELSE JSONB_BUILD_OBJECT(
                'required', FALSE,
                'rows', '[]'::jsonb,
                'imported_detail_refs', JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb)
              )
            END,
            'import_source_rows', CASE
              WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
              ELSE NULL::jsonb
            END
          )
          || JSONB_BUILD_OBJECT(
            'primary_artifact', CASE
              WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
                'id', row_ids.row_json->>'primary_artifact_id',
                'kind', row_ids.row_json->>'primary_artifact_kind',
                'display_name', row_ids.row_json->>'primary_artifact_display_name',
                'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
                'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
              )
              ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
            END,
            'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
            'pay_state', NULL::jsonb,
            'segment_snoozes', '[]'::jsonb
          )
        ),
        'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
        'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
        'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
        'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
      )
      || JSONB_BUILD_OBJECT(
        'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
        'validation_summary', JSONB_BUILD_OBJECT(
          'status', row_ids.row_json->>'validation_status',
          'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
          'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
          'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
          'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
        ),
        'shifts', CASE
          WHEN v_include_import_source_rows = TRUE
           AND (COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE OR UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE') THEN COALESCE(shifts_payload.shifts_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END,
        'healthroster_compare', CASE
          WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
            'required', TRUE,
            'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
            'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
          )
          ELSE JSONB_BUILD_OBJECT(
            'required', FALSE,
            'rows', '[]'::jsonb,
            'imported_detail_refs', JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb)
          )
        END,
        'compare_payload', CASE
          WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
            'required', TRUE,
            'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
            'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
          )
          ELSE NULL::jsonb
        END,
        'import_source_rows', CASE
          WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
          ELSE NULL::jsonb
        END
      )
      || JSONB_BUILD_OBJECT(
        'effective', JSONB_BUILD_OBJECT(
          'route_type', row_ids.row_json->>'route_type',
          'route_display', row_ids.row_json->>'route_display',
          'summary_stage', row_ids.row_json->>'summary_stage',
          'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
          'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
          'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
          'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
          'contract_id', row_ids.row_json->>'contract_id',
          'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
          'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
        ),
        'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
        'summary_stage', row_ids.row_json->>'summary_stage',
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display',
        'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
        'sheet_scope', row_ids.row_json->>'sheet_scope',
        'period_type', row_ids.row_json->>'period_type',
        'qr_status', row_ids.row_json->>'qr_status',
        'qr_generated_at', row_ids.row_json->>'qr_generated_at',
        'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
        'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
        'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
        'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
        'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees'
      )
      || JSONB_BUILD_OBJECT(
        'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
        'policy', (
          CASE
            WHEN tsfin_payload.tsfin_json IS NOT NULL
             AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
            ELSE JSONB_BUILD_OBJECT()
          END
          || JSONB_BUILD_OBJECT(
            'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
            'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
            'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
            'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
            'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
            'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
          )
        ),
        'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
        'evidence_meta', JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE((row_ids.row_json->>'has_any_evidence')::boolean, FALSE),
          'evidence_badges', COALESCE(row_ids.row_json->'evidence_badges', '[]'::jsonb),
          'attached_evidence_count', COALESCE(NULLIF(row_ids.row_json->>'attached_evidence_count', '')::integer, 0),
          'queue_staged_count', COALESCE(NULLIF(row_ids.row_json->>'queue_staged_count', '')::integer, 0),
          'evidence_document_locked', COALESCE((row_ids.row_json->>'evidence_document_locked')::boolean, FALSE),
          'evidence_lock_reason', row_ids.row_json->>'evidence_lock_reason'
        ),
        'primary_artifact', CASE
          WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'id', row_ids.row_json->>'primary_artifact_id',
            'kind', row_ids.row_json->>'primary_artifact_kind',
            'display_name', row_ids.row_json->>'primary_artifact_display_name',
            'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
          )
          ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
        END,
        'primary_artifact_id', row_ids.row_json->>'primary_artifact_id',
        'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_artifact_preview_mode', row_ids.row_json->>'primary_artifact_preview_mode',
        'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', CASE
            WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
              'id', row_ids.row_json->>'primary_artifact_id',
              'kind', row_ids.row_json->>'primary_artifact_kind',
              'display_name', row_ids.row_json->>'primary_artifact_display_name',
              'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
              'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
            )
            ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
          END,
          'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
          'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'compare_payload', CASE
            WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
              'required', TRUE,
              'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
              'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
            )
            ELSE NULL::jsonb
          END,
          'source_items', CASE
            WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        )
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
        'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
        'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
        'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
        'pay_state', NULL::jsonb,
        'segment_snoozes', '[]'::jsonb,
        'is_advanced', FALSE,
        'can_unadvance', FALSE,
        'advanced_consumed_by_batch_id', NULL::text,
        'is_snoozed', FALSE,
        'snooze_until_date', NULL::text,
        'snooze_is_indefinite', FALSE,
        'snooze_note', NULL::text
      ) AS payload_json
    FROM decision_row
    CROSS JOIN row_ids
    LEFT JOIN timesheet_payload ON TRUE
    LEFT JOIN tsfin_payload ON TRUE
    LEFT JOIN contract_week_payload ON TRUE
    LEFT JOIN validation_payload ON TRUE
    LEFT JOIN evidence_payload ON TRUE
    LEFT JOIN related_payload ON TRUE
    LEFT JOIN shifts_payload ON TRUE
  ),
  final_payload AS (
    SELECT
      base_payload.payload_json
      || JSONB_BUILD_OBJECT(
        'details', COALESCE(base_payload.payload_json->'details', JSONB_BUILD_OBJECT()),
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', base_payload.payload_json->>'route_family',
          'route_subfamily', base_payload.payload_json->>'route_subfamily',
          'underlying_channel_family', base_payload.payload_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((base_payload.payload_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((base_payload.payload_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', COALESCE(base_payload.payload_json->'primary_artifact', NULL::jsonb),
          'source_items', CASE
            WHEN v_include_import_source_rows = TRUE THEN COALESCE(NULLIF(base_payload.payload_json->'import_source_rows', 'null'::jsonb), base_payload.payload_json->'shifts', '[]'::jsonb)
            ELSE '[]'::jsonb
          END,
          'primary_left_pane_mode', base_payload.payload_json->>'primary_left_pane_mode'
        ),
        'compare_payload', CASE
          WHEN v_include_compare = TRUE THEN COALESCE(base_payload.payload_json->'healthroster_compare', JSONB_BUILD_OBJECT('required', FALSE, 'rows', '[]'::jsonb, 'imported_detail_refs', JSONB_BUILD_OBJECT()))
          ELSE NULL::jsonb
        END,
        'data_row', COALESCE(base_payload.payload_json->'row', JSONB_BUILD_OBJECT())
      ) AS payload_json
    FROM base_payload
  )
  SELECT final_payload.payload_json
    INTO v_out
  FROM final_payload;

  IF v_out IS NOT NULL THEN
    v_out := v_out || JSONB_BUILD_OBJECT(
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', COALESCE(v_include_evidence, FALSE),
      'compare_loaded', COALESCE(v_include_compare, FALSE),
      'full_loaded', (v_profile = 'full'),
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', CASE
        WHEN v_profile = 'full' THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import', 'full')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE AND COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence')
        WHEN COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'compare_import')
        ELSE JSONB_BUILD_ARRAY('header', 'editor')
      END
    );
  END IF;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'context_kind', 'bulk_authorise_row_context',
    'context_profile', v_profile,
    'profile', v_profile,
    'soft_failure', TRUE,
    'context_degraded', TRUE,
    'degraded_reason', 'ROW_NOT_FOUND',
    'header_loaded', FALSE,
    'header_only', FALSE,
    'editor_loaded', FALSE,
    'evidence_loaded', FALSE,
    'compare_loaded', FALSE,
    'full_loaded', FALSE,
    'schedule_pending', TRUE,
    'schedule_authoritative', FALSE,
    'loaded_layers', '[]'::jsonb,
    'error', 'ROW_NOT_FOUND',
    'message', 'No bulk authorise row context was found for the supplied identity',
    'filters', v_filters
  ));
END;
$function$;


CREATE OR REPLACE FUNCTION public.timesheet_qr_send_enqueue_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid DEFAULT NULL,
  p_actor_user_id uuid DEFAULT NULL,
  p_idempotency_key text DEFAULT NULL,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_timesheet_id uuid := NULL;
  v_requested_booking_id text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_contract_id uuid := NULL;
  v_week_ending_date date := NULL;
  v_worked_start_iso timestamp with time zone := NULL;
  v_worked_end_iso timestamp with time zone := NULL;
  v_actual_schedule_json jsonb := NULL;
  v_qr_status text := NULL;
  v_qr_token text := NULL;
  v_effective_qr_token text := NULL;
  v_qr_payload_json jsonb := '{}'::jsonb;
  v_payload_qr_token text := NULL;
  v_qr_generated_at timestamp with time zone := NULL;
  v_qr_scanned_at timestamp with time zone := NULL;
  v_qr_signed_hash text := NULL;
  v_qr_signed_at_utc timestamp with time zone := NULL;
  v_updated_at timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_tsfin_processing_status text := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_has_hours_for_send boolean := FALSE;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_candidate_email text := NULL;
  v_candidate_name text := NULL;
  v_candidate_opt_in_email boolean := TRUE;
  v_recipient_available boolean := FALSE;
  v_contract_candidate_id uuid := NULL;
  v_contract_client_id uuid := NULL;
  v_idempotency_key text := NULL;
  v_client_idempotency_key text := NULL;
  v_recipient_namespace text := NULL;
  v_mail_held_until_pdf_rendered boolean := TRUE;
  v_existing_mail_id uuid := NULL;
  v_existing_mail_status text := NULL;
  v_mail_job_id uuid := NULL;
  v_pdf_job_id uuid := NULL;
  v_existing_pdf_job_id uuid := NULL;
  v_job_id uuid := NULL;
  v_send_state text := NULL;
  v_pdf_key text := NULL;
  v_mail_subject text := NULL;
  v_mail_body_text text := NULL;
  v_mail_body_html text := NULL;
  v_mail_reference text := NULL;
  v_mail_scheduled_for_utc timestamp with time zone := NULL;
  v_created_by_user_id uuid := NULL;
  v_post_row jsonb := NULL;
  v_row_key text := NULL;
  v_storage_key text := NULL;
  v_row_signature text := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_ID_REQUIRED',
      'message', 'p_timesheet_id is required.',
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT requested_ts.timesheet_id,
         requested_ts.booking_id
    INTO v_requested_timesheet_id,
         v_requested_booking_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_NOT_FOUND',
      'message', 'Timesheet was not found.',
      'requested_timesheet_id', p_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST,
           current_ts.updated_at DESC NULLS LAST,
           current_ts.created_at DESC NULLS LAST,
           current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  IF p_expected_timesheet_id IS NOT NULL AND p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'timesheet_qr_send_enqueue_v1 requires the current timesheet row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', COALESCE(p_expected_timesheet_id, p_timesheet_id),
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT ts_current.sheet_scope::text,
         ts_current.submission_mode::text,
         ts_current.contract_id,
         ts_current.week_ending_date,
         ts_current.worked_start_iso,
         ts_current.worked_end_iso,
         ts_current.actual_schedule_json,
         ts_current.qr_status::text,
         ts_current.qr_token,
         ts_current.qr_payload_json,
         ts_current.qr_generated_at,
         ts_current.qr_scanned_at,
         ts_current.qr_signed_hash,
         ts_current.qr_signed_at_utc,
         ts_current.version,
         ts_current.updated_at
    INTO v_sheet_scope,
         v_submission_mode,
         v_contract_id,
         v_week_ending_date,
         v_worked_start_iso,
         v_worked_end_iso,
         v_actual_schedule_json,
         v_qr_status,
         v_qr_token,
         v_qr_payload_json,
         v_qr_generated_at,
         v_qr_scanned_at,
         v_qr_signed_hash,
         v_qr_signed_at_utc,
         v_current_version,
         v_updated_at
  FROM public.timesheets AS ts_current
  WHERE ts_current.timesheet_id = v_current_timesheet_id
    AND ts_current.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'message', 'Current timesheet was not found.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) NOT IN ('DAILY', 'WEEKLY') THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'UNSUPPORTED_SHEET_SCOPE',
      'message', 'QR send is only supported for DAILY or WEEKLY timesheets.',
      'current_timesheet_id', v_current_timesheet_id,
      'sheet_scope', v_sheet_scope,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NOT_MANUAL_QR_ROUTE',
      'message', 'QR send requires a MANUAL submission-mode timesheet with QR enabled.',
      'current_timesheet_id', v_current_timesheet_id,
      'submission_mode', v_submission_mode,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_qr_status, '')) <> 'PENDING' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_NOT_PENDING',
      'message', 'QR send requires qr_status=PENDING.',
      'current_timesheet_id', v_current_timesheet_id,
      'qr_status', v_qr_status,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_qr_scanned_at IS NOT NULL OR NULLIF(BTRIM(COALESCE(v_qr_signed_hash, '')), '') IS NOT NULL OR v_qr_signed_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_ALREADY_SIGNED',
      'message', 'Cannot queue QR send: timesheet already has signed QR markers.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT tf_current.id,
         tf_current.processing_status::text,
         tf_current.locked_by_invoice_id,
         tf_current.paid_at_utc,
         tf_current.invoice_breakdown_json,
         tf_current.candidate_id,
         tf_current.client_id
    INTO v_tsfin_id,
         v_tsfin_processing_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json,
         v_candidate_id,
         v_client_id
  FROM public.timesheets_financials AS tf_current
  WHERE tf_current.timesheet_id = v_current_timesheet_id
    AND tf_current.is_current = TRUE
  ORDER BY tf_current.computed_at_utc DESC NULLS LAST,
           tf_current.created_at DESC NULLS LAST,
           tf_current.updated_at DESC NULLS LAST,
           tf_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_TSFIN',
      'message', 'No current financial snapshot exists for this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_LOCKED_OR_PAID',
      'message', 'Cannot queue QR send: timesheet is locked, invoice-locked, or paid.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_actual_schedule_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_actual_schedule_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS schedule_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'start', schedule_segment.segment_json->>'start_utc', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'end', schedule_segment.segment_json->>'end_utc', '')), '') IS NOT NULL
    ) OR EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
    ) INTO v_has_hours_for_send;
  ELSE
    v_has_hours_for_send := v_worked_start_iso IS NOT NULL AND v_worked_end_iso IS NOT NULL;
  END IF;

  IF COALESCE(v_has_hours_for_send, FALSE) = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_HOURS_RECORDED',
      'message', 'Cannot queue QR send: no hours are recorded yet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_contract_id IS NOT NULL THEN
    SELECT contract_row.candidate_id,
           contract_row.client_id
      INTO v_contract_candidate_id,
           v_contract_client_id
    FROM public.contracts AS contract_row
    WHERE contract_row.id = v_contract_id
    LIMIT 1;
  END IF;

  v_candidate_id := COALESCE(v_contract_candidate_id, v_candidate_id);
  v_client_id := COALESCE(v_contract_client_id, v_client_id);

  IF v_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_NOT_FOUND',
      'message', 'Cannot queue QR send: candidate could not be resolved.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT NULLIF(BTRIM(candidate_row.email), ''),
         COALESCE(NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(candidate_row.first_name), ''), NULLIF(BTRIM(candidate_row.last_name), ''))), '')),
         COALESCE(candidate_row.opt_in_email, TRUE)
    INTO v_candidate_email,
         v_candidate_name,
         v_candidate_opt_in_email
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = v_candidate_id
  LIMIT 1;

  v_recipient_available := NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NOT NULL AND COALESCE(v_candidate_opt_in_email, TRUE) = TRUE;

  IF v_recipient_available = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'CANDIDATE_EMAIL_MISSING' ELSE 'CANDIDATE_EMAIL_OPTED_OUT' END,
      'message', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'Cannot queue QR send: candidate email is missing.' ELSE 'Cannot queue QR send: candidate has opted out of email.' END,
      'current_timesheet_id', v_current_timesheet_id,
      'candidate_id', v_candidate_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  v_payload_qr_token := CASE
    WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN NULLIF(BTRIM(COALESCE(v_qr_payload_json->>'tok', '')), '')
    ELSE NULL
  END;
  v_effective_qr_token := COALESCE(NULLIF(BTRIM(COALESCE(v_qr_token, '')), ''), v_payload_qr_token, gen_random_uuid()::text);
  v_qr_payload_json := COALESCE(CASE WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN v_qr_payload_json ELSE '{}'::jsonb END, '{}'::jsonb)
    || jsonb_build_object('v', 1, 'tok', v_effective_qr_token);

  UPDATE public.timesheets AS ts_qr_update
     SET qr_token = v_effective_qr_token,
         qr_payload_json = v_qr_payload_json,
         qr_generated_at = COALESCE(ts_qr_update.qr_generated_at, v_now),
         qr_r2_key = NULL,
         qr_scanned_at = NULL,
         qr_scan_info_json = NULL,
         updated_at = v_now
   WHERE ts_qr_update.timesheet_id = v_current_timesheet_id
     AND ts_qr_update.is_current = TRUE
  RETURNING ts_qr_update.qr_generated_at,
            ts_qr_update.updated_at
       INTO v_qr_generated_at,
            v_updated_at;

  IF UPPER(COALESCE(v_tsfin_processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE' THEN
    UPDATE public.timesheets_financials AS tf_update
       SET processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum,
           updated_at = v_now
     WHERE tf_update.id = v_tsfin_id
       AND tf_update.is_current = TRUE;
  END IF;

  SELECT tms_user_check.id
    INTO v_created_by_user_id
  FROM public.tms_users AS tms_user_check
  WHERE tms_user_check.id = p_actor_user_id
  LIMIT 1;

  v_client_idempotency_key := NULLIF(
    REGEXP_REPLACE(
      LOWER(BTRIM(COALESCE(p_idempotency_key, ''))),
      '[^a-z0-9._:-]+',
      '-',
      'g'
    ),
    ''
  );

  IF v_client_idempotency_key IS NULL THEN
    v_client_idempotency_key := 'auto:' || md5(CONCAT_WS('|',
      v_current_timesheet_id::text,
      COALESCE(v_current_version::text, ''),
      LOWER(COALESCE(v_candidate_email, ''))
    ));
  END IF;

  v_recipient_namespace := md5(LOWER(COALESCE(v_candidate_email, '')));

  v_idempotency_key := 'timesheet_qr_send:'
    || v_current_timesheet_id::text
    || ':v' || COALESCE(v_current_version::text, '0')
    || ':recipient:' || v_recipient_namespace
    || ':key:' || md5(v_client_idempotency_key);

  PERFORM pg_advisory_xact_lock(hashtext('timesheet_qr_send:' || v_current_timesheet_id::text));
  PERFORM pg_advisory_xact_lock(hashtext(v_idempotency_key));

  v_pdf_key := 'docs-pdf/timesheets/ts_' || v_current_timesheet_id::text || '.pdf';
  v_mail_reference := v_idempotency_key;
  v_mail_scheduled_for_utc := TIMESTAMPTZ '9999-12-31 00:00:00+00';
  v_mail_subject := CASE
    WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Weekly QR timesheet – week ending ' || COALESCE(v_week_ending_date::text, '(unknown)')
    ELSE 'Daily QR timesheet – ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)')
  END;
  v_mail_body_text := CONCAT_WS(E'\n',
    'Please print the attached timesheet, ask the ward manager to sign it,',
    'and then upload the signed copy via the app.',
    '',
    CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END,
    'Timesheet ID: ' || v_current_timesheet_id::text
  );
  v_mail_body_html := '<p>Please print the attached timesheet, ask the ward manager to sign it, and then upload the signed copy via the app.<br/><br/>'
    || CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END
    || '<br/>Timesheet ID: ' || v_current_timesheet_id::text || '</p>';

  SELECT existing_pdf.id
    INTO v_existing_pdf_job_id
  FROM public.ts_pdfs_outbox AS existing_pdf
  WHERE existing_pdf.timesheet_id = v_current_timesheet_id
    AND existing_pdf.reason = 'FORCE_REGEN'::public.ts_pdf_reason_enum
  ORDER BY existing_pdf.created_at DESC,
           existing_pdf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_existing_pdf_job_id IS NOT NULL THEN
    UPDATE public.ts_pdfs_outbox AS pdf_update
       SET attempt_count = 0,
           next_attempt_at = NULL,
           last_error = NULL,
           prefer_generated = TRUE,
           force_regen = TRUE
     WHERE pdf_update.id = v_existing_pdf_job_id
    RETURNING pdf_update.id INTO v_pdf_job_id;
  ELSE
    INSERT INTO public.ts_pdfs_outbox AS pdf_insert (
      timesheet_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      prefer_generated,
      force_regen,
      created_at
    ) VALUES (
      v_current_timesheet_id,
      'FORCE_REGEN'::public.ts_pdf_reason_enum,
      0,
      NULL,
      NULL,
      TRUE,
      TRUE,
      v_now
    )
    RETURNING pdf_insert.id INTO v_pdf_job_id;
  END IF;

  SELECT mail_existing.id,
         mail_existing.status::text
    INTO v_existing_mail_id,
         v_existing_mail_status
  FROM public.mail_outbox AS mail_existing
  WHERE mail_existing.type = 'TIMESHEET_QR'
    AND mail_existing.reference = v_mail_reference
    AND mail_existing.context_kind = 'timesheets'
    AND mail_existing.context_id = v_current_timesheet_id
    AND mail_existing."to" = v_candidate_email
  ORDER BY mail_existing.created_at_utc DESC,
           mail_existing.id DESC
  LIMIT 1;

  IF v_existing_mail_id IS NOT NULL THEN
    v_mail_job_id := v_existing_mail_id;

    IF UPPER(COALESCE(v_existing_mail_status, '')) = 'SENT' THEN
      v_send_state := 'ALREADY_SENT';
    ELSE
      UPDATE public.mail_outbox AS mail_update
         SET status = 'QUEUED'::public.mail_status_enum,
             subject = v_mail_subject,
             body_html = v_mail_body_html,
             body_text = v_mail_body_text,
             attachments = jsonb_build_array(jsonb_build_object('r2_key', v_pdf_key, 'filename', 'Timesheet_' || COALESCE(v_week_ending_date::text, v_current_timesheet_id::text) || '.pdf')),
             last_error = NULL,
             failed_at = NULL,
             scheduled_for_utc = v_mail_scheduled_for_utc,
             next_attempt_at_utc = v_mail_scheduled_for_utc,
             provider_status = NULL,
             provider_message_id = NULL,
             attempt_lease_token = NULL,
             attempt_leased_at_utc = NULL,
             attempt_lease_expires_at_utc = NULL,
             payment_scope_json = jsonb_build_object(
               'job_kind', 'TIMESHEET_QR_SEND',
               'pdf_job_id', v_pdf_job_id::text,
               'idempotency_key', v_idempotency_key,
               'client_idempotency_key', v_client_idempotency_key,
               'requires_pdf_render', TRUE,
               'release_mail_after_pdf_render', TRUE,
               'mail_delayed_for_pdf_render', TRUE,
        'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
        'mail_hold_reason', 'PDF_RENDER_PENDING',
               'pdf_render_delay_minutes', 10,
               'pdf_storage_key', v_pdf_key,
               'current_timesheet_id', v_current_timesheet_id::text,
               'current_version', v_current_version,
               'recipient_email', v_candidate_email
             )
       WHERE mail_update.id = v_existing_mail_id;

      v_send_state := CASE WHEN UPPER(COALESCE(v_existing_mail_status, '')) = 'FAILED' THEN 'PDF_REQUEUED_MAIL_HELD' ELSE 'PDF_ALREADY_QUEUED_MAIL_HELD' END;
    END IF;
  ELSE
    INSERT INTO public.mail_outbox AS mail_insert (
      type,
      "to",
      cc,
      bcc,
      reply_to,
      importance,
      email_type,
      subject,
      body_html,
      body_text,
      attachments,
      status,
      last_error,
      created_at_utc,
      sent_at,
      created_by,
      reference,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      mailshot_run_id,
      document_template_id,
      provider_status,
      delivered_at,
      read_at,
      scheduled_for_utc,
      next_attempt_at_utc,
      payment_scope_json
    ) VALUES (
      'TIMESHEET_QR',
      v_candidate_email,
      NULL,
      NULL,
      NULL,
      'Normal',
      'html',
      v_mail_subject,
      v_mail_body_html,
      v_mail_body_text,
      jsonb_build_array(jsonb_build_object('r2_key', v_pdf_key, 'filename', 'Timesheet_' || COALESCE(v_week_ending_date::text, v_current_timesheet_id::text) || '.pdf')),
      'QUEUED'::public.mail_status_enum,
      NULL,
      v_now,
      NULL,
      v_created_by_user_id,
      v_mail_reference,
      'candidate',
      v_candidate_id,
      'timesheets',
      v_current_timesheet_id,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      v_mail_scheduled_for_utc,
      v_mail_scheduled_for_utc,
      jsonb_build_object(
        'job_kind', 'TIMESHEET_QR_SEND',
        'pdf_job_id', v_pdf_job_id::text,
        'idempotency_key', v_idempotency_key,
        'client_idempotency_key', v_client_idempotency_key,
        'requires_pdf_render', TRUE,
        'release_mail_after_pdf_render', TRUE,
        'mail_delayed_for_pdf_render', TRUE,
        'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
        'mail_hold_reason', 'PDF_RENDER_PENDING',
        'pdf_render_delay_minutes', 10,
        'pdf_storage_key', v_pdf_key,
        'current_timesheet_id', v_current_timesheet_id::text,
        'current_version', v_current_version,
        'recipient_email', v_candidate_email
      )
    )
    RETURNING id INTO v_mail_job_id;

    v_send_state := 'PDF_QUEUED_MAIL_HELD';
  END IF;

  v_job_id := v_mail_job_id;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_decision_v1(jsonb_build_object(
    'dataset_mode', 'process',
    'timesheet_id', v_current_timesheet_id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');

  INSERT INTO public.audit_events AS audit_insert (
    ts_utc,
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    v_now,
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_QR_SEND_QUEUED',
    NULL,
    jsonb_build_object(
      'timesheet_id', v_current_timesheet_id,
      'job_id', v_job_id,
      'mail_outbox_id', v_mail_job_id,
      'pdf_job_id', v_pdf_job_id,
      'idempotency_key', v_idempotency_key,
      'send_state', v_send_state,
      'recipient_email', v_candidate_email,
      'scheduled_for_utc', v_mail_scheduled_for_utc,
      'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
      'mail_delayed_for_pdf_render', TRUE,
      'mail_hold_reason', 'PDF_RENDER_PENDING',
      'pdf_render_delay_minutes', 10,
      'pdf_storage_key', v_pdf_key,
      'client_idempotency_key', v_client_idempotency_key,
      'recipient_namespace', v_recipient_namespace
    ),
    'Bulk QR send enqueue'
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'queued', TRUE,
    'operation', 'timesheet_qr_send_enqueue',
    'job_id', v_job_id,
    'mail_outbox_id', v_mail_job_id,
    'pdf_job_id', v_pdf_job_id,
    'idempotency_key', v_idempotency_key,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'recipient_available', TRUE,
    'recipient_email', v_candidate_email,
    'recipient_name', v_candidate_name,
    'send_state', v_send_state,
    'scheduled_for_utc', v_mail_scheduled_for_utc,
    'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
    'mail_delayed_for_pdf_render', TRUE,
    'mail_hold_reason', 'PDF_RENDER_PENDING',
    'pdf_render_delay_minutes', 10,
    'pdf_storage_key', v_pdf_key,
    'client_idempotency_key', v_client_idempotency_key,
    'recipient_namespace', v_recipient_namespace,
    'row_patch', COALESCE(v_post_row->'row_patch', jsonb_build_object()),
    'data_row', COALESCE(v_post_row, jsonb_build_object()),
    'row', COALESCE(v_post_row, jsonb_build_object()),
    'cache_invalidation_hints', jsonb_build_object(
      'row_keys', jsonb_build_array(COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', jsonb_build_array(v_current_timesheet_id),
      'storage_keys', jsonb_build_array(v_storage_key, v_pdf_key),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise'),
      'row_signature', v_row_signature,
      'invalidate_context', TRUE,
      'invalidate_preview', TRUE
    ),
    'cache_invalidation', jsonb_build_object(
      'rows', jsonb_build_array(jsonb_build_object(
        'row_key', COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text),
        'timesheet_id', v_current_timesheet_id,
        'new_row_signature', v_row_signature
      )),
      'artifacts', jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_current_timesheet_id,
        'storage_key', COALESCE(v_storage_key, v_pdf_key),
        'pdf_storage_key', v_pdf_key,
        'changed', TRUE
      )),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise')
    )
  );
END;
$function$;


DROP FUNCTION IF EXISTS public.bulk_authorise_import_evidence_page_v1(jsonb, text, text, text, integer, integer, uuid);

CREATE OR REPLACE FUNCTION public.bulk_authorise_import_evidence_page_v1(
  p_items jsonb DEFAULT '[]'::jsonb,
  p_classification text DEFAULT NULL,
  p_section text DEFAULT NULL,
  p_mode text DEFAULT 'view_all',
  p_page integer DEFAULT 1,
  p_page_size integer DEFAULT 20,
  p_actor_user_id uuid DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb := COALESCE(p_items, '[]'::jsonb);
  v_classification text := UPPER(NULLIF(BTRIM(COALESCE(p_classification, '')), ''));
  v_section text := LOWER(NULLIF(BTRIM(COALESCE(p_section, '')), ''));
  v_mode text := LOWER(NULLIF(BTRIM(COALESCE(p_mode, 'view_all')), ''));
  v_page integer := GREATEST(COALESCE(p_page, 1), 1);
  v_page_size integer := CASE WHEN COALESCE(p_page_size, 20) <= 0 THEN NULL ELSE LEAST(GREATEST(COALESCE(p_page_size, 20), 1), 500) END;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{12}$';
  v_result jsonb := NULL;
BEGIN
  IF v_classification = 'HEALTHROSTER' THEN
    v_classification := 'HR';
  END IF;

  IF v_classification NOT IN ('NHSP', 'HR') THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'success', FALSE,
      'error_code', 'INVALID_CLASSIFICATION',
      'message', 'p_classification must be NHSP, HR, or HEALTHROSTER.',
      'items', '[]'::jsonb,
      'page', v_page,
      'page_size', COALESCE(v_page_size, 0),
      'total', 0,
      'stale_items', '[]'::jsonb,
      'accepted_row_keys', '[]'::jsonb
    );
  END IF;

  IF v_section NOT IN ('processed_eligible', 'authorised_eligible') THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'success', FALSE,
      'error_code', 'INVALID_SECTION',
      'message', 'p_section must be processed_eligible or authorised_eligible.',
      'items', '[]'::jsonb,
      'page', v_page,
      'page_size', COALESCE(v_page_size, 0),
      'total', 0,
      'stale_items', '[]'::jsonb,
      'accepted_row_keys', '[]'::jsonb
    );
  END IF;

  IF v_mode NOT IN ('single', 'view_all') THEN
    v_mode := 'view_all';
  END IF;

  IF jsonb_typeof(v_items) IS DISTINCT FROM 'array' THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'success', FALSE,
      'error_code', 'INVALID_ITEMS',
      'message', 'p_items must be a JSON array.',
      'items', '[]'::jsonb,
      'page', v_page,
      'page_size', COALESCE(v_page_size, 0),
      'total', 0,
      'stale_items', '[]'::jsonb,
      'accepted_row_keys', '[]'::jsonb
    );
  END IF;

  WITH raw_items AS (
    SELECT
      raw_item.ordinality::integer AS input_ordinal,
      raw_item.item AS item_json
    FROM jsonb_array_elements(v_items) WITH ORDINALITY AS raw_item(item, ordinality)
    WHERE jsonb_typeof(raw_item.item) = 'object'
  ),
  normalised_items AS (
    SELECT
      raw_items.input_ordinal,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'row_key', '')), '') AS supplied_row_key,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'row_signature', raw_items.item_json->>'expected_row_signature', '')), '') AS supplied_row_signature,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'timesheet_id', '')), '') AS supplied_timesheet_id_text,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'current_timesheet_id', '')), '') AS supplied_current_timesheet_id_text,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'expected_timesheet_id', '')), '') AS supplied_expected_timesheet_id_text,
      NULLIF(BTRIM(COALESCE(raw_items.item_json->>'contract_week_id', '')), '') AS supplied_contract_week_id_text,
      raw_items.item_json AS item_json
    FROM raw_items
  ),
  resolved_items AS (
    SELECT
      normalised_items.input_ordinal,
      normalised_items.supplied_row_key,
      normalised_items.supplied_row_signature,
      CASE
        WHEN COALESCE(normalised_items.supplied_current_timesheet_id_text, normalised_items.supplied_expected_timesheet_id_text, normalised_items.supplied_timesheet_id_text) ~* v_uuid_re
          THEN COALESCE(normalised_items.supplied_current_timesheet_id_text, normalised_items.supplied_expected_timesheet_id_text, normalised_items.supplied_timesheet_id_text)::uuid
        WHEN normalised_items.supplied_row_key LIKE 'timesheet:%'
         AND SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('timesheet:') + 1) ~* v_uuid_re
          THEN SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('timesheet:') + 1)::uuid
        ELSE NULL::uuid
      END AS requested_timesheet_id,
      CASE
        WHEN normalised_items.supplied_contract_week_id_text ~* v_uuid_re
          THEN normalised_items.supplied_contract_week_id_text::uuid
        WHEN normalised_items.supplied_row_key LIKE 'contract_week:%'
         AND SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract_week:') + 1) ~* v_uuid_re
          THEN SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract_week:') + 1)::uuid
        WHEN normalised_items.supplied_row_key LIKE 'contract-week:%'
         AND SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract-week:') + 1) ~* v_uuid_re
          THEN SUBSTRING(normalised_items.supplied_row_key FROM LENGTH('contract-week:') + 1)::uuid
        ELSE NULL::uuid
      END AS requested_contract_week_id,
      normalised_items.item_json
    FROM normalised_items
  ),
  decision_rows AS (
    SELECT
      resolved_items.input_ordinal,
      resolved_items.supplied_row_key,
      resolved_items.supplied_row_signature,
      resolved_items.requested_timesheet_id,
      resolved_items.requested_contract_week_id,
      decision_result.row_json AS row_json
    FROM resolved_items
    LEFT JOIN LATERAL (
      SELECT decision_call.row_json
      FROM public.bulk_timesheet_row_decision_v1(
        CASE
          WHEN resolved_items.requested_timesheet_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'timesheet_id', resolved_items.requested_timesheet_id::text
          )
          WHEN resolved_items.requested_contract_week_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'contract_week_id', resolved_items.requested_contract_week_id::text
          )
          WHEN resolved_items.supplied_row_key IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'row_key', resolved_items.supplied_row_key
          )
          ELSE JSONB_BUILD_OBJECT(
            'dataset_mode', 'authorise',
            'row_key', '__NO_VALID_ROW_KEY__'
          )
        END
      ) AS decision_call(row_json)
      LIMIT 1
    ) AS decision_result ON TRUE
  ),
  evaluated_rows AS (
    SELECT
      decision_rows.input_ordinal,
      decision_rows.supplied_row_key,
      decision_rows.supplied_row_signature,
      decision_rows.requested_timesheet_id,
      decision_rows.requested_contract_week_id,
      decision_rows.row_json,
      NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_key', '')), '') AS decision_row_key,
      NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_signature', '')), '') AS decision_row_signature,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'current_timesheet_id', decision_rows.row_json->>'timesheet_id', '')), '') ~* v_uuid_re
          THEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'current_timesheet_id', decision_rows.row_json->>'timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS decision_timesheet_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'contract_week_id', '')), '') ~* v_uuid_re
          THEN NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'contract_week_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS decision_contract_week_id,
      UPPER(NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'bulk_authorise_classification', decision_rows.row_json->'bulk_authorise'->>'classification', '')), '')) AS decision_classification,
      LOWER(NULLIF(BTRIM(COALESCE(
        decision_rows.row_json->>'bulk_authorise_section',
        decision_rows.row_json->'bulk_authorise'->>'section',
        CASE
          WHEN LOWER(COALESCE(decision_rows.row_json->>'can_bulk_authorise', 'false')) IN ('true', 't', '1', 'yes') THEN 'processed_eligible'
          WHEN LOWER(COALESCE(decision_rows.row_json->>'can_bulk_unauthorise', 'false')) IN ('true', 't', '1', 'yes') THEN 'authorised_eligible'
          ELSE NULL::text
        END,
        ''
      )), '')) AS decision_section,
      (
        LOWER(COALESCE(decision_rows.row_json->>'is_import_authoritative', 'false')) IN ('true', 't', '1', 'yes')
        OR LOWER(COALESCE(decision_rows.row_json->'action_flags'->>'is_import_authoritative', 'false')) IN ('true', 't', '1', 'yes')
        OR UPPER(COALESCE(decision_rows.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE'
      ) AS is_import_authoritative,
      (
        NULLIF(BTRIM(COALESCE(decision_rows.supplied_row_signature, '')), '') IS NOT NULL
        AND COALESCE(NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_signature', '')), ''), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(decision_rows.supplied_row_signature, '')), '')
      ) AS is_stale
    FROM decision_rows
  ),
  accepted_candidates AS (
    SELECT
      evaluated_rows.input_ordinal,
      evaluated_rows.decision_row_key,
      evaluated_rows.decision_row_signature,
      evaluated_rows.decision_timesheet_id,
      evaluated_rows.decision_contract_week_id,
      evaluated_rows.row_json,
      NULLIF(BTRIM(COALESCE(evaluated_rows.row_json->>'candidate_name', evaluated_rows.row_json->>'candidate_display_name', '')), '') AS candidate_name,
      NULLIF(BTRIM(COALESCE(evaluated_rows.row_json->>'client_name', evaluated_rows.row_json->>'client_display_name', '')), '') AS client_name,
      NULLIF(BTRIM(COALESCE(evaluated_rows.row_json->>'week_ending_date', evaluated_rows.row_json->>'contract_week_ending_date', '')), '') AS week_ending_date_text
    FROM evaluated_rows
    WHERE evaluated_rows.row_json IS NOT NULL
      AND evaluated_rows.is_stale IS NOT TRUE
      AND evaluated_rows.is_import_authoritative IS TRUE
      AND evaluated_rows.decision_timesheet_id IS NOT NULL
      AND evaluated_rows.decision_classification = v_classification
      AND evaluated_rows.decision_section = v_section
  ),
  accepted_scope AS (
    SELECT DISTINCT ON (accepted_candidates.decision_timesheet_id)
      accepted_candidates.input_ordinal,
      accepted_candidates.decision_row_key,
      accepted_candidates.decision_row_signature,
      accepted_candidates.decision_timesheet_id,
      accepted_candidates.decision_contract_week_id,
      accepted_candidates.row_json,
      accepted_candidates.candidate_name,
      accepted_candidates.client_name,
      accepted_candidates.week_ending_date_text
    FROM accepted_candidates
    ORDER BY accepted_candidates.decision_timesheet_id, accepted_candidates.input_ordinal
  ),
  stale_items AS (
    SELECT
      evaluated_rows.input_ordinal,
      JSONB_BUILD_OBJECT(
        'code', 'ROW_SIGNATURE_MISMATCH',
        'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
        'timesheet_id', COALESCE(evaluated_rows.decision_timesheet_id, evaluated_rows.requested_timesheet_id),
        'contract_week_id', COALESCE(evaluated_rows.decision_contract_week_id, evaluated_rows.requested_contract_week_id),
        'expected_row_signature', evaluated_rows.supplied_row_signature,
        'current_row_signature', evaluated_rows.decision_row_signature
      ) AS item_json
    FROM evaluated_rows
    WHERE evaluated_rows.is_stale IS TRUE
  ),
  warning_items AS (
    SELECT
      evaluated_rows.input_ordinal,
      CASE
        WHEN evaluated_rows.row_json IS NULL THEN JSONB_BUILD_OBJECT(
          'code', 'ROW_NOT_FOUND',
          'row_key', evaluated_rows.supplied_row_key,
          'timesheet_id', evaluated_rows.requested_timesheet_id,
          'contract_week_id', evaluated_rows.requested_contract_week_id
        )
        WHEN evaluated_rows.is_stale IS TRUE THEN JSONB_BUILD_OBJECT(
          'code', 'ROW_SIGNATURE_MISMATCH',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', COALESCE(evaluated_rows.decision_timesheet_id, evaluated_rows.requested_timesheet_id),
          'contract_week_id', COALESCE(evaluated_rows.decision_contract_week_id, evaluated_rows.requested_contract_week_id),
          'expected_row_signature', evaluated_rows.supplied_row_signature,
          'current_row_signature', evaluated_rows.decision_row_signature
        )
        WHEN evaluated_rows.is_import_authoritative IS NOT TRUE THEN JSONB_BUILD_OBJECT(
          'code', 'NOT_IMPORT_AUTHORITATIVE',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', evaluated_rows.decision_timesheet_id,
          'contract_week_id', evaluated_rows.decision_contract_week_id
        )
        WHEN evaluated_rows.decision_classification IS DISTINCT FROM v_classification THEN JSONB_BUILD_OBJECT(
          'code', 'CLASSIFICATION_MISMATCH',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', evaluated_rows.decision_timesheet_id,
          'contract_week_id', evaluated_rows.decision_contract_week_id,
          'expected_classification', v_classification,
          'actual_classification', evaluated_rows.decision_classification
        )
        WHEN evaluated_rows.decision_section IS DISTINCT FROM v_section THEN JSONB_BUILD_OBJECT(
          'code', 'SECTION_MISMATCH',
          'row_key', COALESCE(evaluated_rows.supplied_row_key, evaluated_rows.decision_row_key),
          'timesheet_id', evaluated_rows.decision_timesheet_id,
          'contract_week_id', evaluated_rows.decision_contract_week_id,
          'expected_section', v_section,
          'actual_section', evaluated_rows.decision_section
        )
        ELSE NULL::jsonb
      END AS warning_json
    FROM evaluated_rows
  ),
  source_imports AS (
    SELECT
      accepted_scope.input_ordinal,
      accepted_scope.decision_row_key,
      accepted_scope.decision_row_signature,
      accepted_scope.decision_timesheet_id,
      accepted_scope.decision_contract_week_id,
      accepted_scope.candidate_name,
      accepted_scope.client_name,
      accepted_scope.week_ending_date_text,
      import_rows.source_system,
      import_rows.import_id,
      import_rows.filename,
      import_rows.uploaded_at_utc,
      import_rows.file_r2_key,
      COALESCE(import_rows.header_rows, '[]'::jsonb) AS header_rows,
      COALESCE(import_rows.header_columns, '[]'::jsonb) AS header_columns,
      COALESCE(import_rows.rows, '[]'::jsonb) AS rows_json
    FROM accepted_scope
    CROSS JOIN LATERAL public.timesheet_import_rows_for_timesheet_current(
      accepted_scope.decision_timesheet_id,
      TRUE,
      NULL::uuid,
      NULL::uuid
    ) AS import_rows(
      requested_timesheet_id,
      current_timesheet_id,
      source_system,
      import_id,
      filename,
      uploaded_at_utc,
      file_r2_key,
      header_rows,
      header_columns,
      rows
    )
  ),
  import_row_elements AS (
    SELECT
      source_imports.input_ordinal,
      source_imports.decision_row_key,
      source_imports.decision_row_signature,
      source_imports.decision_timesheet_id,
      source_imports.decision_contract_week_id,
      source_imports.candidate_name,
      source_imports.client_name,
      source_imports.week_ending_date_text,
      source_imports.source_system,
      source_imports.import_id,
      source_imports.filename,
      source_imports.uploaded_at_utc,
      source_imports.file_r2_key,
      source_imports.header_rows,
      source_imports.header_columns,
      import_row_item.row_value AS row_value,
      (import_row_item.row_ordinality - 1)::integer AS raw_row_index
    FROM source_imports
    CROSS JOIN LATERAL jsonb_array_elements(source_imports.rows_json) WITH ORDINALITY AS import_row_item(row_value, row_ordinality)
  ),
  flattened_items AS (
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          import_row_elements.input_ordinal,
          import_row_elements.uploaded_at_utc NULLS LAST,
          import_row_elements.import_id,
          import_row_elements.raw_row_index
      ) AS result_ordinal,
      JSONB_BUILD_OBJECT(
        'id',
          CONCAT_WS('|',
            import_row_elements.decision_row_key,
            COALESCE(import_row_elements.import_id::text, ''),
            COALESCE(import_row_elements.row_value->'payload'->>'external_row_key', import_row_elements.row_value->'payload'->>'row_key', import_row_elements.row_value->'payload'->>'external_key', ''),
            COALESCE(import_row_elements.row_value->'payload'->>'source_row_key', import_row_elements.row_value->'payload'->>'stable_source_key', import_row_elements.raw_row_index::text)
          ),
        'timesheet_id', import_row_elements.decision_timesheet_id,
        'row_key', import_row_elements.decision_row_key,
        'row_signature', import_row_elements.decision_row_signature,
        'contract_week_id', import_row_elements.decision_contract_week_id,
        'candidate_name', import_row_elements.candidate_name,
        'client_name', import_row_elements.client_name,
        'week_ending_date', import_row_elements.week_ending_date_text,
        'source_system', import_row_elements.source_system,
        'import_id', import_row_elements.import_id,
        'filename', import_row_elements.filename,
        'uploaded_at_utc', import_row_elements.uploaded_at_utc,
        'file_r2_key', import_row_elements.file_r2_key,
        'header_rows', import_row_elements.header_rows,
        'header_columns', import_row_elements.header_columns,
        'raw_row_index', import_row_elements.raw_row_index,
        'raw_columns', import_row_elements.row_value->'raw_columns',
        'raw_row', COALESCE(import_row_elements.row_value->'payload', '{}'::jsonb),
        'external_row_key', NULLIF(BTRIM(COALESCE(import_row_elements.row_value->'payload'->>'external_row_key', import_row_elements.row_value->'payload'->>'row_key', import_row_elements.row_value->'payload'->>'external_key', '')), ''),
        'source_row_key', NULLIF(BTRIM(COALESCE(import_row_elements.row_value->'payload'->>'source_row_key', import_row_elements.row_value->'payload'->>'stable_source_key', import_row_elements.row_value->'payload'->>'source_key', '')), ''),
        'nhsp_shift_id', COALESCE(import_row_elements.row_value->'payload'->'nhsp_shift_id', import_row_elements.row_value->'payload'->'shift_id'),
        'hr_request_id', COALESCE(import_row_elements.row_value->'payload'->'hr_request_id', import_row_elements.row_value->'payload'->'request_id'),
        'hr_shift_id', import_row_elements.row_value->'payload'->'hr_shift_id'
      ) AS item_json
    FROM import_row_elements
  ),
  totals AS (
    SELECT COUNT(*)::integer AS total_count
    FROM flattened_items
  ),
  page_meta AS (
    SELECT
      totals.total_count,
      CASE
        WHEN v_page_size IS NULL THEN 1
        WHEN totals.total_count = 0 THEN 1
        ELSE LEAST(v_page, GREATEST(CEIL(totals.total_count::numeric / v_page_size::numeric)::integer, 1))
      END AS effective_page,
      CASE
        WHEN v_page_size IS NULL THEN totals.total_count
        ELSE v_page_size
      END AS effective_page_size,
      CASE
        WHEN v_page_size IS NULL THEN CASE WHEN totals.total_count > 0 THEN 1 ELSE 0 END
        WHEN totals.total_count = 0 THEN 0
        ELSE CEIL(totals.total_count::numeric / v_page_size::numeric)::integer
      END AS total_pages
    FROM totals
  ),
  page_items AS (
    SELECT flattened_items.item_json
    FROM flattened_items
    CROSS JOIN page_meta
    WHERE v_page_size IS NULL
       OR (
         flattened_items.result_ordinal > ((page_meta.effective_page - 1) * page_meta.effective_page_size)
         AND flattened_items.result_ordinal <= (page_meta.effective_page * page_meta.effective_page_size)
       )
    ORDER BY flattened_items.result_ordinal
  ),
  accepted_json AS (
    SELECT
      COALESCE(JSONB_AGG(accepted_scope.decision_row_key ORDER BY accepted_scope.input_ordinal), '[]'::jsonb) AS accepted_row_keys,
      COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT(
        'row_key', accepted_scope.decision_row_key,
        'row_signature', accepted_scope.decision_row_signature,
        'timesheet_id', accepted_scope.decision_timesheet_id,
        'contract_week_id', accepted_scope.decision_contract_week_id,
        'candidate_name', accepted_scope.candidate_name,
        'client_name', accepted_scope.client_name,
        'week_ending_date', accepted_scope.week_ending_date_text
      ) ORDER BY accepted_scope.input_ordinal), '[]'::jsonb) AS accepted_scope_json
    FROM accepted_scope
  ),
  stale_json AS (
    SELECT COALESCE(JSONB_AGG(stale_items.item_json ORDER BY stale_items.input_ordinal), '[]'::jsonb) AS stale_items_json
    FROM stale_items
  ),
  warnings_json AS (
    SELECT COALESCE(JSONB_AGG(warning_items.warning_json ORDER BY warning_items.input_ordinal), '[]'::jsonb) AS warnings_json
    FROM warning_items
    WHERE warning_items.warning_json IS NOT NULL
  ),
  imports_json AS (
    SELECT COALESCE(JSONB_AGG(import_summary.import_json ORDER BY import_summary.source_system, import_summary.uploaded_at_utc NULLS LAST, import_summary.import_id), '[]'::jsonb) AS imports_array
    FROM (
      SELECT DISTINCT ON (source_imports.decision_timesheet_id, source_imports.import_id, source_imports.file_r2_key)
        source_imports.source_system,
        source_imports.uploaded_at_utc,
        source_imports.import_id,
        JSONB_BUILD_OBJECT(
          'timesheet_id', source_imports.decision_timesheet_id,
          'row_key', source_imports.decision_row_key,
          'contract_week_id', source_imports.decision_contract_week_id,
          'source_system', source_imports.source_system,
          'import_id', source_imports.import_id,
          'filename', source_imports.filename,
          'uploaded_at_utc', source_imports.uploaded_at_utc,
          'file_r2_key', source_imports.file_r2_key,
          'header_rows', source_imports.header_rows,
          'header_columns', source_imports.header_columns
        ) AS import_json
      FROM source_imports
      ORDER BY source_imports.decision_timesheet_id, source_imports.import_id, source_imports.file_r2_key, source_imports.uploaded_at_utc NULLS LAST
    ) AS import_summary
  ),
  headers_json AS (
    SELECT
      COALESCE((
        SELECT source_imports.header_rows
        FROM source_imports
        WHERE jsonb_typeof(source_imports.header_rows) = 'array'
          AND jsonb_array_length(source_imports.header_rows) > 0
        ORDER BY source_imports.input_ordinal, source_imports.uploaded_at_utc NULLS LAST, source_imports.import_id
        LIMIT 1
      ), '[]'::jsonb) AS header_rows,
      COALESCE((
        SELECT source_imports.header_columns
        FROM source_imports
        WHERE jsonb_typeof(source_imports.header_columns) = 'array'
          AND jsonb_array_length(source_imports.header_columns) > 0
        ORDER BY source_imports.input_ordinal, source_imports.uploaded_at_utc NULLS LAST, source_imports.import_id
        LIMIT 1
      ), '[]'::jsonb) AS header_columns
  ),
  fallback_columns AS (
    SELECT COALESCE(JSONB_AGG(DISTINCT payload_keys.payload_key), '[]'::jsonb) AS column_keys
    FROM import_row_elements
    CROSS JOIN LATERAL jsonb_object_keys(COALESCE(import_row_elements.row_value->'payload', '{}'::jsonb)) AS payload_keys(payload_key)
  ),
  mapping_json AS (
    SELECT COALESCE(JSONB_AGG(mapping_rows.mapping_json ORDER BY mapping_rows.result_ordinal), '[]'::jsonb) AS mapping_array
    FROM (
      SELECT
        flattened_items.result_ordinal,
        JSONB_BUILD_OBJECT(
          'row_id', flattened_items.item_json->>'id',
          'timesheet_id', flattened_items.item_json->>'timesheet_id',
          'row_key', flattened_items.item_json->>'row_key',
          'contract_week_id', flattened_items.item_json->>'contract_week_id'
        ) AS mapping_json
      FROM flattened_items
    ) AS mapping_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'success', TRUE,
    'source_system', (
      SELECT source_imports.source_system
      FROM source_imports
      WHERE NULLIF(BTRIM(COALESCE(source_imports.source_system, '')), '') IS NOT NULL
      ORDER BY source_imports.input_ordinal, source_imports.uploaded_at_utc NULLS LAST, source_imports.import_id
      LIMIT 1
    ),
    'classification', v_classification,
    'mode', v_mode,
    'selected_section', v_section,
    'page', page_meta.effective_page,
    'page_size', page_meta.effective_page_size,
    'total', page_meta.total_count,
    'total_rows', page_meta.total_count,
    'total_pages', page_meta.total_pages,
    'items', COALESCE((SELECT JSONB_AGG(page_items.item_json ORDER BY page_items.item_json->>'id') FROM page_items), '[]'::jsonb),
    'rows', COALESCE((SELECT JSONB_AGG(page_items.item_json ORDER BY page_items.item_json->>'id') FROM page_items), '[]'::jsonb),
    'header_rows', headers_json.header_rows,
    'header_columns', headers_json.header_columns,
    'display_columns', CASE
      WHEN jsonb_typeof(headers_json.header_columns) = 'array' AND jsonb_array_length(headers_json.header_columns) > 0 THEN headers_json.header_columns
      ELSE fallback_columns.column_keys
    END,
    'imports', imports_json.imports_array,
    'row_to_timesheet_mapping', mapping_json.mapping_array,
    'stale_items', stale_json.stale_items_json,
    'accepted_row_keys', accepted_json.accepted_row_keys,
    'accepted_scope', accepted_json.accepted_scope_json,
    'warnings', warnings_json.warnings_json,
    'actor_user_id', p_actor_user_id
  )
    INTO v_result
  FROM page_meta
  CROSS JOIN headers_json
  CROSS JOIN fallback_columns
  CROSS JOIN imports_json
  CROSS JOIN mapping_json
  CROSS JOIN stale_json
  CROSS JOIN accepted_json
  CROSS JOIN warnings_json;

  RETURN COALESCE(v_result, JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'success', TRUE,
    'classification', v_classification,
    'mode', v_mode,
    'selected_section', v_section,
    'page', 1,
    'page_size', COALESCE(v_page_size, 0),
    'total', 0,
    'total_rows', 0,
    'total_pages', 0,
    'items', '[]'::jsonb,
    'rows', '[]'::jsonb,
    'header_rows', '[]'::jsonb,
    'header_columns', '[]'::jsonb,
    'display_columns', '[]'::jsonb,
    'imports', '[]'::jsonb,
    'row_to_timesheet_mapping', '[]'::jsonb,
    'stale_items', '[]'::jsonb,
    'accepted_row_keys', '[]'::jsonb,
    'accepted_scope', '[]'::jsonb,
    'warnings', '[]'::jsonb,
    'actor_user_id', p_actor_user_id
  ));
END;
$function$;

CREATE OR REPLACE FUNCTION public.bulk_timesheet_workbench_row_source_v1(p_filters jsonb DEFAULT '{}'::jsonb)
RETURNS TABLE(
  timesheet_id uuid,
  timesheet_status public.timesheet_status_enum,
  week_ending_date date,
  booking_id text,
  occupant_key_norm text,
  hospital_norm text,
  sheet_scope public.timesheet_scope_enum,
  submission_mode public.submission_mode_enum,
  authorised_at_server timestamp with time zone,
  candidate_id uuid,
  client_id uuid,
  pay_method text,
  processing_status public.ts_fin_processing_status_enum,
  basis public.timesheet_fin_basis_enum,
  total_hours numeric,
  total_pay_ex_vat numeric,
  total_charge_ex_vat numeric,
  margin_ex_vat numeric,
  paid_at_utc timestamp with time zone,
  pay_on_hold boolean,
  ready_to_pay boolean,
  locked_by_invoice_id uuid,
  candidate_name text,
  client_name text,
  nhsp_shift_count integer,
  nhsp_shift_included_count integer,
  nhsp_shift_deferred_count integer,
  validation_status public.validation_status_enum,
  summary_stage text,
  route_type text,
  contract_week_id uuid,
  contract_week_ending_date date,
  contract_week_status public.contract_week_status_enum,
  additional_seq integer,
  is_adjustment boolean,
  qr_status public.timesheet_qr_status_enum,
  pay_adjustment_count integer,
  has_pay_adjustments boolean,
  is_adjusted boolean,
  is_qr boolean,
  needs_attention boolean,
  client_autoprocess_hr boolean,
  has_rate_issue boolean,
  has_pay_channel_issue boolean,
  hr_crosscheck_status text,
  hr_crosscheck_issues text[],
  external_source_rows_json jsonb,
  issue_codes text[],
  client_requires_hr boolean,
  client_no_timesheet_required boolean,
  client_is_nhsp boolean,
  client_pay_reference_required boolean,
  client_invoice_reference_required boolean,
  client_hr_validation_required boolean,
  client_ts_reference_required boolean,
  require_reference_to_pay boolean,
  require_reference_to_invoice boolean,
  qr_token text,
  qr_generated_at timestamp with time zone,
  qr_scanned_at timestamp with time zone,
  candidate_hint_text jsonb,
  expenses_pay_ex_vat numeric,
  expenses_description text,
  mileage_units numeric,
  mileage_pay_rate numeric,
  mileage_charge_rate numeric,
  mileage_pay_ex_vat numeric,
  travel_pay_ex_vat numeric,
  travel_charge_ex_vat numeric,
  accommodation_pay_ex_vat numeric,
  accommodation_charge_ex_vat numeric,
  other_pay_ex_vat numeric,
  other_charge_ex_vat numeric,
  hr_validation_required_for_invoice boolean,
  invoice_segments_total integer,
  invoice_segments_locked integer,
  invoice_segments_unlocked integer,
  invoice_segment_stage text,
  tools_stage text,
  processing_status_display text,
  invoice_is_paid boolean,
  refs_block_invoicing boolean,
  refs_block_issuing_invoices boolean,
  refs_block_invoice_and_issuing boolean,
  pay_icon_code text,
  pay_status_code text,
  pay_paid_at_utc timestamp with time zone,
  net_delta_ex_vat numeric
)
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_row_key_timesheet_ids uuid[] := NULL;
  v_row_key_contract_week_ids uuid[] := NULL;
  v_has_contract_week_row_key boolean := FALSE;
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_numeric_re text := E'^[-+]?[0-9]+(\\.[0-9]+)?$';
BEGIN
  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  BEGIN
    IF v_candidate_id_text IS NOT NULL THEN
      v_candidate_id := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id := NULL;
  END;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  BEGIN
    IF v_client_id_text IS NOT NULL THEN
      v_client_id := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id := NULL;
  END;

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  ELSIF v_filters ? 'timesheetId' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheetId', '')), '') IS NOT NULL AND (v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  ELSIF v_filters ? 'contractWeekId' AND NULLIF(BTRIM(COALESCE(v_filters->>'contractWeekId', '')), '') IS NOT NULL AND (v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  ELSIF v_filters ? 'rowKey' AND NULLIF(BTRIM(COALESCE(v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'rowKey'), '')];
  END IF;


  IF v_row_keys IS NOT NULL THEN
    SELECT ARRAY_AGG(parsed_values.uuid_value)
      INTO v_row_key_timesheet_ids
    FROM (
      SELECT DISTINCT SUBSTRING(row_key_values.row_key_value FROM 11)::uuid AS uuid_value
      FROM UNNEST(v_row_keys) AS row_key_values(row_key_value)
      WHERE LOWER(LEFT(row_key_values.row_key_value, 10)) = 'timesheet:'
        AND SUBSTRING(row_key_values.row_key_value FROM 11) ~* v_uuid_re
    ) AS parsed_values;

    SELECT ARRAY_AGG(parsed_values.uuid_value)
      INTO v_row_key_contract_week_ids
    FROM (
      SELECT DISTINCT SUBSTRING(row_key_values.row_key_value FROM 15)::uuid AS uuid_value
      FROM UNNEST(v_row_keys) AS row_key_values(row_key_value)
      WHERE LOWER(LEFT(row_key_values.row_key_value, 14)) = 'contract_week:'
        AND SUBSTRING(row_key_values.row_key_value FROM 15) ~* v_uuid_re
    ) AS parsed_values;

    v_has_contract_week_row_key := COALESCE(ARRAY_LENGTH(v_row_key_contract_week_ids, 1), 0) > 0;

    IF v_row_key_timesheet_ids IS NOT NULL THEN
      SELECT ARRAY_AGG(merged_ids.uuid_value)
        INTO v_timesheet_ids
      FROM (
        SELECT DISTINCT existing_ids.uuid_value
        FROM UNNEST(COALESCE(v_timesheet_ids, ARRAY[]::uuid[])) AS existing_ids(uuid_value)
        UNION
        SELECT DISTINCT row_key_ids.uuid_value
        FROM UNNEST(v_row_key_timesheet_ids) AS row_key_ids(uuid_value)
      ) AS merged_ids;
    END IF;

    IF v_row_key_contract_week_ids IS NOT NULL THEN
      SELECT ARRAY_AGG(merged_ids.uuid_value)
        INTO v_contract_week_ids
      FROM (
        SELECT DISTINCT existing_ids.uuid_value
        FROM UNNEST(COALESCE(v_contract_week_ids, ARRAY[]::uuid[])) AS existing_ids(uuid_value)
        UNION
        SELECT DISTINCT row_key_ids.uuid_value
        FROM UNNEST(v_row_key_contract_week_ids) AS row_key_ids(uuid_value)
      ) AS merged_ids;
    END IF;
  END IF;

  v_dataset_mode := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  IF v_dataset_mode NOT IN ('PROCESS', 'AUTHORISE', 'ROW_CONTEXT') THEN
    v_dataset_mode := NULL;
  END IF;

  v_period_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'period_type', v_filters->>'periodType', v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_period_filter NOT IN ('DAILY', 'WEEKLY') THEN
    v_period_filter := NULL;
  END IF;

  v_date_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), '');
  v_date_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), '');
  v_week_ending_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), '');
  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_date_from_text IS NOT NULL THEN
      v_date_from := v_date_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_from := NULL;
  END;

  BEGIN
    IF v_date_to_text IS NOT NULL THEN
      v_date_to := v_date_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_to := NULL;
  END;

  BEGIN
    IF v_week_ending_text IS NOT NULL THEN
      v_week_ending_date := v_week_ending_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_date := NULL;
  END;

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

  RETURN QUERY
  WITH client_hr AS MATERIALIZED (
    SELECT
      cs0.client_id,
      BOOL_OR(cs0.autoprocess_hr) AS autoprocess_hr,
      BOOL_OR(cs0.requires_hr) AS requires_hr,
      BOOL_OR(cs0.no_timesheet_required) AS no_timesheet_required,
      BOOL_OR(cs0.pay_reference_required) AS pay_reference_required,
      BOOL_OR(cs0.invoice_reference_required) AS invoice_reference_required,
      BOOL_OR(cs0.reference_number_required_to_issue_invoice) AS reference_number_required_to_issue_invoice,
      BOOL_OR(cs0.hr_validation_required) AS hr_validation_required,
      BOOL_OR(cs0.ts_reference_required) AS ts_reference_required,
      BOOL_OR(cs0.is_nhsp) AS is_nhsp
    FROM public.client_settings AS cs0
    GROUP BY cs0.client_id
  ),
  timesheet_scope_rows AS MATERIALIZED (
    SELECT
      ts0.timesheet_id,
      cw0.id AS contract_week_id,
      COALESCE(cw0.week_ending_date, ts0.week_ending_date) AS effective_week_ending_date,
      ts0.sheet_scope AS effective_sheet_scope,
      COALESCE(ts0.contract_id, cw0.contract_id) AS effective_contract_id
    FROM public.timesheets AS ts0
    LEFT JOIN public.contract_weeks AS cw0
      ON cw0.timesheet_id = ts0.timesheet_id
    WHERE ts0.is_current = TRUE
      AND (v_timesheet_ids IS NULL OR ts0.timesheet_id = ANY(v_timesheet_ids))
      AND (
        v_contract_week_ids IS NULL
        OR cw0.id = ANY(v_contract_week_ids)
        OR v_timesheet_ids IS NOT NULL
        OR (v_row_key_timesheet_ids IS NOT NULL AND ts0.timesheet_id = ANY(v_row_key_timesheet_ids))
      )
      AND (
        v_row_keys IS NULL
        OR ('timesheet:' || ts0.timesheet_id::text) = ANY(v_row_keys)
        OR (v_row_key_timesheet_ids IS NOT NULL AND ts0.timesheet_id = ANY(v_row_key_timesheet_ids))
      )
      AND (
        v_week_ending_date IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) = v_week_ending_date
      )
      AND (
        v_week_ending_from IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR COALESCE(cw0.week_ending_date, ts0.week_ending_date) <= v_week_ending_to
      )
      AND (
        v_date_from IS NULL
        OR (
          CASE
            WHEN ts0.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN COALESCE(ts0.worked_start_iso::date, ts0.scheduled_start_iso::date, ts0.week_ending_date)
            ELSE COALESCE(cw0.week_ending_date, ts0.week_ending_date)
          END
        ) >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR (
          CASE
            WHEN ts0.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN COALESCE(ts0.worked_start_iso::date, ts0.scheduled_start_iso::date, ts0.week_ending_date)
            ELSE COALESCE(cw0.week_ending_date, ts0.week_ending_date)
          END
        ) <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR UPPER(ts0.sheet_scope::text) = v_period_filter
      )
  ),
  contract_week_scope_rows AS MATERIALIZED (
    SELECT
      cw0.id AS contract_week_id,
      cw0.contract_id,
      cw0.week_ending_date AS effective_week_ending_date
    FROM public.contract_weeks AS cw0
    JOIN public.contracts AS ct_scope
      ON ct_scope.id = cw0.contract_id
    WHERE cw0.timesheet_id IS NULL
      AND (
        v_timesheet_ids IS NULL
        OR v_contract_week_ids IS NOT NULL
        OR v_has_contract_week_row_key = TRUE
      )
      AND (
        COALESCE(v_dataset_mode, 'PROCESS') <> 'AUTHORISE'
        OR v_has_contract_week_row_key = TRUE
      )
      AND (v_candidate_id IS NULL OR ct_scope.candidate_id = v_candidate_id)
      AND (v_client_id IS NULL OR ct_scope.client_id = v_client_id)
      AND (v_contract_week_ids IS NULL OR cw0.id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR ('contract_week:' || cw0.id::text) = ANY(v_row_keys)
      )
      AND (
        v_week_ending_date IS NULL
        OR cw0.week_ending_date = v_week_ending_date
      )
      AND (
        v_week_ending_from IS NULL
        OR cw0.week_ending_date >= v_week_ending_from
      )
      AND (
        v_week_ending_to IS NULL
        OR cw0.week_ending_date <= v_week_ending_to
      )
      AND (
        v_date_from IS NULL
        OR cw0.week_ending_date >= v_date_from
      )
      AND (
        v_date_to IS NULL
        OR cw0.week_ending_date <= v_date_to
      )
      AND (
        v_period_filter IS NULL
        OR v_period_filter = 'WEEKLY'
      )
  ),
  tf_ranked AS MATERIALIZED (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.candidate_id,
      tf0.client_id,
      tf0.pay_method,
      tf0.processing_status,
      tf0.basis,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.locked_by_invoice_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.invoice_breakdown_json,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_description,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.created_at,
      tf0.updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_tf
        WHERE ts_scope_tf.timesheet_id = tf0.timesheet_id
      )
  ),
  tf_latest AS MATERIALIZED (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.candidate_id,
      tf1.client_id,
      tf1.pay_method,
      tf1.processing_status,
      tf1.basis,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.locked_by_invoice_id,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.external_source_rows_json,
      tf1.invoice_breakdown_json,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_description,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS MATERIALIZED (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.created_at,
      tv0.updated_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_tv
        WHERE ts_scope_tv.timesheet_id = tv0.timesheet_id
      )
  ),
  tv_latest AS MATERIALIZED (
    SELECT
      tv1.timesheet_id,
      tv1.status
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  evidence_agg AS MATERIALIZED (
    SELECT
      te0.timesheet_id,
      COUNT(te0.id)::integer AS evidence_count,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET'), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'MILEAGE'), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'TRAVEL'), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te0.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_te
        WHERE ts_scope_te.timesheet_id = te0.timesheet_id
      )
    GROUP BY te0.timesheet_id
  ),
  nhsp_agg AS MATERIALIZED (
    SELECT
      ns0.timesheet_id,
      COUNT(ns0.id)::integer AS nhsp_shift_count,
      (COUNT(ns0.id) FILTER (WHERE ns0.invoice_status = 'INCLUDED'))::integer AS nhsp_shift_included_count,
      (COUNT(ns0.id) FILTER (WHERE ns0.invoice_status = 'DEFERRED'))::integer AS nhsp_shift_deferred_count
    FROM public.nhsp_shifts AS ns0
    WHERE ns0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_ns
        WHERE ts_scope_ns.timesheet_id = ns0.timesheet_id
      )
    GROUP BY ns0.timesheet_id
  ),
  pay_adjustments_agg AS MATERIALIZED (
    SELECT
      pa0.timesheet_id,
      COUNT(pa0.id)::integer AS pay_adjustment_count
    FROM public.ts_pay_adjustments AS pa0
    WHERE pa0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM timesheet_scope_rows AS ts_scope_pa
        WHERE ts_scope_pa.timesheet_id = pa0.timesheet_id
      )
    GROUP BY pa0.timesheet_id
  ),
  timesheet_rows AS MATERIALIZED (
    SELECT
      ts0.timesheet_id AS timesheet_id,
      ts0.status AS timesheet_status,
      ts0.week_ending_date AS week_ending_date,
      ts0.booking_id AS booking_id,
      ts0.occupant_key_norm AS occupant_key_norm,
      ts0.hospital_norm AS hospital_norm,
      ts0.sheet_scope AS sheet_scope,
      ts0.submission_mode AS submission_mode,
      ts0.authorised_at_server AS authorised_at_server,
      COALESCE(tf2.candidate_id, ct0.candidate_id) AS candidate_id,
      COALESCE(tf2.client_id, ct0.client_id) AS client_id,
      tf2.pay_method AS pay_method,
      tf2.processing_status AS processing_status,
      tf2.basis AS basis,
      tf2.total_hours AS total_hours,
      tf2.total_pay_ex_vat AS total_pay_ex_vat,
      tf2.total_charge_ex_vat AS total_charge_ex_vat,
      tf2.margin_ex_vat AS margin_ex_vat,
      tf2.paid_at_utc AS paid_at_utc,
      tf2.pay_on_hold AS pay_on_hold,
      tf2.locked_by_invoice_id AS locked_by_invoice_id,
      CASE
        WHEN COALESCE(tf2.candidate_id, ct0.candidate_id) IS NULL
         AND ts0.candidate_hint_text IS NOT NULL
         AND jsonb_typeof(ts0.candidate_hint_text) = 'object'
         AND (
           NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(ts0.candidate_hint_text->>'first_name'), ''), NULLIF(BTRIM(ts0.candidate_hint_text->>'surname'), ''))), '') IS NOT NULL
           OR NULLIF(BTRIM(ts0.candidate_hint_text->>'display_name'), '') IS NOT NULL
           OR NULLIF(BTRIM(ts0.candidate_hint_text->>'email'), '') IS NOT NULL
         ) THEN
          'Unresolved Timesheet - '
          || COALESCE(
            NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(ts0.candidate_hint_text->>'first_name'), ''), NULLIF(BTRIM(ts0.candidate_hint_text->>'surname'), ''))), ''),
            NULLIF(BTRIM(ts0.candidate_hint_text->>'display_name'), ''),
            'Candidate'
          )
          || CASE
               WHEN NULLIF(BTRIM(ts0.candidate_hint_text->>'email'), '') IS NOT NULL THEN ', Email - ' || BTRIM(ts0.candidate_hint_text->>'email')
               ELSE ''
             END
        ELSE COALESCE(cand0.display_name, ts0.occupant_key_norm)
      END AS candidate_name,
      cli0.name AS client_name,
      COALESCE(ns1.nhsp_shift_count, 0) AS nhsp_shift_count,
      COALESCE(ns1.nhsp_shift_included_count, 0) AS nhsp_shift_included_count,
      COALESCE(ns1.nhsp_shift_deferred_count, 0) AS nhsp_shift_deferred_count,
      tv2.status AS validation_status,
      cw0.id AS contract_week_id,
      cw0.week_ending_date AS contract_week_ending_date,
      cw0.status AS contract_week_status,
      cw0.additional_seq AS additional_seq,
      COALESCE(ts0.is_adjustment, cw0.is_adjustment, FALSE) AS is_adjustment,
      ts0.qr_status AS qr_status,
      ts0.qr_token AS qr_token,
      ts0.qr_generated_at AS qr_generated_at,
      ts0.qr_scanned_at AS qr_scanned_at,
      ts0.qr_last_sent_hash AS qr_last_sent_hash,
      COALESCE(pa1.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.autoprocess_hr ELSE NULL::boolean END,
        ch0.autoprocess_hr,
        FALSE
      ) AS client_autoprocess_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.requires_hr,
        FALSE
      ) AS client_requires_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.no_timesheet_required ELSE NULL::boolean END,
        ch0.no_timesheet_required,
        FALSE
      ) AS client_no_timesheet_required,
      COALESCE(ch0.pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(ch0.invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(ch0.hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(ch0.ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.is_nhsp ELSE NULL::boolean END,
        ch0.is_nhsp,
        FALSE
      ) AS client_is_nhsp,
      COALESCE(tf2.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf2.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      tf2.hr_crosscheck_status AS hr_crosscheck_status,
      tf2.hr_crosscheck_issues AS hr_crosscheck_issues,
      tf2.external_source_rows_json AS external_source_rows_json,
      ts0.reference_number AS reference_number,
      ts0.day_references_json AS day_references_json,
      ts0.actual_schedule_json AS actual_schedule_json,
      ts0.r2_nurse_key AS r2_nurse_key,
      ts0.r2_auth_key AS r2_auth_key,
      ts0.manual_pdf_r2_key AS manual_pdf_r2_key,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_pay ELSE NULL::boolean END,
        ch0.pay_reference_required,
        FALSE
      ) AS require_reference_to_pay,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_invoice ELSE NULL::boolean END,
        ch0.invoice_reference_required,
        FALSE
      ) AS require_reference_to_invoice,
      COALESCE(ev1.evidence_count, 0) AS evidence_count,
      COALESCE(ev1.has_timesheet_evidence, FALSE) AS has_timesheet_evidence,
      COALESCE(ev1.has_mileage_evidence, FALSE) AS has_mileage_evidence,
      COALESCE(ev1.has_travel_evidence, FALSE) AS has_travel_evidence,
      COALESCE(ev1.has_accommodation_evidence, FALSE) AS has_accommodation_evidence,
      COALESCE(ev1.has_other_evidence, FALSE) AS has_other_evidence,
      ts0.candidate_hint_text AS candidate_hint_text,
      tf2.expenses_pay_ex_vat AS expenses_pay_ex_vat,
      tf2.expenses_description AS expenses_description,
      tf2.mileage_units AS mileage_units,
      tf2.mileage_pay_rate AS mileage_pay_rate,
      tf2.mileage_charge_rate AS mileage_charge_rate,
      tf2.mileage_pay_ex_vat AS mileage_pay_ex_vat,
      tf2.mileage_charge_ex_vat AS mileage_charge_ex_vat,
      tf2.travel_pay_ex_vat AS travel_pay_ex_vat,
      tf2.travel_charge_ex_vat AS travel_charge_ex_vat,
      tf2.accommodation_pay_ex_vat AS accommodation_pay_ex_vat,
      tf2.accommodation_charge_ex_vat AS accommodation_charge_ex_vat,
      tf2.other_pay_ex_vat AS other_pay_ex_vat,
      tf2.other_charge_ex_vat AS other_charge_ex_vat,
      tf2.invoice_breakdown_json AS invoice_breakdown_json
    FROM timesheet_scope_rows AS ts_scope
    JOIN public.timesheets AS ts0
      ON ts0.timesheet_id = ts_scope.timesheet_id
     AND ts0.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw0
      ON cw0.id = ts_scope.contract_week_id
    LEFT JOIN public.contracts AS ct0
      ON ct0.id = COALESCE(ts0.contract_id, cw0.contract_id)
    LEFT JOIN tf_latest AS tf2
      ON tf2.timesheet_id = ts0.timesheet_id
    LEFT JOIN tv_latest AS tv2
      ON tv2.timesheet_id = ts0.timesheet_id
    LEFT JOIN evidence_agg AS ev1
      ON ev1.timesheet_id = ts0.timesheet_id
    LEFT JOIN nhsp_agg AS ns1
      ON ns1.timesheet_id = ts0.timesheet_id
    LEFT JOIN pay_adjustments_agg AS pa1
      ON pa1.timesheet_id = ts0.timesheet_id
    LEFT JOIN public.candidates AS cand0
      ON cand0.id = COALESCE(tf2.candidate_id, ct0.candidate_id)
    LEFT JOIN public.clients AS cli0
      ON cli0.id = COALESCE(tf2.client_id, ct0.client_id)
    LEFT JOIN client_hr AS ch0
      ON ch0.client_id = COALESCE(tf2.client_id, ct0.client_id)
  ),
  planned_week_rows AS MATERIALIZED (
    SELECT
      NULL::uuid AS timesheet_id,
      NULL::public.timesheet_status_enum AS timesheet_status,
      cw0.week_ending_date AS week_ending_date,
      NULL::text AS booking_id,
      NULL::text AS occupant_key_norm,
      NULL::text AS hospital_norm,
      'WEEKLY'::public.timesheet_scope_enum AS sheet_scope,
      cw0.submission_mode_snapshot AS submission_mode,
      NULL::timestamp with time zone AS authorised_at_server,
      ct0.candidate_id AS candidate_id,
      ct0.client_id AS client_id,
      NULL::text AS pay_method,
      NULL::public.ts_fin_processing_status_enum AS processing_status,
      NULL::public.timesheet_fin_basis_enum AS basis,
      ROUND(
        (
          CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,day}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,day}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,night}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,night}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,sat}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,sat}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,sun}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,sun}')::numeric ELSE 0::numeric END
          + CASE WHEN NULLIF(BTRIM(COALESCE(cw0.totals_json #>> '{hours,bh}', '')), '') ~ v_numeric_re THEN (cw0.totals_json #>> '{hours,bh}')::numeric ELSE 0::numeric END
        ),
        2
      ) AS total_hours,
      NULL::numeric AS total_pay_ex_vat,
      NULL::numeric AS total_charge_ex_vat,
      NULL::numeric AS margin_ex_vat,
      NULL::timestamp with time zone AS paid_at_utc,
      FALSE AS pay_on_hold,
      NULL::uuid AS locked_by_invoice_id,
      cand0.display_name AS candidate_name,
      cli0.name AS client_name,
      0::integer AS nhsp_shift_count,
      0::integer AS nhsp_shift_included_count,
      0::integer AS nhsp_shift_deferred_count,
      NULL::public.validation_status_enum AS validation_status,
      cw0.id AS contract_week_id,
      cw0.week_ending_date AS contract_week_ending_date,
      cw0.status AS contract_week_status,
      cw0.additional_seq AS additional_seq,
      cw0.is_adjustment AS is_adjustment,
      NULL::public.timesheet_qr_status_enum AS qr_status,
      NULL::text AS qr_token,
      NULL::timestamp with time zone AS qr_generated_at,
      NULL::timestamp with time zone AS qr_scanned_at,
      NULL::text AS qr_last_sent_hash,
      0::integer AS pay_adjustment_count,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.autoprocess_hr ELSE NULL::boolean END,
        ch0.autoprocess_hr,
        FALSE
      ) AS client_autoprocess_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.requires_hr ELSE NULL::boolean END,
        ch0.requires_hr,
        FALSE
      ) AS client_requires_hr,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.no_timesheet_required ELSE NULL::boolean END,
        ch0.no_timesheet_required,
        FALSE
      ) AS client_no_timesheet_required,
      COALESCE(ch0.pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(ch0.invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(ch0.hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(ch0.ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.is_nhsp ELSE NULL::boolean END,
        ch0.is_nhsp,
        FALSE
      ) AS client_is_nhsp,
      FALSE AS has_rate_issue,
      FALSE AS has_pay_channel_issue,
      NULL::text AS hr_crosscheck_status,
      NULL::text[] AS hr_crosscheck_issues,
      NULL::jsonb AS external_source_rows_json,
      NULL::text AS reference_number,
      NULL::jsonb AS day_references_json,
      NULL::jsonb AS actual_schedule_json,
      NULL::text AS r2_nurse_key,
      NULL::text AS r2_auth_key,
      NULL::text AS manual_pdf_r2_key,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_pay ELSE NULL::boolean END,
        ch0.pay_reference_required,
        FALSE
      ) AS require_reference_to_pay,
      COALESCE(
        CASE WHEN ct0.overrideclientsettings THEN ct0.require_reference_to_invoice ELSE NULL::boolean END,
        ch0.invoice_reference_required,
        FALSE
      ) AS require_reference_to_invoice,
      0::integer AS evidence_count,
      FALSE AS has_timesheet_evidence,
      FALSE AS has_mileage_evidence,
      FALSE AS has_travel_evidence,
      FALSE AS has_accommodation_evidence,
      FALSE AS has_other_evidence,
      NULL::jsonb AS candidate_hint_text,
      NULL::numeric AS expenses_pay_ex_vat,
      NULL::text AS expenses_description,
      NULL::numeric AS mileage_units,
      NULL::numeric AS mileage_pay_rate,
      NULL::numeric AS mileage_charge_rate,
      NULL::numeric AS mileage_pay_ex_vat,
      NULL::numeric AS mileage_charge_ex_vat,
      NULL::numeric AS travel_pay_ex_vat,
      NULL::numeric AS travel_charge_ex_vat,
      NULL::numeric AS accommodation_pay_ex_vat,
      NULL::numeric AS accommodation_charge_ex_vat,
      NULL::numeric AS other_pay_ex_vat,
      NULL::numeric AS other_charge_ex_vat,
      NULL::jsonb AS invoice_breakdown_json
    FROM contract_week_scope_rows AS cw_scope
    JOIN public.contract_weeks AS cw0
      ON cw0.id = cw_scope.contract_week_id
    JOIN public.contracts AS ct0
      ON ct0.id = cw0.contract_id
    LEFT JOIN public.candidates AS cand0
      ON cand0.id = ct0.candidate_id
    LEFT JOIN public.clients AS cli0
      ON cli0.id = ct0.client_id
    LEFT JOIN client_hr AS ch0
      ON ch0.client_id = ct0.client_id
  ),
  base_union AS MATERIALIZED (
    SELECT
      tr0.timesheet_id,
      tr0.timesheet_status,
      tr0.week_ending_date,
      tr0.booking_id,
      tr0.occupant_key_norm,
      tr0.hospital_norm,
      tr0.sheet_scope,
      tr0.submission_mode,
      tr0.authorised_at_server,
      tr0.candidate_id,
      tr0.client_id,
      tr0.pay_method,
      tr0.processing_status,
      tr0.basis,
      tr0.total_hours,
      tr0.total_pay_ex_vat,
      tr0.total_charge_ex_vat,
      tr0.margin_ex_vat,
      tr0.paid_at_utc,
      tr0.pay_on_hold,
      tr0.locked_by_invoice_id,
      tr0.candidate_name,
      tr0.client_name,
      tr0.nhsp_shift_count,
      tr0.nhsp_shift_included_count,
      tr0.nhsp_shift_deferred_count,
      tr0.validation_status,
      tr0.contract_week_id,
      tr0.contract_week_ending_date,
      tr0.contract_week_status,
      tr0.additional_seq,
      tr0.is_adjustment,
      tr0.qr_status,
      tr0.qr_token,
      tr0.qr_generated_at,
      tr0.qr_scanned_at,
      tr0.qr_last_sent_hash,
      tr0.pay_adjustment_count,
      tr0.client_autoprocess_hr,
      tr0.client_requires_hr,
      tr0.client_no_timesheet_required,
      tr0.client_pay_reference_required,
      tr0.client_invoice_reference_required,
      tr0.client_hr_validation_required,
      tr0.client_ts_reference_required,
      tr0.client_is_nhsp,
      tr0.has_rate_issue,
      tr0.has_pay_channel_issue,
      tr0.hr_crosscheck_status,
      tr0.hr_crosscheck_issues,
      tr0.external_source_rows_json,
      tr0.reference_number,
      tr0.day_references_json,
      tr0.actual_schedule_json,
      tr0.r2_nurse_key,
      tr0.r2_auth_key,
      tr0.manual_pdf_r2_key,
      tr0.require_reference_to_pay,
      tr0.require_reference_to_invoice,
      tr0.evidence_count,
      tr0.has_timesheet_evidence,
      tr0.has_mileage_evidence,
      tr0.has_travel_evidence,
      tr0.has_accommodation_evidence,
      tr0.has_other_evidence,
      tr0.candidate_hint_text,
      tr0.expenses_pay_ex_vat,
      tr0.expenses_description,
      tr0.mileage_units,
      tr0.mileage_pay_rate,
      tr0.mileage_charge_rate,
      tr0.mileage_pay_ex_vat,
      tr0.mileage_charge_ex_vat,
      tr0.travel_pay_ex_vat,
      tr0.travel_charge_ex_vat,
      tr0.accommodation_pay_ex_vat,
      tr0.accommodation_charge_ex_vat,
      tr0.other_pay_ex_vat,
      tr0.other_charge_ex_vat,
      tr0.invoice_breakdown_json
    FROM timesheet_rows AS tr0
    UNION ALL
    SELECT
      pwr0.timesheet_id,
      pwr0.timesheet_status,
      pwr0.week_ending_date,
      pwr0.booking_id,
      pwr0.occupant_key_norm,
      pwr0.hospital_norm,
      pwr0.sheet_scope,
      pwr0.submission_mode,
      pwr0.authorised_at_server,
      pwr0.candidate_id,
      pwr0.client_id,
      pwr0.pay_method,
      pwr0.processing_status,
      pwr0.basis,
      pwr0.total_hours,
      pwr0.total_pay_ex_vat,
      pwr0.total_charge_ex_vat,
      pwr0.margin_ex_vat,
      pwr0.paid_at_utc,
      pwr0.pay_on_hold,
      pwr0.locked_by_invoice_id,
      pwr0.candidate_name,
      pwr0.client_name,
      pwr0.nhsp_shift_count,
      pwr0.nhsp_shift_included_count,
      pwr0.nhsp_shift_deferred_count,
      pwr0.validation_status,
      pwr0.contract_week_id,
      pwr0.contract_week_ending_date,
      pwr0.contract_week_status,
      pwr0.additional_seq,
      pwr0.is_adjustment,
      pwr0.qr_status,
      pwr0.qr_token,
      pwr0.qr_generated_at,
      pwr0.qr_scanned_at,
      pwr0.qr_last_sent_hash,
      pwr0.pay_adjustment_count,
      pwr0.client_autoprocess_hr,
      pwr0.client_requires_hr,
      pwr0.client_no_timesheet_required,
      pwr0.client_pay_reference_required,
      pwr0.client_invoice_reference_required,
      pwr0.client_hr_validation_required,
      pwr0.client_ts_reference_required,
      pwr0.client_is_nhsp,
      pwr0.has_rate_issue,
      pwr0.has_pay_channel_issue,
      pwr0.hr_crosscheck_status,
      pwr0.hr_crosscheck_issues,
      pwr0.external_source_rows_json,
      pwr0.reference_number,
      pwr0.day_references_json,
      pwr0.actual_schedule_json,
      pwr0.r2_nurse_key,
      pwr0.r2_auth_key,
      pwr0.manual_pdf_r2_key,
      pwr0.require_reference_to_pay,
      pwr0.require_reference_to_invoice,
      pwr0.evidence_count,
      pwr0.has_timesheet_evidence,
      pwr0.has_mileage_evidence,
      pwr0.has_travel_evidence,
      pwr0.has_accommodation_evidence,
      pwr0.has_other_evidence,
      pwr0.candidate_hint_text,
      pwr0.expenses_pay_ex_vat,
      pwr0.expenses_description,
      pwr0.mileage_units,
      pwr0.mileage_pay_rate,
      pwr0.mileage_charge_rate,
      pwr0.mileage_pay_ex_vat,
      pwr0.mileage_charge_ex_vat,
      pwr0.travel_pay_ex_vat,
      pwr0.travel_charge_ex_vat,
      pwr0.accommodation_pay_ex_vat,
      pwr0.accommodation_charge_ex_vat,
      pwr0.other_pay_ex_vat,
      pwr0.other_charge_ex_vat,
      pwr0.invoice_breakdown_json
    FROM planned_week_rows AS pwr0
  ),
  issue_rows AS MATERIALIZED (
    SELECT
      bu0.*,
      (
        ARRAY[]::text[]
        || CASE
             WHEN COALESCE(bu0.has_rate_issue, FALSE) = TRUE OR bu0.processing_status = 'RATE_MISSING'::public.ts_fin_processing_status_enum THEN ARRAY['Rate'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.has_pay_channel_issue, FALSE) = TRUE OR bu0.processing_status = 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum THEN ARRAY['Pay channel'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.processing_status = 'UNASSIGNED'::public.ts_fin_processing_status_enum THEN ARRAY['Candidate ID'::text]
             WHEN bu0.processing_status = 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum THEN ARRAY['Client ID'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.pay_on_hold, FALSE) = TRUE THEN ARRAY['On hold'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN NOT (
                    bu0.timesheet_id IS NOT NULL
                    AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
                    AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
                    AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
                  )
              AND (
                    bu0.hr_crosscheck_status = 'HOURS_MISMATCH_HR'
                    OR COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['HOURS_MISMATCH_HR'::text]
                  ) THEN ARRAY['Hours mismatch HR'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN NOT (
                    bu0.timesheet_id IS NOT NULL
                    AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
                    AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
                    AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
                  )
              AND COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['HR_HOURS_MISSING'::text] THEN ARRAY['HR hours missing'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN COALESCE(bu0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['DUPLICATE_CONTRACTS'::text] THEN ARRAY['Duplicate contracts'::text]
             ELSE ARRAY[]::text[]
           END

        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND (
                    COALESCE(bu0.travel_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.travel_pay_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.accommodation_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.accommodation_pay_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.other_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.other_pay_ex_vat, 0::numeric) > 0::numeric
                  )
              AND (
                    ((COALESCE(bu0.travel_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.travel_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_travel_evidence, FALSE) = FALSE)
                    OR ((COALESCE(bu0.accommodation_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.accommodation_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_accommodation_evidence, FALSE) = FALSE)
                    OR ((COALESCE(bu0.other_charge_ex_vat, 0::numeric) > 0::numeric OR COALESCE(bu0.other_pay_ex_vat, 0::numeric) > 0::numeric) AND COALESCE(bu0.has_other_evidence, FALSE) = FALSE)
                  ) THEN ARRAY['Expenses evidence'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND (
                    COALESCE(bu0.mileage_units, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.mileage_charge_ex_vat, 0::numeric) > 0::numeric
                    OR COALESCE(bu0.mileage_pay_ex_vat, 0::numeric) > 0::numeric
                  )
              AND COALESCE(bu0.has_mileage_evidence, FALSE) = FALSE THEN ARRAY['Mileage evidence'::text]
             ELSE ARRAY[]::text[]
           END


        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
              AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
              AND (bu0.validation_status IS NULL OR bu0.validation_status = 'PENDING'::public.validation_status_enum) THEN ARRAY['Awaiting validation'::text]
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_hr_validation_required, FALSE) = TRUE
              AND COALESCE(bu0.client_no_timesheet_required, FALSE) = FALSE
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric
              AND bu0.validation_status IS NOT NULL
              AND bu0.validation_status <> ALL (ARRAY['VALIDATION_OK'::public.validation_status_enum, 'OVERRIDDEN'::public.validation_status_enum, 'PENDING'::public.validation_status_enum]) THEN ARRAY['Validation failed'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND COALESCE(bu0.client_requires_hr, FALSE) = TRUE
              AND COALESCE(bu0.client_autoprocess_hr, FALSE) = FALSE
              AND bu0.authorised_at_server IS NULL THEN ARRAY['Authorisation'::text]
             ELSE ARRAY[]::text[]
           END
        || CASE
             WHEN bu0.timesheet_id IS NOT NULL
              AND bu0.qr_status = 'PENDING'::public.timesheet_qr_status_enum
              AND (
                    (NULLIF(BTRIM(COALESCE(bu0.qr_token, '')), '') IS NOT NULL AND bu0.qr_generated_at IS NOT NULL)
                    OR NULLIF(BTRIM(COALESCE(bu0.qr_last_sent_hash, '')), '') IS NOT NULL
                  )
              AND bu0.qr_scanned_at IS NULL
              AND COALESCE(bu0.total_hours, 0::numeric) > 0::numeric THEN ARRAY['Awaiting signed QR timesheet'::text]
             ELSE ARRAY[]::text[]
           END
      ) AS lightweight_issue_codes
    FROM base_union AS bu0
  ),
  segment_rows AS MATERIALIZED (
    SELECT
      ir0.*,
      segment_stats.seg_total AS invoice_segments_total_calc,
      segment_stats.seg_locked AS invoice_segments_locked_calc,
      COALESCE(invoice_paid_stats.invoice_paid_any, FALSE) AS invoice_is_paid_calc
    FROM issue_rows AS ir0
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN ir0.timesheet_id IS NULL THEN NULL::integer
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN jsonb_array_length(ir0.invoice_breakdown_json->'segments')::integer
          ELSE 1::integer
        END AS seg_total,
        CASE
          WHEN ir0.timesheet_id IS NULL THEN NULL::integer
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN (
             SELECT COUNT(*)::integer
             FROM jsonb_array_elements(ir0.invoice_breakdown_json->'segments') AS lock_segment(segment_value)
             WHERE NULLIF(BTRIM(COALESCE(lock_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
           )
          ELSE CASE WHEN ir0.locked_by_invoice_id IS NULL THEN 0::integer ELSE 1::integer END
        END AS seg_locked
    ) AS segment_stats
      ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        CASE
          WHEN ir0.timesheet_id IS NULL THEN FALSE
          WHEN ir0.invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(ir0.invoice_breakdown_json) = 'object'
           AND UPPER(COALESCE(ir0.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
           AND jsonb_typeof(ir0.invoice_breakdown_json->'segments') = 'array' THEN EXISTS (
             SELECT 1
             FROM jsonb_array_elements(ir0.invoice_breakdown_json->'segments') AS paid_segment(segment_value)
             JOIN public.invoices AS inv2
               ON inv2.id = CASE
                 WHEN NULLIF(BTRIM(COALESCE(paid_segment.segment_value->>'invoice_locked_invoice_id', '')), '') ~* v_uuid_re THEN (paid_segment.segment_value->>'invoice_locked_invoice_id')::uuid
                 ELSE NULL::uuid
               END
             WHERE inv2.status = 'PAID'::public.invoice_status_enum
                OR inv2.paid_at_utc IS NOT NULL
           )
          ELSE EXISTS (
             SELECT 1
             FROM public.invoices AS inv3
             WHERE inv3.id = ir0.locked_by_invoice_id
               AND (inv3.status = 'PAID'::public.invoice_status_enum OR inv3.paid_at_utc IS NOT NULL)
           )
        END AS invoice_paid_any
    ) AS invoice_paid_stats
      ON TRUE
  ),
  issue_normalised_rows AS MATERIALIZED (
    SELECT
      sr0.*,
      COALESCE(sr0.lightweight_issue_codes, ARRAY[]::text[]) AS workbench_issue_codes
    FROM segment_rows AS sr0
  ),
  result_rows AS MATERIALIZED (
    SELECT
      sr0.timesheet_id,
      sr0.timesheet_status,
      sr0.week_ending_date,
      sr0.booking_id,
      sr0.occupant_key_norm,
      sr0.hospital_norm,
      sr0.sheet_scope,
      sr0.submission_mode,
      sr0.authorised_at_server,
      sr0.candidate_id,
      sr0.client_id,
      sr0.pay_method,
      sr0.processing_status,
      sr0.basis,
      sr0.total_hours,
      sr0.total_pay_ex_vat,
      sr0.total_charge_ex_vat,
      sr0.margin_ex_vat,
      sr0.paid_at_utc,
      sr0.pay_on_hold,
      FALSE AS ready_to_pay,
      sr0.locked_by_invoice_id,
      sr0.candidate_name,
      sr0.client_name,
      sr0.nhsp_shift_count,
      sr0.nhsp_shift_included_count,
      sr0.nhsp_shift_deferred_count,
      sr0.validation_status,
      CASE
        WHEN sr0.timesheet_id IS NULL THEN CASE sr0.contract_week_status
          WHEN 'PLANNED'::public.contract_week_status_enum THEN 'PLANNED'
          WHEN 'OPEN'::public.contract_week_status_enum THEN 'PLANNED'
          WHEN 'SUBMITTED'::public.contract_week_status_enum THEN 'PENDING_AUTH'
          WHEN 'AUTHORISED'::public.contract_week_status_enum THEN 'READY_FOR_INVOICE'
          WHEN 'INVOICED'::public.contract_week_status_enum THEN 'INVOICED'
          WHEN 'CANCELLED'::public.contract_week_status_enum THEN 'NEEDS_ATTENTION'
          ELSE 'UNKNOWN'
        END
        WHEN sr0.paid_at_utc IS NOT NULL THEN 'PAID'
        WHEN sr0.locked_by_invoice_id IS NOT NULL
          OR (
            sr0.invoice_segments_total_calc IS NOT NULL
            AND sr0.invoice_segments_total_calc > 0
            AND COALESCE(sr0.invoice_segments_locked_calc, 0) >= sr0.invoice_segments_total_calc
          ) THEN 'INVOICED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.qr_status = 'PENDING'::public.timesheet_qr_status_enum
         AND NULLIF(BTRIM(COALESCE(sr0.qr_token, '')), '') IS NULL
         AND sr0.qr_generated_at IS NULL THEN 'QR_NOT_ISSUED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.qr_status = 'PENDING'::public.timesheet_qr_status_enum
         AND NULLIF(BTRIM(COALESCE(sr0.qr_token, '')), '') IS NOT NULL
         AND sr0.qr_generated_at IS NOT NULL
         AND sr0.qr_scanned_at IS NULL THEN 'QR_ISSUED_AWAITING_SIGNATURE'
        WHEN sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'READY_FOR_INVOICE'
        WHEN sr0.processing_status = 'READY_FOR_HR'::public.ts_fin_processing_status_enum THEN 'READY_FOR_HR'
        WHEN sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum THEN 'PENDING_AUTH'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'UNPROCESSED'
        WHEN sr0.processing_status = ANY (ARRAY['UNASSIGNED'::public.ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum, 'RATE_MISSING'::public.ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum]) THEN 'NEEDS_ATTENTION'
        ELSE 'UNKNOWN'
      END AS summary_stage,
      CASE
        WHEN sr0.sheet_scope = 'DAILY'::public.timesheet_scope_enum AND sr0.submission_mode = 'ELECTRONIC'::public.submission_mode_enum THEN 'DAILY_ELECTRONIC'
        WHEN sr0.sheet_scope = 'DAILY'::public.timesheet_scope_enum AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum THEN 'DAILY_MANUAL'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         )
         AND (
           COALESCE(sr0.client_is_nhsp, FALSE) = TRUE
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         )
         AND (
           COALESCE(sr0.client_autoprocess_hr, FALSE) = TRUE
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum
         AND (
           COALESCE(sr0.is_adjustment, FALSE) = TRUE
           OR COALESCE(sr0.additional_seq, 0) > 0
           OR COALESCE(sr0.pay_adjustment_count, 0) > 0
           OR UPPER(COALESCE(sr0.basis::text, '')) IN ('NHSP_ADJUSTMENT', 'HEALTHROSTER_ADJUSTMENT', 'MANUAL_ADJUSTMENT')
         ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND COALESCE(sr0.client_autoprocess_hr, FALSE) = TRUE THEN 'WEEKLY_HEALTHROSTER'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.basis = 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.basis = 'NHSP'::public.timesheet_fin_basis_enum THEN 'WEEKLY_NHSP'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND COALESCE(sr0.client_is_nhsp, FALSE) = TRUE THEN 'WEEKLY_NHSP'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.submission_mode = 'ELECTRONIC'::public.submission_mode_enum THEN 'WEEKLY_ELECTRONIC'
        WHEN sr0.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND sr0.submission_mode = 'MANUAL'::public.submission_mode_enum THEN 'WEEKLY_MANUAL'
        ELSE 'UNKNOWN'
      END AS route_type,
      sr0.contract_week_id,
      sr0.contract_week_ending_date,
      sr0.contract_week_status,
      sr0.additional_seq,
      sr0.is_adjustment,
      sr0.qr_status,
      sr0.pay_adjustment_count,
      (COALESCE(sr0.pay_adjustment_count, 0) > 0) AS has_pay_adjustments,
      (COALESCE(sr0.is_adjustment, FALSE) OR COALESCE(sr0.pay_adjustment_count, 0) > 0) AS is_adjusted,
      (sr0.qr_status IS NOT NULL) AS is_qr,
      (
        sr0.processing_status = ANY (ARRAY['UNASSIGNED'::public.ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum, 'RATE_MISSING'::public.ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum])
        OR (
          NOT (
            sr0.timesheet_id IS NOT NULL
            AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
            AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
            AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
          )
          AND sr0.hr_crosscheck_status IS NOT NULL
          AND sr0.hr_crosscheck_status <> 'OK'
        )
        OR (
          NOT (
            sr0.timesheet_id IS NOT NULL
            AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
            AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
            AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
          )
          AND COALESCE(sr0.hr_crosscheck_issues, ARRAY[]::text[]) && ARRAY['DUPLICATE_CONTRACTS'::text]
        )
        OR COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) > 0
      ) AS needs_attention,
      sr0.client_autoprocess_hr,
      sr0.has_rate_issue,
      sr0.has_pay_channel_issue,
      sr0.hr_crosscheck_status,
      sr0.hr_crosscheck_issues,
      sr0.external_source_rows_json,
      COALESCE(sr0.workbench_issue_codes, ARRAY[]::text[]) AS issue_codes,
      sr0.client_requires_hr,
      sr0.client_no_timesheet_required,
      sr0.client_is_nhsp,
      sr0.client_pay_reference_required,
      sr0.client_invoice_reference_required,
      sr0.client_hr_validation_required,
      sr0.client_ts_reference_required,
      sr0.require_reference_to_pay,
      sr0.require_reference_to_invoice,
      sr0.qr_token,
      sr0.qr_generated_at,
      sr0.qr_scanned_at,
      sr0.candidate_hint_text,
      sr0.expenses_pay_ex_vat,
      sr0.expenses_description,
      sr0.mileage_units,
      sr0.mileage_pay_rate,
      sr0.mileage_charge_rate,
      sr0.mileage_pay_ex_vat,
      sr0.travel_pay_ex_vat,
      sr0.travel_charge_ex_vat,
      sr0.accommodation_pay_ex_vat,
      sr0.accommodation_charge_ex_vat,
      sr0.other_pay_ex_vat,
      sr0.other_charge_ex_vat,
      (
        sr0.timesheet_id IS NOT NULL
        AND COALESCE(sr0.client_hr_validation_required, FALSE) = TRUE
        AND COALESCE(sr0.client_no_timesheet_required, FALSE) = FALSE
        AND COALESCE(sr0.total_hours, 0::numeric) > 0::numeric
      ) AS hr_validation_required_for_invoice,
      sr0.invoice_segments_total_calc AS invoice_segments_total,
      sr0.invoice_segments_locked_calc AS invoice_segments_locked,
      CASE
        WHEN sr0.invoice_segments_total_calc IS NULL THEN NULL::integer
        ELSE GREATEST(sr0.invoice_segments_total_calc - COALESCE(sr0.invoice_segments_locked_calc, 0), 0)
      END AS invoice_segments_unlocked,
      CASE
        WHEN sr0.invoice_segments_total_calc IS NULL THEN NULL::text
        WHEN COALESCE(sr0.invoice_segments_locked_calc, 0) = 0 THEN 'NOT_INVOICED'
        WHEN COALESCE(sr0.invoice_segments_locked_calc, 0) >= sr0.invoice_segments_total_calc THEN 'FULLY_INVOICED'
        ELSE 'PARTIALLY_INVOICED'
      END AS invoice_segment_stage,
      CASE
        WHEN sr0.timesheet_id IS NULL THEN 'UNPROCESSED'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'UNPROCESSED'
        WHEN sr0.locked_by_invoice_id IS NOT NULL OR COALESCE(sr0.invoice_segments_locked_calc, 0) > 0 THEN 'INVOICED'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.authorised_at_server IS NULL
         AND (
           sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR (
             COALESCE(sr0.client_requires_hr, FALSE) = TRUE
             AND COALESCE(sr0.client_autoprocess_hr, FALSE) = FALSE
             AND COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) = 1
             AND sr0.workbench_issue_codes @> ARRAY['Authorisation'::text]
           )
         ) THEN 'AWAITING_AUTHORISATION'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'AUTHORISED_FOR_INVOICING'
        ELSE 'PROCESSING_DELAYED'
      END AS tools_stage,
      CASE
        WHEN sr0.timesheet_id IS NULL THEN 'Unprocessed'
        WHEN sr0.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN 'Unprocessed'
        WHEN sr0.locked_by_invoice_id IS NOT NULL OR COALESCE(sr0.invoice_segments_locked_calc, 0) > 0 THEN
          CASE
            WHEN sr0.invoice_segments_total_calc IS NOT NULL
             AND COALESCE(sr0.invoice_segments_locked_calc, 0) > 0
             AND COALESCE(sr0.invoice_segments_locked_calc, 0) < sr0.invoice_segments_total_calc THEN 'Partially Invoiced'
            ELSE 'Invoiced'
          END
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.workbench_issue_codes @> ARRAY['Awaiting signed QR timesheet'::text] THEN 'Awaiting signed QR timesheet'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.authorised_at_server IS NULL
         AND (
           sr0.processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR (
             COALESCE(sr0.client_requires_hr, FALSE) = TRUE
             AND COALESCE(sr0.client_autoprocess_hr, FALSE) = FALSE
             AND COALESCE(array_length(sr0.workbench_issue_codes, 1), 0) = 1
             AND sr0.workbench_issue_codes @> ARRAY['Authorisation'::text]
           )
         ) THEN 'Awaiting Authorisation'
        WHEN sr0.timesheet_id IS NOT NULL
         AND sr0.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN 'Authorised for Invoicing'
        ELSE 'Processing Delayed'
      END AS processing_status_display,
      COALESCE(sr0.invoice_is_paid_calc, FALSE) AS invoice_is_paid,
      FALSE AS refs_block_invoicing,
      FALSE AS refs_block_issuing_invoices,
      FALSE AS refs_block_invoice_and_issuing,
      'NONE'::text AS pay_icon_code,
      NULL::text AS pay_status_code,
      NULL::timestamp with time zone AS pay_paid_at_utc,
      0::numeric AS net_delta_ex_vat
    FROM issue_normalised_rows AS sr0
  )
  SELECT
    rr0.timesheet_id,
    rr0.timesheet_status,
    rr0.week_ending_date,
    rr0.booking_id,
    rr0.occupant_key_norm,
    rr0.hospital_norm,
    rr0.sheet_scope,
    rr0.submission_mode,
    rr0.authorised_at_server,
    rr0.candidate_id,
    rr0.client_id,
    rr0.pay_method,
    rr0.processing_status,
    rr0.basis,
    rr0.total_hours,
    rr0.total_pay_ex_vat,
    rr0.total_charge_ex_vat,
    rr0.margin_ex_vat,
    rr0.paid_at_utc,
    rr0.pay_on_hold,
    rr0.ready_to_pay,
    rr0.locked_by_invoice_id,
    rr0.candidate_name,
    rr0.client_name,
    rr0.nhsp_shift_count,
    rr0.nhsp_shift_included_count,
    rr0.nhsp_shift_deferred_count,
    rr0.validation_status,
    rr0.summary_stage,
    rr0.route_type,
    rr0.contract_week_id,
    rr0.contract_week_ending_date,
    rr0.contract_week_status,
    rr0.additional_seq,
    rr0.is_adjustment,
    rr0.qr_status,
    rr0.pay_adjustment_count,
    rr0.has_pay_adjustments,
    rr0.is_adjusted,
    rr0.is_qr,
    rr0.needs_attention,
    rr0.client_autoprocess_hr,
    rr0.has_rate_issue,
    rr0.has_pay_channel_issue,
    rr0.hr_crosscheck_status,
    rr0.hr_crosscheck_issues,
    rr0.external_source_rows_json,
    rr0.issue_codes,
    rr0.client_requires_hr,
    rr0.client_no_timesheet_required,
    rr0.client_is_nhsp,
    rr0.client_pay_reference_required,
    rr0.client_invoice_reference_required,
    rr0.client_hr_validation_required,
    rr0.client_ts_reference_required,
    rr0.require_reference_to_pay,
    rr0.require_reference_to_invoice,
    rr0.qr_token,
    rr0.qr_generated_at,
    rr0.qr_scanned_at,
    rr0.candidate_hint_text,
    rr0.expenses_pay_ex_vat,
    rr0.expenses_description,
    rr0.mileage_units,
    rr0.mileage_pay_rate,
    rr0.mileage_charge_rate,
    rr0.mileage_pay_ex_vat,
    rr0.travel_pay_ex_vat,
    rr0.travel_charge_ex_vat,
    rr0.accommodation_pay_ex_vat,
    rr0.accommodation_charge_ex_vat,
    rr0.other_pay_ex_vat,
    rr0.other_charge_ex_vat,
    rr0.hr_validation_required_for_invoice,
    rr0.invoice_segments_total,
    rr0.invoice_segments_locked,
    rr0.invoice_segments_unlocked,
    rr0.invoice_segment_stage,
    rr0.tools_stage,
    rr0.processing_status_display,
    rr0.invoice_is_paid,
    rr0.refs_block_invoicing,
    rr0.refs_block_issuing_invoices,
    rr0.refs_block_invoice_and_issuing,
    rr0.pay_icon_code,
    rr0.pay_status_code,
    rr0.pay_paid_at_utc,
    rr0.net_delta_ex_vat
  FROM result_rows AS rr0
  WHERE (v_candidate_id IS NULL OR rr0.candidate_id = v_candidate_id)
    AND (v_client_id IS NULL OR rr0.client_id = v_client_id);
END;
$function$;


CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_patch_v1(
  p_filters jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(row_json jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
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

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  enriched_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      timesheet_row.version AS timesheet_version,
      timesheet_row.updated_at AS timesheet_updated_at,
      timesheet_row.is_current AS timesheet_is_current,
      timesheet_row.manual_pdf_r2_key,
      timesheet_row.qr_r2_key,
      timesheet_row.generated_pdf_at_utc,
      timesheet_row.manual_pdf_rotation_degrees,
      timesheet_row.qr_token AS timesheet_qr_token,
      timesheet_row.qr_generated_at AS timesheet_qr_generated_at,
      timesheet_row.qr_scanned_at AS timesheet_qr_scanned_at,
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
      financial_row.updated_at AS tsfin_updated_at,
      financial_row.actual_schedule_json AS tsfin_actual_schedule_json,
      financial_row.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
      financial_row.total_hours AS tsfin_total_hours,
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
        AND (
          UPPER(COALESCE(enriched_rows.processing_status::text, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed_calc,
      (
        COALESCE(enriched_rows.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(enriched_rows.paid_at_utc, NULL) IS NOT NULL
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
      ) AS can_unprocess_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = TRUE
      ) AS can_process_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR (decision_rows.contract_week_id IS NOT NULL AND decision_rows.route_family_calc = 'MANUAL_NON_QR'))
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
  signed_rows AS MATERIALIZED (
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
        COALESCE(final_rows.route_family_calc, ''),
        COALESCE(final_rows.route_subfamily_calc, ''),
        COALESCE(final_rows.bulk_process_bucket_calc, ''),
        COALESCE(final_rows.bulk_authorise_classification_calc, ''),
        COALESCE(final_rows.primary_artifact_storage_key_calc, ''),
        COALESCE(final_rows.total_hours::text, ''),
        COALESCE(final_rows.total_pay_ex_vat::text, ''),
        COALESCE(final_rows.total_charge_ex_vat::text, ''),
        COALESCE(final_rows.margin_ex_vat::text, ''),
        COALESCE(final_rows.issue_codes::text, '')
      )) AS row_signature_calc
    FROM final_rows
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
    (
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
        'backend_row_signature', payload_rows.row_signature_calc,
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
        'summary_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.summary_stage END,
        'tools_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.tools_stage END,
        'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
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
        'can_unprocess', payload_rows.can_unprocess_calc,
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
          'can_unprocess', payload_rows.can_unprocess_calc,
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
            'backend_row_signature', payload_rows.row_signature_calc,
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
            'summary_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.summary_stage END,
            'tools_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.tools_stage END,
            'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
            'previous_bulk_process_bucket', NULL::text,
            'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
            'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
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
            'can_unprocess', payload_rows.can_unprocess_calc,
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
    ) AS row_json
  FROM payload_rows
  ORDER BY
    COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date) ASC NULLS LAST,
    payload_rows.client_name ASC NULLS LAST,
    payload_rows.candidate_name ASC NULLS LAST,
    payload_rows.row_key_calc ASC;
END;
$function$;



REVOKE ALL ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO service_role;



