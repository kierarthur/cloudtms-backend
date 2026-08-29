BEGIN;

DROP FUNCTION IF EXISTS public.timesheet_summary_lightweight_rows_v1(jsonb);

CREATE OR REPLACE FUNCTION public.timesheet_summary_lightweight_rows_v1(
  p_filters jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE (
  timesheet_id uuid,
  contract_week_id uuid,
  contract_id uuid,
  candidate_id uuid,
  candidate_name text,
  candidate_display_name text,
  client_id uuid,
  client_name text,
  booking_id text,
  occupant_key_norm text,
  hospital_norm text,
  candidate_hint_text jsonb,
  week_ending_date date,
  work_date date,
  sheet_scope text,
  submission_mode text,
  submission_mode_snapshot text,
  basis text,
  route_type text,
  route_display text,
  route_family text,
  route_subfamily text,
  underlying_channel_family text,
  summary_stage text,
  tools_stage text,
  processing_status text,
  processing_status_display text,
  authorised_at_utc timestamptz,
  authorised_at_server timestamptz,
  processed_at_utc timestamptz,
  is_authorised boolean,
  total_hours numeric,
  total_pay_ex_vat numeric,
  total_charge_ex_vat numeric,
  margin_ex_vat numeric,
  net_delta_ex_vat numeric,
  paid_at_utc timestamptz,
  pay_icon_code text,
  pay_status_code text,
  pay_paid_at_utc timestamptz,
  invoice_is_paid boolean,
  invoice_issue_stage text,
  invoice_segment_stage text,
  invoice_segments_total integer,
  invoice_segments_locked integer,
  invoice_segments_unlocked integer,
  issue_codes text[],
  validation_status text,
  validation_summary text,
  hr_crosscheck_status text,
  hr_crosscheck_issues text[],
  qr_status text,
  is_qr boolean,
  is_adjusted boolean,
  needs_attention boolean,
  has_rate_issue boolean,
  has_pay_channel_issue boolean,
  client_no_timesheet_required boolean,
  client_autoprocess_hr boolean,
  client_is_nhsp boolean,
  has_any_evidence boolean,
  attached_evidence_count integer,
  primary_artifact_storage_key text,
  primary_artifact_display_name text,
  primary_artifact_preview_mode text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_id_text text := NULL;
  v_lookup_ids uuid[] := NULL;

  v_q text := NULL;
  v_tools_stage text := NULL;
  v_route_type text := NULL;
  v_sheet_scope text := NULL;
  v_qr_status text := NULL;
  v_status_code text := NULL;
  v_issues_filter text := NULL;

  v_candidate_paid boolean := NULL;
  v_is_adjusted boolean := NULL;
  v_is_qr boolean := NULL;
  v_hr_issue boolean := NULL;

  v_order_by text := 'candidate_name';
  v_order_dir text := 'asc';
  v_limit integer := 100;
  v_offset integer := 0;
BEGIN
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    v_lookup_ids := ARRAY[v_id_text::uuid];
  ELSIF v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT ARRAY_AGG(x.id_value)
      INTO v_lookup_ids
    FROM (
      SELECT DISTINCT j.value::uuid AS id_value
      FROM jsonb_array_elements_text(v_filters->'ids') AS j(value)
      WHERE j.value ~* v_uuid_re
    ) AS x;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_lookup_ids, 1), 0) > 0 THEN
    /*
      The old summary endpoint used one generic id that could match either
      timesheet_id or contract_week_id. Pass the same uuid list as both filters
      so the existing lightweight row source can resolve either identity.
    */
    v_source_filters :=
      v_source_filters
      || jsonb_build_object(
           'timesheet_ids', to_jsonb(v_lookup_ids),
           'contract_week_ids', to_jsonb(v_lookup_ids)
         );
  END IF;

  v_q := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'query', v_filters->>'name', '')), ''));
  v_tools_stage := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'tools_stage', v_filters->>'toolsStage', '')), ''));
  v_route_type := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'route_type', v_filters->>'routeType', '')), ''));
  v_sheet_scope := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  v_qr_status := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'qr_status', v_filters->>'qrStatus', '')), ''));
  v_status_code := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'status_code', v_filters->>'statusCode', '')), ''));
  v_issues_filter := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'issues_filter', v_filters->>'issuesFilter', '')), ''));

  IF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('true','t','yes','y','1') THEN
    v_candidate_paid := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'candidate_paid', v_filters->>'candidatePaid', '')) IN ('false','f','no','n','0') THEN
    v_candidate_paid := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('true','t','yes','y','1') THEN
    v_is_adjusted := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_adjusted', v_filters->>'isAdjusted', '')) IN ('false','f','no','n','0') THEN
    v_is_adjusted := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('true','t','yes','y','1') THEN
    v_is_qr := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'is_qr', v_filters->>'isQr', '')) IN ('false','f','no','n','0') THEN
    v_is_qr := FALSE;
  END IF;

  IF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('true','t','yes','y','1') THEN
    v_hr_issue := TRUE;
  ELSIF LOWER(COALESCE(v_filters->>'hr_issue', v_filters->>'hrIssue', '')) IN ('false','f','no','n','0') THEN
    v_hr_issue := FALSE;
  END IF;

  v_order_by := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_by', v_filters->>'orderBy', 'candidate_name')), ''));
  v_order_dir := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'order_dir', v_filters->>'orderDir', 'asc')), ''));

  IF v_order_dir NOT IN ('asc', 'desc') THEN
    v_order_dir := 'asc';
  END IF;

  IF COALESCE(v_filters->>'limit', '') ~ '^[0-9]+$' THEN
    v_limit := LEAST(GREATEST((v_filters->>'limit')::integer, 1), 5000);
  ELSIF COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
    v_limit := LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
  END IF;

  IF COALESCE(v_filters->>'offset', '') ~ '^[0-9]+$' THEN
    v_offset := GREATEST((v_filters->>'offset')::integer, 0);
  ELSIF COALESCE(v_filters->>'page', '') ~ '^[0-9]+$'
        AND COALESCE(v_filters->>'page_size', v_filters->>'pageSize', '') ~ '^[0-9]+$' THEN
    v_offset :=
      GREATEST(((v_filters->>'page')::integer - 1), 0)
      * LEAST(GREATEST(COALESCE(v_filters->>'page_size', v_filters->>'pageSize')::integer, 1), 5000);
  END IF;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      sr.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS sr
  ),
  enriched AS MATERIALIZED (
    SELECT
      sr.timesheet_id,
      sr.contract_week_id,
      COALESCE(t.contract_id, cw.contract_id) AS contract_id,

      sr.candidate_id,
      sr.candidate_name,
      sr.candidate_name AS candidate_display_name,
      sr.client_id,
      sr.client_name,

      sr.booking_id,
      sr.occupant_key_norm,
      sr.hospital_norm,
      sr.candidate_hint_text,

      COALESCE(sr.contract_week_ending_date, sr.week_ending_date) AS week_ending_date,

      CASE
        WHEN sr.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN
          COALESCE(t.worked_start_iso::date, t.scheduled_start_iso::date, sr.week_ending_date)
        ELSE NULL::date
      END AS work_date,

      sr.sheet_scope::text AS sheet_scope,
      sr.submission_mode::text AS submission_mode,
      COALESCE(cw.submission_mode_snapshot::text, sr.submission_mode::text) AS submission_mode_snapshot,
      sr.basis::text AS basis,

      sr.route_type,
      CASE
        WHEN UPPER(COALESCE(sr.route_type, '')) = 'NHSP' THEN 'NHSP'
        WHEN UPPER(COALESCE(sr.route_type, '')) = 'HEALTHROSTER' THEN 'HealthRoster'
        WHEN UPPER(COALESCE(sr.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
        WHEN UPPER(COALESCE(sr.route_type, '')) = 'QR' THEN 'QR'
        WHEN UPPER(COALESCE(sr.route_type, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
        WHEN COALESCE(sr.route_type, '') <> '' THEN INITCAP(REPLACE(sr.route_type, '_', ' '))
        ELSE 'Manual'
      END AS route_display,

      CASE
        WHEN UPPER(COALESCE(sr.route_type, '')) LIKE '%NHSP%' THEN 'NHSP'
        WHEN UPPER(COALESCE(sr.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'HEALTHROSTER'
        WHEN COALESCE(sr.is_qr, FALSE) THEN 'QR'
        WHEN COALESCE(sr.client_no_timesheet_required, FALSE) THEN 'NO_TIMESHEET_REQUIRED'
        ELSE 'MANUAL'
      END AS route_family,

      CASE
        WHEN UPPER(COALESCE(sr.route_type, '')) = 'HEALTHROSTER_DAILY' THEN 'DAILY'
        WHEN sr.sheet_scope = 'DAILY'::public.timesheet_scope_enum THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS route_subfamily,

      CASE
        WHEN UPPER(COALESCE(sr.route_type, '')) LIKE '%NHSP%' THEN 'IMPORT'
        WHEN UPPER(COALESCE(sr.route_type, '')) LIKE '%HEALTHROSTER%' THEN 'IMPORT'
        WHEN COALESCE(sr.is_qr, FALSE) THEN 'QR'
        ELSE 'MANUAL'
      END AS underlying_channel_family,

      sr.summary_stage,
      sr.tools_stage,
      sr.processing_status::text AS processing_status,
      sr.processing_status_display,

      sr.authorised_at_server AS authorised_at_utc,
      sr.authorised_at_server,
      tf.processed_at_utc,

      (sr.authorised_at_server IS NOT NULL) AS is_authorised,

      COALESCE(sr.total_hours, 0::numeric) AS total_hours,
      COALESCE(sr.total_pay_ex_vat, 0::numeric) AS total_pay_ex_vat,
      COALESCE(sr.total_charge_ex_vat, 0::numeric) AS total_charge_ex_vat,
      COALESCE(sr.margin_ex_vat, 0::numeric) AS margin_ex_vat,
      COALESCE(sr.net_delta_ex_vat, COALESCE(sr.total_charge_ex_vat, 0::numeric) - COALESCE(sr.total_pay_ex_vat, 0::numeric)) AS net_delta_ex_vat,

      sr.paid_at_utc,
      sr.pay_icon_code,
      sr.pay_status_code,
      sr.pay_paid_at_utc,

      COALESCE(sr.invoice_is_paid, FALSE) AS invoice_is_paid,

      CASE
        WHEN COALESCE(sr.invoice_is_paid, FALSE) THEN 'PAID'
        WHEN COALESCE(sr.invoice_segments_locked, 0) > 0 THEN 'LOCKED'
        WHEN COALESCE(sr.invoice_segments_total, 0) > 0 THEN 'DRAFT'
        ELSE NULL::text
      END AS invoice_issue_stage,

      sr.invoice_segment_stage,
      COALESCE(sr.invoice_segments_total, 0)::integer AS invoice_segments_total,
      COALESCE(sr.invoice_segments_locked, 0)::integer AS invoice_segments_locked,
      COALESCE(sr.invoice_segments_unlocked, 0)::integer AS invoice_segments_unlocked,

      COALESCE(sr.issue_codes, ARRAY[]::text[]) AS issue_codes,
      sr.validation_status::text AS validation_status,

      CASE
        WHEN sr.validation_status IS NULL THEN NULL::text
        ELSE sr.validation_status::text
      END AS validation_summary,

      sr.hr_crosscheck_status,
      COALESCE(sr.hr_crosscheck_issues, ARRAY[]::text[]) AS hr_crosscheck_issues,

      sr.qr_status::text AS qr_status,
      COALESCE(sr.is_qr, FALSE) AS is_qr,
      COALESCE(sr.is_adjusted, FALSE) AS is_adjusted,
      COALESCE(sr.needs_attention, FALSE) AS needs_attention,
      COALESCE(sr.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(sr.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,

      COALESCE(sr.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(sr.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(sr.client_is_nhsp, FALSE) AS client_is_nhsp,

      (
        COALESCE(ev.attached_evidence_count, 0) > 0
        OR NULLIF(t.manual_pdf_r2_key, '') IS NOT NULL
        OR NULLIF(t.qr_r2_key, '') IS NOT NULL
        OR NULLIF(cw.uploaded_pdf_r2_key, '') IS NOT NULL
      ) AS has_any_evidence,

      COALESCE(ev.attached_evidence_count, 0)::integer AS attached_evidence_count,

      COALESCE(
        ev.primary_storage_key,
        NULLIF(t.manual_pdf_r2_key, ''),
        NULLIF(t.qr_r2_key, ''),
        NULLIF(cw.uploaded_pdf_r2_key, '')
      ) AS primary_artifact_storage_key,

      COALESCE(
        ev.primary_display_name,
        CASE WHEN NULLIF(t.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(t.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN NULLIF(cw.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name,

      CASE
        WHEN COALESCE(ev.primary_storage_key, NULLIF(t.manual_pdf_r2_key, ''), NULLIF(t.qr_r2_key, ''), NULLIF(cw.uploaded_pdf_r2_key, '')) IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode

    FROM source_rows AS sr
    LEFT JOIN public.timesheets AS t
      ON t.timesheet_id = sr.timesheet_id
     AND t.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw
      ON cw.id = sr.contract_week_id
    LEFT JOIN public.timesheets_financials AS tf
      ON tf.timesheet_id = sr.timesheet_id
     AND tf.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(te.id)::integer AS attached_evidence_count,
        (ARRAY_AGG(
          te.storage_key
          ORDER BY
            (UPPER(COALESCE(te.kind, '')) = 'TIMESHEET') DESC,
            te.created_at DESC,
            te.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(te.display_name, ''), te.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(te.kind, '')) = 'TIMESHEET') DESC,
            te.created_at DESC,
            te.id DESC
        ))[1] AS primary_display_name
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = sr.timesheet_id
    ) AS ev ON TRUE
  ),
  filtered AS MATERIALIZED (
    SELECT e.*
    FROM enriched AS e
    WHERE
      (
        v_lookup_ids IS NULL
        OR e.timesheet_id = ANY(v_lookup_ids)
        OR e.contract_week_id = ANY(v_lookup_ids)
      )
      AND (
        v_q IS NULL
        OR LOWER(COALESCE(e.candidate_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.candidate_display_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.client_name, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.booking_id, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.occupant_key_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.hospital_norm, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.route_display, '')) LIKE '%' || v_q || '%'
        OR LOWER(COALESCE(e.processing_status_display, '')) LIKE '%' || v_q || '%'
      )
      AND (
        v_tools_stage IS NULL
        OR LOWER(COALESCE(e.tools_stage, '')) = v_tools_stage
      )
      AND (
        v_route_type IS NULL
        OR LOWER(COALESCE(e.route_type, '')) = v_route_type
        OR LOWER(COALESCE(e.route_family, '')) = v_route_type
      )
      AND (
        v_sheet_scope IS NULL
        OR LOWER(COALESCE(e.sheet_scope, '')) = v_sheet_scope
      )
      AND (
        v_qr_status IS NULL
        OR LOWER(COALESCE(e.qr_status, '')) = v_qr_status
      )
      AND (
        v_status_code IS NULL
        OR LOWER(COALESCE(e.processing_status, '')) = v_status_code
        OR LOWER(COALESCE(e.summary_stage, '')) = v_status_code
        OR LOWER(COALESCE(e.tools_stage, '')) = v_status_code
      )
      AND (
        v_candidate_paid IS NULL
        OR ((e.pay_paid_at_utc IS NOT NULL OR e.paid_at_utc IS NOT NULL) = v_candidate_paid)
      )
      AND (
        v_is_adjusted IS NULL
        OR e.is_adjusted = v_is_adjusted
      )
      AND (
        v_is_qr IS NULL
        OR e.is_qr = v_is_qr
      )
      AND (
        v_hr_issue IS NULL
        OR (
          (
            COALESCE(ARRAY_LENGTH(e.hr_crosscheck_issues, 1), 0) > 0
            OR (
              e.hr_crosscheck_status IS NOT NULL
              AND UPPER(e.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
            )
          ) = v_hr_issue
        )
      )
      AND (
        v_issues_filter IS NULL
        OR v_issues_filter IN ('all', 'any') AND (e.needs_attention OR COALESCE(ARRAY_LENGTH(e.issue_codes, 1), 0) > 0)
        OR v_issues_filter IN ('none', 'clear') AND (NOT e.needs_attention AND COALESCE(ARRAY_LENGTH(e.issue_codes, 1), 0) = 0)
        OR v_issues_filter IN ('rate', 'rates') AND e.has_rate_issue
        OR v_issues_filter IN ('pay', 'pay_channel', 'pay-channel') AND e.has_pay_channel_issue
        OR v_issues_filter IN ('hr', 'hr_issue', 'hr-issue') AND (
          COALESCE(ARRAY_LENGTH(e.hr_crosscheck_issues, 1), 0) > 0
          OR (
            e.hr_crosscheck_status IS NOT NULL
            AND UPPER(e.hr_crosscheck_status) NOT IN ('OK', 'MATCHED', 'MATCH', 'VALID', 'PASSED', 'CLEAR')
          )
        )
        OR LOWER(v_issues_filter) = ANY(
          SELECT LOWER(UNNEST(COALESCE(e.issue_codes, ARRAY[]::text[])))
        )
      )
  )
  SELECT
    f.timesheet_id,
    f.contract_week_id,
    f.contract_id,
    f.candidate_id,
    f.candidate_name,
    f.candidate_display_name,
    f.client_id,
    f.client_name,
    f.booking_id,
    f.occupant_key_norm,
    f.hospital_norm,
    f.candidate_hint_text,
    f.week_ending_date,
    f.work_date,
    f.sheet_scope,
    f.submission_mode,
    f.submission_mode_snapshot,
    f.basis,
    f.route_type,
    f.route_display,
    f.route_family,
    f.route_subfamily,
    f.underlying_channel_family,
    f.summary_stage,
    f.tools_stage,
    f.processing_status,
    f.processing_status_display,
    f.authorised_at_utc,
    f.authorised_at_server,
    f.processed_at_utc,
    f.is_authorised,
    f.total_hours,
    f.total_pay_ex_vat,
    f.total_charge_ex_vat,
    f.margin_ex_vat,
    f.net_delta_ex_vat,
    f.paid_at_utc,
    f.pay_icon_code,
    f.pay_status_code,
    f.pay_paid_at_utc,
    f.invoice_is_paid,
    f.invoice_issue_stage,
    f.invoice_segment_stage,
    f.invoice_segments_total,
    f.invoice_segments_locked,
    f.invoice_segments_unlocked,
    f.issue_codes,
    f.validation_status,
    f.validation_summary,
    f.hr_crosscheck_status,
    f.hr_crosscheck_issues,
    f.qr_status,
    f.is_qr,
    f.is_adjusted,
    f.needs_attention,
    f.has_rate_issue,
    f.has_pay_channel_issue,
    f.client_no_timesheet_required,
    f.client_autoprocess_hr,
    f.client_is_nhsp,
    f.has_any_evidence,
    f.attached_evidence_count,
    f.primary_artifact_storage_key,
    f.primary_artifact_display_name,
    f.primary_artifact_preview_mode
  FROM filtered AS f
  ORDER BY
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'asc' THEN f.candidate_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('candidate_name', 'candidate') AND v_order_dir = 'desc' THEN f.candidate_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'asc' THEN f.client_name END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('client_name', 'client') AND v_order_dir = 'desc' THEN f.client_name END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'asc' THEN f.week_ending_date END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('week_ending_date', 'week_ending', 'date') AND v_order_dir = 'desc' THEN f.week_ending_date END DESC NULLS LAST,

    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'asc' THEN f.work_date END ASC NULLS LAST,
    CASE WHEN v_order_by = 'work_date' AND v_order_dir = 'desc' THEN f.work_date END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'asc' THEN f.processing_status END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('processing_status', 'status') AND v_order_dir = 'desc' THEN f.processing_status END DESC NULLS LAST,

    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'asc' THEN f.tools_stage END ASC NULLS LAST,
    CASE WHEN v_order_by = 'tools_stage' AND v_order_dir = 'desc' THEN f.tools_stage END DESC NULLS LAST,

    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'asc' THEN f.route_type END ASC NULLS LAST,
    CASE WHEN v_order_by = 'route_type' AND v_order_dir = 'desc' THEN f.route_type END DESC NULLS LAST,

    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'asc' THEN f.sheet_scope END ASC NULLS LAST,
    CASE WHEN v_order_by = 'sheet_scope' AND v_order_dir = 'desc' THEN f.sheet_scope END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'asc' THEN f.total_pay_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_pay_ex_vat', 'pay') AND v_order_dir = 'desc' THEN f.total_pay_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'asc' THEN f.total_charge_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('total_charge_ex_vat', 'charge') AND v_order_dir = 'desc' THEN f.total_charge_ex_vat END DESC NULLS LAST,

    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'asc' THEN f.margin_ex_vat END ASC NULLS LAST,
    CASE WHEN v_order_by IN ('margin_ex_vat', 'margin') AND v_order_dir = 'desc' THEN f.margin_ex_vat END DESC NULLS LAST,

    f.candidate_name ASC NULLS LAST,
    f.week_ending_date DESC NULLS LAST,
    f.work_date DESC NULLS LAST,
    f.timesheet_id NULLS LAST,
    f.contract_week_id NULLS LAST
  LIMIT v_limit
  OFFSET v_offset;
END;
$function$;

REVOKE ALL ON FUNCTION public.timesheet_summary_lightweight_rows_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.timesheet_summary_lightweight_rows_v1(jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.timesheet_summary_lightweight_rows_v1(jsonb) TO service_role;

COMMIT;
