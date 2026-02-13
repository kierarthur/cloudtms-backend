-- NOTE:
-- This migration is SAFE to re-run:
--  - Adds candidate_hint_text column if missing + (re)applies JSON-object CHECK
--  - Adds TSFIN category columns (travel/accommodation/other) if missing
--  - Adds supporting index on (timesheet_id, kind) for evidence gating
--  - CREATE OR REPLACE VIEW for: v_timesheets_summary_base, v_timesheets_summary, v_timesheets_details (idempotent)
--    ✅ IMPORTANT: we only APPEND new columns at the END of each view output
--  - Re-applies GRANTs (idempotent)
--  - Triggers PostgREST schema reload

-- =========================================================
-- Ensure column exists + enforce JSON object shape
-- =========================================================
ALTER TABLE public.timesheets
  ADD COLUMN IF NOT EXISTS candidate_hint_text jsonb;

ALTER TABLE public.timesheets
  DROP CONSTRAINT IF EXISTS timesheets_candidate_hint_text_is_object;

ALTER TABLE public.timesheets
  ADD CONSTRAINT timesheets_candidate_hint_text_is_object
  CHECK (
    candidate_hint_text IS NULL
    OR jsonb_typeof(candidate_hint_text) = 'object'
  );

-- =========================================================
-- TSFIN: add per-category expense columns (Travel/Accommodation/Other)
-- =========================================================
ALTER TABLE public.timesheets_financials
  ADD COLUMN IF NOT EXISTS travel_pay_ex_vat           numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS travel_charge_ex_vat        numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS accommodation_pay_ex_vat    numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS accommodation_charge_ex_vat numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS other_pay_ex_vat            numeric NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS other_charge_ex_vat         numeric NOT NULL DEFAULT 0;

-- =========================================================
-- Evidence performance: kind lookup index (needed for gating checks)
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_timesheet_evidence_timesheet_kind
  ON public.timesheet_evidence (timesheet_id, kind);

-- =========================================================
-- UPDATED: v_timesheets_summary_base + v_timesheets_summary
-- Adds support for:
--   - timesheets.candidate_hint_text exposed to FE
--   - candidate_name shows hint when UNRESOLVED
--   - ✅ APPENDS (at end of view) the additional expense/mileage fields already present:
--       expenses_pay_ex_vat, expenses_description,
--       mileage_units, mileage_pay_rate, mileage_charge_rate, mileage_pay_ex_vat
--   - ✅ APPENDS (AFTER those) the NEW category columns:
--       travel_pay_ex_vat, travel_charge_ex_vat,
--       accommodation_pay_ex_vat, accommodation_charge_ex_vat,
--       other_pay_ex_vat, other_charge_ex_vat
--
-- IMPORTANT:
--   CREATE OR REPLACE VIEW cannot reorder existing columns.
--   So we keep ALL existing columns in the same order, and only add new ones at the end.
-- =========================================================

-- ============================================================
-- v_timesheets_summary_base (REVISED)
-- ✅ SAFE TO RE-RUN: CREATE OR REPLACE VIEW
-- ✅ ONLY CHANGE: APPEND a new boolean column at the END of the view output:
--     hr_validation_required_for_invoice
-- ============================================================


CREATE OR REPLACE VIEW public.v_timesheets_summary_base AS
WITH latest_tsfin AS (
  SELECT DISTINCT ON (tf.timesheet_id)
    tf.id,
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

    -- existing (already in your view)
    tf.expenses_charge_ex_vat,
    tf.expenses_evidence_r2_key,
    tf.expenses_evidence_manifest,
    tf.mileage_charge_ex_vat,
    tf.mileage_evidence_r2_key,
    tf.mileage_evidence_manifest,

    -- existing backing fields (already appended previously)
    tf.expenses_pay_ex_vat,
    tf.expenses_description,
    tf.mileage_units,
    tf.mileage_pay_rate,
    tf.mileage_charge_rate,
    tf.mileage_pay_ex_vat,

    -- category backing fields
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
),
validations_latest AS (
  SELECT DISTINCT ON (tv.timesheet_id)
    tv.timesheet_id,
    tv.status,
    tv.reason_code
  FROM timesheet_validations tv
  ORDER BY tv.timesheet_id, tv.created_at DESC
),
nhsp_agg AS (
  SELECT
    ns.timesheet_id,
    count(*)::integer AS nhsp_shift_count,
    count(*) FILTER (WHERE ns.invoice_status = 'INCLUDED'::text)::integer AS nhsp_shift_included_count,
    count(*) FILTER (WHERE ns.invoice_status = 'DEFERRED'::text)::integer AS nhsp_shift_deferred_count
  FROM nhsp_shifts ns
  GROUP BY ns.timesheet_id
),
pay_adj AS (
  SELECT
    pa.timesheet_id,
    count(*)::integer AS pay_adjustment_count
  FROM ts_pay_adjustments pa
  GROUP BY pa.timesheet_id
),
evidence_agg AS (
  SELECT
    te.timesheet_id,
    count(*)::integer AS evidence_count
  FROM timesheet_evidence te
  GROUP BY te.timesheet_id
),
client_hr AS (
  SELECT
    cs.client_id,
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
),
ts_base AS (
  SELECT
    ts.timesheet_id,
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

    -- show hint when unresolved (candidate_id null) and hint has useful fields
    CASE
      WHEN COALESCE(tf.candidate_id, ct.candidate_id) IS NULL
        AND ts.candidate_hint_text IS NOT NULL
        AND jsonb_typeof(ts.candidate_hint_text) = 'object'
        AND (
          nullif(btrim(concat_ws(' ',
            nullif(btrim(ts.candidate_hint_text->>'first_name'), ''),
            nullif(btrim(ts.candidate_hint_text->>'surname'), '')
          )), '') IS NOT NULL
          OR nullif(btrim(ts.candidate_hint_text->>'display_name'), '') IS NOT NULL
          OR nullif(btrim(ts.candidate_hint_text->>'email'), '') IS NOT NULL
        )
      THEN
        'Unresolved Timesheet - ' ||
        COALESCE(
          nullif(btrim(concat_ws(' ',
            nullif(btrim(ts.candidate_hint_text->>'first_name'), ''),
            nullif(btrim(ts.candidate_hint_text->>'surname'), '')
          )), ''),
          nullif(btrim(ts.candidate_hint_text->>'display_name'), ''),
          'Candidate'
        ) ||
        CASE
          WHEN nullif(btrim(ts.candidate_hint_text->>'email'), '') IS NOT NULL
          THEN ', Email - ' || btrim(ts.candidate_hint_text->>'email')
          ELSE ''
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
    cw.is_adjustment,

    ts.qr_status,
    ts.qr_token,
    ts.qr_generated_at,
    ts.qr_scanned_at,

    COALESCE(pa.pay_adjustment_count, 0) AS pay_adjustment_count,

    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.autoprocess_hr END, ch.autoprocess_hr, false) AS client_autoprocess_hr,
    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.requires_hr END, ch.requires_hr, false) AS client_requires_hr,
    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.no_timesheet_required END, ch.no_timesheet_required, false) AS client_no_timesheet_required,

    COALESCE(ch.pay_reference_required, false) AS client_pay_reference_required,
    COALESCE(ch.invoice_reference_required, false) AS client_invoice_reference_required,
    COALESCE(ch.hr_validation_required, false) AS client_hr_validation_required,
    COALESCE(ch.ts_reference_required, false) AS client_ts_reference_required,

    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.is_nhsp END, ch.is_nhsp, false) AS client_is_nhsp,

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

    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.require_reference_to_pay END, ch.pay_reference_required, false) AS require_reference_to_pay,
    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.require_reference_to_invoice END, ch.invoice_reference_required, false) AS require_reference_to_invoice,

    COALESCE(ea.evidence_count, 0) AS evidence_count,

    -- existing expense/mileage fields
    tf.expenses_charge_ex_vat,
    tf.expenses_evidence_r2_key,
    tf.expenses_evidence_manifest,
    tf.mileage_charge_ex_vat,
    tf.mileage_evidence_r2_key,
    tf.mileage_evidence_manifest,

    cand.pay_method      AS cand_pay_method,
    cand.account_holder  AS cand_account_holder,
    cand.sort_code       AS cand_sort_code,
    cand.account_number  AS cand_account_number,
    cand.umbrella_id     AS cand_umbrella_id,

    umb.enabled          AS umb_enabled,
    umb.name             AS umb_name,
    umb.sort_code        AS umb_sort_code,
    umb.account_number   AS umb_account_number,

    -- existing last column in your view
    ts.candidate_hint_text AS candidate_hint_text,

    -- existing appended columns (already in your view output)
    tf.expenses_pay_ex_vat,
    tf.expenses_description,
    tf.mileage_units,
    tf.mileage_pay_rate,
    tf.mileage_charge_rate,
    tf.mileage_pay_ex_vat,

    -- category backing fields (already appended in this revision)
    tf.travel_pay_ex_vat,
    tf.travel_charge_ex_vat,
    tf.accommodation_pay_ex_vat,
    tf.accommodation_charge_ex_vat,
    tf.other_pay_ex_vat,
    tf.other_charge_ex_vat,

    -- refs/PDF baseline fields (used for 'Refs - Timesheet PDF invalid' issue)
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
),
planned_weeks AS (
  SELECT
    NULL::uuid AS timesheet_id,
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

    round(
      COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'day'::text, ''::text)::numeric, 0::numeric) +
      COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'night'::text, ''::text)::numeric, 0::numeric) +
      COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'sat'::text, ''::text)::numeric, 0::numeric) +
      COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'sun'::text, ''::text)::numeric, 0::numeric) +
      COALESCE(NULLIF((cw.totals_json -> 'hours'::text) ->> 'bh'::text, ''::text)::numeric, 0::numeric),
      2
    ) AS total_hours,

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
    cw.is_adjustment,

    NULL::timesheet_qr_status_enum AS qr_status,
    NULL::text AS qr_token,
    NULL::timestamp with time zone AS qr_generated_at,
    NULL::timestamp with time zone AS qr_scanned_at,

    0 AS pay_adjustment_count,

    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.autoprocess_hr END, ch.autoprocess_hr, false) AS client_autoprocess_hr,
    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.requires_hr END, ch.requires_hr, false) AS client_requires_hr,
    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.no_timesheet_required END, ch.no_timesheet_required, false) AS client_no_timesheet_required,

    COALESCE(ch.pay_reference_required, false) AS client_pay_reference_required,
    COALESCE(ch.invoice_reference_required, false) AS client_invoice_reference_required,
    COALESCE(ch.hr_validation_required, false) AS client_hr_validation_required,
    COALESCE(ch.ts_reference_required, false) AS client_ts_reference_required,

    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.is_nhsp END, ch.is_nhsp, false) AS client_is_nhsp,

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

    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.require_reference_to_pay END, ch.pay_reference_required, false) AS require_reference_to_pay,
    COALESCE(CASE WHEN ct.overrideclientsettings THEN ct.require_reference_to_invoice END, ch.invoice_reference_required, false) AS require_reference_to_invoice,

    0 AS evidence_count,

    NULL::numeric AS expenses_charge_ex_vat,
    NULL::text    AS expenses_evidence_r2_key,
    NULL::jsonb   AS expenses_evidence_manifest,
    NULL::numeric AS mileage_charge_ex_vat,
    NULL::text    AS mileage_evidence_r2_key,
    NULL::jsonb   AS mileage_evidence_manifest,

    NULL::text    AS cand_pay_method,
    NULL::text    AS cand_account_holder,
    NULL::text    AS cand_sort_code,
    NULL::text    AS cand_account_number,
    NULL::uuid    AS cand_umbrella_id,
    NULL::boolean AS umb_enabled,
    NULL::text    AS umb_name,
    NULL::text    AS umb_sort_code,
    NULL::text    AS umb_account_number,

    NULL::jsonb AS candidate_hint_text,

    NULL::numeric AS expenses_pay_ex_vat,
    NULL::text    AS expenses_description,
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

    -- refs/PDF baseline fields (used for 'Refs - Timesheet PDF invalid' issue)
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
),
all_rows AS (
  SELECT * FROM ts_base
  UNION ALL
  SELECT * FROM planned_weeks
),
with_issues AS (
  SELECT
    ar.*,

    (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        ARRAY[]::text[] ||
                        CASE WHEN ar.has_rate_issue OR ar.processing_status = 'RATE_MISSING'::ts_fin_processing_status_enum THEN ARRAY['Rate'::text] ELSE ARRAY[]::text[] END
                      ) ||
                      CASE WHEN ar.has_pay_channel_issue OR ar.processing_status = 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum THEN ARRAY['Pay channel'::text] ELSE ARRAY[]::text[] END
                    ) ||
                    CASE
                      WHEN ar.processing_status = 'UNASSIGNED'::ts_fin_processing_status_enum THEN ARRAY['Candidate ID'::text]
                      WHEN ar.processing_status = 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum THEN ARRAY['Client ID'::text]
                      ELSE ARRAY[]::text[]
                    END
                  ) ||
                  CASE WHEN ar.pay_on_hold THEN ARRAY['On hold'::text] ELSE ARRAY[]::text[] END
                ) ||
                -- ✅ SUPPRESS HR crosscheck UI issues when HR validation is required for invoice (Option A)
                CASE
                  WHEN NOT (
                    ar.timesheet_id IS NOT NULL
                    AND COALESCE(ar.client_hr_validation_required, false) = true
                    AND COALESCE(ar.client_no_timesheet_required, false) = false
                    AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
                  )
                  AND (
                    ar.hr_crosscheck_status = 'HOURS_MISMATCH_HR'::text
                    OR ar.hr_crosscheck_issues && ARRAY['HOURS_MISMATCH_HR'::text]
                  )
                    THEN ARRAY['Hours mismatch HR'::text]
                  ELSE ARRAY[]::text[]
                END
              ) ||
              -- ✅ SUPPRESS HR crosscheck UI issues when HR validation is required for invoice (Option A)
              CASE
                WHEN NOT (
                  ar.timesheet_id IS NOT NULL
                  AND COALESCE(ar.client_hr_validation_required, false) = true
                  AND COALESCE(ar.client_no_timesheet_required, false) = false
                  AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
                )
                AND (ar.hr_crosscheck_issues && ARRAY['HR_HOURS_MISSING'::text])
                  THEN ARRAY['HR hours missing'::text]
                ELSE ARRAY[]::text[]
              END
            ) ||
            CASE WHEN ar.hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'::text] THEN ARRAY['Duplicate contracts'::text] ELSE ARRAY[]::text[] END
          ) ||
          CASE
            WHEN ar.timesheet_id IS NOT NULL
              AND ar.client_requires_hr
              AND NOT ar.client_no_timesheet_required
              AND ar.sheet_scope = 'WEEKLY'::timesheet_scope_enum
              AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
              AND NOT (
                COALESCE(ar.evidence_count, 0) > 0
                OR (ar.submission_mode = 'ELECTRONIC'::submission_mode_enum AND ar.r2_nurse_key IS NOT NULL AND ar.r2_auth_key IS NOT NULL)
                OR (ar.submission_mode = 'MANUAL'::submission_mode_enum AND ar.manual_pdf_r2_key IS NOT NULL)
              )
              THEN ARRAY['Timesheet evidence'::text]
            ELSE ARRAY[]::text[]
          END
        ) ||
        CASE
          WHEN ar.timesheet_id IS NOT NULL
            AND (
              (COALESCE(ar.travel_charge_ex_vat, 0) > 0 OR COALESCE(ar.travel_pay_ex_vat, 0) > 0)
              OR (COALESCE(ar.accommodation_charge_ex_vat, 0) > 0 OR COALESCE(ar.accommodation_pay_ex_vat, 0) > 0)
              OR (COALESCE(ar.other_charge_ex_vat, 0) > 0 OR COALESCE(ar.other_pay_ex_vat, 0) > 0)
            )
            AND (
              ((COALESCE(ar.travel_charge_ex_vat, 0) > 0 OR COALESCE(ar.travel_pay_ex_vat, 0) > 0)
                AND NOT EXISTS (
                  SELECT 1 FROM public.timesheet_evidence te
                  WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'TRAVEL'
                )
              )
              OR ((COALESCE(ar.accommodation_charge_ex_vat, 0) > 0 OR COALESCE(ar.accommodation_pay_ex_vat, 0) > 0)
                AND NOT EXISTS (
                  SELECT 1 FROM public.timesheet_evidence te
                  WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'ACCOMMODATION'
                )
              )
              OR ((COALESCE(ar.other_charge_ex_vat, 0) > 0 OR COALESCE(ar.other_pay_ex_vat, 0) > 0)
                AND NOT EXISTS (
                  SELECT 1 FROM public.timesheet_evidence te
                  WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'OTHER'
                )
              )
            )
            THEN ARRAY['Expenses evidence'::text]
          ELSE ARRAY[]::text[]
        END
      ) ||
      CASE
        WHEN ar.timesheet_id IS NOT NULL
          AND (
            COALESCE(ar.mileage_units, 0) > 0
            OR COALESCE(ar.mileage_charge_ex_vat, 0) > 0
            OR COALESCE(ar.mileage_pay_ex_vat, 0) > 0
          )
          AND NOT EXISTS (
            SELECT 1 FROM public.timesheet_evidence te
            WHERE te.timesheet_id = ar.timesheet_id AND upper(te.kind) = 'MILEAGE'
          )
          THEN ARRAY['Mileage evidence'::text]
        ELSE ARRAY[]::text[]
      END
    ) ||
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND ar.sheet_scope = 'DAILY'::timesheet_scope_enum
        AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
        AND (
          COALESCE(ar.require_reference_to_pay, false)
          OR COALESCE(ar.require_reference_to_invoice, false)
          OR ar.client_ts_reference_required
          OR ar.client_pay_reference_required
          OR ar.client_invoice_reference_required
        )
        AND (ar.reference_number IS NULL OR length(btrim(ar.reference_number)) = 0)
        THEN ARRAY['Reference'::text]
      ELSE ARRAY[]::text[]
    END ||
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND ar.sheet_scope = 'WEEKLY'::timesheet_scope_enum
        AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
        AND (
          COALESCE(ar.require_reference_to_pay, false)
          OR COALESCE(ar.require_reference_to_invoice, false)
          OR (
            NOT COALESCE(ar.require_reference_to_pay, false)
            AND NOT COALESCE(ar.require_reference_to_invoice, false)
            AND (ar.client_pay_reference_required OR ar.client_invoice_reference_required OR ar.client_ts_reference_required)
          )
        )
        AND (
          -- SEGMENTS mode (per-shift refs; ignore already-invoiced segments; only positive segments)
          (
            ar.invoice_breakdown_json IS NOT NULL
            AND jsonb_typeof(ar.invoice_breakdown_json) = 'object'
            AND upper(coalesce(ar.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
            AND jsonb_typeof(ar.invoice_breakdown_json->'segments') = 'array'
            AND EXISTS (
              SELECT 1
              FROM jsonb_array_elements(ar.invoice_breakdown_json->'segments') s
              WHERE nullif(btrim(coalesce(s->>'invoice_locked_invoice_id','')), '') IS NULL
                AND (
                  coalesce(nullif(s->>'hours_day','')::numeric,0)
                  + coalesce(nullif(s->>'hours_night','')::numeric,0)
                  + coalesce(nullif(s->>'hours_sat','')::numeric,0)
                  + coalesce(nullif(s->>'hours_sun','')::numeric,0)
                  + coalesce(nullif(s->>'hours_bh','')::numeric,0)
                ) > 0
                AND coalesce(btrim(s->>'ref_num'), '') = ''
            )
          )
          OR
          -- WEEKLY MANUAL (per shift in schedule)
          (
            ar.submission_mode = 'MANUAL'::submission_mode_enum
            AND (
              ar.actual_schedule_json IS NULL
              OR jsonb_typeof(ar.actual_schedule_json) <> 'array'
              OR (
                jsonb_typeof(ar.actual_schedule_json) = 'array'
                AND (
                  jsonb_array_length(ar.actual_schedule_json) = 0
                  OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(ar.actual_schedule_json) seg
                    WHERE
                      coalesce(btrim(seg->>'start'), '') <> ''
                      AND coalesce(btrim(seg->>'end'), '') <> ''
                      AND coalesce(btrim(seg->>'ref_num'), '') = ''
                  )
                )
              )
            )
          )
          OR
          -- WEEKLY non-MANUAL aggregate (at least one freeform/day ref; no legacy timesheet reference)
          (
            ar.submission_mode <> 'MANUAL'::submission_mode_enum
            AND NOT (
              EXISTS (
                SELECT 1
                FROM jsonb_array_elements_text(
                  CASE
                    WHEN ar.day_references_json IS NOT NULL
                      AND jsonb_typeof(ar.day_references_json) = 'object'
                      AND jsonb_typeof(ar.day_references_json->'__freeform_refs') = 'array'
                    THEN ar.day_references_json->'__freeform_refs'
                    WHEN ar.day_references_json IS NOT NULL
                      AND jsonb_typeof(ar.day_references_json) = 'object'
                      AND jsonb_typeof(ar.day_references_json->'__freeform') = 'array'
                    THEN ar.day_references_json->'__freeform'
                    WHEN ar.day_references_json IS NOT NULL
                      AND jsonb_typeof(ar.day_references_json) = 'object'
                      AND jsonb_typeof(ar.day_references_json->'__freeform_lines') = 'array'
                    THEN ar.day_references_json->'__freeform_lines'
                    WHEN ar.day_references_json IS NOT NULL
                      AND jsonb_typeof(ar.day_references_json) = 'array'
                    THEN ar.day_references_json
                    ELSE '[]'::jsonb
                  END
                ) t(x)
                WHERE nullif(btrim(coalesce(t.x,'')), '') IS NOT NULL
              )
              OR
              EXISTS (
                SELECT 1
                FROM jsonb_each_text(
                  CASE
                    WHEN ar.day_references_json IS NOT NULL AND jsonb_typeof(ar.day_references_json) = 'object'
                    THEN ar.day_references_json
                    ELSE '{}'::jsonb
                  END
                ) j(k, v)
                WHERE nullif(btrim(coalesce(j.v,'')), '') IS NOT NULL
                  AND left(coalesce(j.k,''), 2) <> '__'
              )
            )
          )
        )
        THEN ARRAY['Reference'::text]
      ELSE ARRAY[]::text[]
    END ||
    -- ✅ Validation-framework issue labels (pending/failed). No TSFIN HR crosscheck surfaced when required.
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND COALESCE(ar.client_hr_validation_required, false) = true
        AND COALESCE(ar.client_no_timesheet_required, false) = false
        AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
        AND ar.validation_status IS NULL
        THEN ARRAY['Awaiting validation'::text]
      WHEN ar.timesheet_id IS NOT NULL
        AND COALESCE(ar.client_hr_validation_required, false) = true
        AND COALESCE(ar.client_no_timesheet_required, false) = false
        AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
        AND ar.validation_status IS NOT NULL
        AND (ar.validation_status <> ALL (ARRAY['VALIDATION_OK'::validation_status_enum, 'OVERRIDDEN'::validation_status_enum]))
        THEN ARRAY['Validation failed'::text]
      ELSE ARRAY[]::text[]
    END ||
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND ar.client_requires_hr
        AND NOT ar.client_autoprocess_hr
        AND ar.authorised_at_server IS NULL
        THEN ARRAY['Authorisation'::text]
      ELSE ARRAY[]::text[]
    END AS issue_codes

  FROM all_rows ar
)
SELECT
  timesheet_id,
  timesheet_status,
  week_ending_date,
  booking_id,
  occupant_key_norm,
  hospital_norm,
  sheet_scope,
  submission_mode,
  authorised_at_server,
  candidate_id,
  client_id,
  pay_method,
  processing_status,
  basis,
  total_hours,
  total_pay_ex_vat,
  total_charge_ex_vat,
  margin_ex_vat,
  paid_at_utc,
  pay_on_hold,

  (
    timesheet_id IS NOT NULL
    AND paid_at_utc IS NULL
    AND COALESCE(pay_on_hold, false) = false
    AND authorised_at_server IS NOT NULL
    AND processing_status IS NOT NULL
    AND processing_status <> ALL (
      ARRAY[
        'UNASSIGNED'::ts_fin_processing_status_enum,
        'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum,
        'RATE_MISSING'::ts_fin_processing_status_enum,
        'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum
      ]
    )
    AND COALESCE(has_rate_issue, false) = false
    AND COALESCE(has_pay_channel_issue, false) = false
    AND (
      (
        UPPER(COALESCE(cand_pay_method, '')) = 'PAYE'
        AND cand_sort_code IS NOT NULL AND length(btrim(cand_sort_code)) > 0
        AND cand_account_number IS NOT NULL AND length(btrim(cand_account_number)) > 0
        AND cand_account_holder IS NOT NULL AND length(btrim(cand_account_holder)) > 0
      )
      OR
      (
        UPPER(COALESCE(cand_pay_method, '')) = 'UMBRELLA'
        AND COALESCE(umb_enabled, false) = true
        AND umb_name IS NOT NULL AND length(btrim(umb_name)) > 0
        AND umb_sort_code IS NOT NULL AND length(btrim(umb_sort_code)) > 0
        AND umb_account_number IS NOT NULL AND length(btrim(umb_account_number)) > 0
      )
    )
    AND (
      COALESCE(require_reference_to_pay, false) = false
      OR COALESCE(total_hours, 0::numeric) <= 0::numeric
      OR (
        CASE
          WHEN invoice_breakdown_json IS NOT NULL
            AND jsonb_typeof(invoice_breakdown_json) = 'object'
            AND upper(coalesce(invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
            AND jsonb_typeof(invoice_breakdown_json->'segments') = 'array'
          THEN NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements(invoice_breakdown_json->'segments') s
            WHERE (
              coalesce(nullif(s->>'hours_day','')::numeric,0)
              + coalesce(nullif(s->>'hours_night','')::numeric,0)
              + coalesce(nullif(s->>'hours_sat','')::numeric,0)
              + coalesce(nullif(s->>'hours_sun','')::numeric,0)
              + coalesce(nullif(s->>'hours_bh','')::numeric,0)
            ) > 0
              AND coalesce(btrim(s->>'ref_num'), '') = ''
          )
          ELSE
            CASE
              WHEN sheet_scope = 'DAILY'::timesheet_scope_enum THEN
                (reference_number IS NOT NULL AND length(btrim(reference_number)) > 0)
              WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum THEN
                CASE
                  WHEN submission_mode = 'MANUAL'::submission_mode_enum THEN
                    (
                      actual_schedule_json IS NOT NULL
                      AND jsonb_typeof(actual_schedule_json) = 'array'
                      AND jsonb_array_length(actual_schedule_json) > 0
                      AND NOT EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(actual_schedule_json) seg
                        WHERE
                          coalesce(btrim(seg->>'start'), '') <> ''
                          AND coalesce(btrim(seg->>'end'), '') <> ''
                          AND coalesce(btrim(seg->>'ref_num'), '') = ''
                      )
                    )
                  ELSE
                    (
                      EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(
                          CASE
                            WHEN day_references_json IS NOT NULL
                              AND jsonb_typeof(day_references_json) = 'object'
                              AND jsonb_typeof(day_references_json->'__freeform_refs') = 'array'
                            THEN day_references_json->'__freeform_refs'
                            WHEN day_references_json IS NOT NULL
                              AND jsonb_typeof(day_references_json) = 'object'
                              AND jsonb_typeof(day_references_json->'__freeform') = 'array'
                            THEN day_references_json->'__freeform'
                            WHEN day_references_json IS NOT NULL
                              AND jsonb_typeof(day_references_json) = 'object'
                              AND jsonb_typeof(day_references_json->'__freeform_lines') = 'array'
                            THEN day_references_json->'__freeform_lines'
                            WHEN day_references_json IS NOT NULL
                              AND jsonb_typeof(day_references_json) = 'array'
                            THEN day_references_json
                            ELSE '[]'::jsonb
                          END
                        ) t(x)
                        WHERE nullif(btrim(coalesce(t.x,'')), '') IS NOT NULL
                      )
                      OR (
                        day_references_json IS NOT NULL
                        AND jsonb_typeof(day_references_json) = 'object'
                        AND EXISTS (
                          SELECT 1
                          FROM jsonb_each_text(day_references_json) j(k, v)
                          WHERE nullif(btrim(coalesce(j.v,'')), '') IS NOT NULL
                            AND left(coalesce(j.k,''), 2) <> '__'
                        )
                      )
                    )
                END
              ELSE
                (reference_number IS NOT NULL AND length(btrim(reference_number)) > 0)
            END
        END
      )
    )
  ) AS ready_to_pay,

  locked_by_invoice_id,
  candidate_name,
  client_name,
  nhsp_shift_count,
  nhsp_shift_included_count,
  nhsp_shift_deferred_count,
  validation_status,

  CASE
    WHEN timesheet_id IS NULL THEN
      CASE contract_week_status
        WHEN 'PLANNED'::contract_week_status_enum THEN 'PLANNED'
        WHEN 'OPEN'::contract_week_status_enum THEN 'PLANNED'
        WHEN 'SUBMITTED'::contract_week_status_enum THEN 'PENDING_AUTH'
        WHEN 'AUTHORISED'::contract_week_status_enum THEN 'READY_FOR_INVOICE'
        WHEN 'INVOICED'::contract_week_status_enum THEN 'INVOICED'
        WHEN 'CANCELLED'::contract_week_status_enum THEN 'NEEDS_ATTENTION'
        ELSE 'UNKNOWN'
      END
    WHEN paid_at_utc IS NOT NULL THEN 'PAID'
    WHEN (
      locked_by_invoice_id IS NOT NULL
      OR (
        seg.seg_total IS NOT NULL
        AND seg.seg_total > 0
        AND COALESCE(seg.seg_locked,0) >= seg.seg_total
      )
    ) THEN 'INVOICED'
    WHEN timesheet_id IS NOT NULL
      AND qr_status = 'PENDING'::timesheet_qr_status_enum
      AND (qr_token IS NULL OR length(btrim(qr_token)) = 0)
      AND qr_generated_at IS NULL
      THEN 'QR_NOT_ISSUED'
    WHEN timesheet_id IS NOT NULL
      AND qr_status = 'PENDING'::timesheet_qr_status_enum
      AND (qr_token IS NOT NULL AND length(btrim(qr_token)) > 0)
      AND qr_generated_at IS NOT NULL
      AND qr_scanned_at IS NULL
      THEN 'QR_ISSUED_AWAITING_SIGNATURE'
    WHEN processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum THEN 'READY_FOR_INVOICE'
    WHEN processing_status = 'READY_FOR_HR'::ts_fin_processing_status_enum THEN 'READY_FOR_HR'
    WHEN processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum THEN 'PENDING_AUTH'
    WHEN processing_status = ANY (
      ARRAY[
        'UNASSIGNED'::ts_fin_processing_status_enum,
        'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum,
        'RATE_MISSING'::ts_fin_processing_status_enum,
        'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum
      ]
    ) THEN 'NEEDS_ATTENTION'
    ELSE 'UNKNOWN'
  END AS summary_stage,

  CASE
    WHEN sheet_scope = 'DAILY'::timesheet_scope_enum AND submission_mode = 'ELECTRONIC'::submission_mode_enum THEN 'DAILY_ELECTRONIC'
    WHEN sheet_scope = 'DAILY'::timesheet_scope_enum AND submission_mode = 'MANUAL'::submission_mode_enum THEN 'DAILY_MANUAL'
    WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum AND client_autoprocess_hr IS TRUE THEN 'WEEKLY_HEALTHROSTER'
    WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum AND basis = 'NHSP_ADJUSTMENT'::timesheet_fin_basis_enum THEN 'WEEKLY_NHSP_ADJUSTMENT'
    WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum AND basis = 'NHSP'::timesheet_fin_basis_enum THEN 'WEEKLY_NHSP'
    WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum AND client_is_nhsp IS TRUE THEN 'WEEKLY_NHSP'
    WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum AND submission_mode = 'ELECTRONIC'::submission_mode_enum THEN 'WEEKLY_ELECTRONIC'
    WHEN sheet_scope = 'WEEKLY'::timesheet_scope_enum AND submission_mode = 'MANUAL'::submission_mode_enum THEN 'WEEKLY_MANUAL'
    ELSE 'UNKNOWN'
  END AS route_type,

  contract_week_id,
  contract_week_ending_date,
  contract_week_status,
  additional_seq,
  is_adjustment,

  qr_status,

  pay_adjustment_count,
  pay_adjustment_count > 0 AS has_pay_adjustments,
  COALESCE(is_adjustment, false) OR pay_adjustment_count > 0 AS is_adjusted,
  qr_status IS NOT NULL AS is_qr,

  (processing_status = ANY (
      ARRAY[
        'UNASSIGNED'::ts_fin_processing_status_enum,
        'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum,
        'RATE_MISSING'::ts_fin_processing_status_enum,
        'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum
      ]
    )
  )
  OR (
    NOT (
      wi.timesheet_id IS NOT NULL
      AND COALESCE(wi.client_hr_validation_required, false) = true
      AND COALESCE(wi.client_no_timesheet_required, false) = false
      AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric
    )
    AND (hr_crosscheck_status IS NOT NULL AND hr_crosscheck_status <> 'OK')
  )
  OR (
    NOT (
      wi.timesheet_id IS NOT NULL
      AND COALESCE(wi.client_hr_validation_required, false) = true
      AND COALESCE(wi.client_no_timesheet_required, false) = false
      AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric
    )
    AND (hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'])
  )
  OR (ic.issue_codes_final IS NOT NULL AND array_length(ic.issue_codes_final, 1) > 0) AS needs_attention,

  client_autoprocess_hr,
  has_rate_issue,
  has_pay_channel_issue,
  hr_crosscheck_status,
  hr_crosscheck_issues,
  external_source_rows_json,
  ic.issue_codes_final AS issue_codes,

  client_requires_hr,
  client_no_timesheet_required,
  client_is_nhsp,

  client_pay_reference_required,
  client_invoice_reference_required,
  client_hr_validation_required,
  client_ts_reference_required,

  require_reference_to_pay,
  require_reference_to_invoice,

  qr_token,
  qr_generated_at,
  qr_scanned_at,

  candidate_hint_text,

  expenses_pay_ex_vat,
  expenses_description,
  mileage_units,
  mileage_pay_rate,
  mileage_charge_rate,
  mileage_pay_ex_vat,

  travel_pay_ex_vat,
  travel_charge_ex_vat,
  accommodation_pay_ex_vat,
  accommodation_charge_ex_vat,
  other_pay_ex_vat,
  other_charge_ex_vat,

  -- ✅ NEW (APPENDED AT END): policy flag for “HR validation required before invoicing”
  (
    timesheet_id IS NOT NULL
    AND COALESCE(client_hr_validation_required, false) = true
    AND COALESCE(client_no_timesheet_required, false) = false
    AND COALESCE(total_hours, 0::numeric) > 0::numeric
  ) AS hr_validation_required_for_invoice,

  -- ✅ NEW (APPENDED AT END): segment-aware invoice stage indicators
  seg.seg_total    AS invoice_segments_total,
  seg.seg_locked   AS invoice_segments_locked,
  CASE
    WHEN seg.seg_total IS NULL THEN NULL::int
    ELSE GREATEST(seg.seg_total - COALESCE(seg.seg_locked,0), 0)
  END AS invoice_segments_unlocked,
  CASE
    WHEN seg.seg_total IS NULL THEN NULL::text
    WHEN COALESCE(seg.seg_locked,0) = 0 THEN 'NOT_INVOICED'
    WHEN COALESCE(seg.seg_locked,0) >= seg.seg_total THEN 'FULLY_INVOICED'
    ELSE 'PARTIALLY_INVOICED'
  END AS invoice_segment_stage,

  -- ✅ NEW (APPENDED AT END): canonical Tools Stage (pipeline) — 5 mutually exclusive states
  CASE
    WHEN timesheet_id IS NULL THEN 'UNPROCESSED'
    WHEN (
      locked_by_invoice_id IS NOT NULL
      OR COALESCE(seg.seg_locked,0) > 0
      OR (
        seg.seg_total IS NOT NULL
        AND COALESCE(seg.seg_locked,0) > 0
      )
    ) THEN 'INVOICED'
    WHEN (
      timesheet_id IS NOT NULL
      AND COALESCE(client_requires_hr,false) = true
      AND COALESCE(client_autoprocess_hr,false) = false
      AND authorised_at_server IS NULL
      AND array_length(ic.issue_codes_final, 1) = 1
      AND ic.issue_codes_final @> ARRAY['Authorisation'::text]
    ) THEN 'AWAITING_AUTHORISATION'
    WHEN (
      timesheet_id IS NOT NULL
      AND COALESCE(array_length(ic.issue_codes_final, 1), 0) = 0
      AND (
        authorised_at_server IS NOT NULL
        OR NOT (COALESCE(client_requires_hr,false) = true AND COALESCE(client_autoprocess_hr,false) = false)
      )
    ) THEN 'AUTHORISED_FOR_INVOICING'
    ELSE 'PROCESSING_DELAYED'
  END AS tools_stage,

  -- ✅ NEW (APPENDED AT END): user-facing Processing Status label (derived from Tools Stage, not TSFIN.processing_status)
  CASE
    WHEN timesheet_id IS NULL THEN 'Unprocessed'
    WHEN (
      locked_by_invoice_id IS NOT NULL
      OR COALESCE(seg.seg_locked,0) > 0
      OR (
        seg.seg_total IS NOT NULL
        AND COALESCE(seg.seg_locked,0) > 0
      )
    ) THEN (
      CASE
        WHEN seg.seg_total IS NOT NULL
          AND COALESCE(seg.seg_locked,0) > 0
          AND COALESCE(seg.seg_locked,0) < seg.seg_total
          THEN 'Partially Invoiced'
        ELSE 'Invoiced'
      END
    )
    WHEN (
      timesheet_id IS NOT NULL
      AND COALESCE(client_requires_hr,false) = true
      AND COALESCE(client_autoprocess_hr,false) = false
      AND authorised_at_server IS NULL
      AND array_length(ic.issue_codes_final, 1) = 1
      AND ic.issue_codes_final @> ARRAY['Authorisation'::text]
    ) THEN 'Awaiting Authorisation'
    WHEN (
      timesheet_id IS NOT NULL
      AND COALESCE(array_length(ic.issue_codes_final, 1), 0) = 0
      AND (
        authorised_at_server IS NOT NULL
        OR NOT (COALESCE(client_requires_hr,false) = true AND COALESCE(client_autoprocess_hr,false) = false)
      )
    ) THEN 'Authorised for Invoicing'
    ELSE 'Processing Delayed'
  END AS processing_status_display,

  -- ✅ NEW (APPENDED AT END): invoice-paid indicator (true if ANY linked invoice is PAID)
  COALESCE(seg.invoice_paid_any, false) AS invoice_is_paid,

  -- ✅ NEW (APPENDED AT END): reference blockers for UI badges
  (CASE WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE' THEN true ELSE false END) AS refs_block_invoicing,
  (CASE WHEN wi.timesheet_id IS NOT NULL AND COALESCE(pc.issue_missing_reference, false) = true THEN true ELSE false END) AS refs_block_issuing_invoices,
  (CASE WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE' AND COALESCE(pc.issue_missing_reference, false) = true THEN true ELSE false END) AS refs_block_invoice_and_issuing

FROM with_issues wi
LEFT JOIN LATERAL (
  SELECT public.timesheet_pdf_reference_sig(wi.timesheet_id) AS current_refs_sig
) rs ON true
LEFT JOIN LATERAL (
  SELECT
    pc0.precheck_status,
    pc0.issue_missing_reference,
    pc0.issue_missing_reference_count
  FROM public.v_ts_invoice_precheck pc0
  WHERE pc0.timesheet_id = wi.timesheet_id
  LIMIT 1
) pc ON true

LEFT JOIN LATERAL (
  SELECT
    (
      CASE
        WHEN wi.timesheet_id IS NOT NULL
          AND (
            pc.precheck_status = 'BLOCK_NO_REFERENCE'
            OR (COALESCE(wi.issue_codes, ARRAY[]::text[]) @> ARRAY['Reference'::text])
          )
          AND COALESCE(pc.issue_missing_reference, false) = true
          THEN ARRAY['Refs (Invoice and Issue Blocked)'::text]
        WHEN wi.timesheet_id IS NOT NULL
          AND (
            pc.precheck_status = 'BLOCK_NO_REFERENCE'
            OR (COALESCE(wi.issue_codes, ARRAY[]::text[]) @> ARRAY['Reference'::text])
          )
          THEN ARRAY['Refs (Invoicing Blocked)'::text]
        WHEN wi.timesheet_id IS NOT NULL
          AND COALESCE(pc.issue_missing_reference, false) = true
          THEN ARRAY['Refs (Issue Invoice Blocked)'::text]
        ELSE ARRAY[]::text[]
      END
      ||
      array_remove(COALESCE(wi.issue_codes, ARRAY[]::text[]), 'Reference'::text)
      ||
      (
        CASE
          WHEN wi.timesheet_id IS NOT NULL
            AND COALESCE(wi.client_no_timesheet_required, false) = false
            AND COALESCE(wi.client_is_nhsp, false) = false
            AND (
              (
                wi.submission_mode = 'ELECTRONIC'::submission_mode_enum
                AND wi.manual_pdf_r2_key IS NULL
                AND wi.generated_pdf_at_utc IS NOT NULL
                AND (
                  wi.generated_pdf_refs_sig IS NULL
                  OR (rs.current_refs_sig IS NOT NULL AND wi.generated_pdf_refs_sig <> rs.current_refs_sig)
                )
              )
              OR
              (
                (
                  (wi.qr_token IS NOT NULL AND wi.qr_generated_at IS NOT NULL)
                  OR wi.qr_last_sent_hash IS NOT NULL
                )
                AND wi.qr_sent_refs_sig IS NOT NULL
                AND (rs.current_refs_sig IS NOT NULL AND wi.qr_sent_refs_sig <> rs.current_refs_sig)
              )
            )
          THEN ARRAY['Refs - Timesheet PDF invalid'::text]
          ELSE ARRAY[]::text[]
        END
      )
    ) AS issue_codes_final
) ic ON true

LEFT JOIN LATERAL (
  SELECT
    -- Segment stats derived from current TSFIN snapshot JSON (when present)
    CASE
      WHEN wi.timesheet_id IS NULL THEN NULL::int
      WHEN tf.invoice_breakdown_json IS NOT NULL
        AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'
        AND coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
        AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
      THEN jsonb_array_length(tf.invoice_breakdown_json->'segments')
      ELSE 1
    END AS seg_total,

    CASE
      WHEN wi.timesheet_id IS NULL THEN NULL::int
      WHEN tf.invoice_breakdown_json IS NOT NULL
        AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'
        AND coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
        AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
      THEN (
        SELECT count(*)::int
        FROM jsonb_array_elements(tf.invoice_breakdown_json->'segments') s
        WHERE nullif(btrim(coalesce(s->>'invoice_locked_invoice_id','')), '') IS NOT NULL
      )
      ELSE (CASE WHEN wi.locked_by_invoice_id IS NULL THEN 0 ELSE 1 END)
    END AS seg_locked,

    CASE
      WHEN wi.timesheet_id IS NULL THEN NULL::boolean
      WHEN tf.invoice_breakdown_json IS NOT NULL
        AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'
        AND coalesce(tf.invoice_breakdown_json->>'mode','') = 'SEGMENTS'
        AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
      THEN EXISTS (
        SELECT 1
        FROM jsonb_array_elements(tf.invoice_breakdown_json->'segments') s
        JOIN public.invoices inv2
          ON inv2.id = (
            CASE
              WHEN nullif(btrim(coalesce(s->>'invoice_locked_invoice_id','')), '') IS NOT NULL
                AND (s->>'invoice_locked_invoice_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              THEN (s->>'invoice_locked_invoice_id')::uuid
              ELSE NULL::uuid
            END
          )
        WHERE inv2.status = 'PAID'::public.invoice_status_enum
      )
      ELSE EXISTS (
        SELECT 1
        FROM public.invoices inv2
        WHERE inv2.id = wi.locked_by_invoice_id
          AND inv2.status = 'PAID'::public.invoice_status_enum
      )
    END AS invoice_paid_any
  FROM public.timesheets_financials tf
  WHERE tf.is_current = true
    AND tf.timesheet_id = wi.timesheet_id
  ORDER BY tf.created_at DESC
  LIMIT 1
) seg ON true;




-- ============================================================
-- UPDATE VIEW: public.v_timesheets_summary
-- FIXES:
--  - Keep existing column order (so CREATE OR REPLACE is allowed)
--  - Fix is_adjustment / is_adjusted to use timesheets.is_adjustment (ts2)
-- ADDS:
--  - route_display (APPENDED AT END ONLY)
-- ============================================================

create or replace view public.v_timesheets_summary as
select
  v.timesheet_id,
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

  -- ✅ FIX (same column name, same position): prefer timesheets.is_adjustment
  coalesce(ts2.is_adjustment, v.is_adjustment, false) as is_adjustment,

  v.qr_status,
  v.pay_adjustment_count,
  v.has_pay_adjustments,

  -- ✅ FIX (same column name, same position): keep is_adjusted consistent
  (coalesce(ts2.is_adjustment, v.is_adjustment, false) = true)
    or coalesce(v.has_pay_adjustments, false) as is_adjusted,

  v.is_qr,
  v.needs_attention,
  v.client_autoprocess_hr,
  v.has_rate_issue,
  v.has_pay_channel_issue,
  v.hr_crosscheck_status,
  v.hr_crosscheck_issues,
  v.external_source_rows_json,
  v.issue_codes,

  -- contract_id kept in same position (resolved for DAILY too)
  coalesce(ts2.contract_id, cw.contract_id) as contract_id,

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

  -- existing appended column
  case
    when v.locked_by_invoice_id is null then null
    when inv.id is null then 'INVOICED_NOT_ISSUED'
    when inv.status in ('ISSUED'::public.invoice_status_enum, 'PAID'::public.invoice_status_enum) then 'INVOICED_ISSUED'
    else 'INVOICED_NOT_ISSUED'
  end as invoice_issue_stage,

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

  -- ✅ NEW (APPENDED AT END ONLY): route_display (safe to add now)
  case
    when coalesce(ts2.is_adjustment, v.is_adjustment, false) = true then
      case
        when upper(coalesce(ts2.adjustment_origin, '')) = 'MANUAL_ADJUSTMENT' then 'Manual Adjustment'
        when upper(coalesce(ts2.adjustment_origin, '')) like 'IMPORT_%'
          or ts2.correction_kind is not null
          or ts2.correction_id is not null
          then 'NHSP Adjustment'
        else
          -- Back-compat for historical manual adjustments where adjustment_origin is NULL
          'Manual Adjustment'
      end
    else
      case
        when v.route_type = 'DAILY_ELECTRONIC' then 'Daily Electronic'
        when v.route_type = 'DAILY_MANUAL' then 'Daily Manual'
        when v.route_type = 'WEEKLY_ELECTRONIC' then 'Weekly Electronic'
        when v.route_type = 'WEEKLY_MANUAL' then 'Weekly Manual'
        when v.route_type = 'WEEKLY_NHSP' then 'Weekly NHSP'
        when v.route_type = 'WEEKLY_NHSP_ADJUSTMENT' then 'Weekly NHSP'
        when v.route_type = 'WEEKLY_HEALTHROSTER' then 'Weekly HealthRoster'
        else 'Unknown'
      end
  end as route_display

from public.v_timesheets_summary_base v
left join public.contract_weeks cw
  on cw.id = v.contract_week_id
left join public.timesheets ts2
  on ts2.timesheet_id = v.timesheet_id
left join public.invoices inv
  on inv.id = v.locked_by_invoice_id
;

select pg_notify('pgrst', 'reload schema');


GRANT SELECT ON public.v_timesheets_summary_base TO service_role;
GRANT SELECT ON public.v_timesheets_summary      TO service_role;
GRANT SELECT ON public.v_timesheets_summary_base TO authenticated;
GRANT SELECT ON public.v_timesheets_summary      TO authenticated;


-- ============================================================
-- v_timesheets_details
-- ✅ SAFE TO RE-RUN: CREATE OR REPLACE VIEW (idempotent)
-- ✅ IMPORTANT: ONLY appends new columns at the END. No other changes.
-- ============================================================

CREATE OR REPLACE VIEW public.v_timesheets_details AS
WITH nhsp_agg AS (
  SELECT
    s.timesheet_id,
    count(*) as nhsp_shift_count,
    count(*) filter (where s.invoice_status = 'INCLUDED'::text) as nhsp_shift_included_count,
    count(*) filter (where s.invoice_status = 'DEFERRED'::text) as nhsp_shift_deferred_count
  FROM nhsp_shifts s
  WHERE s.timesheet_id is not null
  GROUP BY s.timesheet_id
)
SELECT
  t.timesheet_id,
  t.booking_id,
  t.contract_id,
  tf.candidate_id,
  tf.client_id,
  t.week_ending_date,
  t.sheet_scope,
  t.submission_mode,
  t.status as timesheet_status,
  t.reference_number,
  t.occupant_key_norm,
  t.hospital_norm,
  t.ward_norm,
  t.job_title_norm,
  t.shift_label_norm,
  t.authorised_at_server,
  tf.id as tsfin_id,
  tf.basis as tsfin_basis,
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
  tv.status as validation_status,
  tv.reason_code as validation_reason_code,
  tv.hr_request_id,
  tv.validated_at_utc,
  tv.last_source as validation_last_source_import_id,
  n.nhsp_shift_count,
  n.nhsp_shift_included_count,
  n.nhsp_shift_deferred_count,

  -- existing appended columns (already in your view)
  tf.expenses_description,
  tf.expenses_evidence_r2_key,
  tf.expenses_evidence_manifest,
  tf.mileage_units,
  tf.mileage_pay_rate,
  tf.mileage_charge_rate,
  tf.mileage_evidence_r2_key,
  tf.mileage_evidence_manifest,

  -- ✅ NEW columns APPENDED AT END
  tf.travel_pay_ex_vat,
  tf.travel_charge_ex_vat,
  tf.accommodation_pay_ex_vat,
  tf.accommodation_charge_ex_vat,
  tf.other_pay_ex_vat,
  tf.other_charge_ex_vat,

  -- ✅ NEW (APPENDED AT END): reference blockers for UI badges
  (CASE WHEN t.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE' THEN true ELSE false END) AS refs_block_invoicing,
  (CASE WHEN t.timesheet_id IS NOT NULL AND COALESCE(pc.issue_missing_reference, false) = true THEN true ELSE false END) AS refs_block_issuing_invoices,
  (CASE WHEN t.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE' AND COALESCE(pc.issue_missing_reference, false) = true THEN true ELSE false END) AS refs_block_invoice_and_issuing,

  -- ✅ NEW (APPENDED AT END): missing booking reference detail for Timesheet Issues tab
  refsdet.refs_missing_items_count AS refs_missing_items_count,
  refsdet.refs_missing_items_json  AS refs_missing_items_json


FROM timesheets t
LEFT JOIN timesheets_financials tf
  ON tf.timesheet_id = t.timesheet_id
 AND tf.is_current = true
LEFT JOIN timesheet_validations tv
  ON tv.timesheet_id = t.timesheet_id
LEFT JOIN nhsp_agg n
  ON n.timesheet_id = t.timesheet_id
LEFT JOIN public.v_ts_invoice_precheck pc
  ON pc.timesheet_id = t.timesheet_id
LEFT JOIN LATERAL (
  SELECT
    x.items AS refs_missing_items_json,
    COALESCE(jsonb_array_length(x.items), 0) AS refs_missing_items_count
  FROM (
    SELECT
      CASE
        WHEN t.timesheet_id IS NOT NULL
          AND (
            pc.precheck_status = 'BLOCK_NO_REFERENCE'
            OR COALESCE(pc.issue_missing_reference, false) = true
          )
        THEN COALESCE((
          SELECT jsonb_agg(item ORDER BY
            (item->>'day_ymd') NULLS LAST,
            (item->>'kind') NULLS LAST,
            COALESCE(NULLIF(item->>'segment_index','')::int, 0)
          )
          FROM (
            -- DAILY: timesheet-level reference number
            SELECT jsonb_build_object(
              'kind', 'TIMESHEET',
              'day_ymd', to_char(((COALESCE(t.worked_start_iso, t.scheduled_start_iso))::timestamptz AT TIME ZONE 'Europe/London')::date, 'YYYY-MM-DD'),
              'start_utc', COALESCE(t.worked_start_iso, t.scheduled_start_iso),
              'end_utc', COALESCE(t.worked_end_iso, t.scheduled_end_iso),
              'current_reference', NULLIF(btrim(COALESCE(t.reference_number, '')), '')
            ) AS item
            WHERE t.sheet_scope = 'DAILY'::timesheet_scope_enum
              AND (t.reference_number IS NULL OR btrim(t.reference_number) = '')

            UNION ALL

            -- WEEKLY FREEFORM: day_references_json keys with blank values (ignore __meta keys)
            SELECT jsonb_build_object(
              'kind', 'FREEFORM',
              'day_ymd', j.k,
              'current_reference', NULLIF(btrim(COALESCE(j.v, '')), '')
            ) AS item
            FROM jsonb_each_text(
              CASE
                WHEN t.day_references_json IS NOT NULL AND jsonb_typeof(t.day_references_json) = 'object'
                THEN t.day_references_json
                ELSE '{}'::jsonb
              END
            ) j(k, v)
            WHERE left(COALESCE(j.k,''), 2) <> '__'
              AND (
                j.v IS NULL
                OR btrim(j.v) = ''
                OR lower(btrim(j.v)) = 'null'
              )

            UNION ALL

            -- SEGMENTS mode (NHSP/HealthRoster/import): invoice_breakdown_json.segments missing ref_num.
            -- Supports multiple shifts per day via segment_id (preferred) and start/end.
            SELECT jsonb_build_object(
              'kind', 'SEGMENT',
              'segment_index', (s.ord - 1),
              'segment_id', NULLIF(btrim(COALESCE(s.seg->>'segment_id','')), ''),
              'day_ymd', NULLIF(btrim(COALESCE(s.seg->>'date','')), ''),
              'start_utc', NULLIF(btrim(COALESCE(s.seg->>'start_utc','')), ''),
              'end_utc', NULLIF(btrim(COALESCE(s.seg->>'end_utc','')), ''),
              'start', NULLIF(btrim(COALESCE(s.seg->>'start_utc','')), ''),
              'end', NULLIF(btrim(COALESCE(s.seg->>'end_utc','')), ''),
              'current_reference', NULLIF(btrim(COALESCE(s.seg->>'ref_num','')), ''),
              'locked_by_invoice_id', NULLIF(btrim(COALESCE(s.seg->>'invoice_locked_invoice_id','')), ''),
              'scope', CASE
                WHEN NULLIF(btrim(COALESCE(s.seg->>'invoice_locked_invoice_id','')), '') IS NULL THEN 'INVOICING'
                ELSE 'ISSUING'
              END
            ) AS item
            FROM jsonb_array_elements(
              CASE
                WHEN tf.invoice_breakdown_json IS NOT NULL
                  AND jsonb_typeof(tf.invoice_breakdown_json) = 'object'
                  AND upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
                  AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
                THEN tf.invoice_breakdown_json->'segments'
                ELSE '[]'::jsonb
              END
            ) WITH ORDINALITY s(seg, ord)
            WHERE jsonb_typeof(s.seg) = 'object'
              AND (
                (
                  COALESCE(NULLIF(s.seg->>'hours_day','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_night','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_sat','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_sun','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_bh','')::numeric, 0)
                ) > 0
                OR COALESCE(NULLIF(s.seg->>'charge_amount','')::numeric, 0) > 0
              )
              AND COALESCE(btrim(COALESCE(s.seg->>'ref_num','')), '') = ''
              AND (
                (
                  pc.precheck_status = 'BLOCK_NO_REFERENCE'
                  AND NULLIF(btrim(COALESCE(s.seg->>'invoice_locked_invoice_id','')), '') IS NULL
                )
                OR
                (
                  COALESCE(pc.issue_missing_reference, false) = true
                  AND NULLIF(btrim(COALESCE(s.seg->>'invoice_locked_invoice_id','')), '') IS NOT NULL
                )
              )

            UNION ALL

            -- WEEKLY MANUAL (and other schedule-based): actual_schedule_json entries missing ref_num
            SELECT jsonb_build_object(
              'kind', 'SEGMENT',
              'segment_index', (s.ord - 1),
              'day_ymd', CASE
                WHEN (s.seg ? 'date_ymd') THEN NULLIF(btrim(COALESCE(s.seg->>'date_ymd','')), '')
                WHEN substring(COALESCE(s.seg->>'start','') from 1 for 10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                  THEN substring(COALESCE(s.seg->>'start','') from 1 for 10)
                ELSE NULL
              END,
              'start', NULLIF(btrim(COALESCE(s.seg->>'start','')), ''),
              'end', NULLIF(btrim(COALESCE(s.seg->>'end','')), ''),
              'current_reference', NULLIF(btrim(COALESCE(s.seg->>'ref_num','')), '')
            ) AS item
            FROM jsonb_array_elements(
              CASE
                WHEN t.actual_schedule_json IS NOT NULL AND jsonb_typeof(t.actual_schedule_json) = 'array'
                THEN t.actual_schedule_json
                ELSE '[]'::jsonb
              END
            ) WITH ORDINALITY s(seg, ord)
            WHERE COALESCE(btrim(COALESCE(s.seg->>'start','')), '') <> ''
              AND COALESCE(btrim(COALESCE(s.seg->>'end','')), '') <> ''
              AND COALESCE(btrim(COALESCE(s.seg->>'ref_num','')), '') = ''
              AND (
                NOT (
                  (s.seg ? 'hours_day')
                  OR (s.seg ? 'hours_night')
                  OR (s.seg ? 'hours_sat')
                  OR (s.seg ? 'hours_sun')
                  OR (s.seg ? 'hours_bh')
                )
                OR (
                  COALESCE(NULLIF(s.seg->>'hours_day','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_night','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_sat','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_sun','')::numeric, 0)
                  + COALESCE(NULLIF(s.seg->>'hours_bh','')::numeric, 0)
                ) > 0
              )
          ) q
        ), '[]'::jsonb)
        ELSE '[]'::jsonb
      END AS items
  ) x
) refsdet ON true;


GRANT SELECT ON public.v_timesheets_details TO service_role;
GRANT SELECT ON public.v_timesheets_details TO authenticated;

-- Ensure PostgREST sees new columns immediately after commit
SELECT pg_notify('pgrst', 'reload schema');

