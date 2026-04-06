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
        AND (ar.validation_status IS NULL OR ar.validation_status = 'PENDING'::validation_status_enum)
        THEN ARRAY['Awaiting validation'::text]
      WHEN ar.timesheet_id IS NOT NULL
        AND COALESCE(ar.client_hr_validation_required, false) = true
        AND COALESCE(ar.client_no_timesheet_required, false) = false
        AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
        AND ar.validation_status IS NOT NULL
        AND (ar.validation_status <> ALL (ARRAY['VALIDATION_OK'::validation_status_enum, 'OVERRIDDEN'::validation_status_enum, 'PENDING'::validation_status_enum]))
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
    END ||
    -- ✅ NEW: Awaiting signed QR timesheet (hours received; issued proof exists; not scanned yet)
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND ar.qr_status = 'PENDING'::timesheet_qr_status_enum
        AND (
          (
            ar.qr_token IS NOT NULL
            AND length(btrim(ar.qr_token)) > 0
            AND ar.qr_generated_at IS NOT NULL
          )
          OR ar.qr_last_sent_hash IS NOT NULL
        )
        AND ar.qr_scanned_at IS NULL
        AND COALESCE(ar.total_hours, 0::numeric) > 0::numeric
        THEN ARRAY['Awaiting signed QR timesheet'::text]
      ELSE ARRAY[]::text[]
    END AS issue_codes

  FROM all_rows ar
)
,
pay_ts AS (
  SELECT DISTINCT
    wi.timesheet_id,
    COALESCE(wi.pay_on_hold, false) AS pay_on_hold,
    wi.invoice_breakdown_json
  FROM with_issues wi
  WHERE wi.timesheet_id IS NOT NULL
),
pay_is_seg AS (
  SELECT
    pt.timesheet_id,
    (
      pt.invoice_breakdown_json IS NOT NULL
      AND jsonb_typeof(pt.invoice_breakdown_json) = 'object'
      AND upper(coalesce(pt.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
      AND jsonb_typeof(pt.invoice_breakdown_json->'segments') = 'array'
    ) AS is_segments_mode
  FROM pay_ts pt
),
pay_components AS (
  SELECT
    pt.timesheet_id,
    nullif(btrim(coalesce(seg->>'segment_id','')), '') AS component_id,
    COALESCE(NULLIF(seg->>'exclude_from_pay','')::boolean, false) AS is_on_hold
  FROM pay_ts pt
  JOIN pay_is_seg ps
    ON ps.timesheet_id = pt.timesheet_id
  JOIN LATERAL jsonb_array_elements(coalesce(pt.invoice_breakdown_json->'segments','[]'::jsonb)) AS seg ON true
  WHERE ps.is_segments_mode = true
    AND seg IS NOT NULL
    AND jsonb_typeof(seg) = 'object'
    AND nullif(btrim(coalesce(seg->>'segment_id','')), '') IS NOT NULL

  UNION ALL

  SELECT
    pt.timesheet_id,
    'TOTAL'::text AS component_id,
    pt.pay_on_hold AS is_on_hold
  FROM pay_ts pt
  JOIN pay_is_seg ps
    ON ps.timesheet_id = pt.timesheet_id
  WHERE ps.is_segments_mode = false
),
pay_items AS (
  SELECT
    pbi.timesheet_id,
    CASE
      WHEN nullif(btrim(coalesce(pbi.segment_key,'')), '') IS NOT NULL
        THEN nullif(btrim(coalesce(pbi.segment_key,'')), '')
      WHEN pbi.source_ref IS NOT NULL AND btrim(coalesce(pbi.source_ref,'')) LIKE 'seg:%'
        THEN nullif(btrim(split_part(btrim(pbi.source_ref), ':', 2)), '')
      ELSE 'TOTAL'
    END AS component_id,
    upper(coalesce(pb.status,'')) AS batch_status,
    pb.completed_at_utc AS completed_at_utc
  FROM public.pay_batch_items pbi
  JOIN public.pay_batch_candidates pbc
    ON pbc.id = pbi.pay_batch_candidate_id
  JOIN public.pay_batches pb
    ON pb.id = pbc.pay_batch_id
  JOIN pay_ts pt
    ON pt.timesheet_id = pbi.timesheet_id
  WHERE pbi.is_voided = false
    AND pb.cancelled_at_utc IS NULL
    AND pbi.item_type IN ('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA')
),
pay_items_agg AS (
  SELECT
    pi.timesheet_id,
    pi.component_id,
    max(CASE WHEN pi.batch_status = 'SETTLED' THEN 1 ELSE 0 END)::int AS has_settled,
    max(CASE WHEN pi.batch_status IN (
      'DRAFT','DRAFT_CREATED','READY',
      'WAITING_BANK_CONFIRM','PARTIAL','FAILED','BLOCKED_FUNDS',
      'SCHEDULED','EXECUTING','AWAITING_AUTHORISATION','AUTHORISED_FOR_PAYMENT'
    ) THEN 1 ELSE 0 END)::int AS has_processing,
    max(CASE WHEN pi.batch_status = 'SETTLED' THEN pi.completed_at_utc ELSE NULL END) AS settled_at_utc
  FROM pay_items pi
  WHERE pi.component_id IS NOT NULL
  GROUP BY pi.timesheet_id, pi.component_id
),
pay_component_state AS (
  SELECT
    pc.timesheet_id,
    pc.component_id,
    pc.is_on_hold,
    CASE
      WHEN pc.is_on_hold = true THEN 'ON_HOLD'
      WHEN coalesce(pia.has_settled,0) = 1 THEN 'PAID'
      WHEN coalesce(pia.has_processing,0) = 1 THEN 'PROCESSING'
      ELSE 'UNPAID'
    END AS component_stage,
    pia.settled_at_utc
  FROM pay_components pc
  LEFT JOIN pay_items_agg pia
    ON pia.timesheet_id = pc.timesheet_id
   AND pia.component_id = pc.component_id
),
pay_counts AS (
  SELECT
    pcs.timesheet_id,
    count(*)::int AS total_components,
    count(*) FILTER (WHERE pcs.is_on_hold = true)::int AS on_hold_components,
    count(*) FILTER (WHERE pcs.is_on_hold = false)::int AS payable_components,
    count(*) FILTER (WHERE pcs.is_on_hold = false AND pcs.component_stage = 'PAID')::int AS paid_components,
    max(CASE WHEN pcs.is_on_hold = false AND pcs.component_stage = 'PROCESSING' THEN 1 ELSE 0 END)::int AS any_processing,
    max(CASE WHEN pcs.is_on_hold = false AND pcs.component_stage = 'PAID' THEN pcs.settled_at_utc ELSE NULL END) AS pay_paid_at_utc
  FROM pay_component_state pcs
  GROUP BY pcs.timesheet_id
),
pay_delta AS (
  SELECT
    pt.timesheet_id,
    round(coalesce(sum(coalesce(oc.truth_ex_vat,0) - coalesce(oc.baseline_ex_vat,0)),0),2)::numeric AS net_delta_ex_vat
  FROM pay_ts pt
  LEFT JOIN LATERAL public._pay_outstanding_components(ARRAY[pt.timesheet_id]) oc ON true
  GROUP BY pt.timesheet_id
),
pay_rollup AS (
  SELECT
    pc.timesheet_id,
    CASE
      WHEN pc.payable_components IS NULL OR pc.payable_components = 0 THEN 'UNPAID'
      WHEN pc.any_processing = 1 THEN 'PROCESSING'
      WHEN pc.paid_components = pc.payable_components THEN 'PAID'
      WHEN pc.paid_components > 0 THEN 'PARTIALLY_PAID'
      ELSE 'UNPAID'
    END AS pay_status_code,
    pc.pay_paid_at_utc AS pay_paid_at_utc,
    pd.net_delta_ex_vat AS net_delta_ex_vat,
    CASE
      WHEN pc.any_processing = 1 THEN 'CLOCK'
      WHEN pd.net_delta_ex_vat < 0 THEN 'RED_COIN'
      WHEN pd.net_delta_ex_vat > 0 THEN 'HALF_COIN'
      WHEN pc.payable_components > 0 AND pc.paid_components = pc.payable_components THEN 'COIN'
      ELSE 'NONE'
    END AS pay_icon_code
  FROM pay_counts pc
  LEFT JOIN pay_delta pd
    ON pd.timesheet_id = pc.timesheet_id
)
SELECT
  wi.timesheet_id AS timesheet_id,
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
    wi.timesheet_id IS NOT NULL
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
    WHEN wi.timesheet_id IS NULL THEN
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
    WHEN wi.timesheet_id IS NOT NULL
      AND qr_status = 'PENDING'::timesheet_qr_status_enum
      AND (qr_token IS NULL OR length(btrim(qr_token)) = 0)
      AND qr_generated_at IS NULL
      THEN 'QR_NOT_ISSUED'
    WHEN wi.timesheet_id IS NOT NULL
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
    wi.timesheet_id IS NOT NULL
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

  -- ✅ FIX: Tools Stage must respect TSFIN.processing_status (PENDING_AUTH must not appear as authorised)
  CASE
    WHEN wi.timesheet_id IS NULL THEN 'UNPROCESSED'
    WHEN (
      wi.locked_by_invoice_id IS NOT NULL
      OR COALESCE(seg.seg_locked,0) > 0
      OR (
        seg.seg_total IS NOT NULL
        AND COALESCE(seg.seg_locked,0) > 0
      )
    ) THEN 'INVOICED'
    WHEN (
      wi.timesheet_id IS NOT NULL
      AND wi.authorised_at_server IS NULL
      AND (
        wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum
        OR (
          COALESCE(wi.client_requires_hr,false) = true
          AND COALESCE(wi.client_autoprocess_hr,false) = false
          AND array_length(ic.issue_codes_final, 1) = 1
          AND ic.issue_codes_final @> ARRAY['Authorisation'::text]
        )
      )
    ) THEN 'AWAITING_AUTHORISATION'
    WHEN (
      wi.timesheet_id IS NOT NULL
      AND wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum
    ) THEN 'AUTHORISED_FOR_INVOICING'
    ELSE 'PROCESSING_DELAYED'
  END AS tools_stage,

  -- ✅ FIX: Processing Status label must not show "Authorised for Invoicing" while TSFIN=PENDING_AUTH
  CASE
    WHEN wi.timesheet_id IS NULL THEN 'Unprocessed'
    WHEN (
      wi.locked_by_invoice_id IS NOT NULL
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
      wi.timesheet_id IS NOT NULL
      AND ic.issue_codes_final @> ARRAY['Awaiting signed QR timesheet'::text]
    ) THEN 'Awaiting signed QR timesheet'
    WHEN (
      wi.timesheet_id IS NOT NULL
      AND wi.authorised_at_server IS NULL
      AND (
        wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum
        OR (
          COALESCE(wi.client_requires_hr,false) = true
          AND COALESCE(wi.client_autoprocess_hr,false) = false
          AND array_length(ic.issue_codes_final, 1) = 1
          AND ic.issue_codes_final @> ARRAY['Authorisation'::text]
        )
      )
    ) THEN 'Awaiting Authorisation'
    WHEN (
      wi.timesheet_id IS NOT NULL
      AND wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum
    ) THEN 'Authorised for Invoicing'
    ELSE 'Processing Delayed'
  END AS processing_status_display,

  -- ✅ NEW (APPENDED AT END): invoice-paid indicator (true if ANY linked invoice is PAID)
  COALESCE(seg.invoice_paid_any, false) AS invoice_is_paid,

  -- ✅ NEW (APPENDED AT END): reference blockers for UI badges
  (CASE WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE' THEN true ELSE false END) AS refs_block_invoicing,
  (CASE WHEN wi.timesheet_id IS NOT NULL AND COALESCE(pc.issue_missing_reference, false) = true THEN true ELSE false END) AS refs_block_issuing_invoices,
  (CASE WHEN wi.timesheet_id IS NOT NULL AND pc.precheck_status = 'BLOCK_NO_REFERENCE' AND COALESCE(pc.issue_missing_reference, false) = true THEN true ELSE false END) AS refs_block_invoice_and_issuing,
  -- ✅ NEW (APPENDED AT END): pay status/icon for timesheet summary list
  (CASE WHEN wi.timesheet_id IS NULL THEN 'NONE' ELSE COALESCE(payr.pay_icon_code, 'NONE') END) AS pay_icon_code,
  (CASE WHEN wi.timesheet_id IS NULL THEN NULL ELSE payr.pay_status_code END) AS pay_status_code,
  (CASE WHEN wi.timesheet_id IS NULL THEN NULL ELSE payr.pay_paid_at_utc END) AS pay_paid_at_utc,
  (CASE WHEN wi.timesheet_id IS NULL THEN NULL ELSE payr.net_delta_ex_vat END) AS net_delta_ex_vat


FROM with_issues wi
LEFT JOIN pay_rollup payr ON payr.timesheet_id = wi.timesheet_id
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
      -- ------------------------------------------------------------------
      -- Invoice reference blockers (UI-friendly)
      --   - If can't invoice, do NOT show "send invoice blocked"
      --   - Suppress ref blockers when HR validation is required and NOT OK
      -- ------------------------------------------------------------------
      (
        CASE
          WHEN wi.timesheet_id IS NOT NULL
            AND (
              wi.timesheet_id IS NOT NULL
              AND COALESCE(wi.client_hr_validation_required, false) = true
              AND COALESCE(wi.client_no_timesheet_required, false) = false
              AND COALESCE(wi.total_hours, 0::numeric) > 0::numeric
            )
            AND NOT (
              wi.validation_status = 'VALIDATION_OK'::validation_status_enum
              OR wi.validation_status = 'OVERRIDDEN'::validation_status_enum
            )
          THEN ARRAY[]::text[]
          WHEN wi.timesheet_id IS NOT NULL
            AND pc.precheck_status = 'BLOCK_NO_REFERENCE'
          THEN ARRAY['Refs - Can''t invoice'::text]
          WHEN wi.timesheet_id IS NOT NULL
            AND COALESCE(pc.issue_missing_reference, false) = true
          THEN ARRAY['Refs - Send Invoice will be blocked'::text]
          ELSE ARRAY[]::text[]
        END
      )
      ||
      -- ------------------------------------------------------------------
      -- Timesheet PDF/evidence invoice blocker
      -- Replace legacy "Timesheet evidence" label with "Timesheet evidence missing"
      -- when invoicing is blocked by precheck BLOCK_NO_PDF
      -- ------------------------------------------------------------------
      (
        CASE
          WHEN wi.timesheet_id IS NOT NULL
            AND pc.precheck_status = 'BLOCK_NO_PDF'
          THEN ARRAY['Timesheet evidence missing'::text]
          ELSE ARRAY[]::text[]
        END
      )
      ||
      -- ------------------------------------------------------------------
      -- Keep all other issues, but remove legacy "Reference" and "Timesheet evidence"
      -- (we replace them above with invoice-aligned labels)
      -- ------------------------------------------------------------------
      array_remove(
        array_remove(COALESCE(wi.issue_codes, ARRAY[]::text[]), 'Reference'::text),
        'Timesheet evidence'::text
      )
      ||
      -- ------------------------------------------------------------------
      -- Refs/PDF baseline invalid (unchanged)
      -- ------------------------------------------------------------------
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
  end as route_display,

  -- ✅ NEW (APPENDED AT END ONLY): pay status rollup fields from v_timesheets_summary_base
  v.pay_icon_code,
  v.pay_status_code,
  v.pay_paid_at_utc,
  v.net_delta_ex_vat

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

CREATE OR REPLACE FUNCTION public.timesheets_invalidate_prevalidation_on_change()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_tv_status public.validation_status_enum;
  v_tv_pre_validated boolean;
  v_tv_hr_request_id text;
  v_tv_hr_request_source public.reference_source_enum;

  v_changed boolean := false;

  -- DAILY change flags
  v_daily_changed_worked boolean := false;
  v_daily_changed_break boolean := false;
  v_daily_changed_refnum boolean := false;
  v_daily_changed_dayrefs boolean := false;

  -- WEEKLY change flags
  v_weekly_changed_sched boolean := false;
  v_weekly_changed_refnum boolean := false;
  v_weekly_changed_dayrefs boolean := false;
  v_weekly_changed_units_week boolean := false;
  v_weekly_changed_units_per_day boolean := false;

  v_ignore_daily_ref_autoset boolean := false;
begin
  if tg_op <> 'UPDATE' then
    return new;
  end if;

  -- Only apply to current row updates
  if coalesce(new.is_current, false) is not true then
    return new;
  end if;

  -- If the old row was not current, do not invalidate (avoid version-rotation noise)
  if coalesce(old.is_current, false) is not true then
    return new;
  end if;

  -- Load current validation row (if any)
  select
    tv.status,
    coalesce(tv.pre_validated, false),
    tv.hr_request_id,
    tv.hr_request_source
  into
    v_tv_status,
    v_tv_pre_validated,
    v_tv_hr_request_id,
    v_tv_hr_request_source
  from public.timesheet_validations tv
  where tv.timesheet_id = new.timesheet_id
  limit 1;

  if not found then
    return new;
  end if;

  -- Only invalidate when validation was previously "good" OR pre_validated was set
  if not (
    v_tv_pre_validated is true
    or v_tv_status = 'VALIDATION_OK'::public.validation_status_enum
    or v_tv_status = 'OVERRIDDEN'::public.validation_status_enum
  ) then
    return new;
  end if;

  -- ─────────────────────────────────────────────
  -- Determine whether validation-relevant truth changed
  -- ─────────────────────────────────────────────
  if new.sheet_scope = 'DAILY'::public.timesheet_scope_enum then
    v_daily_changed_worked :=
      (new.worked_start_iso is distinct from old.worked_start_iso)
      or (new.worked_end_iso is distinct from old.worked_end_iso);

    v_daily_changed_break :=
      (new.break_start_iso is distinct from old.break_start_iso)
      or (new.break_end_iso is distinct from old.break_end_iso)
      or (new.break_minutes is distinct from old.break_minutes);

    v_daily_changed_refnum :=
      (new.reference_number is distinct from old.reference_number);

    v_daily_changed_dayrefs :=
      (new.day_references_json is distinct from old.day_references_json);

    -- Special-case: DAILY reference_number auto-set to imported HR request id
    -- Do NOT invalidate if the ONLY relevant change is reference_number and it matches the imported hr_request_id.
    if v_daily_changed_refnum is true
       and v_daily_changed_worked is false
       and v_daily_changed_break is false
       and v_daily_changed_dayrefs is false
       and v_tv_hr_request_source = 'IMPORTED'::public.reference_source_enum
       and v_tv_hr_request_id is not null
       and new.reference_number is not distinct from v_tv_hr_request_id
    then
      v_ignore_daily_ref_autoset := true;
    end if;

    v_changed :=
      (v_daily_changed_worked or v_daily_changed_break or v_daily_changed_dayrefs or v_daily_changed_refnum)
      and v_ignore_daily_ref_autoset is false;

  elsif new.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum then
    v_weekly_changed_sched :=
      (new.actual_schedule_json is distinct from old.actual_schedule_json);

    v_weekly_changed_refnum :=
      (new.reference_number is distinct from old.reference_number);

    v_weekly_changed_dayrefs :=
      (new.day_references_json is distinct from old.day_references_json);

    v_weekly_changed_units_week :=
      (new.additional_units_week is distinct from old.additional_units_week);

    v_weekly_changed_units_per_day :=
      (new.additional_units_per_day is distinct from old.additional_units_per_day);

    v_changed :=
      v_weekly_changed_sched
      or v_weekly_changed_refnum
      or v_weekly_changed_dayrefs
      or v_weekly_changed_units_week
      or v_weekly_changed_units_per_day;
  else
    return new;
  end if;

  if v_changed is not true then
    return new;
  end if;

  -- ─────────────────────────────────────────────
  -- Invalidate pre-validation + force re-validation
  -- ─────────────────────────────────────────────
  update public.timesheet_validations tvu
     set status = 'PENDING'::public.validation_status_enum,
         pre_validated = false,
         validated_at_utc = null,
         reason_code = 'TIMESHEET_CHANGED',
         updated_at = now()
   where tvu.timesheet_id = new.timesheet_id;

  return new;
end;
$function$;

DROP TRIGGER IF EXISTS trg_timesheets_invalidate_prevalidation_on_change
ON public.timesheets;

CREATE TRIGGER trg_timesheets_invalidate_prevalidation_on_change
AFTER UPDATE ON public.timesheets
FOR EACH ROW
EXECUTE FUNCTION public.timesheets_invalidate_prevalidation_on_change();

-- Only v_timesheets_summary_base requires amendment for real-row UNPROCESSED support.
-- v_timesheets_summary is a passthrough over this view and does not need separate DDL.

CREATE OR REPLACE VIEW public.v_timesheets_summary_base AS
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
          WHERE (tf.is_current = true)
          ORDER BY tf.timesheet_id, tf.created_at DESC
        ), validations_latest AS (
         SELECT DISTINCT ON (tv.timesheet_id) tv.timesheet_id,
            tv.status,
            tv.reason_code
           FROM timesheet_validations tv
          ORDER BY tv.timesheet_id, tv.created_at DESC
        ), nhsp_agg AS (
         SELECT ns.timesheet_id,
            (count(*))::integer AS nhsp_shift_count,
            (count(*) FILTER (WHERE (ns.invoice_status = 'INCLUDED'::text)))::integer AS nhsp_shift_included_count,
            (count(*) FILTER (WHERE (ns.invoice_status = 'DEFERRED'::text)))::integer AS nhsp_shift_deferred_count
           FROM nhsp_shifts ns
          GROUP BY ns.timesheet_id
        ), pay_adj AS (
         SELECT pa.timesheet_id,
            (count(*))::integer AS pay_adjustment_count
           FROM ts_pay_adjustments pa
          GROUP BY pa.timesheet_id
        ), evidence_agg AS (
         SELECT te.timesheet_id,
            (count(*))::integer AS evidence_count
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
                    WHEN ((COALESCE(tf.candidate_id, ct.candidate_id) IS NULL) AND (ts.candidate_hint_text IS NOT NULL) AND (jsonb_typeof(ts.candidate_hint_text) = 'object'::text) AND ((NULLIF(btrim(concat_ws(' '::text, NULLIF(btrim((ts.candidate_hint_text ->> 'first_name'::text)), ''::text), NULLIF(btrim((ts.candidate_hint_text ->> 'surname'::text)), ''::text))), ''::text) IS NOT NULL) OR (NULLIF(btrim((ts.candidate_hint_text ->> 'display_name'::text)), ''::text) IS NOT NULL) OR (NULLIF(btrim((ts.candidate_hint_text ->> 'email'::text)), ''::text) IS NOT NULL))) THEN (('Unresolved Timesheet - '::text || COALESCE(NULLIF(btrim(concat_ws(' '::text, NULLIF(btrim((ts.candidate_hint_text ->> 'first_name'::text)), ''::text), NULLIF(btrim((ts.candidate_hint_text ->> 'surname'::text)), ''::text))), ''::text), NULLIF(btrim((ts.candidate_hint_text ->> 'display_name'::text)), ''::text), 'Candidate'::text)) ||
                    CASE
                        WHEN (NULLIF(btrim((ts.candidate_hint_text ->> 'email'::text)), ''::text) IS NOT NULL) THEN (', Email - '::text || btrim((ts.candidate_hint_text ->> 'email'::text)))
                        ELSE ''::text
                    END)
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
            COALESCE(ch.hr_validation_required, false) AS client_hr_validation_required,
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
           FROM ((((((((((((timesheets ts
             LEFT JOIN contract_weeks cw ON ((cw.timesheet_id = ts.timesheet_id)))
             LEFT JOIN contracts ct ON ((ct.id = COALESCE(ts.contract_id, cw.contract_id))))
             LEFT JOIN latest_tsfin tf ON ((tf.timesheet_id = ts.timesheet_id)))
             LEFT JOIN candidates c ON ((c.id = COALESCE(tf.candidate_id, ct.candidate_id))))
             LEFT JOIN clients cli ON ((cli.id = COALESCE(tf.client_id, ct.client_id))))
             LEFT JOIN client_hr ch ON ((ch.client_id = COALESCE(tf.client_id, ct.client_id))))
             LEFT JOIN nhsp_agg na ON ((na.timesheet_id = ts.timesheet_id)))
             LEFT JOIN pay_adj pa ON ((pa.timesheet_id = ts.timesheet_id)))
             LEFT JOIN validations_latest vl ON ((vl.timesheet_id = ts.timesheet_id)))
             LEFT JOIN evidence_agg ea ON ((ea.timesheet_id = ts.timesheet_id)))
             LEFT JOIN candidates cand ON ((cand.id = COALESCE(tf.candidate_id, ct.candidate_id))))
             LEFT JOIN umbrellas umb ON ((umb.id = cand.umbrella_id)))
          WHERE (ts.is_current = true)
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
            round(((((COALESCE((NULLIF(((cw.totals_json -> 'hours'::text) ->> 'day'::text), ''::text))::numeric, (0)::numeric) + COALESCE((NULLIF(((cw.totals_json -> 'hours'::text) ->> 'night'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF(((cw.totals_json -> 'hours'::text) ->> 'sat'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF(((cw.totals_json -> 'hours'::text) ->> 'sun'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF(((cw.totals_json -> 'hours'::text) ->> 'bh'::text), ''::text))::numeric, (0)::numeric)), 2) AS total_hours,
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
            COALESCE(ch.hr_validation_required, false) AS client_hr_validation_required,
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
           FROM ((((contract_weeks cw
             JOIN contracts ct ON ((ct.id = cw.contract_id)))
             LEFT JOIN candidates cand ON ((cand.id = ct.candidate_id)))
             LEFT JOIN clients cli ON ((cli.id = ct.client_id)))
             LEFT JOIN client_hr ch ON ((ch.client_id = ct.client_id)))
          WHERE (cw.timesheet_id IS NULL)
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
            (((((((((((((((ARRAY[]::text[] ||
                CASE
                    WHEN (ar.has_rate_issue OR (ar.processing_status = 'RATE_MISSING'::ts_fin_processing_status_enum)) THEN ARRAY['Rate'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN (ar.has_pay_channel_issue OR (ar.processing_status = 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum)) THEN ARRAY['Pay channel'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN (ar.processing_status = 'UNASSIGNED'::ts_fin_processing_status_enum) THEN ARRAY['Candidate ID'::text]
                    WHEN (ar.processing_status = 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum) THEN ARRAY['Client ID'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ar.pay_on_hold THEN ARRAY['On hold'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((NOT ((ar.timesheet_id IS NOT NULL) AND (COALESCE(ar.client_hr_validation_required, false) = true) AND (COALESCE(ar.client_no_timesheet_required, false) = false) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric))) AND ((ar.hr_crosscheck_status = 'HOURS_MISMATCH_HR'::text) OR (ar.hr_crosscheck_issues && ARRAY['HOURS_MISMATCH_HR'::text]))) THEN ARRAY['Hours mismatch HR'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((NOT ((ar.timesheet_id IS NOT NULL) AND (COALESCE(ar.client_hr_validation_required, false) = true) AND (COALESCE(ar.client_no_timesheet_required, false) = false) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric))) AND (ar.hr_crosscheck_issues && ARRAY['HR_HOURS_MISSING'::text])) THEN ARRAY['HR hours missing'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN (ar.hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'::text]) THEN ARRAY['Duplicate contracts'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND ar.client_requires_hr AND (NOT ar.client_no_timesheet_required) AND (ar.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric) AND (NOT ((COALESCE(ar.evidence_count, 0) > 0) OR ((ar.submission_mode = 'ELECTRONIC'::submission_mode_enum) AND (ar.r2_nurse_key IS NOT NULL) AND (ar.r2_auth_key IS NOT NULL)) OR ((ar.submission_mode = 'MANUAL'::submission_mode_enum) AND (ar.manual_pdf_r2_key IS NOT NULL))))) THEN ARRAY['Timesheet evidence'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND ((COALESCE(ar.travel_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.travel_pay_ex_vat, (0)::numeric) > (0)::numeric) OR ((COALESCE(ar.accommodation_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.accommodation_pay_ex_vat, (0)::numeric) > (0)::numeric)) OR ((COALESCE(ar.other_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.other_pay_ex_vat, (0)::numeric) > (0)::numeric))) AND ((((COALESCE(ar.travel_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.travel_pay_ex_vat, (0)::numeric) > (0)::numeric)) AND (NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE ((te.timesheet_id = ar.timesheet_id) AND (upper(te.kind) = 'TRAVEL'::text)))))) OR (((COALESCE(ar.accommodation_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.accommodation_pay_ex_vat, (0)::numeric) > (0)::numeric)) AND (NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE ((te.timesheet_id = ar.timesheet_id) AND (upper(te.kind) = 'ACCOMMODATION'::text)))))) OR (((COALESCE(ar.other_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.other_pay_ex_vat, (0)::numeric) > (0)::numeric)) AND (NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE ((te.timesheet_id = ar.timesheet_id) AND (upper(te.kind) = 'OTHER'::text)))))))) THEN ARRAY['Expenses evidence'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND ((COALESCE(ar.mileage_units, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.mileage_charge_ex_vat, (0)::numeric) > (0)::numeric) OR (COALESCE(ar.mileage_pay_ex_vat, (0)::numeric) > (0)::numeric)) AND (NOT (EXISTS ( SELECT 1
                       FROM timesheet_evidence te
                      WHERE ((te.timesheet_id = ar.timesheet_id) AND (upper(te.kind) = 'MILEAGE'::text)))))) THEN ARRAY['Mileage evidence'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND (ar.sheet_scope = 'DAILY'::timesheet_scope_enum) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric) AND (COALESCE(ar.require_reference_to_pay, false) OR COALESCE(ar.require_reference_to_invoice, false) OR ar.client_ts_reference_required OR ar.client_pay_reference_required OR ar.client_invoice_reference_required) AND ((ar.reference_number IS NULL) OR (length(btrim(ar.reference_number)) = 0))) THEN ARRAY['Reference'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND (ar.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric) AND (COALESCE(ar.require_reference_to_pay, false) OR COALESCE(ar.require_reference_to_invoice, false) OR ((NOT COALESCE(ar.require_reference_to_pay, false)) AND (NOT COALESCE(ar.require_reference_to_invoice, false)) AND (ar.client_pay_reference_required OR ar.client_invoice_reference_required OR ar.client_ts_reference_required))) AND (((ar.invoice_breakdown_json IS NOT NULL) AND (jsonb_typeof(ar.invoice_breakdown_json) = 'object'::text) AND (upper(COALESCE((ar.invoice_breakdown_json ->> 'mode'::text), ''::text)) = 'SEGMENTS'::text) AND (jsonb_typeof((ar.invoice_breakdown_json -> 'segments'::text)) = 'array'::text) AND (EXISTS ( SELECT 1
                       FROM jsonb_array_elements((ar.invoice_breakdown_json -> 'segments'::text)) s(value)
                      WHERE ((NULLIF(btrim(COALESCE((s.value ->> 'invoice_locked_invoice_id'::text), ''::text)), ''::text) IS NULL) AND (((((COALESCE((NULLIF((s.value ->> 'hours_day'::text), ''::text))::numeric, (0)::numeric) + COALESCE((NULLIF((s.value ->> 'hours_night'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF((s.value ->> 'hours_sat'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF((s.value ->> 'hours_sun'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF((s.value ->> 'hours_bh'::text), ''::text))::numeric, (0)::numeric)) > (0)::numeric) AND (COALESCE(btrim((s.value ->> 'ref_num'::text)), ''::text) = ''::text))))) OR ((ar.submission_mode = 'MANUAL'::submission_mode_enum) AND ((ar.actual_schedule_json IS NULL) OR (jsonb_typeof(ar.actual_schedule_json) <> 'array'::text) OR ((jsonb_typeof(ar.actual_schedule_json) = 'array'::text) AND ((jsonb_array_length(ar.actual_schedule_json) = 0) OR (EXISTS ( SELECT 1
                       FROM jsonb_array_elements(ar.actual_schedule_json) seg_1(value)
                      WHERE ((COALESCE(btrim((seg_1.value ->> 'start'::text)), ''::text) <> ''::text) AND (COALESCE(btrim((seg_1.value ->> 'end'::text)), ''::text) <> ''::text) AND (COALESCE(btrim((seg_1.value ->> 'ref_num'::text)), ''::text) = ''::text)))))))) OR ((ar.submission_mode <> 'MANUAL'::submission_mode_enum) AND (NOT ((EXISTS ( SELECT 1
                       FROM jsonb_array_elements_text(
                            CASE
                                WHEN ((ar.day_references_json IS NOT NULL) AND (jsonb_typeof(ar.day_references_json) = 'object'::text) AND (jsonb_typeof((ar.day_references_json -> '__freeform_refs'::text)) = 'array'::text)) THEN (ar.day_references_json -> '__freeform_refs'::text)
                                WHEN ((ar.day_references_json IS NOT NULL) AND (jsonb_typeof(ar.day_references_json) = 'object'::text) AND (jsonb_typeof((ar.day_references_json -> '__freeform'::text)) = 'array'::text)) THEN (ar.day_references_json -> '__freeform'::text)
                                WHEN ((ar.day_references_json IS NOT NULL) AND (jsonb_typeof(ar.day_references_json) = 'object'::text) AND (jsonb_typeof((ar.day_references_json -> '__freeform_lines'::text)) = 'array'::text)) THEN (ar.day_references_json -> '__freeform_lines'::text)
                                WHEN ((ar.day_references_json IS NOT NULL) AND (jsonb_typeof(ar.day_references_json) = 'array'::text)) THEN ar.day_references_json
                                ELSE '[]'::jsonb
                            END) t(x)
                      WHERE (NULLIF(btrim(COALESCE(t.x, ''::text)), ''::text) IS NOT NULL))) OR (EXISTS ( SELECT 1
                       FROM jsonb_each_text(
                            CASE
                                WHEN ((ar.day_references_json IS NOT NULL) AND (jsonb_typeof(ar.day_references_json) = 'object'::text)) THEN ar.day_references_json
                                ELSE '{}'::jsonb
                            END) j(k, v)
                      WHERE ((NULLIF(btrim(COALESCE(j.v, ''::text)), ''::text) IS NOT NULL) AND ("left"(COALESCE(j.k, ''::text), 2) <> '__'::text))))))))) THEN ARRAY['Reference'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND (COALESCE(ar.client_hr_validation_required, false) = true) AND (COALESCE(ar.client_no_timesheet_required, false) = false) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric) AND ((ar.validation_status IS NULL) OR (ar.validation_status = 'PENDING'::validation_status_enum))) THEN ARRAY['Awaiting validation'::text]
                    WHEN ((ar.timesheet_id IS NOT NULL) AND (COALESCE(ar.client_hr_validation_required, false) = true) AND (COALESCE(ar.client_no_timesheet_required, false) = false) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric) AND (ar.validation_status IS NOT NULL) AND (ar.validation_status <> ALL (ARRAY['VALIDATION_OK'::validation_status_enum, 'OVERRIDDEN'::validation_status_enum, 'PENDING'::validation_status_enum]))) THEN ARRAY['Validation failed'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND ar.client_requires_hr AND (NOT ar.client_autoprocess_hr) AND (ar.authorised_at_server IS NULL)) THEN ARRAY['Authorisation'::text]
                    ELSE ARRAY[]::text[]
                END) ||
                CASE
                    WHEN ((ar.timesheet_id IS NOT NULL) AND (ar.qr_status = 'PENDING'::timesheet_qr_status_enum) AND (((ar.qr_token IS NOT NULL) AND (length(btrim(ar.qr_token)) > 0) AND (ar.qr_generated_at IS NOT NULL)) OR (ar.qr_last_sent_hash IS NOT NULL)) AND (ar.qr_scanned_at IS NULL) AND (COALESCE(ar.total_hours, (0)::numeric) > (0)::numeric)) THEN ARRAY['Awaiting signed QR timesheet'::text]
                    ELSE ARRAY[]::text[]
                END) AS issue_codes
           FROM all_rows ar
        ), pay_ts AS (
         SELECT DISTINCT wi_1.timesheet_id,
            COALESCE(wi_1.pay_on_hold, false) AS pay_on_hold,
            wi_1.invoice_breakdown_json
           FROM with_issues wi_1
          WHERE (wi_1.timesheet_id IS NOT NULL)
        ), pay_is_seg AS (
         SELECT pt.timesheet_id,
            ((pt.invoice_breakdown_json IS NOT NULL) AND (jsonb_typeof(pt.invoice_breakdown_json) = 'object'::text) AND (upper(COALESCE((pt.invoice_breakdown_json ->> 'mode'::text), ''::text)) = 'SEGMENTS'::text) AND (jsonb_typeof((pt.invoice_breakdown_json -> 'segments'::text)) = 'array'::text)) AS is_segments_mode
           FROM pay_ts pt
        ), pay_components AS (
         SELECT pt.timesheet_id,
            NULLIF(btrim(COALESCE((seg_1.value ->> 'segment_id'::text), ''::text)), ''::text) AS component_id,
            COALESCE((NULLIF((seg_1.value ->> 'exclude_from_pay'::text), ''::text))::boolean, false) AS is_on_hold
           FROM ((pay_ts pt
             JOIN pay_is_seg ps ON ((ps.timesheet_id = pt.timesheet_id)))
             JOIN LATERAL jsonb_array_elements(COALESCE((pt.invoice_breakdown_json -> 'segments'::text), '[]'::jsonb)) seg_1(value) ON (true))
          WHERE ((ps.is_segments_mode = true) AND (seg_1.value IS NOT NULL) AND (jsonb_typeof(seg_1.value) = 'object'::text) AND (NULLIF(btrim(COALESCE((seg_1.value ->> 'segment_id'::text), ''::text)), ''::text) IS NOT NULL))
        UNION ALL
         SELECT pt.timesheet_id,
            'TOTAL'::text AS component_id,
            pt.pay_on_hold AS is_on_hold
           FROM (pay_ts pt
             JOIN pay_is_seg ps ON ((ps.timesheet_id = pt.timesheet_id)))
          WHERE (ps.is_segments_mode = false)
        ), pay_items AS (
         SELECT pbi.timesheet_id,
                CASE
                    WHEN (NULLIF(btrim(COALESCE(pbi.segment_key, ''::text)), ''::text) IS NOT NULL) THEN NULLIF(btrim(COALESCE(pbi.segment_key, ''::text)), ''::text)
                    WHEN ((pbi.source_ref IS NOT NULL) AND (btrim(COALESCE(pbi.source_ref, ''::text)) ~~ 'seg:%'::text)) THEN NULLIF(btrim(split_part(btrim(pbi.source_ref), ':'::text, 2)), ''::text)
                    ELSE 'TOTAL'::text
                END AS component_id,
            upper(COALESCE(pb.status, ''::text)) AS batch_status,
            pb.completed_at_utc
           FROM (((pay_batch_items pbi
             JOIN pay_batch_candidates pbc ON ((pbc.id = pbi.pay_batch_candidate_id)))
             JOIN pay_batches pb ON ((pb.id = pbc.pay_batch_id)))
             JOIN pay_ts pt ON ((pt.timesheet_id = pbi.timesheet_id)))
          WHERE ((pbi.is_voided = false) AND (pb.cancelled_at_utc IS NULL) AND (pbi.item_type = ANY (ARRAY['SEGMENT_DELTA'::text, 'EXPENSE_DELTA'::text, 'ADJUSTMENT_DELTA'::text, 'MILEAGE_DELTA'::text])))
        ), pay_items_agg AS (
         SELECT pi.timesheet_id,
            pi.component_id,
            max(
                CASE
                    WHEN (pi.batch_status = 'SETTLED'::text) THEN 1
                    ELSE 0
                END) AS has_settled,
            max(
                CASE
                    WHEN (pi.batch_status = ANY (ARRAY['DRAFT'::text, 'DRAFT_CREATED'::text, 'READY'::text, 'WAITING_BANK_CONFIRM'::text, 'PARTIAL'::text, 'FAILED'::text, 'BLOCKED_FUNDS'::text, 'SCHEDULED'::text, 'EXECUTING'::text, 'AWAITING_AUTHORISATION'::text, 'AUTHORISED_FOR_PAYMENT'::text])) THEN 1
                    ELSE 0
                END) AS has_processing,
            max(
                CASE
                    WHEN (pi.batch_status = 'SETTLED'::text) THEN pi.completed_at_utc
                    ELSE NULL::timestamp with time zone
                END) AS settled_at_utc
           FROM pay_items pi
          WHERE (pi.component_id IS NOT NULL)
          GROUP BY pi.timesheet_id, pi.component_id
        ), pay_component_state AS (
         SELECT pc_1.timesheet_id,
            pc_1.component_id,
            pc_1.is_on_hold,
                CASE
                    WHEN (pc_1.is_on_hold = true) THEN 'ON_HOLD'::text
                    WHEN (COALESCE(pia.has_settled, 0) = 1) THEN 'PAID'::text
                    WHEN (COALESCE(pia.has_processing, 0) = 1) THEN 'PROCESSING'::text
                    ELSE 'UNPAID'::text
                END AS component_stage,
            pia.settled_at_utc
           FROM (pay_components pc_1
             LEFT JOIN pay_items_agg pia ON (((pia.timesheet_id = pc_1.timesheet_id) AND (pia.component_id = pc_1.component_id))))
        ), pay_counts AS (
         SELECT pcs.timesheet_id,
            (count(*))::integer AS total_components,
            (count(*) FILTER (WHERE (pcs.is_on_hold = true)))::integer AS on_hold_components,
            (count(*) FILTER (WHERE (pcs.is_on_hold = false)))::integer AS payable_components,
            (count(*) FILTER (WHERE ((pcs.is_on_hold = false) AND (pcs.component_stage = 'PAID'::text))))::integer AS paid_components,
            max(
                CASE
                    WHEN ((pcs.is_on_hold = false) AND (pcs.component_stage = 'PROCESSING'::text)) THEN 1
                    ELSE 0
                END) AS any_processing,
            max(
                CASE
                    WHEN ((pcs.is_on_hold = false) AND (pcs.component_stage = 'PAID'::text)) THEN pcs.settled_at_utc
                    ELSE NULL::timestamp with time zone
                END) AS pay_paid_at_utc
           FROM pay_component_state pcs
          GROUP BY pcs.timesheet_id
        ), pay_delta AS (
         SELECT pt.timesheet_id,
            round(COALESCE(sum((COALESCE(oc.truth_ex_vat, (0)::numeric) - COALESCE(oc.baseline_ex_vat, (0)::numeric))), (0)::numeric), 2) AS net_delta_ex_vat
           FROM (pay_ts pt
             LEFT JOIN LATERAL _pay_outstanding_components(ARRAY[pt.timesheet_id]) oc(timesheet_id, key_type, key_value, truth_ex_vat, baseline_ex_vat, reserved_ex_vat, outstanding_ex_vat, truth_inc_vat, baseline_inc_vat, reserved_inc_vat, outstanding_inc_vat, reservation_overrun_detected) ON (true))
          GROUP BY pt.timesheet_id
        ), pay_rollup AS (
         SELECT pc_1.timesheet_id,
                CASE
                    WHEN ((pc_1.payable_components IS NULL) OR (pc_1.payable_components = 0)) THEN 'UNPAID'::text
                    WHEN (pc_1.any_processing = 1) THEN 'PROCESSING'::text
                    WHEN (pc_1.paid_components = pc_1.payable_components) THEN 'PAID'::text
                    WHEN (pc_1.paid_components > 0) THEN 'PARTIALLY_PAID'::text
                    ELSE 'UNPAID'::text
                END AS pay_status_code,
            pc_1.pay_paid_at_utc,
            pd.net_delta_ex_vat,
                CASE
                    WHEN (pc_1.any_processing = 1) THEN 'CLOCK'::text
                    WHEN (pd.net_delta_ex_vat < (0)::numeric) THEN 'RED_COIN'::text
                    WHEN (pd.net_delta_ex_vat > (0)::numeric) THEN 'HALF_COIN'::text
                    WHEN ((pc_1.payable_components > 0) AND (pc_1.paid_components = pc_1.payable_components)) THEN 'COIN'::text
                    ELSE 'NONE'::text
                END AS pay_icon_code
           FROM (pay_counts pc_1
             LEFT JOIN pay_delta pd ON ((pd.timesheet_id = pc_1.timesheet_id)))
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
    ((wi.timesheet_id IS NOT NULL) AND (wi.paid_at_utc IS NULL) AND (COALESCE(wi.pay_on_hold, false) = false) AND (wi.authorised_at_server IS NOT NULL) AND (wi.processing_status IS NOT NULL) AND (wi.processing_status <> ALL (ARRAY['UNASSIGNED'::ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum, 'RATE_MISSING'::ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum])) AND (COALESCE(wi.has_rate_issue, false) = false) AND (COALESCE(wi.has_pay_channel_issue, false) = false) AND ((COALESCE(wi.require_reference_to_pay, false) = false) OR (COALESCE(wi.total_hours, (0)::numeric) <= (0)::numeric) OR
        CASE
            WHEN ((wi.invoice_breakdown_json IS NOT NULL) AND (jsonb_typeof(wi.invoice_breakdown_json) = 'object'::text) AND (upper(COALESCE((wi.invoice_breakdown_json ->> 'mode'::text), ''::text)) = 'SEGMENTS'::text) AND (jsonb_typeof((wi.invoice_breakdown_json -> 'segments'::text)) = 'array'::text)) THEN (NOT (EXISTS ( SELECT 1
               FROM jsonb_array_elements((wi.invoice_breakdown_json -> 'segments'::text)) s(value)
              WHERE ((((((COALESCE((NULLIF((s.value ->> 'hours_day'::text), ''::text))::numeric, (0)::numeric) + COALESCE((NULLIF((s.value ->> 'hours_night'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF((s.value ->> 'hours_sat'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF((s.value ->> 'hours_sun'::text), ''::text))::numeric, (0)::numeric)) + COALESCE((NULLIF((s.value ->> 'hours_bh'::text), ''::text))::numeric, (0)::numeric)) > (0)::numeric) AND (COALESCE(btrim((s.value ->> 'ref_num'::text)), ''::text) = ''::text)))))
            ELSE
            CASE
                WHEN (wi.sheet_scope = 'DAILY'::timesheet_scope_enum) THEN ((wi.reference_number IS NOT NULL) AND (length(btrim(wi.reference_number)) > 0))
                WHEN (wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) THEN
                CASE
                    WHEN (wi.submission_mode = 'MANUAL'::submission_mode_enum) THEN ((wi.actual_schedule_json IS NOT NULL) AND (jsonb_typeof(wi.actual_schedule_json) = 'array'::text) AND (jsonb_array_length(wi.actual_schedule_json) > 0) AND (NOT (EXISTS ( SELECT 1
                       FROM jsonb_array_elements(wi.actual_schedule_json) seg_1(value)
                      WHERE ((COALESCE(btrim((seg_1.value ->> 'start'::text)), ''::text) <> ''::text) AND (COALESCE(btrim((seg_1.value ->> 'end'::text)), ''::text) <> ''::text) AND (COALESCE(btrim((seg_1.value ->> 'ref_num'::text)), ''::text) = ''::text))))))
                    ELSE ((EXISTS ( SELECT 1
                       FROM jsonb_array_elements_text(
                            CASE
                                WHEN ((wi.day_references_json IS NOT NULL) AND (jsonb_typeof(wi.day_references_json) = 'object'::text) AND (jsonb_typeof((wi.day_references_json -> '__freeform_refs'::text)) = 'array'::text)) THEN (wi.day_references_json -> '__freeform_refs'::text)
                                WHEN ((wi.day_references_json IS NOT NULL) AND (jsonb_typeof(wi.day_references_json) = 'object'::text) AND (jsonb_typeof((wi.day_references_json -> '__freeform'::text)) = 'array'::text)) THEN (wi.day_references_json -> '__freeform'::text)
                                WHEN ((wi.day_references_json IS NOT NULL) AND (jsonb_typeof(wi.day_references_json) = 'object'::text) AND (jsonb_typeof((wi.day_references_json -> '__freeform_lines'::text)) = 'array'::text)) THEN (wi.day_references_json -> '__freeform_lines'::text)
                                WHEN ((wi.day_references_json IS NOT NULL) AND (jsonb_typeof(wi.day_references_json) = 'array'::text)) THEN wi.day_references_json
                                ELSE '[]'::jsonb
                            END) t(x)
                      WHERE (NULLIF(btrim(COALESCE(t.x, ''::text)), ''::text) IS NOT NULL))) OR ((wi.day_references_json IS NOT NULL) AND (jsonb_typeof(wi.day_references_json) = 'object'::text) AND (EXISTS ( SELECT 1
                       FROM jsonb_each_text(wi.day_references_json) j(k, v)
                      WHERE ((NULLIF(btrim(COALESCE(j.v, ''::text)), ''::text) IS NOT NULL) AND ("left"(COALESCE(j.k, ''::text), 2) <> '__'::text))))))
                END
                ELSE ((wi.reference_number IS NOT NULL) AND (length(btrim(wi.reference_number)) > 0))
            END
        END)) AS ready_to_pay,
    wi.locked_by_invoice_id,
    wi.candidate_name,
    wi.client_name,
    wi.nhsp_shift_count,
    wi.nhsp_shift_included_count,
    wi.nhsp_shift_deferred_count,
    wi.validation_status,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN
            CASE wi.contract_week_status
                WHEN 'PLANNED'::contract_week_status_enum THEN 'PLANNED'::text
                WHEN 'OPEN'::contract_week_status_enum THEN 'PLANNED'::text
                WHEN 'SUBMITTED'::contract_week_status_enum THEN 'PENDING_AUTH'::text
                WHEN 'AUTHORISED'::contract_week_status_enum THEN 'READY_FOR_INVOICE'::text
                WHEN 'INVOICED'::contract_week_status_enum THEN 'INVOICED'::text
                WHEN 'CANCELLED'::contract_week_status_enum THEN 'NEEDS_ATTENTION'::text
                ELSE 'UNKNOWN'::text
            END
            WHEN (wi.paid_at_utc IS NOT NULL) THEN 'PAID'::text
            WHEN ((wi.locked_by_invoice_id IS NOT NULL) OR ((seg.seg_total IS NOT NULL) AND (seg.seg_total > 0) AND (COALESCE(seg.seg_locked, 0) >= seg.seg_total))) THEN 'INVOICED'::text
            WHEN ((wi.timesheet_id IS NOT NULL) AND (wi.qr_status = 'PENDING'::timesheet_qr_status_enum) AND ((wi.qr_token IS NULL) OR (length(btrim(wi.qr_token)) = 0)) AND (wi.qr_generated_at IS NULL)) THEN 'QR_NOT_ISSUED'::text
            WHEN ((wi.timesheet_id IS NOT NULL) AND (wi.qr_status = 'PENDING'::timesheet_qr_status_enum) AND ((wi.qr_token IS NOT NULL) AND (length(btrim(wi.qr_token)) > 0)) AND (wi.qr_generated_at IS NOT NULL) AND (wi.qr_scanned_at IS NULL)) THEN 'QR_ISSUED_AWAITING_SIGNATURE'::text
            WHEN (wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum) THEN 'READY_FOR_INVOICE'::text
            WHEN (wi.processing_status = 'READY_FOR_HR'::ts_fin_processing_status_enum) THEN 'READY_FOR_HR'::text
            WHEN (wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum) THEN 'PENDING_AUTH'::text
            WHEN (wi.processing_status = 'UNPROCESSED'::ts_fin_processing_status_enum) THEN 'UNPROCESSED'::text
            WHEN (wi.processing_status = ANY (ARRAY['UNASSIGNED'::ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum, 'RATE_MISSING'::ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum])) THEN 'NEEDS_ATTENTION'::text
            ELSE 'UNKNOWN'::text
        END AS summary_stage,
        CASE
            WHEN ((wi.sheet_scope = 'DAILY'::timesheet_scope_enum) AND (wi.submission_mode = 'ELECTRONIC'::submission_mode_enum)) THEN 'DAILY_ELECTRONIC'::text
            WHEN ((wi.sheet_scope = 'DAILY'::timesheet_scope_enum) AND (wi.submission_mode = 'MANUAL'::submission_mode_enum)) THEN 'DAILY_MANUAL'::text
            WHEN ((wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (wi.client_autoprocess_hr IS TRUE)) THEN 'WEEKLY_HEALTHROSTER'::text
            WHEN ((wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (wi.basis = 'NHSP_ADJUSTMENT'::timesheet_fin_basis_enum)) THEN 'WEEKLY_NHSP_ADJUSTMENT'::text
            WHEN ((wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (wi.basis = 'NHSP'::timesheet_fin_basis_enum)) THEN 'WEEKLY_NHSP'::text
            WHEN ((wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (wi.client_is_nhsp IS TRUE)) THEN 'WEEKLY_NHSP'::text
            WHEN ((wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (wi.submission_mode = 'ELECTRONIC'::submission_mode_enum)) THEN 'WEEKLY_ELECTRONIC'::text
            WHEN ((wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum) AND (wi.submission_mode = 'MANUAL'::submission_mode_enum)) THEN 'WEEKLY_MANUAL'::text
            ELSE 'UNKNOWN'::text
        END AS route_type,
    wi.contract_week_id,
    wi.contract_week_ending_date,
    wi.contract_week_status,
    wi.additional_seq,
    wi.is_adjustment,
    wi.qr_status,
    wi.pay_adjustment_count,
    (wi.pay_adjustment_count > 0) AS has_pay_adjustments,
    (COALESCE(wi.is_adjustment, false) OR (wi.pay_adjustment_count > 0)) AS is_adjusted,
    (wi.qr_status IS NOT NULL) AS is_qr,
    ((wi.processing_status = ANY (ARRAY['UNASSIGNED'::ts_fin_processing_status_enum, 'CLIENT_UNRESOLVED'::ts_fin_processing_status_enum, 'RATE_MISSING'::ts_fin_processing_status_enum, 'PAY_CHANNEL_MISSING'::ts_fin_processing_status_enum])) OR ((NOT ((wi.timesheet_id IS NOT NULL) AND (COALESCE(wi.client_hr_validation_required, false) = true) AND (COALESCE(wi.client_no_timesheet_required, false) = false) AND (COALESCE(wi.total_hours, (0)::numeric) > (0)::numeric))) AND ((wi.hr_crosscheck_status IS NOT NULL) AND (wi.hr_crosscheck_status <> 'OK'::text))) OR ((NOT ((wi.timesheet_id IS NOT NULL) AND (COALESCE(wi.client_hr_validation_required, false) = true) AND (COALESCE(wi.client_no_timesheet_required, false) = false) AND (COALESCE(wi.total_hours, (0)::numeric) > (0)::numeric))) AND (wi.hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'::text])) OR ((ic.issue_codes_final IS NOT NULL) AND (array_length(ic.issue_codes_final, 1) > 0))) AS needs_attention,
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
    ((wi.timesheet_id IS NOT NULL) AND (COALESCE(wi.client_hr_validation_required, false) = true) AND (COALESCE(wi.client_no_timesheet_required, false) = false) AND (COALESCE(wi.total_hours, (0)::numeric) > (0)::numeric)) AS hr_validation_required_for_invoice,
    seg.seg_total AS invoice_segments_total,
    seg.seg_locked AS invoice_segments_locked,
        CASE
            WHEN (seg.seg_total IS NULL) THEN NULL::integer
            ELSE GREATEST((seg.seg_total - COALESCE(seg.seg_locked, 0)), 0)
        END AS invoice_segments_unlocked,
        CASE
            WHEN (seg.seg_total IS NULL) THEN NULL::text
            WHEN (COALESCE(seg.seg_locked, 0) = 0) THEN 'NOT_INVOICED'::text
            WHEN (COALESCE(seg.seg_locked, 0) >= seg.seg_total) THEN 'FULLY_INVOICED'::text
            ELSE 'PARTIALLY_INVOICED'::text
        END AS invoice_segment_stage,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN 'UNPROCESSED'::text
            WHEN (wi.processing_status = 'UNPROCESSED'::ts_fin_processing_status_enum) THEN 'UNPROCESSED'::text
            WHEN ((wi.locked_by_invoice_id IS NOT NULL) OR (COALESCE(seg.seg_locked, 0) > 0) OR ((seg.seg_total IS NOT NULL) AND (COALESCE(seg.seg_locked, 0) > 0))) THEN 'INVOICED'::text
            WHEN ((wi.timesheet_id IS NOT NULL) AND (wi.authorised_at_server IS NULL) AND ((wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum) OR ((COALESCE(wi.client_requires_hr, false) = true) AND (COALESCE(wi.client_autoprocess_hr, false) = false) AND (array_length(ic.issue_codes_final, 1) = 1) AND (ic.issue_codes_final @> ARRAY['Authorisation'::text])))) THEN 'AWAITING_AUTHORISATION'::text
            WHEN ((wi.timesheet_id IS NOT NULL) AND (wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum)) THEN 'AUTHORISED_FOR_INVOICING'::text
            ELSE 'PROCESSING_DELAYED'::text
        END AS tools_stage,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN 'Unprocessed'::text
            WHEN (wi.processing_status = 'UNPROCESSED'::ts_fin_processing_status_enum) THEN 'Unprocessed'::text
            WHEN ((wi.locked_by_invoice_id IS NOT NULL) OR (COALESCE(seg.seg_locked, 0) > 0) OR ((seg.seg_total IS NOT NULL) AND (COALESCE(seg.seg_locked, 0) > 0))) THEN
            CASE
                WHEN ((seg.seg_total IS NOT NULL) AND (COALESCE(seg.seg_locked, 0) > 0) AND (COALESCE(seg.seg_locked, 0) < seg.seg_total)) THEN 'Partially Invoiced'::text
                ELSE 'Invoiced'::text
            END
            WHEN ((wi.timesheet_id IS NOT NULL) AND (ic.issue_codes_final @> ARRAY['Awaiting signed QR timesheet'::text])) THEN 'Awaiting signed QR timesheet'::text
            WHEN ((wi.timesheet_id IS NOT NULL) AND (wi.authorised_at_server IS NULL) AND ((wi.processing_status = 'PENDING_AUTH'::ts_fin_processing_status_enum) OR ((COALESCE(wi.client_requires_hr, false) = true) AND (COALESCE(wi.client_autoprocess_hr, false) = false) AND (array_length(ic.issue_codes_final, 1) = 1) AND (ic.issue_codes_final @> ARRAY['Authorisation'::text])))) THEN 'Awaiting Authorisation'::text
            WHEN ((wi.timesheet_id IS NOT NULL) AND (wi.processing_status = 'READY_FOR_INVOICE'::ts_fin_processing_status_enum)) THEN 'Authorised for Invoicing'::text
            ELSE 'Processing Delayed'::text
        END AS processing_status_display,
    COALESCE(seg.invoice_paid_any, false) AS invoice_is_paid,
        CASE
            WHEN ((wi.timesheet_id IS NOT NULL) AND (pc.precheck_status = 'BLOCK_NO_REFERENCE'::text)) THEN true
            ELSE false
        END AS refs_block_invoicing,
        CASE
            WHEN ((wi.timesheet_id IS NOT NULL) AND (COALESCE(pc.issue_missing_reference, false) = true)) THEN true
            ELSE false
        END AS refs_block_issuing_invoices,
        CASE
            WHEN ((wi.timesheet_id IS NOT NULL) AND (pc.precheck_status = 'BLOCK_NO_REFERENCE'::text) AND (COALESCE(pc.issue_missing_reference, false) = true)) THEN true
            ELSE false
        END AS refs_block_invoice_and_issuing,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN 'NONE'::text
            ELSE COALESCE(payr.pay_icon_code, 'NONE'::text)
        END AS pay_icon_code,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN NULL::text
            ELSE payr.pay_status_code
        END AS pay_status_code,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN NULL::timestamp with time zone
            ELSE payr.pay_paid_at_utc
        END AS pay_paid_at_utc,
        CASE
            WHEN (wi.timesheet_id IS NULL) THEN NULL::numeric
            ELSE payr.net_delta_ex_vat
        END AS net_delta_ex_vat
   FROM (((((with_issues wi
     LEFT JOIN pay_rollup payr ON ((payr.timesheet_id = wi.timesheet_id)))
     LEFT JOIN LATERAL ( SELECT timesheet_pdf_reference_sig(wi.timesheet_id) AS current_refs_sig) rs ON (true))
     LEFT JOIN LATERAL ( SELECT pc0.precheck_status,
            pc0.issue_missing_reference,
            pc0.issue_missing_reference_count
           FROM v_ts_invoice_precheck pc0
          WHERE (pc0.timesheet_id = wi.timesheet_id)
         LIMIT 1) pc ON (true))
     LEFT JOIN LATERAL ( SELECT (((
                CASE
                    WHEN ((wi.timesheet_id IS NOT NULL) AND ((wi.timesheet_id IS NOT NULL) AND (COALESCE(wi.client_hr_validation_required, false) = true) AND (COALESCE(wi.client_no_timesheet_required, false) = false) AND (COALESCE(wi.total_hours, (0)::numeric) > (0)::numeric)) AND (NOT ((wi.validation_status = 'VALIDATION_OK'::validation_status_enum) OR (wi.validation_status = 'OVERRIDDEN'::validation_status_enum)))) THEN ARRAY[]::text[]
                    WHEN ((wi.timesheet_id IS NOT NULL) AND (pc.precheck_status = 'BLOCK_NO_REFERENCE'::text)) THEN ARRAY['Refs - Can''t invoice'::text]
                    WHEN ((wi.timesheet_id IS NOT NULL) AND (COALESCE(pc.issue_missing_reference, false) = true)) THEN ARRAY['Refs - Send Invoice will be blocked'::text]
                    ELSE ARRAY[]::text[]
                END ||
                CASE
                    WHEN ((wi.timesheet_id IS NOT NULL) AND (pc.precheck_status = 'BLOCK_NO_PDF'::text)) THEN ARRAY['Timesheet evidence missing'::text]
                    ELSE ARRAY[]::text[]
                END) || array_remove(array_remove(COALESCE(wi.issue_codes, ARRAY[]::text[]), 'Reference'::text), 'Timesheet evidence'::text)) ||
                CASE
                    WHEN ((wi.timesheet_id IS NOT NULL) AND (COALESCE(wi.client_no_timesheet_required, false) = false) AND (COALESCE(wi.client_is_nhsp, false) = false) AND (((wi.submission_mode = 'ELECTRONIC'::submission_mode_enum) AND (wi.manual_pdf_r2_key IS NULL) AND (wi.generated_pdf_at_utc IS NOT NULL) AND ((wi.generated_pdf_refs_sig IS NULL) OR ((rs.current_refs_sig IS NOT NULL) AND (wi.generated_pdf_refs_sig <> rs.current_refs_sig)))) OR ((((wi.qr_token IS NOT NULL) AND (wi.qr_generated_at IS NOT NULL)) OR (wi.qr_last_sent_hash IS NOT NULL)) AND (wi.qr_sent_refs_sig IS NOT NULL) AND ((rs.current_refs_sig IS NOT NULL) AND (wi.qr_sent_refs_sig <> rs.current_refs_sig))))) THEN ARRAY['Refs - Timesheet PDF invalid'::text]
                    ELSE ARRAY[]::text[]
                END) AS issue_codes_final) ic ON (true))
     LEFT JOIN LATERAL ( SELECT
                CASE
                    WHEN (wi.timesheet_id IS NULL) THEN NULL::integer
                    WHEN ((tf.invoice_breakdown_json IS NOT NULL) AND (jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text) AND (COALESCE((tf.invoice_breakdown_json ->> 'mode'::text), ''::text) = 'SEGMENTS'::text) AND (jsonb_typeof((tf.invoice_breakdown_json -> 'segments'::text)) = 'array'::text)) THEN jsonb_array_length((tf.invoice_breakdown_json -> 'segments'::text))
                    ELSE 1
                END AS seg_total,
                CASE
                    WHEN (wi.timesheet_id IS NULL) THEN NULL::integer
                    WHEN ((tf.invoice_breakdown_json IS NOT NULL) AND (jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text) AND (COALESCE((tf.invoice_breakdown_json ->> 'mode'::text), ''::text) = 'SEGMENTS'::text) AND (jsonb_typeof((tf.invoice_breakdown_json -> 'segments'::text)) = 'array'::text)) THEN ( SELECT (count(*))::integer AS count
                       FROM jsonb_array_elements((tf.invoice_breakdown_json -> 'segments'::text)) s(value)
                      WHERE (NULLIF(btrim(COALESCE((s.value ->> 'invoice_locked_invoice_id'::text), ''::text)), ''::text) IS NOT NULL))
                    ELSE
                    CASE
                        WHEN (wi.locked_by_invoice_id IS NULL) THEN 0
                        ELSE 1
                    END
                END AS seg_locked,
                CASE
                    WHEN (wi.timesheet_id IS NULL) THEN NULL::boolean
                    WHEN ((tf.invoice_breakdown_json IS NOT NULL) AND (jsonb_typeof(tf.invoice_breakdown_json) = 'object'::text) AND (COALESCE((tf.invoice_breakdown_json ->> 'mode'::text), ''::text) = 'SEGMENTS'::text) AND (jsonb_typeof((tf.invoice_breakdown_json -> 'segments'::text)) = 'array'::text)) THEN (EXISTS ( SELECT 1
                       FROM (jsonb_array_elements((tf.invoice_breakdown_json -> 'segments'::text)) s(value)
                         JOIN invoices inv2 ON ((inv2.id =
                            CASE
                                WHEN ((NULLIF(btrim(COALESCE((s.value ->> 'invoice_locked_invoice_id'::text), ''::text)), ''::text) IS NOT NULL) AND ((s.value ->> 'invoice_locked_invoice_id'::text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'::text)) THEN ((s.value ->> 'invoice_locked_invoice_id'::text))::uuid
                                ELSE NULL::uuid
                            END)))
                      WHERE (inv2.status = 'PAID'::invoice_status_enum)))
                    ELSE (EXISTS ( SELECT 1
                       FROM invoices inv2
                      WHERE ((inv2.id = wi.locked_by_invoice_id) AND (inv2.status = 'PAID'::invoice_status_enum))))
                END AS invoice_paid_any
           FROM timesheets_financials tf
          WHERE ((tf.is_current = true) AND (tf.timesheet_id = wi.timesheet_id))
          ORDER BY tf.created_at DESC
         LIMIT 1) seg ON (true));;

