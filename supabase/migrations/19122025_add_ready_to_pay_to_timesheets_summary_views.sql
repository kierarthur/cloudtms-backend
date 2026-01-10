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

    -- ✅ NEW: category backing fields
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

    -- ✅ UPDATED: show hint when unresolved (candidate_id null) and hint has useful fields
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

    COALESCE(ct.autoprocess_hr, ch.autoprocess_hr, false) AS client_autoprocess_hr,
    COALESCE(ct.requires_hr, ch.requires_hr, false) AS client_requires_hr,
    COALESCE(ct.no_timesheet_required, ch.no_timesheet_required, false) AS client_no_timesheet_required,

    COALESCE(ch.pay_reference_required, false) AS client_pay_reference_required,
    COALESCE(ch.invoice_reference_required, false) AS client_invoice_reference_required,
    COALESCE(ch.hr_validation_required, false) AS client_hr_validation_required,
    COALESCE(ch.ts_reference_required, false) AS client_ts_reference_required,

    COALESCE(ct.is_nhsp, ch.is_nhsp, false) AS client_is_nhsp,

    tf.has_rate_issue,
    tf.has_pay_channel_issue,
    tf.hr_crosscheck_status,
    tf.hr_crosscheck_issues,
    tf.external_source_rows_json,

    ts.reference_number,
    ts.day_references_json,

    ts.actual_schedule_json,

    ts.r2_nurse_key,
    ts.r2_auth_key,
    ts.manual_pdf_r2_key,

    ct.require_reference_to_pay,
    ct.require_reference_to_invoice,

    COALESCE(ea.evidence_count, 0) AS evidence_count,

    -- existing expense/mileage fields currently in your view
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

    -- ✅ NEW columns (union compat; will be exposed at END of view output)
    tf.travel_pay_ex_vat,
    tf.travel_charge_ex_vat,
    tf.accommodation_pay_ex_vat,
    tf.accommodation_charge_ex_vat,
    tf.other_pay_ex_vat,
    tf.other_charge_ex_vat

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

    COALESCE(ct.autoprocess_hr, ch.autoprocess_hr, false) AS client_autoprocess_hr,
    COALESCE(ct.requires_hr, ch.requires_hr, false) AS client_requires_hr,
    COALESCE(ct.no_timesheet_required, ch.no_timesheet_required, false) AS client_no_timesheet_required,

    COALESCE(ch.pay_reference_required, false) AS client_pay_reference_required,
    COALESCE(ch.invoice_reference_required, false) AS client_invoice_reference_required,
    COALESCE(ch.hr_validation_required, false) AS client_hr_validation_required,
    COALESCE(ch.ts_reference_required, false) AS client_ts_reference_required,

    COALESCE(ct.is_nhsp, ch.is_nhsp, false) AS client_is_nhsp,

    false AS has_rate_issue,
    false AS has_pay_channel_issue,
    NULL::text AS hr_crosscheck_status,
    NULL::text[] AS hr_crosscheck_issues,
    NULL::jsonb AS external_source_rows_json,

    NULL::text AS reference_number,
    NULL::jsonb AS day_references_json,

    NULL::jsonb AS actual_schedule_json,

    NULL::text AS r2_nurse_key,
    NULL::text AS r2_auth_key,
    NULL::text AS manual_pdf_r2_key,

    ct.require_reference_to_pay,
    ct.require_reference_to_invoice,

    0 AS evidence_count,

    -- existing expense/mileage fields currently in your view
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

    -- existing last column in your view
    NULL::jsonb AS candidate_hint_text,

    -- existing appended columns (already in your view output)
    NULL::numeric AS expenses_pay_ex_vat,
    NULL::text    AS expenses_description,
    NULL::numeric AS mileage_units,
    NULL::numeric AS mileage_pay_rate,
    NULL::numeric AS mileage_charge_rate,
    NULL::numeric AS mileage_pay_ex_vat,

    -- ✅ NEW appended columns (union compat)
    NULL::numeric AS travel_pay_ex_vat,
    NULL::numeric AS travel_charge_ex_vat,
    NULL::numeric AS accommodation_pay_ex_vat,
    NULL::numeric AS accommodation_charge_ex_vat,
    NULL::numeric AS other_pay_ex_vat,
    NULL::numeric AS other_charge_ex_vat

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
                CASE
                  WHEN ar.hr_crosscheck_status = 'HOURS_MISMATCH_HR'::text OR ar.hr_crosscheck_issues && ARRAY['HOURS_MISMATCH_HR'::text]
                    THEN ARRAY['Hours mismatch HR'::text]
                  ELSE ARRAY[]::text[] END
              ) ||
              CASE WHEN ar.hr_crosscheck_issues && ARRAY['HR_HOURS_MISSING'::text] THEN ARRAY['HR hours missing'::text] ELSE ARRAY[]::text[] END
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

        -- ✅ UPDATED: Expenses evidence based on category claims + timesheet_evidence.kind
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

      -- ✅ UPDATED: Mileage evidence based on mileage claim + timesheet_evidence.kind
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
        AND (ar.client_ts_reference_required OR ar.client_pay_reference_required OR ar.client_invoice_reference_required)
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
          (
            ar.submission_mode <> 'MANUAL'::submission_mode_enum
            AND (
              ar.day_references_json IS NULL
              OR ar.day_references_json = '{}'::jsonb
              OR NOT EXISTS (
                SELECT 1
                FROM jsonb_each_text(ar.day_references_json) j(k, v)
                WHERE btrim(j.v) <> ''
              )
            )
          )
        )
        THEN ARRAY['Reference'::text]
      ELSE ARRAY[]::text[]
    END ||
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND ar.client_hr_validation_required
        AND ar.validation_status IS NOT NULL
        AND (ar.validation_status <> ALL (ARRAY['VALIDATION_OK'::validation_status_enum, 'OVERRIDDEN'::validation_status_enum]))
        THEN ARRAY['Validation'::text]
      ELSE ARRAY[]::text[]
    END ||
    CASE
      WHEN ar.timesheet_id IS NOT NULL
        AND ar.client_requires_hr
        AND NOT ar.client_autoprocess_hr
        AND ar.authorised_at_server IS NULL
        AND (ar.processing_status = ANY (ARRAY['PENDING_AUTH'::ts_fin_processing_status_enum, 'READY_FOR_HR'::ts_fin_processing_status_enum]))
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
      OR (
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
                  (reference_number IS NOT NULL AND length(btrim(reference_number)) > 0)
                  OR (
                    day_references_json IS NOT NULL
                    AND day_references_json <> '{}'::jsonb
                    AND EXISTS (
                      SELECT 1
                      FROM jsonb_each_text(day_references_json) j(k, v)
                      WHERE btrim(j.v) <> ''
                    )
                  )
                )
            END
          ELSE
            (reference_number IS NOT NULL AND length(btrim(reference_number)) > 0)
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
    WHEN locked_by_invoice_id IS NOT NULL THEN 'INVOICED'
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
  OR (hr_crosscheck_status IS NOT NULL AND hr_crosscheck_status <> 'OK')
  OR (hr_crosscheck_issues && ARRAY['DUPLICATE_CONTRACTS'])
  OR (issue_codes IS NOT NULL AND array_length(issue_codes, 1) > 0) AS needs_attention,

  client_autoprocess_hr,
  has_rate_issue,
  has_pay_channel_issue,
  hr_crosscheck_status,
  hr_crosscheck_issues,
  external_source_rows_json,
  issue_codes,

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

  -- existing last column in your view
  candidate_hint_text,

  -- existing appended columns (already in your view)
  expenses_pay_ex_vat,
  expenses_description,
  mileage_units,
  mileage_pay_rate,
  mileage_charge_rate,
  mileage_pay_ex_vat,

  -- ✅ NEW columns APPENDED AT END
  travel_pay_ex_vat,
  travel_charge_ex_vat,
  accommodation_pay_ex_vat,
  accommodation_charge_ex_vat,
  other_pay_ex_vat,
  other_charge_ex_vat
FROM with_issues;


CREATE OR REPLACE VIEW public.v_timesheets_summary AS
SELECT
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
  v.is_adjustment,
  v.qr_status,
  v.pay_adjustment_count,
  v.has_pay_adjustments,
  v.is_adjusted,
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

  -- existing last column in your view
  v.candidate_hint_text,

  -- existing appended columns (already in your view)
  v.expenses_pay_ex_vat,
  v.expenses_description,
  v.mileage_units,
  v.mileage_pay_rate,
  v.mileage_charge_rate,
  v.mileage_pay_ex_vat,

  -- ✅ NEW columns APPENDED AT END
  v.travel_pay_ex_vat,
  v.travel_charge_ex_vat,
  v.accommodation_pay_ex_vat,
  v.accommodation_charge_ex_vat,
  v.other_pay_ex_vat,
  v.other_charge_ex_vat

FROM public.v_timesheets_summary_base v
LEFT JOIN public.contract_weeks cw ON cw.id = v.contract_week_id
LEFT JOIN public.timesheets ts2 ON ts2.timesheet_id = v.timesheet_id;

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
  tf.other_charge_ex_vat

FROM timesheets t
LEFT JOIN timesheets_financials tf
  ON tf.timesheet_id = t.timesheet_id
 AND tf.is_current = true
LEFT JOIN timesheet_validations tv
  ON tv.timesheet_id = t.timesheet_id
LEFT JOIN nhsp_agg n
  ON n.timesheet_id = t.timesheet_id;

GRANT SELECT ON public.v_timesheets_details TO service_role;
GRANT SELECT ON public.v_timesheets_details TO authenticated;

-- Ensure PostgREST sees new columns immediately after commit
SELECT pg_notify('pgrst', 'reload schema');
