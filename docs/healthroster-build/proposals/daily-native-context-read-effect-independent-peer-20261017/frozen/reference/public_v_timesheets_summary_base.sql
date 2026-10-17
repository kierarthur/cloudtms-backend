CREATE OR REPLACE VIEW public.v_timesheets_summary_base
WITH (security_invoker=true) AS
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
    (
      COALESCE(ts.is_adjustment, FALSE) = TRUE
      OR COALESCE(cw.is_adjustment, FALSE) = TRUE
      OR COALESCE(cw.additional_seq, 0) > 0
      OR ts.parent_timesheet_id IS NOT NULL
      OR ts.correction_id IS NOT NULL
      OR ts.correction_kind IS NOT NULL
    ) AS is_adjustment,

    ts.qr_status,
    ts.qr_token,
    ts.qr_generated_at,
    ts.qr_scanned_at,

    COALESCE(pa.pay_adjustment_count, 0) AS pay_adjustment_count,

    COALESCE((authority.settings_json#>>'{values,autoprocess_hr}')::boolean,false) AS client_autoprocess_hr,
    COALESCE((authority.settings_json#>>'{values,requires_hr}')::boolean,false) AS client_requires_hr,
    COALESCE((authority.settings_json#>>'{values,no_timesheet_required}')::boolean,false) AS client_no_timesheet_required,

    COALESCE((authority.settings_json#>>'{values,require_reference_to_pay}')::boolean,false) AS client_pay_reference_required,
    COALESCE((authority.settings_json#>>'{values,require_reference_to_invoice}')::boolean,false) AS client_invoice_reference_required,
    COALESCE((authority.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false) AS client_hr_validation_required,
    COALESCE((authority.settings_json#>>'{values,ts_reference_required}')::boolean,false) AS client_ts_reference_required,

    COALESCE((authority.settings_json#>>'{values,is_nhsp}')::boolean,false) AS client_is_nhsp,

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

    COALESCE((authority.settings_json#>>'{values,require_reference_to_pay}')::boolean,false) AS require_reference_to_pay,
    COALESCE((authority.settings_json#>>'{values,require_reference_to_invoice}')::boolean,false) AS require_reference_to_invoice,

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
  CROSS JOIN LATERAL (
    SELECT private._contract_settings_effective_core_v1(
      COALESCE(tf.client_id, ct.client_id),
      COALESCE(ts.contract_id, cw.contract_id),
      COALESCE(
        (ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
        (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,
        ts.week_ending_date
      ),
      CASE WHEN ts.sheet_scope='DAILY'::timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
      ts.timesheet_id
    ) AS settings_json
  ) authority
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
    (
      COALESCE(cw.is_adjustment, FALSE) = TRUE
      OR COALESCE(cw.additional_seq, 0) > 0
    ) AS is_adjustment,

    NULL::timesheet_qr_status_enum AS qr_status,
    NULL::text AS qr_token,
    NULL::timestamp with time zone AS qr_generated_at,
    NULL::timestamp with time zone AS qr_scanned_at,

    0 AS pay_adjustment_count,

    COALESCE((authority.settings_json#>>'{values,autoprocess_hr}')::boolean,false) AS client_autoprocess_hr,
    COALESCE((authority.settings_json#>>'{values,requires_hr}')::boolean,false) AS client_requires_hr,
    COALESCE((authority.settings_json#>>'{values,no_timesheet_required}')::boolean,false) AS client_no_timesheet_required,

    COALESCE((authority.settings_json#>>'{values,require_reference_to_pay}')::boolean,false) AS client_pay_reference_required,
    COALESCE((authority.settings_json#>>'{values,require_reference_to_invoice}')::boolean,false) AS client_invoice_reference_required,
    COALESCE((authority.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false) AS client_hr_validation_required,
    COALESCE((authority.settings_json#>>'{values,ts_reference_required}')::boolean,false) AS client_ts_reference_required,

    COALESCE((authority.settings_json#>>'{values,is_nhsp}')::boolean,false) AS client_is_nhsp,

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

    COALESCE((authority.settings_json#>>'{values,require_reference_to_pay}')::boolean,false) AS require_reference_to_pay,
    COALESCE((authority.settings_json#>>'{values,require_reference_to_invoice}')::boolean,false) AS require_reference_to_invoice,

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
  CROSS JOIN LATERAL (
    SELECT CASE
      WHEN cw.settings_authority_json IS NULL OR cw.settings_authority_json='{}'::jsonb
        THEN private._contract_settings_effective_core_v1(
          ct.client_id,ct.id,cw.week_ending_date,'WEEKLY',NULL::uuid)
      ELSE cw.settings_authority_json
    END AS settings_json
  ) authority
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
    WHEN wi.sheet_scope = 'DAILY'::timesheet_scope_enum AND wi.submission_mode = 'ELECTRONIC'::submission_mode_enum THEN 'DAILY_ELECTRONIC'
    WHEN wi.sheet_scope = 'DAILY'::timesheet_scope_enum AND wi.submission_mode = 'MANUAL'::submission_mode_enum THEN 'DAILY_MANUAL'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum
      AND (
        COALESCE(wi.is_adjustment, FALSE) = TRUE
        OR COALESCE(wi.additional_seq, 0) > 0
      )
      AND (
        wi.basis = 'NHSP_ADJUSTMENT'::timesheet_fin_basis_enum
        OR wi.basis = 'NHSP'::timesheet_fin_basis_enum
        OR COALESCE(wi.client_is_nhsp, FALSE) = TRUE
      ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum
      AND (
        COALESCE(wi.is_adjustment, FALSE) = TRUE
        OR COALESCE(wi.additional_seq, 0) > 0
      )
      AND (
        wi.basis = 'HEALTHROSTER_ADJUSTMENT'::timesheet_fin_basis_enum
        OR wi.basis = 'HEALTHROSTER_SELF_BILL'::timesheet_fin_basis_enum
        OR COALESCE(wi.client_autoprocess_hr, FALSE) = TRUE
      ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum
      AND (
        COALESCE(wi.is_adjustment, FALSE) = TRUE
        OR COALESCE(wi.additional_seq, 0) > 0
      ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.basis = 'NHSP_ADJUSTMENT'::timesheet_fin_basis_enum THEN 'WEEKLY_NHSP_ADJUSTMENT'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.basis = 'HEALTHROSTER_ADJUSTMENT'::timesheet_fin_basis_enum THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.basis = 'NHSP'::timesheet_fin_basis_enum THEN 'WEEKLY_NHSP'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.client_is_nhsp IS TRUE THEN 'WEEKLY_NHSP'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND (wi.client_autoprocess_hr IS TRUE OR wi.basis = 'HEALTHROSTER_SELF_BILL'::timesheet_fin_basis_enum) THEN 'WEEKLY_HEALTHROSTER'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.submission_mode = 'ELECTRONIC'::submission_mode_enum THEN 'WEEKLY_ELECTRONIC'
    WHEN wi.sheet_scope = 'WEEKLY'::timesheet_scope_enum AND wi.submission_mode = 'MANUAL'::submission_mode_enum THEN 'WEEKLY_MANUAL'
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
      -- Refs/PDF baseline invalid (existing behaviour)
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