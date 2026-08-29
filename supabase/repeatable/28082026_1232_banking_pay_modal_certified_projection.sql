-- Shared, data-free projection extracted from the certified preview-page reader.
-- No economic calculation or eligibility rule is added here. The legacy parity
-- tests compare both this source and real results before the old reader delegates.
\set ON_ERROR_STOP on
\ir 20082026_1502_pay_workbench_preview_recovery_residual_current_v1.sql

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_eligible_rows_v2(
  p_session_id uuid, p_session_version bigint, p_section text
) RETURNS SETOF public.banking_pay_workbench_preview_rows
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
SET plan_cache_mode TO 'force_custom_plan'
AS $function$
BEGIN
  -- Keep this certified statement parameter-planned through PL/pgSQL. The
  -- SQL-language wrapper's warm plan caused quadratic repeated section checks.
  -- Preserve the complete certified predicate. Materialize its final ID set
  -- once so the physical-row join cannot repeat the completed section checks.
  RETURN QUERY
  WITH session_preview_rows AS MATERIALIZED (
    SELECT
      preview_row.*,
      UPPER(BTRIM(COALESCE(
        preview_row.row_json->>'line_type',
        preview_row.row_json#>>'{preview_contract,line_type}',
        ''
      ))) AS authority_line_type,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(
          preview_row.row_json->>'finance_case_id', ''
        )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN BTRIM(preview_row.row_json->>'finance_case_id')::uuid
        ELSE NULL::uuid
      END AS typed_finance_case_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(
          preview_row.row_json->>'finance_component_id', ''
        )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN BTRIM(preview_row.row_json->>'finance_component_id')::uuid
        ELSE NULL::uuid
      END AS typed_finance_component_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(
          preview_row.row_json->>'post_draft_overlay_pay_batch_id', ''
        )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN BTRIM(preview_row.row_json->>'post_draft_overlay_pay_batch_id')::uuid
        ELSE NULL::uuid
      END AS typed_overlay_pay_batch_id
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = p_session_id
      AND preview_row.session_version = p_session_version
  ), owned_recovery_cases AS MATERIALIZED (
    SELECT DISTINCT
      preview_row.candidate_id,
      preview_row.typed_finance_case_id AS finance_case_id
    FROM session_preview_rows AS preview_row
    JOIN public.pay_advances AS finance_case
      ON finance_case.id = preview_row.typed_finance_case_id
     AND finance_case.candidate_id = preview_row.candidate_id
    WHERE preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY'
      AND preview_row.status = 'READY'
  ), recovery_reservation_totals AS MATERIALIZED (
    SELECT
      recovery_case.candidate_id,
      recovery_case.finance_case_id,
      ROUND(COALESCE(SUM(reservation.reserved_amount), 0), 2)
        AS current_active_reserved_ex_vat,
      COUNT(reservation.id)::integer AS current_active_reservation_count
    FROM owned_recovery_cases AS recovery_case
    LEFT JOIN public.pay_advance_reservations AS reservation
      ON reservation.finance_case_id = recovery_case.finance_case_id
     AND UPPER(BTRIM(COALESCE(reservation.status, '')))
          IN ('RESERVED', 'COMMITTED')
    GROUP BY recovery_case.candidate_id, recovery_case.finance_case_id
  ), active_item_overlap_rows AS MATERIALIZED (
    SELECT DISTINCT preview_row.id
    FROM session_preview_rows AS preview_row
    JOIN public.pay_batch_candidates AS active_candidate
      ON active_candidate.candidate_id = preview_row.candidate_id
    JOIN public.pay_batches AS active_batch
      ON active_batch.id = active_candidate.pay_batch_id
     AND active_batch.cancelled_at_utc IS NULL
     AND UPPER(BTRIM(COALESCE(active_batch.status, ''))) IN (
       'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
       'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
       'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
     )
    JOIN public.pay_batch_items AS active_item
      ON active_item.pay_batch_candidate_id = active_candidate.id
     AND COALESCE(active_item.is_voided, false) = false
     AND (
       (
         COALESCE(
           active_item.timesheet_id,
           CASE
             WHEN UPPER(BTRIM(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                active_item.frozen_source_basis_json->>'timesheet_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN BTRIM(active_item.frozen_source_basis_json->>'timesheet_id')::uuid
             ELSE NULL::uuid
           END,
           CASE
             WHEN UPPER(BTRIM(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                active_item.frozen_source_basis_json->>'linked_timesheet_id', ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN BTRIM(active_item.frozen_source_basis_json->>'linked_timesheet_id')::uuid
             ELSE NULL::uuid
           END,
           CASE
             WHEN UPPER(BTRIM(COALESCE(active_item.item_type, ''))) = 'OVERPAYMENT_RECOVERY'
              AND NULLIF(BTRIM(COALESCE(
                active_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
                ''
              )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN BTRIM(
                 active_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
               )::uuid
             ELSE NULL::uuid
           END
         ) = preview_row.timesheet_id
         AND (
           (
             NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_type, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_value, '')), '') IS NOT NULL
             AND UPPER(BTRIM(active_item.frozen_component_key_type))
               = UPPER(BTRIM(COALESCE(
                 preview_row.key_type, preview_row.row_json->>'key_type', ''
               )))
             AND BTRIM(active_item.frozen_component_key_value)
               = BTRIM(COALESCE(
                 preview_row.key_value, preview_row.row_json->>'key_value', ''
               ))
           )
           OR (
             NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_type, '')), '') IS NULL
             AND NULLIF(BTRIM(COALESCE(active_item.frozen_component_key_value, '')), '') IS NULL
           )
         )
       )
       OR active_item.finance_component_id = preview_row.typed_finance_component_id
       OR active_item.finance_case_id = preview_row.typed_finance_case_id
       OR (
         NULLIF(BTRIM(COALESCE(
           preview_row.row_json->>'canonical_correction_key', ''
         )), '') IS NOT NULL
         AND NULLIF(BTRIM(COALESCE(
           active_item.frozen_component_snapshot_json->>'canonical_correction_key',
           active_item.frozen_resolution_payload_json->>'canonical_correction_key',
           ''
         )), '') = NULLIF(BTRIM(COALESCE(
           preview_row.row_json->>'canonical_correction_key', ''
         )), '')
       )
     )
  ), strict_recovery_siblings AS MATERIALIZED (
    SELECT
      recovery_sibling.id,
      recovery_sibling.candidate_id,
      recovery_sibling.timesheet_id
    FROM session_preview_rows AS recovery_sibling
    WHERE recovery_sibling.status = 'READY'
      AND recovery_sibling.authority_line_type = 'OVERPAYMENT_RECOVERY'

    UNION

    SELECT DISTINCT
      recovery_sibling.id,
      recovery_sibling.candidate_id,
      recovery_sibling.timesheet_id
    FROM session_preview_rows AS recovery_sibling
    JOIN public.pay_batches AS frozen_recovery_batch
      ON frozen_recovery_batch.id = recovery_sibling.typed_overlay_pay_batch_id
     AND frozen_recovery_batch.cancelled_at_utc IS NULL
     AND UPPER(BTRIM(COALESCE(frozen_recovery_batch.status, ''))) IN (
       'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM', 'PARTIAL',
       'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
       'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
     )
    JOIN public.pay_batch_candidates AS frozen_recovery_candidate
      ON frozen_recovery_candidate.pay_batch_id = frozen_recovery_batch.id
     AND frozen_recovery_candidate.candidate_id = recovery_sibling.candidate_id
    JOIN public.pay_batch_items AS frozen_recovery_item
      ON frozen_recovery_item.pay_batch_candidate_id = frozen_recovery_candidate.id
     AND COALESCE(frozen_recovery_item.is_voided, false) = false
     AND UPPER(BTRIM(COALESCE(frozen_recovery_item.item_type, '')))
          = 'OVERPAYMENT_RECOVERY'
     AND frozen_recovery_item.finance_case_id = recovery_sibling.typed_finance_case_id
     AND frozen_recovery_item.finance_component_id = recovery_sibling.typed_finance_component_id
     AND UPPER(BTRIM(COALESCE(
       frozen_recovery_item.frozen_component_key_type, ''
     ))) = UPPER(BTRIM(COALESCE(
       recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
     )))
     AND BTRIM(COALESCE(frozen_recovery_item.frozen_component_key_value, ''))
          = BTRIM(COALESCE(
              recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
            ))
     AND COALESCE(
       frozen_recovery_item.timesheet_id,
       CASE
         WHEN NULLIF(BTRIM(COALESCE(
           frozen_recovery_item.frozen_source_basis_json->>'timesheet_id', ''
         )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN BTRIM(
             frozen_recovery_item.frozen_source_basis_json->>'timesheet_id'
           )::uuid
         ELSE NULL::uuid
       END,
       CASE
         WHEN NULLIF(BTRIM(COALESCE(
           frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id', ''
         )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN BTRIM(
             frozen_recovery_item.frozen_source_basis_json->>'linked_timesheet_id'
           )::uuid
         ELSE NULL::uuid
       END,
       CASE
         WHEN NULLIF(BTRIM(COALESCE(
           frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}',
           ''
         )), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           THEN BTRIM(
             frozen_recovery_item.frozen_component_snapshot_json#>>'{source_basis_json,linked_timesheet_id}'
           )::uuid
         ELSE NULL::uuid
       END
     ) = recovery_sibling.timesheet_id
    WHERE recovery_sibling.status = 'SUPERSEDED'
      AND recovery_sibling.authority_line_type = 'OVERPAYMENT_RECOVERY'
      AND recovery_sibling.timesheet_id IS NOT NULL
      AND COALESCE(LOWER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_unavailable', ''
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
      AND COALESCE(LOWER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_overlay_applied', ''
      ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
      AND LOWER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_overlay_active', 'true'
      ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
      AND UPPER(BTRIM(COALESCE(
        recovery_sibling.row_json->>'post_draft_overlay_operation_type', ''
      ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
      AND recovery_sibling.typed_finance_case_id IS NOT NULL
      AND recovery_sibling.typed_finance_component_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(
        recovery_sibling.key_type, recovery_sibling.row_json->>'key_type', ''
      )), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(
        recovery_sibling.key_value, recovery_sibling.row_json->>'key_value', ''
      )), '') IS NOT NULL
  ), preview_authority AS MATERIALIZED (
    SELECT
      preview_row.*,
      COALESCE(reservation_total.current_active_reserved_ex_vat, 0)::numeric
        AS current_active_reserved_ex_vat,
      COALESCE(reservation_total.current_active_reservation_count, 0)::integer
        AS current_active_reservation_count,
      (active_overlap.id IS NOT NULL) AS exact_active_item_overlap,
      CASE
        WHEN preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY' THEN
          private.pay_workbench_preview_recovery_residual_is_current_v1(
            preview_row.status,
            preview_row.candidate_id,
            finance_case.candidate_id,
            finance_case.id,
            preview_row.row_json,
            COALESCE(reservation_total.current_active_reserved_ex_vat, 0),
            COALESCE(reservation_total.current_active_reservation_count, 0),
            active_overlap.id IS NOT NULL
          )
        ELSE true
      END AS recovery_residual_is_current
    FROM session_preview_rows AS preview_row
    LEFT JOIN public.pay_advances AS finance_case
      ON finance_case.id = preview_row.typed_finance_case_id
     AND finance_case.candidate_id = preview_row.candidate_id
    LEFT JOIN recovery_reservation_totals AS reservation_total
      ON reservation_total.candidate_id = preview_row.candidate_id
     AND reservation_total.finance_case_id = preview_row.typed_finance_case_id
    LEFT JOIN active_item_overlap_rows AS active_overlap
      ON active_overlap.id = preview_row.id
  ), eligible_rows AS MATERIALIZED (
    SELECT preview_row.id, preview_row.selected
    FROM preview_authority AS preview_row
    WHERE LOWER(private.pay_workbench_preview_effective_section_v1(
      preview_row.section, preview_row.row_json
    )) = p_section
      AND preview_row.status = 'READY'
      AND NOT (
        COALESCE(LOWER(BTRIM(COALESCE(
          preview_row.row_json->>'post_draft_unavailable', ''
        ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
        OR (
          COALESCE(LOWER(BTRIM(COALESCE(
            preview_row.row_json->>'post_draft_overlay_applied', ''
          ))) IN ('true', 't', '1', 'yes', 'y', 'on'), false)
          AND UPPER(BTRIM(COALESCE(
            preview_row.row_json->>'post_draft_overlay_operation_type', ''
          ))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          AND LOWER(BTRIM(COALESCE(
            preview_row.row_json->>'post_draft_overlay_active', 'true'
          ))) NOT IN ('false', 'f', '0', 'no', 'n', 'off')
        )
      )
      AND preview_row.recovery_residual_is_current
      AND NOT (
        preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY'
        AND private.pay_workbench_preview_effective_section_v1(
          preview_row.section, preview_row.row_json
        ) <> 'cases_resolutions'
        AND preview_row.typed_finance_case_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM session_preview_rows AS actionable_sibling
          WHERE actionable_sibling.candidate_id = preview_row.candidate_id
            AND actionable_sibling.id <> preview_row.id
            AND actionable_sibling.status = 'READY'
            AND actionable_sibling.authority_line_type = 'OVERPAYMENT_RECOVERY'
            AND actionable_sibling.typed_finance_case_id = preview_row.typed_finance_case_id
            AND private.pay_workbench_preview_effective_section_v1(
              actionable_sibling.section, actionable_sibling.row_json
            ) = 'cases_resolutions'
        )
      )
      AND NOT (
        preview_row.authority_line_type = 'TIMESHEET_PAYMENT'
        AND UPPER(BTRIM(COALESCE(
          preview_row.row_json->>'presentation_reason', ''
        ))) = 'NEGATIVE_ORDINARY_PRESENTATION_ONLY'
        AND preview_row.timesheet_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM strict_recovery_siblings AS recovery_sibling
          WHERE recovery_sibling.candidate_id = preview_row.candidate_id
            AND recovery_sibling.id <> preview_row.id
            AND recovery_sibling.timesheet_id = preview_row.timesheet_id
        )
      )
      AND (
        preview_row.exact_active_item_overlap IS NOT TRUE
        OR (
          preview_row.authority_line_type = 'OVERPAYMENT_RECOVERY'
          AND preview_row.recovery_residual_is_current
          AND CASE
            WHEN jsonb_typeof(
              preview_row.row_json->'recovery_active_reserved_ex_vat'
            ) = 'number'
              THEN (preview_row.row_json->>'recovery_active_reserved_ex_vat')::numeric > 0
            ELSE false
          END
        )
      )
  )
  SELECT source_row.*
  FROM public.banking_pay_workbench_preview_rows AS source_row
  JOIN eligible_rows AS eligible_row ON eligible_row.id = source_row.id;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_eligible_rows_v2(uuid, bigint, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_eligible_rows_v2(uuid, bigint, text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_row_payload_v2(
  p_row public.banking_pay_workbench_preview_rows
) RETURNS jsonb
LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  -- Evaluate effective section and normalized fields once. Without these two
  -- fences PostgreSQL expands the immutable payload expression repeatedly.
  -- All field expressions remain byte-identical to the certified old reader.
  WITH limited_rows AS MATERIALIZED (
    SELECT p_row.id, p_row.section AS physical_section,
      private.pay_workbench_preview_effective_section_v1(p_row.section, p_row.row_json) AS section,
      p_row.row_json, p_row.row_key, p_row.row_ordinal, p_row.candidate_id,
      p_row.timesheet_id, p_row.key_type, p_row.key_value, p_row.selected,
      p_row.selection_state, p_row.status, p_row.session_version
  ), normalised_rows AS MATERIALIZED (
    SELECT
      limited_rows.*,
      base_values.base_json,
      display_values.amount_display_text,
      display_values.section_amount_display_text,
      post_draft_values.post_draft_unavailable_bool,
      bool_values.selection_allowed_bool,
      bool_values.draftable_bool,
      bool_values.ready_for_draft_bool,
      bool_values.excluded_bool,
      bool_values.deduction_bool,
      bool_values.materialisable_bool,
      name_values.candidate_display_name_text,
      name_values.candidate_name_text
    FROM limited_rows
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN jsonb_typeof(limited_rows.row_json) = 'object' THEN limited_rows.row_json
          ELSE '{}'::jsonb
        END AS stored_row_json
    ) AS stored_values
    CROSS JOIN LATERAL (
      SELECT
        stored_values.stored_row_json
        || jsonb_build_object(
          'preview_row_id', limited_rows.id::text,
          'row_id', limited_rows.id::text,
          'section', limited_rows.section,
          'effective_section', limited_rows.section,
          'physical_section', limited_rows.physical_section,
          'presentation_section', CASE limited_rows.section
            WHEN 'canonical_preview_lines' THEN 'READY_TO_PAY'
            WHEN 'cases_resolutions' THEN 'CASES_RESOLUTIONS'
            WHEN 'blocked_for_pay' THEN 'BLOCKED_FOR_PAY'
            ELSE UPPER(limited_rows.section)
          END,
          'readiness_state', CASE limited_rows.section
            WHEN 'canonical_preview_lines' THEN 'READY_TO_PAY'
            WHEN 'cases_resolutions' THEN 'CASES_RESOLUTIONS'
            WHEN 'blocked_for_pay' THEN 'BLOCKED_FOR_PAY'
            ELSE UPPER(limited_rows.section)
          END,
          'row_key', limited_rows.row_key,
          'row_ordinal', limited_rows.row_ordinal,
          'candidate_id', limited_rows.candidate_id::text,
          'timesheet_id', CASE WHEN limited_rows.timesheet_id IS NULL THEN NULL ELSE limited_rows.timesheet_id::text END,
          'key_type', limited_rows.key_type,
          'key_value', limited_rows.key_value,
          'selected', CASE
            WHEN limited_rows.section = 'canonical_preview_lines'
              THEN COALESCE(limited_rows.selected, false)
            ELSE false
          END,
          'selection_state', CASE
            WHEN limited_rows.section = 'canonical_preview_lines'
              THEN limited_rows.selection_state
            ELSE 'NOT_SELECTABLE'
          END,
          'status', limited_rows.status,
          'session_version', limited_rows.session_version,
          'amount_ex_vat', COALESCE(
            stored_values.stored_row_json->'amount_ex_vat',
            stored_values.stored_row_json->'preview_amount_ex_vat',
            stored_values.stored_row_json->'section_amount_ex_vat',
            'null'::jsonb
          )
        ) AS base_json
      -- Single-row evaluation barrier; not list pagination.
      OFFSET 0
    ) AS base_values
    CROSS JOIN LATERAL (
      SELECT
        COALESCE(
          NULLIF(base_values.base_json->>'amount_display', ''),
          NULLIF(base_values.base_json->>'section_amount_display', ''),
          NULLIF(base_values.base_json->>'amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'preview_amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'section_amount_ex_vat', '')
        ) AS amount_display_raw,
        COALESCE(
          NULLIF(base_values.base_json->>'section_amount_display', ''),
          NULLIF(base_values.base_json->>'amount_display', ''),
          NULLIF(base_values.base_json->>'section_amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'amount_ex_vat', ''),
          NULLIF(base_values.base_json->>'preview_amount_ex_vat', '')
        ) AS section_amount_display_raw
    ) AS display_raw_values
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN display_raw_values.amount_display_raw ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN to_char(display_raw_values.amount_display_raw::numeric, 'FM999999999999999990.00')
          ELSE display_raw_values.amount_display_raw
        END AS amount_display_text,
        CASE
          WHEN display_raw_values.section_amount_display_raw ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN to_char(display_raw_values.section_amount_display_raw::numeric, 'FM999999999999999990.00')
          ELSE display_raw_values.section_amount_display_raw
        END AS section_amount_display_text
    ) AS display_values
    CROSS JOIN LATERAL (
      SELECT
        COALESCE(
          lower(BTRIM(COALESCE(base_values.base_json->>'post_draft_unavailable', '')))
            IN ('true', 't', '1', 'yes', 'y', 'on'),
          false
        )
        OR (
          COALESCE(
            lower(BTRIM(COALESCE(base_values.base_json->>'post_draft_overlay_applied', '')))
              IN ('true', 't', '1', 'yes', 'y', 'on'),
            false
          )
          AND UPPER(BTRIM(COALESCE(base_values.base_json->>'post_draft_overlay_operation_type', '')))
            IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          AND lower(BTRIM(COALESCE(base_values.base_json->>'post_draft_overlay_active', 'true')))
            NOT IN ('false', 'f', '0', 'no', 'n', 'off')
        ) AS post_draft_unavailable_bool
    ) AS post_draft_values
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN limited_rows.section <> 'canonical_preview_lines' THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'selection_allowed'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'selection_allowed'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          WHEN limited_rows.section = 'canonical_preview_lines'
               AND COALESCE(limited_rows.selected, false)
               AND UPPER(BTRIM(COALESCE(limited_rows.selection_state, ''))) = 'SELECTED'
               THEN true
          ELSE false
        END AS selection_allowed_bool,
        CASE
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_excluded_from_allocation'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_excluded_from_allocation'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          ELSE false
        END AS excluded_bool,
        CASE
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_recognised_finance_deduction'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_recognised_finance_deduction'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          ELSE false
        END AS deduction_bool,
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'materialisable'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'materialisable'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          ELSE limited_rows.status = 'READY'
        END AS materialisable_bool,
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN limited_rows.section <> 'canonical_preview_lines' THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'draftable'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'draftable'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          WHEN limited_rows.section = 'canonical_preview_lines'
               AND COALESCE(limited_rows.selected, false)
               AND UPPER(BTRIM(COALESCE(limited_rows.selection_state, ''))) = 'SELECTED'
               AND limited_rows.status = 'READY'
               THEN true
          ELSE false
        END AS draftable_bool,
        CASE
          WHEN post_draft_values.post_draft_unavailable_bool IS TRUE THEN false
          WHEN limited_rows.section <> 'canonical_preview_lines' THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_ready_for_draft'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'is_ready_for_draft'), '')) IN ('false', 'f', '0', 'no', 'n') THEN false
          WHEN lower(NULLIF(BTRIM(base_values.base_json->>'draftable'), '')) IN ('true', 't', '1', 'yes', 'y') THEN true
          WHEN limited_rows.section = 'canonical_preview_lines'
               AND COALESCE(limited_rows.selected, false)
               AND UPPER(BTRIM(COALESCE(limited_rows.selection_state, ''))) = 'SELECTED'
               AND limited_rows.status = 'READY'
               THEN true
          ELSE false
        END AS ready_for_draft_bool
    ) AS bool_values
    CROSS JOIN LATERAL (
      SELECT
        COALESCE(
          NULLIF(BTRIM(base_values.base_json->>'candidate_display_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'candidate_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_display_name'), '')
        ) AS candidate_display_name_text,
        COALESCE(
          NULLIF(BTRIM(base_values.base_json->>'candidate_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'candidate_display_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_name'), ''),
          NULLIF(BTRIM(base_values.base_json->>'worker_display_name'), '')
        ) AS candidate_name_text
    ) AS name_values
  )
  SELECT
      normalised_rows.base_json
      || jsonb_build_object(
        'amount_display', normalised_rows.amount_display_text,
        'section_amount_display', normalised_rows.section_amount_display_text
      )
      || jsonb_strip_nulls(jsonb_build_object(
        'candidate_display_name', normalised_rows.candidate_display_name_text,
        'candidate_name', normalised_rows.candidate_name_text
      ))
      || jsonb_build_object(
        'selection_allowed', normalised_rows.selection_allowed_bool,
        'draftable', normalised_rows.draftable_bool,
        'is_ready_for_draft', normalised_rows.ready_for_draft_bool,
        'is_excluded_from_allocation', normalised_rows.excluded_bool,
        'is_recognised_finance_deduction', normalised_rows.deduction_bool
      )
      || CASE
        WHEN normalised_rows.post_draft_unavailable_bool IS TRUE THEN jsonb_build_object(
          'post_draft_unavailable', true,
          'presentation_section', 'DRAFTED',
          'readiness_state', 'NOT_SELECTABLE',
          'preview_contract',
            CASE
              WHEN jsonb_typeof(normalised_rows.base_json->'preview_contract') = 'object'
                THEN normalised_rows.base_json->'preview_contract'
              ELSE '{}'::jsonb
            END
            || jsonb_build_object(
              'ok', false,
              'status', 'NOT_SELECTABLE',
              'materialisable', false,
              'selection_allowed', false,
              'draftable', false,
              'is_ready_for_draft', false
            )
        )
        WHEN jsonb_typeof(normalised_rows.base_json->'preview_contract') = 'object' THEN '{}'::jsonb
        ELSE jsonb_build_object(
          'preview_contract',
          jsonb_build_object(
            'ok', true,
            'status', 'OK',
            'reasons', '[]'::jsonb,
            'reason_count', 0,
            'key_type', COALESCE(NULLIF(normalised_rows.base_json->>'key_type', ''), normalised_rows.key_type),
            'key_value', COALESCE(NULLIF(normalised_rows.base_json->>'key_value', ''), normalised_rows.key_value),
            'line_key', COALESCE(NULLIF(normalised_rows.base_json->>'line_key', ''), NULLIF(normalised_rows.base_json->>'row_key', ''), normalised_rows.row_key),
            'case_type', COALESCE(NULLIF(normalised_rows.base_json->>'case_type', ''), 'TIMESHEET_PAYMENT'),
            'line_type', NULLIF(normalised_rows.base_json->>'line_type', ''),
            'source_kind', NULLIF(normalised_rows.base_json->>'source_kind', ''),
            'amount_ex_vat', COALESCE(
              normalised_rows.base_json->'amount_ex_vat',
              normalised_rows.base_json->'preview_amount_ex_vat',
              normalised_rows.base_json->'section_amount_ex_vat',
              'null'::jsonb
            ),
            'materialisable', normalised_rows.materialisable_bool,
            'target_section', COALESCE(NULLIF(normalised_rows.base_json->>'target_section', ''), NULLIF(normalised_rows.base_json->>'section', ''), normalised_rows.section),
            'presentation_section', NULLIF(normalised_rows.base_json->>'presentation_section', ''),
            'selection_allowed', normalised_rows.selection_allowed_bool,
            'draftable', normalised_rows.draftable_bool,
            'is_ready_for_draft', normalised_rows.ready_for_draft_bool,
            'is_excluded_from_allocation', normalised_rows.excluded_bool,
            'is_recognised_finance_deduction', normalised_rows.deduction_bool
          )
        )
      END
  FROM normalised_rows;
$function$;
ALTER FUNCTION private.pay_workbench_modal_row_payload_v2(public.banking_pay_workbench_preview_rows) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_row_payload_v2(public.banking_pay_workbench_preview_rows) FROM PUBLIC, anon, authenticated, service_role;

-- Move the existing Workbench route/filter predicate ahead of pagination.
-- Cases prefer the resolution target; ordinary rows prefer the effective route.
-- Keep the legacy alias vocabulary and depth limits, including mixed signals.
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_row_matches_scope_v2(
  p_row jsonb, p_filters jsonb, p_channel text, p_section text
) RETURNS boolean
LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH RECURSIVE walk(value, depth) AS (
    SELECT p_row, 0
    UNION ALL
    SELECT child.value, walk.depth + 1
    FROM walk
    CROSS JOIN LATERAL (
      SELECT field.value FROM jsonb_each(CASE WHEN jsonb_typeof(walk.value) = 'object' THEN walk.value ELSE '{}'::jsonb END) AS field
      UNION ALL
      SELECT item.value FROM jsonb_array_elements(CASE WHEN jsonb_typeof(walk.value) = 'array' THEN walk.value ELSE '[]'::jsonb END) AS item
    ) AS child
    WHERE walk.depth < 7 AND jsonb_typeof(child.value) IN ('object','array')
  ), fields AS (
    SELECT field.key, BTRIM(field.value #>> '{}') AS value, walk.depth,
      BTRIM(REGEXP_REPLACE(UPPER(BTRIM(field.value #>> '{}')), '[^A-Z0-9]+', '_', 'g'), '_') AS route
    FROM walk
    CROSS JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(walk.value) = 'object' THEN walk.value ELSE '{}'::jsonb END) AS field
  ), routes AS (
    SELECT key,
      CASE WHEN route IN ('PAYE','PAYE_ONLY','PAYE_CHANNEL') THEN 'PAYE'
           WHEN route IN ('UMBRELLA','UMBRELLA_ONLY','UMBRELLA_CHANNEL','NON_PAYE','NONPAYE') THEN 'UMBRELLA'
      END AS route
    FROM fields
  ), signals AS (
    SELECT
      ARRAY_AGG(DISTINCT route) FILTER (WHERE route IS NOT NULL AND key IN (
        'current_target_pay_method','currentTargetPayMethod','target_pay_method','targetPayMethod',
        'saved_target_pay_method','savedTargetPayMethod','resolved_target_pay_method','resolvedTargetPayMethod',
        'proposed_target_pay_method','proposedTargetPayMethod','to_pay_method','toPayMethod',
        'resolution_target_pay_method','resolutionTargetPayMethod','frozen_target_pay_method','frozenTargetPayMethod',
        'target_pay_channel','targetPayChannel','resolved_pay_method','resolvedPayMethod','proposed_pay_method','proposedPayMethod'
      )) AS target,
      ARRAY_AGG(DISTINCT route) FILTER (WHERE route IS NOT NULL AND key IN (
        'pay_channel','payChannel','candidate_pay_method','candidatePayMethod',
        'current_pay_method','currentPayMethod','pay_method','payMethod'
      )) AS effective,
      ARRAY_AGG(DISTINCT route) FILTER (WHERE route IS NOT NULL AND key IN (
        'source_pay_method','sourcePayMethod','from_pay_method','fromPayMethod','previous_pay_method','previousPayMethod',
        'timesheet_pay_method','timesheetPayMethod','ts_pay_method','tsPayMethod','frozen_source_pay_method','frozenSourcePayMethod',
        'source_pay_channel','sourcePayChannel'
      )) AS source
    FROM routes
  ), identities AS (
    SELECT
      ARRAY_AGG(DISTINCT value) FILTER (WHERE key IN ('candidate_id','candidateId') AND value <> '' AND depth <= 6) AS candidates,
      ARRAY_AGG(DISTINCT value) FILTER (WHERE key IN ('client_id','clientId') AND value <> '' AND depth <= 6) AS clients
    FROM fields
  ), filters AS (
    SELECT COALESCE(NULLIF(BTRIM(p_filters->>'candidate_id'), ''), NULLIF(BTRIM(p_filters->>'candidateId'), '')) AS candidate_id,
      COALESCE(NULLIF(BTRIM(p_filters->>'client_id'), ''), NULLIF(BTRIM(p_filters->>'clientId'), '')) AS client_id
  )
  SELECT
    (filters.candidate_id IS NULL OR identities.candidates IS NULL OR filters.candidate_id = ANY(identities.candidates))
    AND (filters.client_id IS NULL OR identities.clients IS NULL OR filters.client_id = ANY(identities.clients))
    AND (p_channel = 'ALL' OR p_channel = ANY(COALESCE(
      CASE WHEN p_section = 'cases_resolutions' THEN signals.target ELSE signals.effective END,
      CASE WHEN p_section = 'cases_resolutions' THEN signals.effective ELSE signals.target END,
      signals.source, ARRAY[]::text[]
    )))
  FROM signals CROSS JOIN identities CROSS JOIN filters;
$function$;
ALTER FUNCTION private.pay_workbench_modal_row_matches_scope_v2(jsonb, jsonb, text, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_row_matches_scope_v2(jsonb, jsonb, text, text) FROM PUBLIC, anon, authenticated, service_role;
