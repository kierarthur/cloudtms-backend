CREATE OR REPLACE FUNCTION public._pay_batch_bank_payment_projection_rows(
  p_pay_batch_id uuid,
  p_scope text DEFAULT 'ALL'::text
)
RETURNS TABLE(
  pay_batch_id uuid,
  requested_scope text,
  payment_group_ordinal bigint,
  stable_order_key text,
  representative_pay_batch_item_id uuid,
  pay_batch_item_count bigint,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  pay_channel text,
  umbrella_id uuid,
  paye_net_required boolean,
  has_effective_paye_input boolean,
  effective_paye_net_input_id uuid,
  effective_paye_net_input_source text,
  effective_paye_net_input_amount numeric,
  final_frozen_bank_amount numeric,
  paye_net_classification text,
  is_paye_net_state_row boolean,
  is_positive_bank_payment boolean,
  transfer_id uuid,
  payment_reference text,
  payee_name text,
  sort_code text,
  account_number text,
  account_type text,
  amount numeric,
  currency text,
  rail_provider text,
  rail_env text,
  request_id text,
  transfer_group_key text,
  grouping_mode_used text,
  week_ending_bucket date,
  payee_entity_kind text,
  payee_entity_id uuid,
  bank_details_hash_snapshot text,
  payout_instruction_snapshot_json jsonb,
  paye_net_state_hash text,
  bank_payment_projection_hash text
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_scope text := UPPER(BTRIM(COALESCE(p_scope, 'ALL')));
  v_batch_kind_fixed text := NULL::text;
  v_rail_provider_snapshot text := NULL::text;
  v_rail_env_snapshot text := NULL::text;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_ID_REQUIRED'
            )::text;
  END IF;

  IF v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA', 'LOANS') THEN
    RAISE EXCEPTION 'PAY_BATCH_BANK_PAYMENT_PROJECTION_SCOPE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_BANK_PAYMENT_PROJECTION_SCOPE_INVALID',
              'pay_batch_id', p_pay_batch_id::text,
              'scope', p_scope,
              'allowed_scopes', jsonb_build_array('ALL', 'PAYE', 'UMBRELLA', 'LOANS')
            )::text;
  END IF;

  SELECT
    UPPER(BTRIM(COALESCE(batch_row.batch_kind_fixed, ''))),
    batch_row.rail_provider_snapshot,
    batch_row.rail_env_snapshot
  INTO
    v_batch_kind_fixed,
    v_rail_provider_snapshot,
    v_rail_env_snapshot
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_BANK_PAYMENT_PROJECTION_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  RETURN QUERY
  WITH source_items AS (
    SELECT
      batch_item.id AS pay_batch_item_id,
      batch_candidate.id AS pay_batch_candidate_id,
      batch_candidate.candidate_id,
      batch_item.pay_channel AS original_pay_channel,
      CASE
        WHEN v_batch_kind_fixed = 'LOANS' THEN 'PAYE'
        ELSE batch_item.pay_channel
      END AS effective_pay_channel,
      batch_item.item_type,
      ROUND(COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0), 2)::numeric(14,2) AS source_item_amount,
      batch_candidate.net_bank_amount AS candidate_net_bank_amount,
      batch_item.payout_instruction_snapshot_json,
      UPPER(NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', '')), '')) AS frozen_payee_entity_kind,
      COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '') AS frozen_payee_entity_id_text_raw,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS frozen_payee_entity_id,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), '') AS frozen_payee_name,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), '') AS frozen_sort_code,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'account_number', '')), '') AS frozen_account_number,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'account_type', '')), '') AS frozen_account_type,
      NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), '') AS frozen_bank_details_hash,
      CASE
        WHEN COALESCE(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN batch_item.payout_instruction_snapshot_json->>'week_ending_bucket'
        ELSE 'NO_WEEK'
      END AS frozen_week_ending_bucket_key,
      CASE
        WHEN COALESCE(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
          THEN (batch_item.payout_instruction_snapshot_json->>'week_ending_bucket')::date
        ELSE NULL::date
      END AS frozen_week_ending_bucket,
      CASE
        WHEN (
          CASE
            WHEN v_batch_kind_fixed = 'LOANS' THEN 'PAYE'
            ELSE batch_item.pay_channel
          END
        ) = 'PAYE'
          THEN batch_candidate.candidate_id::text
               || '|PAYE|'
               || COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                    (
                      SELECT NULLIF(
                               BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                               ''
                             )
                      FROM public.pay_batch_items AS fallback_item
                      WHERE fallback_item.pay_batch_candidate_id = batch_candidate.id
                        AND COALESCE(fallback_item.is_voided, false) = false
                        AND fallback_item.item_type <> 'DEBT_CREATED'
                        AND fallback_item.pay_channel = 'PAYE'
                        AND NULLIF(
                              BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                              ''
                            ) IS NOT NULL
                      ORDER BY
                        CASE
                          WHEN NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'sort_code', '')),
                                 ''
                               ) IS NOT NULL
                           AND NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'account_number', '')),
                                 ''
                               ) IS NOT NULL
                            THEN 0
                          ELSE 1
                        END,
                        fallback_item.id
                      LIMIT 1
                    ),
                    ''
                  )
        WHEN (
          CASE
            WHEN v_batch_kind_fixed = 'LOANS' THEN 'PAYE'
            ELSE batch_item.pay_channel
          END
        ) = 'UMBRELLA'
          THEN batch_candidate.candidate_id::text
               || '|UMBRELLA|'
               || COALESCE(
                    CASE
                      WHEN COALESCE(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
                        THEN batch_item.payout_instruction_snapshot_json->>'week_ending_bucket'
                      ELSE 'NO_WEEK'
                    END,
                    'NO_WEEK'
                  )
               || '|'
               || COALESCE(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')
               || '|'
               || COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                    ''
                  )
        ELSE batch_candidate.candidate_id::text
             || '|OTHER|'
             || COALESCE(
                  NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                  ''
                )
      END AS projected_transfer_group_key
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(batch_item.is_voided, false) = false
      AND batch_item.item_type <> 'DEBT_CREATED'
      AND (
        (v_scope = 'ALL' AND batch_item.pay_channel IN ('PAYE', 'UMBRELLA'))
        OR (v_scope IN ('PAYE', 'UMBRELLA') AND batch_item.pay_channel = v_scope)
        OR (v_scope = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
        OR (v_batch_kind_fixed = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
      )
  ),
  grouped_source_items AS (
    SELECT
      source_item_rows.*,
      ROW_NUMBER() OVER (
        PARTITION BY source_item_rows.effective_pay_channel, source_item_rows.projected_transfer_group_key
        ORDER BY
          CASE
            WHEN source_item_rows.frozen_bank_details_hash IS NOT NULL
             AND source_item_rows.frozen_sort_code IS NOT NULL
             AND source_item_rows.frozen_account_number IS NOT NULL
              THEN 0
            ELSE 1
          END,
          source_item_rows.pay_batch_item_id
      ) AS group_item_ordinal,
      COUNT(*) OVER (
        PARTITION BY source_item_rows.effective_pay_channel, source_item_rows.projected_transfer_group_key
      ) AS grouped_item_count,
      ROUND(
        SUM(source_item_rows.source_item_amount) OVER (
          PARTITION BY source_item_rows.effective_pay_channel, source_item_rows.projected_transfer_group_key
        ),
        2
      )::numeric(14,2) AS grouped_item_amount
    FROM source_items AS source_item_rows
  ),
  representative_groups AS (
    SELECT
      grouped_item_rows.pay_batch_item_id AS representative_pay_batch_item_id,
      grouped_item_rows.grouped_item_count AS pay_batch_item_count,
      grouped_item_rows.pay_batch_candidate_id,
      grouped_item_rows.candidate_id,
      grouped_item_rows.effective_pay_channel AS pay_channel,
      grouped_item_rows.candidate_net_bank_amount,
      grouped_item_rows.grouped_item_amount,
      grouped_item_rows.payout_instruction_snapshot_json,
      grouped_item_rows.frozen_payee_entity_kind AS payee_entity_kind,
      grouped_item_rows.frozen_payee_entity_id AS payee_entity_id,
      grouped_item_rows.frozen_payee_name AS payee_name,
      grouped_item_rows.frozen_sort_code AS raw_sort_code,
      grouped_item_rows.frozen_account_number AS raw_account_number,
      grouped_item_rows.frozen_account_type AS account_type,
      grouped_item_rows.frozen_bank_details_hash AS bank_details_hash_snapshot,
      grouped_item_rows.frozen_week_ending_bucket AS week_ending_bucket,
      grouped_item_rows.projected_transfer_group_key AS transfer_group_key
    FROM grouped_source_items AS grouped_item_rows
    WHERE grouped_item_rows.group_item_ordinal = 1
  ),
  groups_with_effective_input AS (
    SELECT
      representative_group_rows.*,
      effective_input_rows.id AS effective_paye_net_input_id,
      effective_input_rows.source AS effective_paye_net_input_source,
      effective_input_rows.net_amount AS effective_paye_net_input_amount
    FROM representative_groups AS representative_group_rows
    LEFT JOIN LATERAL (
      SELECT
        paye_input_row.id,
        paye_input_row.source,
        ROUND(paye_input_row.net_amount, 2)::numeric(14,2) AS net_amount
      FROM public.pay_batch_paye_net_inputs AS paye_input_row
      WHERE paye_input_row.pay_batch_candidate_id = representative_group_rows.pay_batch_candidate_id
      ORDER BY paye_input_row.imported_at_utc DESC, paye_input_row.id DESC
      LIMIT 1
    ) AS effective_input_rows
      ON true
  ),
  projected_groups AS (
    SELECT
      input_group_rows.*,
      (
        input_group_rows.pay_channel = 'PAYE'
        AND v_batch_kind_fixed <> 'LOANS'
      ) AS paye_net_required,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
          THEN input_group_rows.effective_paye_net_input_id IS NOT NULL
               AND input_group_rows.effective_paye_net_input_amount IS NOT NULL
        ELSE NULL::boolean
      END AS has_effective_paye_input,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE'
          THEN ROUND(input_group_rows.candidate_net_bank_amount, 2)::numeric(14,2)
        ELSE ROUND(input_group_rows.grouped_item_amount, 2)::numeric(14,2)
      END AS final_frozen_bank_amount,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
         AND (
           input_group_rows.effective_paye_net_input_id IS NULL
           OR input_group_rows.effective_paye_net_input_amount IS NULL
           OR input_group_rows.candidate_net_bank_amount IS NULL
         )
          THEN 'MISSING'
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
         AND ROUND(input_group_rows.candidate_net_bank_amount, 2) = 0
          THEN 'ZERO'
        WHEN input_group_rows.pay_channel = 'PAYE'
         AND v_batch_kind_fixed <> 'LOANS'
         AND ROUND(input_group_rows.candidate_net_bank_amount, 2) > 0
          THEN 'POSITIVE'
        ELSE NULL::text
      END AS paye_net_classification,
      CASE
        WHEN REGEXP_REPLACE(COALESCE(input_group_rows.raw_sort_code, ''), '[^0-9]', '', 'g') ~ '^[0-9]{6}$'
          THEN REGEXP_REPLACE(
                 REGEXP_REPLACE(COALESCE(input_group_rows.raw_sort_code, ''), '[^0-9]', '', 'g'),
                 '^([0-9]{2})([0-9]{2})([0-9]{2})$',
                 '\1-\2-\3'
               )
        ELSE NULL::text
      END AS projected_sort_code,
      NULLIF(
        REGEXP_REPLACE(COALESCE(input_group_rows.raw_account_number, ''), '[^0-9]', '', 'g'),
        ''
      ) AS projected_account_number,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE' THEN 'Pay'
        ELSE LEFT(COALESCE(input_group_rows.payee_name, input_group_rows.candidate_id::text), 18)
      END AS projected_payment_reference,
      CASE
        WHEN input_group_rows.pay_channel = 'PAYE' THEN 'CANDIDATE_DESTINATION'
        ELSE 'ROW_BACKED_DESTINATION'
      END AS projected_grouping_mode,
      CASE
        WHEN input_group_rows.pay_channel = 'UMBRELLA'
         AND input_group_rows.payee_entity_kind = 'UMBRELLA'
          THEN input_group_rows.payee_entity_id
        ELSE NULL::uuid
      END AS projected_umbrella_id
    FROM groups_with_effective_input AS input_group_rows
  ),
  classified_groups AS (
    SELECT
      projected_group_rows.*,
      CASE
        WHEN projected_group_rows.paye_net_required
          THEN COALESCE(projected_group_rows.paye_net_classification = 'POSITIVE', false)
        ELSE COALESCE(projected_group_rows.final_frozen_bank_amount, 0) > 0
      END AS is_positive_bank_payment
    FROM projected_groups AS projected_group_rows
  ),
  ordered_groups AS (
    SELECT
      classified_group_rows.*,
      ROW_NUMBER() OVER (
        ORDER BY
          classified_group_rows.pay_channel,
          classified_group_rows.transfer_group_key,
          classified_group_rows.representative_pay_batch_item_id
      ) AS payment_group_ordinal,
      classified_group_rows.pay_channel
        || '|'
        || classified_group_rows.transfer_group_key
        || '|'
        || classified_group_rows.representative_pay_batch_item_id::text AS stable_order_key,
      ROW_NUMBER() OVER (
        PARTITION BY classified_group_rows.pay_batch_candidate_id, classified_group_rows.pay_channel
        ORDER BY
          classified_group_rows.transfer_group_key,
          classified_group_rows.representative_pay_batch_item_id
      ) AS candidate_channel_group_ordinal
    FROM classified_groups AS classified_group_rows
  ),
  paye_state_payload AS (
    SELECT
      COALESCE(
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'pay_batch_candidate_id', ordered_group_rows.pay_batch_candidate_id::text,
            'candidate_id', ordered_group_rows.candidate_id::text,
            'classification', ordered_group_rows.paye_net_classification,
            'has_effective_input', ordered_group_rows.has_effective_paye_input,
            'effective_input_amount', ordered_group_rows.effective_paye_net_input_amount,
            'final_frozen_bank_amount', ordered_group_rows.final_frozen_bank_amount
          )
          ORDER BY
            ordered_group_rows.pay_batch_candidate_id,
            ordered_group_rows.candidate_id
        ) FILTER (
          WHERE ordered_group_rows.paye_net_required
            AND ordered_group_rows.candidate_channel_group_ordinal = 1
        ),
        '[]'::jsonb
      ) AS state_rows_json
    FROM ordered_groups AS ordered_group_rows
  ),
  bank_projection_payload AS (
    SELECT
      COALESCE(
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'payment_group_identity', ordered_group_rows.transfer_group_key,
            'pay_batch_candidate_id', ordered_group_rows.pay_batch_candidate_id::text,
            'candidate_id', ordered_group_rows.candidate_id::text,
            'pay_channel', ordered_group_rows.pay_channel,
            'paye_net_classification', ordered_group_rows.paye_net_classification,
            'final_amount', ordered_group_rows.final_frozen_bank_amount,
            'currency', 'GBP',
            'payment_reference', ordered_group_rows.projected_payment_reference,
            'payee_name', ordered_group_rows.payee_name,
            'sort_code', ordered_group_rows.projected_sort_code,
            'account_number', ordered_group_rows.projected_account_number,
            'account_type', ordered_group_rows.account_type,
            'bank_details_hash', ordered_group_rows.bank_details_hash_snapshot,
            'payee_entity_kind', ordered_group_rows.payee_entity_kind,
            'payee_entity_id', CASE
              WHEN ordered_group_rows.payee_entity_id IS NULL THEN NULL::text
              ELSE ordered_group_rows.payee_entity_id::text
            END,
            'umbrella_id', CASE
              WHEN ordered_group_rows.projected_umbrella_id IS NULL THEN NULL::text
              ELSE ordered_group_rows.projected_umbrella_id::text
            END,
            'grouping_mode', ordered_group_rows.projected_grouping_mode,
            'week_ending_bucket', CASE
              WHEN ordered_group_rows.week_ending_bucket IS NULL THEN NULL::text
              ELSE ordered_group_rows.week_ending_bucket::text
            END,
            'rail_provider', v_rail_provider_snapshot,
            'rail_env', v_rail_env_snapshot
          )
          ORDER BY
            ordered_group_rows.payment_group_ordinal,
            ordered_group_rows.stable_order_key
        ),
        '[]'::jsonb
      ) AS projection_rows_json
    FROM ordered_groups AS ordered_group_rows
  ),
  projection_hashes AS (
    SELECT
      MD5(
        JSONB_BUILD_OBJECT(
          'pay_batch_id', p_pay_batch_id::text,
          'scope', v_scope,
          'rows', paye_state_payload_rows.state_rows_json
        )::text
      ) AS paye_net_state_hash,
      MD5(
        JSONB_BUILD_OBJECT(
          'pay_batch_id', p_pay_batch_id::text,
          'scope', v_scope,
          'rows', bank_projection_payload_rows.projection_rows_json
        )::text
      ) AS bank_payment_projection_hash
    FROM paye_state_payload AS paye_state_payload_rows
    CROSS JOIN bank_projection_payload AS bank_projection_payload_rows
  )
  SELECT
    p_pay_batch_id AS pay_batch_id,
    v_scope AS requested_scope,
    ordered_group_rows.payment_group_ordinal,
    ordered_group_rows.stable_order_key,
    ordered_group_rows.representative_pay_batch_item_id,
    ordered_group_rows.pay_batch_item_count,
    ordered_group_rows.pay_batch_candidate_id,
    ordered_group_rows.candidate_id,
    ordered_group_rows.pay_channel,
    ordered_group_rows.projected_umbrella_id AS umbrella_id,
    ordered_group_rows.paye_net_required,
    ordered_group_rows.has_effective_paye_input,
    CASE
      WHEN ordered_group_rows.paye_net_required
        THEN ordered_group_rows.effective_paye_net_input_id
      ELSE NULL::uuid
    END AS effective_paye_net_input_id,
    CASE
      WHEN ordered_group_rows.paye_net_required
        THEN ordered_group_rows.effective_paye_net_input_source
      ELSE NULL::text
    END AS effective_paye_net_input_source,
    CASE
      WHEN ordered_group_rows.paye_net_required
        THEN ordered_group_rows.effective_paye_net_input_amount
      ELSE NULL::numeric
    END AS effective_paye_net_input_amount,
    ordered_group_rows.final_frozen_bank_amount,
    ordered_group_rows.paye_net_classification,
    (
      ordered_group_rows.paye_net_required
      AND ordered_group_rows.candidate_channel_group_ordinal = 1
    ) AS is_paye_net_state_row,
    ordered_group_rows.is_positive_bank_payment,
    NULL::uuid AS transfer_id,
    ordered_group_rows.projected_payment_reference AS payment_reference,
    ordered_group_rows.payee_name,
    ordered_group_rows.projected_sort_code AS sort_code,
    ordered_group_rows.projected_account_number AS account_number,
    ordered_group_rows.account_type,
    ordered_group_rows.final_frozen_bank_amount AS amount,
    'GBP'::text AS currency,
    v_rail_provider_snapshot AS rail_provider,
    v_rail_env_snapshot AS rail_env,
    NULL::text AS request_id,
    ordered_group_rows.transfer_group_key,
    ordered_group_rows.projected_grouping_mode AS grouping_mode_used,
    ordered_group_rows.week_ending_bucket,
    ordered_group_rows.payee_entity_kind,
    ordered_group_rows.payee_entity_id,
    ordered_group_rows.bank_details_hash_snapshot,
    ordered_group_rows.payout_instruction_snapshot_json,
    projection_hash_rows.paye_net_state_hash,
    projection_hash_rows.bank_payment_projection_hash
  FROM ordered_groups AS ordered_group_rows
  CROSS JOIN projection_hashes AS projection_hash_rows
  ORDER BY ordered_group_rows.payment_group_ordinal;
END;
$function$;



