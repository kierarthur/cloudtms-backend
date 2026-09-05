import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '02092026_1040_banking_pay_draft_insert_items_finance_handoff_v1.sql'
);
const outputPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1340_banking_pay_draft_carry_forward_policy_transport_v8.sql'
);
const checkOnly = process.argv.includes('--check');

const source = fs.readFileSync(sourcePath, 'utf8');
const marker = 'CREATE OR REPLACE FUNCTION public.pay_batch_insert_items_from_preview';
const start = source.indexOf(marker);
if (start < 0 || source.indexOf(marker, start + marker.length) >= 0) {
  throw new Error('expected exactly one INSERT_ITEMS owner');
}

let replacement = source.slice(start).trimEnd() + '\n';
if ((replacement.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length !== 1) {
  throw new Error('replacement must contain exactly one function');
}

function replaceExact(label, beforeText, afterText) {
  const count = replacement.split(beforeText).length - 1;
  if (count !== 1) throw new Error(`${label} boundary changed: ${count}`);
  replacement = replacement.replace(beforeText, () => afterText);
}

const before = `      CASE
        WHEN UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.allocation_type, '')) = 'MANUAL_DEBT_RECOVERY'
          THEN UPPER(NULLIF(BTRIM(COALESCE(prepared_rows.line_json->>'paye_treatment', '')), ''))
        ELSE NULL::text
      END AS paye_treatment,`;
const after = `      CASE
        WHEN UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.allocation_type, '')) IN (
          'MANUAL_DEBT_RECOVERY',
          'MANUAL_ADJUSTMENT_CARRY_FORWARD'
        )
          THEN UPPER(NULLIF(BTRIM(COALESCE(prepared_rows.line_json->>'paye_treatment', '')), ''))
        ELSE NULL::text
      END AS paye_treatment,`;
const occurrenceCount = replacement.split(before).length - 1;
if (occurrenceCount !== 1) {
  throw new Error(`carry-forward PAYE policy transport boundary changed: ${occurrenceCount}`);
}
replacement = replacement.replace(before, after);

const declarationBefore = `  v_locked_allocation_row_count integer := 0;
  -- All six visible finance aliases must first match the producer-owned case,`;
const declarationAfter = `  v_locked_allocation_row_count integer := 0;
  v_carry_forward_reservation record;
  v_carry_forward_reservation_result jsonb := '{}'::jsonb;
  v_carry_forward_reservation_count integer := 0;
  -- All six visible finance aliases must first match the producer-owned case,`;
if (replacement.split(declarationBefore).length - 1 !== 1) {
  throw new Error('INSERT_ITEMS declaration boundary changed');
}
replacement = replacement.replace(declarationBefore, declarationAfter);

const expectedEffectsBefore = `      jsonb_build_object('relation_name','pay_batch_items','operation','INSERT'),
      jsonb_build_object('relation_name','pay_batch_items','operation','UPDATE')`;
const expectedEffectsAfter = `      jsonb_build_object('relation_name','pay_batch_items','operation','INSERT'),
      jsonb_build_object('relation_name','pay_batch_items','operation','UPDATE'),
      jsonb_build_object('relation_name','pay_manual_adjustment_carry_forwards','operation','UPDATE')`;
if (replacement.split(expectedEffectsBefore).length - 1 !== 1) {
  throw new Error('INSERT_ITEMS expected-effects boundary changed');
}
replacement = replacement.replace(expectedEffectsBefore, expectedEffectsAfter);

const financeIdentityBlock = `  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A certified finance constituent did not match its producer-owned case, component, source, amount or readiness identity.'
            )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row`;
const certifiedCarryForwardBlock = `  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A certified finance constituent did not match its producer-owned case, component, source, amount or readiness identity.'
            )::text;
  END IF;

  -- A cancellation carry-forward is not a finance-case alias. Certify its
  -- separate durable source row before allowing either sign through the
  -- ordinary item materialiser. The existing reservation owner remains the
  -- authority for the state transition after the exact item is linked.
  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_insert_items_certified_carry_forward;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_insert_items_certified_carry_forward ON COMMIT DROP AS
  WITH carry_candidate AS (
    SELECT
      allocation_row.*,
      allocation_row.allocation_basis_json->'line' AS line_json,
      CASE
        WHEN COALESCE(allocation_row.allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}', '')
               ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (allocation_row.allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}')::uuid
        ELSE NULL::uuid
      END AS carry_forward_id,
      CASE
        WHEN COALESCE(
          allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}',
          allocation_row.allocation_basis_json#>>'{line,timesheet_id}',
          allocation_row.allocation_basis_json#>>'{line,real_business_timesheet_id}',
          ''
        ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN COALESCE(
            allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}',
            allocation_row.allocation_basis_json#>>'{line,timesheet_id}',
            allocation_row.allocation_basis_json#>>'{line,real_business_timesheet_id}'
          )::uuid
        ELSE NULL::uuid
      END AS certified_timesheet_id
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_scope_id IN (
        SELECT supplied_scope.candidate_scope_id_text::uuid
        FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
      )
      AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
      AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) NOT IN ('FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'SKIPPED', 'VOIDED')
  )
  SELECT
    carry_candidate.id AS allocation_row_id,
    carry_candidate.pay_batch_item_id,
    carry_candidate.candidate_id,
    UPPER(BTRIM(COALESCE(carry_candidate.pay_channel, ''))) AS pay_channel,
    carry_candidate.carry_forward_id,
    carry_candidate.certified_timesheet_id,
    carry_candidate.source_ref,
    carry_candidate.operation_source_key,
    ROUND(COALESCE(carry_candidate.allocated_amount, 0), 2)::numeric(12,2) AS allocated_amount,
    carry_source.amount_vat AS expected_amount_vat,
    carry_source.amount_inc_vat AS expected_amount_inc_vat,
    UPPER(NULLIF(BTRIM(COALESCE(carry_source.paye_treatment, '')), '')) AS expected_paye_treatment,
    (
      carry_source.id IS NOT NULL
      AND carry_candidate.finance_case_id IS NULL
      AND carry_candidate.finance_component_id IS NULL
      AND NULLIF(BTRIM(COALESCE(carry_candidate.operation_source_key, '')), '') IS NOT NULL
      AND carry_candidate.source_ref = 'carry_forward:' || carry_candidate.carry_forward_id::text
      AND jsonb_typeof(carry_candidate.line_json) = 'object'
      AND UPPER(BTRIM(COALESCE(carry_candidate.line_json->>'line_type', ''))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
      AND UPPER(BTRIM(COALESCE(carry_candidate.line_json->>'case_type', ''))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
      AND NULLIF(BTRIM(COALESCE(carry_candidate.line_json->>'source_ref', '')), '') = carry_candidate.source_ref
      AND UPPER(BTRIM(COALESCE(carry_candidate.allocation_basis_json#>>'{economic_key,key_type}', ''))) = 'MANUAL_CARRY_FORWARD'
      AND NULLIF(BTRIM(COALESCE(carry_candidate.allocation_basis_json#>>'{economic_key,key_value}', '')), '') = carry_candidate.carry_forward_id::text
      AND LOWER(NULLIF(BTRIM(COALESCE(carry_candidate.line_json->>'candidate_id', '')), '')) = carry_candidate.candidate_id::text
      AND UPPER(NULLIF(BTRIM(COALESCE(carry_candidate.line_json->>'pay_channel', '')), '')) = UPPER(BTRIM(COALESCE(carry_candidate.pay_channel, '')))
      AND LOWER(BTRIM(COALESCE(carry_candidate.line_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(carry_candidate.line_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(carry_candidate.line_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(carry_candidate.line_json#>>'{preview_contract,is_recognised_manual_carry_forward}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND COALESCE(carry_candidate.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
      AND ROUND((carry_candidate.line_json->>'amount_ex_vat')::numeric, 2) = ROUND(COALESCE(carry_candidate.allocated_amount, 0), 2)
      AND (
        (ROUND(COALESCE(carry_candidate.allocated_amount, 0), 2) > 0 AND UPPER(BTRIM(COALESCE(carry_candidate.line_json->>'item_direction', ''))) = 'CREDIT')
        OR (ROUND(COALESCE(carry_candidate.allocated_amount, 0), 2) < 0 AND UPPER(BTRIM(COALESCE(carry_candidate.line_json->>'item_direction', ''))) = 'DEBIT')
      )
      AND carry_source.candidate_id = carry_candidate.candidate_id
      AND UPPER(BTRIM(COALESCE(carry_source.pay_channel, ''))) = UPPER(BTRIM(COALESCE(carry_candidate.pay_channel, '')))
      AND carry_source.timesheet_id IS NOT DISTINCT FROM carry_candidate.certified_timesheet_id
      AND ROUND(COALESCE(carry_source.amount_ex_vat, 0), 2) = ROUND(COALESCE(carry_candidate.allocated_amount, 0), 2)
      AND UPPER(BTRIM(COALESCE(carry_source.adjustment_direction, ''))) = UPPER(BTRIM(COALESCE(carry_candidate.line_json->>'item_direction', '')))
      AND UPPER(NULLIF(BTRIM(COALESCE(carry_source.paye_treatment, '')), '')) IS NOT DISTINCT FROM UPPER(NULLIF(BTRIM(COALESCE(carry_candidate.line_json->>'paye_treatment', '')), ''))
      AND UPPER(BTRIM(COALESCE(carry_source.status, ''))) IN ('PENDING_CARRY_FORWARD', 'RESERVED_IN_DRAFT')
      AND (
        UPPER(BTRIM(COALESCE(carry_source.status, ''))) = 'PENDING_CARRY_FORWARD'
        OR (
          carry_source.target_pay_batch_id = p_pay_batch_id
          AND carry_source.target_operation_source_key = carry_candidate.operation_source_key
        )
      )
    ) AS certification_ok
  FROM carry_candidate
  LEFT JOIN public.pay_manual_adjustment_carry_forwards AS carry_source
    ON carry_source.id = carry_candidate.carry_forward_id;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_insert_items_certified_carry_forward AS carry_row
  WHERE carry_row.certification_ok IS NOT TRUE
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_CARRY_FORWARD_HANDOFF_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'DRAFT_CARRY_FORWARD_HANDOFF_INVALID',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'message', 'A manual carry-forward allocation did not match its durable source identity, sign, amount, channel or reservation state.'
      )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_insert_items_certified_carry_forward AS carry_row
  WHERE (
      carry_row.pay_batch_item_id IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS collision_candidate
        JOIN public.pay_batch_items AS collision_item
          ON collision_item.pay_batch_candidate_id = collision_candidate.id
         AND collision_item.operation_source_key = carry_row.operation_source_key
         AND COALESCE(collision_item.is_voided, false) = false
        WHERE collision_candidate.pay_batch_id = p_pay_batch_id
          AND collision_candidate.candidate_id = carry_row.candidate_id
      )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_candidates AS exact_candidate
      JOIN public.pay_batch_items AS exact_item
        ON exact_item.pay_batch_candidate_id = exact_candidate.id
       AND COALESCE(exact_item.is_voided, false) = false
      WHERE exact_candidate.pay_batch_id = p_pay_batch_id
        AND exact_candidate.candidate_id = carry_row.candidate_id
        AND (exact_item.id = carry_row.pay_batch_item_id OR exact_item.operation_source_key = carry_row.operation_source_key)
        AND exact_item.operation_source_key = carry_row.operation_source_key
        AND exact_item.item_type = 'MANUAL_CREDIT_PAYOUT'
        AND exact_item.finance_case_id IS NULL
        AND exact_item.finance_component_id IS NULL
        AND exact_item.source_ref IS NOT DISTINCT FROM carry_row.source_ref
        AND exact_item.timesheet_id IS NOT DISTINCT FROM carry_row.certified_timesheet_id
        AND UPPER(BTRIM(COALESCE(exact_item.pay_channel::text, ''))) = carry_row.pay_channel
        AND ROUND(COALESCE(exact_item.amount_ex_vat, 0), 2) = carry_row.allocated_amount
        AND ROUND(COALESCE(exact_item.amount_vat, 0), 2) = ROUND(COALESCE(carry_row.expected_amount_vat, 0), 2)
        AND ROUND(COALESCE(exact_item.amount_inc_vat, 0), 2) = ROUND(COALESCE(carry_row.expected_amount_inc_vat, 0), 2)
        AND UPPER(NULLIF(BTRIM(COALESCE(exact_item.paye_treatment, '')), '')) IS NOT DISTINCT FROM carry_row.expected_paye_treatment
    )
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_CARRY_FORWARD_PREEXISTING_ITEM_LINK_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'DRAFT_CARRY_FORWARD_PREEXISTING_ITEM_LINK_MISMATCH',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'message', 'A carry-forward allocation is linked to an item that does not match the durable carry-forward authority.'
      )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row`;
replaceExact('carry-forward durable identity certification', financeIdentityBlock, certifiedCarryForwardBlock);

const existingLinkGeneric = `        OR (
          NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases))
          AND existing_item.item_type <> 'OVERPAYMENT_RECOVERY'
        )`;
const existingLinkWithCarry = `        OR (
          UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
          AND EXISTS (
            SELECT 1
            FROM pg_temp.tmp_pay_batch_insert_items_certified_carry_forward AS carry_row
            WHERE carry_row.allocation_row_id = allocation_row.id
              AND carry_row.certification_ok
              AND existing_item.item_type = 'MANUAL_CREDIT_PAYOUT'
              AND existing_item.finance_case_id IS NULL
              AND existing_item.finance_component_id IS NULL
              AND existing_item.source_ref IS NOT DISTINCT FROM carry_row.source_ref
              AND existing_item.timesheet_id IS NOT DISTINCT FROM carry_row.certified_timesheet_id
              AND UPPER(BTRIM(COALESCE(existing_item.pay_channel::text, ''))) = carry_row.pay_channel
              AND ROUND(COALESCE(existing_item.amount_ex_vat, 0), 2) = carry_row.allocated_amount
              AND ROUND(COALESCE(existing_item.amount_vat, 0), 2) = ROUND(COALESCE(carry_row.expected_amount_vat, 0), 2)
              AND ROUND(COALESCE(existing_item.amount_inc_vat, 0), 2) = ROUND(COALESCE(carry_row.expected_amount_inc_vat, 0), 2)
              AND UPPER(NULLIF(BTRIM(COALESCE(existing_item.paye_treatment, '')), '')) IS NOT DISTINCT FROM carry_row.expected_paye_treatment
          )
        )
        OR (
          NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases))
          AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) <> 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
          AND existing_item.item_type <> 'OVERPAYMENT_RECOVERY'
        )`;
replaceExact('existing link carry-forward identity', existingLinkGeneric, existingLinkWithCarry);

const linkedCountGeneric = `                   OR (
                     NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases))
                     AND existing_item.item_type <> 'OVERPAYMENT_RECOVERY'
                   )`;
const linkedCountWithCarry = `                   OR (
                     UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
                     AND EXISTS (
                       SELECT 1
                       FROM pg_temp.tmp_pay_batch_insert_items_certified_carry_forward AS carry_row
                       WHERE carry_row.allocation_row_id = allocation_row.id
                         AND carry_row.certification_ok
                         AND existing_item.operation_source_key = carry_row.operation_source_key
                         AND existing_item.item_type = 'MANUAL_CREDIT_PAYOUT'
                         AND existing_item.finance_case_id IS NULL
                         AND existing_item.finance_component_id IS NULL
                         AND existing_item.source_ref IS NOT DISTINCT FROM carry_row.source_ref
                         AND existing_item.timesheet_id IS NOT DISTINCT FROM carry_row.certified_timesheet_id
                         AND UPPER(BTRIM(COALESCE(existing_item.pay_channel::text, ''))) = carry_row.pay_channel
                         AND ROUND(COALESCE(existing_item.amount_ex_vat, 0), 2) = carry_row.allocated_amount
                         AND ROUND(COALESCE(existing_item.amount_vat, 0), 2) = ROUND(COALESCE(carry_row.expected_amount_vat, 0), 2)
                         AND ROUND(COALESCE(existing_item.amount_inc_vat, 0), 2) = ROUND(COALESCE(carry_row.expected_amount_inc_vat, 0), 2)
                         AND UPPER(NULLIF(BTRIM(COALESCE(existing_item.paye_treatment, '')), '')) IS NOT DISTINCT FROM carry_row.expected_paye_treatment
                     )
                   )
                   OR (
                     NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases))
                     AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) <> 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
                     AND existing_item.item_type <> 'OVERPAYMENT_RECOVERY'
                   )`;
replaceExact('linked-count carry-forward identity', linkedCountGeneric, linkedCountWithCarry);

const negativeValidationFinance = `         OR EXISTS (
           SELECT 1
           FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
           WHERE finance_row.allocation_row_id = allocation_row.id
             AND finance_row.visible_alias = 'MANUAL_DEBT_RECOVERY'
             AND finance_row.certification_ok
         )`;
const negativeValidationWithCarry = `${negativeValidationFinance}
         OR EXISTS (
           SELECT 1
           FROM pg_temp.tmp_pay_batch_insert_items_certified_carry_forward AS carry_row
           WHERE carry_row.allocation_row_id = allocation_row.id
             AND carry_row.certification_ok
         )`;
replaceExact('negative carry-forward validation', negativeValidationFinance, negativeValidationWithCarry);

const nonPositiveFinanceExclusion = `        AND NOT EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
          WHERE finance_row.allocation_row_id = allocation_row.id
            AND finance_row.visible_alias = 'MANUAL_DEBT_RECOVERY'
            AND finance_row.certification_ok
        )`;
const nonPositiveCarryExclusion = `${nonPositiveFinanceExclusion}
        AND NOT EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_batch_insert_items_certified_carry_forward AS carry_row
          WHERE carry_row.allocation_row_id = allocation_row.id
            AND carry_row.certification_ok
        )`;
replaceExact('non-positive carry-forward validation', nonPositiveFinanceExclusion, nonPositiveCarryExclusion);

const linkageBefore = `  IF COALESCE(v_linked_allocation_row_count, 0) <= 0
     AND COALESCE(v_ordinary_page_allocation_row_count, 0) > 0 THEN`;
const linkageAfter = `  -- The source carry-forward row is a durable reservation authority distinct
  -- from pay_advance_reservations. Reuse its existing strict owner for only the
  -- bounded allocation page just linked above. Any identity/amount/channel
  -- mismatch aborts this whole RPC, including the new item and allocation link.
  FOR v_carry_forward_reservation IN
    SELECT allocation_row.id AS allocation_row_id,
           (allocation_row.allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}')::uuid AS carry_forward_id,
           allocation_row.pay_batch_item_id,
           allocation_row.operation_source_key
    FROM pg_temp.tmp_pay_batch_item_allocation_page AS page_row
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.id = page_row.id
    WHERE UPPER(BTRIM(COALESCE(
            allocation_row.allocation_basis_json#>>'{line,line_type}',
            allocation_row.allocation_type,
            ''
          ))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
      AND COALESCE(allocation_row.allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}', '')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND allocation_row.pay_batch_item_id IS NOT NULL
    ORDER BY page_row.candidate_scope_id, page_row.sort_order, page_row.id
  LOOP
    v_carry_forward_reservation_result := public._pay_manual_adjustment_carry_forward_reserve_for_batch_item(
      v_carry_forward_reservation.carry_forward_id,
      p_pay_batch_id,
      v_carry_forward_reservation.pay_batch_item_id,
      v_carry_forward_reservation.operation_source_key,
      p_actor_user_id
    );
    IF COALESCE((v_carry_forward_reservation_result->>'ok')::boolean, false) IS NOT TRUE
       OR COALESCE((v_carry_forward_reservation_result->>'reserved')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'DRAFT_CARRY_FORWARD_RESERVATION_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_CARRY_FORWARD_RESERVATION_FAILED',
                'operation_id', p_operation_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'allocation_row_id', v_carry_forward_reservation.allocation_row_id::text,
                'carry_forward_id', v_carry_forward_reservation.carry_forward_id::text,
                'reservation_result', v_carry_forward_reservation_result
              )::text;
    END IF;
    v_carry_forward_reservation_count := v_carry_forward_reservation_count + 1;
  END LOOP;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_batch_item_allocation_page AS page_row
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      ON allocation_row.id = page_row.id
    WHERE UPPER(BTRIM(COALESCE(
            allocation_row.allocation_basis_json#>>'{line,line_type}',
            allocation_row.allocation_type,
            ''
          ))) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
      AND (
        COALESCE(allocation_row.allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        OR allocation_row.pay_batch_item_id IS NULL
      )
  ) THEN
    RAISE EXCEPTION 'DRAFT_CARRY_FORWARD_RESERVATION_IDENTITY_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'DRAFT_CARRY_FORWARD_RESERVATION_IDENTITY_INVALID',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text
      )::text;
  END IF;

  IF COALESCE(v_linked_allocation_row_count, 0) <= 0
     AND COALESCE(v_ordinary_page_allocation_row_count, 0) > 0 THEN`;
if (replacement.split(linkageBefore).length - 1 !== 1) {
  throw new Error('INSERT_ITEMS post-link boundary changed');
}
// Use a replacer callback so the regex end anchors in the SQL are preserved
// literally rather than being interpreted as String.replace $' tokens.
replacement = replacement.replace(linkageBefore, () => linkageAfter);

const output = `-- CloudTMS Banking Pay Draft carry-forward policy transport.
-- The correction/cancellation owner freezes the source item's PAYE treatment
-- into MANUAL_ADJUSTMENT_CARRY_FORWARD. INSERT_ITEMS preserves that exact
-- value on the established MANUAL_CREDIT_PAYOUT Draft item so downstream PAYE
-- net entry and execution see the same policy. The existing strict carry-forward
-- owner then reserves that exact source against the created item. No
-- classification, amount or reservation policy is derived here.
-- Generated from the prior exact owner by the checked repository generator.

${replacement}`;

if (checkOnly) {
  if (!fs.existsSync(outputPath)) throw new Error('generated replacement is missing');
  if (fs.readFileSync(outputPath, 'utf8') !== output) {
    throw new Error('generated replacement differs from the checked owner');
  }
  process.stdout.write('Draft carry-forward policy transport: CHECK PASS\n');
} else {
  fs.writeFileSync(outputPath, output, 'utf8');
  process.stdout.write(`${path.relative(root, outputPath)}\n`);
}
