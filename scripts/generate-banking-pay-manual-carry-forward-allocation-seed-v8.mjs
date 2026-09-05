import fs from 'node:fs';
import path from 'node:path';

const root = path.resolve(import.meta.dirname, '..');
const sourcePath = path.join(root, 'supabase/repeatable/02092026_2312_banking_pay_draft_row_backed_orchestration_v8.sql');
const outputPath = path.join(root, 'supabase/repeatable/04092026_1360_banking_pay_manual_carry_forward_allocation_seed_v8.sql');

const fullSource = fs.readFileSync(sourcePath, 'utf8').replaceAll('\r\n', '\n');
const functionStart = 'CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(';
const functionEnd = 'GRANT EXECUTE ON FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb) TO service_role;';
const functionStartIndex = fullSource.indexOf(functionStart);
const functionEndIndex = fullSource.indexOf(functionEnd, functionStartIndex);
if (functionStartIndex < 0 || functionEndIndex < 0) {
  throw new Error('current row-backed allocation owner could not be extracted');
}
let source = fullSource.slice(functionStartIndex, functionEndIndex + functionEnd.length) + '\n';

function replaceExact(needle, replacement, expectedCount, label) {
  const observed = source.split(needle).length - 1;
  if (observed !== expectedCount) {
    throw new Error(`${label}: expected ${expectedCount} occurrences, observed ${observed}`);
  }
  source = source.split(needle).join(replacement);
}

const uuidPattern = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
const carryIdentity = `(
        UPPER(COALESCE(
          NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'line_type', '')), ''),
          NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'case_type', '')), ''),
          'PREVIEW_ROW'
        )) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
        AND UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'case_type', '')), '')) = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
        AND UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'item_direction', scope_selected_lines.line_json->>'direction', '')), '')) IN ('CREDIT', 'DEBIT')
        AND NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_case_id', '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_component_id', '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'manual_adjustment_carry_forward_id', '')), '') ~* '${uuidPattern}'
        AND NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'source_ref', '')), '')
              = 'carry_forward:' || LOWER(scope_selected_lines.line_json->>'manual_adjustment_carry_forward_id')
        AND NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'case_key', '')), '')
              = 'carry_forward:' || LOWER(scope_selected_lines.line_json->>'manual_adjustment_carry_forward_id')
        AND UPPER(NULLIF(BTRIM(COALESCE(
              scope_selected_lines.line_json#>>'{economic_key,key_type}',
              scope_selected_lines.line_json->>'key_type',
              scope_selected_lines.line_json->>'component_key_type',
              ''
            )), '')) = 'MANUAL_CARRY_FORWARD'
        AND NULLIF(BTRIM(COALESCE(
              scope_selected_lines.line_json#>>'{economic_key,key_value}',
              scope_selected_lines.line_json->>'key_value',
              scope_selected_lines.line_json->>'component_key_value',
              ''
            )), '') = LOWER(scope_selected_lines.line_json->>'manual_adjustment_carry_forward_id')
        AND (
          (
            COALESCE(scope_selected_lines.line_json->>'amount_ex_vat', scope_selected_lines.line_json->>'preview_amount_ex_vat', scope_selected_lines.line_json->>'allocated_amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            AND ROUND(COALESCE(scope_selected_lines.line_json->>'amount_ex_vat', scope_selected_lines.line_json->>'preview_amount_ex_vat', scope_selected_lines.line_json->>'allocated_amount')::numeric, 2) > 0
            AND UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'item_direction', scope_selected_lines.line_json->>'direction', '')), '')) = 'CREDIT'
          )
          OR (
            COALESCE(scope_selected_lines.line_json->>'amount_ex_vat', scope_selected_lines.line_json->>'preview_amount_ex_vat', scope_selected_lines.line_json->>'allocated_amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            AND ROUND(COALESCE(scope_selected_lines.line_json->>'amount_ex_vat', scope_selected_lines.line_json->>'preview_amount_ex_vat', scope_selected_lines.line_json->>'allocated_amount')::numeric, 2) < 0
            AND UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'item_direction', scope_selected_lines.line_json->>'direction', '')), '')) = 'DEBIT'
          )
        )
      ) AS is_certified_manual_carry_forward`;

replaceExact(
  `        ELSE NULL::numeric\n      END AS allocated_amount`,
  `        ELSE NULL::numeric\n      END AS allocated_amount,\n      ${carryIdentity}`,
  1,
  'carry-forward identity projection'
);

replaceExact(
  `      normalised_scope_lines_raw.allocated_amount,\n      CASE`,
  `      normalised_scope_lines_raw.allocated_amount,\n      normalised_scope_lines_raw.is_certified_manual_carry_forward,\n      CASE`,
  1,
  'carry-forward identity propagation'
);

replaceExact(
  `          normalised_scope_lines_raw.finance_case_id IS NULL\n          AND backing_preview_row.timesheet_id IS NULL\n        )`,
  `          normalised_scope_lines_raw.finance_case_id IS NULL\n          AND backing_preview_row.timesheet_id IS NULL\n          AND COALESCE(normalised_scope_lines_raw.is_certified_manual_carry_forward, false) IS NOT TRUE\n        )`,
  1,
  'backing-row economic identity gate'
);

replaceExact(
  `        WHEN normalised_scope_lines_raw.finance_case_id IS NULL AND backing_preview_row.timesheet_id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_ECONOMIC_KEY_MISSING'`,
  `        WHEN normalised_scope_lines_raw.finance_case_id IS NULL\n          AND backing_preview_row.timesheet_id IS NULL\n          AND COALESCE(normalised_scope_lines_raw.is_certified_manual_carry_forward, false) IS NOT TRUE\n          THEN 'ALLOCATION_BACKING_PREVIEW_ROW_ECONOMIC_KEY_MISSING'`,
  1,
  'backing-row failure reason'
);

const financeNegative = `(
                  invalid_rows.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
                  AND invalid_rows.finance_case_id IS NOT NULL
                  AND (invalid_rows.item_direction IS NULL OR invalid_rows.item_direction = 'DEDUCTION')
                )`;
replaceExact(
  `AND NOT ${financeNegative}`,
  `AND NOT (\n                  ${financeNegative}\n                  OR COALESCE(invalid_rows.is_certified_manual_carry_forward, false) IS TRUE\n                )`,
  1,
  'diagnostic negative carry-forward allowance'
);

const candidateFinanceNegative = `(
           candidate_row.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
           AND candidate_row.finance_case_id IS NOT NULL
           AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
         )`;
replaceExact(
  `AND NOT ${candidateFinanceNegative}`,
  `AND NOT (\n           ${candidateFinanceNegative}\n           OR COALESCE(candidate_row.is_certified_manual_carry_forward, false) IS TRUE\n         )`,
  1,
  'candidate invalid-row negative carry-forward allowance'
);

const selectedCandidateFinanceNegative = `(
          candidate_row.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
          AND candidate_row.finance_case_id IS NOT NULL
          AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
        )`;
replaceExact(
  selectedCandidateFinanceNegative,
  `${selectedCandidateFinanceNegative}\n        OR COALESCE(candidate_row.is_certified_manual_carry_forward, false) IS TRUE`,
  1,
  'selected candidate negative carry-forward allowance'
);

replaceExact(
  `       OR (candidate_row.timesheet_id IS NULL AND candidate_row.finance_case_id IS NULL)`,
  `       OR (\n         candidate_row.timesheet_id IS NULL\n         AND candidate_row.finance_case_id IS NULL\n         AND COALESCE(candidate_row.is_certified_manual_carry_forward, false) IS NOT TRUE\n       )`,
  1,
  'malformed economic identity gate'
);

replaceExact(
  `      AND (candidate_row.timesheet_id IS NOT NULL OR candidate_row.finance_case_id IS NOT NULL)`,
  `      AND (\n        candidate_row.timesheet_id IS NOT NULL\n        OR candidate_row.finance_case_id IS NOT NULL\n        OR COALESCE(candidate_row.is_certified_manual_carry_forward, false) IS TRUE\n      )`,
  1,
  'selected-row economic identity gate'
);

const header = `-- CloudTMS Banking Pay manual carry-forward allocation handoff.\n-- This exact replacement is derived from the current V8 row-backed allocation\n-- owner. It preserves that bounded transport and all finance/recovery economics.\n-- It only recognises the correction owner's exact signed\n-- MANUAL_ADJUSTMENT_CARRY_FORWARD identity, including the source-valid nullable\n-- Timesheet shape, so the row can reach the existing item and source-reservation\n-- owners without being reclassified as a finance case.\n\n`;

const finalSql = header + source;
const definitions = finalSql.match(/CREATE OR REPLACE FUNCTION\s+public\.pay_workbench_prepare_draft_allocation_rows_seed\s*\(/gi) ?? [];
if (definitions.length !== 1) throw new Error(`expected one allocation owner; observed ${definitions.length}`);
for (const forbidden of ['pg_catalog.coalesce(', 'pg_catalog.nullif(', 'pg_catalog.least(', 'pg_catalog.greatest(']) {
  if (finalSql.toLowerCase().includes(forbidden)) throw new Error(`forbidden PostgreSQL conditional qualification: ${forbidden}`);
}

fs.writeFileSync(outputPath, finalSql);
console.log(`wrote ${path.relative(root, outputPath)} (${Buffer.byteLength(finalSql)} bytes)`);
