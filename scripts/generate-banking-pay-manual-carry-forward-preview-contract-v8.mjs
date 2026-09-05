import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, '..');
const sourcePath = path.join(root, 'supabase', 'repeatable', '26052026_2100HRS_NEW_FUNCTIONS.sql');
const outputPath = path.join(root, 'supabase', 'repeatable', '04092026_1350_banking_pay_manual_carry_forward_preview_contract_v8.sql');
const checkOnly = process.argv.includes('--check');
const source = fs.readFileSync(sourcePath, 'utf8');
const marker = 'CREATE OR REPLACE FUNCTION public.pay_workbench_preview_line_contract_ok(';
const start = source.indexOf(marker);
if (start < 0 || source.indexOf(marker, start + marker.length) >= 0) {
  throw new Error('expected exactly one preview-line contract owner in the historical source');
}
const endMarker = '\n$function$;';
const end = source.indexOf(endMarker, start);
if (end < 0) throw new Error('preview-line contract terminator not found');
let replacement = source.slice(start, end + endMarker.length).trimEnd();
if ((replacement.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length !== 1) {
  throw new Error('replacement must contain exactly one function');
}

function replaceOnce(before, after, label) {
  const count = replacement.split(before).length - 1;
  if (count !== 1) throw new Error(`${label} changed: ${count}`);
  replacement = replacement.replace(before, () => after);
}

replaceOnce(
  `  v_finance_case_id_present boolean := false;
  v_is_recognised_finance_deduction boolean := false;`,
  `  v_finance_case_id_present boolean := false;
  v_is_recognised_finance_deduction boolean := false;
  v_manual_carry_forward_id_text text := NULLIF(BTRIM(COALESCE(v_line_json->>'manual_adjustment_carry_forward_id', '')), '');
  v_is_recognised_manual_carry_forward boolean := false;`,
  'preview-line declarations'
);

replaceOnce(
  `  v_is_recognised_finance_deduction := (
    COALESCE(v_finance_case_id_present, false)
    AND (v_item_direction IS NULL OR v_item_direction = 'DEDUCTION')
    AND (
      v_line_type = 'OVERPAYMENT_RECOVERY'
      OR v_line_type IN ('MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
    )
  );`,
  `  v_is_recognised_finance_deduction := (
    COALESCE(v_finance_case_id_present, false)
    AND (v_item_direction IS NULL OR v_item_direction = 'DEDUCTION')
    AND (
      v_line_type = 'OVERPAYMENT_RECOVERY'
      OR v_line_type IN ('MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
    )
  );

  -- Cancellation freezes signed manual adjustments independently of finance
  -- cases.  Accept that existing owner only when every carry-forward identity
  -- agrees; this does not classify or calculate the signed amount.
  v_is_recognised_manual_carry_forward := (
    v_line_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
    AND v_case_type = 'MANUAL_ADJUSTMENT_CARRY_FORWARD'
    AND v_item_direction IN ('CREDIT', 'DEBIT')
    AND COALESCE(v_manual_carry_forward_id_text, '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND NULLIF(BTRIM(COALESCE(v_line_json->>'source_ref', '')), '')
          = 'carry_forward:' || LOWER(v_manual_carry_forward_id_text)
    AND NULLIF(BTRIM(COALESCE(v_line_json->>'case_key', '')), '')
          = 'carry_forward:' || LOWER(v_manual_carry_forward_id_text)
  );`,
  'recognised carry-forward identity'
);

replaceOnce(
  `    ELSIF ROUND(COALESCE(v_amount, 0), 2) < 0
       AND COALESCE(v_is_recognised_finance_deduction, false) IS NOT TRUE THEN`,
  `    ELSIF ROUND(COALESCE(v_amount, 0), 2) < 0
       AND COALESCE(v_is_recognised_finance_deduction, false) IS NOT TRUE
       AND COALESCE(v_is_recognised_manual_carry_forward, false) IS NOT TRUE THEN`,
  'negative Ready-row routing'
);

replaceOnce(
  `    'is_recognised_finance_deduction', COALESCE(v_is_recognised_finance_deduction, false),
    'line_key', v_line_key,`,
  `    'is_recognised_finance_deduction', COALESCE(v_is_recognised_finance_deduction, false),
    'is_recognised_manual_carry_forward', COALESCE(v_is_recognised_manual_carry_forward, false),
    'line_key', v_line_key,`,
  'diagnostic result'
);

const output = `-- CloudTMS Banking Pay manual carry-forward preview contract.
-- The cancellation owner creates signed CREDIT and DEBIT carry-forwards and
-- the canonical Workbench owner marks both as Ready.  This exact replacement
-- lets the common preview contract recognise that established non-finance-case
-- identity while retaining every existing negative-entitlement fence.
-- Generated from the historical function by the checked repository generator.

${replacement}

ALTER FUNCTION public.pay_workbench_preview_line_contract_ok(jsonb, jsonb, text)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_preview_line_contract_ok(jsonb, jsonb, text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_preview_line_contract_ok(jsonb, jsonb, text)
  TO service_role;
`;

if (checkOnly) {
  if (!fs.existsSync(outputPath)) throw new Error('generated replacement is missing');
  if (fs.readFileSync(outputPath, 'utf8') !== output) throw new Error('generated replacement differs');
  process.stdout.write('Manual carry-forward preview contract: CHECK PASS\n');
} else {
  fs.writeFileSync(outputPath, output, 'utf8');
  process.stdout.write(`${path.relative(root, outputPath)}\n`);
}
