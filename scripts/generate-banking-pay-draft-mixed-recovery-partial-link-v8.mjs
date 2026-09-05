import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = path.join(root, 'supabase', 'repeatable', '02092026_2320_banking_pay_draft_finance_row_transport_v8.sql');
const outputPath = path.join(root, 'supabase', 'repeatable', '04092026_1300_banking_pay_draft_mixed_recovery_partial_link_v8.sql');

function replaceExactCount(source, expected, replacement, label, expectedCount = 1) {
  let count = 0;
  let cursor = 0;
  while ((cursor = source.indexOf(expected, cursor)) !== -1) {
    count += 1;
    cursor += expected.length;
  }
  if (count !== expectedCount) {
    throw new Error(`${label}: expected ${expectedCount} source boundaries, observed ${count}`);
  }
  // A callback is required because SQL validation regexes can contain `$'`,
  // which String.prototype.replace otherwise interprets as a substitution token.
  return source.replace(expected, () => replacement);
}

let sql = fs.readFileSync(sourcePath, 'utf8');

sql = replaceExactCount(
  sql,
  `-- CloudTMS Banking Pay V8 row-backed transport replacement.\n-- Runtime authority is Miget TEST. The historical finance owner remains byte-identical.\n-- The only semantic diff is transport: Stage 16C0 reads the same canonical lines\n-- through the V8 row-backed helper when the certified Draft route is active.`,
  `-- CloudTMS Banking Pay V8 row-backed finance transport and mixed-recovery retry compatibility.\n-- Runtime authority is Miget TEST. Every earlier finance owner remains byte-identical.\n-- The only additional semantic diff preserves an exact, already-linked PAYE NET_DEDUCT\n-- MANUAL_DEBT_RECOVERY item while the same operation materialises other finance rows.\n-- Amounts, headroom, PAYE/Umbrella treatment, VAT and case economics remain owned by\n-- the unchanged canonical preview, allocation, INSERT_ITEMS and finance branches.`,
  'header'
);

const candidateIdsNeedle = `  SELECT COALESCE(array_agg(distinct spr.candidate_id), ARRAY[]::uuid[])\n  INTO v_candidate_ids\n  FROM pg_temp.tmp_pay_build_selected_preview_rows spr;\n\n  IF p_operation_id IS NOT NULL THEN`;

const preservedLinksSql = `  SELECT COALESCE(array_agg(distinct spr.candidate_id), ARRAY[]::uuid[])\n  INTO v_candidate_ids\n  FROM pg_temp.tmp_pay_build_selected_preview_rows spr;\n\n  -- INSERT_ITEMS deliberately freezes a selected PAYE NET_DEDUCT Manual Debt\n  -- Recovery before the PAYE Worksheet amount exists.  If another finance row\n  -- is still pending, that exact link is successful prior-stage work rather than\n  -- a partial APPLY_FINANCE_ADJUSTMENTS attempt.  Preserve only byte-for-byte\n  -- matching business identity; every ambiguous or mismatched link retains the\n  -- existing fail-closed cleanup/error path.\n  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_apply_finance_preserved_manual_debt_links;\n  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_apply_finance_preserved_manual_debt_links ON COMMIT DROP AS\n+  SELECT\n+    allocation_row.id AS allocation_row_id,\n+    allocation_row.pay_batch_item_id,\n+    batch_candidate.id AS pay_batch_candidate_id,\n+    allocation_row.candidate_id,\n+    allocation_row.finance_case_id,\n+    allocation_row.finance_component_id,\n+    allocation_row.pay_channel\n+  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row\n+  JOIN public.pay_batch_candidates AS batch_candidate\n+    ON batch_candidate.pay_batch_id = v_batch_id\n+   AND batch_candidate.candidate_id = allocation_row.candidate_id\n+  JOIN public.pay_batch_items AS linked_item\n+    ON linked_item.id = allocation_row.pay_batch_item_id\n+   AND linked_item.pay_batch_candidate_id = batch_candidate.id\n+  WHERE p_operation_id IS NOT NULL\n+    AND allocation_row.operation_id = p_operation_id\n+    AND allocation_row.candidate_id = ANY(v_candidate_ids)\n+    AND UPPER(BTRIM(COALESCE(allocation_row.pay_channel, ''))) = 'PAYE'\n+    AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = 'MANUAL_DEBT_RECOVERY'\n+    AND allocation_row.status = 'ITEM_CREATED'\n+    AND allocation_row.pay_batch_id = v_batch_id\n+    AND allocation_row.pay_batch_item_id IS NOT NULL\n+    AND allocation_row.finance_case_id IS NOT NULL\n+    AND allocation_row.finance_component_id IS NOT NULL\n+    AND NULLIF(BTRIM(COALESCE(allocation_row.operation_source_key, '')), '') IS NOT NULL\n+    AND linked_item.operation_source_key = allocation_row.operation_source_key\n+    AND linked_item.item_type = 'MANUAL_DEBT_RECOVERY'\n+    AND UPPER(BTRIM(COALESCE(linked_item.pay_channel::text, ''))) = 'PAYE'\n+    AND linked_item.paye_treatment = 'NET_DEDUCT'\n+    AND linked_item.finance_case_id IS NOT DISTINCT FROM allocation_row.finance_case_id\n+    AND linked_item.finance_component_id IS NOT DISTINCT FROM allocation_row.finance_component_id\n+    AND linked_item.source_ref IS NOT DISTINCT FROM allocation_row.source_ref\n+    AND linked_item.frozen_component_key_type IS NOT DISTINCT FROM UPPER(NULLIF(BTRIM(COALESCE(\n+      allocation_row.allocation_basis_json#>>'{economic_key,key_type}',\n+      allocation_row.allocation_basis_json#>>'{line,key_type}',\n+      allocation_row.allocation_basis_json#>>'{line,component_key_type}',\n+      ''\n+    )), ''))\n+    AND linked_item.frozen_component_key_value IS NOT DISTINCT FROM NULLIF(BTRIM(COALESCE(\n+      allocation_row.allocation_basis_json#>>'{economic_key,key_value}',\n+      allocation_row.allocation_basis_json#>>'{line,key_value}',\n+      allocation_row.allocation_basis_json#>>'{line,component_key_value}',\n+      ''\n+    )), '')\n+    AND ROUND(COALESCE(linked_item.amount_ex_vat, 0), 2) = ROUND(COALESCE(allocation_row.allocated_amount, 0), 2)\n+    AND ROUND(COALESCE(linked_item.frozen_target_amount_ex_vat, linked_item.amount_ex_vat, 0), 2) = ROUND(COALESCE(allocation_row.allocated_amount, 0), 2)\n+    AND COALESCE(linked_item.is_voided, false) = false\n+    AND linked_item.reservation_id IS NULL\n+    AND UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,line_type}', ''))) = 'MANUAL_DEBT_RECOVERY'\n+    AND UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,pay_channel}', ''))) = 'PAYE'\n+    AND UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,paye_treatment}', ''))) = 'NET_DEDUCT'\n+    AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,finance_case_id}', '')), '') = allocation_row.finance_case_id::text\n+    AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,finance_component_id}', '')), '') = allocation_row.finance_component_id::text\n+    AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,source_ref}', '')), '') = allocation_row.source_ref\n+    AND LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,draftable}', 'false'))) IN ('true','t','1','yes','y','on')\n+    AND LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,is_ready_for_draft}', 'false'))) IN ('true','t','1','yes','y','on')\n+    AND LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,selection_allowed}', 'false'))) IN ('true','t','1','yes','y','on')\n+    AND COALESCE(allocation_row.allocation_basis_json#>>'{line,amount_ex_vat}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'\n+    AND ROUND((allocation_row.allocation_basis_json#>>'{line,amount_ex_vat}')::numeric, 2) = ROUND(COALESCE(allocation_row.allocated_amount, 0), 2);\n\n  IF p_operation_id IS NOT NULL THEN`;

sql = replaceExactCount(sql, candidateIdsNeedle, preservedLinksSql, 'preserved-link staging');

sql = replaceExactCount(
  sql,
  `        AND allocation_row.pay_batch_item_id = linked_item_delete.id\n        AND linked_item_delete.reservation_id IS NULL;`,
  `        AND allocation_row.pay_batch_item_id = linked_item_delete.id\n        AND linked_item_delete.reservation_id IS NULL\n        AND NOT EXISTS (\n          SELECT 1\n          FROM pg_temp.tmp_pay_batch_apply_finance_preserved_manual_debt_links AS preserved_link\n          WHERE preserved_link.allocation_row_id = allocation_row.id\n            AND preserved_link.pay_batch_item_id = linked_item_delete.id\n        );`,
  'partial delete exclusion'
);

sql = replaceExactCount(
  sql,
  `        AND allocation_reset.status = 'ITEM_CREATED';\n\n      v_operation_allocation_done := 0;`,
  `        AND allocation_reset.status = 'ITEM_CREATED'\n+        AND NOT EXISTS (\n+          SELECT 1\n+          FROM pg_temp.tmp_pay_batch_apply_finance_preserved_manual_debt_links AS preserved_link\n+          WHERE preserved_link.allocation_row_id = allocation_reset.id\n+        );\n\n      SELECT COUNT(*)::integer\n+      INTO v_operation_allocation_done\n+      FROM pg_temp.tmp_pay_batch_apply_finance_preserved_manual_debt_links;`,
  'partial allocation reset exclusion'
);

sql = replaceExactCount(
  sql,
  `  from final_alloc fa\n  where fa.take_target_ex > 0\n    and not (\n      upper(coalesce(fa.pay_channel, '')) = 'PAYE'`,
  `  from final_alloc fa\n  where fa.take_target_ex > 0\n    and not exists (\n+      select 1\n+      from pg_temp.tmp_pay_batch_apply_finance_preserved_manual_debt_links as preserved_link\n+      where preserved_link.pay_batch_candidate_id = fa.pay_batch_candidate_id\n+        and preserved_link.finance_case_id is not distinct from fa.finance_case_id\n+        and preserved_link.finance_component_id is not distinct from fa.finance_component_id\n+        and upper(coalesce(preserved_link.pay_channel, '')) = upper(coalesce(fa.pay_channel, ''))\n+    )\n+    and not (\n+      upper(coalesce(fa.pay_channel, '')) = 'PAYE'`,
  'manual-debt rematerialisation exclusion'
);

const injectedPatchMarkers = sql.match(/^\+/gm) ?? [];
if (injectedPatchMarkers.length !== 76) {
  throw new Error(`generated owner patch-marker count changed: ${injectedPatchMarkers.length}`);
}
sql = sql.replace(/^\+/gm, '');

if ((sql.match(/CREATE OR REPLACE FUNCTION public\.pay_batch_apply_finance_adjustments\(/g) ?? []).length !== 1) {
  throw new Error('generated owner must contain exactly one apply-finance definition');
}

const generated = Buffer.from(sql, 'utf8');
if (process.argv.includes('--check')) {
  if (!fs.existsSync(outputPath) || !fs.readFileSync(outputPath).equals(generated)) {
    throw new Error('generated mixed-recovery owner is stale; run the generator');
  }
} else {
  fs.writeFileSync(outputPath, generated);
}

process.stdout.write(`${path.relative(root, outputPath)} ${generated.length} bytes sha256=${crypto.createHash('sha256').update(generated).digest('hex')}\n`);
