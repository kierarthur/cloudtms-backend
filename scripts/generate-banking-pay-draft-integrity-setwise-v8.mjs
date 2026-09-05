import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1530_banking_pay_draft_integrity_advance_lookup_v8.sql'
);
const outputPath = path.join(
  root,
  'supabase',
  'repeatable',
  '05092026_0310_banking_pay_draft_integrity_setwise_v8.sql'
);
const checkOnly = process.argv.includes('--check');
const source = fs.readFileSync(sourcePath, 'utf8');
const publicMarker = 'CREATE OR REPLACE FUNCTION public.pay_batch_assert_integrity';
const publicStart = source.indexOf(publicMarker);
if (publicStart < 0 || source.indexOf(publicMarker, publicStart + publicMarker.length) >= 0) {
  throw new Error('expected exactly one current public Draft integrity owner');
}

let publicOwner = source.slice(publicStart).trimEnd();

function replaceExactOnce(input, before, after, label) {
  const first = input.indexOf(before);
  if (first < 0 || input.indexOf(before, first + before.length) >= 0) {
    throw new Error(`${label} boundary changed`);
  }
  return `${input.slice(0, first)}${after}${input.slice(first + before.length)}`;
}

const operationCteMarker = '    WITH operation_checks AS (';
const setwiseOperationCtes = `    WITH operation_scope_rows AS MATERIALIZED (
      SELECT
        scope_row.id,
        scope_row.operation_id,
        scope_row.pay_batch_id,
        scope_row.candidate_id,
        scope_row.pay_channel,
        scope_row.candidate_totals_json,
        scope_row.allocation_basis_json
      FROM public.banking_pay_operation_candidate_scope AS scope_row
      WHERE scope_row.operation_id = p_operation_id
        AND scope_row.pay_batch_id = v_batch_id
    ), operation_reservation_rows AS MATERIALIZED (
      SELECT
        scope_row.id AS candidate_scope_id,
        reservation_row.ordinality,
        reservation_row.value AS reservation_json,
        coalesce(
          nullif(btrim(coalesce(reservation_row.value->>'preview_row_id', '')), ''),
          nullif(btrim(coalesce(reservation_row.value->>'line_id', '')), ''),
          nullif(btrim(coalesce(reservation_row.value->>'row_id', '')), ''),
          nullif(btrim(coalesce(reservation_row.value->>'id', '')), '')
        ) AS preview_row_id
      FROM operation_scope_rows AS scope_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}') = 'array'
            THEN scope_row.allocation_basis_json #> '{reservation_availability,preview_rows}'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS reservation_row(value, ordinality)
    ), operation_first_reservation_rows AS MATERIALIZED (
      SELECT DISTINCT ON (reservation_row.candidate_scope_id, reservation_row.preview_row_id)
        reservation_row.candidate_scope_id,
        reservation_row.preview_row_id,
        reservation_row.reservation_json
      FROM operation_reservation_rows AS reservation_row
      WHERE reservation_row.preview_row_id IS NOT NULL
      ORDER BY
        reservation_row.candidate_scope_id,
        reservation_row.preview_row_id,
        reservation_row.ordinality
    ), operation_selected_source_lines AS MATERIALIZED (
      SELECT
        scope_row.id AS candidate_scope_id,
        scope_row.candidate_id,
        scope_row.pay_channel,
        line_element.value AS line_json
      FROM operation_scope_rows AS scope_row
      CROSS JOIN LATERAL private.pay_workbench_operation_selected_lines_v8(
        p_operation_id,
        scope_row.id
      ) AS line_element(value)
    ), operation_selected_line_counts AS MATERIALIZED (
      SELECT
        selected_source.candidate_scope_id,
        count(*)::integer AS line_count
      FROM operation_selected_source_lines AS selected_source
      GROUP BY selected_source.candidate_scope_id
    ), operation_selected_lines AS MATERIALIZED (
      SELECT
        selected_source.candidate_scope_id,
        selected_source.candidate_id,
        selected_source.pay_channel,
        coalesce(
          nullif(btrim(coalesce(selected_source.line_json->>'preview_row_id', '')), ''),
          nullif(btrim(coalesce(selected_source.line_json->>'line_id', '')), ''),
          nullif(btrim(coalesce(selected_source.line_json->>'row_id', '')), ''),
          nullif(btrim(coalesce(selected_source.line_json->>'id', '')), '')
        ) AS preview_row_id,
        CASE WHEN coalesce(selected_source.line_json->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (selected_source.line_json->>'finance_case_id')::uuid ELSE NULL::uuid END AS finance_case_id,
        CASE WHEN coalesce(selected_source.line_json->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN (selected_source.line_json->>'timesheet_id')::uuid ELSE NULL::uuid END AS timesheet_id,
        round(coalesce(
          CASE WHEN coalesce(reservation_row.reservation_json->>'effective_amount_ex_vat', '') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (reservation_row.reservation_json->>'effective_amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN coalesce(selected_source.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (selected_source.line_json->>'amount_ex_vat')::numeric ELSE NULL::numeric END,
          CASE WHEN coalesce(selected_source.line_json->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (selected_source.line_json->>'preview_amount_ex_vat')::numeric ELSE NULL::numeric END,
          0::numeric
        ), 2) AS expected_amount_ex_vat,
        coalesce(
          lower(coalesce(reservation_row.reservation_json->>'skipped_due_to_active_reservation', 'false')) IN ('true','t','1','yes','y'),
          false
        ) AS skipped_due_to_active_reservation
      FROM operation_selected_source_lines AS selected_source
      LEFT JOIN operation_first_reservation_rows AS reservation_row
        ON reservation_row.candidate_scope_id = selected_source.candidate_scope_id
       AND reservation_row.preview_row_id = coalesce(
         nullif(btrim(coalesce(selected_source.line_json->>'preview_row_id', '')), ''),
         nullif(btrim(coalesce(selected_source.line_json->>'line_id', '')), ''),
         nullif(btrim(coalesce(selected_source.line_json->>'row_id', '')), ''),
         nullif(btrim(coalesce(selected_source.line_json->>'id', '')), '')
       )
      WHERE jsonb_typeof(selected_source.line_json) = 'object'
        AND coalesce(CASE WHEN lower(coalesce(selected_source.line_json->>'draftable', 'true')) IN ('true','false') THEN (selected_source.line_json->>'draftable')::boolean ELSE true END, true) = true
    ), operation_allocation_rows AS MATERIALIZED (
      SELECT
        allocation_row.id,
        allocation_row.candidate_scope_id,
        allocation_row.candidate_id,
        allocation_row.pay_channel,
        allocation_row.status,
        allocation_row.pay_batch_item_id,
        coalesce(
          nullif(allocation_row.allocation_basis_json->>'preview_row_id', ''),
          nullif(allocation_row.allocation_basis_json#>>'{line,preview_row_id}', ''),
          nullif(allocation_row.allocation_basis_json#>>'{line,preview_row_pk}', '')
        ) AS bound_preview_row_id
      FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
      WHERE allocation_row.operation_id = p_operation_id
        AND allocation_row.pay_batch_id = v_batch_id
    ), operation_batch_items AS MATERIALIZED (
      SELECT
        item_row.id,
        batch_candidate.candidate_id,
        item_row.pay_channel,
        item_row.amount_ex_vat
      FROM public.pay_batch_candidates AS batch_candidate
      JOIN public.pay_batch_items AS item_row
        ON item_row.pay_batch_candidate_id = batch_candidate.id
      WHERE batch_candidate.pay_batch_id = v_batch_id
        AND coalesce(item_row.is_voided, false) = false
    ), operation_checks AS (`;
publicOwner = replaceExactOnce(
  publicOwner,
  operationCteMarker,
  setwiseOperationCtes,
  'operation materialisation'
);

const countCall = 'private.pay_workbench_operation_selected_line_count_v8(p_operation_id, scope_row.id)';
const countReplacement = "coalesce((SELECT selected_count.line_count FROM operation_selected_line_counts AS selected_count WHERE selected_count.candidate_scope_id = scope_row.id), 0)";
if (publicOwner.split(countCall).length - 1 !== 2) {
  throw new Error('selected-line count call boundary changed');
}
publicOwner = publicOwner.split(countCall).join(countReplacement);

const missingItemStart = "      SELECT 'MISSING_SELECTED_PREVIEW_ROW_ITEM'::text AS check_code,";
const candidateTotalsStart = "      SELECT 'CANDIDATE_SCOPE_TOTAL_MISMATCH'::text AS check_code,";
const missingStart = publicOwner.indexOf(missingItemStart);
const candidateStart = publicOwner.indexOf(candidateTotalsStart, missingStart);
if (missingStart < 0 || candidateStart < 0) {
  throw new Error('missing-selected-item boundary changed');
}
const currentMissingBlock = publicOwner.slice(missingStart, candidateStart);
const setwiseMissingBlock = `      SELECT 'MISSING_SELECTED_PREVIEW_ROW_ITEM'::text AS check_code,
             selected_line.candidate_id,
             jsonb_build_object(
               'candidate_scope_id', selected_line.candidate_scope_id::text,
               'candidate_id', selected_line.candidate_id::text,
               'pay_channel', selected_line.pay_channel,
               'preview_row_id', selected_line.preview_row_id,
               'finance_case_id', CASE WHEN selected_line.finance_case_id IS NULL THEN NULL ELSE selected_line.finance_case_id::text END,
               'timesheet_id', CASE WHEN selected_line.timesheet_id IS NULL THEN NULL ELSE selected_line.timesheet_id::text END
             ) AS detail_json
      FROM operation_selected_lines AS selected_line
      WHERE selected_line.preview_row_id IS NOT NULL
        AND coalesce(selected_line.skipped_due_to_active_reservation, false) = false
        AND NOT EXISTS (
          SELECT 1
          FROM operation_allocation_rows AS allocation_row
          JOIN operation_batch_items AS item_row
            ON item_row.id = allocation_row.pay_batch_item_id
           AND item_row.candidate_id = allocation_row.candidate_id
          WHERE allocation_row.candidate_scope_id = selected_line.candidate_scope_id
            AND allocation_row.candidate_id = selected_line.candidate_id
            AND allocation_row.bound_preview_row_id = selected_line.preview_row_id
            AND upper(btrim(coalesce(allocation_row.status, ''))) IN ('ITEM_CREATED', 'ITEM_INSERTED')
          LIMIT 1
        )

      UNION ALL

`;
publicOwner = `${publicOwner.slice(0, missingStart)}${setwiseMissingBlock}${publicOwner.slice(candidateStart)}`;
if (!currentMissingBlock.includes("item_row.operation_source_key LIKE ('%' || selected_line.preview_row_id || '%')")) {
  throw new Error('wildcard missing-item proof was not present in the prior owner');
}

const candidateBranchStart = publicOwner.indexOf(candidateTotalsStart);
const allocationMismatchStart = publicOwner.indexOf(
  "      SELECT 'ALLOCATION_ITEM_AMOUNT_MISMATCH'::text AS check_code,",
  candidateBranchStart
);
if (candidateBranchStart < 0 || allocationMismatchStart < 0) {
  throw new Error('candidate-total branch boundary changed');
}
let candidateBranch = publicOwner.slice(candidateBranchStart, allocationMismatchStart);
const oldCandidateExpectedStart = candidateBranch.indexOf('          round(coalesce((\n            SELECT sum(');
const oldCandidateExpectedEndMarker = '          ), 0::numeric), 2) AS expected_amount_ex_vat,';
const oldCandidateExpectedEnd = candidateBranch.indexOf(oldCandidateExpectedEndMarker, oldCandidateExpectedStart);
if (oldCandidateExpectedStart < 0 || oldCandidateExpectedEnd < 0) {
  throw new Error('candidate expected-total boundary changed');
}
const newCandidateExpected = `          round(coalesce((
            SELECT sum(selected_line.expected_amount_ex_vat)
            FROM operation_selected_lines AS selected_line
            WHERE selected_line.candidate_scope_id = scope_row.id
          ), 0::numeric), 2) AS expected_amount_ex_vat,`;
candidateBranch = `${candidateBranch.slice(0, oldCandidateExpectedStart)}${newCandidateExpected}${candidateBranch.slice(oldCandidateExpectedEnd + oldCandidateExpectedEndMarker.length)}`;
publicOwner = `${publicOwner.slice(0, candidateBranchStart)}${candidateBranch}${publicOwner.slice(allocationMismatchStart)}`;

const channelBranchStart = publicOwner.indexOf("      SELECT 'PAY_CHANNEL_SCOPE_TOTAL_MISMATCH'::text AS check_code,");
const operationChecksEnd = publicOwner.indexOf(
  '    )\n    SELECT count(*)::integer,',
  channelBranchStart
);
if (channelBranchStart < 0 || operationChecksEnd < 0) {
  throw new Error('channel-total branch boundary changed');
}
let channelBranch = publicOwner.slice(channelBranchStart, operationChecksEnd);
const expectedChannelStartMarker = `        FROM (
          SELECT
            upper(btrim(coalesce(scope_row.pay_channel, ''))) AS pay_channel,`;
const expectedChannelStart = channelBranch.indexOf(expectedChannelStartMarker);
const expectedChannelEndMarker = '        ) AS expected_channel';
const expectedChannelEnd = channelBranch.indexOf(expectedChannelEndMarker, expectedChannelStart);
if (expectedChannelStart < 0 || expectedChannelEnd < 0) {
  throw new Error('channel expected-total source boundary changed');
}
channelBranch = `${channelBranch.slice(0, expectedChannelStart)}        FROM operation_selected_lines AS expected_channel${channelBranch.slice(expectedChannelEnd + expectedChannelEndMarker.length)}`;
publicOwner = `${publicOwner.slice(0, channelBranchStart)}${channelBranch}${publicOwner.slice(operationChecksEnd)}`;

if (publicOwner.includes("item_row.operation_source_key LIKE ('%' || selected_line.preview_row_id || '%')")) {
  throw new Error('quadratic wildcard selected-item lookup remains');
}
const selectedReaderCall = 'private.pay_workbench_operation_selected_lines_v8(';
if (publicOwner.split(selectedReaderCall).length - 1 !== 1) {
  throw new Error('selected-line reader must be materialised exactly once');
}
if (!publicOwner.includes("'MISSING_SELECTED_PREVIEW_ROW_ITEM'::text")) {
  throw new Error('missing-selected-item integrity check was removed');
}

const output = `-- Set-wise read path for the unchanged Draft integrity policy.\n-- Runtime authority is Miget TEST. The \`supabase\` directory name is historical.\n--\n-- Every existing integrity decision, error, output field and economic comparison\n-- is retained. Frozen selected lines and reservation facts are read once, and\n-- selected-line materialisation is proved through the existing exact\n-- allocation-row to batch-item identity rather than a wildcard text scan.\n\n${publicOwner}\n`;

if ((output.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length !== 1) {
  throw new Error('generated owner must contain exactly one function definition');
}
if (checkOnly) {
  if (!fs.existsSync(outputPath) || fs.readFileSync(outputPath, 'utf8') !== output) {
    throw new Error('generated replacement differs from the checked owner');
  }
  process.stdout.write('Draft integrity set-wise owner: CHECK PASS\n');
} else {
  fs.writeFileSync(outputPath, output);
  process.stdout.write(`${path.relative(root, outputPath)}\n`);
}
