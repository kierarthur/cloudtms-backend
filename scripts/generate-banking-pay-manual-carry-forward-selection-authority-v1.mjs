import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1056_banking_pay_cancelled_advance_repayment_preview_authority_v1.sql'
);
const outputPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql'
);
const checkOnly = process.argv.includes('--check');

const source = fs.readFileSync(sourcePath, 'utf8');
const marker = 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines';
const start = source.indexOf(marker);
const end = source.indexOf('CREATE OR REPLACE FUNCTION public.', start + marker.length);
if (start < 0 || end > 0) {
  throw new Error('expected one final canonical-lines owner in the source file');
}

let replacement = source.slice(start).trimEnd() + '\n';
if ((replacement.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length !== 1) {
  throw new Error('replacement must contain exactly one function');
}

const before = `              'manual_adjustment_carry_forward_status', cf_lines.status,
              'target_pay_batch_id', CASE WHEN cf_lines.target_pay_batch_id IS NULL THEN NULL ELSE cf_lines.target_pay_batch_id::text END,
              'target_pay_batch_item_id', CASE WHEN cf_lines.target_pay_batch_item_id IS NULL THEN NULL ELSE cf_lines.target_pay_batch_item_id::text END,
              'target_operation_source_key', cf_lines.target_operation_source_key,
              'readiness_state', 'READY_TO_PAY',
              'draftable', true,
              'is_ready_for_draft', true,
              'is_excluded_from_allocation', false,`;
const after = `              'manual_adjustment_carry_forward_status', cf_lines.status,
              'target_pay_batch_id', CASE WHEN cf_lines.target_pay_batch_id IS NULL THEN NULL ELSE cf_lines.target_pay_batch_id::text END,
              'target_pay_batch_item_id', CASE WHEN cf_lines.target_pay_batch_item_id IS NULL THEN NULL ELSE cf_lines.target_pay_batch_item_id::text END,
              'target_operation_source_key', cf_lines.target_operation_source_key,
              'readiness_state', 'READY_TO_PAY',
              'draftable', true,
              'is_ready_for_draft', true,
              'selection_allowed', true,
              'is_excluded_from_allocation', false,`;
const occurrenceCount = replacement.split(before).length - 1;
if (occurrenceCount !== 1) {
  throw new Error(`manual carry-forward selection boundary changed: ${occurrenceCount}`);
}
replacement = replacement.replace(before, after);

const historicalAcl = `REVOKE ALL ON FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  FROM PUBLIC,anon;
GRANT EXECUTE ON FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  TO postgres,authenticated,service_role;`;
const currentServiceOnlyAcl = `REVOKE ALL ON FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)
  TO postgres,service_role;`;
const aclOccurrenceCount = replacement.split(historicalAcl).length - 1;
if (aclOccurrenceCount !== 1) {
  throw new Error(`canonical-lines ACL boundary changed: ${aclOccurrenceCount}`);
}
replacement = replacement.replace(historicalAcl, currentServiceOnlyAcl);

const output = `-- CloudTMS Banking Pay manual carry-forward selection authority.
-- A current, unconsumed carry-forward that the canonical owner already marks
-- Ready and draftable must also carry the explicit selection flag required by
-- the Workbench selection contract. No amount, tax, VAT or payment policy is
-- recalculated here.
-- Generated from the prior exact owner by the checked repository generator.

${replacement}`;

if (checkOnly) {
  if (!fs.existsSync(outputPath)) throw new Error('generated replacement is missing');
  if (fs.readFileSync(outputPath, 'utf8') !== output) {
    throw new Error('generated replacement differs from the checked owner');
  }
  process.stdout.write('manual carry-forward selection authority: CHECK PASS\n');
} else {
  fs.writeFileSync(outputPath, output, 'utf8');
  process.stdout.write(`${path.relative(root, outputPath)}\n`);
}
