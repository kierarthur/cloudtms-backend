import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '17082026_2052_pay_finance_resolution_cancel_authority.sql'
);
const outputPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1056_banking_pay_cancelled_advance_repayment_preview_authority_v1.sql'
);
const checkOnly = process.argv.includes('--check');

const source = fs.readFileSync(sourcePath, 'utf8');
const marker = 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines';
const start = source.indexOf(marker);
const end = source.indexOf('CREATE OR REPLACE FUNCTION public.', start + marker.length);
if (start < 0 || end <= start) throw new Error('canonical finance-line owner boundary changed');

let replacement = source.slice(start, end).trimEnd() + '\n';
if ((replacement.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length !== 1) {
  throw new Error('replacement must contain exactly one function');
}

function replaceExact(before, after, expectedCount, label) {
  const actualCount = replacement.split(before).length - 1;
  if (actualCount !== expectedCount) {
    throw new Error(`${label}: expected ${expectedCount} source occurrences, found ${actualCount}`);
  }
  replacement = replacement.split(before).join(after);
}

replaceExact(
  "          fcrr.lifecycle_status_display,\n          component_identity.component_count,",
  "          fcrr.lifecycle_status_display,\n          finance_case_baseline.payout_status as source_payout_status,\n          component_identity.component_count,",
  1,
  'source payout projection'
);
replaceExact(
  "upper(coalesce(fcrr.lifecycle_status_display,'')) <> 'PAID'",
  "upper(coalesce(finance_case_baseline.payout_status::text,'')) <> 'PAID'",
  5,
  'visible advance direction predicates'
);
replaceExact(
  "upper(coalesce(fcrr.lifecycle_status_display, '')) = 'PAID'",
  "upper(coalesce(fcrrb.payout_status::text, '')) = 'PAID'",
  1,
  'zero-take repayment template predicate'
);
replaceExact(
  "upper(coalesce(fcl.lifecycle_status_display,'')) = 'PAID'",
  "upper(coalesce(fcl.source_payout_status::text,'')) = 'PAID'",
  1,
  'repayment snooze predicate'
);

const output = `-- CloudTMS Banking Pay finance preview authority.\n-- A cancelled Draft repayment releases only that Draft reservation. The paid\n-- advance and its remaining debt stay authoritative, so a refreshed preview\n-- remains a repayment; the Loans/Snoozes display may still say Cancelled.\n-- Generated from the prior exact owner by the checked repository generator.\n\n${replacement}`;

if (checkOnly) {
  if (!fs.existsSync(outputPath)) throw new Error('generated replacement is missing');
  if (fs.readFileSync(outputPath, 'utf8') !== output) {
    throw new Error('generated replacement differs from the checked owner');
  }
  process.stdout.write('cancelled advance repayment preview authority: CHECK PASS\n');
} else {
  fs.writeFileSync(outputPath, output, 'utf8');
  process.stdout.write(`${path.relative(root, outputPath)}\n`);
}
