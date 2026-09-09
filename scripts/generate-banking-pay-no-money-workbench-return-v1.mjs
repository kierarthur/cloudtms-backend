import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(scriptDir, '..');
const sourceRelative = 'supabase/repeatable/04092026_2350_banking_pay_cancellation_completion_v1.sql';
const outputRelative = 'supabase/repeatable/09092026_0020_banking_pay_no_money_workbench_return_v1.sql';
const sourcePath = path.join(repoRoot, ...sourceRelative.split('/'));
const outputPath = path.join(repoRoot, ...outputRelative.split('/'));
const expectedSourceSha256 = '60e5fc26fbd147991c16ada0aaf2a9691143ccf46aa6e3227dc78d823a7a7ce1';

const sourceBytes = fs.readFileSync(sourcePath);
const sourceSha256 = crypto.createHash('sha256').update(sourceBytes).digest('hex');
assert.equal(sourceSha256, expectedSourceSha256, 'historical cancellation-completion owner changed');

const source = sourceBytes.toString('utf8').replaceAll('\r\n', '\n');
const definitionStart = source.indexOf(
  'CREATE OR REPLACE FUNCTION public.pay_payment_correction_process_chunk('
);
assert.ok(definitionStart >= 0, 'pay_payment_correction_process_chunk definition missing');

const definitionAndAcl = source.slice(definitionStart);
assert.equal(
  (definitionAndAcl.match(/CREATE OR REPLACE FUNCTION public\.pay_payment_correction_process_chunk\(/g) || []).length,
  1,
  'expected one pay_payment_correction_process_chunk definition'
);
assert.equal(
  (definitionAndAcl.match(/CREATE OR REPLACE FUNCTION /g) || []).length,
  1,
  'source owner contains an unexpected second function'
);

const oldRefreshGate =
  "ELSIF v_requested_action IN ('DRAFT_CANCEL','PRE_BANK_CANCEL','CANCEL_PAYMENT','NO_MONEY_UNWIND') THEN";
const newRefreshGate =
  "ELSIF v_requested_action IN ('DRAFT_CANCEL','PRE_BANK_CANCEL','CANCEL_PAYMENT','NO_MONEY_RELEASE','NO_MONEY_UNWIND') THEN";
assert.equal(
  definitionAndAcl.split(oldRefreshGate).length - 1,
  1,
  'terminal Workbench refresh gate did not match exactly once'
);
assert.equal(
  definitionAndAcl.includes(newRefreshGate),
  false,
  'historical owner already contains the corrected refresh gate'
);

const correctedDefinitionAndAcl = definitionAndAcl.replace(oldRefreshGate, newRefreshGate);
assert.equal(
  correctedDefinitionAndAcl.replace(newRefreshGate, oldRefreshGate),
  definitionAndAcl,
  'candidate changed more than the one approved action-alias gate'
);

const output = `-- Final authority for the failed-payment/no-money Workbench return handoff.\n` +
  `-- Generated from ${sourceRelative}; the historical owner remains byte-identical.\n` +
  `-- NO_MONEY_RELEASE is the public request action for the same internal\n` +
  `-- NO_MONEY_UNWIND correction kind. This replacement changes only the final\n` +
  `-- post-cancellation Workbench refresh admission. It changes no payment,\n` +
  `-- eligibility, amount, tax, VAT, settlement, reservation or cancellation rule.\n\n` +
  correctedDefinitionAndAcl;

fs.writeFileSync(outputPath, output.endsWith('\n') ? output : `${output}\n`, 'utf8');

const outputSha256 = crypto.createHash('sha256').update(fs.readFileSync(outputPath)).digest('hex');
process.stdout.write(`${JSON.stringify({
  ok: true,
  source: sourceRelative,
  source_sha256: sourceSha256,
  output: outputRelative,
  output_sha256: outputSha256,
  semantic_change: 'terminal Workbench refresh accepts canonical NO_MONEY_RELEASE alias'
}, null, 2)}\n`);
