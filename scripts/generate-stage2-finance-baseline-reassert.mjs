#!/usr/bin/env node
// A34 is the final reviewed finance-case baseline. The earlier A28 definition
// is adjacent to the canonical producer in A28 but must not win on retry.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = 'supabase/repeatable/26092026_0207_banking_pay_stage2_recovery_order_floor_v1.sql';
const outputPath = 'supabase/repeatable/03102026_0700_stage2_finance_baseline_after_h1h2_retry.sql';
const source = fs.readFileSync(path.join(root, sourcePath), 'utf8').replaceAll('\r\n', '\n');
const marker = 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_finance_case_baseline(';
assert.equal(source.split(marker).length - 1, 1, 'A34 finance baseline owner must be unique');
const start = source.indexOf(marker);
const bodyStart = source.indexOf('AS $function$', start);
const closing = /^\$function\$\s*;/gm;
closing.lastIndex = bodyStart;
const terminator = closing.exec(source);
assert.ok(bodyStart > start && terminator, 'A34 finance baseline function boundary changed');
const definition = source.slice(start, closing.lastIndex);
assert.deepEqual([...definition.matchAll(/^CREATE OR REPLACE FUNCTION ([\w.]+)\(/gm)].map(x => x[1]), [
  'public.pay_preview_candidate_build_finance_case_baseline',
]);
const plannerSetting = 'ALTER FUNCTION public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid) SET jit = off;';
assert.ok(source.includes(plannerSetting), 'A34 finance baseline planner setting changed');
const digest = crypto.createHash('sha256').update(definition).digest('hex');
const output = [
  '-- Exact A34 finance-case baseline after the historical H1/H2 closure and A28 preview reassertion.',
  `-- Generated from ${sourcePath}; function SHA-256 ${digest}.`,
  '-- Restores the reviewed definition and its planner-only jit setting; invokes no payment/provider action.',
  '\\set ON_ERROR_STOP on',
  'BEGIN;',
  definition,
  plannerSetting,
  'COMMIT;',
  '',
].join('\n');
const target = path.join(root, outputPath);
if (process.argv[2] === '--check') {
  assert.equal(fs.readFileSync(target, 'utf8').replaceAll('\r\n', '\n'), output);
  console.log('A34 finance baseline reassertion matches its reviewed owner.');
} else if (process.argv[2] === '--write') {
  fs.writeFileSync(target, output, 'utf8');
  console.log(`Wrote ${outputPath} from ${sourcePath}.`);
} else {
  throw new Error('Use --check or --write');
}
