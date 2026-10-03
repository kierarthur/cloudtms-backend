#!/usr/bin/env node
// Mechanically copy one reviewed current authority; never replay the 2.7 MB
// Stage 2 bundle merely to restore this function after an older closure.
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = 'supabase/repeatable/26092026_0203_banking_pay_stage2_workbench_draft_v1.sql';
const outputPath = 'supabase/repeatable/03102026_0400_stage2_canonical_producer_after_h1h2_retry.sql';
const source = fs.readFileSync(path.join(root, sourcePath), 'utf8').replaceAll('\r\n', '\n');
const marker = 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines(';
assert.equal(source.split(marker).length - 1, 1, 'canonical producer owner must be unique');
const start = source.indexOf(marker);
const bodyStart = source.indexOf('AS $function$', start);
const endMarker = '\n$function$;';
const end = source.indexOf(endMarker, bodyStart);
assert.ok(bodyStart > start && end > bodyStart, 'canonical producer function boundary changed');
const definition = source.slice(start, end + endMarker.length);
assert.match(definition, /'component_key_type', 'MANUAL_CARRY_FORWARD'/);
assert.match(definition, /'economic_key', jsonb_strip_nulls\(jsonb_build_object\(/);
const hash = crypto.createHash('sha256').update(definition).digest('hex');
const output = [
  '-- Exact Stage 2 canonical producer reassertion after a resumed H1/H2 closure.',
  `-- Generated from ${sourcePath}; function SHA-256 ${hash}.`,
  '-- Replaces only this current function; no payment/provider action is executed.',
  '\\set ON_ERROR_STOP on',
  'begin;',
  definition,
  "ALTER FUNCTION public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb) SET plpgsql_check.mode TO 'disabled';",
  'commit;',
  '',
].join('\n');
const target = path.join(root, outputPath);
if (process.argv[2] === '--check') {
  assert.equal(fs.readFileSync(target, 'utf8').replaceAll('\r\n', '\n'), output);
  console.log('Stage 2 canonical producer reassertion matches its reviewed owner.');
} else if (process.argv[2] === '--write') {
  fs.writeFileSync(target, output, 'utf8');
  console.log(`Wrote ${outputPath} from ${sourcePath}.`);
} else {
  throw new Error('Use --check or --write');
}
