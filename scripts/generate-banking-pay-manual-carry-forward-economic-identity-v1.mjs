import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql'
);
const outputPath = path.join(
  root,
  'supabase',
  'repeatable',
  '07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql'
);

const sha256 = (value) => createHash('sha256').update(value).digest('hex');
const occurrences = (value, needle) => value.split(needle).length - 1;

const identityAnchor =
  "              'manual_adjustment_carry_forward_id', cf_lines.manual_adjustment_carry_forward_id::text,\n" +
  "              'source_ref', ('carry_forward:' || cf_lines.manual_adjustment_carry_forward_id::text),";

const identityReplacement =
  "              'manual_adjustment_carry_forward_id', cf_lines.manual_adjustment_carry_forward_id::text,\n" +
  "              'component_key_type', 'MANUAL_CARRY_FORWARD',\n" +
  "              'component_key_value', cf_lines.manual_adjustment_carry_forward_id::text,\n" +
  "              'key_type', 'MANUAL_CARRY_FORWARD',\n" +
  "              'key_value', cf_lines.manual_adjustment_carry_forward_id::text,\n" +
  "              'economic_key', jsonb_strip_nulls(jsonb_build_object(\n" +
  "                'timesheet_id', CASE WHEN cf_lines.timesheet_id IS NULL THEN NULL ELSE cf_lines.timesheet_id::text END,\n" +
  "                'key_type', 'MANUAL_CARRY_FORWARD',\n" +
  "                'key_value', cf_lines.manual_adjustment_carry_forward_id::text\n" +
  "              )),\n" +
  "              'source_ref', ('carry_forward:' || cf_lines.manual_adjustment_carry_forward_id::text),";

async function build() {
  const source = await readFile(sourcePath, 'utf8');
  assert.equal(
    occurrences(source, 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines('),
    1,
    'historical owner must contain exactly one target function'
  );
  assert.equal(
    occurrences(source, identityAnchor),
    1,
    'historical owner no longer has the exact proved missing-identity boundary'
  );
  assert.equal(
    occurrences(source, "'key_type', 'MANUAL_CARRY_FORWARD'"),
    0,
    'historical owner already contains the proposed carry-forward identity'
  );

  const output = source.replace(identityAnchor, identityReplacement);
  assert.notEqual(output, source, 'replacement produced no change');
  assert.equal(
    output.replace(identityReplacement, identityAnchor),
    source,
    'generated owner contains a change outside the exact identity addition'
  );
  assert.equal(
    occurrences(output, 'CREATE OR REPLACE FUNCTION public.pay_preview_candidate_build_canonical_lines('),
    1,
    'generated owner must contain exactly one target function'
  );
  assert.equal(occurrences(output, "'key_type', 'MANUAL_CARRY_FORWARD'"), 2);
  assert.equal(occurrences(output, "'key_value', cf_lines.manual_adjustment_carry_forward_id::text"), 2);
  assert.equal(occurrences(output, "'component_key_type', 'MANUAL_CARRY_FORWARD'"), 1);
  assert.equal(occurrences(output, "'component_key_value', cf_lines.manual_adjustment_carry_forward_id::text"), 1);
  assert.equal(occurrences(output, "'economic_key', jsonb_strip_nulls(jsonb_build_object("), 2);
  assert.match(output, /ALTER FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+OWNER TO postgres;/);
  assert.match(output, /REVOKE ALL ON FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+FROM PUBLIC,anon,authenticated,service_role;/);
  assert.match(output, /GRANT EXECUTE ON FUNCTION public\.pay_preview_candidate_build_canonical_lines\(jsonb,uuid\)\s+TO postgres,service_role;/);

  return { source, output };
}

const { source, output } = await build();
if (process.argv.includes('--check')) {
  const current = await readFile(outputPath, 'utf8');
  assert.equal(current, output, 'generated repeatable is stale');
  console.log(JSON.stringify({
    ok: true,
    source_sha256: sha256(source),
    output_sha256: sha256(output),
    output_path: path.relative(root, outputPath).replaceAll('\\\\', '/')
  }));
} else {
  await writeFile(outputPath, output, 'utf8');
  console.log(JSON.stringify({
    ok: true,
    source_sha256: sha256(source),
    output_sha256: sha256(output),
    output_path: path.relative(root, outputPath).replaceAll('\\\\', '/')
  }));
}
