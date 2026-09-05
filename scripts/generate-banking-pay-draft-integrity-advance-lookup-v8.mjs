import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '03092026_1620_banking_pay_draft_integrity_row_backed_v8.sql'
);
const outputPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1530_banking_pay_draft_integrity_advance_lookup_v8.sql'
);
const checkOnly = process.argv.includes('--check');
const source = fs.readFileSync(sourcePath, 'utf8');
const publicMarker = 'CREATE OR REPLACE FUNCTION public.pay_batch_assert_integrity';
const publicStart = source.indexOf(publicMarker);
if (publicStart < 0 || source.indexOf(publicMarker, publicStart + publicMarker.length) >= 0) {
  throw new Error('expected exactly one public integrity owner');
}

let publicOwner = source.slice(publicStart).trimEnd();
const advancedStartMarker = '    WITH selected_advanced_lines AS (';
const advancedEndMarker = '    ), expected_advanced_overrides AS (';
const advancedStart = publicOwner.indexOf(advancedStartMarker);
const advancedEnd = publicOwner.indexOf(advancedEndMarker, advancedStart);
if (advancedStart < 0 || advancedEnd < 0) {
  throw new Error('advance-consumption assertion boundary changed');
}
const advancedBlock = publicOwner.slice(advancedStart, advancedEnd);
const oldCall = 'private.pay_workbench_operation_selected_lines_v8(p_operation_id, scope_row.id) AS line_element(value)';
if (advancedBlock.split(oldCall).length - 1 !== 1) {
  throw new Error('advance selected-line reader boundary changed');
}
const updatedAdvancedBlock = advancedBlock.replace(
  oldCall,
  'private.pay_workbench_operation_selected_advanced_lines_v8(p_operation_id, scope_row.id) AS line_element(value)'
);
publicOwner = `${publicOwner.slice(0, advancedStart)}${updatedAdvancedBlock}${publicOwner.slice(advancedEnd)}`;

const helper = `-- Bounded advance-override lookup for the established Draft integrity owner.
-- This is an orchestration/read-path correction only. It preserves the exact
-- ADVANCE_THIS_PAYMENT predicate and every existing mismatch outcome.

CREATE OR REPLACE FUNCTION private.pay_workbench_operation_selected_advanced_lines_v8(
  p_operation_id uuid,
  p_candidate_scope_id uuid
)
RETURNS TABLE(value jsonb)
LANGUAGE plpgsql
SECURITY INVOKER
STABLE
PARALLEL UNSAFE
SET search_path TO ''
AS $function$
DECLARE
  v_is_v8_operation boolean := false;
BEGIN
  IF p_operation_id IS NULL OR p_candidate_scope_id IS NULL THEN
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
    WHERE certificate_scope.operation_id = p_operation_id
      AND certificate_scope.freeze_state = 'FROZEN'
  )
  INTO v_is_v8_operation;

  IF v_is_v8_operation THEN
    RETURN QUERY
    SELECT payload.payload_json
    FROM public.banking_pay_operation_candidate_scope AS public_scope
    JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
      ON frozen_scope.operation_id = public_scope.operation_id
     AND frozen_scope.candidate_id = public_scope.candidate_id
     AND frozen_scope.resolved_pay_channel = public_scope.pay_channel
     AND frozen_scope.scope_digest_sha256 = public_scope.scope_hash
     AND frozen_scope.scope_state IN ('FROZEN', 'BATCH_LINKED', 'COMPLETE')
    JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
      ON payload.operation_id = frozen_scope.operation_id
     AND payload.candidate_id = frozen_scope.candidate_id
     AND payload.resolved_pay_channel = frozen_scope.resolved_pay_channel
    JOIN private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
      ON member.operation_id = frozen_scope.operation_id
     AND member.candidate_scope_ordinal = frozen_scope.candidate_scope_ordinal
     AND member.constituent_ordinal = payload.constituent_ordinal
    WHERE public_scope.operation_id = p_operation_id
      AND public_scope.id = p_candidate_scope_id
      AND (
        LOWER(COALESCE(payload.payload_json->>'is_advanced', 'false')) IN ('true', 't', '1', 'yes', 'y')
        OR NULLIF(BTRIM(COALESCE(payload.payload_json->>'advanced_override_id', '')), '') IS NOT NULL
      )
    ORDER BY member.constituent_ordinal;
    RETURN;
  END IF;

  RETURN QUERY
  SELECT selected_line.value
  FROM public.banking_pay_operation_candidate_scope AS legacy_scope
  CROSS JOIN LATERAL pg_catalog.jsonb_array_elements(
    CASE
      WHEN pg_catalog.jsonb_typeof(legacy_scope.selected_canonical_preview_lines_json) = 'array'
        THEN legacy_scope.selected_canonical_preview_lines_json
      ELSE '[]'::jsonb
    END
  ) AS selected_line(value)
  WHERE legacy_scope.operation_id = p_operation_id
    AND legacy_scope.id = p_candidate_scope_id
    AND (
      LOWER(COALESCE(selected_line.value->>'is_advanced', 'false')) IN ('true', 't', '1', 'yes', 'y')
      OR NULLIF(BTRIM(COALESCE(selected_line.value->>'advanced_override_id', '')), '') IS NOT NULL
    );
END;
$function$;

ALTER FUNCTION private.pay_workbench_operation_selected_advanced_lines_v8(uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_operation_selected_advanced_lines_v8(uuid,uuid)
  FROM PUBLIC, anon, authenticated, service_role;
`;

const metadata = `

ALTER FUNCTION public.pay_batch_assert_integrity(uuid,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_assert_integrity(uuid,uuid,uuid)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_batch_assert_integrity(uuid,uuid,uuid)
  TO service_role;
`;

const output = `-- Bounded advance lookup for the unchanged Draft integrity policy.\n-- Runtime authority is Miget TEST. The \`supabase\` directory name is historical.\n\n${helper}\n${publicOwner}${metadata}`;
if ((output.match(/^CREATE OR REPLACE FUNCTION /gm) || []).length !== 2) {
  throw new Error('generated owner must contain exactly the private lookup and public integrity function');
}
if (checkOnly) {
  if (!fs.existsSync(outputPath) || fs.readFileSync(outputPath, 'utf8') !== output) {
    throw new Error('generated replacement differs from the checked owner');
  }
  process.stdout.write('Draft integrity advance lookup: CHECK PASS\n');
} else {
  fs.writeFileSync(outputPath, output);
  process.stdout.write(`${path.relative(root, outputPath)}\n`);
}
