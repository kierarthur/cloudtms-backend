const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const currentFunctionsPath = path.join(
  repeatableDir,
  '26052026_2100HRS_NEW_FUNCTIONS.sql'
);
const currentFunctionsSql = fs.readFileSync(currentFunctionsPath, 'utf8');

function functionBody(source, qualifiedName) {
  const startPattern = new RegExp(
    `CREATE OR REPLACE FUNCTION\\s+${qualifiedName.replace('.', '\\.')}\\s*\\(`,
    'i'
  );
  const startMatch = startPattern.exec(source);
  assert.ok(startMatch, `${qualifiedName} must exist`);
  const end = source.indexOf('$function$;', startMatch.index);
  assert.ok(end > startMatch.index, `${qualifiedName} must have a complete body`);
  return source.slice(startMatch.index, end + '$function$;'.length);
}

test('candidate preview preserves the existing bounded four-argument contract', () => {
  assert.match(
    currentFunctionsSql,
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_session_get_candidate_preview\(p_session_id uuid, p_candidate_id uuid, p_cursor_json jsonb DEFAULT '\{\}'::jsonb, p_limit integer DEFAULT 100\)/
  );
  assert.match(
    currentFunctionsSql,
    /DROP FUNCTION IF EXISTS public\.pay_workbench_session_get_candidate_preview\(uuid, uuid\);/
  );
  assert.match(
    currentFunctionsSql,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_get_candidate_preview\(uuid, uuid, jsonb, integer\)\s+FROM PUBLIC, anon, authenticated;/
  );
  assert.match(
    currentFunctionsSql,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_get_candidate_preview\(uuid, uuid, jsonb, integer\)\s+TO service_role;/
  );
});

test('candidate preview excludes rows reserved by an active draft or later payment stage', () => {
  const body = functionBody(
    currentFunctionsSql,
    'public.pay_workbench_session_get_candidate_preview'
  );

  assert.match(body, /preview_row\.status = 'READY'/);
  assert.match(body, /row_json->>'post_draft_unavailable'/);
  assert.match(body, /row_json->>'post_draft_overlay_applied'/);
  assert.match(body, /row_json->>'post_draft_overlay_operation_type'/);
  assert.match(body, /'DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE'/);
  assert.match(body, /row_json->>'post_draft_overlay_active'/);
  assert.match(body, /NOT IN \('false', 'f', '0', 'no', 'n', 'off'\)/);
});

test('only the current candidate-preview function body remains in repeatable sources', () => {
  const definitions = [];
  for (const entry of fs.readdirSync(repeatableDir, { withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const filePath = path.join(repeatableDir, entry.name);
    const source = fs.readFileSync(filePath, 'utf8');
    const matches = source.match(
      /CREATE OR REPLACE FUNCTION\s+public\.pay_workbench_session_get_candidate_preview\s*\(/gi
    );
    for (let index = 0; index < (matches || []).length; index += 1) {
      definitions.push(entry.name);
    }
  }

  assert.deepEqual(definitions, ['26052026_2100HRS_NEW_FUNCTIONS.sql']);
});

test('draft cancellation still restores preview eligibility in the batch overlay function', () => {
  const body = functionBody(
    currentFunctionsSql,
    'public.pay_workbench_patch_preview_after_batch_mutation'
  );

  assert.match(
    body,
    /v_is_cancel_delete := v_operation_type IN \('DRAFT_DELETE', 'DRAFT_CANCEL'\)/
  );
  assert.match(body, /'post_draft_unavailable', false/);
  assert.match(body, /'post_draft_overlay_active', false/);
  assert.match(body, /'post_draft_overlay_operation_type', v_operation_type/);
});
