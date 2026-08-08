const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const sourcePath = path.join(
  root,
  'supabase',
  'repeatable',
  '08082026_0717_pay_workbench_prepare_draft_allocation_rows_seed_sort_order.sql'
);
const legacyPath = path.join(
  root,
  'supabase',
  'repeatable',
  '26052026_2100HRS_NEW_FUNCTIONS.sql'
);

const source = fs.readFileSync(sourcePath, 'utf8').replace(/\r\n/g, '\n');
const legacy = fs.readFileSync(legacyPath, 'utf8').replace(/\r\n/g, '\n');

function extractLegacyFunction() {
  const startMarker =
    'CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed';
  const start = legacy.indexOf(startMarker);
  assert.notEqual(start, -1, 'legacy allocation seeder must exist');

  const endMarker = '\n$function$;';
  const end = legacy.indexOf(endMarker, start);
  assert.notEqual(end, -1, 'legacy allocation seeder must have a complete body');
  return legacy.slice(start, end + endMarker.length).trim();
}

test('allocation seeder preserves its exact public signature and object contract', () => {
  assert.match(
    source,
    /^CREATE OR REPLACE FUNCTION public\.pay_workbench_prepare_draft_allocation_rows_seed\(p_operation_id uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb\)/
  );
  assert.match(source, /RETURNS TABLE\(candidate_scopes_processed integer, allocation_rows_inserted integer, allocation_rows_reused integer, failures integer\)/);
  assert.match(source, /LANGUAGE plpgsql\s+SECURITY DEFINER\s+SET search_path TO 'public', 'pg_temp'/);
});

test('durable sort order is deterministic and candidate-local', () => {
  assert.match(
    source,
    /row_number\(\) OVER \(\s*PARTITION BY\s*allocation_expanded_rows\.operation_id,\s*allocation_expanded_rows\.candidate_scope_id\s*ORDER BY\s*allocation_expanded_rows\.row_ordinal,\s*allocation_expanded_rows\.preview_row_id,\s*allocation_expanded_rows\.operation_source_key\s*\)::integer AS sort_order/
  );
  assert.doesNotMatch(
    source,
    /allocation_expanded_rows\.row_ordinal AS sort_order/
  );
});

test('component-aware bigint lineage remains intact and no schema or economic rule changes', () => {
  assert.match(
    source,
    /\(finance_component_source_rows\.row_ordinal \* 1000 \+ finance_component_source_rows\.finance_component_ordinality\)::bigint AS row_ordinal/
  );
  assert.match(
    source,
    /'row_ordinal', allocation_expanded_rows\.row_ordinal/
  );
});

test('replacement differs from the saved authority only at the final sort-order expression', () => {
  const oldExpression =
    '    allocation_expanded_rows.row_ordinal AS sort_order';
  const newExpression = [
    '    row_number() OVER (',
    '      PARTITION BY',
    '        allocation_expanded_rows.operation_id,',
    '        allocation_expanded_rows.candidate_scope_id',
    '      ORDER BY',
    '        allocation_expanded_rows.row_ordinal,',
    '        allocation_expanded_rows.preview_row_id,',
    '        allocation_expanded_rows.operation_source_key',
    '    )::integer AS sort_order'
  ].join('\n');

  const legacyFunction = extractLegacyFunction();
  assert.equal(legacyFunction.split(oldExpression).length - 1, 1);
  assert.equal(
    source.trim(),
    legacyFunction.replace(oldExpression, newExpression)
  );
});

test('the reproduced Kier ordinal overflows int4 before the targeted correction', () => {
  const publicRowOrdinal = 4_000_029;
  const expandedOrdinal = publicRowOrdinal * 1_000 + 1;
  assert.ok(expandedOrdinal > 2_147_483_647);

  const candidateLocalAllocationRows = 9;
  assert.ok(candidateLocalAllocationRows < 2_147_483_647);
});
