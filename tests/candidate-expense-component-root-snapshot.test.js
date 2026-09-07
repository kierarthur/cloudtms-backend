import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(
  new URL(
    '../supabase/repeatable/06092026_1636_candidate_advanced_expense_component_policy_v1.sql',
    import.meta.url
  ),
  'utf8'
);

test('expense component values accept combined and later expense-only snapshots', () => {
  const componentValues = source.match(
    /create or replace function private\._candidate_expense_component_values_v1\([\s\S]*?\n\$function\$;/
  )?.[0] || '';

  assert.match(
    componentValues,
    /v_submission#>'\{expense_submission,canonical_tsfin_snapshot\}'/
  );
  assert.match(
    componentValues,
    /v_submission#>'\{expense_claim,canonical_tsfin_snapshot\}'/
  );
  assert.match(componentValues, /v_submission->'canonical_tsfin_snapshot'/);
  assert.ok(
    componentValues.indexOf("v_submission->'canonical_tsfin_snapshot'") <
      componentValues.indexOf("'{}'::jsonb"),
    'the valid later-expense snapshot must be considered before the empty fallback'
  );
});

test('release reconciliation is limited to mismatched live later-expense components', () => {
  const reconciliation = source.match(
    /do \$reconcile_later_expense_components\$[\s\S]*?\$reconcile_later_expense_components\$;/
  )?.[0] || '';

  assert.match(reconciliation, /workflow\.workflow_kind='CONTRACT_EXPENSE'/);
  assert.match(
    reconciliation,
    /workflow\.immutable_submission_json->'canonical_tsfin_snapshot'/
  );
  assert.match(reconciliation, /workflow\.immutable_submission_json->'expense_submission' is null/);
  assert.match(reconciliation, /workflow\.immutable_submission_json->'expense_claim' is null/);
  assert.match(
    reconciliation,
    /component\.lifecycle_state not in \([\s\S]*?'MANAGER_REFUSED'[\s\S]*?'SUPERSEDED'/
  );
  assert.match(reconciliation, /component\.amount is distinct from value\.amount/);
  assert.match(reconciliation, /component\.mileage_units is distinct from value\.mileage_units/);
  assert.match(reconciliation, /_candidate_expense_components_sync_v1/);
});

