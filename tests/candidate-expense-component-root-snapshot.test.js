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
const finalWorkflowAuthority = readFileSync(
  new URL(
    '../supabase/repeatable/04092026_1952_candidate_expense_history_anchor_recovery_v1.sql',
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

test('category removal clears a root-level canonical later-expense snapshot', () => {
  const removal = source.match(
    /create or replace function private\._candidate_expense_submission_without_category_v1\([\s\S]*?\n\$function\$;/
  )?.[0] || '';
  const submit = source.match(
    /create or replace function public\.candidate_expense_update_submit_atomic_v1\([\s\S]*?\n\$function\$;/
  )?.[0] || '';

  assert.match(
    removal,
    /v_result->'canonical_tsfin_snapshot'[\s\S]*jsonb_set\([\s\S]*v_result,'\{canonical_tsfin_snapshot\}',v_snapshot,true/
  );
  assert.match(
    submit,
    /update_kind' in \('REMOVE_CATEGORY','OFFICE_REJECT_CATEGORY'\)[\s\S]*_candidate_expense_submission_without_category_v1/
  );
  assert.match(
    submit,
    /v_response:=public\.candidate_workflow_transition_atomic_v1\([\s\S]*jsonb_set\([\s\S]*p_payload[\s\S]*'\{immutable_submission\}',v_new_submission,true/
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

test('final workflow authority preserves the protected pending-expense update context', () => {
  assert.match(
    finalWorkflowAuthority,
    /v_expense_update_context jsonb;[\s\S]*v_is_pending_expense_update boolean:=false;/
  );
  assert.match(
    finalWorkflowAuthority,
    /current_setting\([\s\S]*'cloudtms\.candidate_expense_update_submit_context'[\s\S]*v_is_pending_expense_update:=true;/
  );
  assert.match(
    finalWorkflowAuthority,
    /if not v_is_pending_expense_update and \([\s\S]*CANDIDATE_DUPLICATE_EXPENSE_CONFIRMATION_REQUIRED/
  );
  assert.match(
    finalWorkflowAuthority,
    /v_is_pending_expense_update[\s\S]*v_pending_expense_update\.from_workflow_generation[\s\S]*CANDIDATE_SIGNATURE_REQUIRED_AFTER_AMENDMENT/
  );
  assert.match(
    finalWorkflowAuthority,
    /when v_is_pending_expense_update then nullif\([\s\S]*prior_workflow_snapshot_json->>'candidate_signed_at_utc'/
  );
});
