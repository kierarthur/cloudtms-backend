import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const manifestUrls = [
  '../supabase/verification/banking_pay_revision5_catalog_manifest.json',
  '../supabase/verification/banking_pay_workbench_certified_source_preview_catalog_manifest.json',
  '../supabase/verification/banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json',
  '../supabase/verification/banking_pay_semantic_ready_cancellation_reversion_catalog_manifest.json',
].map((relativePath) => new URL(relativePath, import.meta.url));

const browserGrantees = new Set(['PUBLIC', 'anon', 'authenticated']);
const serviceRoleBoundaryNames = new Set([
  '_pay_active_settled_components',
  '_pay_resolve_payment_scope_for_cancel_rewind',
  'banking_pay_operation_finish',
  'pay_batch_build_item_breakdowns',
  'pay_batch_create_timesheet_snapshots',
  'pay_batch_finalize_reservations_and_markers',
  'pay_timesheet_summary_pay_state_refresh_trigger',
  'pay_workbench_complete_job',
  'pay_workbench_dirty_apply_jobs_chunk',
  'pay_workbench_enqueue_stage_continuation',
  'pay_workbench_patch_preview_after_batch_mutation',
  'pay_workbench_prepare_draft_allocation_rows_seed',
  'pay_workbench_session_clear_case_resolution',
  'pay_workbench_session_clone_eligibility_v1',
  'pay_workbench_session_recompute_progress_counters',
  'pay_workbench_session_set_selected_rows',
]);

function catalogEntries(manifest) {
  return Object.values(manifest)
    .filter(Array.isArray)
    .flat()
    .filter((entry) => (
      entry
      && typeof entry === 'object'
      && typeof entry.schema === 'string'
      && typeof entry.name === 'string'
      && typeof entry.identity_arguments === 'string'
    ));
}

test('Banking Pay catalogue manifests preserve the general browser-isolation boundary', () => {
  let checked = 0;
  const checkedServiceRoleNames = new Set();

  for (const manifestUrl of manifestUrls) {
    const manifest = JSON.parse(readFileSync(manifestUrl, 'utf8'));

    for (const entry of catalogEntries(manifest)) {
      if (
        entry.schema !== 'public'
        || entry.security_definer !== true
        || entry.name.toLowerCase().includes('candidate')
        || entry.name === 'cloudtms_data_api_mfa_gate'
      ) {
        continue;
      }

      const grantees = new Set((entry.expanded_acl ?? []).map((acl) => acl.grantee));
      const exposed = [...grantees].filter((grantee) => browserGrantees.has(grantee));
      const identity = `${entry.schema}.${entry.name}(${entry.identity_arguments})`;

      assert.deepEqual(exposed, [], `${identity} must not retain browser EXECUTE authority`);
      assert.equal(grantees.has('postgres'), true, `${identity} must retain postgres EXECUTE authority`);
      if (serviceRoleBoundaryNames.has(entry.name)) {
        assert.equal(grantees.has('service_role'), true, `${identity} must retain service_role EXECUTE authority`);
        checkedServiceRoleNames.add(entry.name);
      }
      checked += 1;
    }
  }

  assert.ok(checked > 0, 'the Banking Pay catalogue boundary test must inspect functions');
  assert.deepEqual(
    [...checkedServiceRoleNames].sort(),
    [...serviceRoleBoundaryNames].sort(),
    'all 16 browser-isolated Banking Pay catalogue functions must remain covered'
  );
});
