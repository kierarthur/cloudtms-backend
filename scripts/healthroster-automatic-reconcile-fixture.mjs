import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { stageWeeklyProtectedAction } from '../broker/src/weekly-source/protected-action-orchestrator-stage-only.mjs';
import { projectWeeklyProtectedComponentIdentity } from '../broker/src/weekly-source/protected-component-identity.mjs';
import { HASH, browserRequest, dependenciesFor } from '../tests/fixtures/healthroster-protected-orchestrator-fixture.mjs';

export async function produceAutomaticReconcileStageFixture() {
  const dependencies = dependenciesFor('RECONCILE');
  const calculate = dependencies.calculateWeeklySnapshot;
  dependencies.calculateWeeklySnapshot = async (input) => {
    const result = await calculate(input);
    // Fixture calculator facts are synthetic. Apply the real identity owner to
    // exact output ordinals; no approved inventory or £0 certificate is made.
    result.snapshot.invoice_breakdown_json.segments = result.snapshot.invoice_breakdown_json.segments.map((segment, index) => ({
      ...segment, start: input.schedule[index].start, end: input.schedule[index].end,
      break_mins: input.schedule[index].break_mins,
      overnight: input.schedule[index].end <= input.schedule[index].start,
    }));
    return projectWeeklyProtectedComponentIdentity({ calculation: { ...result, ok: true },
      schedule: input.schedule, approvedComponents: [] });
  };
  const result = await stageWeeklyProtectedAction({ action: 'ACCEPT_SOURCE_AND_RECONCILE',
    request: browserRequest('ACCEPT_SOURCE_AND_RECONCILE'), dependencies,
    automaticReconcileSelectionSha256: HASH });
  return { fixture_contract: 'SYNTHETIC_AUTOMATIC_RECONCILE_TARGET_PRODUCER_V1',
    scope: 'Actual JS transport/identity producer with synthetic calculator and RPC responses; no SQL admission or financial authority',
    expected_sql_delta_assertion: 'REFUSE_UNAVAILABLE_UNTIL_ACTUAL_APPROVED_INVENTORY_AND_SEALED_GENERATION_ARE_QUALIFIED',
    stage_request: dependencies.staged(), stage_only_result: result };
}

if (process.argv.includes('--write')) {
  const fixture = await produceAutomaticReconcileStageFixture();
  writeFileSync(resolve(import.meta.dirname, '../tests/fixtures/healthroster-automatic-reconcile-produced-stage.json'), `${JSON.stringify(fixture, null, 2)}\n`);
  process.stdout.write('Synthetic actual-producer stage fixture saved; no native admission claimed.\n');
}
