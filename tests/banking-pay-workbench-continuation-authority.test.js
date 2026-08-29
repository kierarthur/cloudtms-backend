import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const canonical = read('supabase/repeatable/04082026_1219_pay_workbench_enqueue_stage_continuation.sql');
const lateAuthority = read('supabase/repeatable/08082026_0245_pay_workbench_enqueue_stage_continuation_authority.sql');
const repair = read('supabase/migrations/08082026_0246_repair_workbench_source_build_continuation_linkage.sql');

test('late authority reasserts the focused continuation owner after historical omnibus files', () => {
  assert.match(lateAuthority, /\\ir 04082026_1219_pay_workbench_enqueue_stage_continuation\.sql/);
  assert.match(canonical, /v_economic_build_id:=v_source_job_row\.economic_build_id/);
  assert.match(canonical, /PAY_WORKBENCH_CONTINUATION_CURSOR_STAGE_MISMATCH/);
  assert.match(canonical, /PAY_WORKBENCH_CONTINUATION_BUILD_STALE/);
  assert.match(canonical, /economic_build_id,[\s\S]*private_stage,[\s\S]*private_cursor_kind,[\s\S]*private_cursor_json/);
});

test('repair is limited to unattempted malformed continuations and proves exact live authority', () => {
  assert.match(repair, /v_target_count > 20/);
  assert.match(repair, /job\.attempt_count = 0/);
  assert.match(repair, /source_job\.status = 'SUCCEEDED'/);
  assert.match(repair, /build\.id = registry\.current_build_id/);
  assert.match(repair, /build\.captured_candidate_generation = registry\.dirty_generation/);
  assert.match(repair, /build\.source_change_seq = registry\.current_source_change_seq/);
  assert.match(repair, /PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_PROOF_FAILED/);
  assert.match(repair, /PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_UPDATE_MISMATCH/);
  assert.match(repair, /PAY_WORKBENCH_CONTINUATION_LINKAGE_REPAIR_INCOMPLETE/);
  assert.doesNotMatch(repair, /UPDATE\s+private\.banking_pay_workbench_economic_builds/i);
  assert.doesNotMatch(repair, /DELETE\s+FROM/i);
});
