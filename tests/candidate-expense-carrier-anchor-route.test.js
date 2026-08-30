import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const repeatablePath = 'supabase/repeatable/30082026_1903_candidate_expense_carrier_anchor_route_v1.sql';
const verifierPath = 'supabase/verification/30082026_1910_candidate_expense_carrier_anchor_route_verification.sql';
const repeatable = read(repeatablePath);
const verifier = read(verifierPath);
const release = JSON.parse(read('supabase/release/current-release.json'));
const runtime = read('.github/workflows/candidate-db-runtime.yml');

test('expense workflow creation derives approval route from the worked anchor before carrier admission', () => {
  const anchorResolution = repeatable.indexOf("if nullif(v_payload->>'anchor_timesheet_id','') is not null then");
  const routeResolution = repeatable.indexOf('v_route_authority:=private._candidate_route_family_v1(', anchorResolution);
  const workflowInsert = repeatable.indexOf('insert into public.candidate_submission_workflows(', routeResolution);

  assert.ok(anchorResolution > 0, 'worked-anchor resolution must exist');
  assert.ok(routeResolution > anchorResolution, 'worked anchor must be resolved before route admission');
  assert.ok(workflowInsert > routeResolution, 'route admission must precede workflow creation');
  assert.match(
    repeatable,
    /case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week\.timesheet_id else v_week\.timesheet_id end[\s\S]*case when v_workflow_kind='CONTRACT_EXPENSE' then v_anchor_week\.id else v_week\.id end/i
  );
  assert.match(
    repeatable,
    /v_workflow_kind='CONTRACT_EXPENSE'[\s\S]*CANDIDATE_WORKFLOW_ANCHOR_NOT_WORKED[\s\S]*v_route_authority:=private\._candidate_route_family_v1/i
  );
  assert.match(
    repeatable,
    /v_workflow_kind='CONTRACT_EXPENSE'[\s\S]*route_family'='QR'[\s\S]*v_route:='PAPER'/i
  );
  assert.doesNotMatch(repeatable, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('rollback-contained proof reproduces the app carrier-first sequence and protects finance', () => {
  assert.match(verifier, /expense_carrier_resolve_or_create_atomic_v1[\s\S]*candidate_workflow_transition_atomic_v1/i);
  assert.match(verifier, /submission_mode_snapshot='MANUAL'[\s\S]*route','ELECTRONIC'/i);
  assert.match(verifier, /idempotent_replay[\s\S]*count\(\*\)[\s\S]*candidate_submission_workflows/i);
  assert.match(verifier, /v_after_timesheet is distinct from v_before_timesheet/i);
  assert.match(verifier, /v_after_financial is distinct from v_before_financial/i);
  assert.match(verifier, /qr_status,qr_token[\s\S]*'PENDING','carrier-qr-route-token'/i);
  assert.match(
    verifier,
    /creation_identity_json#>>'\{request,initial_route\}'='ELECTRONIC'[\s\S]*creation_identity_json#>>'\{derived,initial_route\}'='PAPER'/i
  );
  assert.match(verifier, /QR-backed expense replay created duplicate state/i);
  assert.match(verifier, /v_qr_after_financial is distinct from v_qr_before_financial/i);
  assert.match(verifier, /begin;[\s\S]*rollback;/i);
});

test('release and Candidate runtime install the successor before executing its first-use verifier', () => {
  assert.ok(release.verificationFiles.includes(verifierPath));
  assert.ok(release.newVerificationFiles.includes(verifierPath));
  assert.match(
    runtime,
    /29082026_0951_candidate_expense_resubmission_anchor_v1\.sql[\s\S]*30082026_1903_candidate_expense_carrier_anchor_route_v1\.sql[\s\S]*30082026_1910_candidate_expense_carrier_anchor_route_verification\.sql/i
  );
});
