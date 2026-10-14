import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const here = path.dirname(fileURLToPath(import.meta.url));
const current = 'c432594d9203421f009d3c7f21780f8c7df4f24e';
const sourceRoot = process.argv[2];
if (!sourceRoot) throw new Error('Exact current I2 source checkout required');
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
const git = file => execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show', `${current}:${file}`], { cwd: sourceRoot, maxBuffer: 25000000 });
const indexPath = 'broker/src/index.js', before = git(indexPath);
if (!fs.readFileSync(path.join(sourceRoot, indexPath)).equals(before)) throw new Error('Current saved index differs from exact publication');
let after = before.toString('utf8'); const hunks = [];
function replaceUnique(original, replacement) {
  if (after.split(original).length !== 2) throw new Error('Non-unique current owner hunk');
  after = after.replace(original, replacement); hunks.push({ original, replacement });
}
replaceUnique("import { createTimesheetCorrectionOrchestrator } from './timesheet-correction-orchestrator.mjs';",
  "import { createTimesheetCorrectionOrchestrator } from './timesheet-correction-orchestrator.mjs';\n" +
  "import { assertTimesheetCorrectionOfficeActor, createTimesheetCorrectionLifecycleResolver, handleTimesheetCorrectionOfficeAction, matchTimesheetCorrectionOfficeRoute } from './timesheet-correction-office-routes.mjs';");
const marker = "// Root's guarded route mounting supplies its existing qualified Office actor,\n// RPC, request-read, cleanup and ordinary lifecycle dependencies to this factory.\n// They remain exactly the original adapter dependencies; no new route/auth is\n// mounted by this source proposal.\n\nasync function runTsfinWorkerOnce(env, { limit = 50, onlyTimesheetIds = null, forceUnprocessedTimesheetIds = null } = {}) {";
const wiring = `// This finite Office consumer binds existing native owners. Feature/certificate,
// actual actor, CAS and isolation admission remain inside those native owners.
function createTimesheetCorrectionOfficeOrchestrator(env, req, ctx, admittedActor) {
  const nativeNames = new Set(['timesheet_correction_send_v1', 'timesheet_correction_prepare_v1',
    'timesheet_correction_authorise_atomic_v1', 'timesheet_correction_cancel_atomic_v1',
    'timesheet_correction_return_to_draft_v1']);
  const error = (code, status = 409) => { const e = new Error(code); e.code = code; e.status = status; throw e; };
  let orchestrator;
  const resolveLifecycleItem = createTimesheetCorrectionLifecycleResolver({
    readContexts: timesheetIds => rpcTsfinLoadContextBatch(env, { timesheetIds }),
    readProjection: (timesheetId, candidateId) => sbRpc(env, 'timesheet_correction_request_projection_v1', {
      p_timesheet_id: timesheetId, p_candidate_id: candidateId
    }),
    readPrepare: payload => orchestrator.prepare(payload, req),
    readTransition: (timesheetId, action, actor) => sbRpc(env, 'timesheet_correction_pair_transition_v1', {
      p_timesheet_id: timesheetId, p_action: action, p_actor_user_id: actor.actor_user_id,
      p_operation_id: null, p_expected_chain_fingerprint: null, p_lock_rows: false, p_max_members: 100
    })
  });
  orchestrator = createTimesheetCorrectionNativeOrchestrator(env, {
    assertActor: async (context, action) => {
      if (context !== req || !['SEND', 'PREPARE', 'AUTHORISE', 'CANCEL', 'RETURN', 'UNAUTHORISE'].includes(action))
        error('CORRECTION_ACTOR_NOT_ALLOWED', 403);
      const actor = assertTimesheetCorrectionOfficeActor(env, await requireUser(env, req, ['admin']));
      if (actor.actor_user_id !== admittedActor.actor_user_id) error('CORRECTION_ACTOR_NOT_ALLOWED', 403);
      return actor;
    },
    rpc: (name, args) => {
      if (!nativeNames.has(name)) error('CORRECTION_NATIVE_DEPENDENCY_UNAVAILABLE', 503);
      return sbRpc(env, name, args, { timeoutMs: 20000 });
    },
    readRequestAuthority: ({ requestId, actor }) => sbRpc(env, 'timesheet_correction_request_authority_v1', {
      p_request_id: requestId, p_actor_user_id: actor.actor_user_id
    }),
    deleteCommittedTimesheetR2Keys: (result, options) => deleteCommittedTimesheetR2Keys(env, result, options),
    resolveLifecycleItem,
    runOrdinaryLifecycle: async ({ items, action, actor }) => {
      if (!['AUTHORISE', 'UNAUTHORISE'].includes(action)) error('CORRECTION_PAYLOAD_INVALID', 400);
      if (action === 'UNAUTHORISE') {
        // Identical current Source/NEXT preflight, including native expanded whole members.
        const scope = await weeklySourceUnauthorisePairPreflight({
          rpc: (name, args, options) => sbRpc(env, name, args, options), ownerStatusRpc: bpayNextOwnerStatusRpc(env),
          actorUserId: actor.actor_user_id, readPhysicalTimesheet: id => sbGetOne(env,
            env.SUPABASE_URL + '/rest/v1/timesheets?timesheet_id=eq.' + encodeURIComponent(id) + '&select=timesheet_id,sheet_scope,contract_id'),
          timesheetIds: [...new Set(items.map(item => item.timesheet_id || item.timesheetId || item.current_timesheet_id ||
            item.currentTimesheetId || item.requested_timesheet_id || item.requestedTimesheetId || item.row_key?.slice(10)))]
        });
        if (!scope.ordinary) error(scope.body?.code || scope.body?.error_code || 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED', scope.status || 409);
      }
      const result = await sbRpc(env, action === 'AUTHORISE' ? 'timesheet_authorise_bulk_atomic' : 'timesheet_unauthorise_bulk_atomic', {
        p_items: items, p_actor_user_id: actor.actor_user_id, p_now_utc: nowIso()
      }, { timeoutMs: 20000 });
      requestTimesheetLifecycleWorkbenchWake(env, ctx, { action, sourceRoute: 'TIMESHEET_CORRECTION_NATIVE_LIFECYCLE',
        lifecycleChangedCount: timesheetLifecycleMutationChangedCount(result), actorUserId: actor.actor_user_id });
      return result;
    }
  });
  return orchestrator;
}

function timesheetCorrectionOfficeFailure(error) {
  const values = [error?.json?.error_code, error?.json?.message, error?.error_code, error?.code, error?.message];
  const code = values.find(value => typeof value === 'string' && /^(?:CORRECTION_|WEEKLY_SOURCE_|BPAY_NEXT_|CANDIDATE_CONTEXT_)[A-Z0-9_]+$/.test(value))
    || 'CORRECTION_NATIVE_DEPENDENCY_UNAVAILABLE';
  const sqlstate = error?.json?.code || error?.code;
  const explicitStatus = Number(error?.status);
  const status = code === 'CORRECTION_NATIVE_DEPENDENCY_UNAVAILABLE' ? 503
    : Number.isInteger(explicitStatus) && explicitStatus >= 400 && explicitStatus <= 599 ? explicitStatus
    : sqlstate === '42501' ? 403 : sqlstate === '22023' ? 400 : sqlstate === 'P0002' ? 404 : 409;
  return new Response(JSON.stringify({ ok: false, error_code: code }), {
    status, headers: { ...JSON_HEADERS, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }
  });
}

async function handleTimesheetCorrectionOfficeRoute(env, req, action, ctx) {
  try {
    const result = await handleTimesheetCorrectionOfficeAction(env, req, action, {
      requireUser,
      createOrchestrator: ({ actor }) => createTimesheetCorrectionOfficeOrchestrator(env, req, ctx, actor),
      afterAuthorise: (receipt, actor) => {
        const receipts = Array.isArray(receipt.results) ? receipt.results.filter(result => result.kind === 'CORRECTION_REQUEST' &&
          result.success === true).map(result => result.receipt) : [receipt];
        for (const applied of receipts) {
          if (applied?.state !== 'APPLIED' || applied.replayed === true) continue;
          // The orchestrator already requires the exact native singleton P0 list.
          requestTimesheetLifecycleWorkbenchWake(env, ctx, { action: 'AUTHORISE',
            sourceRoute: 'TIMESHEET_CORRECTION_NATIVE_APPROVAL', lifecycleChangedCount: 1,
            actorUserId: actor.actor_user_id, timesheetId: applied.ordinary_notification_timesheet_ids[0] });
        }
      }
    });
    return withCORS(env, req, new Response(JSON.stringify(result.body), {
      status: result.status, headers: { ...JSON_HEADERS, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }
    }));
  } catch (error) { return withCORS(env, req, timesheetCorrectionOfficeFailure(error)); }
}

async function runTsfinWorkerOnce(env, { limit = 50, onlyTimesheetIds = null, forceUnprocessedTimesheetIds = null } = {}) {`;
replaceUnique(marker, wiring);
replaceUnique("if (req.method === 'POST' && p === '/api/timesheets/bulk-authorise-selected') {",
  "const correctionOfficeAction = matchTimesheetCorrectionOfficeRoute(req.method, p);\n" +
  "if (correctionOfficeAction) return handleTimesheetCorrectionOfficeRoute(env, req, correctionOfficeAction, ctx);\n\n" +
  "if (req.method === 'POST' && p === '/api/timesheets/bulk-authorise-selected') {");
// Preserve every untouched byte, including current Bank/PP and previous Report/Money hunks.
for (const hunk of [...hunks].reverse()) {
  if (after.split(hunk.replacement).length !== 2) throw new Error('Non-unique reverse owner hunk');
}
let reverse = after;
for (const hunk of [...hunks].reverse()) reverse = reverse.replace(hunk.replacement, hunk.original);
if (!Buffer.from(reverse).equals(before)) throw new Error('Whole current index reverse mismatch');
const forward = path.join(here, 'forward', indexPath); fs.mkdirSync(path.dirname(forward), { recursive: true }); fs.writeFileSync(forward, after);
const modules = ['timesheet-correction-orchestrator.mjs','native-correction-snapshot-producers.mjs','native-daily-role-helpers.mjs',
  'native-weekly-correction-dispatch.mjs','exact-request-a0-weekly-clock-arm.mjs'];
const unchanged = modules.map(name => {
  const file = `broker/src/${name}`, bytes = git(file);
  if (!fs.readFileSync(path.join(sourceRoot,file)).equals(bytes)) throw new Error(`Current saved module changed: ${file}`);
  const destination = path.join(here,'reference',file); fs.mkdirSync(path.dirname(destination),{recursive:true}); fs.writeFileSync(destination,bytes);
  return { path:file, sha256:sha(bytes), bytes:bytes.length, action:'PRESERVE_CURRENT_SOURCE' };
});
const fixtureOwner = 'docs/healthroster-build/proposals/a0-current-root-install2-forward/original-test-reference/tests/healthroster-cn-a0-callbacks.test.mjs';
const fixtureBytes = git(fixtureOwner), fixtureSource = fixtureBytes.toString('utf8');
const fixtureStart = fixtureSource.indexOf('const id = n =>'), fixtureEnd = fixtureSource.indexOf('function harness(options = {})');
if (fixtureStart < 0 || fixtureEnd < fixtureStart) throw new Error('Exact current synthetic fixture source absent');
const fixtures = fixtureSource.slice(fixtureStart,fixtureEnd);
fs.writeFileSync(path.join(here,'unit-fixtures.mjs'), '// UNIT_ONLY synthetic owner-shaped frames, never native evidence or business rows.\n'+fixtures+
  '\nexport { id, I, H, FH, copy, action, finance, prepareFixture, replacementFixture, appliedFixture, cancelledFixture };\n');
fs.writeFileSync(path.join(here,'CURRENT_INDEX_FULL_REVERSE.json'), JSON.stringify({schema_version:'A0_OFFICE_CURRENT_RUNTIME_REVERSE_V1',
  source_only:true,input_commit:current,owner:indexPath,before_sha256:sha(before),before_bytes:before.length,
  after_sha256:sha(Buffer.from(after)),after_bytes:Buffer.byteLength(after),hunks,unchanged_modules:unchanged,
  unit_fixture_original:{path:fixtureOwner,sha256:sha(fixtureBytes),classification:'UNIT_ONLY_SYNTHETIC_TEST_SOURCE',
    change:'Exact synthetic helper extraction; no identity, money or semantic change.'}},null,2)+'\n');
console.log(JSON.stringify({input_commit:current,index_sha256:sha(Buffer.from(after)),hunks:hunks.length,unchanged_modules:unchanged.length,native_calls:0}));
