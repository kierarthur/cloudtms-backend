import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';
import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const out=path.join(here,'business-entry-inline-construction');fs.mkdirSync(path.join(out,'before'),{recursive:true});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');const put=(p,s)=>fs.writeFileSync(path.join(out,p),s);
const reverses=[];
function compose(rel,name,hunks){const input=fs.readFileSync(path.join(repo,rel),'utf8');let forward=input;
 for(const h of hunks){assert.equal(forward.split(h.before).length,2,h.label);forward=forward.replace(h.before,()=>h.after);}
 let restored=forward;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2,h.label);restored=restored.replace(h.after,()=>h.before);}
 assert.equal(restored,input);put(name,forward);put('before/'+name+'.txt',input);
 reverses.push({input_path:rel,output:name,input_sha256:sha(input),forward_sha256:sha(forward),hunks});}
const index='docs/healthroster-build/proposals/agency-mode-a-source-support-issue-construction-20261014/construction/10_complete_worker_index_with_native_source_crosscheck.js';
const importBefore="import { decodeAgencyWorkerTargets, runAgencyLeasedCanonicalRecompute, decodeAgencyAuthoriseStepResult, runAgencyLeasedAuthorisedValidation, agencyNativeCausalCode } from './weekly-source/agency-canonical-worker.mjs';";
const dependencyBefore='        // WP-37 (WP-31 hostile review of WP-04, finding F2). The Mode A';
const dependencyAfter=`        // Primary26.5.1: one genuine click; calculator/native writer and
        // existing targeted lease owner, with fresh stored phase observations.
        orchestrateAgencyTimesheetChange: createAgencyTimesheetChangeRuntime({
          nativeCommand: args => sbRpcRecordingGuardRefusal(env, 'weekly_source_agency_timesheet_change_v1', args),
          runManual: requestId => runAgencyCanonicalManual({
            requestId,
            readContext: id => sbRpc(env, 'weekly_source_agency_change_worker_context_v1', { p_request_id: id }),
            buildWeeklySnapshot: (ts, cw, contract, curFin, options) =>
              buildWeeklyScheduleSegmentsSnapshot(env, ts, cw, contract, curFin, options),
            applySnapshot: args => sbRpcRecordingGuardRefusal(env, 'weekly_source_agency_change_worker_apply_v1', args)
          }),
          runWorkerOnce: timesheetId => runTsfinWorkerOnce(env, { limit: 1, onlyTimesheetIds: [timesheetId] })
        }),
`+dependencyBefore;
compose(index,'81_complete_worker_index_with_one_click_agency_continuation.js',[
 {label:'literal existing canonical MANUAL import',before:importBefore,after:importBefore.replace('runAgencyLeasedCanonicalRecompute,','runAgencyLeasedCanonicalRecompute, runAgencyCanonicalManual,')},
 {label:'bounded actual runtime import',before:"import { decodeAgencyReturnStep } from './weekly-source/agency-native-return.mjs';",after:"import { decodeAgencyReturnStep } from './weekly-source/agency-native-return.mjs';\nimport { createAgencyTimesheetChangeRuntime } from './weekly-source/agency-timesheet-change-runtime.mjs';"},
 {label:'actual factory dependencies and existing one-Timesheet worker',before:dependencyBefore,after:dependencyAfter}
]);
const route='docs/healthroster-build/proposals/agency-mode-a-preset-policy-source-successor-20261014/construction/25_complete_current_routes_with_native_mode_a_continuation.js';
const branchBefore="  if (action === 'LATER_CHANGE_DECISION') {";
const branchAfter=`  if (action === 'AGENCY_TIMESHEET_CHANGE') {
    if (body.action !== action || Object.keys(body).length !== 2 || !Object.hasOwn(body, 'payload')) {
      fail('AGENCY_CHANGE_BUSINESS_REQUEST_INVALID', 'This Timesheet action is invalid.', 400);
    }
    const deployment = weeklySourceDeploymentContext(env);
    if (deployment.environment !== 'TEST') {
      fail('AGENCY_CHANGE_TEST_SCOPE_REQUIRED', 'This Timesheet action is unavailable in this environment.', 403);
    }
    try {
      // Closed input has only the primary business fields. Actor/T0/D1/source,
      // financial snapshots, phase and qualification are server-owned.
      const args = agencyBusinessRequest(payload, user.id);
      if (typeof dependencies.orchestrateAgencyTimesheetChange !== 'function') {
        fail('AGENCY_CHANGE_INLINE_DEPENDENCY_INVALID', 'This Timesheet action is unavailable. Try again shortly.', 503);
      }
      const result = decodeAgencyInlineReply(await dependencies.orchestrateAgencyTimesheetChange({ request: payload, actor: user }), args);
      const native = result.result;
      // The actual mounted orchestrator strictly decodes this native result.
      // A queue acceptance/count never supplies the HTTP success predicate.
      return jsonResponse(native?.completed === true ? 200 : 409, {
        ok: native?.completed === true,
        code: result.runtime_refusal_code || native.code,
        result: native
      });
    } catch (error) {
      const code = typeof error?.code === 'string' && /^[A-Z][A-Z0-9_]{0,199}$/.test(error.code)
        ? error.code : typeof error?.json?.message === 'string' && /^[A-Z][A-Z0-9_]{0,199}$/.test(error.json.message)
          ? error.json.message : 'AGENCY_CHANGE_INLINE_NATIVE_DEPENDENCY_UNAVAILABLE';
      const status = code === 'AGENCY_CHANGE_BUSINESS_REQUEST_INVALID' ? 400
        : error?.json?.code === '42501' ? 403
          : error?.json?.code === 'PGRST202' || /DEPENDENCY|RESULT_INVALID|TARGET_CHANGED/.test(code) ? 503 : 409;
      fail(code, status === 400 ? 'This Timesheet action is invalid.'
        : status === 403 ? 'This Timesheet action is not available for this user.'
          : 'The Timesheet change has not been confirmed. Retry the same action after refreshing its status.', status);
    }
  }

`+branchBefore;
compose(route,'82_complete_current_routes_with_one_click_agency_command.js',[
 {label:'closed primary business input codec',before:"import {checklistReadRequest,decodeChecklistRead,ChecklistReadError} from './checklist-read.mjs';",after:"import {checklistReadRequest,decodeChecklistRead,ChecklistReadError} from './checklist-read.mjs';\nimport {agencyBusinessRequest,decodeAgencyInlineReply} from './agency-timesheet-change-runtime.mjs';"},
 {label:'same native scoped payment actor action',before:"  'ACCEPT_SYSTEM_HOURS', 'FIRST_AUTHORISE', 'WITHDRAW_FIRST_AUTHORISATION',",after:"  'ACCEPT_SYSTEM_HOURS', 'FIRST_AUTHORISE', 'WITHDRAW_FIRST_AUTHORISATION', 'AGENCY_TIMESHEET_CHANGE',"},
 {label:'actual command branch before legacy dispatch',before:branchBefore,after:branchAfter}
]);
compose('docs/healthroster-build/proposals/agency-native-whole-week-validation-runtime-forward-20261014/construction/69_complete_canonical_worker_with_authorised_validation.mjs',
 '83_complete_canonical_worker_with_manual_typed_validation.mjs',[
 {label:'actual MANUAL validation DTO marker',before:"['timesheet_id','status','reason_code','validated_at_utc','updated_at'],code);",after:"['timesheet_id','status','reason_code','validated_at_utc','updated_at','validation_source_kind'],code);"},
 {label:'closed native validation kind in MANUAL frame',before:"if(value.weekly_validation.timesheet_id!==ts||typeof value.weekly_validation.status!=='string')fail(code);",after:"if(value.weekly_validation.timesheet_id!==ts||typeof value.weekly_validation.status!=='string'\n      ||!['LEGACY','AGENCY_INVOICE_WEEK'].includes(value.weekly_validation.validation_source_kind))fail(code);"},
 {label:'same native marker to existing readiness calculator',before:"hr_validation_reason_code:frame.weekly_validation?.reason_code??null});",after:"hr_validation_reason_code:frame.weekly_validation?.reason_code??null,\n    hr_validation_source_kind:frame.weekly_validation?.validation_source_kind??null});"}
 ]);
compose('docs/healthroster-build/proposals/agency-native-paid-purpose-forward-20261014/construction/39_complete_worker_context_with_native_paid_purpose.sqlinc',
 '84_complete_native_worker_context_with_manual_typed_validation.sqlinc',[
 {label:'actual stored typed marker from native validation row',before:"'validated_at_utc',v.validated_at_utc,'updated_at',v.updated_at) into validation",after:"'validated_at_utc',v.validated_at_utc,'updated_at',v.updated_at,\n   'validation_source_kind',v.validation_source_kind) into validation"}
 ]);
const root=path.resolve(repo,'../healthroster-install2-14102026');
const actorPath='broker/src/payment-actor-permission.js',actor=fs.readFileSync(path.join(root,actorPath));
const committed=execFileSync('git',['-C',root,'show','fa66183f01913e86651e82816f5e88f97e07123c:'+actorPath]);
assert.ok(actor.equals(committed),'actual current payment actor saved/Git mismatch');put('payment-actor-permission.js',actor);
put('CURRENT_PAYMENT_ACTOR_PIN.json',JSON.stringify({commit:'fa66183f01913e86651e82816f5e88f97e07123c',path:actorPath,sha256:sha(actor),bytes:actor.length},null,2)+'\n');
put('BUSINESS_ENTRY_CURRENT_REVERSES.json',JSON.stringify({source_only:true,native:false,reverses},null,2)+'\n');
console.log(JSON.stringify({complete_owners:4,unique_full_reverse_hunks:10,native:false}));
