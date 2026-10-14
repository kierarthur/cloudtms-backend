import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const published=null,source='docs/healthroster-build/proposals/install2-a0-scoped-pay-permission-successor-20261014/forward/broker/src/index.js';
const before=fs.readFileSync(path.join(own,source));
if(sha(before)!=='41efc91deec656ee740b3c0a2b4a8765af74fcf832e05aad9c002c18554c7961')throw Error('Frozen scoped A0 input changed');
let after=before.toString();const hunks=[];function apply(original,replacement){if(after.split(original).length!==2)throw Error('Nonunique current-owner hunk');after=after.replace(original,replacement);hunks.push({original,replacement});}
const importLine="import { assertTimesheetCorrectionOfficeActor, createTimesheetCorrectionLifecycleResolver, handleTimesheetCorrectionOfficeAction, matchTimesheetCorrectionOfficeRoute } from './timesheet-correction-office-routes.mjs';";
apply(importLine,importLine+"\nimport { readTimesheetCorrectionOfficeContext } from './timesheet-correction-office-context.mjs';");
const marker='async function handleTimesheetCorrectionOfficeRoute(env, req, action, ctx) {';
const read=`async function handleTimesheetCorrectionOfficeContext(env, req, timesheetId) {
  try {
    const result = await readTimesheetCorrectionOfficeContext(env, req, timesheetId, {
      requireUser,
      assertScope: ({ timesheetId, user }) => assertTimesheetCorrectionSourceSubjects(env, req, user, [timesheetId]),
      readContexts: timesheetIds => rpcTsfinLoadContextBatch(env, { timesheetIds }),
      // Read-only native projections retain their real existing purpose/selector.
      // The total WP-14c mutation wrapper remains unchanged on mutation calls.
      readCreateProjection: ({ candidateId, timesheetId }) => sbRpc(env, 'timesheet_correction_create_projection_v1', {
        p_candidate_id: candidateId, p_contract_week_id: null, p_target_timesheet_id: timesheetId, p_anchor_timesheet_id: null
      }, { timeoutMs: 12000 }),
      readRequestProjection: ({ candidateId, timesheetId }) => sbRpc(env, 'timesheet_correction_request_projection_v1', {
        p_timesheet_id: timesheetId, p_candidate_id: candidateId
      }, { timeoutMs: 12000 }),
      readRequestAuthority: ({ requestId, actor }) => sbRpc(env, 'timesheet_correction_request_authority_v1', {
        p_request_id: requestId, p_actor_user_id: actor.actor_user_id
      }, { timeoutMs: 12000 })
    });
    return withCORS(env, req, new Response(JSON.stringify(result), {
      status: 200, headers: { ...JSON_HEADERS, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }
    }));
  } catch (error) { return withCORS(env, req, timesheetCorrectionOfficeFailure(error)); }
}

`+marker;
apply(marker,read);
const route='const correctionOfficeAction = matchTimesheetCorrectionOfficeRoute(req.method, p);';
apply(route,"const correctionOfficeContext = matchPath(p, '/api/timesheets/:id/corrections/context');\n"+
 "if (correctionOfficeContext && req.method === 'GET') return handleTimesheetCorrectionOfficeContext(env, req, correctionOfficeContext.id);\n\n"+route);
let restored=after;for(const h of [...hunks].reverse()){if(restored.split(h.replacement).length!==2)throw Error('Nonunique reverse');restored=restored.replace(h.replacement,h.original);}if(!Buffer.from(restored).equals(before))throw Error('Full reviewed A0 reverse mismatch');
const file=path.join(here,'forward/backend/broker/src/index.js');fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file,after);
fs.writeFileSync(path.join(here,'BACKEND_CONTEXT_FULL_REVERSE.json'),JSON.stringify({schema_version:'OFFICE_CORRECTION_CONTEXT_CURRENT_RUNTIME_REVERSE_V1',
 input_publication:published,input_freeze:'b6726cb2f897c0abba319b148535b255f1fd3c10684acf9d3d53342045608d14',input_artifact:source,input_sha256:sha(before),output_sha256:sha(after),hunks,
 existing_sql_signatures_changed:0,private_context_or_finance_serialization:false,no_native_calls:true},null,2)+'\n');
const currentRoute='docs/healthroster-build/proposals/install2-a0-scoped-pay-permission-successor-20261014/forward/broker/src/timesheet-correction-office-routes.mjs';
const routeBytes=fs.readFileSync(path.join(own,currentRoute));
const reference=path.join(here,'reference/backend/broker/src/timesheet-correction-office-routes.mjs');fs.mkdirSync(path.dirname(reference),{recursive:true});fs.writeFileSync(reference,routeBytes);
const contextSource=fs.readFileSync(path.join(here,'forward/backend/broker/src/timesheet-correction-office-context.mjs'),'utf8');
const contextRef=path.join(here,'reference/backend/broker/src/timesheet-correction-office-context.mjs');fs.writeFileSync(contextRef,contextSource);
fs.copyFileSync(path.join(own,'docs/healthroster-build/proposals/install2-a0-scoped-pay-permission-successor-20261014/forward/broker/src/payment-actor-permission.js'),path.join(here,'reference/backend/broker/src/payment-actor-permission.js'));
console.log(JSON.stringify({complete_index_sha256:sha(after),unique_hunks:hunks.length,new_sql_signatures:0}));
