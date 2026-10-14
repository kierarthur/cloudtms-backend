import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),root=path.resolve(own,'../healthroster-v2-backend-13102026'),sha=x=>createHash('sha256').update(x).digest('hex');
const original=fs.readFileSync(path.join(root,'broker/src/index.js'),'utf8');let value=original;const changes=[];
function edit(before,after){assert.equal(value.split(before).length-1,1,before);value=value.replace(before,()=>after);changes.push({before,after});}
edit("import { createWeeklySourceUploadPublicationOwner } from './weekly-source/upload-publication-owner.mjs';",
 "import { createWeeklySourceUploadPublicationOwner } from './weekly-source/upload-publication-owner.mjs';\r\nimport { createOffRuleTransportRpc } from './weekly-source/off-rule-transport-rpc.mjs';");
const start=value.indexOf('      const weeklySourceUploadOwner = createWeeklySourceUploadPublicationOwner({');
const end=value.indexOf('      if (weeklySourceResponse) return withCORS(env, req, weeklySourceResponse);',start);assert.ok(start>0&&end>start);
const body=value.slice(start,end);let after=body;
const injection=`      let weeklySourceAuthenticatedActorId = null;
      const weeklySourceOffScope = {
        getActor: () => weeklySourceAuthenticatedActorId,
        agencyId: String(env.MYTMS_OFFICE_AGENCY_ID || '').trim().toLowerCase(),
        environment: String(env.CANDIDATE_APP_ENVIRONMENT || '').trim().toUpperCase()
      };
      const weeklySourceOffRpc = createOffRuleTransportRpc({ ...weeklySourceOffScope,
        baseRpc: (functionName, args, options) => sbRpc(env, functionName, args, options) });
      const weeklySourceOffRecordingRpc = createOffRuleTransportRpc({ ...weeklySourceOffScope,
        baseRpc: (functionName, args, options) => sbRpcRecordingGuardRefusal(env, functionName, args, options) });
` .replaceAll('\n','\r\n');
const direct='rpc: (functionName, args, options) => sbRpc(env, functionName, args, options)';assert.equal(after.split(direct).length-1,2);
after=after.replaceAll(direct,()=> 'rpc: weeklySourceOffRpc');
const recording='dataRpc: (functionName, args, options) => sbRpcRecordingGuardRefusal(env, functionName, args, options)';assert.equal(after.split(recording).length-1,5);
after=after.replaceAll(recording,()=> 'dataRpc: weeklySourceOffRecordingRpc');
const actor='requireUser: (runtimeEnv, request, roles) => requireUser(runtimeEnv, request, roles),';assert.equal(after.split(actor).length-1,1);
after=after.replace(actor,()=>`requireUser: async (runtimeEnv, request, roles) => {
          const actor = await requireUser(runtimeEnv, request, roles);
          weeklySourceAuthenticatedActorId = actor?.is_active === true ? actor.id : null;
          return actor;
        },`.replaceAll('\n','\r\n'));
edit(body,injection+after);
let reverse=value;for(const h of [...changes].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,original);
fs.writeFileSync(path.join(out,'BROKER_INDEX_SOURCE_FORWARD.js'),value);
fs.writeFileSync(path.join(out,'BROKER_TRANSPORT_REVERSE.json'),JSON.stringify({source_only:true,input_file:'broker/src/index.js',input_sha256:sha(original),output_sha256:sha(value),
 reverse_hunk_matches_entire_input_definition:true,scope:'Only existing Weekly Source request dependency block and one import',
 direct_rpc_callers:2,native_recording_rpc_callers:5,unchanged_calculator_and_c1_raw_dependencies:true,no_new_http_route:true,
 changes:changes.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)})),no_remote_acceptance:true},null,2)+'\n');
const setup=fs.readFileSync(path.join(out,'06_existing_controlled_setup.sqlinc'),'utf8');
const before="  elsif p_request->>'qualification_action'='OFF_RULE_STATUS' then";
const added="  elsif p_request->>'qualification_action'='OFF_RULE_TRANSPORT_STATUS' then\n    return private.weekly_source_rule_qualification_transport_status_v1(p_request);\n"+before;
assert.equal(setup.split(before).length-1,1);const setupAfter=setup.replace(before,()=>added);
assert.equal(setupAfter.replace(added,()=>before),setup);
fs.writeFileSync(path.join(out,'09_complete_controlled_setup_transport.sqlinc'),setupAfter);
fs.writeFileSync(path.join(out,'CONTROLLED_TRANSPORT_REVERSE.json'),JSON.stringify({source_only:true,input_sha256:sha(setup),output_sha256:sha(setupAfter),
 reverse_hunk_matches_entire_input_definition:true,new_dispatch_only:'OFF_RULE_TRANSPORT_STATUS',no_new_service_grant:true},null,2)+'\n');
