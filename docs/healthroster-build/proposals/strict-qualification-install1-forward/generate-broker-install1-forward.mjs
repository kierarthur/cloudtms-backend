import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),commit='37ddadc0a50721834c9e8a6c06becfc983f0dc32',file='broker/src/index.js',sha=x=>createHash('sha256').update(x).digest('hex');
const original=execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',commit+':'+file],{cwd:own,maxBuffer:32_000_000}).toString('utf8');let value=original;const hunks=[];
function edit(before,after){assert.equal(value.split(before).length-1,1);value=value.replace(before,()=>after);hunks.push({before,after,before_sha256:sha(before),after_sha256:sha(after)});}
edit("import { createWeeklySourceUploadPublicationOwner } from './weekly-source/upload-publication-owner.mjs';","import { createWeeklySourceUploadPublicationOwner } from './weekly-source/upload-publication-owner.mjs';\r\nimport { createOffRuleTransportRpc } from './weekly-source/off-rule-transport-rpc.mjs';");
const start=value.indexOf('      const weeklySourceUploadOwner = createWeeklySourceUploadPublicationOwner({'),end=value.indexOf('      if (weeklySourceResponse) return withCORS(env, req, weeklySourceResponse);',start);assert.ok(start>0&&end>start);const before=value.slice(start,end);let after=before;
const direct='rpc: (functionName, args, options) => sbRpc(env, functionName, args, options)',recording='dataRpc: (functionName, args, options) => sbRpcRecordingGuardRefusal(env, functionName, args, options)',actor='requireUser: (runtimeEnv, request, roles) => requireUser(runtimeEnv, request, roles),';
assert.equal(after.split(direct).length-1,2);assert.equal(after.split(recording).length-1,5);assert.equal(after.split(actor).length-1,1);
after=after.replaceAll(direct,()=> 'rpc: weeklySourceOffRpc').replaceAll(recording,()=> 'dataRpc: weeklySourceOffRecordingRpc');
after=after.replace(actor,()=>`requireUser: async (runtimeEnv, request, roles) => {
          const actor = await requireUser(runtimeEnv, request, roles);
          weeklySourceAuthenticatedActorId = actor?.is_active === true ? actor.id : null;
          return actor;
        },`.replaceAll('\n','\r\n'));
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
`.replaceAll('\n','\r\n');edit(before,injection+after);
let reverse=value;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,original);
fs.writeFileSync(path.join(out,'BROKER_INDEX_INSTALL1_SOURCE_FORWARD.js'),value);
fs.writeFileSync(path.join(out,'BROKER_INSTALL1_ENTIRE_SOURCE_REVERSE.json'),JSON.stringify({source_only:true,input_commit:commit,input_file:file,input_sha256:sha(original),output_file:'BROKER_INDEX_INSTALL1_SOURCE_FORWARD.js',output_sha256:sha(value),
 complete_reverse_matches_actual_entire_install1_broker:true,hunks,direct_rpc_callers:2,native_recording_rpc_callers:5,requireUser_actor_callbacks:1,actual_bank_routes_and_native_calculators_c1_unchanged:true,
 root_composition_boundary:'Apply these two exact reviewed transport hunks to the final joined current-Banking/broad Broker. This complete actual37 reference is not authority to replace a joined Install2 Broker or omit broad routes. Re-derive and reverse-check complete final joined source before freeze.',no_runtime_or_http_acceptance:true},null,2)+'\n');
console.log(JSON.stringify({source_only:true,actual_input_sha256:sha(original),complete_output_sha256:sha(value),reverse_entire_actual_source:true,direct:2,nested:5}));
