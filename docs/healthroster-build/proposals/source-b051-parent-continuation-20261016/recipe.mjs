import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';
export const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026');
export const checkpoint='39aa5b30042711956b83a7bf899f416d2f3b4771',hash=x=>createHash('sha256').update(x).digest('hex');
export const blob=p=>execFileSync('git',['show',checkpoint+':'+p],{cwd:repo,maxBuffer:64_000_000,windowsHide:true});
export const targets={routes:'broker/src/weekly-source/routes.js',index:'broker/src/index.js',module:'broker/src/weekly-source/source-pay-decision-http.mjs'};
export function spliceExactly(source,before,after){if(!before||source.split(before).length!==2)throw Error('SOURCE_PARENT_UNIQUE_PREIMAGE_REQUIRED');return source.replace(before,after);}
export function createRecipe(){
 const files=[];
 for(const [kind,target]of Object.entries(targets).filter(([k])=>k!=='module')){
  const bytes=blob(target),source=bytes.toString(),edits=[];
  const add=(id,before,after)=>{if(source.split(before).length!==2)throw Error('SOURCE_PARENT_ANCHOR_REFUSED:'+id);edits.push({id,before,after,beforeSha256:hash(before),afterSha256:hash(after)});};
  // Match the actual anchor line endings; every other byte remains untouched.
  const eol=source.includes('\r\n')?'\r\n':'\n';const lines=s=>s.split('\n').join(eol);
  if(kind==='routes'){
   const anchor="import {CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS,isContactManualEnquiryCommand,parseContactManualEnquiryBody,handleContactManualEnquiryOfficeCommand} from './contact-manual-enquiry-office.mjs';";
   add('source-pay-import',anchor,anchor+eol+"import {matchSourcePayDecisionRoute,handleSourcePayDecisionHttp,SOURCE_PAY_DECISION_HTTP_ROUTES} from './source-pay-decision-http.mjs';");
   const admission="  if (!path.startsWith(`${officePrefix}/`) && path !== officePrefix) return null;";
   add('source-pay-path-admission',admission,lines("  const payDecisionRoute = matchSourcePayDecisionRoute(path);\n  if (!path.startsWith(`${officePrefix}/`) && path !== officePrefix && !payDecisionRoute) return null;"));
   const auth='    const user = await requireOfficeUser(req, env, dependencies);';
   add('source-pay-authenticated-dispatch',auth,auth+eol+lines(`    if (payDecisionRoute) return await handleSourcePayDecisionHttp({
      req, url, user, route: payDecisionRoute,
      preview: dependencies.previewSourcePayDecision,
      confirm: dependencies.confirmSourcePayDecision,
      readSource: dependencies.readSourcePayDecisionSource,
    });`));
   const contract='  contactManualEnquiryRpcCommands: CONTACT_MANUAL_ENQUIRY_RPC_COMMANDS,';
   add('source-pay-route-contract',contract,contract+eol+'  sourcePayDecisionRoutes: SOURCE_PAY_DECISION_HTTP_ROUTES,');
  }else{
   const anchor="import { dispatchWeeklySourceRequest, weeklySourceDeploymentContext } from './weekly-source/routes.js';";
   add('source-pay-worker-import',anchor,anchor+eol+"import { previewSourcePayDecision,confirmSourcePayDecision,readSourcePayDecisionSource } from './weekly-source/source-pay-decision-worker.mjs';");
   const dependencyLine='        rpc: weeklySourceOffRpc,',nextLine='        previewUpload: weeklySourceUploadOwner.previewUpload,',dependencyEol=source.includes(dependencyLine+'\r\n'+nextLine)?'\r\n':'\n',dependency=dependencyLine+dependencyEol+nextLine;
   add('source-pay-native-worker-dependencies',dependency,dependencyLine+dependencyEol+`        previewSourcePayDecision: ({ request, actorUserId }) => previewSourcePayDecision({
          request, actorUserId, dataRpc: weeklySourceOffRecordingRpc,
          calculateWeeklySnapshot: (input) => calculateWeeklyProtectedSnapshot(env, input)
        }),
        confirmSourcePayDecision: ({ request, actorUserId }) => confirmSourcePayDecision({
          request, actorUserId, dataRpc: weeklySourceOffRecordingRpc,
          calculateWeeklySnapshot: (input) => calculateWeeklyProtectedSnapshot(env, input)
        }),
        readSourcePayDecisionSource: ({ request, actorUserId }) => readSourcePayDecisionSource({
          request, actorUserId, dataRpc: weeklySourceOffRecordingRpc
        }),`.split('\n').join(dependencyEol)+dependencyEol+nextLine);
  }
  let after=source;for(const e of edits)after=spliceExactly(after,e.before,e.after);let reverse=after;for(const e of edits.slice().reverse())reverse=spliceExactly(reverse,e.after,e.before);if(reverse!==source)throw Error('SOURCE_PARENT_FULL_REVERSE_REQUIRED');
  files.push({target,kind,preimageBytes:bytes.length,preimageSha256:hash(bytes),postimageBytes:Buffer.byteLength(after),postimageSha256:hash(after),preimageCrlf:(source.match(/\r\n/g)||[]).length,preimageBareLf:(source.match(/(?<!\r)\n/g)||[]).length,fullReverseByteEquality:true,edits});
 }
 const module=fs.readFileSync(path.join(here,'runtime/source-pay-decision-http.mjs'));
 return{version:'HEALTH_SOURCE_PAY_HTTP_PARENT_CONTRIBUTION_V1',checkpoint,files,module:{source:'runtime/source-pay-decision-http.mjs',target:targets.module,sha256:hash(module),bytes:module.length},rootSoleIntegrator:true,bankBodiesChanged:0,sqlChanged:0,native:'NOT_RUN'};
}
export function compose(recipe,read=blob){
 if(recipe.version!=='HEALTH_SOURCE_PAY_HTTP_PARENT_CONTRIBUTION_V1'||recipe.checkpoint!==checkpoint)throw Error('SOURCE_PARENT_RECIPE_IDENTITY_REQUIRED');
 const result={};for(const f of recipe.files){const before=read(f.target);if(hash(before)!==f.preimageSha256||before.length!==f.preimageBytes)throw Error('SOURCE_PARENT_FULL_PREIMAGE_CHANGED');let after=before.toString();for(const e of f.edits){if(hash(e.before)!==e.beforeSha256||hash(e.after)!==e.afterSha256)throw Error('SOURCE_PARENT_EDIT_CHANGED');after=spliceExactly(after,e.before,e.after);}if(hash(after)!==f.postimageSha256)throw Error('SOURCE_PARENT_POSTIMAGE_CHANGED');let reverse=after;for(const e of f.edits.slice().reverse())reverse=spliceExactly(reverse,e.after,e.before);if(!Buffer.from(reverse).equals(before))throw Error('SOURCE_PARENT_FULL_REVERSE_REQUIRED');result[f.target]=after;}
 return result;
}
