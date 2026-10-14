import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)), root=process.argv[2];
assert.ok(root,'explicit controlling backend input required');
const relative='broker/src/invoice-async-http.js';
const bytes=fs.readFileSync(path.join(root,relative)), native=bytes.toString('utf8').replace(/\r\n/g,'\n');
let next=native;const hunks=[];
function change(anchor,replacement){assert.equal(next.split(anchor).length,2,'changed AI source anchor '+anchor.slice(0,60));hunks.push({anchor,replacement});next=next.replace(anchor,replacement);}
change("import {\n  buildInvoiceDocumentDownloadUrl,", "import {loadAutomaticCorrectionCredits,startAutomaticCorrectionCredits} from './weekly-source/agency-invoice-credit-consumer.mjs';\nimport {\n  buildInvoiceDocumentDownloadUrl,");
change('async function handleCandidates(env, req, deps, rpcName, options = {}) {','async function handleCandidates(env, req, deps, rpcName, options = {}, correctionActor = null) {');
change('  return jsonResponse(responseEnvelope);\n}\n\nasync function handleNhspCandidates',`  if (action==='GENERATE' && correctionActor && query.mode==='PAGE' && !options.forcedInvoiceStreams) {
    const credits=await loadAutomaticCorrectionCredits(deps,correctionActor.id,query);
    if (credits && credits.rows.length) responseEnvelope={...responseEnvelope,healthroster_credit_notes:credits};
  }
  return jsonResponse(responseEnvelope);
}

async function handleNhspCandidates`);
change('async function handleBatchGenerateConfirm(env, req, ctx, user, deps) {','async function handleBatchGenerateConfirmNative(env, req, ctx, user, deps) {');
const wrapper=`// Root mounts this wrapper only with the qualified release/feature capability.
// The complete ordinary handler above is preserved byte-for-byte apart from
// its name. Every credit remains a separate call, outside its selection root.
async function handleBatchGenerateConfirm(env,req,ctx,user,deps) {
  if(typeof deps.healthRosterCorrectionConsumersReady!=='function'
    ||await deps.healthRosterCorrectionConsumersReady()!==true) return handleBatchGenerateConfirmNative(env,req,ctx,user,deps);
  const body=await readInvoiceBatchJsonBody(req.clone(),{
    maximumBytes:Number(env?.INVOICE_BATCH_REQUEST_MAX_BYTES)||INVOICE_BATCH_REQUEST_MAX_BYTES});
  if(Object.keys(body).some(key=>!['selection_contract','weekly_source_selection_contract','command_token'].includes(key))) throw invoiceBatchContractError('BATCH_QUERY_UNKNOWN_FIELD');
  commandToken(req,body,{requiredCode:'GENERATE_COMMAND_TOKEN_REQUIRED',invalidCode:'GENERATE_COMMAND_TOKEN_INVALID'});
  const selectionRoot=normaliseInvoiceBatchSelectionRoot(body.selection_contract,'GENERATE');
  if(!selectionRoot.query.snapshot) throw invoiceBatchContractError('BATCH_SNAPSHOT_REQUIRED');
  if(selectionRoot.query.mode==='EXPLICIT_KEYS') return handleBatchGenerateConfirmNative(env,req,ctx,user,deps);
  if(!['PAGE','SUMMARY'].includes(selectionRoot.query.mode)) throw invoiceBatchContractError('INVOICE_BATCH_QUERY_MODE_INVALID');
  const query={contract_version:INVOICE_BATCH_QUERY_CONTRACT,action:'GENERATE',mode:'PAGE',snapshot:selectionRoot.query.snapshot,
    page_size:100,cursor:null,filters:selectionRoot.query.filters,sort:selectionRoot.query.sort,selection:selectionRoot.selection};
  const credits=await loadAutomaticCorrectionCredits(deps,user.id,query);
  if(!credits?.rows.length) return handleBatchGenerateConfirmNative(env,req,ctx,user,deps);
  let response;let ordinaryFailure;
  try {response=await handleBatchGenerateConfirmNative(env,req,ctx,user,deps);} catch(error) {ordinaryFailure=error;}
  const credit_note_results=await startAutomaticCorrectionCredits(credits,command=>
    startCommands(env,req,ctx,user,[command],deps,['DATABASE'],{priorityClass:'INTERACTIVE'}));
  const creditOk=credit_note_results.some(row=>(row.accepted&&!row.blocked)||row.already_credited);
  const creditFailed=credit_note_results.some(row=>row.blocked||(!row.accepted&&!row.already_credited));
  const ordinary=response?await response.json():{ok:false,accepted:false,operation_ids:[],per_command_results:[],root_operation_id:null};
  const ordinaryOk=ordinary.ok===true;
  // Keep the ordinary root and operation array exact. Credit-only commands are
  // registered separately by the Office and use native Outbox recovery.
  return jsonResponse({...ordinary,ok:ordinaryOk||creditOk,partial:ordinaryFailure?true:ordinary.partial||(!ordinaryOk&&creditOk)||creditFailed,
    ...(ordinaryFailure?{error:String(ordinaryFailure?.code||'ORDINARY_INVOICE_BATCH_START_FAILED').slice(0,160)}:{}),
    credit_note_results,credit_note_operation_ids:credit_note_results.map(row=>row.operation_id).filter(Boolean)},
    (ordinaryFailure||creditFailed||(!ordinaryOk&&creditOk))?207:(response?.status||202));
}

`;
change('async function handleBatchIssueConfirm(env, req, ctx, user, deps) {',wrapper+'async function handleBatchIssueConfirm(env, req, ctx, user, deps) {');
change("return await handleCandidates(env, req, deps, 'invoice_batch_generate_candidates');","return await handleCandidates(env, req, deps, 'invoice_batch_generate_candidates',{},user);");
const ordinaryStart=native.indexOf('async function handleBatchGenerateConfirm('),ordinaryEnd=native.indexOf('async function handleBatchIssueConfirm(',ordinaryStart);
const successorStart=next.indexOf('async function handleBatchGenerateConfirmNative('),successorEnd=next.indexOf(wrapper,successorStart);
assert.equal(next.slice(successorStart,successorEnd).replace('handleBatchGenerateConfirmNative','handleBatchGenerateConfirm'),native.slice(ordinaryStart,ordinaryEnd));
fs.writeFileSync(path.join(here,'invoice-async-http.complete-proposed.js'),next);
const proposed=path.join(here,'invoice-async-http.complete-proposed.js');
const diff=spawnSync('git',['diff','--no-index','--',path.join(root,relative),proposed],{encoding:'utf8',windowsHide:true,maxBuffer:8*1024*1024});
assert.ok([0,1].includes(diff.status),diff.stderr);const lines=diff.stdout.split('\n');
fs.writeFileSync(path.join(here,'AI_WORKER_ROOT.diff'),`--- a/${relative}\n+++ b/${relative}\n${lines.slice(lines.findIndex(line=>line.startsWith('@@'))).join('\n')}`);
const sha=value=>crypto.createHash('sha256').update(value).digest('hex');
fs.writeFileSync(path.join(here,'AI_WORKER_SOURCE_GUARD.json'),JSON.stringify({input:{path:relative,sha256:sha(bytes)},
  output:{path:'invoice-async-http.complete-proposed.js',sha256:sha(Buffer.from(next))},
  ordinary_confirm_after_name_restoration_is_identical:true,
  mount:'UNMOUNTED_QUALIFIED_CAPABILITY_ABSENT',hunks},null,2)+'\n');
console.log(JSON.stringify({status:'SOURCE_GENERATED',hunks:hunks.length,ordinaryNativePreserved:true}));
