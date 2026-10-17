import fs from'node:fs';import path from'node:path';import{execFileSync}from'node:child_process';
import{here,checkpoint,blob,sha,apply,reverse}from'./build.mjs';
const target='broker/src/weekly-source/routes.js',input=blob(checkpoint,target).toString(),hunks=[];
const add=(before,after,purpose)=>hunks.push({before,after,purpose});
add("import { isActivePaymentActor } from '../payment-actor-permission.js';","import { isActivePaymentActor } from '../payment-actor-permission.js';\nimport{parseKeptPayCommand,validateKeptPayCommand,decodeKeptPayAcknowledgement,keptPayFailure,readWeeklySourceBoundedText}from'./kept-pay-command.mjs';",'Concrete kept command parser/receipt and bounded actual-parent reader.');
add("  'OPEN_MANUAL_REVIEW', 'RESOLVE_MANUAL_REVIEW', 'PREPARE_PROTECTED_EDITOR',","  'OPEN_MANUAL_REVIEW', 'RESOLVE_MANUAL_REVIEW', 'PREPARE_PROTECTED_EDITOR',\n  'KEEP_AS_PAID',",'Preserve WP-PP role rule for non-admin genuine payment actors.');
add(`    text = new TextDecoder('utf-8',{fatal:true}).decode(await req.arrayBuffer());
  } catch {`, `    text = await readWeeklySourceBoundedText(req, maximumBytes);
  } catch (error) {
    if(error?.code==='WEEKLY_SOURCE_REQUEST_TOO_LARGE')fail('WEEKLY_SOURCE_REQUEST_TOO_LARGE', 'The request is too large.', 413);`, 'Enforce byte cap before retaining overflow; preserve exact older command parsing and error envelope.');
add(`    const parsed = plainObject(JSON.parse(text || '{}'), 'WEEKLY_SOURCE_REQUEST_INVALID', 'Request');`, `    const parsed = plainObject(JSON.parse(text || '{}'), 'WEEKLY_SOURCE_REQUEST_INVALID', 'Request');
    if(typeof parsed.action==='string'&&parsed.action.trim().toUpperCase()==='KEEP_AS_PAID')return parseKeptPayCommand(text);`, 'Strict duplicate-aware JSON and closed browser fields for the new action.');
add("  if (action === 'LATER_CHANGE_DECISION') {",`  if (action === 'KEEP_AS_PAID') {
    try{validateKeptPayCommand(body);}catch{fail('WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_INVALID','This decision could not be prepared.',400);}
    let result;
    try{result=await withRefusalRecording(dependencies,
      'broker:weekly-source:weekly_source_keep_as_paid_decide_atomic_v1',user.id,
      ()=>callRpc(dependencies,'weekly_source_keep_as_paid_decide_atomic_v1',request));}
    catch(error){const refusal=keptPayFailure(error);fail(refusal.code,'Open the current review before continuing.',refusal.status);}
    try{return jsonResponse(200,decodeKeptPayAcknowledgement(result,payload,user.id));}
    catch{fail('WEEKLY_SOURCE_KEEP_AS_PAID_RESULT_INVALID','Open the current review before continuing.',502);}
  }

  if (action === 'LATER_CHANGE_DECISION') {`, 'One actual service RPC with authenticated actor; closed factual ACK output; no retry or financial assertion.');
const output=apply(input,hunks);if(reverse(output,hunks)!==input)throw Error('COMPLETE_PARENT_REVERSE_REQUIRED');
execFileSync(process.execPath,['--input-type=module','--check'],{input:output,windowsHide:true});
fs.writeFileSync(path.join(here,'PROPOSED_ROUTES.js'),output);
fs.writeFileSync(path.join(here,'PARENT_EDITS.json'),JSON.stringify({checkpoint,target,inputSha256:sha(input),outputSha256:sha(output),hunks,completeReverseByteEquality:true,
  sourceHttpComposition:'Complete current5ecc parent retains Source pay HTTP route composition. Integrator applies these unique anchors to its saved current complete parent and proves combined reverse; refuse any current source drift.',module:{path:'runtime/kept-pay-command.mjs',target:'broker/src/weekly-source/kept-pay-command.mjs',sha256:sha(fs.readFileSync(path.join(here,'runtime/kept-pay-command.mjs')))},native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({wholeParentReverse:true,hunks:hunks.length,native:'NOT_RUN'}));
