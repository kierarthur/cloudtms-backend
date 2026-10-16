import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const prefix='docs/healthroster-build/proposals/',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const blob=(commit,p)=>execFileSync('git',['show',commit+':'+prefix+p],{cwd:repo});
const selected=[
 ['96c5a7ec4655481499f53d3e2c59361ede102a74','candidate-contract-move-native-20261016/forward/11_broker_native_contexts.mjsinc','managerStartResult','broker/src/candidate-app-backend.js'],
 ['a10f080f2272155c423991ffc81de8670d3c756b','candidate-move-qr-paper-20261016/forward/06_captured_paper_context.mjsinc','candidatePaperPackContext','broker/src/candidate-app-backend.js'],
 ['a10f080f2272155c423991ffc81de8670d3c756b','candidate-move-qr-paper-20261016/forward/06_captured_paper_context.mjsinc','assembleCandidatePaperPack','broker/src/candidate-app-backend.js'],
 ['529ecc404c29c923b1fcfca3d764e9dc9d6762ad','candidate-move-paper-draft-20261016/forward/04_original_paper_status.mjsinc','restartCandidatePaperSourceDocumentFromStatus','broker/src/candidate-app-backend.js'],
 ['529ecc404c29c923b1fcfca3d764e9dc9d6762ad','candidate-move-paper-draft-20261016/forward/04_original_paper_status.mjsinc','handlePaperPackStatus','broker/src/candidate-app-backend.js'],
 ['529ecc404c29c923b1fcfca3d764e9dc9d6762ad','candidate-move-paper-draft-20261016/forward/08_factual_snapshot.mjsinc','validateCandidateWorkflowFactualSnapshot','broker/src/candidate-app-backend.js'],
 ['529ecc404c29c923b1fcfca3d764e9dc9d6762ad','candidate-move-paper-draft-20261016/forward/09_create_payload.mjsinc','normaliseCandidateWorkflowCreatePayload','broker/src/candidate-app-backend.js'],
 ['c03c010dfabe8280fcbb36779fa210e7e5242a02','candidate-move-native-reset-20261016/forward/10_resubmit_handler.mjsinc','handleWorkflowResubmit','broker/src/candidate-app-backend.js'],
 ['2b5f58431df26561b1423d40c48bef15209494fd','candidate-move-expense-preparation-20261016/forward/01_current_expense_preparation.mjsinc','prepareImmutableSubmission','broker/src/candidate-app-backend.js'],
 ['2b5f58431df26561b1423d40c48bef15209494fd','candidate-move-expense-preparation-20261016/forward/01_current_expense_preparation.mjsinc','buildCandidateWeeklySubmissionThroughCanonicalAuthority','broker/src/index.js']
];
fs.mkdirSync(path.join(here,'forward/worker'),{recursive:true});
const functions=[];
for(const [commit,file,name,module] of selected){
 const raw=blob(commit,file),s=raw.toString('utf8');const declarations=[...s.matchAll(/^(?:async )?function (\w+)\(/gm)];
 const i=declarations.findIndex(m=>m[1]===name);if(i<0||declarations.filter(m=>m[1]===name).length!==1)throw Error('FUNCTION_NOT_UNIQUE:'+name);
 const start=declarations[i].index,end=declarations[i+1]?.index??s.length,complete=s.slice(start,end).trim();
 // Parse a complete function expression only; never execute its body or I/O.
 new Function('return ('+complete+'\n);');
 const payload='forward/worker/'+name+'.mjsinc';fs.writeFileSync(path.join(here,payload),complete+'\n');
 functions.push({module,name,sourceCommit:commit,sourcePath:prefix+file,sourceFileSha256:sha(raw),sourceStartByte:Buffer.byteLength(s.slice(0,start)),sourceEndByteExclusive:Buffer.byteLength(s.slice(0,end)),sourceSpanSha256:sha(Buffer.from(s.slice(start,end))),payload,sha256:sha(fs.readFileSync(path.join(here,payload))),mode:name==='validateCandidateWorkflowFactualSnapshot'?'Add exact pure helper adjacent to existing normalizer':'Replace complete exact same-name function, preserving every unrelated wrapper/caller'});
}
const decoderCommit='96c5a7ec4655481499f53d3e2c59361ede102a74',decoderPath='candidate-contract-move-native-20261016/forward/candidate-contract-move.mjs';const decoder=blob(decoderCommit,decoderPath);
fs.writeFileSync(path.join(here,'forward/worker/candidate-contract-move.mjs'),decoder);
const callerCommit='c47530caa7e4c7e2412a5d6b03b76bbb7f6dc8bb',callerPath='candidate-contract-move-lifecycle-20261016/BROKER_CALLER.json';const caller=blob(callerCommit,callerPath);
fs.writeFileSync(path.join(here,'BROKER_CALLER.json'),caller);
fs.writeFileSync(path.join(here,'WORKER_MAP.json'),JSON.stringify({status:'SOURCE_COMPOSITION_NATIVE_API_NOT_RUN',functions,sharedDecoder:{sourceCommit:decoderCommit,sourcePath:prefix+decoderPath,payload:'forward/worker/candidate-contract-move.mjs',sha256:sha(decoder),target:'broker/src/candidate-contract-move.mjs',import:'decodeCandidateContractMove'},callerPatch:{sourceCommit:callerCommit,sourcePath:prefix+callerPath,sha256:sha(caller),payload:'BROKER_CALLER.json',mode:'Exact sole prepareImmutableSubmission caller adds genuine access argument; no new route.'},preserve:['Outer handleManagerAction START enrichManagerCorrection/deps/env/request wrapper; manager fragment grants no Candidate read','Current Source/self-entry/contact/manual wrappers, all72 method/path/operation/security boundaries','Immutable original signed official presentation/body/component/signature identities','MyTMS f8aba9d7b7a7a5ba9c67d4c5ece424d594e3bd79 additive CREATE/resubmit typed snapshot schemas/journals; no third repair','Native movedExpense submit controlled refusal; actual currentB finalizer remains separately owned']},null,2)+'\n');
console.log('10 complete Worker functions + unchanged shared decoder + sole exact caller patch composed from frozen Git bytes; syntax parsed, no execution.');
