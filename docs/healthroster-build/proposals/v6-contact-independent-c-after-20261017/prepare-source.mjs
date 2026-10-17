// Offline six-case derivation only. No child/credential/native operation.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {source,buildInput,sha,requireThat} from './input-builder.mjs';
import {createFixedMetadataReader} from './fixed-metadata-reader.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const prior=JSON.parse(fs.readFileSync(path.join(here,'../v6-contact-local-metadata-mcp-adapter-20261016/OFFLINE_INPUTS.json'),'utf8'));
const identity=prior.identity,{plan,live}=await source(root),cases=[];
const turn={identity,signal:new AbortController().signal,assertHeld:()=>{throw Error('OFFLINE_MUST_NOT_CONNECT');},runOwnedChild:()=>{throw Error('OFFLINE_MUST_NOT_CONNECT');}};
for(const c of plan.cases){
 const old=prior.cases.find(r=>r.caseId===c.id),built=await buildInput({root,caseId:c.id,identity});
 requireThat(JSON.stringify(built.input)===JSON.stringify(old.input),'WHOLE_FIRST_B_INPUT_CHANGED');
 requireThat(JSON.stringify(built.receipt)===JSON.stringify(old.receipt),'WHOLE_FIRST_B_RECEIPT_CHANGED');
 const before={backgroundRows:Object.fromEntries(live.backgroundRelations.map(r=>[r,[]]))};
 const transaction={database:'cloudtms_test_clone',backendPid:202,xid:'105',epochZero:true},observations=[{unit:0,...transaction,rows:[]}];
 const reader=await createFixedMetadataReader({root,turn,caseId:c.id}),program=reader.inspectSource({transaction,observations,before});
 requireThat(program.after.fixtureSql===old.canonicalFixtureProof,'CANONICAL_FIXTURE_SQL_CHANGED');
 requireThat(program.after.input===program.input.slice(0,-'rollback;\n'.length)+old.canonicalFixtureProof+'\nrollback;\n','EXACT_C_AFTER_APPEND_REQUIRED');
 cases.push({caseId:c.id,input:built.input,receipt:built.receipt,beforeInput:program.input,afterInput:program.after.input,
  canonicalSnapshotSql:program.canonicalSql,canonicalFixtureProof:program.after.fixtureSql,canonicalFixtureSqlSha256:sha(program.after.fixtureSql),
  wholeFirstBInputUnchanged:true,wholeFirstBReceiptUnchanged:true});
}
fs.writeFileSync(path.join(here,'OFFLINE_PROGRAMS.json'),JSON.stringify({scope:'SYNTHETIC_SQL_SOURCE_ONLY_NEVER_NATIVE_INPUT',cases,
 completeBusinessMapsCopied:false,databaseCalls:0,credentialFilesRead:0,native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({status:'SIX_OFFLINE_C_AFTER_PROGRAMS_DERIVED',cases:cases.length,native:'NOT_RUN'}));
