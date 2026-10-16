// Reproducible offline source packet; no native client, credentials or raw maps.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {source,buildInput,sha,proofSql} from './input-builder.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:20,backendPid:100,turnId:'hr_turn_'+'0'.repeat(32),key:[20261013,1]};
const {plan,live}=await source(root),cases=[];
for(const c of plan.cases){
 const before={backgroundRows:Object.fromEntries(live.backgroundRelations.map(r=>[r,[]]))},transaction={database:'cloudtms_test_clone',backendPid:202,xid:'105',epochZero:true},observations=[{unit:0,...transaction,rows:[]}];
 const canonicalFixtureProof=live.freshFixtureProofSql(transaction,observations,before);
 cases.push({caseId:c.id,...await buildInput({root,caseId:c.id,identity}),offlineSecondProofSql:canonicalFixtureProof+'\n'+proofSql(c,identity,0),canonicalFixtureProof});
}
fs.writeFileSync(path.join(here,'OFFLINE_INPUTS.json'),JSON.stringify({scope:'SYNTHETIC_TURN_SOURCE_ONLY_NEVER_NATIVE_INPUT',identity,cases},null,2)+'\n');
const files=['../v6-contact-large-frame-feasibility-20261016/fixed-metadata-reader.mjs','../v6-contact-large-frame-feasibility-20261016/SOURCE_PINS.json',
 '../v6-contact-large-frame-feasibility-20261016/SOURCE_MANIFEST.json','../v6-contact-large-frame-feasibility-20261016/GRAMMAR_RESULT.json',
 '../v6-contact-six-lock-mcp-successor-20261016/input-builder.mjs','../v6-contact-six-lock-mcp-successor-20261016/AUTHORITY.json',
 '../v6-contact-six-lock-contention-20261016/prepare.mjs','../v6-contact-six-lock-contention-20261016/PLAN.json'];
const pinnedPredecessorFiles=files.map(file=>({file,sha256:sha(fs.readFileSync(path.join(here,file)))}));
fs.writeFileSync(path.join(here,'AUTHORITY.json'),JSON.stringify({scope:'SOURCE_ONLY_FIXED_METADATA_KIER_MCP_SUCCESSOR',rootHead:execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true}).trim(),
 originalMcpCommit:'6f131e34e2b4ae93f801bc0cc9420f95b38b9398',heldChunkTransport:'737b82cadea8d9e73b73421de1d4f36565e5b29e',
 fixedReaderCommit:'7fda1321502f1576487dd86889aa22ebe7896766',pinnedPredecessorFiles,
 fixedReaderUnchanged:{stdoutBytes:67108864,childMilliseconds:70000,sourceSha256:pinnedPredecessorFiles[0].sha256},
 metadataAuthority:'Exact SEND5B before/fresh-after fixed metadata phases under actual SEND7 root callback; proposed B only Kier MCP',
 caseCount:cases.length,fullMaps:'PROCESS_MEMORY_ONLY; original qualifier/WeakMap/fresh restoration unchanged',native:'NOT_RUN'},null,2)+'\n');
const heldFile='.codex-tmp/contact-six-mcp-20261016-contact20-lock1-attempt3/NATIVE_RESULT.json',heldBytes=fs.readFileSync(path.join(root,heldFile));
const heldResult=JSON.parse(heldBytes),nativeError=heldResult.statements.find(s=>s.error);
fs.writeFileSync(path.join(here,'NATIVE_ERROR_SHAPE.json'),JSON.stringify({scope:'READ_ONLY_SAFE_FRAME_SHAPE_FROM_HISTORICAL_HELD_ATTEMPT',source:heldFile,sourceSha256:sha(heldBytes),
 failedStatement:nativeError.statement,sqlstate:nativeError.error.code,topKeys:Object.keys(nativeError),errorKeys:Object.keys(nativeError.error),
 noRowsTruncatedProperty:Object.hasOwn(nativeError,'rowsTruncated')===false,rawValuesRetained:false,
 fatalErrorKeys:Object.keys(heldResult.fatalError??{}),fatalErrorMatchesFailedRow:JSON.stringify(heldResult.fatalError)===JSON.stringify(nativeError.error),
 handling:'Exact final statement/error-only branch; successful rows still require rowsTruncated=false; partial error payload or unknown keys refused; never mutate native result',qualification:'HELD_HISTORICAL_NOT_NATIVE_PASS'},null,2)+'\n');
console.log(JSON.stringify({status:'SIX_FINITE_MCP_INPUTS_PREPARED_OFFLINE',cases:cases.length,expectedFinalIndices:cases.map(c=>c.receipt.indices.expectedFailedStatement),native:'NOT_RUN'}));
