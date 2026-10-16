// Offline source checks and manifest only; no native/tool/credential operations.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {sha} from '../v6-contact-local-metadata-mcp-adapter-20261016/input-builder.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),old=path.join(here,'../v6-contact-local-metadata-mcp-adapter-20261016');
const results=[['adapter.test.mjs',40],['../v6-contact-large-frame-feasibility-20261016/reader.test.mjs',10]];
for(const [file,count]of results){const output=execFileSync(process.execPath,['--test',path.resolve(here,file)],{encoding:'utf8',windowsHide:true});
 if(!new RegExp(`(?:#|ℹ) pass ${count}(?:\\r?\\n)`).test(output)||!/(?:#|ℹ) fail 0/.test(output))throw Error('FRESH_VERIFICATION_SOURCE_CHECKS_FAILED');}
const grammar=JSON.parse(fs.readFileSync(path.join(here,'GRAMMAR_RESULT.json'),'utf8'));
if(grammar.cases.length!==6||grammar.native!=='NOT_RUN'||grammar.canonicalSqlChanged!==false)throw Error('EXACT_PG17_SOURCE_PROOF_REQUIRED');
for(const file of ['result-validator.mjs','hold-case.mjs','adapter.test.mjs','freeze-source.mjs'])execFileSync(process.execPath,['--check',path.join(here,file)],{windowsHide:true});
const predecessor=JSON.parse(fs.readFileSync(path.join(old,'SOURCE_MANIFEST.json'),'utf8'));
for(const f of predecessor.files)if(sha(fs.readFileSync(path.resolve(old,f.file)))!==f.sha256)throw Error('BF11_FROZEN_SOURCE_DRIFT');
const pins=[...predecessor.pinnedPredecessorFiles,...['SOURCE_MANIFEST.json','input-builder.mjs','control.mjs','result-validator.mjs','hold-case.mjs','OFFLINE_INPUTS.json'].map(file=>({file:'../v6-contact-local-metadata-mcp-adapter-20261016/'+file,sha256:sha(fs.readFileSync(path.join(old,file)))}))];
for(const f of pins)if(sha(fs.readFileSync(path.resolve(here,f.file)))!==f.sha256)throw Error('UNCHANGED_PREDECESSOR_REQUIRED');
fs.writeFileSync(path.join(here,'AUTHORITY.json'),JSON.stringify({scope:'SOURCE_ONLY_SECOND_MCP_FRESH_READ_ONLY_VERIFICATION_SUCCESSOR',
 sourceParentCommit:'bf11b6f2c4dbdcba4d381f02a302922ea510b755',fixedReaderCommit:'7fda1321502f1576487dd86889aa22ebe7896766',
 proposedBRoute:'mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback',
 secondMain:'UNCHANGED_CANONICAL_PAIR_EXECUTION_EVIDENCE_ONLY_MAY_REUSE_ORIGINAL_B_POOL_PID',
 secondVerification:'EXACT_SAME_CANONICAL_PAIR_ON_TOOL_FRESH_READ_ONLY_CONNECTION_ONLY_QUALIFIES_FIXTURES',
 requiredDistinctPids:['originalB','A','C-before','C-after','secondMain'],historicalFirstMcpVerificationPidExcluded:false,
 fixedMetadataReaderUnchanged:{captureBytes:67108864,childMilliseconds:70000},wholeSequenceMilliseconds:180000,
 completeMaps:'PROCESS_MEMORY_ONLY_UNCHANGED_CANONICAL_WRITER_ATTRIBUTION_WEAKMAP_AND_RESTORATION',
 holderChanges:'THREE_IMPORT_PATHS_ONLY_WHOLE_SOURCE_REVERSE',pinnedPredecessorFiles:pins,
 historicalSecondCase:'GENUINE_POOLED_MAIN_PID_HOLD_PRESERVED',native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({scope:'OFFLINE_SOURCE_SYNTHETIC_EVIDENCE_ONLY',adapterTests:40,
 unchangedFixedReaderTests:10,pass:50,fail:0,pg17Cases:6,firstBFinalIndices:grammar.cases.map(c=>c.firstBFinalLockIndex),
 firstBAndCanonicalSqlUnchanged:true,wholeHolderLifecycleAndCleanupUnchanged:true,completeMapsPersisted:false,
 canonicalWeakMapFixtureAndAttributionChecksUnchanged:true,historicalPoolPidReuseAcceptedOnlyForFreshCurrentTransaction:true,
 databaseCalls:0,credentialFilesRead:0,native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
const files=fs.readdirSync(here).filter(file=>file!=='SOURCE_MANIFEST.json').sort().map(file=>{const bytes=fs.readFileSync(path.join(here,file));return{file,bytes:bytes.length,sha256:sha(bytes)};});
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'CONTACT_FRESH_READ_ONLY_VERIFICATION_SUCCESSOR_V2',
 sourceParentCommit:'bf11b6f2c4dbdcba4d381f02a302922ea510b755',files,pinnedPredecessorFiles:pins,
 sourceVerdict:'BOUNDED_SOURCE_READY_FOR_ROOT_REVIEW_NATIVE_NOT_RUN',native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({status:'FRESH_VERIFICATION_SOURCE_FROZEN',files:files.length,pass:50,pg17Cases:6,
 manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),native:'NOT_RUN'}));
