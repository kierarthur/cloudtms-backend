// Freeze reviewed source only. No database/credential/connector operations.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {sha,source} from './input-builder.mjs';
import {reviewedPathRepair} from './canonical-input-builder.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
await source(root);const repair=reviewedPathRepair(root);
execFileSync('python',[path.join(here,'source-review.py')],{windowsHide:true});
const output=execFileSync(process.execPath,['--test',path.join(here,'adapter.test.mjs'),path.join(here,'reader.test.mjs')],{encoding:'utf8',windowsHide:true});
if(!/(?:#|ℹ) pass 56(?:\r?\n)/.test(output)||!/(?:#|ℹ) fail 0/.test(output))throw Error('INDEPENDENT_C_AFTER_SOURCE_TESTS_FAILED');
const grammar=JSON.parse(fs.readFileSync(path.join(here,'GRAMMAR_RESULT.json'),'utf8'));
if(grammar.cases.length!==6||grammar.native!=='NOT_RUN'||grammar.canonicalSqlChanged!==false)throw Error('EXACT_PG17_SOURCE_PROOF_REQUIRED');
for(const f of fs.readdirSync(here).filter(f=>f.endsWith('.mjs')))execFileSync(process.execPath,['--check',path.join(here,f)],{windowsHide:true});
const priorPath=path.join(here,'../v6-contact-fresh-verification-successor-20261016'),prior=JSON.parse(fs.readFileSync(path.join(priorPath,'SOURCE_MANIFEST.json'),'utf8'));
for(const f of prior.files)if(sha(fs.readFileSync(path.join(priorPath,f.file)))!==f.sha256)throw Error('FEB525_FROZEN_PREDECESSOR_DRIFT');
const extra=['SOURCE_MANIFEST.json','result-validator.mjs','hold-case.mjs'].map(file=>({file:'../v6-contact-fresh-verification-successor-20261016/'+file,sha256:sha(fs.readFileSync(path.join(priorPath,file)))}));
const pins=[...prior.pinnedPredecessorFiles,...extra];
for(const f of pins)if(sha(fs.readFileSync(path.join(here,f.file)))!==f.sha256)throw Error('HISTORICAL_PREDECESSOR_DRIFT');
fs.writeFileSync(path.join(here,'AUTHORITY.json'),JSON.stringify({scope:'SOURCE_ONLY_INDEPENDENT_C_AFTER_THIRD_CANONICAL_FRAME',
 sourceParentCommit:'abff0e36d3be46498f749244428f398af1b79857',freshVerificationPredecessor:'feb525',fixedReaderCommit:'7fda1321502f1576487dd86889aa22ebe7896766',
 proposedBRoute:'mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback',firstBWholeInputAndReceiptUnchanged:true,
 originalCanonicalLiveRestorationSha256:'fb4ffa42acf7cf319cdcb45a233babdb9720bb1bc73e4cb1f6ddcd3be696a4a6',
 restorationAuthority:'EXACT_ORIGINAL_CANONICAL_FIXTURE_QUERY_THIRD_FRAME_AFTER_COMPLETE_CANONICAL_SNAPSHOT_IN_INDEPENDENT_C_AFTER_READ_ONLY_REPEATABLE_READ_SUBPROCESS',
 fixturePidMustEqual:'C-after',fixturePidMustDifferFrom:['B','A','C-before'],initialMcpVerification:'COMPLETE_HELD_A_POOL_CONTEXT_NOT_RESTORATION_AUTHORITY_MAY_REUSE_B',
 secondMcpRemoved:true,releasePhaseRemoved:true,originalHeldCasesRemainHeld:true,pathOnlyRepair:repair,
 limits:{stdoutBytes:67108864,childMilliseconds:70000,workMilliseconds:120000,cleanupReserveMilliseconds:60000,wholeMilliseconds:180000},
 maps:'PROCESS_MEMORY_ONLY',canonicalQualifierWeakMapAttributionAndFinalAssertions:'UNCHANGED',pinnedPredecessorFiles:pins,native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({scope:'OFFLINE_SYNTHETIC_SOURCE_ONLY',tests:56,pass:56,fail:0,
 pg17Cases:6,wholeSourceReverses:5,firstBFinalIndices:grammar.cases.map(c=>c.firstBFinalLockIndex),
 complete28MiBSyntheticFramePassed:true,childFailureEventsAndUnconfirmedCloseRefused:true,
 originalPidXidFixtureWeakMapAttributionLedgerCatalogueRelationMarkerLegacyChecksUnchanged:true,
 databaseCalls:0,credentialFilesRead:0,completeBusinessMapsCopied:false,native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
const files=fs.readdirSync(here).filter(f=>f!=='SOURCE_MANIFEST.json').sort().map(file=>{const b=fs.readFileSync(path.join(here,file));return {file,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'CONTACT_INDEPENDENT_C_AFTER_CANONICAL_RESTORATION_SOURCE_V1',files,pinnedPredecessorFiles:pins,
 sourceVerdict:'SOURCE_READY_FOR_ROOT_REVIEW_NATIVE_NOT_RUN',native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({status:'INDEPENDENT_C_AFTER_SOURCE_FROZEN',files:files.length,pass:56,pg17Cases:6,
 manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),native:'NOT_RUN'}));
