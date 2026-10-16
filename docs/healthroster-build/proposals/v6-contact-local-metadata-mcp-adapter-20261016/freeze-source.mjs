// Source tests/manifest only; no native coordinator entry or database.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {sha} from './input-builder.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const outputs=[];
for(const file of [path.join(here,'adapter.test.mjs'),path.join(here,'../v6-contact-large-frame-feasibility-20261016/reader.test.mjs')])outputs.push(execFileSync(process.execPath,['--test',file],{encoding:'utf8',windowsHide:true}));
if(!/(?:#|ℹ) pass 31/.test(outputs[0])||!/(?:#|ℹ) fail 0/.test(outputs[0])||!/(?:#|ℹ) pass 10/.test(outputs[1])||!/(?:#|ℹ) fail 0/.test(outputs[1]))throw Error('FIXED_LOCAL_ADAPTER_SOURCE_CHECKS_FAILED');
const grammar=JSON.parse(fs.readFileSync(path.join(here,'GRAMMAR_RESULT.json'),'utf8'));
if(grammar.cases.length!==6||grammar.native!=='NOT_RUN')throw Error('SIX_CASE_PG17_PROOF_REQUIRED');
for(const file of ['input-builder.mjs','result-validator.mjs','hold-case.mjs','control.mjs'])execFileSync(process.execPath,['--check',path.join(here,file)],{encoding:'utf8',windowsHide:true});
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({scope:'OFFLINE_SYNTHETIC_SOURCE_ONLY_NOT_NATIVE_RECEIPTS',
 adapterTests:31,unchangedFixedReaderTests:10,total:41,pass:41,fail:0,pg17Cases:6,finalIndices:grammar.cases.map(c=>c.expectedFinalLockIndex),
 allOldBStatementsExceptSnapshotRawAndASTIdentical:true,unchangedCanonicalWeakMapQualifier:true,completeMapsPersisted:false,
 proposedBOnlyKierMcp:true,databaseCalls:0,credentialFilesRead:0,native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
const authority=JSON.parse(fs.readFileSync(path.join(here,'AUTHORITY.json'),'utf8'));
const files=fs.readdirSync(here).filter(f=>f!=='SOURCE_MANIFEST.json').sort().map(file=>{const b=fs.readFileSync(path.join(here,file));return {file,bytes:b.length,sha256:sha(b)};});
for(const f of authority.pinnedPredecessorFiles)if(sha(fs.readFileSync(path.join(here,f.file)))!==f.sha256)throw Error('PREDECESSOR_SOURCE_CHANGED');
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'CONTACT_FIXED_LOCAL_METADATA_FINITE_MCP_ADAPTER_V1',
 sourceParentCommit:'6ba4c06ed5843231fab10c16fb6273ea1f640908',files,pinnedPredecessorFiles:authority.pinnedPredecessorFiles,
 sourceVerdict:'FINITE_ROOT_COORDINATOR_SOURCE_ONLY_READY_FOR_INDEPENDENT_REVIEW; NATIVE_NOT_RUN',native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({status:'FINITE_LOCAL_METADATA_MCP_ADAPTER_FROZEN',files:files.length,totalSourceChecks:41,pg17Cases:6,
 manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),native:'NOT_RUN'}));
