// Offline tests/manifest only; no network or database.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const result=execFileSync(process.execPath,['--test',path.join(here,'reader.test.mjs')],{encoding:'utf8',windowsHide:true});
if(!/(?:#|ℹ) pass 10/.test(result)||!/(?:#|ℹ) fail 0/.test(result))throw Error('FIXED_READER_OFFLINE_TESTS_FAILED');
const grammar=JSON.parse(fs.readFileSync(path.join(here,'GRAMMAR_RESULT.json'),'utf8'));
if(grammar.cases.length!==6||grammar.native!=='NOT_RUN')throw Error('FIXED_READER_GRAMMAR_REQUIRED');
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({scope:'OFFLINE_SOURCE_ONLY',tests:10,pass:10,fail:0,
 grammarPrograms:6,canonicalCompleteQueryBytesPreserved:true,canonicalCompleteQueryASTPreserved:true,
 native:'NOT_RUN',priorAttempt3:'HELD',metadataReaderNativeLifecycle:'NOT_RUN',finiteMcpAdapter:'ROOT_IMPLEMENTATION_REQUIRED',
 rawLogsRetained:false,credentialsRead:false,databaseCalls:0},null,2)+'\n');
const files=fs.readdirSync(here).filter(f=>f!=='SOURCE_MANIFEST.json').sort().map(file=>{const b=fs.readFileSync(path.join(here,file));return{file,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'CONTACT_LARGE_CANONICAL_FIXED_METADATA_PROPOSAL_V1',
 parentHistoricalReport:'fb3aac4d163c1b7a4520e0ff42120fc484cf4f46',rootSource:'39aa5b30042711956b83a7bf899f416d2f3b4771',files,
 sourceVerdict:'NARROW_OWNER_FIXED_READ_ONLY_METADATA_ROUTE_SUPPORTED; COMPLETE_MCP_ADAPTER_AND_NATIVE_REVIEW_PENDING',native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({status:'FIXED_METADATA_SOURCE_ONLY_FROZEN',files:files.length,sourceChecks:10,grammarPrograms:6,
 manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json')))}));
