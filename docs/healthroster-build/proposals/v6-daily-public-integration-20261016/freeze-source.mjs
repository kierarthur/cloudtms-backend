import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const result=execFileSync(process.execPath,['--test',path.join(here,'original-http.test.mjs'),path.join(here,'integration.test.mjs')],{encoding:'utf8',windowsHide:true});
if(!/(?:#|ℹ) pass 22/.test(result)||!/(?:#|ℹ) fail 0/.test(result))throw Error('DAILY_PUBLIC_SOURCE_TESTS_FAILED');
const grammar=JSON.parse(fs.readFileSync(path.join(here,'GRAMMAR_RESULT.json'),'utf8'));
if(grammar.completeFunctions.length!==4||grammar.expandedRootStatements!==18)throw Error('DAILY_PUBLIC_COMPLETE_SOURCE_GRAMMAR_REQUIRED');
fs.writeFileSync(path.join(here,'SOURCE_CHECKS.json'),JSON.stringify({scope:'LOCAL_SOURCE_ONLY_ISOLATED_TRANSPORT_FIXTURES_NOT_NATIVE_RECEIPTS',
 tests:22,pass:22,fail:0,originalHttpWholeImportReverse:true,wholeCurrentParentRawReverse:true,wholeProposedParentNodeSyntax:'PASS',
 soleOfficeDtoWholeGenerationExact:true,currentNative15Unchanged:true,newFunctions:4,expandedPG17Statements:18,
 databaseCalls:0,credentialFilesRead:0,freshEconomicsComplete:false,native:'NOT_RUN',PartB:'NOT_RUN'},null,2)+'\n');
const files=[];
function walk(dir){for(const d of fs.readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,d.name);if(d.isDirectory())walk(p);else{const file=path.relative(here,p).replaceAll('\\','/');if(file!=='SOURCE_MANIFEST.json'){const b=fs.readFileSync(p);files.push({file,bytes:b.length,sha256:sha(b)});}}}}
walk(here);files.sort((a,b)=>a.file.localeCompare(b.file));
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({version:'HEALTH_DAILY_PUBLIC_INTEGRATION_SOURCE_V1',
 sourceStatus:'FOUR_COMPLETE_SERVICE_CALLERS_AND_ACTUAL_ROUTER_PROPOSAL; FRESH_ECONOMIC_COMPOSITION_HELD',
 historicalPredecessor:'7fda1321502f1576487dd86889aa22ebe7896766',files,native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({status:'DAILY_PUBLIC_SOURCE_FROZEN',files:files.length,tests:22,functions:4,expandedStatements:18,
 manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),freshEconomicsComplete:false,native:'NOT_RUN'}));
