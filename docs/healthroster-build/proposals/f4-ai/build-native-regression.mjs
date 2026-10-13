import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root,'explicit root source input');
const nativeModule=pathToFileURL(path.join(root,'broker/src/invoice-async-http.js'));
const nativeTest=pathToFileURL(path.join(root,'tests/invoice_async_backend.test.js'));
const proposed=fs.readFileSync(path.join(here,'invoice-async-http.complete-proposed.js'),'utf8');
const module=proposed.replace(/from '([^']+)'/g,(_,specifier)=>`from '${specifier==='./weekly-source/agency-invoice-credit-consumer.mjs'
  ?pathToFileURL(path.join(here,'agency-invoice-credit-consumer.mjs')).href:new URL(specifier,nativeModule).href}'`);
const outputModule=path.join(here,'native-regression-runtime.mjs');fs.writeFileSync(outputModule,module);
const testBytes=fs.readFileSync(fileURLToPath(nativeTest));
const testSource=testBytes.toString('utf8').replace(/from '([^']+)'/g,(_,specifier)=>specifier.startsWith('.')
  ?`from '${specifier==='../broker/src/invoice-async-http.js'?pathToFileURL(outputModule).href:new URL(specifier,nativeTest).href}'`
  :`from '${specifier}'`).replace(/import\.meta\.url/g,JSON.stringify(nativeTest.href))
  +fs.readFileSync(path.join(here,'AI_NATIVE_HTTP_CASES.mjsfrag'),'utf8');
const testPath=path.resolve(here,'../../../../tests/healthroster-f4-ai-native-runtime-regression.test.mjs');
fs.writeFileSync(testPath,testSource);
const testOnly=path.join(here,'invoice_async_backend.complete-proposed-tests.js');
fs.writeFileSync(testOnly,testBytes.toString('utf8')+fs.readFileSync(path.join(here,'AI_NATIVE_HTTP_CASES.mjsfrag'),'utf8'));
const diff=spawnSync('git',['diff','--no-index','--',fileURLToPath(nativeTest),testOnly],{encoding:'utf8',windowsHide:true,maxBuffer:8*1024*1024});
assert.ok([0,1].includes(diff.status),diff.stderr);const lines=diff.stdout.split('\n');
fs.writeFileSync(path.join(here,'AI_NATIVE_TEST_ROOT.diff'),'--- a/tests/invoice_async_backend.test.js\n+++ b/tests/invoice_async_backend.test.js\n'
  +lines.slice(lines.findIndex(line=>line.startsWith('@@'))).join('\n'));
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
fs.writeFileSync(path.join(here,'NATIVE_REGRESSION_INPUT.json'),JSON.stringify({status:'EXISTING_MOCK_SUITE_WITH_PROPOSED_COMPLETE_HTTP_OWNER',
  input:{path:fileURLToPath(nativeTest),sha256:sha(testBytes)},output:{path:testPath,sha256:sha(Buffer.from(testSource))},
  adaptation:'Only import/source-reference locations changed in the existing 86 cases. Four adjacent actual-handler cases added; all receipts/capabilities remain mocks.'},null,2)+'\n');
console.log(JSON.stringify({status:'REGRESSION_SOURCE_GENERATED',test:testPath}));
