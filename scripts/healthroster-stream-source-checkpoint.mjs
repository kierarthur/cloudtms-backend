// One-time source/evidence checkpoint; no database/API/credential access.
import fs from 'node:fs';import assert from 'node:assert/strict';import {execFileSync,spawnSync} from 'node:child_process';import {createHash} from 'node:crypto';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:40000000});
const predecessor='54d57b4367425d63e7121771017d2c2168fdf8cd';assert.equal(git(['rev-parse','HEAD']).toString().trim(),predecessor);
const files=git(['diff','--cached','--name-only','-z']).toString().split('\0').filter(Boolean),rules=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/];
const pins=files.map(file=>{const bytes=fs.readFileSync(file);assert.deepEqual(bytes,git(['show',':'+file]),file);assert.ok(rules.every(re=>!re.test(bytes.toString())),file+' credential scan');return {file,bytes:bytes.length,sha256:sha(bytes)};});
const syntax=[];for(const p of pins.filter(p=>p.file.endsWith('.mjs'))){const r=spawnSync(process.execPath,['--check',p.file],{encoding:'utf8',windowsHide:true});assert.equal(r.status,0);syntax.push({file:p.file,exitCode:0});}
const changed=['scripts/healthroster-install1-archive-preflight.mjs','scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-install1-native-coordinator.mjs'];
const reverse=git(['diff','--cached','--binary',predecessor,'--',...changed]);const r=spawnSync('git',['apply','--reverse','--check','-'],{input:reverse,encoding:'utf8',windowsHide:true});assert.equal(r.status,0);
const whitespace=spawnSync('git',['diff','--cached','--check'],{encoding:'utf8',windowsHide:true});assert.equal(whitespace.status,0);
assert.equal(sha(JSON.stringify(JSON.parse(fs.readFileSync('supabase/release/current-contract.json')))),'7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6');
const result={status:'DIAGNOSTIC_DURABILITY_SOURCE_BACKUP_NATIVE_HELD',predecessor,pins,syntax,reverseSha256:sha(reverse),wholeChangedOwnerReversePassed:true,rawStagedWhitespaceExitCode:0,selectedFileCredentialFindings:0,
 observedRootSourceGroups:[{files:['tests/healthroster-rehearsal-stream-observer.test.mjs','tests/healthroster-install1-native-coordinator.test.mjs','tests/healthroster-turn-control.test.mjs'],tests:21,pass:21,fail:0,exitCode:0},{files:['tests/healthroster-install1-full-rehearsal.test.mjs','tests/healthroster-full-rehearsal-protocol.test.mjs','tests/healthroster-live-test-restoration.test.mjs'],tests:50,pass:50,fail:0,exitCode:0}],
 observedCredentialScan:{files:7079,exitCode:0},observedDatabaseContractCoupling:{exitCode:0},
 diagnosticOnly:true,qualifiedPartialRecords:false,sqlVerifierBodiesChanged:false,transactionAndFixtureObserversChanged:false,finalNativePublicationGuardsChanged:false,
 native:false,installation:false,restartCauseFixed:false,fourthRehearsalDispatched:false,independentSourceReview:'PENDING',futureExactCommittedArchiveGate:'REQUIRED'};
const file='docs/healthroster-build/install1-rehearsal/STREAM_DIAGNOSTIC_SOURCE_CHECKPOINT_14102026.json';fs.writeFileSync(file,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({status:result.status,pins:pins.length,syntax:syntax.length,sourceTests:71,sha256:sha(fs.readFileSync(file))}));
