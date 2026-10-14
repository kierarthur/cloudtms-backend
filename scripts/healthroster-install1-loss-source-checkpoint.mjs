// One-time source checkpoint after the recorded effect-free source test runs.
import fs from 'node:fs';import assert from 'node:assert/strict';import {execFileSync,spawnSync} from 'node:child_process';import {createHash} from 'node:crypto';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:40000000});
const predecessor='37643920a1f71170fe54804a9eee324a56408769';assert.equal(git(['rev-parse','HEAD']).toString().trim(),predecessor);
const files=git(['diff','--cached','--name-only','-z']).toString().split('\0').filter(Boolean);
const pins=files.map(file=>{const bytes=fs.readFileSync(file);assert.deepEqual(bytes,git(['show',':'+file]),file+' saved/staged bytes');return {file,bytes:bytes.length,sha256:sha(bytes)};});
const syntax=[];for(const p of pins.filter(p=>p.file.endsWith('.mjs'))){const r=spawnSync(process.execPath,['--check',p.file],{encoding:'utf8',windowsHide:true});syntax.push({file:p.file,exitCode:r.status});assert.equal(r.status,0);}
const changed=['scripts/healthroster-install1-full-rehearsal.mjs','tests/healthroster-install1-full-rehearsal.test.mjs'];
const diff=git(['diff','--cached','--binary',predecessor,'--',...changed]);
const reversed=spawnSync('git',['apply','--reverse','--check','-'],{input:diff,encoding:'utf8',windowsHide:true});assert.equal(reversed.status,0);
const whitespace=spawnSync('git',['diff','--cached','--check'],{encoding:'utf8',windowsHide:true});assert.equal(whitespace.status,0);
const contract=sha(JSON.stringify(JSON.parse(fs.readFileSync('supabase/release/current-contract.json'))));assert.equal(contract,'7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6');
const result={status:'SOURCE_STATUS_CORRECTION_AND_LOSS_BACKUP_READY_NATIVE_HELD',predecessor,pins,syntax,fullReverseDiffSha256:sha(diff),fullReversePassed:true,rawStagedWhitespaceExitCode:0,
 observedRootSourceCommands:[{command:'node --test --test-reporter=tap tests/healthroster-install1-full-rehearsal.test.mjs',tests:19,pass:19,fail:0,exitCode:0},{command:'node --test --test-reporter=tap tests/healthroster-install1-native-coordinator.test.mjs',tests:3,pass:3,fail:0,exitCode:0},{command:'node scripts/cloudtms-db-release.mjs check',migrations:389,repeatables:1019,exitCode:0},{command:'node scripts/verify-database-contract-coupling.mjs',exitCode:0},{command:'node scripts/verify-source-secret-safety.mjs',files:7072,exitCode:0}],
 sourceOnly:true,sqlVerifierBodiesChanged:false,sharedLockPolicyChanged:false,finalNativePublicationGuardsChanged:false,contractSha256:contract,
 restartCauseFixed:false,fourthRehearsalDispatched:false,installation:false,backgroundJobsPaused:false,independentSourceReview:'PENDING',exactFutureCommittedArchiveGate:'REQUIRED_BEFORE_ANY_NEW_NATIVE_WINDOW'};
const file='docs/healthroster-build/install1-rehearsal/FULL_ATTEMPT3_SOURCE_STATUS_CORRECTION_14102026.json';fs.writeFileSync(file,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({status:result.status,pins:pins.length,syntax:syntax.length,sha256:sha(fs.readFileSync(file))}));
