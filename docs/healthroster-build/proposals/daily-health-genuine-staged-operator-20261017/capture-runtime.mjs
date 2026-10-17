import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6});
const file='broker/src/daily-canonical-authority.js',commit='f743b81fe537830d81b1de93ec4f9cb0331532a7';
const b=git('show',commit+':'+file);assert.equal(sha(b),'64cd2109da90f9ee277c2829a734683e7e06649ebebe14b1e2d296ac1c04d00b');
fs.mkdirSync(here+'/reference/published',{recursive:true});fs.writeFileSync(here+'/reference/published/daily-canonical-authority.js',b,{flag:'wx'});
const front='D:/Development/GitHub/TEST-Frontend',frontFile='js/main.js';
const frontBytes=fs.readFileSync(front+'/'+frontFile);assert.equal(sha(frontBytes),'02b44699ffef0dabab73b2ce8743d62195f003e86a3879881909a04f11cdc684');
const receipt='docs/healthroster-build/v6-owner-delivery-20261016/TEST_AUTH_REFRESH_R4_SUCCESS_20261017.json';
const safe=git('show','c6f0e9f124db7d2b9846d97526a1ddf158b11e4a:'+receipt);
assert.equal(sha(safe),'f31df2271a058efff80934a62fae580d00f5145e3716682fa0304797c0adc632');
fs.writeFileSync(here+'/reference/TEST_AUTH_REFRESH_R4_SUCCESS_20261017.json',safe,{flag:'wx'});
const runtime=JSON.parse(fs.readFileSync(new URL('../test-auth-refresh-suppressed-startup-forward-20261017/SOURCE_PINS.json',import.meta.url)));
const source={version:'DAILY_GENUINE_OPERATOR_PUBLIC_SOURCE_PINS_V1',root,
 auditCommit:'e1c13ecb123224aea0fa95beff3727cd4c58bbb2',publishedCalculator:{commit,file,bytes:b.length,sha256:sha(b),captured:'reference/published/daily-canonical-authority.js'},
 authObservation:{commit:'c6f0e9f124db7d2b9846d97526a1ddf158b11e4a',file:receipt,sha256:sha(safe)},
 frontend:{root:front,commit:'7a75111ec2923693e75feef4a7ccbcb6ecba942e',file:frontFile,bytes:frontBytes.length,sha256:sha(frontBytes)},
 localRuntime:runtime.localRuntime,native:'NOT_RUN',deployedRuntime:'ROOT_ACTIVE_RUNTIME_BINDING_REQUIRED',privateStateHandling:'PATH_METADATA_ONLY_PARENT_NO_STATE_READ_HASH_COPY'};
fs.writeFileSync(here+'/SOURCE_PINS.json',JSON.stringify(source,null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({publishedCalculatorBytes:b.length,publicAuthReceiptBound:true,privateFilesRead:0}));
