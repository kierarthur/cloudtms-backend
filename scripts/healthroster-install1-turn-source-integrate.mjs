// Source-only integration. Retain exact predecessor caller and frozen lane pins.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {repoRoot} from './cloudtms-db-release-lib.mjs';import {digest} from './cloudtms-managed-release-sql.mjs';
const lane='c18c4ee7703fac27422f38e92cbd1ca391ebdfe7',laneBase='171778d7214c2dad98ce392981fecd2f47db20b6';
const base='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const folder='codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026';
const git=args=>execFileSync('git',args,{cwd:repoRoot,encoding:'utf8',maxBuffer:16000000,windowsHide:true});
const read=(commit,file)=>git(['show',`${commit}:${file}`]);
const manifestFile=folder+'/BUILD_RELEASE_TURN_FROZEN_MANIFEST_20261014.json',manifest=JSON.parse(read(lane,manifestFile));
if(read(base,'scripts/request-test-release.mjs')!==read(laneBase,'scripts/request-test-release.mjs'))throw Error('BANKING_RELEASE_CALLER_CHANGED_COMPOSE_REQUIRED');
const selected=manifest.completeFiles.filter(row=>/^(scripts|tests)\//.test(row.path));
if(selected.length!==7)throw Error('TURN_LANE_FILE_CENSUS_DRIFT');
const files=selected.map(row=>{const source=read(lane,row.path);if(digest(source)!==row.committedLfSha256)throw Error('TURN_FROZEN_FILE_DRIFT');return {file:row.path,source};});
for(const file of ['scripts/healthroster-full-rehearsal-protocol.mjs','tests/healthroster-full-rehearsal-protocol.test.mjs'])files.push({file,source:read(laneBase,file)});
for(const file of [manifestFile,folder+'/BUILD_RELEASE_TURN_SOURCE_REVIEW_20261014.md'])files.push({file,source:read(lane,file)});
for(const {file} of files){const full=path.join(repoRoot,file);if(fs.existsSync(full)&&fs.readFileSync(full,'utf8').replaceAll('\r\n','\n')!==read(base,file).replaceAll('\r\n','\n'))throw Error('TURN_INTEGRATION_TARGET_DIRTY:'+file);}
for(const {file,source} of files){const full=path.join(repoRoot,file);fs.mkdirSync(path.dirname(full),{recursive:true});fs.writeFileSync(full,source);}
fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/TURN_SOURCE_INTEGRATION.json'),JSON.stringify({status:'SOURCE_INTEGRATED_FORWARD_KEY_TAG_FIX_REQUIRED',base,lane,laneBase,
 manifestSha256:digest(read(lane,manifestFile)),files:files.map(row=>({file:row.file,sha256:digest(row.source)})),
 retainedBankingCallerBase:true,noNativeClaim:true,
 requiredCorrection:'SEND7 takes a two-int32 advisory key: its pg_locks objsubid is 2. The frozen lane used 1; root will correct it forward before native use, preserving frozen original.'},null,2)+'\n');
console.log(JSON.stringify({status:'TURN_SOURCE_INTEGRATED_NATIVE_USE_HELD',files:files.length}));
