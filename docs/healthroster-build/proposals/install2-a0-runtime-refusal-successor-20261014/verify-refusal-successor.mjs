import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(here,'../install2-a0-runtime-route-wiring-20261014'),own=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=f=>fs.readFileSync(path.join(here,f)),r=JSON.parse(read('CURRENT_REFUSAL_FULL_REVERSE.json'));
const after=read('forward/broker/src/index.js');assert.equal(sha(after),r.after_sha256);
let reversed=after.toString();for(const h of [...r.hunks].reverse()){assert.equal(reversed.split(h.replacement).length,2);reversed=reversed.replace(h.replacement,h.original);}
assert.equal(sha(reversed),r.before_sha256);assert.ok(Buffer.from(reversed).equals(fs.readFileSync(path.join(prior,'forward/broker/src/index.js'))));
const old=JSON.parse(fs.readFileSync(path.join(prior,'CURRENT_INDEX_FULL_REVERSE.json')));for(const h of [...old.hunks].reverse())reversed=reversed.replace(h.replacement,h.original);
const git=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',`${r.input_commit}:broker/src/index.js`],{cwd:own,maxBuffer:25000000});assert.ok(Buffer.from(reversed).equals(git));
const guard=JSON.parse(read('CURRENT_GUARD_REFUSAL_OWNER.json'));assert.equal(sha(guard.source),guard.sha256);assert.ok(git.toString().includes(guard.source));
const manifest=fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json'));assert.equal(sha(manifest),r.prior19_manifest_sha256);
const oldManifest=JSON.parse(manifest);for(const f of oldManifest.artifacts)assert.equal(sha(fs.readFileSync(path.join(prior,f.path))),f.sha256);
const output=execFileSync(process.execPath,['--test',path.join(here,'office-route-wiring-successor.test.mjs')],{encoding:'utf8'});
for(const f of ['forward/broker/src/index.js','prepare-refusal-successor.mjs','verify-refusal-successor.mjs','office-route-wiring-successor.test.mjs'])execFileSync(process.execPath,['--check',path.join(here,f)]);
const result={schema_version:'A0_CURRENT_REFUSAL_SUCCESSOR_SOURCE_CHECKS_V1',input_commit:r.input_commit,unique_hunks:2,
 full_current_reverse:true,prior19_artifacts_unchanged:oldManifest.artifacts.length+1,unit_tests:21,unit_pass:21,syntax_files:4,
 exact_current_guard_wrapper:true,native_calls:0,output_sha256:sha(output),native_contract:null};
if(process.argv.includes('--save'))fs.writeFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
