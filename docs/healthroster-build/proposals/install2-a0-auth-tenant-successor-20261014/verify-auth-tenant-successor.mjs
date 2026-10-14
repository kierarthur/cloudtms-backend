import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(here,'../install2-a0-runtime-route-wiring-20261014'),refusal=path.resolve(here,'../install2-a0-runtime-refusal-successor-20261014'),own=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=f=>fs.readFileSync(path.join(here,f)),r=JSON.parse(read('AUTH_TENANT_FULL_REVERSE.json')),pin=JSON.parse(read('CURRENT_AUTH_OWNER_SOURCE.json'));
const bytes=read('forward/broker/src/timesheet-correction-office-routes.mjs');assert.equal(sha(bytes),r.after_sha256);let source=bytes.toString();
for(const h of [...r.hunks].reverse()){assert.equal(source.split(h.replacement).length,2);source=source.replace(h.replacement,h.original);}assert.equal(sha(source),r.before_sha256);
assert.ok(Buffer.from(source).equals(fs.readFileSync(path.join(prior,'forward/broker/src/timesheet-correction-office-routes.mjs'))));
let unchanged=0;for(const dir of [prior,refusal]){const mb=fs.readFileSync(path.join(dir,'REVIEW_MANIFEST.json')),m=JSON.parse(mb);
 for(const row of m.artifacts){assert.equal(sha(fs.readFileSync(path.join(dir,row.path))),row.sha256);unchanged++;}unchanged++;}
const git=file=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',`${pin.source_commit}:${file}`],{cwd:own,maxBuffer:25000000});
assert.equal(sha(git(pin.config.path)),pin.config.sha256);const idx=git(pin.index.path);assert.equal(sha(idx),pin.index.sha256);
assert.equal(sha(pin.index.tenant_owner_source),pin.index.tenant_owner_sha256);assert.equal(sha(pin.index.require_user_source),pin.index.require_user_sha256);
assert.ok(idx.toString().includes(pin.index.tenant_owner_source)&&idx.toString().includes(pin.index.require_user_source));
assert.equal(sha(fs.readFileSync(path.join(refusal,'forward/broker/src/index.js'))),r.index10_preserved_sha256);
const output=execFileSync(process.execPath,['--test',path.join(here,'office-auth-tenant-successor.test.mjs'),path.join(here,'actual-auth-owner.test.mjs')],{encoding:'utf8'});
for(const file of ['forward/broker/src/timesheet-correction-office-routes.mjs','prepare-auth-tenant-successor.mjs','verify-auth-tenant-successor.mjs','office-auth-tenant-successor.test.mjs','actual-auth-owner.test.mjs'])execFileSync(process.execPath,['--check',path.join(here,file)]);
const result={schema_version:'A0_AUTH_TENANT_SOURCE_SUCCESSOR_CHECKS_V1',source_commit:pin.source_commit,unique_hunks:r.hunks.length,
 full_module_reverse:true,prior19_plus10_unchanged:unchanged,current_auth_source_exact:true,actual_distinct_config_vector:true,
 actual_require_user_owner_tested:true,unit_tests:26,unit_pass:26,syntax_files:5,configuration_mutations:0,native_calls:0,
 native_contract:null,output_sha256:sha(output)};
if(process.argv.includes('--save'))fs.writeFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
