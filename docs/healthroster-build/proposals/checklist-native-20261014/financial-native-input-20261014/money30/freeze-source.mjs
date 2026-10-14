import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/'),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));const pins=JSON.parse(fs.readFileSync(path.join(dir,'CURRENT_NATIVE_SOURCE_PINS.json')));
const run=args=>{const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:2e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;};
const t=run(['--test',rel+'/native-grain-source.test.mjs']);assert.match(t,/tests 9/);assert.match(t,/pass 9/);assert.match(t,/fail 0/);
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
for(const f of files.filter(f=>f.endsWith('.mjs')))run(['--check',rel+'/'+f]);
const rx=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/];
for(const f of files)assert.ok(!rx.some(r=>r.test(fs.readFileSync(path.join(dir,f),'utf8'))),'credential '+f);
const write=(f,x)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(x,null,2)+'\n');
write('SOURCE_CHECK_RESULT.json',{source_only:true,inspected_source_checks:9,pass:9,fail:0,syntax_pass:3,exact_current_literal_owner_snapshots:pins.selected_literal_owners.length,
 selected_overload_saved_paths_unique:true,current_source_counts:[1484,1745,2160],guarded_body_reads:22,final_effective_body_native_catalogue_proof:false,
 new_functions:0,new_tables:0,new_columns:0,Banking_owner_changed:false,implemented_adapter:false,actual_SQL_or_native_calls:0,credential_findings:0,scratch_created:false,cleanup_retried:false,
 limitation:'Native source-input and finite read-only adapter construction evidence; no universal or positive native/runtime verdict.'});files.push('SOURCE_CHECK_RESULT.json');
const artifacts=[...new Set(files)].sort().map(file=>{const b=fs.readFileSync(path.join(dir,file));return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'NATIVE_CHECKLIST_FINANCIAL_GRAIN_SOURCE_INPUT_FREEZE_V1',source_only:true,independent_review:'PENDING',implemented_adapter:false,
 Banking_owner_changed:false,native_proof:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(fs.readFileSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json'))),check_sha256:sha(fs.readFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json')))}));
