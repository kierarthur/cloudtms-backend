import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const walk=d=>fs.readdirSync(d,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(d,e.name)):[path.join(d,e.name)]);
const immutable='aea33728bb76c4d2c57a9e82abffa570e743a0f0',pinsPath='docs/healthroster-build/proposals/checklist-native-20261014/SOURCE_PINS.json';
const original=execFileSync('git',['cat-file','blob',immutable+':'+pinsPath],{cwd:repo,maxBuffer:4e6});assert.ok(original.equals(fs.readFileSync(path.join(repo,pinsPath))));
const old=JSON.parse(original);assert.equal(old.source_pins.length,193);for(const p of old.source_pins)assert.equal(sha(fs.readFileSync(path.join(repo,p.path))),p.saved_sha256,p.path);
for(const [file,count] of [['SOURCE_TEST_OUTPUT.txt',148],['WHOLE_TRANSPORT_OUTPUT.txt',64]]){
 const output=fs.readFileSync(path.join(here,file),'utf8');assert.match(output,new RegExp('tests '+count+'(?:\\r?\\n|$)'));
 assert.match(output,new RegExp('pass '+count+'(?:\\r?\\n|$)'));assert.match(output,/fail 0(?:\r?\n|$)/);
}
const syntax=[];for(const f of walk(here).filter(f=>f.endsWith('.mjs'))){execFileSync(process.execPath,['--check',f],{cwd:repo});syntax.push(path.relative(repo,f).replaceAll('\\','/'));}
const artifacts=walk(here).filter(f=>path.basename(f)!=='SOURCE_MANIFEST.json').concat(path.join(repo,'tests/healthroster-checklist-invoice-decision-transport.test.mjs')).sort().map(f=>{
 const b=fs.readFileSync(f);return {file:path.relative(repo,f).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});
const manifest={status:'UNMOUNTED_FINITE_NATIVE_INVOICE_PENDING_DECISION_PUBLIC_TRANSPORT_SOURCE_SUCCESSOR',source_only:true,native:false,SQL_executed:false,
 predecessor_invoice_freeze:immutable,predecessor_private_core:'7a553954afe79ec46e1a9270220e19ce330efb0b',predecessor_193_pins_unchanged:true,
 source_cases:148,source_failed:0,whole_successor_predecessor_compatibility_cases:64,compatibility_failed:0,
 TRUE_native_parity:false,FALSE_native_purity:false,typed_composite_native_roundtrip:false,whole_caller_performance:false,public_root_mounted:false,syntax,artifacts};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({...manifest,artifacts:artifacts.length}));
