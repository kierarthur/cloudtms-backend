import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const walk=d=>fs.readdirSync(d,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(d,e.name)):[path.join(d,e.name)]);
for(const [file,count] of [['SOURCE_TEST_OUTPUT.txt',170],['WHOLE_TRANSPORT_OUTPUT.txt',81]]){const o=fs.readFileSync(path.join(here,file),'utf8');
 assert.match(o,new RegExp('tests '+count+'(?:\\r?\\n|$)'));assert.match(o,new RegExp('pass '+count+'(?:\\r?\\n|$)'));assert.match(o,/fail 0(?:\r?\n|$)/);}
const previous='docs/healthroster-build/proposals/checklist-native-20261014/SOURCE_PINS.json',original=execFileSync('git',['cat-file','blob','aea33728bb76c4d2c57a9e82abffa570e743a0f0:'+previous],{cwd:repo,maxBuffer:4e6});
assert.ok(original.equals(fs.readFileSync(path.join(repo,previous))));const pins=JSON.parse(original);assert.equal(pins.source_pins.length,193);
for(const p of pins.source_pins)assert.equal(sha(fs.readFileSync(path.join(repo,p.path))),p.saved_sha256,p.path);
const syntax=[];for(const f of walk(here).filter(f=>f.endsWith('.mjs'))){execFileSync(process.execPath,['--check',f],{cwd:repo});syntax.push(path.relative(repo,f).replaceAll('\\','/'));}
const lexical=[];for(const f of walk(path.join(here,'forward')).filter(f=>f.endsWith('.sqlinc'))){const p=scanManagedSql(fs.readFileSync(f,'utf8'));assert.equal(p.meta.length,0);lexical.push({file:path.relative(repo,f).replaceAll('\\','/'),statements:p.statements.length});}
const artifacts=walk(here).filter(f=>path.basename(f)!=='SOURCE_MANIFEST.json').concat(path.join(repo,'tests/healthroster-checklist-ordinary-next-hold.test.mjs')).sort().map(f=>{const b=fs.readFileSync(f);return{file:path.relative(repo,f).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});
const result={status:'UNMOUNTED_FINITE_ORDINARY_NEXT_AVAILABLE_EX_VAT_HOLD_SOURCE_CALLER_SUCCESSOR',source_only:true,native:false,SQL_executed:false,public_root_mounted:false,
 predecessor:'9b44412d7fdda7f8e099036e7c8a7297a8eeb795',predecessor_193_pins_unchanged:true,source_cases:170,source_failed:0,
 whole_successor_predecessor_compatibility_cases:81,compatibility_failed:0,TRUE_native_parity:false,FALSE_native_purity:false,compiled_crypto_ABI:false,
 whole_caller_performance:false,ordinary_LEGACY:false,Daily_null_Contract:false,own_calendar_mapping:false,treatment:false,whole_row_amounts:false,full_process_completion:false,
 syntax,lexical,artifacts};fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({...result,artifacts:artifacts.length}));
