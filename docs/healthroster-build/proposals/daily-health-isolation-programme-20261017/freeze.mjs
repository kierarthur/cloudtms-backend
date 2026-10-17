// Seal reviewed source only; no native calls.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash}from'node:crypto';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex');
assert(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'));
const raw=execFileSync(process.execPath,['--test','--test-reporter=tap',here+'/test.mjs'],{encoding:'utf8',maxBuffer:1e6});assert(raw.includes('# pass 8')&&raw.includes('# fail 0'));
fs.writeFileSync(here+'/TEST_RESULTS.json',JSON.stringify({tests:8,pass:8,fail:0,names:[...raw.matchAll(/^ok \d+ - (.+)$/gm)].map(m=>m[1]),native:'NOT_RUN',dbConnections:0},null,2)+'\n');
const grammar=JSON.parse(fs.readFileSync(here+'/GRAMMAR_RESULTS.json'));assert.equal(grammar.examples.length,6);assert(grammar.examples.every(e=>e.grammar==='PASS'&&e.nativeBodyCompilation==='NOT_PROVED'));assert.deepEqual(grammar.substitutions,[]);
function list(dir,prefix=''){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>{const f=prefix+e.name;if(f==='MANIFEST.json')return[];assert(!e.isSymbolicLink());return e.isDirectory()?list(path.join(dir,e.name),f+'/'):[f];});}
const files=list(here).sort().map(file=>{const b=fs.readFileSync(here+'/'+file);return{file,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(here+'/MANIFEST.json',JSON.stringify({version:'DAILY_HEALTH_ISOLATION_PROGRAMME_FREEZE_V1',rootCommit:'b4cb19f04be68f67b8aad81692c26e96f7b6f0ed',definitions:4,examples:6,businessFixtureRows:0,executionAdapter:'UNPROVIDED',sourceOnly:true,native:'NOT_RUN',files},null,2)+'\n');
console.log(JSON.stringify({files:files.length+1,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),native:'NOT_RUN',executionAdapter:'UNPROVIDED'}));
