// Read-only source verification. Never imports or invokes the actual root adapter.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(here,f));
const sha=b=>createHash('sha256').update(b).digest('hex'),json=f=>JSON.parse(read(f));
const git=(repo,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',repo,...args],{windowsHide:true,maxBuffer:8*1024*1024,stdio:['ignore','pipe','pipe'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
assert.match(fs.realpathSync.native(here).replaceAll('\\','/'),/^D:\/Development\/GitHub\//);
const manifest=json('MANIFEST.json');
for(const r of manifest.files){
 assert.equal(typeof r.file,'string');assert(!path.isAbsolute(r.file)&&!r.file.split('/').includes('..'));
 const b=read(r.file);assert.equal(b.length,r.bytes,r.file);assert.equal(sha(b),r.sha256,r.file);
}
const pins=json('SOURCE_PINS.json');
for(const r of pins.pins){
 assert.match(r.repository,/^D:\/Development\/GitHub\/\.codex-worktrees\//);
 assert.equal(fs.realpathSync.native(r.repository).replaceAll('\\','/'),r.repository);
 const b=fs.readFileSync(r.repository+'/'+r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);
 for(const revision of [r.commit,'HEAD',''])assert(b.equals(git(r.repository,'show',revision+':'+r.file)),r.file+' '+revision);
}
const forward=json('FULL_PARENT_FORWARD.json'),parent=read(forward.completeParent),post=read('run.mjs');
assert.equal(parent.length,forward.beforeBytes);assert.equal(sha(parent),forward.beforeSha256);
assert.equal(post.length,forward.afterBytes);assert.equal(sha(post),forward.afterSha256);
let inverse=post.toString('utf8');
for(const e of [...forward.edits].reverse()){
 assert.equal(inverse.split(e.after).length,2,'unique literal postimage');
 inverse=inverse.replace(e.after,()=>e.before);
}
assert(Buffer.from(inverse).equals(parent),'complete raw parent inverse');
assert(parent.equals(git(pins.pins[0].repository,'show',forward.parentCommit+':'+forward.parentFile)));
const slice=(s,start,end)=>s.slice(s.indexOf(start),s.indexOf(end,s.indexOf(start)));
for(const [start,end] of [['export function checkedPublicReader','\nfunction packet'],['function packet','\nexport async function run']])
 assert.equal(slice(post.toString(),start,end),slice(parent.toString(),start,end),'unchanged full original guard');
assert.equal((parent.toString().match(/\r\n/g)||[]).length,0);assert.equal((post.toString().match(/\r\n/g)||[]).length,0);
const fixture=json('TEST_FIXTURE_FULL_PARENT_FORWARD.json'),oldFixture=read(fixture.completeParent),newFixture=read('reference/data-fixtures.mjs');
assert.equal(oldFixture.length,fixture.beforeBytes);assert.equal(sha(oldFixture),fixture.beforeSha256);
assert.equal(newFixture.length,fixture.afterBytes);assert.equal(sha(newFixture),fixture.afterSha256);
assert(newFixture.equals(Buffer.concat([oldFixture,Buffer.from(fixture.append)])));
assert(oldFixture.equals(git(pins.pins[0].repository,'show',fixture.parentCommit+':'+fixture.parentFile)));
assert(read('reference/recorder.mjs').equals(fs.readFileSync(pins.pins[3].repository+'/'+pins.pins[3].file)));
const grammar=['run.mjs','validate.mjs','test.mjs','data.test.mjs','verify.mjs','reference/data-fixtures.mjs','reference/recorder.mjs'];
for(const file of grammar)execFileSync(process.execPath,['--check',path.join(here,file)],{windowsHide:true,stdio:['ignore','pipe','pipe']});
console.log(JSON.stringify({status:'PASS_BOUNDED_SOURCE_ONLY',manifestFiles:manifest.files.length,sourcePins:pins.pins.length,wholeRuntimeInverse:true,literalEdits:forward.edits.length,originalReaderAndPacketUnchanged:true,wholeTestFixtureInverse:true,grammarInputs:grammar.length,adapterRun:'NOT_RUN',nativePrivateRootWrites:'NONE'}));
