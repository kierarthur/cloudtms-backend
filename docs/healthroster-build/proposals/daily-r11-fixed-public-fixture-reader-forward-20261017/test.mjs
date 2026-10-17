import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import {createHash} from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const H=path.dirname(fileURLToPath(import.meta.url)),F=JSON.parse(fs.readFileSync(H+'/FORWARD.json')),old=fs.readFileSync(H+'/parent/publisher.mjs','utf8'),next=fs.readFileSync(H+'/payload/publisher.mjs','utf8'),fixture=fs.readFileSync(H+'/parent/HISTORICAL_SOURCE_FIXTURE.json'),fixed=F.parentImages[1].file,ROOT=F.root,REPORT='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',C='c'.repeat(40),E='e'.repeat(40),K='a'.repeat(40),normal='docs/healthroster-build/public.json';
function harness(source=next,o={}){
 let bytes=o.bytes??fixture,calls=[],reads=0,headReads=0;const publicBytes=file=>o.entry&&file===F.target?Buffer.from(next):o.entry?Buffer.from('controlled public source'):bytes;
 const fakeFs={lstatSync:f=>({size:o.size??publicBytes(f.slice(ROOT.length+1)).length,isFile:()=>o.regular!==false,isSymbolicLink:()=>o.symlink===true}),realpathSync:{native:f=>o.physicalWrong?f+'/redirect':f},readFileSync:f=>{reads++;return publicBytes(f.slice(ROOT.length+1));}};
 const cp={execFileSync:(_bin,args)=>{let at=args.indexOf('-C'),root=args[at+1],a=args.slice(at+2);calls.push({root,args:a});if(a[0]==='rev-parse'){
  if(a[1]==='HEAD')return Buffer.from(root===REPORT?K:(o.headMoved&&headReads++>0?'d'.repeat(40):C));
  if(a[1]==='--show-toplevel')return Buffer.from(root);
  if(a[1]==='--absolute-git-dir')return Buffer.from('D:/Development/GitHub/cloudtms-backend/.git/worktrees/'+path.basename(root));
  if(a[1]==='--git-common-dir')return Buffer.from('D:/Development/GitHub/cloudtms-backend/.git');
 }if(a[0]==='merge-base'){if(o.ancestor===false)throw Error('CONTROLLED_NON_ANCESTOR');return Buffer.alloc(0);}if(a[0]==='show'){
  const ref=a[1],file=ref.slice(ref.indexOf(':')+1);if(o.drift!==undefined&&ref.startsWith(o.drift+':'))return Buffer.from('controlled drift');return publicBytes(file);
 }throw Error('UNEXPECTED_GIT_'+a.join(','));}};
 const context=vm.createContext({fs:fakeFs,cp,createHash,path,fileURLToPath,pathToFileURL,Buffer,process:{env:{},argv:[]},console});
 const code=source.replace(/^import[^\n]*\n/m,'').replace(/^export /gm,'').split("if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))")[0];
 new vm.Script(code+'\nglobalThis.API={committedReader,entryImages};').runInContext(context);
 return {api:context.API,calls,reads:()=>reads};
}
test('real reviewed 2,975,604-byte fixture reproduces old bound HOLD',()=>{const h=harness(old);assert.throws(()=>h.api.committedReader()(fixed,F.parentCommit),/BOUNDED_PHYSICAL_PUBLIC_REQUIRED/);assert.equal(h.reads(),0);});
test('only exact size/SHA fixture is read and all four actual reader images remain required',()=>{const h=harness();assert.ok(h.api.committedReader()(fixed,F.parentCommit).equals(fixture));assert.deepEqual(h.calls.filter(x=>x.args[0]==='show').map(x=>x.args[1]),[C+':'+fixed,':'+fixed,F.parentCommit+':'+fixed]);assert.ok(h.calls.some(x=>x.args[0]==='merge-base'));});
test('rewritten same-size fixture refuses even if all controlled Git images agree',()=>{let b=Buffer.from(fixture);b[0]^=1;assert.throws(()=>harness(next,{bytes:b}).api.committedReader()(fixed),/EXACT_LARGE_PUBLIC_RECIPE_REQUIRED/);});
test('truncated fixture refuses on metadata before content read',()=>{const h=harness(next,{bytes:fixture.subarray(0,-1)});assert.throws(()=>h.api.committedReader()(fixed),/BOUNDED_PHYSICAL_PUBLIC_REQUIRED/);assert.equal(h.reads(),0);});
test('read-length mismatch despite claimed lstat size refuses',()=>assert.throws(()=>harness(next,{bytes:fixture.subarray(0,-1),size:fixture.length}).api.committedReader()(fixed),/EXACT_LARGE_PUBLIC_RECIPE_REQUIRED/));
for(const file of [normal,fixed.replace('/HISTORICAL_SOURCE_FIXTURE.json','/OTHER.json'),fixed.replace('HISTORICAL_SOURCE_FIXTURE','historical_source_fixture'),fixed+'/extra',fixed.replace('source-recipe-20261017/','source-recipe-20261017-copy/')])test('same large bytes at other path refuse: '+file,()=>assert.throws(()=>harness().api.committedReader()(file),/BOUNDED_PHYSICAL_PUBLIC_REQUIRED/));
test('unchanged ordinary cap admits exact decimal2e6',()=>assert.equal(harness(next,{bytes:Buffer.alloc(2000000)}).api.committedReader()(normal).length,2000000));
test('ordinary cap refuses decimal2e6+1',()=>assert.throws(()=>harness(next,{bytes:Buffer.alloc(2000001)}).api.committedReader()(normal),/BOUNDED_PHYSICAL_PUBLIC_REQUIRED/));
for(const [name,o] of [['symlink',{symlink:true}],['not regular',{regular:false}],['physical redirect',{physicalWrong:true}]])test('actual reader retains '+name+' refusal',()=>assert.throws(()=>harness(next,o).api.committedReader()(fixed),/BOUNDED_PHYSICAL_PUBLIC_REQUIRED/));
for(const ref of ['',C,F.parentCommit])test('actual reader retains changed '+(ref||'index')+' refusal',()=>assert.throws(()=>harness(next,{drift:ref}).api.committedReader()(fixed,F.parentCommit),/CURRENT_COMPLETE_PUBLIC_IMAGES_REQUIRED/));
test('actual ancestry failure remains refusal',()=>assert.throws(()=>harness(next,{ancestor:false}).api.committedReader()(fixed,F.parentCommit),/CONTROLLED_NON_ANCESTOR/));
test('actual end-HEAD movement remains refusal',()=>assert.throws(()=>harness(next,{headMoved:true}).api.committedReader()(fixed),/PUBLIC_HEAD_MOVED/));
test('unsafe non-public/traversal path refuses before metadata',()=>{const h=harness();for(const f of ['.codex-tmp/storage-state.json','docs/healthroster-build/../x'])assert.throws(()=>h.api.committedReader()(f),/PUBLIC_FILE_REQUIRED/);assert.equal(h.reads(),0);});
test('new real entrypoint E is accepted by unchanged actual entryImages reader contract',()=>{const h=harness(next,{entry:true});h.api.entryImages({operatorCommit:K,entrypointCommit:E});const selected=h.calls.filter(x=>x.args[0]==='show'&&x.args[1]===E+':'+F.target);assert.equal(selected.length,1);assert.ok(!h.calls.some(x=>x.args[0]==='show'&&x.args[1]===F.parentCommit+':'+F.target));});
test('complete raw parent inverse changes only three declared reader source spans',()=>{let s=next;for(const e of [...F.edits].reverse()){assert.equal(s.split(e.after).length-1,1);s=s.replace(e.after,e.before);}assert.equal(s,old);assert.equal(F.edits.length,3);assert.equal(next.slice(next.indexOf('function entryImages')),old.slice(old.indexOf('function entryImages')));});
