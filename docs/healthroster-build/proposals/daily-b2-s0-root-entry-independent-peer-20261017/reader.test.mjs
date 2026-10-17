import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const original=fs.readFileSync(here+'/captured/payload/docs/healthroster-build/proposals/daily-b2-s0-publish-stageplans-20261017.mjs','utf8');
const code=original.slice(0,original.indexOf('\nif(process.argv[1]')).replace(/^import[^\n]*\n/gm,'').replace(/^export /gm,'');
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const file='docs/healthroster-build/v6-owner-delivery-20261016/SYNTHETIC_PUBLIC.json',head='a'.repeat(40),history='b'.repeat(40),bytes=Buffer.from('{"synthetic":true}\n');
function reader(mode='ok') {
 let heads=0; const calls=[];
 const memoryFs={lstatSync(f){assert.equal(f,root+'/'+file);return {isFile:()=>mode!=='directory',isSymbolicLink:()=>mode==='symlink',size:mode==='empty'?0:mode==='oversize'?2000001:bytes.length}},readFileSync(f){assert.equal(f,root+'/'+file);return mode==='saved'?Buffer.from('changed'):bytes},realpathSync:{native(f){return mode==='redirect'?f+'-redirect':f}}};
 const memoryCp={execFileSync(exe,args){assert.equal(exe,'git');const at=args.indexOf('-C');assert.equal(args[at+1],root);const a=args.slice(at+2);calls.push(a);if(a[0]==='rev-parse'){heads++;return Buffer.from(mode==='moved'&&heads===2?'c'.repeat(40):head)}if(a[0]==='merge-base'){if(mode==='nonancestor')throw Error('SYNTHETIC_NONANCESTOR');assert.equal(JSON.stringify(a),JSON.stringify(['merge-base','--is-ancestor',history,head]));return Buffer.alloc(0)}if(a[0]==='show'){const prefix=a[1].split(':')[0];assert.equal(a[1].slice(prefix.length+1),file);if(mode==='index'&&prefix===''||mode==='current'&&prefix===head||mode==='history'&&prefix===history)return Buffer.from('changed');return bytes}throw Error('UNEXPECTED_PROCESS_CALL')}};
 const context=vm.createContext({Buffer,Uint8Array,JSON,createHash,path,fs:memoryFs,cp:memoryCp,process:{env:{}},pathToFileURL(){throw Error('NO_IMPORT_ALLOWED')},fileURLToPath(){throw Error('NO_IMPORT_ALLOWED')}});
 new vm.Script(code+'\nglobalThis.makeReader=committedReader;').runInContext(context);
 return {read:context.makeReader(root),calls};
}
test('actual reader admits exact saved/index/current/history and checks ancestor and end HEAD',()=>{const {read,calls}=reader();assert(read(file,history).equals(bytes));assert.equal(calls.filter(x=>x[0]==='rev-parse').length,2);assert.equal(calls.filter(x=>x[0]==='show').length,3);assert.equal(calls.filter(x=>x[0]==='merge-base').length,1)});
for(const mode of ['saved','index','current','history'])test('actual reader refuses '+mode+' image drift',()=>{assert.throws(()=>reader(mode).read(file,history),/CURRENT_COMPLETE_PUBLIC_IMAGES_REQUIRED/)});
test('actual reader refuses nonancestor history before accepting images',()=>{assert.throws(()=>reader('nonancestor').read(file,history),/SYNTHETIC_NONANCESTOR/)});
test('actual reader refuses end HEAD movement',()=>{assert.throws(()=>reader('moved').read(file,history),/PUBLIC_HEAD_MOVED/)});
for(const mode of ['directory','symlink','empty','oversize','redirect'])test('actual reader refuses '+mode+' public metadata',()=>{assert.throws(()=>reader(mode).read(file,history),/BOUNDED_PHYSICAL_PUBLIC_REQUIRED/)});
test('actual reader rejects private path, traversal, absolute path and noncanonical history',()=>{for(const f of ['.codex-tmp/PRIVATE_BINDING.json','docs/healthroster-build/../bad.json',root+'/'+file])assert.throws(()=>reader().read(f),/PUBLIC_FILE_REQUIRED/);assert.throws(()=>reader().read(file,'B'.repeat(40)),/ACTUAL_HISTORY_REQUIRED/)});
