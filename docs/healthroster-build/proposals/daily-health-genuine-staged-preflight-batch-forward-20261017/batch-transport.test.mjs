// Real local Git object transport. Synthetic files only; no private state, browser or native calls.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
import {blobImages,assertExecutionContext,reviewedImage} from './git-image-batch.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
function repositories(run){
 const parent=path.resolve(here,'../../../..').replaceAll('\\','/')+'/.codex-tmp';fs.mkdirSync(parent,{recursive:true});const dir=fs.mkdtempSync(parent+'/daily-batch-git-').replaceAll('\\','/');
 const make=name=>{const root=dir+'/'+name;fs.mkdirSync(root);const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','user.name=Offline Batch Image','-c','user.email=offline@example.invalid','-c','commit.gpgsign=false','-c','core.hooksPath='+dir+'/empty-hooks','-C',root,...a],{windowsHide:true,stdio:['ignore','pipe','ignore']});git('init','--quiet');return {root,git};};
 try{return run(make);}finally{assert(fs.realpathSync.native(dir).replaceAll('\\','/')===dir&&dir.startsWith(parent+'/daily-batch-git-'));fs.rmSync(dir,{recursive:true});}
}
test('real batch bytes equal individual Git show for HEAD/index/history, binary/empty/duplicate blobs; missing or unbounded expressions refuse',()=>repositories(make=>{
 const {root,git}=make('source'),binary=Buffer.from('binary\0\r\n0123456789 blob 999\nnot-a-header\n');fs.writeFileSync(root+'/binary.dat',binary);fs.writeFileSync(root+'/empty.dat','');git('add','--','.');git('commit','--quiet','-m','Reviewed images');const commit=git('rev-parse','HEAD').toString().trim();
 const rows=['binary.dat','empty.dat'].flatMap(file=>[{ref:commit,file},{ref:null,file},{ref:commit,file}]),images=blobImages(root,rows);
 for(const {ref,file}of rows)assert(images.get(ref,file).equals(git('show',(ref??'')+':'+file)));
 assert.throws(()=>blobImages(root,[{ref:commit,file:'missing.dat'}]),/REAL_BLOB/);
 for(const row of [{ref:'HEAD',file:'binary.dat'},{ref:commit,file:'../binary.dat'},{ref:commit,file:'binary.dat\nHEAD:other'}])assert.throws(()=>blobImages(root,[row]));
 assert.throws(()=>blobImages(root,Array.from({length:4097},()=>rows[0])));assert.throws(()=>images.get(commit,'not-selected.dat'));
}));
test('registered Report execution is separate from I2 effect/setup commits; copied I2 execution refuses',()=>repositories(make=>{
 const op=make('operator'),data=make('root');fs.writeFileSync(op.root+'/source.mjs','exact reviewed source\n');op.git('add','--','.');op.git('commit','--quiet','-m','Operator freeze');const reviewed=op.git('rev-parse','HEAD').toString().trim();
 const effect={operatorSourceCommit:reviewed};fs.writeFileSync(data.root+'/effect.json',JSON.stringify(effect));data.git('add','--','.');data.git('commit','--quiet','-m','Commit root effect dossier');
 assert.equal(effect.operatorSourceCommit,op.git('rev-parse','HEAD').toString().trim());assert.equal(reviewedImage(op.root,reviewed,['source.mjs']).reviewedCommit,reviewed);
 fs.writeFileSync(data.root+'/SETUP_ENTRY.md','exact committed stage body');data.git('add','--','.');data.git('commit','--quiet','-m','Commit exact setup');assert.equal(effect.operatorSourceCommit,op.git('rev-parse','HEAD').toString().trim());
 const report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',i2='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';assertExecutionContext(report,i2);assert.throws(()=>assertExecutionContext(i2,i2),/REGISTERED_REPORT/);assert.throws(()=>assertExecutionContext(report,report));
}));
