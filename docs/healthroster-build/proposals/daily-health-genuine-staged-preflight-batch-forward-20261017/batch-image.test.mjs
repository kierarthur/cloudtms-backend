// Real isolated D: Git history, synthetic text only; no auth/browser/native/network.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {reviewedImage as assertReviewedImage} from './git-image-batch.mjs';import {sha,entryText} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';import {assertSetupEntry} from '../daily-health-genuine-staged-operator-20261017/stages.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
test('entry then actual setup-manifest commit admits the exact historical image; dirty/index/HEAD/runtime/nonancestor changes refuse',()=>{
 const parent=path.resolve(here,'../../../..').replaceAll('\\','/')+'/.codex-tmp';fs.mkdirSync(parent,{recursive:true});
 const dir=fs.mkdtempSync(parent+'/daily-genuine-git-').replaceAll('\\','/');assert.equal(fs.realpathSync.native(dir).replaceAll('\\','/'),dir);
 const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','user.name=Offline Fixture Source Test','-c','user.email=offline-source@example.invalid','-c','commit.gpgsign=false','-c','core.hooksPath='+dir+'/empty-hooks','-C',dir,...a],{windowsHide:true,stdio:['ignore','pipe','ignore']}).toString().trim();
 const write=(p,s)=>{fs.mkdirSync(path.dirname(dir+'/'+p),{recursive:true});fs.writeFileSync(dir+'/'+p,s);};
 const selected=['operator/source.mjs','operator/MANIFEST.json','runtime/current.js'];
 try{
  git('init','--quiet');for(const p of selected)write(p,'reviewed '+p+'\n');write('TEST_SETUP_MANIFEST.md','retained original\n');
  git('add','--','.');git('commit','--quiet','-m','Exact reviewed source');const reviewed=git('rev-parse','HEAD');
  const before=Buffer.from('synthetic observed native envelope'),effect=Buffer.from('synthetic actual-root effect dossier'),receipt={sqlSha256:sha('sql'),verificationSqlSha256:sha('fresh')};
  const entry={sourceCommit:reviewed,beforeSqlSha256:receipt.sqlSha256,verificationSqlSha256:receipt.verificationSqlSha256,beforeEnvelopeSha256:sha(before),effectAdmissionSha256:sha(effect)};
  // Actual mandatory commit occurs AFTER entry generation and advances the execution HEAD.
  const manifest='retained original\n'+entryText('S1',entry);write('TEST_SETUP_MANIFEST.md',manifest);git('add','--','TEST_SETUP_MANIFEST.md');git('commit','--quiet','-m','Append exact stage entry');
  const execution=git('rev-parse','HEAD');assert.notEqual(execution,reviewed);
  assert.ok(assertSetupEntry(fs.readFileSync(dir+'/TEST_SETUP_MANIFEST.md','utf8'),entry,'S1',before,receipt,effect));
  assert.deepEqual(assertReviewedImage(dir,entry.sourceCommit,selected),{reviewedCommit:reviewed,executionCommit:execution,selectedImagesUnchanged:true});
  // Unstaged source, index-only source and genuinely committed source changes each refuse.
  const file='runtime/current.js',original=fs.readFileSync(dir+'/'+file);write(file,'dirty runtime');assert.throws(()=>assertReviewedImage(dir,reviewed,selected));
  fs.writeFileSync(dir+'/'+file,original);assertReviewedImage(dir,reviewed,selected);
  write(file,'index runtime');git('add','--',file);fs.writeFileSync(dir+'/'+file,original);assert.throws(()=>assertReviewedImage(dir,reviewed,selected));
  git('add','--',file);assertReviewedImage(dir,reviewed,selected);
  write(file,'changed committed runtime');git('add','--',file);git('commit','--quiet','-m','Runtime drift is not a docs successor');assert.throws(()=>assertReviewedImage(dir,reviewed,selected));
  // Even an exact image on a separate root commit cannot pass the ancestry boundary.
  const tree=git('rev-parse',reviewed+'^{tree}'),unrelated=git('commit-tree',tree,'-m','Unrelated exact image');assert.throws(()=>assertReviewedImage(dir,unrelated,selected),/ANCESTRY/);
 }finally{
  assert(dir.startsWith(parent+'/daily-genuine-git-')&&fs.realpathSync.native(dir).replaceAll('\\','/')===dir);fs.rmSync(dir,{recursive:true});
 }
});
