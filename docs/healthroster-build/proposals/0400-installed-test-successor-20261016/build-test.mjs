// Bounded installed-test successor. No integrator edits.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),beforeDir=path.resolve(dir,'../0400-compiler-proposal-32ee8711-20261016'),target='tests/healthroster-capability-predecessor-boundary.test.mjs',hash=x=>createHash('sha256').update(x).digest('hex'),edits=[];
let source=fs.readFileSync(path.join(beforeDir,'forward',target),'utf8');const before=source;
function replace(from,to){assert.equal(source.split(from).length,2,'One exact installed-test anchor');edits.push({from,to,count:1});source=source.replace(from,to);}
replace("import test from'node:test';", "import{execFileSync}from'node:child_process';\nimport{createRequire}from'node:module';\nimport test from'node:test';");
replace("const baseCompiler=await import(pathToFileURL(path.join(sourceRoot,'scripts/cloudtms-managed-release.mjs'))),baseHarness=await import(pathToFileURL(path.join(sourceRoot,'scripts/healthroster-install1-full-rehearsal.mjs')));",String.raw`const preservedProposalPath='docs/healthroster-build/v6-owner-delivery-20261016/CAPABILITY_PREDECESSOR_COMPILER_SOURCE_20261016/PROPOSAL.json';
const compilerProposal=JSON.parse(fs.readFileSync(path.join(targetRoot,preservedProposalPath)));
assert.equal(compilerProposal.rootCommit,'32ee8711f54829b5a13a0c59dabc9c29a39a2aa3');
const frozenCommit=compilerProposal.rootCommit,gitRoot=process.env.HEALTHROSTER_FROZEN_BASELINE_GIT_ROOT||sourceRoot;
async function frozenBaselineGraph(){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'hr0400-frozen32ee-')),sources=new Map(),barePackageRewrites=[],executedFiles=[],packageRootRequire=createRequire(path.join(gitRoot,'package.json'));
 function dispose(){const at=fs.realpathSync(root);assert.equal(path.dirname(at),fs.realpathSync(os.tmpdir()));assert(path.basename(at).startsWith('hr0400-frozen32ee-'));fs.rmSync(at,{recursive:true,force:false});}
 function collect(file){
  assert(!file.startsWith('../')&&!path.posix.isAbsolute(file)&&/\.(?:[cm]?js|json|sql)$/.test(file));if(sources.has(file))return;
  const bytes=execFileSync('git',['show',frozenCommit+':'+file],{cwd:gitRoot,maxBuffer:10e6}),text=bytes.toString();sources.set(file,text);
  const to=path.join(root,file);fs.mkdirSync(path.dirname(to),{recursive:true});fs.writeFileSync(to,bytes);
  if(!/\.[cm]?js$/.test(file))return;
  const imports=[...text.matchAll(/(?:^|[;\n])[ \t]*(?:import\b[^;]*?\bfrom\s*|export[ \t]+\{[^}]*\}[ \t]+from\s*|import[ \t]+)(['"])([^'"]+)\1/g),...text.matchAll(/\bimport\(\s*(['"])([^'"]+)\1/g)];let executable=text;const changes=[];
  for(const match of imports){const spec=match[2];if(spec.startsWith('.')){collect(path.posix.normalize(path.posix.join(path.posix.dirname(file),spec)));continue;}if(spec.startsWith('node:'))continue;
   assert.equal(spec,'xlsx','Only the existing exact locked bare package may resolve outside the frozen graph');collect('package.json');collect('package-lock.json');
   const locked=JSON.parse(sources.get('package-lock.json')).packages['node_modules/xlsx'];assert(locked?.version);
   assert.equal(digest(canonical(fs.readFileSync(path.join(gitRoot,'package-lock.json'),'utf8'))),digest(sources.get('package-lock.json')),'Current dependency lock must equal32ee');
   const entry=packageRootRequire.resolve(spec);let packageDir=path.dirname(entry);while(!fs.existsSync(path.join(packageDir,'package.json'))){const parent=path.dirname(packageDir);assert.notEqual(parent,packageDir);packageDir=parent;}const installed=JSON.parse(fs.readFileSync(path.join(packageDir,'package.json')));assert.equal(installed.name,spec);assert.equal(installed.version,locked.version);
   const esm=installed.exports?.['.']?.import;assert.equal(esm,'./xlsx.mjs');const esmEntry=path.resolve(packageDir,esm);assert(esmEntry.startsWith(packageDir+path.sep));
   const from=match[0],url=pathToFileURL(esmEntry).href,toToken=from.replace(match[1]+spec+match[1],match[1]+url+match[1]);assert.equal(executable.split(from).length,2);executable=executable.replace(from,toToken);changes.push({from,to:toToken});barePackageRewrites.push({file,kind:'BARE_PACKAGE_SPECIFIER_ONLY',package:spec,version:locked.version,entrySha256:digest(fs.readFileSync(esmEntry)),lockSha256:digest(sources.get('package-lock.json')),from,to:toToken});
  }
  let reversed=executable;for(const change of changes.toReversed()){assert.equal(reversed.split(change.to).length,2);reversed=reversed.replace(change.to,change.from);}assert.equal(reversed,text);fs.writeFileSync(to,executable);executedFiles.push({file,originalSha256:digest(text),executedSha256:digest(executable),fullReverseEquality:true});
 }
 try{
  for(const p of compilerProposal.pins){collect(p.file);assert.equal(digest(sources.get(p.file)),p.beforeSha256,'Exact frozen critical baseline '+p.file);}collect('supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql');
  const compiler=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release.mjs'))),harness=await import(pathToFileURL(path.join(root,'scripts/healthroster-install1-full-rehearsal.mjs'))),lib=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs'))),sql=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'))),policy=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-execution-policy.mjs')));
  const files=[...sources].map(([file,text])=>({file,sha256:digest(text)})).sort((a,b)=>a.file.localeCompare(b.file));
  console.log('HR_CAPABILITY_FROZEN_BASELINE '+JSON.stringify({commit:frozenCommit,completeFiles:files.length,relativeImportRewrites:0,currentBackendModulesBorrowed:0,barePackageRewrites,graphSha256:digest(JSON.stringify(files)),executedGraphSha256:digest(JSON.stringify(executedFiles.sort((a,b)=>a.file.localeCompare(b.file)))),allReverseEquality:executedFiles.every(f=>f.fullReverseEquality),criticalFiles:compilerProposal.pins.map(p=>({file:p.file,sha256:p.beforeSha256}))}));
  return{root,sources,compiler,harness,lib,sql,policy,files,dispose};
 }catch(error){dispose();throw error;}
}
const frozenBaseline=await frozenBaselineGraph(),baseCompiler=frozenBaseline.compiler,baseHarness=frozenBaseline.harness;
test.after(()=>frozenBaseline.dispose());
// Installed runs use their real repository inventory. The isolated default-path
// simulation may enumerate the real Git source through the exact unchanged lib;
// this never supplies a baseline compiler/harness or invents an inventory count.
const repoInventory=path.resolve(gitRoot)===path.resolve(sourceRoot)?inventory:(await import(pathToFileURL(path.join(gitRoot,'scripts/cloudtms-db-release-lib.mjs')))).inventory;
assert.equal(digest(canonical(fs.readFileSync(path.join(gitRoot,'scripts/cloudtms-db-release-lib.mjs'),'utf8'))),digest(frozenBaseline.sources.get('scripts/cloudtms-db-release-lib.mjs')));
const inventoryForSource=()=>repoInventory();`);
replace("old=JSON.parse(fs.readFileSync(path.join(sourceRoot,file)))","old=JSON.parse(fs.readFileSync(path.join(frozenBaseline.root,file)))");
replace("const proposal=JSON.parse(fs.readFileSync(path.resolve(targetRoot,'../PROPOSAL.json')));", "const proposal=JSON.parse(fs.readFileSync(path.join(targetRoot,preservedProposalPath)));");
replace("assert.equal(value,canonical(fs.readFileSync(path.join(sourceRoot,p.file),'utf8')));", "assert.equal(value,frozenBaseline.sources.get(p.file));");
replace('const current=inventory(),release=','const current=inventoryForSource(),release=');
replace('const current=inventory(),review=','const current=inventoryForSource(),review=');
for(const pin of ['MONEY_PREDECESSOR_MIGRATION','CONTACT_CLAIM_MIGRATION']){
 const before=`const i={path:${pin}.path,sha256:${pin}.contentSha256},before=baseCompiler.compileManagedUnit(i,'migration',{root:sourceRoot,mapSource:mapExecutableSqlSource})`;
 replace(before,`const i={path:${pin}.path,sha256:${pin}.contentSha256},before=baseCompiler.compileManagedUnit(i,'migration',{root:sourceRoot,mapSource:frozenBaseline.lib.mapExecutableSqlSource})`);
}
source+=String.raw`
test('installed preservation baseline is exact original Git32ee code and cannot borrow current compiler/harness',()=>{
 assert.notEqual(baseCompiler.compileManagedUnit,compileManagedUnit);assert.notEqual(baseCompiler.unitCheckpointSql,unitCheckpointSql);assert.notEqual(baseHarness.compiledAtomicRehearsalUnit,compiledAtomicRehearsalUnit);
 assert(!baseCompiler.compileManagedUnit.toString().includes('capabilityPredecessorCompanion'));assert(!baseHarness.compiledAtomicRehearsalUnit.toString().includes('capabilityPrebody'));
 for(const p of compilerProposal.pins){assert.equal(digest(frozenBaseline.sources.get(p.file)),p.beforeSha256);assert.equal(digest(canonical(fs.readFileSync(path.join(targetRoot,p.file),'utf8'))),p.afterSha256);}
 assert(frozenBaseline.sources.has('scripts/healthroster-money-predecessor-boundary.mjs'));assert(frozenBaseline.sources.has('scripts/healthroster-contact-claim-boundary.mjs'));assert.equal(frozenBaseline.sources.has('scripts/healthroster-capability-predecessor-boundary.mjs'),false);
});
`;
// Separate appended test from exact reversible edits.
const appended=source.slice(source.indexOf("\ntest('installed preservation baseline is exact original Git32ee"));
let reverse=source.slice(0,-appended.length);for(const e of edits.toReversed()){assert.equal(reverse.split(e.to).length,2);reverse=reverse.replace(e.to,e.from);}assert.equal(reverse,before);
fs.mkdirSync(path.join(dir,'forward/tests'),{recursive:true});fs.writeFileSync(path.join(dir,'forward',target),source);
const out={version:'HEALTHROSTER_CAPABILITY0400_INSTALLED_TEST_SUCCESSOR_V1',baseProposalCommit:'7fbafccff632603e9d5e59ac960296353195e5b5',frozenBaselineCommit:'32ee8711f54829b5a13a0c59dabc9c29a39a2aa3',target,beforeSha256:hash(before),afterSha256:hash(source),preservedProposalPath:'docs/healthroster-build/v6-owner-delivery-20261016/CAPABILITY_PREDECESSOR_COMPILER_SOURCE_20261016/PROPOSAL.json',edits,appendedTest:appended,fullReverseEquality:true,native:'NOT_RUN',rootEdits:0};fs.writeFileSync(path.join(dir,'PROPOSAL.json'),JSON.stringify(out,null,2)+'\n');console.log(JSON.stringify({edits:edits.length,afterSha256:out.afterSha256,reverse:true}));
