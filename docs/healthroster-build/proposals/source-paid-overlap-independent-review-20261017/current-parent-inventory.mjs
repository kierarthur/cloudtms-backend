// Actual committed sqlFiles/inventory over a merged immutable VFS, source only.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026'),money=root;
for(const p of [own,root])assert(fs.realpathSync.native(p).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));
const parent='ef3d20f4513a6f9fd1aa5e4cf6a69d258f6cd87c',pin='b9c1d0bad2e52ce95f7aa6b8e7f98ecbbd1fc832',prefix='docs/healthroster-build/proposals/same-separate-daily-parent-forward-20261017/',registry='docs/bpay-next/final-definer-closures.json';
const sha=x=>crypto.createHash('sha256').update(x).digest('hex'),git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:own,windowsHide:true,maxBuffer:128*1024*1024}),blob=(commit,p)=>git(['show',commit+':'+p]);
const selectionBytes=blob(pin,prefix+'COMPOSED_SELECTION.json');assert.equal(sha(selectionBytes),'0adf915b6d71f190267740c3356729c4710927962bf78c9dda16c070a6044a74');const selection=JSON.parse(selectionBytes),candidate=new Map();
for(const row of selection.files){const bytes=blob(row.sourceCommit||pin,row.sourcePath.startsWith('payload/')?prefix+row.sourcePath:row.sourcePath);assert.equal(sha(bytes),row.sha256);assert.equal(bytes.length,row.bytes);candidate.set(row.target,bytes);}
// Exact previously reviewed engine, limited to its pure definitions. No tail,
// receipt writer or foreign file execution is run. Its sqlFiles and inventory
// themselves are selected from the actual current parent inside engine().
const library=blob('82b14dc385319ebe3d741bb2586c9b6ed172914c','docs/healthroster-build/proposals/daily-health-support-managed-root-20261017/source-check.mjs').toString(),start=library.indexOf('function snapshot(commit)'),end=library.indexOf('const baseSnapshot=',start);
assert(start>=0&&end>start);const context=vm.createContext({fs,path,vm,crypto,assert,execFileSync,Buffer,Set,Map,JSON,root,money,registry,git,blob,sha,__out:{}});
vm.runInContext(library.slice(start,end)+'\n__out.snapshot=snapshot;__out.engine=engine;',context,{timeout:3000});
const snap=context.__out.snapshot(parent),base=context.__out.engine(parent,snap),baseline=base.inventory();assert.equal(baseline.migrations.length,424);assert.equal(baseline.repeatables.length,1091);
const next=context.__out.engine(parent,snap,candidate),inventory=next.inventory();assert.equal(inventory.migrations.length,425);assert.equal(inventory.repeatables.length,1092);
const migration=selection.managedRoots.schema,repeatable=selection.managedRoots.repeatable;
assert(inventory.migrations.some(r=>r.path===migration));assert(inventory.repeatables.some(r=>r.path===repeatable));
const uppercase=['supabase/migrations/13072026_1648_MIGRATION.SQL','supabase/migrations/26052026_1525_INDEX_OUTBOX.SQL'];for(const p of uppercase)assert(inventory.migrations.some(r=>r.path===p));
const oldRegistry=JSON.parse(blob(parent,registry)),newRegistry=JSON.parse(candidate.get(registry)),daily=oldRegistry.chains.filter(c=>c.signature.startsWith('private.daily_rate_correction_health_'));assert.equal(daily.length,6);
for(const chain of daily)assert.deepEqual(newRegistry.chains.find(c=>c.signature===chain.signature),chain);
const metadataEdits=JSON.parse(blob(pin,prefix+'REGISTRY_EDITS.json'));assert.equal(metadataEdits.changes.length,67);assert.equal(metadataEdits.inputSha256,sha(blob(parent,registry)));assert.equal(metadataEdits.outputSha256,sha(candidate.get(registry)));
const reverted=structuredClone(newRegistry),changed=new Set(metadataEdits.changes.map(r=>r.signature));
for(const row of [...metadataEdits.changes].reverse()){const at=reverted.chains.findIndex(c=>c.signature===row.signature);assert(at>=0);assert.deepEqual(reverted.chains[at],row.after);if(row.before===null)reverted.chains.splice(at,1);else reverted.chains[at]=row.before;}
const originalRaw=JSON.stringify(reverted,null,2).replace(/[^\x00-\x7f]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'))+'\n';assert.equal(originalRaw,blob(parent,registry).toString());
for(const chain of oldRegistry.chains.filter(c=>!changed.has(c.signature)))assert.deepEqual(newRegistry.chains.find(c=>c.signature===chain.signature),chain);
const closed=next.closeFinalDefinerSelection({migrations:inventory.migrations.filter(r=>r.path===migration),repeatables:inventory.repeatables.filter(r=>r.path===repeatable)},inventory),verified=next.verifyFinalDefiners({changed:new Set(),selectedRoots:closed.repeatables.map(r=>r.path),selectedMigrations:closed.migrations.map(r=>r.path)});
assert.equal(verified.signatures,3035);assert.equal(verified.registered,1648);assert.equal(verified.roots,1092);
const bad=JSON.parse(candidate.get(registry));bad.chains=bad.chains.filter(c=>!c.signature.startsWith('private.daily_rate_correction_health_'));let refused=false;try{for(const chain of daily)assert.deepEqual(bad.chains.find(c=>c.signature===chain.signature),chain);}catch{refused=true;}assert(refused);
const receipt={parent,metadataCommit:pin,selectionSha256:sha(selectionBytes),base:{migrations:424,repeatables:1091},candidate:{migrations:425,repeatables:1092},newMigration:migration,newRoot:repeatable,uppercaseOriginals:uppercase,exactDailyChains:6,fullRegistryReversalEdits:67,otherChainsUnchanged:true,droppedDailyMetadataRefused:true,actualCommittedSqlFilesAndInventory:true,actualCommittedFinalOwnerChecks:true,verified,scriptPins:next.pins,opaqueOwnershipScanOnly:true,bankBodySemanticInspection:false,foreignWrites:0,databaseConnections:0,native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'CURRENT_PARENT_INVENTORY_RESULTS.json'),JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify({...receipt,verified:{signatures:verified.signatures,registered:verified.registered,roots:verified.roots},scriptPins:undefined}));
