// Mechanical candidate overlay of actual committed source; never runtime admission.
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const read=p=>fs.readFileSync(path.join(here,p)),json=p=>JSON.parse(read(p));
const selection=json('PAYLOAD_SELECTION.json'),parents=json('PARENTS.json');
const parent=selection.parent,hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(args,extra={})=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',...args],{cwd:root,windowsHide:true,maxBuffer:192000000,...extra});
const head=git(['rev-parse','HEAD']).toString().trim();
const pins=[];
function script(p){const row=parents.rows.find(x=>x.target===p);assert(row);const b=read(row.capture);assert.equal(hash(b),row.sha256);assert.deepEqual(git(['show',parent+':'+p]),b);assert.deepEqual(git(['show',':'+p]),b);assert.deepEqual(fs.readFileSync(path.join(root,p)),b);pins.push({file:p,sha256:hash(b)});return b.toString();}
const lib=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
script('scripts/cloudtms-db-release-lib.mjs');
const managedSource=script('scripts/cloudtms-managed-release-sql.mjs'),definerSource=script('scripts/bpay-next-final-definers.mjs');
const paths=git(['ls-tree','-r','-z','--name-only',parent,'--','supabase']).toString().split('\0').filter(p=>/[.](?:sql(?:inc)?|inc)$/i.test(p));
const tracked=new Set(paths),snapshot=new Map(),batch=git(['cat-file','--batch'],{input:paths.map(p=>parent+':'+p+'\n').join('')});
let offset=0;for(const p of paths){const end=batch.indexOf(10,offset),m=batch.subarray(offset,end).toString().match(/^[0-9a-f]+ blob (\d+)$/);assert(m);const n=Number(m[1]);assert(end+1+n<batch.length);snapshot.set(p,Buffer.from(batch.subarray(end+1,end+1+n)));assert.equal(batch[end+1+n],10);offset=end+n+2;}assert.equal(offset,batch.length);
const current=lib.inventory();assert(current.migrations.every(x=>tracked.has(x.path)));assert(current.repeatables.every(x=>tracked.has(x.path)));
const overrides=new Map(selection.targets.map(r=>{const b=read(r.source);assert.equal(hash(b),r.sha256);return [r.target,b];}));
function load(source,context,names){return vm.runInNewContext(source.replace(/^import .*;\r?\n/gm,'').replaceAll('export ','')+'\n;({'+names.join(',')+'})',{Buffer,process:{env:{}},...context});}
function run({omit=null,badRegistry=false,predecessor=false}={}){
 const local=new Map(overrides);if(omit)local.delete(omit);
 if(badRegistry){const r=JSON.parse(local.get('docs/bpay-next/final-definer-closures.json'));r.chains.at(-1).signature='private.d10_missing_owner_v1(uuid)';local.set('docs/bpay-next/final-definer-closures.json',Buffer.from(JSON.stringify(r)));}
 const vfs={...fs,readFileSync(file,options){const relative=path.relative(root,path.resolve(String(file))).replaceAll('\\','/');assert(!relative.startsWith('../')&&!path.isAbsolute(relative));const b=local.get(relative)??snapshot.get(relative);assert(b,'MISSING_COMPLETE_SOURCE '+relative);return typeof options==='string'||options?.encoding?b.toString(typeof options==='string'?options:options.encoding):Buffer.from(b);}};
 const managed=load(managedSource,{fs:vfs,path,crypto},['scanManagedSql','readManagedClosure']);
 const candidate={migrations:[...current.migrations],repeatables:[...current.repeatables]};
 for(const x of selection.targets){if(/^supabase\/migrations\/[^/]+[.]sql$/i.test(x.target))candidate.migrations.push({path:x.target,sha256:hash(overrides.get(x.target))});if(/^supabase\/repeatable\/[^/]+[.]sql$/i.test(x.target)){const c=managed.readManagedClosure(x.target,root);candidate.repeatables.push({path:x.target,sha256:c.closureHash,paths:[...c.files.keys()]});}}
 const order=(a,b)=>lib.sqlDateKey(a.path).localeCompare(lib.sqlDateKey(b.path))||a.path.localeCompare(b.path);candidate.migrations.sort(order);candidate.repeatables.sort(order);
 const definers=load(definerSource,{fs:vfs,path,crypto,repoRoot:root,inventory:()=>candidate,readManagedClosure:managed.readManagedClosure,scanManagedSql:managed.scanManagedSql,spawnSync:()=>{throw Error('NO_SHELL_OR_NATIVE_FROM_OVERLAY');}},['definerInventory','closeFinalDefinerSelection','verifyFinalDefiners']);
 const pending=predecessor?'supabase/repeatable/13102026_1730_healthroster_checklist_private_read_v1.sql':selection.module;
 const closed=definers.closeFinalDefinerSelection({migrations:candidate.migrations.filter(x=>x.path===selection.admission),repeatables:candidate.repeatables.filter(x=>x.path===pending)},candidate);
 assert(closed.repeatables.some(x=>x.path===selection.module));
 const verified=definers.verifyFinalDefiners({changed:new Set(),selectedRoots:closed.repeatables.map(x=>x.path),selectedMigrations:closed.migrations.map(x=>x.path)});
 const inv=definers.definerInventory();
 const owners=json('SOURCE_PROVENANCE.json').owners.map(o=>{const d=inv.routines.get(o.id)?.at(-1);assert.equal(d?.root,selection.module);assert.equal(d.physical,json('ASSEMBLY.json').ownerPages[o.id]);return {signature:o.id,finalRoot:d.root,finalPhysical:d.physical};});
 return {candidate:{migrations:candidate.migrations.length,repeatables:candidate.repeatables.length},verified,closedRoots:closed.repeatables.map(x=>x.path),owners};
}
const positive=run(),predecessor=run({predecessor:true});
assert.deepEqual(positive.candidate,{migrations:current.migrations.length+1,repeatables:current.repeatables.length+1});
const negatives=[];
for(const setup of [{omit:json('ASSEMBLY.json').ownerPages['private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)']},{badRegistry:true}]){let refusal;try{run(setup);}catch(e){refusal=e.message;}assert(refusal);negatives.push({setup,refusal});}
const gate=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));script('scripts/bpay-next-source-gates.mjs');
const rawCensus=[];
for(const row of selection.targets.filter(x=>/[.]sql(?:inc)?$/i.test(x.target))){const s=read(row.source).toString();gate.assertNextExactNumericTypes(s,row.target);gate.assertSqlWriteBoundaries(s,row.target);rawCensus.push({target:row.target,hazards:gate.specialConstructViolations(s,row.target)});}
assert.deepEqual(rawCensus,json('RAW_F23_CENSUS.json'));
assert.equal(git(['rev-parse','HEAD']).toString().trim(),head,'HEAD_MOVED_DURING_SOURCE_CHECK');
const result={version:'HEALTH_D10_ACTUAL_LOADER_OVERLAY_V1',parent,before:{migrations:current.migrations.length,repeatables:current.repeatables.length},positive,predecessorReapply:predecessor,negativeCases:negatives,scriptPins:pins,rawF23Hazards:rawCensus.reduce((n,x)=>n+x.hazards.length,0),sourceInventoryCaseInsensitive:true,globalSourceGate:'ROOT_REVIEW_AND_FINITE_CLASSIFICATION_REQUIRED_NOT_RUN',native:'NOT_RUN',rootWrites:0};
if(process.argv.includes('--verify'))assert.deepEqual(json('SOURCE_CHECK_RESULTS.json'),JSON.parse(JSON.stringify(result)));else fs.writeFileSync(path.join(here,'SOURCE_CHECK_RESULTS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({before:result.before,candidate:positive.candidate,verification:positive.verified,negativeCases:negatives.length,rawF23Hazards:result.rawF23Hazards,native:'NOT_RUN'}));
