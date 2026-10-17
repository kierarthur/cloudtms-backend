// Execute the unchanged actual managed loader on only the two new Health roots.
// No whole-repository body inventory or Banking source read is performed.
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),repo=path.resolve(here,'../../../..');
const root=path.resolve(repo,'../healthroster-install2-14102026'),pins=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_PINS.json')));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const parserFile='scripts/cloudtms-managed-release-sql.mjs',parser=fs.readFileSync(path.join(root,parserFile),'utf8');
const {functionEvents}=await import(pathToFileURL(path.join(root,'docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs')).href);
const candidate=new Map(Object.entries(pins.selectedTargets).filter(([p])=>p.startsWith('supabase/')).map(([p,row])=>[p,fs.readFileSync(path.join(root,row.path))]));
const reads=new Set(),virtualFs={readFileSync(file,options){
 const relative=path.relative(root,path.resolve(file)).replaceAll('\\','/');
 assert(candidate.has(relative),'NO_UNPINNED_HEALTH_INPUT:'+relative);reads.add(relative);
 const bytes=candidate.get(relative);return options==='utf8'?bytes.toString():Buffer.from(bytes);
}};
const context=vm.createContext({fs:virtualFs,path,crypto,Buffer});
vm.runInContext(parser.replace(/^import[^\n]+\n/gm,'').replace(/^export\s+/gm,'')+
 '\nthis.loader={readManagedClosure,scanManagedSql};',context,{timeout:3000});
const roots=['supabase/repeatable/17102026_0722_weekly_source_current_read_health_v1.sql',
 'supabase/repeatable/17102026_0741_weekly_source_complete_classification_v1.sql'];
const final=new Map(),rows=[];
for(const file of roots){
 const closure=context.loader.readManagedClosure(file,root),defs=functionEvents(closure.expanded).events.filter(v=>v.kind==='CREATE');
 assert.equal(defs.length,file===roots[0]?12:6);
 for(const definition of defs)final.set(definition.id,{root:file,definition});
 rows.push({root:file,closureSha256:closure.closureHash,files:[...closure.files.keys()],declarations:defs.map(v=>v.id)});
}
const registryPath=pins.selectedTargets['docs/bpay-next/final-definer-closures.json'].path;
const registry=JSON.parse(fs.readFileSync(path.join(root,registryPath)));
assert.equal(final.size,12);
for(const [signature,definition]of final){
 const row=registry.chains.find(v=>v.signature===signature);assert(row,signature);
 assert.equal(row.finalRoot,definition.root);
 const physical=candidate.get(row.finalPhysical);assert(physical,'MISSING_ACTUAL_REGISTERED_FINAL_PHYSICAL:'+signature);
 assert(functionEvents(physical.toString()).events.some(v=>v.kind==='CREATE'&&v.id===signature&&v.definition===definition.definition.definition));
}
const missing='supabase/repeatable/17102026_0722_weekly_source_current_read_health/01_native_status_child.sqlinc';
const saved=candidate.get(missing);candidate.delete(missing);
assert.throws(()=>context.loader.readManagedClosure(roots[0],root),/NO_UNPINNED_HEALTH_INPUT/);candidate.set(missing,saved);
fs.writeFileSync(path.join(here,'MANAGED_HEALTH_CLOSURE_RESULTS.json'),JSON.stringify({actualLoader:{path:parserFile,sha256:sha(parser)},
 roots:rows,distinctFinalHealthSignatures:12,registeredFinalOwnersMatched:12,missingNewChildRefused:true,
 filesRead:[...reads],BankBodyReads:0,globalDefinerInventory:'ROOT_OWNED_NOT_RERUN',native:'NOT_RUN',dbConnections:0},null,2)+'\n');
console.log(JSON.stringify({managedRoots:2,distinctFinalHealthSignatures:12,missingNewChildRefused:true,native:'NOT_RUN'}));
