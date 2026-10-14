// Complete conservative source callee census. No native SQL.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
import {readCurrentNativeIndex,sha} from '../checklist-native-invoice-20261014/read-current-native-index.mjs';
import {sqlTokens,functionEvents} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),native=readCurrentNativeIndex(repo);
const forward=fs.readFileSync(path.join(here,'NATIVE_RELEASE_QUALIFICATION.forward.sqlinc'),'utf8');
const decision=fs.readFileSync(path.join(here,'CHECKLIST_RELEASE_DECISION_READ.sqlinc'),'utf8');
const prerequisiteCommit='aea33728bb76c4d2c57a9e82abffa570e743a0f0';
const prerequisiteFiles=['supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_invoice_block_native.forward.sqlinc',
 'supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_source_invoice_block.sqlinc'];
const prerequisites=prerequisiteFiles.map(file=>({file,raw:execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',prerequisiteCommit+':'+file],{cwd:repo,maxBuffer:3e6})}));
const local=new Map(functionEvents(forward+'\n'+decision+'\n'+prerequisites.map(x=>x.raw.toString()).join('\n'))
 .events.filter(e=>e.definition&&e.name.startsWith('private.')).map(e=>[e.id,e]));
const queue=[...local.keys()],seen=new Map(),saved=[];
const byName=new Map(native.byName);for(const r of local.values())byName.set(r.name,[r]);
while(queue.length){const id=queue.shift();if(seen.has(id))continue;const r=local.get(id)??native.index.live.get(id);assert.ok(r,id);
 const references=native.references(r.body),callees=references.flatMap(name=>(byName.get(name)??[]).map(x=>x.id));
 const words=sqlTokens(r.body).filter(t=>t.kind==='word').map(t=>t.value);
 const flags=[...new Set(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','set_config',
  'pg_advisory_xact_lock','pg_try_advisory_xact_lock'].includes(w)))];
 const file='read-graph/'+id.replaceAll(/[^a-z0-9_.-]/g,'_')+'.sqlinc';const target=path.join(here,file);
 fs.mkdirSync(path.dirname(target),{recursive:true});if(fs.existsSync(target)&&!fs.readFileSync(target).equals(Buffer.from(r.definition)))throw Error('RELEASE_READ_GRAPH_CHANGED:'+id);
 fs.writeFileSync(target,r.definition);saved.push(file);
 seen.set(id,{id,name:r.name,file:r.file??'PROPOSED_SHARED_PRIVATE_CORE',definition_sha256:sha(r.definition),saved_file:file,
  callees,direct_flags:flags,row_lock_sites:(r.body.match(/\bfor\s+(?:update|share|key\s+share|no\s+key\s+update)\b/gi)??[]).length,
  source_kind:local.has(id)?'PROPOSED_SHARED_CORE':'CURRENT_LITERAL_NATIVE_CALLEE'});queue.push(...callees);}
const result={status:'SOURCE_CALLEE_CENSUS_NATIVE_FALSE_MODE_PURITY_UNRUN',source_commit:native.sourceCommit,census_sha256:native.censusSha256,
 prerequisite_pins:prerequisites.map(x=>({commit:prerequisiteCommit,file:x.file,sha256:sha(x.raw)})),
 roots:[...local.keys()],routines:[...seen.values()],unresolved_amendments:native.index.unresolved.length,
 public_writer_executed:false,SQL_executed:false,Checklist_private_false_mode_fact_caller_built:true,public_producer_consumes_decision:false};
fs.writeFileSync(path.join(here,'NATIVE_READ_GRAPH.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({routines:seen.size,flagged:[...seen.values()].filter(x=>x.direct_flags.length||x.row_lock_sites).map(x=>({id:x.id,flags:x.direct_flags,locks:x.row_lock_sites})),native:false}));
