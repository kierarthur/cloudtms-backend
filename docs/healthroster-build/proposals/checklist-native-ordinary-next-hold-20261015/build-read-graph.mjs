// Finite executed-callee source census; TRUE/FALSE runtime parity stays held.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {readCurrentNativeIndex,sha} from '../checklist-native-invoice-20261014/read-current-native-index.mjs';
import {functionEvents,sqlTokens} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),native=readCurrentNativeIndex(repo);
const kernel=fs.readFileSync(path.join(here,'input/Native77_shared_financial_kernel.sqlinc'),'utf8');
const purpose=fs.readFileSync(path.join(here,'forward/wp_u3_ordinary_next_hold.sqlinc'),'utf8');
const local=new Map(functionEvents(kernel+'\n'+purpose).events.filter(e=>e.definition&&e.name.startsWith('private.')).map(e=>[e.id,e]));
const byName=new Map(native.byName);for(const r of local.values())byName.set(r.name,[r]);
const root=[...local.values()].find(r=>r.name==='private.weekly_source_checklist_ordinary_next_hold_money_v1');assert.ok(root);
const queue=[root.id],seen=new Map(),external=[],dynamic=[];fs.mkdirSync(path.join(here,'read-graph'),{recursive:true});
while(queue.length){const id=queue.shift();if(seen.has(id))continue;const r=local.get(id)??native.index.live.get(id);assert.ok(r,'Missing actual native callee '+id);
 const names=native.references(r.body),callees=names.flatMap(name=>{
  if(name==='extensions.digest'&&!byName.has(name)){external.push({caller:id,name,kind:'EXTERNAL_PGCRYPTO_EXTENSION_SOURCE_BOUNDARY',compiled_ABI:false,native_purity:false});return [];}
  assert.ok(byName.has(name),'Unresolved callee '+name);return byName.get(name).map(r=>r.id);});
 const words=sqlTokens(r.body).filter(t=>t.kind==='word').map(t=>t.value),flags=[...new Set(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','set_config','pg_advisory_xact_lock','pg_try_advisory_xact_lock'].includes(w)))];
 if(flags.includes('execute')){
  assert.equal(r.name,'private.weekly_source_inventory_approval_basis_v2');const statements=[...r.body.matchAll(/execute\s+'((?:[^']|'')+)'/gi)];assert.equal(statements.length,1);
  const sql=statements[0][1].replaceAll("''","'");assert.match(sql,/^select count\(\*\),jsonb_agg\(to_jsonb\(d\)\)->0\s+from private\.bpay_next_source_chosen_detail d/);
  assert.doesNotMatch(sql,/for (?:update|share)|\b(?:insert|update|delete|execute|call|create|drop|alter|lock|notify)\b/i);
  dynamic.push({caller:id,body_sha256:sha(r.definition),literal_SQL:sql,literal_SQL_sha256:sha(sql),input_binding:'v_head.id, actual component UUID, v_head.decision_bundle_id, v_head.bundle_revision',native_purity:false});
 }
 const file='read-graph/'+id.replaceAll(/[^a-z0-9_.-]/g,'_')+'.sqlinc';fs.writeFileSync(path.join(here,file),r.definition);
 seen.set(id,{id,name:r.name,file:r.file??'PINNED_NATIVE77_SHARED_CORE_OR_NEW_PURPOSE',definition_sha256:sha(r.definition),saved_file:file,callees,direct_flags:flags,
  row_lock_sites:(r.body.match(/\bfor\s+(?:update|share|key\s+share|no\s+key\s+update)\b/gi)??[]).length,source_kind:local.has(id)?'PROPOSED_PRIVATE_CORE_OR_PURPOSE':'CURRENT_LITERAL_NATIVE'});queue.push(...callees);}
fs.writeFileSync(path.join(here,'NATIVE_READ_GRAPH.json'),JSON.stringify({status:'UNMOUNTED_ORDINARY_NEXT_FINANCIAL_READ_SOURCE_GRAPH_NATIVE_PURITY_UNRUN',
 native:false,SQL_executed:false,source_commit:native.sourceCommit,census_sha256:native.censusSha256,roots:[root.id],routines:[...seen.values()],
 unresolved_amendments:native.index.unresolved.length,external_boundaries:external,dynamic_read_boundaries:dynamic,TRUE_native_parity:false,FALSE_native_purity:false,public_root_mounted:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,routines:seen.size,flagged:[...seen.values()].filter(r=>r.direct_flags.length||r.row_lock_sites).map(r=>({id:r.id,flags:r.direct_flags,locks:r.row_lock_sites})),native:false}));
