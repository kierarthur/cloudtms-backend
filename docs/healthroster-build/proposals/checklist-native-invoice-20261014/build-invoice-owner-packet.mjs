import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';
import {readCurrentNativeIndex,sha} from './read-current-native-index.mjs';
import {sqlTokens} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),snapshot=readCurrentNativeIndex(repo);
const roots=['private.weekly_source_invoice_line_amounts_v1(uuid,uuid)','private.weekly_source_invoice_movement_money_v1(uuid,uuid)',
  'private.weekly_source_frozen_rule_values_v1(uuid)','public.weekly_source_invoice_hold_release_atomic_v1(jsonb)',
  'public.weekly_source_invoice_admit_atomic_v1(jsonb)'];
const save=(file,raw)=>{const p=path.join(here,file);fs.mkdirSync(path.dirname(p),{recursive:true});if(fs.existsSync(p)&&!fs.readFileSync(p).equals(Buffer.from(raw)))throw Error('INVOICE_NATIVE_INPUT_CHANGED:'+file);fs.writeFileSync(p,raw);};
const metadata=r=>({id:r.id,name:r.name,file:r.file,source_file_sha256:r.source_file_sha256,definition_sha256:sha(r.definition),body_sha256:sha(r.body)});
const selected=roots.map(id=>{const r=snapshot.index.live.get(id);assert.ok(r,id);const file='input/'+r.name.replace('.','_')+'.sqlinc';save(file,r.definition);return {...metadata(r),saved_file:file};});
const readRoots=roots.filter(id=>!id.startsWith('public.')),queue=[...readRoots],seen=new Map();
while(queue.length){const id=queue.shift();if(seen.has(id))continue;const r=snapshot.index.live.get(id);assert.ok(r,id);
  const callees=snapshot.references(r.body).flatMap(name=>(snapshot.byName.get(name)||[]).map(c=>c.id));
  const words=sqlTokens(r.body).filter(t=>t.kind==='word').map(t=>t.value);
  const forbidden=[...new Set(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','set_config',
    'pg_advisory_xact_lock','pg_try_advisory_xact_lock'].includes(w)))];
  const file='read-graph/'+id.replaceAll(/[^a-z0-9_.-]/g,'_')+'.sqlinc';save(file,r.definition);
  seen.set(id,{...metadata(r),saved_file:file,callees,conservative_write_or_lock_tokens:forbidden,
    direct_row_lock:/\bfor\s+(?:update|share|key\s+share|no\s+key\s+update)\b/i.test(r.body)});queue.push(...callees);}
const graph=[...seen.values()];save('NATIVE_INVOICE_OWNER_PINS.json',JSON.stringify({status:'SOURCE_CONSTRUCTION_INPUT_NOT_MOUNTED_OR_NATIVE_QUALIFIED',
  sourceCommit:snapshot.sourceCommit,censusSha256:snapshot.censusSha256,roots:snapshot.roots,uniqueInputs:snapshot.uniqueInputs,occurrences:snapshot.occurrences,
  unresolved_dynamic_statements:snapshot.index.unresolved.length,selected,readRoots,readGraph:graph,
  read_graph_routine_count:graph.length,SQL_executed:false,another_lane_edited:false,admission_and_release_are_writer_reference_only:true},null,2)+'\n');
console.log(JSON.stringify({selected:selected.length,read_routines:graph.length,read_graph_flags:graph.filter(r=>r.direct_row_lock||r.conservative_write_or_lock_tokens.length).map(r=>({id:r.id,tokens:r.conservative_write_or_lock_tokens,locks:r.direct_row_lock})),native:false}));
