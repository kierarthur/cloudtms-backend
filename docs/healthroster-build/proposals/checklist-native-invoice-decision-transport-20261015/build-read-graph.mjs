// Reuse every immutable predecessor native body; replace only the per-member
// prototype root with the scope-local purpose reader. No SQL or native action.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents,sqlTokens} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='7a553954afe79ec46e1a9270220e19ce330efb0b';
const previous='docs/healthroster-build/proposals/checklist-native-invoice-decision-20261014/',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const get=file=>execFileSync('git',['cat-file','blob',commit+':'+previous+file],{cwd:repo,maxBuffer:4e6}),graphBytes=get('NATIVE_READ_GRAPH.json'),graph=JSON.parse(graphBytes);
fs.mkdirSync(path.join(here,'read-graph'),{recursive:true});const routines=graph.routines.filter(r=>r.name!=='private.weekly_source_checklist_source_invoice_decision_v1');
for(const r of routines){const b=get(r.saved_file);assert.equal(sha(b),r.definition_sha256);fs.writeFileSync(path.join(here,r.saved_file),b);}
const raw=fs.readFileSync(path.join(here,'forward/wp_u3_source_invoice_decisions.sqlinc'),'utf8'),r=functionEvents(raw).events.find(e=>e.definition);
const callees=[...new Set(routines.filter(x=>r.body.includes(x.name+'(')).map(x=>x.id))];
const words=sqlTokens(r.body).filter(t=>t.kind==='word').map(t=>t.value),flags=[...new Set(words.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','set_config','pg_advisory_xact_lock','pg_try_advisory_xact_lock'].includes(w)))];
const saved='read-graph/'+r.id.replaceAll(/[^a-z0-9_.-]/g,'_')+'.sqlinc';fs.writeFileSync(path.join(here,saved),r.definition);
routines.push({id:r.id,name:r.name,file:'forward/wp_u3_source_invoice_decisions.sqlinc',definition_sha256:sha(r.definition),saved_file:saved,callees,direct_flags:flags,row_lock_sites:0,source_kind:'PROPOSED_SCOPE_LOCAL_PURPOSE_READER'});
for(const r of routines)for(const id of r.callees)assert.ok(routines.some(x=>x.id===id),id);
const bodyFiles=['NATIVE_RELEASE_QUALIFICATION.forward.sqlinc','NATIVE_RELEASE_QUALIFICATION.reverse.sqlinc','NATIVE_RELEASE_REVERSE.json'];
for(const file of bodyFiles){const b=get(file);fs.mkdirSync(path.join(here,'shared-owner'),{recursive:true});fs.writeFileSync(path.join(here,'shared-owner',file),b);}
fs.writeFileSync(path.join(here,'NATIVE_READ_GRAPH.json'),JSON.stringify({...graph,status:'SOURCE_SCOPE_LOCAL_DECISION_CALLEE_CENSUS_NATIVE_PURITY_UNRUN',
 roots:graph.roots.map(id=>id.startsWith('private.weekly_source_checklist_source_invoice_decision_v1(')?r.id:id),routines,
 predecessor:{commit,graph_path:previous+'NATIVE_READ_GRAPH.json',graph_sha256:sha(graphBytes)},
 public_producer_consumes_decision:true,public_root_mounted:false,SQL_executed:false,native:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,routines:routines.length,flagged:routines.filter(r=>r.direct_flags.length||r.row_lock_sites).map(r=>({id:r.id,flags:r.direct_flags,locks:r.row_lock_sites})),native:false}));
