import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..');
const root=path.resolve(own,'../healthroster-v2-backend-13102026'),commit='2151af656561ad4d68e61e242769ed80a631fcc9';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:20_000_000});
const {sqlDateKey}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const objectFiles=git(['ls-tree','-r','--name-only',commit]).toString('utf8').trim().split('\n');
const chronological=files=>files.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
const release=JSON.parse(git(['show',`${commit}:supabase/release/current-release.json`]));
const top=[...release.baselineFiles,...chronological(objectFiles.filter(f=>/^supabase\/migrations\/[^/]+\.sql$/.test(f))),
 ...chronological(objectFiles.filter(f=>/^supabase\/repeatable\/[^/]+\.sql$/.test(f)))];
const cache=new Map(),closureInputs=new Map();
function read(file){
 if(!cache.has(file)){const b=git(['show',`${commit}:${file}`]),saved=fs.readFileSync(path.join(root,file));assert.deepEqual(saved,b,file);cache.set(file,b.toString('utf8').replaceAll('\r\n','\n'));closureInputs.set(file,{file,sha256:sha(b),bytes:b.length});}
 return cache.get(file);
}
let order=0;const all=[],triggers=[];
function identity(params){
 const args=[];let start=0,depth=0,quoted=false;
 for(let i=0;i<=params.length;i++){const c=params[i];if(c==="'")quoted=!quoted;if(!quoted){if(c==='(')depth++;if(c===')')depth--;if((c===','&&depth===0)||i===params.length){const v=params.slice(start,i).trim();if(v)args.push(v);start=i+1;}}}
 return args.map(arg=>arg.replace(/\s+(?:default|=)[\s\S]*$/i,'').replace(/^(?:in\s+|inout\s+)?[A-Za-z_][\w]*\s+/i,'').replace(/\s+/g,' ').toLowerCase()).join(',');
}
function visit(file,stack=[]){
 assert.equal(stack.includes(file),false,'NATIVE_INCLUDE_CYCLE');
 const source=read(file),parsed=scanManagedSql(source),entries=[...parsed.statements.map(item=>({...item,kind:'statement'})),...parsed.meta.map(item=>({...item,kind:'meta'}))].sort((a,b)=>a.start-b.start);
 for(const item of entries){
  const text=source.slice(item.start,item.end);
  if(item.kind==='meta'){
   const match=text.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/i);if(match){const next=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]??match[2]??match[3]));assert.ok(/^supabase\//.test(next),`${file}: ${next}`);visit(next,[...stack,file]);}continue;
  }
  const normalizedIdentifiers=text.replace(/"(private|public)"(?=\s*\.)/gi,(_,schema)=>schema).replace(/\b(private|public)\s*\.\s*"([A-Za-z_][\w]*)"/gi,(_,schema,name)=>schema+'.'+name);
  const dropped=normalizedIdentifiers.match(/^\s*drop\s+trigger\s+(?:if\s+exists\s+)?(\w+)\s+on\s+((?:private|public)\.\w+)/i);
  if(dropped)triggers.push({order:order++,file,name:dropped[1].toLowerCase(),relation:dropped[2].toLowerCase(),dropped:true});
  const trigger=normalizedIdentifiers.match(/^\s*create\s+(?:or\s+replace\s+)?(?:constraint\s+)?trigger\s+(\w+)[\s\S]*?\bon\s+((?:private|public)\.\w+)[\s\S]*?execute\s+(?:function|procedure)\s+((?:private|public)\.\w+)\s*\(/i);
  if(trigger)triggers.push({order:order++,file,name:trigger[1].toLowerCase(),relation:trigger[2].toLowerCase(),callee:trigger[3].toLowerCase(),definition_sha256:sha(Buffer.from(text)),definition:text});
  const head=normalizedIdentifiers.match(/^\s*create\s+(?:or\s+replace\s+)?function\s+((?:private|public)\.[A-Za-z_][\w]*)\s*\(([\s\S]*?)\)\s*returns\b/i);
  if(!head)continue;
  const name=head[1].toLowerCase(),signature=name+'('+identity(head[2])+')';
  const noStrings=normalizedIdentifiers.replace(/'(?:''|[^'])*'/g,match=>' '.repeat(match.length));
  const direct=[...noStrings.matchAll(/\b((?:private|public)\.[A-Za-z_][\w]*)\s*\(/gi)]
    .filter(match=>!/(?:\binsert\s+into|\bupdate|\bdelete\s+from)\s*$/i.test(noStrings.slice(Math.max(0,match.index-256),match.index)))
    .map(match=>match[1].toLowerCase()).filter(call=>call!==name);
  const dynamic=[...normalizedIdentifiers.matchAll(/'((?:''|[^'])*)'/g)].filter(match=>/\b(?:select|perform|call)\s+(?:private|public)\./i.test(match[1]))
    .flatMap(match=>[...match[1].matchAll(/\b((?:private|public)\.\w+)\s*\(/gi)].map(call=>call[1].toLowerCase()));
  const calls=[...new Set([...direct,...dynamic])].sort();
  const writes=[...new Set([...normalizedIdentifiers.matchAll(/\b(?:insert\s+into|update|delete\s+from)\s+((?:public|private)\.[A-Za-z_][\w]*)\b/gi)].map(match=>match[1].toLowerCase()))].sort();
  all.push({order:order++,file,name,signature,definition_sha256:sha(Buffer.from(text)),definition:text,calls,writes});
 }
}
for(const file of top)visit(file);
const final=new Map();for(const row of all)final.set(row.signature,row);
const finalTriggers=new Map();for(const row of triggers){const key=row.relation+'.'+row.name;if(row.dropped)finalTriggers.delete(key);else finalTriggers.set(key,row);}
const consumers=[...final.values()].filter(row=>row.name!=='private.weekly_source_effective_rule_set_v1'&&row.calls.includes('private.weekly_source_effective_rule_set_v1'));
const byName=new Map();for(const row of final.values()){if(!byName.has(row.name))byName.set(row.name,[]);byName.get(row.name).push(row);}
const entryNames=['public.weekly_source_group_rule_feature_set_atomic_v1','public.weekly_source_automatic_reconcile_atomic_v1',
 'public.weekly_exceptional_pay_prepare_family_v1','public.weekly_exceptional_pay_prepare_action_v1','public.weekly_exceptional_pay_action_context_v1',
 'public.weekly_exceptional_pay_stage_c1_request_v1','public.weekly_exceptional_pay_complete_local_v1','private.weekly_source_automatic_reconcile_delta_assert_v1',
 'public.weekly_source_upload_context_v1','public.weekly_source_upload_stage_begin_atomic_v1','public.weekly_source_upload_stage_rows_atomic_v1',
 'public.weekly_source_upload_seal_atomic_v1','public.weekly_source_projection_begin_atomic_v1','public.weekly_source_projection_rows_apply_atomic_v1',
 'public.weekly_source_projection_publish_atomic_v1','public.weekly_source_import_prepare_atomic_v1','public.weekly_source_finalise_atomic_v1',
 'public.weekly_source_finalisation_pay_open_atomic_v1','public.weekly_source_finalisation_pay_task_start_atomic_v1',
 'public.weekly_source_ordinary_pay_projection_apply_atomic_v1','public.weekly_source_finalisation_pay_task_finish_atomic_v1'];
entryNames.push('public.weekly_exceptional_pay_action_publication_status_v1');
const chain=new Map();function follow(name,via){for(const row of byName.get(name)??[]){if(chain.has(row.signature))continue;chain.set(row.signature,{...row,via});for(const call of row.calls)follow(call,row.signature);}}
for(const name of entryNames)follow(name,'REQUESTED_ENTRY');
let expanded=true;const triggerChain=new Map();while(expanded){expanded=false;const relations=new Set([...chain.values()].flatMap(row=>row.writes));
 for(const trigger of finalTriggers.values()){const key=trigger.relation+'.'+trigger.name;if(relations.has(trigger.relation)&&!triggerChain.has(key)){triggerChain.set(key,trigger);follow(trigger.callee,key);expanded=true;}}
}
const unknownCalls=[...new Set([...chain.values()].flatMap(row=>row.calls).filter(name=>!byName.has(name)))].sort();
const perEntry=entryNames.map(entry=>{
 const local=new Map(),usedTriggers=new Map();function walk(name){for(const row of byName.get(name)??[]){if(local.has(row.signature))continue;local.set(row.signature,row);for(const call of row.calls)walk(call);}}
 walk(entry);let growth=true;while(growth){growth=false;const relations=new Set([...local.values()].flatMap(row=>row.writes));for(const trigger of finalTriggers.values()){
  const key=trigger.relation+'.'+trigger.name;if(relations.has(trigger.relation)&&!usedTriggers.has(key)){usedTriggers.set(key,trigger);walk(trigger.callee);growth=true;}}}
 return {entry,signatures:[...local.keys()].sort(),write_relations:[...new Set([...local.values()].flatMap(row=>row.writes))].sort(),
  literal_write_trigger_signatures:[...usedTriggers.keys()].sort(),unresolved_callees:[...new Set([...local.values()].flatMap(row=>row.calls).filter(name=>!byName.has(name)))].sort()};
});
const sourceFactsFile=path.resolve(out,'../native-financial-fixtures/CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc');
const sourceFacts=fs.readFileSync(sourceFactsFile,'utf8');
const compact=row=>({signature:row.signature,owner_name:row.name,file:row.file,definition_sha256:row.definition_sha256,calls:row.calls,writes:row.writes});
const record={schema_version:'STRICT_QUALIFICATION_NATIVE_INTERFACE_LITERAL_CENSUS_V1',source_only:true,root_source_commit:commit,
 ordering:'Current release baselineFiles then actual repository sqlDateKey migrations/top-level repeatables, each include expanded at its literal occurrence; full current definitions by source identity. Definitions and literal triggers are source declarations, not installed catalogue proof. Dynamic DDL needs separate owner review.',
 current_effective_rule_owner:(byName.get('private.weekly_source_effective_rule_set_v1')??[]).map(compact),
 direct_effective_rule_consumers:consumers.map(compact),consumer_count:consumers.length,
 source_executor_entries:entryNames,per_entry_executor_closure:perEntry,source_executor_chain:[...chain.values()].map(row=>({...compact(row),via:row.via})),
 unresolved_callees:unknownCalls,inputs:[...closureInputs.values()],source_write_trigger_chain:[...triggerChain.values()].map(({definition,...row})=>row),
 source_cas_owner_pin:{file:path.relative(own,sourceFactsFile).replaceAll('\\','/'),sha256:sha(Buffer.from(sourceFacts))},
 physical_timesheet_write_leaves:[...chain.values()].filter(row=>row.writes.includes('public.timesheets')).map(compact),
 source_effect_review:'Lexical calls include conservative dynamic SQL references. Every actual callable/write leaf needs full source review; an absent relation intersection is not native own-stage equality. No external effect is permitted and no executor is run by this census.',
 actual39_fact_equality:false,actual_nullable_identity_materialisation:false,actual_native_executor_acceptance:false,actual_activation:false,actual_off_automatic_journey:false};
fs.writeFileSync(path.join(out,'NATIVE_CONSUMER_AND_EXECUTOR_CENSUS.json'),JSON.stringify(record,null,2)+'\n');
const selected=[...new Map([...consumers,...chain.values()].filter(row=>row.name.startsWith('private.weekly_source_')||entryNames.includes(row.name)).map(row=>[row.signature,row])).values()];
fs.writeFileSync(path.join(out,'CURRENT_SOURCE_OWNER_INPUTS.sqlinc'),selected.map(row=>`-- Source input ${row.signature}; ${row.file}; SHA256 ${row.definition_sha256}\n${row.definition}`).join('\n\n')+'\n');
fs.writeFileSync(path.join(out,'SOURCE_EXECUTOR_EFFECT_INPUTS.sqlinc'),[...chain.values()].map(row=>
 `-- Source effect input ${row.signature}; ${row.file}; SHA256 ${row.definition_sha256}\n${row.definition}`).join('\n\n')+'\n');
console.log(JSON.stringify({source_only:true,effective_rule_consumers:consumers.length,executor_chain:chain.size,unresolved_callees:unknownCalls.length,physical_timesheet_write_leaves:record.physical_timesheet_write_leaves.length,root_source_commit:commit}));
