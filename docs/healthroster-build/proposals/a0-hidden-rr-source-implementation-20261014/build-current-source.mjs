// SOURCE ONLY. Reads exact saved/published I2; writes only this adjacent packet.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
import {functionEvents,applyFunctionEvents,emptyFunctionIndex,sqlTokens,parseTriggerIdentity} from '../cn-a0-isolation-source-followup/sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]),stamp='14102026_1030';
const pin='5db7abee7925fffbeaf7c4e885cce809980fa079',sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=new Map();
const git=args=>{const r=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:null,maxBuffer:64*1024*1024,windowsHide:true});assert.equal(r.status,0,r.stderr?.toString());return r.stdout;};
assert.equal(git(['rev-parse','HEAD']).toString().trim(),pin,'Exact I2 pin');
const cache=new Map();function read(file){if(cache.has(file))return cache.get(file);const b=fs.readFileSync(path.join(root,file));assert.deepEqual(b,git(['show',pin+':'+file]),'Saved/published input mismatch '+file);inputs.set(file,{file,bytes:b.length,sha256:sha(b)});const text=b.toString('utf8');cache.set(file,text);return text;}
const release=JSON.parse(read('supabase/release/current-release.json')),catalogue=JSON.parse(read('supabase/release/current-contract.json'));
read('scripts/cloudtms-db-release-lib.mjs');const lib=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
const context={defaultSchema:'public',types:new Map()};for(const r of [...catalogue.enums,...catalogue.relations]){context.types.set(r.schema+'.'+r.name,r.schema+'.'+r.name);if(r.schema==='public')context.types.set(r.name,r.schema+'.'+r.name);}
const state=emptyFunctionIndex(),unresolved=[];let sequence=0;
function scan(file,text,offset=0){const p=functionEvents(text,context);applyFunctionEvents(state,p.events,e=>({...e,file,line:offset+text.slice(0,e.offset).split('\n').length,sequence:sequence++,definitionSha256:e.definition?sha(e.definition):null}));unresolved.push(...p.unresolved.map(u=>({file,kind:u.kind,line:offset+text.slice(0,u.offset).split('\n').length})));}
function expand(file,stack=[]){assert.ok(!stack.includes(file));const lines=read(file).split(/\r?\n/);let part='',start=0;for(let i=0;i<lines.length;i++){const m=lines[i].match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);if(m){scan(file,part,start);part='';start=i+1;const child=path.relative(root,path.resolve(root,path.dirname(file),m[1]??m[2]??m[3])).replaceAll('\\','/');assert.ok(child.startsWith('supabase/'));expand(child,[...stack,file]);}else part+=lines[i]+'\n';}scan(file,part,start);}
for(const f of release.baselineFiles||[])scan(f,read(f));const migrations=lib.sqlFiles('supabase/migrations'),repeatables=lib.sqlFiles('supabase/repeatable');assert.equal(migrations.length,399);assert.equal(repeatables.length,1060);
for(const f of migrations)expand(f);for(const f of repeatables)expand(f);
const targets=new Set(['timesheet_validations','invoice_lines','timesheets','candidates']);function mutations(body,wanted=targets){const ts=sqlTokens(body),out=[];for(let i=0;i<ts.length;i++){let op,j;if(ts[i].kind!=='word')continue;if(ts[i].value==='insert'&&ts[i+1]?.value==='into'){op='INSERT';j=i+2;}else if(ts[i].value==='update'){op='UPDATE';j=i+1;}else if(ts[i].value==='delete'&&ts[i+1]?.value==='from'){op='DELETE';j=i+2;}else continue;if(ts[j]?.value==='only')j++;let table=ts[j]?.value;if(table==='public'&&ts[j+1]?.value==='.')table=ts[j+2]?.value;if(wanted.has(table))out.push({table,operation:op,bodyLine:body.slice(0,ts[i].start).split('\n').length});}return out;}
function handlers(body){const ts=sqlTokens(body);return ts.flatMap((t,i)=>t.kind==='word'&&t.value==='exception'&&ts[i+1]?.value==='when'?[{bodyLine:body.slice(0,t.start).split('\n').length,text:body.slice(t.start,Math.min(body.length,t.start+1600))}]:[]);}
const writers=[...state.live.values()].map(d=>({id:d.id,file:d.file,line:d.line,definitionSha256:d.definitionSha256,mutations:mutations(d.body),dynamicExecute:sqlTokens(d.body).some(t=>t.kind==='word'&&t.value==='execute'),exceptions:handlers(d.body)})).filter(d=>d.mutations.length);
const byName=new Map();for(const d of state.live.values()){const list=byName.get(d.name)||[];list.push(d.id);byName.set(d.name,list);}
function calls(body){const ts=sqlTokens(body),out=new Set();for(let i=0;i<ts.length;i++){if(!['word','identifier'].includes(ts[i].kind))continue;let n=ts[i].value,j=i+1;if(ts[j]?.value==='.'&&['word','identifier'].includes(ts[j+1]?.kind)){n+='.'+ts[j+1].value;j+=2;}else if(n.includes('.'))continue;else n='public.'+n;if(ts[j]?.value==='('&&byName.has(n))for(const id of byName.get(n))out.add(id);}return [...out].sort();}
const routinePages=[...state.live.values()].map(d=>({id:d.id,file:d.file,line:d.line,definitionSha256:d.definitionSha256,definition:d.definition,body:d.body,literalPotentialCallees:calls(d.body),dynamicExecute:sqlTokens(d.body).some(t=>t.kind==='word'&&t.value==='execute'),exceptions:handlers(d.body)}));
const beforeTriggers=catalogue.triggers.filter(t=>(targets.has(t.table)||t.table==='timesheets_financials')&&/\bBEFORE\b/i.test(t.definition)&&/\bFOR EACH ROW\b/i.test(t.definition)).map(t=>{const p=parseTriggerIdentity(t),ids=byName.get(p.routineName)||[];assert.equal(ids.length,1,'Trigger owner '+p.routineName);const d=state.live.get(ids[0]),ts=sqlTokens(d.body);const assignments=ts.flatMap((x,i)=>x.value==='new'&&ts[i+1]?.value==='.'&&['word','identifier'].includes(ts[i+2]?.kind)&&((ts[i+3]?.value===':'&&ts[i+4]?.value==='=')||ts[i+3]?.value==='=')?[{field:ts[i+2].value,bodyLine:d.body.slice(0,x.start).split('\n').length}]:[]);return {...t,routineId:d.id,file:d.file,line:d.line,definitionSha256:d.definitionSha256,literalNewFieldAssignments:assignments,literalPotentialCallees:calls(d.body),dynamicExecute:sqlTokens(d.body).some(x=>x.kind==='word'&&x.value==='execute')};});
const selectedIds=new Set(writers.map(w=>w.id));for(const p of routinePages)if(p.id.includes('timesheet_correction')||mutations(p.body,new Set(['timesheets_financials'])).length)selectedIds.add(p.id);for(const t of catalogue.triggers.filter(t=>targets.has(t.table)||t.table==='timesheets_financials'))for(const id of byName.get(parseTriggerIdentity(t).routineName)||[])selectedIds.add(id);const pagesById=new Map(routinePages.map(p=>[p.id,p])),queue=[...selectedIds];for(let i=0;i<queue.length;i++)for(const id of pagesById.get(queue[i])?.literalPotentialCallees||[])if(!selectedIds.has(id)){selectedIds.add(id);queue.push(id);}
fs.writeFileSync(path.join(here,'ACTUAL_SOURCE_CALLEE_AND_TRIGGER_CLOSURE.json'),JSON.stringify({sourceOnly:true,rootCommit:pin,edgeSemantics:'LITERAL_NAME_POTENTIAL_OVERLOADS_NOT_PROVEN_INVOCATIONS; dynamic SQL and conditional paths stay explicit',completeDefinitionSelection:'71 raw writer roots, all direct FIN writers, Timesheet correction owners, all actual five-table trigger roots, and complete transitive potential callees',completeDefinitionCount:selectedIds.size,catalogueBeforeTriggers:beforeTriggers,catalogueIncludesNew0519A0Guards:false,additionalActual0519Guards:['aaa_healthroster_a0_booking_before_v1','aaa_healthroster_a0_reference_before_v1','zzzz_healthroster_a0_reference_before_v1','zzzz_healthroster_a0_candidate_before_v1'],routines:routinePages.map(p=>selectedIds.has(p.id)?p:(({definition,body,exceptions,...m})=>({...m,exceptions:exceptions.map(e=>({bodyLine:e.bodyLine}))}))(p))},null,2)+'\n');
assert.ok(writers.some(d=>d.id==='public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb)'),'Unchanged Bank71 input');
const base='supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition/',names=['04_complete_native_lock_fence.sqlinc','05_native_creator_intents_and_triggers.sqlinc','06_send_family_fence.sqlinc','08_complete_current_request.sqlinc'];
const sources=new Map(names.map(n=>[n,read(base+n)])),hunks=[],successors=new Map();
function change(file,source,before,after,label){const variants=[before,before.replaceAll('\n','\r\n')].filter((v,i,a)=>a.indexOf(v)===i),match=variants.filter(v=>source.includes(v));assert.equal(match.length,1,'Exact anchor '+label);before=match[0];assert.equal(source.split(before).length,2,'Unique anchor '+label);if(before.includes('\r\n'))after=after.replaceAll('\n','\r\n');const output=source.replace(before,after);hunks.push({file,label,before,after});return output;}
const reference=`create or replace function private.timesheet_correction_a0_reference_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ids uuid[];v_bookings text[];
begin
  if tg_table_schema<>'public' or tg_table_name not in('timesheet_validations','invoice_lines','timesheets_financials') or tg_op not in('INSERT','UPDATE','DELETE') then
    raise exception 'CORRECTION_A0_WRITER_TRIGGER_CONTEXT_INVALID' using errcode='55000';end if;
  if tg_op='DELETE' then v_ids:=array[old.timesheet_id];
    if tg_table_name='invoice_lines' then v_bookings:=array[old.booking_id];end if;
  elsif tg_op='UPDATE' then v_ids:=array[old.timesheet_id,new.timesheet_id];
    if tg_table_name='invoice_lines' then v_bookings:=array[old.booking_id,new.booking_id];end if;
  else v_ids:=array[new.timesheet_id];
    if tg_table_name='invoice_lines' then v_bookings:=array[new.booking_id];end if;
  end if;
  select array_agg(distinct x.id order by x.id) into v_ids from unnest(v_ids) x(id) where x.id is not null;
  -- Every actual reference mutation participates, including unchanged pointers
  -- with changed validation/invoice facts and historical DELETE. No FK/lookup
  -- is required for a raw future P0 UUID. NULL/no-identity keeps the native arm.
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v1(
    null,v_ids,v_bookings,null,null),false);
  -- FIN's native owners retain every financial rule/admission. The only added
  -- fact is raw PHYSICAL_UUID serialization before native FIN/AFTER effects.
  -- Existing T0 parent locks do not refresh an older RR snapshot.
  if tg_table_name='timesheets_financials' then
    if tg_op='DELETE' then return old;end if;return new;
  end if;
  if v_ids is not null or v_bookings is not null then
    perform private.timesheet_correction_a0_writer_admit_v1(v_ids,v_bookings,
      case when tg_table_name='invoice_lines' then 'INVOICE_REFERENCE' else 'VALIDATION_REFERENCE' end);
  end if;
  if tg_op='DELETE' then return old;end if;return new;
end;
$function$;`;
const booking=`create or replace function private.timesheet_correction_a0_booking_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ids uuid[];v_bookings text[];v_parents uuid[];v_keys text[];v_aliases jsonb[];v_admit jsonb;r record;v_match boolean;
begin
  if tg_table_schema<>'public' or tg_table_name<>'timesheets' or tg_op not in('INSERT','UPDATE','DELETE') then
    raise exception 'CORRECTION_A0_WRITER_TRIGGER_CONTEXT_INVALID' using errcode='55000';end if;
  if tg_op='DELETE' then
    v_ids:=array[old.timesheet_id];v_bookings:=array[old.booking_id];v_parents:=array[old.parent_timesheet_id];
    v_keys:=array[old.occupant_key_norm];
  elsif tg_op='UPDATE' then
    v_ids:=array[old.timesheet_id,new.timesheet_id];v_bookings:=array[old.booking_id,new.booking_id];
    v_parents:=array[old.parent_timesheet_id,new.parent_timesheet_id];v_keys:=array[old.occupant_key_norm,new.occupant_key_norm];
  else
    v_ids:=array[new.timesheet_id];v_bookings:=array[new.booking_id];v_parents:=array[new.parent_timesheet_id];v_keys:=array[new.occupant_key_norm];
  end if;
  select array_agg(to_jsonb(array[k.value]::text[])) into v_aliases from unnest(v_keys) k(value);
  -- Full Timesheet authority/signature/schedule/rank mutation coverage, not
  -- merely booking/Contract changes. Parent is a native raw UUID, not a resolver.
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v1(
    v_parents,v_ids||v_parents,v_bookings,v_keys,v_aliases),false);
  v_admit:=private.timesheet_correction_a0_writer_admit_v1(v_ids,v_bookings,'BOOKING_IDENTITY');
  if tg_op='DELETE' then return old;end if;
  for r in select x.value from jsonb_array_elements(v_admit->'requests') x(value) loop
    v_match:=btrim(new.booking_id) in(btrim(r.value->>'original_booking'),btrim(r.value->>'reversed_booking'),btrim(r.value->>'p0_booking'));
    if v_match and (new.contract_id is distinct from (r.value->>'contract_id')::uuid
      or new.booking_id not in(r.value->>'original_booking',r.value->>'reversed_booking',r.value->>'p0_booking')) then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
    if new.timesheet_id=(r.value->>'p0')::uuid and (new.booking_id is distinct from r.value->>'p0_booking'
      or new.contract_id is distinct from (r.value->>'contract_id')::uuid) then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
    if new.timesheet_id=(r.value->>'original')::uuid and (new.booking_id is distinct from r.value->>'reversed_booking'
      or new.contract_id is distinct from (r.value->>'contract_id')::uuid) then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
    if new.timesheet_id=(r.value->>'root')::uuid and new.booking_id is distinct from r.value->>'original_booking' then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
  end loop;
  return new;
end;
$function$;`;
let file=names[1],text=sources.get(file);for(const [name,after] of [['private.timesheet_correction_a0_reference_before_v1',reference],['private.timesheet_correction_a0_booking_before_v1',booking]]){const matches=functionEvents(text,context).events.filter(e=>e.kind==='CREATE'&&e.name===name);assert.equal(matches.length,1);text=change(file,text,matches[0].definition,after,name+' complete current owner');}
text=change(file,text,"  -- Exact same equality/JSON containment as tsfin_load_context_batch; no",`  -- Raw OLD/NEW key/alias tokens precede every snapshot-visible classification.
  -- Even a born-later physical family and unchanged foreign ranking alias
  -- intersect the actual retained occupant-domain tokens.
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v1(
    null,null,null,array[v_old_key,v_new_key],array[v_old_aliases,v_new_aliases]),false);
  -- Exact same equality/JSON containment as tsfin_load_context_batch; no`,'Candidate raw intent before visible scan');
for(const table of ['timesheet_validations','invoice_lines','timesheets'])text=change(file,text,'before insert or update on public.'+table,'before insert or update or delete on public.'+table,'Actual DELETE event '+table);
text=change(file,text,'-- No existing invoice BEFORE owner changes the reference;',`-- Native FIN direct writers and synchronous late hooks use this same raw
-- physical fence. No Bank function or native financial decision is replaced.
drop trigger if exists aaa_healthroster_a0_financial_before_v1 on public.timesheets_financials;
create trigger aaa_healthroster_a0_financial_before_v1 before insert or update or delete on public.timesheets_financials
for each row execute function private.timesheet_correction_a0_reference_before_v1();
-- No existing invoice BEFORE owner changes the reference;`,'Native FIN raw physical guard');successors.set(file,text);
file=names[2];text=sources.get(file);text=change(file,text,'  select count(*),jsonb_agg(to_jsonb(ctx))->0 into v_context_count,v_context',`  -- Phase EXISTING follows the unchanged native Candidate then whole I1.
  -- Acquire all actual existing family identities before canonical reread.
  select array_agg(distinct s.family_timesheet_id order by s.family_timesheet_id) into v_ids
    from public._pay_timesheet_rotation_scope(v_ids) s;
  perform private.timesheet_correction_a0_family_tokens_retain_v1((v_chain->>'root_timesheet_id')::uuid,v_ids);
  select count(*),jsonb_agg(to_jsonb(ctx))->0 into v_context_count,v_context`,'SEND existing native token phase');successors.set(file,text);
file=names[3];text=sources.get(file);text=change(file,text,'  v_invoice uuid; v_invoice_status text; v_key text; v_hash text; v_leg jsonb; v_candidate uuid; v_client uuid; v_date date;',`  v_invoice uuid; v_invoice_status text; v_key text; v_hash text; v_leg jsonb; v_candidate uuid; v_client uuid; v_date date;
  v_a0_transaction_fence boolean:=false;`,'Scoped SEND local intent');text=change(file,text,'  perform private.timesheet_correction_a0_send_family_fence_v1(v_id);',`  perform private.timesheet_correction_a0_send_family_fence_v1(v_id);
  v_a0_transaction_fence:=private.timesheet_correction_a0_signed_precursor_v1(v_id);`,'Private native missing-A0 intent');text=change(file,text,'  v_req.created_by_user_id:=p_actor_user_id; v_req.created_at_utc:=v_now; v_req.updated_at_utc:=v_now;',`  v_req.created_by_user_id:=p_actor_user_id; v_req.created_at_utc:=v_now; v_req.updated_at_utc:=v_now;
  -- Phase ALLOCATED uses the genuine owner-assigned P0/R0 UUIDs/bookings.
  -- It precedes adjustment-week, WAITING and physical P0 publication.
  if v_a0_transaction_fence then perform private.timesheet_correction_a0_request_tokens_retain_v1(v_req);end if;`,'Genuine assigned request phase before first publication');successors.set(file,text);
file=names[0];text=sources.get(file);text=change(file,text,"  return jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_NATIVE_LOCK_FENCE_V1',",`  -- The exact owned frame/native CAS above supplies trusted intent. No new
  -- token can authorize or replace that admission. This precedes materialisation.
  perform private.timesheet_correction_a0_family_tokens_retain_v1(r.root_timesheet_id,v_members);
  perform private.timesheet_correction_a0_request_tokens_retain_v1(r);
  return jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_NATIVE_LOCK_FENCE_V1',`,'Atomic actual frame retain before native business writes');successors.set(file,text);
const reversals=[];for(const [file,after] of successors){let reversed=after;for(const h of hunks.filter(h=>h.file===file).reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,sources.get(file),'Entire current file reverse');const out=stamp+'_'+file.replace(/\.sqlinc$/,'.complete-proposed.sqlinc');fs.writeFileSync(path.join(here,'sql',out),after);reversals.push({input:base+file,output:'sql/'+out,beforeBytes:Buffer.byteLength(sources.get(file)),beforeSha256:sha(sources.get(file)),afterBytes:Buffer.byteLength(after),afterSha256:sha(after),byteReverseEqual:true,hunks:hunks.filter(h=>h.file===file)});}
const stage=read('supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition_v1.sql');const acl=stage.slice(stage.indexOf('alter function private.timesheet_correction_canonical_input_assert_v1')).replace(/commit;\s*$/,'');
const rootName=stamp+'_a0_hidden_rr_transaction_fence_v1.sql';const closure='-- Source-only successor; native/ordinary overlap/contract seals remain held.\nbegin;\n\\ir '+stamp+'_a0_transaction_fence_helpers.sqlinc\n'+names.map(n=>'\\ir '+stamp+'_'+n.replace(/\.sqlinc$/,'.complete-proposed.sqlinc')).join('\n')+'\n'+acl+'commit;\n';fs.writeFileSync(path.join(here,'sql',rootName),closure);
const newFunctions=functionEvents(fs.readFileSync(path.join(here,'sql',stamp+'_a0_transaction_fence_helpers.sqlinc'),'utf8'),context).events.filter(e=>e.kind==='CREATE').map(e=>e.id);assert.equal(newFunctions.length,4);
const changed=[];for(const [file,after] of successors){const beforeFns=new Map(functionEvents(sources.get(file),context).events.filter(e=>e.kind==='CREATE').map(e=>[e.id,e]));for(const e of functionEvents(after,context).events.filter(e=>e.kind==='CREATE')){const b=beforeFns.get(e.id);assert.ok(b);if(sha(b.definition)!==sha(e.definition))changed.push({id:e.id,input:base+file,beforeSha256:sha(b.definition),afterSha256:sha(e.definition)});}}
assert.equal(changed.length,6);assert.ok(changed.every(d=>!/^.*bpay|banking/.test(d.id)));
fs.writeFileSync(path.join(here,'CURRENT_SOURCE_REVERSE_AND_CENSUS.json'),JSON.stringify({sourceOnly:true,rootCommit:pin,migrations:migrations.length,repeatables:repeatables.length,finalRoutineCount:state.live.size,writerCandidates:writers,unresolvedSourceExecution:unresolved,reversals,changedCurrentOwners:changed,newPrivateSignatures:newFunctions,newPrivateTables:['private.timesheet_correction_a0_transaction_fences'],newPublicOrServiceSignatures:[],bankWriterUnchanged:true,nativeSchedulesRun:false,ordinaryAbsentSharedAliasNativeSemanticsQualified:false,inputs:[...inputs.values()].sort((a,b)=>a.file.localeCompare(b.file))},null,2)+'\n');
const delta={rootCommit:pin,newPrivateTables:[{name:'private.timesheet_correction_a0_transaction_fences',classification:'OWNER_PRIVATE_CONCURRENCY_ONLY',businessAuthority:false}],newOwnerPrivateSignatures:newFunctions,serviceSignaturesChanged:[],allCurrentInventoriesRetained:true,canonicalContractAndDefinitionHashes:'NOT_GENERATED_NATIVE_COMPILATION_BLOCKED',mandatoryVerifier:stamp+'_a0_transaction_fence_source_contract.sql'};
fs.writeFileSync(path.join(here,'EXACT_REGISTRY_DELTA.json'),JSON.stringify(delta,null,2)+'\n');
const nextRelease=structuredClone(release);for(const k of ['verificationFiles','newVerificationFiles']){assert.ok(Array.isArray(nextRelease[k]));const f='supabase/verification/'+delta.mandatoryVerifier;assert.ok(!nextRelease[k].includes(f));nextRelease[k].push(f);}fs.writeFileSync(path.join(here,'current-release.complete-proposed.json'),JSON.stringify(nextRelease,null,2)+'\n');
console.log(JSON.stringify({pin,migrations:migrations.length,repeatables:repeatables.length,routines:state.live.size,writerCandidates:writers.length,changed:changed.length,newPrivateFunctions:newFunctions.length,completeReversals:reversals.length,inputs:inputs.size}));
