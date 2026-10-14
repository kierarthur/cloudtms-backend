// Unmounted owner composition proposal. Reads immutable native Git/source input;
// writes only this lane's source packet, never canonical Banking or a database.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),out=path.join(here,'financial-native-input-20261014');
const moneyRoot='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026';
const moneyPacket=path.join(moneyRoot,'docs/healthroster-build/proposals/native-checklist-financial-grain-reconciliation-20261014');
const sourceCommit='fa66183f01913e86651e82816f5e88f97e07123c',moneyCommit='76a5dbd0721ae2c41edb049312aa81d5df5a06be';
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(args,input)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:repo,input,maxBuffer:50e6});
const save=(file,bytes)=>{const target=path.join(out,file);fs.mkdirSync(path.dirname(target),{recursive:true});if(fs.existsSync(target)&&!fs.readFileSync(target).equals(Buffer.from(bytes)))throw Error('Frozen financial input changed: '+file);fs.writeFileSync(target,bytes);};
export function financialNativeCore(definition,{nativeName,coreName,args,lockStatements=[],calls=[]}){
  let core=definition;const changes=[];
  const once=(before,after)=>{assert.equal(core.split(before).length,2,'Exact native seam changed: '+before);core=core.replace(before,after);changes.push({before,after});};
  once('function '+nativeName+'(','function '+coreName+'(');
  const start=core.indexOf('('),end=core.indexOf(')',start);once(core.slice(start,end+1),core.slice(start,end)+',p_native_locks boolean)');
  // Original lock statements stay exactly in the native branch, at the same
  // point between the existing qualifications. Read mode removes only those
  // locks and propagates the same owner-private mode through nested cores.
  for(const statement of lockStatements){assert.ok(/ for share;$/i.test(statement));
    once(statement,'if p_native_locks then\n  '+statement+'\n  else\n  '+statement.replace(/ for share;$/i,';')+'\n  end if;');}
  for(const [before,after] of calls)once(before,after);
  once('begin\n','begin\n  if p_native_locks is null then raise exception \'WEEKLY_SOURCE_CHECKLIST_NATIVE_ACCESS_MODE_REQUIRED\' using errcode=\'22023\'; end if;\n');
  let restored=core;for(const change of [...changes].reverse()){assert.equal(restored.split(change.after).length,2);restored=restored.replace(change.after,change.before);}assert.equal(restored,definition);
  const tag=definition.match(/\bas\s+(\$\w*\$)/i)?.[1];assert.ok(tag);const bodyStart=definition.indexOf(tag)+tag.length,bodyEnd=definition.indexOf(tag,bodyStart);
  const header=definition.slice(0,bodyStart),voidResult=/\breturns\s+void\b/i.test(header),setResult=/\breturns\s+(?:table\s*\(|setof\b)/i.test(header);
  const wrapper=header+'\nbegin\n  '+(voidResult?'perform ':setResult?'return query select * from ':'return ')+coreName+'('+args.join(',')+',true);\nend\n'+definition.slice(bodyEnd);
  return {definition,core,wrapper,changes,nativeName,coreName,args};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const manifestBytes=fs.readFileSync(path.join(moneyPacket,'FROZEN_ARTIFACT_MANIFEST.json'));
  assert.equal(hash(manifestBytes),'a864f502301c8fd4f6eb95404c06fd3c14b90e4400e0180ba782dee396b9977f');
  const manifest=JSON.parse(manifestBytes);assert.equal(manifest.artifact_count,30);
  for(const artifact of manifest.artifacts){const bytes=fs.readFileSync(path.join(moneyPacket,artifact.file));assert.equal(hash(bytes),artifact.sha256);assert.equal(bytes.length,artifact.bytes);save('money30/'+artifact.file,bytes);}
  save('money30/FROZEN_ARTIFACT_MANIFEST.json',manifestBytes);
  const parserFile='docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
  const dateFile='docs/healthroster-build/proposals/strict-qualification-install1-forward/PINNED_SQL_DATE_ORDER.mjs';
  for(const file of [parserFile,dateFile]){const bytes=execFileSync('git',['show',moneyCommit+':'+file],{cwd:moneyRoot,maxBuffer:5e6});save(path.basename(file),bytes);}
  const {functionEvents,applyFunctionEvents,emptyFunctionIndex,sqlTokens}=await import(pathToFileURL(path.join(out,path.basename(parserFile))));
  const {sqlDateKey}=await import(pathToFileURL(path.join(out,path.basename(dateFile))));
  const files=git(['ls-tree','-r','--name-only',sourceCommit,'--','supabase']).toString().trim().split('\n').filter(f=>/\.(sql|sqlinc|inc)$/i.test(f)||f==='supabase/release/current-release.json'),cache=new Map();
  for(let i=0;i<files.length;i+=64){const paths=files.slice(i,i+64),bytes=git(['cat-file','--batch'],paths.map(p=>sourceCommit+':'+p+'\n').join(''));let cursor=0;
    for(const file of paths){const end=bytes.indexOf(10,cursor),header=bytes.subarray(cursor,end).toString().split(' ');assert.equal(header[1],'blob');const length=Number(header[2]);cursor=end+1;cache.set(file,bytes.subarray(cursor,cursor+length));cursor+=length+1;}assert.equal(cursor,bytes.length);}
  const release=JSON.parse(cache.get('supabase/release/current-release.json')),sort=rows=>rows.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
  const roots=[...release.baselineFiles,...sort(files.filter(f=>/^supabase\/migrations\/.*\.sql$/i.test(f))),...sort(files.filter(f=>/^supabase\/repeatable\/.*\.sql$/i.test(f)))],index=emptyFunctionIndex(),inputs=new Map(),occurrences=[];let order=0;
  function visit(file,stack=[]){assert.ok(!stack.includes(file));const bytes=cache.get(file);assert.ok(bytes,file);const source=bytes.toString().replaceAll('\r\n','\n'),parsed=scanManagedSql(source);inputs.set(file,{file,bytes:bytes.length,sha256:hash(bytes)});occurrences.push({file,include_stack:stack,sha256:hash(bytes)});
    for(const unit of [...parsed.statements.map(s=>({...s,kind:'statement'})),...parsed.meta.map(s=>({...s,kind:'meta'}))].sort((a,b)=>a.start-b.start)){const statement=source.slice(unit.start,unit.end);
      if(unit.kind==='meta'){const match=statement.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);if(match)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]??match[2]??match[3])),[...stack,file]);continue;}
      const events=functionEvents(statement);index.unresolved.push(...events.unresolved.map(e=>({...e,file,order:order++})));applyFunctionEvents(index,events.events,e=>({...e,file,include_stack:stack,order:order++,source_file_sha256:hash(bytes),definition_sha256:hash(e.definition??statement)}));}}
  for(const file of roots)visit(file);
  const censusPath='docs/healthroster-build/proposals/root-effective-body-amendment-census-case-forward-20261014/BODY_AMENDMENT_CENSUS.json',censusBytes=git(['show','973625f17cd51a49803872a819638e10f13bbbff:'+censusPath]),census=JSON.parse(censusBytes),expected=new Map(census.sourceInputs.map(p=>[p.file,p]));
  assert.deepEqual([roots.length,inputs.size,occurrences.length],[1484,1745,2160]);for(const [file,pin] of inputs){assert.equal(pin.sha256,expected.get(file)?.sha256);assert.equal(pin.bytes,expected.get(file)?.bytes);}
  const selectedNames=['private.bpay_next_bound_source_snapshot_v1(uuid,bigint)','private.bpay_next_initial_tsfin_certificate_v1(uuid)',
    'private.bpay_next_initial_tsfin_bound_assert_v1(uuid)','private.bpay_next_workbench_work_members_v1(uuid,uuid)'];
  const selected=selectedNames.map(name=>{const record=index.live.get(name);assert.ok(record,name);return record;});
  const configurations=[
    {nativeName:'private.bpay_next_bound_source_snapshot_v1',coreName:'private.weekly_source_checklist_native_bound_snapshot_core_v1',args:['p_binding_version_id','p_expected_book_revision'],
      lockStatements:['perform 1 from private.bpay_next_module_control c where c.id=1 and c.active_owner=\'NEXT\' for share;',
        'perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_version.candidate_id for share;',
        'select b.* into strict v_binding from private.bpay_next_source_component_binding b\n    where b.canonical_work_id=v_version.canonical_work_id and b.canonical_component_key=v_version.canonical_component_key for share;',
        'select p.* into v_position from private.bpay_next_position p\n    where p.work_id=v_version.canonical_work_id and p.component_key=v_version.canonical_component_key for share;'],
      calls:[['private.bpay_next_initial_tsfin_bound_assert_v1(v_version.id)','private.weekly_source_checklist_native_initial_bound_assert_core_v1(v_version.id,p_native_locks)']]},
    {nativeName:'private.bpay_next_initial_tsfin_certificate_v1',coreName:'private.weekly_source_checklist_native_initial_certificate_core_v1',args:['p_root_authorisation_id'],
      lockStatements:['select m.owner_epoch into epoch from private.bpay_next_module_control m\n    where m.id=1 and m.active_owner=\'NEXT\' for share;']},
    {nativeName:'private.bpay_next_initial_tsfin_bound_assert_v1',coreName:'private.weekly_source_checklist_native_initial_bound_assert_core_v1',args:['p_binding_version_id'],
      calls:[['private.bpay_next_initial_tsfin_certificate_v1(v.initial_root_authorisation_id)','private.weekly_source_checklist_native_initial_certificate_core_v1(v.initial_root_authorisation_id,p_native_locks)']]},
    {nativeName:'private.bpay_next_workbench_work_members_v1',coreName:'private.weekly_source_checklist_native_members_core_v1',args:['p_work_id','p_revision_id'],
      calls:[['private.bpay_next_bound_source_snapshot_v1(v.id,p.source_book_revision)','private.weekly_source_checklist_native_bound_snapshot_core_v1(v.id,p.source_book_revision,p_native_locks)']]}
  ];
  const proposals=selected.map((record,i)=>financialNativeCore(record.definition,configurations[i]));
  const routinesByName=new Map(),namesByBase=new Map(),referenceCache=new Map();
  for(const record of index.live.values()){if(!routinesByName.has(record.name))routinesByName.set(record.name,[]);
    routinesByName.get(record.name).push(record);const base=record.name.split('.').at(-1);if(!namesByBase.has(base))namesByBase.set(base,new Set());namesByBase.get(base).add(record.name);}
  const references=body=>{if(referenceCache.has(body))return referenceCache.get(body);const tokens=sqlTokens(body),names=new Set();for(let i=0;i<tokens.length-3;i++){
    if(['public','private','extensions'].includes(tokens[i].value)&&tokens[i+1].value==='.'&&
      ['word','identifier'].includes(tokens[i+2].kind)&&tokens[i+3].value==='(')names.add(tokens[i].value+'.'+tokens[i+2].value);
    // Keep actual unqualified native calls visible too. Resolving every
    // overload conservatively avoids choosing an invented caller signature.
    if(tokens[i].kind==='word'&&tokens[i+1].value==='('&&tokens[i-1]?.value!=='.'){
      for(const name of namesByBase.get(tokens[i].value)||[])names.add(name);}}
    const result=[...names].sort();referenceCache.set(body,result);return result;};
  const graphRoots=[...selectedNames,'private.weekly_source_pay_query_scope_v1(uuid)','private.weekly_source_pay_query_gate_v1(uuid)',
    'private.bpay_next_source_query_segment_exclusions_v1(uuid,jsonb)','private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)',
    'private.weekly_source_query_require_service_v1()','private._weekly_source_settings_assert_request_v1(jsonb,text[],text)',
    'private.bpay_next_workbench_accepted_work_v1(uuid,text,text)','private.bpay_next_snooze_at_cut_v1(uuid,uuid,text,text,bigint,date)'];
  const graph=new Map(),queue=[...graphRoots];while(queue.length){const id=queue.shift();if(graph.has(id))continue;const record=index.live.get(id);assert.ok(record,id);
    const names=references(record.body),callees=names.flatMap(name=>(routinesByName.get(name)||[]).map(r=>r.id));
    const tokens=sqlTokens(record.body).filter(t=>t.kind==='word').map(t=>t.value);
    const writes=[...new Set(tokens.filter(t=>['insert','update','delete','truncate','lock','notify','execute','call','create','set_config',
      'pg_advisory_lock','pg_advisory_xact_lock','pg_try_advisory_xact_lock'].includes(t)))];
    const directLocks=/\bfor\s+(?:update|share|no\s+key\s+update|key\s+share)\b/i.test(record.body);
    const file='read-graph/'+id.replaceAll(/[^a-z0-9_.-]/g,'_')+'.sqlinc';save(file,record.definition);
    graph.set(id,{id,name:record.name,file,source_file:record.file,source_file_sha256:record.source_file_sha256,
      definition_sha256:hash(record.definition),body_sha256:hash(record.body),callees,qualified_names_without_source:names.filter(name=>!routinesByName.has(name)),
      conservative_direct_write_or_lock_tokens:writes,direct_row_locks:directLocks,mode_extracted:selectedNames.includes(id)});
    queue.push(...callees);}
  fs.writeFileSync(path.join(out,'NATIVE_READ_CALL_GRAPH.json'),JSON.stringify({status:'FINITE_SOURCE_GRAPH_REQUIRES_EXECUTED_PATH_AND_INSTALLED_BODY_REVIEW',
    sourceCommit,roots:graphRoots,routines:[...graph.values()].sort((a,b)=>a.id.localeCompare(b.id)),
    public_Bank_wrapper_used:false,refresh_mutator_used:false,nativeExecuted:false},null,2)+'\n');
  const callerCensus=selectedNames.map(id=>({id,callers:[...index.live.values()].filter(r=>references(r.body).includes(index.live.get(id).name))
    .map(r=>({id:r.id,file:r.file,source_file_sha256:r.source_file_sha256,definition_sha256:hash(r.definition)}))}));
  fs.writeFileSync(path.join(out,'NATIVE_WRAPPER_CALLER_CENSUS.json'),JSON.stringify({status:'CURRENT_LITERAL_SOURCE_CALLER_CENSUS_INSTALLED_BODY_HELD',
    sourceCommit,wrapper_signature_and_native_locked_mode_preserved:true,callers:callerCensus},null,2)+'\n');
  const hookNames=['private.bpay_next_source_query_refresh_v1(uuid,uuid)','private.bpay_next_workbench_mark_processing_v1(uuid)'];
  const hooks=hookNames.map(id=>{const r=index.live.get(id);assert.ok(r,id);const file='freshness-hooks/'+r.name.replace('.','_')+'.sqlinc';save(file,r.definition);
    return {id,file,source_file:r.file,source_file_sha256:r.source_file_sha256,definition_sha256:hash(r.definition),
      callers:[...index.live.values()].filter(c=>references(c.body).includes(r.name)).map(c=>({id:c.id,file:c.file,
        source_file_sha256:c.source_file_sha256,definition_sha256:hash(c.definition)}))};});
  fs.writeFileSync(path.join(out,'NATIVE_FRESHNESS_HOOK_BOUNDARY.json'),JSON.stringify({status:'BOUNDED_ACTUAL_NEXT_HOOK_CENSUS_NOT_COMPLETE_INSTALLED_DIRTYING_PROOF',
    sourceCommit,not_in_GET_graph:true,legacy_mark_candidate_dirty_is_not_NEXT_projection_coverage:true,hooks,nativeExecuted:false},null,2)+'\n');
  const acl=(name,types)=>`alter function ${name}(${types}) owner to postgres;\nrevoke all on function ${name}(${types}) from public,anon,authenticated,service_role;\n`;
  // Dependency order: certificate -> bound assertion -> bound snapshot -> members.
  const coreOrder=[1,2,0,3],dropOrder=[3,0,2,1];
  const forward=coreOrder.map(i=>proposals[i].core+'\n'+acl(proposals[i].coreName,selected[i].inputTypes+',boolean')).join('\n')+'\n'+
    proposals.map((p,i)=>p.wrapper+'\n'+acl(p.nativeName,selected[i].inputTypes)).join('\n');
  const reverse=proposals.map((p,i)=>p.definition+'\n'+acl(p.nativeName,selected[i].inputTypes)).join('\n')+'\n'+
    dropOrder.map(i=>`drop function ${proposals[i].coreName}(${selected[i].inputTypes},boolean);`).join('\n')+'\n';
  const proposalDir=path.join(repo,'supabase/repeatable/healthroster_wp_u3_source_proposals');
  fs.writeFileSync(path.join(proposalDir,'wp_u3_financial_native_kernel.forward.sqlinc'),forward);
  fs.writeFileSync(path.join(proposalDir,'wp_u3_financial_native_kernel.reverse.sqlinc'),reverse);
  const pins=selected.map(record=>{const {body,definition,...metadata}=record;save('native/'+record.name.replace('.','_')+'.sqlinc',definition);return {...metadata,body_sha256:hash(body)};});
  fs.writeFileSync(path.join(out,'NATIVE_KERNEL_INPUT_PINS.json'),JSON.stringify({status:'SOURCE_ONLY_SINGLE_SHARED_KERNEL_PROPOSAL_NOT_MOUNTED_OR_NATIVE_QUALIFIED',
    sourceCommit,moneyCommit,moneyManifestSha256:hash(manifestBytes),roots:roots.length,uniqueInputs:inputs.size,occurrences:occurrences.length,
    rootCensusSha256:hash(censusBytes),unresolvedDynamicStatements:index.unresolved.length,nativeExecuted:false,canonicalBankBodyEdited:false,
    selected:pins,transformations:proposals.map(p=>({nativeName:p.nativeName,coreName:p.coreName,changes:p.changes,fullReverseMatched:true})),
    forward_sha256:hash(forward),reverse_sha256:hash(reverse)},null,2)+'\n');
  console.log('Money30 exact source inputs preserved; four native qualification/member bodies factored once with exact native-mode mechanical reverses. Canonical Bank source unchanged; read graph/native parity remains held.');
}
