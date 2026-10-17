import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
export const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),parent='af933e3e611a0b83d361a185bfda269cb6543bc8';
export const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
export const get=(commit,p)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+p],{cwd:repo,maxBuffer:40000000,windowsHide:true}).toString();
export const write=(p,b)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),b);};
const read=p=>fs.readFileSync(path.join(here,p),'utf8'),registry=JSON.parse(get(parent,'docs/bpay-next/final-definer-closures.json')),files=[];
function load(target,{signature,origin=target,commit=parent,save=true}={}){
 let raw=get(commit,origin);const physicalSha256=sha(raw);
 if(signature){const ds=functionEvents(raw).events.filter(e=>e.kind==='CREATE'&&e.id===signature);assert.equal(ds.length,1,signature);raw=ds[0].definition+'\n';}
 if(save)write('current/'+target,raw);return {target,signature,origin,commit,raw,original:raw,physicalSha256,save,hunks:[]};
}
function native(signature,leaf){const c=registry.chains.find(c=>c.signature===signature);assert(c);return load('native/'+leaf,{signature,origin:c.finalPhysical});}
function edit(f,before,after){
 const candidates=[...new Set([before,before.replaceAll('\r\n','\n'),before.replaceAll('\r\n','\n').replaceAll('\n','\r\n')])];
 const exact=candidates.filter(b=>f.raw.split(b).length===2);assert.equal(exact.length,1,'ONE_EXACT_ANCHOR '+f.target+' '+before.slice(0,100));before=exact[0];
 const newline=before.includes('\r\n')?'\r\n':before.includes('\n')?'\n':(f.raw.match(/\r\n/g)||[]).length>(f.raw.match(/(?<!\r)\n/g)||[]).length?'\r\n':'\n';
 after=after.replaceAll('\r\n','\n').replaceAll('\n',newline);f.raw=f.raw.replace(before,()=>after);f.hunks.push({before,after});}
function finish(f){let raw=f.raw;for(const h of [...f.hunks].reverse()){assert.equal(raw.split(h.after).length,2);raw=raw.replace(h.after,()=>h.before);}assert.equal(raw,f.original);if(f.save)write('forward/'+f.target,f.raw);files.push({target:f.target,signature:f.signature??null,input:{commit:f.commit,path:f.origin,wholePhysicalSha256:f.physicalSha256,sha256:sha(f.original),bytes:Buffer.byteLength(f.original),definitionOnly:!!f.signature},outputSha256:sha(f.raw),outputBytes:Buffer.byteLength(f.raw),hunks:f.hunks,fullRawReverse:true,savedWholeOutput:f.save});}

const gate=native('private.weekly_source_pay_decision_native_normal_gate_v1(uuid,uuid,uuid,text)','03_complete_native_gate.sqlinc');
edit(gate,'v_source:=private.bpay_next_source_decision_native_status_v1(p_family_id,p_run_id,p_actor_user_id);',
 'v_source:=private.weekly_source_pay_decision_native_status_child_v1(\n  private.bpay_next_source_decision_native_status_v1(p_family_id,p_run_id,p_actor_user_id),p_family_id,p_run_id,null,null);');finish(gate);
const saved=native('private.weekly_source_pay_decision_read_source_v1(uuid,uuid,uuid)','04_complete_internal_source_read.sqlinc');
edit(saved,'applications jsonb;native jsonb;cut jsonb;','applications jsonb;native jsonb;cut jsonb;current_request uuid;');
edit(saved,'begin\r\n if p_actor is null', 'begin\r\n perform private.weekly_source_pay_decision_read_access_v1(p_decision,p_request,p_actor);\r\n if p_actor is null');
edit(saved," if j.receipt_json->>'version' is distinct from 'SOURCE_PAY_DECISION_RECEIPT_V1'",` if not exists(select 1 from private.weekly_source_pay_decision_requests original_request
   where original_request.request_id=d.accept_request_id and original_request.decision_id=d.id and original_request.request_kind='ACCEPT'
    and original_request.metadata_state='COMMITTED' and original_request.receipt_json=d.receipt_json
    and original_request.request_hash=private.weekly_source_sha256_jsonb_v1('SOURCE_PAY_DECISION_CONFIRMATION_V1',original_request.request_json)
    and d.receipt_json->>'request_id'=original_request.request_id::text
    and d.receipt_json->>'review_id'=original_request.review_id::text
    and d.receipt_json->>'actor_user_id'=original_request.actor_user_id::text) then
  raise exception 'SOURCE_PAY_DECISION_SAVED_RECEIPT_INVALID' using errcode='23514';end if;
 if j.receipt_json->>'version' is distinct from 'SOURCE_PAY_DECISION_RECEIPT_V1'`.replaceAll('\n','\r\n'));
edit(saved,"   where a.decision_id=d.id and a.state='COMMITTED';",`   where a.decision_id=d.id and a.state='COMMITTED';
 if not exists(select 1 from private.weekly_source_pay_decision_acceptance_applications current_application
   join private.bpay_next_source_pay_decision_cut current_cut on current_cut.id=current_application.cut_id
    and current_cut.decision_id=current_application.decision_id and current_cut.review_id=current_application.review_id
   where current_application.decision_id=d.id and current_application.cut_id=d.current_financial_cut_id
    and current_application.state='COMMITTED' and current_application.application_sha256 is not null)
  or exists(select 1 from private.weekly_source_pay_decision_acceptance_applications application
   left join private.weekly_source_pay_decision_requests request on request.request_id=application.request_id
    and request.decision_id=application.decision_id and request.review_id=application.review_id and request.metadata_state='COMMITTED'
   left join private.bpay_next_source_pay_decision_cut application_cut on application_cut.id=application.cut_id
    and application_cut.decision_id=application.decision_id and application_cut.review_id=application.review_id
   where application.decision_id=d.id and (application.state<>'COMMITTED' or application.application_sha256 is null
    or request.request_id is null or application_cut.id is null
    or request.receipt_json#>>'{financial_publications,cut_id}' is distinct from application.cut_id::text)) then
  raise exception 'SOURCE_PAY_DECISION_SAVED_APPLICATION_INVALID' using errcode='23514';end if;`.replaceAll('\n','\r\n'));
edit(saved,"   on a.id=n.application_id where a.decision_id=d.id and (a.state<>'COMMITTED' or n.native_result_sha256 is distinct from",`   on a.id=n.application_id where a.decision_id=d.id and (a.state<>'COMMITTED'
    or n.decision_id is distinct from a.decision_id or n.review_id is distinct from a.review_id
    or not exists(select 1 from private.weekly_source_pay_decision_unprotect_preparations preparation
     where preparation.id=n.preparation_id and preparation.decision_id=a.decision_id and preparation.review_id=a.review_id
      and preparation.accept_or_resume_request_id=a.request_id and preparation.member_key=n.member_key and preparation.route=n.route)
    or (n.native_result_json is not null and (n.native_result_json->>'application_id' is distinct from a.id::text
     or n.native_result_json->>'decision_id' is distinct from a.decision_id::text
     or n.native_result_json->>'request_id' is distinct from a.request_id::text
     or n.native_result_json->>'review_id' is distinct from a.review_id::text
     or n.native_result_json->>'member_key' is distinct from n.member_key
     or n.native_result_json->>'preparation_id' is distinct from n.preparation_id::text
     or n.native_result_json->>'origin' is distinct from n.origin or n.native_result_json->>'route' is distinct from n.route))
    or n.native_result_sha256 is distinct from`.replaceAll('\n','\r\n'));
edit(saved," select jsonb_build_object('cut_id',c.id,'cut_no',c.cut_no::text,'review_id',c.review_id,'qualification_state',c.qualification_state,",` select application.request_id into strict current_request
  from private.weekly_source_pay_decision_acceptance_applications application
  where application.decision_id=d.id and application.cut_id=d.current_financial_cut_id and application.state='COMMITTED';
 if current_request is distinct from j.request_id then
  perform private.weekly_source_pay_decision_read_access_v1(d.id,current_request,p_actor);
 end if;
 select jsonb_build_object('cut_id',c.id,'cut_no',c.cut_no::text,'review_id',c.review_id,'qualification_state',c.qualification_state,`.replaceAll('\n','\r\n'));finish(saved);
const internal=native('public.weekly_source_pay_decision_read_source_v1(jsonb)','05_complete_internal_service_read.sqlinc');
edit(internal,'begin\r\n if coalesce','declare k text;\r\nbegin\r\n if coalesce');
edit(internal," return private.weekly_source_pay_decision_read_source_v1((p_request->>'decision_id')::uuid,(p_request->>'request_id')::uuid,(p_request->>'actor_user_id')::uuid);",` foreach k in array array['decision_id','request_id','actor_user_id'] loop
  if jsonb_typeof(p_request->k) is distinct from 'string'
   or coalesce(p_request->>k,'') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
   raise exception 'SOURCE_PAY_DECISION_REQUEST_INVALID' using errcode='22023';end if;
 end loop;
 return private.weekly_source_pay_decision_read_source_v1((p_request->>'decision_id')::uuid,(p_request->>'request_id')::uuid,(p_request->>'actor_user_id')::uuid);`.replaceAll('\n','\r\n'));finish(internal);
const accept=native('public.weekly_source_pay_decision_accept_v1(jsonb)','06_complete_public_source_accept_read.sqlinc');
edit(accept,"  perform private.weekly_source_pay_decision_read_source_v1((body->>'decision_id')::uuid,(body->>'request_id')::uuid,actor);",`  select request.* into j from private.weekly_source_pay_decision_requests request
   where request.request_id=(body->>'request_id')::uuid and request.decision_id=(body->>'decision_id')::uuid
    and request.request_kind in('ACCEPT','RESUME_FINANCIAL');
  if not found then raise exception 'SOURCE_PAY_DECISION_SAVED_RESULT_NOT_FOUND' using errcode='22023';end if;
  if j.metadata_state='COMMITTED' then
   perform private.weekly_source_pay_decision_read_source_v1(j.decision_id,j.request_id,actor);
  else
   perform private.weekly_source_pay_decision_read_access_v1(j.decision_id,j.request_id,actor);
  end if;`.replaceAll('\n','\r\n'));finish(accept);

const worker=load('broker/src/weekly-source/source-pay-decision-worker.mjs');
worker.raw+=`\n// Actual public READ_RESULT service path. The approved public current-result\n// envelope/B051 financial child is not supplied. Do not promote internal9, an\n// accepted15 receipt, or opaque native JSON when a producer unexpectedly returns.\nexport async function readCurrentSourcePayDecision({request,actorUserId,...provided}){\n const body=decodeSourcePayDecisionBrowserRequest(request,'READ_RESULT'),owner=dependencies(actorUserId,provided);\n await owner.dataRpc('weekly_source_pay_decision_accept_v1',{p_request:{...body,actor_user_id:actorUserId,\n  preview_assembly_ref:null,unprotect_preparations:[]}});\n const error=new Error('The Source pay decision result is invalid.');error.code='SOURCE_PAY_DECISION_RESULT_INVALID';error.status=502;throw error;\n}\n`;
worker.hunks.push({before:'',after:worker.raw.slice(worker.original.length)});finish(worker);
const http=load('broker/src/weekly-source/source-pay-decision-http.mjs');
edit(http,'preview,confirm,readSource})','preview,confirm,readResult})');
edit(http,"   if(typeof readSource!=='function')fail('SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED',503);\r\n   // Authenticate/reprove exact saved request/current SQL scope through its\r\n   // already declared real owner. Its private nine-key result is never sent.\r\n   await readSource({request,actorUserId:user.id});\r\n   fail('SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED',503);",`   if(typeof readResult!=='function')fail('SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED',503);
   // The genuine public SQL READ_RESULT arm owns saved request/current scope
   // and native-child validation. It returns the exact dependency refusal until
   // the separately missing public envelope/financial child is supplied.
   await readResult({request,actorUserId:user.id});
   fail('SOURCE_PAY_DECISION_RESULT_INVALID',502);`.replaceAll('\n','\r\n'));
edit(http," SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED:['55000',503]\r\n",` SOURCE_PAY_DECISION_READ_PROJECTION_DEPENDENCY_REQUIRED:['55000',503],
 SOURCE_PAY_DECISION_SCOPE_DENIED:['42501',403],SOURCE_PAY_DECISION_SAVED_RESULT_NOT_FOUND:['22023',404],
 SOURCE_PAY_DECISION_SAVED_RECEIPT_INVALID:['23514',502],SOURCE_PAY_DECISION_SAVED_APPLICATION_INVALID:['23514',502],
 SOURCE_PAY_DECISION_SAVED_NATIVE_RESULT_INVALID:['23514',502],SOURCE_NATIVE_STATUS_INVALID:['55000',502],
 SOURCE_NATIVE_OWNERSHIP_CONTRADICTION:['55000',502]
`.replaceAll('\n','\r\n'));finish(http);
const contract=load('broker/src/weekly-source/source-pay-decision-contract.mjs');
edit(contract," const cut=v.current_cut;",` // Internal Source evidence remains separate from the public accepted receipt.
 if(v.accepted_receipt.decision!==v.request_receipt.decision
  ||v.accepted_receipt.reason!==v.request_receipt.reason
  ||BigInt(v.accepted_receipt.state_revision)>BigInt(v.request_receipt.state_revision))invalid();
 const acceptedMembers=new Map(v.accepted_receipt.members.map(m=>[m.member_key,m]));
 if(acceptedMembers.size!==v.request_receipt.members.length)invalid();
 // Original Source-only NULL family/event evidence stays in accepted_receipt.
 // A later genuine native binding belongs to its separate request receipt;
 // current SQL proves that association. Do not equate absence with current IDs.
 for(const m of v.request_receipt.members){const original=acceptedMembers.get(m.member_key);
  if(!original||!['scope_kind','scope_mode','source_row_id'].every(k=>same(m[k],original[k])))invalid();}
 const cut=v.current_cut;`.replaceAll('\n','\r\n'));
edit(contract," list(v.native_members,n=>{",` const requestApplication=v.applications.filter(a=>a.request_id===v.request_id),
  acceptedApplication=v.applications.filter(a=>a.request_id===v.accepted_receipt.request_id),
  currentApplication=v.applications.filter(a=>a.cut_id===cut.cut_id&&a.review_id===cut.review_id);
 if(new Set(v.applications.map(a=>a.request_id)).size!==v.applications.length
  ||new Set(v.applications.map(a=>a.cut_id)).size!==v.applications.length
  ||new Set(v.applications.map(a=>a.review_id)).size!==v.applications.length
  ||requestApplication.length!==1||acceptedApplication.length!==1||currentApplication.length!==1
  ||requestApplication[0].review_id!==v.request_receipt.review_id
  ||requestApplication[0].cut_id!==v.request_receipt.financial_publications.cut_id
  ||acceptedApplication[0].review_id!==v.accepted_receipt.review_id
  ||acceptedApplication[0].cut_id!==v.accepted_receipt.financial_publications.cut_id)invalid();
 list(v.native_members,n=>{`.replaceAll('\n','\r\n'));
edit(contract,"  if(n.origin==='NOT_APPLIED_DEPENDENCY')",`  if(!acceptedMembers.has(n.member_key))return false;
  const application=v.applications.find(a=>a.application_id===n.application_id);
  if(n.origin==='NOT_APPLIED_DEPENDENCY')`.replaceAll('\n','\r\n'));
edit(contract,'decodeSourceNativeComposedResult(n.original_native_result,{application_id:n.application_id,decision_id:v.decision_id,request_id:n.request_id,member_key:n.member_key,',
 'decodeSourceNativeComposedResult(n.original_native_result,{application_id:n.application_id,decision_id:v.decision_id,request_id:n.request_id,review_id:application.review_id,member_key:n.member_key,');finish(contract);

const sameCommit='a7e4522cb87ae6961cb40efcb83e4e69b12fbd6c',samePrefix='docs/healthroster-build/proposals/same-separate-counter-forward-20261017/',same=JSON.parse(get(sameCommit,samePrefix+'COMPOSED_SELECTION.json'));
const routeTarget='broker/src/weekly-source/routes.js',routeParent=same.files.find(f=>f.target===routeTarget),routes=load(routeTarget,{commit:routeParent.sourceCommit??sameCommit,origin:routeParent.sourceCommit?routeParent.sourcePath:samePrefix+routeParent.sourcePath});
edit(routes,'readSource: dependencies.readSourcePayDecisionSource,','readResult: dependencies.readCurrentSourcePayDecision,');finish(routes);
const index=load('broker/src/index.js',{save:false});
edit(index,'previewSourcePayDecision,confirmSourcePayDecision,readSourcePayDecisionSource','previewSourcePayDecision,confirmSourcePayDecision,readCurrentSourcePayDecision');
edit(index,'        readSourcePayDecisionSource: ({ request, actorUserId }) => readSourcePayDecisionSource({','        readCurrentSourcePayDecision: ({ request, actorUserId }) => readCurrentSourcePayDecision({');finish(index);
const classificationTarget='supabase/repeatable/17102026_0231_weekly_source_paid_same_separate_health/98_complete_current_classification.sqlinc',classificationParent=same.files.find(f=>f.target===classificationTarget);
const classification=load('native/98_complete_current_classification.sqlinc',{commit:classificationParent.sourceCommit??sameCommit,origin:classificationParent.sourceCommit?classificationParent.sourcePath:samePrefix+classificationParent.sourcePath});
const newPrivate=['private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid)','private.weekly_source_pay_decision_read_access_v1(uuid,uuid,uuid)'],
 existingPrivate=['private.weekly_source_pay_decision_native_normal_gate_v1(uuid,uuid,uuid,text)','private.weekly_source_pay_decision_read_source_v1(uuid,uuid,uuid)'],
 existingService=['public.weekly_source_pay_decision_accept_v1(jsonb)','public.weekly_source_pay_decision_read_source_v1(jsonb)'];
for(const signature of [...newPrivate,...existingPrivate,...existingService])assert(!classification.raw.includes("('"+signature+"')"));
const privateAnchor="    ('private.weekly_source_keep_as_paid_scope_rows_v1(uuid,uuid,uuid,uuid,uuid,date)'),";
edit(classification,privateAnchor,privateAnchor+'\n'+[...newPrivate,...existingPrivate].sort().map(s=>"    ('"+s+"'),").join('\n'));
const serviceEvent=functionEvents(classification.raw).events.find(e=>e.kind==='CREATE'&&e.id==='private._weekly_source_acl_service_rpc_contract_v1()');assert(serviceEvent);
const serviceAnchor=serviceEvent.definition.match(/\(\'public[.]weekly_source_[^\r\n]+/)[0];
edit(classification,serviceAnchor,existingService.map(s=>"('"+s+"'),\n    ").join('')+serviceAnchor);finish(classification);
for(const f of files.filter(f=>f.target.startsWith('broker/'))){const raw=f.savedWholeOutput?read('forward/'+f.target):index.raw;execFileSync(process.execPath,['--input-type=module','--check'],{input:raw,windowsHide:true});}
write('CURRENT_OWNER_EDITS.json',JSON.stringify({parent,SameFunctionalParent:sameCommit,files,newPrivate,newService:[],newRelations:[],expectedClassificationDelta:{newPrivate,existingPrivate,existingService},publicResultSuccess:'HELD_EXACT_B051_CHILD_AND_OUTER_SCHEMA_NOT_SUPPLIED',native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({completeCurrentInputs:files.length,rawReversals:files.length,newPrivate:2,newService:0,classificationDeltaPrivate:4,classificationDeltaService:2,indexHunksOnly:true,rootEdits:0,native:'NOT_RUN'}));
