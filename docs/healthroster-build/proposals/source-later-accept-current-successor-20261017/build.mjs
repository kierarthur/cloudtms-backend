// Health-owned Later Accept source composition. No DB or managed-source writes.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026');
export const checkpoint='24c57e6c7af4a8fb07d5e3126ab9ac76e9448560',missingCommit='01ec68ef68a2cb16aa86ddb058b5d37989a60f43',sha=b=>createHash('sha256').update(b).digest('hex');
const prefix='docs/healthroster-build/proposals/',inputs=[],owners=[],leaves={};
export const write=(p,s)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),s);};
export const splice=(s,b,a)=>{assert.equal(s.split(b).length,2,'ONE_EXACT_ANCHOR: '+b.slice(0,100));return s.replace(b,()=>a);};
export const apply=(s,hs)=>hs.reduce((v,h)=>splice(v,h.before,h.after),s),reverse=(s,hs)=>apply(s,[...hs].reverse().map(h=>({before:h.after,after:h.before})));
export const blob=(commit,p)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+p],{cwd:repo,windowsHide:true,maxBuffer:64_000_000}).toString();
const read=p=>fs.readFileSync(path.join(repo,p),'utf8');
function selected(packet,p){const file=prefix+packet+'/'+p,source=read(file);assert.equal(source,blob(missingCommit,file),'PRESERVED_HEALTH_INPUT_DRIFT '+file);inputs.push({commit:missingCommit,file,bytes:Buffer.byteLength(source),sha256:sha(source)});return source;}
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'))),{routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
export function complete(source,signature){const parsed=scanManagedSql(source),all=parsed.statements.map(s=>parsed.source.slice(s.start,s.end).trim()).filter(s=>routineDefinitions(s,signature).includes(signature));assert.equal(all.length,1,signature);return all[0];}
function current(signature,physical){const raw=blob(checkpoint,physical);assert.equal(fs.readFileSync(path.join(root,physical),'utf8'),raw,'CURRENT_SAVED_SOURCE_DRIFT '+physical);inputs.push({commit:checkpoint,file:physical,bytes:Buffer.byteLength(raw),sha256:sha(raw)});return complete(raw,signature);}
function amended(signature,leaf,before,hunks,provenance){const after=apply(before,hunks);assert.equal(reverse(after,hunks),before);write('current/'+leaf,before+'\n');leaves[leaf]=after+'\n';owners.push({signature,leaf,provenance,inputSha256:sha(before),outputSha256:sha(after),hunks,completeReverseByteEquality:true});return after;}
const old='missing-accept-current-native-construction-20261014',off='missing-accept-off-admission-construction-20261014',missing='missing-source-health-current-successor-20261017';
for(const [leaf,packet,p]of[
 ['00_admission.sqlinc',off,'forward/00_missing_accept_admission.sqlinc'],
 ['01_head_chain.sqlinc','missing-accept-head-continuity-construction-20261014','forward/00_missing_accept_head_chain.sqlinc'],
 ['02_current_removal.sqlinc','missing-accept-deferred-anchor-successor-20261014','forward/01_complete_current_removal.sqlinc'],
 ['03_inventory_inputs.sqlinc',off,'forward/05_complete_missing_accept_inputs.sqlinc'],
 ['04_proposal_read.sqlinc',old,'forward/03_missing_accept_proposal_read.sqlinc'],
 ['05_prepare.sqlinc',off,'forward/06_complete_missing_accept_prepare.sqlinc'],
 ['06_proof_guard.sqlinc',old,'forward/02_missing_accept_proof_guard.sqlinc'],
 ['07_prepare_entry.sqlinc',old,'forward/09_missing_accept_prepare_public_entry.sqlinc'],
 ['08_context_entry.sqlinc',old,'forward/10_missing_accept_context_public_entry.sqlinc'],
])leaves[leaf]=selected(packet,p);
leaves['01_head_chain.sqlinc']=splice(leaves['01_head_chain.sqlinc'],"'canonical_components',v_components)));","'canonical_components',v_components));");
// The complete current a34-compatible context is the virtual parent01ec,
// never the older draft context that selected an ACK after filtering.
const oldEdits=JSON.parse(selected(old,'COMPLETE_CURRENT_REVERSES.json'));
const contextBefore=complete(selected(missing,'forward/01_target_publication_context.sqlinc'),'private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid)');
const contextHunks=oldEdits.proofs.find(p=>p.file==='08_complete_kept_context.sqlinc').hunks;
amended('private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid)','09_complete_current_context.sqlinc',contextBefore,contextHunks,{commit:missingCommit,virtualParent:missing+'/forward/01_target_publication_context.sqlinc',typedACKWriterAuditCounterUnchanged:true});
const laterSignature='public.weekly_source_later_change_decide_atomic_v1(jsonb)',laterPath='supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql';
amended(laterSignature,'10_complete_later_change_decide.sqlinc',current(laterSignature,laterPath),oldEdits.proofs.find(p=>p.file==='05_complete_later_change_decide.sqlinc').hunks,{commit:checkpoint,physical:laterPath});
const basisSignature='private.weekly_source_missing_cancel_basis_v1(uuid,uuid,uuid)',basisBefore=complete(selected(missing,'forward/03_missing_cancel_basis.sqlinc'),basisSignature);
amended(basisSignature,'11_complete_cancel_basis.sqlinc',basisBefore,[
 {before:'  v_disposition text; v_stack text;',after:'  v_disposition text; v_stack text; v_accepted_removed boolean;'},
 {before:'  v_original:=private.weekly_source_missing_snapshot_movement_v1(v_snapshot.id);',after:"  -- Decide from the genuine committed removal before comparing the old\n  -- immutable snapshot with the new native head. No cash/paid inference.\n  v_accepted_removed:=v_context->'accepted_source_removed'='true'::jsonb;\n  v_original:=private.weekly_source_missing_snapshot_movement_v1(v_snapshot.id);"},
 {before:"  if v_original->>'movement_id' is distinct from v_context->>'current_pay_head_movement_id' then",after:"  if not coalesce(v_accepted_removed,false)\n    and v_original->>'movement_id' is distinct from v_context->>'current_pay_head_movement_id' then"},
 {before:"  v_disposition:=case\n    when v_context->'invoiced'",after:"  v_disposition:=case\n    when v_accepted_removed then 'ACCEPTED_SOURCE_REMOVED'\n    when v_context->'invoiced'"},
 {before:"  v_result:=v_result||jsonb_build_object('basis_sha256'",after:"  if v_accepted_removed then\n    v_result:=v_result||jsonb_build_object('accepted_removals',v_context->'accepted_source_removal_proofs');\n  end if;\n  v_result:=v_result||jsonb_build_object('basis_sha256'"},
 {before:"'exclude_cancel',v_disposition<>'CANCEL_NOT_PAID_OR_INVOICED'",after:"'exclude_cancel',v_disposition in ('KEEP_INVOICED','KEEP_PAID_NOT_INVOICED')"},
],{commit:missingCommit,virtualParent:missing+'/forward/03_missing_cancel_basis.sqlinc'});
const preparedSignature='private.weekly_source_missing_prepared_census_recheck_v1(uuid,uuid)',preparedBefore=complete(selected(missing,'forward/14_missing_prepared_census_recheck.sqlinc'),preparedSignature);
amended(preparedSignature,'12_complete_prepared_census.sqlinc',preparedBefore,oldEdits.proofs.find(p=>p.file==='07_complete_missing_prepared_census.sqlinc').hunks,{commit:missingCommit,virtualParent:missing+'/forward/14_missing_prepared_census_recheck.sqlinc'});
// New Source protocol counters are canonical decimal strings. Existing native
// canonical-proposal JSON is not rewritten: its genuine native hash is retained.
leaves['03_inventory_inputs.sqlinc']=splice(leaves['03_inventory_inputs.sqlinc'],"'family_booking_id',v_root.booking_id,'root_version',v_root.version,","'family_booking_id',v_root.booking_id,'root_version',v_root.version::text,");
leaves['03_inventory_inputs.sqlinc']=splice(leaves['03_inventory_inputs.sqlinc'],"where publication.id=p_projection_publication_id and publication.state='CURRENT'\n      and v_cycle.current_projection_publication_id=publication.id",`where publication.id=p_projection_publication_id and publication.state in ('CURRENT','APPLIED')
      -- The exact target context above has already proved CURRENT or the
      -- genuine successful APPLIED correction session and its current export.
      and (publication.state='APPLIED' or v_cycle.current_projection_publication_id=publication.id)`);
leaves['05_prepare.sqlinc']=splice(leaves['05_prepare.sqlinc'],"  v_version:=(private.weekly_source_publication_scalar_v1(p_request->'expected_root_version','expected_root_version','INT')#>>'{}')::bigint;",`  if jsonb_typeof(p_request->'expected_root_version') is distinct from 'string'
    or not coalesce(p_request->>'expected_root_version'~'^[1-9][0-9]*$',false)
    or length(p_request->>'expected_root_version')>19
    or (length(p_request->>'expected_root_version')=19 and p_request->>'expected_root_version'>'9223372036854775807') then
    raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_PREPARE_INVALID' using errcode='22023';end if;
  v_version:=(p_request->>'expected_root_version')::bigint;`);
leaves['05_prepare.sqlinc']=leaves['05_prepare.sqlinc'].replaceAll("'bundle_revision',1,","'bundle_revision','1',");
assert.equal(leaves['05_prepare.sqlinc'].includes("'bundle_revision',1,"),false);
// Scope is freshly checked before both historical proposal replay reads. It
// returns the old proposal, without claiming a current financial position.
const replayAuthority="  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',\n    v_cycle.source_group_id,v_event.client_id,v_cycle.finalisation_week_ending);";
leaves['05_prepare.sqlinc']=splice(leaves['05_prepare.sqlinc'],replayAuthority,replayAuthority+"\n  perform private.weekly_source_kept_as_paid_read_admit_v1(v_event_id,v_actor);");
// A native proof can only carry the new positive canonical-string fields;
// validate before bigint casts even if a forged payload carries a huge value.
leaves['06_proof_guard.sqlinc']=splice(leaves['06_proof_guard.sqlinc'],"  v_canonical:=private.weekly_source_publication_request_canonical_v1",`  if jsonb_typeof(v_proof->'bundle_revision') is distinct from 'string'
    or v_proof->>'bundle_revision' is distinct from '1'
    or jsonb_typeof(v_proof#>'{inputs,root_version}') is distinct from 'string'
    or not coalesce(v_proof#>>'{inputs,root_version}'~'^[1-9][0-9]*$',false)
    or length(v_proof#>>'{inputs,root_version}')>19
    or (length(v_proof#>>'{inputs,root_version}')=19 and v_proof#>>'{inputs,root_version}'>'9223372036854775807') then
    raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSAL_UNBOUND' using errcode='55000';end if;
  v_canonical:=private.weekly_source_publication_request_canonical_v1`);
const triggerAt=leaves['06_proof_guard.sqlinc'].indexOf('create or replace trigger ');assert(triggerAt>0);
const guardDDL=leaves['06_proof_guard.sqlinc'].slice(triggerAt);leaves['06_proof_guard.sqlinc']=leaves['06_proof_guard.sqlinc'].slice(0,triggerAt);
write('forward/90_one_time_guard.sqlinc',"lock table public.audit_events in share row exclusive mode;\n"+selected(old,'one-time/00_missing_accept_reserved_namespace.sqlinc')+'\n'+leaves['06_proof_guard.sqlinc']+guardDDL);
let runtime=selected(old,'forward/broker/src/weekly-source/missing-accept.mjs');
runtime=splice(runtime,"const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;","const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;\nexport const positiveDecimal=value=>typeof value==='string'&&/^[1-9][0-9]*$/.test(value)\n  &&(value.length<19||(value.length===19&&value<='9223372036854775807'));");
runtime=splice(runtime,'!Number.isSafeInteger(payload.expected_root_version)||payload.expected_root_version<1||payload.expected_root_version>2147483647','!positiveDecimal(payload.expected_root_version)');
write('forward/broker/src/weekly-source/missing-accept.mjs',runtime);
// Exact route contribution only; do not replace a34's current whole route.
const route='broker/src/weekly-source/routes.js',routeCommit='a34cd60f2c75580a9e37aabfaf26f7dab680cb37',routeInput=prefix+'kept-pay-rekey-current-root-successor-20261016/payload/'+route,routeBefore=blob(routeCommit,routeInput);assert.equal(sha(readFromRoot(route)),sha(routeBefore),'EXACT_MOUNTED_A34_ROUTE_REQUIRED');
function readFromRoot(p){return fs.readFileSync(path.join(root,p),'utf8');}
const routeHunks=[...oldEdits.proofs.find(p=>p.file===route).hunks,
 {before:'const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;',after:'const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;',purpose:'canonicalUuid already lowercases; accept the genuine full PostgreSQL UUID domain at the actual actor/route boundary.'},
 {before:"  'KEEP_AS_PAID',",after:"  'KEEP_AS_PAID', 'MISSING_ACCEPT_CONTEXT', 'PREPARE_MISSING_ACCEPT', 'LATER_CHANGE_DECISION',",purpose:'The actual payment-authoriser route shares the native protected-pay purpose.'},
 {before:"import {missingAcceptPrepareRequest,missingAcceptContextRequest} from './missing-accept.mjs';",after:"import {missingAcceptPrepareRequest,missingAcceptContextRequest,positiveDecimal} from './missing-accept.mjs';",purpose:'Lossless new bigint revision with bounded historical numeric compatibility.'},
 {before:'|| !Number.isSafeInteger(payload.bundle_revision) || payload.bundle_revision < 1',after:"|| !(positiveDecimal(payload.bundle_revision)\n          || (Number.isSafeInteger(payload.bundle_revision) && payload.bundle_revision >= 1))",purpose:'Admit new canonical string without converting it to Number; keep the existing native request domain.'},
],routeAfter=apply(routeBefore,routeHunks);assert.equal(reverse(routeAfter,routeHunks),routeBefore);
write('current/'+route,routeBefore);write('forward/'+route,routeAfter);write('ROUTE_EDITS.json',JSON.stringify({checkpoint,dependencyCommit:routeCommit,inputPath:routeInput,target:route,inputSha256:sha(routeBefore),outputSha256:sha(routeAfter),hunks:routeHunks,completeReverseByteEquality:true},null,2)+'\n');
// One monotonic repeatable installs the COMPLETE Missing contribution after
// Entry0135, with Later's three explicit shared-owner successors selected once.
// The older0126 repeatable/classifier must never run after these helpers exist.
const ownSignatures=new Set(Object.values(leaves).flatMap(source=>routineDefinitions(source,'later'))),missingComposition=[],missingOwnerEdits=JSON.parse(selected(missing,'CURRENT_OWNER_EDITS.json')),currentRegistry=JSON.parse(blob(checkpoint,'docs/bpay-next/final-definer-closures.json'));let missingOrdinal=0;
for(const leaf of fs.readdirSync(path.join(repo,prefix+missing+'/forward')).filter(n=>n.endsWith('.sqlinc')&&!/^(19_|20_|99_)/.test(n)).sort()){
 const source=selected(missing,'forward/'+leaf);
 for(const signature of routineDefinitions(source,leaf)){
  if(ownSignatures.has(signature)){assert(owners.some(o=>o.signature===signature&&o.inputSha256===sha(complete(source,signature))),'ONLY_EXPLICIT_LATER_OVERLAPS_ALLOWED '+signature);missingComposition.push({signature,inputLeaf:leaf,inputSha256:sha(complete(source,signature)),selected:'EXPLICIT_LATER_SUCCESSOR'});continue;}
  const target='13_missing_'+String(++missingOrdinal).padStart(2,'0')+'_'+signature.split('(')[0].replaceAll('.','_')+'.sqlinc',definition=complete(source,signature),oldEdit=missingOwnerEdits.owners.find(o=>o.signature===signature);
  if(oldEdit){const entry=currentRegistry.chains.find(c=>c.signature===signature);assert(entry,'EXACT_CURRENT_MISSING_PARENT '+signature);const before=current(signature,entry.finalPhysical),after=amended(signature,target,before,oldEdit.hunks,{commit:checkpoint,physical:entry.finalPhysical,contributionCommit:missingCommit,contributionInputSha256:oldEdit.completeInputSha256,physicalReview:'14c4fb1f'});missingComposition.push({signature,inputLeaf:leaf,originalFrozenOutputSha256:sha(definition),currentPhysical:entry.finalPhysical,currentInputSha256:sha(before),selected:target,outputSha256:sha(after),completeCurrentReverseByteEquality:true});}
  else{leaves[target]=definition+'\n';missingComposition.push({signature,inputLeaf:leaf,inputSha256:sha(definition),selected:target,completeByteEquality:true});}
  ownSignatures.add(signature);
 }
}
write('MISSING_PARENT_COMPOSITION.json',JSON.stringify({commit:missingCommit,completeMissingDefinitions:missingComposition.length,functions:missingComposition,excludedRepeatable:'supabase/repeatable/17102026_0126_weekly_source_missing_health_authority_v1.sql',reason:'Single fresh combined root follows Entry0135; no later stale classifier can remove newly defined helper identities.',native:'NOT_RUN'},null,2)+'\n');
for(const[leaf,source]of Object.entries(leaves))write('forward/'+leaf,source);
const declarations=Object.entries(leaves).flatMap(([leaf,source])=>routineDefinitions(source,leaf).map(signature=>({signature,leaf})));assert.equal(new Set(declarations.map(d=>d.signature)).size,declarations.length);assert(declarations.every(d=>!d.signature.startsWith('private.bpay_')));
write('forward/99_acl.sqlinc',declarations.map(d=>`alter function ${d.signature} owner to postgres;\nrevoke all on function ${d.signature} from public,anon,authenticated,service_role;\n${d.signature.startsWith('public.')?`grant execute on function ${d.signature} to service_role;\n`:''}`).join('')+"notify pgrst,'reload schema';\n");
write('CURRENT_OWNER_EDITS.json',JSON.stringify({checkpoint,missingCommit,owners,completeCurrentOwners:owners.length,ACKWriterAuditCounterUnchanged:true,native:'NOT_RUN'},null,2)+'\n');
write('INPUT_MANIFEST.json',JSON.stringify({checkpoint,missingCommit,files:inputs,native:'NOT_RUN'},null,2)+'\n');
write('OWNER_DELTA.json',JSON.stringify({checkpoint,virtualParent:missingCommit,functions:declarations,bankOwnedBodyChanges:0,newTables:0,newColumns:0,installable:false,native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({owners:owners.length,functions:declarations.length,ACKPreserved:true,bankBodies:0,installable:false}));
