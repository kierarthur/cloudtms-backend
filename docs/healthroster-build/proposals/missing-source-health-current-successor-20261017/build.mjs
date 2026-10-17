// Health-owned source construction only. No Bank bodies, I2 writes or DB calls.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import{fileURLToPath,pathToFileURL}from'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026'),checkpoint='5ecc4c73113660edf48a5bca2f3a478453a81c2b';
const draft=path.join(here,'../missing-source-retained-basis-current-native-20261014'),kept=path.join(here,'../kept-pay-rekey-current-root-successor-20261016');
export const sha=x=>createHash('sha256').update(x).digest('hex'),blob=p=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',checkpoint+':'+p],{cwd:repo,windowsHide:true,maxBuffer:64_000_000}).toString();
export const splice=(s,b,a)=>{assert.equal(s.split(b).length,2,'ONE_EXACT_CURRENT_ANCHOR_REQUIRED');return s.replace(b,()=>a);};
export const apply=(s,hs)=>hs.reduce((s,h)=>splice(s,h.before,h.after),s),reverse=(s,hs)=>apply(s,[...hs].reverse().map(h=>({before:h.after,after:h.before})));
const read=(dir,p)=>fs.readFileSync(path.join(dir,p),'utf8'),json=(dir,p)=>JSON.parse(read(dir,p));
const keptCommit='a34cd60f2c75580a9e37aabfaf26f7dab680cb37',keptPrefix='docs/healthroster-build/proposals/kept-pay-rekey-current-root-successor-20261016/';
const keptSource=p=>{const exact=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',keptCommit+':'+keptPrefix+p],{cwd:repo,windowsHide:true,maxBuffer:64_000_000}).toString();assert.equal(read(kept,p),exact,'COMPLETE_A34_KEPT_DEPENDENCY_DRIFT');return exact;};
const write=(p,s)=>{fs.mkdirSync(path.dirname(path.join(here,p)),{recursive:true});fs.writeFileSync(path.join(here,p),s);};
const inputNames=['00_target_publication_facts.sqlinc','01_target_publication_context.sqlinc','02_missing_snapshot_movement.sqlinc','03_missing_cancel_basis.sqlinc','04_cancel_exclusion_schema.sqlinc','05_missing_finalisation_root_inventory.sqlinc','07_missing_cancel_transition_assert.sqlinc','08_missing_cancel_native_insert_guard.sqlinc','10_missing_current_selection.sqlinc','14_missing_prepared_census_recheck.sqlinc'];
const inputs=path.join(here,'inputs');
if(!fs.existsSync(path.join(here,'INPUT_MANIFEST.json'))){
 const pins=[];for(const name of inputNames){const original=read(draft,'forward/'+name),source=original.replace(/\n+$/,'\n');write('inputs/'+name,source);pins.push({path:'inputs/'+name,sha256:sha(source),bytes:Buffer.byteLength(source),originalSourceSha256:sha(original),normalization:'Trailing empty EOF lines only; routine body bytes unchanged.'});}
 const finite=[['FIN4_HUNKS.json',json(draft,'FIN4_DEFAULT_DRAFT_COMPLETE_REVERSE.json').hunks],['COUPLED_HEALTH_HUNKS.json',json(draft,'CURRENT_COUPLED_OWNER_COMPLETE_REVERSES.json').proofs.filter(p=>p.owner.startsWith('private.weekly_source_')||p.owner.startsWith('public.weekly_source_')).map(p=>({owner:p.owner,forward_file:p.forward_file,hunks:p.hunks}))]];
 for(const[name,value]of finite){const source=JSON.stringify(value,null,2)+'\n';write('inputs/'+name,source);pins.push({path:'inputs/'+name,sha256:sha(source),bytes:Buffer.byteLength(source)});}
 write('INPUT_MANIFEST.json',JSON.stringify({source:'Preserved Health-only untracked draft; selected full source and literal hunks retained here as published build inputs. Shared09/12 excluded.',files:pins,native:'NOT_RUN'},null,2)+'\n');
}
for(const pin of json(here,'INPUT_MANIFEST.json').files)assert.equal(sha(read(here,pin.path)),pin.sha256,'EXACT_HEALTH_DRAFT_INPUT_REQUIRED');
const{scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'))),{routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const registry=JSON.parse(blob('docs/bpay-next/final-definer-closures.json')),owners=[],leaves={},changes=[];
function owner(signature,leaf,hunks){const chain=registry.chains.find(c=>c.signature===signature);assert(chain,signature);const whole=blob(chain.finalPhysical),parsed=scanManagedSql(whole),matches=parsed.statements.map(s=>parsed.source.slice(s.start,s.end).trim()).filter(s=>routineDefinitions(s,chain.finalPhysical).includes(signature));assert.equal(matches.length,1);const before=matches[0],after=apply(before,hunks);assert.equal(reverse(after,hunks),before);write('current/'+leaf,before+'\n');leaves[leaf]=after+'\n';owners.push({signature,finalRoot:chain.finalRoot,finalPhysical:chain.finalPhysical,inputPhysicalSha256:sha(whole),completeInputSha256:sha(before),completeOutputSha256:sha(after),current:'current/'+leaf,successor:'forward/'+leaf,hunks,completeReverseByteEquality:true});}
owner('private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)','06_complete_fin4.sqlinc',json(inputs,'FIN4_HUNKS.json'));
const coupled=json(inputs,'COUPLED_HEALTH_HUNKS.json');
assert.equal(coupled.length,5);
for(const old of coupled)owner(old.owner,path.basename(old.forward_file),old.hunks);
for(const name of inputNames.filter(p=>p!=='04_cancel_exclusion_schema.sqlinc'))leaves[name]=read(inputs,name);
function amend(leaf,before,after,purpose){leaves[leaf]=splice(leaves[leaf],before,after);changes.push({leaf,before,after,purpose});}

// The target-publication readers retain reviewed CURRENT/APPLIED and exact
// CORRECTION_READY authority; they now use the genuinely latest typed ACK.
const target=leaves['01_target_publication_context.sqlinc'],keptContext=keptSource('forward/06_kept_as_paid_context.sqlinc');
const start=target.indexOf('  if v_reason is not null then'),end=target.indexOf("  return jsonb_build_object('schema_version','WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_V1'");assert(start>0&&end>start);
const ackStart=keptContext.indexOf('  -- Latest actual event acknowledgement'),ackEnd=keptContext.indexOf("  return jsonb_build_object('schema_version','WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_V1'");
const latest=keptContext.slice(ackStart,ackEnd).replace('private.weekly_source_kept_as_paid_facts_v1(v_event.id,v_reason)','private.weekly_source_kept_as_paid_facts_for_publication_v1(v_event.id,v_reason,v_publication)');
amend('01_target_publication_context.sqlinc',target.slice(start,end),latest,'Latest genuine event ACK before reason/hash, on the exact reviewed target publication.');
// New integer/bigint revision/counter fields use canonical decimal strings.
amend('03_missing_cancel_basis.sqlinc',"'projection_generation',coalesce(v_publication.projection_generation,v_publication.authority_scope_version::integer),","'projection_generation',coalesce(v_publication.projection_generation,v_publication.authority_scope_version::integer)::text,",'Canonical decimal-string generation at the newly introduced immutable basis boundary.');
amend('07_missing_cancel_transition_assert.sqlinc',"  if v_publication.upload_id is distinct from v_revision.upload_id",`  if jsonb_typeof(v_basis->'projection_generation') is distinct from 'string'
    or v_basis->>'projection_generation' !~ '^[1-9][0-9]*$'
    or v_publication.upload_id is distinct from v_revision.upload_id`,'Native readback refuses a numeric/noncanonical introduced generation; the exact native value is checked as text.');

// Share the exact read-authority prefix without invoking money/paid evidence.
// The current read wrapper reuses the publication selector, including genuine
// APPLIED correction exports, before either existing saved-ACK replay branch.
const context=leaves['01_target_publication_context.sqlinc'],prefixStart=context.indexOf('  select * into strict v_event'),prefixEnd=context.indexOf('  v_source_present:='),prefix=context.slice(prefixStart,prefixEnd);
assert(prefixStart>0&&prefixEnd>prefixStart);
const wrapperAt=context.indexOf('create or replace function private.weekly_source_kept_as_paid_context_v1('),wrapper=context.slice(wrapperAt),wrapperPrefix=wrapper.slice(wrapper.indexOf('  select * into strict v_event'),wrapper.indexOf('  return private.weekly_source_kept_as_paid_context_for_publication_v1'));
leaves['04_current_read_admission.sqlinc']=`-- Scope admission is read-only; no financial evidence or saved ACK is read.
create or replace function private.weekly_source_kept_as_paid_read_for_publication_v1(p_work_event_id uuid,p_actor_user_id uuid,p_projection_publication_id uuid)
returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare
 v_event public.weekly_work_events%rowtype; v_resolution public.weekly_source_row_resolutions%rowtype;
 v_publication uuid; v_upload uuid; v_cycle uuid; v_week date;
begin
 perform private.weekly_source_query_require_service_v1();
 if private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) is distinct from true then
  raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501';end if;
${prefix}end;
$function$;
alter function private.weekly_source_kept_as_paid_read_for_publication_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_read_for_publication_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
create or replace function private.weekly_source_kept_as_paid_read_admit_v1(p_work_event_id uuid,p_actor_user_id uuid)
returns void language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_event public.weekly_work_events%rowtype; v_resolution public.weekly_source_row_resolutions%rowtype; v_publication uuid;
begin
 perform private.weekly_source_query_require_service_v1();
 if private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) is distinct from true then
  raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501';end if;
${wrapperPrefix} perform private.weekly_source_kept_as_paid_read_for_publication_v1(v_event.id,p_actor_user_id,v_publication);
end;
$function$;
alter function private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid) from public,anon,authenticated,service_role;
`;

// Every overlap is an explicit complete a34 -> composed successor edit. Never
// treat an older APPLIED target-reader source as the current ACK parent.
const complete=(source,signature)=>{const parsed=scanManagedSql(source),found=parsed.statements.map(s=>parsed.source.slice(s.start,s.end).trim()).filter(s=>routineDefinitions(s,signature).includes(signature));assert.equal(found.length,1);return found[0];};
const ackComposition=[];
for(const[signature,beforePath,leaf]of[
 ['private.weekly_source_kept_as_paid_facts_v1(uuid,text)','forward/05_kept_as_paid_facts.sqlinc','00_target_publication_facts.sqlinc'],
 ['private.weekly_source_kept_as_paid_context_v1(uuid,uuid)','forward/06_kept_as_paid_context.sqlinc','01_target_publication_context.sqlinc'],
 ['private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid)','forward/04_kept_read_admission.sqlinc','04_current_read_admission.sqlinc'],
]){const before=complete(keptSource(beforePath),signature),after=complete(leaves[leaf],signature),hunks=[{before,after,purpose:'Explicit a34 owner delegates to exact target-publication authority; all typed ACK logic remains in the complete target context and original writer/guard are unchanged.'}];assert.equal(reverse(apply(before,hunks),hunks),before);write('current/a34_'+path.basename(beforePath),before+'\n');ackComposition.push({signature,dependencyCommit:keptCommit,inputPath:keptPrefix+beforePath,inputSha256:sha(before),outputLeaf:leaf,outputSha256:sha(after),hunks,completeReverseByteEquality:true});}
write('ACK_COMPOSITION_EDITS.json',JSON.stringify({dependencyCommit:keptCommit,overlaps:ackComposition,writerAndAuditGuardUnchanged:true,native:'NOT_RUN'},null,2)+'\n');

const guardName='08_missing_cancel_native_insert_guard.sqlinc',guard=leaves[guardName],triggerAt=guard.indexOf('create or replace trigger ');assert(triggerAt>0);
const trigger=guard.slice(triggerAt);leaves[guardName]=guard.slice(0,triggerAt);
const schema=read(inputs,'04_cancel_exclusion_schema.sqlinc');
leaves['20_one_time_native_guard_dependency.sqlinc']=leaves[guardName]+trigger;
for(const[name,source]of Object.entries(leaves))write('forward/'+name,source);
write('forward/19_one_time_existing_footprint.sqlinc',schema);

// Exact private/service closure. Service grants on existing public owners are
// retained; every new helper stays owner-only, with fixed search_path.
const declarations=Object.entries(leaves).filter(([name])=>name!=='20_one_time_native_guard_dependency.sqlinc').flatMap(([leaf,source])=>routineDefinitions(source,leaf).map(signature=>({signature,leaf}))),unique=[...new Map(declarations.map(d=>[d.signature,d])).values()];
assert(!unique.some(f=>f.signature.startsWith('private.bpay_')));
leaves['99_exact_acl.sqlinc']=unique.map(f=>`alter function ${f.signature} owner to postgres;\nrevoke all on function ${f.signature} from public,anon,authenticated,service_role;\n${f.signature.startsWith('public.')?`grant execute on function ${f.signature} to service_role;\n`:''}`).join('')+"notify pgrst,'reload schema';\n";
write('forward/99_exact_acl.sqlinc',leaves['99_exact_acl.sqlinc']);
write('CURRENT_OWNER_EDITS.json',JSON.stringify({checkpoint,owners,changes,completeCurrentOwnerReversals:owners.length,keptScopePrefixSha256:sha(prefix),currentScopeWrapperPrefixSha256:sha(wrapperPrefix),allSharedBankBodiesExcluded:true,native:'NOT_RUN'},null,2)+'\n');
write('OWNER_DELTA.json',JSON.stringify({checkpoint,functions:unique.map(f=>({...f,security:'DEFINER',serviceExecute:f.signature.startsWith('public.')})),existingFields:[{table:'public.weekly_source_state_transitions',column:'exclude_cancel'},{table:'public.weekly_source_state_transitions',column:'cancel_exclusion_basis'},{table:'public.weekly_source_final_revisions',column:'missing_cancel_basis_census'}],newRelations:[],allSharedBankBodiesExcluded:true,installable:false,native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({currentHealthOwners:owners.length,functions:unique.length,completeReverse:true,sharedBankBodies:0,installable:false,native:'NOT_RUN'}));
