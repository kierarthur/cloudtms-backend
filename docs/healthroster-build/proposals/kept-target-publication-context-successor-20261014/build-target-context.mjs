import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';

// Source construction only. The financial grain remains one existing Source
// body, and its unavailable native paid facts remain unavailable.
const dir=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(dir,'../../../..');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const inputCommit='cda43b0160999f95482e27a70b86c0bc775895c0';
const current='8685b8cf855f46b5b35100d338e294459d87317c';
const file='docs/healthroster-build/proposals/kept-pay-identical-rekey-current-root-forward-20261014/forward/06_kept_as_paid_context.sqlinc';
const input=execFileSync('git',['--no-replace-objects','show',inputCommit+':'+file],{cwd:root,maxBuffer:2000000});
const sha=b=>createHash('sha256').update(b).digest('hex');
const old=input.toString('utf8');
const oldId='private.weekly_source_kept_as_paid_context_v1(uuid,uuid)';
assert.equal(functionEvents(old).events.filter(e=>e.kind==='CREATE'&&e.id===oldId).length,1);
const newline=old.includes('\r\n')?'\r\n':'\n';
const normalize=s=>s.replaceAll('\r\n','\n').replaceAll('\n',newline);
const hunks=[];
const replace=(s,name,before,after)=>{
  before=normalize(before);after=normalize(after);
  assert.equal(s.split(before).length,2,name+' before must occur once');
  const out=s.replace(before,after);
  assert.equal(out.split(after).length,2,name+' after must occur once');
  hunks.push({name,before,after});return out;
};
const newName='private.weekly_source_kept_as_paid_context_for_publication_v1';
let kernel=replace(old,'purpose-bound signature',
 'private.weekly_source_kept_as_paid_context_v1(p_work_event_id uuid,p_actor_user_id uuid)',
 newName+'(p_work_event_id uuid,p_actor_user_id uuid,p_projection_publication_id uuid)');
kernel=replace(kernel,'exact publication and real target authority',
 `  where cycle.source_group_id=v_event.first_source_group_id
    and (cycle.scope_client_id is null or cycle.scope_client_id=v_event.client_id)
    and publication.state='CURRENT' and upload.state='CURRENT' and upload.purpose='ORDINARY'
    and publication.authority_scope_kind='CYCLE' and upload.report_scope_id is null`,
 `  where publication.id=p_projection_publication_id
    and publication.source_cycle_id=cycle.id and publication.authority_scope_kind='CYCLE'
    and publication.report_scope_id is null and upload.report_scope_id is null
    and cycle.source_group_id=v_event.first_source_group_id
    and (cycle.scope_client_id is null or cycle.scope_client_id=v_event.client_id)
    and (
      (publication.state='CURRENT' and upload.state='CURRENT' and upload.purpose='ORDINARY'
        and upload.correction_session_id is null and publication.correction_session_id is null
        and cycle.current_projection_publication_id=publication.id)
      or
      (publication.state='CORRECTION_READY' and upload.state='CORRECTION_READY'
        and upload.purpose='FINAL_SOURCE_CORRECTION' and exists(
          select 1 from public.weekly_final_source_correction_sessions correction
          join public.weekly_source_final_revisions prior on prior.id=correction.expected_current_final_revision_id
          where correction.id=upload.correction_session_id and publication.correction_session_id=correction.id
            and correction.source_cycle_id=cycle.id and correction.authority_scope_kind='CYCLE'
            and correction.report_scope_id is null
            and correction.replacement_correction_upload_id=upload.id
            and correction.replacement_projection_publication_id=publication.id
            and correction.state in ('REVIEWED','PREPARING','PREPARED','COMMITTING')
            and (p_actor_user_id is null or correction.actor_user_id=p_actor_user_id)
            and prior.source_cycle_id=cycle.id and prior.authority_scope_kind='CYCLE'
            and prior.report_scope_id is null and prior.state='CURRENT'
            and prior.manifest_hash=correction.expected_final_manifest_hash
            and cycle.current_final_revision_id=prior.id
            and correction.review_result_hash=private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_RESULT_V1',correction.review_result_json)))
    )`);
kernel=replace(kernel,'kernel owner ACL',
 `alter function private.weekly_source_kept_as_paid_context_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_context_v1(uuid,uuid) from public,anon,authenticated,service_role;`,
 `alter function private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;`);
kernel=replace(kernel,'target facts uses the same explicit publication',
 'private.weekly_source_kept_as_paid_facts_v1(v_event.id,v_reason)',
 'private.weekly_source_kept_as_paid_facts_for_publication_v1(v_event.id,v_reason,v_publication)');

// Preserve the original latest-current selection in the old ABI wrapper.
// All existence, Client/coverage/profile/target and financial facts are then
// rederived by the one kernel. The exact supplied target is never reselected.
const selectionStart=old.indexOf('  select publication.id,upload.id,cycle.id,cycle.finalisation_week_ending into v_publication,v_upload,v_cycle,v_week');
const selectionEnd=old.indexOf("  if v_publication is null then",selectionStart);
assert.ok(selectionStart>0&&selectionEnd>selectionStart);
const originalSelection=old.slice(selectionStart,selectionEnd);
const wrapperSelection=originalSelection.replace('select publication.id,upload.id,cycle.id,cycle.finalisation_week_ending into v_publication,v_upload,v_cycle,v_week','select publication.id into v_publication');
const originalAdmissionStart=old.indexOf('  select * into strict v_event from public.weekly_work_events where id=p_work_event_id;');
assert.ok(originalAdmissionStart>0&&originalAdmissionStart<selectionStart);
const originalAdmission=old.slice(originalAdmissionStart,selectionStart);
const wrapper=normalize(`-- Retained two-argument ABI: latest real CURRENT export only.
create or replace function private.weekly_source_kept_as_paid_context_v1(p_work_event_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_event public.weekly_work_events%rowtype; v_resolution public.weekly_source_row_resolutions%rowtype; v_publication uuid;
begin
`)+originalAdmission+wrapperSelection+normalize(`  if v_publication is null then raise exception 'WEEKLY_SOURCE_KEEP_AS_PAID_EXPORT_UNAVAILABLE' using errcode='55000'; end if;
  return private.weekly_source_kept_as_paid_context_for_publication_v1(v_event.id,p_actor_user_id,v_publication);
end;
$function$;
alter function private.weekly_source_kept_as_paid_context_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_context_v1(uuid,uuid) from public,anon,authenticated,service_role;
`);
const contextForward=replace(kernel,'retain original ABI wrapper after kernel',
 'revoke all on function private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;',
 'revoke all on function private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;'+newline+wrapper);
let reversed=contextForward;
for(const h of [...hunks].reverse()){
  assert.equal(reversed.split(h.after).length,2,h.name+' reverse');reversed=reversed.replace(h.after,h.before);
}
assert.equal(reversed,old);
const financialStart='  v_source_present:=exists(';const financialEnd='end;'+newline+'$function$;';
const financialBody=s=>s.slice(s.indexOf(financialStart),s.indexOf(financialEnd));
assert.equal(financialBody(kernel).replace('private.weekly_source_kept_as_paid_facts_for_publication_v1(v_event.id,v_reason,v_publication)',
 'private.weekly_source_kept_as_paid_facts_v1(v_event.id,v_reason)'),financialBody(old),
 'All raw membership, root/invoice/payment/credit/latestACK/unknown calculations remain in one body; facts binds only the exact target publication');
fs.mkdirSync(path.join(dir,'forward'),{recursive:true});
fs.writeFileSync(path.join(dir,'before/source_keep32_context.sqlinc'),input);
fs.writeFileSync(path.join(dir,'forward/01_target_publication_context.sqlinc'),contextForward);
const schemaFile='supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql';
const schema=execFileSync('git',['--no-replace-objects','show',current+':'+schemaFile],{cwd:root,maxBuffer:2000000});
const selected={source_only:true,unfinished_construction:true,current_root:current,input_commit:inputCommit,input_file:file,input_bytes:input.length,input_sha256:sha(input),forward_file:'forward/01_target_publication_context.sqlinc',forward_bytes:Buffer.byteLength(contextForward),forward_sha256:sha(contextForward),full_file_reverse_verified:true,unique_hunks:hunks.length,hunks,financial_body_bytes:Buffer.byteLength(financialBody(old)),financial_body_sha256:sha(financialBody(old)),financial_grain_preserved_except_literal_target_facts_call:true,wrapper_latest_current_selection_and_admission_unchanged:true,unchanged_wrapper_signature:oldId,new_owner_private_signature:'private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid)',exact_session_schema:{commit:current,file:schemaFile,bytes:schema.length,sha256:sha(schema)},shared13_approved:false,canonical_source_changed:false,sql_compiled:false,native_proof:false,default_accept_prepared_consumer_wiring_complete:false};
fs.writeFileSync(path.join(dir,'TARGET_CONTEXT_COMPLETE_REVERSE.json'),JSON.stringify(selected,null,2)+'\n');
// The facts helper must not silently reselect CURRENT while the context reads
// a real correction target. Preserve the primary V1 hash domain and tuple:
// target raw absence, genuine current pay head, exact sorted credit IDs.
const factsFile=file.replace('06_kept_as_paid_context','05_kept_as_paid_facts');
const factsInput=execFileSync('git',['--no-replace-objects','show',inputCommit+':'+factsFile],{cwd:root,maxBuffer:2000000});
const factsOld=factsInput.toString('utf8');const factsHunks=[];
const factsReplace=(s,name,before,after)=>{
 before=normalize(before);after=normalize(after);assert.equal(s.split(before).length,2,name);
 const out=s.replace(before,after);assert.equal(out.split(after).length,2,name+' forward');factsHunks.push({name,before,after});return out;
};
let factsKernel=factsReplace(factsOld,'target facts signature',
 'private.weekly_source_kept_as_paid_facts_v1(p_work_event_id uuid,p_reason text)',
 'private.weekly_source_kept_as_paid_facts_for_publication_v1(p_work_event_id uuid,p_reason text,p_projection_publication_id uuid)');
const scopeHunk=hunks.find(h=>h.name==='exact publication and real target authority');
const factsBefore=scopeHunk.before;
const factsAfter=scopeHunk.after.replace('            and (p_actor_user_id is null or correction.actor_user_id=p_actor_user_id)'+newline,'');
factsKernel=factsReplace(factsKernel,'same literal target/session scope',factsBefore,factsAfter);
factsKernel=factsReplace(factsKernel,'target facts owner ACL',
 `alter function private.weekly_source_kept_as_paid_facts_v1(uuid,text) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_facts_v1(uuid,text) from public,anon,authenticated,service_role;`,
 `alter function private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid) from public,anon,authenticated,service_role;`);
const factsSelectionStart=factsOld.indexOf('  select publication.* into v_publication');
const factsSelectionEnd=factsOld.indexOf('  if not found then',factsSelectionStart);
assert.ok(factsSelectionStart>0&&factsSelectionEnd>factsSelectionStart);
const factsSelection=factsOld.slice(factsSelectionStart,factsSelectionEnd).replace('select publication.* into v_publication','select publication.id into v_publication');
const factsWrapper=normalize(`-- Retained two-argument ABI and primary V1 facts domain.
create or replace function private.weekly_source_kept_as_paid_facts_v1(p_work_event_id uuid,p_reason text)
returns bytea language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_event public.weekly_work_events%rowtype; v_publication uuid;
begin
  if p_work_event_id is null or p_reason is null or p_reason not in ('CREDITED','NOT_INVOICED_DISAPPEARED') then
    raise exception 'WEEKLY_SOURCE_KEEP_AS_PAID_REASON_INVALID' using errcode='22023'; end if;
  select * into strict v_event from public.weekly_work_events where id=p_work_event_id;
`)+factsSelection+normalize(`  if not found then raise exception 'WEEKLY_SOURCE_KEEP_AS_PAID_EXPORT_UNAVAILABLE' using errcode='55000'; end if;
  return private.weekly_source_kept_as_paid_facts_for_publication_v1(v_event.id,p_reason,v_publication);
end;
$function$;
alter function private.weekly_source_kept_as_paid_facts_v1(uuid,text) owner to postgres;
revoke all on function private.weekly_source_kept_as_paid_facts_v1(uuid,text) from public,anon,authenticated,service_role;
`);
const factsBodyStart='  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;';
const factsBody=s=>s.slice(s.indexOf(factsBodyStart),s.indexOf(financialEnd));
assert.equal(factsBody(factsKernel),factsBody(factsOld),'Original financial/credit/facts hash is unchanged');
fs.writeFileSync(path.join(dir,'before/source_keep32_facts.sqlinc'),factsInput);
const factsForward=factsReplace(factsKernel,'retain original facts ABI wrapper after kernel',
 'revoke all on function private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid) from public,anon,authenticated,service_role;',
 'revoke all on function private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid) from public,anon,authenticated,service_role;'+newline+factsWrapper);
let factsReversed=factsForward;for(const h of [...factsHunks].reverse()){
 assert.equal(factsReversed.split(h.after).length,2,h.name+' reverse');factsReversed=factsReversed.replace(h.after,h.before);
}assert.equal(factsReversed,factsOld);
fs.writeFileSync(path.join(dir,'forward/00_target_publication_facts.sqlinc'),factsForward);
fs.writeFileSync(path.join(dir,'TARGET_FACTS_COMPLETE_REVERSE.json'),JSON.stringify({source_only:true,unfinished_construction:true,current_root:current,input_commit:inputCommit,input_file:factsFile,input_bytes:factsInput.length,input_sha256:sha(factsInput),forward_file:'forward/00_target_publication_facts.sqlinc',forward_bytes:Buffer.byteLength(factsForward),forward_sha256:sha(factsForward),full_file_reverse_verified:true,unique_hunks:factsHunks.length,hunks:factsHunks,financial_and_hash_body_sha256:sha(factsBody(factsOld)),financial_and_hash_body_unchanged:true,unchanged_wrapper_signature:'private.weekly_source_kept_as_paid_facts_v1(uuid,text)',new_owner_private_signature:'private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid)',facts_domain_unchanged:'WEEKLY_SOURCE_KEPT_AS_PAID_FACTS_V1',sql_compiled:false,native_proof:false,consumers_complete:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,complete_source_functions:4,full_kernel_reverses:2,unique_hunks:hunks.length+factsHunks.length,financial_and_hash_bodies_preserved:true,new_owner_private_signatures:[selected.new_owner_private_signature,'private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid)'],sql_compiled:false,consumers_complete:false}));
