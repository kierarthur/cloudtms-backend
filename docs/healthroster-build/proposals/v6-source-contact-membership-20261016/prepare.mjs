import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
const root=path.dirname(fileURLToPath(import.meta.url));
const savedNative='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const inputs=[['supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql','admission.sql'],
 ['supabase/repeatable/16102026_0130_weekly_contact_submission_consume_v1.sql','result-hook.sql'],
 ['supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql','native-targets.sql'],
 ['supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql','native-query.sql'],
 ['supabase/repeatable/includes/weekly_contact_source_native_start_v1.sqlinc','native-start.sqlinc'],
 ['supabase/repeatable/includes/weekly_contact_timing/040_native_timing.sqlinc','native-timing.sqlinc']];
fs.mkdirSync(path.join(root,'reference'),{recursive:true});
const commit=execFileSync('git',['rev-parse','HEAD'],{cwd:savedNative,encoding:'utf8'}).trim();
const records=[];
for(const [file,reference] of inputs){const bytes=fs.readFileSync(path.join(savedNative,file));fs.writeFileSync(path.join(root,'reference',reference),bytes.toString('utf8').replaceAll('\r\n','\n'));
 records.push({file,reference:'reference/'+reference,sha256:crypto.createHash('sha256').update(bytes).digest('hex'),commit});}
fs.writeFileSync(path.join(root,'SOURCE_INPUTS.json'),JSON.stringify({root:savedNative,inputs:records,database_execution:false},null,2)+'\n');
const patches=[
 {target:inputs[0][0],source:'reference/admission.sql',description:'Under already-held common sorted scopes, before native attempt allocation; same original command siblings stay eligible',
 before:" v_admission:=jsonb_build_object('scope_revisions',v_scopes,'content_hash',v_after->>'content_hash',\n",after:" if not (private.weekly_contact_source_budget_v1(v_command.id)->>'allowed')::boolean then\n  return jsonb_build_object('ok',false,'version','CONTACT_START_RESULT_V1','owner_kind','WEEKLY_SOURCE_TARGET',\n   'channel',v_target.channel,'reason','OBSOLETE','dispatch_target_id',v_target.id,'next_eligible_at_utc',null);end if;\n v_admission:=jsonb_build_object('scope_revisions',v_scopes,'content_hash',v_after->>'content_hash',\n"},
 {target:inputs[0][0],source:'reference/admission.sql',description:'Only the genuine native START permit allocates frozen command members',
 before:' returning id into v_permit;\n if v_target.channel=\'PUSH\' then',after:' returning id into v_permit;\n perform private.weekly_contact_source_memberships_v1(v_permit,true);\n if v_target.channel=\'PUSH\' then'},
 {target:inputs[1][0],source:'reference/result-hook.sql',description:'Serialize actual command before permit, then sample acceptance after every native row-lock wait',
 before:' select * into strict v_target from public.weekly_message_dispatch_targets where id=v_attempt.dispatch_target_id;\n select * into v_permit from public.weekly_contact_submission_permits where provider_attempt_id=p_attempt_id for update;\n v_no_network:=',
 after:' select * into strict v_target from public.weekly_message_dispatch_targets where id=v_attempt.dispatch_target_id;\n perform 1 from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id for update;\n select * into v_permit from public.weekly_contact_submission_permits where provider_attempt_id=p_attempt_id for update;\n v_at:=clock_timestamp();\n v_no_network:='},
 {target:inputs[1][0],source:'reference/result-hook.sql',description:'First true accepted native device result records logical acceptance once; no target/aggregate/sibling writes',
 before:"  accepted_result_json=case when p_outcome='ACCEPTED' then v_result end where id=v_permit.id;\nend;",
 after:"  accepted_result_json=case when p_outcome='ACCEPTED' then v_result end where id=v_permit.id;\n if p_outcome='ACCEPTED' then perform private.weekly_contact_source_membership_accept_v1(p_attempt_id);end if;\nend;"}];
fs.writeFileSync(path.join(root,'NATIVE_PATCH.json'),JSON.stringify({version:'SOURCE_LOGICAL_MEMBERSHIP_COMPOSITION_V1',patches},null,2)+'\n');
console.log(JSON.stringify({saved_current_owners:records.length,finite_patches:patches.length,commit,database_execution:false}));
