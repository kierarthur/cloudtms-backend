// Source-only native owner extraction. No hosted access or Office mutation.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
import {d10OfficePins,applyD10ReviewEdits} from './build-d10-review-packet.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
export const reminderNativeRoot='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const normal=source=>source.replaceAll('\r\n','\n'),hash=source=>crypto.createHash('sha256').update(source).digest('hex');
export function reminderOfficeEdits(){
  const helper=fs.readFileSync(path.join(here,'office-d10-candidate-reminder.jsinc'),'utf8');
  const before='    const label = asText(detail.label);\n    if (detail.kind === \'CHECK_ONLY_D10_REVIEW\') {';
  return [{path:d10OfficePins[0].path,before:'  function handleAction(detailValue) {',after:helper+'\n  function handleAction(detailValue) {'},
    {path:d10OfficePins[0].path,before,after:"    const label = asText(detail.label);\n    if (detail.kind === 'CHECK_ONLY_D10_CANDIDATE_REMINDER') return openCheckOnlyD10Reminder(detail);\n    if (detail.kind === 'CHECK_ONLY_D10_REVIEW') {"}];
}
export function applyReminderOfficeEdits(source,direction='forward'){
  const edits=direction==='forward'?reminderOfficeEdits():reminderOfficeEdits().reverse().map(e=>({...e,before:e.after,after:e.before}));
  for(const edit of edits){if(source.split(edit.before).length!==2)throw Error('Exact R254 Office seam changed.');source=source.replace(edit.before,edit.after);}return source;
}
export function definition(source,name){const parsed=scanManagedSql(source);const matches=parsed.statements.map(s=>parsed.source.slice(s.start,s.end))
  .filter(s=>new RegExp(`^create\\s+or\\s+replace\\s+function\\s+${name.replaceAll('.','\\.')}\\s*\\(`,'i').test(s));
  if(matches.length!==1)throw Error('Native owner declaration changed: '+name);return matches[0]+'\n';}
export function reminderVisibilityForward(current){
  const before="  if p_notification.deep_link_json->>'destination'='WEEKLY_SOURCE_REQUEST' then";
  if(current.split(before).length!==2)throw Error('Native inbox lifecycle seam changed.');
  const branch=`  if p_notification.event_type='CHECK_ONLY_TIMESHEET_REMINDER' then
    -- Purpose-bound in-app reminder: preserve common retention/Undo semantics
    -- above, then delegate the same pure native-action/submission observation.
    if pg_catalog.jsonb_typeof(p_notification.template_params->'scope') is distinct from 'object'
      or pg_catalog.jsonb_typeof(p_notification.template_params->'native_action') is distinct from 'object'
      or p_notification.candidate_id is null then return false; end if;
    if not exists(select 1 from public.candidate_app_global_membership_links link
      join public.candidate_app_accounts account on account.id=link.account_id
      join public.weekly_source_groups source_group on source_group.environment=account.environment and source_group.active
      where link.membership_id::text=p_notification.template_params->>'membership_id'
        and link.account_id=p_notification.account_id and link.candidate_id=p_notification.candidate_id
        and link.state='ACTIVE' and account.status='ACTIVE'
        and source_group.id::text=p_notification.template_params#>>'{scope,source_group_id}') then return false; end if;
    begin
      return exists(select 1 from private.weekly_source_check_only_candidate_reminder_subject_v1(
        p_notification.template_params->'scope',p_notification.template_params->>'action_id',p_notification.candidate_id) subject
        where subject->'current_native_action'='true'::jsonb and subject->'actual_submission_present'='false'::jsonb
          and subject->'native_action'=p_notification.template_params->'native_action');
    exception when invalid_text_representation or invalid_parameter_value then return false; end;
  end if;
`;
  return current.replace(before,branch+before);
}
export function reminderCandidateCopyForward(current){
  const before="  if (eventType === 'WEEKLY_SOURCE_REQUEST'";
  if(current.split(before).length!==2)throw Error('Candidate native copy seam changed.');
  return current.replace(before,`  if (eventType === 'CHECK_ONLY_TIMESHEET_REMINDER') {
    const scope = upper(parameters?.sheet_scope);
    const client = text(parameters?.client_name).replace(/[\\u0000-\\u001f\\u007f]+/g, ' ').replace(/\\s+/g, ' ').trim().slice(0, 120);
    const day = text(parameters?.work_date), week = text(parameters?.week_ending);
    const date = scope === 'DAILY' ? day : week;
    const label = scope === 'DAILY' ? 'Daily' : 'Weekly';
    const when = /^\\d{4}-\\d{2}-\\d{2}$/.test(date) ? (scope === 'DAILY' ? \` for \${date}\` : \` for the week ending \${date}\`) : '';
    return \`Please submit your \${label} Timesheet\${when}\${client ? \` at \${client}\` : ''} so your hours can be checked.\`;
  }
`+before);
}
export function reminderOwnerPacket(){
  const read=p=>normal(fs.readFileSync(path.join(reminderNativeRoot,p),'utf8'));
  const current=definition(read('supabase/repeatable/10102026_1623_candidate_notification_management_v1.sql'),'private.candidate_notification_visible_v1');
  const broker=read('broker/src/candidate-app-backend.js'),start=broker.indexOf('function candidateNotificationMessage('),end=broker.indexOf('\nfunction optionalUuid(',start);
  if(start<0||end<start)throw Error('Complete native Candidate copy boundary absent.');
  const copy=broker.slice(start,end).trimEnd()+'\n';
  const acl='alter function private.candidate_notification_visible_v1(public.candidate_notifications,timestamptz,text) owner to postgres;\nrevoke all on function private.candidate_notification_visible_v1(public.candidate_notifications,timestamptz,text) from public,anon,authenticated,service_role;\n';
  return {visibilityReverse:current+acl,visibilityForward:reminderVisibilityForward(current)+acl,copyReverse:copy,copyForward:reminderCandidateCopyForward(copy)};
}
export function reminderCommandOwnerPacket(){
  const source=normal(fs.readFileSync(path.join(repo,'broker/src/weekly-source/routes.js'),'utf8'));
  const previous=normal(execFileSync('git',['show','5804a7d0be822e2f133d86d934fca8a2897ff33f:broker/src/weekly-source/routes.js'],{cwd:repo,encoding:'utf8'}));
  const start=source.indexOf("  if (action === 'CHECK_ONLY_D10_REMIND_CANDIDATE') {"),end=source.indexOf("  if (action === 'LATER_CHANGE_DECISION') {",start);
  if(start<0||end<start)throw Error('Purpose command seam absent.');
  const hunk=source.slice(start,end),importHunk="import {checkOnlyReminderRequest,checkOnlyReminderReceipt,CheckOnlyReminderError} from './check-only-candidate-reminder.mjs';\n";
  if(source.replace(hunk,'').replace(importHunk,'')!==previous)throw Error('Unrelated current route owner changed.');
  const whole=value=>value.slice(value.indexOf('async function handleCommand('),value.indexOf('async function loadUploadBytes(')).trimEnd()+'\n';
  return {forward:whole(source),reverse:whole(previous),hunk,importHunk,predecessorSavedSha256:hash(previous)};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const censusPath='docs/healthroster-build/proposals/root-effective-body-amendment-census-case-forward-20261014/BODY_AMENDMENT_CENSUS.json';
  const censusBytes=fs.readFileSync(path.join(reminderNativeRoot,censusPath)),census=JSON.parse(censusBytes);
  if(census.rootCount!==1484||census.sourceInputCount!==1745||census.sourceOccurrences!==2160)throw Error('Canonical root census changed; reconcile first.');
  const names=['private._candidate_notification_insert_v1','private.candidate_notification_visible_v1'];
  const declarations=[],amendments=[],references=[],pins=[];
  for(const input of census.sourceInputs){
    const bytes=fs.readFileSync(path.join(reminderNativeRoot,input.file)),source=normal(bytes.toString('utf8'));
    if(input.sha256!==hash(bytes)&&input.sha256!==hash(source))throw Error('Canonical native source changed: '+input.file);
    if(!names.some(name=>source.includes(name)))continue;
    pins.push({path:input.file,saved_sha256:hash(bytes),canonical_sha256:hash(source)});
    const parsed=scanManagedSql(source);
    for(const row of parsed.statements){const statement=source.slice(row.start,row.end);
      for(const name of names)if(statement.includes(name)){
        const record={path:input.file,owner:name,statement_sha256:hash(statement),head:row.tokens[0]};
        if(new RegExp(`^create\\s+(?:or\\s+replace\\s+)?function\\s+${name.replaceAll('.','\\.')}\\s*\\(`,'i').test(statement))declarations.push(record);
        else if(row.tokens[0]==='DO')amendments.push(record);else references.push(record);
      }
    }
  }
  const expectedDeclarations={
    'private._candidate_notification_insert_v1':['supabase/baseline/22082026_1503_cloudtms_test_routines_14.sql','supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql'],
    'private.candidate_notification_visible_v1':['supabase/repeatable/10102026_1623_candidate_notification_management_v1.sql']};
  for(const name of names)if(JSON.stringify(declarations.filter(d=>d.owner===name).map(d=>d.path).sort())!==JSON.stringify(expectedDeclarations[name].sort()))
    throw Error('Native kernel declaration census changed: '+name);
  if(amendments.length)throw Error('Guarded native kernel amendment requires full review.');
  const packet=reminderOwnerPacket(),proposals=path.join(repo,'supabase/repeatable/healthroster_wp_u3_source_proposals');
  fs.writeFileSync(path.join(proposals,'wp_u3_candidate_notification_visible.forward.sqlinc'),packet.visibilityForward);
  fs.writeFileSync(path.join(proposals,'wp_u3_candidate_notification_visible.reverse.sqlinc'),packet.visibilityReverse);
  fs.writeFileSync(path.join(here,'candidate-notification-copy.forward.jsinc'),packet.copyForward);
  fs.writeFileSync(path.join(here,'candidate-notification-copy.reverse.jsinc'),packet.copyReverse);
  const officePin=d10OfficePins[0],officeBytes=fs.readFileSync(officePin.path);
  if(hash(officeBytes)!==officePin.saved_sha256)throw Error('Exact Office predecessor changed.');
  const d10=applyD10ReviewEdits(normal(officeBytes.toString('utf8')),officePin.path),officeForward=applyReminderOfficeEdits(d10);
  if(applyReminderOfficeEdits(officeForward,'reverse')!==d10)throw Error('R254 Office complete forward/reverse mismatch.');
  for(const direction of ['forward','reverse'])fs.writeFileSync(path.join(here,`office-d10-reminder.${direction}.json`),JSON.stringify({
    pins:[officePin],requires_source_closed_D10_predecessor:'9e55341f49902a3f212aac888739424ca4241bce',
    edits:direction==='forward'?reminderOfficeEdits():reminderOfficeEdits().reverse().map(e=>({...e,before:e.after,after:e.before}))},null,2)+'\n');
  fs.writeFileSync(path.join(here,'office-workspace-actions-reminder.forward.jsinc'),officeForward);
  fs.writeFileSync(path.join(here,'office-workspace-actions-reminder.reverse.jsinc'),d10);
  const command=reminderCommandOwnerPacket();
  fs.writeFileSync(path.join(here,'broker-r254-command.forward.jsinc'),command.forward);
  fs.writeFileSync(path.join(here,'broker-r254-command.reverse.jsinc'),command.reverse);
  fs.writeFileSync(path.join(here,'broker-r254-command.import.jsinc'),command.importHunk);
  const selected=['supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql',
    'supabase/repeatable/10102026_1623_candidate_notification_management_v1.sql','supabase/repeatable/10102026_1624_candidate_notification_consumers_v1.sql',
    'supabase/repeatable/21082026_1758_candidate_app_federated_session_project_v1.sql','supabase/repeatable/21082026_2335_candidate_app_federated_membership_link_set_v1.sql',
    'supabase/repeatable/13102026_1438_weekly_source_protection_queries/06_automatic_actor_settings.sqlinc',
    'supabase/repeatable/13102026_1205_healthroster_issued_signed_query_v1.sql',
    'supabase/migrations/07082026_2055_candidate_app_foundation_schema.sql','supabase/migrations/21082026_1757_candidate_federated_session_projection.sql',
    'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','broker/src/candidate-app-backend.js'];
  fs.writeFileSync(path.join(here,'R254_NATIVE_OWNER_PINS.json'),JSON.stringify({status:'SOURCE_ONLY_NATIVE_KERNEL_CENSUS_NOT_EXECUTION',
    sourceRoot:reminderNativeRoot,canonicalSQLCommit:'fa66183f01913e86651e82816f5e88f97e07123c',
    rootCensus:{path:censusPath,sha256:hash(censusBytes),roots:census.rootCount,uniqueInputs:census.sourceInputCount,occurrences:census.sourceOccurrences},
    declarations,effectiveOwners:Object.fromEntries(names.map(name=>[name,expectedDeclarations[name].find(p=>p.startsWith('supabase/repeatable/'))])),
    guardedAmendments:amendments,references,pins,selectedNativeInputs:selected.map(p=>({path:p,saved_sha256:hash(fs.readFileSync(path.join(reminderNativeRoot,p)))})),
    nativeExecuted:false,pushDeliveryQualified:false},null,2)+'\n');
  console.log('Exact baseline plus effective repeatable notification kernel/visibility declarations and no guarded amendments across verified 1745 canonical inputs; full visibility/copy forwards and reverses saved.');
}
