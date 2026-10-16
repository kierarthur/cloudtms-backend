import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {schedulerPrefix,groupedSchedulerFollowups} from './build-scheduler-prefix.mjs';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',checkpoint='18f844f1c01c7d1cf293bc4e92a2ab7e0fb762eb';
const sha=s=>createHash('sha256').update(s).digest('hex'),inputs=[];
function source(file,ref=commit){const text=execFileSync('git',['show',`${ref}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 inputs.push({source_commit:ref,path:file,sha256_normalized_lf:sha(text)});return text;}
function extract(text,name){const start=text.indexOf(`create or replace function ${name}(`);if(start<0)throw Error(name);
 const end=text.indexOf('$function$;',text.indexOf('as $function$',start));if(end<0)throw Error(name+' terminator');return text.slice(start,end+12)+'\n';}
function once(text,old,next){if(text.split(old).length!==2)throw Error('Unique source anchor: '+old.slice(0,120));return text.replace(old,next);}
function write(file,text){writeFileSync(path.join(base,'forward',file),text.trimEnd()+'\n');}
let retire=extract(source('supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql'),'private.weekly_source_candidate_notification_retire_v1');
retire=once(retire,`  if new.audience_kind='CANDIDATE' and new.state='RETIRED' and old.state<>'RETIRED' then`,
`  if new.audience_kind='CANDIDATE' and new.state='RETIRED' and old.state<>'RETIRED' then
    -- Retiring an alert alone cannot retire the original still-current in-app
    -- question. A real replacement notification, when present, owns the one
    -- useful alert; otherwise retain the genuine native question fallback.
    if exists(select 1 from public.weekly_candidate_message_notifications link
      join public.candidate_notifications notification on notification.id=link.notification_id
      where link.message_intent_id=new.id and private.candidate_notification_visible_v1(notification,clock_timestamp()))
      and not exists(select 1 from public.weekly_candidate_message_notifications link
        join public.weekly_message_intents intent on intent.id=link.message_intent_id
        join public.candidate_notifications notification on notification.id=link.notification_id
        where link.candidate_generation_id=new.candidate_generation_id and intent.id<>new.id and intent.state in('DUE','RENDERED')
          and private.candidate_notification_visible_v1(notification,clock_timestamp())) then
      update public.candidate_notifications notification set
        push_state=case when notification.push_state in('PENDING','CLAIMED','FAILED') then 'SKIPPED' else notification.push_state end,
        last_error=case when notification.push_state in('PENDING','CLAIMED','FAILED') then 'ALERT_RETIRED_QUESTION_CURRENT' else notification.last_error end
      from public.weekly_candidate_message_notifications link where link.message_intent_id=new.id and notification.id=link.notification_id;
      update public.weekly_candidate_message_notifications set retired_at_utc=coalesce(retired_at_utc,clock_timestamp())
        where message_intent_id=new.id;
      return new;
    end if;`);
write('30_native_notification_retirement.sqlinc',retire);
let scheduler=source('docs/healthroster-build/proposals/source-contact-recomposition-native-20261016/forward/08_native_scheduler.sqlinc',checkpoint);
scheduler=once(scheduler,`          where submission.candidate_cohort_id=generation.candidate_cohort_id
            and submission.state in ('ACTIVE','PARTLY_SUBMITTED')`,`          where submission.candidate_cohort_id=generation.candidate_cohort_id
            and submission.source_cycle_id=generation.source_cycle_id and submission.candidate_id=generation.candidate_id
            and submission.request_generation=generation.generation_number and submission.membership_hash=generation.membership_hash
            and submission.state in ('ACTIVE','PARTLY_SUBMITTED')`);
scheduler=once(scheduler,'  v_actionable integer;','  v_actionable integer;\n  v_followup record;\n  v_followup_result jsonb;\n  v_followup_results jsonb:=\'[]\';\n  v_followup_held integer:=0;');
scheduler=once(scheduler,'  perform private.weekly_contact_source_candidate_due_collapse_v1(v_now,v_limit);',
`  perform private.weekly_contact_source_candidate_due_collapse_v1(v_now,v_limit);
  -- Actual accepted native history, not consumed initial due-events, supplies
  -- later follow-up candidates. The bounded current batch retains genuine due
  -- ordering. Pending original owners are excluded without a lifetime cap.
  for v_followup in
    with current_candidates as materialized(
      select incident.id,private.weekly_contact_source_followup_candidate_v1(incident.id,v_now) state
      from public.weekly_discrepancy_incidents incident
      where incident.state='OPEN' and incident.manager_potential_state='AVAILABLE'
        and incident.manager_action_state not in('RESPONDED','NOT_REQUIRED')
        and(exists(select 1 from public.weekly_contact_delivery_memberships member
          where member.incident_id=incident.id and member.incident_episode=incident.episode_number
            and member.audience='MANAGER' and member.accepted_at_utc is not null)
          or exists(select 1 from public.weekly_manager_review_items item join public.weekly_manager_review_batches batch on batch.id=item.review_batch_id
            join public.weekly_message_dispatch_commands command on command.message_render_id=batch.message_render_id
            where item.incident_id=incident.id and item.incident_episode=incident.episode_number and(
              command.provider_accepted_at_utc is not null
              or exists(select 1 from public.weekly_message_provider_attempts attempt where attempt.dispatch_command_id=command.id
                and attempt.outcome='ACCEPTED' and attempt.completed_at_utc is not null)
              or exists(select 1 from public.weekly_message_dispatch_targets target join public.weekly_message_target_attempts attempt
                on attempt.dispatch_target_id=target.id where target.dispatch_command_id=command.id
                  and attempt.outcome='ACCEPTED' and attempt.completed_at_utc is not null))))
    )
    select candidate.* from current_candidates candidate where candidate.state->>'reason'='HELD_DEPENDENCY'
      or(candidate.state->>'reason'='SCHEDULED' and candidate.state->>'mode'='AUTOMATIC'
        and(candidate.state->>'due_at_utc')::timestamptz<=v_now and not exists(
          select 1 from public.weekly_contact_source_ordinal_owners owner join public.weekly_message_intents intent on intent.id=owner.message_intent_id
          where owner.logical_contact_key=decode(candidate.state->>'logical_contact_key','hex') and intent.state in('DUE','RENDERED')
            and not exists(select 1 from public.weekly_contact_delivery_memberships accepted where accepted.logical_contact_key=owner.logical_contact_key
              and accepted.send_ordinal=owner.send_ordinal and accepted.accepted_at_utc is not null)))
    order by(candidate.state->>'due_at_utc')::timestamptz nulls last,candidate.id limit v_limit
  loop
    begin
      if v_followup.state->>'reason'='HELD_DEPENDENCY' then
        v_followup_held:=v_followup_held+1;v_followup_result:=v_followup.state;
      else
        v_followup_result:=private.weekly_contact_source_manager_followup_enqueue_v1(v_followup.id,clock_timestamp());
      end if;
    exception when others then
      v_followup_held:=v_followup_held+1;
      v_followup_result:=jsonb_build_object('incident_id',v_followup.id,'disposition','HELD_DEPENDENCY','code','CONTACT_SOURCE_FOLLOWUP_ITEM_HELD');
    end;
    v_followup_results:=v_followup_results||jsonb_build_array(v_followup_result);
  end loop;`);
scheduler=once(scheduler,`    'timesheet_requests_marked_overdue',v_overdue_count,`,`    'timesheet_requests_marked_overdue',v_overdue_count,
    'routine_followup_results',v_followup_results,'routine_followup_held',v_followup_held,`);
const prefixed=schedulerPrefix(scheduler);
write('32_native_scheduler_followups.sqlinc',groupedSchedulerFollowups(prefixed.output));
write('47_native_scheduler_selection.sqlinc',prefixed.snapshot);
const queryPath='supabase/repeatable/includes/weekly_contact_query/040_native_recomposition.sqlinc';
source(queryPath,'1859790b0596761f405a10c258394a1d47ca304e');
source('supabase/repeatable/10102026_1623_candidate_notification_management_v1.sql');
writeFileSync(path.join(base,'SCHEDULER_NOTIFICATION_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_SCHEDULER_NOTIFICATION_OWNER_INPUTS_V1',inputs,
 existing_query_owner:{signature:'private.weekly_contact_query_recompose_v1(uuid,uuid,text,uuid,text)',
 dependency_status:'PUBLISHED_SOURCE_PRESENT_NOT_REDECLARED_BY_THIS_LANE'},
 outputs:['30_native_notification_retirement.sqlinc','32_native_scheduler_followups.sqlinc','47_native_scheduler_selection.sqlinc'].map(file=>({path:'forward/'+file,
 sha256:sha(readFileSync(path.join(base,'forward',file)))})),qualification:'Authored source; actual PostgreSQL/scheduler/provider-free qualification NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({complete_native_owners:2,status:'SOURCE_ONLY_NATIVE_NOT_RUN'}));
