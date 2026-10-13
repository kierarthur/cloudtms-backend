// Mechanical full-definition extraction from the latest reviewed owners.
// Run before qualification, never during deployment. No hosted access.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const read=file=>fs.readFileSync(path.join(root,file),'utf8').replaceAll('\r\n','\n');
function definition(file,name) {
  const source=read(file); const start=source.indexOf(`create or replace function ${name}(`);
  assert(start>=0,`Missing exact owner ${name}`);
  const end=source.indexOf('$function$;',source.indexOf('as $function$',start));
  assert(end>start,`Missing complete function ${name}`);
  return source.slice(start,end+'$function$;'.length);
}
function replaceOnce(source,before,after) {
  assert.equal(source.split(before).length,2,`Expected one exact replacement: ${before}`);
  return source.replace(before,after);
}
let home=definition('supabase/repeatable/26082026_1537_candidate_home_draft_identity_v1.sql','private._candidate_home_summary_v1');
home=replaceOnce(home,"where n.account_id=p_account_id and n.state='UNREAD';", "where n.account_id=p_account_id and n.candidate_id=p_candidate_id and n.state='UNREAD'\n      and private.candidate_notification_visible_v1(n,p_now_utc);");
let claim=definition('supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql','public.weekly_source_message_dispatch_target_claim_v1');
claim=replaceOnce(claim,'    order by target.next_attempt_at_utc nulls first,target.id',`    and (target.channel<>'PUSH' or exists(
      select 1 from public.weekly_message_dispatch_commands command
      join public.weekly_candidate_message_notifications message_notification on message_notification.message_intent_id=command.message_intent_id
      join public.candidate_notifications notification on notification.id=message_notification.notification_id
      where command.id=target.dispatch_command_id and notification.state='UNREAD'
        and private.candidate_notification_visible_v1(notification,pg_catalog.transaction_timestamp())
    ))
    order by target.next_attempt_at_utc nulls first,target.id`);
let start=definition('supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql','public.weekly_source_message_dispatch_target_start_atomic_v1');
// Deliberately after the SUBMISSION_STARTED replay branch: unknown/sent attempts
// are never reopened, reset or guessed. Manager EMAIL branches are untouched.
start=replaceOnce(start,"  if v_intent.audience_kind='CANDIDATE' and not exists(",`  if v_target.channel='PUSH' and not exists(
    select 1 from public.weekly_candidate_message_notifications message_notification
    join public.candidate_notifications notification on notification.id=message_notification.notification_id
    where message_notification.message_intent_id=v_intent.id and notification.state='UNREAD'
      and private.candidate_notification_visible_v1(notification,pg_catalog.transaction_timestamp())
  ) then
    update public.weekly_message_dispatch_targets
    set state='RETIRED',terminal_at_utc=pg_catalog.transaction_timestamp(),terminal_reason='NOTIFICATION_NO_LONGER_ACTIONABLE',
      next_attempt_at_utc=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null
    where id=v_target.id;
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return pg_catalog.jsonb_build_object('ok',false,'reason','NOTIFICATION_NO_LONGER_ACTIONABLE',
      'dispatch_target_id',v_target.id,'dispatch_command_id',v_command.id);
  end if;
  if v_intent.audience_kind='CANDIDATE' and not exists(`);
const acl=`
alter function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz) owner to postgres;
revoke all on function private._candidate_home_summary_v1(text,uuid,uuid,jsonb,timestamptz) from public,anon,authenticated,service_role;
alter function public.weekly_source_message_dispatch_target_claim_v1(jsonb) owner to postgres;
alter function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_message_dispatch_target_claim_v1(jsonb) from public,anon,authenticated;
revoke all on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_message_dispatch_target_claim_v1(jsonb) to service_role;
grant execute on function public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb) to service_role;
`;
fs.writeFileSync(path.join(root,'supabase/repeatable/10102026_1624_candidate_notification_consumers_v1.sql'),
  '-- Complete current definitions, mechanically derived from their reviewed owners.\n'+[home,claim,start].join('\n\n')+'\n'+acl);
console.log('Generated complete Home, PUSH claim and pre-submission recheck definitions.');
