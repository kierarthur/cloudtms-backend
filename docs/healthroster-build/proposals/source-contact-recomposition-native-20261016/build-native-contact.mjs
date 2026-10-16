import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { scanManagedSql, exactAtomicBody } from '../../../../scripts/cloudtms-managed-release-sql.mjs';

const base=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e';
const sha=s=>createHash('sha256').update(s).digest('hex');
const inputs=[];const cache=new Map();
function source(file){if(cache.has(file))return cache.get(file);const text=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');inputs.push({path:file,sha256:sha(text)});cache.set(file,text);return text;}
function extract(text,name){const marker=`create or replace function ${name}(`;const start=text.indexOf(marker);if(start<0)throw Error(name);const end=text.indexOf('$function$;',text.indexOf('as $function$',start))+12;return text.slice(start,end)+'\n';}
function once(text,old,replacement){if(text.split(old).length!==2)throw Error(`Expected one exact operand: ${old.slice(0,100)}`);return text.replace(old,replacement);}
function write(file,text){writeFileSync(path.join(base,file),text);}
const delivery=source('supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql');
const manager=source('supabase/repeatable/13102026_1438_weekly_source_protection_queries/13_manager_generation.sqlinc');
const runtime=source('broker/src/weekly-source/delivery-runtime.mjs');
// Read and pin complete controlling producers, including their final current winners.
for(const file of ['docs/healthroster-build/v5-amendment-20261015/authority/CONTACT_PAUSE_SPEC.md',
 'supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql',
 'supabase/repeatable/includes/weekly_contact_source_native_start_v1.sqlinc',
 'supabase/repeatable/includes/weekly_contact_source_memberships/010_native_logical_members.sqlinc',
 'supabase/repeatable/includes/weekly_contact_source_memberships/020_native_budget.sqlinc',
 'supabase/repeatable/includes/weekly_contact_source_memberships/030_native_enrol_assert.sqlinc',
 'supabase/repeatable/includes/weekly_contact_source_memberships/040_native_accept.sqlinc',
 'supabase/repeatable/includes/weekly_contact_timing/040_native_timing.sqlinc',
 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/12_manager_owned_facts.sqlinc',
 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/15_import_outreach.sqlinc',
 'supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql',
 'supabase/repeatable/10102026_1624_candidate_notification_consumers_v1.sql'])source(file);

const lockIntent=`  -- Service-owned discovery, then shared pause locks BEFORE the native row locks.
  perform private.weekly_contact_source_cycle_lock_v1((select source_cycle_id
    from public.weekly_message_intents where id=v_intent_id));
`;
let input=extract(delivery,'public.weekly_source_message_render_input_v1');
input=once(input,'\nstable\n','\nvolatile\n');
input=once(input,'  select * into strict v_intent from public.weekly_message_intents',lockIntent+'  select * into strict v_intent from public.weekly_message_intents');
const start=input.indexOf('  v_membership:=coalesce(('),end=input.indexOf("  v_shift_count:=",start);
if(start<0||end<0)throw Error('input projection');
input=input.slice(0,start)+'  v_membership:=private.weekly_contact_source_manager_members_v1(v_intent.id);\n'+input.slice(end);
// Candidate CHECK_HOURS remains one genuine worker/client generation. Never
// filter its captured members under the original candidate payload identity.
input=once(input,"    return pg_catalog.jsonb_build_object(\n      'ok',true,'audience_kind','CANDIDATE'",`    perform private.weekly_contact_source_candidate_render_assert_v1(v_candidate_generation.id);
    return pg_catalog.jsonb_build_object(
      'ok',true,'audience_kind','CANDIDATE'`);
write('forward/02_native_render_input.sqlinc',input);

let route=extract(delivery,'public.weekly_source_manager_route_prepare_atomic_v1');
route=once(route,'  select * into strict v_intent from public.weekly_message_intents',lockIntent+'  select * into strict v_intent from public.weekly_message_intents');
// A previous external route registration did not authorize a different subset.
// Retire only the unbound preparation; no accepted receipt or token is rewritten.
route=once(route,"  if found then\n    if v_preparation.state<>'PREPARED'",`  if found and v_preparation.state='PREPARED'
     and (v_preparation.projection_publication_id<>v_publication_id
       or v_preparation.original_membership_hash<>v_membership_hash
       or v_preparation.expires_at_utc<=clock_timestamp()) then
    update public.weekly_manager_route_preparations set state='RETIRED',retired_at_utc=clock_timestamp()
     where id=v_preparation.id;
    v_preparation.id:=null;
  end if;
  if v_preparation.id is not null then
    if v_preparation.state<>'PREPARED'`);
write('forward/03_native_route_prepare.sqlinc',route);

let stage=extract(delivery,'public.weekly_source_message_render_stage_atomic_v1');
stage=once(stage,'  select * into strict v_intent from public.weekly_message_intents',lockIntent+'  select * into strict v_intent from public.weekly_message_intents');
const a=stage.indexOf('    v_membership:=coalesce(('),b=stage.indexOf('    v_shift_count:=',a);
if(a<0||b<0)throw Error('stage projection');
stage=stage.slice(0,a)+'    v_membership:=private.weekly_contact_source_manager_members_v1(v_intent.id);\n'+stage.slice(b);
stage=once(stage,'    v_membership_hash:=v_candidate_generation.membership_hash;',`    perform private.weekly_contact_source_candidate_render_assert_v1(v_candidate_generation.id);
    v_membership_hash:=v_candidate_generation.membership_hash;`);
write('forward/04_native_render_stage.sqlinc',stage);

let generation=extract(manager,'private.weekly_source_query_manager_generation_v1');
generation=once(generation,'declare\n','declare\n  v_contact_at timestamptz;\n  v_contact_due public.weekly_manager_cohort_due_events%rowtype;\n');
generation=once(generation,'  select * into strict v_route',`  perform private.weekly_contact_source_cycle_lock_v1((select source_cycle_id
    from public.weekly_manager_recipient_routes where id=p_recipient_route_id));
  -- The same target -> command order as START, before generation/route mutation.
  perform target.id from public.weekly_message_dispatch_targets target
   join public.weekly_message_dispatch_commands command on command.id=target.dispatch_command_id
   where command.recipient_route_id=p_recipient_route_id order by target.id for update of target;
  perform id from public.weekly_message_dispatch_commands where recipient_route_id=p_recipient_route_id order by id for update;
  v_contact_at:=clock_timestamp();
  select * into strict v_route`);
generation=once(generation,"  v_membership:=coalesce((",`  if v_old.id is not null and exists(select 1 from public.weekly_message_intents intent
    join public.weekly_message_dispatch_commands command on command.message_intent_id=intent.id
    where intent.recipient_generation_id=v_old.id and command.state in('READY','LEASED','FAILED')
      and private.weekly_contact_source_render_attempted_v1(command.message_render_id)) then
    -- Existing ABI permits retry only on its original current native command.
    -- Never supersede that generation into a fresh budget owner on a recheck.
    return jsonb_build_object('ok',true,'status','ORIGINAL_ATTEMPT_OWNER_PENDING',
      'recipient_generation_id',v_old.id,'recipient_route_id',v_route.id);
  end if;
  v_membership:=coalesce((`);
const filter='      and not private.weekly_source_covered_hours_incident_v1(incident.id)';
if(generation.split(filter).length!==3)throw Error('two generation membership projections');
generation=generation.replaceAll(filter,filter+'\n      and private.weekly_contact_source_manager_member_eligible_v1(incident.id,v_route.id,v_contact_at)');
// Do not revoke any real original attempt, even if later summary state changed.
generation=once(generation,"      and render.state not in ('SUBMISSION_STARTED','ACCEPTED','AMBIGUOUS');",`      and render.state not in ('SUBMISSION_STARTED','ACCEPTED','AMBIGUOUS')
      and not private.weekly_contact_source_render_attempted_v1(render.id);`);
generation=once(generation,"    where recipient_generation_id=v_old.id and state in ('DUE','RENDERED');",`    where recipient_generation_id=v_old.id and state in ('DUE','RENDERED')
      and not exists(select 1 from public.weekly_message_renders render where render.message_intent_id=public.weekly_message_intents.id
        and private.weekly_contact_source_render_attempted_v1(render.id));`);
generation=once(generation,"      and command.state in ('READY','LEASED');",`      and command.state in ('READY','LEASED')
      and not private.weekly_contact_source_render_attempted_v1(command.message_render_id);`);
generation=once(generation,"      and render.state='CURRENT';",`      and render.state='CURRENT' and not private.weekly_contact_source_render_attempted_v1(render.id);`);
generation=once(generation,'    v_candidate_generation.id:=null;','    v_contact_due.id:=null;\n    v_candidate_generation.id:=null;');
generation=once(generation,"      v_due:=private.weekly_source_manager_interval_due_v1(v_row.client_id,p_started_at_utc,v_settings.manager_first_delay_value,v_settings.manager_first_delay_unit);",`      -- Recomposition preserves the actual prior native due event for the same
      -- saved member/purpose. No activation timestamp is guessed as a send clock.
      select event.* into v_contact_due from public.weekly_manager_cohort_due_events event
       join public.weekly_manager_recipient_generations old_generation on old_generation.id=event.recipient_generation_id
       where old_generation.recipient_route_id=v_route.id and event.candidate_cohort_id=v_row.candidate_cohort_id
        and event.recipient_generation_id<>v_new.id and event.event_kind='EARLY_ALL'
        and exists(select 1 from public.weekly_manager_recipient_memberships old_member
         join public.weekly_manager_recipient_memberships new_member on new_member.incident_id=old_member.incident_id
         where old_member.recipient_generation_id=event.recipient_generation_id
          and new_member.recipient_generation_id=v_new.id and new_member.candidate_cohort_id=event.candidate_cohort_id
          and private.weekly_source_manager_due_event_covers_v1(event.id,new_member.incident_id))
       order by event.due_at_utc,event.id limit 1;
      if found then v_due:=v_contact_due.due_at_utc;
      else v_due:=private.weekly_source_manager_interval_due_v1(v_row.client_id,p_started_at_utc,
        v_settings.manager_first_delay_value,v_settings.manager_first_delay_unit);end if;`);
generation=once(generation,"      coalesce(v_candidate_generation.started_at_utc,p_started_at_utc),v_due,'PENDING'",`      case when v_contact_due.id is not null then v_contact_due.cohort_started_at_utc
        else coalesce(v_candidate_generation.started_at_utc,p_started_at_utc) end,v_due,'PENDING'`);
write('forward/05_native_manager_generation.sqlinc',generation);

let candidate=extract(delivery,'private.weekly_source_query_candidate_generation_v1');
candidate=once(candidate,'declare\n','declare\n  v_contact_guard jsonb;\n');
candidate=once(candidate,'  select incident.id,comparison.contract_id,work_event.work_date',`  perform private.weekly_contact_source_cycle_lock_v1(p_source_cycle_id);
  select incident.id,comparison.contract_id,work_event.work_date`);
candidate=once(candidate,'  v_cohort_context:=private.weekly_source_query_cohort_ensure_v1(',`
  v_contact_guard:=private.weekly_contact_scope_guard_v1(p_candidate_id,p_client_id,'WORKER',clock_timestamp());
  if v_contact_guard->'allow' is distinct from 'true'::jsonb then
    return jsonb_build_object('ok',true,'status','CONTACT_PAUSED','candidate_id',p_candidate_id,'client_id',p_client_id,
      'next_allowed_utc',v_contact_guard->'next_allowed_utc');
  end if;
  v_cohort_context:=private.weekly_source_query_cohort_ensure_v1(`);
write('forward/06_native_candidate_activation.sqlinc',candidate);

let scheduler=extract(delivery,'public.weekly_source_query_scheduler_tick_v1');
scheduler=once(scheduler,"    if exists(\n      select 1 from public.weekly_message_intents intent\n      where intent.recipient_generation_id=v_route.generation_id",`    -- Only the actual native due-route batch selected above, with its existing
    -- ordering/limit. No global sweep, new queue or unrelated-route capacity gate.
    perform private.weekly_source_query_manager_generation_v1(v_route.route_id,'NEW_INCIDENT',clock_timestamp(),false);
    perform private.weekly_contact_source_manager_recompose_v1(v_route.route_id);
    if exists(
      select 1 from public.weekly_message_intents intent
      where intent.recipient_generation_id=v_route.generation_id`);
// Scheduler input is a timestamp of a real trigger, never a freshness/cutoff authority.
scheduler=once(scheduler,'  if v_limit<1 or v_limit>1000 then',`  v_now:=clock_timestamp();
  if v_limit<1 or v_limit>1000 then`);
scheduler=once(scheduler,"  return pg_catalog.jsonb_build_object(\n    'ok',true,'now_utc',v_now",`  perform private.weekly_contact_source_candidate_due_collapse_v1(v_now,v_limit);
  return pg_catalog.jsonb_build_object(
    'ok',true,'now_utc',v_now`);
write('forward/08_native_scheduler.sqlinc',scheduler);

let admission=extract(source('supabase/repeatable/16102026_0214_weekly_contact_source_admission_v1.sql'),
 'private.weekly_contact_source_membership_v1');
admission=once(admission,"     m.response_state<>'UNANSWERED' or i.state<>'OPEN' or i.manager_action_state='RESPONDED'",`     m.response_state<>'UNANSWERED' or i.state<>'OPEN' or i.manager_action_state='RESPONDED'
     or not private.weekly_contact_source_manager_current_need_v1(i.id,v_intent.recipient_route_id,clock_timestamp())`);
write('forward/09_native_current_need.sqlinc',admission);

// Runtime is a finite complete current contribution, not our historical host.
// Counts/links/body continue through the exact existing renderer and route owner.
let worker=runtime;
worker=once(worker,'  const token = await deterministicManagerToken(env, preparation);',`  if (preparation?.membership_hash !== input.membership_hash) {
    throw new Error('WEEKLY_SOURCE_RENDER_MEMBERSHIP_STALE');
  }
  if (!Array.isArray(input.rows) || input.rows.length !== input.shift_count
      || new Set(input.rows.map(row => row.clientId)).size !== input.client_count
      || new Set(input.rows.map(row => row.candidateId)).size !== input.candidate_count) {
    throw new Error('WEEKLY_SOURCE_MANAGER_RENDER_INVALID');
  }
  const token = await deterministicManagerToken(env, preparation);`);
worker=once(worker,'  let failed = 0;\n  for (const item', '  let failed = 0;\n  let deferred = 0;\n  for (const item');
worker=once(worker,"      failed += 1;\n      console.warn('[weekly-source-delivery] render failed'",`      if (['CONTACT_SOURCE_RENDER_PAUSED','WEEKLY_SOURCE_MANAGER_RENDER_EMPTY'].includes(text(error?.message))) {
        deferred += 1;
        continue;
      }
      failed += 1;
      console.warn('[weekly-source-delivery] render failed'`);
worker=once(worker,'  return { due: Number(due?.due_count || 0), rendered, failed };','  return { due: Number(due?.due_count || 0), rendered, deferred, failed };');
write('runtime/delivery-runtime.mjs',worker);

const acl=[];
for(const file of readdirSync(path.join(base,'forward')).filter(x=>x.endsWith('.sqlinc')&&!x.endsWith('_acl.sqlinc'))){
 const sql=readFileSync(path.join(base,'forward',file),'utf8');
 for(const m of sql.matchAll(/create or replace function ((public|private)\.[a-z0-9_]+)\(([\s\S]*?)\)\s*returns/gi)){
  const signature=m[1]+'('+m[3].split(',').map(s=>s.trim().split(/\s+/)[1]).join(',')+')';
  acl.push(`alter function ${signature} owner to postgres;\nrevoke all on function ${signature} from public,anon,authenticated,service_role;`+(m[2]==='public'?`\ngrant execute on function ${signature} to service_role;`:''));
 }
}
write('forward/99_acl.sqlinc',acl.join('\n')+'\n');
const outputs=readdirSync(path.join(base,'forward')).filter(x=>x.endsWith('.sqlinc')).sort().map(name=>({path:'forward/'+name,sha256:sha(readFileSync(path.join(base,'forward',name)))}));
const atomic='begin;\n'+outputs.map(o=>readFileSync(path.join(base,o.path),'utf8')).join('\n')+'\ncommit;\n';
const statements=scanManagedSql(exactAtomicBody(atomic,{allowAtomicBody:false})).statements.length;
write('SOURCE_RECIPE.json',JSON.stringify({version:'SOURCE_CONTACT_NATIVE_RECOMPOSITION_V1',base_commit:commit,
 input_hashes_normalized_lf:true,inputs,outputs,runtime:{path:'runtime/delivery-runtime.mjs',mount:'broker/src/weekly-source/delivery-runtime.mjs',sha256:sha(worker)},
 sql_statements:statements,packaging:'scanManagedSql + exactAtomicBody only; PL/pgSQL opaque, not PostgreSQL/native proof',
 builder_sha256:sha(readFileSync(fileURLToPath(import.meta.url))),
 tests:{path:'tests/current-native-contact.test.mjs',sha256:sha(readFileSync(path.join(base,'tests/current-native-contact.test.mjs'))),
  actual_renderer:{path:'broker/src/weekly-source/manager-email.js',source_commit:commit,sha256:sha(source('broker/src/weekly-source/manager-email.js'))},
  qualification:'Actual complete published renderer + Worker orchestration unit tests, supplied RPC facts; no PostgreSQL/native fixture proof'},
 native_status:'REQUIRED_NOT_RUN',runtime_status:'SOURCE_TESTS_ONLY',
 prerequisites:['Current 0214 Source admission / 0226 START / 0403 logical memberships and timing/native schema',
  'Current 13_manager_generation and 12_manager_owned_facts at pinned source; exact complete successors only'],
 gaps:['Client-specific contact cutoff/prohibition is not the Source-group report cutoff; actual native contact read/START qualification remains open',
  'Health-owned AUTOMATIC follow-up and explicit manual preview/accept ordinal producers remain implementation work; no ordinal fabricated',
  'Health-owned expiry/resume recheck task consumer remains implementation work; this finite scheduler amendment only consumes its real selected due-route batch',
  'Multi-client SUBMIT_TIMESHEET frozen request recomposition and pre-activation pause remains open',
  'CHECK_HOURS answered-member subset generation with original +6/+12 clock preservation remains open',
  'Full same-TX native qualification, concurrency, ACL and provider-free rollback proof remains Root work']},null,2)+'\n');
console.log(JSON.stringify({outputs:outputs.length,statements,native:'REQUIRED_NOT_RUN'}));
