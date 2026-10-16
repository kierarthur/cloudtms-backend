import {execFileSync} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',inputs=[];
const sha=s=>createHash('sha256').update(s).digest('hex');
function source(file){const text=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 inputs.push({path:file,source_commit:commit,sha256_normalized_lf:sha(text)});return text;}
function extract(text,name){const start=text.indexOf(`create or replace function ${name}(`),end=text.indexOf('$function$;',text.indexOf('as $function$',start));
 if(start<0||end<start)throw Error('Complete current owner '+name);return text.slice(start,end+12)+'\n';}
function once(text,old,next){if(text.split(old).length!==2)throw Error('Unique native source anchor '+old.slice(0,130));return text.replace(old,next);}
const current=source('supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql');
let reminder=extract(current,'public.weekly_source_candidate_reminder_atomic_v1');
reminder=once(reminder,'  v_actor uuid;','  v_actor uuid;\n  v_contact_pair record;\n  v_contact_guard jsonb;');
reminder=once(reminder,`  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=v_generation_id for update;`,`  -- Plain actor/current scope validation precedes shared contact locks.
  select * into strict v_generation from public.weekly_candidate_outreach_generations where id=v_generation_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
  perform private.weekly_source_office_authority_v1(v_actor,'ASK_CANDIDATES',v_cycle.source_group_id,
    v_generation.client_id,v_cycle.finalisation_week_ending);
  perform private.weekly_contact_source_submission_lock_v1(v_cycle.id,null,'[]',v_publication_id);
  -- Original native locking/currentness/authority/cooldown checks revalidate.
  select * into strict v_generation from public.weekly_candidate_outreach_generations
  where id=v_generation_id for update;`);
reminder=once(reminder,`        and request.request_generation=v_generation.generation_number`,
 `        and request.request_generation=v_generation.generation_number and request.membership_hash=v_generation.membership_hash
        and request.source_cycle_id=v_generation.source_cycle_id and request.candidate_id=v_generation.candidate_id`);
reminder=once(reminder,'  select * into strict v_settings from public.weekly_source_global_settings where singleton;',
`  for v_contact_pair in
    select v_generation.candidate_id candidate_id,v_generation.client_id client_id where v_generation.request_kind='CHECK_HOURS'
    union select request.candidate_id,member.client_id from public.weekly_timesheet_submission_requests request
      join public.weekly_timesheet_submission_request_memberships member on member.submission_request_id=request.id
      where v_generation.request_kind='SUBMIT_TIMESHEET'
        and request.candidate_cohort_id=v_generation.candidate_cohort_id and request.request_generation=v_generation.generation_number
        and request.candidate_id=v_generation.candidate_id and request.source_cycle_id=v_generation.source_cycle_id
        and request.membership_hash=v_generation.membership_hash and request.state in('ACTIVE','OVERDUE','PARTLY_SUBMITTED') and member.state='WAITING'
    order by candidate_id,client_id
  loop
    v_contact_guard:=private.weekly_contact_scope_guard_v1(v_contact_pair.candidate_id,v_contact_pair.client_id,'WORKER',clock_timestamp());
    if v_contact_guard->'allow' is distinct from 'true'::jsonb then
      raise exception 'CONTACT_PAUSED' using errcode='55000',detail=jsonb_build_object('version','CONTACT_PAUSE_REFUSAL_V1',
        'candidate_id',v_contact_pair.candidate_id,'client_id',v_contact_pair.client_id,'audience','WORKER',
        'next_allowed_utc',v_contact_guard->'next_allowed_utc',
        'next_allowed_local',to_char((v_contact_guard->>'next_allowed_utc')::timestamptz at time zone 'Europe/London',
          'YYYY-MM-DD HH24:MI')||' Europe/London')::text;
    end if;
  end loop;
  select * into strict v_settings from public.weekly_source_global_settings where singleton;`);
let submit=extract(source('supabase/repeatable/10102026_0853_weekly_source_pay_query_entry_admission_v1.sql'),
 'public.weekly_source_candidate_app_submit_atomic_v1');
submit=once(submit,'  -- SP-12: existing admission only; no producer/version evidence.',
`  -- Current authenticated Candidate and real original generation supply only
  -- contact lock scope. Do not pause replies, change replay ordering, allocate
  -- a question or introduce contact dependencies into ordinary self-submit.
  select * into strict v_generation from public.weekly_candidate_outreach_generations
    where id=p_request_id and candidate_id=v_candidate_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
  perform private.weekly_contact_source_submission_lock_v1(v_cycle.id,null,'[]',v_cycle.current_projection_publication_id);
  -- SP-12: existing admission only; no producer/version evidence.`);
const output=(reminder+'\n'+submit).trimEnd()+'\n';
writeFileSync(path.join(base,'forward/40_candidate_contact_entry_owners.sqlinc'),output);
writeFileSync(path.join(base,'CANDIDATE_CONTACT_ENTRY_INPUTS.json'),JSON.stringify({version:'CONTACT_CANDIDATE_ENTRY_INPUTS_V1',inputs,
 output:{path:'forward/40_candidate_contact_entry_owners.sqlinc',sha256:sha(output)},
 refusal:{sqlstate:'55000',message:'CONTACT_PAUSED',http_status:409,success_shape_changed:false,
  detail:{version:'CONTACT_PAUSE_REFUSAL_V1',candidate_id:'canonical UUID',client_id:'canonical UUID',audience:'WORKER',
   next_allowed_utc:'actual native guard canonical UTC6 future timestamp',
   next_allowed_local:'same actual expiry through existing native pause_shape formatter: YYYY-MM-DD HH:mm Europe/London'}},
 qualification:'Finite complete current owners; replies remain available while paused; native first-use/replay/order/concurrency NOT_RUN.'},null,2)+'\n');
console.log(JSON.stringify({complete_owners:2,source_only:true}));
