import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',inputs=[],owners=[];
const sha=s=>createHash('sha256').update(s).digest('hex');
function source(file){const value=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 inputs.push({path:file,source_commit:commit,sha256_normalized_lf:sha(value)});return value;}
function extract(source,name){const start=source.indexOf(`create or replace function ${name}(`),end=source.indexOf('$function$;',source.indexOf('as $function$',start));
 if(start<0||end<start)throw Error('Exact complete owner '+name);const value=source.slice(start,end+12)+'\n';owners.push({name,original_sha256_normalized_lf:sha(value)});return value;}
function once(text,old,next){if(text.split(old).length!==2)throw Error('Unique source anchor '+old.slice(0,150));return text.replace(old,next);}
const native=source('supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql'),outputs=[];
let get=extract(native,'public.weekly_source_candidate_query_get_v1');
get=once(get,`    where candidate_cohort_id=v_generation.candidate_cohort_id
      and state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED');`,`    where id=private.weekly_contact_submission_request_for_generation_v1(v_generation.id)
      and state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED');`);outputs.push(get);
let complete=extract(native,'public.weekly_source_timesheet_submission_complete_atomic_v1');
complete=once(complete,`    and generation.request_kind='SUBMIT_TIMESHEET' and generation.state='ACTIVE'`,
 `    and generation.source_cycle_id=v_submission.source_cycle_id and generation.candidate_id=v_submission.candidate_id
    and generation.generation_number=v_submission.request_generation and generation.membership_hash=v_submission.membership_hash
    and generation.request_kind='SUBMIT_TIMESHEET' and generation.state='ACTIVE'`);outputs.push(complete);
let legacyStart=extract(native,'public.weekly_source_message_dispatch_submission_start_atomic_v1');
const old=`on submission.candidate_cohort_id=generation.candidate_cohort_id
       and submission.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')`;
if(legacyStart.split(old).length!==3)throw Error('Two original native legacy START membership lookups');
legacyStart=legacyStart.replaceAll(old,`on submission.candidate_cohort_id=generation.candidate_cohort_id
       and submission.source_cycle_id=generation.source_cycle_id and submission.candidate_id=generation.candidate_id
       and submission.request_generation=generation.generation_number and submission.membership_hash=generation.membership_hash
       and submission.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED')`);outputs.push(legacyStart);
let projection=extract(source('supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql'),
 'private.weekly_source_candidate_app_projection_v1');
projection=once(projection,`    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=p_candidate_id
      and state not in ('SUPERSEDED','CANCELLED')
    order by request_generation desc,id desc limit 1;`,`    where id=private.weekly_contact_submission_request_for_generation_v1(v_generation.id)
      and candidate_id=p_candidate_id and state not in ('SUPERSEDED','CANCELLED');`);outputs.push(projection);
let materialise=extract(source('supabase/repeatable/10102026_0853_weekly_source_pay_query_entry_admission_v1.sql'),
 'public.weekly_source_candidate_check_materialise_atomic_v1');
materialise=once(materialise,`    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=v_candidate_id and source_cycle_id=v_cycle.id`,
 `    where id=private.weekly_contact_submission_request_for_generation_v1(v_generation.id)
      and candidate_id=v_candidate_id and source_cycle_id=v_cycle.id`);outputs.push(materialise);
materialise=once(materialise,'      and current_projection_publication_id=v_projection_publication_id\n','');
materialise=once(materialise,'  -- SP-12: existing admission only; no producer/version evidence.',
`  if v_kind<>'SELF_SUBMIT' then
    -- Plain discovery before workflow/generation/family locks. Existing
    -- materialisation replay/current actor/work checks remain in their order.
    select * into strict v_generation from public.weekly_candidate_outreach_generations
      where id=v_candidate_generation_id and candidate_id=v_candidate_id and request_kind=v_kind;
    select * into strict v_cycle from public.weekly_source_cycles where id=v_generation.source_cycle_id;
    perform private.weekly_contact_source_submission_lock_v1(v_cycle.id,null,'[]',v_projection_publication_id);
  end if;
  -- SP-12: existing admission only; no producer/version evidence.`);
materialise=once(materialise,`    where id=v_scope_id and submission_request_id=v_submission.id and state='WAITING'
    for update;`,`    where id=v_scope_id and submission_request_id=v_submission.id and state='WAITING'
    for update;
    if not private.weekly_source_missing_scope_unchanged_v1(v_submission.current_projection_publication_id,
      v_projection_publication_id,v_candidate_id,v_membership.client_id,v_membership.contract_id,v_membership.week_ending) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_MATERIALISATION_STALE' using errcode='40001';end if;`);
outputs[outputs.length-1]=materialise;
let compare=extract(source('supabase/repeatable/14102026_0420_healthroster_current_native_composition/weekly_source_candidate_submission_compare_sync_v1.complete.sqlinc'),
 'private.weekly_source_candidate_submission_compare_sync_v1');
compare=once(compare,`    where candidate_cohort_id=v_generation.candidate_cohort_id
      and candidate_id=v_generation.candidate_id`,
 `    where id=private.weekly_contact_submission_request_for_generation_v1(v_generation.id)
      and candidate_id=v_generation.candidate_id`);outputs.push(compare);
compare=once(compare,'      and current_projection_publication_id=v_publication.id\n','');
compare=once(compare,'  perform private.weekly_source_pay_query_admit_v2();',
 `  perform private.weekly_contact_source_submission_lock_v1(v_discovered_cycle_id,null,'[]',p_projection_publication_id);
  perform private.weekly_source_pay_query_admit_v2();`);
compare=once(compare,`    where id=p_membership_id and submission_request_id=v_submission.id
      and state='WAITING'
    for update;`,`    where id=p_membership_id and submission_request_id=v_submission.id
      and state='WAITING'
    for update;
    if not private.weekly_source_missing_scope_unchanged_v1(v_submission.current_projection_publication_id,
      v_publication.id,v_generation.candidate_id,v_membership.client_id,v_membership.contract_id,v_membership.week_ending) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_COMPARISON_STALE' using errcode='40001';end if;`);
outputs[outputs.length-1]=compare;
let notification=extract(source('supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql'),
 'private.weekly_source_candidate_notification_intent_v1');
notification=once(notification,`      and submission.request_generation=v_generation.generation_number`,
 `      and submission.request_generation=v_generation.generation_number and submission.membership_hash=v_generation.membership_hash`);outputs.push(notification);
let view=extract(source('supabase/repeatable/17092026_1100_weekly_source_candidate_view_producer_v1.sql'),
 'private.weekly_source_candidate_view_request_v1');
const selectStart=view.indexOf("  select generation.id,membership.id,'SUBMIT_TIMESHEET'"),selectEnd=view.indexOf('  if v_request_id is not null then',selectStart);
if(selectStart<0||selectEnd<selectStart)throw Error('Exact current Candidate view native submission lookup');
view=view.slice(0,selectStart)+`  -- Actual current active owner takes precedence over a completed original.
  -- More than one genuine owner is unresolved, never greatest-generation wins.
  select result->>'request_id',result->>'scope_id',result->>'request_kind'
    into v_request_id,v_scope_id,v_kind
  from (select private.weekly_contact_submission_view_request_v1(v_candidate_id,v_contract_id,v_week,v_timesheet_id) result) exact;
`+view.slice(selectEnd);outputs.push(view);
const output=outputs.join('\n').trimEnd()+'\n';writeFileSync(path.join(base,'forward/38_native_submission_readers.sqlinc'),output);
writeFileSync(path.join(base,'SUBMISSION_READER_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_SUBMISSION_READER_OWNER_INPUTS_V1',inputs,owners,
 output:{path:'forward/38_native_submission_readers.sqlinc',sha256:sha(output)},
 qualification:'Finite complete current Health-owned readers only; native/ABI/concurrency NOT_RUN; original financial/admission bodies preserved except exact request identity operands.'},null,2)+'\n');
console.log(JSON.stringify({complete_owners:outputs.length,source_only:true}));
