import {execFileSync} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',file='supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql';
const sha=s=>createHash('sha256').update(s).digest('hex');
const source=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
const name='private.weekly_source_submission_request_start_v1',start=source.indexOf(`create or replace function ${name}(`),end=source.indexOf('$function$;',source.indexOf('as $function$',start));
if(start<0||end<0)throw Error('Native complete owner missing');const original=source.slice(start,end+12)+'\n';let output=original;
function once(before,after){if(output.split(before).length!==2)throw Error('Unique native source anchor '+before.slice(0,120));output=output.replace(before,after);}
once('  v_actor uuid;','  v_actor uuid;\n  v_contact_pair record;\n  v_contact_group public.weekly_source_groups%rowtype;\n  v_contact_guard jsonb;\n  v_contact_now timestamptz;\n  v_contact_owners uuid[];');
const oldStart=output.indexOf('  v_context:=private.weekly_source_query_cohort_ensure_v1(');
const oldEnd=output.indexOf('  insert into public.weekly_route_activations(',oldStart);
if(oldStart<0||oldEnd<oldStart||!output.slice(oldStart,oldEnd).includes("set state='SUPERSEDED'"))throw Error('Complete original request choice/supersession anchor');
output=output.slice(0,oldStart)+`  -- Same current deployment and sorted scope protocol before the native
  -- cohort/request/generation locks. Scope operands are the validated real
  -- Candidate/Client/Contract request members above, never a fake client.
  perform private.weekly_contact_source_submission_lock_v1(v_cycle_id,v_candidate_id,v_scopes,v_publication_id);
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  select * into strict v_contact_group from public.weekly_source_groups where id=(v_guard->>'source_group_id')::uuid;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-import-outreach:'||v_cycle_id::text||':'||v_candidate_id::text,0));
  select array_agg(distinct request.id order by request.id) into v_contact_owners
  from public.weekly_timesheet_submission_requests request
  join public.weekly_timesheet_submission_request_memberships member on member.submission_request_id=request.id
  join jsonb_array_elements(v_scopes)scope on(member.client_id,member.contract_id,member.week_ending)=
    ((scope->>'client_id')::uuid,(scope->>'contract_id')::uuid,(scope->>'week_ending')::date)
  where request.source_cycle_id=v_cycle_id and request.candidate_id=v_candidate_id
    and request.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED');
  if cardinality(v_contact_owners)>1 then
    raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
  if cardinality(v_contact_owners)=1 then
    select * into strict v_old_request from public.weekly_timesheet_submission_requests where id=v_contact_owners[1] for update;
    perform private.weekly_contact_submission_request_assert_v1(v_old_request.id);
    if exists(select 1 from jsonb_array_elements(v_scopes)scope where not exists(
      select 1 from public.weekly_timesheet_submission_request_memberships member
      where member.submission_request_id=v_old_request.id and(member.client_id,member.contract_id,member.week_ending)=
        ((scope->>'client_id')::uuid,(scope->>'contract_id')::uuid,(scope->>'week_ending')::date)
        and(member.expected_source_fingerprint=private.weekly_source_query_hex32_v1(
          scope->>'expected_source_fingerprint','WEEKLY_SOURCE_SCOPE_FINGERPRINT_INVALID')
          or private.weekly_source_missing_scope_unchanged_v1(v_old_request.current_projection_publication_id,v_publication_id,
            v_candidate_id,member.client_id,member.contract_id,member.week_ending)))) then
      raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
    select * into strict v_old_generation from public.weekly_candidate_outreach_generations generation
      where generation.candidate_cohort_id=v_old_request.candidate_cohort_id and generation.request_kind='SUBMIT_TIMESHEET'
        and generation.generation_number=v_old_request.request_generation;
    -- A B-only lookup may resolve the genuine legacy A-owned mixed request.
    -- Return that actual original generation and clock, never B's pointer.
    return pg_catalog.jsonb_build_object('ok',true,'status','UNCHANGED','submission_request_id',v_old_request.id,
      'candidate_generation_id',v_old_generation.id,'started_at_utc',v_old_request.started_at_utc,
      'reminder_due_at_utc',v_old_request.reminder_due_at_utc,'deadline_at_utc',v_old_request.deadline_at_utc);
  end if;
  -- Public operation is atomic across its exact original scope set. The
  -- private partition caller handles independent Client exclusions instead.
  for v_contact_pair in select distinct (scope->>'client_id')::uuid client_id from jsonb_array_elements(v_scopes)scope
    order by (scope->>'client_id')::uuid loop
    v_contact_guard:=private.weekly_contact_scope_guard_v1(v_candidate_id,v_contact_pair.client_id,'WORKER',clock_timestamp());
    if v_contact_guard->'allow' is distinct from 'true'::jsonb then
      raise exception 'CONTACT_PAUSED' using errcode='55000';
    end if;
  end loop;
  v_context:=private.weekly_source_query_cohort_ensure_v1(v_cycle_id,v_candidate_id,v_first_client_id,v_first_contract_id,v_first_work_date);
  select * into strict v_cohort from public.weekly_candidate_cohorts where id=(v_context->>'cohort_id')::uuid for update;
  if exists(select 1 from public.weekly_timesheet_submission_requests where candidate_cohort_id=v_cohort.id
    and state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')) then
    -- Do not append a new week to an already sent link or restart its clock.
    raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
  if exists(select 1 from public.weekly_timesheet_submission_requests request
    join public.weekly_timesheet_submission_request_memberships member on member.submission_request_id=request.id
    where request.source_cycle_id=v_cycle_id and request.candidate_id=v_candidate_id
      and request.state in('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED')
      and exists(select 1 from jsonb_array_elements(v_scopes)scope where member.client_id=(scope->>'client_id')::uuid)) then
    -- A legacy A-owned mixed question may already own B's older week. Its
    -- cohort identity cannot authorize a fresh B question for a new week.
    raise exception 'CONTACT_SOURCE_SUBMISSION_SCOPE_CONFLICT' using errcode='55000';end if;
  v_contact_now:=clock_timestamp();
`+output.slice(oldEnd);
const generationStart=output.indexOf('  select * into v_old_generation\n');
const generationEnd=output.indexOf('  select * into strict v_settings',generationStart);
if(generationStart<0||generationEnd<generationStart)throw Error('Original generation supersession anchor');
output=output.slice(0,generationStart)+output.slice(generationEnd);
output=output.replaceAll('pg_catalog.transaction_timestamp()', 'v_contact_now');
writeFileSync(path.join(base,'forward/33_native_submission_pause.sqlinc'),output.trimEnd()+'\n');
writeFileSync(path.join(base,'SUBMISSION_PAUSE_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_SUBMISSION_PAUSE_OWNER_INPUTS_V1',source_commit:commit,
 source_path:file,source_sha256_normalized_lf:sha(source),original_owner_sha256_normalized_lf:sha(original),
 successor_path:'forward/33_native_submission_pause.sqlinc',successor_sha256:sha(output.trimEnd()+'\n'),
 qualification:'SOURCE_ONLY_NOT_FOR_MOUNT_NATIVE_NOT_RUN',
 unchanged:'Existing signature, same-scope actual UNCHANGED receipt and original hash/clock inputs retained.',
 owner_delta:'Exact actual business membership lookup; genuine original generation/hash/clocks; no Candidate-wide supersession; per-cohort unique native creation after pause; public success shape unchanged. Private partition is separate internal composition.',
 native_dependencies:'Guarded cohort key/index successor and exact current controlling reader/caller successors; Root PostgreSQL/ACL/concurrency qualification NOT_RUN.'},null,2)+'\n');
console.log(JSON.stringify({complete_owner:1,status:'SOURCE_ONLY_NOT_FOR_MOUNT_NATIVE_NOT_RUN'}));
