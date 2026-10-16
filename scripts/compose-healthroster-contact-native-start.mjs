import assert from 'node:assert/strict';
import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';

// Finite amendment of the actual current complete START. Do not regenerate
// Home/claim from older owners or rewrite their historical source pages.
const commit='abd5400e384a815aa2c3964cdb68848f43d5eb2d';
const sourcePath='supabase/repeatable/10102026_1624_candidate_notification_consumers_v1.sql';
const target='supabase/repeatable/includes/weekly_contact_source_native_start_v1.sqlinc';
const sha=value=>createHash('sha256').update(value).digest('hex');
const bytes=execFileSync('git',['cat-file','blob',`${commit}:${sourcePath}`]);
assert.equal(sha(bytes),'7203e1229a43f4545bcc85b3ce280ffadc2968dceb5f80aa8a7b54d01328978b');
const source=bytes.toString('utf8');
const name='public.weekly_source_message_dispatch_target_start_atomic_v1';
const begin=source.indexOf(`create or replace function ${name}(`);
const end=source.indexOf('$function$;',source.indexOf('as $function$',begin));
assert(begin>=0&&end>begin);
const baseline=source.slice(begin,end+'$function$;'.length);
let output=baseline;
function once(before,after){assert.equal(output.split(before).length,2,`START seam changed: ${before}`);output=output.replace(before,after);}
once('  v_reason text;',`  v_reason text;
  v_contact jsonb;
  v_agency uuid;
  v_environment text;
  v_next timestamptz;`);
once(`  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key
               where key not in ('dispatch_target_id','lease_token','worker_id')) then
    raise exception 'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID' using errcode='22023';
  end if;`, `  perform private.weekly_contact_assert_closed_v1(p_request,
    array['dispatch_target_id','lease_token','worker_id','agency_id','environment'],
    array['dispatch_target_id','lease_token','worker_id','agency_id','environment'],
    'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID');
  if jsonb_typeof(p_request->'worker_id') is distinct from 'string'
    or char_length(p_request->>'worker_id') not between 1 and 200
    or p_request->>'environment' is null or p_request->>'environment' not in('TEST','LIVE') then
    raise exception 'WEEKLY_SOURCE_TARGET_START_REQUEST_INVALID' using errcode='22023';end if;`);
once(`    v_target_id:=(p_request->>'dispatch_target_id')::uuid;
    v_lease_token:=(p_request->>'lease_token')::uuid;
    v_worker:=p_request->>'worker_id';`, `    v_target_id:=private.weekly_contact_uuid_v1(p_request->'dispatch_target_id');
    v_lease_token:=private.weekly_contact_uuid_v1(p_request->'lease_token');
    v_agency:=private.weekly_contact_uuid_v1(p_request->'agency_id');
    v_environment:=p_request->>'environment';
    v_worker:=p_request->>'worker_id';`);
once(`  select * into strict v_target from public.weekly_message_dispatch_targets
  where id=v_target_id for update;`, `  -- Read-only recovery never admits/resends or acquires a later lower-order
  -- lock. Exact old lease replay and any still-unknown native attempt return
  -- the original identity, even after a lease was reclaimed by another worker.
  perform private.weekly_contact_context_v1(v_agency,v_environment);
  select * into strict v_target from public.weekly_message_dispatch_targets where id=v_target_id;
  select * into strict v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id;
  if v_command.agency_id is distinct from v_agency or v_command.environment is distinct from v_environment then
    raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
  select * into v_attempt from public.weekly_message_target_attempts
  where dispatch_target_id=v_target.id and(completed_at_utc is null
    or(lease_token=v_lease_token and lease_owner=v_worker))
  order by attempt_number desc limit 1;
  if found then
    return jsonb_build_object('ok',true,'replay',true,'provider_attempt_id',v_attempt.id,
      'call_once',false,'reason','CHECK_ORIGINAL_ATTEMPT');end if;
  -- The common entry takes deployment -> PUSH policy -> sorted scopes ->
  -- required calendar row locks BEFORE the original target/command locks.
  -- It derives channel and memberships from these genuine native rows.
  v_contact:=public.weekly_contact_submission_start_v1(jsonb_build_object(
    'version','CONTACT_START_V1','owner_kind','WEEKLY_SOURCE_TARGET',
    'dispatch_target_id',v_target_id,'lease_token',v_lease_token,'worker_id',v_worker,
    'agency_id',v_agency,'environment',v_environment));
  if v_contact->'ok' is distinct from 'true'::jsonb then
    select * into strict v_target from public.weekly_message_dispatch_targets where id=v_target_id for update;
    select * into strict v_command from public.weekly_message_dispatch_commands where id=v_target.dispatch_command_id for update;
    if v_contact->>'reason' not in('NOT_STARTED_PAUSED','NOT_STARTED_QUIET','EXPIRED_TIMING','OBSOLETE')
      or v_contact->>'reason' is null or v_contact->>'dispatch_target_id' is distinct from v_target.id::text
      or v_contact->>'channel' is distinct from v_target.channel then
      raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
    if v_contact->>'reason'='NOT_STARTED_QUIET'
      or(v_contact->>'reason'='NOT_STARTED_PAUSED' and v_target.channel='PUSH') then
      v_next:=(v_contact->>'next_eligible_at_utc')::timestamptz;
      if v_next is null or not isfinite(v_next) then
        raise exception 'CONTACT_DISPATCH_SCOPE_INVALID' using errcode='42501';end if;
      update public.weekly_message_dispatch_targets set state='TRANSIENT_FAILURE',
        next_attempt_at_utc=v_next,terminal_at_utc=null,terminal_reason=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null where id=v_target.id;
    else
      update public.weekly_message_dispatch_targets set state='RETIRED',terminal_at_utc=clock_timestamp(),
        terminal_reason=v_contact->>'reason',next_attempt_at_utc=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null where id=v_target.id;
      if v_target.channel='EMAIL' then
        -- Retire the whole unsent frozen manager body/link, never filter its
        -- members under that same body. Original recheck/recomposition owns
        -- any later eligible-only render with unchanged logical allowances.
        update public.weekly_manager_review_batches set state='REVOKED'
          where message_render_id=v_command.message_render_id and state='ACTIVE';
        update public.weekly_manager_route_receipts r set state='REVOKED',revoked_at_utc=clock_timestamp()
          from public.weekly_manager_review_batches b
          where r.review_batch_id=b.id and b.message_render_id=v_command.message_render_id and r.state='ACTIVE';
        update public.weekly_manager_review_items m set response_state='OBSOLETE'
          from public.weekly_manager_review_batches b
          where m.review_batch_id=b.id and b.message_render_id=v_command.message_render_id and m.response_state='UNANSWERED';
      end if;
    end if;
    perform private.weekly_source_delivery_aggregate_command_v1(v_command.id);
    return v_contact;
  end if;
  select * into strict v_target from public.weekly_message_dispatch_targets
  where id=v_target_id for update;`);
once(`      'ok',true,'replay',true,'provider_attempt_id',v_attempt.id,
      'provider_idempotency_key',v_attempt.provider_idempotency_key`, `      'ok',true,'replay',true,'provider_attempt_id',v_attempt.id,
      'call_once',false,'reason','CHECK_ORIGINAL_ATTEMPT'`);
once(`    v_worker,v_lease_token,pg_catalog.transaction_timestamp()
  ) returning * into v_attempt;`, `    v_worker,v_lease_token,(v_contact#>>'{admission,admitted_at_utc}')::timestamptz
  ) returning * into v_attempt;`);
once(`  return pg_catalog.jsonb_build_object(
    'ok',true,'replay',false,'provider_attempt_id',v_attempt.id,
    'provider_idempotency_key',v_attempt.provider_idempotency_key,
    'submission_started_at_utc',v_attempt.submission_started_at_utc
  );`, `  return private.weekly_contact_source_permit_v1(v_target.id,v_attempt.id,v_contact->'admission');`);
assert.equal((output.match(/create or replace function /g)||[]).length,1);
assert(output.indexOf('v_contact:=public.weekly_contact_submission_start_v1')<output.indexOf('where id=v_target_id for update;'));
const expected=`-- Exact current native START amended before its original native locks.
-- Baseline ${commit}:${sourcePath}
-- Baseline complete-definition SHA256 ${sha(baseline)}
${output}

alter function ${name}(jsonb) owner to postgres;
revoke all on function ${name}(jsonb) from public,anon,authenticated,service_role;
grant execute on function ${name}(jsonb) to service_role;
`;
assert(process.argv.slice(2).every(v=>v==='--write'),'unknown composition option');
if(fs.existsSync(target))assert.equal(fs.readFileSync(target,'utf8'),expected,'preserve unexpected native START successor');
if(process.argv.includes('--write')&&!fs.existsSync(target))fs.writeFileSync(target,expected);
console.log(JSON.stringify({target,sha256:sha(expected),bytes:Buffer.byteLength(expected),baseline_sha256:sha(baseline),
  functions:1,source_only:true,native:'REQUIRED_NOT_RUN',installed:false}));
