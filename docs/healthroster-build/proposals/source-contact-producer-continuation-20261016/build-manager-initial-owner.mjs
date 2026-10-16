import{execFileSync}from'node:child_process';
import{readFileSync,writeFileSync}from'node:fs';
import{createHash}from'node:crypto';
import path from'node:path';
import{fileURLToPath}from'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',file='supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql';
const sha=value=>createHash('sha256').update(value).digest('hex');
const text=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
const start=text.indexOf('create or replace function public.weekly_source_query_send_manager_now_atomic_v1('),end=text.indexOf('$function$;',text.indexOf('as $function$',start));
if(start<0||end<0)throw Error('Complete genuine initial owner missing');
const parent=text.slice(start,end+12).trimEnd()+'\n';let output=parent;const deltas=[];
function once(from,to,reason){if(output.split(from).length!==2)throw Error('Initial owner exact anchor: '+from.slice(0,100));
 output=output.replace(from,to);deltas.push({from,to,reason});}
once('  v_route public.weekly_manager_recipient_routes%rowtype;',`  v_route public.weekly_manager_recipient_routes%rowtype;
  v_initial_ids uuid[]:='{}';
  v_contact_members jsonb:='[]';
  v_contact_member jsonb;
  v_contact_fact jsonb;
  v_followup jsonb;
  v_contact_at timestamptz;
  v_contact_available timestamptz;
  v_send_available timestamptz;
  v_worker_intent record;
  v_had_candidate_generation boolean;`,'Typed server-only selection/provenance operands; public four-key request/signature unchanged.');
once('  for v_incident in\n',`  -- Actual selected cohort/retained-route union first; no browser member,
  -- recipient, ordinal or clock acquires authority here.
  if cardinality(v_requested)>500 then raise exception 'WEEKLY_SOURCE_MANAGER_DIGEST_CAPACITY_EXCEEDED'using errcode='54000';end if;
  perform private.weekly_contact_source_manager_selection_lock_v1(v_requested,'[]');
  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  perform private.weekly_source_office_authority_v1(v_actor,'SEND_MANAGER',(v_guard->>'source_group_id')::uuid,null,
    (v_guard->>'finalisation_week_ending')::date);
  v_contact_at:=clock_timestamp();
  for v_selected in select incident.id from public.weekly_discrepancy_incidents incident where incident.id=any(v_requested)order by incident.id loop
    v_contact_fact:=private.weekly_contact_manual_member_facts_v1(v_selected.id,'MANAGER',v_contact_at);
    if v_contact_fact->'eligible'is distinct from 'true'::jsonb and v_contact_fact->>'exclusion_reason'<>'MANUAL_ENQUIRY_COOLDOWN'
      or v_contact_fact->'cutoff'->'routine'->'in_scope'is distinct from 'true'::jsonb then
      raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_STALE'using errcode='40001';end if;
    if v_contact_fact->>'cooldown_until_utc'is not null then
      v_contact_available:=greatest(v_contact_available,(v_contact_fact->>'cooldown_until_utc')::timestamptz);end if;
    if v_contact_fact->'last_accepted_send'='null'::jsonb then
      -- Preserve the genuine public initial owner's signed-work requirement.
      -- Other independently reviewed direct pending producers keep their ABI.
      if not exists(select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        where incident.id=v_selected.id and comparison.candidate_timesheet_id is not null)then
        raise exception 'WEEKLY_SOURCE_MANAGER_REQUIRES_TIMESHEET'using errcode='55000';end if;
      v_initial_ids:=array_append(v_initial_ids,v_selected.id);v_followup:=null;
    else
      v_followup:=private.weekly_contact_source_manager_followup_state_v1(v_selected.id,v_contact_at);
      if v_followup->>'reason'is distinct from 'SCHEDULED'or v_followup->>'mode'is distinct from 'MANUAL'
        or(v_followup->>'due_at_utc')::timestamptz>v_contact_at then
        raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_STALE'using errcode='40001';end if;
    end if;
    v_contact_members:=v_contact_members||jsonb_build_array(v_contact_fact||jsonb_build_object('followup_state',v_followup,
      'routine_step',v_followup->'routine_sequence','basis_accepted_membership_id',v_followup->'basis_accepted_membership_id'));
  end loop;

  for v_incident in
`,'Discover/revalidate actual complete selected union once; classify original signed INITIAL versus actual due MANUAL before native writes.');
once('    if v_comparison.candidate_timesheet_id is null then',`    select value into strict v_contact_member from jsonb_array_elements(v_contact_members)where value->>'issue_id'=v_incident.id::text;
    v_contact_fact:=private.weekly_contact_manual_member_facts_v1(v_incident.id,'MANAGER',v_contact_at);
    if(v_contact_fact-array['recipient_route_id','cooldown_remaining_seconds'])is distinct from(
      v_contact_member-array['recipient_route_id','cooldown_remaining_seconds','followup_state','routine_step','basis_accepted_membership_id'])then
      raise exception 'WEEKLY_SOURCE_MANAGER_SEND_SELECTION_STALE'using errcode='40001';end if;
    if v_incident.id=any(v_initial_ids)and v_comparison.candidate_timesheet_id is null then`,'Exact original signed INITIAL rule; real accepted due-MANUAL is classified separately, never invented unsigned INITIAL.');
once("    if not ((v_context->>'cohort_id')::uuid=any(v_selected_cohort_ids)) then",`    if v_incident.id=any(v_initial_ids)and not ((v_context->>'cohort_id')::uuid=any(v_selected_cohort_ids)) then`,
 'Original cancellation cohort set contains only genuinely eligible selected initial members.');
once('    if not (v_key=any(v_seen_keys)) then','    if v_incident.id=any(v_initial_ids)and not (v_key=any(v_seen_keys)) then',
 'Initial native manager-direct activation only for genuine initial members.');
const candidateRead=`    select * into v_candidate_generation
    from public.weekly_candidate_outreach_generations
    where candidate_cohort_id=v_selected.id and state='ACTIVE' and request_kind='CHECK_HOURS'
    for update;
    if found then`;
const cohortLoop=`  loop
    insert into public.weekly_route_activations(
      source_cycle_id,candidate_id,client_id,audience_route,route_mode,
      activated_by_user_id,activated_at_utc,updated_at_utc
    ) values (
      v_cycle_id,v_selected.candidate_id,v_selected.client_id,'CANDIDATE','MANAGER_DIRECT',`;
once(cohortLoop,`  loop
    select * into v_candidate_generation from public.weekly_candidate_outreach_generations
    where candidate_cohort_id=v_selected.id and state='ACTIVE'and request_kind='CHECK_HOURS'for update;
    v_had_candidate_generation:=found;
    if v_had_candidate_generation and exists(select 1 from public.weekly_message_intents intent
      join public.weekly_message_renders render on render.message_intent_id=intent.id
      where intent.candidate_generation_id=v_candidate_generation.id and private.weekly_contact_source_render_attempted_v1(render.id))then
      -- An accepted, started, uncertain or original retry owner keeps its real
      -- question/current pointer/route/body. Manager checking remains separate.
      continue;
    end if;
    if v_had_candidate_generation and exists(select 1 from public.weekly_candidate_outreach_memberships member
      where member.candidate_generation_id=v_candidate_generation.id and member.state='ACTIONABLE'
        and not(member.incident_id=any(v_initial_ids)))then
      -- Only the actual selected initial memberships retire. The original
      -- remaining question keeps its ID/current pointer/start/+6/deadline.
      update public.weekly_candidate_outreach_memberships set state='SUPERSEDED'
        where candidate_generation_id=v_candidate_generation.id and incident_id=any(v_initial_ids)and state='ACTIONABLE';
      update public.weekly_discrepancy_incidents set candidate_action_state='NOT_REQUIRED'
        where source_cycle_id=v_cycle_id and candidate_id=v_selected.candidate_id and client_id=v_selected.client_id
          and id=any(v_initial_ids)and candidate_action_state in('NOT_ASKED','ASKED');
      for v_worker_intent in select id from public.weekly_message_intents where candidate_generation_id=v_candidate_generation.id
        and state in('DUE','RENDERED')order by id loop
        perform private.weekly_contact_source_recompose_intent_v1(v_worker_intent.id,null);end loop;
      continue;
    end if;
    insert into public.weekly_route_activations(
      source_cycle_id,candidate_id,client_id,audience_route,route_mode,
      activated_by_user_id,activated_at_utc,updated_at_utc
    ) values (
      v_cycle_id,v_selected.candidate_id,v_selected.client_id,'CANDIDATE','MANAGER_DIRECT',`,
 'Same native initial cancellation body, with exact actual original-attempt fence before route/pointer mutation.');
once(candidateRead,'    if v_had_candidate_generation then','Reuse the exact already-locked genuine CHECK generation; no second lookup/latest inference.');
once("      and id=any(v_requested) and candidate_action_state in ('NOT_ASKED','ASKED');", "      and id=any(v_initial_ids) and candidate_action_state in ('NOT_ASKED','ASKED');",
 'Initial cancellation cannot retire accepted followup members or unrelated selected purposes.');
once(`  if v_route.manager_send_available_at_utc is not null
     and pg_catalog.transaction_timestamp()<v_route.manager_send_available_at_utc then`,
`  if greatest(v_route.manager_send_available_at_utc,v_contact_available)>clock_timestamp()then`,
 'Same real cooldown refusal point; include independently actual accepted-contact manual cooldown.');
once("        'available_at_utc',v_route.manager_send_available_at_utc", "        'available_at_utc',greatest(v_route.manager_send_available_at_utc,v_contact_available)",
 'Refusal detail is the actual current limiting cooldown; no UI clock or guessed default.');
once('  if not exists(\n    select 1 from public.weekly_discrepancy_incidents incident\n    where incident.id=any(v_requested)',
 '  if cardinality(v_initial_ids)>0 and not exists(\n    select 1 from public.weekly_discrepancy_incidents incident\n    where incident.id=any(v_initial_ids)',
 'Original free-initial-row guard stays exact for INITIAL; genuine accepted due MANUAL is not mistaken for already-sent initial.');
const generationStart=output.indexOf('  v_generation:=private.weekly_source_query_manager_generation_v1('),generationEnd=output.indexOf('  update public.weekly_manager_recipient_routes\n  set manager_send_available_at_utc=',generationStart);
if(generationStart<0||generationEnd<0)throw Error('Exact original generation/intent seam missing');
const oldGeneration=output.slice(generationStart,generationEnd);
once(oldGeneration,`  -- One actual retained-route native generation and one captured body. There
  -- is no intermediate intent/render to enqueue, retire, resend or charge.
  v_generation:=private.weekly_contact_source_manager_selection_capture_v1(v_actor,v_cycle_id,v_publication_id,v_contact_members);
  v_generation_id:=(v_generation->>'recipient_generation_id')::uuid;
  v_intent_id:=(v_generation->>'message_intent_id')::uuid;
`, 'Factor only actual generation/body construction into one typed private owner; preserve actual due/start lineage and each member provenance.');
const clockOperand='pg_catalog.transaction_timestamp()+v_settings.manager_manual_send_cooldown';
if(output.split(clockOperand).length!==4)throw Error('Exact original three cooldown write/audit/reply operands');
const clockBefore=output;
output=output.split(clockOperand).join('v_send_available');
output=output.replace('  update public.weekly_manager_recipient_routes\n  set manager_send_available_at_utc=v_send_available',
 '  v_send_available:=clock_timestamp()+v_settings.manager_manual_send_cooldown;\n  update public.weekly_manager_recipient_routes\n  set manager_send_available_at_utc=v_send_available');
deltas.push({from:clockBefore,to:output,reason:'One actual current server cooldown sample at original native write point, echoed exactly by the retained audit/result; no second send/clock.'});
if(output.includes('pg_catalog.transaction_timestamp()')){
 const before=output;output=output.split('pg_catalog.transaction_timestamp()').join('v_contact_at');
 deltas.push({from:before,to:output,reason:'Future initial activation/update observations use the one genuine server eligibility sample after deployment/scope waits; existing original starts stay coalesced unchanged.'});}
writeFileSync(path.join(base,'forward/51_native_initial_manager_owner.sqlinc'),output);
writeFileSync(path.join(base,'MANAGER_INITIAL_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_NATIVE_INITIAL_OWNER_COMPOSITION_V1',status:'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN',
 input:{commit,path:file,sha256_normalized_lf:sha(text),complete_function_sha256:sha(parent)},
 signature:'public.weekly_source_query_send_manager_now_atomic_v1(jsonb)',public_request_keys:['actor_user_id','source_cycle_id','projection_publication_id','incident_ids'],
 output:{path:'forward/51_native_initial_manager_owner.sqlinc',sha256:sha(output)},deltas,
 retained:'Every complete genuine parent byte outside the exact ordered finite seams; existing route cooldown write/audit/result shape stay at original native point.',
 database_actions:'NONE'},null,2)+'\n');
console.log(JSON.stringify({native_initial_owner_sha256:sha(output),finite_deltas:deltas.length,status:'SOURCE_ONLY_NATIVE_NOT_RUN'}));
