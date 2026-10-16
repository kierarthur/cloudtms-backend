create or replace function private.weekly_contact_manual_manager_generation_v1(p_members jsonb,p_actor uuid,p_at timestamptz)
returns uuid language plpgsql volatile security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare member jsonb;context jsonb;route_id uuid;generation public.weekly_manager_recipient_generations%rowtype;
 route public.weekly_manager_recipient_routes%rowtype;native_intent record;activation uuid;cohort uuid;generation_number integer;
 membership jsonb;ordinal integer:=0;i public.weekly_discrepancy_incidents%rowtype;c public.weekly_issue_comparison_revisions%rowtype;
 w public.weekly_work_events%rowtype;old_generation_id uuid;due_event public.weekly_manager_cohort_due_events%rowtype;
 due_hash bytea;due_source jsonb;new_due_id uuid;selected_ids uuid[];
begin
 select array_agg((value->>'issue_id')::uuid order by(value->>'issue_id')::uuid) into selected_ids from jsonb_array_elements(p_members);
 -- The actual complete retained route work is locked before cohort ensure,
 -- activation or generation rows can be written. Callers may already hold it.
 perform private.weekly_contact_source_manager_selection_lock_v1(selected_ids,'[]');
 for member in select value from jsonb_array_elements(p_members) order by value->>'candidate_id',value->>'client_id',value->>'issue_id' loop
  select * into strict i from public.weekly_discrepancy_incidents where id=(member->>'issue_id')::uuid;
  select * into strict c from public.weekly_issue_comparison_revisions where id=i.current_comparison_revision_id;
  select * into strict w from public.weekly_work_events where id=i.work_event_id;
  context:=private.weekly_source_query_cohort_ensure_v1(i.source_cycle_id,i.candidate_id,i.client_id,c.contract_id,w.work_date);
  if context->>'manager_route_id' is null or context->>'manager_recipient' is distinct from member->>'manager_address'
   or(route_id is not null and route_id<>(context->>'manager_route_id')::uuid) then
   raise exception 'CONTACT_MANUAL_ENQUIRY_REVIEW_STALE' using errcode='40001';end if;
  route_id:=(context->>'manager_route_id')::uuid;
  insert into public.weekly_route_activations(source_cycle_id,candidate_id,client_id,audience_route,route_mode,
   activated_by_user_id,activated_at_utc,updated_at_utc)
  values(i.source_cycle_id,i.candidate_id,i.client_id,'MANAGER','MANAGER_DIRECT',p_actor,p_at,p_at)
   on conflict(source_cycle_id,candidate_id,client_id,audience_route) do nothing;
 end loop;
 select * into strict route from public.weekly_manager_recipient_routes where id=route_id for update;
 select * into generation from public.weekly_manager_recipient_generations where id=route.current_generation_id and state='ACTIVE';
 if generation.id is not null and not exists(select 1 from jsonb_array_elements(p_members)m where not exists(
  select 1 from public.weekly_manager_recipient_memberships native where native.recipient_generation_id=generation.id
   and native.incident_id=(m->>'issue_id')::uuid and native.comparison_revision_id=(m->>'comparison_revision_id')::uuid)) then
  return generation.id;
 end if;
 if generation.id is not null then
  old_generation_id:=generation.id;
  if exists(select 1 from public.weekly_message_intents intent join public.weekly_message_renders render on render.message_intent_id=intent.id
   join public.weekly_message_dispatch_commands command on command.message_render_id=render.id
   where intent.recipient_generation_id=generation.id and command.state in('READY','LEASED','FAILED')
    and private.weekly_contact_source_render_attempted_v1(render.id) and exists(select 1 from public.weekly_message_dispatch_targets target
     where target.dispatch_command_id=command.id and target.state in('READY','LEASED','TRANSIENT_FAILURE'))) then
   raise exception 'CONTACT_MANUAL_ENQUIRY_REVIEW_STALE' using errcode='40001';end if;
  update public.weekly_manager_recipient_routes set current_generation_id=null where id=route.id;
  update public.weekly_manager_recipient_generations set state='SUPERSEDED',superseded_at_utc=p_at where id=generation.id;
 end if;
 select coalesce(max(g.generation_number),0)+1 into generation_number from public.weekly_manager_recipient_generations g where recipient_route_id=route.id;
 -- Retain the complete actual prior route membership, including paused and
 -- previously owned rows, then overlay only the selected current comparisons.
 -- This is not the send body; exact eligible captures remain independently
 -- bounded. No unrelated member, original due event or clock is discarded.
 select jsonb_agg(to_jsonb(retained)-'priority' order by retained.incident_id) into membership from(
  select distinct on(combined.incident_id) combined.* from(
   select 0 priority,activation_row.id activation_id,cohort_row.id candidate_cohort_id,cohort_row.current_generation_id candidate_generation_id,
    (m->>'issue_id')::uuid incident_id,(m->>'comparison_revision_id')::uuid comparison_revision_id,'MANAGER_ACTIONABLE'::text membership_role
   from jsonb_array_elements(p_members)m join public.weekly_route_activations activation_row
    on activation_row.source_cycle_id=route.source_cycle_id and activation_row.candidate_id=(m->>'candidate_id')::uuid
     and activation_row.client_id=(m->>'client_id')::uuid and activation_row.audience_route='MANAGER'
   join public.weekly_candidate_cohorts cohort_row on cohort_row.source_cycle_id=route.source_cycle_id
    and cohort_row.candidate_id=(m->>'candidate_id')::uuid and cohort_row.client_id=(m->>'client_id')::uuid
    and cohort_row.manager_recipient_route_key=route.normalised_recipient_hash
   union all
   select 1,native.route_activation_id,native.candidate_cohort_id,native.candidate_outreach_generation_id,native.incident_id,
    native.comparison_revision_id,native.membership_role from public.weekly_manager_recipient_memberships native
    where native.recipient_generation_id=old_generation_id
  )combined order by combined.incident_id,combined.priority
 )retained;
 insert into public.weekly_manager_recipient_generations(recipient_route_id,generation_number,trigger_kind,state,membership_hash,created_at_utc)
 values(route.id,generation_number,case when p_actor is null then 'CONTACT_COMPOSITION' else 'MANUAL_RESEND' end,
  'ACTIVE',private.weekly_source_sha256_jsonb_v1('CONTACT_MANUAL_MANAGER_GENERATION_V1',membership),clock_timestamp())
 returning * into generation;
 for member in select value from jsonb_array_elements(membership) order by value->>'incident_id' loop
  ordinal:=ordinal+1;
  insert into public.weekly_manager_recipient_memberships(recipient_generation_id,route_activation_id,candidate_outreach_generation_id,
   candidate_cohort_id,incident_id,comparison_revision_id,membership_role,ordinal,created_at_utc)
  values(generation.id,(member->>'activation_id')::uuid,(member->>'candidate_generation_id')::uuid,(member->>'candidate_cohort_id')::uuid,
   (member->>'incident_id')::uuid,(member->>'comparison_revision_id')::uuid,member->>'membership_role',ordinal,clock_timestamp());
 end loop;
 update public.weekly_manager_recipient_routes set current_generation_id=generation.id where id=route.id;
 for due_event in select * from public.weekly_manager_cohort_due_events where recipient_generation_id=old_generation_id order by id loop
  due_source:=jsonb_build_object('original_event_id',due_event.id,'original_trigger_hash',encode(due_event.trigger_hash,'hex'),
   'recipient_generation_id',generation.id,'candidate_cohort_id',due_event.candidate_cohort_id,'event_kind',due_event.event_kind,
   'cohort_started_at_utc',to_char(due_event.cohort_started_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'due_at_utc',to_char(due_event.due_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'requested_week_membership_id',due_event.requested_week_membership_id);
  due_hash:=private.weekly_source_sha256_jsonb_v1('CONTACT_SOURCE_RETAINED_DUE_EVENT_V1',due_source);
  insert into public.weekly_manager_cohort_due_events(recipient_generation_id,candidate_cohort_id,event_kind,cohort_started_at_utc,
   due_at_utc,state,trigger_hash,requested_week_membership_id,created_at_utc)
  values(generation.id,due_event.candidate_cohort_id,due_event.event_kind,due_event.cohort_started_at_utc,due_event.due_at_utc,
   due_event.state,due_hash,due_event.requested_week_membership_id,clock_timestamp()) returning id into new_due_id;
  insert into public.weekly_contact_source_due_event_rebases(original_event_id,recipient_generation_id,rebased_event_id,source_sha256,source_json,recorded_at_utc)
   values(due_event.id,generation.id,new_due_id,due_hash,due_source,clock_timestamp());
 end loop;
 -- Immediate exact rebase of every actual wholly-unsubmitted old body. An
 -- accepted/started/uncertain owner stays unchanged and is never auto-retried.
 for native_intent in select intent.id from public.weekly_message_intents intent where intent.recipient_generation_id=old_generation_id
  and intent.state in('DUE','RENDERED') and not exists(select 1 from public.weekly_message_renders render
   where render.message_intent_id=intent.id and private.weekly_contact_source_render_attempted_v1(render.id)) order by intent.id loop
  perform private.weekly_contact_source_recompose_intent_v1(native_intent.id,generation.id);
 end loop;
 return generation.id;
end;
$function$;