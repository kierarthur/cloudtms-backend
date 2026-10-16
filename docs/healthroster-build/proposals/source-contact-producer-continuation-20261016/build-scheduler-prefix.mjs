// Finite composition of the complete current scheduler. Selection uses its
// actual ordered batch queries, before any business row locks or mutations.
export function schedulerPrefix(source){
 const once=(text,old,next)=>{if(text.split(old).length!==2)throw Error('Scheduler exact anchor: '+old.slice(0,100));return text.replace(old,next);};
 const query=(name)=>{const start=source.indexOf(`  for ${name} in\n`);if(start<0)throw Error(name);
  const end=source.indexOf('\n  loop',start);if(end<0)throw Error(name+' loop');
  return source.slice(start+`  for ${name} in\n`.length,end).replace(/\n    for update skip locked\s*$/,'').trim();};
 const candidates=query('v_generation'),routes=query('v_route'),followups=query('v_followup');
 const snapshot=`-- Exact bounded scheduler selection; no native row lock, queue or clock is allocated here.
create or replace function private.weekly_contact_source_scheduler_selection_v1(v_now timestamptz,v_limit integer)
returns jsonb language plpgsql stable security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare candidates jsonb;routes jsonb;followups jsonb;overdue jsonb;collapse_ids jsonb;
begin
 select coalesce(jsonb_agg(to_jsonb(native)order by native.reminder_due_at_utc,native.id),'[]')into candidates from(
${candidates}
 )native;
 select coalesce(jsonb_agg(to_jsonb(native)order by native.first_due,native.route_id,native.event_kind),'[]')into routes from(
${routes}
 )native;
 select coalesce(jsonb_agg(to_jsonb(native)order by(native.state->>'due_at_utc')::timestamptz nulls last,native.id),'[]')into followups from(
${followups}
 )native;
 -- The old global overdue UPDATE has no cursor. Its same factual transition
 -- now advances a deadline/id ordered native batch; later calls select the
 -- actual remaining ACTIVE rows without a lifetime ceiling or second queue.
 select coalesce(jsonb_agg(to_jsonb(native)order by native.deadline_at_utc,native.id),'[]')into overdue from(
  select submission.* from public.weekly_timesheet_submission_requests submission
   where submission.state in('ACTIVE','PARTLY_SUBMITTED')and submission.deadline_at_utc<=v_now
   order by submission.deadline_at_utc,submission.id limit v_limit
 )native;
 -- Include an initial body whose later reminder is already real OR is exactly
 -- selected for construction in this same scheduler transaction.
 select coalesce(jsonb_agg(to_jsonb(native)order by native.id),'[]')into collapse_ids from(
  select earlier.* from public.weekly_message_intents earlier where earlier.audience_kind='CANDIDATE'
   and earlier.state in('DUE','RENDERED')and earlier.tranche_kind in('CANDIDATE_INITIAL','TIMESHEET_SUBMISSION_INITIAL')
   and earlier.due_at_utc<=v_now and(
    exists(select 1 from public.weekly_message_intents later where later.candidate_generation_id=earlier.candidate_generation_id
     and later.tranche_kind in('CANDIDATE_REMINDER_6H','TIMESHEET_SUBMISSION_REMINDER_6H')and later.due_at_utc<=v_now
     and later.state in('DUE','RENDERED','DISPATCHED'))
    or exists(select 1 from jsonb_array_elements(candidates)selected where(selected->>'id')::uuid=earlier.candidate_generation_id))
   order by earlier.id limit v_limit
 )native;
 return jsonb_build_object('version','CONTACT_NATIVE_SCHEDULER_SELECTION_V1','candidate_generations',candidates,'manager_routes',routes,
  'followups',followups,'overdue_requests',overdue,'collapse_intents',collapse_ids);
end;
$function$;
`;
 let output=once(source,'  v_actionable integer;','  v_actionable integer;\n  v_selected jsonb;\n  v_collapse record;');
 output=once(output,"  for v_generation in\n",`  -- Freeze actual native batch IDs, then deployment and complete connected
  -- scopes in sorted order, before the original first FOR UPDATE or overdue UPDATE.
  v_selected:=private.weekly_contact_source_scheduler_selection_v1(v_now,v_limit);
  perform private.weekly_contact_source_scheduler_selection_lock_v1(v_selected);

  for v_generation in
`);
 output=once(output,"    where generation.state='ACTIVE' and generation.reminder_due_at_utc<=v_now",`    where generation.id in(select(value->>'id')::uuid from jsonb_array_elements(v_selected->'candidate_generations'))
      and generation.state='ACTIVE' and generation.reminder_due_at_utc<=v_now`);
 output=once(output,"  where submission.state in ('ACTIVE','PARTLY_SUBMITTED') and submission.deadline_at_utc<=v_now;",`  where submission.id in(select(value->>'id')::uuid from jsonb_array_elements(v_selected->'overdue_requests'))
    and submission.state in ('ACTIVE','PARTLY_SUBMITTED') and submission.deadline_at_utc<=v_now;`);
 output=once(output,'    group by route.id,generation.id,event.event_kind',`    where exists(select 1 from jsonb_array_elements(v_selected->'manager_routes')selected
      where(selected->>'route_id')::uuid=route.id and(selected->>'generation_id')::uuid=generation.id
        and selected->>'event_kind'=event.event_kind)
    group by route.id,generation.id,event.event_kind`);
 output=once(output,'  perform private.weekly_contact_source_candidate_due_collapse_v1(v_now,v_limit);',`  for v_collapse in select earlier.id from public.weekly_message_intents earlier
    where earlier.id in(select(value->>'id')::uuid from jsonb_array_elements(v_selected->'collapse_intents'))
      and earlier.audience_kind='CANDIDATE'and earlier.state in('DUE','RENDERED')
      and earlier.tranche_kind in('CANDIDATE_INITIAL','TIMESHEET_SUBMISSION_INITIAL')and earlier.due_at_utc<=v_now
      and exists(select 1 from public.weekly_message_intents later where later.candidate_generation_id=earlier.candidate_generation_id
        and later.tranche_kind in('CANDIDATE_REMINDER_6H','TIMESHEET_SUBMISSION_REMINDER_6H')and later.due_at_utc<=v_now
        and later.state in('DUE','RENDERED','DISPATCHED'))order by earlier.id
  loop perform private.weekly_contact_source_retire_unsubmitted_v1(v_collapse.id);end loop;`);
 output=once(output,"      where incident.state='OPEN' and incident.manager_potential_state='AVAILABLE'",`      where incident.id in(select(value->>'id')::uuid from jsonb_array_elements(v_selected->'followups'))
        and incident.state='OPEN' and incident.manager_potential_state='AVAILABLE'`);
 return{output,snapshot};
}

export function groupedSchedulerFollowups(source){
 const once=(text,old,next)=>{if(text.split(old).length!==2)throw Error('Grouped scheduler exact anchor: '+old.slice(0,100));return text.replace(old,next);};
 let output=once(source,'  v_followup_held integer:=0;',`  v_followup_held integer:=0;
  v_followup_member jsonb;
  v_followup_groups jsonb:='[]';
  v_followup_group record;`);
 output=once(output,'        v_followup_result:=private.weekly_contact_source_manager_followup_enqueue_v1(v_followup.id,clock_timestamp());',
`        v_followup_member:=private.weekly_contact_manual_member_facts_v1(v_followup.id,'MANAGER',clock_timestamp());
        if v_followup_member->>'manager_address'is null then
          v_followup_result:=jsonb_build_object('version','CONTACT_SOURCE_FOLLOWUP_ENQUEUE_V1','incident_id',v_followup.id,
            'message_intent_id',null,'disposition',coalesce(v_followup_member->>'exclusion_reason','MANAGER_CONTACT_DISABLED'));
        else
          v_followup_groups:=v_followup_groups||jsonb_build_array(jsonb_build_object('incident_id',v_followup.id,
            'source_cycle_id',v_followup_member->'source_cycle_id','manager_address',v_followup_member->'manager_address',
            'candidate_id',v_followup_member->'candidate_id','client_id',v_followup_member->'client_id',
            'due_at_utc',v_followup.state->'due_at_utc'));
          continue;
        end if;`);
 output=once(output,"  return pg_catalog.jsonb_build_object(\n    'ok',true,'now_utc',v_now",`  -- Partition the actual bounded selected due set by its genuine current
  -- cycle/recipient. Native bodies retain500rows/100candidates/100clients;
  -- no member is truncated and every member has its own accepted-step witness.
  for v_followup_group in
    with ordered as(
      select member.value,(row_number()over(partition by member.value->>'source_cycle_id',member.value->>'manager_address'
        order by(member.value->>'due_at_utc')::timestamptz,(member.value->>'incident_id')::uuid)-1)/500 partition_number,
        (dense_rank()over(partition by member.value->>'source_cycle_id',member.value->>'manager_address'
          order by(member.value->>'candidate_id')::uuid)-1)/100 candidate_partition,
        (dense_rank()over(partition by member.value->>'source_cycle_id',member.value->>'manager_address'
          order by(member.value->>'client_id')::uuid)-1)/100 client_partition
      from jsonb_array_elements(v_followup_groups)member(value)
    )select array_agg((value->>'incident_id')::uuid order by(value->>'incident_id')::uuid)incident_ids
      from ordered group by value->>'source_cycle_id',value->>'manager_address',partition_number,candidate_partition,client_partition
      order by value->>'source_cycle_id',value->>'manager_address',partition_number,candidate_partition,client_partition
  loop
    begin
      v_followup_result:=private.weekly_contact_source_manager_followup_group_enqueue_v1(v_followup_group.incident_ids,clock_timestamp());
      v_followup_results:=v_followup_results||(v_followup_result->'results');
    exception when others then
      v_followup_held:=v_followup_held+cardinality(v_followup_group.incident_ids);
      select v_followup_results||jsonb_agg(jsonb_build_object('incident_id',id,'disposition','HELD_DEPENDENCY',
        'code','CONTACT_SOURCE_FOLLOWUP_ITEM_HELD')order by id)into v_followup_results from unnest(v_followup_group.incident_ids)id;
    end;
  end loop;
  return pg_catalog.jsonb_build_object(
    'ok',true,'now_utc',v_now`);
 return output;
}
