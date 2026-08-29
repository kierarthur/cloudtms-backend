begin;

create or replace function public.candidate_daily_system_policy_activate_ready_v1(
  p_internal_context jsonb,
  p_candidate_source_hmacs jsonb default '[]'::jsonb,
  p_projection_outbox_ids jsonb default '[]'::jsonb,
  p_correlation_id text default null
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_environment text;
  v_candidate_id uuid;
  v_candidate public.candidates%rowtype;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_entitlement boolean:=false;
  v_global_enabled boolean:=false;
  v_source_primary_count integer:=0;
  v_source_group_count integer:=0;
  v_day_count integer:=0;
  v_blocking_count integer:=0;
  v_deferred_count integer:=0;
  v_transition jsonb;
  v_transition_item jsonb;
  v_evidence text;
  v_outcomes jsonb:='[]'::jsonb;
  v_status text;
begin
  v_context:=private._candidate_daily_context_v1(
    p_internal_context,'SIGNED_SYSTEM_SYNC',false
  );
  v_environment:=v_context->>'environment';
  if pg_catalog.jsonb_typeof(pg_catalog.coalesce(p_candidate_source_hmacs,'[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(pg_catalog.coalesce(p_projection_outbox_ids,'[]'::jsonb))<>'array'
     or pg_catalog.jsonb_array_length(pg_catalog.coalesce(p_candidate_source_hmacs,'[]'::jsonb))>100
     or pg_catalog.jsonb_array_length(pg_catalog.coalesce(p_projection_outbox_ids,'[]'::jsonb))>100
     or (pg_catalog.jsonb_array_length(pg_catalog.coalesce(p_candidate_source_hmacs,'[]'::jsonb))=0
       and pg_catalog.jsonb_array_length(pg_catalog.coalesce(p_projection_outbox_ids,'[]'::jsonb))=0)
     or pg_catalog.coalesce(p_correlation_id,'')!~'^[0-7][0-9A-HJKMNP-TV-Z]{25}$'
     or exists(select 1 from pg_catalog.jsonb_array_elements_text(
       pg_catalog.coalesce(p_candidate_source_hmacs,'[]'::jsonb)
     ) h(value) where h.value!~'^[a-f0-9]{64}$')
     or exists(select 1 from pg_catalog.jsonb_array_elements_text(
       pg_catalog.coalesce(p_projection_outbox_ids,'[]'::jsonb)
     ) i(value) where i.value!~'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
  then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;

  select pg_catalog.coalesce(
    s.candidate_app_feature_flags_json->'candidate_daily_enabled'='true'::jsonb,false
  ) into v_global_enabled from public.settings_defaults s where s.id=1;

  for v_candidate_id in
    select distinct candidates.candidate_id
    from (
      select private._candidate_daily_source_candidate_v1(v_environment,h.value) candidate_id
      from pg_catalog.jsonb_array_elements_text(
        pg_catalog.coalesce(p_candidate_source_hmacs,'[]'::jsonb)
      ) h(value)
      union all
      select o.candidate_id
      from pg_catalog.jsonb_array_elements_text(
        pg_catalog.coalesce(p_projection_outbox_ids,'[]'::jsonb)
      ) i(value)
      join public.candidate_daily_sheet_projection_outbox o
        on o.outbox_id=i.value::uuid and o.environment=v_environment
    ) candidates order by candidates.candidate_id
  loop
    v_status:='NOT_READY';
    begin
      select * into v_candidate from public.candidates c
      where c.id=v_candidate_id and c.active is true;
      select * into v_scope from private.candidate_daily_authority_scopes s
      where s.environment=v_environment and s.candidate_id=v_candidate_id;
      select pg_catalog.coalesce(e.enabled,false) into v_entitlement
      from private.candidate_daily_entitlements e
      where e.environment=v_environment and e.candidate_id=v_candidate_id;
      v_entitlement:=pg_catalog.coalesce(v_entitlement,false);
      if v_entitlement and v_scope.authority_mode='SUPABASE_PRIMARY' then
        v_status:='ALREADY_ENABLED';
      elsif not v_global_enabled or v_candidate.id is null
         or pg_catalog.coalesce(v_candidate.key_norm,'')!~'^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
         or v_scope.candidate_id is null or v_scope.authority_mode<>'GOOGLE_PRIMARY'
         or v_scope.transition_in_progress or v_entitlement
         or v_scope.active_generation_id is null then
        v_status:='NOT_READY';
      else
        select * into v_generation from public.candidate_daily_rota_generations g
        where g.generation_id=v_scope.active_generation_id
          and g.environment=v_environment and g.candidate_id=v_candidate_id and g.state='ACTIVE';
        select pg_catalog.count(*)::integer into v_day_count
        from public.candidate_daily_rota_days d where d.generation_id=v_scope.active_generation_id
          and d.environment=v_environment and d.candidate_id=v_candidate_id;
        perform private._candidate_daily_refresh_sync_state_v1(
          v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',pg_catalog.clock_timestamp()
        );
        select * into v_sync from private.candidate_daily_sync_state s
        where s.environment=v_environment and s.candidate_id=v_candidate_id
          and s.target='MASTER_AVAILABILITY_SHEET';
        select pg_catalog.count(*) filter(where l.state='PRIMARY'
            and l.valid_from_utc<=pg_catalog.clock_timestamp()
            and (l.valid_to_utc is null or l.valid_to_utc>pg_catalog.clock_timestamp())),
          pg_catalog.count(distinct l.link_group_id) filter(where l.state in ('PRIMARY','OVERLAP')
            and l.valid_from_utc<=pg_catalog.clock_timestamp()
            and (l.valid_to_utc is null or l.valid_to_utc>pg_catalog.clock_timestamp()))
        into v_source_primary_count,v_source_group_count
        from private.candidate_daily_source_links l
        where l.environment=v_environment and l.candidate_id=v_candidate_id
          and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID';
        select
          (select pg_catalog.count(*) from public.candidate_daily_sheet_projection_outbox o
            where o.environment=v_environment and o.candidate_id=v_candidate_id
              and o.target='MASTER_AVAILABILITY_SHEET' and o.state in ('PENDING','CLAIMED','RETRY','TERMINAL'))
          +(select pg_catalog.count(*) from public.candidate_daily_command_receipts c
            where c.environment=v_environment and c.candidate_id=v_candidate_id and c.state='IN_PROGRESS')
          +(select pg_catalog.count(*) from private.candidate_daily_batch_receipts b
            where b.environment=v_environment and b.state='IN_PROGRESS'
              and b.item_keys_json @> pg_catalog.jsonb_build_array(v_candidate_id::text))
          +(select pg_catalog.count(*) from private.candidate_daily_external_effect_receipts e
            where e.environment=v_environment and e.candidate_id=v_candidate_id
              and e.state in ('IN_PROGRESS','UNKNOWN')),
          (select pg_catalog.count(*) from public.candidate_daily_sheet_projection_outbox o
            where o.environment=v_environment and o.candidate_id=v_candidate_id
              and o.target='MASTER_AVAILABILITY_SHEET' and o.state='DEFERRED_OVERLAY')
        into v_blocking_count,v_deferred_count;

        if v_generation.generation_id is null or v_generation.expected_day_count<>14
           or v_generation.actual_day_count<>14 or v_day_count<>14
           or v_generation.published_at_utc is null
           or v_generation.published_at_utc<pg_catalog.clock_timestamp()-interval '120 seconds'
           or v_source_primary_count<>1 or v_source_group_count<>1
           or v_sync.candidate_id is null or v_sync.state<>'READY'
           or v_sync.pending_count<>0 or v_sync.retry_count<>0 or v_sync.terminal_count<>0
           or v_sync.accepted_canonical_cursor<>v_scope.canonical_version
           or v_sync.required_visible_cursor<>v_scope.canonical_version
           or v_sync.effective_visible_cursor<>v_scope.canonical_version
           or pg_catalog.coalesce(v_blocking_count,0)<>0 then
          v_status:='NOT_READY';
        else
          v_transition_item:=pg_catalog.jsonb_build_object(
            'candidate_id',v_candidate_id,'expected_authority_mode','GOOGLE_PRIMARY',
            'expected_canonical_version',v_scope.canonical_version,
            'expected_entitlement_enabled',false,'new_authority_mode','SUPABASE_PRIMARY',
            'entitlement_enabled',true,
            'in_flight_disposition',case when v_deferred_count>0 then 'RECONCILED' else 'DRAINED' end,
            'expected_generation_id',v_generation.generation_id,
            'expected_generation_version',v_generation.generation_version,
            'expected_accepted_canonical_cursor',v_sync.accepted_canonical_cursor,
            'expected_required_visible_cursor',v_sync.required_visible_cursor,
            'expected_effective_visible_cursor',v_sync.effective_visible_cursor
          );
          v_evidence:=private._candidate_daily_json_sha256_v1(pg_catalog.jsonb_build_object(
            'policy','CANDIDATE_FIRST_READY_ACTIVATION_V1','environment',v_environment,
            'candidate_id',v_candidate_id,'generation_id',v_generation.generation_id,
            'generation_version',v_generation.generation_version,
            'canonical_version',v_scope.canonical_version
          ));
          v_transition:=public.candidate_daily_authority_transition_atomic_v1(
            v_context||pg_catalog.jsonb_build_object(
              'actor_user_id','00000000-0000-4000-8000-000000009221'
            ),gen_random_uuid(),'candidate.system.first-ready.'||gen_random_uuid()::text,
            pg_catalog.jsonb_build_array(v_transition_item),
            '00000000-0000-4000-8000-000000009222',
            'System policy activation after the first complete Candidate Rota publication',
            v_evidence,p_correlation_id
          );
          if v_transition#>>'{outcomes,0,status}'='COMMITTED'
             and v_transition#>>'{outcomes,0,entitlement_enabled}'='true'
          then v_status:='ACTIVATED'; else v_status:='NOT_READY'; end if;
        end if;
      end if;
    exception when others then
      if sqlerrm in ('CANDIDATE_DAILY_NOT_READY','GENERATION_INCOMPLETE',
        'SEMANTIC_REJECTION','IDENTITY_LINK_MISSING','IDENTITY_LINK_AMBIGUOUS',
        'BATCH_IN_PROGRESS','COMMAND_IN_PROGRESS','EFFECT_STATUS_UNKNOWN',
        'PROJECTION_STALE_COMPLETION')
      then v_status:='NOT_READY'; else raise; end if;
    end;
    v_outcomes:=v_outcomes||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('candidate_id',v_candidate_id,'status',v_status)
    );
  end loop;
  return pg_catalog.jsonb_build_object('ok',true,
    'policy_version','CANDIDATE_FIRST_READY_ACTIVATION_V1','outcomes',v_outcomes);
end;
$function$;

alter function public.candidate_daily_system_policy_activate_ready_v1(
  jsonb,jsonb,jsonb,text
) owner to postgres;
revoke all on function public.candidate_daily_system_policy_activate_ready_v1(
  jsonb,jsonb,jsonb,text
) from public,anon,authenticated;
grant execute on function public.candidate_daily_system_policy_activate_ready_v1(
  jsonb,jsonb,jsonb,text
) to service_role;

commit;
