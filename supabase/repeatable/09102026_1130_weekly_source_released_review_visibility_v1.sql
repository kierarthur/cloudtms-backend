-- Released checking-file review visibility only. No financial, contract,
-- final-report, membership, or source authority is changed.
\set ON_ERROR_STOP on
begin;

CREATE OR REPLACE FUNCTION public.weekly_source_combined_review_workspace_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_request jsonb; v_scope_page jsonb; v_scopes jsonb:='[]'; v_scope jsonb;
  v_workspace jsonb; v_owner_request jsonb; v_rows jsonb:='[]'; v_item jsonb;
  v_manual_children jsonb; v_question_children jsonb; v_child jsonb;
  v_follow_workspace jsonb; v_follow_up jsonb;
  v_versions jsonb:='[]'; v_owners jsonb:='[]'; v_summary jsonb;
  v_tab text:=coalesce(nullif(p_request->>'tab',''),'queries');
  v_section text:=coalesce(nullif(p_request->>'section',''),'questions');
  v_sort text:=coalesce(nullif(p_request->>'sort_key',''),case when nullif(p_request->>'client_id','') is null then 'client' else 'candidate' end);
  v_direction text:=coalesce(nullif(p_request->>'sort_direction',''),'asc');
  v_seek text:=private.weekly_source_query_ascii_fold_v1(coalesce(p_request->>'seek',''));
  v_limit integer:=coalesce((p_request->>'limit')::integer,50);
  v_offset integer:=0; v_base integer:=0; v_total integer; v_page jsonb; v_counts jsonb;
  v_version text; v_cursor jsonb; v_client uuid; v_seen text[]:='{}'; v_key text; v_owner_key text;
  v_attention jsonb; v_attention_first boolean:=coalesce((p_request->>'attention_first')::boolean,false);
  v_attention_kind text:=coalesce(p_request->>'attention_kind','');
  v_released_scope record;
  v_pending record; v_pending_checks jsonb:='[]'::jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform private._weekly_source_settings_assert_request_v1(p_request,
    array['actor_user_id','source_group_id','client_id','week_ending','tab','section',
      'sort_key','sort_direction','seek','cursor','limit','report_key','attention_first','attention_kind'],'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID');
  if v_tab='history' then
    return public.weekly_source_report_history_v1(p_request-'tab'-'section');
  end if;
  if p_request ? 'report_key' then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  if v_tab not in ('imports','queries') or v_section not in ('questions','checks','protected','current','archive')
    or v_sort not in ('client','candidate','day_date','status','file','uploaded')
    or v_direction not in ('asc','desc') or v_limit not between 1 and 100 or length(v_seek)>100
    or v_attention_kind not in ('','missing_source','questions','checks','protected')
    or (p_request ? 'attention_first' and jsonb_typeof(p_request->'attention_first')<>'boolean') then
    raise exception 'WEEKLY_SOURCE_COMBINED_REQUEST_INVALID' using errcode='22023';
  end if;
  v_client:=nullif(p_request->>'client_id','')::uuid;
  v_request:=p_request-'tab'-'section'-'sort_key'-'sort_direction'-'seek'-'cursor'-'limit'-'attention_first'-'attention_kind';
  loop
    v_scope_page:=public.weekly_source_workspace_scopes_v1(v_request);
    v_summary:=v_scope_page-'rows'-'next_cursor'-'has_more';
    v_scopes:=v_scopes||(v_scope_page->'rows');
    exit when not (v_scope_page->>'has_more')::boolean;
    v_request:=v_request||jsonb_build_object('cursor',v_scope_page->>'next_cursor');
  end loop;

  -- Checking-only released NHSP files have row-wise client authority. These
  -- review scopes are not report obligations and must never feed Finalise.
  -- Keep contract, charge and Hours questions visible for eligible row clients
  -- even when they are outside the final backing report's source membership.
  if v_tab='queries' then
    for v_released_scope in
      select distinct cycle.id cycle_id,cycle.source_group_id,
        cycle.finalisation_week_ending,cycle.cutoff_at_utc,
        source_group.display_name source_name,resolution.client_id,client.name client_name,
        upload.id upload_id,publication.id publication_id
      from public.weekly_source_cycles cycle
      join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
      join public.weekly_source_uploads upload on upload.id=cycle.current_complete_upload_id
        and upload.source_cycle_id=cycle.id and upload.state='CURRENT' and upload.report_scope_id is null
      join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
      join public.weekly_source_projection_publications publication on publication.upload_id=upload.id
        and publication.source_cycle_id=cycle.id and publication.authority_scope_kind='CYCLE'
        and publication.report_scope_id is null and publication.authority_scope_version=cycle.version
      left join private.weekly_source_office_rechecks recheck on recheck.publication_id=publication.id
      left join public.weekly_source_projection_publications prior_publication on prior_publication.id=recheck.prior_publication_id
      join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
      join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
        and resolution.generation=case when publication.state='CURRENT'
          then coalesce(publication.projection_generation,publication.authority_scope_version)
          else coalesce(prior_publication.projection_generation,prior_publication.authority_scope_version) end
      join public.clients client on client.id=resolution.client_id
      where source_group.active and source_group.source_family='NHSP'
        and profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
        and profile.row_finalisation_capability='CHECKING_ONLY' and not profile.single_client_required
        and ((publication.state='CURRENT' and cycle.current_projection_publication_id=publication.id)
          or (publication.state='BUILDING' and recheck.upload_id=upload.id))
        and (nullif(p_request->>'source_group_id','') is null or cycle.source_group_id=(p_request->>'source_group_id')::uuid)
        and (v_client is null or resolution.client_id=v_client)
        and (nullif(p_request->>'week_ending','') is null or cycle.finalisation_week_ending=(p_request->>'week_ending')::date)
        and private.weekly_source_upload_client_eligible_v1(upload.id,resolution.client_id,source_row.work_date)
        and not exists(select 1 from jsonb_array_elements(v_scopes) existing
          where existing->>'source_cycle_id'=cycle.id::text and existing->>'client_id'=resolution.client_id::text)
      order by cycle.id,resolution.client_id,publication.id
    loop
      perform private.weekly_source_office_authority_v1((p_request->>'actor_user_id')::uuid,
        'VIEW_SOURCE_PROGRESS',v_released_scope.source_group_id);
      v_scopes:=v_scopes||jsonb_build_array(jsonb_build_object(
        'key',v_released_scope.cycle_id::text||':'||v_released_scope.client_id::text||':RELEASED_REVIEW',
        'source_group_id',v_released_scope.source_group_id,'source_cycle_id',v_released_scope.cycle_id,
        'report_scope_id',null,'client_id',v_released_scope.client_id,'client',v_released_scope.client_name,
        'source',v_released_scope.source_name,'source_family','NHSP',
        'week_ending',v_released_scope.finalisation_week_ending,
        'period',to_char(v_released_scope.finalisation_week_ending,'FMDD Mon YYYY'),
        'cutoff',v_released_scope.cutoff_at_utc,'upload_id',v_released_scope.upload_id,
        'projection_publication_id',v_released_scope.publication_id,
        'review_only',true,'prepared',false,'completed',false,'completion_kind',null,
        'missing_previous_report',false));
    end loop;
  end if;

  -- A cycle publication is shared across its client scopes. If it has no
  -- cycle publication, each NHSP client's current report scope can instead
  -- own distinct query work. Visit each such scope exactly once.
  for v_scope in select distinct on (item->>'source_cycle_id',
      case when v_tab='queries' and cycle.current_projection_publication_id is null then item->>'client_id' else '' end)
      item||jsonb_build_object('cycle_publication_id',cycle.current_projection_publication_id)
    from jsonb_array_elements(v_scopes) item
    join public.weekly_source_cycles cycle on cycle.id=(item->>'source_cycle_id')::uuid
    order by item->>'source_cycle_id',
      case when v_tab='queries' and cycle.current_projection_publication_id is null then item->>'client_id' else '' end,
      case when item->>'report_scope_id' is null then 1 else 0 end,
      item->>'cutoff' desc nulls last,item->>'key'
  loop
    v_owner_request:=jsonb_build_object('actor_user_id',p_request->>'actor_user_id',
      'tab',v_tab,'source_group_id',v_scope->>'source_group_id',
      'source_cycle_id',v_scope->>'source_cycle_id','limit',100);
    if v_scope->>'source_family'='ROSTER' or
      (v_tab='queries' and v_scope->>'source_family'='NHSP' and v_scope->>'cycle_publication_id' is null
        and v_scope->>'report_scope_id' is not null) then
      v_owner_request:=v_owner_request||jsonb_build_object('client_id',v_scope->>'client_id');
    end if;
    if v_tab='queries' and v_scope->>'source_family'='NHSP' and v_scope->>'cycle_publication_id' is null
      and v_scope->>'report_scope_id' is not null then
      v_owner_request:=v_owner_request||jsonb_build_object('report_scope_id',v_scope->>'report_scope_id');
    end if;
    loop
      v_workspace:=public.weekly_source_office_workspace_v1(v_owner_request);
      v_owner_key:=case when v_tab='imports' then v_scope->>'source_cycle_id'
        else (v_scope->>'source_cycle_id')||':'||
          coalesce(v_workspace#>>'{selected,projection_publication_id}','none') end;
      v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_scope->>'source_cycle_id',v_workspace->>'workspace_version'));
      if not (v_owner_request ? 'cursor') then
        v_owners:=v_owners||jsonb_build_array(jsonb_build_object('key',v_owner_key,
          'source',v_scope->>'source','period',v_scope->>'period','scope',v_workspace->'selected',
          'bulk_actions',v_workspace#>'{queries,bulk_actions}',
          'protected_pay_enabled',v_workspace#>'{queries,protected_pay_enabled}'));
      end if;
      for v_item in select item from jsonb_array_elements(coalesce(v_workspace#>array[v_tab,'rows'],'[]')) item
      loop
        v_key:=v_tab||':'||v_owner_key;
        v_key:=v_key||':'||(v_item->>'row_key');
        if v_key=any(v_seen) then continue; end if;
        v_seen:=array_append(v_seen,v_key);
        if nullif(v_item->>'client_id','') is not null and not exists(select 1
          from jsonb_array_elements(v_scopes) permitted where permitted->>'source_cycle_id'=v_scope->>'source_cycle_id'
            and permitted->>'client_id'=v_item->>'client_id') then continue; end if;
        if v_client is not null and nullif(v_item->>'client_id','') is not null
          and (v_item->>'client_id')::uuid<>v_client then continue; end if;
        if v_tab='imports' then
          -- Upload errors remain in their immediate review receipt, not an
          -- ever-growing archive. Retain only accepted current work here.
          if v_item->>'state' is distinct from 'CURRENT' then continue; end if;
          if exists(select 1 from public.weekly_source_final_revisions revision
            join public.weekly_source_client_manifests manifest on manifest.final_revision_id=revision.id
            where revision.upload_id=(v_item->>'row_key')::uuid
              and revision.state in ('CURRENT','SUPERSEDED')
              and (nullif(v_item->>'client_id','') is null
                or manifest.client_id=(v_item->>'client_id')::uuid))
            and not exists(select 1 from jsonb_array_elements(v_scopes) sibling
              where sibling->>'upload_id'=v_item->>'row_key'
                and not coalesce((sibling->>'completed')::boolean,false)) then continue; end if;
        end if;
        if v_tab='queries' then
          -- An Office-created pay review is an Office check, not a request for
          -- candidate/manager hours evidence. Split only those children so a
          -- mixed group can retain its genuine Hours questions independently.
          select coalesce(jsonb_agg(child.value order by child.ordinality)
              filter(where child.value ? 'manual_review_id'),'[]'::jsonb),
            coalesce(jsonb_agg(child.value order by child.ordinality)
              filter(where not (child.value ? 'manual_review_id')),'[]'::jsonb)
            into v_manual_children,v_question_children
          from jsonb_array_elements(coalesce(v_item->'children','[]'::jsonb))
            with ordinality child(value,ordinality);
          for v_child in select value from jsonb_array_elements(v_manual_children)
          loop
            v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
              'combined_key','manual-check:'||(v_child->>'manual_review_id'),
              'row_key',v_child->>'row_key','section','checks',
              'scope_key',v_owner_key,
              'source',v_scope->>'source','source_family',v_scope->>'source_family',
              'period',v_scope->>'period',
              'client',v_item->>'client','client_id',v_item->>'client_id',
              'candidate',v_item->>'candidate','candidate_id',v_item->>'candidate_id',
              'day_date',v_child->>'day_date','system_hours',v_child->>'system_hours',
              'status',v_child->'status','manual_query',v_child->'manual_query',
              'problem','Accept current source hours or protect pay.',
              'pay_blocking',true,'actions',v_child->'actions'));
          end loop;
          if jsonb_array_length(v_question_children)=0 then continue; end if;
          if jsonb_array_length(v_manual_children)>0 then
            v_item:=jsonb_set(v_item,'{actions,0,payload,detail,shifts}',v_question_children,false)
              ||jsonb_build_object('children',v_question_children,
                'issues',greatest(0,coalesce((v_item->>'issues')::integer,0)
                  -jsonb_array_length(v_manual_children)));
          end if;
        end if;
        v_rows:=v_rows||jsonb_build_array(v_item||jsonb_build_object(
          'combined_key',v_key,'scope_key',v_owner_key,'source',v_scope->>'source',
          'source_family',v_scope->>'source_family',
          'period',v_scope->>'period','section',case when v_tab='queries' then 'questions' else 'current' end,
          'client',coalesce(nullif(v_item->>'client',''),(select name from public.clients
            where id=nullif(v_item->>'client_id','')::uuid),'Source-wide file')));
      end loop;
      if v_tab='queries' and not (v_owner_request ? 'cursor') then
        for v_item in
          select item||jsonb_build_object('section','checks') from jsonb_array_elements(coalesce(v_workspace#>'{queries,office_checks,rows}','[]')) item
          union all
          select item||jsonb_build_object('section','protected') from jsonb_array_elements(coalesce(v_workspace#>'{queries,protected_shifts,rows}','[]')) item
        loop
          v_key:=case when v_item->>'section'='protected' then
            'protected:'||(v_item->>'family_id')||':'||(v_item->>'work_event_id')
            else (v_item->>'section')||':'||v_owner_key||':'||(v_item->>'row_key') end;
          if v_key=any(v_seen) then continue; end if;
          v_seen:=array_append(v_seen,v_key);
          if nullif(v_item->>'client_id','') is not null and not exists(select 1
            from jsonb_array_elements(v_scopes) permitted where permitted->>'source_group_id'=v_scope->>'source_group_id'
              and permitted->>'client_id'=v_item->>'client_id') then continue; end if;
          if v_client is not null and nullif(v_item->>'client_id','') is not null
            and (v_item->>'client_id')::uuid<>v_client then continue; end if;
          v_rows:=v_rows||jsonb_build_array(v_item||jsonb_build_object('combined_key',v_key,
            'scope_key',v_owner_key,'source',v_scope->>'source','period',
              case when v_item->>'section'='protected' then 'Awaiting source period' else v_scope->>'period' end));
        end loop;
      end if;
      exit when not coalesce((v_workspace#>>array[v_tab,'has_more'])::boolean,false);
      v_owner_request:=v_owner_request||jsonb_build_object('cursor',v_workspace#>>array[v_tab,'next_cursor']);
    end loop;
  end loop;
  -- Finalisation does not complete its separately tracked approved-hours work.
  -- Keep that exact owner/action reachable from outstanding Office checks.
  if v_tab='queries' then
    for v_scope in select item from jsonb_array_elements(v_scopes) item
      where item->>'completion_kind'='FINAL_SOURCE'
    loop
      v_follow_workspace:=public.weekly_source_office_workspace_v1(jsonb_build_object(
        'actor_user_id',p_request->>'actor_user_id','tab','finalise',
        'source_group_id',v_scope->>'source_group_id','source_cycle_id',v_scope->>'source_cycle_id',
        'client_id',v_scope->>'client_id','report_scope_id',v_scope->>'report_scope_id'));
      v_follow_up:=v_follow_workspace#>'{finalise,approved_hours_follow_up}';
      if nullif(v_follow_up->>'title','') is null then continue; end if;
      v_key:='approved-hours:'||(v_scope->>'key');
      v_versions:=v_versions||jsonb_build_array(jsonb_build_array(v_key,v_follow_workspace->>'workspace_version',v_follow_up));
      v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
        'combined_key',v_key,'row_key',v_key,'section','checks',
        'client',v_scope->>'client','client_id',v_scope->>'client_id',
        'source',v_scope->>'source','period',v_scope->>'period',
        'candidate','Report follow-up','requires_attention',
          v_follow_up->>'state' in ('ACTION_REQUIRED','RECOVERY_REQUIRED')
          or jsonb_typeof(v_follow_up->'action')='object',
        'status',jsonb_build_object('text',v_follow_up->>'title'),
        'problem',v_follow_up->>'body','follow_up_scope',v_follow_workspace->'selected','actions','[]'::jsonb));
    end loop;
  end if;
  -- Rechecks invalidate the old authority before the replacement is ready.
  -- Retain old unresolved rows as explicitly non-actionable history, with only
  -- the exact saved recheck available. Never treat a missing CURRENT pointer
  -- as proof that Office has no work remaining.
  if v_tab='queries' then
    for v_pending in
      select recheck.request_id,recheck.actor_user_id,recheck.request_json,
        recheck.upload_id,recheck.publication_id,source_row.id as upload_row_id,
        source_row.work_date,source_row.source_client_identity,source_row.source_candidate_identity,
        source_row.external_source_key,source_row.start_at_local,source_row.end_at_local,source_row.break_minutes,
        coalesce(candidate.display_name,source_row.bounded_raw_columns_json->>'worker_name',
          source_row.bounded_raw_columns_json->>'candidate',source_row.source_candidate_identity) as candidate_name,
        resolution.mapping_state,charge.phase_severity,cycle.source_group_id,cycle.finalisation_week_ending,
        (select item->>'source' from jsonb_array_elements(v_scopes) item
          where item->>'source_cycle_id'=cycle.id::text limit 1) as source_name
      from private.weekly_source_office_rechecks recheck
      join public.weekly_source_projection_publications publication on publication.id=recheck.publication_id
      join public.weekly_source_projection_publications prior_publication on prior_publication.id=recheck.prior_publication_id
      join public.weekly_source_uploads upload on upload.id=recheck.upload_id
      join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
      left join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
      join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
      join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
        and resolution.generation=coalesce(prior_publication.projection_generation,prior_publication.authority_scope_version)
      left join public.weekly_source_charge_checks charge on charge.upload_row_id=source_row.id
        and charge.generation=resolution.generation
      left join public.candidates candidate on candidate.id=coalesce(
        (select choice.candidate_id from private.weekly_source_office_row_choices choice
          where choice.upload_row_id=source_row.id order by choice.id desc limit 1),resolution.candidate_id)
      where publication.state='BUILDING' and upload.state='CURRENT'
        and publication.authority_scope_version=case when upload.report_scope_id is null then cycle.version else scope.version end
        and upload.id=case when upload.report_scope_id is null then cycle.current_complete_upload_id else scope.current_complete_upload_id end
        and exists(select 1 from jsonb_array_elements(v_scopes) item where item->>'source_cycle_id'=cycle.id::text)
        and (v_client is null or resolution.client_id=v_client or scope.client_id=v_client)
      order by recheck.request_id,source_row.source_row_ordinal
    loop
      v_key:='recheck:'||v_pending.request_id::text||':'||v_pending.upload_row_id::text;
      v_pending_checks:=v_pending_checks||jsonb_build_array(jsonb_build_object(
        'combined_key',v_key,'row_key',v_key,'section','checks','recheck_pending',true,
        'client',v_pending.source_client_identity,'candidate',v_pending.candidate_name,
        'source_reference',v_pending.source_candidate_identity,'booking_reference',v_pending.external_source_key,
        'source',v_pending.source_name,'period',to_char(v_pending.finalisation_week_ending,'FMDD Mon YYYY'),
        'work_date',v_pending.work_date,'day_date',to_char(v_pending.work_date,'FMDD Mon YYYY'),
        'system_hours',to_char(v_pending.start_at_local,'HH24:MI')||'–'||to_char(v_pending.end_at_local,'HH24:MI')
          ||' · '||v_pending.break_minutes::text||' min break',
        'pay_blocking',v_pending.mapping_state<>'RESOLVED',
        'status',jsonb_build_object('text','Recheck incomplete'),
        'problem',case when v_pending.mapping_state<>'RESOLVED' then 'Previous linking check — selection saved; replacement check incomplete.'
          when v_pending.phase_severity in ('PROVISIONAL_WARNING','FINALISATION_BLOCKER') then 'Previous contract charge warning — replacement check incomplete.'
          else 'Previous check — replacement check incomplete.' end,
        'actions',jsonb_build_array(jsonb_build_object('label','Retry recheck',
          'enabled',v_pending.actor_user_id=(p_request->>'actor_user_id')::uuid,
          'command','RECHECK_SOURCE','payload',v_pending.request_json-'actor_user_id'))));
    end loop;
    v_rows:=v_rows||v_pending_checks;
    v_summary:=v_summary||jsonb_build_object('recheck_pending_count',jsonb_array_length(v_pending_checks));
  end if;
  -- Classify the complete permitted collection before counting or paging.
  -- A waiting signature alone is informational; red unresolved work and green
  -- charge/reconciliation decisions remain visible and count as Office work.
  select coalesce(jsonb_agg(item||jsonb_build_object(
    'attention_missing_source_count',case when item->>'section'='questions' then
      (select count(*) from jsonb_array_elements(coalesce(item->'children','[]')) child
        where child->'candidate_shift_absent_from_import'='true'::jsonb) else 0 end,
    'attention_question_count',case when item->>'section'='questions' then
      (select count(*) from jsonb_array_elements(coalesce(item->'children','[]')) child
        where child->>'issue' is distinct from 'Timesheet missing'
          and child->'candidate_shift_absent_from_import' is distinct from 'true'::jsonb) else 0 end,
    'requires_attention',case
    when item->>'section'='questions' then exists(select 1
      from jsonb_array_elements(coalesce(item->'children','[]')) child
      where child->>'issue' is distinct from 'Timesheet missing')
    when item->>'section'='checks' then case when item ? 'follow_up_scope'
      then coalesce((item->>'requires_attention')::boolean,false) else true end
    when item->>'section'='protected' then coalesce((item->>'requires_attention')::boolean,false)
    else false end)),'[]') into v_rows from jsonb_array_elements(v_rows) item;
  select jsonb_build_object('missing_source',coalesce(sum((item->>'attention_missing_source_count')::integer),0),
    'questions',coalesce(sum((item->>'attention_question_count')::integer),0),
    'checks',count(*) filter(where item->>'section'='checks'),
    'protected',count(*) filter(where item->>'section'='protected'),
    'total',coalesce(sum((item->>'attention_missing_source_count')::integer
      +(item->>'attention_question_count')::integer),0)
      +count(*) filter(where item->>'section' in ('checks','protected')),'complete',true) into v_attention
  from jsonb_array_elements(v_rows) item where item->'requires_attention'='true'::jsonb;
  select jsonb_build_object('questions',count(*) filter(where item->>'section'='questions'),
    'checks',count(*) filter(where item->>'section'='checks'),'protected',count(*) filter(where item->>'section'='protected'),
    'current',count(*) filter(where item->>'section'='current'),'archive',count(*) filter(where item->>'section'='archive'))
    into v_counts from jsonb_array_elements(v_rows) item;
  v_version:=encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_COMBINED_REVIEW_V1',
    jsonb_build_object('scopes',v_summary->>'version','owners',v_versions,'attention_rows',v_rows,
      'filters',p_request-'cursor'-'limit')),'hex');
  -- Attention is an exact view filter, not a tab-total alias. The full census
  -- above stays intact; only this page collection is narrowed. Never rewrite
  -- server-owned action selections or comparison/financial proof payloads.
  if v_attention_kind<>'' then
    select coalesce(jsonb_agg(item),'[]') into v_rows from jsonb_array_elements(v_rows) item
    where item->'requires_attention'='true'::jsonb and case v_attention_kind
      when 'missing_source' then item->>'section'='questions' and (item->>'attention_missing_source_count')::integer>0
      when 'questions' then item->>'section'='questions' and (item->>'attention_question_count')::integer>0
      when 'checks' then item->>'section'='checks'
      when 'protected' then item->>'section'='protected'
      else false end;
    if v_attention_kind in ('missing_source','questions') then
      for v_item in select item from jsonb_array_elements(v_rows) item
      loop
        select coalesce(jsonb_agg(child.value order by child.ordinality),'[]') into v_question_children
          from jsonb_array_elements(v_item->'children') with ordinality child(value,ordinality)
          where child.value->>'issue' is distinct from 'Timesheet missing' and case v_attention_kind
            when 'missing_source' then child.value->'candidate_shift_absent_from_import'='true'::jsonb
            else child.value->'candidate_shift_absent_from_import' is distinct from 'true'::jsonb end;
        v_item:=v_item||jsonb_build_object('children',v_question_children,'issues',jsonb_array_length(v_question_children),
          'actions',(select coalesce(jsonb_agg(case when action->>'label'='Open'
            then jsonb_set(action,'{payload,detail,shifts}',v_question_children,false) else action end),'[]')
            from jsonb_array_elements(coalesce(v_item->'actions','[]')) action));
        select coalesce(jsonb_agg(case when item->>'combined_key'=v_item->>'combined_key' then v_item else item end),'[]')
          into v_rows from jsonb_array_elements(v_rows) item;
      end loop;
    end if;
  end if;
  if nullif(p_request->>'cursor','') is not null then
    begin
      v_cursor:=convert_from(decode(p_request->>'cursor','base64'),'UTF8')::jsonb;
      v_offset:=(v_cursor->>'offset')::integer;
      if v_offset is null or v_offset<0 or v_cursor->>'version' is distinct from v_version then raise exception 'stale'; end if;
    exception when others then raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001'; end;
  end if;
  with keyed as (
    select item,private.weekly_source_query_ascii_fold_v1(coalesce(case v_sort
      when 'candidate' then coalesce(item->>'candidate_sort',item->>'candidate') when 'client' then item->>'client'
      when 'day_date' then item->>'work_date' when 'file' then item->>'file' when 'uploaded' then item->>'uploaded_at'
      when 'status' then item#>>'{status,text}' end,'')) sort_value
    from jsonb_array_elements(v_rows) item where item->>'section'=v_section
  ), ordered as (
    select *,row_number() over(order by case when v_attention_first and v_seek='' then
      case v_attention_kind when 'missing_source' then ((item->>'attention_missing_source_count')::integer>0)::integer
        when 'questions' then ((item->>'attention_question_count')::integer>0)::integer
        else 0 end else 0 end desc,
      case when v_attention_first and v_seek=''
      then coalesce((item->>'requires_attention')::boolean,false)::integer else 0 end desc,
      case when v_direction='asc' then sort_value end collate "C" asc,
      case when v_direction='desc' then sort_value end collate "C" desc,
      private.weekly_source_query_ascii_fold_v1(item->>'client') collate "C",
      private.weekly_source_query_ascii_fold_v1(coalesce(item->>'candidate_sort',item->>'candidate')) collate "C",
      item->>'work_date',item->>'combined_key') ordinal from keyed
  ), origin as (select coalesce(min(ordinal) filter(where v_seek<>'' and starts_with(sort_value,v_seek)),1)-1 base from ordered)
  select count(*)::integer,coalesce(max(origin.base),0)::integer,coalesce(jsonb_agg(item order by ordinal)
    filter(where ordinal>origin.base+v_offset and ordinal<=origin.base+v_offset+v_limit),'[]')
    into v_total,v_base,v_page from ordered cross join origin;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_COMBINED_REVIEW_V1','tab',v_tab,'section',v_section,
    'version',v_version,'rows',v_page,'total_count',v_total,'owners',v_owners,'scope_options',v_scopes,
    'summary',v_summary,'counts',v_counts,'attention',v_attention,
    'attention_kind',v_attention_kind,
    'sort_key',v_sort,'sort_direction',v_direction,
    'has_more',v_base+v_offset+v_limit<v_total,'next_cursor',case when v_base+v_offset+v_limit<v_total
      then encode(convert_to(jsonb_build_object('version',v_version,'offset',v_offset+v_limit)::text,'UTF8'),'base64') else '' end);
end;
$function$;

alter function public.weekly_source_combined_review_workspace_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_combined_review_workspace_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_combined_review_workspace_v1(jsonb) to service_role;
notify pgrst, 'reload schema';
commit;
