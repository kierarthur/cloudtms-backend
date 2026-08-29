-- Repeatable CloudTMS authority for immediate, service-only Candidate Rota activation.
-- Existing Google Candidate creation/publication remains the first trigger. Active federated
-- membership is the second trigger, allowing a current complete 14-day window to be activated
-- without waiting for the next daily refresh. Blank availability values remain valid.
\set ON_ERROR_STOP on

begin;

-- R17 effective authority for:
-- public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)
-- It preserves the Phase 2 contract while integrating the R16 all-history
-- source-identity guard into one deterministic cross-writer lock order.

create or replace function public.candidate_daily_authority_transition_atomic_v1(
  p_internal_context jsonb,
  p_batch_request_id uuid,
  p_idempotency_key text,
  p_transition_items jsonb,
  p_independent_approver uuid,
  p_reason text,
  p_evidence_sha256 text,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_item_keys jsonb; v_item jsonb; v_candidate_id uuid; v_scope private.candidate_daily_authority_scopes%rowtype;
  v_generation public.candidate_daily_rota_generations%rowtype;
  v_sync private.candidate_daily_sync_state%rowtype;
  v_transition_id uuid; v_index integer:=0; v_outcomes jsonb:='[]'::jsonb; v_terminal jsonb;
  v_prior_mode text; v_new_mode text; v_entitlement_before boolean:=false; v_entitlement_after boolean;
  v_actor uuid; v_error text; v_link jsonb; v_link_group uuid; v_link_state text;
  v_now timestamptz:=clock_timestamp(); v_global_enabled boolean:=false;
  v_expected_mode text; v_expected_version bigint; v_expected_entitlement boolean;
  v_expected_generation_id uuid; v_expected_generation_version bigint;
  v_expected_accepted_cursor bigint; v_expected_required_cursor bigint; v_expected_effective_cursor bigint;
  v_requested_disposition text; v_actual_disposition text; v_strict_barrier boolean:=false;
  v_source_primary_count integer:=0; v_source_group_count integer:=0; v_generation_day_count integer:=0;
  v_pending_count integer:=0; v_claimed_count integer:=0; v_retry_count integer:=0;
  v_deferred_count integer:=0; v_terminal_count integer:=0; v_invalid_overlay_count integer:=0;
  v_command_count integer:=0; v_other_batch_count integer:=0; v_effect_count integer:=0; v_unknown_effect_count integer:=0;
  v_latest_fact_at timestamptz; v_source_link_changed boolean:=false;
  v_source_lock_identity text;
  v_membership_activation boolean:=false;
  v_membership_id uuid;
  v_membership_generation integer;
  v_membership_candidate_id uuid;
  v_membership_link public.candidate_app_global_membership_links%rowtype;
  v_today date;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  v_membership_activation:=
    p_internal_context->>'activation_reason'='FEDERATED_MEMBERSHIP_ACTIVE';
  if v_membership_activation then
    begin
      v_membership_id:=(p_internal_context->>'membership_id')::uuid;
      v_membership_generation:=(p_internal_context->>'membership_generation')::integer;
      v_membership_candidate_id:=(p_internal_context->>'candidate_id')::uuid;
    exception when others then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end;
    if v_membership_id is null or v_membership_candidate_id is null
       or coalesce(v_membership_generation,0)<1 then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end if;
  end if;
  begin v_actor:=nullif(p_internal_context->>'actor_user_id','')::uuid; exception when others then v_actor:=null; end;
  if p_batch_request_id is null or v_actor is null or p_independent_approver is null or p_independent_approver=v_actor
     or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_transition_items)<>'array' or jsonb_array_length(p_transition_items) not between 1 and 100
     or length(btrim(p_reason)) not between 1 and 500 or p_evidence_sha256 !~ '^[a-f0-9]{64}$'
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(i->>'candidate_id' order by ord) into v_item_keys
    from jsonb_array_elements(p_transition_items) with ordinality x(i,ord);
  if exists(select 1 from jsonb_array_elements(p_transition_items)i where
    i->>'candidate_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    or jsonb_array_length(v_item_keys)<>(select count(distinct x) from jsonb_array_elements_text(v_item_keys)x) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation','AUTHORITY_TRANSITION',
    'batch_request_id',p_batch_request_id,'items',p_transition_items,'independent_approver',p_independent_approver,
    'reason',p_reason,'evidence_sha256',p_evidence_sha256));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(v_batch_id,v_environment,'OFFICE_ADMIN','AUTHORITY_TRANSITION',p_idempotency_key,v_request_hash,
    v_item_keys,jsonb_array_length(p_transition_items),'IN_PROGRESS',p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts where environment=v_environment
    and actor_class='OFFICE_ADMIN' and operation_class='AUTHORITY_TRANSITION' and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash then raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED'; end if;
  if v_batch.state<>'IN_PROGRESS' then
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  select coalesce(sd.candidate_app_feature_flags_json->'candidate_daily_enabled'='true'::jsonb,false)
    into v_global_enabled
  from public.settings_defaults sd where sd.id=1 for update;
  -- R17 effective lock order: every syntactically safe SOURCE identity first,
  -- then Candidate authority scopes. The trigger reacquires this same xact lock.
  for v_source_lock_identity in
    select distinct
      v_environment||':SOURCE:'||(x.source_link->>'hmac_key_version')||':'||
        (x.source_link->>'identifier_hmac') as lock_identity
    from jsonb_array_elements(p_transition_items) as item(value)
    cross join lateral (
      select item.value->'source_link' as source_link
    ) x
    where item.value ? 'source_link'
      and x.source_link is not null
      and jsonb_typeof(x.source_link)='object'
      and x.source_link ?& array['identifier_hmac','hmac_key_version']
      and x.source_link->>'identifier_hmac' ~ '^[a-f0-9]{64}$'
      and x.source_link->>'hmac_key_version' ~ '^[1-9]\d*$'
      and length(x.source_link->>'hmac_key_version')<=10
      and (
        length(x.source_link->>'hmac_key_version')<10
        or x.source_link->>'hmac_key_version'<='2147483647'
      )
    order by lock_identity
  loop
    perform pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended(v_source_lock_identity,0)
    );
  end loop;

  perform 1 from private.candidate_daily_authority_scopes s where s.environment=v_environment
    and s.candidate_id in (select (i->>'candidate_id')::uuid from jsonb_array_elements(p_transition_items)i)
    order by s.candidate_id for update;
  for v_item in select value from jsonb_array_elements(p_transition_items) loop
    v_error:=null; v_scope:=null; v_generation:=null; v_sync:=null; v_link:=null;
    v_link_group:=null; v_link_state:=null; v_source_link_changed:=false;
    v_prior_mode:=null; v_new_mode:=null; v_entitlement_before:=false; v_entitlement_after:=null;
    v_expected_mode:=null; v_expected_version:=null; v_expected_entitlement:=null;
    v_expected_generation_id:=null; v_expected_generation_version:=null;
    v_expected_accepted_cursor:=null; v_expected_required_cursor:=null; v_expected_effective_cursor:=null;
    v_requested_disposition:=null; v_actual_disposition:=null; v_strict_barrier:=false;
    v_latest_fact_at:=null;
    v_source_primary_count:=0; v_source_group_count:=0; v_generation_day_count:=0;
    v_pending_count:=0; v_claimed_count:=0; v_retry_count:=0; v_deferred_count:=0;
    v_terminal_count:=0; v_invalid_overlay_count:=0; v_command_count:=0;
    v_other_batch_count:=0; v_effect_count:=0; v_unknown_effect_count:=0;
    begin
      v_now:=clock_timestamp();
      v_candidate_id:=(v_item->>'candidate_id')::uuid;
      if v_membership_activation then
        if v_candidate_id<>v_membership_candidate_id then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        select * into v_membership_link
        from public.candidate_app_global_membership_links l
        where l.membership_id=v_membership_id
          and l.candidate_id=v_candidate_id
          and l.membership_generation=v_membership_generation
          and l.state='ACTIVE'
        for share;
        if v_membership_link.membership_id is null then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
      end if;
      if not(v_item ?& array['candidate_id','expected_authority_mode','expected_canonical_version',
        'expected_entitlement_enabled','new_authority_mode','entitlement_enabled','in_flight_disposition'])
         or v_item->>'expected_canonical_version' !~ '^\d+$'
         or v_item->'expected_entitlement_enabled' not in ('true'::jsonb,'false'::jsonb)
         or v_item->'entitlement_enabled' not in ('true'::jsonb,'false'::jsonb) then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      v_expected_mode:=upper(v_item->>'expected_authority_mode');
      v_expected_version:=(v_item->>'expected_canonical_version')::bigint;
      v_expected_entitlement:=(v_item->>'expected_entitlement_enabled')::boolean;
      v_new_mode:=upper(v_item->>'new_authority_mode');
      v_entitlement_after:=(v_item->>'entitlement_enabled')::boolean;
      v_requested_disposition:=upper(v_item->>'in_flight_disposition');
      if v_expected_mode not in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
         or v_new_mode not in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
         or v_requested_disposition not in ('DRAINED','CANCELLED','RECONCILED','NONE') then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      select * into v_scope from private.candidate_daily_authority_scopes
        where environment=v_environment and candidate_id=v_candidate_id for update;
      if v_scope.candidate_id is null then
        raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
      end if;
      if v_scope.transition_in_progress then
        raise exception using errcode='55000',message='BATCH_IN_PROGRESS';
      end if;
      v_prior_mode:=v_scope.authority_mode;
      if v_expected_mode<>v_prior_mode or v_expected_version<>v_scope.canonical_version then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      select coalesce(e.enabled,false) into v_entitlement_before
      from private.candidate_daily_entitlements e
      where e.environment=v_environment and e.candidate_id=v_candidate_id for update;
      v_entitlement_before:=coalesce(v_entitlement_before,false);
      if v_expected_entitlement<>v_entitlement_before then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if not ((v_prior_mode='GOOGLE_PRIMARY' and v_new_mode in ('GOOGLE_PRIMARY','SUPABASE_PRIMARY'))
        or (v_prior_mode='SUPABASE_PRIMARY' and v_new_mode in ('SUPABASE_PRIMARY','ROLLBACK_PENDING'))
        or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode in ('ROLLBACK_PENDING','GOOGLE_PRIMARY'))) then
        raise exception using errcode='22023',message='SEMANTIC_REJECTION';
      end if;
      v_link:=v_item->'source_link';
      if v_prior_mode=v_new_mode and v_entitlement_before=v_entitlement_after and v_link is null then
        if v_requested_disposition<>'NONE' then
          raise exception using errcode='40001',message='SEMANTIC_REJECTION';
        end if;
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','NO_CHANGE',
          'candidate_id',v_candidate_id,'authority_mode',v_prior_mode,
          'entitlement_enabled',v_entitlement_before));
        v_index:=v_index+1;
        continue;
      end if;
      if v_new_mode in ('GOOGLE_PRIMARY','ROLLBACK_PENDING') and v_entitlement_after then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if v_entitlement_after and not v_global_enabled then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if v_new_mode in ('ROLLBACK_PENDING','GOOGLE_PRIMARY')
         and v_prior_mode in ('SUPABASE_PRIMARY','ROLLBACK_PENDING') and v_global_enabled then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      update private.candidate_daily_authority_scopes set transition_in_progress=true,updated_at_utc=v_now
      where environment=v_environment and candidate_id=v_candidate_id;
      if v_link is not null then
        if jsonb_typeof(v_link)<>'object' or not(v_link ?& array['identifier_hmac','hmac_key_version'])
           or v_link->>'identifier_hmac' !~ '^[a-f0-9]{64}$'
           or v_link->>'hmac_key_version' !~ '^[1-9]\d*$'
           or length(v_link->>'hmac_key_version')>10
           or (length(v_link->>'hmac_key_version')=10
             and v_link->>'hmac_key_version'>'2147483647')
           or (nullif(v_link->>'link_group_id','') is not null and
             v_link->>'link_group_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_link_state:=upper(coalesce(nullif(v_link->>'state',''),'PRIMARY'));
        if v_link_state not in ('PRIMARY','OVERLAP') then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_link_group:=coalesce(nullif(v_link->>'link_group_id','')::uuid,gen_random_uuid());
        insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
          canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,rotation_receipt_id,
          actor_user_id,independent_approver_user_id,evidence_sha256)
        values(v_environment,v_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',v_link_group,
          v_link->>'identifier_hmac',(v_link->>'hmac_key_version')::integer,
          v_link_state,v_batch.batch_receipt_id,v_actor,
          p_independent_approver,p_evidence_sha256)
        on conflict(environment,source_system,hmac_key_version,identifier_hmac) where state in ('PRIMARY','OVERLAP')
        do nothing;
        if not exists(select 1 from private.candidate_daily_source_links l where l.environment=v_environment
          and l.candidate_id=v_candidate_id and l.identifier_hmac=v_link->>'identifier_hmac'
          and l.hmac_key_version=(v_link->>'hmac_key_version')::integer and l.state in ('PRIMARY','OVERLAP')) then
          raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
        end if;
        v_source_link_changed:=true;
      end if;

      perform 1 from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
      order by l.link_id for update;
      select count(*) filter(where l.state='PRIMARY' and l.valid_from_utc<=v_now
          and (l.valid_to_utc is null or l.valid_to_utc>v_now)),
        count(distinct l.link_group_id) filter(where l.state in ('PRIMARY','OVERLAP')
          and l.valid_from_utc<=v_now and (l.valid_to_utc is null or l.valid_to_utc>v_now))
      into v_source_primary_count,v_source_group_count
      from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID';
      if (v_source_link_changed or v_new_mode='SUPABASE_PRIMARY'
          or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode='GOOGLE_PRIMARY'))
         and v_source_primary_count=0 then
        raise exception using errcode='55000',message='IDENTITY_LINK_MISSING';
      end if;
      if (v_source_link_changed or v_new_mode='SUPABASE_PRIMARY'
          or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode='GOOGLE_PRIMARY'))
         and (v_source_primary_count<>1 or v_source_group_count<>1) then
        raise exception using errcode='55000',message='IDENTITY_LINK_AMBIGUOUS';
      end if;

      perform 1 from public.candidate_daily_command_receipts c
      where c.environment=v_environment and c.candidate_id=v_candidate_id and c.state='IN_PROGRESS'
      order by c.command_id for update;
      select count(*) into v_command_count from public.candidate_daily_command_receipts c
      where c.environment=v_environment and c.candidate_id=v_candidate_id and c.state='IN_PROGRESS';
      perform 1 from private.candidate_daily_batch_receipts b
      where b.environment=v_environment and b.state='IN_PROGRESS'
        and b.batch_receipt_id<>v_batch.batch_receipt_id
        and b.item_keys_json @> jsonb_build_array(v_candidate_id::text)
      order by b.batch_receipt_id for update;
      select count(*) into v_other_batch_count from private.candidate_daily_batch_receipts b
      where b.environment=v_environment and b.state='IN_PROGRESS'
        and b.batch_receipt_id<>v_batch.batch_receipt_id
        and b.item_keys_json @> jsonb_build_array(v_candidate_id::text);
      perform 1 from private.candidate_daily_external_effect_receipts e
      where e.environment=v_environment and e.candidate_id=v_candidate_id and e.state in ('IN_PROGRESS','UNKNOWN')
      order by e.effect_receipt_id for update;
      select count(*),count(*) filter(where e.state='UNKNOWN')
      into v_effect_count,v_unknown_effect_count
      from private.candidate_daily_external_effect_receipts e
      where e.environment=v_environment and e.candidate_id=v_candidate_id and e.state in ('IN_PROGRESS','UNKNOWN');
      perform 1 from public.candidate_daily_sheet_projection_outbox o
      where o.environment=v_environment and o.candidate_id=v_candidate_id
        and o.target='MASTER_AVAILABILITY_SHEET'
      order by o.availability_version,o.outbox_id for update;
      select count(*) filter(where o.state='PENDING'),count(*) filter(where o.state='CLAIMED'),
        count(*) filter(where o.state='RETRY'),count(*) filter(where o.state='DEFERRED_OVERLAY'),
        count(*) filter(where o.state='TERMINAL')
      into v_pending_count,v_claimed_count,v_retry_count,v_deferred_count,v_terminal_count
      from public.candidate_daily_sheet_projection_outbox o
      where o.environment=v_environment and o.candidate_id=v_candidate_id
        and o.target='MASTER_AVAILABILITY_SHEET';
      v_actual_disposition:=case
        when v_pending_count+v_claimed_count+v_retry_count+v_terminal_count+v_command_count
          +v_other_batch_count+v_effect_count>0 then 'NONE'
        when v_deferred_count>0 then 'RECONCILED'
        else 'DRAINED' end;
      if v_requested_disposition<>v_actual_disposition then
        raise exception using errcode='40001',message='SEMANTIC_REJECTION';
      end if;
      if v_prior_mode<>v_new_mode and v_actual_disposition='NONE' then
        raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
      end if;

      v_strict_barrier:=v_new_mode='SUPABASE_PRIMARY'
        or (v_prior_mode='ROLLBACK_PENDING' and v_new_mode='GOOGLE_PRIMARY');
      if v_strict_barrier then
        if not(v_item ?& array['expected_generation_id','expected_generation_version',
          'expected_accepted_canonical_cursor','expected_required_visible_cursor','expected_effective_visible_cursor'])
           or v_item->>'expected_generation_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
           or v_item->>'expected_generation_version' !~ '^[1-9]\d*$'
           or v_item->>'expected_accepted_canonical_cursor' !~ '^\d+$'
           or v_item->>'expected_required_visible_cursor' !~ '^\d+$'
           or v_item->>'expected_effective_visible_cursor' !~ '^\d+$' then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_expected_generation_id:=(v_item->>'expected_generation_id')::uuid;
        v_expected_generation_version:=(v_item->>'expected_generation_version')::bigint;
        v_expected_accepted_cursor:=(v_item->>'expected_accepted_canonical_cursor')::bigint;
        v_expected_required_cursor:=(v_item->>'expected_required_visible_cursor')::bigint;
        v_expected_effective_cursor:=(v_item->>'expected_effective_visible_cursor')::bigint;
        if v_scope.active_generation_id is null or v_scope.active_generation_id<>v_expected_generation_id then
          raise exception using errcode='40001',message='SEMANTIC_REJECTION';
        end if;
        select * into v_generation from public.candidate_daily_rota_generations g
        where g.generation_id=v_scope.active_generation_id and g.environment=v_environment
          and g.candidate_id=v_candidate_id for update;
        select count(*) into v_generation_day_count from public.candidate_daily_rota_days d
        where d.generation_id=v_scope.active_generation_id and d.environment=v_environment
          and d.candidate_id=v_candidate_id;
        v_now:=clock_timestamp();
        if v_generation.generation_id is null or v_generation.state<>'ACTIVE'
           or v_generation.generation_version<>v_expected_generation_version
           or v_generation.expected_day_count<>14 or v_generation.actual_day_count<>14
           or v_generation_day_count<>14 or v_generation.activated_at_utc is null
           or v_generation.published_at_utc is null then
          raise exception using errcode='55000',message='GENERATION_INCOMPLETE';
        end if;
        if not v_membership_activation
           and v_generation.published_at_utc<v_now-interval '120 seconds' then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        if v_membership_activation then
          v_today:=(v_now at time zone 'Europe/London')::date;
          if v_generation.window_start<>v_today
             or v_generation.window_end<>v_today+13 then
            raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
          end if;
        end if;
        select * into v_sync from private.candidate_daily_sync_state ss
        where ss.environment=v_environment and ss.candidate_id=v_candidate_id
          and ss.target='MASTER_AVAILABILITY_SHEET' for update;
        if v_sync.candidate_id is null then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        perform private._candidate_daily_refresh_sync_state_v1(
          v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_now);
        select * into v_sync from private.candidate_daily_sync_state ss
        where ss.environment=v_environment and ss.candidate_id=v_candidate_id
          and ss.target='MASTER_AVAILABILITY_SHEET' for update;
        select count(*) into v_invalid_overlay_count
        from public.candidate_daily_sheet_projection_outbox o
        where o.environment=v_environment and o.candidate_id=v_candidate_id
          and o.target='MASTER_AVAILABILITY_SHEET' and o.state='DEFERRED_OVERLAY'
          and not(o.overlay_generation_id=v_generation.generation_id
            and o.overlay_generation_version=v_generation.generation_version
            and exists(select 1 from public.candidate_daily_rota_days d
              where d.generation_id=v_generation.generation_id and d.environment=v_environment
                and d.candidate_id=v_candidate_id and d.rota_date=o.availability_date
                and (d.booked or d.system_blocked) and d.source_row_hash=o.overlay_source_row_hash));
        if v_invalid_overlay_count>0 then
          raise exception using errcode='55000',message='PROJECTION_STALE_COMPLETION';
        end if;
        if v_pending_count+v_claimed_count+v_retry_count+v_terminal_count+v_command_count
             +v_other_batch_count+v_effect_count>0 or v_unknown_effect_count>0 then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        if v_sync.accepted_canonical_cursor<>v_expected_accepted_cursor
           or v_sync.required_visible_cursor<>v_expected_required_cursor
           or v_sync.effective_visible_cursor<>v_expected_effective_cursor
           or v_expected_accepted_cursor<>v_scope.canonical_version
           or v_expected_required_cursor<>v_scope.canonical_version
           or v_expected_effective_cursor<>v_scope.canonical_version
           or v_sync.state<>'READY' or v_sync.pending_count<>0 or v_sync.retry_count<>0
           or v_sync.terminal_count<>0
           or (not v_membership_activation and (
             nullif(v_sync.observed_source_revision,'') is null
             or v_sync.last_reconciled_at_utc is null
           )) then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
        select greatest(v_generation.published_at_utc,
          coalesce(max(a.changed_at_utc),v_generation.published_at_utc),
          coalesce((select max(o.updated_at_utc) from public.candidate_daily_sheet_projection_outbox o
            where o.environment=v_environment and o.candidate_id=v_candidate_id
              and o.target='MASTER_AVAILABILITY_SHEET'),v_generation.published_at_utc))
        into v_latest_fact_at
        from public.candidate_daily_availability_days a
        where a.environment=v_environment and a.candidate_id=v_candidate_id;
        if not v_membership_activation
           and v_sync.last_reconciled_at_utc<v_latest_fact_at then
          raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
        end if;
      elsif v_scope.active_generation_id is not null then
        select * into v_generation from public.candidate_daily_rota_generations g
        where g.generation_id=v_scope.active_generation_id and g.environment=v_environment
          and g.candidate_id=v_candidate_id;
        select * into v_sync from private.candidate_daily_sync_state ss
        where ss.environment=v_environment and ss.candidate_id=v_candidate_id
          and ss.target='MASTER_AVAILABILITY_SHEET';
      end if;
      v_transition_id:=gen_random_uuid();
      insert into private.candidate_daily_authority_transitions(transition_id,batch_receipt_id,environment,
        candidate_id,prior_authority_mode,new_authority_mode,effective_at_utc,canonical_version_snapshot,
        generation_id_snapshot,generation_version_snapshot,sync_snapshot_json,in_flight_disposition,
        entitlement_before,entitlement_after,actor_user_id,independent_approver_user_id,reason,
        evidence_sha256,outcome,correlation_id)
      select v_transition_id,v_batch.batch_receipt_id,v_environment,v_candidate_id,v_prior_mode,v_new_mode,now(),
        v_scope.canonical_version,v_scope.active_generation_id,v_generation.generation_version,
        coalesce(to_jsonb(v_sync),'{}'::jsonb),
        v_actual_disposition,v_entitlement_before,v_entitlement_after,v_actor,
        p_independent_approver,p_reason,p_evidence_sha256,'COMMITTED',p_correlation_id
      from (values(1))x(n);
      insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,valid_from_utc,valid_to_utc,
        actor_user_id,reason,evidence_sha256)
      values(v_environment,v_candidate_id,v_entitlement_after,case when v_entitlement_after then now() else null end,
        case when v_entitlement_after then null else now() end,v_actor,p_reason,p_evidence_sha256)
      on conflict(environment,candidate_id) do update set enabled=excluded.enabled,
        valid_from_utc=excluded.valid_from_utc,valid_to_utc=excluded.valid_to_utc,actor_user_id=excluded.actor_user_id,
        reason=excluded.reason,evidence_sha256=excluded.evidence_sha256,updated_at_utc=now();
      update private.candidate_daily_authority_scopes set authority_mode=v_new_mode,
        last_transition_id=v_transition_id,transition_in_progress=false,updated_at_utc=now()
        where environment=v_environment and candidate_id=v_candidate_id;
      v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','COMMITTED',
        'candidate_id',v_candidate_id,'transition_id',v_transition_id,'authority_mode',v_new_mode,
        'entitlement_enabled',v_entitlement_after));
    exception when others then
      v_error:=sqlerrm;
      if v_error in ('VALIDATION_FAILED','SEMANTIC_REJECTION','SOURCE_EVENT_CONFLICT',
        'CANDIDATE_DAILY_NOT_READY','GENERATION_INCOMPLETE','IDENTITY_LINK_MISSING',
        'IDENTITY_LINK_AMBIGUOUS','IDENTITY_LINK_CONFLICT','BATCH_IN_PROGRESS','COMMAND_IN_PROGRESS',
        'EFFECT_STATUS_UNKNOWN','PROJECTION_STALE_COMPLETION') then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REJECTED','error_code',v_error));
      else raise; end if;
    end;
    v_index:=v_index+1;
  end loop;
  v_terminal:=jsonb_build_object('batch_receipt_id',v_batch.batch_receipt_id,'outcomes',v_outcomes);
  update private.candidate_daily_batch_receipts set state='COMPLETED',terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

revoke all on function public.candidate_daily_authority_transition_atomic_v1(
  jsonb,uuid,text,jsonb,uuid,text,text,text
) from public;

do $grants$
begin
  if exists(select 1 from pg_roles where rolname='anon') then
    revoke all on function public.candidate_daily_authority_transition_atomic_v1(
      jsonb,uuid,text,jsonb,uuid,text,text,text
    ) from anon;
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') then
    revoke all on function public.candidate_daily_authority_transition_atomic_v1(
      jsonb,uuid,text,jsonb,uuid,text,text,text
    ) from authenticated;
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    grant execute on function public.candidate_daily_authority_transition_atomic_v1(
      jsonb,uuid,text,jsonb,uuid,text,text,text
    ) to service_role;
  end if;
end;
$grants$;


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
  v_membership_activation boolean:=false;
  v_membership_id uuid;
  v_membership_generation integer;
  v_membership_candidate_id uuid;
  v_membership_link public.candidate_app_global_membership_links%rowtype;
begin
  v_context:=private._candidate_daily_context_v1(
    p_internal_context,'SIGNED_SYSTEM_SYNC',false
  );
  v_environment:=v_context->>'environment';
  v_membership_activation:=
    p_internal_context->>'activation_reason'='FEDERATED_MEMBERSHIP_ACTIVE';
  if jsonb_typeof(coalesce(p_candidate_source_hmacs,'[]'::jsonb))<>'array'
     or jsonb_typeof(coalesce(p_projection_outbox_ids,'[]'::jsonb))<>'array'
     or jsonb_array_length(coalesce(p_candidate_source_hmacs,'[]'::jsonb))>100
     or jsonb_array_length(coalesce(p_projection_outbox_ids,'[]'::jsonb))>100
     or (jsonb_array_length(coalesce(p_candidate_source_hmacs,'[]'::jsonb))=0
       and jsonb_array_length(coalesce(p_projection_outbox_ids,'[]'::jsonb))=0
       and not v_membership_activation)
     or coalesce(p_correlation_id,'')!~'^[0-7][0-9A-HJKMNP-TV-Z]{25}$'
     or exists(select 1 from pg_catalog.jsonb_array_elements_text(
       coalesce(p_candidate_source_hmacs,'[]'::jsonb)
     ) h(value) where h.value!~'^[a-f0-9]{64}$')
     or exists(select 1 from pg_catalog.jsonb_array_elements_text(
       coalesce(p_projection_outbox_ids,'[]'::jsonb)
     ) i(value) where i.value!~'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
  then raise exception using errcode='22023',message='VALIDATION_FAILED'; end if;

  if v_membership_activation then
    begin
      v_membership_id:=(p_internal_context->>'membership_id')::uuid;
      v_membership_generation:=(p_internal_context->>'membership_generation')::integer;
      v_membership_candidate_id:=(p_internal_context->>'candidate_id')::uuid;
    exception when others then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end;
    if v_membership_id is null or v_membership_candidate_id is null
       or coalesce(v_membership_generation,0)<1 then
      raise exception using errcode='22023',message='VALIDATION_FAILED';
    end if;
    select * into v_membership_link
    from public.candidate_app_global_membership_links l
    where l.membership_id=v_membership_id
      and l.candidate_id=v_membership_candidate_id
      and l.membership_generation=v_membership_generation
      and l.state='ACTIVE';
    if v_membership_link.membership_id is null then
      raise exception using errcode='55000',message='CANDIDATE_DAILY_NOT_READY';
    end if;
  end if;

  select coalesce(
    s.candidate_app_feature_flags_json->'candidate_daily_enabled'='true'::jsonb,false
  ) into v_global_enabled from public.settings_defaults s where s.id=1;

  for v_candidate_id in
    select distinct candidates.candidate_id
    from (
      select private._candidate_daily_source_candidate_v1(v_environment,h.value) candidate_id
      from pg_catalog.jsonb_array_elements_text(
        coalesce(p_candidate_source_hmacs,'[]'::jsonb)
      ) h(value)
      union all
      select o.candidate_id
      from pg_catalog.jsonb_array_elements_text(
        coalesce(p_projection_outbox_ids,'[]'::jsonb)
      ) i(value)
      join public.candidate_daily_sheet_projection_outbox o
        on o.outbox_id=i.value::uuid and o.environment=v_environment
      union all
      select v_membership_candidate_id where v_membership_activation
    ) candidates order by candidates.candidate_id
  loop
    v_status:='NOT_READY';
    begin
      select * into v_candidate from public.candidates c
      where c.id=v_candidate_id and c.active is true;
      select * into v_scope from private.candidate_daily_authority_scopes s
      where s.environment=v_environment and s.candidate_id=v_candidate_id;
      select coalesce(e.enabled,false) into v_entitlement
      from private.candidate_daily_entitlements e
      where e.environment=v_environment and e.candidate_id=v_candidate_id;
      v_entitlement:=coalesce(v_entitlement,false);
      if v_entitlement and v_scope.authority_mode='SUPABASE_PRIMARY' then
        v_status:='ALREADY_ENABLED';
      elsif not v_global_enabled or v_candidate.id is null
         or upper(btrim(coalesce(v_candidate.key_norm,'')))!~'^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
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
           or (not v_membership_activation
             and v_generation.published_at_utc<pg_catalog.clock_timestamp()-interval '120 seconds')
           or (v_membership_activation and (
             v_generation.window_start<>(pg_catalog.clock_timestamp() at time zone 'Europe/London')::date
             or v_generation.window_end<>((pg_catalog.clock_timestamp() at time zone 'Europe/London')::date+13)
           ))
           or v_source_primary_count<>1 or v_source_group_count<>1
           or v_sync.candidate_id is null or v_sync.state<>'READY'
           or v_sync.pending_count<>0 or v_sync.retry_count<>0 or v_sync.terminal_count<>0
           or v_sync.accepted_canonical_cursor<>v_scope.canonical_version
           or v_sync.required_visible_cursor<>v_scope.canonical_version
           or v_sync.effective_visible_cursor<>v_scope.canonical_version
           or coalesce(v_blocking_count,0)<>0 then
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
            p_internal_context||pg_catalog.jsonb_build_object(
              'actor_user_id','00000000-0000-4000-8000-000000009221',
              'activation_reason',case when v_membership_activation
                then 'FEDERATED_MEMBERSHIP_ACTIVE' else 'FIRST_COMPLETE_PUBLICATION' end,
              'membership_id',v_membership_id,
              'membership_generation',v_membership_generation,
              'candidate_id',v_candidate_id
            ),gen_random_uuid(),'candidate.system.first-ready.'||gen_random_uuid()::text,
            pg_catalog.jsonb_build_array(v_transition_item),
            '00000000-0000-4000-8000-000000009222',
            case when v_membership_activation
              then 'System policy activation after active MyTMS membership linked to a current complete Rota window'
              else 'System policy activation after the first complete Candidate Rota publication' end,
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


notify pgrst, 'reload schema';

commit;
