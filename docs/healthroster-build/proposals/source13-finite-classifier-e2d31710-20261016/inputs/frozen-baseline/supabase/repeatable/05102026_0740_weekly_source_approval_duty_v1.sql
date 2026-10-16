-- Pure current approval-duty qualification. No freeze census, release or write.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_approval_duty_v1(p_root_timesheet_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb; v_family text; v_pending public.weekly_source_pending_entitlement_bundles%rowtype;
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_canonical jsonb; v_request jsonb; v_context jsonb; v_identity jsonb; v_inventory jsonb;
  v_before jsonb; v_position jsonb; v_expected_heads jsonb; v_actual_heads jsonb;
  v_raw_position jsonb; v_entitlement jsonb;
  v_ids uuid[]; v_families text[]; v_versions integer[]; v_heads uuid[];
  v_i integer; v_count integer; v_head_id uuid; v_observation boolean;
  v_local_publication_qualified boolean;
  v_present boolean:=false; v_unknown boolean:=false; v_basis jsonb:='[]'::jsonb;
begin
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null then return null; end if;
  v_family:=v_scope->>'family_booking_id';
  for v_pending in select * from public.weekly_source_pending_entitlement_bundles
    where v_family=any(member_family_booking_ids) order by id loop
    v_observation:=null;
    v_local_publication_qualified:=false;
    <<qualify>>
    begin
      select * into v_bundle from public.weekly_source_entitlement_decision_bundles
        where decision_bundle_id=v_pending.decision_bundle_id and bundle_revision=v_pending.bundle_revision;
      if not found then exit qualify; end if;
      -- Disposition belongs to the accepted common owner, not lingering Local
      -- PENDING_FREEZE/generation history. Never mutate or "release" it here.
      if v_pending.state in ('RELEASED','SUPERSEDED')
         or v_bundle.state in ('SUPERSEDED','ABANDONED') then v_observation:=false; exit qualify; end if;
      if v_pending.state not in ('PENDING','RELEASING','MANUAL_REVIEW') or v_bundle.state<>'PROPOSED'
         or v_bundle.candidate_id is distinct from v_pending.candidate_id
         or v_bundle.decision_id is distinct from v_pending.decision_id
         or v_bundle.decided_by_user_id is distinct from v_pending.decided_by_user_id
         or v_bundle.proposed_head_ids is distinct from v_pending.proposed_head_ids then exit qualify; end if;
      begin
        v_canonical:=private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'DEFERRED',v_pending.id);
        select array_agg(e.value::uuid order by e.ordinality) into v_ids
          from jsonb_array_elements_text(v_canonical->'member_root_ids') with ordinality e(value,ordinality);
        select array_agg(e.value order by e.ordinality) into v_families
          from jsonb_array_elements_text(v_canonical->'member_family_booking_ids') with ordinality e(value,ordinality);
        select array_agg(e.value::integer order by e.ordinality) into v_versions
          from jsonb_array_elements_text(v_canonical->'member_root_versions') with ordinality e(value,ordinality);
        select array_agg(e.value::uuid order by e.ordinality) into v_heads
          from jsonb_array_elements_text(v_canonical->'head_ids') with ordinality e(value,ordinality);
      exception when sqlstate '22023' or sqlstate '22P02' then exit qualify; end;
      if v_ids is distinct from v_pending.member_root_ids or v_families is distinct from v_pending.member_family_booking_ids
         or v_versions is distinct from v_pending.member_root_versions or v_heads is distinct from v_bundle.proposed_head_ids
         or v_canonical->>'decision_bundle_id' is distinct from v_bundle.decision_bundle_id::text
         or v_canonical->>'bundle_revision' is distinct from v_bundle.bundle_revision::text
         or v_canonical->>'candidate_id' is distinct from v_bundle.candidate_id::text
         or v_canonical->>'decision_id' is distinct from v_bundle.decision_id::text
         or private.weekly_source_publication_request_digest_v1(v_canonical) is distinct from v_pending.request_digest
         or private.weekly_source_publication_request_digest_v1(v_canonical#>'{financial_request,source_revision}')
           is distinct from v_bundle.source_revision_digest
         or v_pending.source_revision_digest is distinct from v_bundle.source_revision_digest
         or private.weekly_source_publication_request_digest_v1(v_canonical#>'{financial_request,contract_choices}')
           is distinct from v_bundle.contract_choice_digest
         or v_pending.contract_choice_digest is distinct from v_bundle.contract_choice_digest
         or private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_before_inventory_v1(v_pending.request_json->'control',cardinality(v_ids)))
           is distinct from v_bundle.before_inventory_digest
         or private.weekly_source_publication_request_digest_v1(
           private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'IMMEDIATE',null::uuid))
           is distinct from v_bundle.request_digest then exit qualify; end if;
      if cardinality(v_ids) not between 1 and 2 or not p_root_timesheet_id=any(v_ids)
         or v_bundle.source_root_timesheet_id is distinct from v_ids[1]
         or btrim(v_bundle.source_root_family_booking_id) is distinct from btrim(v_families[1])
         or (v_bundle.bundle_kind='SINGLE_ROOT') is distinct from (cardinality(v_ids)=1)
         or (cardinality(v_ids)=2 and (v_bundle.target_root_timesheet_id is distinct from v_ids[2]
           or btrim(v_bundle.target_root_family_booking_id) is distinct from btrim(v_families[2]))) then exit qualify; end if;
      v_actual_heads:='[]'::jsonb;
      for v_i in 1..cardinality(v_ids) loop
        v_identity:=private.weekly_source_resolve_root_identity_v1(v_ids[v_i]);
        if v_identity->>'ok' is distinct from 'true'
           or v_identity->>'canonical_timesheet_id' is distinct from v_ids[v_i]::text
           or v_identity->>'canonical_version' is distinct from v_versions[v_i]::text
           or v_identity->>'family_booking_id' is distinct from v_families[v_i] then exit qualify; end if;
        if not exists(select 1 from public.timesheets t join public.contracts c on c.id=t.contract_id
          where t.timesheet_id=v_ids[v_i] and c.candidate_id=v_bundle.candidate_id
            and t.week_ending_date=v_bundle.week_ending_date
            and t.contract_id=case when v_i=1 then v_bundle.source_contract_id else v_bundle.target_contract_id end)
          then exit qualify; end if;
        select count(*),min(h.id::text)::uuid into v_count,v_head_id from public.weekly_source_entitlement_heads h
          where btrim(h.root_family_booking_id)=btrim(v_families[v_i]) and h.state='COMMITTED_CURRENT';
        if v_count>1 or (v_head_id is not null and not exists(select 1 from public.weekly_source_entitlement_heads
          where id=v_head_id and root_timesheet_id=v_ids[v_i]))
          or (v_head_id is null and exists(select 1 from public.weekly_source_entitlement_heads
            where root_timesheet_id=v_ids[v_i] and state='COMMITTED_CURRENT')) then exit qualify; end if;
        v_actual_heads:=v_actual_heads||jsonb_build_array(v_head_id);
      end loop;
      if v_canonical#>>'{financial_request,source_revision,origin_kind}'='PROTECTED_LOCAL_DECISION_V1' then
        -- Read the same owner readiness facts without calling the publisher's
        -- volatile FOR SHARE admission helper from this pure STABLE reader.
        -- NEXT also proves that the legacy financial callback is disabled.
        if cardinality(v_ids)<>1
           or not exists(select 1 from private.bpay_next_module_control
             where id=1 and active_owner='NEXT' and owner_epoch>0)
           or pg_catalog.to_regprocedure('private.weekly_source_local_publication_context_v2(uuid,uuid,uuid)') is null
           or pg_catalog.to_regprocedure('private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)') is null
           or pg_catalog.to_regprocedure('private.bpay_next_capture_local_source_detail_v1(uuid,uuid,bigint,uuid,uuid,uuid,jsonb)') is null
           then exit qualify; end if;
        begin
          v_context:=private.weekly_source_local_publication_context_v2(
            (v_canonical#>>'{financial_request,source_revision,publication_request_id}')::uuid,
            (v_canonical#>>'{financial_request,source_revision,generation_id}')::uuid,v_ids[1]);
          if v_context is null or v_context->>'actor_user_id' is distinct from v_bundle.decided_by_user_id::text
             or v_context->>'agency_id' is distinct from v_bundle.agency_id::text then exit qualify; end if;
          v_request:=private.weekly_source_entitlement_proposal_request_v2(v_ids[1],
            v_canonical#>'{financial_request,source_revision}','PROTECTED',v_bundle.decision_bundle_id,
            v_bundle.bundle_revision,v_heads[1],v_bundle.decision_id,
            v_canonical#>'{financial_request,member_entitlements,0,components}','UNCHANGED');
          if private.weekly_source_publication_request_canonical_v1(v_request,'DEFERRED',v_pending.id)
             is distinct from v_canonical
             or private.weekly_source_publication_before_inventory_v1(v_request->'control',1)
               is distinct from private.weekly_source_publication_before_inventory_v1(v_pending.request_json->'control',1)
             then exit qualify; end if;
          v_local_publication_qualified:=true;
        exception when sqlstate '55000' or sqlstate '22023' or sqlstate '22P02' then exit qualify; end;
      elsif not exists(select 1 from public.weekly_source_final_revisions f
        where f.id=(v_canonical#>>'{financial_request,source_revision,final_revision_id}')::uuid
          and f.source_cycle_id=(v_canonical#>>'{financial_request,source_revision,source_cycle_id}')::uuid
          and f.revision_number=(v_canonical#>>'{financial_request,source_revision,revision_number}')::integer
          and f.state='CURRENT' and encode(f.manifest_hash,'hex')=v_canonical#>>'{financial_request,source_revision,manifest_hash}'
          and encode(f.policy_fingerprint,'hex')=v_canonical#>>'{financial_request,source_revision,policy_fingerprint}') then
        v_observation:=false; exit qualify;
      end if;
      begin
        select jsonb_agg(to_jsonb(e.value::uuid) order by e.ordinality) into v_expected_heads
          from jsonb_array_elements_text(v_pending.request_json#>'{control,expected_current_head_ids}')
            with ordinality e(value,ordinality);
        if jsonb_array_length(v_expected_heads) is distinct from cardinality(v_ids) then exit qualify; end if;
      exception when sqlstate '22023' or sqlstate '22P02' then exit qualify; end;
      if v_actual_heads is distinct from v_expected_heads then
        -- Follow the actual common owner order: producer qualification FIRST.
        -- Local-context NULL above remains unknown, never guessed supersession.
        v_observation:=false; exit qualify; end if;
      v_before:=private.weekly_source_publication_before_inventory_v1(v_pending.request_json->'control',cardinality(v_ids));
      for v_i in 1..cardinality(v_ids) loop
        v_inventory:=private.weekly_source_effective_inventory_v1(v_ids[v_i]);
        select value into v_position from jsonb_array_elements(v_before)
          where (value->>'root_ordinal')::integer=v_i;
        select value into v_raw_position from jsonb_array_elements(v_pending.request_json#>'{control,before_positions}')
          where (value->>'root_ordinal')::integer=v_i;
        if v_inventory->>'ok' is distinct from 'true'
           or (v_raw_position ? 'inventory_digest' and v_inventory->>'inventory_digest' is distinct from v_raw_position->>'inventory_digest')
           or (select coalesce(jsonb_agg(c.value->'component_id' order by c.value->>'component_id'),'[]'::jsonb)
             from jsonb_array_elements(v_inventory->'components') c(value)) is distinct from v_position->'component_ids'
           then exit qualify; end if;
        select value into v_entitlement from jsonb_array_elements(v_canonical#>'{financial_request,member_entitlements}')
          where (value->>'root_ordinal')::integer=v_i;
        -- The common publisher permits a same-component content amendment only
        -- after qualifying this exact sealed Local producer and before-position.
        -- Keep the retained-content guard for every other origin.
        if not v_local_publication_qualified and exists(select 1 from jsonb_array_elements(v_entitlement->'components') proposed(value)
          join jsonb_array_elements(v_inventory->'components') retained(value)
            on retained.value->>'component_id'=proposed.value->>'component_id'
          where retained.value->>'component_sha256' is distinct from encode(
            private.weekly_source_publication_request_digest_v1(
              private.weekly_source_publication_component_content_v1(proposed.value)),'hex')) then exit qualify; end if;
      end loop;
      v_observation:=true;
    end qualify;
    v_present:=v_present or v_observation is true; v_unknown:=v_unknown or v_observation is null;
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('pending',to_jsonb(v_pending),
      'accepted_bundle',to_jsonb(v_bundle),'present',v_observation));
  end loop;
  -- Direct proposals carry no saved request. Digests cannot reconstruct the
  -- complete vector; without their actual pending owner, remain unavailable.
  for v_bundle in select b.* from public.weekly_source_entitlement_decision_bundles b
    where b.state='PROPOSED' and (btrim(b.source_root_family_booking_id)=btrim(v_family)
      or btrim(b.target_root_family_booking_id)=btrim(v_family))
      and not exists(select 1 from public.weekly_source_pending_entitlement_bundles p
        where p.decision_bundle_id=b.decision_bundle_id and p.bundle_revision=b.bundle_revision) order by decision_bundle_id loop
    v_unknown:=true;
    v_basis:=v_basis||jsonb_build_array(jsonb_build_object('accepted_bundle',to_jsonb(v_bundle),'present',null));
  end loop;
  return jsonb_build_object('scope',v_scope,'present',case when v_present then true when v_unknown then null else false end,
    'discovery_complete',not v_unknown,'basis',v_basis);
end;
$function$;
alter function private.weekly_source_approval_duty_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_approval_duty_v1(uuid) from public,anon,authenticated,service_role;
commit;
