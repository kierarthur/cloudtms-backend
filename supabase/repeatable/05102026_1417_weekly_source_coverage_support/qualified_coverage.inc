-- Canonical transaction-neutral qualified-coverage definitions.
-- Included inside each owning repeatable's atomic transaction, before SQL
-- readers are parsed. One body authority; no financial/release transitions.

-- Factual admission only. No reader locks, monetary hold, zero entitlement,
-- financial write, invoice change or present-day recheck of a retained Draft.
create or replace function private.weekly_source_pay_query_facts_v1(p_root_timesheet_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_scope jsonb; v_manual jsonb; v_hours jsonb:='[]'::jsonb;
  v_blocked boolean:=false; v_unavailable boolean:=false;
  v_row record; v_latest public.weekly_exceptional_pay_family_events%rowtype;
  v_generation uuid; v_count integer; v_approval_run uuid;
  v_invalid_wait_audit boolean;
  v_local_count integer; v_c1_count integer; v_next_count integer; v_audit_count integer;
  v_local jsonb; v_c1 jsonb; v_next jsonb; v_receipt jsonb; v_exception jsonb;
  v_state jsonb; v_digest bytea;
begin
  if p_root_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_PAY_QUERY_ROOT_REQUIRED' using errcode='22023'; end if;
  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
  if v_scope is null then
    return jsonb_build_object('ok',false,'code','WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE',
      'scope',null,'blocked',null,'query_state_sha256',null); end if;

  -- Work membership is the actual configured root week, across every owning
  -- report cutoff. Never substitute a Sunday or the report's cutoff period.
  select coalesce(jsonb_agg(jsonb_build_object('id',r.id,'source_group_id',r.source_group_id,
    'source_cycle_id',r.source_cycle_id,'work_event_id',r.work_event_id,
    'candidate_id',r.candidate_id,'client_id',r.client_id,'contract_id',r.contract_id,
    'work_date',to_char(r.work_date,'YYYY-MM-DD'),'state',r.state) order by r.id),'[]'::jsonb),
    coalesce(bool_or(e.id is null or e.candidate_id is distinct from r.candidate_id
      or e.client_id is distinct from r.client_id or e.work_date is distinct from r.work_date
      or c.source_group_id is distinct from r.source_group_id),false)
  into v_manual,v_unavailable
  from private.weekly_source_manual_reviews r
  left join public.weekly_work_events e on e.id=r.work_event_id
  left join public.weekly_source_cycles c on c.id=r.source_cycle_id
  where r.state='OPEN' and r.candidate_id=(v_scope->>'candidate_id')::uuid
    and r.client_id=(v_scope->>'client_id')::uuid and r.contract_id=(v_scope->>'contract_id')::uuid
    and r.work_date between (v_scope->>'week_ending_date')::date-6 and (v_scope->>'week_ending_date')::date;
  v_blocked:=jsonb_array_length(v_manual)>0;

  for v_row in
    select i.*,e.work_date,e.candidate_id as event_candidate_id,e.client_id as event_client_id,
      x.id as comparison_id,x.incident_id as comparison_incident_id,x.contract_id as comparison_contract_id,
      owning.id as owning_comparison_id,owning.contract_id as owning_contract_id,
      tail.id as tail_comparison_id,
      x.issue_family,encode(x.material_comparison_fingerprint,'hex') as material_fingerprint,
      c.source_group_id as cycle_group_id
    from public.weekly_discrepancy_incidents i
    left join public.weekly_work_events e on e.id=i.work_event_id
    -- These immutable incident-owned revisions establish relevance independently
    -- of the suspect current pointer. The unique incident/revision index bounds
    -- both the first-owner point read and the actual backward-tail probe.
    left join public.weekly_issue_comparison_revisions owning
      on owning.incident_id=i.id and owning.revision_number=1
    left join lateral (select r.id from public.weekly_issue_comparison_revisions r
      where r.incident_id=i.id order by r.revision_number desc limit 1) tail on true
    left join public.weekly_issue_comparison_revisions x on x.id=i.current_comparison_revision_id
    left join public.weekly_source_cycles c on c.id=i.source_cycle_id
    where i.state='OPEN' and i.candidate_id=(v_scope->>'candidate_id')::uuid
      and i.client_id=(v_scope->>'client_id')::uuid
      and (e.work_date is null or e.work_date between
        (v_scope->>'week_ending_date')::date-6 and (v_scope->>'week_ending_date')::date)
      and (owning.contract_id is null or owning.contract_id=(v_scope->>'contract_id')::uuid)
    order by i.id
  loop
    if v_row.work_date is null or v_row.event_candidate_id is distinct from v_row.candidate_id
       or v_row.event_client_id is distinct from v_row.client_id
       or v_row.cycle_group_id is distinct from v_row.source_group_id
       or v_row.owning_comparison_id is null
       or v_row.owning_contract_id is distinct from (v_scope->>'contract_id')::uuid
       or v_row.comparison_id is null or v_row.comparison_incident_id is distinct from v_row.id
       or v_row.comparison_id is distinct from v_row.tail_comparison_id
       or v_row.comparison_contract_id is distinct from (v_scope->>'contract_id')::uuid
       or v_row.issue_family is null or v_row.material_fingerprint is null then
      v_unavailable:=true;
    end if;
    v_exception:=null;
    v_latest:=null;
    if v_scope->>'target_family_id' is not null then
      select e.* into v_latest from public.weekly_exceptional_pay_family_events e
        where e.family_id=(v_scope->>'target_family_id')::uuid
          and e.durable_work_event_id=v_row.work_event_id
        order by e.event_sequence desc limit 1;
    end if;
    if v_latest.id is not null and v_latest.state='WAIT' then
      -- Choose the actual approval's UNIQUE financial target-generation link.
      -- The Wait owner also appends generation-less, money-neutral audit rows
      -- for that same approval. They are not second payment publications, but
      -- must be genuine unchanged-vector WAIT evidence, never arbitrary nulls.
      -- A sibling publication may supersede the original financial generation;
      -- today's current_generation is not the immutable approval's pin.
      select count(*) filter (where t.financial_generation_id is not null),
        (array_agg(t.financial_generation_id) filter (where t.financial_generation_id is not null))[1],
        coalesce(bool_or(t.financial_generation_id is null and (
          t.reason is distinct from 'WAIT'
          or t.resulting_lifecycle_state is distinct from 'WAITING_SOURCE'
          or t.actor_user_id is null
          or t.complete_prior_family_vector_fingerprint is distinct from t.complete_next_family_vector_fingerprint
          or private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',
            t.fixed_target_component_snapshot) is distinct from t.complete_next_family_vector_fingerprint
          or jsonb_typeof(t.current_source_proposal_snapshot) is distinct from 'object'
          or t.event_hash is distinct from private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_PROTECTED_TARGET_EVENT_V1',jsonb_build_object(
              'family_id',t.family_id,'event_sequence',t.event_sequence,
              'approval_id',t.approval_id,'generation_id',null,
              'prior_vector_hash',encode(t.complete_prior_family_vector_fingerprint,'hex'),
              'next_vector_hash',encode(t.complete_next_family_vector_fingerprint,'hex'),
              'reason','WAIT','actor_user_id',t.actor_user_id))
        )),false)
        into v_count,v_generation,v_invalid_wait_audit
        from public.weekly_exceptional_pay_target_events t
        where t.family_id=v_latest.family_id and t.approval_id=v_latest.evidence_approval_id;
      if v_latest.evidence_approval_id is null or v_count<>1 or v_generation is null or v_invalid_wait_audit then
        v_unavailable:=true;
      else
        v_local:=private.weekly_source_pay_query_local_receipt_v1(p_root_timesheet_id,
          v_latest.evidence_approval_id,v_generation);
        v_c1:=private.weekly_source_pay_query_c1_receipt_v1(p_root_timesheet_id,
          v_latest.evidence_approval_id,v_generation);
        v_next:=private.weekly_source_pay_query_next_receipt_v1(p_root_timesheet_id,
          v_latest.evidence_approval_id,v_generation);
        v_count:=(v_local is not null)::integer+(v_c1 is not null)::integer+(v_next is not null)::integer;
        select a.creation_orchestration_run_id into v_approval_run
          from public.weekly_exceptional_payment_approvals a where a.id=v_latest.evidence_approval_id;
        -- Count partial evidence too. A local RETIRED request is its audit row,
        -- not a second transport lane; unrelated sibling/history stays excluded.
        select count(*) into v_local_count
          from private.weekly_source_local_protected_decision_receipts l
          join public.weekly_exceptional_c1_publication_requests r on r.id=l.publication_request_id
          where l.generation_id=v_generation or r.orchestration_run_id=v_approval_run;
        select count(*),count(*) filter (where
          (v_local is not null and r.id::text=v_local->>'publication_request_id' and r.state='RETIRED')
          or (v_c1 is not null and r.id::text=v_c1->>'publication_request_id' and r.state='PUBLISHED'))
          into v_c1_count,v_audit_count
          from public.weekly_exceptional_c1_publication_requests r
          where r.generation_id=v_generation or r.orchestration_run_id=v_approval_run;
        select count(*) into v_next_count from private.bpay_next_protected_source_receipt r
          where r.approval_id=v_latest.evidence_approval_id or r.generation_id=v_generation
            or r.orchestration_run_id=v_approval_run;
        if v_count<>1 or v_approval_run is null
           or (v_local is not null and (v_local_count<>1 or v_c1_count<>1 or v_audit_count<>1 or v_next_count<>0))
           or (v_c1 is not null and (v_local_count<>0 or v_c1_count<>1 or v_audit_count<>1 or v_next_count<>0))
           or (v_next is not null and (v_local_count<>0 or v_c1_count<>0 or v_next_count<>1)) then v_unavailable:=true;
        else
          v_receipt:=coalesce(v_local,v_c1,v_next);
          v_exception:=jsonb_build_object('family_id',v_latest.family_id,
            'bound_version',v_scope->>'family_bound_version','event_sequence',v_latest.event_sequence::text,
            'state','WAIT','evidence_approval_id',v_latest.evidence_approval_id,'accepted_receipt',v_receipt);
        end if;
      end if;
    end if;
    if v_row.issue_family is distinct from 'CANDIDATE_TIMESHEET_MISSING' and v_exception is null then
      v_blocked:=true;
    end if;
    v_hours:=v_hours||jsonb_build_array(jsonb_build_object('id',v_row.id,
      'source_group_id',v_row.source_group_id,'source_cycle_id',v_row.source_cycle_id,
      'work_event_id',v_row.work_event_id,'candidate_id',v_row.candidate_id,'client_id',v_row.client_id,
      'state',v_row.state,'work_date',to_char(v_row.work_date,'YYYY-MM-DD'),
      'current_comparison_revision_id',v_row.comparison_id,'comparison_contract_id',v_row.comparison_contract_id,
      'issue_family',v_row.issue_family,'material_comparison_fingerprint',v_row.material_fingerprint,
      'latest_qualified_exception',v_exception));
  end loop;
  if v_unavailable then
    return jsonb_build_object('ok',false,'code','WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE',
      'scope',v_scope,'blocked',null,'query_state_sha256',null); end if;
  v_state:=jsonb_build_object('schema_version','PAY_QUERY_STATE_V3','scope',v_scope,
    'manual_reviews',v_manual,'hours_incidents',v_hours);
  v_digest:=extensions.digest(convert_to(v_state::text,'UTF8'),'sha256');
  return jsonb_build_object('ok',true,'code','OK','scope',v_scope,'blocked',v_blocked,
    'query_state_sha256',encode(v_digest,'hex'),'hours_incidents',v_hours,'manual_reviews',v_manual);
end;
$function$;
alter function private.weekly_source_pay_query_facts_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_pay_query_facts_v1(uuid) from public,anon,authenticated,service_role;

create or replace function private.weekly_source_covered_hours_incident_v1(p_incident_id uuid)
returns boolean language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_root uuid; v_count integer; v_facts jsonb;
begin
  select count(*),(array_agg(family.root_timesheet_id))[1] into v_count,v_root
  from public.weekly_discrepancy_incidents incident
  join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
  join public.weekly_work_events work on work.id=incident.work_event_id
  join public.weekly_exceptional_pay_target_families family
    on family.candidate_id=incident.candidate_id and family.contract_id=comparison.contract_id
    and work.work_date between family.week_start_date and family.week_ending_date
  join public.contracts contract on contract.id=family.contract_id and contract.client_id=incident.client_id
  where incident.id=p_incident_id and incident.state='OPEN'
    and exists(select 1 from public.weekly_exceptional_pay_family_events event
      where event.family_id=family.id and event.durable_work_event_id=work.id);
  if v_count<>1 or v_root is null then return false; end if;
  v_facts:=private.weekly_source_pay_query_facts_v1(v_root);
  if v_facts->'ok' is distinct from 'true'::jsonb then return false; end if;
  return exists(select 1 from jsonb_array_elements(v_facts->'hours_incidents') item
    where item->>'id'=p_incident_id::text and jsonb_typeof(item->'latest_qualified_exception')='object');
end;
$function$;
alter function private.weekly_source_covered_hours_incident_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_covered_hours_incident_v1(uuid) from public,anon,authenticated,service_role;


