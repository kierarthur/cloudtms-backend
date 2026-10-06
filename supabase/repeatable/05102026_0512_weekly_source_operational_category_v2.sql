-- Repeatable CloudTMS function/view authority: weekly_source_operational_category_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- I4 is a read, not a lifecycle, money, query-resolution or invoice owner.
-- An unavailable duty remains NULL. In particular the independently owned
-- adjustment duty must not be inferred from the pay-only HEAD inventory.
create or replace function private.weekly_source_operational_empty_v1(
  p_root_timesheet_id uuid,p_expected_origin_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;
  v_identity jsonb;
  v_scope jsonb;
  v_authorisation jsonb;
  v_authorisation_state text:='UNAVAILABLE';
  v_inventory jsonb;
  v_basis jsonb;
  v_origin jsonb;
  v_origin_id uuid;
  v_family_ids uuid[];
  v_query_gate jsonb;
  v_approval_duty jsonb;
  v_invoice_duty jsonb;
  v_family_id uuid;
  v_count integer;
  v_work_id uuid;
  v_revision_id uuid;
  v_bank jsonb;
  v_component jsonb;
  v_work boolean;
  v_additional boolean;
  v_adjustment boolean;
  v_magnit_pay boolean;
  v_magnit_charge boolean;
  v_query boolean;
  v_approval boolean;
  v_invoice boolean;
  v_duties jsonb;
  v_complete boolean:=false;
  v_empty boolean;
  v_code text:='UNAVAILABLE';
  v_result jsonb;
begin
  <<evaluate>>
  begin
    if p_root_timesheet_id is null then exit evaluate; end if;
    v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
    if v_identity->>'ok' is distinct from 'true'
       or v_identity->>'canonical_timesheet_id' is distinct from p_root_timesheet_id::text
       or v_identity->>'family_is_current' is distinct from 'true' then exit evaluate; end if;
    select * into v_root from public.timesheets where timesheet_id=p_root_timesheet_id;
    if not found then exit evaluate; end if;
    select array_agg(e.value::uuid order by e.value) into v_family_ids
      from jsonb_array_elements_text(v_identity->'member_timesheet_ids') e(value);
    if cardinality(v_family_ids) is null or cardinality(v_family_ids)=0 then exit evaluate; end if;
    -- Preserve the installed authorisation predicate and detect actual prior
    -- authority. A cleared current timestamp never proves NEVER_AUTHORISED.
    v_authorisation:=private.weekly_source_root_authorisation_state_v1(p_root_timesheet_id,v_family_ids);
    if v_authorisation->>'relation_present' is distinct from 'true'
       or v_authorisation ? 'reason'
       or v_authorisation->>'live_on_other_member_count' is distinct from '0' then exit evaluate; end if;
    if v_authorisation->>'live_on_canonical'='true'
       and v_authorisation->>'timesheet_currently_authorised'='true' then
      v_authorisation_state:='AUTHORISED_CURRENT';
    elsif v_authorisation->>'live_on_canonical'='false'
       and v_authorisation->>'timesheet_currently_authorised'='false' then
      if exists(select 1 from public.weekly_source_root_authorisations a
        where a.family_booking_id=v_root.booking_id) then
        v_authorisation_state:='PREVIOUSLY_AUTHORISED_WITHDRAWN';
      else v_authorisation_state:='NEVER_AUTHORISED'; end if;
    else exit evaluate;
    end if;
    v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);
    if v_scope is null then exit evaluate; end if;
    v_family_id:=(v_scope->>'target_family_id')::uuid;
    -- Neither self-bill alone nor a Candidate/week match creates Source duty.
    if not exists(select 1 from public.weekly_source_row_timesheet_lineages l
      where l.family_booking_id=v_root.booking_id and l.timesheet_id=any(v_family_ids))
      and not exists(select 1 from public.weekly_exceptional_pay_target_families f
        where f.id=v_family_id and f.ownership_state='TARGET_MANAGED') then exit evaluate; end if;
    if v_authorisation_state<>'AUTHORISED_CURRENT' then exit evaluate; end if;
    v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
    v_basis:=v_inventory->'approval_basis';
    v_origin:=v_basis->'origin';
    if v_inventory->>'ok' is distinct from 'true'
       or v_basis->>'coverage_complete' is distinct from 'true'
       or v_basis->'scope' is distinct from v_scope
       or v_basis->>'inventory_digest' is distinct from v_inventory->>'inventory_digest'
       or v_origin->>'inventory_digest' is distinct from v_inventory->>'inventory_digest'
       or jsonb_typeof(v_inventory->'components') is distinct from 'array' then exit evaluate; end if;
    if v_origin->>'kind'='INITIAL_AUTHORISED_TSFIN_V1' then
      v_origin_id:=(v_origin->>'root_authorisation_id')::uuid;
    elsif v_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1' then
      v_origin_id:=(v_origin->>'head_id')::uuid;
    else exit evaluate; end if;
    if p_expected_origin_id is not null and p_expected_origin_id is distinct from v_origin_id then
      v_code:='ORIGIN_CHANGED'; exit evaluate;
    end if;
    v_work:=false; v_additional:=false; v_magnit_pay:=false; v_magnit_charge:=false;
    for v_component in select value from jsonb_array_elements(v_inventory->'components') loop
      case v_component->>'component_kind'
        when 'WORKED_TIME' then
          -- Even a zero-priced/excluded component still owns worked-time or
          -- Source charge duty. Cancellation of two amounts is not emptiness.
          v_work:=true;
        when 'ADDITIONAL_UNIT' then
          if (v_component->>'unit_count')::numeric<>0
             or (v_component->>'pay_ex_vat')::numeric<>0
             or (v_component->>'charge_ex_vat')::numeric<>0 then v_additional:=true; end if;
        when 'SOURCE_FIXED_EXPENSE' then
          if (v_component->>'pay_ex_vat')::numeric<>0 then v_magnit_pay:=true; end if;
          if (v_component->>'charge_ex_vat')::numeric<>0 then v_magnit_charge:=true; end if;
        else exit evaluate;
      end case;
    end loop;
    -- Reuse the factual gate's exact configured-workweek discovery across all
    -- cutoffs, including new events not yet bound to an imported lineage. Its
    -- receipt/current-pointer qualification and nonblocking exceptions remain
    -- authoritative. Unknown evidence is NULL, never an empty query census.
    v_query_gate:=private.weekly_source_pay_query_gate_v1(p_root_timesheet_id);
    if v_query_gate->>'ok'='true' and v_query_gate->'scope'=v_scope
       and jsonb_typeof(v_query_gate->'blocked')='boolean'
       and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$' then
      v_query:=(v_query_gate->>'blocked')::boolean;
    end if;
    -- Reuse pure, exact-owner duty qualifiers. Lingering generation status is
    -- not an accepted pending obligation, and movement-only absence misses
    -- pre-Final unions, Correct Final and queued invoice work. Incomplete or
    -- contradictory evidence stays NULL and cannot file a root as Withdrawn.
    v_approval_duty:=private.weekly_source_approval_duty_v1(p_root_timesheet_id);
    if v_approval_duty->'scope'=v_scope
       and v_approval_duty->'discovery_complete'='true'::jsonb
       and jsonb_typeof(v_approval_duty->'present')='boolean' then
      v_approval:=(v_approval_duty->>'present')::boolean;
    end if;
    v_invoice_duty:=private.weekly_source_invoice_duty_v1(p_root_timesheet_id);
    if v_invoice_duty->'scope'=v_scope
       and v_invoice_duty->'discovery_complete'='true'::jsonb
       and jsonb_typeof(v_invoice_duty->'present')='boolean' then
      v_invoice:=(v_invoice_duty->>'present')::boolean;
    end if;
    -- The independently reviewed one-way absence certificate covers EVERY
    -- physical member, not just today's root or unpaid rows. A present row or
    -- current correction candidate is unavailable here, not a newly invented
    -- NEXT obligation. Retired/noncurrent history is not a filing gate.
    if not exists(select 1 from public.ts_pay_adjustments a
         where a.timesheet_id=any(v_family_ids) and not a.as_advance)
       and not exists(select 1 from public.timesheets child
         where child.parent_timesheet_id=any(v_family_ids) and child.is_adjustment
           and child.is_current and child.revoked_at is null
           and child.archived_at_utc is null) then v_adjustment:=false;
    end if;
    if to_regprocedure('private.bpay_next_source_origin_state_v1(uuid,uuid)') is null then exit evaluate; end if;
    select count(*),min(r.work_id::text)::uuid,min(r.id::text)::uuid
      into v_count,v_work_id,v_revision_id
      from private.bpay_next_work_revision r
      join private.bpay_next_work w on w.id=r.work_id
      where r.source_event_id=v_origin_id and w.work_kind='SOURCE'
        and w.candidate_id=(v_scope->>'candidate_id')::uuid
        and w.contract_id=(v_scope->>'contract_id')::uuid
        and w.booking_id=v_root.booking_id and w.week_ending_date=v_root.week_ending_date;
    if v_count<>1 then exit evaluate; end if;
    v_bank:=private.bpay_next_source_origin_state_v1(v_work_id,v_revision_id);
    if v_bank->>'ok' is distinct from 'true'
       or v_bank->>'source_event_id' is distinct from v_origin_id::text
       or v_bank->>'physical_root_id' is distinct from p_root_timesheet_id::text
       or v_bank->>'physical_root_version' is distinct from v_scope->>'root_version'
       or v_bank->>'source_inventory_digest' is distinct from v_inventory->>'inventory_digest'
       or (v_origin->>'kind'='COMMITTED_SOURCE_HEAD_V1'
         and v_bank->>'source_head_id' is distinct from v_origin_id::text)
       or (v_origin->>'kind'='INITIAL_AUTHORISED_TSFIN_V1'
         and (v_bank->>'source_head_id' is not null
           or v_bank->>'financial_snapshot_id' is distinct from v_origin->>'financial_snapshot_id')) then
      exit evaluate;
    end if;
    v_complete:=v_work is not null and v_additional is not null and v_adjustment is not null
      and v_magnit_pay is not null and v_magnit_charge is not null and v_query is not null
      and v_approval is not null and v_invoice is not null;
    if v_complete then
      v_empty:=not (v_work or v_additional or v_adjustment or v_magnit_pay
        or v_magnit_charge or v_query or v_approval or v_invoice);
      v_code:='OK';
    end if;
  end evaluate;
  v_duties:=jsonb_build_object('work',v_work,'owned_additional',v_additional,
    'owned_adjustment',v_adjustment,'magnit_pay',v_magnit_pay,'magnit_charge',v_magnit_charge,
    'source_query',v_query,'source_approval',v_approval,'invoice_task',v_invoice);
  v_result:=jsonb_build_object('ok',v_complete,'code',v_code,'scope',v_scope,
    'approval_origin',v_origin,'authorisation_state',v_authorisation_state,
    'coverage_complete',v_complete,'effective_empty',v_empty,
    'inventory_digest',v_basis->'inventory_digest','entitlement_digest',v_basis->'entitlement_digest',
    'banking_application',v_bank,'duties',v_duties);
  return v_result||jsonb_build_object('basis_sha256',encode(
    private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OPERATIONAL_EMPTY_V1',
      jsonb_build_object('result',v_result,
        'pay_query_state_sha256',v_query_gate->'query_state_sha256',
        'approval_duty',v_approval_duty,'invoice_duty',v_invoice_duty)),'hex'));
end;
$function$;

-- Null presentation_category is an explicit NO OVERRIDE: consumers preserve
-- their genuine existing ordinary category/action precedence. This helper
-- never calls the workbench or lifecycle-signature readers (no recursion).
create or replace function private.weekly_source_timesheet_category_v2(p_root_timesheet_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;
  v_duty jsonb;
  v_category text;
  v_reason text;
  v_basis jsonb;
  v_inventory jsonb;
  v_component jsonb;
  v_event public.weekly_exceptional_pay_family_events%rowtype;
  v_event_id uuid;
  v_detail jsonb;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_count integer;
  v_generation_id uuid;
  v_local jsonb;
  v_c1 jsonb;
  v_next jsonb;
  v_source jsonb;
  v_waiting_import jsonb:='[]'::jsonb;
begin
  select * into v_root from public.timesheets where timesheet_id=p_root_timesheet_id;
  if not found then return null; end if;
  if v_root.archived_at_utc is not null then v_category:='ARCHIVED';
  elsif v_root.authorised_at_server is not null and v_root.is_current
        and v_root.revoked_at is null and v_root.sheet_scope='WEEKLY'
        and v_root.line_type='HOURS' and v_root.is_adjustment=false then
    v_duty:=private.weekly_source_operational_empty_v1(p_root_timesheet_id,null);
    if v_duty->>'ok'='true' and v_duty->>'authorisation_state'='AUTHORISED_CURRENT'
       and v_duty->>'effective_empty'='true'
       and v_duty#>>'{banking_application,application_state}'='APPLIED'
       and v_duty#>>'{scope,target_family_id}' is not null then v_category:='WITHDRAWN';
    elsif v_duty->>'authorisation_state'='AUTHORISED_CURRENT'
       and v_duty#>>'{scope,target_family_id}' is not null
       and v_duty->'approval_origin' is not null then
      -- Today's positive approved protected work, not a lifetime invoice-count
      -- proxy. An old issued document cannot hide a new Source-absent shift;
      -- a zero component or a pending/unqualified decision cannot create this
      -- positive-hours presentation. No candidate-pay gate is added.
      v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
      if v_inventory->>'ok'='true'
         and v_inventory#>>'{approval_basis,coverage_complete}'='true'
         and v_inventory#>'{approval_basis,scope}'=v_duty->'scope'
         and v_inventory#>'{approval_basis,origin}'=v_duty->'approval_origin'
         and v_inventory->>'inventory_digest'=v_duty->>'inventory_digest' then
        for v_component in select value from jsonb_array_elements(v_inventory->'components')
          where value->>'component_kind'='WORKED_TIME'
            and value->'exclude_from_pay'='false'::jsonb
            and (coalesce((value->>'hours_day')::numeric,0)
              +coalesce((value->>'hours_night')::numeric,0)
              +coalesce((value->>'hours_sat')::numeric,0)
              +coalesce((value->>'hours_sun')::numeric,0)
              +coalesce((value->>'hours_bh')::numeric,0))>0
        loop
          -- Protected-only calculator segments can retain their stable local
          -- segment identity rather than a durable-event UUID. Use the exact
          -- I1-validated chosen detail, then require ONE current protected
          -- event with those full clocks/breaks. Never match only duration or
          -- select an arbitrary event on the same day.
          if v_duty#>>'{approval_origin,kind}'='COMMITTED_SOURCE_HEAD_V1' then
            select count(*),jsonb_agg(d.detail_json)->0 into v_count,v_detail
              from private.bpay_next_source_chosen_detail d
              where d.head_id=(v_duty#>>'{approval_origin,head_id}')::uuid
                and d.component_id=(v_component->>'component_id')::uuid
                and d.decision_bundle_id=(v_duty#>>'{approval_origin,decision_bundle_id}')::uuid
                and d.bundle_revision=(v_duty#>>'{approval_origin,bundle_revision}')::bigint;
          else
            select count(*),jsonb_agg(s.value)->0 into v_count,v_detail
              from public.timesheets_financials f
              cross join lateral jsonb_array_elements(f.invoice_breakdown_json->'segments') s(value)
              where f.id=(v_duty#>>'{approval_origin,financial_snapshot_id}')::uuid
                and f.timesheet_id=p_root_timesheet_id and f.is_current
                and s.value->>'segment_id'=v_component->>'segment_id';
          end if;
          if v_count<>1 then continue; end if;
          select count(*),min(e.id::text)::uuid into v_count,v_event_id
            from public.weekly_exceptional_pay_family_events e
            join public.weekly_exceptional_payment_approvals a on a.id=e.evidence_approval_id
            where e.family_id=(v_duty#>>'{scope,target_family_id}')::uuid and e.state='WAIT'
              and not exists(select 1 from public.weekly_exceptional_pay_family_events newer
                where newer.family_id=e.family_id and newer.durable_work_event_id=e.durable_work_event_id
                  and newer.event_sequence>e.event_sequence)
              and a.withdrawn_at_utc is null and a.pay_target_family_id=e.family_id
              and a.work_event_id=e.durable_work_event_id
              and a.protected_work_date=(v_component->>'work_date')::date
              and to_char(a.protected_start_at_local,'HH24:MI')=v_detail->>'start'
              and to_char(a.protected_end_at_local,'HH24:MI')=v_detail->>'end'
              and (a.protected_end_at_local::date>a.protected_start_at_local::date)
                =(v_detail->>'overnight')::boolean
              and a.protected_break_minutes=coalesce(v_detail->>'break_minutes',v_detail->>'break_mins')::integer;
          if v_count<>1 then continue; end if;
          select * into strict v_event from public.weekly_exceptional_pay_family_events where id=v_event_id;
          select a.* into v_approval from public.weekly_exceptional_payment_approvals a
            where a.id=v_event.evidence_approval_id and a.pay_target_family_id=v_event.family_id
              and a.work_event_id=v_event.durable_work_event_id
              and a.candidate_id=(v_duty#>>'{scope,candidate_id}')::uuid
              and a.client_id=(v_duty#>>'{scope,client_id}')::uuid
              and a.contract_id=(v_duty#>>'{scope,contract_id}')::uuid
              and a.week_ending=v_root.week_ending_date;
          if not found then continue; end if;
          select count(*),min(t.financial_generation_id::text)::uuid into v_count,v_generation_id
            from public.weekly_exceptional_pay_target_events t
            where t.family_id=v_event.family_id and t.approval_id=v_approval.id;
          if v_count<>1 or v_generation_id is null then continue; end if;
          v_local:=private.weekly_source_pay_query_local_receipt_v1(p_root_timesheet_id,v_approval.id,v_generation_id);
          v_c1:=private.weekly_source_pay_query_c1_receipt_v1(p_root_timesheet_id,v_approval.id,v_generation_id);
          v_next:=private.weekly_source_pay_query_next_receipt_v1(p_root_timesheet_id,v_approval.id,v_generation_id);
          if (v_local is not null)::integer+(v_c1 is not null)::integer+(v_next is not null)::integer<>1 then
            continue;
          end if;
          v_source:=private.weekly_source_selected_source_witness_v1(
            v_event.family_id,v_approval.source_cycle_id,v_event.durable_work_event_id);
          if v_source->>'kind'='CERTIFIED_ABSENCE'
             and v_source#>>'{source_proposal,selected_work_event_id}'=v_event.durable_work_event_id::text
             and v_source#>'{source_proposal,source_present}'='false'::jsonb then
            v_waiting_import:=v_waiting_import||jsonb_build_array(jsonb_build_object(
              'component_id',v_component->'component_id','approval_id',v_approval.id,
              'generation_id',v_generation_id,'accepted_receipt',coalesce(v_local,v_c1,v_next),
              'selected_source_witness',v_source));
          end if;
        end loop;
        if jsonb_array_length(v_waiting_import)>0
           and v_duty#>'{duties,invoice_task}'='false'::jsonb then
          v_category:='PROCESSING_DELAYED'; v_reason:='Awaiting a valid import for invoicing';
        end if;
      end if;
    end if;
  end if;
  v_basis:=jsonb_build_object('root_timesheet_id',v_root.timesheet_id,'root_version',v_root.version,
    'is_current',v_root.is_current,'revoked_at',v_root.revoked_at,
    'authorised_at_server',v_root.authorised_at_server,'archived_at_utc',v_root.archived_at_utc,
    'presentation_category',v_category,'source_duty',v_duty,'waiting_import',v_waiting_import);
  return jsonb_build_object('presentation_category',v_category,
    'category_basis',jsonb_build_object('facts',v_basis,'sha256',encode(
      private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_TIMESHEET_CATEGORY_V2',v_basis),'hex')),
    'is_archived',v_root.archived_at_utc is not null,'archived_at_utc',v_root.archived_at_utc,
    'authorised_at_server',v_root.authorised_at_server,'is_current',v_root.is_current,
    'revoked_at',v_root.revoked_at,'processing_reason',v_reason);
end;
$function$;

alter function private.weekly_source_operational_empty_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_timesheet_category_v2(uuid) owner to postgres;
revoke all on function private.weekly_source_operational_empty_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_timesheet_category_v2(uuid)
  from public,anon,authenticated,service_role;

commit;
