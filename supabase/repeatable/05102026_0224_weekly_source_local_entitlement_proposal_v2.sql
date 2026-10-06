-- Repeatable CloudTMS function/view authority: weekly_source_local_entitlement_proposal_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Closed local extension of the existing single-root proposal shape. It
-- carries the actual calculator's complete components without repricing and
-- has no publication, TSFIN, Banking, Draft or invoice write authority.
create or replace function private.weekly_source_entitlement_proposal_request_v2(
  p_root_timesheet_id uuid,p_source_origin jsonb,p_authority_kind text,
  p_decision_bundle_id uuid,p_bundle_revision bigint,p_head_id uuid,p_decision_id uuid,
  p_components jsonb,p_selection_method text default 'UNCHANGED'
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_origin jsonb;
  v_context jsonb;
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_components jsonb;
  v_before_ids jsonb;
  v_scope jsonb;
  v_inventory jsonb;
begin
  if p_root_timesheet_id is null or p_decision_bundle_id is null or p_head_id is null
     or p_decision_id is null or p_bundle_revision is null or p_bundle_revision<1
     or p_authority_kind is distinct from 'PROTECTED' or p_selection_method is distinct from 'UNCHANGED'
     or jsonb_typeof(p_components) is distinct from 'array' then
    raise exception 'WEEKLY_PROTECTED_LOCAL_PROPOSAL_INVALID' using errcode='22023'; end if;
  v_origin:=private.weekly_source_local_origin_canonical_v2(p_source_origin);
  select r.* into strict v_local from private.weekly_source_local_protected_decision_receipts r
    where r.publication_request_id=(v_origin->>'publication_request_id')::uuid;
  v_context:=private.weekly_source_local_publication_context_v2(v_local.publication_request_id,
    (v_origin->>'generation_id')::uuid,p_root_timesheet_id);
  if v_context is null or v_local.common_decision_bundle_id is distinct from p_decision_bundle_id
     or v_local.common_bundle_revision is distinct from p_bundle_revision
     or v_local.publication_origin_kind is distinct from 'PROTECTED_LOCAL_DECISION_V1'
     or v_local.publication_origin_digest is distinct from private.weekly_source_publication_request_digest_v1(v_origin)
     or v_local.source_qualification_digest is distinct from decode(v_origin->>'source_qualification_digest','hex')
     or v_local.approved_snapshot_json->'local_publication_origin' is distinct from v_origin
     or v_local.approved_snapshot_json->'local_source_qualification' is distinct from v_context->'qualification'
     or v_origin->'before_origin' is distinct from v_context->'before_origin'
     or v_origin->'before_inventory_digest' is distinct from v_context->'before_inventory_digest'
     or v_origin->'source_qualification_digest' is distinct from v_context->'source_qualification_digest'
     or v_origin->'policy_fingerprint' is distinct from v_context->'policy_fingerprint'
     or v_origin->>'request_sha256' is distinct from encode(v_local.request_sha256,'hex') then
    raise exception 'WEEKLY_PROTECTED_LOCAL_PROPOSAL_UNQUALIFIED' using errcode='55000'; end if;
  select g.* into strict v_generation from public.weekly_exceptional_pay_generations g where g.id=v_local.generation_id;
  select coalesce(jsonb_agg(private.weekly_source_publication_component_canonical_v1(c.value,
    'local.proposed_component') order by c.ordinality),'[]'::jsonb) into v_components
    from jsonb_array_elements(p_components) with ordinality c(value,ordinality);
  if v_local.approved_snapshot_json->>'common_components_digest' is distinct from
       encode(private.weekly_source_publication_request_digest_v1(v_components),'hex')
     or jsonb_array_length(v_components) is distinct from
        (v_generation.complete_next_vector_json->>'component_count')::integer
     or exists(select 1 from jsonb_array_elements(v_components) with ordinality c(value,ordinality)
       where (c.value->>'component_ordinal')::integer is distinct from c.ordinality)
     or exists(select 1 from jsonb_array_elements(v_components) c(value)
       group by c.value->>'component_id' having count(*)<>1) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_PROPOSAL_COMPONENTS_INVALID' using errcode='55000'; end if;
  v_scope:=v_context->'scope';
  -- Use the original sealed whole before-position, whose currentness and
  -- factual approval origin were qualified by the single context owner above.
  v_inventory:=v_generation.complete_next_vector_json->'prior_effective_inventory';
  select coalesce(jsonb_agg(c.value->'component_id' order by c.value->>'component_id'),'[]'::jsonb)
    into v_before_ids from jsonb_array_elements(v_inventory->'components') c(value);
  return jsonb_build_object('decision_bundle_id',p_decision_bundle_id,'pending_bundle_id',null,
    'bundle_revision',p_bundle_revision,'candidate_id',v_scope->'candidate_id',
    'member_root_ids',jsonb_build_array(p_root_timesheet_id),
    'member_family_booking_ids',jsonb_build_array(v_scope->'family_booking_id'),
    'member_root_versions',jsonb_build_array((v_scope->>'root_version')::integer),
    'head_ids',jsonb_build_array(p_head_id),'decision_id',p_decision_id,'publication_mode','IMMEDIATE',
    'financial_request',jsonb_build_object('source_revision',v_origin,
      'contract_choices',jsonb_build_array(jsonb_build_object('root_ordinal',1,
        'contract_id',v_scope->'contract_id','week_ending_date',v_scope->'week_ending_date',
        'selection_method',p_selection_method)),
      'member_entitlements',jsonb_build_array(jsonb_build_object('root_ordinal',1,
        'authority_kind',p_authority_kind,'certified_zero',jsonb_array_length(v_components)=0,
        'component_count',jsonb_array_length(v_components),'components',v_components))),
    'control',jsonb_build_object('bundle_kind','SINGLE_ROOT','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
      'expected_current_head_ids',jsonb_build_array(v_inventory->'head_id'),
      'before_positions',jsonb_build_array(jsonb_build_object('root_ordinal',1,
        'component_ids',v_before_ids,'inventory_digest',v_inventory->'inventory_digest')),
      'moved_component_ids','[]'::jsonb,'target_root_authorisation',null,'whole_root_office_review',null));
end;
$function$;
alter function private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text) owner to postgres;
revoke all on function private.weekly_source_entitlement_proposal_request_v2(uuid,jsonb,text,uuid,bigint,uuid,uuid,jsonb,text)
  from public,anon,authenticated,service_role;

commit;
