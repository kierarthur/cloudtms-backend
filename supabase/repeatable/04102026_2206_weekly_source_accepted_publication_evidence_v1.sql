-- Repeatable CloudTMS function/view authority: weekly_source_accepted_publication_evidence_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- The actual APP public owner records these witnesses atomically with its
-- publication. Winning shared Banking guards still require joined verification.
-- No actor/digest field on an existing immutable proposal may be overwritten.

-- Read the actual complete before-position and current genuine Final. The
-- caller owns the candidate/family/rotation locks; this read cannot certify a
-- saved unauthorised proposal, KEEP, move, or an arbitrary partial vector.
create or replace function private.weekly_source_accepted_publication_context_v1(
  p_canonical jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_canonical jsonb;
  v_origin jsonb;
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_final public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_inventory jsonb;
  v_basis jsonb;
  v_before_ids jsonb;
  v_control jsonb;
begin
  v_canonical:=private.weekly_source_publication_request_canonical_v1(
    p_canonical,'IMMEDIATE',null::uuid);
  v_origin:=v_canonical#>'{financial_request,source_revision}';
  if v_origin->>'origin_kind' is distinct from 'CURRENT_FINAL_SOURCE_V1'
     or jsonb_array_length(v_canonical->'member_root_ids')<>1
     or v_canonical#>>'{financial_request,member_entitlements,0,authority_kind}'
          is distinct from 'LOCKED_FINAL_SOURCE' then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' using errcode='22023';
  end if;
  select * into v_bundle from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=(v_canonical->>'decision_bundle_id')::uuid
      and b.bundle_revision=(v_canonical->>'bundle_revision')::bigint;
  if not found or v_bundle.state not in ('PROPOSED','COMMITTED')
     or v_bundle.bundle_kind<>'SINGLE_ROOT'
     or v_bundle.publication_mode<>'IMMEDIATE'
     or v_bundle.target_root_timesheet_id is not null
     or v_bundle.whole_root_review_required
     or cardinality(v_bundle.proposed_head_ids)<>1
     or to_jsonb(v_bundle.proposed_head_ids) is distinct from v_canonical->'head_ids'
     or v_bundle.decision_id is distinct from (v_canonical->>'decision_id')::uuid
     or v_bundle.candidate_id is distinct from (v_canonical->>'candidate_id')::uuid
     or v_bundle.source_root_timesheet_id is distinct from
          (v_canonical#>>'{member_root_ids,0}')::uuid
     or v_bundle.source_root_family_booking_id is distinct from
          v_canonical#>>'{member_family_booking_ids,0}'
     or v_bundle.request_digest is distinct from
          private.weekly_source_publication_request_digest_v1(v_canonical)
     or v_bundle.source_revision_digest is distinct from
          private.weekly_source_publication_request_digest_v1(v_origin)
     or v_bundle.contract_choice_digest is distinct from
          private.weekly_source_publication_request_digest_v1(
            v_canonical#>'{financial_request,contract_choices}')
     then
    return null;
  end if;
  select * into v_root from public.timesheets t where t.timesheet_id=v_bundle.source_root_timesheet_id;
  if not found or not v_root.is_current
     or v_root.version is distinct from (v_canonical#>>'{member_root_versions,0}')::bigint
     or v_root.booking_id is distinct from v_bundle.source_root_family_booking_id
     or v_root.contract_id is distinct from v_bundle.source_contract_id
     or v_root.week_ending_date is distinct from v_bundle.week_ending_date then return null; end if;
  select * into v_contract from public.contracts c where c.id=v_root.contract_id;
  if not found or v_contract.candidate_id is distinct from v_bundle.candidate_id
     or v_contract.id is distinct from (v_canonical#>>'{financial_request,contract_choices,0,contract_id}')::uuid
     or v_root.week_ending_date is distinct from
          (v_canonical#>>'{financial_request,contract_choices,0,week_ending_date}')::date then return null; end if;
  select * into v_final from public.weekly_source_final_revisions f
    where f.id=(v_origin->>'final_revision_id')::uuid;
  if not found or v_final.state<>'CURRENT'
     or v_final.source_cycle_id is distinct from (v_origin->>'source_cycle_id')::uuid
     or v_final.revision_number is distinct from (v_origin->>'revision_number')::integer
     or encode(v_final.manifest_hash,'hex') is distinct from v_origin->>'manifest_hash'
     or encode(v_final.policy_fingerprint,'hex') is distinct from v_origin->>'policy_fingerprint' then return null; end if;
  select * into strict v_cycle from public.weekly_source_cycles where id=v_final.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  if v_group.agency_id is distinct from v_bundle.agency_id then return null; end if;
  -- Final scope remains report/cutoff based, never substituted by workweek.
  if not exists(select 1 from public.weekly_source_client_manifests m
      where m.final_revision_id=v_final.id and m.source_cycle_id=v_cycle.id
        and m.client_id=v_contract.client_id) then return null; end if;
  v_inventory:=private.weekly_source_effective_inventory_v1(v_root.timesheet_id);
  v_basis:=v_inventory->'approval_basis';
  if (v_inventory->>'ok') is distinct from 'true'
     or (v_basis->>'coverage_complete') is distinct from 'true'
     or v_basis#>>'{origin,kind}' not in ('INITIAL_AUTHORISED_TSFIN_V1','COMMITTED_SOURCE_HEAD_V1')
     or v_basis#>>'{scope,root_timesheet_id}' is distinct from v_root.timesheet_id::text then return null; end if;
  select coalesce(jsonb_agg(c.value->'component_id' order by c.value->>'component_id'),'[]'::jsonb)
    into v_before_ids from jsonb_array_elements(v_inventory->'components') c(value);
  v_control:=jsonb_build_object('bundle_kind','SINGLE_ROOT','reason','WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION',
    'expected_current_head_ids',jsonb_build_array(coalesce(v_inventory->'head_id','null'::jsonb)),
    'before_positions',jsonb_build_array(jsonb_build_object('root_ordinal',1,
      'component_ids',v_before_ids,'inventory_digest',v_basis->>'inventory_digest')),
    'moved_component_ids','[]'::jsonb,'target_root_authorisation',null,'whole_root_office_review',null);
  if v_bundle.before_inventory_digest is distinct from private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_before_inventory_v1(v_control,1)) then return null; end if;
  -- The existing eleven-field canonical encoder excludes control. Do not
  -- change its receipt hash domain. The complete before-origin/content digest
  -- is stored separately, and the live publisher compares its own controls.
  -- At first acceptance we also require the actual constructor's controls.
  if p_canonical ? 'control' and p_canonical->'control' is distinct from v_control then return null; end if;
  perform private.weekly_source_office_authority_v1(v_bundle.decided_by_user_id,
    'APPROVE_PROTECTED_PAY',v_group.id,v_contract.client_id,v_cycle.finalisation_week_ending);
  return jsonb_build_object('canonical',v_canonical,'basis',v_basis,
    'actor_user_id',v_bundle.decided_by_user_id,'agency_id',v_bundle.agency_id,
    'finalisation_week_ending',v_cycle.finalisation_week_ending);
end;
$function$;

create or replace function private.weekly_source_accepted_publication_action_record_v1(
  p_request jsonb,p_canonical_publication_request jsonb,p_before_origin jsonb
) returns uuid
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_root uuid;
  v_bundle uuid;
  v_revision bigint;
  v_requested_revision bigint;
  v_final uuid;
  v_key text;
  v_hash bytea;
  v_canonical jsonb;
  v_origin jsonb;
  v_id uuid;
  v_context jsonb;
  v_basis jsonb;
  v_scope jsonb;
  v_lock jsonb;
  v_saved private.weekly_source_accepted_publication_actions_v1%rowtype;
  v_original public.weekly_source_entitlement_decision_bundles%rowtype;
  v_original_canonical jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501'; end if;
  perform private.weekly_source_publication_require_keys_v1(p_request,array[
    'actor_user_id','bundle_revision','decision','decision_bundle_id',
    'final_revision_id','idempotency_key','root_timesheet_id','schema_version'],'accepted_action.request');
  if p_request->>'schema_version' is distinct from 'WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1'
     or p_request->>'decision' is distinct from 'APPROVE_UPDATED_HOURS' then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' using errcode='22023'; end if;
  v_actor:=(private.weekly_source_publication_scalar_v1(p_request->'actor_user_id','actor_user_id','UUID')#>>'{}')::uuid;
  v_root:=(private.weekly_source_publication_scalar_v1(p_request->'root_timesheet_id','root_timesheet_id','UUID')#>>'{}')::uuid;
  v_bundle:=(private.weekly_source_publication_scalar_v1(p_request->'decision_bundle_id','decision_bundle_id','UUID')#>>'{}')::uuid;
  v_final:=(private.weekly_source_publication_scalar_v1(p_request->'final_revision_id','final_revision_id','UUID')#>>'{}')::uuid;
  v_requested_revision:=(private.weekly_source_publication_scalar_v1(p_request->'bundle_revision','bundle_revision','INT')#>>'{}')::bigint;
  v_key:=btrim(coalesce(p_request->>'idempotency_key',''));
  if v_requested_revision<1 or jsonb_typeof(p_request->'idempotency_key')<>'string'
     or char_length(v_key) not between 16 and 200 then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' using errcode='22023'; end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',p_request-'idempotency_key');
  if p_canonical_publication_request->>'publication_mode' is distinct from 'IMMEDIATE'
     or p_canonical_publication_request->'pending_bundle_id' is distinct from 'null'::jsonb then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' using errcode='22023'; end if;
  v_canonical:=private.weekly_source_publication_request_canonical_v1(
    p_canonical_publication_request,'IMMEDIATE',null::uuid);
  v_origin:=v_canonical#>'{financial_request,source_revision}';
  v_id:=(v_origin->>'accepted_action_id')::uuid;
  v_revision:=(v_canonical->>'bundle_revision')::bigint;
  if v_id is null or p_canonical_publication_request#>>'{control,bundle_kind}' is distinct from 'SINGLE_ROOT'
     or v_origin->>'origin_kind' is distinct from 'CURRENT_FINAL_SOURCE_V1'
     or (v_origin->>'final_revision_id')::uuid is distinct from v_final
     or (v_canonical->>'decision_bundle_id')::uuid is distinct from v_bundle
     or v_revision is distinct from v_requested_revision+1
     or v_canonical->'member_root_ids' is distinct from jsonb_build_array(v_root) then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' using errcode='22023'; end if;
  -- Exact accepted replay is historical evidence, not a claim that the former
  -- before-position is still current. Do not re-decide after a valid advance.
  select * into v_saved from private.weekly_source_accepted_publication_actions_v1 a
    where a.idempotency_key=v_key;
  if found then
    if v_saved.action_id is distinct from v_id or v_saved.request_sha256 is distinct from v_hash
       or v_saved.actor_user_id is distinct from v_actor or v_saved.root_timesheet_id is distinct from v_root
       or v_saved.decision_bundle_id is distinct from v_bundle or v_saved.bundle_revision is distinct from v_revision
       or v_saved.expected_prior_origin is distinct from p_before_origin
       or v_saved.accepted_canonical_json is distinct from v_canonical
       or v_saved.accepted_canonical_sha256 is distinct from private.weekly_source_publication_request_digest_v1(v_canonical) then
      raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_REPLAY_CONFLICT' using errcode='22023'; end if;
    return v_saved.action_id;
  end if;
  -- Reuse I-1; never introduce an ad-hoc family lock class. The public owner
  -- must take this same lock order before writing/locking a bundle revision.
  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    (v_canonical->>'candidate_id')::uuid,array[v_root],
    'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',gen_random_uuid(),'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  if (v_lock->>'ok') is distinct from 'true' or v_lock->>'gate' is distinct from 'GRANTED' then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_UNAVAILABLE' using errcode='55000'; end if;
  -- A concurrent compatible first attempt can have committed while I-1 waited.
  -- Re-read before interpreting changed current authority as a new decision.
  select * into v_saved from private.weekly_source_accepted_publication_actions_v1 a
    where a.idempotency_key=v_key;
  if found then
    if v_saved.action_id is distinct from v_id or v_saved.request_sha256 is distinct from v_hash
       or v_saved.actor_user_id is distinct from v_actor or v_saved.root_timesheet_id is distinct from v_root
       or v_saved.decision_bundle_id is distinct from v_bundle or v_saved.bundle_revision is distinct from v_revision
       or v_saved.expected_prior_origin is distinct from p_before_origin
       or v_saved.accepted_canonical_json is distinct from v_canonical
       or v_saved.accepted_canonical_sha256 is distinct from private.weekly_source_publication_request_digest_v1(v_canonical) then
      raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_REPLAY_CONFLICT' using errcode='22023'; end if;
    return v_saved.action_id;
  end if;
  perform 1 from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_bundle and b.bundle_revision=v_revision and b.state='PROPOSED'
    for update;
  if not found then raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_STALE' using errcode='55000'; end if;
  -- The public request still names the proposal the user actually reviewed.
  -- Its immutable successor names the real accepted action. Reconstruct the
  -- former complete request by changing ONLY those explicit provenance fields;
  -- never silently accept a newly composed different economic proposal.
  select * into v_original from public.weekly_source_entitlement_decision_bundles b
    where b.decision_bundle_id=v_bundle and b.bundle_revision=v_requested_revision;
  v_original_canonical:=jsonb_set(jsonb_set(v_canonical,'{bundle_revision}',to_jsonb(v_requested_revision)),
    '{financial_request,source_revision}',v_origin-'origin_kind'-'accepted_action_id');
  if not found or v_original.state<>'ABANDONED'
     or v_original.bundle_kind<>'SINGLE_ROOT'
     or v_original.source_root_timesheet_id is distinct from v_root
     or v_original.request_digest is distinct from
          private.weekly_source_publication_request_digest_v1(v_original_canonical)
     or v_original.source_revision_digest is distinct from
          private.weekly_source_publication_request_digest_v1(v_origin-'origin_kind'-'accepted_action_id') then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_STALE' using errcode='55000'; end if;
  v_context:=private.weekly_source_accepted_publication_context_v1(p_canonical_publication_request);
  v_basis:=v_context->'basis'; v_scope:=v_basis->'scope';
  if v_context is null or (v_context->>'actor_user_id')::uuid is distinct from v_actor
     or v_basis->'origin' is distinct from p_before_origin then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_STALE' using errcode='55000'; end if;
  insert into private.weekly_source_accepted_publication_actions_v1(
    action_id,decision_bundle_id,bundle_revision,idempotency_key,request_sha256,actor_user_id,action,
    root_timesheet_id,family_booking_id,root_version,agency_id,candidate_id,client_id,contract_id,
    week_ending_date,source_cycle_id,final_revision_id,finalisation_week_ending,final_revision_number,
    manifest_hash,policy_fingerprint,expected_prior_origin,before_inventory_sha256,
    before_entitlement_sha256,origin_sha256,planned_head_id,accepted_canonical_json,accepted_canonical_sha256
  ) values (
    v_id,v_bundle,v_revision,v_key,v_hash,v_actor,'APPROVE_UPDATED_HOURS',v_root,
    v_scope->>'family_booking_id',(v_scope->>'root_version')::bigint,(v_context->>'agency_id')::uuid,
    (v_scope->>'candidate_id')::uuid,(v_scope->>'client_id')::uuid,(v_scope->>'contract_id')::uuid,
    (v_scope->>'week_ending_date')::date,(v_origin->>'source_cycle_id')::uuid,v_final,
    (v_context->>'finalisation_week_ending')::date,(v_origin->>'revision_number')::bigint,
    decode(v_origin->>'manifest_hash','hex'),decode(v_origin->>'policy_fingerprint','hex'),p_before_origin,
    decode(v_basis->>'inventory_digest','hex'),decode(v_basis->>'entitlement_digest','hex'),
    private.weekly_source_publication_request_digest_v1(v_origin),(v_canonical#>>'{head_ids,0}')::uuid,
    v_canonical,private.weekly_source_publication_request_digest_v1(v_canonical));
  return v_id;
end;
$function$;

create or replace function private.weekly_source_accepted_publication_action_verify_v1(
  p_action_id uuid,p_decision_bundle_id uuid,p_bundle_revision bigint,
  p_canonical_publication_digest bytea,p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_row private.weekly_source_accepted_publication_actions_v1%rowtype;
  v_context jsonb;
  v_basis jsonb;
  v_scope jsonb;
  v_origin jsonb;
begin
  if p_action_id is null or p_decision_bundle_id is null or p_root_timesheet_id is null
     or coalesce(p_bundle_revision,0)<1 or coalesce(octet_length(p_canonical_publication_digest),0)<>32 then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ACTION_INVALID' using errcode='22023'; end if;
  select * into v_row from private.weekly_source_accepted_publication_actions_v1 a where a.action_id=p_action_id;
  if not found then return jsonb_build_object('ok',false,'code','UNAVAILABLE','action_id',p_action_id,
    'prior_origin',null,'accepted_canonical_sha256',null); end if;
  if v_row.decision_bundle_id is distinct from p_decision_bundle_id
     or v_row.bundle_revision is distinct from p_bundle_revision
     or v_row.root_timesheet_id is distinct from p_root_timesheet_id
     or v_row.accepted_canonical_sha256 is distinct from p_canonical_publication_digest
     or v_row.accepted_canonical_sha256 is distinct from
          private.weekly_source_publication_request_digest_v1(v_row.accepted_canonical_json) then
    return jsonb_build_object('ok',false,'code','INCOMPATIBLE','action_id',p_action_id,
      'prior_origin',null,'accepted_canonical_sha256',null); end if;
  v_context:=private.weekly_source_accepted_publication_context_v1(v_row.accepted_canonical_json);
  if v_context is null then return jsonb_build_object('ok',false,'code','STALE','action_id',p_action_id,
    'prior_origin',null,'accepted_canonical_sha256',null); end if;
  v_basis:=v_context->'basis'; v_scope:=v_basis->'scope';
  v_origin:=v_row.accepted_canonical_json#>'{financial_request,source_revision}';
  if (v_context->>'actor_user_id')::uuid is distinct from v_row.actor_user_id
     or (v_context->>'agency_id')::uuid is distinct from v_row.agency_id
     or (v_context->>'finalisation_week_ending')::date is distinct from v_row.finalisation_week_ending
     or v_basis->'origin' is distinct from v_row.expected_prior_origin
     or decode(v_basis->>'inventory_digest','hex') is distinct from v_row.before_inventory_sha256
     or decode(v_basis->>'entitlement_digest','hex') is distinct from v_row.before_entitlement_sha256
     or v_scope->>'family_booking_id' is distinct from v_row.family_booking_id
     or (v_scope->>'root_version')::bigint is distinct from v_row.root_version
     or (v_scope->>'candidate_id')::uuid is distinct from v_row.candidate_id
     or (v_scope->>'client_id')::uuid is distinct from v_row.client_id
     or (v_scope->>'contract_id')::uuid is distinct from v_row.contract_id
     or (v_scope->>'week_ending_date')::date is distinct from v_row.week_ending_date
     or (v_origin->>'accepted_action_id')::uuid is distinct from v_row.action_id
     or (v_origin->>'final_revision_id')::uuid is distinct from v_row.final_revision_id
     or (v_origin->>'source_cycle_id')::uuid is distinct from v_row.source_cycle_id
     or (v_origin->>'revision_number')::bigint is distinct from v_row.final_revision_number
     or decode(v_origin->>'manifest_hash','hex') is distinct from v_row.manifest_hash
     or decode(v_origin->>'policy_fingerprint','hex') is distinct from v_row.policy_fingerprint
     or private.weekly_source_publication_request_digest_v1(v_origin) is distinct from v_row.origin_sha256
     or (v_row.accepted_canonical_json#>>'{head_ids,0}')::uuid is distinct from v_row.planned_head_id then
    return jsonb_build_object('ok',false,'code','INCOMPATIBLE','action_id',p_action_id,
      'prior_origin',null,'accepted_canonical_sha256',null); end if;
  return jsonb_build_object('ok',true,'code','OK','action_id',p_action_id,
    'prior_origin',v_row.expected_prior_origin,
    'accepted_canonical_sha256',encode(v_row.accepted_canonical_sha256,'hex'));
end;
$function$;

alter function private.weekly_source_accepted_publication_context_v1(jsonb) owner to postgres;
alter function private.weekly_source_accepted_publication_action_record_v1(jsonb,jsonb,jsonb) owner to postgres;
alter function private.weekly_source_accepted_publication_action_verify_v1(uuid,uuid,bigint,bytea,uuid) owner to postgres;
revoke all on function private.weekly_source_accepted_publication_context_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_accepted_publication_action_record_v1(jsonb,jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_accepted_publication_action_verify_v1(uuid,uuid,bigint,bytea,uuid) from public,anon,authenticated,service_role;

commit;
