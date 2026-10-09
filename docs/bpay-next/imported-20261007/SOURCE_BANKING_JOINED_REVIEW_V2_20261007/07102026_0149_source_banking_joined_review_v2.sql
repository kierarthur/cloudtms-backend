-- E-only joined Source/Banking successor. Source V2 prerequisite is frozen.
-- Same0105 certificate; no future HEAD/binding, balances or alternate pricing.
\set ON_ERROR_STOP on
begin;

-- A closed server-owned annotation, not arbitrary caller JSON additions.
create or replace function private.weekly_source_destination_joined_source_context_v2(p_context jsonb)
returns jsonb language plpgsql immutable security invoker
set search_path=pg_catalog,private as $function$
declare v_joined boolean:=p_context ? 'joined_review_schema'; v_source jsonb;
begin
  perform private.weekly_source_publication_require_keys_v1(p_context,array[
    'schema_version','actor_user_id','agency_id','base_request','before_canonical',
    'destination_pay_channel','stage_snapshot','components']
    ||case when v_joined then array['financial_context','financial_plan','financial_plan_digest',
      'financial_selections','joined_review_schema'] else array[]::text[] end,'destination.joined_context');
  if v_joined and p_context->>'joined_review_schema' is distinct from 'SOURCE_DESTINATION_PAY_FINANCIAL_REVIEW_V2' then
    raise exception 'WEEKLY_SOURCE_JOINED_REVIEW_SCHEMA_INVALID' using errcode='22023'; end if;
  v_source:=p_context-array['financial_context','financial_plan','financial_plan_digest','financial_selections','joined_review_schema'];
  if v_source->>'schema_version' is distinct from 'WEEKLY_SOURCE_DESTINATION_RATE_CONTEXT_V1' then
    raise exception 'WEEKLY_SOURCE_JOINED_CONTEXT_INVALID' using errcode='22023'; end if;
  return v_source;
end $function$;

-- Cold additive complete-destination successor. Every genuine moved-A and
-- own-B HEAD component is priced and financially selected in the SAME review.
-- V2 financial packets were E-only, never installed/accepted in this rollout;
-- only exact V3 financial context/plan is admitted. No historical subsystem.
-- Pure frozen review/integrity composition. No current rates, settings, books,
-- effects, held children or binding discovery is performed by this function.
create or replace function private.weekly_source_destination_financial_frozen_review_v2(
  p_context jsonb,p_source_quote jsonb)
returns jsonb language plpgsql immutable security invoker
set search_path=pg_catalog,private as $function$
declare v_source jsonb; v_valid jsonb; v_review jsonb; v_item jsonb; v_selection jsonb; v_count integer;
  v_moved jsonb;v_own jsonb;v_combined jsonb;v_planned_ids jsonb;v_control_ids jsonb;
begin
  v_source:=private.weekly_source_destination_joined_source_context_v2(p_context);
  if p_context->>'joined_review_schema' is distinct from 'SOURCE_DESTINATION_PAY_FINANCIAL_REVIEW_V2'
     or jsonb_typeof(p_context->'financial_selections') is distinct from 'array'
     or coalesce(p_context->>'financial_plan_digest','')!~'^[0-9a-f]{64}$'
     or p_source_quote#>'{review,context}' is distinct from v_source
     or p_source_quote->>'review_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
       'WEEKLY_SOURCE_DESTINATION_REPRICE_REVIEW_V1',p_source_quote->'review'),'hex') then
    raise exception 'WEEKLY_SOURCE_JOINED_REVIEW_NOT_EXACT' using errcode='23514'; end if;
  perform private.weekly_source_publication_require_keys_v1(p_context->'financial_context',array[
    'schema_version','candidate_id','actor_user_id','planned_request_sha256','financial_scope','components'],'destination.financial_context');
  perform private.weekly_source_publication_require_keys_v1(p_context->'financial_plan',array[
    'schema_version','candidate_id','planned_request_sha256','components'],'destination.financial_plan');
  perform private.weekly_source_publication_require_keys_v1(p_context#>'{financial_context,financial_scope}',array[
    'schema_version','moved_component_ids','own_b_component_ids','comparison_component_ids'],'destination.financial_scope');
  select coalesce(jsonb_agg(c#>'{component,component_id}' order by c#>>'{component,component_id}'), '[]'::jsonb)
    into v_moved from jsonb_array_elements(v_source->'components') c where c->>'before_root_ordinal'='1';
  select coalesce(jsonb_agg(c#>'{component,component_id}' order by c#>>'{component,component_id}'), '[]'::jsonb)
    into v_own from jsonb_array_elements(v_source->'components') c where c->>'before_root_ordinal'='2';
  select coalesce(jsonb_agg(c#>'{component,component_id}' order by c#>>'{component,component_id}'), '[]'::jsonb)
    into v_combined from jsonb_array_elements(v_source->'components') c;
  select coalesce(jsonb_agg(c->'component_id' order by c->>'component_id'), '[]'::jsonb)
    into v_planned_ids from jsonb_array_elements(p_source_quote#>'{request,financial_request,member_entitlements,1,components}') c;
  select coalesce(jsonb_agg(c order by c#>>'{}'), '[]'::jsonb)
    into v_control_ids from jsonb_array_elements(p_source_quote#>'{request,control,moved_component_ids}') c;
  if p_context#>>'{financial_context,schema_version}' is distinct from 'BPAY_NEXT_SOURCE_FINANCIAL_CONTEXT_V3'
     or p_context#>>'{financial_plan,schema_version}' is distinct from 'BPAY_NEXT_SOURCE_FINANCIAL_PLAN_V3'
     or p_context#>>'{financial_context,financial_scope,schema_version}' is distinct from 'BPAY_NEXT_SOURCE_FINANCIAL_SCOPE_V3'
     or p_context#>'{financial_context,financial_scope,moved_component_ids}' is distinct from v_moved
     or p_context#>'{financial_context,financial_scope,own_b_component_ids}' is distinct from v_own
     or p_context#>'{financial_context,financial_scope,comparison_component_ids}' is distinct from v_combined
     or v_control_ids is distinct from v_moved or v_planned_ids is distinct from v_combined
     or exists(select 1 from jsonb_array_elements(v_source->'components') c
       where c->>'before_root_ordinal' not in ('1','2') or c->>'before_root_ordinal' is null
         or c->>'before_root_timesheet_id' is distinct from
           v_source#>>array['base_request','member_root_ids',((c->>'before_root_ordinal')::integer-1)::text]) then
    raise exception 'WEEKLY_SOURCE_JOINED_COMPLETE_DESTINATION_SCOPE_INVALID' using errcode='23514';end if;
  v_valid:=private.bpay_next_source_comparison_validate_plan_snapshot_v2(p_source_quote->'request',
    p_context->'financial_context',p_context->'financial_plan');
  if v_valid->'ok' is distinct from 'true'::jsonb
     or v_valid->>'financial_plan_digest' is distinct from p_context->>'financial_plan_digest'
     or p_context#>>'{financial_context,actor_user_id}' is distinct from v_source->>'actor_user_id'
     or p_context#>>'{financial_context,candidate_id}' is distinct from p_source_quote#>>'{request,candidate_id}'
     or jsonb_array_length(p_context->'financial_selections') is distinct from
       jsonb_array_length(p_context#>'{financial_plan,components}') then
    raise exception 'WEEKLY_SOURCE_JOINED_FINANCIAL_SNAPSHOT_INVALID' using errcode='23514'; end if;
  for v_item in select value from jsonb_array_elements(p_context#>'{financial_plan,components}')
    order by value->>'source_component_id' loop
    perform private.weekly_source_publication_require_keys_v1(v_item,array[
      'source_component_id','context','plan'],'destination.financial_plan_component');
    select count(*)::integer,jsonb_agg(value)->0 into v_count,v_selection
      from jsonb_array_elements(p_context->'financial_selections')
      where value->>'component_id'=v_item->>'source_component_id';
    if v_count<>1 or v_selection is distinct from v_item#>'{plan,quote_json,selection}'
       or v_item#>>'{plan,target_pay_channel}' is distinct from v_source->>'destination_pay_channel' then
      raise exception 'WEEKLY_SOURCE_JOINED_SELECTION_NOT_BOUND' using errcode='23514'; end if;
  end loop;
  v_review:=jsonb_build_object('schema_version','SOURCE_DESTINATION_PAY_FINANCIAL_REVIEW_V2',
    'source_review',p_source_quote->'review','financial_context',p_context->'financial_context',
    'financial_plan',p_context->'financial_plan','financial_plan_digest',p_context->>'financial_plan_digest',
    'financial_selections',p_context->'financial_selections');
  return jsonb_build_object('review',v_review,'review_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'SOURCE_DESTINATION_PAY_FINANCIAL_REVIEW_V2',v_review),'hex'));
end $function$;

-- Genuine pricing comes from the existing Source quote.request ONLY. Financial
-- book/policy selection is captured in the planner's one STABLE statement,
-- using the exact accepted Source channel, never a mutable Contract fallback.
create or replace function private.weekly_source_destination_financial_quote_v2(
  p_source_context jsonb,p_calculations jsonb,p_financial_selections jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public as $function$
declare v_source jsonb; v_source_quote jsonb; v_financial jsonb; v_context jsonb; v_joined jsonb;
begin
  v_source:=private.weekly_source_destination_joined_source_context_v2(p_source_context);
  if p_source_context ? 'joined_review_schema' then
    raise exception 'WEEKLY_SOURCE_JOINED_SOURCE_CONTEXT_REQUIRED' using errcode='22023'; end if;
  v_source_quote:=private.weekly_source_destination_reprice_quote_v1(v_source,p_calculations);
  select private.bpay_next_source_comparison_plan_v2((v_source->>'actor_user_id')::uuid,
    v_source_quote->'request',p_financial_selections,v_source->>'destination_pay_channel') into v_financial;
  perform private.weekly_source_publication_require_keys_v1(v_financial,array[
    'financial_context','financial_plan','financial_plan_digest'],'destination.financial_planner_result');
  v_context:=v_source||v_financial||jsonb_build_object('financial_selections',p_financial_selections,
    'joined_review_schema','SOURCE_DESTINATION_PAY_FINANCIAL_REVIEW_V2');
  v_joined:=private.weekly_source_destination_financial_frozen_review_v2(v_context,v_source_quote);
  return v_source_quote||v_joined||jsonb_build_object('context',v_context);
end $function$;

create or replace function public.weekly_source_destination_financial_preview_v2(
  p_actor uuid,p_base_request jsonb,p_calculations jsonb default null,p_financial_selections jsonb default null)
returns jsonb language plpgsql stable security definer
set search_path=public,private,pg_catalog,pg_temp as $function$
declare v_context jsonb;
begin
  v_context:=private.weekly_source_destination_reprice_context_v1(p_actor,p_base_request);
  if p_calculations is null then return jsonb_build_object('ok',true,'context',v_context); end if;
  return jsonb_build_object('ok',true,'quote',private.weekly_source_destination_financial_quote_v2(
    v_context,p_calculations,p_financial_selections),'financial_review_accepted',false);
end $function$;

-- Complete versioned acceptance; existing Source V2 public v1 entry is untouched.
create or replace function public.weekly_source_destination_financial_accept_v2(
  p_actor uuid,p_base_request jsonb,p_calculations jsonb,p_financial_selections jsonb,p_review_sha256 text,
  p_idempotency_key text,p_reason text
) returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_input bytea; v_saved private.weekly_source_destination_reprice_witness_v1%rowtype;
  v_context jsonb; v_source_context jsonb; v_quote jsonb; v_plan_item jsonb; v_request jsonb; v_lock jsonb; v_roots uuid[];
  v_id uuid; v_time timestamptz; v_record jsonb; v_result jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501'; end if;
  if p_actor is null or char_length(btrim(coalesce(p_reason,''))) not between 1 and 2000
     or char_length(coalesce(p_idempotency_key,'')) not between 16 and 200
     or coalesce(p_review_sha256,'')!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_ACCEPT_INVALID' using errcode='22023'; end if;
  v_input:=private.weekly_source_sha256_jsonb_v1('SOURCE_DESTINATION_PAY_FINANCIAL_ACCEPT_V2',jsonb_build_object(
    'actor',p_actor,'base',p_base_request,'calculations',p_calculations,'financial_selections',p_financial_selections,
    'review',p_review_sha256,'reason',p_reason));
  select * into v_saved from private.weekly_source_destination_reprice_witness_v1 where idempotency_key=p_idempotency_key;
  if found then
    if v_saved.input_sha256 is distinct from v_input or v_saved.actor_user_id is distinct from p_actor then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_REPLAY_CONFLICT' using errcode='22023'; end if;
    v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_saved.accepted_request_json);
    if v_result->'ok' is distinct from 'true'::jsonb or not (
      v_result->'published' is not distinct from 'true'::jsonb or (v_result->>'code' is not distinct from 'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE'
        and v_result->>'pending_bundle_verified' is not null and v_result#>'{pending,ok}' is not distinct from 'true'::jsonb)) then
      raise exception 'WEEKLY_SOURCE_JOINED_PUBLICATION_NOT_ACCEPTED' using errcode='55000',detail=v_result::text;
    end if;
    return jsonb_build_object('witness_id',v_saved.id,'source_publication',v_result,'financial_comparison_resolved',false,'financial_review_accepted',true);
  end if;
  -- A new decision must see a fresh post-I1 statement snapshot. Historical
  -- exact replay above does not acquire today's economics or this new gate.
  if current_setting('transaction_isolation') is distinct from 'read committed' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_READ_COMMITTED_REQUIRED' using errcode='25001'; end if;
  v_roots:=array[(p_base_request#>>'{member_root_ids,0}')::uuid,(p_base_request#>>'{member_root_ids,1}')::uuid];
  v_lock:=private.weekly_source_lock_and_resolve_families_v1((p_base_request->>'candidate_id')::uuid,v_roots,
    'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',gen_random_uuid(),'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  if v_lock->>'ok' is distinct from 'true' or v_lock->>'gate' is distinct from 'GRANTED' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_UNAVAILABLE' using errcode='55000'; end if;
  -- Re-enter the exact replay after I-1. This is a bounded single reread,
  -- never a replay using today's rates or a second independent decision.
  select * into v_saved from private.weekly_source_destination_reprice_witness_v1 where idempotency_key=p_idempotency_key;
  if found then
    if v_saved.input_sha256 is distinct from v_input or v_saved.actor_user_id is distinct from p_actor then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_REPLAY_CONFLICT' using errcode='22023'; end if;
    v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_saved.accepted_request_json);
    if v_result->'ok' is distinct from 'true'::jsonb or not (
      v_result->'published' is not distinct from 'true'::jsonb or (v_result->>'code' is not distinct from 'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE'
        and v_result->>'pending_bundle_verified' is not null and v_result#>'{pending,ok}' is not distinct from 'true'::jsonb)) then
      raise exception 'WEEKLY_SOURCE_JOINED_PUBLICATION_NOT_ACCEPTED' using errcode='55000',detail=v_result::text;
    end if;
    return jsonb_build_object('witness_id',v_saved.id,'source_publication',v_result,'financial_comparison_resolved',false,'financial_review_accepted',true);
  end if;
  -- Exactly ONE fresh STABLE context SELECT after I1 and its replay reread.
  -- Pricing descendants and full stage helper inputs use this statement's
  -- snapshot; the nested lifecycle signature(false) is not globally STABLE.
  -- Existing I1/currentwork/publisher guards retain work currentness. This is
  -- accepted-at-this-read pricing, never latest-at-release pricing.
  select private.weekly_source_destination_reprice_context_v1(p_actor,p_base_request) into v_source_context;
  select private.weekly_source_destination_financial_quote_v2(v_source_context,p_calculations,p_financial_selections) into v_quote;
  v_context:=v_quote->'context';
  if v_quote->>'review_sha256' is distinct from p_review_sha256 then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_REVIEW_STALE' using errcode='40001'; end if;
  v_id:=gen_random_uuid(); v_time:=transaction_timestamp(); v_request:=v_quote->'request';
  v_request:=jsonb_set(v_request,'{financial_request,contract_choices,1,destination_reprice}',jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_DESTINATION_REPRICE_V1','witness_id',v_id,'review_sha256',p_review_sha256));
  -- Whole own-B acknowledgement is accepted at the same exact shown digest;
  -- the recorder persists the existing actor/time columns and replay checks.
  v_request:=jsonb_set(v_request,'{control,whole_root_office_review}',jsonb_build_object(
    'reviewed',true,'reviewed_by_user_id',p_actor,'reviewed_at_utc',v_time,'decision_id',v_request->>'decision_id'));
  insert into private.weekly_source_destination_reprice_witness_v1(id,decision_bundle_id,bundle_revision,
    idempotency_key,input_sha256,actor_user_id,reason,context_json,economic_snapshots_json,review_sha256,
    accepted_request_json,accepted_request_sha256,accepted_control_sha256,accepted_at_utc)
  values(v_id,(v_request->>'decision_bundle_id')::uuid,(v_request->>'bundle_revision')::bigint,p_idempotency_key,
    v_input,p_actor,p_reason,v_context,v_quote->'components',decode(p_review_sha256,'hex'),v_request,
    private.weekly_source_publication_request_digest_v1(private.weekly_source_publication_request_canonical_v1(v_request,'IMMEDIATE',null)),
    sha256(convert_to((v_request->'control')::text,'UTF8')),v_time);
  -- ONE accepted family per component, on the REAL CURRENT input binding.
  -- Pending N is planned; observed N/native balances remain actual. Register
  -- under the real owner locks before I4; a changed book rolls back this pair.
  for v_plan_item in select value from jsonb_array_elements(v_context#>'{financial_plan,components}')
    order by value->>'source_component_id' loop
    perform private.bpay_next_source_comparison_register_pending_core_v2(v_id,v_plan_item->'context',v_plan_item->'plan');
  end loop;
  v_record:=private.weekly_source_entitlement_proposal_record_v1(v_request,(v_context->>'agency_id')::uuid,
    (v_request#>>'{financial_request,contract_choices,0,contract_id}')::uuid,
    (v_request#>>'{financial_request,contract_choices,0,week_ending_date}')::date,p_actor);
  if v_record->>'ok' is distinct from 'true' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_PROPOSAL_NOT_RECORDED' using errcode='55000'; end if;
  v_result:=private.weekly_source_entitlement_publish_immediate_v1(v_request);
  -- Only the existing verified I5 pending save is an accepted non-publication.
  -- Ordinary publisher refusal cannot commit a stranded PENDING comparison.
  if v_result->'ok' is distinct from 'true'::jsonb or not (
    v_result->'published' is not distinct from 'true'::jsonb or (v_result->>'code' is not distinct from 'WEEKLY_SOURCE_PUBLICATION_DEFERRED_PENDING_FREEZE'
      and v_result->>'pending_bundle_verified' is not null and v_result#>'{pending,ok}' is not distinct from 'true'::jsonb)) then
    raise exception 'WEEKLY_SOURCE_JOINED_PUBLICATION_NOT_ACCEPTED' using errcode='55000',detail=v_result::text;
  end if;
  return jsonb_build_object('witness_id',v_id,'review_sha256',p_review_sha256,
    'source_publication',v_result,'financial_comparison_resolved',false,'financial_review_accepted',true);
end;
$function$;

-- Complete winning verifier successor. Pure Source witnesses retain their
-- original integrity branch; joined witnesses strip ONLY the five known keys
-- and pure-validate the frozen financial snapshot, never current economics.
create or replace function private.weekly_source_destination_reprice_verify_v1(
  p_canonical jsonb,p_control jsonb
) returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_saved private.weekly_source_destination_reprice_witness_v1%rowtype;
  v_request jsonb; v_calculations jsonb; v_quote jsonb; v_expected jsonb;
  v_source_context jsonb; v_joined jsonb; v_expected_review_sha256 text;
begin
  if p_canonical#>'{financial_request,contract_choices,1,destination_reprice}' is null then return null; end if;
  select * into strict v_saved from private.weekly_source_destination_reprice_witness_v1
    where id=(p_canonical#>>'{financial_request,contract_choices,1,destination_reprice,witness_id}')::uuid for share;
  v_request:=jsonb_set(jsonb_set(p_canonical,'{publication_mode}','"IMMEDIATE"'::jsonb),'{pending_bundle_id}','null'::jsonb);
  if v_saved.review_sha256 is distinct from decode(p_canonical#>>'{financial_request,contract_choices,1,destination_reprice,review_sha256}','hex')
     or v_saved.accepted_request_sha256 is distinct from private.weekly_source_publication_request_digest_v1(v_request)
     or v_saved.accepted_control_sha256 is distinct from sha256(convert_to(p_control::text,'UTF8'))
     or v_saved.accepted_request_json->'control' is distinct from p_control
     or not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
       where b.decision_bundle_id=v_saved.decision_bundle_id and b.bundle_revision=v_saved.bundle_revision
         and b.decided_by_user_id=v_saved.actor_user_id and b.whole_root_review_required
         and b.whole_root_reviewed_by_user_id=v_saved.actor_user_id
         and b.whole_root_reviewed_at_utc=v_saved.accepted_at_utc) then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_WITNESS_NOT_EXACT' using errcode='23514'; end if;
  -- Re-derive the accepted review and output from immutable certificate
  -- inputs. There is deliberately NO fresh mutable context/rate/settings read.
  perform private.weekly_source_destination_stage_snapshot_check_v1(v_saved.context_json->'stage_snapshot');
  select jsonb_agg(jsonb_build_object('component_id',value->>'component_id',
      'economic_snapshot',value->'economic_snapshot','price_comparison',value->'price_comparison')
      order by value->>'component_id') into v_calculations
    from jsonb_array_elements(v_saved.economic_snapshots_json);
  v_source_context:=private.weekly_source_destination_joined_source_context_v2(v_saved.context_json);
  v_quote:=private.weekly_source_destination_reprice_quote_v1(v_source_context,v_calculations);
  v_expected_review_sha256:=v_quote->>'review_sha256';
  if v_saved.context_json ? 'joined_review_schema' then
    v_joined:=private.weekly_source_destination_financial_frozen_review_v2(v_saved.context_json,v_quote);
    v_expected_review_sha256:=v_joined->>'review_sha256';
  end if;
  v_expected:=jsonb_set(v_quote->'request','{financial_request,contract_choices,1,destination_reprice}',
    jsonb_build_object('schema_version','WEEKLY_SOURCE_DESTINATION_REPRICE_V1','witness_id',v_saved.id,
      'review_sha256',encode(v_saved.review_sha256,'hex')));
  v_expected:=jsonb_set(v_expected,'{control,whole_root_office_review}',jsonb_build_object(
    -- Preserve the actual accepted JSON time representation across session
    -- timezone changes; its typed instant is checked against server time below.
    'reviewed',true,'reviewed_by_user_id',v_saved.actor_user_id,
    'reviewed_at_utc',v_saved.accepted_request_json#>'{control,whole_root_office_review,reviewed_at_utc}',
    'decision_id',v_expected->>'decision_id'));
  if v_saved.context_json->>'actor_user_id' is distinct from v_saved.actor_user_id::text
     or (v_saved.accepted_request_json#>>'{control,whole_root_office_review,reviewed_at_utc}')::timestamptz
       is distinct from v_saved.accepted_at_utc
     or v_saved.context_json#>>'{stage_snapshot,destination_pay_channel}' is distinct from
       v_saved.context_json->>'destination_pay_channel'
     or v_saved.context_json#>>'{stage_snapshot,root_timesheet_id}' is distinct from
       v_saved.accepted_request_json#>>'{member_root_ids,1}'
     or v_saved.context_json#>>'{stage_snapshot,contract_id}' is distinct from
       v_saved.accepted_request_json#>>'{financial_request,contract_choices,1,contract_id}'
     or v_saved.context_json#>>'{stage_snapshot,candidate_id}' is distinct from
       v_saved.accepted_request_json->>'candidate_id'
     or v_expected_review_sha256 is distinct from encode(v_saved.review_sha256,'hex')
     or v_quote->'components' is distinct from v_saved.economic_snapshots_json
     or v_expected is distinct from v_saved.accepted_request_json
     or v_saved.decision_bundle_id is distinct from (v_expected->>'decision_bundle_id')::uuid
     or v_saved.bundle_revision is distinct from (v_expected->>'bundle_revision')::bigint
     or v_saved.accepted_request_sha256 is distinct from private.weekly_source_publication_request_digest_v1(
       private.weekly_source_publication_request_canonical_v1(v_expected,'IMMEDIATE',null))
     or v_saved.accepted_control_sha256 is distinct from sha256(convert_to((v_expected->'control')::text,'UTF8')) then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_CERTIFICATE_CORRUPT' using errcode='23514'; end if;
  return jsonb_build_object('witness_id',v_saved.id,'review_sha256',encode(v_saved.review_sha256,'hex'),
    'components',v_saved.economic_snapshots_json,'destination_pay_channel',v_saved.context_json->>'destination_pay_channel');
end;
$function$;

create or replace function private.weekly_source_destination_reprice_context_v1(
  p_actor uuid,p_base_request jsonb
) returns jsonb language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_base jsonb; v_rebuilt jsonb; v_source jsonb; v_target jsonb; v_inventory jsonb;
  v_before jsonb; v_component jsonb; v_components jsonb:='[]'::jsonb;
  v_before_root_ordinal integer;
  v_roots uuid[]; v_moved uuid[]; v_head uuid; v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype; v_detail private.bpay_next_source_chosen_detail%rowtype;
  v_tf public.timesheets_financials%rowtype; v_candidate public.candidates%rowtype; v_client public.clients%rowtype;
  v_stage jsonb; v_schedule jsonb;
  v_e public.weekly_source_row_economic_snapshots%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_final public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype; v_group public.weekly_source_groups%rowtype;
  v_policy jsonb; v_settings jsonb; v_profile text; v_key text; v_prefix text; v_pay jsonb; v_charge jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501'; end if;
  v_base:=private.weekly_source_publication_request_canonical_v1(p_base_request,'IMMEDIATE',null);
  if p_actor is null or v_base#>'{financial_request,contract_choices,1,destination_reprice}' is not null
     or jsonb_array_length(v_base->'member_root_ids')<>2
     or p_base_request#>>'{control,bundle_kind}' is distinct from 'CROSS_CONTRACT_A_B'
     or v_base#>>'{financial_request,source_revision,origin_kind}' is not null then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_BASE_INVALID' using errcode='22023'; end if;
  v_roots:=array[(v_base#>>'{member_root_ids,0}')::uuid,(v_base#>>'{member_root_ids,1}')::uuid];
  select array_agg((value#>>'{}')::uuid order by value#>>'{}') into v_moved
    from jsonb_array_elements(p_base_request#>'{control,moved_component_ids}');
  v_source:=v_base#>'{financial_request,member_entitlements,0}';
  v_target:=v_base#>'{financial_request,member_entitlements,1}';
  select * into strict v_root from public.timesheets where timesheet_id=v_roots[2];
  select * into strict v_contract from public.contracts where id=v_root.contract_id;
  -- These are the actual1550 inputs, read under this one STABLE pricing
  -- statement. No invented fallback, rows, rates, clocks or bucket units.
  select * into v_tf from public.timesheets_financials
    where timesheet_id=v_root.timesheet_id and is_current=true;
  select * into strict v_candidate from public.candidates where id=v_contract.candidate_id;
  select * into strict v_client from public.clients where id=v_contract.client_id;
  select jsonb_agg(to_jsonb(r) order by r.rate_family,r.rate_code) into v_schedule
    from private.bpay_next_contract_rate_rows_v1(v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,
      coalesce(v_tf.mileage_pay_rate,v_contract.mileage_pay_rate,v_candidate.mileage_pay_rate),
      coalesce(v_tf.mileage_charge_rate,v_contract.mileage_charge_rate,v_client.mileage_charge_rate)) r;
  v_stage:=jsonb_build_object('schema_version','WEEKLY_SOURCE_DESTINATION_STAGE_SNAPSHOT_V1',
    'root_timesheet_id',v_root.timesheet_id,'root_timesheet_version',v_root.version,'root_booking_id',v_root.booking_id,
    'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,'contract_id',v_contract.id,
    'financial_snapshot_id',v_tf.id,'destination_pay_channel',v_contract.pay_method_snapshot,
    'metadata',jsonb_build_object('candidate_display_name',coalesce(nullif(btrim(v_candidate.display_name),''),
      nullif(btrim(concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),''),v_candidate.id::text),
      'candidate_reference',v_candidate.tms_ref,'client_display_name',v_client.name,
      'job_title',v_contract.role,'band_label',v_contract.band,'timesheet_reference',v_root.reference_number),
    'metadata_inputs',jsonb_build_object('candidate_display_name',v_candidate.display_name,
      'candidate_first_name',v_candidate.first_name,'candidate_last_name',v_candidate.last_name,
      'candidate_tms_ref',v_candidate.tms_ref,'client_name',v_client.name,'contract_role',v_contract.role,
      'contract_band',v_contract.band,'timesheet_reference',v_root.reference_number),
    'rate_inputs',jsonb_build_object('rates_json',v_contract.rates_json,'bucket_labels_json',v_contract.bucket_labels_json,
      'additional_rates_json',v_contract.additional_rates_json,
      'mileage_pay_rate',coalesce(v_tf.mileage_pay_rate,v_contract.mileage_pay_rate,v_candidate.mileage_pay_rate),
      'mileage_charge_rate',coalesce(v_tf.mileage_charge_rate,v_contract.mileage_charge_rate,v_client.mileage_charge_rate),
      'mileage_fallback_inputs',jsonb_build_object('financial_snapshot_id',v_tf.id,
        'financial_pay_rate',v_tf.mileage_pay_rate,'financial_charge_rate',v_tf.mileage_charge_rate,
        'contract_pay_rate',v_contract.mileage_pay_rate,'contract_charge_rate',v_contract.mileage_charge_rate,
        'candidate_pay_rate',v_candidate.mileage_pay_rate,'client_charge_rate',v_client.mileage_charge_rate)),
    'rate_schedule',v_schedule);
  perform private.weekly_source_destination_stage_snapshot_check_v1(v_stage);
  if v_contract.candidate_id is distinct from (v_base->>'candidate_id')::uuid
     or v_contract.pay_method_snapshot is null
     or v_contract.pay_method_snapshot not in ('PAYE','UMBRELLA') then
    raise exception 'WEEKLY_SOURCE_DESTINATION_RATE_OWNER_INVALID' using errcode='23514'; end if;
  v_rebuilt:=private.weekly_source_entitlement_proposal_cross_contract_request_v1(
    v_roots[1],v_roots[2],(v_base#>>'{financial_request,source_revision,final_revision_id}')::uuid,
    (v_base->>'decision_bundle_id')::uuid,(v_base->>'bundle_revision')::bigint,
    (v_base#>>'{head_ids,0}')::uuid,(v_base#>>'{head_ids,1}')::uuid,(v_base->>'decision_id')::uuid,
    v_moved,v_source->>'authority_kind',v_target->>'authority_kind',
    v_base#>>'{financial_request,contract_choices,0,selection_method}',
    v_base#>>'{financial_request,contract_choices,1,selection_method}',
    case when p_base_request#>'{control,target_root_authorisation}'<>'null'::jsonb then p_actor end,
    case when p_base_request#>'{control,whole_root_office_review}'<>'null'::jsonb then p_actor end,
    (p_base_request#>>'{control,whole_root_office_review,reviewed_at_utc}')::timestamptz);
  if private.weekly_source_publication_request_canonical_v1(v_rebuilt,'IMMEDIATE',null) is distinct from v_base
     or v_rebuilt->'control' is distinct from p_base_request->'control' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_BEFORE_CHANGED' using errcode='40001'; end if;
  select * into strict v_final from public.weekly_source_final_revisions
    where id=(v_base#>>'{financial_request,source_revision,final_revision_id}')::uuid and state='CURRENT';
  select * into strict v_cycle from public.weekly_source_cycles where id=v_final.source_cycle_id;
  select * into strict v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(p_actor,'APPROVE_PROTECTED_PAY',v_group.id,
    v_contract.client_id,v_cycle.finalisation_week_ending);
  perform private.weekly_source_office_authority_v1(p_actor,'APPROVE_PROTECTED_PAY',v_group.id,
    (select c.client_id from public.contracts c join public.timesheets t on t.contract_id=c.id
      where t.timesheet_id=v_roots[1]),v_cycle.finalisation_week_ending);
  -- Same actual pricing statement and SAME rate-row calculator inputs for
  -- every genuine A and own-B component. Neither B work nor money is invented.
  for v_before_root_ordinal in 1..2 loop
  v_inventory:=private.weekly_source_effective_inventory_v1(v_roots[v_before_root_ordinal]);
  if v_inventory->'ok' is distinct from 'true'::jsonb
     or jsonb_typeof(v_inventory->'components') is distinct from 'array' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_BEFORE_INVENTORY_REQUIRED' using errcode='55000';end if;
  if v_before_root_ordinal=2 and jsonb_array_length(v_inventory->'components')=0 then
    continue; -- Only real empty B is blank, not a missing financial snapshot.
  end if;
  v_head:=(v_inventory->>'head_id')::uuid;
  -- V1 positively supports genuine chosen HEAD economics only. Initial TSFIN,
  -- amount-led components and later bucket/policy changes are explicit OPEN
  -- obligations, never implicitly priced with invented hourly equivalence.
  if v_head is null then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_INITIAL_DETAIL_OWNER_REQUIRED' using errcode='55000'; end if;
  for v_before in select value from jsonb_array_elements(v_inventory->'components')
    order by value->>'component_id'
  loop
    v_component:=private.weekly_source_publication_component_canonical_v1(v_before-'component_sha256','destination.before');
    if v_component->>'component_kind' is distinct from 'WORKED_TIME'
       or v_component->'unit_count'<>'null'::jsonb or v_component->'unit_pay_rate'<>'null'::jsonb then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_COMPONENT_POLICY_REQUIRED' using errcode='55000',
        detail=jsonb_build_object('component_id',v_component->>'component_id','component_kind',v_component->>'component_kind')::text;
    end if;
    select * into strict v_detail from private.bpay_next_source_chosen_detail
      where head_id=v_head and component_id=(v_component->>'component_id')::uuid;
    if v_detail.component_sha256 is distinct from private.weekly_source_publication_request_digest_v1(
         private.weekly_source_publication_component_content_v1(v_component))
       or v_detail.detail_sha256 is distinct from sha256(convert_to(v_detail.detail_json::text,'UTF8'))
       or nullif(v_detail.detail_json->>'economic_snapshot_id','') is null then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_ECONOMIC_ORIGIN_REQUIRED' using errcode='23514'; end if;
    select * into strict v_e from public.weekly_source_row_economic_snapshots
      where id=(v_detail.detail_json->>'economic_snapshot_id')::uuid;
    select * into strict v_row from public.weekly_source_upload_rows where id=v_e.upload_row_id;
    select p.profile_code into strict v_profile from public.weekly_source_uploads u
      join public.weekly_source_format_profiles p on p.id=u.source_format_profile_id where u.id=v_row.upload_id;
    if v_e.work_event_id::text is distinct from v_component->>'component_member_identity'
       or v_e.candidate_id is distinct from v_contract.candidate_id
       or v_detail.detail_json->>'work_date' is distinct from v_component->>'work_date'
       or (v_detail.detail_json->>'break_minutes')::integer is distinct from v_e.break_minutes
       or to_char(v_row.start_at_local,'HH24:MI') is distinct from v_detail.detail_json->>'start'
       or to_char(v_row.end_at_local,'HH24:MI') is distinct from v_detail.detail_json->>'end'
       or v_row.work_date::text is distinct from v_component->>'work_date' then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_WORK_FACTS_CHANGED' using errcode='23514'; end if;
    v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,(v_component->>'work_date')::date);
    v_settings:=private._contract_settings_effective_core_v1(v_contract.client_id,v_contract.id,
      (v_component->>'work_date')::date,'IMPORT',null);
    if v_policy->>'weekly_rate_classification_method' is distinct from v_e.rate_method then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_BUCKET_POLICY_REVIEW_REQUIRED' using errcode='55000'; end if;
    v_prefix:=case when v_contract.pay_method_snapshot='PAYE' then 'paye_' else 'umb_' end;
    v_pay:='{}'::jsonb; v_charge:='{}'::jsonb;
    foreach v_key in array array['day','night','sat','sun','bh'] loop
      if coalesce(v_contract.rates_json->>(v_prefix||v_key),'')!~'^[+]?[0-9]+([.][0-9]{1,6})?$'
         or coalesce(v_contract.rates_json->>('charge_'||v_key),'')!~'^[+]?[0-9]+([.][0-9]{1,6})?$'
         or (v_contract.rates_json->>(v_prefix||v_key))::numeric<=0
         or (v_contract.rates_json->>('charge_'||v_key))::numeric<=0 then
        raise exception 'WEEKLY_SOURCE_DESTINATION_RATE_AUTHORITY_INCOMPLETE' using errcode='55000'; end if;
      v_pay:=v_pay||jsonb_build_object(v_key,(v_contract.rates_json->>(v_prefix||v_key))::numeric);
      v_charge:=v_charge||jsonb_build_object(v_key,(v_contract.rates_json->>('charge_'||v_key))::numeric);
      if coalesce((v_component->>('hours_'||v_key))::numeric,0) is distinct from
        (to_jsonb(v_e)->>('hours_'||v_key))::numeric then
        raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_BUCKET_FACTS_CHANGED' using errcode='23514'; end if;
    end loop;
    v_components:=v_components||jsonb_build_array(jsonb_build_object(
      'component',v_component,'before_root_ordinal',v_before_root_ordinal,
      'before_root_timesheet_id',v_roots[v_before_root_ordinal],'before_head_id',v_head,
      'before_component_sha256',encode(v_detail.component_sha256,'hex'),
      'before_detail',v_detail.detail_json,'before_detail_sha256',encode(v_detail.detail_sha256,'hex'),
      'original_economic_snapshot_id',v_e.id,'original_calculation_fingerprint',encode(v_e.calculation_fingerprint,'hex'),
      'original_economic',v_e.canonical_result_json,
      'server_row',jsonb_build_object('profile_id',v_profile,'start_at_local',to_char(v_row.start_at_local,'YYYY-MM-DD"T"HH24:MI:SS'),
        'end_at_local',to_char(v_row.end_at_local,'YYYY-MM-DD"T"HH24:MI:SS'),'break_minutes',v_e.break_minutes,
        'actual_net_minutes',v_e.paid_minutes,'source_shift_charge_pence',v_row.source_shift_charge_pence::text),
      'destination_contract',jsonb_build_object('contract_id',v_contract.id,'pay_type',v_contract.pay_method_snapshot,
        'updated_at',v_contract.updated_at,'rates_json',v_contract.rates_json,'additional_rates_json',v_contract.additional_rates_json,
        -- The real owner includes a read-time clock outside its fingerprint.
        -- Retain every authority fact; only that non-economic clock is omitted
        -- so two unchanged reads can share one exact review digest.
        'effective_policy',v_policy,'settings_authority',v_settings-'resolved_at_utc'),
      'destination_pay_rates',v_pay,'destination_charge_rates',v_charge));
  end loop;
  end loop;
  return jsonb_build_object('schema_version','WEEKLY_SOURCE_DESTINATION_RATE_CONTEXT_V1',
    'actor_user_id',p_actor,'agency_id',v_group.agency_id,'base_request',p_base_request,
    'before_canonical',v_base,'destination_pay_channel',v_contract.pay_method_snapshot,
    'stage_snapshot',v_stage,'components',v_components);
end;
$function$;

create or replace function private.weekly_source_destination_reprice_quote_v1(
  p_context jsonb,p_calculations jsonb
) returns jsonb language plpgsql immutable
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare
  v_items jsonb:='[]'::jsonb; v_target jsonb:='[]'::jsonb; v_item jsonb; v_calc jsonb; v_e jsonb;
  v_original jsonb; v_component jsonb; v_after jsonb; v_request jsonb; v_detail jsonb;
  v_key text; v_pay numeric; v_charge numeric; v_sign integer; v_review jsonb; v_count integer;
  v_price jsonb; v_price_expected jsonb; v_source_pence bigint; v_calculated_pence bigint;
  v_difference bigint; v_result text; v_needs_acceptance boolean; v_acceptance text;
begin
  if jsonb_typeof(p_calculations) is distinct from 'array'
     or jsonb_array_length(p_calculations) is distinct from jsonb_array_length(p_context->'components') then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_CALCULATION_INCOMPLETE' using errcode='22023'; end if;
  for v_item in select value from jsonb_array_elements(p_context->'components') order by value#>>'{component,component_id}' loop
    select count(*)::integer,jsonb_agg(value)->0 into v_count,v_calc from jsonb_array_elements(p_calculations)
      where value->>'component_id'=v_item#>>'{component,component_id}';
    if v_count<>1 then raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_CALCULATION_INCOMPLETE' using errcode='22023'; end if;
    perform private.weekly_source_publication_require_keys_v1(v_calc,array['component_id','economic_snapshot','price_comparison'],'destination.calculation');
    v_e:=v_calc->'economic_snapshot'; v_original:=v_item->'original_economic'; v_component:=v_item->'component';
    perform private.weekly_source_publication_require_keys_v1(v_e,array[
      'schema_version','calculator_version','source_mode','rate_method','sign','paid_minutes','break_minutes',
      'bucket_minutes','hours','pay_rates','charge_rates','total_pay_pence','calculated_charge_pence']
      ||case when v_e ? 'break_allocation' then array['break_allocation'] else array[]::text[] end,'destination.economic_snapshot');
    if v_e->>'schema_version' is distinct from 'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1'
       or v_e->>'calculator_version' is distinct from 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1'
       or v_e->'source_mode' is distinct from v_original->'source_mode'
       or v_e->'rate_method' is distinct from v_original->'rate_method'
       or v_e->'sign' is distinct from v_original->'sign'
       or v_e->'paid_minutes' is distinct from v_original->'paid_minutes'
       or v_e->'break_minutes' is distinct from v_original->'break_minutes'
       or v_e->'bucket_minutes' is distinct from v_original->'bucket_minutes'
       or v_e->'hours' is distinct from v_original->'hours'
       or coalesce(v_e->'break_allocation','null'::jsonb) is distinct from coalesce(v_original->'break_allocation','null'::jsonb)
       or v_e->'pay_rates' is distinct from v_item->'destination_pay_rates'
       or v_e->'charge_rates' is distinct from v_item->'destination_charge_rates'
       or coalesce(v_e->>'total_pay_pence','')!~'^-?[0-9]{1,12}$'
       or coalesce(v_e->>'calculated_charge_pence','')!~'^-?[0-9]{1,12}$' then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_CALCULATION_NOT_GENUINE' using errcode='23514'; end if;
    v_pay:=0; v_charge:=0; v_sign:=(v_original->>'sign')::integer;
    foreach v_key in array array['day','night','sat','sun','bh'] loop
      v_pay:=v_pay+abs((v_original#>>array['hours',v_key])::numeric)*(v_item#>>array['destination_pay_rates',v_key])::numeric;
      v_charge:=v_charge+abs((v_original#>>array['hours',v_key])::numeric)*(v_item#>>array['destination_charge_rates',v_key])::numeric;
    end loop;
    v_pay:=round(v_pay,2)*v_sign; v_charge:=round(v_charge,2)*v_sign;
    if (v_e->>'total_pay_pence')::bigint is distinct from (v_pay*100)::bigint
       or (v_e->>'calculated_charge_pence')::bigint is distinct from (v_charge*100)::bigint then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_TOTAL_NOT_REDERIVED' using errcode='23514'; end if;
    -- Re-derive the existing actual comparator's signed-pence classification.
    -- It is shown and accepted with the whole vector, not silently discarded
    -- when a destination charge differs from immutable source invoice facts.
    v_price:=v_calc->'price_comparison';
    perform private.weekly_source_publication_require_keys_v1(v_price,array['source_mode','result',
      'requires_office_acceptance','acceptance_kind','calculated_comparison_charge_pence',
      'source_validation_charge_pence','invoice_presentation_charge_pence','amount_authority',
      'canonical_pay_pence','signed_difference_pence'],'destination.price_comparison');
    v_calculated_pence:=(v_e->>'calculated_charge_pence')::bigint;
    if v_e->>'source_mode'='NHSP_WEEKLY' then
      if coalesce(v_item#>>'{server_row,source_shift_charge_pence}','')!~'^-?[0-9]{1,12}$' then
        raise exception 'NHSP_SOURCE_CHARGE_INVALID' using errcode='23514'; end if;
      v_source_pence:=(v_item#>>'{server_row,source_shift_charge_pence}')::bigint;
      if v_calculated_pence=0 and v_source_pence<>0 then
        raise exception 'NHSP_CALCULATED_CHARGE_ZERO_INVALID' using errcode='23514'; end if;
      if (v_source_pence>0 and v_calculated_pence<0) or (v_source_pence<0 and v_calculated_pence>0) then
        raise exception 'NHSP_SOURCE_CHARGE_SIGN_INVALID' using errcode='23514'; end if;
      v_difference:=v_source_pence-v_calculated_pence;
      v_result:=case when v_source_pence<>0 and v_calculated_pence<>0 and v_difference=0 then 'EXACT'
        when v_source_pence<>0 and v_calculated_pence<>0 and abs(v_difference)=1 then 'SOURCE_ROUNDING_EQUIVALENT'
        when v_source_pence=0 and v_calculated_pence<>0 then 'ZERO_SOURCE_CHARGE' else 'MISMATCH' end;
      v_needs_acceptance:=v_result in ('MISMATCH','ZERO_SOURCE_CHARGE');
      v_acceptance:=case when v_result='ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO'
        when v_result='MISMATCH' then 'ACCEPTED_DISPARITY' end;
      v_price_expected:=jsonb_build_object('source_mode','NHSP_WEEKLY','result',v_result,
        'requires_office_acceptance',v_needs_acceptance,'acceptance_kind',v_acceptance,
        'calculated_comparison_charge_pence',v_calculated_pence::text,'source_validation_charge_pence',v_source_pence::text,
        'invoice_presentation_charge_pence',v_source_pence::text,'amount_authority','VALIDATED_SOURCE_PENCE',
        'canonical_pay_pence',v_e->>'total_pay_pence','signed_difference_pence',v_difference::text);
    else
      v_price_expected:=jsonb_build_object('source_mode','HEALTHROSTER_WEEKLY','result','NOT_APPLICABLE',
        'requires_office_acceptance',false,'acceptance_kind',null,
        'calculated_comparison_charge_pence',v_calculated_pence::text,'source_validation_charge_pence',null,
        'invoice_presentation_charge_pence',v_calculated_pence::text,'amount_authority','CLOUDTMS_CALCULATION',
        'canonical_pay_pence',v_e->>'total_pay_pence','signed_difference_pence',null);
    end if;
    if v_price is distinct from v_price_expected then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_PRICE_COMPARISON_NOT_REDERIVED' using errcode='23514'; end if;
    -- Stable work identity, quantity, exclusions and original charge remain
    -- exact. Destination charge calculation is informational, never invoice
    -- or Source billing-movement authority.
    v_after:=v_component||jsonb_build_object('pay_ex_vat',v_pay::numeric(12,2)::text);
    v_detail:=v_item->'before_detail'||jsonb_build_object('rates',v_item->'destination_pay_rates',
      'source_component_sha256',encode(private.weekly_source_publication_request_digest_v1(
        private.weekly_source_publication_component_content_v1(v_after)),'hex'),
      'destination_reprice',jsonb_build_object('schema_version','WEEKLY_SOURCE_DESTINATION_REPRICE_V1',
        'destination_contract_id',v_item#>>'{destination_contract,contract_id}',
        'destination_pay_channel',p_context->>'destination_pay_channel',
        'original_economic_snapshot_id',v_item->>'original_economic_snapshot_id',
        'before_component_sha256',v_item->>'before_component_sha256',
        'before_detail_sha256',v_item->>'before_detail_sha256'));
    if octet_length(v_detail::text)>4096 then
      raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_DETAIL_LIMIT' using errcode='22023'; end if;
    v_items:=v_items||jsonb_build_array(jsonb_build_object('component_id',v_component->>'component_id',
      'before_component',v_component,'after_content',private.weekly_source_publication_component_content_v1(v_after),
      'detail',v_detail,'economic_snapshot',v_e,'price_comparison',v_price_expected)
      -- Old frozen Source certificates retain their byte-exact quote shape.
      ||case when v_item ? 'before_root_ordinal' then jsonb_build_object(
        'before_root_ordinal',v_item->'before_root_ordinal',
        'before_root_timesheet_id',v_item->'before_root_timesheet_id','before_head_id',v_item->'before_head_id',
        'before_component_sha256',v_item->'before_component_sha256')
        else '{}'::jsonb end);
  end loop;
  v_request:=p_context->'base_request';
  for v_component in select value from jsonb_array_elements(v_request#>'{financial_request,member_entitlements,1,components}')
    order by (value->>'component_ordinal')::integer loop
    select value into v_item from jsonb_array_elements(v_items) where value->>'component_id'=v_component->>'component_id';
    v_target:=v_target||jsonb_build_array(case when v_item is null then v_component else
      v_component||jsonb_build_object('pay_ex_vat',v_item#>'{after_content,pay_ex_vat}') end);
  end loop;
  v_request:=jsonb_set(v_request,'{financial_request,member_entitlements,1,components}',v_target);
  v_review:=jsonb_build_object('schema_version','WEEKLY_SOURCE_DESTINATION_REPRICE_REVIEW_V1',
    'context',p_context,'calculated_components',v_items,'complete_destination_components',v_target);
  return jsonb_build_object('review',v_review,'review_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_DESTINATION_REPRICE_REVIEW_V1',v_review),'hex'),'request',v_request,'components',v_items);
end;
$function$;

create or replace function private.weekly_source_destination_reprice_detail_v1(
  p_bundle uuid,p_revision bigint,p_component jsonb,p_origin integer
) returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare v_saved private.weekly_source_destination_reprice_witness_v1%rowtype; v_item jsonb;
begin
  select * into v_saved from private.weekly_source_destination_reprice_witness_v1
    where decision_bundle_id=p_bundle and bundle_revision=p_revision;
  if not found then return null; end if;
  select value into v_item from jsonb_array_elements(v_saved.economic_snapshots_json)
    where value->>'component_id'=p_component->>'component_id';
  if v_item is null then return null; end if;
  if p_origin not in (1,2) or p_origin is distinct from
       coalesce((v_item->>'before_root_ordinal')::integer,1)
     or (v_item ? 'before_root_ordinal' and (
       v_item->>'before_root_timesheet_id' is distinct from
         v_saved.accepted_request_json#>>array['member_root_ids',(p_origin-1)::text]
       or not exists(select 1 from public.weekly_source_entitlement_head_components oldc
         where oldc.head_id=(v_item->>'before_head_id')::uuid
           and oldc.component_id=(p_component->>'component_id')::uuid
           and encode(oldc.component_sha256,'hex')=v_item->>'before_component_sha256')))
     or not exists(select 1 from public.weekly_source_entitlement_head_components c
      where c.head_id=(v_saved.accepted_request_json#>>'{head_ids,1}')::uuid
        and c.component_id=(p_component->>'component_id')::uuid)
     or private.weekly_source_publication_component_content_v1(p_component) is distinct from v_item->'after_content' then
    raise exception 'WEEKLY_SOURCE_DESTINATION_REPRICE_DETAIL_NOT_EXACT' using errcode='23514'; end if;
  perform private.weekly_source_destination_reprice_verify_v1(
    private.weekly_source_publication_request_canonical_v1(v_saved.accepted_request_json,'IMMEDIATE',null),
    v_saved.accepted_request_json->'control');
  return jsonb_set(v_item->'detail','{destination_reprice}',(v_item#>'{detail,destination_reprice}')||
    jsonb_build_object('witness_id',v_saved.id,'review_sha256',encode(v_saved.review_sha256,'hex')));
end;
$function$;


alter function private.weekly_source_destination_joined_source_context_v2(jsonb) owner to postgres;
alter function private.weekly_source_destination_financial_frozen_review_v2(jsonb,jsonb) owner to postgres;
alter function private.weekly_source_destination_financial_quote_v2(jsonb,jsonb,jsonb) owner to postgres;
alter function public.weekly_source_destination_financial_preview_v2(uuid,jsonb,jsonb,jsonb) owner to postgres;
alter function public.weekly_source_destination_financial_accept_v2(uuid,jsonb,jsonb,jsonb,text,text,text) owner to postgres;
alter function private.weekly_source_destination_reprice_verify_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.weekly_source_destination_joined_source_context_v2(jsonb),
  private.weekly_source_destination_financial_frozen_review_v2(jsonb,jsonb),
  private.weekly_source_destination_financial_quote_v2(jsonb,jsonb,jsonb),
  private.weekly_source_destination_reprice_verify_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_destination_financial_preview_v2(uuid,jsonb,jsonb,jsonb),
  public.weekly_source_destination_financial_accept_v2(uuid,jsonb,jsonb,jsonb,text,text,text) from public,anon,authenticated;
grant execute on function public.weekly_source_destination_financial_preview_v2(uuid,jsonb,jsonb,jsonb),
  public.weekly_source_destination_financial_accept_v2(uuid,jsonb,jsonb,jsonb,text,text,text) to service_role;
alter function private.weekly_source_destination_reprice_context_v1(uuid,jsonb) owner to postgres;
alter function private.weekly_source_destination_reprice_quote_v1(jsonb,jsonb) owner to postgres;
alter function private.weekly_source_destination_reprice_detail_v1(uuid,bigint,jsonb,integer) owner to postgres;
-- Cold target: the joined V3 review is the sole service move acceptance writer.
-- Preserve pure pricing/preview/internal dependencies, not its bypassing accept.
revoke all on function public.weekly_source_destination_reprice_accept_v1(uuid,jsonb,jsonb,text,text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_destination_reprice_context_v1(uuid,jsonb),
  private.weekly_source_destination_reprice_quote_v1(jsonb,jsonb),
  private.weekly_source_destination_reprice_detail_v1(uuid,bigint,jsonb,integer) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
