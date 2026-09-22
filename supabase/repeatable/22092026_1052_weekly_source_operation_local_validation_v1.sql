-- Repeatable CloudTMS function/view authority: weekly_source_operation_local_validation_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Temporary relations are owned by the trusted function owner, never by the
-- service caller. They contain identifiers only and empty at every COMMIT.
-- No caller-controlled GUC or tuple xmin establishes attribution.
create or replace function private.weekly_source_observation_storage_v1()
returns void language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $fn$
declare v_name text; v_rel oid; v_owner oid;
begin
  select oid into strict v_owner from pg_catalog.pg_roles where rolname=current_user;
  foreach v_name in array array['ws_observation_frame_v1','ws_observation_rows_v1','ws_inventory_checked_v1'] loop
    select c.oid into v_rel from pg_catalog.pg_class c
      where c.relnamespace=pg_catalog.pg_my_temp_schema() and c.relname=v_name;
    if v_rel is not null and not exists (
      select 1 from pg_catalog.pg_class c where c.oid=v_rel
        and c.relowner=v_owner and c.relpersistence='t' and c.relkind='r'
    ) then
      raise exception 'WEEKLY_SOURCE_OBSERVATION_STORAGE_UNTRUSTED' using errcode='42501';
    end if;
  end loop;
  if pg_catalog.to_regclass('pg_temp.ws_observation_frame_v1') is null then
    create temporary table pg_temp.ws_observation_frame_v1 (
      singleton boolean primary key check(singleton), frame_id uuid not null,
      sealed boolean not null default false
    ) on commit delete rows;
    revoke all on pg_temp.ws_observation_frame_v1 from public,anon,authenticated,service_role;
  end if;
  if pg_catalog.to_regclass('pg_temp.ws_observation_rows_v1') is null then
    create temporary table pg_temp.ws_observation_rows_v1 (
      frame_id uuid not null, kind text not null check(kind in ('JOB','TOKEN','SCOPE','REGISTRY')),
      row_id uuid not null, deleted boolean not null default false,
      primary key(frame_id,kind,row_id)
    ) on commit delete rows;
    revoke all on pg_temp.ws_observation_rows_v1 from public,anon,authenticated,service_role;
  end if;
  if pg_catalog.to_regclass('pg_temp.ws_inventory_checked_v1') is null then
    create temporary table pg_temp.ws_inventory_checked_v1 (
      head_id uuid primary key
    ) on commit delete rows;
    revoke all on pg_temp.ws_inventory_checked_v1 from public,anon,authenticated,service_role;
  end if;
end;
$fn$;

create or replace function private.weekly_source_observation_begin_v1()
returns uuid language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $fn$
declare v_frame uuid:=pg_catalog.gen_random_uuid();
begin
  perform private.weekly_source_observation_storage_v1();
  if exists(select 1 from pg_temp.ws_observation_frame_v1) then
    raise exception 'WEEKLY_SOURCE_OBSERVATION_NESTED' using errcode='55000';
  end if;
  insert into pg_temp.ws_observation_frame_v1(singleton,frame_id) values(true,v_frame);
  return v_frame;
end;
$fn$;

create or replace function private.weekly_source_observation_seal_v1()
returns void language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $fn$
begin
  perform private.weekly_source_observation_storage_v1();
  update pg_temp.ws_observation_frame_v1 set sealed=true where singleton;
  if not found then raise exception 'WEEKLY_SOURCE_OBSERVATION_REQUIRED' using errcode='55000'; end if;
end;
$fn$;

create or replace function private.weekly_source_observation_end_v1(p_frame uuid)
returns void language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $fn$
begin
  perform private.weekly_source_observation_storage_v1();
  if not exists(select 1 from pg_temp.ws_observation_frame_v1 where frame_id=p_frame) then
    raise exception 'WEEKLY_SOURCE_OBSERVATION_FRAME_MISMATCH' using errcode='55000';
  end if;
  if exists(select 1 from pg_temp.ws_observation_rows_v1 where frame_id=p_frame and deleted) then
    raise exception 'WEEKLY_SOURCE_OBSERVATION_UNEXPECTED_DELETE' using errcode='55000';
  end if;
  delete from pg_temp.ws_observation_rows_v1 where frame_id=p_frame;
  delete from pg_temp.ws_observation_frame_v1 where frame_id=p_frame;
end;
$fn$;

create or replace function private.weekly_source_observe_effect_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $fn$
declare v_frame uuid; v_id uuid; v_old_id uuid; v_kind text; v_sealed boolean;
begin
  if pg_catalog.to_regclass('pg_temp.ws_observation_frame_v1') is null then return null; end if;
  perform private.weekly_source_observation_storage_v1();
  select frame_id,sealed into v_frame,v_sealed from pg_temp.ws_observation_frame_v1 where singleton;
  if v_frame is null then return null; end if;
  if v_sealed then raise exception 'WEEKLY_SOURCE_OBSERVATION_WRITE_AFTER_PROOF' using errcode='55000'; end if;
  if tg_relid='public.banking_pay_workbench_jobs'::regclass then
    v_kind:='JOB';
    if tg_op='DELETE' then v_id:=old.id; else v_id:=new.id; end if;
    if tg_op='UPDATE' then v_old_id:=old.id; end if;
  elsif tg_relid='public.banking_pay_scope_change_transactions'::regclass then
    v_kind:='TOKEN';
    if tg_op='DELETE' then v_id:=old.tx_token; else v_id:=new.tx_token; end if;
    if tg_op='UPDATE' then v_old_id:=old.tx_token; end if;
  elsif tg_relid='private.banking_pay_workbench_timesheet_scope_state'::regclass then
    v_kind:='SCOPE';
    if tg_op='DELETE' then v_id:=old.timesheet_id; else v_id:=new.timesheet_id; end if;
    if tg_op='UPDATE' then v_old_id:=old.timesheet_id; end if;
  elsif tg_relid='private.banking_pay_workbench_candidate_scope_registry'::regclass then
    v_kind:='REGISTRY';
    if tg_op='DELETE' then v_id:=old.candidate_id; else v_id:=new.candidate_id; end if;
    if tg_op='UPDATE' then v_old_id:=old.candidate_id; end if;
  else
    raise exception 'WEEKLY_SOURCE_OBSERVATION_RELATION_INVALID' using errcode='55000';
  end if;
  if tg_op='UPDATE' and v_old_id is distinct from v_id then
    raise exception 'WEEKLY_SOURCE_OBSERVATION_IDENTITY_CHANGE' using errcode='55000';
  end if;
  insert into pg_temp.ws_observation_rows_v1(frame_id,kind,row_id,deleted)
    values(v_frame,v_kind,v_id,tg_op='DELETE')
    on conflict(frame_id,kind,row_id) do update
      set deleted=pg_temp.ws_observation_rows_v1.deleted or excluded.deleted;
  return null;
end;
$fn$;

-- AFTER capture sees the value after existing BEFORE staging triggers.
-- Attribution does not depend on that value being the expected token/Candidate.
drop trigger if exists ws_operation_effect_capture_v1 on public.banking_pay_workbench_jobs;
create trigger ws_operation_effect_capture_v1 after insert or update or delete
  on public.banking_pay_workbench_jobs for each row execute function private.weekly_source_observe_effect_v1();
drop trigger if exists ws_operation_effect_capture_v1 on public.banking_pay_scope_change_transactions;
create trigger ws_operation_effect_capture_v1 after insert or update or delete
  on public.banking_pay_scope_change_transactions for each row execute function private.weekly_source_observe_effect_v1();
drop trigger if exists ws_operation_effect_capture_v1 on private.banking_pay_workbench_timesheet_scope_state;
create trigger ws_operation_effect_capture_v1 after insert or update or delete
  on private.banking_pay_workbench_timesheet_scope_state for each row execute function private.weekly_source_observe_effect_v1();

-- A native (Candidate,NULL Timesheet) invalidation with skipped enqueue writes
-- only this registry. It is a real effect, not implied by JOB/TOKEN/SCOPE rows.
drop trigger if exists ws_operation_effect_capture_v1 on private.banking_pay_workbench_candidate_scope_registry;
create trigger ws_operation_effect_capture_v1 after insert or update or delete
  on private.banking_pay_workbench_candidate_scope_registry for each row execute function private.weekly_source_observe_effect_v1();

create or replace function private.weekly_source_inventory_changed_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $fn$
declare v_head uuid;
begin
  if tg_relid='public.weekly_source_entitlement_heads'::regclass then
    if tg_op='DELETE' then v_head:=old.id; else v_head:=new.id; end if;
  else
    if tg_op='DELETE' then v_head:=old.head_id; else v_head:=new.head_id; end if;
  end if;
  -- Serialize component membership against head validation, not unrelated heads.
  perform 1 from public.weekly_source_entitlement_heads where id=v_head for no key update;
  perform private.weekly_source_observation_storage_v1();
  delete from pg_temp.ws_inventory_checked_v1 where head_id=v_head;
  if tg_relid='public.weekly_source_entitlement_head_components'::regclass then
    if tg_op='UPDATE' then
      if old.head_id is distinct from new.head_id then
        raise exception 'WEEKLY_SOURCE_COMPONENT_HEAD_MOVE_FORBIDDEN' using errcode='55000';
      end if;
    end if;
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$fn$;

drop trigger if exists ws_inventory_changed_v1 on public.weekly_source_entitlement_heads;
-- Invalidate BEFORE the write: an IMMEDIATE constraint may otherwise consume
-- an AFTER event before an alphabetically later AFTER invalidator runs.
create trigger ws_inventory_changed_v1 before insert or update or delete
  on public.weekly_source_entitlement_heads for each row execute function private.weekly_source_inventory_changed_v1();
drop trigger if exists ws_inventory_changed_v1 on public.weekly_source_entitlement_head_components;
create trigger ws_inventory_changed_v1 before insert or update or delete
  on public.weekly_source_entitlement_head_components for each row execute function private.weekly_source_inventory_changed_v1();

-- BEGIN COMPLETE OWNER REPLACEMENTS
-- Full source replacements; generated offline, no hosted source rewriting.
CREATE OR REPLACE FUNCTION private.weekly_source_entitlement_publish_core_v1(p_request jsonb, p_publication_mode text, p_lock_result jsonb, p_pending_bundle_id uuid, p_worker_id text, p_worker_run_id uuid, p_census jsonb, p_proof jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_mode text;
  v_canonical jsonb;
  v_digest bytea;
  v_now timestamptz;
  v_err_message text;
  v_err_detail text;
  v_err_state text;
  v_bundle record;
  v_pending record;
  v_receipt record;
  v_decision_bundle_id uuid;
  v_bundle_revision bigint;
  v_candidate_id uuid;
  v_decision_id uuid;
  v_member_root_ids uuid[];
  v_member_family_booking_ids text[];
  v_member_root_versions integer[];
  v_head_ids uuid[];
  v_expected_head_ids uuid[]:=array[]::uuid[];
  v_current_head_ids uuid[]:=array[]::uuid[];
  v_prior_head_ids uuid[]:=array[]::uuid[];
  v_head_revisions bigint[]:=array[]::bigint[];
  v_n integer;
  v_i integer;
  v_control jsonb;
  v_family jsonb;
  v_member_timesheet_ids uuid[]:=array[]::uuid[];
  v_entitlement jsonb;
  v_choice jsonb;
  v_component jsonb;
  v_component_sha bytea;
  v_component_ids uuid[];
  v_before_ids uuid[];
  v_after_1 uuid[]:=array[]::uuid[];
  v_after_2 uuid[]:=array[]::uuid[];
  v_before_1 uuid[]:=array[]::uuid[];
  v_before_2 uuid[]:=array[]::uuid[];
  v_moved uuid[]:=array[]::uuid[];
  -- WP-06c review F1: the CONTENT hash the SOURCE authority holds for every
  -- component of member 1's before-position, keyed by component_id.  It is what
  -- a MOVED component must still equal on the destination side.
  v_source_component_hashes jsonb:='{}'::jsonb;
  v_moved_found integer;
  v_inventory_pairs jsonb;
  v_component_hashes jsonb;
  v_head_revision bigint;
  v_token uuid;
  v_invalidation jsonb;
  v_rows integer;
  v_live_generations integer;
  v_family_live_generations integer;
  v_family_members uuid[];
  v_any_generations integer;
  v_any_heads integer;
  v_target_auth jsonb;
  v_target_authorise_result jsonb;
  v_review jsonb;
  v_source_generation_digest bytea;
  v_contract_choice_digest bytea;
  v_before_inventory jsonb;
  v_before_inventory_digest bytea;
  v_acceptance_digest bytea;
  v_before_source text[]:=array[]::text[];
  v_effective jsonb;
  v_blank jsonb;
  v_observation_frame uuid;
  v_token_recheck uuid;
  v_declared_scope uuid[];
  v_normalised jsonb;
  v_offending jsonb;
  v_result_heads jsonb:='[]'::jsonb;
  v_lineage_rows integer:=0;
begin
  -- Service-only, exactly as public.weekly_source_timesheet_lineage_ensure_atomic_v1.
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  -- Shape validation and the one canonical digest.  Nothing has been written,
  -- so a malformed request is returned rather than raised.
  begin
    v_mode:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_publication_mode,'')));
    if v_mode not in ('IMMEDIATE','DEFERRED') then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','p_publication_mode','reason','EXPECTED_IMMEDIATE_OR_DEFERRED')::text;
    end if;
    -- proof/32 section 9 and H2-038: both Worker fields are required when the
    -- mode is DEFERRED and null when it is IMMEDIATE.
    if (v_mode='DEFERRED')<>(p_worker_id is not null and p_worker_run_id is not null)
       or (v_mode='IMMEDIATE' and (p_worker_id is not null or p_worker_run_id is not null)) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','p_worker_id','reason','WORKER_FIELDS_MUST_MATCH_MODE')::text;
    end if;
    if pg_catalog.jsonb_typeof(coalesce(p_census,'{}'::jsonb))<>'object'
       or pg_catalog.jsonb_typeof(coalesce(p_proof,'{}'::jsonb))<>'object' then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID'
        using errcode='22023',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID',
                'field','p_census','reason','EXPECTED_OBJECT')::text;
    end if;
    v_canonical:=private.weekly_source_publication_request_canonical_v1(
      p_request,v_mode,p_pending_bundle_id);
    v_digest:=private.weekly_source_publication_request_digest_v1(v_canonical);
  -- Review finding F10: catching only `invalid_parameter_value` let a malformed
  -- request RAISE instead of returning - an impossible calendar date (22008),
  -- an integer outside the range (22003) and a non-uuid in the control scope
  -- (22P02) all escaped.  Nothing has been written at this point, so every one
  -- of them is a returned refusal, and WP-08b no longer counts a caller's typo
  -- as a technical failure on its way to MANUAL_REVIEW.
  exception when invalid_parameter_value
              or datetime_field_overflow
              or invalid_datetime_format
              or numeric_value_out_of_range
              or invalid_text_representation then
    get stacked diagnostics v_err_message=message_text, v_err_detail=pg_exception_detail,
                            v_err_state=returned_sqlstate;
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code',case when v_err_state='22023' then v_err_message
                  else 'WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID' end,
      'retryable',false,
      'detail',case when coalesce(v_err_detail,'') ~ '^\{' then v_err_detail::jsonb
                    else pg_catalog.jsonb_build_object(
                           'reason','REQUEST_VALUE_COULD_NOT_BE_PARSED',
                           'sqlstate',v_err_state,'message',v_err_message) end);
  end;

  v_now:=pg_catalog.clock_timestamp();
  v_control:=coalesce(p_request->'control','{}'::jsonb);
  v_decision_bundle_id:=(v_canonical->>'decision_bundle_id')::uuid;
  v_bundle_revision:=(v_canonical->>'bundle_revision')::bigint;
  v_candidate_id:=(v_canonical->>'candidate_id')::uuid;
  v_decision_id:=(v_canonical->>'decision_id')::uuid;

  select pg_catalog.array_agg(root_element.value::uuid order by root_element.ordinality)
    into v_member_root_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_root_ids')
         with ordinality as root_element(value,ordinality);
  select pg_catalog.array_agg(booking_element.value order by booking_element.ordinality)
    into v_member_family_booking_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_family_booking_ids')
         with ordinality as booking_element(value,ordinality);
  select pg_catalog.array_agg(version_element.value::integer order by version_element.ordinality)
    into v_member_root_versions
    from pg_catalog.jsonb_array_elements_text(v_canonical->'member_root_versions')
         with ordinality as version_element(value,ordinality);
  select pg_catalog.array_agg(head_element.value::uuid order by head_element.ordinality)
    into v_head_ids
    from pg_catalog.jsonb_array_elements_text(v_canonical->'head_ids')
         with ordinality as head_element(value,ordinality);
  v_n:=pg_catalog.cardinality(v_member_root_ids);

  -- H2-032: no duplicate root and no duplicate head id.  The receipt relation
  -- repeats both rules; refusing here keeps the failure a refusal rather than a
  -- rolled-back transaction.
  if not private.weekly_source_uuid_array_is_distinct_v1(v_member_root_ids)
     or not private.weekly_source_uuid_array_is_distinct_v1(v_head_ids) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DUPLICATE_ROOT_OR_HEAD_ID'));
  end if;
  if v_n=2 and pg_catalog.btrim(v_member_family_booking_ids[1])
              =pg_catalog.btrim(v_member_family_booking_ids[2]) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','MEMBERS_SHARE_ONE_FAMILY'));
  end if;

  -- ---- 1. exact receipt replay, before any mutation (proof/32 section 8.1) --
  -- The digest is only an index lookup: every immutable receipt field is
  -- compared before a replay may be returned (H2-032).
  select * into v_receipt
    from private.weekly_source_entitlement_publication_receipts as receipt_row
   where receipt_row.request_digest=v_digest;
  if found then
    if v_receipt.decision_bundle_id<>v_decision_bundle_id
       or v_receipt.bundle_revision<>v_bundle_revision
       or v_receipt.candidate_id<>v_candidate_id
       or v_receipt.member_root_ids is distinct from v_member_root_ids
       or v_receipt.member_family_booking_ids is distinct from v_member_family_booking_ids
       or v_receipt.member_root_versions is distinct from v_member_root_versions
       or v_receipt.head_ids is distinct from v_head_ids
       or v_receipt.decision_id<>v_decision_id
       or v_receipt.publication_mode<>v_mode
       or v_receipt.pending_bundle_id is distinct from p_pending_bundle_id then
      -- Round-5 ruling A1 control 5: "conflicting replay and a tampered row are
      -- PERMANENT integrity failures and go DIRECTLY to manual review; they are
      -- not retried ten times".  The same digest over different immutable
      -- receipt fields means two different economic decisions share one money
      -- identity, which no amount of retrying can resolve.  `retryable:false`
      -- alone only stops the immediate retry; the disposition below is what
      -- tells the deferred release owner to route the bundle to MANUAL_REVIEW
      -- at once instead of counting it as one of its ten technical failures.
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REPLAY_CONFLICT','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','A_COMMITTED_RECEIPT_CARRIES_THIS_DIGEST_WITH_DIFFERENT_IMMUTABLE_FIELDS',
          'receipt_id',v_receipt.id,
          'request_digest',pg_catalog.encode(v_digest,'hex')));
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'published',true,'replayed',true,
      'receipt',private.weekly_source_publication_receipt_json_v1(v_receipt.id));
  end if;

  -- ---- 2. the immutable accepted decision bundle -------------------------
  select * into v_bundle
    from public.weekly_source_entitlement_decision_bundles as bundle_row
   where bundle_row.decision_bundle_id=v_decision_bundle_id
     and bundle_row.bundle_revision=v_bundle_revision
   for update;
  if not found then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_FOUND',
        'decision_bundle_id',v_decision_bundle_id,'bundle_revision',v_bundle_revision));
  end if;
  if v_bundle.candidate_id<>v_candidate_id
     or v_bundle.decision_id<>v_decision_id
     or v_bundle.proposed_head_ids is distinct from v_head_ids
     or (v_bundle.bundle_kind='SINGLE_ROOT')<>(v_n=1)
     or (v_bundle.bundle_kind='CROSS_CONTRACT_A_B')<>(v_n=2) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_DISAGREES_WITH_REQUEST',
        'bundle_kind',v_bundle.bundle_kind,'member_count',v_n));
  end if;
  if v_bundle.state not in ('PROPOSED','COMMITTED') then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','DECISION_BUNDLE_NOT_LIVE',
        'state',v_bundle.state));
  end if;
  -- Review finding F7.  The replay check above runs BEFORE the bundle lock and
  -- matches on the digest, so a SECOND, DIFFERENT request under an already
  -- COMMITTED bundle revision used to pass every pre-write check and be stopped
  -- only by the head primary key, as a raised 23505 that WP-08b would count as
  -- a technical failure.  A committed revision has had its one publication:
  -- anything that is not that exact request is refused here, with a code.
  if v_bundle.state='COMMITTED'
     and not exists (
       select 1
         from private.weekly_source_entitlement_publication_receipts as receipt_row
        where receipt_row.decision_bundle_id=v_decision_bundle_id
          and receipt_row.bundle_revision=v_bundle_revision
          and receipt_row.request_digest=v_digest) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_BUNDLE_REVISION_ALREADY_PUBLISHED','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','THIS_BUNDLE_REVISION_HAS_ALREADY_PUBLISHED_A_DIFFERENT_REQUEST',
        'decision_bundle_id',v_decision_bundle_id,'bundle_revision',v_bundle_revision));
  end if;

  -- ---- 2a0. DEFERRED mode is not trusted on the census (review U5) ---------
  -- The core's contract is "the caller ran the census", but a frozen root must
  -- never publish (24 section 4.4; proof/32 section 7, last bullet), and the
  -- reviewer executed a DEFERRED release that published with
  -- p_census = {"result":"FROZEN"} and a p_pending_bundle_id naming no row at
  -- all.  So the one step where the answer decides whether money moves is
  -- checked here as well as by the caller.
  if v_mode='DEFERRED' then
    if coalesce(p_census->>'result','')<>'RELEASABLE' then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_CENSUS_NOT_RELEASABLE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','A_DEFERRED_RELEASE_MAY_ONLY_PUBLISH_ON_A_RELEASABLE_CENSUS',
          'census_result',coalesce(p_census->'result','null'::jsonb)));
    end if;
    select * into v_pending
      from public.weekly_source_pending_entitlement_bundles as pending_row
     where pending_row.id=p_pending_bundle_id
       and pending_row.decision_bundle_id=v_decision_bundle_id
       and pending_row.bundle_revision=v_bundle_revision
       and pending_row.candidate_id=v_candidate_id
     for update;
    if not found then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','NO_PENDING_BUNDLE_ROW_FOR_THIS_DECISION_BUNDLE_AND_REVISION',
          'pending_bundle_id',p_pending_bundle_id,
          'decision_bundle_id',v_decision_bundle_id,
          'bundle_revision',v_bundle_revision));
    end if;
    -- Decision D10.  The migration cannot bind the pending bundle's stored
    -- request to its stored digest, because such a trigger would have to call
    -- the canonical encoder, which lives in a repeatable, and migrations are
    -- applied first: a rebuild from empty would fail.  The safety therefore
    -- belongs here, where the decision to move money is actually made.  Under
    -- the lock already held, the stored request and the stored digest are
    -- verified TOGETHER, and against the request being released.
    --
    -- Round-5 ruling A1 control 3 is exactly this: "release must lock the row,
    -- recalculate the digest from the stored canonical request and refuse
    -- BEFORE EFFECTS on any mismatch".  The row is locked `for update` above,
    -- the digest is recomputed by this coordinator from the stored request, and
    -- both refusals below happen before the first write.  Control 5 adds that a
    -- tampered row is a PERMANENT integrity failure that goes DIRECTLY to
    -- manual review, so each one carries the disposition the deferred release
    -- owner must honour instead of counting a technical failure.
    if private.weekly_source_publication_request_digest_v1(
         private.weekly_source_publication_request_canonical_v1(
           v_pending.request_json,'DEFERRED',p_pending_bundle_id))
       is distinct from v_pending.request_digest then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','THE_STORED_REQUEST_DOES_NOT_MATCH_ITS_STORED_DIGEST',
          'pending_bundle_id',p_pending_bundle_id,
          'stored_digest',pg_catalog.encode(v_pending.request_digest,'hex'),
          'recomputed_digest',pg_catalog.encode(
            private.weekly_source_publication_request_digest_v1(
              private.weekly_source_publication_request_canonical_v1(
                v_pending.request_json,'DEFERRED',p_pending_bundle_id)),'hex')));
    end if;
    if v_pending.request_digest is distinct from v_digest then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PENDING_BUNDLE_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'integrity_failure',true,'disposition','MANUAL_REVIEW',
          'reason','THE_REQUEST_BEING_RELEASED_IS_NOT_THE_ONE_THAT_WAS_SAVED',
          'pending_bundle_id',p_pending_bundle_id,
          'stored_digest',pg_catalog.encode(v_pending.request_digest,'hex'),
          'release_digest',pg_catalog.encode(v_digest,'hex')));
    end if;
  end if;

  -- ---- 2b. the accepted decision BINDS what is published (review U1) -------
  -- 24 section 4.5 step 2 and file 26 Gate 5 step 4 require the coordinator to
  -- revalidate "the current source revision, Contract choices, Candidate,
  -- Client, week, approval digest and the immutable identity of every moved
  -- component".  Quoting the right bundle id, decision id and head ids is not
  -- enough: the request must equal the accepted decision in every identity the
  -- bundle row carries, or a caller could publish a different root, Contract,
  -- week and amount of money under the Office decision's identity and actor.
  v_choice:=(
    select choice_element.value
      from pg_catalog.jsonb_array_elements(
             v_canonical->'financial_request'->'contract_choices') as choice_element(value)
     where (choice_element.value->>'root_ordinal')::integer=1);
  if v_member_root_ids[1] is distinct from v_bundle.source_root_timesheet_id
     or v_member_family_booking_ids[1] is distinct from v_bundle.source_root_family_booking_id
     or (v_choice->>'contract_id')::uuid is distinct from v_bundle.source_contract_id then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','SOURCE_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
        'accepted_root',v_bundle.source_root_timesheet_id,'requested_root',v_member_root_ids[1],
        'accepted_family',v_bundle.source_root_family_booking_id,
        'requested_family',v_member_family_booking_ids[1],
        'accepted_contract',v_bundle.source_contract_id,
        'requested_contract',v_choice->'contract_id'));
  end if;
  v_choice:=null;
  if v_n=2 then
    v_choice:=(
      select choice_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'contract_choices') as choice_element(value)
       where (choice_element.value->>'root_ordinal')::integer=2);
    if v_member_root_ids[2] is distinct from v_bundle.target_root_timesheet_id
       or v_member_family_booking_ids[2] is distinct from v_bundle.target_root_family_booking_id
       or (v_choice->>'contract_id')::uuid is distinct from v_bundle.target_contract_id then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','TARGET_ROOT_DISAGREES_WITH_ACCEPTED_DECISION',
          'accepted_root',v_bundle.target_root_timesheet_id,'requested_root',v_member_root_ids[2],
          'accepted_family',v_bundle.target_root_family_booking_id,
          'requested_family',v_member_family_booking_ids[2],
          'accepted_contract',v_bundle.target_contract_id,
          'requested_contract',v_choice->'contract_id'));
    end if;
    v_choice:=null;
  elsif v_bundle.target_root_timesheet_id is not null
     or v_bundle.target_root_family_booking_id is not null
     or v_bundle.target_contract_id is not null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','SINGLE_ROOT_BUNDLE_CARRIES_A_TARGET_ROOT'));
  end if;

  -- The week is the accepted decision's, and it is also each root Timesheet's
  -- own week and Contract.  One ordinary root per Candidate, Contract and week
  -- (contract section 5) only means anything if the week is checked.
  for v_i in 1..v_n loop
    v_choice:=(
      select choice_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'contract_choices') as choice_element(value)
       where (choice_element.value->>'root_ordinal')::integer=v_i);
    if (v_choice->>'week_ending_date')::date is distinct from v_bundle.week_ending_date
       or not exists (
         select 1
           from public.timesheets as root_row
          where root_row.timesheet_id=v_member_root_ids[v_i]
            and root_row.week_ending_date=v_bundle.week_ending_date
            and root_row.contract_id=(v_choice->>'contract_id')::uuid) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','WEEK_OR_CONTRACT_DISAGREES_WITH_ACCEPTED_DECISION',
          'root_ordinal',v_i,'accepted_week',v_bundle.week_ending_date,
          'requested_week',v_choice->'week_ending_date',
          'requested_contract',v_choice->'contract_id'));
    end if;
    -- The Candidate must actually OWN the root.  The reviewer published a head,
    -- a receipt and an invalidation under a second Candidate for a root it did
    -- not own: the installed invalidator's ownership rule fires only when a
    -- current TSFIN row exists, so the coordinator checks the Contract itself
    -- rather than leaving it to I-1 or to Banking Pay.
    if not exists (
      select 1
        from public.contracts as contract_row
       where contract_row.id=(v_choice->>'contract_id')::uuid
         and contract_row.candidate_id=v_candidate_id) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','CANDIDATE_DOES_NOT_OWN_THE_ROOT',
          'root_ordinal',v_i,'candidate_id',v_candidate_id,
          'contract_id',v_choice->'contract_id'));
    end if;
    v_choice:=null;
  end loop;

  -- The three approval digests the bundle row carries, recomputed here with the
  -- one canonical encoder.  These are mode-independent by construction, so the
  -- same values bind an immediate publication and a deferred release.
  v_source_generation_digest:=private.weekly_source_publication_request_digest_v1(
    v_canonical->'financial_request'->'source_revision');
  v_contract_choice_digest:=private.weekly_source_publication_request_digest_v1(
    v_canonical->'financial_request'->'contract_choices');
  v_before_inventory:=private.weekly_source_publication_before_inventory_v1(v_control,v_n);
  v_before_inventory_digest:=private.weekly_source_publication_request_digest_v1(v_before_inventory);
  -- The acceptance digest: the canonical request in IMMEDIATE mode with no
  -- pending bundle.  publication_mode and pending_bundle_id are digest fields
  -- (proof/32 section 9), so the digest the RECEIPT carries changes with the
  -- mode and cannot be the one stored on the accepted decision.  The bundle
  -- row's request_digest is therefore always taken in IMMEDIATE/null, which is
  -- what the proposal composer can compute at proposal time, before any pending
  -- bundle exists.  Written into interfaces\PUBLICATION_REQUEST_SHAPE.md.
  v_acceptance_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_request_canonical_v1(p_request,'IMMEDIATE',null::uuid));
  if v_bundle.source_revision_digest is distinct from v_source_generation_digest
     or v_bundle.contract_choice_digest is distinct from v_contract_choice_digest
     or v_bundle.before_inventory_digest is distinct from v_before_inventory_digest
     or v_bundle.request_digest is distinct from v_acceptance_digest then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','APPROVAL_DIGESTS_DISAGREE_WITH_ACCEPTED_DECISION',
        'source_revision_digest_matches',
          v_bundle.source_revision_digest is not distinct from v_source_generation_digest,
        'contract_choice_digest_matches',
          v_bundle.contract_choice_digest is not distinct from v_contract_choice_digest,
        'before_inventory_digest_matches',
          v_bundle.before_inventory_digest is not distinct from v_before_inventory_digest,
        'request_digest_matches',
          v_bundle.request_digest is not distinct from v_acceptance_digest));
  end if;

  -- ---- 2a. the current source revision (24 section 4.5 step 2; R7) --------
  -- "revalidate the current source revision".  A head built from a superseded
  -- or merely prepared final revision must never become current.
  if not exists (
    select 1
      from public.weekly_source_final_revisions as revision_row
     where revision_row.id=(v_canonical->'financial_request'->'source_revision'->>'final_revision_id')::uuid
       and revision_row.source_cycle_id=(v_canonical->'financial_request'->'source_revision'->>'source_cycle_id')::uuid
       and revision_row.revision_number=(v_canonical->'financial_request'->'source_revision'->>'revision_number')::integer
       and revision_row.state='CURRENT'
       and pg_catalog.encode(revision_row.manifest_hash,'hex')
           =(v_canonical->'financial_request'->'source_revision'->>'manifest_hash')
       and pg_catalog.encode(revision_row.policy_fingerprint,'hex')
           =(v_canonical->'financial_request'->'source_revision'->>'policy_fingerprint')
  ) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_SOURCE_REVISION_STALE','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'source_revision',v_canonical->'financial_request'->'source_revision'));
  end if;

  -- ---- 3. member identity against the I-1 lock result ---------------------
  -- proof/32 section 4.0 integrity gate; proof/34 section 6: after first
  -- authorisation an unexpected rotation is an INTEGRITY FAILURE, never a stale
  -- rebuild.  This coordinator never rebuilds anything.
  if p_lock_result is null
     or pg_catalog.jsonb_typeof(p_lock_result)<>'object'
     or coalesce((p_lock_result->>'ok')::boolean,false) is not true
     or coalesce(p_lock_result->>'gate','')<>'GRANTED' then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object('reason','LOCK_RESULT_NOT_GRANTED'));
  end if;

  for v_i in 1..v_n loop
    select family_element.value into v_family
      from pg_catalog.jsonb_array_elements(p_lock_result->'families') as family_element(value)
     where family_element.value->>'requested_timesheet_id'=v_member_root_ids[v_i]::text;
    if v_family is null
       or coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true
       or coalesce((v_family->>'family_is_current')::boolean,false) is not true
       or (v_family->>'canonical_timesheet_id')::uuid is distinct from v_member_root_ids[v_i]
       or (v_family->>'canonical_version')::integer is distinct from v_member_root_versions[v_i]
       or pg_catalog.btrim(coalesce(v_family->>'family_booking_id',''))
          is distinct from pg_catalog.btrim(v_member_family_booking_ids[v_i]) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,'requested_timesheet_id',v_member_root_ids[v_i],
          'lock_result_family',v_family));
    end if;
    select v_member_timesheet_ids||coalesce(
             pg_catalog.array_agg(member_element.value::uuid),array[]::uuid[])
      into v_member_timesheet_ids
      from pg_catalog.jsonb_array_elements_text(
             coalesce(v_family->'member_timesheet_ids','[]'::jsonb)) as member_element(value);

    -- WP-01a review U1: `root_family_booking_id` is free text on the head row and
    -- nothing in the schema ties it to the root's real booking_id, so the
    -- coordinator validates it itself.  The raw value must equal
    -- public.timesheets.booking_id of the canonical root BYTE FOR BYTE: the
    -- trimmed form is the lock and index key (proof/32 section 6 step 2), not an
    -- identity, and a differently cased or unrelated string would take a
    -- different advisory lock from the one protecting the uniqueness index.
    if not exists (
      select 1
        from public.timesheets as root_row
       where root_row.timesheet_id=v_member_root_ids[v_i]
         and root_row.booking_id=v_member_family_booking_ids[v_i]
         and root_row.version=v_member_root_versions[v_i]
         and root_row.is_current) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','FAMILY_BOOKING_ID_DOES_NOT_MATCH_THE_ROOT',
          'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
          'declared_family_booking_id',v_member_family_booking_ids[v_i],
          'declared_version',v_member_root_versions[v_i]));
    end if;
    v_family:=null;
  end loop;

  -- ---- 4. compare-and-swap on each member's expected current head ---------
  -- jsonb_array_elements_text turns a JSON null into a SQL NULL, which is
  -- exactly "the caller believes this root has no committed head yet".
  select pg_catalog.array_agg(expected_element.value::uuid
                              order by expected_element.ordinality)
    into v_expected_head_ids
    from pg_catalog.jsonb_array_elements_text(
           coalesce(v_control->'expected_current_head_ids','[]'::jsonb))
         with ordinality as expected_element(value,ordinality);
  if coalesce(pg_catalog.cardinality(v_expected_head_ids),0)<>v_n then
    return pg_catalog.jsonb_build_object(
      'ok',false,'published',false,'replayed',false,
      'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
      'detail',pg_catalog.jsonb_build_object(
        'reason','EXPECTED_CURRENT_HEAD_IDS_NOT_ALIGNED'));
  end if;

  for v_i in 1..v_n loop
    -- One committed current head per root across both authority kinds
    -- (24 section 4.3), read under lock (H2-024).
    --
    -- WP-01a review U1: "at most one committed current head" is a partial unique
    -- index on the TRIMMED family string only, so it is not a schema guarantee
    -- for a physical root.  The coordinator therefore counts the rows itself,
    -- refuses more than one as an integrity failure rather than choosing one,
    -- and asserts that the head it found belongs to the canonical root I-1
    -- resolved.  It never falls back to stale TSFIN (27 section 5.1).
    select pg_catalog.count(*)::integer into v_rows
      from public.weekly_source_entitlement_heads as head_row
     where pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(v_member_family_booking_ids[v_i])
       and head_row.state='COMMITTED_CURRENT';
    if v_rows>1 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','MORE_THAN_ONE_COMMITTED_CURRENT_HEAD',
          'root_ordinal',v_i,'committed_current_heads',v_rows));
    end if;
    v_current_head_ids:=v_current_head_ids||(
      select head_row.id
        from public.weekly_source_entitlement_heads as head_row
       where pg_catalog.btrim(head_row.root_family_booking_id)
             =pg_catalog.btrim(v_member_family_booking_ids[v_i])
         and head_row.state='COMMITTED_CURRENT'
         for update);
    if v_current_head_ids[v_i] is not null
       and not exists (
         select 1
           from public.weekly_source_entitlement_heads as head_row
          where head_row.id=v_current_head_ids[v_i]
            and head_row.root_timesheet_id=v_member_root_ids[v_i]) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','COMMITTED_HEAD_BELONGS_TO_ANOTHER_PHYSICAL_ROOT',
          'root_ordinal',v_i,'current_head_id',v_current_head_ids[v_i]));
    end if;
    -- The MIRROR of the guard above, and the same defect class as WP-06 review
    -- finding F2: the family key found no head, but the PHYSICAL root carries
    -- one.  Since schema change S8 removed unique(root_timesheet_id) from the
    -- family relation, nothing keyed on the physical id is unique any more, so
    -- the two identities really can disagree.  Falling through here would take a
    -- TSFIN — or an unproved — before-position for a root that demonstrably HAS
    -- a committed head, understating the effective entitlement, which is exactly
    -- how a residual turns into an overpayment.  It costs one indexed lookup
    -- (weekly_source_entitlement_heads_committed_root_uq), so it is checked
    -- rather than assumed, and it is checked HERE so it covers the I-7 branch
    -- and the no-I-7 branch alike.
    if v_current_head_ids[v_i] is null
       and exists (
         select 1
           from public.weekly_source_entitlement_heads as head_row
          where head_row.root_timesheet_id=v_member_root_ids[v_i]
            and head_row.state='COMMITTED_CURRENT') then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','COMMITTED_HEAD_EXISTS_FOR_THE_PHYSICAL_ROOT_BUT_NOT_THE_DECLARED_FAMILY',
          'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
          'declared_family_booking_id',v_member_family_booking_ids[v_i],
          'physical_root_head_id',(
            select head_row.id
              from public.weekly_source_entitlement_heads as head_row
             where head_row.root_timesheet_id=v_member_root_ids[v_i]
               and head_row.state='COMMITTED_CURRENT')));
    end if;
    if v_current_head_ids[v_i] is distinct from v_expected_head_ids[v_i] then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_HEAD_CAS_CONFLICT','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,
          'expected_current_head_id',v_expected_head_ids[v_i],
          'actual_current_head_id',v_current_head_ids[v_i]));
    end if;
    select coalesce(pg_catalog.max(head_row.head_revision),0)+1
      into v_head_revision
      from public.weekly_source_entitlement_heads as head_row
     where pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(v_member_family_booking_ids[v_i]);
    v_head_revisions:=v_head_revisions||v_head_revision;
    v_prior_head_ids:=v_prior_head_ids||(
      select head_row.id
        from public.weekly_source_entitlement_heads as head_row
       where pg_catalog.btrim(head_row.root_family_booking_id)
             =pg_catalog.btrim(v_member_family_booking_ids[v_i])
       order by head_row.head_revision desc
       limit 1);
  end loop;

  -- ---- 5. the complete post-decision head proofs (H2-024) -----------------
  for v_i in 1..v_n loop
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=v_i);
    select coalesce(pg_catalog.array_agg(
             (component_element.value->>'component_id')::uuid
             order by (component_element.value->>'component_ordinal')::integer),
             array[]::uuid[])
      into v_component_ids
      from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value);

    select coalesce(pg_catalog.array_agg(before_element.value::uuid),array[]::uuid[])
      into v_before_ids
      from pg_catalog.jsonb_array_elements_text(
             coalesce((select position_element.value->'component_ids'
                         from pg_catalog.jsonb_array_elements(
                                coalesce(v_control->'before_positions','[]'::jsonb)) as position_element(value)
                        where (position_element.value->>'root_ordinal')::integer=v_i),
                      'null'::jsonb)) as before_element(value);
    if (select position_element.value
          from pg_catalog.jsonb_array_elements(
                 coalesce(v_control->'before_positions','[]'::jsonb)) as position_element(value)
         where (position_element.value->>'root_ordinal')::integer=v_i) is null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','BEFORE_POSITION_MISSING','root_ordinal',v_i));
    end if;

    -- A head may never contain an adjustment, whatever key it arrives under
    -- (24 section 5; WB-007, WB-013; review U3 case H6c).  component_kind,
    -- economic_key_type and origin are free text on the relation, so the
    -- coordinator allowlists the kind and refuses the adjustment vocabulary
    -- outright rather than trusting the absence of an `adjustment_id` key.
    v_offending:=(
      select component_element.value
        from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
       where (component_element.value->>'component_kind') not in
               ('WORKED_TIME','ADDITIONAL_UNIT','SOURCE_FIXED_EXPENSE')
          or component_element.value->>'component_kind' ~* 'ADJUST|ADVANCE'
          or component_element.value->>'economic_key_type' ~* 'ADJUST|ADVANCE'
          or component_element.value->>'origin' ~* 'ADJUST|ADVANCE'
       limit 1);
    if v_offending is not null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_COMPONENT_KIND_FORBIDDEN','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,'component',v_offending,
          'permitted_kinds',pg_catalog.jsonb_build_array(
            'WORKED_TIME','ADDITIONAL_UNIT','SOURCE_FIXED_EXPENSE')));
    end if;
    v_offending:=null;

    -- H2-024: "each before-position is read from the single committed effective
    -- authority under lock".  Three cases, and only the third is ever accepted
    -- without a server-side read (review U3).
    if v_current_head_ids[v_i] is not null then
      -- (1) A committed head exists: the declared position must equal its
      -- component set EXACTLY, and every RETAINED component must be
      -- byte-identical - the committed row stores component_sha256, so
      -- re-pricing a component that is merely "kept by id" is detectable and
      -- is refused (review U3 cases H10 and H4c).
      if exists (
        select 1
          from (select component_row.component_id
                  from public.weekly_source_entitlement_head_components as component_row
                 where component_row.head_id=v_current_head_ids[v_i]
                except
                select pg_catalog.unnest(v_before_ids)) as missing_from_declared
        union all
        select 1
          from (select pg_catalog.unnest(v_before_ids)
                except
                select component_row.component_id
                  from public.weekly_source_entitlement_head_components as component_row
                 where component_row.head_id=v_current_head_ids[v_i]) as extra_in_declared
      ) then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_MISMATCH','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'current_head_id',v_current_head_ids[v_i]));
      end if;
      -- `component_sha256` is a CONTENT identity and deliberately excludes
      -- `component_ordinal`: the ordinal is the component's position inside a
      -- head, and a component that is retained while another moves out from in
      -- front of it legitimately changes position.  Position is still covered,
      -- by `inventory_digest` (the ordered ordinal/id pairs) and by
      -- `entitlement_digest` (the content hashes in ordinal order).
      v_offending:=(
        select pg_catalog.jsonb_build_object(
                 'component_id',component_element.value->'component_id',
                 'committed_sha256',pg_catalog.encode(committed_row.component_sha256,'hex'),
                 'requested_sha256',pg_catalog.encode(
                   private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value)),'hex'))
          from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
          join public.weekly_source_entitlement_head_components as committed_row
            on committed_row.head_id=v_current_head_ids[v_i]
           and committed_row.component_id=(component_element.value->>'component_id')::uuid
         where committed_row.component_sha256
               is distinct from private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value))
         limit 1);
      if v_offending is not null then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_RETAINED_COMPONENT_CHANGED','retryable',false,
          'detail',pg_catalog.jsonb_build_object('root_ordinal',v_i,'component',v_offending));
      end if;
      v_offending:=null;
      -- WP-06c review F1: member 1 is the SOURCE of a cross-Contract move, so
      -- its committed head is the authority for what every MOVED component
      -- actually held.  The content hashes are taken here, under the same lock
      -- and from the same rows the retained check just used, and are compared
      -- against the destination side after the set proofs.
      if v_i=1 then
        select coalesce(pg_catalog.jsonb_object_agg(
                 committed_row.component_id::text,
                 pg_catalog.encode(committed_row.component_sha256,'hex')),'{}'::jsonb)
          into v_source_component_hashes
          from public.weekly_source_entitlement_head_components as committed_row
         where committed_row.head_id=v_current_head_ids[v_i];
      end if;
      v_before_source:=v_before_source||'HEAD'::text;

    elsif pg_catalog.to_regprocedure(
            'private.weekly_source_effective_inventory_v1(uuid)') is not null then
      -- (2) No head yet, but interface I-7 exists (decision D9, built by WP-06):
      -- the before-position is derived SERVER-SIDE from the single committed
      -- effective authority - the ordinary financial snapshot - under the same
      -- locks, and the declared position must equal it.
      v_effective:=private.weekly_source_effective_inventory_v1(v_member_root_ids[v_i]);
      if coalesce(pg_catalog.jsonb_typeof(v_effective),'')<>'object' then
        raise exception 'WEEKLY_SOURCE_PUBLICATION_EFFECTIVE_INVENTORY_INVALID'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'code','WEEKLY_SOURCE_PUBLICATION_EFFECTIVE_INVENTORY_INVALID',
                  'root_ordinal',v_i,'result',v_effective)::text;
      end if;
      -- I-7 answers `ok:false` with an EMPTY components array when it cannot
      -- resolve the root.  Reading that as "the root holds nothing" would turn
      -- an integrity failure into a silent certified-zero publication, so the
      -- refusal is honoured before the sets are compared at all.
      if coalesce((v_effective->>'ok')::boolean,false) is not true then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'reason','INTERFACE_I7_REFUSED_THE_ROOT',
            'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
            'effective_inventory',v_effective));
      end if;
      -- I-7 is reached ONLY when the family key above found no committed head,
      -- so the only authority it can legitimately report here is the ordinary
      -- financial snapshot.  If it reports a HEAD, the two keyings disagree
      -- about the same root — the shape of WP-06 review finding F2, in whichever
      -- direction the disagreement runs — and the coordinator must not pick a
      -- winner between them.  It refuses, cheaply, on a field it already holds.
      if v_effective->>'authority' is distinct from 'TSFIN' then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'reason','INTERFACE_I7_REPORTS_AN_AUTHORITY_THE_FAMILY_KEY_DOES_NOT',
            'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
            'declared_family_booking_id',v_member_family_booking_ids[v_i],
            'i7_authority',v_effective->'authority','i7_head_id',v_effective->'head_id'));
      end if;
      if exists (
        select 1
          from (select (effective_element.value->>'component_id')::uuid as component_id
                  from pg_catalog.jsonb_array_elements(
                         coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value)
                except
                select pg_catalog.unnest(v_before_ids)) as missing_from_declared
        union all
        select 1
          from (select pg_catalog.unnest(v_before_ids)
                except
                select (effective_element.value->>'component_id')::uuid
                  from pg_catalog.jsonb_array_elements(
                         coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value)) as extra_in_declared
      ) then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_MISMATCH','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'authority',v_effective->'authority',
            'reason','DECLARED_POSITION_DIFFERS_FROM_THE_EFFECTIVE_INVENTORY'));
      end if;
      -- I-7 also carries each component's content hash, so a component RETAINED
      -- from a head-less root gets the same byte-identity guarantee as one
      -- retained from a committed head (review finding U3(a), extended).
      v_offending:=(
        select pg_catalog.jsonb_build_object(
                 'component_id',component_element.value->'component_id',
                 'effective_sha256',effective_element.value->'component_sha256',
                 'requested_sha256',pg_catalog.encode(
                   private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(
                       component_element.value)),'hex'))
          from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
          join pg_catalog.jsonb_array_elements(
                 coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value)
            on effective_element.value->>'component_id'=component_element.value->>'component_id'
         where effective_element.value->>'component_sha256'
               is distinct from pg_catalog.encode(
                 private.weekly_source_publication_request_digest_v1(
                   private.weekly_source_publication_component_content_v1(
                     component_element.value)),'hex')
         limit 1);
      if v_offending is not null then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_RETAINED_COMPONENT_CHANGED','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'authority',v_effective->'authority',
            'component',v_offending));
      end if;
      v_offending:=null;
      -- WP-06c review F1, the head-less source: I-7 carries each component's
      -- own content hash, computed through this file's canonicaliser, content
      -- projection and encoder, so it is directly comparable with a committed
      -- head row's `component_sha256` and with the destination side.
      if v_i=1 then
        select coalesce(pg_catalog.jsonb_object_agg(
                 effective_element.value->>'component_id',
                 effective_element.value->'component_sha256'),'{}'::jsonb)
          into v_source_component_hashes
          from pg_catalog.jsonb_array_elements(
                 coalesce(v_effective->'components','[]'::jsonb)) as effective_element(value);
      end if;
      v_before_source:=v_before_source||'I7'::text;

    else
      -- (3) No head and no I-7.  The declared position cannot be proved, so it
      -- is never recorded as if it were.  An A-to-B bundle is REFUSED rather
      -- than published on an unverified before-position, because that is where
      -- "minus only the moved components" and "keeps every existing component"
      -- would otherwise rest on the caller's word (review U3 case G1-a).
      if v_n=2 then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_BEFORE_POSITION_UNPROVABLE','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
            'reason','NO_COMMITTED_HEAD_AND_INTERFACE_I7_IS_NOT_INSTALLED',
            'required_owner','private.weekly_source_effective_inventory_v1(uuid)'));
      end if;
      v_before_source:=v_before_source||'DECLARED_UNPROVED'::text;
    end if;

    if v_i=1 then
      v_after_1:=v_component_ids; v_before_1:=v_before_ids;
    else
      v_after_2:=v_component_ids; v_before_2:=v_before_ids;
    end if;
    v_entitlement:=null;
  end loop;

  select coalesce(pg_catalog.array_agg(moved_element.value::uuid),array[]::uuid[])
    into v_moved
    from pg_catalog.jsonb_array_elements_text(
           coalesce(v_control->'moved_component_ids','[]'::jsonb)) as moved_element(value);

  if v_n=1 then
    if pg_catalog.cardinality(v_moved)>0 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('reason','MOVED_SET_ON_SINGLE_ROOT_BUNDLE'));
    end if;
  else
    -- "an already-authorised B keeps EVERY existing component" (24 section 4.5
    -- step 3).  This is tested FIRST so that a B which loses one gets the exact
    -- code the interface documents.  Placed after the combined set proof it was
    -- unreachable, because `B-after = B-before union moved` already implies
    -- `B-before is a subset of B-after` (review finding F12).
    if pg_catalog.cardinality(
         private.weekly_source_uuid_set_difference_v1(v_before_2,v_after_2))>0 then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_TARGET_COMPONENT_DROPPED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'dropped',pg_catalog.to_jsonb(
            private.weekly_source_uuid_set_difference_v1(v_before_2,v_after_2))));
    end if;
    -- 24 section 4.5 step 3 and H2-024, proved in the same transaction.
    if pg_catalog.cardinality(v_moved)=0
       or not private.weekly_source_uuid_array_is_distinct_v1(v_moved)
       or private.weekly_source_uuid_set_equals_v1(
            v_before_1,private.weekly_source_uuid_set_union_v1(v_after_1,v_moved)) is not true
       or pg_catalog.cardinality(private.weekly_source_uuid_set_intersect_v1(v_after_1,v_moved))>0
       or pg_catalog.cardinality(private.weekly_source_uuid_set_intersect_v1(v_before_2,v_moved))>0
       or private.weekly_source_uuid_set_equals_v1(
            v_after_2,private.weekly_source_uuid_set_union_v1(v_before_2,v_moved)) is not true then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'a_before',pg_catalog.to_jsonb(v_before_1),'a_after',pg_catalog.to_jsonb(v_after_1),
          'b_before',pg_catalog.to_jsonb(v_before_2),'b_after',pg_catalog.to_jsonb(v_after_2),
          'moved',pg_catalog.to_jsonb(v_moved)));
    end if;

    -- Round-5 ruling, Part E, "Partial Contract-to-Contract move":
    --
    --   "Not in scope for this release.  The supported operation is the
    --    whole-entitlement move.  A requested partial move must take the named
    --    fail-closed path and explain that partial movement is unsupported; it
    --    must not approximate the move."
    --
    -- THE DEFINITION IS THE WHOLE OF THE RULE.  A move is WHOLE when the source
    -- member retains nothing after it, and PARTIAL otherwise.  Three statements
    -- of that are available here and they are the SAME statement, because the
    -- set proofs immediately above have already established
    -- `A_after = A_before \ moved`:
    --
    --   (i)   A-after is empty                     -> cardinality(v_after_1)=0
    --   (ii)  every component of A-before moved    -> moved = A_before
    --   (iii) member 1 is certified zero           -> the declared flag, which
    --         the coordinator re-checks against `component_count` below and
    --         which the head relation's own CHECK enforces
    --
    -- (i) is the one tested, because it is read from what the request actually
    -- carries rather than from a flag the caller sets.  (ii) is asserted
    -- alongside it: if the two ever disagreed, the set proofs above would have
    -- been wrong, so the disagreement is itself refused rather than resolved.
    --
    -- This is a SCOPE gate, so it is tested before the content proof below: a
    -- partial move is refused for being partial, in plain English, not for a
    -- hash mismatch it may not even have.  It is a returned refusal before the
    -- first write, exactly like every other check in this section.
    if pg_catalog.cardinality(v_after_1)>0
       or private.weekly_source_uuid_set_equals_v1(v_moved,v_before_1) is not true then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_PARTIAL_MOVE_UNSUPPORTED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','ONLY_A_WHOLE_ENTITLEMENT_MOVE_IS_SUPPORTED_IN_THIS_RELEASE',
          'message','This bundle moves only part of the entitlement from one Contract '
                  ||'to the other and leaves the rest behind. Moving part of an '
                  ||'entitlement is not supported in this release: a Contract-to-Contract '
                  ||'amendment must move the whole entitlement, so that the old Contract '
                  ||'is left holding nothing. Nothing has been published. Either move '
                  ||'every component of the old Contract''s entitlement, or leave the '
                  ||'entitlement where it is.',
          'definition','A move is WHOLE when the source root retains nothing after it.',
          'a_before',pg_catalog.to_jsonb(v_before_1),
          'a_after',pg_catalog.to_jsonb(v_after_1),
          'a_after_component_count',pg_catalog.cardinality(v_after_1),
          'moved',pg_catalog.to_jsonb(v_moved),
          'moved_is_the_whole_source_position',
            private.weekly_source_uuid_set_equals_v1(v_moved,v_before_1)));
    end if;

    -- WP-06c review finding F1 (HIGH).  Everything above proves the move by
    -- IDENTITY.  A moved component's CONTENT was never compared with anything:
    -- it is absent from A-after and from B-before, so neither retained-component
    -- check above joins it, and a request composed outside the builder could
    -- re-price a shift on its way from A to B and be published with a receipt
    -- (executed: 85.50 -> 850.50 and 4.25 -> 40 hours).  The coordinator is the
    -- last gate before an entitlement is published, so it verifies rather than
    -- trusts: every moved component must still equal, byte for byte, what the
    -- SOURCE authority actually held - A's committed head row's
    -- `component_sha256` where A has a head, else interface I-7's own content
    -- hash for the same component.  `24 section 4.5` step 3 is "B-after is B's
    -- previously effective complete entitlement plus only those component(s)" -
    -- THOSE components, not components with those identities.
    --
    -- The comparison uses the same content projection as the retained checks, so
    -- `component_ordinal`, `movement_id` and `movement_group_id` - the three
    -- things a move legitimately changes - are excluded, and nothing else is.
    -- A moved component the source authority does not carry at all has a null
    -- hash here and is refused by the same test (`is distinct from`).
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=2);
    select pg_catalog.count(*)::integer into v_moved_found
      from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
     where (component_element.value->>'component_id')::uuid=any(v_moved);
    if v_moved_found<>pg_catalog.cardinality(v_moved) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVE_SET_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'reason','THE_DESTINATION_DOES_NOT_CARRY_EACH_MOVED_COMPONENT_EXACTLY_ONCE',
          'moved',pg_catalog.to_jsonb(v_moved),
          'moved_components_in_b_after',v_moved_found));
    end if;
    v_offending:=(
      select pg_catalog.jsonb_build_object(
               'component_id',component_element.value->'component_id',
               'source_authority',v_before_source[1],
               'source_sha256',pg_catalog.to_jsonb(
                 v_source_component_hashes->>(component_element.value->>'component_id')),
               'requested_sha256',pg_catalog.encode(
                 private.weekly_source_publication_request_digest_v1(
                   private.weekly_source_publication_component_content_v1(
                     component_element.value)),'hex'))
        from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
       where (component_element.value->>'component_id')::uuid=any(v_moved)
         and (v_source_component_hashes->>(component_element.value->>'component_id'))
             is distinct from pg_catalog.encode(
               private.weekly_source_publication_request_digest_v1(
                 private.weekly_source_publication_component_content_v1(
                   component_element.value)),'hex')
       limit 1);
    if v_offending is not null then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVED_COMPONENT_CHANGED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',2,'component',v_offending));
    end if;
    v_offending:=null;
    v_entitlement:=null;
  end if;

  -- Movement identity: every moved component carries movement_id in its
  -- destination head and no retained component carries one.
  for v_i in 1..v_n loop
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=v_i);
    if exists (
      select 1
        from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
       where ((component_element.value->>'component_id')::uuid=any(v_moved)
              and v_i=2
              and component_element.value->'movement_id'='null'::jsonb)
          or (not ((component_element.value->>'component_id')::uuid=any(v_moved) and v_i=2)
              and component_element.value->'movement_id'<>'null'::jsonb)
    ) then
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_MOVEMENT_IDENTITY_INVALID','retryable',false,
        'detail',pg_catalog.jsonb_build_object('root_ordinal',v_i));
    end if;
    v_entitlement:=null;
  end loop;

  -- ---- 6. the target root's authorisation state ---------------------------
  v_target_auth:=case when pg_catalog.jsonb_typeof(coalesce(v_control->'target_root_authorisation','null'::jsonb))='object'
                      then v_control->'target_root_authorisation' end;
  v_review:=case when pg_catalog.jsonb_typeof(coalesce(v_control->'whole_root_office_review','null'::jsonb))='object'
                 then v_control->'whole_root_office_review' end;

  for v_i in 1..v_n loop
    -- Decision D8: the authorisation record is per ROOT and lives in
    -- public.weekly_source_root_authorisations, keyed on the physical
    -- root_timesheet_id, with at most one live generation per root.  It is no
    -- longer the per-source-row lineage binding.
    select pg_catalog.count(*)::integer into v_live_generations
      from public.weekly_source_root_authorisations as authorisation_row
     where authorisation_row.root_timesheet_id=v_member_root_ids[v_i]
       and authorisation_row.withdrawn_at_utc is null;

    -- WP-07 review finding F2, answered from THIS side of the seam.  The
    -- authorisation record is keyed on the PHYSICAL root_timesheet_id, and since
    -- schema change S8 removed the physical uniqueness a single family can hold
    -- several of them.  A test that counts only the canonical root's own
    -- generations lets a ROTATED family whose NON-CANONICAL member is already
    -- authorised be authorised a second time; the root is then payable and
    -- carries two live generations, which every withdrawal path treats as an
    -- impossible state, so it can never be withdrawn again.
    --
    -- The "is it already authorised?" test is therefore over the whole FAMILY:
    -- every member Timesheet the lock result carries for this root, and the
    -- family booking id the authorisation row itself records.  The coordinator
    -- does not rely on interface I-6 having been widened to match — that is
    -- precisely the assumption this finding punished.
    select coalesce(pg_catalog.array_agg(member_element.value::uuid),array[]::uuid[])
      into v_family_members
      from pg_catalog.jsonb_array_elements(p_lock_result->'families') as family_element(value)
     cross join pg_catalog.jsonb_array_elements_text(
                  coalesce(family_element.value->'member_timesheet_ids','[]'::jsonb))
                as member_element(value)
     where family_element.value->>'requested_timesheet_id'=v_member_root_ids[v_i]::text;
    select pg_catalog.count(*)::integer into v_family_live_generations
      from public.weekly_source_root_authorisations as authorisation_row
     where authorisation_row.withdrawn_at_utc is null
       and (authorisation_row.root_timesheet_id=v_member_root_ids[v_i]
            or authorisation_row.root_timesheet_id=any(v_family_members)
            or pg_catalog.btrim(authorisation_row.family_booking_id)
               =pg_catalog.btrim(v_member_family_booking_ids[v_i]));

    if v_i=2 and v_target_auth is not null then
      -- 24 section 4.5 step 4: only a genuinely new, never-authorised B root is
      -- created and authorised here, and it is never authorised silently.
      if v_family_live_generations>0 then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ALREADY_AUTHORISED','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'live_generations',v_family_live_generations,
            'live_generations_on_this_physical_root',v_live_generations,
            'root_timesheet_id',v_member_root_ids[v_i],
            'family_booking_id',v_member_family_booking_ids[v_i],
            'family_member_timesheet_ids',pg_catalog.to_jsonb(v_family_members)));
      end if;
      if (v_target_auth->>'timesheet_id')::uuid is distinct from v_member_root_ids[2] then
        return pg_catalog.jsonb_build_object(
          'ok',false,'published',false,'replayed',false,
          'code','WEEKLY_SOURCE_PUBLICATION_REQUEST_INVALID','retryable',false,
          'detail',pg_catalog.jsonb_build_object(
            'reason','TARGET_AUTHORISATION_TIMESHEET_MISMATCH'));
      end if;

      -- Review U2.  "Unknown to Weekly Source" is NOT the same as "blank": a B
      -- Timesheet that already existed, was never touched by Weekly Source and
      -- carries its own unrelated shifts satisfied the old test and was
      -- authorised silently, which 24 section 4.5 step 4, file 26 Gate 5 step 6,
      -- 27 section 8 step 6 and H2-024 all forbid.  B is now treated as
      -- genuinely new only when it is provably blank as well as unknown.
      v_blank:=private.weekly_source_publication_target_root_blank_v1(
        v_member_root_ids[2],v_member_family_booking_ids[2]);
      if coalesce((v_blank->>'blank')::boolean,false) is not true then
        -- The whole-root Office review is the only way past this, and it is a
        -- real control or it is nothing: a genuine boolean true, an existing
        -- reviewer, a time, bound to the accepted decision, and PERSISTED on
        -- the accepted decision so the act can be shown afterwards.  The three
        -- identity columns now exist on the bundle relation, so the check is
        -- the real one: the ACCEPTED DECISION must carry the review, and the
        -- request must quote exactly what it carries.
        if v_bundle.whole_root_review_required is not true
           or v_bundle.whole_root_reviewed_by_user_id is null
           or v_bundle.whole_root_reviewed_at_utc is null then
          return pg_catalog.jsonb_build_object(
            'ok',false,'published',false,'replayed',false,
            'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED','retryable',false,
            'detail',pg_catalog.jsonb_build_object(
              'reason','THE_ACCEPTED_DECISION_CARRIES_NO_WHOLE_ROOT_OFFICE_REVIEW',
              'blank_check',v_blank,
              'decision_bundle_id',v_decision_bundle_id,
              'bundle_revision',v_bundle_revision));
        end if;
        -- The request's own review object: a real boolean, a real reviewer, a
        -- real time, and bound to this accepted decision.
        if v_review is null
           or pg_catalog.jsonb_typeof(v_review->'reviewed')<>'boolean'
           or (v_review->'reviewed')::boolean is not true
           or (v_review->>'reviewed_by_user_id') is null
           or (v_review->>'reviewed_at_utc') is null
           or (v_review->>'decision_id')::uuid is distinct from v_decision_id then
          return pg_catalog.jsonb_build_object(
            'ok',false,'published',false,'replayed',false,
            'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED','retryable',false,
            'detail',pg_catalog.jsonb_build_object(
              'reason','WHOLE_ROOT_OFFICE_REVIEW_MISSING_OR_MALFORMED',
              'blank_check',v_blank,'review',v_review));
        end if;
        -- And it must be the review the ACCEPTED DECISION carries, not one the
        -- caller invented for this request.  The reviewer's existence is the
        -- bundle column's own foreign key, so it is proved by the row, not
        -- re-checked here.
        if (v_review->>'reviewed_by_user_id')::uuid
             is distinct from v_bundle.whole_root_reviewed_by_user_id
           or (v_review->>'reviewed_at_utc')::timestamptz
                is distinct from v_bundle.whole_root_reviewed_at_utc then
          return pg_catalog.jsonb_build_object(
            'ok',false,'published',false,'replayed',false,
            'code','WEEKLY_SOURCE_PUBLICATION_TARGET_ROOT_REVIEW_REQUIRED','retryable',false,
            'detail',pg_catalog.jsonb_build_object(
              'reason','REVIEW_IS_NOT_THE_ONE_ON_THE_ACCEPTED_DECISION',
              'accepted_reviewer',v_bundle.whole_root_reviewed_by_user_id,
              'requested_reviewer',v_review->'reviewed_by_user_id'));
        end if;
      end if;
    elsif v_live_generations=0 then
      -- A head over a root Weekly Source has not authorised would never reach
      -- payroll, so it is refused rather than written (decision D8: the live
      -- authorisation row is the test).
      return pg_catalog.jsonb_build_object(
        'ok',false,'published',false,'replayed',false,
        'code','WEEKLY_SOURCE_PUBLICATION_TARGET_NOT_AUTHORISED','retryable',false,
        'detail',pg_catalog.jsonb_build_object(
          'root_ordinal',v_i,'root_timesheet_id',v_member_root_ids[v_i],
          'family_booking_id',v_member_family_booking_ids[v_i]));
    end if;
  end loop;

  -- =========================================================================
  -- Everything from here writes.  Every failure below RAISES and rolls the
  -- whole transaction back: no head, pointer, invalidation, dirty job, B
  -- authorisation or receipt may survive (R11; 26 Gate 5 pass condition).
  -- =========================================================================

  -- Observe exact writes in this invocation, not differences against global history.
  v_observation_frame:=private.weekly_source_observation_begin_v1();

  -- ---- 7. stage every member head and its complete component inventory ----
  v_source_generation_digest:=private.weekly_source_publication_request_digest_v1(
    v_canonical->'financial_request'->'source_revision');

  for v_i in 1..v_n loop
    v_entitlement:=(
      select entitlement_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'member_entitlements') as entitlement_element(value)
       where (entitlement_element.value->>'root_ordinal')::integer=v_i);
    v_choice:=(
      select choice_element.value
        from pg_catalog.jsonb_array_elements(
               v_canonical->'financial_request'->'contract_choices') as choice_element(value)
       where (choice_element.value->>'root_ordinal')::integer=v_i);

    select coalesce(pg_catalog.jsonb_agg(
             pg_catalog.jsonb_build_object(
               'component_ordinal',component_element.value->'component_ordinal',
               'component_id',component_element.value->'component_id')
             order by (component_element.value->>'component_ordinal')::integer),'[]'::jsonb),
           coalesce(pg_catalog.jsonb_agg(
             pg_catalog.to_jsonb(pg_catalog.encode(
               private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value)),'hex'))
             order by (component_element.value->>'component_ordinal')::integer),'[]'::jsonb)
      into v_inventory_pairs,v_component_hashes
      from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value);

    insert into public.weekly_source_entitlement_heads(
      id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
      prior_head_id,state,certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,
      decided_by_user_id,staged_at_utc,created_at_utc
    ) values (
      v_head_ids[v_i],
      v_entitlement->>'authority_kind',
      v_bundle.agency_id,
      v_candidate_id,
      (v_choice->>'contract_id')::uuid,
      (v_choice->>'week_ending_date')::date,
      v_member_root_ids[v_i],
      v_member_family_booking_ids[v_i],
      v_member_root_versions[v_i],
      v_head_revisions[v_i],
      v_prior_head_ids[v_i],
      'STAGED',
      (v_entitlement->>'certified_zero')::boolean,
      (v_entitlement->>'component_count')::integer,
      -- entitlement_digest: the complete economic content of the head.
      private.weekly_source_publication_request_digest_v1(
        pg_catalog.jsonb_build_object(
          'authority_kind',v_entitlement->'authority_kind',
          'certified_zero',v_entitlement->'certified_zero',
          'component_count',v_entitlement->'component_count',
          'components',v_component_hashes)),
      -- inventory_digest: which components the head contains, identity only.
      private.weekly_source_publication_request_digest_v1(
        pg_catalog.jsonb_build_object('components',v_inventory_pairs)),
      v_source_generation_digest,
      v_decision_bundle_id,v_bundle_revision,v_decision_id,
      v_bundle.decided_by_user_id,v_now,v_now);

    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,segment_id,segment_key,
      segment_stable_key,work_date,reference_number,hours_day,hours_night,hours_sat,
      hours_sun,hours_bh,additional_code_raw,unit_count,unit_pay_rate,unit_charge_rate,
      expense_code,pay_ex_vat,charge_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,movement_id,movement_group_id,
      component_sha256,created_at_utc)
    select
      v_head_ids[v_i],
      (component_element.value->>'component_ordinal')::integer,
      (component_element.value->>'component_id')::uuid,
      component_element.value->>'component_kind',
      component_element.value->>'economic_key_type',
      component_element.value->>'economic_key_value',
      component_element.value->>'component_member_identity',
      component_element.value->>'segment_id',
      component_element.value->>'segment_key',
      component_element.value->>'segment_stable_key',
      (component_element.value->>'work_date')::date,
      component_element.value->>'reference_number',
      (component_element.value->>'hours_day')::numeric,
      (component_element.value->>'hours_night')::numeric,
      (component_element.value->>'hours_sat')::numeric,
      (component_element.value->>'hours_sun')::numeric,
      (component_element.value->>'hours_bh')::numeric,
      component_element.value->>'additional_code_raw',
      (component_element.value->>'unit_count')::numeric,
      (component_element.value->>'unit_pay_rate')::numeric,
      (component_element.value->>'unit_charge_rate')::numeric,
      component_element.value->>'expense_code',
      (component_element.value->>'pay_ex_vat')::numeric,
      (component_element.value->>'charge_ex_vat')::numeric,
      (component_element.value->>'exclude_from_pay')::boolean,
      component_element.value->>'origin',
      v_decision_bundle_id,v_bundle_revision,
      (component_element.value->>'movement_id')::uuid,
      (component_element.value->>'movement_group_id')::uuid,
      private.weekly_source_publication_request_digest_v1(
                     private.weekly_source_publication_component_content_v1(component_element.value)),
      v_now
    from pg_catalog.jsonb_array_elements(v_entitlement->'components') as component_element(value)
    order by (component_element.value->>'component_ordinal')::integer;

    v_entitlement:=null; v_choice:=null;
  end loop;

  -- ---- 8. interface I-6 for a genuinely new B root ------------------------
  -- After staging and before the single invalidation.  I-6 calls the UNCHANGED
  -- ordinary Authorise owner exactly once and inserts the lineage generation of
  -- proof/34 section 4.  It is late-bound: WP-07 owns it.
  if v_target_auth is not null then
    v_target_authorise_result:=private.weekly_source_first_authorise_core_v1(
      v_member_root_ids[2],
      v_target_auth->>'expected_row_signature',
      (v_target_auth->>'actor_user_id')::uuid,
      p_lock_result);
    if coalesce((v_target_authorise_result->>'ok')::boolean,false) is not true then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_TARGET_AUTHORISATION_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_TARGET_AUTHORISATION_FAILED',
                'first_authorise_result',v_target_authorise_result)::text;
    end if;
  end if;

  -- ---- 9. activate every head together ------------------------------------
  -- 24 section 5.1: pass the same transaction token and scalar head/currentness
  -- evidence used by the publication receipt.  The token is taken before
  -- activation so the head, the invalidation and the receipt all carry it.
  v_token:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token is null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_SCOPE_TOKEN_UNAVAILABLE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_SCOPE_TOKEN_UNAVAILABLE')::text;
  end if;

  for v_i in 1..v_n loop
    if v_current_head_ids[v_i] is not null then
      update public.weekly_source_entitlement_heads
         set state='SUPERSEDED',
             superseded_at_utc=v_now,
             superseded_by_head_id=v_head_ids[v_i]
       where id=v_current_head_ids[v_i];
    end if;
  end loop;
  for v_i in 1..v_n loop
    update public.weekly_source_entitlement_heads
       set state='COMMITTED_CURRENT',
           committed_at_utc=v_now,
           publication_receipt_digest=v_digest,
           scope_change_tx_token=v_token
     where id=v_head_ids[v_i];
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_HEAD_ACTIVATION_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_HEAD_ACTIVATION_FAILED',
                'head_id',v_head_ids[v_i],'rows',v_rows)::text;
    end if;
  end loop;

  -- ---- 10. current_entitlement_head_id on the live ROOT AUTHORISATION ------
  -- Decision D8 (WP-01c): the authorisation record, and therefore this pointer,
  -- is per ROOT in public.weekly_source_root_authorisations, not on the
  -- per-source-row lineage binding.  proof/34 section 4: written by the
  -- head-publication coordinator only, and nothing else on the generation row
  -- is touched.  The relation's own partial unique index gives at most one live
  -- generation per root, so exactly one row must move.
  for v_i in 1..v_n loop
    update public.weekly_source_root_authorisations
       set current_entitlement_head_id=v_head_ids[v_i]
     where root_timesheet_id=v_member_root_ids[v_i]
       and withdrawn_at_utc is null;
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_ROOT_AUTHORISATION_POINTER_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_PUBLICATION_ROOT_AUTHORISATION_POINTER_FAILED',
                'root_ordinal',v_i,
                'root_timesheet_id',v_member_root_ids[v_i],
                'live_authorisation_rows_updated',v_rows)::text;
    end if;
    v_lineage_rows:=v_lineage_rows+v_rows;
  end loop;

  -- ---- 11. ONE bounded Workbench invalidation -----------------------------
  -- proof/32 section 8 step 3 and H2-036: exactly once, after every head and
  -- any B creation/Authorise, with the complete aligned Candidate/root pairs
  -- and the transaction token, success required before the receipt.  The
  -- ordinary Authorise trigger may queue a strict-subset job for the same
  -- Candidate under the same token; that is the accepted option 1 outcome and
  -- Weekly Source neither suppresses it nor folds it.
  v_invalidation:=private.pay_workbench_scope_invalidate_v1(
    p_candidate_ids=>pg_catalog.array_fill(v_candidate_id,array[v_n]),
    p_timesheet_ids=>v_member_root_ids,
    p_reason=>'WEEKLY_SOURCE_ENTITLEMENT_HEAD_PUBLICATION',
    p_scope_change_tx_token=>v_token,
    p_payload_json=>pg_catalog.jsonb_build_object(
      'weekly_source_publication',pg_catalog.jsonb_build_object(
        'decision_bundle_id',v_decision_bundle_id,
        'bundle_revision',v_bundle_revision,
        'publication_mode',v_mode,
        'request_digest',pg_catalog.encode(v_digest,'hex'),
        'head_ids',pg_catalog.to_jsonb(v_head_ids),
        'head_revisions',pg_catalog.to_jsonb(v_head_revisions))));
  -- `<>` on a missing key yields NULL, the IF does not fire, and the receipt is
  -- written anyway.  An invalidation result that does not say it covered
  -- exactly one Candidate must FAIL, so this is `is distinct from` (review
  -- finding, lower severity 1: executed with a wrapper that stripped the key,
  -- and the publication went through).
  if coalesce((v_invalidation->>'ok')::boolean,false) is not true
     or (v_invalidation->>'candidate_count')::integer is distinct from 1
     or (v_invalidation->>'scope_change_tx_token')::uuid is distinct from v_token
     or coalesce((v_invalidation->>'job_inserted_count')::integer,0)
        +coalesce((v_invalidation->>'job_coalesced_count')::integer,0)<1 then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_FAILED',
              'invalidation',v_invalidation)::text;
  end if;

  -- ---- 11a. the R25 invalidation contract (HANDOVER 2 round 4, ruling 6) ---
  -- Restated R25: EVERY invalidation and dirty job created for this Candidate
  -- in this transaction - the explicit call, the ordinary Authorise route and
  -- every registered DIRTY_TRIGGER:<table>:<op> path - must carry the same
  -- token UUID and the same generation, and every non-complete job's scope,
  -- after the installed rotation normaliser, must be an exact subset of the
  -- declared aligned scope for that Candidate.  A different Candidate, an
  -- outside root or a token disagreement is a CONTRACT FAILURE, not extra
  -- harmless work, and rolls the whole transaction back.  Checked here, after
  -- the one explicit invalidation and BEFORE the receipt.
  --
  -- Ruling 6 point 1: calling the token owner again counts as the same token
  -- only if the returned UUID equals the controlling transaction token.
  v_token_recheck:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token_recheck is distinct from v_token then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','CONTROLLING_TOKEN_CHANGED_INSIDE_THE_TRANSACTION',
              'controlling_token',v_token,'recheck_token',v_token_recheck)::text;
  end if;

  -- The declared complete aligned scope for this Candidate, expanded through
  -- the INSTALLED rotation normaliser (call-only, contract section 2).
  v_normalised:=public._pay_workbench_normalise_timesheet_rotation_scope_payload(
    v_member_root_ids,array[]::uuid[]);
  select coalesce(pg_catalog.array_agg(distinct scope_element.value::uuid),array[]::uuid[])
    into v_declared_scope
    from pg_catalog.jsonb_array_elements_text(
           coalesce(v_normalised->'family_timesheet_ids',
                    coalesce(v_normalised->'canonical_timesheet_ids',
                             pg_catalog.to_jsonb(v_member_root_ids)))) as scope_element(value);
  v_declared_scope:=private.weekly_source_uuid_set_union_v1(v_declared_scope,v_member_root_ids);

  -- At most one new scope-change transaction, and it is the controlling token.
  if exists (
    select 1 from pg_temp.ws_observation_rows_v1 as effect
      cross join lateral (select native.* from public.banking_pay_scope_change_transactions native
        where native.tx_token=effect.row_id offset 0) as scope_tx_row
     where effect.frame_id=v_observation_frame and effect.kind='TOKEN' and exists(select 1 from pg_temp.ws_observation_rows_v1 observed
       where observed.frame_id=v_observation_frame and observed.kind='TOKEN'
         and observed.row_id=scope_tx_row.tx_token)
       and scope_tx_row.tx_token is distinct from v_token) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','A_SECOND_SCOPE_CHANGE_TOKEN_WAS_CREATED_IN_THIS_TRANSACTION')::text;
  end if;

  -- Every job this transaction queued: same token, same (absent) generation,
  -- this Candidate, and a scope inside the declared aligned scope.
  v_offending:=(
    select pg_catalog.jsonb_build_object(
             'job_id',job_row.id,'job_type',job_row.job_type,
             'candidate_id',job_row.candidate_id,
             'scope_change_tx_token',job_row.scope_change_tx_token,
             'scope_change_generation',job_row.scope_change_generation,
             'targeted_timesheet_ids',job_row.payload_json->'targeted_timesheet_ids',
             'reason',job_row.payload_json->'reason')
      from pg_temp.ws_observation_rows_v1 as effect
      cross join lateral (select native.* from public.banking_pay_workbench_jobs native
        where native.id=effect.row_id offset 0) as job_row
     where effect.frame_id=v_observation_frame and effect.kind='JOB' and exists(select 1 from pg_temp.ws_observation_rows_v1 observed
       where observed.frame_id=v_observation_frame and observed.kind='JOB'
         and observed.row_id=job_row.id)
       and (
         job_row.scope_change_tx_token is distinct from v_token
         or job_row.scope_change_generation is not null
         or (job_row.candidate_id is not null and job_row.candidate_id<>v_candidate_id)
         or exists (
           select 1
             from pg_catalog.jsonb_array_elements_text(
                    coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb)) as target(value)
            where not (target.value::uuid=any(v_declared_scope)))
         or exists (
           select 1
             from pg_catalog.jsonb_array_elements_text(
                    coalesce(job_row.payload_json->'linked_timesheet_ids','[]'::jsonb)) as linked(value)
            where not (linked.value::uuid=any(v_declared_scope))))
     limit 1);
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','A_JOB_QUEUED_IN_THIS_TRANSACTION_BREAKS_THE_R25_CONTRACT',
              'controlling_token',v_token,
              'declared_scope',pg_catalog.to_jsonb(v_declared_scope),
              'job',v_offending)::text;
  end if;

  -- Validate every captured scope-state write, including reused rows and
  -- writes carrying an incorrect Candidate or token. No historical snapshot.
  v_offending:=(
    select pg_catalog.jsonb_build_object(
             'timesheet_id',scope_state_row.timesheet_id,
             'candidate_id',scope_state_row.candidate_id,
             'last_scope_change_tx_token',scope_state_row.last_scope_change_tx_token,
             'last_dirty_reason',scope_state_row.last_dirty_reason)
      from pg_temp.ws_observation_rows_v1 as effect
      cross join lateral (select native.* from private.banking_pay_workbench_timesheet_scope_state native
        where native.timesheet_id=effect.row_id offset 0) as scope_state_row
     where effect.frame_id=v_observation_frame and effect.kind='SCOPE'
       and exists(select 1 from pg_temp.ws_observation_rows_v1 observed
         where observed.frame_id=v_observation_frame and observed.kind='SCOPE'
           and observed.row_id=scope_state_row.timesheet_id)
       and (scope_state_row.candidate_id is distinct from v_candidate_id
         or scope_state_row.last_scope_change_tx_token is distinct from v_token
         or not (scope_state_row.timesheet_id=any(v_declared_scope)))
     limit 1);
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','A_SCOPE_STATE_ROW_OUTSIDE_THE_DECLARED_ALIGNED_SCOPE_WAS_DIRTIED',
              'declared_scope',pg_catalog.to_jsonb(v_declared_scope),
              'scope_state',v_offending)::text;
  end if;

  -- Registry-only writes are part of the same proof, even with no Timesheet
  -- or enqueue. Preserve missing/deleted observations and probe by native PK.
  v_offending:=(
    select pg_catalog.jsonb_build_object(
      'candidate_id',effect.row_id,'deleted',effect.deleted,
      'last_scope_change_tx_token',registry_row.last_scope_change_tx_token,
      'reason',case
        when effect.deleted or registry_row.candidate_id is null then 'REGISTRY_ROW_MISSING_OR_DELETED'
        when registry_row.candidate_id is distinct from v_candidate_id then 'REGISTRY_FOR_A_DIFFERENT_CANDIDATE'
        else 'REGISTRY_CARRIES_A_DIFFERENT_TOKEN' end)
    from pg_temp.ws_observation_rows_v1 as effect
    left join lateral (
      select native.candidate_id,native.last_scope_change_tx_token
      from private.banking_pay_workbench_candidate_scope_registry native
      where native.candidate_id=effect.row_id offset 0
    ) as registry_row on true
    where effect.frame_id=v_observation_frame and effect.kind='REGISTRY'
      and (effect.deleted or registry_row.candidate_id is null
        or registry_row.candidate_id is distinct from v_candidate_id
        or registry_row.last_scope_change_tx_token is distinct from v_token)
    order by effect.row_id limit 1);
  if v_offending is not null then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',detail=pg_catalog.jsonb_build_object(
        'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
        'reason',v_offending->>'reason','registry',v_offending)::text;
  end if;

  -- Exactly one effective complete-scope dirty result per Candidate and token
  -- (ruling 6 point 6).
  if (select pg_catalog.count(*)
        from pg_temp.ws_observation_rows_v1 as effect
      cross join lateral (select native.* from public.banking_pay_workbench_jobs native
        where native.id=effect.row_id offset 0) as job_row
       where effect.frame_id=v_observation_frame and effect.kind='JOB'
         and exists(select 1 from pg_temp.ws_observation_rows_v1 observed
           where observed.frame_id=v_observation_frame and observed.kind='JOB'
             and observed.row_id=job_row.id)
         and job_row.candidate_id=v_candidate_id
         and job_row.scope_change_tx_token=v_token
         and job_row.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
         and job_row.status in ('QUEUED','RUNNING')
         and private.weekly_source_uuid_set_equals_v1(
               (select coalesce(pg_catalog.array_agg(distinct target.value::uuid),array[]::uuid[])
                  from pg_catalog.jsonb_array_elements_text(
                         coalesce(job_row.payload_json->'targeted_timesheet_ids','[]'::jsonb)) as target(value)),
               v_member_root_ids))<>1 then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_PUBLICATION_INVALIDATION_CONTRACT_FAILURE',
              'reason','NOT_EXACTLY_ONE_COMPLETE_SCOPE_JOB_FOR_THE_CANDIDATE_UNDER_THIS_TOKEN')::text;
  end if;

  -- No new Workbench effect may appear after the positive proof and before return.
  perform private.weekly_source_observation_seal_v1();
  -- ---- 12. exactly one immutable receipt ----------------------------------
  insert into private.weekly_source_entitlement_publication_receipts(
    decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
    scope_change_tx_token,decision_id,decided_by_user_id,released_by_worker_id,
    released_by_worker_run_id,census_json,proof_json,created_at_utc
  ) values (
    v_decision_bundle_id,p_pending_bundle_id,v_bundle_revision,v_digest,v_mode,
    v_candidate_id,v_member_root_ids,v_member_family_booking_ids,v_member_root_versions,
    v_head_ids,v_token,v_decision_id,v_bundle.decided_by_user_id,p_worker_id,
    p_worker_run_id,coalesce(p_census,'{}'::jsonb),coalesce(p_proof,'{}'::jsonb),v_now)
  returning * into v_receipt;

  if v_bundle.state='PROPOSED' then
    update public.weekly_source_entitlement_decision_bundles
       set state='COMMITTED',committed_at_utc=v_now
     where decision_bundle_id=v_decision_bundle_id
       and bundle_revision=v_bundle_revision;
  end if;

  for v_i in 1..v_n loop
    v_result_heads:=v_result_heads||pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'root_ordinal',v_i,
        'head_id',v_head_ids[v_i],
        'head_revision',v_head_revisions[v_i],
        'prior_head_id',v_prior_head_ids[v_i],
        'superseded_head_id',v_current_head_ids[v_i],
        'root_timesheet_id',v_member_root_ids[v_i],
        'family_booking_id',v_member_family_booking_ids[v_i],
        'root_timesheet_version',v_member_root_versions[v_i]));
  end loop;

  perform private.weekly_source_observation_end_v1(v_observation_frame);
  return pg_catalog.jsonb_build_object(
    'ok',true,'published',true,'replayed',false,
    'receipt',private.weekly_source_publication_receipt_json_v1(v_receipt.id),
    'heads',v_result_heads,
    'scope_change_tx_token',v_token,
    'invalidation',v_invalidation,
    'root_authorisations_pointed',v_lineage_rows,
    'before_position_source',pg_catalog.to_jsonb(v_before_source),
    'declared_aligned_scope',pg_catalog.to_jsonb(v_declared_scope),
    'member_timesheet_ids',pg_catalog.to_jsonb(v_member_timesheet_ids));
end;
$function$;

CREATE OR REPLACE FUNCTION public.weekly_source_first_authorisation_withdraw_v1(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_signature text;
  v_actor public.tms_users%rowtype;
  v_recorded jsonb;
  v_context jsonb;
  v_candidate_id uuid;
  v_lock jsonb;
  v_census jsonb;
  v_members uuid[];
  v_verdict jsonb;
  v_canonical uuid;
  v_authorisation jsonb;
  v_authorisation_id uuid;
  v_token uuid;
  v_token_again uuid;
  v_observation_frame uuid;
  v_unauthorise jsonb;
  v_invalidation jsonb;
  v_contract jsonb;
  v_protected jsonb;
  v_head jsonb;
  v_head_id uuid;
  v_head_locked uuid;
  v_head_lock_count integer;
  v_receipt_id uuid;
  v_canonical_request jsonb;
  v_digest bytea;
  v_now timestamptz;
  v_rows integer;
  v_result jsonb;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  v_signature:=nullif(pg_catalog.btrim(coalesce(p_expected_row_signature,'')),'');
  if p_timesheet_id is null or p_actor_user_id is null or v_signature is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_UNAUTHORISE_REQUEST_INVALID','retryable',false,
      'reason','TIMESHEET_ACTOR_AND_EXPECTED_ROW_SIGNATURE_REQUIRED');
  end if;

  select * into v_actor from public.tms_users where id=p_actor_user_id;
  if not found or not coalesce(v_actor.is_active,false) then
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code','WEEKLY_SOURCE_UNAUTHORISE_ACTOR_INVALID','retryable',false,
      'reason','ACTOR_NOT_FOUND_OR_INACTIVE');
  end if;

  -- ---- section 5 step 1 / ruling A3 step 5: exact replay first, before the
  -- gate and the locks.  An exact replay returns the durable receipt's own
  -- recorded result and calls nothing.  A CONFLICTING replay -- the same replay
  -- identity presented with a different physical Timesheet expectation -- and a
  -- receipt that no longer rebuilds to its own digest both refuse, permanently,
  -- for manual review, and write no lifecycle row.
  v_recorded:=private.weekly_source_first_authorisation_withdrawal_recorded_v1(
    p_timesheet_id,v_signature,p_expected_timesheet_id);
  if v_recorded is not null then
    if coalesce((v_recorded->>'ok')::boolean,false) is not true then
      perform public._audit_insert(
        'timesheets',p_timesheet_id::text,
        'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
        null,
        pg_catalog.jsonb_build_object(
          'expected_row_signature',v_signature,
          'expected_timesheet_id',p_expected_timesheet_id,
          'code',v_recorded->>'code','reason',v_recorded->>'reason',
          'receipt_id',v_recorded->>'receipt_id',
          'stage','REPLAY'),
        'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
      return v_recorded;
    end if;
    return v_recorded||pg_catalog.jsonb_build_object('replayed',true);
  end if;

  -- One plain read so the gate can be pinned to a Candidate.  No write.
  v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    perform public._audit_insert(
      'timesheets',p_timesheet_id::text,
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
      null,
      pg_catalog.jsonb_build_object(
        'expected_row_signature',v_signature,
        'expected_timesheet_id',p_expected_timesheet_id,
        'code',v_context->>'code','reason',v_context->>'reason',
        'stage','IDENTITY'),
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
    return v_context||pg_catalog.jsonb_build_object('withdrawn',false,'replayed',false);
  end if;
  v_candidate_id:=(v_context->>'candidate_id')::uuid;

  -- ---- ruling A3 step 1, first half: the Candidate serial gate, then the
  -- rotation lock set, both through interface I-1 in the deadlock-free order of
  -- proof/34 section 5.
  v_lock:=private.weekly_source_lock_and_resolve_families_v1(
    v_candidate_id,
    array[p_timesheet_id]::uuid[],
    'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION_WITHDRAWAL',
    pg_catalog.gen_random_uuid(),
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL');
  if coalesce((v_lock->>'ok')::boolean,false) is not true then
    return v_lock||pg_catalog.jsonb_build_object('withdrawn',false,'replayed',false);
  end if;

  -- ---- ruling A3 step 1, second half: the currentness guards.  The live
  -- root-authorisation row and any committed current head of the family are
  -- taken FOR UPDATE, by PREDICATE rather than by an id read earlier, so a
  -- publication that committed while this call was resolving identity is seen
  -- and locked rather than missed.  Heads are locked after the family and
  -- Timesheet locks, matching the publication coordinator's own order.
  v_context:=private.weekly_source_first_authorisation_context_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    perform public._audit_insert(
      'timesheets',p_timesheet_id::text,
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
      null,
      pg_catalog.jsonb_build_object(
        'expected_row_signature',v_signature,
        'code',v_context->>'code','reason',v_context->>'reason',
        'stage','IDENTITY_UNDER_LOCK'),
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
    return v_context||pg_catalog.jsonb_build_object('withdrawn',false,'replayed',false);
  end if;

  v_canonical:=(v_context->>'canonical_timesheet_id')::uuid;
  v_authorisation:=v_context->'authorisation';
  select pg_catalog.array_agg(member_element.value::uuid)
    into v_members
  from pg_catalog.jsonb_array_elements_text(v_context->'member_timesheet_ids')
    as member_element(value);
  v_members:=coalesce(v_members,array[]::uuid[]);

  if pg_catalog.jsonb_typeof(coalesce(v_authorisation,'null'::jsonb))='object' then
    v_authorisation_id:=(v_authorisation->>'id')::uuid;
    perform 1
    from public.weekly_source_root_authorisations as authorisation_row
    where authorisation_row.id=v_authorisation_id
      and authorisation_row.withdrawn_at_utc is null
    for update;
  end if;

  -- Explicit cardinality, never `limit 1`: two committed current heads for one
  -- family is impossible under the partial unique index, and if it ever happened
  -- W11 refuses on the count rather than this statement picking one.
  -- `FOR UPDATE` cannot appear in the same query level as an aggregate, so the
  -- lock is taken in a CTE and counted outside it.
  with locked_head as (
    select head_row.id
    from public.weekly_source_entitlement_heads as head_row
    where head_row.state='COMMITTED_CURRENT'
      and (pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(coalesce(v_context->>'family_booking_id',''))
           or head_row.root_timesheet_id=any(v_members))
    for update
  )
  select pg_catalog.count(*)::integer,pg_catalog.min(locked_head.id::text)::uuid
    into v_head_lock_count,v_head_locked
  from locked_head;

  -- W1 to W11 under those locks, over the complete family.
  v_census:=private.weekly_source_freeze_census_v1(v_candidate_id,v_members);
  v_verdict:=private.weekly_source_first_authorisation_withdraw_checks_v1(
    v_context,v_census,p_expected_timesheet_id,v_signature);

  if coalesce((v_verdict->>'ok')::boolean,false) is not true then
    -- UNA-010: refused with no lifecycle write and an audit row.
    perform public._audit_insert(
      'timesheets',p_timesheet_id::text,
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL_REFUSED',
      null,
      pg_catalog.jsonb_build_object(
        'expected_row_signature',v_signature,
        'expected_timesheet_id',p_expected_timesheet_id,
        'candidate_id',v_candidate_id,
        'family_booking_id',v_context->>'family_booking_id',
        'canonical_timesheet_id',v_canonical,
        'code',v_verdict->>'code',
        'refusal_nature',v_verdict->>'refusal_nature',
        'refusal_message',v_verdict->>'refusal_message',
        'failed_checks',v_verdict->'failed_checks',
        'census_result',v_verdict->>'census_result',
        'stage','CHECKS'),
      'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);
    return pg_catalog.jsonb_build_object(
      'ok',false,'withdrawn',false,'replayed',false,
      'code',v_verdict->>'code',
      'refusal_nature',v_verdict->>'refusal_nature',
      'refusal_message',v_verdict->>'refusal_message',
      'retryable',coalesce((v_verdict->>'retryable')::boolean,false),
      -- An INTEGRITY refusal is the package's "Office review" disposition, and
      -- round 5 section A5 requires the finance-case write-off in particular to
      -- be routed to review rather than presented as a retry.
      'review_required',coalesce(v_verdict->>'refusal_nature','')='INTEGRITY',
      'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_canonical,
      'family_booking_id',v_context->>'family_booking_id',
      'checks',v_verdict->'checks',
      'failed_checks',v_verdict->'failed_checks',
      'census_result',v_verdict->>'census_result',
      'census_class_counts',v_verdict->'census_class_counts');
  end if;

  -- The head the verdict decided may be superseded, re-proved against the row
  -- this transaction actually holds FOR UPDATE.  A disagreement means the head
  -- moved between the lock and the verdict, which cannot happen under these
  -- locks; it fails closed rather than being written through.
  v_head:=case when pg_catalog.jsonb_typeof(coalesce(v_verdict->'head_supersession','null'::jsonb))='object'
               then v_verdict->'head_supersession' end;
  v_head_id:=(v_head->>'head_id')::uuid;
  if v_head_id is distinct from v_head_locked
     or v_head_lock_count is distinct from (case when v_head_id is null then 0 else 1 end) then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_LOCK_DISAGREEMENT'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_UNAUTHORISE_HEAD_LOCK_DISAGREEMENT',
              'locked_head_id',v_head_locked,
              'locked_head_count',v_head_lock_count,
              'verdict_head_id',v_head_id)::text;
  end if;

  -- ---- ruling A3 step 4: the controlling transaction token is established
  -- before the unauthorisation so that the ordinary Unauthorise trigger route
  -- and every registered DIRTY_TRIGGER path share it (round 4 ruling 6 point 1).
  v_token:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token is null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING',
              'context','WITHDRAWAL')::text;
  end if;

  v_observation_frame:=private.weekly_source_observation_begin_v1();

  -- ---------------------------------------------------------------------
  -- ORDERING RULE — WHY THE WRITES BELOW MUST NOT BE SEPARATED
  -- (WP-03 handoff N16, carried into WP-07b and kept by WP-07c.)
  --
  -- The rule, in plain English.  The Timesheet must never be observable as
  -- UNAUTHORISED while its Weekly Source authorisation record is still LIVE,
  -- and the entitlement head must never be observable as SUPERSEDED while the
  -- Timesheet is still authorised.  Those facts are what
  -- `private.weekly_source_managed_root_guard_v1` and the Gate 4 Workbench
  -- selector read, and a reader that saw a contradictory pair would be told
  -- something untrue about a root.  So all of them must be written in ONE
  -- transaction -- unless they are provably unobservable apart, which is the
  -- case here.
  --
  -- Why the present order is SAFE, and it is a property of PostgreSQL rather
  -- than of this code:
  --
  --   * every write below is in ONE transaction, and this owner has NO
  --     exception handler anywhere, so there is no sub-transaction that could
  --     commit one without the others;
  --   * PostgreSQL offers no dirty read at any isolation level — READ
  --     UNCOMMITTED is mapped to READ COMMITTED — so no concurrent session can
  --     see the order of statements inside this transaction.  Proved at READ
  --     COMMITTED and REPEATABLE READ, in both directions;
  --   * the one remaining way the window could be seen is an IN-TRANSACTION
  --     reader: a trigger that calls the managed-root guard or the selector
  --     while this owner runs.  Every non-internal trigger on
  --     `public.timesheets`, `public.timesheets_financials`,
  --     `public.contract_weeks` and `public.weekly_source_entitlement_heads`
  --     was enumerated and NONE calls
  --     `private.weekly_source_managed_root_guard_v1`,
  --     `…_guard_decision_v1` or the Gate 4 selector.  That matches open ruling
  --     OR-2, which explicitly does NOT build a trigger-level backstop on those
  --     tables.
  --
  -- WHAT WOULD BREAK IT, and therefore what must not be done without reordering
  -- first:
  --
  --   1. splitting this sequence across two transactions, or introducing an
  --      intermediate COMMIT, an autonomous transaction or a dblink/background
  --      call between any two of the writes;
  --   2. adding an `EXCEPTION` handler around any of them, which puts it in a
  --      sub-transaction that can roll back on its own;
  --   3. OPEN RULING OR-2 being decided the other way — that is, a guard
  --      trigger being attached to `public.timesheets` or
  --      `public.timesheets_financials`.  The order would then be observable
  --      INSIDE this transaction and reordering becomes MANDATORY, not merely
  --      prudent: the withdrawal marks would have to be written before the call
  --      to `public.timesheet_unauthorise_atomic` below.
  --
  -- Section 16 of this package's verifier is the executed guard on points 1 and
  -- 2: it fails if these writes stop sharing one transaction, if an exception
  -- handler appears, or if a concurrent reader can ever see a live
  -- authorisation record on an unauthorised Timesheet.
  -- ---------------------------------------------------------------------
  -- WP-24, 18 September 2026.  POINT 3 ABOVE HAS NOW HAPPENED, so the
  -- reordering this note calls MANDATORY is done here rather than left prudent.
  --
  -- Gate 13 hostile review finding F1 (CRITICAL) proved that the ORDINARY
  -- Unauthorise owner, which the Office's own `/unauthorise` route calls, left a
  -- committed entitlement head current on a managed root and paid it in place of
  -- what the Office authorised next: GBP 90.00 / 9 h paid against GBP 140.00 /
  -- 14 h authorised, executed with the real Gate 4 selector.  The fix is a
  -- BEFORE UPDATE guard trigger on `public.timesheets.authorised_at_server`
  -- (`supabase/repeatable/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql`),
  -- which HANDOVER 2 round-5 Part D authorises when it rules OR-2 CONFIRMED:
  -- "If the guard attaches to Timesheet, financial or contract-week writes, the
  -- withdrawal owner obeys the same canonical lock order as Part A3."
  --
  -- That trigger IS the in-transaction reader point 3 names, so the order below
  -- became observable INSIDE this transaction and the withdrawal marks now go
  -- FIRST.  Nothing else about this sequence changes: it is still one
  -- transaction, still has no exception handler anywhere, and the two invariants
  -- at the head of this note are both STRENGTHENED rather than weakened - the
  -- Timesheet is now never observable as unauthorised while its authorisation
  -- record is live even to an in-transaction reader, and the head is still
  -- superseded only after the Timesheet has been unauthorised.
  --
  -- It is also what ruling A3 itself describes: "marks the authorisation
  -- withdrawn" is step 2 and the head supersession is step 3, so the two halves
  -- of step 2 are now adjacent and both precede step 3.
  --
  -- `v_now` moves up with the marks it stamps.  It is read after this point by
  -- the protected-hours withdrawal, the replay receipt and the supersession, and
  -- by nothing before it.
  -- ---------------------------------------------------------------------
  v_now:=pg_catalog.clock_timestamp();

  -- Ruling A3 step 2, second half (now written FIRST): the withdrawal marks on
  -- the live root-authorisation generation and nothing else on it; the head
  -- pointer is cleared in the SAME statement.
  --
  -- Decision D8 and proof/36 section 5.6.  The check constraint
  -- `withdrawn_at_utc IS NULL OR current_entitlement_head_id IS NULL` is
  -- evaluated on the finished row, so a two-statement version fails; this one
  -- does not.
  --
  -- WP-24: this is also the STATE that tells this owner apart from an ordinary
  -- Unauthorise at the guard trigger below.  Decision D8 makes `managed` the
  -- conjunction of a LIVE generation and a currently authorised Timesheet, so
  -- once this statement has run the root is not managed and the guard permits
  -- the unauthorisation that follows.  An ordinary Unauthorise never runs this
  -- statement, so its generation is still live and it is refused.  The guard
  -- asks nothing about who is calling.
  update public.weekly_source_root_authorisations
     set withdrawn_at_utc=v_now,
         withdrawn_by_user_id=p_actor_user_id,
         current_entitlement_head_id=null
   where id=v_authorisation_id
     and withdrawn_at_utc is null;
  get diagnostics v_rows=row_count;
  if v_rows<>1 then
    raise exception 'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_FAILED',
              'root_authorisation_id',v_authorisation_id,
              'rows',v_rows)::text;
  end if;

  -- Ruling A3 step 2, first half: the UNCHANGED owner, exactly once, requiring
  -- {ok:true}.
  v_unauthorise:=public.timesheet_unauthorise_atomic(
    p_timesheet_id=>v_canonical,
    p_expected_timesheet_id=>v_canonical,
    p_actor_user_id=>p_actor_user_id,
    p_expected_row_signature=>v_signature);

  if coalesce((v_unauthorise->>'ok')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_OWNER_REFUSED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_UNAUTHORISE_OWNER_REFUSED',
              'timesheet_id',v_canonical,
              'owner_result',v_unauthorise)::text;
  end if;

  -- Ruling A3 step 4: one aligned invalidation for the pair (Candidate,
  -- canonical root) only, and success required.  HANDOVER 2 round 3 answer 10:
  -- not every physical family member; the installed normaliser expands the pair
  -- to the full physical rotation family.
  v_invalidation:=private.pay_workbench_scope_invalidate_v1(
    p_candidate_ids=>array[v_candidate_id]::uuid[],
    p_timesheet_ids=>array[v_canonical]::uuid[],
    p_reason=>'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',
    p_scope_change_tx_token=>v_token,
    p_payload_json=>pg_catalog.jsonb_build_object(
      'weekly_source_first_authorisation_withdrawal',pg_catalog.jsonb_build_object(
        'timesheet_id',v_canonical,
        'family_booking_id',v_context->>'family_booking_id',
        'authorisation_generation',v_authorisation->>'authorisation_generation',
        'superseded_head_id',v_head_id,
        'expected_row_signature',v_signature)));

  if coalesce((v_invalidation->>'ok')::boolean,false) is not true
     or (v_invalidation->>'candidate_count')::integer<>1
     or (v_invalidation->>'scope_change_tx_token')::uuid is distinct from v_token
     or coalesce((v_invalidation->>'job_inserted_count')::integer,0)
        +coalesce((v_invalidation->>'job_coalesced_count')::integer,0)<1 then
    raise exception 'WEEKLY_SOURCE_UNAUTHORISE_INVALIDATION_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_UNAUTHORISE_INVALIDATION_FAILED',
              'invalidation',v_invalidation)::text;
  end if;

  v_token_again:=public.pay_workbench_scope_change_tx_token_v1();
  if v_token_again is distinct from v_token then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED',
              'reason','TRANSACTION_TOKEN_CHANGED',
              'context','WITHDRAWAL')::text;
  end if;


  -- WP-24: ruling A3 step 2's withdrawal marks USED TO SIT HERE.  They are now
  -- written before the call to `public.timesheet_unauthorise_atomic` above, for
  -- the reason given in full at that point: the guard trigger on
  -- `public.timesheets.authorised_at_server` is the in-transaction reader that
  -- the ORDERING RULE note lists as making this reordering mandatory.  Nothing
  -- was added or removed - the same single statement, with the same three
  -- assignments and the same one-row assertion, in a different place.

  -- proof/36 section 5 step 7: the approved protected-hours decision, marked
  -- withdrawn and never deleted.
  v_protected:=private.weekly_source_first_authorisation_withdraw_protected_v1(
    v_members,p_actor_user_id,v_now);

  v_contract:=private.weekly_source_invalidation_contract_assert_v1(
    v_candidate_id,array[v_canonical]::uuid[],v_token,'WITHDRAWAL',array[]::uuid[]);


  -- ---- ruling A3 step 5: the durable replay receipt, and step 3: the head
  -- supersession that points at it.
  --
  -- The receipt id is generated first so that the recorded result and the
  -- returned result are the SAME object: an exact replay hands back exactly
  -- what this call returned, receipt id and supersession included.
  v_receipt_id:=pg_catalog.gen_random_uuid();
  v_canonical_request:=private.weekly_source_withdrawal_canonical_request_v1(
    (v_head->>'agency_id')::uuid,
    v_candidate_id,
    (v_context->>'contract_id')::uuid,
    v_canonical,
    v_context->>'family_booking_id',
    (v_context->>'canonical_version')::integer,
    p_timesheet_id,
    p_expected_timesheet_id,
    v_authorisation_id,
    (v_authorisation->>'authorisation_generation')::integer,
    v_signature,
    v_head_id,
    (v_head->>'head_revision')::bigint);
  v_digest:=private.weekly_source_publication_request_digest_v1(v_canonical_request);

  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,'withdrawn',true,'replayed',false,
    -- WP-12 handoff N2 reads the response as
    -- `{ ok, withdrawn, code, refusal_nature, refusal_message, retryable }`, so
    -- the SUCCESS shape carries those keys as explicit nulls rather than
    -- omitting them.  An absent key and a null key are different things to a
    -- browser, and the screen should never have to tell them apart.
    'code',null::text,
    'refusal_nature',null::text,
    'refusal_message',null::text,
    'retryable',false,
    'review_required',false,
    'timesheet_id',p_timesheet_id,
    'canonical_timesheet_id',v_canonical,
    'family_booking_id',v_context->>'family_booking_id',
    'timesheet_version',(v_context->>'canonical_version')::integer,
    'candidate_id',v_candidate_id,
    'expected_row_signature',v_signature,
    'root_authorisation_id',v_authorisation_id,
    'authorisation_generation',(v_authorisation->>'authorisation_generation')::integer,
    'entitlement_head_cleared',(v_authorisation->>'current_entitlement_head_id')::uuid,
    'withdrawn_at_utc',v_now,
    'withdrawn_by_user_id',p_actor_user_id,
    'scope_change_tx_token',v_token,
    'withdrawal_receipt_id',v_receipt_id,
    'request_digest',pg_catalog.encode(v_digest,'hex'),
    'head_superseded',v_head_id is not null,
    'head_supersession',case when v_head_id is null then null
      else pg_catalog.jsonb_build_object(
        'head_id',v_head_id,
        'head_revision',(v_head->>'head_revision')::bigint,
        'state_before',v_head->>'state_before',
        'state_after','SUPERSEDED',
        'superseded_reason','FIRST_AUTHORISATION_WITHDRAWN',
        'certified_zero',(v_head->>'certified_zero')::boolean,
        'component_count',(v_head->>'component_count')::integer,
        'predecessor_link_receipt_id',v_receipt_id) end,
    'invalidation',pg_catalog.jsonb_build_object(
      'ok',true,
      'candidate_count',(v_invalidation->>'candidate_count')::integer,
      'job_inserted_count',coalesce((v_invalidation->>'job_inserted_count')::integer,0),
      'job_coalesced_count',coalesce((v_invalidation->>'job_coalesced_count')::integer,0)),
    'invalidation_contract',v_contract,
    'protected_hours',v_protected,
    'unauthorise_result',pg_catalog.jsonb_build_object(
      'ok',true,
      'operation',v_unauthorise->>'operation',
      'timesheet_id',v_unauthorise->>'timesheet_id',
      'contract_week_id',v_unauthorise->>'contract_week_id',
      'processing_status',v_unauthorise->>'processing_status'),
    'checks',v_verdict->'checks',
    'census_result',v_verdict->>'census_result',
    'census_class_counts',v_verdict->'census_class_counts');

  insert into private.weekly_source_first_authorisation_withdrawal_receipts(
    id,agency_id,candidate_id,contract_id,root_timesheet_id,root_family_booking_id,
    root_timesheet_version,requested_timesheet_id,expected_timesheet_id,
    root_authorisation_id,authorisation_generation,expected_row_signature,
    request_digest,predecessor_head_id,predecessor_head_revision,
    predecessor_head_state_before,predecessor_head_certified_zero,head_superseded,
    scope_change_tx_token,withdrawn_at_utc,withdrawn_by_user_id,checks_json,result_json
  ) values (
    v_receipt_id,
    (v_head->>'agency_id')::uuid,
    v_candidate_id,
    (v_context->>'contract_id')::uuid,
    v_canonical,
    v_context->>'family_booking_id',
    (v_context->>'canonical_version')::integer,
    p_timesheet_id,
    p_expected_timesheet_id,
    v_authorisation_id,
    (v_authorisation->>'authorisation_generation')::integer,
    v_signature,
    v_digest,
    v_head_id,
    (v_head->>'head_revision')::bigint,
    case when v_head_id is null then null else v_head->>'state_before' end,
    case when v_head_id is null then null else (v_head->>'certified_zero')::boolean end,
    v_head_id is not null,
    v_token,
    v_now,
    p_actor_user_id,
    coalesce(v_verdict->'checks','[]'::jsonb),
    v_result);

  -- Ruling A3 step 3.  This is the ONLY statement in the Weekly Source
  -- withdrawal path that writes a head row, and it can only be reached when
  -- W1 to W11 have all passed, which means no Draft dependency, payment item,
  -- reservation, bank transfer, execution, provider attempt, settlement,
  -- remittance, recovery, invoice or other committed financial effect stands
  -- against the head or the root.  It sets the explicit reason and the link to
  -- the receipt whose `predecessor_head_id` is the immutable link back; both are
  -- frozen from here on by
  -- `private.weekly_source_entitlement_head_withdrawal_supersession_guard_v1`,
  -- which also makes reviving the head impossible.
  --
  -- The head's own `scope_change_tx_token` and `publication_receipt_digest` are
  -- deliberately NOT touched: they are the publication's evidence, and the
  -- deferred `weekly_source_entitlement_head_receipt_assert` re-runs on this
  -- update and would refuse if they moved.
  if v_head_id is not null then
    update public.weekly_source_entitlement_heads
       set state='SUPERSEDED',
           superseded_at_utc=v_now,
           superseded_reason='FIRST_AUTHORISATION_WITHDRAWN',
           superseded_by_withdrawal_id=v_receipt_id
     where id=v_head_id
       and state='COMMITTED_CURRENT';
    get diagnostics v_rows=row_count;
    if v_rows<>1 then
      raise exception 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_SUPERSESSION_FAILED'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_UNAUTHORISE_HEAD_SUPERSESSION_FAILED',
                'head_id',v_head_id,'rows',v_rows)::text;
    end if;

    -- Prove the post-state inside the transaction rather than asserting it in
    -- prose: no committed current head may survive for this family.
    select pg_catalog.count(*)::integer into v_rows
    from public.weekly_source_entitlement_heads as head_row
    where head_row.state='COMMITTED_CURRENT'
      and (pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(coalesce(v_context->>'family_booking_id',''))
           or head_row.root_timesheet_id=any(v_members));
    if v_rows<>0 then
      raise exception 'WEEKLY_SOURCE_UNAUTHORISE_HEAD_STILL_CURRENT'
        using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'code','WEEKLY_SOURCE_UNAUTHORISE_HEAD_STILL_CURRENT',
                'head_id',v_head_id,'remaining',v_rows)::text;
    end if;
  end if;

  -- proof/36 section 5 step 4: the withdrawal recorded through the existing
  -- audit owner as well, so the previous authorisation and the withdrawal are
  -- both visible in Audit.  The durable replay copy is the receipt above; this
  -- is the human record and no longer the replay source (WP-07b finding F4).
  perform public._audit_insert(
    'timesheets',v_canonical::text,
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWN',
    pg_catalog.jsonb_build_object(
      'authorised_at_server',v_context#>>'{root,authorised_at_server}',
      'current_entitlement_head_id',v_authorisation->>'current_entitlement_head_id',
      'authorisation_generation',(v_authorisation->>'authorisation_generation')::integer),
    pg_catalog.jsonb_build_object(
      'expected_row_signature',v_signature,
      'expected_timesheet_id',p_expected_timesheet_id,
      'requested_timesheet_id',p_timesheet_id,
      'withdrawal_receipt_id',v_receipt_id,
      'result',v_result),
    'WEEKLY_SOURCE_FIRST_AUTHORISATION_WITHDRAWAL',p_actor_user_id);

  perform private.weekly_source_observation_end_v1(v_observation_frame);
  return v_result;
end;
$function$;

CREATE OR REPLACE FUNCTION private.weekly_source_invalidation_contract_assert_v1(p_candidate_id uuid, p_declared_root_ids uuid[], p_token uuid, p_context text, p_pre_existing_job_ids uuid[] DEFAULT ARRAY[]::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_declared uuid[];
  v_jobs jsonb:='[]'::jsonb;
  v_complete integer:=0;
  v_whole_candidate integer:=0;
  v_tokens integer;
  v_failure text;
  v_job record;
  v_scope_row record;
  v_registry_row record;
  v_scope uuid[];
  v_targeted uuid[];
  v_frame uuid;
begin
  perform private.weekly_source_observation_storage_v1();
  select frame_id into v_frame from pg_temp.ws_observation_frame_v1 where singleton;
  if v_frame is null or coalesce(pg_catalog.cardinality(p_pre_existing_job_ids),0)<>0 then
    raise exception 'WEEKLY_SOURCE_OBSERVATION_REQUIRED' using errcode='55000';
  end if;
  if exists(select 1 from pg_temp.ws_observation_rows_v1
      where frame_id=v_frame and kind='TOKEN' and row_id is distinct from p_token) then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_SECOND_TOKEN' using errcode='55000';
  end if;
  if p_token is null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_TOKEN_MISSING',
              'context',p_context)::text;
  end if;

  -- The declared complete aligned scope, expanded through the installed
  -- normaliser exactly as the Workbench expands a queued job.
  select coalesce(private.weekly_source_withdrawal_uuid_array_v1(
           public._pay_workbench_normalise_timesheet_rotation_scope_payload(
             coalesce(p_declared_root_ids,array[]::uuid[]),array[]::uuid[]
           )->'family_timesheet_ids'),array[]::uuid[])
    into v_declared;

  -- The controlling token row must exist and still be open.
  select pg_catalog.count(*)::integer into v_tokens
  from public.banking_pay_scope_change_transactions as token_row
  where token_row.tx_token=p_token and token_row.state='PENDING';
  if v_tokens<>1 then
    v_failure:='CONTROLLING_TOKEN_NOT_OPEN';
  end if;

  -- The protected observer captures INSERT and UPDATE alike, including reused
  -- jobs, independently of their claimed token/Candidate. Rolled-back writes
  -- and other sessions' writes cannot enter this invocation's membership.
  for v_job in
    select job_row.id, job_row.candidate_id, job_row.job_type, job_row.dedupe_key,
           job_row.scope_change_tx_token, job_row.scope_change_generation,
           job_row.payload_json
    from pg_temp.ws_observation_rows_v1 as effect
    cross join lateral (select native.* from public.banking_pay_workbench_jobs native
      where native.id=effect.row_id offset 0) as job_row
    where effect.frame_id=v_frame and effect.kind='JOB' and job_row.status in ('QUEUED','RUNNING')
      and exists(select 1 from pg_temp.ws_observation_rows_v1 observed
        where observed.frame_id=v_frame and observed.kind='JOB'
          and observed.row_id=job_row.id)
    order by job_row.id
  loop
    -- Point 1: every invalidation produced by this operation carries the same
    -- token, whatever registered path produced it — including the
    -- CONTRACT_CLIENT_DIRTY_FANOUT and finance-case paths, which carry no
    -- Candidate at all.
    if v_job.scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'JOB_CARRIES_A_DIFFERENT_TOKEN');
    end if;

    v_targeted:=private.weekly_source_withdrawal_uuid_array_v1(
      v_job.payload_json->'targeted_timesheet_ids');

    select coalesce(private.weekly_source_withdrawal_uuid_array_v1(
             public._pay_workbench_normalise_timesheet_rotation_scope_payload(
               v_targeted,
               private.weekly_source_withdrawal_uuid_array_v1(
                 v_job.payload_json->'linked_timesheet_ids')
             )->'family_timesheet_ids'),array[]::uuid[])
      into v_scope;

    -- Point 5 is a rule about CANDIDATE jobs: a job queued for a different
    -- Candidate, or naming a root outside the declared aligned scope, is a
    -- contract failure.  A job with no Candidate is a registered non-Candidate
    -- fanout path; it is recorded and its token is still required to match.
    if v_job.candidate_id is not null then
      if v_job.candidate_id is distinct from p_candidate_id then
        v_failure:=coalesce(v_failure,'JOB_FOR_A_DIFFERENT_CANDIDATE');
      end if;

      if exists (select 1 from pg_catalog.unnest(v_scope) as scope_id(value)
                 where not (scope_id.value=any(v_declared))) then
        v_failure:=coalesce(v_failure,'JOB_SCOPE_OUTSIDE_THE_DECLARED_SCOPE');
      end if;

      if coalesce(pg_catalog.cardinality(v_targeted),0)=0 then
        -- An empty target list is the Workbench's whole-Candidate job.  It
        -- cannot name a root outside the Candidate and it can only widen the
        -- refresh, never narrow it, so it is neither a subset violation nor the
        -- declared complete-scope job.  It is counted and reported separately.
        v_whole_candidate:=v_whole_candidate+1;
      elsif coalesce(pg_catalog.cardinality(v_scope),0)
            =coalesce(pg_catalog.cardinality(v_declared),0)
        and not exists (select 1 from pg_catalog.unnest(v_declared) as declared_id(value)
                        where not (declared_id.value=any(v_scope))) then
        v_complete:=v_complete+1;
      end if;
    end if;

    v_jobs:=v_jobs||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'job_id',v_job.id,
      'candidate_id',v_job.candidate_id,
      'job_type',v_job.job_type,
      'dedupe_key',v_job.dedupe_key,
      'scope_change_tx_token',v_job.scope_change_tx_token,
      'scope_change_generation',v_job.scope_change_generation,
      'targeted_timesheet_ids',pg_catalog.to_jsonb(v_targeted),
      'normalised_scope',pg_catalog.to_jsonb(v_scope),
      'whole_candidate',v_job.candidate_id is not null
        and coalesce(pg_catalog.cardinality(v_targeted),0)=0));
  end loop;

  -- Scope-only invalidation is legitimate (enqueue may be coalesced/skipped),
  -- but every captured row must still belong to this operation's declared
  -- Candidate, token and aligned family. Drive native PK probes from the frame,
  -- retaining missing/deleted identities as failures, never as filtered evidence.
  for v_scope_row in
    select effect.row_id,effect.deleted,scope_state_row.timesheet_id,
           scope_state_row.candidate_id,scope_state_row.last_scope_change_tx_token
    from pg_temp.ws_observation_rows_v1 as effect
    left join lateral (
      select native.* from private.banking_pay_workbench_timesheet_scope_state native
      where native.timesheet_id=effect.row_id offset 0
    ) as scope_state_row on true
    where effect.frame_id=v_frame and effect.kind='SCOPE'
    order by effect.row_id
  loop
    if v_scope_row.deleted or v_scope_row.timesheet_id is null then
      v_failure:=coalesce(v_failure,'SCOPE_ROW_MISSING_OR_DELETED');
    elsif v_scope_row.candidate_id is distinct from p_candidate_id then
      v_failure:=coalesce(v_failure,'SCOPE_FOR_A_DIFFERENT_CANDIDATE');
    elsif v_scope_row.last_scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'SCOPE_CARRIES_A_DIFFERENT_TOKEN');
    elsif not (v_scope_row.timesheet_id=any(v_declared)) then
      v_failure:=coalesce(v_failure,'SCOPE_OUTSIDE_THE_DECLARED_SCOPE');
    end if;
  end loop;

  -- Candidate-only native invalidation must not escape a Timesheet-only proof.
  -- Capture is independent of claimed Candidate/token; only current-frame keys
  -- drive these native PK probes. Deletion is sticky, including delete/reinsert.
  for v_registry_row in
    select effect.row_id,effect.deleted,registry_row.candidate_id,
           registry_row.last_scope_change_tx_token
    from pg_temp.ws_observation_rows_v1 as effect
    left join lateral (
      select native.candidate_id,native.last_scope_change_tx_token
      from private.banking_pay_workbench_candidate_scope_registry native
      where native.candidate_id=effect.row_id offset 0
    ) as registry_row on true
    where effect.frame_id=v_frame and effect.kind='REGISTRY'
    order by effect.row_id
  loop
    if v_registry_row.deleted or v_registry_row.candidate_id is null then
      v_failure:=coalesce(v_failure,'REGISTRY_ROW_MISSING_OR_DELETED');
    elsif v_registry_row.candidate_id is distinct from p_candidate_id then
      v_failure:=coalesce(v_failure,'REGISTRY_FOR_A_DIFFERENT_CANDIDATE');
    elsif v_registry_row.last_scope_change_tx_token is distinct from p_token then
      v_failure:=coalesce(v_failure,'REGISTRY_CARRIES_A_DIFFERENT_TOKEN');
    end if;
  end loop;

  -- Point 6: exactly one effective complete-scope dirty result per Candidate.
  if v_complete<>1 then
    v_failure:=coalesce(v_failure,
      case when v_complete=0 then 'NO_COMPLETE_SCOPE_JOB'
           else 'MORE_THAN_ONE_COMPLETE_SCOPE_JOB' end);
  end if;

  if v_failure is not null then
    raise exception 'WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'code','WEEKLY_SOURCE_INVALIDATION_CONTRACT_FAILED',
              'reason',v_failure,
              'context',p_context,
              'scope_change_tx_token',p_token,
              'declared_scope',pg_catalog.to_jsonb(v_declared),
              'jobs',v_jobs)::text;
  end if;

  perform private.weekly_source_observation_seal_v1();
  return pg_catalog.jsonb_build_object(
    'ok',true,
    'scope_change_tx_token',p_token,
    'declared_scope',pg_catalog.to_jsonb(v_declared),
    'complete_scope_job_count',v_complete,
    'whole_candidate_job_count',v_whole_candidate,
    'jobs',v_jobs);
end;
$function$;

CREATE OR REPLACE FUNCTION private.weekly_source_entitlement_head_inventory_assert_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_head_id uuid;
  v_component_count integer;
  v_certified_zero boolean;
  v_actual integer;
begin
  if tg_relid='public.weekly_source_entitlement_heads'::pg_catalog.regclass then
    v_head_id:=new.id;
  else
    v_head_id:=new.head_id;
  end if;

  perform private.weekly_source_observation_storage_v1();
  if exists(select 1 from pg_temp.ws_inventory_checked_v1 where head_id=v_head_id) then
    return null;
  end if;
  select head_row.component_count,head_row.certified_zero
    into v_component_count,v_certified_zero
  from public.weekly_source_entitlement_heads head_row
  where head_row.id=v_head_id for no key update;
  if not found then
    return null;
  end if;

  select pg_catalog.count(*) into v_actual
  from public.weekly_source_entitlement_head_components component_row
  where component_row.head_id=v_head_id;

  if v_actual<>v_component_count or v_certified_zero<>(v_actual=0) then
    raise exception 'WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH',
              'head_id',v_head_id,
              'declared_component_count',v_component_count,
              'declared_certified_zero',v_certified_zero,
              'actual_component_count',v_actual
            )::text;
  end if;
  insert into pg_temp.ws_inventory_checked_v1(head_id) values(v_head_id)
    on conflict(head_id) do nothing;
  return null;
end;
$function$;
-- END COMPLETE OWNER REPLACEMENTS

alter function private.weekly_source_observation_storage_v1() owner to postgres;
alter function private.weekly_source_observation_begin_v1() owner to postgres;
alter function private.weekly_source_observation_seal_v1() owner to postgres;
alter function private.weekly_source_observation_end_v1(uuid) owner to postgres;
alter function private.weekly_source_observe_effect_v1() owner to postgres;
alter function private.weekly_source_inventory_changed_v1() owner to postgres;
revoke all on function private.weekly_source_observation_storage_v1(),
 private.weekly_source_observation_begin_v1(),private.weekly_source_observation_end_v1(uuid),
 private.weekly_source_observation_seal_v1(),
 private.weekly_source_observe_effect_v1(),private.weekly_source_inventory_changed_v1()
 from public,anon,authenticated,service_role;

commit;
