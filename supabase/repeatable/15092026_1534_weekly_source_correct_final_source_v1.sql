-- Repeatable CloudTMS authority: weekly_source_correct_final_source_v1
-- Replaces one same-cycle final source only while every affected ordinary
-- Weekly root is still outside invoice and Banking/Draft ownership.  Prior
-- source evidence is immutable; the old authority is superseded, never erased.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_correct_final_preconditions_v1(
  p_prior_final_revision_id uuid,
  p_actor_user_id uuid,
  p_excluding_correction_session_id uuid,
  p_lock_roots boolean
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
  v_root record;
  v_preflight jsonb;
  v_roots jsonb;
  v_family uuid[];
  v_family_after_lock uuid[];
begin
  if p_prior_final_revision_id is null or p_actor_user_id is null then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PRECONDITION_INPUT_INVALID'
      using errcode='22023';
  end if;

  select * into v_revision
  from public.weekly_source_final_revisions
  where id=p_prior_final_revision_id;
  if not found or v_revision.state<>'CURRENT' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE'
      using errcode='40001';
  end if;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_revision.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_revision.upload_id;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;

  select * into v_manifest
  from public.weekly_source_client_manifests
  where final_revision_id=v_revision.id;
  if not found or exists(
    select 1 from public.weekly_source_client_manifests other_manifest
    where other_manifest.final_revision_id=v_revision.id
      and other_manifest.id<>v_manifest.id
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_CLIENT_SCOPE_INVALID'
      using errcode='55000';
  end if;

  if exists(
    select 1
    from public.weekly_source_final_revisions later_revision
    join public.weekly_source_cycles later_cycle
      on later_cycle.id=later_revision.source_cycle_id
    join public.weekly_source_client_manifests later_manifest
      on later_manifest.final_revision_id=later_revision.id
     and later_manifest.client_id=v_manifest.client_id
    join public.weekly_source_uploads later_upload on later_upload.id=later_revision.upload_id
    join public.weekly_source_format_profiles later_profile
      on later_profile.id=later_upload.source_format_profile_id
    where later_revision.state='CURRENT'
      and later_cycle.source_group_id=v_group.id
      and later_cycle.finalisation_week_ending>v_cycle.finalisation_week_ending
      and later_profile.final_authority_kind=v_profile.final_authority_kind
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_LATER_FINAL_CYCLE_EXISTS'
      using errcode='55000';
  end if;

  if exists(
    select 1 from public.weekly_source_final_revisions descendant
    where descendant.predecessor_revision_id=v_revision.id
      and not exists(
        select 1
        from public.weekly_final_source_correction_sessions own_correction
        where own_correction.id=p_excluding_correction_session_id
          and own_correction.expected_current_final_revision_id=v_revision.id
          and own_correction.prepared_final_revision_id=descendant.id
          and descendant.state='PREPARED'
      )
  ) or exists(
    select 1 from public.weekly_final_source_correction_sessions correction
    where correction.expected_current_final_revision_id=v_revision.id
      and correction.id is distinct from p_excluding_correction_session_id
      and correction.state not in ('CANCELLED','FAILED')
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS'
      using errcode='55000';
  end if;

  if exists(
    select 1
    from public.weekly_exceptional_pay_target_events target_event
    where target_event.triggering_final_revision_id=v_revision.id
  ) or exists(
    select 1
    from public.weekly_exceptional_pending_reconciliation_targets pending_target
    where pending_target.current_final_revision_id=v_revision.id
      and pending_target.state='ACTIVE'
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS'
      using errcode='55000';
  end if;

  if exists(
    select 1
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    where movement.final_revision_id=v_revision.id
  ) or exists(
    select 1
    from public.weekly_source_invoice_placements placement
    join public.weekly_source_billing_movements movement
      on movement.id=placement.billing_movement_id
    where movement.final_revision_id=v_revision.id
      and placement.placement_state='PLACED'
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS'
      using errcode='55000';
  end if;

  if exists(
    select 1 from public.invoice_operations operation
    where operation.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and operation.operation_type in ('GENERATE_INVOICES','BUILD_DOCUMENT','ISSUE_INVOICES')
      and (
        (operation.entity_type='CLIENT' and operation.entity_id=v_manifest.client_id)
        or operation.input_json->>'client_id'=v_manifest.client_id::text
        or operation.input_json->'client_ids' @> pg_catalog.jsonb_build_array(v_manifest.client_id::text)
      )
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_ACTIVE_INVOICE_OPERATION'
      using errcode='55000';
  end if;

  for v_receipt in
    select receipt.*
    from public.weekly_source_ordinary_pay_projection_receipts receipt
    where receipt.final_revision_id=v_revision.id
      and receipt.outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED')
    order by receipt.root_timesheet_id
  loop
    -- Gate 13 hostile review F5 / standing rule 3.  These preconditions guard
    -- one ordinary Weekly root against protected-pay ownership and against an
    -- existing invoice line.  After S8 the physical root id is not a family
    -- identity, so a protected family or an invoice line recorded against a
    -- rotated sibling was invisible here and the correction proceeded.
    --
    -- The lock is taken BEFORE the guarded state is read.  The family has to be
    -- named before its rows can be locked, so the identity is resolved, its
    -- rows are locked in a deterministic order, and the identity is then
    -- re-resolved and required to be unchanged (the same shape as the
    -- lock-and-resolve helper's post-lock re-read).  A membership change across
    -- the lock fails closed.
    v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
      v_receipt.root_timesheet_id
    );
    if v_family is null or pg_catalog.cardinality(v_family)=0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_FAMILY_UNRESOLVED'
        using errcode='55000';
    end if;
    if coalesce(p_lock_roots,true) then
      perform 1 from public.timesheets family_row
      where family_row.timesheet_id=any(v_family)
      order by family_row.timesheet_id
      for update;
      v_family_after_lock:=private.weekly_source_invoice_family_timesheet_ids_v1(
        v_receipt.root_timesheet_id
      );
      if v_family_after_lock is null
         or pg_catalog.cardinality(v_family_after_lock)=0
         or not (v_family_after_lock @> v_family and v_family @> v_family_after_lock)
      then
        raise exception 'WEEKLY_SOURCE_CORRECTION_FAMILY_UNRESOLVED'
          using errcode='40001';
      end if;
      v_family:=v_family_after_lock;
    end if;
    if exists(
      select 1 from public.weekly_exceptional_pay_target_families family
      where family.root_timesheet_id=any(v_family)
        and family.ownership_state='TARGET_MANAGED'
    ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PROTECTED_TARGET_EXISTS'
        using errcode='55000';
    end if;
    if exists(
      select 1 from public.invoice_lines invoice_line
      where invoice_line.timesheet_id=any(v_family)
    ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS'
        using errcode='55000';
    end if;
    if exists(
      select 1 from public.timesheets timesheet_row
      where timesheet_row.timesheet_id=v_receipt.root_timesheet_id
        and timesheet_row.active_document_operation_id is not null
    ) or exists(
      select 1 from public.invoice_operations operation
      where operation.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and operation.entity_type='TIMESHEET'
        and operation.entity_id=v_receipt.root_timesheet_id
    ) or exists(
      select 1 from public.invoice_operation_chunks chunk
      where chunk.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and chunk.entity_type='TIMESHEET'
        and chunk.entity_id=v_receipt.root_timesheet_id
    ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ACTIVE_INVOICE_OPERATION'
        using errcode='55000';
    end if;

    v_preflight:=public.import_timesheet_financial_preflight_v1(
      array[v_receipt.root_timesheet_id],
      'WEEKLY_SOURCE_CORRECT_FINAL_SOURCE',p_actor_user_id,'{}'::jsonb,
      coalesce(p_lock_roots,true),1
    );
    if coalesce((v_preflight->>'blocking_batch_count')::integer,0)>0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ACTIVE_PAY_DRAFT'
        using errcode='55000',detail=v_preflight::text;
    elsif coalesce((v_preflight->>'paid_count')::integer,0)>0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ALREADY_PAID'
        using errcode='55000',detail=v_preflight::text;
    elsif coalesce((v_preflight->>'invoice_lined_count')::integer,0)>0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS'
        using errcode='55000',detail=v_preflight::text;
    elsif coalesce((v_preflight->>'allowed')::boolean,false) is not true
       or v_preflight->>'required_path' not in (
         -- Gate 2 / XSG-032.  UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE is
         -- deliberately NOT admitted: Correct Final Source stays the narrow
         -- same-cycle correction for a root that is not yet authorised.  An
         -- already-authorised root always goes to proposal and Office decision.
         'DIRECT_AMEND_RECALCULATE'
       ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ROOT_NOT_MUTABLE'
        using errcode='55000',detail=v_preflight::text;
    end if;
  end loop;

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'root_timesheet_id',receipt.root_timesheet_id,
    'prior_projection_receipt_id',receipt.id,
    'client_id',receipt.client_id,
    'source_profile_kind',receipt.source_profile_kind,
    'source_mode',receipt.source_mode,
    'root_after_hash',pg_catalog.encode(receipt.root_after_hash,'hex')
  ) order by receipt.root_timesheet_id),'[]'::jsonb)
  into v_roots
  from public.weekly_source_ordinary_pay_projection_receipts receipt
  where receipt.final_revision_id=v_revision.id
    and receipt.outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED');

  return pg_catalog.jsonb_build_object(
    'prior_final_revision_id',v_revision.id,
    'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
    'authority_scope_kind',v_revision.authority_scope_kind,
    'report_scope_id',v_revision.report_scope_id,
    'client_id',v_manifest.client_id,
    'source_profile_kind',v_profile.final_authority_kind,
    'source_mode',case when v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
      then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end,
    'final_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
    'affected_roots',v_roots
  );
end;
$function$;

create or replace function public.weekly_source_correct_final_open_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','authority_scope_kind','expected_current_final_revision_id',
    'expected_final_manifest_hash','idempotency_key','reason','report_scope_id',
    'schema_version','source_cycle_id'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_cycle_id uuid;
  v_scope_kind text;
  v_report_scope_id uuid;
  v_revision_id uuid;
  v_expected_manifest bytea;
  v_idempotency_key text;
  v_reason text;
  v_request_hash bytea;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_existing public.weekly_final_source_correction_sessions%rowtype;
  v_existing_by_key public.weekly_final_source_correction_sessions%rowtype;
  v_existing_by_hash public.weekly_final_source_correction_sessions%rowtype;
  v_existing_key_count bigint;
  v_existing_hash_count bigint;
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_guard jsonb;
  v_group_id uuid;
  v_client_id uuid;
  v_guard_fingerprint bytea;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_scope_kind:=pg_catalog.upper(pg_catalog.btrim(p_request->>'authority_scope_kind'));
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_revision_id:=(p_request->>'expected_current_final_revision_id')::uuid;
    v_expected_manifest:=pg_catalog.decode(p_request->>'expected_final_manifest_hash','hex');
  exception when others then
    raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_VALUE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  if v_actor is null or v_cycle_id is null or v_revision_id is null
     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE')
     or (v_scope_kind='NHSP_REPORT_SCOPE') is distinct from (v_report_scope_id is not null)
     or pg_catalog.octet_length(v_expected_manifest)<>32
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200
     or pg_catalog.char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_VALUE_INVALID' using errcode='22023';
  end if;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',p_request-'idempotency_key'
  );
  -- WP-59.  Two defects lived in this one replay lookup.
  --
  -- (1) It chose between an idempotency-key match and a request-hash match with
  --     `order by ... limit 1`.  That is a safety decision taken by an
  --     ordering, which this programme forbids.  Both candidates are now read
  --     explicitly, each with its own cardinality check, and a disagreement
  --     between them fails closed instead of being silently ranked away.
  --
  -- (2) It matched a session in ANY state.  Before WP-59 that was harmless
  --     because no session could ever leave the live set.  Now that
  --     public.weekly_source_correct_final_cancel_atomic_v1 exists, re-sending
  --     the request that opened a since-cancelled session would have returned
  --     that cancelled session as an idempotent replay -- telling Office it
  --     still has a correction session when it has none.  A terminal session is
  --     never replayed as if it were live; the caller is told so in a typed
  --     refusal and must open the new correction with its own key.
  select pg_catalog.count(*) into v_existing_key_count
  from public.weekly_final_source_correction_sessions correction
  where correction.actor_user_id=v_actor
    and correction.idempotency_key=v_idempotency_key;
  select pg_catalog.count(*) into v_existing_hash_count
  from public.weekly_final_source_correction_sessions correction
  where correction.request_hash=v_request_hash;
  if v_existing_key_count>1 or v_existing_hash_count>1 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_IDEMPOTENCY_AMBIGUOUS'
      using errcode='55000',
        detail='key_matches='||v_existing_key_count::text
               ||' request_hash_matches='||v_existing_hash_count::text;
  end if;
  if v_existing_key_count=1 then
    select * into v_existing_by_key
    from public.weekly_final_source_correction_sessions correction
    where correction.actor_user_id=v_actor
      and correction.idempotency_key=v_idempotency_key;
  end if;
  if v_existing_hash_count=1 then
    select * into v_existing_by_hash
    from public.weekly_final_source_correction_sessions correction
    where correction.request_hash=v_request_hash;
  end if;
  if v_existing_key_count=1 and v_existing_hash_count=1
     and v_existing_by_key.id is distinct from v_existing_by_hash.id then
    raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_IDEMPOTENCY_COLLISION'
      using errcode='22023';
  end if;
  if v_existing_key_count=1 then
    v_existing:=v_existing_by_key;
  elsif v_existing_hash_count=1 then
    v_existing:=v_existing_by_hash;
  end if;
  if v_existing_key_count=1 or v_existing_hash_count=1 then
    if v_existing.request_hash is distinct from v_request_hash then
      raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    if v_existing.state in ('CANCELLED','FAILED') then
      raise exception 'WEEKLY_SOURCE_CORRECTION_OPEN_SESSION_TERMINAL'
        using errcode='22023',
          detail='correction session '||v_existing.id::text||' is '
                 ||v_existing.state||'. Open the new correction with its own '
                 'idempotency key and its own reason; a terminal session is '
                 'never replayed as a live one.';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_existing.state,'correction_session_id',v_existing.id,
      'version',v_existing.version,'idempotent_replay',true
    );
  end if;

  select * into v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  select source_group_id into v_group_id
  from public.weekly_source_cycles where id=v_cycle.id;
  if v_scope_kind='NHSP_REPORT_SCOPE' then
    select * into v_scope from public.weekly_source_report_scopes
    where id=v_report_scope_id for update;
    if not found or v_scope.source_cycle_id is distinct from v_cycle.id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SCOPE_INVALID' using errcode='22023';
    end if;
    v_client_id:=v_scope.client_id;
    if v_scope.current_final_revision_id is distinct from v_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' using errcode='40001';
    end if;
  elsif v_cycle.current_final_revision_id is distinct from v_revision_id then
    raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' using errcode='40001';
  end if;
  select * into v_revision from public.weekly_source_final_revisions
  where id=v_revision_id for update;
  if not found or v_revision.state<>'CURRENT'
     or v_revision.source_cycle_id is distinct from v_cycle.id
     or v_revision.authority_scope_kind is distinct from v_scope_kind
     or v_revision.report_scope_id is distinct from v_report_scope_id
     or v_revision.manifest_hash is distinct from v_expected_manifest then
    raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' using errcode='40001';
  end if;
  if v_client_id is null then
    select client_id into strict v_client_id
    from public.weekly_source_client_manifests where final_revision_id=v_revision.id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'CORRECT_FINAL_SOURCE',v_group_id,v_client_id,v_cycle.finalisation_week_ending
  );
  v_guard:=private.weekly_source_correct_final_preconditions_v1(
    v_revision.id,v_actor,null::uuid,false
  );
  v_guard_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_GUARD_V1',v_guard
  );
  insert into public.weekly_final_source_correction_sessions(
    source_cycle_id,authority_scope_kind,report_scope_id,
    expected_current_final_revision_id,expected_final_manifest_hash,state,version,
    actor_user_id,reason,idempotency_key,request_hash,guard_fingerprint
  ) values (
    v_cycle.id,v_scope_kind,v_report_scope_id,v_revision.id,v_revision.manifest_hash,
    'DRAFT',1,v_actor,v_reason,v_idempotency_key,v_request_hash,v_guard_fingerprint
  ) returning * into v_session;
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'weekly_final_source_correction_sessions',v_session.id::text,
         'WEEKLY_SOURCE_CORRECT_FINAL_OPENED',
         pg_catalog.jsonb_build_object(
           'current_final_revision_id',v_revision.id,
           'current_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex')
         ),
         pg_catalog.jsonb_build_object(
           'state','DRAFT','source_cycle_id',v_cycle.id,
           'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
           'affected_root_count',pg_catalog.jsonb_array_length(v_guard->'affected_roots'),
           'guard_fingerprint',pg_catalog.encode(v_guard_fingerprint,'hex')
         ),v_reason
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','DRAFT','correction_session_id',v_session.id,
    'version',v_session.version,'affected_root_count',
      pg_catalog.jsonb_array_length(v_guard->'affected_roots'),
    'guard_fingerprint',pg_catalog.encode(v_guard_fingerprint,'hex'),
    'idempotent_replay',false
  );
end;
$function$;

create or replace function private.weekly_source_correct_final_prepared_context_v1(
  p_correction_session_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
  v_root record;
  v_source_mode text;
  v_source_units jsonb;
  v_segments jsonb;
  v_expenses jsonb;
  v_actual jsonb;
  v_rate_refs jsonb;
  v_context jsonb;
  v_contexts jsonb:='[]'::jsonb;
  v_source_unit_hash bytea;
  v_source_expense_hash bytea;
  v_active_segment_hash bytea;
begin
  select * into v_session
  from public.weekly_final_source_correction_sessions
  where id=p_correction_session_id;
  if not found or v_session.state not in ('PREPARING','PREPARED','COMMITTING')
     or v_session.prepared_final_revision_id is null then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARED_CONTEXT_UNAVAILABLE'
      using errcode='55000';
  end if;
  select * into strict v_revision
  from public.weekly_source_final_revisions
  where id=v_session.prepared_final_revision_id;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_revision.source_cycle_id;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_revision.upload_id;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  select * into v_manifest
  from public.weekly_source_client_manifests
  where final_revision_id=v_revision.id;
  if not found or exists(
    select 1 from public.weekly_source_client_manifests other_manifest
    where other_manifest.final_revision_id=v_revision.id
      and other_manifest.id<>v_manifest.id
  ) or v_revision.state<>'PREPARED'
     or v_revision.reason<>'CORRECT_FINAL_SOURCE'
     or v_revision.predecessor_revision_id
          is distinct from v_session.expected_current_final_revision_id
     or v_upload.id is distinct from v_session.replacement_correction_upload_id
     or v_upload.state<>'CORRECTION_READY' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARED_CONTEXT_INVALID'
      using errcode='55000';
  end if;
  v_source_mode:=case when v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
    then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end;

  -- Rebuild the complete union, not merely roots published by the mistaken
  -- authority.  A valid complete replacement may add a root or remove the
  -- last movement from an old root; both need one exact ordinary projection.
  for v_root in
    with prior_roots as (
      select receipt.root_timesheet_id,receipt.id as prior_projection_receipt_id
      from public.weekly_source_ordinary_pay_projection_receipts receipt
      where receipt.final_revision_id=v_session.expected_current_final_revision_id
        and receipt.outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED')
    ), replacement_roots as (
      select distinct movement.invoice_timesheet_id as root_timesheet_id
      from public.weekly_source_billing_movements movement
      where movement.final_revision_id=v_revision.id
    )
    select coalesce(replacement.root_timesheet_id,prior.root_timesheet_id)
             as root_timesheet_id,
           prior.prior_projection_receipt_id
    from prior_roots prior
    full join replacement_roots replacement using(root_timesheet_id)
    order by coalesce(replacement.root_timesheet_id,prior.root_timesheet_id)
  loop
    v_source_units:=private.weekly_source_ordinary_projection_source_units_v1(
      v_revision.id,v_root.root_timesheet_id
    );
    v_segments:=private.weekly_source_ordinary_projection_current_segments_v1(
      v_root.root_timesheet_id,v_revision.id
    );
    v_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(
      v_root.root_timesheet_id,v_revision.id
    );
    v_actual:=private.weekly_source_ordinary_projection_actual_schedule_v1(v_segments);
    v_source_unit_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ORDINARY_UNIT_MANIFEST_V1',v_source_units
    );
    v_source_expense_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',v_expenses
    );
    v_active_segment_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ORDINARY_ACTIVE_SEGMENTS_V1',v_segments
    );
    v_rate_refs:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
      'source_mode',v_source_mode,'root_timesheet_id',v_root.root_timesheet_id,
      'final_revision_id',v_revision.id,
      'final_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
      'final_policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex'),
      'client_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex'),
      'source_unit_manifest_hash',pg_catalog.encode(v_source_unit_hash,'hex'),
      'source_expense_manifest_hash',pg_catalog.encode(v_source_expense_hash,'hex'),
      'active_segment_manifest_hash',pg_catalog.encode(v_active_segment_hash,'hex')
    );
    v_context:=pg_catalog.jsonb_build_object(
      'root_timesheet_id',v_root.root_timesheet_id,
      'prior_projection_receipt_id',v_root.prior_projection_receipt_id,
      'client_id',v_manifest.client_id,
      'source_profile_kind',v_profile.final_authority_kind,
      'source_mode',v_source_mode,
      'source_units',v_source_units,
      'expected_segments',v_segments,
      'expected_actual_schedule',v_actual,
      'expected_source_expenses',v_expenses,
      'expected_rate_source_refs',v_rate_refs,
      'root_state_hash',pg_catalog.encode(
         private.weekly_source_ordinary_projection_root_hash_v1(
          v_root.root_timesheet_id
        ),'hex'
      )
    );
    v_contexts:=v_contexts||pg_catalog.jsonb_build_array(
      v_context||pg_catalog.jsonb_build_object(
        'prepared_context_hash',pg_catalog.encode(
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_CORRECT_FINAL_PREPARED_ROOT_CONTEXT_V1',v_context
          ),'hex'
        )
      )
    );
  end loop;

  return pg_catalog.jsonb_build_object(
    'correction_session_id',v_session.id,
    'prepared_final_revision_id',v_revision.id,
    'prior_final_revision_id',v_session.expected_current_final_revision_id,
    'source_cycle_id',v_cycle.id,
    'client_id',v_manifest.client_id,
    'source_profile_kind',v_profile.final_authority_kind,
    'source_mode',v_source_mode,
    'root_contexts',v_contexts
  );
end;
$function$;

-- Build the exact Office-facing review from server-owned staged data.  The
-- result deliberately contains business wording only.  REVIEW seals its hash
-- before any inactive final revision, Timesheet or lineage row is built;
-- PREPARE and APPLY then prove Office confirmed that same immutable review.
create or replace function private.weekly_source_correct_final_office_preview_v1(
  p_correction_session_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_prior_revision public.weekly_source_final_revisions%rowtype;
  v_prior_profile public.weekly_source_format_profiles%rowtype;
  v_changes jsonb:='[]'::jsonb;
  v_blockers jsonb:='[]'::jsonb;
  v_confirmation text;
  v_old_report text;
  v_preview jsonb;
begin
  select * into v_session
  from public.weekly_final_source_correction_sessions
  where id=p_correction_session_id;
  if not found or v_session.state not in ('READY','REVIEWED','PREPARING','PREPARED','COMMITTING','APPLIED')
     or v_session.replacement_correction_upload_id is null
     or v_session.replacement_projection_publication_id is null then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREVIEW_NOT_READY' using errcode='55000';
  end if;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=v_session.replacement_projection_publication_id;
  select * into strict v_prior_revision
  from public.weekly_source_final_revisions
  where id=v_session.expected_current_final_revision_id;
  select profile.* into strict v_prior_profile
  from public.weekly_source_uploads upload_row
  join public.weekly_source_format_profiles profile
    on profile.id=upload_row.source_format_profile_id
  where upload_row.id=v_prior_revision.upload_id;

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'candidate',coalesce(candidate.display_name,source_row.source_candidate_identity,'Candidate'),
    'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
    'problem',case
      when resolution.id is null then 'This row has not been linked yet'
      when resolution.mapping_state='CANDIDATE_NOT_FOUND' then 'Candidate could not be found'
      when resolution.mapping_state='CLIENT_NOT_FOUND' then 'Client could not be found'
      when resolution.mapping_state='NO_ELIGIBLE_CONTRACT' then 'No matching contract'
      when resolution.mapping_state in ('AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED')
        then 'More than one contract matches'
      when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
      when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
      else 'Check the hours for this shift' end,
    'action',case
      when resolution.mapping_state='CANDIDATE_NOT_FOUND' then 'Link candidate'
      when resolution.mapping_state='CLIENT_NOT_FOUND' then 'Link client'
      when resolution.mapping_state in ('NO_ELIGIBLE_CONTRACT','AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED')
        then 'Choose contract'
      when charge.phase_severity='FINALISATION_BLOCKER' then 'Open charge details'
      else 'View details' end
  ) order by source_row.work_date,source_row.source_candidate_identity,source_row.source_row_ordinal),'[]'::jsonb)
  into v_blockers
  from public.weekly_source_upload_rows source_row
  left join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id
   and resolution.generation=coalesce(
     v_publication.projection_generation,
     v_publication.authority_scope_version::integer
   )
  left join public.candidates candidate on candidate.id=resolution.candidate_id
  left join public.weekly_source_charge_checks charge
    on charge.upload_row_id=source_row.id
   and charge.row_resolution_id=resolution.id
   and charge.generation=coalesce(
     v_publication.projection_generation,
     v_publication.authority_scope_version::integer
   )
  where source_row.upload_id=v_session.replacement_correction_upload_id
    and (
      resolution.id is null or resolution.mapping_state<>'RESOLVED'
      or source_row.row_finalisation_state in (
        'SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE'
      )
      or charge.phase_severity='FINALISATION_BLOCKER'
    );

  if pg_catalog.jsonb_array_length(v_blockers)=0 then
    if v_prior_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT' then
      with prior_rows as (
        select movement.work_event_id,movement.source_line_kind,movement.candidate_id,
               movement.source_facts_json prior_facts,
               -- WP-54.  The money this line currently puts on the self-bill.
               -- Pack 14 section 4.3.1: for NHSP "The validated source total is
               -- the invoice value", and that is what this column holds.
               movement.invoice_presentation_charge_pence prior_charge_pence,
               pg_catalog.row_number() over(
                 partition by movement.work_event_id,movement.source_line_kind
                 order by source_row.source_row_ordinal,movement.id
               ) as row_number
        from public.weekly_source_billing_movements movement
        join public.weekly_source_upload_rows source_row
          on source_row.id=movement.nhsp_upload_row_id
        where movement.final_revision_id=v_session.expected_current_final_revision_id
      ), replacement_rows as (
        select resolution.work_event_id,
               case when economic.row_sign=1 then 'NHSP_PHYSICAL_POSITIVE'
                    else 'NHSP_PHYSICAL_FULL_NEGATIVE' end source_line_kind,
               resolution.candidate_id,
               pg_catalog.jsonb_build_object(
                 'work_date',source_row.work_date,
                 'start_at_local',source_row.start_at_local,
                 'end_at_local',source_row.end_at_local,
                 'break_minutes',source_row.break_minutes
               ) replacement_facts,
               -- WP-54.  The money the replacement line would put on the
               -- self-bill: the signed Commission + Total Cost the replacement
               -- report supplies, which pack 14 section 4.3.1 makes the invoice
               -- value once the server's own comparison has passed.
               source_row.source_shift_charge_pence replacement_charge_pence,
               pg_catalog.row_number() over(
                 partition by resolution.work_event_id,
                   case when economic.row_sign=1 then 'NHSP_PHYSICAL_POSITIVE'
                        else 'NHSP_PHYSICAL_FULL_NEGATIVE' end
                 order by source_row.source_row_ordinal,resolution.id
               ) as row_number
        from public.weekly_source_upload_rows source_row
        join public.weekly_source_row_resolutions resolution
          on resolution.upload_row_id=source_row.id
         and resolution.generation=coalesce(
           v_publication.projection_generation,
           v_publication.authority_scope_version::integer
         )
         and resolution.mapping_state='RESOLVED'
        join public.weekly_source_row_economic_snapshots economic
          on economic.row_resolution_id=resolution.id
        where source_row.upload_id=v_session.replacement_correction_upload_id
      ), paired as (
        select coalesce(replacement.work_event_id,prior.work_event_id) work_event_id,
               coalesce(replacement.source_line_kind,prior.source_line_kind) source_line_kind,
               coalesce(replacement.candidate_id,prior.candidate_id) candidate_id,
               prior.prior_facts,replacement.replacement_facts,
               prior.prior_charge_pence,replacement.replacement_charge_pence
        from prior_rows prior
        full join replacement_rows replacement
          on replacement.work_event_id=prior.work_event_id
         and replacement.source_line_kind=prior.source_line_kind
         and replacement.row_number=prior.row_number
      ), presented as (
        select paired.*,candidate.display_name,
          coalesce(replacement_facts->>'work_date',prior_facts->>'work_date') work_date,
          case when prior_facts is null then 'Not present' else
            to_char((prior_facts->>'start_at_local')::timestamp,'HH24:MI')||'-'||
            to_char((prior_facts->>'end_at_local')::timestamp,'HH24:MI')||' ('||
            coalesce(prior_facts->>'break_minutes','0')||' min break)' end current_final,
          case when replacement_facts is null then 'Not present' else
            to_char((replacement_facts->>'start_at_local')::timestamp,'HH24:MI')||'-'||
            to_char((replacement_facts->>'end_at_local')::timestamp,'HH24:MI')||' ('||
            coalesce(replacement_facts->>'break_minutes','0')||' min break)' end replacement,
          -- WP-54.  The same two lines expressed as money.  The preview
          -- compared the formatted TIME only, so a report that repriced every
          -- line produced changes=[] and the confirmation the Office signed --
          -- and the preview_hash that binds it -- covered an empty list.
          -- Pack 24 section 7 requires Office to see the exact consequence it
          -- is confirming; pack 14 section 4.3.1 makes this figure the invoice
          -- value.  The DECISION below compares exact signed pence; only the
          -- display is formatted.
          case when paired.prior_charge_pence is null then 'Not present'
               else private.weekly_source_office_money_text_v1(paired.prior_charge_pence::numeric/100)
          end current_final_amount,
          case when paired.replacement_charge_pence is null then 'Not present'
               else private.weekly_source_office_money_text_v1(paired.replacement_charge_pence::numeric/100)
          end replacement_amount,
          case when prior_facts is null then 'Added'
               when replacement_facts is null then 'Removed'
               else 'Changed' end result
        from paired
        left join public.candidates candidate on candidate.id=paired.candidate_id
      )
      select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'candidate',coalesce(display_name,'Candidate'),
        'day_date',to_char(work_date::date,'Dy FMDD Mon YYYY'),
        'current_final',current_final,'replacement',replacement,
        'current_final_amount',current_final_amount,
        'replacement_amount',replacement_amount,
        'amount_changed',prior_charge_pence is distinct from replacement_charge_pence,
        'result',result
      ) order by work_date,coalesce(display_name,''),work_event_id),'[]'::jsonb)
      into v_changes
      from presented
      where current_final is distinct from replacement
         or prior_charge_pence is distinct from replacement_charge_pence;
    else
      with prior_rows as (
        select snapshot.work_event_id,snapshot.candidate_id,snapshot.work_date,
               snapshot.start_at_local,snapshot.end_at_local,snapshot.break_minutes,
               -- WP-54.  The same blindness the NHSP branch had: this preview
               -- compared work date, times and break only.  On this route the
               -- money is the server's own calculation, so it moves when the
               -- Contract or the rate policy behind the row moves even though
               -- every displayed time is identical.  LEFT join: a row with no
               -- economic snapshot keeps its place in the pairing and simply
               -- carries no amount.
               prior_economic.calculated_charge_pence prior_charge_pence
        from public.weekly_source_final_snapshot_lines snapshot
        left join public.weekly_source_row_economic_snapshots prior_economic
          on prior_economic.row_resolution_id=snapshot.row_resolution_id
        where snapshot.final_revision_id=v_session.expected_current_final_revision_id
      ), replacement_rows as (
        select resolution.work_event_id,resolution.candidate_id,source_row.work_date,
               source_row.start_at_local,source_row.end_at_local,source_row.break_minutes,
               replacement_economic.calculated_charge_pence replacement_charge_pence
        from public.weekly_source_upload_rows source_row
        join public.weekly_source_row_resolutions resolution
          on resolution.upload_row_id=source_row.id
         and resolution.generation=coalesce(
           v_publication.projection_generation,
           v_publication.authority_scope_version::integer
         )
         and resolution.mapping_state='RESOLVED'
        left join public.weekly_source_row_economic_snapshots replacement_economic
          on replacement_economic.row_resolution_id=resolution.id
        where source_row.upload_id=v_session.replacement_correction_upload_id
          and (
            source_row.row_finalisation_state='SOURCE_WORKED'
            or (v_prior_profile.final_authority_kind='GENERIC_COMPLETE_SNAPSHOT'
                and source_row.row_finalisation_state='NOT_APPLICABLE')
          )
      ), paired as (
        select coalesce(replacement.work_event_id,prior.work_event_id) work_event_id,
               coalesce(replacement.candidate_id,prior.candidate_id) candidate_id,
               prior.work_date prior_date,prior.start_at_local prior_start,
               prior.end_at_local prior_end,prior.break_minutes prior_break,
               prior.prior_charge_pence,
               replacement.work_date replacement_date,
               replacement.start_at_local replacement_start,
               replacement.end_at_local replacement_end,
               replacement.break_minutes replacement_break,
               replacement.replacement_charge_pence
        from prior_rows prior
        full join replacement_rows replacement using(work_event_id)
      )
      select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'candidate',coalesce(candidate.display_name,'Candidate'),
        'day_date',to_char(coalesce(paired.replacement_date,paired.prior_date),'Dy FMDD Mon YYYY'),
        'current_final',case when paired.prior_date is null then 'Not present' else
          to_char(paired.prior_start,'HH24:MI')||'-'||to_char(paired.prior_end,'HH24:MI')||
          ' ('||paired.prior_break||' min break)' end,
        'replacement',case when paired.replacement_date is null then 'Not present' else
          to_char(paired.replacement_start,'HH24:MI')||'-'||to_char(paired.replacement_end,'HH24:MI')||
          ' ('||paired.replacement_break||' min break)' end,
        'current_final_amount',case when paired.prior_charge_pence is null then 'Not present'
          else private.weekly_source_office_money_text_v1(
            paired.prior_charge_pence::numeric/100) end,
        'replacement_amount',case when paired.replacement_charge_pence is null then 'Not present'
          else private.weekly_source_office_money_text_v1(
            paired.replacement_charge_pence::numeric/100) end,
        'amount_changed',paired.prior_charge_pence is distinct from paired.replacement_charge_pence,
        'result',case when paired.prior_date is null then 'Added'
          when paired.replacement_date is null then 'Removed' else 'Changed' end
      ) order by coalesce(paired.replacement_date,paired.prior_date),
        coalesce(candidate.display_name,''),paired.work_event_id),'[]'::jsonb)
      into v_changes
      from paired
      left join public.candidates candidate on candidate.id=paired.candidate_id
      -- WP-54: money joins date, times and break in the comparison, so a
      -- correction that moves only the figure can no longer present the Office
      -- with an empty change list.
      where pg_catalog.jsonb_build_array(
              paired.prior_date,paired.prior_start,paired.prior_end,paired.prior_break
            ) is distinct from pg_catalog.jsonb_build_array(
              paired.replacement_date,paired.replacement_start,
              paired.replacement_end,paired.replacement_break
            )
         or paired.prior_charge_pence is distinct from paired.replacement_charge_pence;
    end if;
  end if;

  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select backing.backing_report_number into strict v_old_report
    from public.weekly_source_nhsp_backing_reports backing
    where backing.final_revision_id=v_session.expected_current_final_revision_id;
    v_confirmation:='I understand this will replace report '||v_old_report||
      ' as the final report for this Trust and cutoff.';
  else
    v_confirmation:='I understand this will replace the final version for this group and week.';
  end if;
  v_preview:=pg_catalog.jsonb_build_object(
    'changes',v_changes,'blockers',v_blockers,'confirmation_text',v_confirmation
  );
  return v_preview||pg_catalog.jsonb_build_object(
    'preview_hash',pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CORRECT_FINAL_OFFICE_PREVIEW_V1',v_preview
    ),'hex')
  );
end;
$function$;

-- Review is deliberately separate from materialisation.  It may update only
-- this correction session and its audit trail; it never creates or changes a
-- Contract Week, Timesheet, lineage, financial, invoice, query or token row.
create or replace function public.weekly_source_correct_final_review_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','correction_session_id','expected_authority_scope_version',
    'expected_comparison_manifest_hash','expected_issue_set_hash',
    'expected_row_manifest_hash','expected_session_version','idempotency_key',
    'replacement_projection_publication_id','replacement_upload_id','schema_version'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_session_id uuid;
  v_upload_id uuid;
  v_publication_id uuid;
  v_expected_session_version bigint;
  v_scope_version bigint;
  v_expected_row_hash bytea;
  v_expected_comparison_hash bytea;
  v_expected_issue_hash bytea;
  v_idempotency_key text;
  v_request_hash bytea;
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_prior_revision public.weekly_source_final_revisions%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_prior_publication public.weekly_source_projection_publications%rowtype;
  v_prior_profile public.weekly_source_format_profiles%rowtype;
  v_replacement_upload public.weekly_source_uploads%rowtype;
  v_replacement_publication public.weekly_source_projection_publications%rowtype;
  v_replacement_profile public.weekly_source_format_profiles%rowtype;
  v_guard jsonb;
  v_guard_hash bytea;
  v_client_id uuid;
  v_office_preview jsonb;
  v_result jsonb;
  v_result_hash bytea;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REVIEW_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_session_id:=(p_request->>'correction_session_id')::uuid;
    v_upload_id:=(p_request->>'replacement_upload_id')::uuid;
    v_publication_id:=(p_request->>'replacement_projection_publication_id')::uuid;
    v_expected_session_version:=(p_request->>'expected_session_version')::bigint;
    v_scope_version:=(p_request->>'expected_authority_scope_version')::bigint;
    v_expected_row_hash:=pg_catalog.decode(p_request->>'expected_row_manifest_hash','hex');
    v_expected_comparison_hash:=pg_catalog.decode(p_request->>'expected_comparison_manifest_hash','hex');
    v_expected_issue_hash:=pg_catalog.decode(p_request->>'expected_issue_set_hash','hex');
  exception when others then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REVIEW_VALUE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  if v_actor is null or v_session_id is null or v_upload_id is null
     or v_publication_id is null or v_expected_session_version is null
     or v_scope_version is null or v_scope_version<1
     or pg_catalog.octet_length(v_expected_row_hash)<>32
     or pg_catalog.octet_length(v_expected_comparison_hash)<>32
     or pg_catalog.octet_length(v_expected_issue_hash)<>32
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REVIEW_VALUE_INVALID' using errcode='22023';
  end if;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1',p_request-'idempotency_key'
  );

  select * into v_session
  from public.weekly_final_source_correction_sessions where id=v_session_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_NOT_FOUND' using errcode='22023';
  end if;
  if v_session.review_idempotency_key=v_idempotency_key then
    if v_session.actor_user_id is distinct from v_actor
       or v_session.review_request_hash is distinct from v_request_hash then
      raise exception 'WEEKLY_SOURCE_CORRECTION_REVIEW_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    return v_session.review_result_json||pg_catalog.jsonb_build_object(
      'idempotent_replay',true
    );
  end if;
  if v_session.state not in ('READY','REVIEWED') then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
  end if;

  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_session.source_cycle_id for update;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into strict v_scope from public.weekly_source_report_scopes
    where id=v_session.report_scope_id for update;
  end if;
  select * into strict v_session from public.weekly_final_source_correction_sessions
  where id=v_session_id for update;
  if v_session.state not in ('READY','REVIEWED')
     or v_session.actor_user_id is distinct from v_actor
     or v_session.version<>v_expected_session_version
     or v_session.replacement_correction_upload_id is distinct from v_upload_id
     or v_session.replacement_projection_publication_id is distinct from v_publication_id
     or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
          is distinct from v_session.expected_current_final_revision_id then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
  end if;
  select * into strict v_prior_revision from public.weekly_source_final_revisions
  where id=v_session.expected_current_final_revision_id for share;
  select * into strict v_prior_upload from public.weekly_source_uploads
  where id=v_prior_revision.upload_id for share;
  select * into strict v_prior_publication
  from public.weekly_source_projection_publications
  where id=coalesce(v_scope.current_projection_publication_id,
                    v_cycle.current_projection_publication_id)
  for share;
  select * into strict v_prior_profile from public.weekly_source_format_profiles
  where id=v_prior_upload.source_format_profile_id;
  select * into strict v_replacement_upload from public.weekly_source_uploads
  where id=v_upload_id for share;
  select * into strict v_replacement_publication
  from public.weekly_source_projection_publications where id=v_publication_id for share;
  select * into strict v_replacement_profile from public.weekly_source_format_profiles
  where id=v_replacement_upload.source_format_profile_id;
  if v_prior_revision.state<>'CURRENT'
     or v_prior_revision.manifest_hash is distinct from v_session.expected_final_manifest_hash
     or v_prior_upload.state<>'CURRENT'
     or v_prior_publication.state<>'CURRENT'
     or v_replacement_upload.state<>'CORRECTION_READY'
     or v_replacement_upload.purpose<>'FINAL_SOURCE_CORRECTION'
     or v_replacement_upload.correction_session_id is distinct from v_session.id
     or v_replacement_upload.source_cycle_id is distinct from v_cycle.id
     or v_replacement_upload.report_scope_id is distinct from v_session.report_scope_id
     or v_replacement_upload.row_manifest_hash is distinct from v_expected_row_hash
     or v_replacement_publication.state<>'CORRECTION_READY'
     or v_replacement_publication.correction_session_id is distinct from v_session.id
     or v_replacement_publication.upload_id is distinct from v_replacement_upload.id
     or v_replacement_publication.source_cycle_id is distinct from v_cycle.id
     or v_replacement_publication.authority_scope_kind is distinct from v_session.authority_scope_kind
     or v_replacement_publication.report_scope_id is distinct from v_session.report_scope_id
      or v_replacement_publication.authority_scope_version<>v_scope_version
      or v_replacement_publication.comparison_manifest_hash is distinct from v_expected_comparison_hash
      or v_replacement_publication.issue_set_hash is distinct from v_expected_issue_hash then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE' using errcode='40001';
  end if;
  if v_replacement_profile.final_authority_kind is distinct from v_prior_profile.final_authority_kind
     or (v_replacement_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT')
          is distinct from (v_group.source_family='NHSP') then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SOURCE_FAMILY_MISMATCH'
      using errcode='55000';
  end if;
  select client_id into strict v_client_id
  from public.weekly_source_client_manifests
  where final_revision_id=v_prior_revision.id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'CORRECT_FINAL_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  v_guard:=private.weekly_source_correct_final_preconditions_v1(
    v_prior_revision.id,v_actor,v_session.id,false
  );
  v_guard_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_GUARD_V1',v_guard
  );
  if v_guard_hash is distinct from v_session.guard_fingerprint then
    raise exception 'WEEKLY_SOURCE_CORRECTION_GUARD_STALE' using errcode='40001';
  end if;
  v_office_preview:=private.weekly_source_correct_final_office_preview_v1(v_session.id);
  v_result:=pg_catalog.jsonb_build_object(
    'ok',true,
    'status',case when pg_catalog.jsonb_array_length(v_office_preview->'blockers')>0
      then 'BLOCKED' else 'READY_FOR_CONFIRMATION' end,
    'correction_session_id',v_session.id,
    'prior_final_revision_id',v_prior_revision.id,
    'upload_id',v_replacement_upload.id,
    'version',v_expected_session_version+1,
    'office_preview',v_office_preview,
    'idempotent_replay',false
  );
  v_result_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_RESULT_V1',v_result
  );
  update public.weekly_final_source_correction_sessions
  set state='REVIEWED',review_idempotency_key=v_idempotency_key,
      review_request_hash=v_request_hash,review_result_json=v_result,
      review_result_hash=v_result_hash,version=version+1,
      updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state in ('READY','REVIEWED')
    and version=v_expected_session_version;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'weekly_final_source_correction_sessions',v_session.id::text,
         'WEEKLY_SOURCE_CORRECT_FINAL_REVIEWED',
         pg_catalog.jsonb_build_object('state',v_session.state,'version',v_expected_session_version),
         pg_catalog.jsonb_build_object(
           'state','REVIEWED','version',v_expected_session_version+1,
           'status',v_result->>'status',
           'change_count',pg_catalog.jsonb_array_length(v_office_preview->'changes'),
           'blocker_count',pg_catalog.jsonb_array_length(v_office_preview->'blockers'),
           'review_result_hash',pg_catalog.encode(v_result_hash,'hex')
         ),v_session.reason
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;
  return v_result;
end;
$function$;

create or replace function public.weekly_source_correct_final_prepare_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','correction_session_id','expected_authority_scope_version',
    'expected_comparison_manifest_hash','expected_issue_set_hash',
    'expected_preview_hash','expected_row_manifest_hash','expected_session_version',
    'idempotency_key','reason','confirmation_text',
    'replacement_projection_publication_id','replacement_upload_id','schema_version'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_session_id uuid;
  v_upload_id uuid;
  v_publication_id uuid;
  v_expected_session_version bigint;
  v_scope_version bigint;
  v_expected_row_hash bytea;
  v_expected_comparison_hash bytea;
  v_expected_issue_hash bytea;
  v_expected_preview_hash bytea;
  v_idempotency_key text;
  v_reason text;
  v_confirmation_text text;
  v_request_hash bytea;
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_prior_revision public.weekly_source_final_revisions%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_prior_publication public.weekly_source_projection_publications%rowtype;
  v_prior_profile public.weekly_source_format_profiles%rowtype;
  v_replacement_upload public.weekly_source_uploads%rowtype;
  v_replacement_publication public.weekly_source_projection_publications%rowtype;
  v_replacement_profile public.weekly_source_format_profiles%rowtype;
  v_guard jsonb;
  v_guard_hash bytea;
  v_client_id uuid;
  v_core_result jsonb;
  v_context jsonb;
  v_result jsonb;
  v_result_hash bytea;
  v_prepared_revision_id uuid;
  v_prepared_session_version bigint;
  v_office_preview jsonb;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_V1' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_session_id:=(p_request->>'correction_session_id')::uuid;
    v_upload_id:=(p_request->>'replacement_upload_id')::uuid;
    v_publication_id:=(p_request->>'replacement_projection_publication_id')::uuid;
    v_expected_session_version:=(p_request->>'expected_session_version')::bigint;
    v_scope_version:=(p_request->>'expected_authority_scope_version')::bigint;
    v_expected_row_hash:=pg_catalog.decode(p_request->>'expected_row_manifest_hash','hex');
    v_expected_comparison_hash:=pg_catalog.decode(p_request->>'expected_comparison_manifest_hash','hex');
    v_expected_issue_hash:=pg_catalog.decode(p_request->>'expected_issue_set_hash','hex');
    v_expected_preview_hash:=pg_catalog.decode(p_request->>'expected_preview_hash','hex');
  exception when others then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_VALUE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  v_confirmation_text:=pg_catalog.btrim(coalesce(p_request->>'confirmation_text',''));
  if v_actor is null or v_session_id is null or v_upload_id is null
     or v_publication_id is null or v_expected_session_version is null
     or v_scope_version is null or v_scope_version<1
     or pg_catalog.octet_length(v_expected_row_hash)<>32
     or pg_catalog.octet_length(v_expected_comparison_hash)<>32
     or pg_catalog.octet_length(v_expected_issue_hash)<>32
     or pg_catalog.octet_length(v_expected_preview_hash)<>32
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200
     or pg_catalog.char_length(v_reason) not between 1 and 1000
     or pg_catalog.char_length(v_confirmation_text) not between 1 and 500 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_VALUE_INVALID' using errcode='22023';
  end if;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_V1',p_request-'idempotency_key'
  );
  select * into v_session
  from public.weekly_final_source_correction_sessions where id=v_session_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_NOT_FOUND' using errcode='22023';
  end if;
  if v_session.state in ('PREPARED','COMMITTING','APPLIED') then
    if v_session.actor_user_id is distinct from v_actor
       or v_session.prepare_idempotency_key is distinct from v_idempotency_key
       or v_session.prepare_request_hash is distinct from v_request_hash then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    return v_session.prepare_result_json||pg_catalog.jsonb_build_object(
      'status','PREPARED','idempotent_replay',true
    );
  end if;

  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_session.source_cycle_id for update;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into strict v_scope from public.weekly_source_report_scopes
    where id=v_session.report_scope_id for update;
  end if;
  select * into strict v_session from public.weekly_final_source_correction_sessions
  where id=v_session_id for update;
  if v_session.state<>'REVIEWED' or v_session.actor_user_id is distinct from v_actor
     or v_session.version<>v_expected_session_version
     or v_session.replacement_correction_upload_id is distinct from v_upload_id
     or v_session.replacement_projection_publication_id is distinct from v_publication_id
     or v_session.review_result_json is null
     or v_session.review_result_hash is distinct from private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_RESULT_V1',v_session.review_result_json
        )
     or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
          is distinct from v_session.expected_current_final_revision_id then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
  end if;
  select * into strict v_prior_revision from public.weekly_source_final_revisions
  where id=v_session.expected_current_final_revision_id for share;
  select * into strict v_prior_upload from public.weekly_source_uploads
  where id=v_prior_revision.upload_id for share;
  select * into strict v_prior_publication
  from public.weekly_source_projection_publications
  where id=coalesce(v_scope.current_projection_publication_id,
                    v_cycle.current_projection_publication_id)
  for share;
  select * into strict v_prior_profile from public.weekly_source_format_profiles
  where id=v_prior_upload.source_format_profile_id;
  select * into strict v_replacement_upload from public.weekly_source_uploads
  where id=v_upload_id for update;
  select * into strict v_replacement_publication
  from public.weekly_source_projection_publications where id=v_publication_id for update;
  select * into strict v_replacement_profile from public.weekly_source_format_profiles
  where id=v_replacement_upload.source_format_profile_id;
  if v_prior_revision.state<>'CURRENT'
     or v_prior_revision.manifest_hash is distinct from v_session.expected_final_manifest_hash
     or v_prior_upload.state<>'CURRENT'
     or v_prior_publication.state<>'CURRENT'
     or v_replacement_upload.state<>'CORRECTION_READY'
     or v_replacement_upload.purpose<>'FINAL_SOURCE_CORRECTION'
     or v_replacement_upload.correction_session_id is distinct from v_session.id
     or v_replacement_upload.source_cycle_id is distinct from v_cycle.id
     or v_replacement_upload.report_scope_id is distinct from v_session.report_scope_id
     or v_replacement_upload.row_manifest_hash is distinct from v_expected_row_hash
     or v_replacement_publication.state<>'CORRECTION_READY'
     or v_replacement_publication.correction_session_id is distinct from v_session.id
     or v_replacement_publication.upload_id is distinct from v_replacement_upload.id
     or v_replacement_publication.source_cycle_id is distinct from v_cycle.id
     or v_replacement_publication.authority_scope_kind is distinct from v_session.authority_scope_kind
     or v_replacement_publication.report_scope_id is distinct from v_session.report_scope_id
     or v_replacement_publication.authority_scope_version<>v_scope_version
     or v_replacement_publication.comparison_manifest_hash is distinct from v_expected_comparison_hash
     or v_replacement_publication.issue_set_hash is distinct from v_expected_issue_hash then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE' using errcode='40001';
  end if;
  if v_replacement_profile.final_authority_kind is distinct from v_prior_profile.final_authority_kind
     or (v_replacement_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT')
          is distinct from (v_group.source_family='NHSP') then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SOURCE_FAMILY_MISMATCH'
      using errcode='55000';
  end if;
  select client_id into strict v_client_id
  from public.weekly_source_client_manifests
  where final_revision_id=v_prior_revision.id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'CORRECT_FINAL_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  v_guard:=private.weekly_source_correct_final_preconditions_v1(
    v_prior_revision.id,v_actor,v_session.id,true
  );
  v_guard_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_GUARD_V1',v_guard
  );
  if v_guard_hash is distinct from v_session.guard_fingerprint then
    raise exception 'WEEKLY_SOURCE_CORRECTION_GUARD_STALE' using errcode='40001';
  end if;
  v_office_preview:=private.weekly_source_correct_final_office_preview_v1(v_session.id);
  if v_session.review_result_json->'office_preview' is distinct from v_office_preview
     or private.weekly_source_finalisation_hex32_v1(
          v_office_preview->>'preview_hash','WEEKLY_SOURCE_CORRECTION_PREVIEW_HASH_INVALID'
        ) is distinct from v_expected_preview_hash
     or v_confirmation_text is distinct from v_office_preview->>'confirmation_text'
     or pg_catalog.jsonb_array_length(v_office_preview->'blockers')<>0 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREVIEW_STALE' using errcode='40001';
  end if;
  update public.weekly_final_source_correction_sessions
  set state='PREPARING',prepare_idempotency_key=v_idempotency_key,
      prepare_request_hash=v_request_hash,reason=v_reason,version=version+1,
      updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='REVIEWED' and version=v_expected_session_version;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;

  v_core_result:=private.weekly_source_finalise_engine_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
      'authority_scope_kind',v_session.authority_scope_kind,
      'report_scope_id',v_session.report_scope_id,
      'upload_id',v_replacement_upload.id,
      'projection_publication_id',v_replacement_publication.id,
      'expected_authority_scope_version',v_scope_version,
      'expected_row_manifest_hash',pg_catalog.encode(v_expected_row_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(v_expected_comparison_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(v_expected_issue_hash,'hex')
    ),v_session.id,true
  );
  if coalesce((v_core_result->>'ok')::boolean,false) is not true
     or v_core_result->>'status'<>'PREPARED' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARE_FAILED'
      using errcode='55000',detail=v_core_result::text;
  end if;
  v_prepared_revision_id:=(v_core_result->>'final_revision_id')::uuid;
  update public.weekly_final_source_correction_sessions
  set prepared_final_revision_id=v_prepared_revision_id
  where id=v_session.id and state='PREPARING'
    and prepare_request_hash=v_request_hash;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;
  v_context:=private.weekly_source_correct_final_prepared_context_v1(v_session.id);
  v_office_preview:=private.weekly_source_correct_final_office_preview_v1(v_session.id);
  v_prepared_session_version:=v_expected_session_version+2;
  v_result:=v_core_result||pg_catalog.jsonb_build_object(
    'correction_session_id',v_session.id,
    'prior_final_revision_id',v_prior_revision.id,
    'root_contexts',v_context->'root_contexts',
    'office_preview',v_office_preview,
    'version',v_prepared_session_version,
    'idempotent_replay',false
  );
  v_result_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_RESULT_V1',v_result
  );
  update public.weekly_final_source_correction_sessions
  set state='PREPARED',prepare_result_json=v_result,prepare_result_hash=v_result_hash,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='PREPARING'
    and prepared_final_revision_id=v_prepared_revision_id
    and prepare_request_hash=v_request_hash;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'weekly_final_source_correction_sessions',v_session.id::text,
         'WEEKLY_SOURCE_CORRECT_FINAL_PREPARED',
         pg_catalog.jsonb_build_object(
           'state','READY','current_final_revision_id',v_prior_revision.id
         ),
         pg_catalog.jsonb_build_object(
           'state','PREPARED','prepared_final_revision_id',v_prepared_revision_id,
           'replacement_upload_id',v_replacement_upload.id,
           'replacement_projection_publication_id',v_replacement_publication.id,
           'affected_root_count',pg_catalog.jsonb_array_length(v_context->'root_contexts'),
           'prepare_result_hash',pg_catalog.encode(v_result_hash,'hex')
         ),v_session.reason
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;
  return v_result;
end;
$function$;

create or replace function public.weekly_source_correct_final_apply_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','correction_session_id','expected_authority_scope_version',
    'expected_comparison_manifest_hash','expected_issue_set_hash',
    'expected_preview_hash','expected_row_manifest_hash','expected_session_version',
    'idempotency_key','reason','confirmation_text',
    'replacement_projection_publication_id','replacement_upload_id',
    'root_service_snapshots','schema_version'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_session_id uuid;
  v_upload_id uuid;
  v_publication_id uuid;
  v_expected_session_version bigint;
  v_scope_version bigint;
  v_expected_row_hash bytea;
  v_expected_comparison_hash bytea;
  v_expected_issue_hash bytea;
  v_expected_preview_hash bytea;
  v_idempotency_key text;
  v_reason text;
  v_confirmation_text text;
  v_apply_request_hash bytea;
  v_root_snapshots jsonb;
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_prior_revision public.weekly_source_final_revisions%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_prior_publication public.weekly_source_projection_publications%rowtype;
  v_replacement_upload public.weekly_source_uploads%rowtype;
  v_replacement_publication public.weekly_source_projection_publications%rowtype;
  v_prior_profile public.weekly_source_format_profiles%rowtype;
  v_replacement_profile public.weekly_source_format_profiles%rowtype;
  v_prepared_revision public.weekly_source_final_revisions%rowtype;
  v_guard jsonb;
  v_guard_hash bytea;
  v_prepared_context jsonb;
  v_live_preview jsonb;
  v_server_roots jsonb;
  v_client_id uuid;
  v_new_revision_id uuid;
  v_root record;
  v_snapshot jsonb;
  v_prior_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
  v_projection_result jsonb;
  v_root_preflight jsonb;
  v_impact_hash bytea;
  v_result jsonb;
  v_result_hash bytea;
  v_completion_generation integer;
  v_completion_hash bytea;
  v_voided_count integer;
  v_old_movement_count integer;
  v_cas_count integer;
  v_family uuid[];
  v_family_after_lock uuid[];
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_APPLY_REQUEST_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_CORRECT_FINAL_APPLY_V1'
     or pg_catalog.jsonb_typeof(p_request->'root_service_snapshots')<>'array' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_APPLY_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_session_id:=(p_request->>'correction_session_id')::uuid;
    v_upload_id:=(p_request->>'replacement_upload_id')::uuid;
    v_publication_id:=(p_request->>'replacement_projection_publication_id')::uuid;
    v_expected_session_version:=(p_request->>'expected_session_version')::bigint;
    v_scope_version:=(p_request->>'expected_authority_scope_version')::bigint;
    v_expected_row_hash:=pg_catalog.decode(p_request->>'expected_row_manifest_hash','hex');
    v_expected_comparison_hash:=pg_catalog.decode(p_request->>'expected_comparison_manifest_hash','hex');
    v_expected_issue_hash:=pg_catalog.decode(p_request->>'expected_issue_set_hash','hex');
    v_expected_preview_hash:=pg_catalog.decode(p_request->>'expected_preview_hash','hex');
  exception when others then
    raise exception 'WEEKLY_SOURCE_CORRECTION_APPLY_VALUE_INVALID' using errcode='22023';
  end;
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  v_confirmation_text:=pg_catalog.btrim(coalesce(p_request->>'confirmation_text',''));
  v_root_snapshots:=p_request->'root_service_snapshots';
  if v_actor is null or v_session_id is null or v_upload_id is null
     or v_publication_id is null or v_expected_session_version is null
     or v_scope_version is null or v_scope_version<1
     or pg_catalog.octet_length(v_expected_row_hash)<>32
     or pg_catalog.octet_length(v_expected_comparison_hash)<>32
     or pg_catalog.octet_length(v_expected_issue_hash)<>32
     or pg_catalog.octet_length(v_expected_preview_hash)<>32
     or pg_catalog.char_length(v_idempotency_key) not between 1 and 200
     or pg_catalog.char_length(v_reason) not between 1 and 1000
     or pg_catalog.char_length(v_confirmation_text) not between 1 and 500 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_APPLY_VALUE_INVALID' using errcode='22023';
  end if;
  v_apply_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_APPLY_V1',p_request-'idempotency_key'
  );

  select * into v_session from public.weekly_final_source_correction_sessions
  where id=v_session_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_NOT_FOUND' using errcode='22023';
  end if;
  if v_session.state='APPLIED' then
    if v_session.actor_user_id is distinct from v_actor
       or v_session.apply_idempotency_key is distinct from v_idempotency_key
       or v_session.apply_request_hash is distinct from v_apply_request_hash then
      raise exception 'WEEKLY_SOURCE_CORRECTION_APPLY_IDEMPOTENCY_COLLISION'
        using errcode='22023';
    end if;
    return v_session.result_json||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_session.source_cycle_id for update;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into strict v_scope from public.weekly_source_report_scopes
    where id=v_session.report_scope_id for update;
  end if;
  select * into strict v_session from public.weekly_final_source_correction_sessions
  where id=v_session_id for update;
  if v_session.state<>'PREPARED' or v_session.actor_user_id is distinct from v_actor
     or v_session.version<>v_expected_session_version
     or v_session.replacement_correction_upload_id is distinct from v_upload_id
     or v_session.replacement_projection_publication_id is distinct from v_publication_id
     or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
          is distinct from v_session.expected_current_final_revision_id then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
  end if;
  select * into strict v_prior_revision from public.weekly_source_final_revisions
  where id=v_session.expected_current_final_revision_id for update;
  select * into strict v_prepared_revision from public.weekly_source_final_revisions
  where id=v_session.prepared_final_revision_id for update;
  select * into strict v_prior_upload from public.weekly_source_uploads
  where id=v_prior_revision.upload_id for update;
  select * into strict v_prior_profile from public.weekly_source_format_profiles
  where id=v_prior_upload.source_format_profile_id;
  select * into strict v_prior_publication
  from public.weekly_source_projection_publications
  where id=coalesce(v_scope.current_projection_publication_id,v_cycle.current_projection_publication_id)
  for update;
  select * into strict v_replacement_upload from public.weekly_source_uploads
  where id=v_upload_id for update;
  select * into strict v_replacement_publication
  from public.weekly_source_projection_publications where id=v_publication_id for update;

  if v_prior_revision.state<>'CURRENT'
     or v_prior_revision.manifest_hash is distinct from v_session.expected_final_manifest_hash
     or v_prepared_revision.state<>'PREPARED'
     or v_prepared_revision.reason<>'CORRECT_FINAL_SOURCE'
     or v_prepared_revision.predecessor_revision_id is distinct from v_prior_revision.id
     or v_prepared_revision.source_cycle_id is distinct from v_cycle.id
     or v_prepared_revision.upload_id is distinct from v_replacement_upload.id
     or v_session.prepare_result_json->>'final_revision_id'
          is distinct from v_prepared_revision.id::text
     or v_session.prepare_result_hash is distinct from private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_RESULT_V1',v_session.prepare_result_json
        )
     or v_prior_upload.state<>'CURRENT'
     or v_prior_publication.state<>'CURRENT'
     or v_replacement_upload.state<>'CORRECTION_READY'
     or v_replacement_upload.purpose<>'FINAL_SOURCE_CORRECTION'
     or v_replacement_upload.correction_session_id is distinct from v_session.id
     or v_replacement_upload.source_cycle_id is distinct from v_cycle.id
     or v_replacement_upload.report_scope_id is distinct from v_session.report_scope_id
     or v_replacement_upload.row_manifest_hash is distinct from v_expected_row_hash
     or v_replacement_publication.state<>'CORRECTION_READY'
     or v_replacement_publication.correction_session_id is distinct from v_session.id
     or v_replacement_publication.upload_id is distinct from v_replacement_upload.id
     or v_replacement_publication.source_cycle_id is distinct from v_cycle.id
     or v_replacement_publication.authority_scope_kind is distinct from v_session.authority_scope_kind
     or v_replacement_publication.report_scope_id is distinct from v_session.report_scope_id
     or v_replacement_publication.authority_scope_version<>v_scope_version
     or v_replacement_publication.comparison_manifest_hash is distinct from v_expected_comparison_hash
     or v_replacement_publication.issue_set_hash is distinct from v_expected_issue_hash then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE' using errcode='40001';
  end if;
  select * into strict v_replacement_profile from public.weekly_source_format_profiles
  where id=v_replacement_upload.source_format_profile_id;
  if v_replacement_profile.final_authority_kind is distinct from v_prior_profile.final_authority_kind
     or (v_replacement_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT')
          is distinct from (v_group.source_family='NHSP') then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SOURCE_FAMILY_MISMATCH'
      using errcode='55000';
  end if;

  select client_id into strict v_client_id
  from public.weekly_source_client_manifests
  where final_revision_id=v_prior_revision.id;
  perform private.weekly_source_office_authority_v1(
    v_actor,'CORRECT_FINAL_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  v_guard:=private.weekly_source_correct_final_preconditions_v1(
    v_prior_revision.id,v_actor,v_session.id,true
  );
  v_guard_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_GUARD_V1',v_guard
  );
  if v_guard_hash is distinct from v_session.guard_fingerprint then
    raise exception 'WEEKLY_SOURCE_CORRECTION_GUARD_STALE' using errcode='40001';
  end if;
  v_prepared_context:=private.weekly_source_correct_final_prepared_context_v1(v_session.id);
  v_server_roots:=v_prepared_context->'root_contexts';
  if v_server_roots is distinct from v_session.prepare_result_json->'root_contexts' then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARED_CONTEXT_STALE'
      using errcode='40001';
  end if;
  v_live_preview:=private.weekly_source_correct_final_office_preview_v1(v_session.id);
  if v_session.prepare_result_json->'office_preview' is distinct from v_live_preview
     or private.weekly_source_finalisation_hex32_v1(
          v_live_preview->>'preview_hash','WEEKLY_SOURCE_CORRECTION_PREVIEW_HASH_INVALID'
        ) is distinct from v_expected_preview_hash
     or v_confirmation_text is distinct from v_live_preview->>'confirmation_text'
     or pg_catalog.jsonb_array_length(v_live_preview->'blockers')<>0 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREVIEW_STALE' using errcode='40001';
  end if;

  if exists(
    select 1 from pg_catalog.jsonb_array_elements(v_root_snapshots) supplied(value)
    where pg_catalog.jsonb_typeof(supplied.value)<>'object'
       or not (supplied.value ? 'root_timesheet_id'
               and supplied.value ? 'prepared_context_hash'
               and supplied.value ? 'service_snapshot')
       or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(supplied.value))<>3
       or coalesce(supplied.value->>'root_timesheet_id','')
            !~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(supplied.value->>'prepared_context_hash','') !~*'^[0-9a-f]{64}$'
       or pg_catalog.jsonb_typeof(supplied.value->'service_snapshot')<>'object'
  ) or (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(v_root_snapshots))
       <>pg_catalog.jsonb_array_length(v_server_roots)
     or exists(
       select 1
       from pg_catalog.jsonb_array_elements(v_root_snapshots) supplied(value)
       group by supplied.value->>'root_timesheet_id'
       having pg_catalog.count(*)<>1
     ) or exists(
       select 1 from pg_catalog.jsonb_array_elements(v_server_roots) server_root(value)
       where not exists(
         select 1 from pg_catalog.jsonb_array_elements(v_root_snapshots) supplied(value)
         where supplied.value->>'root_timesheet_id'=server_root.value->>'root_timesheet_id'
           and supplied.value->>'prepared_context_hash'=
                 server_root.value->>'prepared_context_hash'
       )
     ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_ROOT_SNAPSHOT_SET_MISMATCH'
      using errcode='40001';
  end if;

  -- PREPARE can create a replacement-only root that did not exist when OPEN
  -- froze the prior-root guard.  Apply the same invoice/Banking/Draft
  -- preconditions to every sealed union root immediately before COMMITTING so
  -- a newly created root cannot enter another owner between PREPARE and APPLY.
  for v_root in
    select (server_root.value->>'root_timesheet_id')::uuid as root_timesheet_id
    from pg_catalog.jsonb_array_elements(v_server_roots) server_root(value)
    order by server_root.value->>'root_timesheet_id'
  loop
    -- Gate 13 hostile review F5 / standing rule 3, at COMMITTING time.  Same
    -- family keying and same lock-before-read order as the OPEN-time
    -- preconditions above: name the family, lock its rows in a deterministic
    -- order, re-resolve, require the membership to be unchanged, and only then
    -- read the protected-family and invoice-line state that is being guarded.
    v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
      v_root.root_timesheet_id
    );
    if v_family is null or pg_catalog.cardinality(v_family)=0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_FAMILY_UNRESOLVED'
        using errcode='55000';
    end if;
    perform 1 from public.timesheets family_row
    where family_row.timesheet_id=any(v_family)
    order by family_row.timesheet_id
    for update;
    v_family_after_lock:=private.weekly_source_invoice_family_timesheet_ids_v1(
      v_root.root_timesheet_id
    );
    if v_family_after_lock is null
       or pg_catalog.cardinality(v_family_after_lock)=0
       or not (v_family_after_lock @> v_family and v_family @> v_family_after_lock)
    then
      raise exception 'WEEKLY_SOURCE_CORRECTION_FAMILY_UNRESOLVED'
        using errcode='40001';
    end if;
    v_family:=v_family_after_lock;
    if exists(
      select 1 from public.weekly_exceptional_pay_target_families family
      where family.root_timesheet_id=any(v_family)
        and family.ownership_state='TARGET_MANAGED'
    ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PROTECTED_TARGET_EXISTS'
        using errcode='55000';
    end if;
    if exists(
      select 1 from public.invoice_lines invoice_line
      where invoice_line.timesheet_id=any(v_family)
    ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS'
        using errcode='55000';
    end if;
    if exists(
      select 1 from public.timesheets timesheet_row
      where timesheet_row.timesheet_id=v_root.root_timesheet_id
        and timesheet_row.active_document_operation_id is not null
    ) or exists(
      select 1 from public.invoice_operations operation
      where operation.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and operation.entity_type='TIMESHEET'
        and operation.entity_id=v_root.root_timesheet_id
    ) or exists(
      select 1 from public.invoice_operation_chunks chunk
      where chunk.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and chunk.entity_type='TIMESHEET'
        and chunk.entity_id=v_root.root_timesheet_id
    ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ACTIVE_INVOICE_OPERATION'
        using errcode='55000';
    end if;
    v_root_preflight:=public.import_timesheet_financial_preflight_v1(
      array[v_root.root_timesheet_id],
      'WEEKLY_SOURCE_CORRECT_FINAL_SOURCE',v_actor,'{}'::jsonb,true,1
    );
    if coalesce((v_root_preflight->>'blocking_batch_count')::integer,0)>0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ACTIVE_PAY_DRAFT'
        using errcode='55000',detail=v_root_preflight::text;
    elsif coalesce((v_root_preflight->>'paid_count')::integer,0)>0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ALREADY_PAID'
        using errcode='55000',detail=v_root_preflight::text;
    elsif coalesce((v_root_preflight->>'invoice_lined_count')::integer,0)>0 then
      raise exception 'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS'
        using errcode='55000',detail=v_root_preflight::text;
    elsif coalesce((v_root_preflight->>'allowed')::boolean,false) is not true
       or v_root_preflight->>'required_path' not in (
         -- Gate 2 / XSG-032, the same rule at COMMITTING time.
         'DIRECT_AMEND_RECALCULATE'
       ) then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ROOT_NOT_MUTABLE'
        using errcode='55000',detail=v_root_preflight::text;
    end if;
  end loop;

  update public.weekly_final_source_correction_sessions
  set state='COMMITTING',apply_idempotency_key=v_idempotency_key,
      apply_request_hash=v_apply_request_hash,reason=v_reason,version=version+1,
      updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='PREPARED' and version=v_expected_session_version
    and prepared_final_revision_id=v_prepared_revision.id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;

  update public.weekly_source_expense_materialisations materialisation
  set state='SUPERSEDED'
  where materialisation.state='MATERIALISED'
    and materialisation.expense_authority_generation_id in (
      select expense.id from public.weekly_expense_authority_generations expense
      where expense.final_revision_id=v_prior_revision.id and expense.state='CURRENT'
    );

  -- Expense authority is activated in the same CAS as source authority.  The
  -- prepared generation wins where one exists; otherwise correcting away an
  -- erroneous same-cycle generation restores its exact predecessor (or zero
  -- when there was no predecessor).
  update public.weekly_expense_authority_generations current_expense
  set state='SUPERSEDED'
  where current_expense.state='CURRENT'
    and current_expense.work_event_id in (
      select old_expense.work_event_id
      from public.weekly_expense_authority_generations old_expense
      where old_expense.final_revision_id=v_prior_revision.id
      union
      select prepared_expense.work_event_id
      from public.weekly_expense_authority_generations prepared_expense
      where prepared_expense.final_revision_id=v_prepared_revision.id
        and prepared_expense.state='PREPARED'
    );
  update public.weekly_expense_authority_generations prepared_expense
  set state='CURRENT'
  where prepared_expense.final_revision_id=v_prepared_revision.id
    and prepared_expense.state='PREPARED';
  update public.weekly_expense_authority_generations baseline_expense
  set state='CURRENT'
  where baseline_expense.state='SUPERSEDED'
    and baseline_expense.id in (
      select old_expense.prior_expense_authority_generation_id
      from public.weekly_expense_authority_generations old_expense
      where old_expense.final_revision_id=v_prior_revision.id
        and old_expense.prior_expense_authority_generation_id is not null
        and not exists(
          select 1
          from public.weekly_expense_authority_generations prepared_expense
          where prepared_expense.final_revision_id=v_prepared_revision.id
            and prepared_expense.work_event_id=old_expense.work_event_id
            and prepared_expense.state='CURRENT'
        )
    );

  perform pg_catalog.set_config(
    'cloudtms.weekly_source_correction_owner',
    'weekly_source_correct_final_apply_atomic_v1',true
  );
  select pg_catalog.count(*)::integer into v_old_movement_count
  from public.weekly_source_billing_movements
  where final_revision_id=v_prior_revision.id;
  if exists(
    select 1 from public.weekly_source_billing_movements
    where final_revision_id=v_prior_revision.id and placement_state<>'UNPLACED'
  ) or exists(
    select 1 from public.weekly_source_billing_movements
    where final_revision_id=v_prepared_revision.id and placement_state<>'UNPLACED'
  ) then
    raise exception 'WEEKLY_SOURCE_CORRECTION_MOVEMENT_STATE_STALE' using errcode='40001';
  end if;
  update public.weekly_source_billing_movements
  set placement_state='VOIDED_BY_CORRECT_FINAL'
  where final_revision_id=v_prior_revision.id and placement_state='UNPLACED';
  get diagnostics v_voided_count=row_count;
  if v_voided_count<>v_old_movement_count then
    raise exception 'WEEKLY_SOURCE_CORRECTION_MOVEMENT_CAS_LOST' using errcode='40001';
  end if;
  update public.weekly_source_final_revisions
  set state='SUPERSEDED' where id=v_prior_revision.id and state='CURRENT';
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_CAS_LOST' using errcode='40001';
  end if;
  update public.weekly_source_final_revisions
  set state='CURRENT' where id=v_prepared_revision.id and state='PREPARED';
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PREPARED_REVISION_CAS_LOST'
      using errcode='40001';
  end if;
  update public.weekly_source_uploads set state='SUPERSEDED'
  where id=v_prior_upload.id and state='CURRENT';
  get diagnostics v_cas_count=row_count;
  if v_cas_count<>1 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PRIOR_UPLOAD_CAS_LOST'
      using errcode='40001';
  end if;
  update public.weekly_source_projection_publications
  set state='STALE',failure_code='SUPERSEDED_BY_CORRECT_FINAL_SOURCE'
  where id=v_prior_publication.id and state='CURRENT';
  get diagnostics v_cas_count=row_count;
  if v_cas_count<>1 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_PRIOR_PUBLICATION_CAS_LOST'
      using errcode='40001';
  end if;
  update public.weekly_source_uploads set state='CURRENT'
  where id=v_replacement_upload.id and state='CORRECTION_READY';
  get diagnostics v_cas_count=row_count;
  if v_cas_count<>1 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_UPLOAD_CAS_LOST'
      using errcode='40001';
  end if;
  update public.weekly_source_projection_publications set state='CURRENT'
  where id=v_replacement_publication.id and state='CORRECTION_READY';
  get diagnostics v_cas_count=row_count;
  if v_cas_count<>1 then
    raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_PUBLICATION_CAS_LOST'
      using errcode='40001';
  end if;

  if v_session.authority_scope_kind='CYCLE' then
    update public.weekly_source_cycles
    set current_complete_upload_id=v_replacement_upload.id,
        current_projection_publication_id=v_replacement_publication.id,
        current_final_revision_id=v_prepared_revision.id,projection_state='CURRENT',
        state='FINALISED',finalised_at_utc=pg_catalog.transaction_timestamp(),
        finalised_by_user_id=v_actor
    where id=v_cycle.id
      and current_complete_upload_id=v_prior_upload.id
      and current_projection_publication_id=v_prior_publication.id
      and current_final_revision_id=v_prior_revision.id
      and version=v_scope_version;
  else
    update public.weekly_source_report_scopes
    set current_complete_upload_id=v_replacement_upload.id,
        current_projection_publication_id=v_replacement_publication.id,
        current_final_revision_id=v_prepared_revision.id,projection_state='CURRENT',
        state='FINALISED',updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_scope.id
      and current_complete_upload_id=v_prior_upload.id
      and current_projection_publication_id=v_prior_publication.id
      and current_final_revision_id=v_prior_revision.id
      and version=v_scope_version;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_AUTHORITY_CAS_LOST' using errcode='40001';
  end if;
  v_new_revision_id:=v_prepared_revision.id;

  update public.weekly_source_client_cycle_completions completion
  set state='SUPERSEDED',superseded_at_utc=pg_catalog.transaction_timestamp()
  where completion.source_cycle_id=v_cycle.id
    and completion.client_id=v_client_id and completion.state='CURRENT';
  select coalesce(pg_catalog.max(completion.completion_generation),0)+1
  into v_completion_generation
  from public.weekly_source_client_cycle_completions completion
  where completion.source_cycle_id=v_cycle.id and completion.client_id=v_client_id;
  v_completion_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CLIENT_CYCLE_COMPLETION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
      'client_id',v_client_id,'completion_generation',v_completion_generation,
      'completion_kind','FINAL_SOURCE','final_revision_id',v_new_revision_id,
      'actor_user_id',v_actor
    )
  );
  insert into public.weekly_source_client_cycle_completions(
    source_cycle_id,source_group_id,client_id,completion_generation,
    completion_kind,final_revision_id,attested_by_user_id,attested_at_utc,
    attestation_text,completion_hash,state
  ) values (
    v_cycle.id,v_group.id,v_client_id,v_completion_generation,
    'FINAL_SOURCE',v_new_revision_id,v_actor,pg_catalog.transaction_timestamp(),
    null,v_completion_hash,'CURRENT'
  );
  if v_session.authority_scope_kind='NHSP_REPORT_SCOPE' then
    if not exists(
      select 1
      from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and v_cycle.finalisation_week_ending between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
        and not exists(
          select 1
          from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id
            and completion.client_id=membership.client_id
            and completion.state='CURRENT'
        )
    ) then
      update public.weekly_source_cycles
      set state='FINALISED',finalised_at_utc=pg_catalog.transaction_timestamp(),
          finalised_by_user_id=v_actor
      where id=v_cycle.id;
    else
      update public.weekly_source_cycles set state='FINALISABLE'
      where id=v_cycle.id;
    end if;
  end if;

  for v_root in
    select (server_root.value->>'root_timesheet_id')::uuid as root_timesheet_id,
           (server_root.value->>'prior_projection_receipt_id')::uuid as prior_receipt_id,
           server_root.value->>'source_profile_kind' as source_profile_kind,
           server_root.value->>'source_mode' as source_mode,
           supplied.value->'service_snapshot' as service_snapshot
    from pg_catalog.jsonb_array_elements(v_server_roots) server_root(value)
    join pg_catalog.jsonb_array_elements(v_root_snapshots) supplied(value)
      on supplied.value->>'root_timesheet_id'=server_root.value->>'root_timesheet_id'
    order by server_root.value->>'root_timesheet_id'
  loop
    v_prior_receipt:=null;
    if v_root.prior_receipt_id is not null then
      select * into strict v_prior_receipt
      from public.weekly_source_ordinary_pay_projection_receipts
      where id=v_root.prior_receipt_id;
      if v_prior_receipt.final_revision_id is distinct from v_prior_revision.id
         or v_prior_receipt.root_timesheet_id is distinct from v_root.root_timesheet_id
         or v_prior_receipt.outcome not in ('PREPARED_FOR_AUTHORISATION','PROPOSED') then
        raise exception 'WEEKLY_SOURCE_CORRECTION_PRIOR_ROOT_RECEIPT_STALE'
          using errcode='40001';
      end if;
    end if;
    v_impact_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CORRECT_FINAL_ROOT_IMPACT_V1',
      pg_catalog.jsonb_build_object(
        'correction_session_id',v_session.id,
        'prior_final_revision_id',v_prior_revision.id,
        'replacement_final_revision_id',v_new_revision_id,
        'root_timesheet_id',v_root.root_timesheet_id,
        'prior_projection_receipt_id',v_root.prior_receipt_id,
        'client_id',v_client_id,
        'source_profile_kind',v_root.source_profile_kind,
        'source_mode',v_root.source_mode,
        'submitted_service_snapshot_hash',pg_catalog.encode(
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_ORDINARY_SERVICE_SNAPSHOT_V1',v_root.service_snapshot
          ),'hex'
        )
      )
    );
    insert into public.weekly_final_source_correction_root_impacts(
      correction_session_id,prior_final_revision_id,replacement_final_revision_id,
      root_timesheet_id,prior_projection_receipt_id,client_id,source_profile_kind,
      source_mode,impact_kind,submitted_service_snapshot_hash,impact_hash
    ) values (
      v_session.id,v_prior_revision.id,v_new_revision_id,v_root.root_timesheet_id,
      v_root.prior_receipt_id,v_client_id,v_root.source_profile_kind,v_root.source_mode,
      case when v_root.prior_receipt_id is null
        then 'PUBLISHED_REPLACEMENT_ONLY_ROOT'
        when exists(
        select 1 from public.weekly_source_billing_movements movement
        where movement.final_revision_id=v_new_revision_id
          and movement.invoice_timesheet_id=v_root.root_timesheet_id
      ) then 'REPROJECTED_WITH_CURRENT_REVISION_MOVEMENTS'
        else 'REPROJECTED_WITHOUT_CURRENT_REVISION_MOVEMENTS' end,
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_ORDINARY_SERVICE_SNAPSHOT_V1',v_root.service_snapshot
      ),v_impact_hash
    );
    v_projection_result:=public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1',
        'actor_user_id',v_actor,'final_revision_id',v_new_revision_id,
        'root_timesheet_id',v_root.root_timesheet_id,
        'idempotency_key','correct-final:'||v_session.id::text||':'||v_root.root_timesheet_id::text,
        'service_snapshot',v_root.service_snapshot
      )
    );
    -- Gate 2: the projection no longer publishes.  The only outcome reachable
    -- from here is PREPARED_FOR_AUTHORISATION, because the gates above now
    -- admit DIRECT_AMEND_RECALCULATE only, i.e. a root that is not yet
    -- authorised.  Anything else means the root moved under us.
    if v_projection_result->>'outcome'<>'PREPARED_FOR_AUTHORISATION' then
      raise exception 'WEEKLY_SOURCE_CORRECTION_ROOT_REPROJECT_FAILED'
        using errcode='55000',detail=v_projection_result::text;
    end if;
  end loop;

  v_result:=(v_session.prepare_result_json-'root_contexts')||pg_catalog.jsonb_build_object(
    'status','CORRECTED','correction_session_id',v_session.id,
    'prior_final_revision_id',v_prior_revision.id,
    'affected_root_count',pg_catalog.jsonb_array_length(v_server_roots),
    'version',v_expected_session_version+2,
    'idempotent_replay',false
  );
  v_result_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CORRECT_FINAL_RESULT_V1',v_result
  );
  update public.weekly_final_source_correction_sessions
  set state='APPLIED',applied_final_revision_id=v_new_revision_id,
      result_json=v_result,result_hash=v_result_hash,version=version+1,
      updated_at_utc=pg_catalog.transaction_timestamp(),
      completed_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='COMMITTING';
  if not found then
    raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_CAS_LOST' using errcode='40001';
  end if;
  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'weekly_final_source_correction_sessions',v_session.id::text,
         'WEEKLY_SOURCE_CORRECT_FINAL_APPLIED',
         pg_catalog.jsonb_build_object(
           'state','CURRENT','final_revision_id',v_prior_revision.id,
           'upload_id',v_prior_upload.id,'projection_publication_id',v_prior_publication.id
         ),
         pg_catalog.jsonb_build_object(
           'state','APPLIED','final_revision_id',v_new_revision_id,
           'upload_id',v_replacement_upload.id,
           'projection_publication_id',v_replacement_publication.id,
           'affected_root_count',pg_catalog.jsonb_array_length(v_server_roots),
           'voided_prior_movement_count',v_voided_count,
           'result_hash',pg_catalog.encode(v_result_hash,'hex')
         ),v_reason
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;
  return v_result;
end;
$function$;

alter function private.weekly_source_correct_final_preconditions_v1(uuid,uuid,uuid,boolean)
  owner to postgres;
alter function private.weekly_source_correct_final_prepared_context_v1(uuid)
  owner to postgres;
alter function private.weekly_source_correct_final_office_preview_v1(uuid)
  owner to postgres;
alter function public.weekly_source_correct_final_open_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_correct_final_review_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_correct_final_prepare_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_correct_final_apply_atomic_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_correct_final_preconditions_v1(uuid,uuid,uuid,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_correct_final_prepared_context_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_correct_final_office_preview_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_correct_final_open_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_correct_final_review_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_correct_final_prepare_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_correct_final_apply_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_correct_final_open_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_correct_final_review_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_correct_final_prepare_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_correct_final_apply_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_correct_final_open_atomic_v1(jsonb) is
  'Opens an audited same-cycle final-source correction only while all affected Weekly roots remain outside invoices, Drafts, reservations, frozen or paid payment state.';
comment on function public.weekly_source_correct_final_review_atomic_v1(jsonb) is
  'Seals a plain-English Changes and Blocked review from immutable staged correction rows without creating or changing live Timesheets, lineage, source authority, pay, invoice, query or token state.';
comment on function public.weekly_source_correct_final_prepare_atomic_v1(jsonb) is
  'Materialises and seals an inactive immutable replacement revision while preserving the old CURRENT source authority. Returns exact root contexts for service-owned economic snapshot calculation.';
comment on function public.weekly_source_correct_final_apply_atomic_v1(jsonb) is
  'Atomically supersedes a mistaken final source and finalises the proved replacement publication. Gate 2 / XSG-032: this is the narrow same-cycle correction for roots that are NOT yet authorised, so it never unauthorises, never overwrites submitted schedule evidence, never rotates a current TSFIN and never reauthorises. An already-authorised root is refused here and goes to proposal and Office decision instead. It never writes Banking Pay or Workbench state.';

notify pgrst, 'reload schema';

commit;
