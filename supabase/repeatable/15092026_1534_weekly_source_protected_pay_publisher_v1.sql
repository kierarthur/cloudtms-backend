-- Repeatable CloudTMS function/view authority: weekly_source_protected_pay_publisher_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_exceptional_json_keys_exact_v1(
  p_value jsonb,
  p_allowed_keys text[]
) returns boolean
language sql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_typeof(p_value)='object'
    and not exists(
      select 1
      from pg_catalog.jsonb_object_keys(p_value) supplied(key)
      where not (supplied.key=any(p_allowed_keys))
    );
$function$;

-- Return only a proved Candidate signature for one current Weekly evidence
-- Timesheet.  Office authorisation is deliberately not a signature: it may
-- follow a source-only path and must never make unsigned Candidate evidence
-- look signed.  A protected shift can still be created without any Timesheet;
-- callers express that case with a null evidence_timesheet_id.
create or replace function private.weekly_exceptional_candidate_signed_evidence_v1(
  p_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_hash bytea;
  v_signed_at timestamptz;
  v_kind text;
begin
  select timesheet.* into strict v_timesheet
  from public.timesheets timesheet
  where timesheet.timesheet_id=p_timesheet_id
    and timesheet.sheet_scope='WEEKLY'::public.timesheet_scope_enum
    and timesheet.is_current
    and timesheet.revoked_at is null
    and timesheet.archived_at_utc is null;

  if v_timesheet.candidate_workflow_id is not null then
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=v_timesheet.candidate_workflow_id
      and workflow.generation=v_timesheet.candidate_workflow_generation
      and workflow.scope='WEEKLY'
      and workflow.contract_id=v_timesheet.contract_id
      and workflow.week_ending_date=v_timesheet.week_ending_date
      and workflow.candidate_signature_component_id is not null
      and workflow.candidate_signature_sha256 is not null
      and workflow.candidate_signed_at_utc is not null
      and workflow.worker_submitted_at_utc is not null
      and workflow.state not in (
        'CREATED','WORKER_DRAFT','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
      )
      and (
        workflow.target_timesheet_id=v_timesheet.timesheet_id
        or workflow.anchor_timesheet_id=v_timesheet.timesheet_id
      );
    if found then
      v_hash:=v_workflow.candidate_signature_sha256;
      v_signed_at:=v_workflow.candidate_signed_at_utc;
      v_kind:='CANDIDATE_WORKFLOW';
    end if;
  end if;

  if v_hash is null
     and v_timesheet.qr_status='USED'::public.timesheet_qr_status_enum
     and v_timesheet.qr_scanned_at is not null
     and v_timesheet.qr_signed_at_utc is not null
     and coalesce(v_timesheet.qr_signed_hash,'')~'^[0-9a-fA-F]{64}$' then
    v_hash:=pg_catalog.decode(pg_catalog.lower(v_timesheet.qr_signed_hash),'hex');
    v_signed_at:=v_timesheet.qr_signed_at_utc;
    v_kind:='SIGNED_QR';
  end if;

  if v_hash is null
     and nullif(pg_catalog.btrim(coalesce(v_timesheet.r2_nurse_key,'')),'') is not null
     and coalesce(v_timesheet.img_sha256_nurse,'')~'^[0-9a-fA-F]{64}$' then
    v_hash:=pg_catalog.decode(pg_catalog.lower(v_timesheet.img_sha256_nurse),'hex');
    v_signed_at:=v_timesheet.created_at;
    v_kind:='CANDIDATE_SIGNATURE_ASSET';
  end if;

  if v_hash is null or v_signed_at is null then
    raise exception 'WEEKLY_PROTECTED_EVIDENCE_UNSIGNED' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'timesheet_id',v_timesheet.timesheet_id,
    'contract_id',v_timesheet.contract_id,
    'week_ending_date',v_timesheet.week_ending_date,
    'timesheet_version',v_timesheet.version,
    'signature_kind',v_kind,
    'signature_sha256',pg_catalog.encode(v_hash,'hex'),
    'signed_at_utc',v_signed_at
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_EVIDENCE_INVALID' using errcode='55000';
end;
$function$;

create or replace function public.weekly_exceptional_pay_prepare_family_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_cycle_id','candidate_id','client_id','contract_id',
    'week_ending_date','work_event_id','work_date','start_at_local',
    'end_at_local','break_minutes','evidence_timesheet_id','reason',
    'idempotency_key'
  ];
  v_actor_user_id uuid;
  v_source_cycle_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_week_start_date date;
  v_work_event_id uuid;
  v_work_date date;
  v_start_at_local timestamp without time zone;
  v_end_at_local timestamp without time zone;
  v_break_minutes integer;
  v_evidence_timesheet_id uuid;
  v_reason text;
  v_idempotency_key text;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_contract public.contracts%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_root public.timesheets%rowtype;
  v_evidence public.timesheets%rowtype;
  v_signed_evidence jsonb;
  v_candidate public.candidates%rowtype;
  v_client public.clients%rowtype;
  v_policy jsonb;
  v_source_mode text;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_occupant_norm text;
  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_booking_material text;
  v_booking_id text;
  v_work_event_hash bytea;
  v_first_evidence_hash bytea;
  v_request_hash bytea;
  v_before_hash bytea;
  v_created_root boolean:=false;
  v_created_family boolean:=false;
  v_created_event boolean:=false;
  v_replay boolean:=false;
  v_request_kind text:='APPROVE';
  v_lock_result jsonb;
  v_family_identity jsonb;
  v_known_root_timesheet_id uuid;
begin
  if not private.weekly_exceptional_json_keys_exact_v1(p_request,v_allowed_keys)
     or not (p_request ?& array[
       'actor_user_id','source_cycle_id','candidate_id','client_id','contract_id',
       'week_ending_date','work_date','start_at_local','end_at_local',
       'break_minutes','reason','idempotency_key'
     ]) then
    raise exception 'WEEKLY_PROTECTED_FAMILY_REQUEST_INVALID' using errcode='22023';
  end if;

  begin
    v_actor_user_id:=(p_request->>'actor_user_id')::uuid;
    v_source_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_candidate_id:=(p_request->>'candidate_id')::uuid;
    v_client_id:=(p_request->>'client_id')::uuid;
    v_contract_id:=(p_request->>'contract_id')::uuid;
    v_week_ending_date:=(p_request->>'week_ending_date')::date;
    v_work_event_id:=nullif(p_request->>'work_event_id','')::uuid;
    v_work_date:=(p_request->>'work_date')::date;
    v_start_at_local:=(p_request->>'start_at_local')::timestamp without time zone;
    v_end_at_local:=(p_request->>'end_at_local')::timestamp without time zone;
    v_break_minutes:=(p_request->>'break_minutes')::integer;
    v_evidence_timesheet_id:=nullif(p_request->>'evidence_timesheet_id','')::uuid;
  exception when others then
    raise exception 'WEEKLY_PROTECTED_FAMILY_REQUEST_INVALID' using errcode='22023';
  end;
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  v_idempotency_key:=pg_catalog.btrim(coalesce(p_request->>'idempotency_key',''));
  v_week_start_date:=v_week_ending_date-6;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_PREPARE_FAMILY_REQUEST_V1',p_request
  );

  if v_break_minutes is null or v_break_minutes<0
     or v_end_at_local<=v_start_at_local
     or v_break_minutes>=extract(epoch from (v_end_at_local-v_start_at_local))/60
     or v_work_date<>v_start_at_local::date
     or v_work_date not between v_week_start_date and v_week_ending_date
     or pg_catalog.char_length(v_reason) not between 1 and 1000
     or pg_catalog.char_length(v_idempotency_key) not between 16 and 200 then
    raise exception 'WEEKLY_PROTECTED_FAMILY_REQUEST_INVALID' using errcode='22023';
  end if;

  -- Review F1 and F4.  The I-1 lock set must be the FIRST lock this owner
  -- takes: no row lock, no FOR UPDATE and no other advisory key before it,
  -- because every installed rotation owner takes the family advisory key and
  -- the family rows before anything else (core 08082026_2035_...:126-131,
  -- legacy 16122025_...:844-849, confirmed route :2374-2376).  This owner
  -- previously locked the orchestration run, the cycle, the Contract and the
  -- Contract Week first and asked for the family keys afterwards, which is a
  -- proven deadlock (reproductions D1b and D2).  The root is discovered with a
  -- PLAIN read from the request's Contract and week.
  select contract_week.timesheet_id into v_known_root_timesheet_id
  from public.contract_weeks contract_week
  where contract_week.contract_id=v_contract_id
    and contract_week.week_ending_date=v_week_ending_date
    and contract_week.additional_seq=0;

  if v_known_root_timesheet_id is not null then
    v_lock_result:=private.weekly_source_lock_family_rows_v1(
      array[v_known_root_timesheet_id]::uuid[],v_candidate_id
    );
    if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
      raise exception '%',v_lock_result->>'code'
        using errcode='55000',detail=v_lock_result::text;
    end if;
  end if;

  -- Serialize the global idempotency namespace before looking up the run.
  -- Without this lock, two first attempts can both observe no row and the
  -- loser receives a raw unique violation rather than an exact replay.  It is
  -- a Weekly-Source-private key that no rotation owner takes, and it is now
  -- requested after the family keys.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-exceptional-orchestration|'||v_idempotency_key,0
  ));

  -- A replay is recognized before any mutable work, but every immutable scope
  -- fact is still compared below.  A reused key can never name another family.
  select run.* into v_run
  from public.weekly_exceptional_orchestration_runs run
  where run.idempotency_key=v_idempotency_key
  for update;
  if found then
    v_replay:=true;
  end if;

  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle
  where cycle.id=v_source_cycle_id
  for update;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id
    and source_group.active
  for share;

  perform private.weekly_source_office_authority_v1(
    v_actor_user_id,'APPROVE_PROTECTED_PAY',v_group.id,v_client_id,v_work_date
  );

  -- The source cycle records when Office discovered or is reconciling the
  -- protected item.  It is deliberately not required to be the work week:
  -- a missing or queried historic shift can remain protected for later cycles.
  if not exists(
       select 1 from public.weekly_source_group_clients membership
       where membership.source_group_id=v_group.id
         and membership.client_id=v_client_id
         and v_work_date between membership.valid_from
           and coalesce(membership.valid_to,'infinity'::date)
     ) then
    raise exception 'WEEKLY_PROTECTED_FAMILY_SCOPE_INVALID' using errcode='55000';
  end if;

  select contract.* into strict v_contract
  from public.contracts contract
  where contract.id=v_contract_id
  for update;
  if v_contract.candidate_id is distinct from v_candidate_id
     or v_contract.client_id is distinct from v_client_id
     or v_work_date not between v_contract.start_date and v_contract.end_date
     or v_contract.week_ending_weekday_snapshot not between 0 and 6
     or v_week_ending_date<>v_work_date+
       ((v_contract.week_ending_weekday_snapshot-
         extract(dow from v_work_date)::integer+7)%7) then
    raise exception 'WEEKLY_PROTECTED_CONTRACT_SCOPE_INVALID' using errcode='55000';
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_client_id,v_contract_id,v_work_date
  );
  v_source_mode:=v_policy->>'c1_source_mode';
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY') then
    raise exception 'WEEKLY_PROTECTED_POLICY_INVALID' using errcode='55000';
  end if;

  select candidate.* into strict v_candidate
  from public.candidates candidate where candidate.id=v_candidate_id;
  select client.* into strict v_client
  from public.clients client where client.id=v_client_id;

  -- G6-9 / WB-016.  Note on the Candidate serial gate (proof/32 section 6
  -- step 1): preparing the protected-pay family is not one of the four pinned
  -- Weekly Source job types, so this owner does not take the gate; the
  -- first-authorisation, publication and release owners do, through interface
  -- I-1.  It does take the full rotation lock set below (proof/34 rules 1, 6
  -- and 11), and adds no lock of its own on any Banking Pay table.

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_BASE_WEEK|'||v_contract_id::text||'|'||v_week_ending_date::text,0
  ));
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id,is_adjustment,created_at,updated_at
  ) values (
    v_contract_id,v_week_ending_date,0,'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,null,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) on conflict (contract_id,week_ending_date,additional_seq) do nothing;
  select contract_week.* into strict v_contract_week
  from public.contract_weeks contract_week
  where contract_week.contract_id=v_contract_id
    and contract_week.week_ending_date=v_week_ending_date
    and contract_week.additional_seq=0
  for update;
  if v_contract_week.is_adjustment
     or v_contract_week.status='CANCELLED'::public.contract_week_status_enum then
    raise exception 'WEEKLY_PROTECTED_BASE_WEEK_INVALID' using errcode='55000';
  end if;

  -- Review G1.  Under the locks the Contract Week must still name the root
  -- whose family this transaction locked FIRST - including the null to non-null
  -- case, where the week gained a root between the plain read and here.  The
  -- earlier form guarded this with "v_known_root_timesheet_id is not null", so
  -- that case fell through to "select timesheet ... for update" before the
  -- family advisory keys, which is the reverse of every installed rotation
  -- owner and a proven deadlock.  This is the ensure owner's own unconditional
  -- check, in the same place relative to the locked read.
  if v_contract_week.timesheet_id is distinct from v_known_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'expected_root_timesheet_id',v_known_root_timesheet_id,
          'observed_root_timesheet_id',v_contract_week.timesheet_id
        )::text;
  end if;

  if v_contract_week.timesheet_id is null then
    if v_contract_week.status in (
      'AUTHORISED'::public.contract_week_status_enum,
      'INVOICED'::public.contract_week_status_enum,
      'CANCELLED'::public.contract_week_status_enum
    ) then
      raise exception 'WEEKLY_PROTECTED_FINAL_WEEK_TIMESHEET_MISSING' using errcode='55000';
    end if;
    v_occupant_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_candidate.tms_ref),''),
      nullif(pg_catalog.btrim(v_candidate.display_name),''),v_candidate.id::text
    ));
    v_hospital_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.display_site),''),
      nullif(pg_catalog.btrim(v_client.name),''),v_client.id::text
    ));
    v_ward_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.ward_hint),''),'contract'
    ));
    v_role_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.role),''),'weekly'
    ));
    v_booking_material:='weekly-source|'||v_contract_id::text||'|'||v_week_ending_date::text;
    v_booking_id:='bk_'||pg_catalog.substr(pg_catalog.encode(
      extensions.digest(pg_catalog.convert_to(v_booking_material,'UTF8'),'sha256'),'hex'
    ),1,24);
    -- A minted booking id must not collide with an existing family, and must
    -- not differ from an existing one by surrounding whitespace only: the
    -- confirmed rotation route serialises on the trimmed key while family
    -- membership is by the raw value, so such a pair is an ambiguous family.
    if exists(
      select 1 from public.timesheets existing
      where (existing.booking_id=v_booking_id and existing.is_current)
         or pg_catalog.btrim(existing.booking_id)=v_booking_id
    ) then
      raise exception 'WEEKLY_SOURCE_BOOKING_ID_COLLISION' using errcode='55000';
    end if;
    insert into public.timesheets(
      booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
      occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
      week_ending_date,contract_id,actual_schedule_json,qr_payload_json,
      is_adjustment,created_at,updated_at
    ) values (
      v_booking_id,1,true,'RECEIVED'::public.timesheet_status_enum,
      'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,v_occupant_norm,v_hospital_norm,
      v_ward_norm,v_role_norm,'weekly-0',v_week_ending_date,v_contract_id,
      '[]'::jsonb,'{}'::jsonb,false,
      pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
    ) returning * into v_root;
    v_created_root:=true;
    update public.contract_weeks
    set timesheet_id=v_root.timesheet_id,
        status='SUBMITTED'::public.contract_week_status_enum,
        updated_at=pg_catalog.statement_timestamp()
    where id=v_contract_week.id
    returning * into v_contract_week;
  else
    select timesheet.* into strict v_root
    from public.timesheets timesheet
    where timesheet.timesheet_id=v_contract_week.timesheet_id
    for update;
  end if;

  -- G6-9 / proof/34 section 5 step 2 and rule 6.  A stored contract-week
  -- Timesheet id, or a freshly minted one, is only a lookup key: take the
  -- deadlock-free rotation lock set (trimmed key, raw key when different, raw
  -- booking family rows FOR UPDATE) and re-resolve the canonical current
  -- version through the installed Workbench authority before trusting it.
  -- For an existing root the lock set was already taken, before every other
  -- lock, so this call is re-entrant and re-resolves under the locks.  For a
  -- root this transaction has just minted, no other session can know the
  -- booking id yet, so taking the family keys here cannot join a cycle.
  v_lock_result:=private.weekly_source_lock_family_rows_v1(
    array[v_root.timesheet_id]::uuid[],v_candidate_id
  );
  if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
    raise exception '%',v_lock_result->>'code'
      using errcode='55000',detail=v_lock_result::text;
  end if;
  v_family_identity:=v_lock_result->'families'->0;

  -- The root this owner ends up holding must still be the one whose family was
  -- locked first, unless this transaction minted it itself.
  if not v_created_root
     and v_root.timesheet_id is distinct from v_known_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK'
      using errcode='40001';
  end if;

  -- proof/34 section 5 step 3: a changed current id makes the request stale.
  -- The older id is never used and nothing is written.
  if coalesce((v_family_identity->>'requested_is_canonical')::boolean,false) is not true
     or coalesce((v_family_identity->>'family_is_current')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION'
      using errcode='55000',detail=v_family_identity::text;
  end if;

  -- Re-read the root under the lock before any fact derived from it is frozen.
  select timesheet.* into strict v_root
  from public.timesheets timesheet
  where timesheet.timesheet_id=v_root.timesheet_id;
  if v_root.booking_id is distinct from (v_family_identity->>'family_booking_id')
     or v_root.version is distinct from (v_family_identity->>'canonical_version')::integer then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',detail=v_family_identity::text;
  end if;

  if v_root.contract_id is distinct from v_contract_id
     or v_root.week_ending_date is distinct from v_week_ending_date
     or v_root.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     or v_root.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_root.is_adjustment
     or not v_root.is_current
     or v_root.revoked_at is not null
     or v_root.archived_at_utc is not null then
    raise exception 'WEEKLY_PROTECTED_ROOT_INVALID' using errcode='55000';
  end if;

  if v_evidence_timesheet_id is not null then
    v_signed_evidence:=private.weekly_exceptional_candidate_signed_evidence_v1(
      v_evidence_timesheet_id
    );
    if (v_signed_evidence->>'contract_id')::uuid is distinct from v_contract_id
       or (v_signed_evidence->>'week_ending_date')::date is distinct from v_week_ending_date then
      raise exception 'WEEKLY_PROTECTED_EVIDENCE_INVALID' using errcode='55000';
    end if;
  end if;

  if v_work_event_id is not null then
    select event.id into strict v_work_event_id
    from public.weekly_work_events event
    where event.id=v_work_event_id
      and event.candidate_id=v_candidate_id
      and event.client_id=v_client_id
      and event.work_date=v_work_date
    for share;
  else
    v_work_event_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_OFFICE_PROTECTED_WORK_EVENT_V1',
      pg_catalog.jsonb_build_object(
        'agency_id',v_group.agency_id,'candidate_id',v_candidate_id,
        'client_id',v_client_id,'work_date',v_work_date,
        'start_at_local',v_start_at_local,'end_at_local',v_end_at_local,
        'break_minutes',v_break_minutes
      )
    );
    insert into public.weekly_work_events(
      candidate_id,client_id,work_date,identity_kind,profile_external_key,
      durable_identity_hash,first_source_group_id,source_format_profile_id
    ) values (
      v_candidate_id,v_client_id,v_work_date,'OFFICE_PROTECTED_SHIFT',null,
      v_work_event_hash,v_group.id,null
    ) on conflict (durable_identity_hash) do nothing
    returning id into v_work_event_id;
    if found then
      v_created_event:=true;
    else
      select event.id into strict v_work_event_id
      from public.weekly_work_events event
      where event.durable_identity_hash=v_work_event_hash
        and event.candidate_id=v_candidate_id
        and event.client_id=v_client_id
        and event.work_date=v_work_date
        and event.identity_kind='OFFICE_PROTECTED_SHIFT';
    end if;
  end if;

  v_first_evidence_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_FIRST_EVIDENCE_V1',
    pg_catalog.jsonb_build_object(
      'agency_id',v_group.agency_id,'candidate_id',v_candidate_id,
      'client_id',v_client_id,'contract_id',v_contract_id,
      'week_ending_date',v_week_ending_date,'root_timesheet_id',v_root.timesheet_id,
      'evidence_timesheet_id',v_evidence_timesheet_id,'work_event_id',v_work_event_id,
      'work_date',v_work_date,'start_at_local',v_start_at_local,
      'end_at_local',v_end_at_local,'break_minutes',v_break_minutes
    )
  );

  select family.* into v_family
  from public.weekly_exceptional_pay_target_families family
  where family.agency_id=v_group.agency_id
    and family.candidate_id=v_candidate_id
    and family.contract_id=v_contract_id
    and family.week_ending_date=v_week_ending_date
  for update;
  if found then
    if v_family.root_timesheet_id is distinct from v_root.timesheet_id
       or v_family.week_start_date is distinct from v_week_start_date
       or v_family.agency_id is distinct from v_group.agency_id
       or v_family.candidate_id is distinct from v_candidate_id
       or v_family.contract_id is distinct from v_contract_id
       or (v_replay and v_run.family_id is distinct from v_family.id) then
      raise exception 'WEEKLY_PROTECTED_FAMILY_SCOPE_INVALID' using errcode='55000';
    end if;
    -- One family is the complete Candidate + Contract + week entitlement.
    -- A later protected shift or an amended protected schedule is another
    -- immutable orchestration run against that same family, never a second
    -- public Timesheet and never a duplicate family.
    if v_replay then
      if v_run.request_kind not in ('APPROVE','AMEND') then
        raise exception 'WEEKLY_PROTECTED_REPLAY_SCOPE_INVALID' using errcode='55000';
      end if;
      v_request_kind:=v_run.request_kind;
    else
      if v_family.c1_publication_state in ('PENDING','PUBLISHING') then
        raise exception 'WEEKLY_PROTECTED_PUBLICATION_BUSY' using errcode='55000';
      end if;
      v_request_kind:='AMEND';
    end if;
  else
    if v_replay then
      raise exception 'WEEKLY_PROTECTED_REPLAY_SCOPE_INVALID' using errcode='55000';
    end if;
    insert into public.weekly_exceptional_pay_target_families(
      agency_id,candidate_id,contract_id,week_start_date,week_ending_date,
      root_timesheet_id,ownership_state,first_signed_evidence_fingerprint,
      current_lifecycle_state,c1_publication_state,creation_idempotency_key
    ) values (
      v_group.agency_id,v_candidate_id,v_contract_id,v_week_start_date,
      v_week_ending_date,v_root.timesheet_id,'TARGET_MANAGED',v_first_evidence_hash,
      'PENDING_APPROVAL','NONE',v_idempotency_key
    ) returning * into v_family;
    v_created_family:=true;
    v_request_kind:='APPROVE';
  end if;

  v_before_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_PROTECTED_PREPARE_BEFORE_V1',
    pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'bound_version',v_family.bound_version,
      'root_timesheet_id',v_root.timesheet_id,
      'root_version',v_root.version,'contract_week_id',v_contract_week.id,
      'current_generation_id',v_family.current_generation_id,
      'current_generation_number',v_family.current_generation_number,
      'family_bound_version',v_family.bound_version,
      'work_event_id',v_work_event_id,
      'work_date',v_work_date,'start_at_local',v_start_at_local,
      'end_at_local',v_end_at_local,'break_minutes',v_break_minutes,
      'evidence_timesheet_id',v_evidence_timesheet_id,
      'request_kind',v_request_kind
    )
  );
  if not v_replay then
    insert into public.weekly_exceptional_orchestration_runs(
      family_id,request_kind,idempotency_key,requested_by_user_id,state,
      request_fingerprint,before_state_fingerprint
    ) values (
      v_family.id,v_request_kind,v_idempotency_key,v_actor_user_id,'RUNNING',
      v_request_hash,v_before_hash
    ) returning * into v_run;
  elsif v_run.request_kind<>v_request_kind
     or v_run.requested_by_user_id is distinct from v_actor_user_id
     or v_run.request_fingerprint is distinct from v_request_hash then
    raise exception 'WEEKLY_PROTECTED_REPLAY_SCOPE_INVALID' using errcode='55000';
  end if;

  select financial.* into v_fin
  from public.timesheets_financials financial
  where financial.timesheet_id=v_root.timesheet_id
    and financial.is_current
  for share;

  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,
    object_type,object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor_user_id,actor.display_name,actor.role,
    'weekly_exceptional_pay_target_families',v_family.id::text,
    case when v_created_family then 'WEEKLY_PROTECTED_FAMILY_PREPARED'
      else 'WEEKLY_PROTECTED_FAMILY_PREPARE_REPLAY' end,
    null,pg_catalog.jsonb_build_object(
      'family_id',v_family.id,'source_cycle_id',v_source_cycle_id,
      'work_event_id',v_work_event_id,'root_timesheet_id',v_root.timesheet_id,
      'contract_week_id',v_contract_week.id,'candidate_id',v_candidate_id,
      'client_id',v_client_id,'contract_id',v_contract_id,
      'week_ending_date',v_week_ending_date,'source_mode',v_source_mode,
      'request_kind',v_request_kind,
      'created_root',v_created_root,'created_work_event',v_created_event,
      'requires_zero_financial',v_fin.id is null
    ),v_reason
  from public.tms_users actor where actor.id=v_actor_user_id;

  return pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'orchestration_run_id',v_run.id,
    'source_cycle_id',v_source_cycle_id,'source_group_id',v_group.id,
    'agency_id',v_group.agency_id,'candidate_id',v_candidate_id,
    'client_id',v_client_id,'contract_id',v_contract_id,
    'contract_week_id',v_contract_week.id,'root_timesheet_id',v_root.timesheet_id,
    'work_event_id',v_work_event_id,'week_start_date',v_week_start_date,
    'week_ending_date',v_week_ending_date,'source_mode',v_source_mode,
    'root_version',v_root.version,'financial_row_id',v_fin.id,
    'requires_zero_financial',v_fin.id is null,
    'created_root',v_created_root,'created_family',v_created_family,
    'created_work_event',v_created_event,'idempotent_replay',v_replay,
    'family_bound_version',v_family.bound_version,'request_kind',v_request_kind,
    'run_state',v_run.state
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_PROTECTED_FAMILY_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function private.weekly_exceptional_json_keys_exact_v1(jsonb,text[]) owner to postgres;
alter function private.weekly_exceptional_candidate_signed_evidence_v1(uuid) owner to postgres;
alter function public.weekly_exceptional_pay_prepare_family_v1(jsonb) owner to postgres;

revoke all on function private.weekly_exceptional_json_keys_exact_v1(jsonb,text[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_exceptional_candidate_signed_evidence_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_prepare_family_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_prepare_family_v1(jsonb)
  to service_role;

comment on function public.weekly_exceptional_pay_prepare_family_v1(jsonb) is
  'Service-only, idempotent preparation of the sole ordinary Weekly root and protected-pay family. Creates no protected payment, residual, Draft, recovery, invoice or Banking Pay row.';

notify pgrst, 'reload schema';

commit;
