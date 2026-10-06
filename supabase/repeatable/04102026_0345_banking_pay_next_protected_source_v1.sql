-- One protected approval -> one complete immutable SOURCE work revision and
-- one ordered POSITION_APPLY receipt. No C1/head fabrication or finance drain.
\set ON_ERROR_STOP on
begin;
\ir includes/05102026_0659_bpay_next_protected_owner_route_v1.sqlinc

-- BEGIN SOURCE LOCAL EXCLUSIVE ENTRY HELPERS
-- Source owns the nine-key accepted Local proof. Banking adds only an exact
-- original common-HEAD -> sealed revision/publication mapping, never a latest
-- winner or a receipt inferred from Local COMPLETE/PUBLISHED labels.
create or replace function private.bpay_next_protected_local_receipt_v1(
  p_family_id uuid,p_run_id uuid,p_actor_id uuid
) returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_saved jsonb;
  v_local private.weekly_source_local_protected_decision_receipts%rowtype;
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_mapping jsonb:=null;
  v_keys constant text[]:=array['family_id','orchestration_run_id','publication_request_id','request_sha256',
    'generation_id','state','requires_first_authorisation','idempotent_replay','result'];
begin
  if p_family_id is null or p_run_id is null or p_actor_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_LOCAL_SCOPE_REQUIRED';
  end if;
  if pg_catalog.to_regprocedure('private.weekly_source_local_saved_status_v1(uuid,uuid,uuid)') is null then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_STATUS_UNAVAILABLE';
  end if;
  v_saved:=private.weekly_source_local_saved_status_v1(p_family_id,p_run_id,p_actor_id);
  if v_saved is null then return null;end if;
  if pg_catalog.jsonb_typeof(v_saved) is distinct from 'object'
     or not private.weekly_exceptional_json_keys_exact_v1(v_saved,v_keys) or not (v_saved ?& v_keys)
     or v_saved->>'family_id' is distinct from p_family_id::text
     or v_saved->>'orchestration_run_id' is distinct from p_run_id::text
     or v_saved->'idempotent_replay' is distinct from 'true'::jsonb
     or pg_catalog.jsonb_typeof(v_saved->'requires_first_authorisation') is distinct from 'boolean'
     or pg_catalog.jsonb_typeof(v_saved->'result') is distinct from 'object'
     or coalesce(v_saved->>'state','') not in ('COMPLETE','PENDING_FREEZE')
     or coalesce(v_saved->>'request_sha256','') !~ '^[0-9a-f]{64}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_LOCAL_RECEIPT_NOT_EXACT';
  end if;
  select l.* into strict v_local from private.weekly_source_local_protected_decision_receipts l
    join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
    where l.publication_request_id=(v_saved->>'publication_request_id')::uuid
      and request.orchestration_run_id=p_run_id and l.family_id=p_family_id and l.actor_user_id=p_actor_id
      and l.generation_id=(v_saved->>'generation_id')::uuid and l.state=v_saved->>'state'
      and pg_catalog.encode(l.request_sha256,'hex')=v_saved->>'request_sha256';
  if exists(select 1 from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=p_run_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_OWNER_CONFLICT';
  end if;
  if v_saved->'requires_first_authorisation'='true'::jsonb then
    if v_local.common_decision_bundle_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_LOCAL_RECEIPT_NOT_EXACT';
    end if;
  elsif v_local.state='COMPLETE' then
    if v_local.publication_origin_kind is distinct from 'PROTECTED_LOCAL_DECISION_V1'
       or v_local.publication_origin_digest is null or v_local.source_qualification_digest is null then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_LOCAL_RECEIPT_NOT_EXACT';
    end if;
    select b.* into strict v_bundle from public.weekly_source_entitlement_decision_bundles b
      where b.decision_bundle_id=v_local.common_decision_bundle_id and b.bundle_revision=v_local.common_bundle_revision
        and b.bundle_kind='SINGLE_ROOT' and b.state in ('COMMITTED','SUPERSEDED')
        and pg_catalog.cardinality(b.proposed_head_ids)=1 and b.source_root_timesheet_id=v_local.root_timesheet_id;
    select h.* into strict v_head from public.weekly_source_entitlement_heads h
      where h.id=v_bundle.proposed_head_ids[1] and h.decision_bundle_id=v_bundle.decision_bundle_id
        and h.bundle_revision=v_bundle.bundle_revision and h.authority_kind='PROTECTED'
        and h.state in ('COMMITTED_CURRENT','SUPERSEDED') and h.committed_at_utc is not null
        and h.root_timesheet_id=v_local.root_timesheet_id and h.candidate_id=v_bundle.candidate_id
        and h.contract_id=v_bundle.source_contract_id and h.week_ending_date=v_bundle.week_ending_date
        and h.root_family_booking_id=v_bundle.source_root_family_booking_id
        and h.source_origin_json=v_local.approved_snapshot_json->'local_publication_origin'
        and h.source_origin_json->>'origin_kind'='PROTECTED_LOCAL_DECISION_V1'
        and h.source_origin_json->>'publication_request_id'=v_local.publication_request_id::text
        and h.source_origin_json->>'generation_id'=v_local.generation_id::text
        and h.source_origin_json->>'request_sha256'=pg_catalog.encode(v_local.request_sha256,'hex')
        and h.source_origin_json->>'source_qualification_digest'=pg_catalog.encode(v_local.source_qualification_digest,'hex')
        and h.source_generation_digest=v_local.publication_origin_digest
        and private.weekly_source_publication_request_digest_v1(h.source_origin_json)=v_local.publication_origin_digest;
    select r.* into v_revision from private.bpay_next_work_revision r where r.source_event_id=v_head.id;
    if found then
      select pg_catalog.jsonb_build_object('source_head_id',h.id,'work_id',w.id,'revision_id',r.id,
        'command_id',p.command_id,'agency_sequence',c.agency_sequence::text,'publication_status',p.status,
        'application',private.bpay_next_source_origin_state_v1(w.id,r.id)) into v_mapping
      from public.weekly_source_entitlement_heads h
      join private.bpay_next_work_revision r on r.id=v_revision.id and r.source_event_id=h.id and r.source_head_id=h.id
      join private.bpay_next_work w on w.id=r.work_id
      join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
      join private.bpay_next_command c on c.id=p.command_id
      join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=w.candidate_id
      -- I7 can retain a real optional TF identity alongside a HEAD. HEAD is
      -- the authority in this lane; absence of that metadata is not required.
      where h.id=v_head.id and r.source_kind='PROTECTED'
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.week_ending_date=h.week_ending_date and r.source_inventory_digest=h.inventory_digest
        and r.approved_at_utc is not null and r.sealed_at_utc is not null
        and pg_catalog.isfinite(r.approved_at_utc) and pg_catalog.isfinite(r.sealed_at_utc)
        and w.work_kind='SOURCE' and w.candidate_id=h.candidate_id and w.contract_id=h.contract_id
        and w.booking_id=h.root_family_booking_id and w.week_ending_date=h.week_ending_date
        and p.candidate_id=w.candidate_id and p.revision_no=r.revision_no
        and p.status in ('QUEUED','APPLYING','APPLIED') and c.command_kind='POSITION_APPLY'
        and c.expected_member_count=1 and c.sealed_at_utc is not null and m.member_no=1;
      if v_mapping is null then
        raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_LOCAL_MAPPING_NOT_EXACT';
      end if;
    end if;
  end if;
  return v_saved||pg_catalog.jsonb_build_object('authority','SOURCE_LOCAL','banking_receipt',v_mapping);
exception when no_data_found or too_many_rows then
  raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_LOCAL_RECEIPT_NOT_EXACT';
end
$function$;

-- Fresh direct Save is not first Authorise. The existing live-root unique
-- index is the exact authority probe; it neither stamps Authorise nor prices.
create or replace function private.bpay_next_protected_require_authorised_root_v1(p_root_id uuid)
returns void language plpgsql volatile security invoker
set search_path=pg_catalog,private,public
as $function$
declare v_root public.timesheets%rowtype;v_auth public.weekly_source_root_authorisations%rowtype;
begin
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=p_root_id for key share;
  select a.* into v_auth from public.weekly_source_root_authorisations a
    where a.root_timesheet_id=p_root_id and a.withdrawn_at_utc is null for share;
  if v_root.authorised_at_server is null or not pg_catalog.isfinite(v_root.authorised_at_server)
     or v_auth.id is null or v_auth.family_booking_id is distinct from v_root.booking_id
     or v_auth.timesheet_version is distinct from v_root.version
     or v_auth.authorised_at_utc is null or not pg_catalog.isfinite(v_auth.authorised_at_utc)
     or v_auth.current_entitlement_head_id is not null then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_FIRST_AUTHORISATION_REQUIRED';
  end if;
exception when no_data_found or too_many_rows then
  raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_FIRST_AUTHORISATION_REQUIRED';
end
$function$;
-- END SOURCE LOCAL EXCLUSIVE ENTRY HELPERS

-- NEXT pins the actual named current physical root; it does not resolve or
-- lock an all-version family. The advisory keys/order are identical to the
-- installed Source rotation/import owners. A changed/noncurrent root refuses
-- instead of discovering a replacement or manufacturing a current identity.
create or replace function private.bpay_next_protected_lock_current_root_v1(
  p_root_id uuid,p_candidate_id uuid,p_contract_id uuid,p_week_ending date,p_booking_id text
) returns void language plpgsql volatile security invoker
set search_path=pg_catalog,private,public
as $function$
declare v_root public.timesheets%rowtype;v_before_booking text;v_current_id uuid;v_current_count integer;
begin
  if p_root_id is null or p_candidate_id is null or p_contract_id is null or p_week_ending is null
     or p_booking_id is null or pg_catalog.btrim(p_booking_id)='' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_ROOT_NOT_EXACT';
  end if;
  select t.booking_id into strict v_before_booking from public.timesheets t where t.timesheet_id=p_root_id;
  if v_before_booking is distinct from p_booking_id then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_ROOT_NOT_EXACT';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext(pg_catalog.btrim(v_before_booking)));
  if v_before_booking<>pg_catalog.btrim(v_before_booking) then
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext(v_before_booking));
  end if;
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=p_root_id for update;
  if v_root.booking_id is distinct from v_before_booking or not v_root.is_current or v_root.is_adjustment
     or v_root.revoked_at is not null or v_root.archived_at_utc is not null
     or v_root.contract_id is distinct from p_contract_id or v_root.week_ending_date is distinct from p_week_ending
     or not exists(select 1 from public.contracts c where c.id=p_contract_id and c.candidate_id=p_candidate_id)
     or not exists(select 1 from public.contract_weeks cw where cw.contract_id=p_contract_id
       and cw.week_ending_date=p_week_ending and cw.additional_seq=0 and cw.timesheet_id=p_root_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_ROOT_NOT_EXACT';
  end if;
  -- The partial current trim-booking index bounds this probe. LIMIT 2 proves
  -- ambiguity; it is not a latest-head/timestamp ranking or history census.
  select pg_catalog.count(*)::integer,pg_catalog.min(t.timesheet_id::text)::uuid
    into v_current_count,v_current_id from (
      select ts.timesheet_id from public.timesheets ts
      where ts.is_current and ts.booking_id is not null and pg_catalog.btrim(ts.booking_id)<>''
        and pg_catalog.btrim(ts.booking_id)=pg_catalog.btrim(v_before_booking) limit 2
    ) t;
  if v_current_count<>1 or v_current_id is distinct from p_root_id then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_ROOT_NOT_EXACT';
  end if;
end
$function$;

create or replace function private.bpay_next_protected_receipt_json_v1(
  p_run_id uuid,p_replay boolean
) returns jsonb language sql stable security definer
set search_path=pg_catalog,private,public
as $function$
  select pg_catalog.jsonb_build_object('ok',true,'outcome','PUBLISHED_NEXT',
    'idempotent_replay',p_replay,'family_id',r.family_id,
    'orchestration_run_id',r.orchestration_run_id,'approval_id',r.approval_id,
    'generation_id',r.generation_id,'work_id',r.work_id,'revision_id',r.revision_id,
    'command_id',r.command_id,'agency_sequence',r.agency_sequence::text,
    'new_family_bound_version',r.accepted_family_bound_version::text,
    'approved_pay_ex_vat',a.approved_target_gross::text,'state','NEXT_PUBLISHED')
  from private.bpay_next_protected_source_receipt r
  join public.weekly_exceptional_payment_approvals a on a.id=r.approval_id
  where r.orchestration_run_id=p_run_id
$function$;

create or replace function public.weekly_exceptional_pay_next_publication_status_v1(
  p_request jsonb
) returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_receipt private.bpay_next_protected_source_receipt%rowtype;
  v_actor uuid;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_group uuid;
  v_local jsonb;
begin
  if coalesce(nullif(pg_catalog.current_setting('request.jwt.claim.role',true),''),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_SERVICE_REQUIRED';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,
       array['schema_version','actor_user_id','family_id','orchestration_run_id'])
     or not (p_request ?& array['schema_version','actor_user_id','family_id','orchestration_run_id'])
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_NEXT_STATUS_V1' then
    raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_STATUS_INVALID';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  select f.* into strict v_family from public.weekly_exceptional_pay_target_families f
    where f.id=(p_request->>'family_id')::uuid;
  select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=(p_request->>'orchestration_run_id')::uuid and r.family_id=v_family.id;
  if v_run.requested_by_user_id is distinct from v_actor or not exists(
      select 1 from public.tms_users u where u.id=v_actor and u.is_active
        and (u.payment_authoriser or u.payment_golden_key)) then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  select r.* into v_receipt from private.bpay_next_protected_source_receipt r
    where r.orchestration_run_id=v_run.id;
  if found then
    if v_receipt.family_id<>v_family.id or v_receipt.actor_user_id<>v_actor
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or v_run.state<>'COMPLETE'
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_RECEIPT_NOT_EXACT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a
      where a.id=v_receipt.approval_id;
    select c.source_group_id into strict v_group from public.weekly_source_cycles c
      where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',
      v_group,v_approval.client_id,v_approval.protected_work_date);
    return pg_catalog.jsonb_build_object('ok',true,'route','NEXT','published',true,
      'receipt',private.bpay_next_protected_receipt_json_v1(v_run.id,true));
  end if;
  -- Retain LEGACY fresh routing; accepted Local replay is independent of the
  -- current module. Its real Source helper, not a C1 staging-row label, owns it.
  if (select active_owner from private.bpay_next_module_control where id=1)<>'NEXT'
     and not exists(select 1 from private.weekly_source_local_protected_decision_receipts l
       join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
       where request.orchestration_run_id=v_run.id) then
    return pg_catalog.jsonb_build_object('ok',true,'route','C1','published',false,'receipt',null);
  end if;
  v_local:=private.bpay_next_protected_local_receipt_v1(v_family.id,v_run.id,v_actor);
  if v_local is not null then
    return pg_catalog.jsonb_build_object('ok',true,'route','SOURCE_LOCAL',
      'published',v_local->'banking_receipt'<>'null'::jsonb,'receipt',v_local);
  end if;
  if v_run.state<>'RUNNING' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_DOMAIN_UNSUPPORTED';
  end if;
  return pg_catalog.jsonb_build_object('ok',true,
    'route',private.bpay_next_protected_owner_route_v1(v_family.id),'published',false,'receipt',null);
exception when no_data_found or too_many_rows then
  raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_STATUS_SCOPE_INVALID';
end
$function$;

create or replace function private.bpay_next_stage_protected_source_v1(
  p_approval_id uuid,p_generation_id uuid,p_root_timesheet_id uuid,p_target_components jsonb
) returns table(work_id uuid,revision_id uuid,source_event_id uuid)
language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_ts public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_existing private.bpay_next_work_revision%rowtype;
  v_revision uuid;
  v_component jsonb;
  v_component_id uuid;
  v_line uuid;
  v_shift uuid;
  v_n integer:=0;
  v_rate_n integer;
  v_hours numeric;
  v_amount numeric;
  v_candidate_name text;
  v_candidate_reference text;
  v_client_name text;
  v_schedule_count integer;
  v_bucket text;
  v_break jsonb;
  v_break_n integer;
begin
  select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a where a.id=p_approval_id;
  select g.* into strict v_generation from public.weekly_exceptional_pay_generations g
    where g.id=p_generation_id and g.family_id=v_approval.pay_target_family_id;
  if v_generation.complete_next_vector_json->'components' is distinct from p_target_components
     or pg_catalog.jsonb_typeof(p_target_components)<>'array'
     or pg_catalog.jsonb_array_length(p_target_components)>100 then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_TARGET_NOT_APPROVED';
  end if;
  select r.* into v_existing from private.bpay_next_work_revision r where r.source_event_id=p_approval_id;
  if found then
    if v_existing.source_kind<>'PROTECTED' or v_existing.physical_timesheet_id<>p_root_timesheet_id
       or v_existing.source_head_id is not null or v_existing.financial_snapshot_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REVISION_REPLAY_CONFLICT';
    end if;
    work_id:=v_existing.work_id; revision_id:=v_existing.id; source_event_id:=p_approval_id;
    return next; return;
  end if;
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  select t.* into strict v_ts from public.timesheets t where t.timesheet_id=p_root_timesheet_id for key share;
  select c.* into strict v_contract from public.contracts c where c.id=v_ts.contract_id for key share;
  if private.bpay_next_approval_route_v1(v_ts.timesheet_id)<>'SOURCE_HOURS'
     or v_ts.contract_id<>v_approval.contract_id or v_contract.candidate_id<>v_approval.candidate_id
     or v_ts.week_ending_date<>v_approval.week_ending or v_ts.is_current is distinct from true
     or v_ts.revoked_at is not null or v_ts.archived_at_utc is not null or v_ts.is_adjustment
     or upper(v_contract.pay_method_snapshot) not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_PHYSICAL_SCOPE_INVALID';
  end if;
  -- Immutable revision replay above precedes these fresh owner/auth gates.
  if private.bpay_next_protected_owner_route_v1(v_approval.pay_target_family_id)<>'NEXT' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED';
  end if;
  perform private.bpay_next_protected_require_authorised_root_v1(v_ts.timesheet_id);
  insert into private.bpay_next_worker_control(candidate_id) values(v_approval.candidate_id) on conflict do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_approval.candidate_id for update;
  select w.* into v_work from private.bpay_next_work w where w.booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work(candidate_id,contract_id,original_timesheet_id,booking_id,work_kind,week_ending_date)
      values(v_approval.candidate_id,v_approval.contract_id,v_ts.timesheet_id,v_ts.booking_id,'SOURCE',v_ts.week_ending_date)
      returning * into v_work;
  elsif v_work.candidate_id<>v_approval.candidate_id or v_work.contract_id<>v_approval.contract_id
      or v_work.work_kind<>'SOURCE' or v_work.week_ending_date<>v_ts.week_ending_date then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_WORK_IDENTITY_CONFLICT';
  end if;
  select coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),c.id::text),c.tms_ref
    into strict v_candidate_name,v_candidate_reference from public.candidates c where c.id=v_approval.candidate_id;
  select c.name into strict v_client_name from public.clients c where c.id=v_approval.client_id;
  select pg_catalog.count(*)::integer into v_schedule_count from private.bpay_next_contract_rate_rows_v1(
    v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
    v_contract.mileage_pay_rate,v_contract.mileage_charge_rate);
  insert into private.bpay_next_work_revision(work_id,revision_no,source_kind,source_head_id,source_event_id,
    physical_timesheet_id,financial_snapshot_id,physical_timesheet_version,source_pay_channel,week_ending_date,
    detail_kind,expected_line_count,expected_rate_schedule_count,certified_zero,approved_source_ex_vat,
    candidate_display_name,candidate_reference,client_display_name,job_title,band_label,timesheet_reference)
  values(v_work.id,v_work.current_revision_no+1,'PROTECTED',null,p_approval_id,v_ts.timesheet_id,null,v_ts.version,
    upper(v_contract.pay_method_snapshot),v_ts.week_ending_date,
    case when exists(select 1 from pg_catalog.jsonb_array_elements(p_target_components) x(value)
      where x.value->>'kind'='WORK') then 'SHIFT' else 'FIXED' end,
    pg_catalog.jsonb_array_length(p_target_components),v_schedule_count,v_approval.approved_target_gross=0,
    v_approval.approved_target_gross,v_candidate_name,v_candidate_reference,v_client_name,
    v_contract.role,v_contract.band,v_ts.reference_number) returning id into v_revision;
  insert into private.bpay_next_rate_schedule(revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate)
    select v_revision,r.rate_family,r.rate_code,r.unit_label,r.paye_rate,r.umbrella_rate,r.charge_rate
    from private.bpay_next_contract_rate_rows_v1(v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,v_contract.mileage_pay_rate,v_contract.mileage_charge_rate) r;
  for v_component in select value from pg_catalog.jsonb_array_elements(p_target_components)
  loop
    v_n:=v_n+1; v_hours:=0; v_rate_n:=0;
    if v_component->>'kind'='WORK' then
      v_component_id:=private.weekly_source_entitlement_component_id_v1('WORKED_TIME','SEGMENT',
        'weekly-source-event:'||(v_component->>'work_event_id'),v_component->>'work_event_id');
      foreach v_bucket in array array['day','night','sat','sun','bh'] loop
        v_hours:=v_hours+(v_component#>>array['hours',v_bucket])::numeric;
        if (v_component#>>array['hours',v_bucket])::numeric<>0 then v_rate_n:=v_rate_n+1; end if;
      end loop;
    else
      v_component_id:=private.weekly_source_entitlement_component_id_v1('SOURCE_FIXED_EXPENSE','EXPENSE_AUTHORITY',
        'weekly-source-expense:'||(v_component->>'work_event_id'),v_component->>'work_event_id');
    end if;
    v_amount:=(v_component->>'pay_ex_vat')::numeric;
    insert into private.bpay_next_approved_line(revision_id,line_no,component_key,source_component_id,component_kind,
      work_date,approved_quantity,expected_rate_detail_count,source_pay_ex_vat,evidence_ref)
    values(v_revision,v_n,'SOURCE:'||v_component_id::text,v_component_id,
      case when v_component->>'kind'='EXPENSE' then 'EXPENSE'
        when (v_component->>'protected')::boolean then 'PROTECTED_WORK' else 'WORK' end,
      nullif(v_component->>'date','')::date,case when v_component->>'kind'='WORK' then v_hours else null end,
      v_rate_n,v_amount,'protected-approval:'||p_approval_id::text||'#'||p_generation_id::text)
    returning id into v_line;
    if v_component->>'kind'='WORK' then
      insert into private.bpay_next_shift_detail(approved_line_id,detail_no,work_date,
        shift_start_at,shift_end_at,shift_start_local,shift_end_local,shift_overnight,
        approved_hours,detail_label,immutable_evidence_ref)
      values(v_line,1,(v_component->>'date')::date,nullif(v_component->>'start_at_utc','')::timestamptz,
        nullif(v_component->>'end_at_utc','')::timestamptz,v_component->>'start',v_component->>'end',
        (v_component->>'overnight')::boolean,v_hours,'weekly-source-event:'||(v_component->>'work_event_id'),
        'protected-approval:'||p_approval_id::text) returning id into v_shift;
      foreach v_bucket in array array['day','night','sat','sun','bh'] loop
        if (v_component#>>array['hours',v_bucket])::numeric<>0 then
          insert into private.bpay_next_rate_detail(approved_line_id,bucket,approved_hours,source_pay_rate)
          values(v_line,upper(v_bucket),(v_component#>>array['hours',v_bucket])::numeric,
            (v_component#>>array['rates',v_bucket])::numeric);
        end if;
      end loop;
      v_break_n:=0;
      for v_break in select value from pg_catalog.jsonb_array_elements(v_component->'breaks') loop
        v_break_n:=v_break_n+1;
        insert into private.bpay_next_break_detail(shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
          values(v_shift,v_break_n,v_break->>'start',v_break->>'end',(v_break->>'minutes')::integer);
      end loop;
      if v_break_n=0 and (v_component->>'break_minutes')::integer>0 then
        insert into private.bpay_next_break_detail(shift_detail_id,break_no,break_minutes)
          values(v_shift,1,(v_component->>'break_minutes')::integer);
      end if;
    end if;
  end loop;
  work_id:=v_work.id; revision_id:=v_revision; source_event_id:=p_approval_id; return next;
end
$function$;

create or replace function public.weekly_exceptional_pay_publish_next_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_keys constant text[]:=array['schema_version','actor_user_id','family_id','orchestration_run_id',
    'source_cycle_id','client_id','work_event_id','evidence_timesheet_id','expected_family_bound_version',
    'protected_schedule','rate_classification','source_proposal','target_snapshot',
    'current_comparison_revision_id','current_final_revision_id','reason','idempotency_key'];
  v_request_hash bytea;
  v_source_hash bytea;
  v_target_hash bytea;
  v_schedule_hash bytea;
  v_policy_hash bytea;
  v_approval_hash bytea;
  v_prior_vector jsonb;
  v_prior_vector_hash bytea;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_run public.weekly_exceptional_orchestration_runs%rowtype;
  v_receipt private.bpay_next_protected_source_receipt%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_contract public.contracts%rowtype;
  v_root public.timesheets%rowtype;
  v_evidence_row public.timesheets%rowtype;
  v_approval public.weekly_exceptional_payment_approvals%rowtype;
  v_generation public.weekly_exceptional_pay_generations%rowtype;
  v_context jsonb;
  v_policy jsonb;
  v_signed jsonb;
  v_components jsonb;
  v_component jsonb;
  v_source jsonb;
  v_break jsonb;
  v_actor uuid;
  v_event uuid;
  v_evidence uuid;
  v_work_date date;
  v_key text;
  v_total numeric:=0;
  v_match integer;
  v_n integer;
  v_issue_ids uuid[];
  v_issue_hash bytea;
  v_reason text;
  v_generation_reason text;
  v_state text;
  v_lifecycle text;
  v_event_sequence bigint;
  v_family_event_id uuid;
  v_prior_event_hash bytea;
  v_event_hash bytea;
  v_target_sequence bigint;
  v_prior_target_hash bytea;
  v_audit_hash bytea;
  v_step integer;
  v_stage record;
  v_publication record;
  v_bucket text;
  v_rate numeric;
  v_charge_rate numeric;
  v_hours numeric;
  v_local jsonb;
begin
  if coalesce(nullif(pg_catalog.current_setting('request.jwt.claim.role',true),''),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_SERVICE_REQUIRED';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request,v_keys) or not (p_request ?& v_keys)
     or p_request->>'schema_version'<>'WEEKLY_PROTECTED_NEXT_PUBLISH_V1'
     or pg_catalog.octet_length(p_request::text)>128000
     or pg_catalog.jsonb_typeof(p_request->'target_snapshot')<>'object'
     or not private.weekly_exceptional_json_keys_exact_v1(p_request->'target_snapshot',array['schema_version','components'])
     or p_request#>>'{target_snapshot,schema_version}'<>'WEEKLY_PROTECTED_NEXT_TARGET_V1'
     or pg_catalog.jsonb_typeof(p_request#>'{target_snapshot,components}')<>'array'
     or pg_catalog.jsonb_array_length(p_request#>'{target_snapshot,components}')>100 then
    raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_REQUEST_INVALID';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid; v_event:=(p_request->>'work_event_id')::uuid;
  v_evidence:=nullif(p_request->>'evidence_timesheet_id','')::uuid;
  v_work_date:=(p_request#>>'{protected_schedule,work_date}')::date;
  v_key:=p_request->>'idempotency_key'; v_reason:=p_request->>'reason';
  if pg_catalog.char_length(v_key) not between 16 and 240 or pg_catalog.char_length(btrim(v_reason)) not between 1 and 1000
     or not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active
       and (u.payment_authoriser or u.payment_golden_key)) then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  v_request_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_NEXT_PUBLISH_V1',p_request);
  -- Immutable exact replay precedes mutable current-root checks. A later
  -- legitimate physical rotation cannot invalidate this accepted receipt.
  select f.* into strict v_family from public.weekly_exceptional_pay_target_families f
    where f.id=(p_request->>'family_id')::uuid;
  select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=(p_request->>'orchestration_run_id')::uuid and r.family_id=v_family.id;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;
  if found then
    if v_receipt.publication_request_sha256<>v_request_hash or v_receipt.actor_user_id<>v_actor
       or v_receipt.family_id<>v_family.id or v_run.state<>'COMPLETE'
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REPLAY_CONFLICT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a where a.id=v_receipt.approval_id;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_cycle.source_group_id,
      v_approval.client_id,v_approval.protected_work_date);
    return private.bpay_next_protected_receipt_json_v1(v_run.id,true);
  end if;
  -- BEGIN QUERY V2 DIRECT NEXT ADMISSION
  -- Exact immutable receipt replay above is lock-free. Fresh acceptance must
  -- admit before the protected booking/root lock; propagate exact 55P03.
  perform private.weekly_source_pay_query_admit_v2();
  -- END QUERY V2 DIRECT NEXT ADMISSION

  -- Accepted Local cannot be republished by the direct financial owner.
  -- Its status route exposes the actual original common-HEAD mapping instead.
  v_local:=private.bpay_next_protected_local_receipt_v1(v_family.id,v_run.id,v_actor);
  if v_local is not null then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED';
  end if;

  -- Actual first acceptance retains the Source advisory ordering, then only
  -- the exact current physical root, family and run mutexes. Recheck receipt
  -- after those locks for simultaneous identical first requests.
  begin
    perform private.bpay_next_protected_lock_current_root_v1(v_family.root_timesheet_id,v_family.candidate_id,
      v_family.contract_id,v_family.week_ending_date,v_family.root_family_booking_id);
  exception when sqlstate '55000' then
    if sqlerrm<>'BPAY_NEXT_PROTECTED_ROOT_NOT_EXACT' then raise;end if;
    -- A concurrent identical first call may have committed while this call
    -- waited on the booking key, followed by a legitimate root rotation.
    -- Read only the immutable receipt; never retry an owner or adopt a root.
    select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;
    if not found then raise;end if;
    select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
      where r.id=v_run.id and r.family_id=v_family.id;
    if v_run.requested_by_user_id is distinct from v_actor
       or v_receipt.publication_request_sha256<>v_request_hash or v_receipt.actor_user_id<>v_actor
       or v_receipt.family_id<>v_family.id or v_run.state<>'COMPLETE'
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REPLAY_CONFLICT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a where a.id=v_receipt.approval_id;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_cycle.source_group_id,
      v_approval.client_id,v_approval.protected_work_date);
    return private.bpay_next_protected_receipt_json_v1(v_run.id,true);
  end;
  select f.* into strict v_family from public.weekly_exceptional_pay_target_families f where f.id=v_family.id for update;
  select r.* into strict v_run from public.weekly_exceptional_orchestration_runs r
    where r.id=(p_request->>'orchestration_run_id')::uuid and r.family_id=v_family.id for update;
  if v_run.requested_by_user_id is distinct from v_actor then
    raise exception using errcode='42501',message='BPAY_NEXT_PROTECTED_ACTOR_REFUSED';
  end if;
  select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;
  if found then
    if v_receipt.publication_request_sha256<>v_request_hash or v_receipt.actor_user_id<>v_actor
       or v_receipt.prepared_request_sha256 is distinct from v_run.request_fingerprint
       or v_receipt.family_id is distinct from v_family.id or v_run.state<>'COMPLETE'
       or exists(select 1 from private.weekly_source_local_protected_decision_receipts l
         join public.weekly_exceptional_c1_publication_requests request on request.id=l.publication_request_id
         where request.orchestration_run_id=v_run.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_REPLAY_CONFLICT';
    end if;
    select a.* into strict v_approval from public.weekly_exceptional_payment_approvals a
      where a.id=v_receipt.approval_id;
    select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_approval.source_cycle_id;
    perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_cycle.source_group_id,
      v_approval.client_id,v_approval.protected_work_date);
    return private.bpay_next_protected_receipt_json_v1(v_run.id,true);
  end if;
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  -- Recheck after the same family/run locks: no lane switch can occur between
  -- the pure status choice and fresh financial staging. First Authorise is an
  -- actual independent owner, never an approval metadata or saved-hour label.
  v_local:=private.bpay_next_protected_local_receipt_v1(v_family.id,v_run.id,v_actor);
  if v_local is not null or private.bpay_next_protected_owner_route_v1(v_family.id)<>'NEXT' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED';
  end if;
  perform private.bpay_next_protected_require_authorised_root_v1(v_family.root_timesheet_id);
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=(p_request->>'source_cycle_id')::uuid for share;
  select g.* into strict v_group from public.weekly_source_groups g where g.id=v_cycle.source_group_id and g.active for share;
  select c.* into strict v_contract from public.contracts c where c.id=v_family.contract_id for share;
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=v_family.root_timesheet_id for key share;
  perform private.weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY',v_group.id,
    v_contract.client_id,v_work_date);
  if v_run.state<>'RUNNING' or v_run.request_kind not in ('APPROVE','AMEND','WITHDRAW','RECONCILE','RECORD_NOT_WORKED')
     or v_family.bound_version<>(p_request->>'expected_family_bound_version')::bigint
     or v_run.request_fingerprint is null or v_family.agency_id<>v_group.agency_id
     or (v_family.current_generation_id is not null and v_family.target_domain_version<>'NEXT_V1')
     or v_family.c1_publication_state<>'NONE'
     or (p_request->>'client_id')::uuid<>v_contract.client_id
     or exists(select 1 from public.weekly_exceptional_c1_publication_requests r where r.family_id=v_family.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_SCOPE_INVALID';
  end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_work_date);
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY' or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_policy->>'c1_source_mode' not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or (v_group.source_family='NHSP') is distinct from (v_policy->>'c1_source_mode'='NHSP_WEEKLY') then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_POLICY_INVALID';
  end if;
  v_context:=public.weekly_exceptional_pay_action_context_v1(pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_PROTECTED_ACTION_CONTEXT_V1','actor_user_id',v_actor,
    'family_id',v_family.id,'orchestration_run_id',v_run.id,'source_cycle_id',v_cycle.id,
    'work_event_id',v_event,'protected_schedule',p_request->'protected_schedule','evidence_timesheet_id',v_evidence));
  if v_context->'source_proposal' is distinct from p_request->'source_proposal'
     or v_context->>'current_comparison_revision_id' is distinct from p_request->>'current_comparison_revision_id'
     or v_context->>'current_final_revision_id' is distinct from p_request->>'current_final_revision_id'
     or v_context->'protected_schedule' is distinct from p_request->'protected_schedule' then
    raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_SOURCE_CHANGED';
  end if;
  if p_request->'rate_classification' is distinct from pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_PROTECTED_SERVER_CALCULATION_V1',
      'calculator_owner','buildWeeklyScheduleSegmentsSnapshot',
      'policy_sha256',v_context#>>'{zero_rate_source_refs,effective_policy_sha256}')
     or v_context#>>'{candidate_submission,submission_id}' is distinct from v_evidence::text
     or pg_catalog.jsonb_typeof(p_request->'expected_family_bound_version') is distinct from 'string'
     or p_request->>'expected_family_bound_version' !~ '^[1-9][0-9]{0,18}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_PREVIEW_SCOPE_NOT_EXACT';
  end if;
  v_components:=p_request#>'{target_snapshot,components}';
  if pg_catalog.jsonb_array_length(v_components)<>(
      select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
      where d.value->>'state'='WAIT')+(
      select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(v_context->'next_source_segments') s(value)
      where not exists(select 1 from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
        where d.value->>'work_event_id'=s.value#>>'{weekly_source,work_event_id}'
          and d.value->>'state' in ('WAIT','NOT_WORKED')))
      +pg_catalog.jsonb_array_length(v_context->'source_expenses') then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_MANIFEST_NOT_EXACT';
  end if;
  if exists(select 1 from pg_catalog.jsonb_array_elements(v_components) x(value)
      group by x.value->>'kind',x.value->>'work_event_id' having pg_catalog.count(*)<>1) then
    raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_COMPONENT_DUPLICATE';
  end if;
  for v_component in select value from pg_catalog.jsonb_array_elements(v_components) loop
    if pg_catalog.jsonb_typeof(v_component)<>'object' or v_component->>'kind' not in ('WORK','EXPENSE')
       or pg_catalog.jsonb_typeof(v_component->'pay_ex_vat')<>'string'
       or pg_catalog.jsonb_typeof(v_component->'charge_ex_vat')<>'string'
       or (v_component->>'pay_ex_vat') !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'
       or (v_component->>'charge_ex_vat') !~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_COMPONENT_INVALID';
    end if;
    if v_component->>'kind'='EXPENSE' then
      if not private.weekly_exceptional_json_keys_exact_v1(v_component,array['kind','work_event_id','source_expense_id',
          'final_revision_id','source_observation_kind','authority_sha256','pay_ex_vat','charge_ex_vat'])
         or not (v_component ?& array['kind','work_event_id','source_expense_id','final_revision_id',
          'source_observation_kind','authority_sha256','pay_ex_vat','charge_ex_vat']) then
        raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_EXPENSE_INVALID';
      end if;
      select pg_catalog.count(*)::integer into v_match from pg_catalog.jsonb_array_elements(v_context->'source_expenses') e(value)
        where e.value->>'work_event_id'=v_component->>'work_event_id'
          and e.value->>'source_expense_id'=v_component->>'source_expense_id'
          and e.value->>'final_revision_id'=v_component->>'final_revision_id'
          and e.value->>'source_observation_kind'=v_component->>'source_observation_kind'
          and e.value->>'document_sha256'=v_component->>'authority_sha256'
          and (e.value->>'pay_ex_vat')::numeric=(v_component->>'pay_ex_vat')::numeric
          and (e.value->>'charge_ex_vat')::numeric=(v_component->>'charge_ex_vat')::numeric;
      if v_match<>1 then raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_EXPENSE_NOT_EXACT'; end if;
    else
      if not private.weekly_exceptional_json_keys_exact_v1(v_component,array['kind','work_event_id','protected',
        'date','start','end','overnight','start_at_utc','end_at_utc','break_minutes','breaks','hours','rates',
        'charge_rates','pay_ex_vat','charge_ex_vat','source_provenance'])
        or not (v_component ?& array['kind','work_event_id','protected','date','start','end','overnight',
          'start_at_utc','end_at_utc','break_minutes','breaks','hours','rates','charge_rates','pay_ex_vat',
          'charge_ex_vat','source_provenance'])
        or pg_catalog.jsonb_typeof(v_component->'protected')<>'boolean'
        or pg_catalog.jsonb_typeof(v_component->'overnight')<>'boolean'
        or pg_catalog.jsonb_typeof(v_component->'breaks')<>'array'
        or pg_catalog.jsonb_array_length(v_component->'breaks')>100
        or v_component->>'start' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
        or v_component->>'end' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
        or (v_component->>'break_minutes')::integer<0 then
        raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_SHIFT_INVALID';
      end if;
      if (v_component->>'protected')::boolean then
        select pg_catalog.count(*)::integer into v_match from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
          where d.value->>'work_event_id'=v_component->>'work_event_id' and d.value->>'state'='WAIT'
            and d.value#>>'{fixed_schedule,date}'=v_component->>'date'
            and d.value#>>'{fixed_schedule,start}'=v_component->>'start'
            and d.value#>>'{fixed_schedule,end}'=v_component->>'end'
            and (d.value#>>'{fixed_schedule,break_mins}')::integer=(v_component->>'break_minutes')::integer;
        if v_match<>1 or v_component->'source_provenance'<>'null'::jsonb then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_SCHEDULE_NOT_APPROVED';
        end if;
      else
        select pg_catalog.count(*)::integer,pg_catalog.jsonb_agg(s.value)->0 into v_match,v_source
          from pg_catalog.jsonb_array_elements(v_context->'next_source_segments') s(value)
          where s.value#>>'{weekly_source,work_event_id}'=v_component->>'work_event_id'
            and s.value->>'date'=v_component->>'date' and s.value->>'start'=v_component->>'start'
            and s.value->>'end'=v_component->>'end' and s.value->>'overnight'=v_component->>'overnight'
            and (s.value->>'break_mins')::integer=(v_component->>'break_minutes')::integer
            and (s.value#>>'{weekly_source,pay_vector,total_pence}')::numeric/100=(v_component->>'pay_ex_vat')::numeric
            and (s.value#>>'{weekly_source,charge_vector,total_pence}')::numeric/100=(v_component->>'charge_ex_vat')::numeric
            and not exists(select 1 from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
              where d.value->>'work_event_id'=v_component->>'work_event_id' and d.value->>'state' in ('WAIT','NOT_WORKED'));
        if v_match<>1 or v_component->'source_provenance' is distinct from pg_catalog.jsonb_build_object(
          'movement_id',v_source#>>'{weekly_source,movement_id}','final_revision_id',v_source#>>'{weekly_source,final_revision_id}',
          'row_resolution_id',v_source#>>'{weekly_source,row_resolution_id}','economic_snapshot_id',v_source#>>'{weekly_source,economic_snapshot_id}',
          'calculation_fingerprint',v_source#>>'{weekly_source,calculation_fingerprint}') then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_IMPORTED_SOURCE_NOT_EXACT';
        end if;
      end if;
      foreach v_bucket in array array['hours','rates','charge_rates'] loop
        if pg_catalog.jsonb_typeof(v_component->v_bucket)<>'object'
           or not private.weekly_exceptional_json_keys_exact_v1(v_component->v_bucket,array['day','night','sat','sun','bh'])
           or not ((v_component->v_bucket) ?& array['day','night','sat','sun','bh']) then
          raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_BUCKET_INVALID';
        end if;
      end loop;
      foreach v_bucket in array array['day','night','sat','sun','bh'] loop
        if pg_catalog.jsonb_typeof(v_component#>array['hours',v_bucket]) is distinct from 'string'
           or v_component#>>array['hours',v_bucket] !~ '^(0|[1-9][0-9]*)\.[0-9]{6}$'
           or pg_catalog.jsonb_typeof(v_component#>array['rates',v_bucket]) not in ('string','null')
           or pg_catalog.jsonb_typeof(v_component#>array['charge_rates',v_bucket]) not in ('string','null')
           or (v_component#>array['rates',v_bucket]<>'null'::jsonb
             and v_component#>>array['rates',v_bucket] !~ '^-?(0|[1-9][0-9]*)\.[0-9]{6}$')
           or (v_component#>array['charge_rates',v_bucket]<>'null'::jsonb
             and v_component#>>array['charge_rates',v_bucket] !~ '^-?(0|[1-9][0-9]*)\.[0-9]{6}$') then
          raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_DECIMAL_TEXT_REQUIRED';
        end if;
        v_hours:=(v_component#>>array['hours',v_bucket])::numeric;
        v_rate:=(v_component#>>array['rates',v_bucket])::numeric;
        v_charge_rate:=(v_component#>>array['charge_rates',v_bucket])::numeric;
        if pg_catalog.jsonb_typeof(v_component#>array['hours',v_bucket])<>'string'
           or v_hours is null or v_hours<0 or v_hours<>pg_catalog.round(v_hours,6)
           or ((v_component->>'protected')::boolean and v_hours<>0 and v_rate is null)
           or (v_rate is not null and v_rate<>pg_catalog.round(v_rate,6))
           or (v_charge_rate is not null and v_charge_rate<>pg_catalog.round(v_charge_rate,6)) then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_USED_RATE_INVALID';
        end if;
        if not (v_component->>'protected')::boolean then
          if v_hours is distinct from (v_source#>>array['weekly_source','pay_vector','hours',v_bucket])::numeric
             or v_rate is distinct from (v_source#>>array['weekly_source','pay_vector','rates',v_bucket])::numeric
             or v_charge_rate is distinct from (v_source#>>array['weekly_source','charge_vector','rates',v_bucket])::numeric then
            raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_IMPORTED_RATE_NOT_EXACT';
          end if;
        else
          if v_hours<>0 and not exists(select 1 from private.bpay_next_contract_rate_rows_v1(
            v_contract.rates_json,v_contract.bucket_labels_json,v_contract.additional_rates_json,
            v_contract.mileage_pay_rate,v_contract.mileage_charge_rate) r where r.rate_family='STANDARD'
              and r.rate_code=upper(v_bucket) and v_rate=case upper(v_contract.pay_method_snapshot)
                when 'PAYE' then r.paye_rate else r.umbrella_rate end and v_charge_rate=r.charge_rate) then
            raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_CALCULATED_RATE_NOT_EXACT';
          end if;
        end if;
      end loop;
      v_n:=0;
      for v_break in select value from pg_catalog.jsonb_array_elements(v_component->'breaks') loop
        if not private.weekly_exceptional_json_keys_exact_v1(v_break,array['start','end','minutes'])
           or not (v_break ?& array['start','end','minutes'])
           or v_break->>'start' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or v_break->>'end' !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or (v_break->>'minutes')::integer<=0 then
          raise exception using errcode='22023',message='BPAY_NEXT_PROTECTED_BREAK_INVALID';
        end if;
        if (v_break->>'minutes')::integer is distinct from
          ((extract(epoch from ((v_break->>'end')::time-(v_break->>'start')::time))/60)::integer
            +case when (v_break->>'end')::time<(v_break->>'start')::time then 1440 else 0 end) then
          raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_BREAK_DURATION_NOT_EXACT';
        end if;
        v_n:=v_n+(v_break->>'minutes')::integer;
      end loop;
      if v_n<>0 and v_n<>(v_component->>'break_minutes')::integer then
        raise exception using errcode='23514',message='BPAY_NEXT_PROTECTED_BREAK_NOT_EXACT';
      end if;
    end if;
    v_total:=v_total+(v_component->>'pay_ex_vat')::numeric;
  end loop;
  if v_evidence is not null then
    select t.* into strict v_evidence_row from public.timesheets t where t.timesheet_id=v_evidence
      and t.contract_id=v_family.contract_id and t.week_ending_date=v_family.week_ending_date
      and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum and t.is_current
      and t.revoked_at is null and t.archived_at_utc is null for key share;
    v_signed:=private.weekly_exceptional_candidate_signed_evidence_v1(v_evidence);
  end if;
  select coalesce(pg_catalog.array_agg(i.id order by i.id),'{}'::uuid[]) into v_issue_ids
    from public.weekly_discrepancy_incidents i where i.source_group_id=v_group.id
      and i.work_event_id=v_event and i.candidate_id=v_family.candidate_id and i.client_id=v_contract.client_id;
  v_issue_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_ISSUE_SET_V1',pg_catalog.to_jsonb(v_issue_ids));
  v_schedule_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SCHEDULE_V1',p_request->'protected_schedule');
  v_source_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_SOURCE_PROPOSAL_V1',p_request->'source_proposal');
  v_policy_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_RATE_POLICY_V1',pg_catalog.jsonb_build_object(
    'contract_id',v_contract.id,'rates_json',v_contract.rates_json,'policy',v_policy,'classification',p_request->'rate_classification'));
  v_target_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',p_request->'target_snapshot');
  v_approval_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_NEXT_APPROVAL_V1',pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'work_event_id',v_event,'schedule_hash',pg_catalog.encode(v_schedule_hash,'hex'),
    'source_hash',pg_catalog.encode(v_source_hash,'hex'),'policy_hash',pg_catalog.encode(v_policy_hash,'hex'),
    'target_hash',pg_catalog.encode(v_target_hash,'hex'),'actor_user_id',v_actor,'reason',v_reason));
  if v_family.current_generation_id is null then
    if v_run.request_kind<>'APPROVE' then raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_INITIAL_ACTION_INVALID'; end if;
    v_prior_vector:=pg_catalog.jsonb_build_object('schema_version','WEEKLY_PROTECTED_NEXT_TARGET_V1','components','[]'::jsonb);
    v_generation_reason:='INITIAL_APPROVAL';
  else
    select g.complete_next_vector_json into strict v_prior_vector from public.weekly_exceptional_pay_generations g
      where g.id=v_family.current_generation_id and g.family_id=v_family.id and g.lifecycle_state='PUBLISHED' for share;
    v_generation_reason:=case v_run.request_kind when 'AMEND' then 'OFFICE_AMENDMENT' else v_run.request_kind end;
  end if;
  v_prior_vector_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_COMPLETE_VECTOR_V1',v_prior_vector);
  v_state:=case when v_run.request_kind in ('APPROVE','AMEND') then 'WAIT'
    when v_run.request_kind in ('WITHDRAW','RECONCILE') then 'ACCEPTED_SOURCE' else 'NOT_WORKED' end;
  v_lifecycle:=case when exists(select 1 from pg_catalog.jsonb_array_elements(v_context->'protected_decisions') d(value)
      where d.value->>'state'='WAIT') then 'WAITING_SOURCE' when v_state='NOT_WORKED' then 'NOT_WORKED' else 'RECONCILED' end;
  insert into public.weekly_exceptional_payment_approvals(pay_target_family_id,work_event_id,evidence_timesheet_id,
    candidate_id,client_id,contract_id,week_ending,protected_work_date,protected_start_at_local,protected_end_at_local,
    protected_break_minutes,signed_submission_timesheet_id,signed_submission_revision,signed_submission_hash,signed_at_utc,
    contributing_issue_episode_ids,contributing_issue_episode_ids_hash,signed_schedule_fact_hash,
    contract_rate_policy_source_fingerprint,approved_by_user_id,approval_reason,source_cycle_id,comparison_revision_id,
    final_revision_id,approved_target_pay_components_json,approved_target_gross,creation_orchestration_run_id,
    approval_hash,creation_idempotency_key)
  values(v_family.id,v_event,v_evidence,v_family.candidate_id,v_contract.client_id,v_contract.id,v_family.week_ending_date,
    v_work_date,(p_request#>>'{protected_schedule,start_at_local}')::timestamp,(p_request#>>'{protected_schedule,end_at_local}')::timestamp,
    (p_request#>>'{protected_schedule,break_minutes}')::integer,v_evidence,(v_signed->>'timesheet_version')::integer,
    case when v_evidence is null then null else private.weekly_exceptional_hex_sha256_v1(v_signed->>'signature_sha256') end,
    (v_signed->>'signed_at_utc')::timestamptz,
    v_issue_ids,v_issue_hash,v_schedule_hash,v_policy_hash,v_actor,v_reason,v_cycle.id,
    nullif(p_request->>'current_comparison_revision_id','')::uuid,nullif(p_request->>'current_final_revision_id','')::uuid,
    p_request->'target_snapshot',v_total,v_run.id,v_approval_hash,v_key||':approval') returning * into v_approval;
  insert into public.weekly_exceptional_pay_generations(family_id,generation_number,prior_generation_id,prior_generation_hash,
    request_idempotency_key,reason,complete_prior_vector_json,complete_prior_vector_hash,complete_next_vector_json,
    complete_next_vector_hash,fixed_target_source_state_fingerprint,lifecycle_state)
  values(v_family.id,v_family.current_generation_number+1,v_family.current_generation_id,
    case when v_family.current_generation_id is null then null else v_prior_vector_hash end,v_key||':generation',
    v_generation_reason,v_prior_vector,v_prior_vector_hash,p_request->'target_snapshot',v_target_hash,v_source_hash,'READY_TO_STAGE')
  returning * into v_generation;
  -- Existing UNIQUE(family_id,event_sequence) indexes provide a one-row
  -- backward tail probe while the exact family mutex is held; no history aggregate.
  select e.event_sequence,e.event_hash into v_event_sequence,v_prior_event_hash
    from public.weekly_exceptional_pay_family_events e where e.family_id=v_family.id
    order by e.event_sequence desc limit 1;
  v_event_sequence:=coalesce(v_event_sequence,0)+1;
  v_event_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_FAMILY_EVENT_V1',pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'event_sequence',v_event_sequence,'work_event_id',v_event,'approval_id',v_approval.id,
    'target_vector_hash',pg_catalog.encode(v_target_hash,'hex'),'prior_event_hash',pg_catalog.encode(v_prior_event_hash,'hex')));
  insert into public.weekly_exceptional_pay_family_events(family_id,event_sequence,durable_work_event_id,evidence_approval_id,
    work_date,start_at_local,end_at_local,break_minutes,rate_classification_json,source_proposal_snapshot_json,source_proposal_hash,
    fixed_office_target_snapshot_json,fixed_office_target_hash,state,current_comparison_revision_id,current_final_revision_id,
    office_actor_user_id,office_reason,prior_event_hash,event_hash)
  values(v_family.id,v_event_sequence,v_event,v_approval.id,v_work_date,v_approval.protected_start_at_local,
    v_approval.protected_end_at_local,v_approval.protected_break_minutes,p_request->'rate_classification',p_request->'source_proposal',
    v_source_hash,p_request->'protected_schedule',v_schedule_hash,v_state,v_approval.comparison_revision_id,v_approval.final_revision_id,
    v_actor,v_reason,v_prior_event_hash,v_event_hash) returning id into v_family_event_id;
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    perform private.bpay_next_protected_current_decision_capture_v1(v_family_event_id);
  end if;
  select e.event_sequence,e.event_hash into v_target_sequence,v_prior_target_hash
    from public.weekly_exceptional_pay_target_events e where e.family_id=v_family.id
    order by e.event_sequence desc limit 1;
  v_target_sequence:=coalesce(v_target_sequence,0)+1;
  v_audit_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_NEXT_TARGET_EVENT_V1',pg_catalog.jsonb_build_object(
    'family_id',v_family.id,'approval_id',v_approval.id,'generation_id',v_generation.id,'prior_hash',pg_catalog.encode(v_prior_target_hash,'hex'),
    'target_hash',pg_catalog.encode(v_target_hash,'hex')));
  insert into public.weekly_exceptional_pay_target_events(family_id,approval_id,event_sequence,triggering_comparison_revision_id,
    triggering_final_revision_id,prior_event_fingerprint,fixed_target_component_snapshot,current_source_proposal_snapshot,
    complete_prior_family_vector_fingerprint,complete_next_family_vector_fingerprint,reason,resulting_lifecycle_state,
    financial_generation_id,actor_user_id,event_hash,idempotency_key)
  values(v_family.id,v_approval.id,v_target_sequence,v_approval.comparison_revision_id,v_approval.final_revision_id,v_prior_target_hash,
    p_request->'target_snapshot',p_request->'source_proposal',v_prior_vector_hash,v_target_hash,v_generation_reason,v_lifecycle,
    v_generation.id,v_actor,v_audit_hash,v_key||':target-event');
  update public.weekly_exceptional_pending_reconciliation_targets t set state=case when v_run.request_kind='WITHDRAW'
    then 'WITHDRAWN' else 'CONSUMED' end,completed_at_utc=pg_catalog.transaction_timestamp()
    where t.family_id=v_family.id and t.durable_work_event_id=v_event and t.state='ACTIVE';
  if v_state='WAIT' then
    insert into public.weekly_exceptional_pending_reconciliation_targets(family_id,approval_id,durable_work_event_id,incident_id,
      current_final_revision_id,intended_outcome,source_action_policy_target_fingerprint,state)
    values(v_family.id,v_approval.id,v_event,(select i.id from public.weekly_discrepancy_incidents i
      where i.source_group_id=v_group.id and i.work_event_id=v_event order by i.episode_number desc limit 1),
      v_approval.final_revision_id,'WAIT',v_source_hash,'ACTIVE');
  end if;
  select s.* into strict v_stage from private.bpay_next_stage_protected_source_v1(
    v_approval.id,v_generation.id,v_root.timesheet_id,v_components) s;
  -- All Source/family/work/revision locks precede the agency clock receipt.
  select p.* into strict v_publication from private.bpay_next_publish_source_staged_pair_v1(
    array[v_stage.work_id],array[v_stage.revision_id],array[v_approval.id]) p;
  if v_family.current_generation_id is not null then
    update public.weekly_exceptional_pay_generations set lifecycle_state='SUPERSEDED',
      superseded_by_generation_id=v_generation.id,superseded_at_utc=pg_catalog.transaction_timestamp()
      where id=v_family.current_generation_id and lifecycle_state='PUBLISHED';
  end if;
  update public.weekly_exceptional_pay_generations set lifecycle_state='PUBLISHED',
    published_at_utc=pg_catalog.transaction_timestamp(),result_hash=v_request_hash where id=v_generation.id;
  update public.weekly_exceptional_pay_target_families set ownership_state='TARGET_MANAGED',target_domain_version='NEXT_V1',
    current_generation_id=v_generation.id,current_generation_number=v_generation.generation_number,
    current_complete_target_vector_hash=v_target_hash,current_source_proposal_hash=v_source_hash,
    current_lifecycle_state=v_lifecycle,current_component_count=pg_catalog.jsonb_array_length(v_components),
    bound_version=bound_version+1 where id=v_family.id;
  update public.weekly_exceptional_orchestration_runs set state='COMPLETE',after_state_fingerprint=v_request_hash,
    completed_at_utc=pg_catalog.transaction_timestamp() where id=v_run.id;
  insert into private.bpay_next_protected_source_receipt(orchestration_run_id,family_id,actor_user_id,approval_id,generation_id,
    work_id,revision_id,command_id,agency_sequence,accepted_family_bound_version,prepared_request_sha256,
    publication_request_sha256,source_manifest_sha256)
  values(v_run.id,v_family.id,v_actor,v_approval.id,v_generation.id,v_stage.work_id,v_stage.revision_id,v_publication.command_id,
    v_publication.agency_sequence,v_family.bound_version+1,v_run.request_fingerprint,v_request_hash,v_source_hash);
  insert into public.weekly_exceptional_payment_events(family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key)
  values(v_family.id,v_approval.id,case when v_state='WAIT' then 'APPROVED' when v_state='NOT_WORKED' then 'RECORDED_NOT_WORKED'
    else 'MATCH_ACCEPTED' end,'NEXT_PUBLISHED',pg_catalog.jsonb_build_object('work_event_id',v_event,'generation_id',v_generation.id,
      'approval_id',v_approval.id,'revision_id',v_stage.revision_id,'command_id',v_publication.command_id,
      'approved_pay_ex_vat',v_total::text),v_key||':payment-event');
  select coalesce(max(s.sequence),0)+1 into v_step from public.weekly_exceptional_orchestration_steps s where s.orchestration_run_id=v_run.id;
  insert into public.weekly_exceptional_orchestration_steps(orchestration_run_id,sequence,step_kind,idempotency_key,
    allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,before_state_fingerprint,bounded_owner_response_json,
    owner_response_hash,after_state_fingerprint,outcome,completed_at_utc)
  values(v_run.id,v_step,'PUBLISH_NEXT',v_key,'public.weekly_exceptional_pay_publish_next_v1','jsonb->jsonb',v_request_hash,
    v_run.before_state_fingerprint,private.bpay_next_protected_receipt_json_v1(v_run.id,false),v_request_hash,v_request_hash,
    'COMPLETE',pg_catalog.transaction_timestamp());
  return private.bpay_next_protected_receipt_json_v1(v_run.id,false);
exception when no_data_found or too_many_rows then
  raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_SCOPE_INVALID';
end
$function$;

alter function private.bpay_next_protected_receipt_json_v1(uuid,boolean) owner to postgres;
alter function private.bpay_next_protected_local_receipt_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_protected_require_authorised_root_v1(uuid) owner to postgres;
alter function private.bpay_next_protected_lock_current_root_v1(uuid,uuid,uuid,date,text) owner to postgres;
alter function private.bpay_next_stage_protected_source_v1(uuid,uuid,uuid,jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_next_publication_status_v1(jsonb) owner to postgres;
alter function public.weekly_exceptional_pay_publish_next_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_protected_receipt_json_v1(uuid,boolean),
  private.bpay_next_protected_local_receipt_v1(uuid,uuid,uuid),
  private.bpay_next_protected_require_authorised_root_v1(uuid),
  private.bpay_next_protected_lock_current_root_v1(uuid,uuid,uuid,date,text),
  private.bpay_next_stage_protected_source_v1(uuid,uuid,uuid,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_exceptional_pay_next_publication_status_v1(jsonb),
  public.weekly_exceptional_pay_publish_next_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_next_publication_status_v1(jsonb),
  public.weekly_exceptional_pay_publish_next_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
