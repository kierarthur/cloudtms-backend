-- Immutable CloudTMS TEST function snapshot, page 14.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- private._candidate_daily_save_recalculate_atomic_v1(uuid,integer,jsonb,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_daily_save_recalculate_atomic_v1(p_workflow_id uuid, p_generation integer, p_materialisation jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_current_fin public.timesheets_financials%rowtype;
  v_candidate public.candidates%rowtype;
  v_contract public.contracts%rowtype;
  v_input jsonb;
  v_patch jsonb;
  v_snapshot jsonb;
  v_rate_request jsonb;
  v_context jsonb;
  v_context_hash bytea;
  v_supplied_rate jsonb;
  v_fresh_rate jsonb;
  v_signature jsonb;
  v_before_signature text;
  v_after_signature text;
  v_write_result jsonb;
  v_financials_id uuid;
  v_receipt jsonb;
  v_client_id uuid;
  v_rate_type text;
begin
  if jsonb_typeof(p_materialisation)<>'object'
     or p_materialisation->>'contract_version'<>'CANDIDATE_DAILY_ATOMIC_FINALISATION_V2' then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_INVALID' using errcode='22023';
  end if;
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_generation
  for update;
  if not found or v_workflow.workflow_kind<>'DAILY' or v_workflow.scope<>'DAILY'
     or v_workflow.target_timesheet_id is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;
  if nullif(p_materialisation->>'workflow_id','')::uuid is distinct from v_workflow.id
     or nullif(p_materialisation->>'workflow_generation','')::integer is distinct from v_workflow.generation
     or nullif(p_materialisation->>'timesheet_id','')::uuid is distinct from v_workflow.target_timesheet_id then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_IDENTITY_MISMATCH' using errcode='40001';
  end if;
  v_input:=private._candidate_daily_canonical_save_input_v1(v_workflow.id,v_workflow.generation);
  if coalesce(p_materialisation->>'canonical_save_input_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
     or decode(p_materialisation->>'canonical_save_input_sha256_hex','hex')
        is distinct from private._candidate_sha256_jsonb_v1(v_input) then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_STALE' using errcode='40001';
  end if;
  v_patch:=p_materialisation->'timesheet_patch_json';
  if jsonb_typeof(v_patch)<>'object'
     or (v_patch - 'worked_minutes') is distinct from (v_input->'timesheet_patch_json') then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_MISMATCH' using errcode='40001';
  end if;
  v_snapshot:=p_materialisation->'canonical_snapshot';
  v_rate_request:=p_materialisation->'rate_request';
  v_supplied_rate:=p_materialisation->'resolved_rate_row';
  if jsonb_typeof(v_snapshot)<>'object' or jsonb_typeof(v_rate_request)<>'object'
     or jsonb_typeof(v_supplied_rate)<>'object' then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_INVALID' using errcode='22023';
  end if;

  select * into v_timesheet
  from public.timesheets
  where timesheet_id=v_workflow.target_timesheet_id and is_current=true
    and archived_at_utc is null and sheet_scope='DAILY'::public.timesheet_scope_enum
  for update;
  if not found then raise exception 'CANDIDATE_DAILY_SHIFT_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_current_fin
  from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc
  limit 1 for update;
  if not found or v_current_fin.candidate_id is distinct from v_workflow.candidate_id then
    raise exception 'CANDIDATE_DAILY_SHIFT_IDENTITY_MISMATCH' using errcode='40001';
  end if;
  if v_timesheet.authorised_at_server is not null or v_current_fin.authorised_at_utc is not null
     or v_current_fin.locked_by_invoice_id is not null or v_current_fin.paid_at_utc is not null then
    raise exception 'CANDIDATE_RECORD_MUTATION_LOCKED' using errcode='55000';
  end if;
  if not private._candidate_daily_entitled_v1(v_workflow.candidate_id) then
    raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,null,false);
  v_before_signature:=coalesce(
    nullif(btrim(v_signature->>'backend_row_signature'),''),
    nullif(btrim(v_signature->>'row_signature'),'')
  );
  if v_before_signature is null
     or v_before_signature is distinct from nullif(btrim(p_materialisation->>'expected_pre_save_row_signature'),'') then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_STALE' using errcode='40001';
  end if;

  v_context:=private._candidate_daily_context_contract_v1(
    v_workflow.id,v_workflow.generation
  );
  v_context_hash:=private._candidate_sha256_jsonb_v1(v_context);
  if coalesce(p_materialisation->>'canonical_context_sha256_hex','') !~ '^[0-9a-fA-F]{64}$'
     or v_workflow.daily_context_sha256 is null
     or decode(p_materialisation->>'canonical_context_sha256_hex','hex')
        is distinct from v_workflow.daily_context_sha256
     or v_context_hash is distinct from v_workflow.daily_context_sha256
     or nullif(v_context->>'pre_save_row_signature','') is distinct from v_before_signature then
    raise exception 'CANDIDATE_DAILY_LOCKED_CONTEXT_STALE' using errcode='40001';
  end if;

  select * into v_candidate from public.candidates where id=v_workflow.candidate_id and active=true;
  if not found then raise exception 'CANDIDATE_DAILY_ENTITLEMENT_REQUIRED' using errcode='55000'; end if;
  if v_timesheet.contract_id is not null then
    select * into v_contract from public.contracts
    where id=v_timesheet.contract_id and candidate_id=v_workflow.candidate_id;
  end if;
  v_client_id:=coalesce(v_current_fin.client_id,v_contract.client_id);
  v_rate_type:=case when upper(coalesce(v_candidate.pay_method,'')) in ('PAYE','UMBRELLA')
    then upper(v_candidate.pay_method) else 'UMBRELLA' end;
  if nullif(v_rate_request->>'k','')::uuid is distinct from v_timesheet.timesheet_id
     or nullif(v_rate_request->>'candidate_id','')::uuid is distinct from v_workflow.candidate_id
     or nullif(v_rate_request->>'client_id','')::uuid is distinct from v_client_id
     or nullif(v_rate_request->>'date','')::date
        is distinct from nullif(v_context->>'canonical_work_date','')::date
     or upper(coalesce(v_rate_request->>'rate_type',''))
        is distinct from upper(coalesce(v_context->>'rate_type',''))
     or nullif(v_rate_request->>'role','') is distinct from nullif(v_context->>'role','')
     or nullif(v_rate_request->>'band','') is distinct from nullif(v_context->>'band','')
     or nullif(v_snapshot->>'role','') is distinct from nullif(v_context->>'role','')
     or nullif(v_snapshot->>'band','') is distinct from nullif(v_context->>'band','')
     or encode(private._candidate_sha256_jsonb_v1(
          coalesce(v_snapshot->'policy_snapshot_json','{}'::jsonb)
        ),'hex') is distinct from v_context->>'financial_policy_sha256_hex' then
    raise exception 'CANDIDATE_DAILY_RATE_CONTEXT_STALE' using errcode='40001';
  end if;
  select to_jsonb(r) into v_fresh_rate
  from public.tsfin_resolve_rates_batch(jsonb_build_array(v_rate_request)) r
  limit 1;
  if v_fresh_rate is null
     or private._candidate_daily_rate_contract_v1(v_fresh_rate)
        is distinct from private._candidate_daily_rate_contract_v1(v_supplied_rate)
     or private._candidate_daily_rate_contract_v1(v_fresh_rate)
        is distinct from private._candidate_daily_rate_contract_v1(
          jsonb_build_object(
            'source_kind',v_snapshot#>>'{rate_source_refs_json,kind}',
            'override_id',v_snapshot#>>'{rate_source_refs_json,override_id}',
            'default_id',v_snapshot#>>'{rate_source_refs_json,default_id}',
            'rate_type',v_snapshot#>>'{rate_source_refs_json,rate_type}',
            'pay_day',v_snapshot->'pay_day','pay_night',v_snapshot->'pay_night',
            'pay_sat',v_snapshot->'pay_sat','pay_sun',v_snapshot->'pay_sun','pay_bh',v_snapshot->'pay_bh',
            'charge_day',v_snapshot->'charge_day','charge_night',v_snapshot->'charge_night',
            'charge_sat',v_snapshot->'charge_sat','charge_sun',v_snapshot->'charge_sun','charge_bh',v_snapshot->'charge_bh'
          )
        ) then
    raise exception 'CANDIDATE_DAILY_RATE_CONTEXT_STALE' using errcode='40001';
  end if;
  if nullif(v_snapshot->>'timesheet_id','')::uuid is distinct from v_timesheet.timesheet_id
     or nullif(v_snapshot->>'candidate_id','')::uuid is distinct from v_workflow.candidate_id
     or nullif(v_snapshot->>'client_id','')::uuid is distinct from v_client_id
     or nullif(v_snapshot->>'worked_start_iso','')::timestamptz
        is distinct from nullif(v_patch->>'worked_start_iso','')::timestamptz
     or nullif(v_snapshot->>'worked_end_iso','')::timestamptz
        is distinct from nullif(v_patch->>'worked_end_iso','')::timestamptz
     or nullif(v_snapshot->>'break_start_iso','')::timestamptz
        is distinct from nullif(v_patch->>'break_start_iso','')::timestamptz
     or nullif(v_snapshot->>'break_end_iso','')::timestamptz
        is distinct from nullif(v_patch->>'break_end_iso','')::timestamptz
     or nullif(v_snapshot->>'break_minutes','')::integer
        is distinct from nullif(v_patch->>'break_minutes','')::integer
     or upper(coalesce(v_snapshot->>'processing_status',''))<>'UNPROCESSED' then
    raise exception 'CANDIDATE_DAILY_MATERIALISATION_MISMATCH' using errcode='40001';
  end if;

  update public.timesheets set
    worked_start_iso=nullif(v_patch->>'worked_start_iso','')::timestamptz,
    worked_end_iso=nullif(v_patch->>'worked_end_iso','')::timestamptz,
    worked_minutes=nullif(v_patch->>'worked_minutes','')::integer,
    break_start_iso=nullif(v_patch->>'break_start_iso','')::timestamptz,
    break_end_iso=nullif(v_patch->>'break_end_iso','')::timestamptz,
    break_minutes=nullif(v_patch->>'break_minutes','')::integer,
    actual_schedule_json=v_patch->'actual_schedule_json',
    candidate_submission_route_intent=null,
    updated_at=p_now_utc
  where timesheet_id=v_timesheet.timesheet_id and is_current=true;

  v_write_result:=public.tsfin_write_current_snapshot_single_bounded(
    v_timesheet.timesheet_id,v_timesheet.version,v_snapshot,p_actor_user_id,p_now_utc
  );
  if not coalesce((v_write_result->>'ok')::boolean,false) then
    raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_FAILED'
      using errcode='55000',detail=v_write_result::text;
  end if;
  v_financials_id:=nullif(v_write_result->>'timesheet_financials_id','')::uuid;
  select * into v_current_fin from public.timesheets_financials
  where id=v_financials_id and timesheet_id=v_timesheet.timesheet_id and is_current=true
  for update;
  if not found or v_current_fin.processing_status<>'UNPROCESSED'
     or coalesce(v_current_fin.has_rate_issue,false)
     or coalesce(v_current_fin.has_pay_channel_issue,false) then
    raise exception 'CANDIDATE_DAILY_CANONICAL_RECALCULATION_NOT_READY' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,null,false);
  v_after_signature:=coalesce(
    nullif(btrim(v_signature->>'backend_row_signature'),''),
    nullif(btrim(v_signature->>'row_signature'),'')
  );
  if v_after_signature is null then
    raise exception 'CANDIDATE_DAILY_CANONICAL_SAVE_RECEIPT_INVALID' using errcode='55000';
  end if;
  v_receipt:=jsonb_build_object(
    'contract_version','CANDIDATE_DAILY_ATOMIC_FINALISATION_RECEIPT_V2',
    'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,'financials_id',v_financials_id,
    'canonical_save_input_sha256_hex',p_materialisation->>'canonical_save_input_sha256_hex',
    'canonical_context_sha256_hex',p_materialisation->>'canonical_context_sha256_hex',
    'pre_save_row_signature',v_before_signature,'post_save_row_signature',v_after_signature,
    'canonical_work_date',private._candidate_daily_work_date_v1(
      nullif(v_patch->>'worked_start_iso','')::timestamptz,v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date
    )
  );
  update public.candidate_submission_workflows set
    canonical_save_input_sha256=decode(p_materialisation->>'canonical_save_input_sha256_hex','hex'),
    canonical_save_row_signature=v_after_signature,
    canonical_save_financials_id=v_financials_id,
    canonical_save_receipt_json=v_receipt,
    canonical_saved_at_utc=p_now_utc,
    updated_at_utc=p_now_utc
  where id=v_workflow.id;
  return v_receipt;
end;
$function$;

-- private._candidate_daily_source_candidate_bind_on_generation_v1(text,text,text,integer)
CREATE OR REPLACE FUNCTION private._candidate_daily_source_candidate_bind_on_generation_v1(p_environment text, p_candidate_global_key text, p_candidate_source_hmac text, p_source_hmac_key_version integer)
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  v_environment text:=upper(nullif(btrim(p_environment),''));
  v_global_key text:=upper(nullif(btrim(p_candidate_global_key),''));
  v_candidate_id uuid;
  v_candidate_count integer:=0;
  v_link private.candidate_daily_source_links%rowtype;
  v_candidate_link_history_count integer:=0;
begin
  if v_environment not in ('TEST','LIVE')
     or v_global_key !~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
     or p_candidate_source_hmac !~ '^[a-f0-9]{64}$'
     or p_source_hmac_key_version<>1 then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  perform 1
  from public.candidates c
  where c.active is true and upper(btrim(c.key_norm))=v_global_key
  order by c.id
  for update;

  select count(*),min(c.id::text)::uuid
    into v_candidate_count,v_candidate_id
  from public.candidates c
  where c.active is true and upper(btrim(c.key_norm))=v_global_key;

  if v_candidate_count=0 then
    raise exception using errcode='55000',message='IDENTITY_LINK_MISSING';
  end if;
  if v_candidate_count<>1 then
    raise exception using errcode='55000',message='IDENTITY_LINK_AMBIGUOUS';
  end if;

  insert into private.candidate_daily_authority_scopes(
    environment,candidate_id,authority_mode,canonical_version,transition_in_progress
  )
  values(v_environment,v_candidate_id,'GOOGLE_PRIMARY',0,false)
  on conflict(environment,candidate_id) do nothing;

  perform 1
  from private.candidate_daily_authority_scopes s
  where s.environment=v_environment and s.candidate_id=v_candidate_id
  for update;

  select *
    into v_link
  from private.candidate_daily_source_links l
  where l.environment=v_environment
    and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
    and l.hmac_key_version=p_source_hmac_key_version
    and l.identifier_hmac=p_candidate_source_hmac
  for update;

  if v_link.link_id is not null then
    if v_link.candidate_id<>v_candidate_id then
      raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
    end if;
    if v_link.state in ('PRIMARY','OVERLAP')
       and v_link.valid_from_utc<=now()
       and (v_link.valid_to_utc is null or v_link.valid_to_utc>now()) then
      return v_candidate_id;
    end if;
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
  end if;

  perform 1
  from private.candidate_daily_source_links l
  where l.environment=v_environment
    and l.candidate_id=v_candidate_id
    and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
  order by l.link_id
  for update;

  select count(*) into v_candidate_link_history_count
  from private.candidate_daily_source_links l
  where l.environment=v_environment
    and l.candidate_id=v_candidate_id
    and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID';

  if v_candidate_link_history_count<>0 then
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
  end if;

  insert into private.candidate_daily_source_links(
    environment,candidate_id,source_system,canonicalization_version,link_group_id,
    identifier_hmac,hmac_key_version,state,evidence_sha256
  )
  values(
    v_environment,v_candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    gen_random_uuid(),p_candidate_source_hmac,p_source_hmac_key_version,'PRIMARY',
    private._candidate_daily_json_sha256_v1(jsonb_build_object(
      'authority','FIRST_GENERATION_EXACT_CID1_LINK',
      'environment',v_environment,
      'candidate_id',v_candidate_id,
      'candidate_global_key_sha256',
        encode(extensions.digest(convert_to(v_global_key,'UTF8'),'sha256'),'hex'),
      'source_system','GOOGLE_CREDENTIALLY_PUBLIC_ID',
      'identifier_hmac',p_candidate_source_hmac,
      'hmac_key_version',p_source_hmac_key_version
    ))
  );

  return v_candidate_id;
exception
  when unique_violation then
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
end;
$function$;

-- private._candidate_daily_source_candidate_v1(text,text)
CREATE OR REPLACE FUNCTION private._candidate_daily_source_candidate_v1(p_environment text, p_candidate_source_hmac text)
 RETURNS uuid
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare v_candidate_id uuid; v_count integer;
begin
  if p_candidate_source_hmac !~ '^[a-f0-9]{64}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select count(distinct l.candidate_id),min(l.candidate_id::text)::uuid
    into v_count,v_candidate_id
  from private.candidate_daily_source_links l
  where l.environment=upper(p_environment)
    and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID'
    and l.identifier_hmac=p_candidate_source_hmac
    and l.state in ('PRIMARY','OVERLAP')
    and l.valid_from_utc<=now() and (l.valid_to_utc is null or l.valid_to_utc>now());
  if v_count=0 then raise exception using errcode='55000',message='IDENTITY_LINK_MISSING'; end if;
  if v_count<>1 then raise exception using errcode='55000',message='IDENTITY_LINK_AMBIGUOUS'; end if;
  return v_candidate_id;
end;
$function$;

-- private._candidate_daily_source_link_identity_history_guard_v1()
CREATE OR REPLACE FUNCTION private._candidate_daily_source_link_identity_history_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  v_existing_count integer:=0;
begin
  if new.environment not in ('TEST','LIVE')
     or new.source_system<>'GOOGLE_CREDENTIALLY_PUBLIC_ID'
     or new.hmac_key_version<=0
     or new.identifier_hmac !~ '^[a-f0-9]{64}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      new.environment||':SOURCE:'||new.hmac_key_version::text||':'||new.identifier_hmac,
      0
    )
  );

  if tg_op='UPDATE' and (
    new.environment is distinct from old.environment
    or new.source_system is distinct from old.source_system
    or new.hmac_key_version is distinct from old.hmac_key_version
    or new.identifier_hmac is distinct from old.identifier_hmac
    or new.candidate_id is distinct from old.candidate_id
  ) then
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
  end if;

  select count(*)
  into v_existing_count
  from private.candidate_daily_source_links l
  where l.environment=new.environment
    and l.source_system=new.source_system
    and l.hmac_key_version=new.hmac_key_version
    and l.identifier_hmac=new.identifier_hmac
    and (tg_op='INSERT' or l.link_id<>new.link_id);

  if v_existing_count<>0 then
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
  end if;

  return new;
end;
$function$;

-- private._candidate_daily_transition_immutable_v1()
CREATE OR REPLACE FUNCTION private._candidate_daily_transition_immutable_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
begin
  raise exception using errcode='55000', message='CANDIDATE_DAILY_TRANSITION_IMMUTABLE';
end;
$function$;

-- private._candidate_daily_work_date_v1(timestamp with time zone,timestamp with time zone,date)
CREATE OR REPLACE FUNCTION private._candidate_daily_work_date_v1(p_worked_start_iso timestamp with time zone, p_scheduled_start_iso timestamp with time zone DEFAULT NULL::timestamp with time zone, p_fallback_date date DEFAULT NULL::date)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog'
AS $function$
  select coalesce(
    (p_worked_start_iso at time zone 'Europe/London')::date,
    (p_scheduled_start_iso at time zone 'Europe/London')::date,
    p_fallback_date
  );
$function$;

-- private._candidate_dataset_overlay_v1(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_dataset_overlay_v1(p_payload jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_key text;
  v_rows jsonb;
begin
  if jsonb_typeof(v_payload)<>'object'
     or not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return v_payload;
  end if;
  foreach v_key in array array['unprocessed_rows','processed_rows','rows'] loop
    if jsonb_typeof(v_payload->v_key)='array' then
      select coalesce(jsonb_agg(private._candidate_office_context_overlay_v1(item.value) order by item.ordinality),'[]'::jsonb)
      into v_rows
      from jsonb_array_elements(v_payload->v_key) with ordinality item(value,ordinality);
      v_payload:=jsonb_set(v_payload,array[v_key],v_rows,true);
    end if;
  end loop;
  return v_payload;
end;
$function$;

-- private._candidate_draft_totals_guard_v1(uuid,jsonb)
CREATE OR REPLACE FUNCTION private._candidate_draft_totals_guard_v1(p_contract_week_id uuid, p_totals_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_policy jsonb;
  v_hours numeric;
  v_additional numeric;
  v_expenses numeric;
  v_import boolean;
  v_role text;
  v_import_authority jsonb;
begin
  if not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return '{}'::jsonb;
  end if;
  select * into v_week from public.contract_weeks where id=p_contract_week_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id;
  v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,v_week.week_ending_date);
  v_hours:=private._candidate_json_numeric_sum(coalesce(p_totals_json->'hours','{}'::jsonb));
  v_additional:=private._candidate_json_numeric_sum(coalesce(p_totals_json->'additional_units_week','{}'::jsonb))
    +private._candidate_json_numeric_sum(coalesce(p_totals_json->'additional_units_per_day','{}'::jsonb));
  v_expenses:=private._candidate_json_numeric_sum(coalesce(p_totals_json->'expenses_draft','{}'::jsonb));
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,v_week.timesheet_id,null,v_week.week_ending_date
  );
  v_import:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);
  if v_import and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  if coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false)
     and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then
    raise exception 'HOURS_AND_EXPENSES_REQUIRE_SEPARATE_TIMESHEETS' using errcode='22023';
  end if;
  v_role:=case
    when v_import then 'IMPORT_HOURS'
    when v_expenses<>0 and v_hours=0 and v_additional=0 then 'EXPENSE_ONLY'
    when (v_hours<>0 or v_additional<>0) and v_expenses=0 then 'HOURS_ONLY'
    when not coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false) and v_expenses<>0 then 'COMBINED_ALLOWED'
    else 'FLEXIBLE' end;
  return jsonb_build_object('record_role',v_role,'hours_value',v_hours,
    'additional_units_value',v_additional,'expense_value',v_expenses,'policy',v_policy);
end;
$function$;

-- private._candidate_email_eligibility_v1(text,text)
CREATE OR REPLACE FUNCTION private._candidate_email_eligibility_v1(p_environment text, p_email text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_email text := private._candidate_normalize_email(p_email);
  v_candidate_ids jsonb := '[]'::jsonb;
  v_match_count integer := 0;
begin
  v_environment := private._candidate_assert_environment(p_environment);

  if v_email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    return jsonb_build_object(
      'eligible',false,
      'match_count',0,
      'candidate_ids','[]'::jsonb,
      'reason_code','EMAIL_NOT_ELIGIBLE'
    );
  end if;

  select count(*)::integer,
         coalesce(jsonb_agg(candidate_row.id order by candidate_row.id),'[]'::jsonb)
  into v_match_count,v_candidate_ids
  from public.candidates candidate_row
  where candidate_row.active=true
    and lower(btrim(coalesce(candidate_row.email,'')))=v_email;

  if v_environment='LIVE' and v_match_count>1 then
    return jsonb_build_object(
      'eligible',false,
      'match_count',v_match_count,
      'candidate_ids','[]'::jsonb,
      'reason_code','LIVE_DUPLICATE_ACTIVE_EMAIL'
    );
  end if;

  return jsonb_build_object(
    'eligible',v_match_count>0,
    'match_count',v_match_count,
    'candidate_ids',case when v_match_count>0 then v_candidate_ids else '[]'::jsonb end,
    'selection_required',v_environment='TEST' and v_match_count>1,
    'reason_code',case when v_match_count>0 then 'ELIGIBLE' else 'EMAIL_NOT_ELIGIBLE' end
  );
end;
$function$;

-- private._candidate_evidence_lineage_guard_v1()
CREATE OR REPLACE FUNCTION private._candidate_evidence_lineage_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_paper_page jsonb;
  v_expected_kind text;
  v_expected_role text;
begin
  if new.candidate_component_id is null then return new; end if;
  select * into v_component from public.candidate_submission_components
  where id=new.candidate_component_id;
  if not found or v_component.state<>'IMMUTABLE' or v_component.immutable_at_utc is null then
    raise exception 'CANDIDATE_COMPONENT_NOT_FINAL_SIGNED' using errcode='55000';
  end if;
  if v_component.component_kind='SIGNED_RETURN' then
    if v_component.storage_key is distinct from new.storage_key then
      raise exception 'CANDIDATE_COMPONENT_NOT_FINAL_SIGNED' using errcode='55000';
    end if;
    select * into v_workflow from public.candidate_submission_workflows
    where id=v_component.workflow_id and generation=v_component.workflow_generation
      and route='PAPER' and paper_return_manifest_sha256 is not null;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='55000'; end if;
    select expected_page into v_paper_page
    from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected_page
    where expected_page->>'page_key'=v_component.paper_return_page_key;
    if not found then raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='55000'; end if;
  elsif v_component.final_signed_render_state<>'READY'
     or v_component.final_signed_storage_key is distinct from new.storage_key then
    raise exception 'CANDIDATE_COMPONENT_NOT_FINAL_SIGNED' using errcode='55000';
  end if;
  if v_component.timesheet_id is not null and v_component.timesheet_id is distinct from new.timesheet_id then
    raise exception 'CANDIDATE_COMPONENT_TIMESHEET_MISMATCH' using errcode='22023';
  end if;

  if v_component.component_kind='SIGNED_RETURN' then
    v_expected_kind:=case v_paper_page->>'component_kind'
      when 'HOURS_TIMESHEET' then 'TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE'
      when 'EXPENSE_SUMMARY' then 'OTHER'
      when 'EXPENSE_EVIDENCE' then v_paper_page->>'expense_category'
      else null end;
    v_expected_role:=case v_paper_page->>'component_kind'
      when 'HOURS_TIMESHEET' then 'SIGNED_TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
      when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
      when 'EXPENSE_EVIDENCE' then 'SOURCE_EVIDENCE'
      else null end;
  else
    v_expected_kind:=case v_component.component_kind
      when 'HOURS_TIMESHEET' then 'TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE'
      when 'EXPENSE_SUMMARY' then 'OTHER'
      when 'EXPENSE_EVIDENCE' then v_component.expense_category
      else null end;
    v_expected_role:=case v_component.component_kind
      when 'HOURS_TIMESHEET' then 'SIGNED_TIMESHEET'
      when 'MILEAGE_FORM' then 'MILEAGE_CLAIM_FORM'
      when 'EXPENSE_SUMMARY' then 'EXPENSE_MILEAGE_APPROVAL_SUMMARY'
      when 'EXPENSE_EVIDENCE' then 'SOURCE_EVIDENCE'
      else null end;
  end if;
  if v_expected_kind is null
     or upper(btrim(new.kind))<>v_expected_kind
     or new.document_role<>v_expected_role then
    raise exception 'CANDIDATE_COMPONENT_DOCUMENT_ROLE_MISMATCH' using errcode='22023';
  end if;
  return new;
end;
$function$;

-- private._candidate_feature_enabled_current_v1(text)
CREATE OR REPLACE FUNCTION private._candidate_feature_enabled_current_v1(p_feature_key text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
  select coalesce((sd.candidate_app_feature_flags_json->>btrim(coalesce(p_feature_key,'')))::boolean,false)
  from public.settings_defaults sd
  where sd.id=1;
$function$;

-- private._candidate_feature_enabled_v1(text,text)
CREATE OR REPLACE FUNCTION private._candidate_feature_enabled_v1(p_environment text, p_feature_key text)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_flags jsonb;
  v_key text := btrim(coalesce(p_feature_key,''));
begin
  v_environment := private._candidate_assert_environment(p_environment);
  if v_key='' then
    raise exception 'CANDIDATE_FEATURE_KEY_REQUIRED' using errcode='22023';
  end if;

  select candidate_app_feature_flags_json into v_flags
  from public.settings_defaults where id=1;

  return coalesce((v_flags->>v_key)::boolean,false);
exception
  when invalid_text_representation then
    raise exception 'CANDIDATE_FEATURE_FLAG_INVALID'
      using errcode='22023', detail=jsonb_build_object('code','CANDIDATE_FEATURE_FLAG_INVALID','feature',v_key)::text;
end;
$function$;

-- private._candidate_financial_content_sha256_v1(uuid)
CREATE OR REPLACE FUNCTION private._candidate_financial_content_sha256_v1(p_financials_id uuid)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_fin public.timesheets_financials%rowtype;
  v_business_content jsonb;
begin
  select * into v_fin
  from public.timesheets_financials
  where id=p_financials_id;
  if not found then
    raise exception 'CANDIDATE_CANONICAL_FINANCIALS_NOT_FOUND' using errcode='P0002';
  end if;

  -- Keep every current/future business and economic field by default. Remove
  -- only row identity, version routing, timestamps and lifecycle state so that
  -- moving an otherwise identical snapshot between route generations cannot
  -- change the economic fingerprint.
  v_business_content:=to_jsonb(v_fin)-array[
    'id','created_at','updated_at','computed_at_utc','timesheet_id',
    'timesheet_version','is_current','is_stale','processing_status',
    'authorised_at_utc','authorised_by_user_id','paid_at_utc',
    'locked_by_invoice_id','locked_at_utc'
  ];
  return private._candidate_sha256_jsonb_v1(v_business_content);
end;
$function$;

-- private._candidate_google_mobile_normalize_v1(text)
CREATE OR REPLACE FUNCTION private._candidate_google_mobile_normalize_v1(p_mobile text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO ''
AS $function$
  select case
    when v like '0044%' then substring(v from 3)
    when v like '0%' then '44'||substring(v from 2)
    else v
  end
  from (select pg_catalog.regexp_replace(coalesce(p_mobile,''),'[^0-9]','','g') v) normalized;
$function$;

-- private._candidate_import_authoritative_v1(uuid,uuid,uuid,jsonb,date)
CREATE OR REPLACE FUNCTION private._candidate_import_authoritative_v1(p_client_id uuid, p_contract_id uuid DEFAULT NULL::uuid, p_timesheet_id uuid DEFAULT NULL::uuid, p_snapshot_json jsonb DEFAULT NULL::jsonb, p_evaluation_date date DEFAULT NULL::date)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_evaluation_date date:=coalesce(p_evaluation_date,(current_timestamp at time zone 'Europe/London')::date);
  v_client public.client_settings%rowtype;
  v_contract public.contracts%rowtype;
  v_fin jsonb:=coalesce(p_snapshot_json,'{}'::jsonb);
  v_client_import boolean:=false;
  v_contract_import boolean:=false;
  v_snapshot_import boolean:=false;
  v_has_external_source_rows boolean:=false;
  v_route_import boolean:=false;
  v_route_type text:='';
  v_route_no_timesheet_required boolean:=false;
  v_source text:='NONE';
begin
  if p_client_id is null then
    raise exception 'CANDIDATE_IMPORT_CLIENT_REQUIRED' using errcode='22023';
  end if;
  select * into v_client from public.client_settings cs
  where cs.client_id=p_client_id
    and (cs.effective_from is null or cs.effective_from<=v_evaluation_date)
  order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc
  limit 1;
  v_client_import:=coalesce(v_client.is_nhsp,false)
    or (coalesce(v_client.requires_hr,false) and coalesce(v_client.no_timesheet_required,false));
  if p_contract_id is not null then
    select * into v_contract from public.contracts c
    where c.id=p_contract_id and c.client_id=p_client_id;
    if not found then raise exception 'CANDIDATE_IMPORT_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    v_contract_import:=coalesce(v_contract.weekly_timesheet_source::text,'NONE')<>'NONE';
  end if;
  if p_snapshot_json is null and p_timesheet_id is not null then
    select to_jsonb(tf) into v_fin from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id and tf.is_current=true
    order by tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc limit 1;
    v_fin:=coalesce(v_fin,'{}'::jsonb);
  end if;
  if p_timesheet_id is not null then
    begin
      select upper(coalesce(summary.route_type,'')),
        coalesce(summary.client_no_timesheet_required,false)
      into v_route_type,v_route_no_timesheet_required
      from public.v_timesheets_summary summary
      where summary.timesheet_id=p_timesheet_id
      limit 1;
    exception when others then
      v_route_type:='';
      v_route_no_timesheet_required:=false;
    end;
  end if;
  v_route_import:=v_route_type in ('WEEKLY_NHSP','WEEKLY_NHSP_ADJUSTMENT')
    or (v_route_type='WEEKLY_HEALTHROSTER' and v_route_no_timesheet_required);
  v_has_external_source_rows:=case
    when jsonb_typeof(v_fin->'external_source_rows_json')='array'
      then jsonb_array_length(v_fin->'external_source_rows_json')>0
    when jsonb_typeof(v_fin->'external_source_rows_json')='object'
      then v_fin->'external_source_rows_json'<>'{}'::jsonb
    else false end;
  v_snapshot_import:=nullif(v_fin->>'nhsp_import_id','') is not null
    or v_has_external_source_rows
    or upper(coalesce(v_fin->>'basis','')) in (
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_SELF_BILL_ADJUSTMENT',
      'HEALTHROSTER_ADJUSTMENT','HEALTHROSTER_WEEKLY','HEALTHROSTER_WEEKLY_ADJUSTMENT'
    );
  v_source:=case
    when coalesce(v_client.is_nhsp,false) then 'NHSP_CLIENT'
    when coalesce(v_client.requires_hr,false) and coalesce(v_client.no_timesheet_required,false) then 'HEALTHROSTER_CREATE_CLIENT'
    when v_contract_import then 'CONTRACT_SOURCE_'||upper(v_contract.weekly_timesheet_source::text)
    when v_route_import then 'ROUTE_'||v_route_type
    when nullif(v_fin->>'nhsp_import_id','') is not null then 'NHSP_IMPORT_SNAPSHOT'
    when v_has_external_source_rows then 'EXTERNAL_SOURCE_SNAPSHOT'
    when v_snapshot_import then 'IMPORT_BASIS_SNAPSHOT'
    else 'NONE' end;
  return jsonb_build_object(
    'is_import_authoritative',v_client_import or v_contract_import or v_route_import or v_snapshot_import,
    'source_family',v_source,
    'candidate_hours_view_only',v_client_import or v_contract_import or v_route_import or v_snapshot_import,
    'mandatory_expense_separation',v_client_import or v_contract_import or v_route_import or v_snapshot_import
  );
end;
$function$;

-- private._candidate_json_numeric_sum(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_json_numeric_sum(p_value jsonb)
 RETURNS numeric
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog'
AS $function$
  select coalesce(sum((value_item #>> '{}')::numeric),0::numeric)
  from jsonb_path_query(coalesce(p_value,'{}'::jsonb), '$.** ? (@.type() == "number")') value_item;
$function$;

-- private._candidate_manager_email_allowed_v1(jsonb,text,jsonb)
CREATE OR REPLACE FUNCTION private._candidate_manager_email_allowed_v1(p_policy jsonb, p_email text, p_barred_domains jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'pg_catalog', 'private'
AS $function$
declare
  v_email text:=private._candidate_normalize_email(p_email);
  v_domain text;
  v_full boolean:=false;
  v_domain_allowed boolean:=false;
  v_barred boolean:=false;
  v_barred_policy_ready boolean:=jsonb_typeof(p_barred_domains)='array'
    and jsonb_array_length(p_barred_domains)>0;
  v_free boolean:=coalesce((p_policy->>'allow_free_business_email')::boolean,false);
begin
  if v_email !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    return jsonb_build_object('allowed',false,'reason_code','MANAGER_EMAIL_INVALID');
  end if;
  v_domain:=split_part(v_email,'@',2);
  select exists(select 1 from jsonb_array_elements_text(coalesce(p_policy->'approved_emails','[]'::jsonb)) x where lower(btrim(x))=v_email) into v_full;
  select exists(select 1 from jsonb_array_elements_text(coalesce(p_policy->'approved_domains','[]'::jsonb)) x
    where private._candidate_normalize_domain_v1(x)=v_domain) into v_domain_allowed;
  select exists(select 1 from jsonb_array_elements_text(coalesce(p_barred_domains,'[]'::jsonb)) x
    where private._candidate_normalize_domain_v1(x)=v_domain) into v_barred;
  return jsonb_build_object(
    'allowed',v_full or v_domain_allowed or (v_free and v_barred_policy_ready and not v_barred),
    'reason_code',case
      when v_full then 'APPROVED_EMAIL'
      when v_domain_allowed then 'APPROVED_DOMAIN'
      when v_free and not v_barred_policy_ready then 'BARRED_DOMAIN_POLICY_NOT_CONFIGURED'
      when v_free and not v_barred then 'FREE_BUSINESS_EMAIL'
      when v_barred then 'BARRED_DOMAIN'
      else 'MANAGER_EMAIL_NOT_APPROVED' end,
    'email_normalized',v_email,
    'domain',v_domain
  );
end;
$function$;

-- private._candidate_manager_mail_retire_v1(uuid,integer,uuid[],text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_manager_mail_retire_v1(p_workflow_id uuid, p_expected_workflow_generation integer, p_approval_request_ids uuid[], p_reason_code text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_reason text:=upper(btrim(coalesce(p_reason_code,'')));
  v_mail public.mail_outbox%rowtype;
  v_seen integer:=0;
  v_retired integer:=0;
  v_sent integer:=0;
  v_failed integer:=0;
  v_request_count integer:=0;
  v_sent_request_ids uuid[]:=array[]::uuid[];
  v_request_id uuid;
begin
  if p_workflow_id is null or coalesce(p_expected_workflow_generation,0)<1
     or coalesce(cardinality(p_approval_request_ids),0)=0 or v_reason='' then
    raise exception 'CANDIDATE_MANAGER_MAIL_RETIREMENT_CONTEXT_INVALID' using errcode='22023';
  end if;

  select count(*) into v_request_count
  from (
    select request_row.id
    from public.candidate_approval_requests request_row
    where request_row.id=any(p_approval_request_ids)
      and request_row.workflow_id=p_workflow_id
      and request_row.workflow_generation=p_expected_workflow_generation
      and request_row.method='EMAIL'
    order by request_row.id
    for update
  ) locked_requests;
  if v_request_count<>cardinality(p_approval_request_ids) then
    raise exception 'CANDIDATE_MANAGER_MAIL_RETIREMENT_REQUEST_MISMATCH'
      using errcode='40001';
  end if;

  for v_mail in
    select mail_row.*
    from public.mail_outbox mail_row
    where upper(coalesce(mail_row.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
      and mail_row.payment_scope_json->>'candidate_manager_workflow_id'=p_workflow_id::text
      and mail_row.payment_scope_json->>'candidate_manager_workflow_generation'
            =p_expected_workflow_generation::text
      and coalesce(mail_row.payment_scope_json->>'candidate_approval_request_id','')
            =any(select request_id::text from unnest(p_approval_request_ids) request_id)
      and upper(coalesce(mail_row.payment_scope_json->>'candidate_manager_mail_kind',''))
            in ('INITIAL','REMINDER','RENEWAL')
    order by mail_row.id
    for update
  loop
    v_seen:=v_seen+1;
    v_request_id:=(v_mail.payment_scope_json->>'candidate_approval_request_id')::uuid;
    if v_mail.status<>'SENT'
       and nullif(btrim(coalesce(v_mail.attempt_lease_token,'')),'') is not null
       and (v_mail.attempt_lease_expires_at_utc is null
         or v_mail.attempt_lease_expires_at_utc>p_now_utc) then
      raise exception 'CANDIDATE_MANAGER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_MANAGER_MAIL_DELIVERY_IN_PROGRESS',
          'workflow_id',p_workflow_id,'workflow_generation',p_expected_workflow_generation,
          'approval_request_id',v_request_id,'mail_outbox_id',v_mail.id
        )::text;
    end if;
    if v_mail.status='SENT' and v_mail.sent_at is not null
       and upper(coalesce(v_mail.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK') then
      v_sent:=v_sent+1;
      if not v_request_id=any(v_sent_request_ids) then
        v_sent_request_ids:=array_append(v_sent_request_ids,v_request_id);
      end if;
    elsif v_mail.status='FAILED' then
      v_failed:=v_failed+1;
      update public.mail_outbox mail_row set
        payment_scope_json=coalesce(mail_row.payment_scope_json,'{}'::jsonb)
          ||jsonb_build_object(
            'candidate_manager_mail_retired',true,
            'candidate_manager_mail_retired_at_utc',p_now_utc,
            'candidate_manager_mail_retired_reason',v_reason
          )
      where mail_row.id=v_mail.id;
    else
      update public.mail_outbox mail_row set
        attachments='[]'::jsonb,
        scheduled_for_utc='infinity'::timestamptz,
        next_attempt_at_utc='infinity'::timestamptz,
        attempt_lease_token=null,
        attempt_leased_at_utc=null,
        attempt_lease_expires_at_utc=null,
        payment_scope_json=coalesce(mail_row.payment_scope_json,'{}'::jsonb)
          ||jsonb_build_object(
            'candidate_manager_mail_retired',true,
            'candidate_manager_mail_retired_at_utc',p_now_utc,
            'candidate_manager_mail_retired_reason',v_reason
          )
      where mail_row.id=v_mail.id and mail_row.status='QUEUED' and mail_row.sent_at is null;
      if found then v_retired:=v_retired+1; end if;
    end if;
  end loop;

  return jsonb_build_object(
    'ok',true,'workflow_id',p_workflow_id,
    'workflow_generation',p_expected_workflow_generation,
    'reason_code',v_reason,'mail_seen_count',v_seen,
    'mail_retired_count',v_retired,'sent_mail_count',v_sent,
    'failed_mail_count',v_failed,
    'withdrawal_required',cardinality(v_sent_request_ids)>0,
    'withdrawal_request_ids',to_jsonb(v_sent_request_ids)
  );
end;
$function$;

-- private._candidate_manager_request_mail_guard_v1()
CREATE OR REPLACE FUNCTION private._candidate_manager_request_mail_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
begin
  if old.method='EMAIL' and old.state in ('PENDING','APPROVED')
     and new.state is distinct from old.state then
    perform private._candidate_manager_mail_retire_v1(
      old.workflow_id,old.workflow_generation,array[old.id],
      'APPROVAL_REQUEST_'||upper(new.state),coalesce(new.updated_at_utc,now())
    );
  end if;
  return new;
end;
$function$;

-- private._candidate_normalize_domain_array_v1(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_normalize_domain_array_v1(p_domains jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog', 'private'
AS $function$
  select coalesce(jsonb_agg(domain order by domain),'[]'::jsonb)
  from (
    select distinct private._candidate_normalize_domain_v1(value) as domain
    from jsonb_array_elements_text(case when jsonb_typeof(p_domains)='array' then p_domains else '[]'::jsonb end)
    where private._candidate_normalize_domain_v1(value)<>''
  ) normalized;
$function$;

-- private._candidate_normalize_domain_v1(text)
CREATE OR REPLACE FUNCTION private._candidate_normalize_domain_v1(p_domain text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog'
AS $function$
  select lower(regexp_replace(btrim(coalesce(p_domain,'')), '^@', ''));
$function$;

-- private._candidate_normalize_email(text)
CREATE OR REPLACE FUNCTION private._candidate_normalize_email(p_email text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog'
AS $function$
  select lower(btrim(coalesce(p_email,'')));
$function$;

-- private._candidate_normalize_manager_policy_v1(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_normalize_manager_policy_v1(p_policy jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'pg_catalog', 'private'
AS $function$
  select coalesce(case when jsonb_typeof(p_policy)='object' then p_policy else '{}'::jsonb end,'{}'::jsonb)
    ||jsonb_build_object(
      'approved_emails',coalesce((
        select jsonb_agg(email order by email)
        from (
          select distinct private._candidate_normalize_email(value) email
          from jsonb_array_elements_text(case when jsonb_typeof(p_policy->'approved_emails')='array'
            then p_policy->'approved_emails' else '[]'::jsonb end)
          where private._candidate_normalize_email(value)<>''
        ) emails
      ),'[]'::jsonb),
      'approved_domains',private._candidate_normalize_domain_array_v1(p_policy->'approved_domains')
    );
$function$;

-- private._candidate_notification_insert_v1(uuid,uuid,uuid,uuid,text,text,text,jsonb,jsonb,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_notification_insert_v1(p_account_id uuid, p_candidate_id uuid, p_workflow_id uuid, p_timesheet_id uuid, p_event_type text, p_preference_category text, p_template_key text, p_template_params jsonb, p_deep_link jsonb, p_dedupe_key text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare v_row public.candidate_notifications%rowtype;
begin
  insert into public.candidate_notifications(
    account_id,candidate_id,workflow_id,timesheet_id,event_type,preference_category,
    template_key,template_params,deep_link_json,dedupe_key,created_at_utc
  ) values (
    p_account_id,p_candidate_id,p_workflow_id,p_timesheet_id,btrim(p_event_type),btrim(p_preference_category),
    btrim(p_template_key),coalesce(p_template_params,'{}'::jsonb),coalesce(p_deep_link,'{}'::jsonb),btrim(p_dedupe_key),p_now_utc
  ) on conflict (dedupe_key) do update set dedupe_key=excluded.dedupe_key
  returning * into v_row;
  return jsonb_build_object('notification_id',v_row.id,'dedupe_key',v_row.dedupe_key,'state',v_row.state);
end;
$function$;

-- private._candidate_office_action_v1(text,text,text,boolean,text,text,boolean,boolean,text,text,jsonb,jsonb,text,boolean)
CREATE OR REPLACE FUNCTION private._candidate_office_action_v1(p_code text, p_label text, p_group text, p_enabled boolean, p_disabled_reason_code text, p_disabled_reason text, p_requires_confirmation boolean, p_requires_reason boolean, p_method text, p_path text, p_fixed_body jsonb DEFAULT '{}'::jsonb, p_required_user_inputs jsonb DEFAULT '[]'::jsonb, p_idempotency text DEFAULT 'REQUIRED'::text, p_prominent boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
  select jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_ACTION_V1',
    'code',upper(btrim(coalesce(p_code,''))),
    'label',coalesce(p_label,''),
    'group',upper(btrim(coalesce(p_group,'GENERAL'))),
    'prominent',coalesce(p_prominent,false),
    'enabled',coalesce(p_enabled,false),
    'disabled_reason_code',case when coalesce(p_enabled,false) then null else p_disabled_reason_code end,
    'disabled_reason',case when coalesce(p_enabled,false) then null else p_disabled_reason end,
    'requires_confirmation',coalesce(p_requires_confirmation,false),
    'requires_reason',coalesce(p_requires_reason,false),
    'invocation',jsonb_build_object(
      'version',1,
      'kind',case when upper(btrim(coalesce(p_method,'')))='CLIENT'
        then 'CLIENT_DESTINATION' else 'HTTP' end,
      'method',case when upper(btrim(coalesce(p_method,'')))='CLIENT'
        then null else upper(btrim(coalesce(p_method,''))) end,
      'path',p_path,
      'fixed_body',coalesce(p_fixed_body,'{}'::jsonb),
      'required_user_inputs',coalesce(p_required_user_inputs,'[]'::jsonb),
      'idempotency',coalesce(p_idempotency,'REQUIRED')
    )
  );
$function$;

-- private._candidate_office_capabilities_v1(text,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_office_capabilities_v1(p_environment text, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_environment text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null then
    raise exception 'OFFICE_AUTH_REQUIRED' using errcode='28000';
  end if;
  return jsonb_build_object(
    'ok',true,
    'contract_version','CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    'capabilities_version','OFFICE_CANDIDATE_CAPABILITIES_V1',
    'office_contract_version','CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    'projection_version','OFFICE_CANDIDATE_TIMESHEET_V1',
    'typed_action_version',1,
    'batch_version','OFFICE_CANDIDATE_REMINDER_BATCH_V1',
    'environment',v_environment,
    'authority_applies',true,
    'mode','ENABLED',
    'required_office_role','admin',
    'permission_source','OFFICE_ADMIN_ROLE_V1',
    'observed_at_utc',coalesce(p_now_utc,now()),
    'surfaces',jsonb_build_object(
      'simple_timesheet',true,
      'timesheet_summary',true,
      'bulk_process',true,
      'bulk_authorise',true,
      'invoice_generator',true,
      'invoice_issuer',true
    ),
    'permissions',jsonb_build_object(
      'view_candidate_state',true,
      'change_route',true,
      'reject_submission',true,
      'resubmit_rejected',true,
      'send_manager_reminder',true,
      'send_manager_reminder_batch',true,
      'renew_manager_request',true,
      'cancel_manager_request',true,
      'manage_phone_approval',true,
      'manage_paper',true,
      'retry_finalisation',true,
      'mark_no_work',true
    )
  );
end;
$function$;

-- private._candidate_office_context_overlay_v1(jsonb)
CREATE OR REPLACE FUNCTION private._candidate_office_context_overlay_v1(p_payload jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_timesheet_text text;
  v_week_text text;
  v_timesheet_id uuid;
  v_week_id uuid;
  v_capabilities jsonb;
  v_overlay jsonb;
  v_action_overlay jsonb;
  v_workflow_state text;
  v_workflow_generation integer;
  v_approval_state text;
  v_approval_method text;
  v_resend_count integer;
  v_last_sent_at timestamptz;
  v_manager_state text;
  v_can_remind boolean:=false;
  v_key text;
begin
  if jsonb_typeof(v_payload)<>'object'
     or not private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') then
    return v_payload;
  end if;
  v_timesheet_text:=nullif(btrim(coalesce(
    v_payload->>'timesheet_id',v_payload->>'current_timesheet_id',
    v_payload#>>'{row,timesheet_id}',v_payload#>>'{row,current_timesheet_id}',
    v_payload#>>'{data_row,timesheet_id}',v_payload#>>'{row_patch,timesheet_id}','')),'');
  v_week_text:=nullif(btrim(coalesce(
    v_payload->>'contract_week_id',v_payload#>>'{row,contract_week_id}',
    v_payload#>>'{data_row,contract_week_id}',v_payload#>>'{row_patch,contract_week_id}','')),'');
  if v_timesheet_text~*'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_timesheet_id:=v_timesheet_text::uuid;
  end if;
  if v_week_text~*'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    v_week_id:=v_week_text::uuid;
  end if;
  if v_timesheet_id is null and v_week_id is null then return v_payload; end if;

  begin
    v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet_id,v_week_id,'{}'::jsonb);
  exception when others then
    return v_payload||jsonb_build_object(
      'candidate_capabilities_unavailable',true,
      'candidate_capabilities_error_code','CANDIDATE_CAPABILITIES_UNAVAILABLE'
    );
  end;

  select w.state,w.generation,a.state,a.method,a.resend_count,a.last_sent_at_utc
  into v_workflow_state,v_workflow_generation,v_approval_state,v_approval_method,v_resend_count,v_last_sent_at
  from public.candidate_submission_workflows w
  left join lateral (
    select ar.state,ar.method,ar.resend_count,ar.last_sent_at_utc
    from public.candidate_approval_requests ar
    where ar.workflow_id=w.id and ar.workflow_generation=w.generation
    order by ar.updated_at_utc desc,ar.id desc limit 1
  ) a on true
  where (v_timesheet_id is not null and w.target_timesheet_id=v_timesheet_id)
     or (v_week_id is not null and w.contract_week_id=v_week_id)
  order by w.updated_at_utc desc,w.id desc
  limit 1;

  v_manager_state:=case
    when v_approval_state='APPROVED' or v_workflow_state in (
      'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED'
    ) then 'MANAGER_APPROVED'
    when v_approval_state='PENDING' or v_workflow_state='AWAITING_MANAGER_APPROVAL' then 'AWAITING_MANAGER_APPROVAL'
    else null end;
  v_can_remind:=v_manager_state='AWAITING_MANAGER_APPROVAL'
    and v_approval_method='EMAIL'
    and coalesce(v_resend_count,0)<5
    and (v_last_sent_at is null or v_last_sent_at<=now()-interval '24 hours');

  v_action_overlay:=jsonb_build_object(
    'can_send_manager_reminder',v_can_remind,
    'can_reject_candidate_submission',coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false),
    'candidate_reject_scope',v_capabilities->>'reject_scope',
    'can_attach_timesheet',coalesce((v_capabilities->>'can_attach_timesheet')::boolean,false),
    'can_edit_candidate_expenses',coalesce((v_capabilities->>'can_edit_expenses')::boolean,false)
  );
  v_overlay:=jsonb_strip_nulls(jsonb_build_object(
    'candidate_capabilities',v_capabilities,
    'candidate_record_role',v_capabilities->>'record_role',
    'candidate_capability_hash',v_capabilities->>'capability_hash',
    'candidate_manager_approval_state',v_manager_state,
    'candidate_workflow_state',v_workflow_state,
    'candidate_workflow_generation',v_workflow_generation,
    'candidate_can_send_manager_reminder',v_can_remind,
    'candidate_can_reject_submission',coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false),
    'candidate_reject_scope',v_capabilities->>'reject_scope',
    'candidate_attach_options',jsonb_build_object(
      'TIMESHEET',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_timesheet')::boolean,false)),
      'MILEAGE',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_mileage_evidence')::boolean,false)),
      'TRAVEL',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_travel_evidence')::boolean,false)),
      'ACCOMMODATION',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_accommodation_evidence')::boolean,false)),
      'OTHER',jsonb_build_object('enabled',coalesce((v_capabilities->>'can_attach_other_evidence')::boolean,false))
    ),
    'candidate_preview_border',jsonb_build_object(
      'width_px',4,
      'tone',case when coalesce((v_capabilities->>'can_attach_timesheet')::boolean,false) then 'success' else 'danger' end
    )
  ));
  v_payload:=v_payload||v_overlay||jsonb_build_object(
    'action_flags',coalesce(v_payload->'action_flags','{}'::jsonb)||v_action_overlay
  );
  foreach v_key in array array['row','data_row','row_patch','details','left_pane'] loop
    if jsonb_typeof(v_payload->v_key)='object' then
      v_payload:=jsonb_set(v_payload,array[v_key],(v_payload->v_key)||v_overlay||jsonb_build_object(
        'action_flags',coalesce(v_payload#>array[v_key,'action_flags'],'{}'::jsonb)||v_action_overlay
      ),true);
    end if;
  end loop;
  return v_payload;
end;
$function$;

-- private._candidate_office_paper_retry_receipt_v1(uuid,uuid,integer,text,integer,jsonb,boolean,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_office_paper_retry_receipt_v1(p_actor_user_id uuid, p_workflow_id uuid, p_generation integer, p_idempotency_key text, p_http_status integer DEFAULT NULL::integer, p_result jsonb DEFAULT NULL::jsonb, p_reconstruct boolean DEFAULT false, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_key text:=nullif(btrim(coalesce(p_idempotency_key,'')),'');
  v_request_hash text;
  v_existing_before jsonb;
  v_existing_after jsonb;
  v_result jsonb:=p_result;
  v_http_status integer:=p_http_status;
  v_mail public.mail_outbox%rowtype;
  v_scope jsonb;
  v_state text;
begin
  if p_actor_user_id is null or p_workflow_id is null or p_generation is null
     or p_generation<1 or v_key is null or length(v_key)>200 then
    raise exception 'CANDIDATE_PAPER_RETRY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  v_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_REQUEST_V1',
    'workflow_id',p_workflow_id,'generation',p_generation,
    'idempotency_key',v_key
  )::text,'UTF8'),'sha256'),'hex');
  perform pg_advisory_xact_lock(hashtextextended(
    'OFFICE_CANDIDATE_PAPER_RETRY:'||p_actor_user_id::text||':'
      ||p_workflow_id::text||':'||v_key,0
  ));
  select ae.before_json,ae.after_json into v_existing_before,v_existing_after
  from public.audit_events ae
  where ae.object_type='cloudtms_office_candidate_paper_retry'
    and ae.object_id_text=p_workflow_id::text
    and ae.actor_user_id=p_actor_user_id
    and ae.correlation_id=v_key
  order by ae.ts_utc desc,ae.id desc limit 1;
  if found then
    if v_existing_before->>'request_sha256' is distinct from v_request_hash then
      raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object(
      'found',true,'idempotent_replay',true
    );
  end if;

  if v_result is null and coalesce(p_reconstruct,false) then
    select mail.* into v_mail
    from public.mail_outbox mail
    where mail.type='TIMESHEET_QR'
      and mail.context_kind='timesheets'
      and upper(coalesce(mail.payment_scope_json->>'candidate_mail_authority',''))
        ='CANDIDATE_PAPER_V1'
      and mail.payment_scope_json->>'candidate_workflow_id'=p_workflow_id::text
      and mail.payment_scope_json->>'candidate_workflow_generation'=p_generation::text
      and mail.payment_scope_json->>'candidate_paper_pack_operation_id'=v_key
    order by mail.created_at_utc desc,mail.id desc
    limit 1;
    if found then
      v_scope:=coalesce(v_mail.payment_scope_json,'{}'::jsonb);
      v_state:=upper(coalesce(v_scope->>'candidate_paper_pack_operation_state',''));
      if v_state='READY' then
        v_http_status:=200;
        v_result:=jsonb_build_object(
          'ok',true,'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_key,'workflow_id',p_workflow_id,
          'generation',p_generation,'paper_pack_state','READY',
          'page_count',coalesce(
            nullif(v_scope->>'candidate_complete_pack_page_count','')::integer,0
          ),'reconstructed_from_inner_receipt',true
        );
      elsif v_state in ('FAILED_RETRYABLE','FAILED_TERMINAL') then
        v_http_status:=case when v_state='FAILED_RETRYABLE' then 503 else 409 end;
        v_result:=jsonb_build_object(
          'ok',false,'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_key,'workflow_id',p_workflow_id,
          'generation',p_generation,'paper_pack_state',v_state,
          'retryable',v_state='FAILED_RETRYABLE',
          'error_code',coalesce(
            nullif(v_scope->>'candidate_paper_pack_failure_code',''),
            'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
          ),
          'next_retry_at_utc',v_scope->'candidate_paper_pack_next_retry_at_utc',
          'reconstructed_from_inner_receipt',true
        );
      end if;
    end if;
  end if;
  if v_result is null then
    return jsonb_build_object(
      'ok',true,'found',false,
      'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RECEIPT_V1'
    );
  end if;
  if jsonb_typeof(v_result)<>'object'
     or v_result->>'contract_version'<>'OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3'
     or v_result->>'workflow_id'<>p_workflow_id::text
     or nullif(v_result->>'generation','')::integer<>p_generation
     or v_result->>'idempotency_key'<>v_key
     or upper(coalesce(v_result->>'paper_pack_state','')) not in (
       'READY','FAILED_RETRYABLE','FAILED_TERMINAL'
     )
     or v_http_status not in (200,409,503) then
    raise exception 'CANDIDATE_PAPER_RETRY_RESULT_INVALID' using errcode='22023';
  end if;
  v_result:=jsonb_build_object(
    'ok',true,'found',true,
    'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RECEIPT_V1',
    'http_status',v_http_status,'result',v_result,'idempotent_replay',false
  );
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'cloudtms_office_candidate_paper_retry',p_workflow_id::text,
    'CANDIDATE_OFFICE_PAPER_RETRY_COMPLETED',
    jsonb_build_object('request_sha256',v_request_hash),v_result,
    'Durable Office PAPER retry operation result',v_key,coalesce(p_now_utc,now())
  );
  return v_result;
end;
$function$;

-- private._candidate_office_projection_identity_v1(uuid,uuid)
CREATE OR REPLACE FUNCTION private._candidate_office_projection_identity_v1(p_timesheet_id uuid, p_contract_week_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_requested public.timesheets%rowtype;
  v_requested_current_id uuid;
  v_week public.contract_weeks%rowtype;
  v_week_timesheet public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_week_current_id uuid;
  v_current_id uuid;
  v_current_count integer:=0;
  v_week_count integer:=0;
  v_resolved_week_id uuid;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select requested.* into v_requested
    from public.timesheets requested
    where requested.timesheet_id=p_timesheet_id;
    if not found then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND' using errcode='P0002';
    end if;
    if v_requested.is_current and v_requested.archived_at_utc is null then
      v_requested_current_id:=v_requested.timesheet_id;
    elsif nullif(btrim(coalesce(v_requested.booking_id,'')),'') is not null then
      select count(distinct current_row.timesheet_id)::integer,
        min(current_row.timesheet_id::text)::uuid
      into v_current_count,v_requested_current_id
      from public.timesheets current_row
      where current_row.is_current=true
        and current_row.archived_at_utc is null
        and current_row.contract_id is not distinct from v_requested.contract_id
        and current_row.week_ending_date is not distinct from v_requested.week_ending_date
        and nullif(btrim(coalesce(current_row.booking_id,'')),'')
          =nullif(btrim(v_requested.booking_id),'');
      if v_current_count<>1 then
        raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
      end if;
    else
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
  end if;

  if p_contract_week_id is not null then
    select week_row.* into v_week
    from public.contract_weeks week_row
    where week_row.id=p_contract_week_id;
    if not found or v_week.timesheet_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select week_timesheet.* into v_week_timesheet
    from public.timesheets week_timesheet
    where week_timesheet.timesheet_id=v_week.timesheet_id;
    if not found then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
    if v_week_timesheet.is_current and v_week_timesheet.archived_at_utc is null then
      v_week_current_id:=v_week_timesheet.timesheet_id;
    elsif nullif(btrim(coalesce(v_week_timesheet.booking_id,'')),'') is not null then
      select count(distinct current_row.timesheet_id)::integer,
        min(current_row.timesheet_id::text)::uuid
      into v_current_count,v_week_current_id
      from public.timesheets current_row
      where current_row.is_current=true
        and current_row.archived_at_utc is null
        and current_row.contract_id is not distinct from v_week_timesheet.contract_id
        and current_row.week_ending_date is not distinct from v_week_timesheet.week_ending_date
        and nullif(btrim(coalesce(current_row.booking_id,'')),'')
          =nullif(btrim(v_week_timesheet.booking_id),'');
      if v_current_count<>1 then
        raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
      end if;
    else
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
  end if;

  if v_requested_current_id is not null and v_week_current_id is not null
     and v_requested_current_id is distinct from v_week_current_id then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_current_id:=coalesce(v_requested_current_id,v_week_current_id);

  if p_contract_week_id is null then
    select current_row.* into v_current
    from public.timesheets current_row
    where current_row.timesheet_id=v_current_id;
    if not found then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
    end if;
    if v_current.sheet_scope='DAILY'::public.timesheet_scope_enum then
      -- DAILY is owned by its current timesheet/booking family and has no
      -- contract_weeks row. The requested timesheet is the complete identity.
      v_resolved_week_id:=null;
    else
      select count(*)::integer,min(week_row.id::text)::uuid
      into v_week_count,v_resolved_week_id
      from public.contract_weeks week_row
      where week_row.timesheet_id=v_current_id;
      if v_week_count<>1 then
        raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
      end if;
      select week_row.* into v_week
      from public.contract_weeks week_row
      where week_row.id=v_resolved_week_id;
    end if;
  elsif v_week_current_id is distinct from v_current_id then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;

  return jsonb_build_object(
    'requested_timesheet_id',p_timesheet_id,
    'current_timesheet_id',v_current_id,
    'contract_week_id',v_week.id,
    'additional_seq',v_week.additional_seq,
    'moved',p_timesheet_id is not null and p_timesheet_id is distinct from v_current_id
  );
end;
$function$;

-- private._candidate_office_reject_preview_v1(text,uuid,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_office_reject_preview_v1(p_environment text, p_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_timesheet public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_capabilities jsonb;
  v_signature text;
  v_targets jsonb:='[]'::jsonb;
  v_context jsonb;
  v_context_sha text;
  v_permitted boolean:=false;
  v_disabled_code text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or p_timesheet_id is null then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then raise exception 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND' using errcode='P0002'; end if;
  if not v_timesheet.is_current or v_timesheet.archived_at_utc is not null then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks where timesheet_id=v_timesheet.timesheet_id
  order by updated_at desc,id desc limit 1;
  if v_week.id is null and v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false)->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false)->>'backend_row_signature'
  );
  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet.timesheet_id,v_week.id,'{}'::jsonb);
  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',w.id,
    'workflow_generation',w.generation,
    'workflow_kind',w.workflow_kind,
    'route',w.route,
    'state',w.state,
    'scope',case when w.workflow_kind='CONTRACT_EXPENSE' then 'EXPENSE'
      when w.workflow_kind='CONTRACT_COMBINED' then 'COMBINED' else 'HOURS' end
  ) order by w.id),'[]'::jsonb)
  into v_targets
  from public.candidate_submission_workflows w
  where w.environment=v_environment and (
    (w.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
      and (w.target_timesheet_id=v_timesheet.timesheet_id or w.anchor_timesheet_id=v_timesheet.timesheet_id))
    or (w.state='FINALISED' and w.target_timesheet_id=v_timesheet.timesheet_id)
  );
  v_permitted:=coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false)
    and coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is null
    and v_fin.paid_at_utc is null and v_fin.locked_by_invoice_id is null
    and jsonb_array_length(v_targets)>0;
  v_disabled_code:=case
    when coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is not null then 'CANDIDATE_REQUIRES_UNAUTHORISE'
    when v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY'
    when not coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false) then 'CANDIDATE_ACTION_NOT_ELIGIBLE'
    when jsonb_array_length(v_targets)=0 then 'CANDIDATE_REJECTION_SCOPE_CONFLICT'
    else null end;
  v_context:=jsonb_build_object(
    'contract_version','OFFICE_CANDIDATE_REJECTION_PREVIEW_V1',
    'environment',v_environment,
    'timesheet_id',v_timesheet.timesheet_id,
    'timesheet_version',v_timesheet.version,
    'contract_week_id',v_week.id,
    'row_signature',v_signature,
    'reject_scope',v_capabilities->>'reject_scope',
    'target_workflows',v_targets,
    'permitted',v_permitted,
    'requires_unauthorise',coalesce(v_fin.authorised_at_utc,v_timesheet.authorised_at_server) is not null,
    'protected_financial_history',v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null
  );
  v_context_sha:=encode(extensions.digest(convert_to(v_context::text,'UTF8'),'sha256'),'hex');
  return v_context||jsonb_build_object(
    'ok',true,
    'observed_at_utc',coalesce(p_now_utc,now()),
    'expected_timesheet_id',v_timesheet.timesheet_id,
    'expected_row_signature',v_signature,
    'scope',case v_capabilities->>'reject_scope'
      when 'COMPLETE_EXPENSE_CLAIM' then 'EXPENSE'
      else case when exists(select 1 from jsonb_array_elements(v_targets) x where x->>'workflow_kind'='CONTRACT_COMBINED') then 'COMBINED' else 'HOURS' end end,
    'target_workflow_id',case when jsonb_array_length(v_targets)=1 then (v_targets->0)->>'workflow_id' else null end,
    'target_workflow_generation',case when jsonb_array_length(v_targets)=1 then ((v_targets->0)->>'workflow_generation')::integer else null end,
    'requires_reason',true,
    'disabled_reason_code',v_disabled_code,
    'disabled_reason',case v_disabled_code
      when 'CANDIDATE_REQUIRES_UNAUTHORISE' then 'Unauthorise this timesheet before rejecting the Candidate submission.'
      when 'CANDIDATE_PROTECTED_FINANCIAL_HISTORY' then 'This submission can no longer be rejected because protected financial history exists.'
      when 'CANDIDATE_REJECTION_SCOPE_CONFLICT' then 'CloudTMS could not establish one safe Candidate rejection scope.'
      when 'CANDIDATE_ACTION_NOT_ELIGIBLE' then 'This Candidate submission is not currently eligible for rejection.'
      else null end,
    'context_sha256',v_context_sha,
    'affected_rows',jsonb_build_array(jsonb_build_object(
      'timesheet_id',v_timesheet.timesheet_id,'contract_week_id',v_week.id
    ))
  );
end;
$function$;

-- private._candidate_office_service_context_close_v1()
CREATE OR REPLACE FUNCTION private._candidate_office_service_context_close_v1()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
begin
  perform set_config('cloudtms.office_candidate_context','{}',true);
end;
$function$;

-- private._candidate_office_service_context_open_v1(text,uuid,text,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_office_service_context_open_v1(p_environment text, p_actor_user_id uuid, p_permission text, p_action text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_permission text:=lower(btrim(coalesce(p_permission,'')));
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_context jsonb;
begin
  if p_actor_user_id is null
     or v_permission not in (
       'change_route','reject_submission','send_manager_reminder',
       'send_manager_reminder_batch','renew_manager_request',
       'cancel_manager_request','manage_phone_approval','manage_paper',
       'retry_finalisation'
     )
     or v_action not in (
       'ROUTE_CONFIRM','REJECT_CONFIRM','REMIND','RENEW',
       'MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
       'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE',
       'MANAGER_REFUSE','REGISTER_REVIEW_COMPONENT',
       'REGISTER_FINAL_SIGNED_DOCUMENT','BEGIN_CANONICAL_DAILY_SAVE',
       'PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM','PAPER_PACK_MARK_FAILURE',
       'RETRY_FINALISATION'
     ) then
    raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
  end if;
  v_context:=jsonb_build_object(
    'contract_version','CANDIDATE_OFFICE_SERVICE_CONTEXT_V1',
    'environment',v_environment,
    'actor_user_id',p_actor_user_id,
    'permission',v_permission,
    'action',v_action,
    'opened_at_utc',coalesce(p_now_utc,now())
  );
  perform set_config('cloudtms.office_candidate_context',v_context::text,true);
  return v_context;
end;
$function$;

-- private._candidate_office_service_context_valid_v1(text,uuid,text)
CREATE OR REPLACE FUNCTION private._candidate_office_service_context_valid_v1(p_environment text, p_actor_user_id uuid, p_action text)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_context jsonb;
begin
  begin
    v_context:=nullif(current_setting('cloudtms.office_candidate_context',true),'')::jsonb;
  exception when others then
    return false;
  end;
  return coalesce(v_context->>'contract_version','')='CANDIDATE_OFFICE_SERVICE_CONTEXT_V1'
    and (p_environment is null
      or v_context->>'environment'=private._candidate_assert_environment(p_environment))
    and (p_actor_user_id is null
      or v_context->>'actor_user_id'=p_actor_user_id::text)
    and v_context->>'action'=upper(btrim(coalesce(p_action,'')));
end;
$function$;

-- private._candidate_office_timesheet_projection_v1(text,uuid,uuid,text,text,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_office_timesheet_projection_v1(p_environment text, p_timesheet_id uuid, p_contract_week_id uuid, p_row_key text, p_expected_row_signature text, p_actor_user_id uuid, p_observed_at_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_identity jsonb;
  v_week public.contract_weeks%rowtype;
  v_requested public.timesheets%rowtype;
  v_current public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_capabilities jsonb;
  v_signature text;
  v_manager_first_accepted timestamptz;
  v_manager_accepted timestamptz;
  v_manager_delivery_state text;
  v_manager_provider_status text;
  v_manager_pending integer:=0;
  v_manager_lease boolean:=false;
  v_reminder_eligible boolean:=false;
  v_renewal_eligible boolean:=false;
  v_cancel_eligible boolean:=false;
  v_resends_remaining integer:=0;
  v_actions jsonb:='[]'::jsonb;
  v_action jsonb;
  v_rejections jsonb:='[]'::jsonb;
  v_paper_state text:='NOT_APPLICABLE';
  v_paper_pack jsonb:='{}'::jsonb;
  v_paper_delivery_generation integer;
  v_paper_outbox public.mail_outbox%rowtype;
  v_candidate_status_code text;
  v_candidate_status_label text;
  v_candidate_status_tone text;
  v_reject_preview jsonb;
  v_primary_action jsonb;
  v_writes boolean:=true;
  v_manager_enabled boolean:=true;
  v_paper_enabled boolean:=true;
  v_route_enabled boolean:=true;
  v_expense_email_missing boolean:=false;
  v_active_workflow_state text;
  v_actionable_rejection boolean:=false;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or (p_timesheet_id is null and p_contract_week_id is null) then
    raise exception 'CANDIDATE_OFFICE_PROJECTION_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_identity:=private._candidate_office_projection_identity_v1(
    p_timesheet_id,p_contract_week_id
  );
  if p_timesheet_id is not null then
    select * into v_requested from public.timesheets where timesheet_id=p_timesheet_id;
  end if;
  select * into v_week from public.contract_weeks
  where id=(v_identity->>'contract_week_id')::uuid;
  select * into v_current from public.timesheets
  where timesheet_id=(v_identity->>'current_timesheet_id')::uuid;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_current.timesheet_id and is_current=true
  order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  -- Financial truth outranks Candidate workflow lifecycle on every surface.
  -- Protected records remain readable, but no Candidate mutation may be
  -- advertised once paid, authorised or invoice-locked.
  v_writes:=not (
    v_fin.paid_at_utc is not null
    or v_fin.authorised_at_utc is not null
    or v_current.authorised_at_server is not null
    or v_fin.locked_by_invoice_id is not null
    or upper(coalesce(v_current.status::text,''))='INVOICED'
  );
  v_manager_enabled:=v_writes;
  v_paper_enabled:=v_writes;
  v_route_enabled:=v_writes;
  v_capabilities:=private._candidate_record_capabilities_v1(v_current.timesheet_id,v_week.id,'{}'::jsonb);
  v_signature:=coalesce(
    public.timesheet_lifecycle_guard_signature_v1(v_current.timesheet_id,v_week.id,false)->>'row_signature',
    public.timesheet_lifecycle_guard_signature_v1(v_current.timesheet_id,v_week.id,false)->>'backend_row_signature'
  );

  select w.* into v_workflow
  from public.candidate_submission_workflows w
  where w.environment=v_environment and (
    w.target_timesheet_id=v_current.timesheet_id
    or w.anchor_timesheet_id=v_current.timesheet_id
    or w.contract_week_id=v_week.id
  )
  order by (w.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')) desc,
    w.updated_at_utc desc,w.id desc limit 1;
  if v_workflow.id is not null then
    select ar.* into v_approval from public.candidate_approval_requests ar
    where ar.workflow_id=v_workflow.id and ar.workflow_generation=v_workflow.generation
    order by ar.request_generation desc,ar.updated_at_utc desc,ar.id desc limit 1;
  end if;

  if v_approval.id is not null and v_approval.method='EMAIL' then
    select min(m.sent_at) filter (where m.status='SENT' and m.sent_at is not null
        and upper(coalesce(m.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')),
      max(m.sent_at) filter (where m.status='SENT' and m.sent_at is not null
        and upper(coalesce(m.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')),
      (array_agg(m.status::text order by m.created_at_utc desc,m.id desc))[1],
      (array_agg(m.provider_status order by m.created_at_utc desc,m.id desc))[1],
      count(*) filter (where m.status='QUEUED' and m.sent_at is null
        and lower(coalesce(m.payment_scope_json->>'candidate_manager_mail_retired','false')) in ('false','f','0','no'))::integer,
      bool_or(m.attempt_lease_token is not null and m.attempt_lease_expires_at_utc>p_observed_at_utc)
    into v_manager_first_accepted,v_manager_accepted,v_manager_delivery_state,
      v_manager_provider_status,v_manager_pending,v_manager_lease
    from public.mail_outbox m
    where upper(coalesce(m.payment_scope_json->>'candidate_mail_authority',''))='MANAGER_APPROVAL_V1'
      and m.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
      and m.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
      and m.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
      and m.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text;
  end if;
  v_resends_remaining:=greatest(0,5-coalesce(v_approval.resend_count,0));
  v_reminder_eligible:=v_manager_enabled and v_approval.id is not null
    and v_approval.method='EMAIL' and v_approval.state='PENDING'
    and v_approval.expires_at_utc>p_observed_at_utc
    and v_manager_accepted is not null
    and v_manager_accepted+interval '24 hours'<=p_observed_at_utc
    and v_resends_remaining>0 and coalesce(v_manager_pending,0)=0 and not coalesce(v_manager_lease,false)
    and v_approval.review_manifest_sha256 is not distinct from v_workflow.review_manifest_sha256;
  v_renewal_eligible:=v_manager_enabled and v_approval.id is not null
    and v_approval.method='EMAIL' and (
      v_approval.state='EXPIRED'
      or (v_approval.state='PENDING' and v_approval.expires_at_utc<=p_observed_at_utc)
    ) and v_workflow.state='AWAITING_MANAGER_APPROVAL';
  v_cancel_eligible:=v_manager_enabled and v_approval.id is not null
    and v_approval.state='PENDING' and v_workflow.state='AWAITING_MANAGER_APPROVAL'
    and not coalesce(v_manager_lease,false);

  if v_workflow.route='PAPER' then
    v_paper_delivery_generation:=case
      when v_workflow.state='FINALISED' then greatest(v_workflow.generation-1,1)
      else v_workflow.generation
    end;
    select m.* into v_paper_outbox from public.mail_outbox m
    where m.type='TIMESHEET_QR'
      and m.context_kind='timesheets'
      and m.context_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
      and upper(coalesce(m.payment_scope_json->>'candidate_mail_authority',''))='CANDIDATE_PAPER_V1'
      and m.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and m.payment_scope_json->>'candidate_workflow_generation'=v_paper_delivery_generation::text
      and lower(coalesce(m.payment_scope_json->>'paper_return_manifest_sha256',''))
        =encode(v_workflow.paper_return_manifest_sha256,'hex')
    order by m.created_at_utc desc,m.id desc limit 1;
    if v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED') then
      v_paper_pack:=private._candidate_paper_pack_readiness_v1(
        v_workflow.id,v_workflow.generation
      );
      v_paper_state:=coalesce(v_paper_pack->>'state','STALE');
    else
    v_paper_state:=case
      when v_workflow.state in ('CANCELLED','REJECTED','SUPERSEDED','EXPIRED') then 'RETIRED'
      when lower(coalesce(v_paper_outbox.payment_scope_json->>'candidate_paper_generation_retired','false'))
        in ('true','t','1','yes') then 'RETIRED'
      when lower(coalesce(v_paper_outbox.payment_scope_json->>'candidate_paper_pack_retryable','false'))
        in ('true','t','1','yes') then 'FAILED_RETRYABLE'
      when upper(coalesce(v_paper_outbox.payment_scope_json->>'candidate_paper_pack_failure_class',''))='TERMINAL'
        then 'FAILED_TERMINAL'
      when v_paper_outbox.status::text='FAILED'
        or upper(coalesce(v_current.document_state,''))='FAILED' then 'FAILED_TERMINAL'
      when coalesce((v_paper_outbox.payment_scope_json->>'candidate_paper_pack_ready')::boolean,false) then 'READY'
      when v_workflow.state='AWAITING_PAPER_RETURN' then 'PREPARING'
      else 'STALE' end;
    end if;
  end if;

  select coalesce(jsonb_agg(jsonb_build_object(
    'scope',case when w.workflow_kind='CONTRACT_EXPENSE' then 'EXPENSE'
      when w.workflow_kind='CONTRACT_COMBINED' then 'COMBINED' else 'HOURS' end,
    'workflow_id',w.id,'workflow_generation',w.generation,'state',w.state,
    'reason',w.rejection_reason,
    'rejection_actionable',case when w.state='REJECTED'
      then not private._candidate_rejection_replaced_v1(w.id) else false end,
    'replacement_workflow_id',replacement.id,
    'replacement_state',replacement.state,
    'recovery_action',case when w.state='REJECTED'
      and not private._candidate_rejection_replaced_v1(w.id) then private._candidate_office_action_v1(
      'RESUBMIT_REJECTED','Resubmit rejected submission','REJECTION',v_writes,null,null,true,false,
      'CLIENT','CANDIDATE_APP:WORKFLOW_RESUBMISSION',
      jsonb_build_object('generation',w.generation),'[]'::jsonb,'REQUIRED',true
    ) else null end
  ) order by w.updated_at_utc desc,w.id desc),'[]'::jsonb)
  into v_rejections
  from public.candidate_submission_workflows w
  left join lateral (
    select later.id,later.state
    from public.candidate_submission_workflows later
    where later.replacement_of_workflow_id=w.id
    order by later.created_at_utc, later.id
    limit 1
  ) replacement on true
  where w.environment=v_environment and w.state in ('REFUSED','REJECTED')
    and (w.contract_week_id=v_week.id or w.target_timesheet_id=v_current.timesheet_id
      or w.anchor_timesheet_id=v_current.timesheet_id);
  select exists(
    select 1 from jsonb_array_elements(v_rejections) rejection
    where rejection->>'state'='REJECTED'
      and coalesce((rejection->>'rejection_actionable')::boolean,false)
  ) into v_actionable_rejection;

  if v_workflow.id is not null and v_approval.id is not null and v_approval.method='EMAIL' then
    v_action:=private._candidate_office_action_v1(
      'SEND_MANAGER_REMINDER','Send manager reminder','MANAGER_APPROVAL',v_reminder_eligible,
      case when v_approval.expires_at_utc<=p_observed_at_utc then 'MANAGER_REQUEST_EXPIRED'
        when v_manager_accepted is null then 'MANAGER_DELIVERY_NOT_PROVIDER_ACCEPTED'
        when v_manager_accepted+interval '24 hours'>p_observed_at_utc then 'MANAGER_REMINDER_TOO_EARLY'
        when v_resends_remaining=0 then 'MANAGER_REMINDER_LIMIT_REACHED'
        when coalesce(v_manager_pending,0)>0 then 'MANAGER_DELIVERY_PENDING'
        when coalesce(v_manager_lease,false) then 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS'
        else 'CANDIDATE_ACTION_NOT_ELIGIBLE' end,
      'The manager reminder is not currently available.',true,false,
      'POST','/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/remind',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),'[]'::jsonb,'REQUIRED',false
    );
    v_actions:=v_actions||jsonb_build_array(v_action);
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RENEW_MANAGER_REQUEST','Renew manager approval request','MANAGER_APPROVAL',v_renewal_eligible,
      'MANAGER_REQUEST_NOT_EXPIRED','Renewal becomes available only after the current request expires.',true,false,
      'POST','/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/renew',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),'[]'::jsonb,'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'CANCEL_MANAGER_REQUEST','Cancel manager approval request','MANAGER_APPROVAL',v_cancel_eligible,
      case when coalesce(v_manager_lease,false) then 'CANDIDATE_PROVIDER_HANDOFF_IN_PROGRESS' else 'CANDIDATE_ACTION_NOT_ELIGIBLE' end,
      'This manager approval request cannot currently be cancelled.',true,true,
      'POST','/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/cancel',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(jsonb_build_object('name','reason','type','string','required',true,'max_length',1000)),
      'REQUIRED',false
    ));
  elsif v_workflow.id is not null and v_approval.id is not null and v_approval.method='PHONE' then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'BEGIN_PHONE_REVIEW','Review manager approval by phone','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone review is not currently available.',false,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-review',
       jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),'[]'::jsonb,'REQUIRED',true
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'PREPARE_PHONE_SIGNATURE','Prepare manager signature','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','A phone-approval signature cannot currently be prepared.',false,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/signature/prepare',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(
        jsonb_build_object('name','media_type','type','enum','values',jsonb_build_array('image/png','image/jpeg'),'required',true),
        jsonb_build_object('name','byte_size','type','integer','required',true,'minimum',1)
      ),'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RECORD_PHONE_REVIEW_PROGRESS','Record reviewed document','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone review progress cannot currently be recorded.',false,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-progress',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(
        jsonb_build_object('name','manifest_sha256_hex','type','sha256','required',true),
        jsonb_build_object('name','component_id','type','uuid','required',true),
        jsonb_build_object('name','component_sha256_hex','type','sha256','required',true),
        jsonb_build_object('name','viewed_receipt','type','object','required',true)
      ),'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'APPROVE_BY_PHONE','Approve by phone','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone approval cannot currently be completed.',true,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-approve',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(
        jsonb_build_object('name','manifest_sha256_hex','type','sha256','required',true),
        jsonb_build_object('name','manager_name','type','string','required',true,'max_length',200),
        jsonb_build_object('name','manager_position','type','string','required',true,'max_length',200),
        jsonb_build_object('name','signature_component_id','type','uuid','required',true)
      ),'REQUIRED',true
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'REFUSE_BY_PHONE','Refuse by phone','MANAGER_APPROVAL',v_manager_enabled and v_approval.state='PENDING',
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Phone refusal cannot currently be recorded.',true,true,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/phone-refuse',
      jsonb_build_object('generation',v_workflow.generation,'approval_request_id',v_approval.id,
        'approval_request_generation',v_approval.request_generation),
      jsonb_build_array(jsonb_build_object('name','reason','type','string','required',true,'max_length',1000)),
      'REQUIRED',false
    ));
  end if;

  if v_workflow.id is not null and v_workflow.state in ('RECEIVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE') then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RETRY_FINALISATION','Retry finalisation','FINALISATION',v_writes,
      'CANDIDATE_ACTION_NOT_ELIGIBLE','Finalisation cannot currently be retried.',true,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/retry-finalisation',
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'REQUIRED',false
    ));
  end if;
  if v_workflow.id is not null and v_workflow.route='PAPER' then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'VIEW_PAPER_PACK','View current paper pack','PAPER',v_paper_state='READY',
      'CANDIDATE_PAPER_PACK_NOT_READY','The current paper pack is not ready to view.',false,false,'GET',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/paper-pack?generation='||v_workflow.generation::text,
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'NONE',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'REVIEW_PAPER_RETURN','Review returned paper documents','PAPER',v_paper_state='RETURN_RECEIVED',
      'CANDIDATE_PAPER_RETURN_NOT_RECEIVED','A complete paper return has not been received.',false,false,'GET',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/paper-return-review?generation='||v_workflow.generation::text,
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'NONE',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'RETRY_PAPER_PREPARATION','Retry paper pack preparation','PAPER',
      v_paper_enabled and v_paper_state='FAILED_RETRYABLE',
      'CANDIDATE_PAPER_PACK_RETRY_NOT_READY','Paper pack preparation is not currently retryable.',true,false,'POST',
      '/api/candidate-app/workflows/'||v_workflow.id::text||'/actions/retry-paper-preparation',
      jsonb_build_object('generation',v_workflow.generation),'[]'::jsonb,'REQUIRED',false
    ));
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'ISSUE_REPLACEMENT_PAPER_PACK','Issue replacement paper pack','PAPER',v_route_enabled and v_paper_state in ('READY','FAILED_RETRYABLE','FAILED_TERMINAL','STALE'),
      'CANDIDATE_PAPER_REPLACEMENT_NOT_READY','A replacement paper pack cannot currently be issued.',true,true,'GET',
      '/api/candidate-app/timesheets/'||v_current.timesheet_id::text||'/route-preview?action=REISSUE_QR',
      '{}'::jsonb,'[]'::jsonb,'NONE',false
    ));
  end if;

  begin
    v_reject_preview:=private._candidate_office_reject_preview_v1(
      v_environment,v_current.timesheet_id,p_actor_user_id,p_observed_at_utc
    );
  exception when others then
    v_reject_preview:=jsonb_build_object('permitted',false,'disabled_reason_code','CANDIDATE_ACTION_NOT_ELIGIBLE');
  end;
  v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
    'REJECT_CANDIDATE_SUBMISSION','Reject Candidate submission','REJECTION',
    v_writes and coalesce((v_reject_preview->>'permitted')::boolean,false),
    coalesce(v_reject_preview->>'disabled_reason_code','CANDIDATE_ACTION_NOT_ELIGIBLE'),
    coalesce(v_reject_preview->>'disabled_reason','This Candidate submission is not currently eligible for rejection.'),
    true,true,'GET','/api/candidate-app/timesheets/'||v_current.timesheet_id::text||'/reject-preview',
    '{}'::jsonb,'[]'::jsonb,'NONE',false
  ));
  if coalesce((v_capabilities->>'candidate_no_work_allowed')::boolean,false) then
    v_actions:=v_actions||jsonb_build_array(private._candidate_office_action_v1(
      'MARK_NO_WORK','I did not work this week','CANDIDATE_DESTINATION',v_writes,
      'CANDIDATE_ACTION_NOT_ELIGIBLE','No-work is not currently available for this record.',true,false,
      'CLIENT','CANDIDATE_APP:CONTRACT_WEEK_NO_WORK',jsonb_build_object(
        'contract_week_id',v_week.id,'row_signature',v_signature
      ),'[]'::jsonb,'REQUIRED',false
    ));
  end if;

  v_active_workflow_state:=case when v_workflow.state in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  ) then v_workflow.state else null end;
  v_candidate_status_code:=private._candidate_status_code_v1(
    v_fin.paid_at_utc is not null,
    v_fin.authorised_at_utc is not null or v_current.authorised_at_server is not null,
    v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_current.status::text,''))='INVOICED',
    v_active_workflow_state,v_actionable_rejection,v_fin.processing_status::text,v_week.status::text
  );
  v_candidate_status_label:=initcap(replace(lower(v_candidate_status_code),'_',' '));
  v_candidate_status_tone:=case
    when v_candidate_status_code in ('PAID','AUTHORISED','FINALISED','MANAGER_APPROVED','READY_TO_FINALISE') then 'success'
    when v_candidate_status_code in ('REFUSED','REJECTED','FAILED') then 'danger'
    when v_candidate_status_code in ('AWAITING_MANAGER_APPROVAL','AWAITING_PAPER_RETURN','RECEIVED') then 'warning'
    else 'neutral' end;
  v_expense_email_missing:=coalesce((v_capabilities->>'expense_value')::numeric,0)<>0
    and not coalesce((v_capabilities->>'expense_invoice_email_ready')::boolean,false);
  select action_item into v_primary_action
  from jsonb_array_elements(v_actions) with ordinality actions(action_item,ordinality)
  where coalesce((action_item->>'enabled')::boolean,false)
    and coalesce((action_item->>'prominent')::boolean,false)
  order by ordinality
  limit 1;

  return jsonb_build_object(
    'ok',true,
    'contract_version','OFFICE_CANDIDATE_TIMESHEET_V1',
    'office_contract_version','CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    'typed_action_version',1,
    'observed_at_utc',p_observed_at_utc,
    'requested_identity',jsonb_build_object(
      'timesheet_id',p_timesheet_id,'contract_week_id',p_contract_week_id,
      'row_key',nullif(btrim(coalesce(p_row_key,'')),''),
      'expected_row_signature',nullif(btrim(coalesce(p_expected_row_signature,'')),'')
    ),
    'current_identity',jsonb_build_object(
      'timesheet_id',v_current.timesheet_id,'timesheet_version',v_current.version,
      'contract_week_id',v_week.id,
      'row_key',coalesce(
        nullif(btrim(coalesce(p_row_key,'')),''),v_week.id::text,v_current.timesheet_id::text
      ),
      'row_signature',v_signature,'route_family',v_capabilities->>'route_family',
      'record_role',v_capabilities->>'record_role',
      'moved',coalesce((v_identity->>'moved')::boolean,false),
      'stale_signature',nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null
        and p_expected_row_signature is distinct from v_signature
    ),
    'candidate_status',jsonb_build_object(
      'code',v_candidate_status_code,'label',v_candidate_status_label,
      'tone',v_candidate_status_tone,'description',null
    ),
    'workflow',case when v_workflow.id is null then null else jsonb_build_object(
      'workflow_id',v_workflow.id,'generation',v_workflow.generation,
      'state',v_workflow.state,'workflow_kind',v_workflow.workflow_kind,
      'route',v_workflow.route,'approval_method',v_approval.method,
      'is_current_action_workflow',v_active_workflow_state is not null,
      'historical',v_active_workflow_state is null
    ) end,
    'manager_approval',case when v_approval.id is null then null else jsonb_build_object(
      'method',v_approval.method,'request_id',v_approval.id,
      'request_generation',v_approval.request_generation,'state',v_approval.state,
      'provider_first_accepted_at_utc',v_manager_first_accepted,
      'provider_accepted_at_utc',v_manager_accepted,
      'delivery_state',v_manager_delivery_state,'provider_status',v_manager_provider_status,
      'delivery_pending',coalesce(v_manager_pending,0)>0,
      'provider_handoff_in_progress',coalesce(v_manager_lease,false),
      'expires_at_utc',v_approval.expires_at_utc,'resend_count',v_approval.resend_count,
      'resends_remaining',v_resends_remaining,
      'next_reminder_at_utc',case when v_manager_accepted is not null then v_manager_accepted+interval '24 hours' else null end,
      'reminder_eligible',v_reminder_eligible,'renewal_eligible',v_renewal_eligible,
      'cancel_eligible',v_cancel_eligible
    ) end,
    'paper_pack',jsonb_build_object(
      'state',v_paper_state,'lifecycle_code',v_workflow.state,
      'delivery_generation',v_paper_delivery_generation,
      'page_count',case when coalesce(v_paper_outbox.payment_scope_json,'{}'::jsonb)->>'candidate_complete_pack_page_count'~'^[1-9][0-9]*$'
        then (v_paper_outbox.payment_scope_json->>'candidate_complete_pack_page_count')::integer else null end,
      'reason_code',case when v_paper_state in ('FAILED_RETRYABLE','FAILED_TERMINAL')
        then coalesce(v_paper_pack->>'failure_code',
          v_paper_outbox.payment_scope_json->>'candidate_paper_pack_failure_code',
          'CANDIDATE_PAPER_PACK_'||v_paper_state)
        when v_paper_state='BACKOFF' then 'CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE'
        when v_paper_state='STALE' then 'CANDIDATE_PAPER_PACK_STALE' else null end,
      'failure_scope',coalesce(v_paper_pack->>'failure_scope',
        case when v_paper_outbox.id is not null then 'OUTBOX' else null end),
      'retryable',v_paper_state='FAILED_RETRYABLE',
      'attempt_count',coalesce((v_paper_pack->>'attempt_count')::integer,
        case when coalesce(v_paper_outbox.payment_scope_json,'{}'::jsonb)
          ->>'candidate_paper_pack_attempt_count'~'^[0-9]+$'
        then (v_paper_outbox.payment_scope_json->>'candidate_paper_pack_attempt_count')::integer else 0 end),
      'next_retry_at_utc',coalesce(nullif(v_paper_pack->>'next_retry_at_utc','')::timestamptz,nullif(
        v_paper_outbox.payment_scope_json->>'candidate_paper_pack_next_retry_at_utc',''
      )::timestamptz),
      'retry_in_progress',coalesce((v_paper_pack->>'retry_in_progress')::boolean,false),
      'operation_id',coalesce(v_paper_pack->>'operation_id',
        v_paper_outbox.payment_scope_json->>'candidate_paper_pack_operation_id'),
      'issued_at_utc',v_paper_outbox.sent_at,
      'returned_at_utc',case when v_workflow.state='RECEIVED' then v_workflow.updated_at_utc else null end
    ),
    'rejections',v_rejections,
    'primary_action',v_primary_action,
    'available_actions',v_actions,
    'diagnostics',case when v_expense_email_missing then jsonb_build_array(jsonb_build_object(
      'code','EXPENSE_EMAIL_MISSING','severity','WARNING','message','Expense Email missing',
      'calculation_effect','NONE','authority_effect','PRESENTATION_ONLY'
    )) else '[]'::jsonb end,
    'refresh_hints',jsonb_build_object(
      'summary',true,'simple_timesheet',true,'bulk_process',true,'bulk_authorise',true,
      'affected_timesheet_ids',jsonb_build_array(v_current.timesheet_id),
      'affected_contract_week_ids',case when v_week.id is null
        then '[]'::jsonb else jsonb_build_array(v_week.id) end
    )
  );
end;
$function$;

-- private._candidate_paper_delivery_retire_set_v1(uuid[],integer[],text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_paper_delivery_retire_set_v1(p_workflow_ids uuid[], p_expected_generations integer[], p_reason_code text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_reason text:=upper(btrim(coalesce(p_reason_code,'')));
  v_workflow public.candidate_submission_workflows%rowtype;
  v_source public.timesheets%rowtype;
  v_current_source public.timesheets%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_input record;
  v_owner record;
  v_source_key text;
  v_workflow_source_key text;
  v_source_keys text[]:='{}'::text[];
  v_family_key text;
  v_family_keys text[]:='{}'::text[];
  v_processed_keys text[]:='{}'::text[];
  v_selected_workflow_ids uuid[]:='{}'::uuid[];
  v_selected_count integer:=0;
  v_selected_mail_count integer:=0;
  v_current_source_count integer:=0;
  v_current_token_owner_count integer:=0;
  v_current_token_owner_workflow_id uuid;
  v_current_token_owner_generation integer;
  v_current_token_owner_state text;
  v_current_token_hash text;
  v_result jsonb;
  v_retirement_receipts jsonb:='[]'::jsonb;
  v_source_receipts jsonb:='[]'::jsonb;
  v_preserved_workflows jsonb:='[]'::jsonb;
  v_unselected_nonterminal_workflows jsonb:='[]'::jsonb;
  v_source_invalidated boolean;
  v_source_already_invalidated boolean;
begin
  if coalesce(cardinality(p_workflow_ids),0)<1
     or cardinality(p_workflow_ids) is distinct from cardinality(p_expected_generations)
     or v_reason='' then
    raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_CONTEXT_INVALID' using errcode='22023';
  end if;
  if cardinality(p_workflow_ids)<>cardinality(array(select distinct item from unnest(p_workflow_ids) item)) then
    raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_DUPLICATE_WORKFLOW' using errcode='22023';
  end if;

  -- Establish the common family lock order before freezing any selected
  -- workflow. Each identity is revalidated under row locks below.
  for v_input in
    select workflow.environment,workflow.contract_id,
      coalesce(workflow.week_ending_date,workflow.work_date) as family_date
    from unnest(p_workflow_ids) input(workflow_id)
    join public.candidate_submission_workflows workflow on workflow.id=input.workflow_id
    order by workflow.environment,workflow.contract_id,
      coalesce(workflow.week_ending_date,workflow.work_date)
  loop
    v_family_key:='CANDIDATE_PAPER_FAMILY:'||v_input.environment||':'
      ||coalesce(v_input.contract_id::text,'-')||':'
      ||coalesce(v_input.family_date::text,'-');
    if not (v_family_key=any(v_family_keys)) then
      v_family_keys:=array_append(v_family_keys,v_family_key);
    end if;
  end loop;
  if cardinality(v_family_keys)<1 then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  for v_family_key in
    select family_item from unnest(v_family_keys) family_item order by family_item
  loop
    perform pg_advisory_xact_lock(hashtextextended(v_family_key,0));
  end loop;

  -- Freeze the exact selected workflow set first. Each selected immutable
  -- delivery generation must identify exactly one QR source family.
  for v_input in
    select input.workflow_id,input.expected_generation
    from unnest(p_workflow_ids,p_expected_generations)
      as input(workflow_id,expected_generation)
    order by input.workflow_id
  loop
    select workflow.* into v_workflow
    from public.candidate_submission_workflows workflow
    where workflow.id=v_input.workflow_id
    for update;
    if not found then
      raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
    end if;
    if v_workflow.generation<>v_input.expected_generation then
      raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
    end if;
    if v_workflow.route<>'PAPER'
       or v_workflow.state not in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
      raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_WORKFLOW_INVALID'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_RETIREMENT_SET_WORKFLOW_INVALID',
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'state',v_workflow.state,'route',v_workflow.route
        )::text;
    end if;

    v_selected_count:=v_selected_count+1;
    v_selected_workflow_ids:=array_append(v_selected_workflow_ids,v_workflow.id);
    v_selected_mail_count:=0;
    v_workflow_source_key:=null;
    for v_mail in
      select mail_row.*
      from public.mail_outbox mail_row
      where mail_row.type='TIMESHEET_QR'
        and mail_row.context_kind='timesheets'
        and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
        and mail_row.payment_scope_json->>'candidate_workflow_generation'=
          (case when v_workflow.state='FINALISED'
            then greatest(v_workflow.generation-1,1)
            else v_workflow.generation end)::text
      order by mail_row.id
    loop
      v_selected_mail_count:=v_selected_mail_count+1;
      if v_mail.context_id is null then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'reason','MAIL_CONTEXT_MISSING'
          )::text;
      end if;
      select source_row.* into v_source
      from public.timesheets source_row
      where source_row.timesheet_id=v_mail.context_id;
      if not found
         or v_source.contract_id is distinct from v_workflow.contract_id
         or v_source.week_ending_date is distinct from v_workflow.week_ending_date
         or upper(coalesce(v_source.line_type::text,'')) in ('EXPENSES','MILEAGE') then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'reason','QR_SOURCE_SCOPE_MISMATCH'
          )::text;
      end if;
      v_source_key:=case
        when nullif(btrim(coalesce(v_source.booking_id,'')),'') is not null
          then 'BOOKING:'||v_source.booking_id
        else 'TIMESHEET:'||v_source.timesheet_id::text
      end;
      if v_workflow_source_key is null then
        v_workflow_source_key:=v_source_key;
      elsif v_workflow_source_key<>v_source_key then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'reason','MULTIPLE_QR_SOURCE_FAMILIES'
          )::text;
      end if;
    end loop;
    if v_selected_mail_count<1 or v_workflow_source_key is null then
      raise exception 'CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND',
          'workflow_id',v_workflow.id,'generation',v_workflow.generation
        )::text;
    end if;
    if not (v_workflow_source_key=any(v_source_keys)) then
      v_source_keys:=array_append(v_source_keys,v_workflow_source_key);
    end if;
  end loop;

  -- Coordinate by QR source, not by workflow UUID. Resolve and invalidate the
  -- current token owner first, then retire every selected older generation.
  for v_source_key in
    select source_item from unnest(v_source_keys) as source_item order by source_item
  loop
    perform pg_advisory_xact_lock(hashtextextended(
      'CANDIDATE_PAPER_SOURCE:'||v_source_key,0
    ));
    if v_source_key like 'BOOKING:%' then
      select count(*)::integer into v_current_source_count
      from public.timesheets current_source
      where current_source.booking_id=substring(v_source_key from 9)
        and current_source.is_current=true
        and current_source.archived_at_utc is null
        and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE');
      if v_current_source_count<>1 then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'source_key',v_source_key,'reason','QR_SOURCE_CURRENT_VERSION_CONFLICT'
          )::text;
      end if;
      select current_source.* into v_current_source
      from public.timesheets current_source
      where current_source.booking_id=substring(v_source_key from 9)
        and current_source.is_current=true
        and current_source.archived_at_utc is null
        and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      for update;
    else
      select current_source.* into v_current_source
      from public.timesheets current_source
      where current_source.timesheet_id=substring(v_source_key from 11)::uuid
        and current_source.is_current=true
        and current_source.archived_at_utc is null
        and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      for update;
      if not found then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'source_key',v_source_key,'reason','QR_SOURCE_CURRENT_VERSION_MISSING'
          )::text;
      end if;
    end if;

    -- Freeze every live/received/finalised PAPER workflow and bound outbox row on
    -- this source before checking provider leases or changing any lifecycle.
    perform 1
    from public.candidate_submission_workflows relevant_workflow
    where relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and exists(
        select 1
        from public.mail_outbox relevant_mail
        join public.timesheets mail_source on mail_source.timesheet_id=relevant_mail.context_id
        where relevant_mail.type='TIMESHEET_QR'
          and relevant_mail.context_kind='timesheets'
          and relevant_mail.payment_scope_json->>'candidate_workflow_id'=relevant_workflow.id::text
          and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when relevant_workflow.state='FINALISED'
              then greatest(relevant_workflow.generation-1,1)
              else relevant_workflow.generation end)::text
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      )
    order by relevant_workflow.id
    for update;

    perform 1
    from public.mail_outbox relevant_mail
    join public.candidate_submission_workflows relevant_workflow
      on relevant_workflow.id::text=relevant_mail.payment_scope_json->>'candidate_workflow_id'
    join public.timesheets mail_source on mail_source.timesheet_id=relevant_mail.context_id
    where relevant_mail.type='TIMESHEET_QR'
      and relevant_mail.context_kind='timesheets'
      and relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
        (case when relevant_workflow.state='FINALISED'
          then greatest(relevant_workflow.generation-1,1)
          else relevant_workflow.generation end)::text
      and (case
        when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
          then 'BOOKING:'||mail_source.booking_id
        else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
    order by relevant_mail.id
    for update of relevant_mail;

    -- Source-wide retirement may preserve immutable FINALISED history, but it
    -- must never destroy the only delivery surface of an unselected live or
    -- retryable workflow. Claim-level callers therefore fail closed, while
    -- source-wide callers must explicitly select every affected nonterminal
    -- workflow before any mail, notification or QR mutation occurs.
    select coalesce(jsonb_agg(jsonb_build_object(
      'workflow_id',relevant_workflow.id,
      'generation',relevant_workflow.generation,
      'state',relevant_workflow.state,
      'workflow_kind',relevant_workflow.workflow_kind
    ) order by relevant_workflow.id),'[]'::jsonb)
    into v_unselected_nonterminal_workflows
    from public.candidate_submission_workflows relevant_workflow
    where relevant_workflow.route='PAPER'
      and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED')
      and not (relevant_workflow.id=any(v_selected_workflow_ids))
      and exists(
        select 1
        from public.mail_outbox relevant_mail
        join public.timesheets mail_source
          on mail_source.timesheet_id=relevant_mail.context_id
        where relevant_mail.type='TIMESHEET_QR'
          and relevant_mail.context_kind='timesheets'
          and relevant_mail.payment_scope_json->>'candidate_workflow_id'=
            relevant_workflow.id::text
          and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
            relevant_workflow.generation::text
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      );
    if jsonb_array_length(v_unselected_nonterminal_workflows)>0 then
      raise exception 'CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT',
          'source_key',v_source_key,
          'selected_workflow_ids',to_jsonb(v_selected_workflow_ids),
          'unselected_nonterminal_workflows',v_unselected_nonterminal_workflows
        )::text;
    end if;

    if exists(
      select 1
      from public.mail_outbox leased_mail
      join public.candidate_submission_workflows leased_workflow
        on leased_workflow.id::text=leased_mail.payment_scope_json->>'candidate_workflow_id'
      join public.timesheets mail_source on mail_source.timesheet_id=leased_mail.context_id
      where leased_mail.type='TIMESHEET_QR'
        and leased_mail.context_kind='timesheets'
        and leased_workflow.route='PAPER'
        and leased_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
        and leased_mail.payment_scope_json->>'candidate_workflow_generation'=
          (case when leased_workflow.state='FINALISED'
            then greatest(leased_workflow.generation-1,1)
            else leased_workflow.generation end)::text
        and (case
          when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
            then 'BOOKING:'||mail_source.booking_id
          else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
        and leased_mail.status<>'SENT'
        and nullif(btrim(coalesce(leased_mail.attempt_lease_token,'')),'') is not null
        and (leased_mail.attempt_lease_expires_at_utc is null
          or leased_mail.attempt_lease_expires_at_utc>p_now_utc)
    ) then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS',
          'source_key',v_source_key
        )::text;
    end if;

    v_current_token_owner_workflow_id:=null;
    v_current_token_owner_generation:=null;
    v_current_token_owner_count:=0;
    v_current_token_hash:=null;
    v_source_invalidated:=false;
    v_source_already_invalidated:=false;
    if nullif(btrim(coalesce(v_current_source.qr_token,'')),'') is null then
      v_source_already_invalidated:=true;
    else
      v_current_token_hash:=encode(extensions.digest(
        convert_to(v_current_source.qr_token,'UTF8'),'sha256'
      ),'hex');
      select count(*)::integer,
        (array_agg(owner.workflow_id order by owner.workflow_id))[1],
        (array_agg(owner.workflow_generation order by owner.workflow_id))[1],
        (array_agg(owner.workflow_state order by owner.workflow_id))[1]
      into v_current_token_owner_count,
        v_current_token_owner_workflow_id,v_current_token_owner_generation,
        v_current_token_owner_state
      from (
        select distinct relevant_workflow.id as workflow_id,
          relevant_workflow.generation as workflow_generation,
          relevant_workflow.state as workflow_state
        from public.mail_outbox owner_mail
        join public.candidate_submission_workflows relevant_workflow
          on relevant_workflow.id::text=owner_mail.payment_scope_json->>'candidate_workflow_id'
        join public.timesheets mail_source on mail_source.timesheet_id=owner_mail.context_id
        where owner_mail.type='TIMESHEET_QR'
          and owner_mail.context_kind='timesheets'
          and relevant_workflow.route='PAPER'
          and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
          and owner_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when relevant_workflow.state='FINALISED'
              then greatest(relevant_workflow.generation-1,1)
              else relevant_workflow.generation end)::text
          and lower(coalesce(owner_mail.payment_scope_json->>'qr_token_hash',''))=v_current_token_hash
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      ) owner;
      if v_current_token_owner_count<>1 then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'source_key',v_source_key,
            'reason','CURRENT_QR_TOKEN_OWNER_CONFLICT',
            'owner_count',v_current_token_owner_count
          )::text;
      end if;

      v_result:=private._candidate_paper_delivery_retire_v1(
        v_current_token_owner_workflow_id,v_current_token_owner_generation,
        v_reason,p_now_utc
      );
      if not coalesce((v_result->>'retired')::boolean,false)
         or not coalesce((v_result->>'qr_invalidated')::boolean,false) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
            'source_key',v_source_key,'retirement_receipt',v_result
          )::text;
      end if;
      v_source_invalidated:=true;
      v_processed_keys:=array_append(
        v_processed_keys,
        v_current_token_owner_workflow_id::text||':'||v_current_token_owner_generation::text
      );
      v_retirement_receipts:=v_retirement_receipts||jsonb_build_array(v_result);
      if not (v_current_token_owner_workflow_id=any(v_selected_workflow_ids)) then
        v_preserved_workflows:=v_preserved_workflows||jsonb_build_array(
          jsonb_build_object(
            'workflow_id',v_current_token_owner_workflow_id,
            'generation',v_current_token_owner_generation,
            'workflow_state',v_current_token_owner_state,
            'workflow_preserved',true,'delivery_surface_retired',true
          )
        );
      end if;
    end if;

    -- Every live/received/finalised delivery surface on the source becomes obsolete
    -- when that source is rejected/rotated. Retire all of them, even where the
    -- QR token had already been cleared, while preserving workflows that are
    -- outside the selected rejection set and all immutable sent/R2 history.
    for v_owner in
      select distinct relevant_workflow.id as workflow_id,
        relevant_workflow.generation as expected_generation,
        relevant_workflow.state as workflow_state
      from public.candidate_submission_workflows relevant_workflow
      join public.mail_outbox relevant_mail
        on relevant_mail.payment_scope_json->>'candidate_workflow_id'=
          relevant_workflow.id::text
      join public.timesheets mail_source on mail_source.timesheet_id=relevant_mail.context_id
      where relevant_workflow.route='PAPER'
        and relevant_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
        and relevant_mail.type='TIMESHEET_QR'
        and relevant_mail.context_kind='timesheets'
        and relevant_mail.payment_scope_json->>'candidate_workflow_generation'=
          (case when relevant_workflow.state='FINALISED'
            then greatest(relevant_workflow.generation-1,1)
            else relevant_workflow.generation end)::text
        and (case
          when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
            then 'BOOKING:'||mail_source.booking_id
          else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      order by relevant_workflow.id
    loop
      if (v_owner.workflow_id::text||':'||v_owner.expected_generation::text)=any(v_processed_keys) then
        continue;
      end if;
      v_result:=private._candidate_paper_delivery_retire_v1(
        v_owner.workflow_id,v_owner.expected_generation,v_reason,p_now_utc
      );
      if not coalesce((v_result->>'retired')::boolean,false)
         or (
           not coalesce((v_result->>'qr_invalidated')::boolean,false)
           and not coalesce((v_result->>'qr_already_invalidated')::boolean,false)
         ) then
        raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
            'workflow_id',v_owner.workflow_id,
            'generation',v_owner.expected_generation,
            'retirement_receipt',v_result
          )::text;
      end if;
      v_processed_keys:=array_append(
        v_processed_keys,v_owner.workflow_id::text||':'||v_owner.expected_generation::text
      );
      v_retirement_receipts:=v_retirement_receipts||jsonb_build_array(v_result);
      if not (v_owner.workflow_id=any(v_selected_workflow_ids)) then
        v_preserved_workflows:=v_preserved_workflows||jsonb_build_array(
          jsonb_build_object(
            'workflow_id',v_owner.workflow_id,
            'generation',v_owner.expected_generation,
            'workflow_state',v_owner.workflow_state,
            'workflow_preserved',true,'delivery_surface_retired',true
          )
        );
      end if;
    end loop;

    v_source_receipts:=v_source_receipts||jsonb_build_array(jsonb_build_object(
      'qr_source_timesheet_id',v_current_source.timesheet_id,
      'source_key',v_source_key,
      'current_token_owner_workflow_id',v_current_token_owner_workflow_id,
      'current_token_owner_generation',v_current_token_owner_generation,
      'qr_invalidated',v_source_invalidated,
      'qr_already_invalidated',v_source_already_invalidated,
      'invalidation_proven',v_source_invalidated or v_source_already_invalidated
    ));
  end loop;

  for v_input in
    select input.workflow_id,input.expected_generation
    from unnest(p_workflow_ids,p_expected_generations)
      as input(workflow_id,expected_generation)
  loop
    if not (v_input.workflow_id::text||':'||v_input.expected_generation::text)=any(v_processed_keys) then
      raise exception 'CANDIDATE_PAPER_RETIREMENT_SET_INCOMPLETE'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_RETIREMENT_SET_INCOMPLETE',
          'workflow_id',v_input.workflow_id,'generation',v_input.expected_generation
        )::text;
    end if;
  end loop;

  return jsonb_build_object(
    'retired',true,
    'qr_invalidation_proven',true,
    'selected_workflow_count',v_selected_count,
    'selected_workflow_ids',to_jsonb(v_selected_workflow_ids),
    'source_count',cardinality(v_source_keys),
    'source_receipts',v_source_receipts,
    'retirement_receipts',v_retirement_receipts,
    'preserved_workflows',v_preserved_workflows,
    'reason_code',v_reason
  );
end;
$function$;

-- private._candidate_paper_delivery_retire_v1(uuid,integer,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_paper_delivery_retire_v1(p_workflow_id uuid, p_expected_generation integer, p_reason_code text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_qr_source public.timesheets%rowtype;
  v_qr_current public.timesheets%rowtype;
  v_rejected_target_timesheet_id uuid;
  v_qr_source_timesheet_id uuid;
  v_delivery_generation integer;
  v_source_key text;
  v_reason text:=upper(btrim(coalesce(p_reason_code,'')));
  v_qr_token_hash text;
  v_mail_count integer:=0;
  v_qr_token_hash_missing_count integer:=0;
  v_mail_retired_count integer:=0;
  v_notification_count integer:=0;
  v_qr_invalidated boolean:=false;
  v_qr_already_invalidated boolean:=false;
  v_current_source_count integer:=0;
begin
  if p_workflow_id is null or coalesce(p_expected_generation,0)<1 or v_reason='' then
    raise exception 'CANDIDATE_PAPER_RETIREMENT_CONTEXT_INVALID' using errcode='22023';
  end if;

  select workflow.* into v_workflow
  from public.candidate_submission_workflows workflow
  where workflow.id=p_workflow_id
  for update;
  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.generation<>p_expected_generation then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.route<>'PAPER'
     or v_workflow.state not in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
    return jsonb_build_object(
      'retired',false,'workflow_id',v_workflow.id,
      'generation',v_workflow.generation,'reason_code',v_reason
    );
  end if;

  -- Finalisation advances the workflow generation after freezing the PAPER
  -- return artefacts. RECEIVED remains on the immutable delivery generation
  -- while canonical finalisation is retryable; FINALISED owns the immediately
  -- preceding generation.
  v_delivery_generation:=case
    when v_workflow.state='FINALISED' then greatest(v_workflow.generation-1,1)
    else v_workflow.generation
  end;

  v_rejected_target_timesheet_id:=coalesce(
    v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id
  );
  if v_rejected_target_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_TIMESHEET_NOT_READY' using errcode='55000';
  end if;

  -- Lock every delivery row for this exact immutable PAPER generation before
  -- deciding whether retirement can proceed. A provider-owned active lease is
  -- an explicit retryable lifecycle conflict: the workflow remains current.
  for v_mail in
    select mail_row.*
    from public.mail_outbox mail_row
    where mail_row.type='TIMESHEET_QR'
      and mail_row.context_kind='timesheets'
      and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
      and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_delivery_generation::text
    order by mail_row.id
    for update
  loop
    v_mail_count:=v_mail_count+1;
    if v_mail.context_id is null then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','MAIL_CONTEXT_MISSING'
        )::text;
    end if;
    if v_qr_source_timesheet_id is null then
      v_qr_source_timesheet_id:=v_mail.context_id;
    elsif v_qr_source_timesheet_id<>v_mail.context_id then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','MULTIPLE_MAIL_CONTEXTS'
        )::text;
    end if;
    if v_mail.status<>'SENT'
       and nullif(btrim(coalesce(v_mail.attempt_lease_token,'')),'') is not null
       and (v_mail.attempt_lease_expires_at_utc is null
         or v_mail.attempt_lease_expires_at_utc>p_now_utc) then
      raise exception 'CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_MAIL_DELIVERY_IN_PROGRESS',
          'workflow_id',v_workflow.id,'generation',v_workflow.generation,
          'delivery_generation',v_delivery_generation,
          'mail_outbox_id',v_mail.id
        )::text;
    end if;
    if nullif(btrim(coalesce(v_mail.payment_scope_json->>'qr_token_hash','')),'') is null then
      v_qr_token_hash_missing_count:=v_qr_token_hash_missing_count+1;
    elsif lower(v_mail.payment_scope_json->>'qr_token_hash') !~ '^[0-9a-f]{64}$' then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_TOKEN_HASH_INVALID'
        )::text;
    else
      if v_qr_token_hash is null then
        v_qr_token_hash:=lower(v_mail.payment_scope_json->>'qr_token_hash');
      elsif v_qr_token_hash<>lower(v_mail.payment_scope_json->>'qr_token_hash') then
        raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
          using errcode='40001',detail=jsonb_build_object(
            'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
            'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
            'reason','MULTIPLE_QR_TOKEN_HASHES'
          )::text;
      end if;
    end if;
  end loop;

  if v_mail_count<1 or v_qr_source_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_DELIVERY_RECEIPT_NOT_FOUND',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation
      )::text;
  end if;

  select source_row.* into v_qr_source
  from public.timesheets source_row
  where source_row.timesheet_id=v_qr_source_timesheet_id;
  if not found
     or v_qr_source.contract_id is distinct from v_workflow.contract_id
     or v_qr_source.week_ending_date is distinct from v_workflow.week_ending_date
     or upper(coalesce(v_qr_source.line_type::text,'')) in ('EXPENSES','MILEAGE') then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
        'reason','QR_SOURCE_SCOPE_MISMATCH'
      )::text;
  end if;

  v_source_key:=case
    when nullif(btrim(coalesce(v_qr_source.booking_id,'')),'') is not null
      then 'BOOKING:'||v_qr_source.booking_id
    else 'TIMESHEET:'||v_qr_source.timesheet_id::text
  end;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_PAPER_SOURCE:'||v_source_key,0
  ));

  if nullif(btrim(coalesce(v_qr_source.booking_id,'')),'') is not null then
    select count(*)::integer into v_current_source_count
    from public.timesheets current_source
    where current_source.booking_id=v_qr_source.booking_id
      and current_source.contract_id is not distinct from v_qr_source.contract_id
      and current_source.week_ending_date is not distinct from v_qr_source.week_ending_date
      and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      and current_source.is_current=true
      and current_source.archived_at_utc is null;
    if v_current_source_count<>1 then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_SOURCE_CURRENT_VERSION_CONFLICT'
        )::text;
    end if;
    select current_source.* into v_qr_current
    from public.timesheets current_source
    where current_source.booking_id=v_qr_source.booking_id
      and current_source.contract_id is not distinct from v_qr_source.contract_id
      and current_source.week_ending_date is not distinct from v_qr_source.week_ending_date
      and upper(coalesce(current_source.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      and current_source.is_current=true
      and current_source.archived_at_utc is null
    for update;
  else
    select current_source.* into v_qr_current
    from public.timesheets current_source
    where current_source.timesheet_id=v_qr_source.timesheet_id
      and current_source.is_current=true
      and current_source.archived_at_utc is null
    for update;
    if not found then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_SOURCE_CURRENT_VERSION_MISSING'
        )::text;
    end if;
  end if;

  update public.mail_outbox mail_row
  set attachments='[]'::jsonb,
      scheduled_for_utc='infinity'::timestamptz,
      next_attempt_at_utc='infinity'::timestamptz,
      attempt_lease_token=null,
      attempt_leased_at_utc=null,
      attempt_lease_expires_at_utc=null,
      payment_scope_json=coalesce(mail_row.payment_scope_json,'{}'::jsonb)
        ||jsonb_build_object(
          'candidate_paper_generation_retired',true,
          'candidate_paper_generation_retired_at_utc',p_now_utc,
          'candidate_paper_generation_retired_reason',v_reason,
          'candidate_paper_pack_ready',false,
          'candidate_paper_pack_retryable',false,
          'candidate_paper_pack_failure_class','RETIRED',
          'mail_held_until_pdf_rendered',true,
          'mail_delayed_for_pdf_render',true,
          'mail_hold_reason','CANDIDATE_PAPER_GENERATION_RETIRED',
          'candidate_retired_delivery_receipt',coalesce(
            mail_row.payment_scope_json->'candidate_retired_delivery_receipt',
            jsonb_build_object(
              'attachments',coalesce(mail_row.attachments,'[]'::jsonb),
              'candidate_complete_pack_storage_key',mail_row.payment_scope_json->>'candidate_complete_pack_storage_key',
              'candidate_complete_pack_sha256',mail_row.payment_scope_json->>'candidate_complete_pack_sha256',
              'candidate_complete_pack_size_bytes',mail_row.payment_scope_json->>'candidate_complete_pack_size_bytes',
              'candidate_complete_pack_page_count',mail_row.payment_scope_json->>'candidate_complete_pack_page_count',
              'candidate_complete_pack_media_type',mail_row.payment_scope_json->>'candidate_complete_pack_media_type'
            )
          )
        )
  where mail_row.type='TIMESHEET_QR'
    and mail_row.context_kind='timesheets'
    and mail_row.status<>'SENT'
    and mail_row.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail_row.payment_scope_json->>'candidate_workflow_generation'=v_delivery_generation::text;
  get diagnostics v_mail_retired_count=row_count;

  update public.candidate_notifications notification
  set state='DISMISSED',
      dismissed_at_utc=coalesce(notification.dismissed_at_utc,p_now_utc),
      push_state=case
        when notification.push_state in ('PENDING','FAILED','CLAIMED') then 'SKIPPED'
        else notification.push_state end,
      last_error=case
        when notification.push_state in ('PENDING','FAILED','CLAIMED')
          then 'CANDIDATE_PAPER_GENERATION_RETIRED'
        else notification.last_error end,
      deep_link_json=coalesce(notification.deep_link_json,'{}'::jsonb)
        ||jsonb_build_object(
          'obsolete',true,'obsolete_reason',v_reason,
          'obsolete_at_utc',p_now_utc
        )
  where notification.workflow_id=v_workflow.id
    and notification.event_type='PAPER_PACK_READY'
    and notification.dedupe_key like
      'CANDIDATE_PAPER_PACK_READY_V1:'||v_workflow.id::text||':'||v_delivery_generation::text||':%'
    and (notification.state<>'DISMISSED'
      or notification.push_state in ('PENDING','FAILED','CLAIMED'));
  get diagnostics v_notification_count=row_count;

  -- QR token/document ownership is generation-scoped. Clear it only where the
  -- current token still matches the retiring generation's bound mail receipt;
  -- the existing document invalidation trigger preserves historical bytes and
  -- makes the old printable document non-current.
  if nullif(btrim(coalesce(v_qr_current.qr_token,'')),'') is null then
    v_qr_already_invalidated:=true;
  elsif v_qr_token_hash is null or v_qr_token_hash_missing_count>0 then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
        'reason','QR_TOKEN_HASH_MISSING',
        'qr_source_timesheet_id',v_qr_current.timesheet_id
      )::text;
  elsif encode(extensions.digest(
      convert_to(v_qr_current.qr_token,'UTF8'),'sha256'
    ),'hex')<>v_qr_token_hash then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
      using errcode='40001',detail=jsonb_build_object(
        'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
        'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
        'reason','CURRENT_QR_TOKEN_HASH_MISMATCH',
        'qr_source_timesheet_id',v_qr_current.timesheet_id
      )::text;
  else
    update public.timesheets timesheet_row
    set qr_token=null,
        qr_payload_json='{}'::jsonb,
        qr_generated_at=null,
        qr_scanned_at=null,
        qr_scan_info_json=null,
        qr_r2_key=null,
        qr_last_sent_hash=null,
        qr_last_sent_at_utc=null,
        qr_signed_hash=null,
        qr_signed_at_utc=null,
        updated_at=p_now_utc
    where timesheet_row.timesheet_id=v_qr_current.timesheet_id
      and timesheet_row.is_current=true
      and nullif(btrim(coalesce(timesheet_row.qr_token,'')),'') is not null
      and encode(extensions.digest(
        convert_to(timesheet_row.qr_token,'UTF8'),'sha256'
      ),'hex')=v_qr_token_hash;
    v_qr_invalidated:=found;
    if not v_qr_invalidated then
      raise exception 'CANDIDATE_PAPER_QR_SOURCE_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_SOURCE_CONFLICT',
          'workflow_id',v_workflow.id,'delivery_generation',v_delivery_generation,
          'reason','QR_TOKEN_INVALIDATION_LOST_RACE',
          'qr_source_timesheet_id',v_qr_current.timesheet_id
        )::text;
    end if;
  end if;

  perform private._candidate_audit_v1(
    'candidate_submission_workflow',v_workflow.id::text,
    'CANDIDATE_PAPER_DELIVERY_RETIRED',
    jsonb_build_object('state',v_workflow.state,'generation',v_workflow.generation),
    jsonb_build_object(
      'delivery_generation',v_delivery_generation,
      'reason_code',v_reason,'mail_count',v_mail_count,
      'mail_retired_count',v_mail_retired_count,
      'notification_retired_count',v_notification_count,
      'qr_source_timesheet_id',v_qr_current.timesheet_id,
      'rejected_target_timesheet_id',v_rejected_target_timesheet_id,
      'qr_invalidated',v_qr_invalidated,
      'qr_already_invalidated',v_qr_already_invalidated
    ),v_reason,null,
    'candidate-paper-retire:'||v_workflow.id::text||':'||v_workflow.generation::text||':'
      ||v_delivery_generation::text||':'||v_reason,
    p_now_utc
  );

  return jsonb_build_object(
    'retired',true,'workflow_id',v_workflow.id,'generation',v_workflow.generation,
    'delivery_generation',v_delivery_generation,
    'reason_code',v_reason,'mail_count',v_mail_count,
    'mail_retired_count',v_mail_retired_count,
    'notification_retired_count',v_notification_count,
    'qr_source_timesheet_id',v_qr_current.timesheet_id,
    'rejected_target_timesheet_id',v_rejected_target_timesheet_id,
    'qr_invalidated',v_qr_invalidated,
    'qr_already_invalidated',v_qr_already_invalidated
  );
end;
$function$;

-- private._candidate_paper_pack_readiness_v1(uuid,integer)
CREATE OR REPLACE FUNCTION private._candidate_paper_pack_readiness_v1(p_workflow_id uuid, p_expected_generation integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_count integer:=0;
  v_manifest text;
  v_scope jsonb;
  v_attachment jsonb;
  v_state text:='PREPARING';
  v_reason text:='CANDIDATE_PAPER_PACK_PREPARING';
  v_ready boolean:=false;
  v_retryable boolean:=false;
  v_failure_scope text;
  v_failure_code text;
  v_attempt_count integer:=0;
  v_next_retry_at timestamptz;
  v_retry_in_progress boolean:=false;
  v_operation_id text;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_expected_generation;
  if not found or v_workflow.route<>'PAPER' then
    return jsonb_build_object('state','NOT_APPLICABLE','download_available',false,
      'upload_available',false,'page_count',null,'reason_code','CANDIDATE_PAPER_PACK_NOT_APPLICABLE');
  end if;
  if v_workflow.state<>'AWAITING_PAPER_RETURN' then
    return jsonb_build_object('state',case when v_workflow.state='RECEIVED' then 'RETURN_RECEIVED' else 'NOT_APPLICABLE' end,
      'download_available',false,'upload_available',false,'page_count',null,
      'reason_code',case when v_workflow.state='RECEIVED' then 'CANDIDATE_PAPER_RETURN_RECEIVED'
        else 'CANDIDATE_PAPER_PACK_NOT_APPLICABLE' end);
  end if;
  if coalesce(v_workflow.last_mutation_response_json->>'failure_scope','')='WORKFLOW'
     and coalesce(v_workflow.last_mutation_response_json->>'paper_pack_state','')='FAILED_TERMINAL' then
    return jsonb_build_object(
      'state','FAILED_TERMINAL','download_available',false,'upload_available',false,
      'page_count',null,'reason_code',coalesce(
        v_workflow.last_mutation_response_json->>'failure_code',
        'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
      ),
      'failure_scope','WORKFLOW','failure_code',coalesce(
        v_workflow.last_mutation_response_json->>'failure_code',
        'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
      ),
      'retryable',false,'attempt_count',0,'next_retry_at_utc',null,
      'retry_in_progress',false,
      'operation_id',v_workflow.last_mutation_response_json->>'paper_pack_operation_id'
    );
  end if;
  v_manifest:=case when v_workflow.paper_return_manifest_sha256 is null then null
    else encode(v_workflow.paper_return_manifest_sha256,'hex') end;
  if v_manifest is null then
    return jsonb_build_object('state',v_state,'download_available',false,'upload_available',false,
      'page_count',null,'reason_code',v_reason);
  end if;
  select count(*)::integer into v_count
  from public.mail_outbox mail
  where mail.type='TIMESHEET_QR' and mail.context_kind='timesheets'
    and mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(mail.payment_scope_json->>'paper_return_manifest_sha256',''))=v_manifest;
  if v_count>1 then
    return jsonb_build_object('state','STALE','download_available',false,'upload_available',false,
      'page_count',null,'reason_code','CANDIDATE_PAPER_OUTBOX_CONFLICT');
  elsif v_count=0 then
    return jsonb_build_object('state',v_state,'download_available',false,'upload_available',false,
      'page_count',null,'reason_code',v_reason);
  end if;
  select mail.* into v_mail from public.mail_outbox mail
  where mail.type='TIMESHEET_QR' and mail.context_kind='timesheets'
    and mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(mail.payment_scope_json->>'paper_return_manifest_sha256',''))=v_manifest;
  v_scope:=coalesce(v_mail.payment_scope_json,'{}'::jsonb);
  v_failure_scope:='OUTBOX';
  v_failure_code:=nullif(v_scope->>'candidate_paper_pack_failure_code','');
  v_retryable:=lower(coalesce(v_scope->>'candidate_paper_pack_retryable','false'))
    in ('true','t','1','yes');
  v_attempt_count:=case when coalesce(v_scope->>'candidate_paper_pack_attempt_count','')~'^[0-9]+$'
    then (v_scope->>'candidate_paper_pack_attempt_count')::integer else 0 end;
  v_next_retry_at:=nullif(v_scope->>'candidate_paper_pack_next_retry_at_utc','')::timestamptz;
  v_retry_in_progress:=coalesce(
    nullif(v_scope->>'candidate_paper_pack_attempt_token','') is not null
      and nullif(v_scope->>'candidate_paper_pack_attempt_expires_at_utc','')::timestamptz>now(),
    false
  );
  v_operation_id:=nullif(v_scope->>'candidate_paper_pack_operation_id','');
  if lower(coalesce(v_scope->>'candidate_paper_generation_retired','false')) in ('true','t','1','yes') then
    v_state:='RETIRED'; v_reason:='CANDIDATE_PAPER_GENERATION_RETIRED';
  elsif v_retryable and v_failure_code='CANDIDATE_PAPER_DOCUMENT_PENDING_TIMEOUT' then
    -- Legacy observations of a slow source document are not pack failures.  The
    -- document pipeline still owns the work, so neither Candidate nor Office may
    -- expose backoff/retry authority while the source remains pending.
    v_state:='PREPARING'; v_reason:='CANDIDATE_PAPER_DOCUMENT_PENDING';
    v_retryable:=false;
    v_next_retry_at:=null;
    v_retry_in_progress:=false;
  elsif v_retryable then
    if v_next_retry_at is not null and v_next_retry_at>now() then
      v_state:='BACKOFF'; v_reason:='CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE';
    else
      v_state:='FAILED_RETRYABLE';
      v_reason:=coalesce(v_failure_code,'CANDIDATE_PAPER_PACK_FAILED_RETRYABLE');
    end if;
  elsif upper(coalesce(v_scope->>'candidate_paper_pack_failure_class',''))='TERMINAL' then
    v_state:='FAILED_TERMINAL';
    v_reason:=coalesce(v_failure_code,'CANDIDATE_PAPER_PACK_FAILED_TERMINAL');
  elsif v_mail.status='FAILED' then
    v_state:='FAILED_TERMINAL'; v_reason:='CANDIDATE_PAPER_OUTBOX_FAILED';
  elsif jsonb_typeof(v_mail.attachments)='array' and jsonb_array_length(v_mail.attachments)=1 then
    v_attachment:=v_mail.attachments->0;
    v_ready:=v_mail.status in ('QUEUED','SENT')
      and lower(coalesce(v_scope->>'candidate_paper_pack_ready','false')) in ('true','t','1','yes')
      and lower(coalesce(v_scope->>'mail_held_until_pdf_rendered','false')) in ('false','f','0','no')
      and nullif(btrim(coalesce(v_scope->>'mail_hold_reason','')),'') is null
      and v_attachment->>'r2_key'=v_scope->>'candidate_complete_pack_storage_key'
      and lower(coalesce(v_attachment->>'sha256',''))=lower(coalesce(v_scope->>'candidate_complete_pack_sha256',''))
      and v_attachment->>'size_bytes'=v_scope->>'candidate_complete_pack_size_bytes'
      and v_attachment->>'page_count'=v_scope->>'candidate_complete_pack_page_count'
      and lower(coalesce(v_attachment->>'content_type',''))='application/pdf'
      and v_attachment->>'candidate_workflow_id'=v_workflow.id::text
      and v_attachment->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(v_attachment->>'paper_return_manifest_sha256',''))=v_manifest;
    if v_ready then v_state:='READY'; v_reason:=null; end if;
  end if;
  return jsonb_build_object('state',v_state,'download_available',v_ready,'upload_available',v_ready,
    'page_count',case when v_ready then nullif(v_scope->>'candidate_complete_pack_page_count','')::integer else null end,
    'reason_code',v_reason,'failure_scope',v_failure_scope,'failure_code',v_failure_code,
    'retryable',v_retryable,'attempt_count',v_attempt_count,
    'next_retry_at_utc',v_next_retry_at,'retry_in_progress',v_retry_in_progress,
    'operation_id',v_operation_id);
end;
$function$;

-- private._candidate_paper_source_workflow_context_v1(uuid)
CREATE OR REPLACE FUNCTION private._candidate_paper_source_workflow_context_v1(p_source_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_requested public.timesheets%rowtype;
  v_current_source public.timesheets%rowtype;
  v_source_key text;
  v_current_token_hash text;
  v_source_workflow_count integer:=0;
  v_nonterminal_count integer:=0;
  v_affected_nonterminal_count integer:=0;
  v_token_owner_count integer:=0;
  v_source_workflows jsonb:='[]'::jsonb;
  v_affected_nonterminal_workflows jsonb:='[]'::jsonb;
  v_affected_nonterminal_workflow_ids uuid[]:='{}'::uuid[];
  v_token_owner_workflow_id uuid;
  v_token_owner_generation integer;
  v_token_owner_state text;
  v_sole_nonterminal_workflow_id uuid;
  v_sole_nonterminal_generation integer;
  v_sole_nonterminal_state text;
  v_latest_finalised_workflow_id uuid;
  v_latest_finalised_generation integer;
  v_latest_finalised_state text;
  v_selected_workflow_id uuid;
  v_selected_generation integer;
  v_selected_state text;
  v_identity_conflict boolean:=false;
  v_conflict_reason text;
begin
  if p_source_timesheet_id is null then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_REQUIRED' using errcode='22023';
  end if;
  select source_row.* into v_requested
  from public.timesheets source_row
  where source_row.timesheet_id=p_source_timesheet_id;
  if not found then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_NOT_FOUND' using errcode='P0002';
  end if;

  if nullif(btrim(coalesce(v_requested.booking_id,'')),'') is not null then
    select source_row.* into v_current_source
    from public.timesheets source_row
    where source_row.booking_id=v_requested.booking_id
      and source_row.is_current=true
      and source_row.archived_at_utc is null
      and upper(coalesce(source_row.line_type::text,'')) not in ('EXPENSES','MILEAGE')
    order by source_row.version desc,source_row.updated_at desc,source_row.timesheet_id
    limit 1;
    v_source_key:='BOOKING:'||v_requested.booking_id;
  else
    select source_row.* into v_current_source
    from public.timesheets source_row
    where source_row.timesheet_id=v_requested.timesheet_id
      and source_row.is_current=true
      and source_row.archived_at_utc is null
      and upper(coalesce(source_row.line_type::text,'')) not in ('EXPENSES','MILEAGE');
    v_source_key:='TIMESHEET:'||v_requested.timesheet_id::text;
  end if;
  if not found then
    raise exception 'CANDIDATE_PAPER_QR_SOURCE_CURRENT_VERSION_MISSING'
      using errcode='40001';
  end if;

  if nullif(btrim(coalesce(v_current_source.qr_token,'')),'') is not null then
    v_current_token_hash:=encode(extensions.digest(
      convert_to(v_current_source.qr_token,'UTF8'),'sha256'
    ),'hex');
  end if;

  with source_workflows as (
    select workflow.id,workflow.generation,workflow.state,
      workflow.workflow_kind,workflow.updated_at_utc,workflow.created_at_utc,
      exists(
        select 1
        from public.mail_outbox owner_mail
        join public.timesheets owner_source
          on owner_source.timesheet_id=owner_mail.context_id
        where v_current_token_hash is not null
          and owner_mail.type='TIMESHEET_QR'
          and owner_mail.context_kind='timesheets'
          and owner_mail.payment_scope_json->>'candidate_workflow_id'=workflow.id::text
          and owner_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when workflow.state='FINALISED'
              then greatest(workflow.generation-1,1)
              else workflow.generation end)::text
          and lower(coalesce(owner_mail.payment_scope_json->>'qr_token_hash',''))=
            v_current_token_hash
          and (case
            when nullif(btrim(coalesce(owner_source.booking_id,'')),'') is not null
              then 'BOOKING:'||owner_source.booking_id
            else 'TIMESHEET:'||owner_source.timesheet_id::text end)=v_source_key
      ) as owns_current_token
    from public.candidate_submission_workflows workflow
    where workflow.route='PAPER'
      and workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
      and workflow.contract_id is not distinct from v_current_source.contract_id
      and workflow.week_ending_date is not distinct from v_current_source.week_ending_date
      and exists(
        select 1
        from public.mail_outbox source_mail
        join public.timesheets mail_source on mail_source.timesheet_id=source_mail.context_id
        where source_mail.type='TIMESHEET_QR'
          and source_mail.context_kind='timesheets'
          and source_mail.payment_scope_json->>'candidate_workflow_id'=workflow.id::text
          and source_mail.payment_scope_json->>'candidate_workflow_generation'=
            (case when workflow.state='FINALISED'
              then greatest(workflow.generation-1,1)
              else workflow.generation end)::text
          and (case
            when nullif(btrim(coalesce(mail_source.booking_id,'')),'') is not null
              then 'BOOKING:'||mail_source.booking_id
            else 'TIMESHEET:'||mail_source.timesheet_id::text end)=v_source_key
      )
  )
  select count(*)::integer,
    count(*) filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED'))::integer,
    count(*) filter(where owns_current_token)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'workflow_id',id,'generation',generation,'state',state,
      'workflow_kind',workflow_kind,'owns_current_qr_token',owns_current_token
    ) order by id),'[]'::jsonb),
    (array_agg(id order by id) filter(where owns_current_token))[1],
    (array_agg(generation order by id) filter(where owns_current_token))[1],
    (array_agg(state order by id) filter(where owns_current_token))[1],
    (array_agg(id order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED')))[1],
    (array_agg(generation order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED')))[1],
    (array_agg(state order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state in ('AWAITING_PAPER_RETURN','RECEIVED')))[1],
    (array_agg(id order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state='FINALISED'))[1],
    (array_agg(generation order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state='FINALISED'))[1],
    (array_agg(state order by updated_at_utc desc,created_at_utc desc,id)
      filter(where state='FINALISED'))[1]
  into v_source_workflow_count,v_nonterminal_count,v_token_owner_count,
    v_source_workflows,
    v_token_owner_workflow_id,v_token_owner_generation,v_token_owner_state,
    v_sole_nonterminal_workflow_id,v_sole_nonterminal_generation,
    v_sole_nonterminal_state,
    v_latest_finalised_workflow_id,v_latest_finalised_generation,
    v_latest_finalised_state
  from source_workflows;

  -- Delivery ownership and workflow lifecycle are deliberately separate
  -- catalogues.  WORKER_DRAFT, approval states and amendable REFUSED do not
  -- necessarily have a current immutable mail receipt, but rotating their
  -- worked-row source would still make their recovery anchor historical.
  -- Resolve those workflows through the stable booking/version family rather
  -- than requiring an exact current timesheet id or a mail_outbox row.
  with affected_nonterminal_workflows as (
    select workflow.id,workflow.generation,workflow.state,
      workflow.workflow_kind,workflow.updated_at_utc,workflow.created_at_utc
    from public.candidate_submission_workflows workflow
    where workflow.route='PAPER'
      and workflow.state not in (
        'FINALISED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED'
      )
      and workflow.contract_id is not distinct from v_current_source.contract_id
      and workflow.week_ending_date is not distinct from v_current_source.week_ending_date
      and exists(
        select 1
        from public.timesheets binding_source
        where binding_source.timesheet_id in (
            workflow.target_timesheet_id,workflow.anchor_timesheet_id
          )
          and (case
            when nullif(btrim(coalesce(binding_source.booking_id,'')),'') is not null
              then 'BOOKING:'||binding_source.booking_id
            else 'TIMESHEET:'||binding_source.timesheet_id::text end)=v_source_key
      )
  )
  select count(*)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'workflow_id',id,'generation',generation,'state',state,
      'workflow_kind',workflow_kind
    ) order by id),'[]'::jsonb),
    coalesce(array_agg(id order by id),'{}'::uuid[])
  into v_affected_nonterminal_count,v_affected_nonterminal_workflows,
    v_affected_nonterminal_workflow_ids
  from affected_nonterminal_workflows;

  if v_source_workflow_count=0 then
    -- Ordinary pre-Candidate QR remains an exact legacy route family. With no
    -- Candidate delivery receipts there is no Candidate workflow to select or
    -- supersede, and the public compatibility authority remains unchanged.
    null;
  elsif v_current_token_hash is not null then
    if v_token_owner_count<>1 then
      v_identity_conflict:=true;
      v_conflict_reason:='CURRENT_QR_TOKEN_OWNER_CONFLICT';
    elsif v_nonterminal_count>1 then
      v_identity_conflict:=true;
      v_conflict_reason:='MULTIPLE_NONTERMINAL_PAPER_WORKFLOWS';
    elsif v_token_owner_state='FINALISED' and v_nonterminal_count>0 then
      v_identity_conflict:=true;
      v_conflict_reason:='CURRENT_QR_TOKEN_OWNER_TERMINAL_WITH_LIVE_WORKFLOW';
    else
      v_selected_workflow_id:=v_token_owner_workflow_id;
      v_selected_generation:=v_token_owner_generation;
      v_selected_state:=v_token_owner_state;
    end if;
  elsif v_nonterminal_count>1 then
    v_identity_conflict:=true;
    v_conflict_reason:='MULTIPLE_NONTERMINAL_PAPER_WORKFLOWS';
  elsif v_nonterminal_count=1 then
    v_selected_workflow_id:=v_sole_nonterminal_workflow_id;
    v_selected_generation:=v_sole_nonterminal_generation;
    v_selected_state:=v_sole_nonterminal_state;
  else
    v_selected_workflow_id:=v_latest_finalised_workflow_id;
    v_selected_generation:=v_latest_finalised_generation;
    v_selected_state:=v_latest_finalised_state;
  end if;

  -- A source-rotating route caller must make an explicit lifecycle decision
  -- where its selected delivery owner differs from an affected recoverable
  -- workflow.  The context reports that conflict; a confirmed office route
  -- intervention may resolve one standalone incomplete expense claim, while
  -- claim-level cancellation and ambiguous/multiple cases still fail closed.
  if not v_identity_conflict and v_affected_nonterminal_count>0 and (
       v_selected_workflow_id is null
       or v_affected_nonterminal_count>1
       or not (v_selected_workflow_id=any(v_affected_nonterminal_workflow_ids))
     ) then
    v_identity_conflict:=true;
    v_conflict_reason:='CANDIDATE_PAPER_SHARED_SOURCE_WORKFLOW_CONFLICT';
  end if;

  return jsonb_build_object(
    'contract_version','CANDIDATE_PAPER_SOURCE_WORKFLOW_CONTEXT_V1',
    'qr_source_timesheet_id',v_current_source.timesheet_id,
    'source_key',v_source_key,
    'current_qr_token_present',v_current_token_hash is not null,
    'source_workflow_count',v_source_workflow_count,
    'nonterminal_workflow_count',v_nonterminal_count,
    'affected_nonterminal_workflow_count',v_affected_nonterminal_count,
    'current_token_owner_count',v_token_owner_count,
    'current_token_owner_workflow_id',v_token_owner_workflow_id,
    'current_token_owner_generation',v_token_owner_generation,
    'current_token_owner_state',v_token_owner_state,
    'selected_workflow_id',v_selected_workflow_id,
    'selected_workflow_generation',v_selected_generation,
    'selected_workflow_state',v_selected_state,
    'identity_conflict',v_identity_conflict,
    'conflict_reason',v_conflict_reason,
    'source_workflows',v_source_workflows,
    'affected_nonterminal_workflows',v_affected_nonterminal_workflows
  );
end;
$function$;

-- private._candidate_password_authority_sha256_v1(uuid,text,smallint,bytea,bytea,jsonb)
CREATE OR REPLACE FUNCTION private._candidate_password_authority_sha256_v1(p_account_id uuid, p_password_scheme text, p_password_scheme_version smallint, p_password_salt bytea, p_password_digest bytea, p_password_params jsonb)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
  select encode(extensions.digest(convert_to(concat_ws(E'\n',
    'CANDIDATE_PASSWORD_AUTHORITY_V1',
    coalesce(p_account_id::text,''),
    upper(btrim(coalesce(p_password_scheme,''))),
    coalesce(p_password_scheme_version,0)::text,
    encode(coalesce(p_password_salt,''::bytea),'hex'),
    encode(coalesce(p_password_digest,''::bytea),'hex'),
    upper(btrim(coalesce(p_password_params->>'hash','SHA-256'))),
    coalesce(nullif(p_password_params->>'iterations','')::integer,100000)::text,
    coalesce(nullif(p_password_params->>'length_bytes','')::integer,32)::text
  ),'UTF8'),'sha256'),'hex')
$function$;

-- private._candidate_policy_resolve_v1(uuid,uuid,date)
CREATE OR REPLACE FUNCTION private._candidate_policy_resolve_v1(p_client_id uuid, p_contract_id uuid DEFAULT NULL::uuid, p_evaluation_date date DEFAULT NULL::date)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_evaluation_date date := coalesce(p_evaluation_date,(current_timestamp at time zone 'Europe/London')::date);
  v_global public.settings_defaults%rowtype;
  v_client public.client_settings%rowtype;
  v_contract public.contracts%rowtype;
  v_client_found boolean := false;
  v_contract_found boolean := false;
  v_auto boolean;
  v_auto_source text;
  v_separate boolean;
  v_separate_source text;
  v_paper boolean;
  v_paper_source text;
  v_expense_email text;
  v_import_mandatory boolean := false;
  v_expense_email_ready boolean := false;
  v_manager_policy jsonb;
  v_result jsonb;
  v_import_authority jsonb;
begin
  if p_client_id is null then
    raise exception 'CANDIDATE_POLICY_CLIENT_REQUIRED' using errcode='22023';
  end if;

  select * into v_global from public.settings_defaults where id=1;
  if not found then raise exception 'CANDIDATE_GLOBAL_SETTINGS_MISSING' using errcode='55000'; end if;

  select * into v_client from public.client_settings cs
  where cs.client_id=p_client_id
    and (cs.effective_from is null or cs.effective_from<=v_evaluation_date)
  order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc
  limit 1;
  v_client_found:=found;

  if p_contract_id is not null then
    select * into v_contract from public.contracts c where c.id=p_contract_id;
    v_contract_found:=found;
    if not v_contract_found then raise exception 'CANDIDATE_POLICY_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    if v_contract.client_id is distinct from p_client_id then
      raise exception 'CANDIDATE_POLICY_CONTRACT_CLIENT_MISMATCH' using errcode='22023';
    end if;
  end if;

  v_import_authority:=private._candidate_import_authoritative_v1(
    p_client_id,p_contract_id,null,null,v_evaluation_date
  );
  v_import_mandatory:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);

  if v_contract_found and v_contract.candidate_electronic_auto_authorise_override is not null then
    v_auto:=v_contract.candidate_electronic_auto_authorise_override; v_auto_source:='CONTRACT';
  elsif v_client_found and v_client.candidate_electronic_auto_authorise is not null then
    v_auto:=v_client.candidate_electronic_auto_authorise; v_auto_source:='CLIENT';
  else
    v_auto:=v_global.candidate_electronic_auto_authorise_default; v_auto_source:='GLOBAL';
  end if;

  if v_import_mandatory then
    v_separate:=true; v_separate_source:='IMPORT_MANDATORY';
  elsif v_contract_found and v_contract.candidate_expenses_require_separate_timesheet_override is not null then
    v_separate:=v_contract.candidate_expenses_require_separate_timesheet_override; v_separate_source:='CONTRACT';
  elsif v_client_found then
    v_separate:=v_client.candidate_expenses_require_separate_timesheet; v_separate_source:='CLIENT';
  else
    v_separate:=false; v_separate_source:='SAFE_DEFAULT';
  end if;

  if v_contract_found and v_contract.candidate_paper_submission_enabled_override is not null then
    v_paper:=v_contract.candidate_paper_submission_enabled_override; v_paper_source:='CONTRACT';
  elsif v_client_found then
    v_paper:=v_client.candidate_paper_submission_enabled; v_paper_source:='CLIENT';
  else
    v_paper:=false; v_paper_source:='SAFE_DEFAULT';
  end if;

  v_expense_email:=nullif(btrim(case
    when v_contract_found and nullif(btrim(v_contract.candidate_expense_invoice_email_override),'') is not null
      then v_contract.candidate_expense_invoice_email_override
    when v_client_found then v_client.candidate_expense_invoice_email
    else null end),'');
  v_expense_email_ready:=v_expense_email is not null
    and char_length(v_expense_email)<=320
    and v_expense_email ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';

  v_manager_policy:=private._candidate_normalize_manager_policy_v1(coalesce(case
    when v_contract_found and upper(coalesce(v_contract.candidate_manager_approval_policy_json->>'mode','INHERIT'))<>'INHERIT'
      then v_contract.candidate_manager_approval_policy_json
    when v_client_found then v_client.candidate_manager_approval_policy_json
    else '{}'::jsonb end,'{}'::jsonb));

  v_result:=jsonb_build_object(
    'client_id',p_client_id,
    'contract_id',p_contract_id,
    'evaluation_date',v_evaluation_date,
    'candidate_electronic_auto_authorise',v_auto,
    'candidate_electronic_auto_authorise_source',v_auto_source,
    'expenses_require_separate_timesheet',v_separate,
    'expenses_require_separate_timesheet_source',v_separate_source,
    'import_expense_separation_mandatory',v_import_mandatory,
    'import_source_family',v_import_authority->>'source_family',
    'paper_submission_enabled',v_paper,
    'paper_submission_enabled_source',v_paper_source,
    'expense_invoice_email',v_expense_email,
    'expense_invoice_email_ready',v_expense_email_ready,
    'manager_approval_policy',v_manager_policy,
    'allow_daily_manager_authorise_on_phone',coalesce(v_client.allow_daily_manager_authorise_on_phone,true),
    'allow_daily_manager_authorise_by_email',coalesce(v_client.allow_daily_manager_authorise_by_email,false),
    'hours_deviation_pct',v_global.candidate_hours_deviation_pct,
    'barred_manager_email_domains',private._candidate_normalize_domain_array_v1(v_global.candidate_barred_manager_email_domains),
    'client_setting_found',v_client_found,
    'client_settings_id',case when v_client_found then v_client.id else null end,
    'contract_found',v_contract_found,
    'global_settings_updated_at',v_global.updated_at
  );

  return v_result||jsonb_build_object(
    'policy_fingerprint',encode(extensions.digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
end;
$function$;

-- private._candidate_qr_pack_ready_notification_v1()
CREATE OR REPLACE FUNCTION private._candidate_qr_pack_ready_notification_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_notification jsonb;
begin
  if (not private._candidate_feature_enabled_current_v1('candidate_route_confirmation')
       and not private._candidate_office_service_context_valid_v1(null,null,'ROUTE_CONFIRM'))
     or new.candidate_submission_route_intent is distinct from 'PAPER'
     or new.sheet_scope is distinct from 'WEEKLY'::public.timesheet_scope_enum
     or upper(coalesce(new.document_state::text,''))<>'READY'
     or upper(coalesce(new.qr_status::text,'')) not in ('PENDING','SENT','READY') then
    return new;
  end if;
  if new.current_document_version_id is null or not exists(
    select 1 from public.invoice_document_versions document_version
    where document_version.id=new.current_document_version_id
      and document_version.entity_type='TIMESHEET'
      and document_version.entity_id=new.timesheet_id
      and upper(document_version.status)='READY'
  ) then
    return new;
  end if;
  if exists(
    select 1
    from public.candidate_submission_workflows workflow
    where workflow.route='PAPER'
      and workflow.state='AWAITING_PAPER_RETURN'
      and (workflow.target_timesheet_id=new.timesheet_id
        or workflow.anchor_timesheet_id=new.timesheet_id)
  ) then
    return new;
  end if;
  v_notification:=private._timesheet_route_resubmission_notifications_v1(
    new.timesheet_id,new.timesheet_id,new.version,'ALLOW_QR_AGAIN',now()
  );
  new.candidate_submission_route_intent:=null;
  return new;
end;
$function$;

-- private._candidate_queue_mail_v1(jsonb,text,text,text,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION private._candidate_queue_mail_v1(p_mail jsonb, p_to text, p_deterministic_key text, p_reference text, p_context_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS uuid
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare v_id uuid;
declare v_scope jsonb:=coalesce(p_mail->'payment_scope_json','{}'::jsonb);
declare v_request public.candidate_approval_requests%rowtype;
begin
  if jsonb_typeof(p_mail)<>'object'
     or nullif(btrim(coalesce(p_mail->>'subject','')),'') is null
     or (nullif(btrim(coalesce(p_mail->>'body_text','')),'') is null
       and nullif(btrim(coalesce(p_mail->>'body_html','')),'') is null) then
    raise exception 'CANDIDATE_MAIL_PAYLOAD_REQUIRED' using errcode='22023';
  end if;
  if jsonb_typeof(v_scope)<>'object' then
    raise exception 'CANDIDATE_MAIL_SCOPE_INVALID' using errcode='22023';
  end if;
  if upper(coalesce(v_scope->>'candidate_mail_authority',''))='MANAGER_APPROVAL_V1' then
    if upper(coalesce(v_scope->>'candidate_manager_mail_kind','')) not in (
         'INITIAL','REMINDER','RENEWAL','WITHDRAWAL'
       )
       or coalesce(v_scope->>'candidate_manager_workflow_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_scope->>'candidate_manager_workflow_generation','') !~ '^[1-9][0-9]{0,8}$'
       or coalesce(v_scope->>'candidate_approval_request_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_scope->>'candidate_approval_request_generation','') !~ '^[1-9][0-9]{0,8}$'
       or lower(coalesce(v_scope->>'candidate_manager_mail_retired','false'))
          not in ('false','f','0','no') then
      raise exception 'CANDIDATE_MANAGER_MAIL_SCOPE_INVALID' using errcode='22023';
    end if;
    select request_row.* into v_request
    from public.candidate_approval_requests request_row
    where request_row.id=(v_scope->>'candidate_approval_request_id')::uuid
      and request_row.workflow_id=(v_scope->>'candidate_manager_workflow_id')::uuid
      and request_row.workflow_id=p_context_id
      and request_row.workflow_generation=(v_scope->>'candidate_manager_workflow_generation')::integer
      and request_row.request_generation=(v_scope->>'candidate_approval_request_generation')::integer
      and request_row.method='EMAIL'
      and request_row.manager_email_normalized=p_to;
    if not found then
      raise exception 'CANDIDATE_MANAGER_MAIL_SCOPE_INVALID' using errcode='40001';
    end if;
    if upper(v_scope->>'candidate_manager_mail_kind')='WITHDRAWAL' then
      if v_request.state not in ('CANCELLED','SUPERSEDED','EXPIRED','REFUSED') then
        raise exception 'CANDIDATE_MANAGER_WITHDRAWAL_NOT_READY' using errcode='40001';
      end if;
    elsif v_request.state<>'PENDING' then
      raise exception 'CANDIDATE_MANAGER_MAIL_REQUEST_NOT_CURRENT' using errcode='40001';
    end if;
  end if;
  insert into public.mail_outbox(
    type,"to",subject,body_html,body_text,attachments,status,created_at_utc,
    reference,recipient_kind,context_kind,context_id,email_type,scheduled_for_utc,
    next_attempt_at_utc,deterministic_outbox_key,payment_scope_json
  ) values (
    'TIMESHEET_GENERAL',p_to,btrim(p_mail->>'subject'),nullif(p_mail->>'body_html',''),
    nullif(p_mail->>'body_text',''),coalesce(p_mail->'attachments','[]'::jsonb),'QUEUED',p_now_utc,
    p_reference,'CANDIDATE_MANAGER','CANDIDATE_WORKFLOW',p_context_id,
    coalesce(nullif(btrim(p_mail->>'email_type'),''),'CANDIDATE_APP_TRANSACTIONAL'),
    coalesce(nullif(p_mail->>'scheduled_for_utc','')::timestamptz,p_now_utc),p_now_utc,
    p_deterministic_key,v_scope
  ) on conflict (deterministic_outbox_key) do update
    set deterministic_outbox_key=excluded.deterministic_outbox_key
  returning id into v_id;
  return v_id;
end;
$function$;

-- private._candidate_record_capabilities_v1(uuid,uuid,jsonb)
CREATE OR REPLACE FUNCTION private._candidate_record_capabilities_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid, p_proposed_claim jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_policy jsonb;
  v_hours numeric:=0;
  v_additional numeric:=0;
  v_expenses numeric:=0;
  v_mileage numeric:=0;
  v_travel numeric:=0;
  v_accommodation numeric:=0;
  v_other numeric:=0;
  v_import boolean:=false;
  v_protected boolean:=false;
  v_candidate_mutation_locked boolean:=false;
  v_separate boolean:=false;
  v_has_timesheet boolean:=false;
  v_role text;
  v_route jsonb;
  v_route_family text;
  v_hours_route_allowed boolean:=false;
  v_expense_route_allowed boolean:=false;
  v_paper_route_allowed boolean:=false;
  v_no_work_route_allowed boolean:=false;
  v_reasons jsonb:='[]'::jsonb;
  v_result jsonb;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  end if;

  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select cw.* into v_week from public.contract_weeks cw
    where cw.timesheet_id=p_timesheet_id order by cw.updated_at desc,cw.id desc limit 1;
  end if;
  if v_week.id is null then
    -- DAILY is timesheet-owned and intentionally has no contract_weeks row.
    if v_timesheet.timesheet_id is null
       or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_timesheet.contract_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  else
    select * into v_contract from public.contracts where id=v_week.contract_id;
  end if;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;

  if v_timesheet.timesheet_id is null and v_week.timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_week.timesheet_id;
  end if;

  if v_timesheet.timesheet_id is not null then
    select * into v_fin from public.timesheets_financials
    where timesheet_id=v_timesheet.timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;

  v_policy:=private._candidate_policy_resolve_v1(
    v_contract.client_id,v_contract.id,coalesce(v_week.week_ending_date,v_timesheet.week_ending_date)
  );
  v_route:=private._candidate_route_family_v1(v_timesheet.timesheet_id,v_week.id);
  v_route_family:=v_route->>'route_family';
  v_hours_route_allowed:=coalesce((v_route->>'candidate_hours_submission_allowed')::boolean,false);
  v_expense_route_allowed:=coalesce((v_route->>'candidate_expenses_allowed')::boolean,false);
  v_paper_route_allowed:=coalesce((v_route->>'candidate_paper_submission_allowed')::boolean,false);
  v_no_work_route_allowed:=coalesce((v_route->>'candidate_no_work_allowed')::boolean,false);
  v_separate:=coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false);
  v_hours:=coalesce(v_fin.total_hours,0);
  v_additional:=private._candidate_json_numeric_sum(coalesce(v_fin.additional_units_json,'{}'::jsonb));
  if v_additional=0 then
    v_additional:=private._candidate_json_numeric_sum(coalesce(v_timesheet.additional_units_week,'{}'::jsonb))
      +private._candidate_json_numeric_sum(coalesce(v_timesheet.additional_units_per_day,'{}'::jsonb));
  end if;
  v_mileage:=abs(coalesce(v_fin.mileage_units,0))+abs(coalesce(v_fin.mileage_pay_ex_vat,0))+abs(coalesce(v_fin.mileage_charge_ex_vat,0));
  v_travel:=abs(coalesce(v_fin.travel_pay_ex_vat,0))+abs(coalesce(v_fin.travel_charge_ex_vat,0));
  v_accommodation:=abs(coalesce(v_fin.accommodation_pay_ex_vat,0))+abs(coalesce(v_fin.accommodation_charge_ex_vat,0));
  v_other:=abs(coalesce(v_fin.expenses_pay_ex_vat,0))+abs(coalesce(v_fin.expenses_charge_ex_vat,0))
    +abs(coalesce(v_fin.other_pay_ex_vat,0))+abs(coalesce(v_fin.other_charge_ex_vat,0));
  v_expenses:=v_mileage+v_travel+v_accommodation+v_other;

  if jsonb_typeof(p_proposed_claim)='object' then
    v_expenses:=greatest(v_expenses,
      abs(coalesce(nullif(p_proposed_claim->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_charge_ex_vat','')::numeric,0))
    );
    v_mileage:=greatest(v_mileage,
      abs(coalesce(nullif(p_proposed_claim->>'mileage_units','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_charge_ex_vat','')::numeric,0)));
    v_travel:=greatest(v_travel,
      abs(coalesce(nullif(p_proposed_claim->>'travel_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'travel_charge_ex_vat','')::numeric,0)));
    v_accommodation:=greatest(v_accommodation,
      abs(coalesce(nullif(p_proposed_claim->>'accommodation_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'accommodation_charge_ex_vat','')::numeric,0)));
    v_other:=greatest(v_other,
      abs(coalesce(nullif(p_proposed_claim->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'other_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'other_charge_ex_vat','')::numeric,0)));
    v_expenses:=greatest(v_expenses,v_mileage+v_travel+v_accommodation+v_other);
  end if;

  v_import:=coalesce((v_route->>'import_authoritative')::boolean,false);
  v_protected:=v_timesheet.archived_at_utc is not null
    or (v_timesheet.timesheet_id is not null and (not v_timesheet.is_current))
    or v_fin.paid_at_utc is not null
    or v_fin.locked_by_invoice_id is not null
    or coalesce(v_week.status in (
      'INVOICED'::public.contract_week_status_enum,'CANCELLED'::public.contract_week_status_enum
    ),false);
  v_candidate_mutation_locked:=v_fin.authorised_at_utc is not null;
  if v_candidate_mutation_locked then
    v_reasons:=v_reasons||'"CANDIDATE_MUTATION_LOCKED_AUTHORISED"'::jsonb;
  end if;

  select exists(
    select 1 from public.timesheet_evidence e
    where e.timesheet_id=v_timesheet.timesheet_id
      and upper(btrim(e.kind))='TIMESHEET'
      and e.processing_state<>'SUPERSEDED'
  ) into v_has_timesheet;

  if v_protected then v_role:='PROTECTED'; v_reasons:=v_reasons||'"LIFECYCLE_PROTECTED"'::jsonb;
  elsif v_import and v_expenses<>0 then v_role:='CONFLICT'; v_reasons:=v_reasons||'"IMPORT_SOURCE_HAS_EXPENSES"'::jsonb;
  elsif v_import then v_role:='IMPORT_HOURS'; v_reasons:=v_reasons||'"IMPORT_AUTHORITATIVE_HOURS"'::jsonb;
  elsif v_separate and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then v_role:='CONFLICT'; v_reasons:=v_reasons||'"SEPARATION_MIXED_ECONOMICS"'::jsonb;
  elsif v_expenses<>0 and v_hours=0 and v_additional=0 then v_role:='EXPENSE_ONLY';
  elsif (v_hours<>0 or v_additional<>0) and v_expenses=0 then v_role:='HOURS_ONLY';
  elsif not v_separate and v_timesheet.timesheet_id is not null then v_role:='COMBINED_ALLOWED';
  elsif v_week.additional_seq>0 and v_timesheet.timesheet_id is null then v_role:='FLEXIBLE';
  elsif v_week.additional_seq>0 and v_hours=0 and v_additional=0 and v_expenses=0 then v_role:='FLEXIBLE';
  else v_role:='HOURS_ONLY';
  end if;

  v_result:=jsonb_build_object(
    'record_role',v_role,
    'reason_codes',v_reasons,
    'timesheet_id',v_timesheet.timesheet_id,
    'contract_week_id',v_week.id,
    'contract_id',v_contract.id,
    'candidate_id',v_contract.candidate_id,
    'client_id',v_contract.client_id,
    'week_ending_date',v_week.week_ending_date,
    'additional_seq',v_week.additional_seq,
    'hours_value',v_hours,
    'additional_units_value',v_additional,
    'expense_value',v_expenses,
    'effective_separation',v_separate,
    'import_authoritative',v_import,
    'route_family',v_route_family,
    'effective_submission_mode',v_route->'effective_submission_mode',
    'protected',v_protected,
    'candidate_mutation_locked',v_candidate_mutation_locked,
    'has_active_timesheet_evidence',v_has_timesheet,
    'candidate_hours_submission_allowed',v_hours_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'candidate_expenses_allowed',v_expense_route_allowed and not v_protected,
    'candidate_paper_submission_allowed',v_paper_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'candidate_no_work_allowed',v_no_work_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'can_edit_hours',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and not v_import,
    'can_edit_expenses',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_timesheet',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED') and not v_protected and not v_candidate_mutation_locked and not v_has_timesheet,
    'can_attach_expense_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_mileage_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_mileage<>0,
    'can_attach_travel_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_travel<>0,
    'can_attach_accommodation_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_accommodation<>0,
    'can_attach_other_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_other<>0,
    'can_process',v_role not in ('PROTECTED','CONFLICT') and not v_protected and not v_candidate_mutation_locked,
    'can_reject_candidate_submission',v_timesheet.timesheet_id is not null and not v_protected and v_fin.authorised_at_utc is null,
    'reject_scope',case when v_role='EXPENSE_ONLY' then 'COMPLETE_EXPENSE_CLAIM' else 'COMPLETE_TIMESHEET_RECORD' end,
    'requires_carrier',v_role='IMPORT_HOURS' or (v_separate and v_role='HOURS_ONLY'),
    'expense_invoice_email_ready',coalesce((v_policy->>'expense_invoice_email_ready')::boolean,false),
    'policy',v_policy
  );

  return v_result||jsonb_build_object(
    'capability_hash',encode(extensions.digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
exception
  when invalid_text_representation then
    raise exception 'CANDIDATE_PROPOSED_CLAIM_INVALID' using errcode='22023';
end;
$function$;

-- private._candidate_rejection_replaced_v1(uuid)
CREATE OR REPLACE FUNCTION private._candidate_rejection_replaced_v1(p_rejected_workflow_id uuid)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_rejected public.candidate_submission_workflows%rowtype;
begin
  if p_rejected_workflow_id is null then
    return false;
  end if;

  select workflow.* into v_rejected
  from public.candidate_submission_workflows workflow
  where workflow.id=p_rejected_workflow_id;

  if not found or v_rejected.state<>'REJECTED' then
    return false;
  end if;

  -- Direct replacement lineage is durable historical truth.  A successor
  -- remains the replacement even if it is later cancelled, expires or is
  -- superseded; the original rejected workflow must never advertise a second
  -- impossible direct resubmission.
  if exists(
    select 1
    from public.candidate_submission_workflows direct_replacement
    where direct_replacement.replacement_of_workflow_id=v_rejected.id
  ) then
    return true;
  end if;

  return exists(
    select 1
    from public.candidate_submission_workflows later
    where later.candidate_id=v_rejected.candidate_id
      and later.contract_id is not distinct from v_rejected.contract_id
      and later.id<>v_rejected.id
      and later.state not in ('CANCELLED','EXPIRED','SUPERSEDED')
      and later.created_at_utc>=v_rejected.updated_at_utc
      and (
        (
          (
            v_rejected.workflow_kind='CONTRACT_EXPENSE'
            or v_rejected.rejection_scope='COMPLETE_EXPENSE_CLAIM'
          )
          and later.week_ending_date is not distinct from v_rejected.week_ending_date
          and later.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
        )
        or (
          v_rejected.workflow_kind='CONTRACT_COMBINED'
          and later.workflow_kind='CONTRACT_COMBINED'
          and later.contract_week_id is not distinct from v_rejected.contract_week_id
        )
        or (
          v_rejected.workflow_kind='CONTRACT_HOURS'
          and later.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
          and later.contract_week_id is not distinct from v_rejected.contract_week_id
        )
        or (
          v_rejected.workflow_kind='DAILY'
          and later.workflow_kind='DAILY'
          and later.work_date is not distinct from v_rejected.work_date
          and exists(
            select 1
            from public.timesheets rejected_timesheet
            join public.timesheets later_timesheet
              on nullif(btrim(coalesce(later_timesheet.booking_id,'')),'')
                =nullif(btrim(coalesce(rejected_timesheet.booking_id,'')),'')
            where rejected_timesheet.timesheet_id=coalesce(
                v_rejected.target_timesheet_id,v_rejected.anchor_timesheet_id
              )
              and later_timesheet.timesheet_id=coalesce(
                later.target_timesheet_id,later.anchor_timesheet_id
              )
              and nullif(btrim(coalesce(rejected_timesheet.booking_id,'')),'') is not null
          )
        )
      )
  );
end;
$function$;

-- private._candidate_render_contract_v1(uuid,integer,text)
CREATE OR REPLACE FUNCTION private._candidate_render_contract_v1(p_workflow_id uuid, p_workflow_generation integer, p_form_variant text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_variant text:=upper(btrim(coalesce(p_form_variant,'')));
  v_phase text;
  v_components jsonb;
begin
  if v_variant in ('ELECTRONIC_MANAGER_REVIEW','EXPENSE_MANAGER_REVIEW','MANAGER_REVIEW') then
    v_phase:='REVIEW';
  elsif v_variant in ('ELECTRONIC_SIGNED','EXPENSE_MANAGER_SIGNED','FINAL_SIGNED') then
    v_phase:='FINAL';
  else
    raise exception 'CANDIDATE_RENDER_VARIANT_INVALID' using errcode='22023';
  end if;

  select coalesce(jsonb_agg(
    private._candidate_component_render_contract_v1(
      p_workflow_id,p_workflow_generation,c.id,v_phase)
    order by c.review_ordinal,c.id
  ),'[]'::jsonb)
  into v_components
  from public.candidate_submission_components c
  where c.workflow_id=p_workflow_id
    and c.workflow_generation=p_workflow_generation
    and c.required=true
    and c.state<>'SUPERSEDED';
  if jsonb_array_length(v_components)=0 then
    raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
  end if;

  return jsonb_build_object(
    'workflow_id',p_workflow_id,
    'workflow_generation',p_workflow_generation,
    'phase',v_phase,
    'components',v_components
  );
end;
$function$;

-- private._candidate_render_input_v1(uuid,integer)
CREATE OR REPLACE FUNCTION private._candidate_render_input_v1(p_workflow_id uuid, p_workflow_generation integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_candidate_signature public.candidate_submission_components%rowtype;
  v_signature_digest bytea;
  v_core jsonb;
  v_signature_json jsonb := 'null'::jsonb;
begin
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_workflow_generation;
  if not found then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;
  if v_workflow.immutable_submission_json is null
     or v_workflow.immutable_submission_sha256 is null then
    raise exception 'CANDIDATE_IMMUTABLE_SUBMISSION_REQUIRED' using errcode='55000';
  end if;

  if v_workflow.workflow_kind<>'CONTRACT_EXPENSE' then
    select * into v_candidate_signature
    from public.candidate_submission_components
    where id=v_workflow.candidate_signature_component_id
      and workflow_id=v_workflow.id
      and workflow_generation=v_workflow.generation
      and component_kind='CANDIDATE_SIGNATURE'
      and document_role='CANDIDATE_SIGNATURE'
      and state='IMMUTABLE';
    if not found then
      raise exception 'CANDIDATE_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_signature_digest:=v_candidate_signature.source_content_sha256;
    if v_signature_digest is null and v_candidate_signature.source_component_id is not null then
      select source_content_sha256 into v_signature_digest
      from public.candidate_submission_components
      where id=v_candidate_signature.source_component_id and state='IMMUTABLE';
    end if;
    if v_signature_digest is null
       or v_signature_digest is distinct from v_workflow.candidate_signature_sha256 then
      raise exception 'CANDIDATE_SIGNATURE_REQUIRED' using errcode='55000';
    end if;
    v_signature_json:=jsonb_build_object(
      'component_id',v_candidate_signature.id,
      'source_component_id',coalesce(v_candidate_signature.source_component_id,v_candidate_signature.id),
      'storage_key',v_candidate_signature.storage_key,
      'sha256',encode(v_signature_digest,'hex'),
      'media_type',v_candidate_signature.media_type,
      'signed_at_utc',v_workflow.candidate_signed_at_utc
    );
  elsif v_workflow.candidate_signature_component_id is not null
        or v_workflow.candidate_signature_sha256 is not null then
    raise exception 'CONTRACT_EXPENSE_CANDIDATE_SIGNATURE_FORBIDDEN' using errcode='55000';
  end if;

  v_core:=jsonb_build_object(
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'scope',v_workflow.scope,
    'workflow_kind',v_workflow.workflow_kind,
    'contract_id',v_workflow.contract_id,
    'contract_week_id',v_workflow.contract_week_id,
    'work_date',v_workflow.work_date,
    'week_ending_date',v_workflow.week_ending_date,
    'immutable_submission',v_workflow.immutable_submission_json,
    'immutable_submission_sha256',encode(v_workflow.immutable_submission_sha256,'hex'),
    'policy_snapshot_sha256',encode(v_workflow.policy_snapshot_sha256,'hex'),
    'candidate_signature',v_signature_json,
    'renderer_contract_version',v_workflow.renderer_contract_version
  );
  return v_core||jsonb_build_object(
    'render_input_sha256',encode(private._candidate_sha256_jsonb_v1(v_core),'hex')
  );
end;
$function$;

-- private._candidate_require_feature_v1(text,text)
CREATE OR REPLACE FUNCTION private._candidate_require_feature_v1(p_environment text, p_feature_key text)
 RETURNS void
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
begin
  if not private._candidate_feature_enabled_v1(p_environment,p_feature_key) then
    raise exception 'CANDIDATE_FEATURE_DISABLED'
      using errcode='42501', detail=jsonb_build_object(
        'code','CANDIDATE_FEATURE_DISABLED',
        'feature',btrim(coalesce(p_feature_key,''))
      )::text;
  end if;
end;
$function$;

-- private._candidate_review_manifest_v1(uuid,integer)
CREATE OR REPLACE FUNCTION private._candidate_review_manifest_v1(p_workflow_id uuid, p_workflow_generation integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_components jsonb;
  v_component_ids jsonb;
  v_manifest_core jsonb;
  v_required_count integer;
  v_ready_count integer;
  v_hours_component_id uuid;
  v_hours_sha256 bytea;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_workflow_generation;
  if not found then
    raise exception 'WORKFLOW_GENERATION_CONFLICT' using errcode='40001';
  end if;

  select
    count(*)::integer,
    count(*) filter (where c.review_render_state='READY')::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'component_id',c.id,
      'component_kind',c.component_kind,
      'document_role',c.document_role,
      'expense_category',c.expense_category,
      'ordinal',c.review_ordinal,
      'required',c.required,
      'content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
      'media_type',c.review_media_type,
      'page_count',c.review_page_count,
      'render_input_sha256',case when c.review_render_input_sha256 is null then null else encode(c.review_render_input_sha256,'hex') end,
      'renderer_contract_version',c.review_renderer_contract_version,
      'render_state',c.review_render_state
    ) order by c.review_ordinal,c.id),'[]'::jsonb),
    coalesce(jsonb_agg(to_jsonb(c.id) order by c.review_ordinal,c.id),'[]'::jsonb),
    (array_agg(c.id order by c.review_ordinal,c.id)
      filter (where c.component_kind='HOURS_TIMESHEET'))[1],
    (array_agg(c.review_content_sha256 order by c.review_ordinal,c.id)
      filter (where c.component_kind='HOURS_TIMESHEET'))[1]
  into v_required_count,v_ready_count,v_components,v_component_ids,
       v_hours_component_id,v_hours_sha256
  from public.candidate_submission_components c
  where c.workflow_id=p_workflow_id
    and c.workflow_generation=p_workflow_generation
    and c.required=true
    and c.state<>'SUPERSEDED';

  if v_workflow.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY')
     and v_hours_component_id is null then
    raise exception 'MANAGER_REVIEW_DOCUMENT_NOT_READY' using errcode='55000';
  end if;
  if v_workflow.workflow_kind='CONTRACT_EXPENSE' and v_hours_component_id is not null then
    raise exception 'CONTRACT_EXPENSE_HOURS_COMPONENT_FORBIDDEN' using errcode='55000';
  end if;

  v_manifest_core:=jsonb_build_object(
    'workflow_id',p_workflow_id,
    'workflow_generation',p_workflow_generation,
    'workflow_kind',v_workflow.workflow_kind,
    'required_components',v_components
  );
  return jsonb_build_object(
    'workflow_id',p_workflow_id,
    'workflow_generation',p_workflow_generation,
    'workflow_kind',v_workflow.workflow_kind,
    'all_ready',v_required_count>0 and v_ready_count=v_required_count,
    'required_count',v_required_count,
    'ready_count',v_ready_count,
    'required_component_ids',v_component_ids,
    'required_components',v_components,
    'manager_review_timesheet_component_id',v_hours_component_id,
    'manager_review_timesheet_sha256',case when v_hours_sha256 is null then null else encode(v_hours_sha256,'hex') end,
    'manifest_sha256',encode(private._candidate_sha256_jsonb_v1(v_manifest_core),'hex')
  );
end;
$function$;

-- private._candidate_route_family_v1(uuid,uuid)
CREATE OR REPLACE FUNCTION private._candidate_route_family_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_policy jsonb;
  v_effective_mode public.submission_mode_enum;
  v_import boolean:=false;
  v_qr_backed boolean:=false;
  v_family text;
  v_paper_fallback boolean:=false;
  v_is_daily boolean:=false;
  v_import_authority jsonb;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  end if;
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select week_row.* into v_week
    from public.contract_weeks week_row
    where week_row.timesheet_id=p_timesheet_id
    order by week_row.updated_at desc,week_row.id desc
    limit 1;
  end if;
  if v_week.id is null then
    -- DAILY rows have no contract-week route snapshot.  Their contract and the
    -- constrained pending route-intent column are the complete authority.
    if v_timesheet.timesheet_id is null
       or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_timesheet.contract_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  else
    select * into v_contract from public.contracts where id=v_week.contract_id;
  end if;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  if v_timesheet.timesheet_id is null and v_week.timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_week.timesheet_id;
  end if;
  if v_timesheet.timesheet_id is not null then
    select * into v_fin
    from public.timesheets_financials financial_row
    where financial_row.timesheet_id=v_timesheet.timesheet_id and financial_row.is_current=true
    order by financial_row.computed_at_utc desc nulls last,financial_row.updated_at desc,financial_row.id desc
    limit 1;
  end if;
  v_policy:=private._candidate_policy_resolve_v1(
    v_contract.client_id,v_contract.id,
    coalesce(v_week.week_ending_date,v_timesheet.week_ending_date,
      private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
      ))
  );
  -- A fresh electronic resubmission cannot be stored as an unsigned
  -- ELECTRONIC timesheet because the canonical two-signature constraint is
  -- intentionally unchanged.  Its authoritative route therefore comes from
  -- the ELECTRONIC contract-week snapshot until finalisation writes both
  -- signatures and switches the canonical row atomically.
  v_effective_mode:=case
    when v_timesheet.submission_mode='ELECTRONIC'::public.submission_mode_enum
      or v_timesheet.candidate_submission_route_intent='ELECTRONIC'
      or v_week.submission_mode_snapshot='ELECTRONIC'::public.submission_mode_enum
      then 'ELECTRONIC'::public.submission_mode_enum
    else coalesce(
      v_timesheet.submission_mode,
      v_week.submission_mode_snapshot,
      private._candidate_submission_mode_v1(
        v_contract.client_id,v_contract.id,
        coalesce(v_week.week_ending_date,v_timesheet.week_ending_date,
          private._candidate_daily_work_date_v1(
            v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
          ))
      )
    ) end;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,v_timesheet.timesheet_id,to_jsonb(v_fin),
    coalesce(v_week.week_ending_date,v_timesheet.week_ending_date,
      private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,null
      ))
  );
  v_import:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);
  v_qr_backed:=v_timesheet.qr_status is not null
    or v_timesheet.qr_token is not null
    or v_timesheet.qr_r2_key is not null
    or exists(
      select 1
      from public.candidate_submission_workflows workflow
      where workflow.target_timesheet_id=v_timesheet.timesheet_id
        and workflow.route='PAPER'
        and workflow.state not in ('CANCELLED','REJECTED','REFUSED','EXPIRED','SUPERSEDED')
    );
  v_is_daily:=v_timesheet.timesheet_id is not null
    and v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum;
  -- Candidate PAPER/QR is a WEEKLY route only.  Existing office DAILY QR
  -- facts may still classify a legacy row as QR, but they never grant a
  -- Candidate paper mutation capability.
  v_paper_fallback:=not v_is_daily
    and coalesce((v_policy->>'paper_submission_enabled')::boolean,false);
  v_family:=case
    when v_import then 'IMPORT_AUTHORITATIVE'
    when v_qr_backed then 'QR'
    when v_effective_mode='ELECTRONIC' then 'ELECTRONIC'
    when v_timesheet.timesheet_id is null and v_paper_fallback then 'QR'
    else 'MANUAL_NON_QR'
  end;
  return jsonb_build_object(
    'route_family',v_family,
    'effective_submission_mode',v_effective_mode,
    'pending_route_intent',v_timesheet.candidate_submission_route_intent,
    'import_authoritative',v_import,
    'import_source_family',v_import_authority->>'source_family',
    'qr_backed',v_qr_backed,
    'electronic_paper_fallback_enabled',v_family='ELECTRONIC' and v_paper_fallback,
    'candidate_hours_submission_allowed',v_family='ELECTRONIC'
      or (v_family='QR' and not v_is_daily),
    'candidate_expenses_allowed',v_family in ('ELECTRONIC','QR','IMPORT_AUTHORITATIVE'),
    'candidate_paper_submission_allowed',not v_is_daily
      and (v_family='QR' or (v_family='ELECTRONIC' and v_paper_fallback)),
    'candidate_no_work_allowed',v_family='ELECTRONIC'
      or (v_family='QR' and not v_is_daily),
    'policy',v_policy
  );
end;
$function$;

-- private._candidate_session_context_v1(uuid,text,integer,timestamp with time zone,boolean)
CREATE OR REPLACE FUNCTION private._candidate_session_context_v1(p_session_id uuid, p_environment text, p_expected_rotation integer DEFAULT NULL::integer, p_now_utc timestamp with time zone DEFAULT now(), p_lock boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_session public.candidate_app_sessions%rowtype;
  v_account public.candidate_app_accounts%rowtype;
  v_eligibility jsonb;
  v_selected_owned boolean := false;
begin
  v_environment := private._candidate_assert_environment(p_environment);
  if p_session_id is null then
    raise exception 'CANDIDATE_SESSION_REQUIRED' using errcode='28000';
  end if;

  if p_lock then
    select * into v_session from public.candidate_app_sessions
    where id=p_session_id for update;
  else
    select * into v_session from public.candidate_app_sessions
    where id=p_session_id;
  end if;

  if not found or v_session.environment<>v_environment then
    raise exception 'CANDIDATE_SESSION_INVALID' using errcode='28000';
  end if;
  if v_session.status<>'ACTIVE'
     or v_session.expires_at_utc<=p_now_utc
     or v_session.absolute_expires_at_utc<=p_now_utc then
    raise exception 'CANDIDATE_SESSION_EXPIRED' using errcode='28000';
  end if;
  if p_expected_rotation is not null and v_session.rotation<>p_expected_rotation then
    raise exception 'CANDIDATE_SESSION_ROTATION_MISMATCH'
      using errcode='40001', detail=jsonb_build_object('code','CANDIDATE_SESSION_ROTATION_MISMATCH')::text;
  end if;

  select * into v_account from public.candidate_app_accounts
  where id=v_session.account_id;
  if not found or v_account.environment<>v_environment or v_account.status<>'ACTIVE' then
    raise exception 'CANDIDATE_ACCOUNT_INACTIVE' using errcode='28000';
  end if;

  v_eligibility := private._candidate_email_eligibility_v1(v_environment,v_account.email_normalized);
  if coalesce((v_eligibility->>'eligible')::boolean,false)=false then
    raise exception 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' using errcode='28000';
  end if;

  if v_session.selected_candidate_id is not null then
    select exists(
      select 1 from public.candidates c
      where c.id=v_session.selected_candidate_id
        and c.active=true
        and lower(btrim(coalesce(c.email,'')))=v_account.email_normalized
    ) into v_selected_owned;
    if not v_selected_owned then
      raise exception 'CANDIDATE_SELECTION_NOT_ALLOWED' using errcode='28000';
    end if;
  end if;

  return jsonb_build_object(
    'session_id',v_session.id,
    'account_id',v_account.id,
    'environment',v_environment,
    'email_normalized',v_account.email_normalized,
    'selected_candidate_id',v_session.selected_candidate_id,
    'rotation',v_session.rotation,
    'session_version',v_account.session_version,
    'eligibility',v_eligibility,
    'notification_preferences',v_account.notification_preferences_json,
    'expires_at_utc',v_session.expires_at_utc,
    'absolute_expires_at_utc',v_session.absolute_expires_at_utc
  );
end;
$function$;

