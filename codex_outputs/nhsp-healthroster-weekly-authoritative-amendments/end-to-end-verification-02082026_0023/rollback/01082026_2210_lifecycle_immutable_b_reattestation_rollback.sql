-- TEST rollback captured immediately before the lifecycle immutable-B re-attestation deployment.
-- Restores the exact previously installed function definition only.
begin;
CREATE OR REPLACE FUNCTION public.import_review_correction_generation_transition_v1(p_import_id uuid, p_operation_id uuid, p_request_hash text, p_action text, p_actor_user_id uuid, p_action_ids text[] DEFAULT '{}'::text[], p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_operation public.import_apply_operations%rowtype;
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_units jsonb:='[]'::jsonb;
  v_request_units jsonb:='[]'::jsonb;
  v_applied_units jsonb:='[]'::jsonb;
  v_policy_units jsonb:='[]'::jsonb;
  v_unit jsonb;
  v_balance jsonb;
  v_capability_token text;
  v_items jsonb;
  v_result jsonb;
  v_target_ids uuid[];
  v_pending_target_ids uuid[];
  v_member_count integer;
  v_bad_count integer;
  v_id uuid;
  v_signature jsonb;
  v_current_invoice_fingerprint text;
  v_recomputed_unit_fingerprint text;
  v_all_authorised boolean:=false;
  v_any_authorised boolean:=false;
  v_unit_fingerprints jsonb:='[]'::jsonb;
  v_expected_a_day numeric;
  v_expected_a_night numeric;
  v_expected_a_sat numeric;
  v_expected_a_sun numeric;
  v_expected_a_bh numeric;
  v_expected_a_total numeric;
  v_frozen_a_bucket_total numeric;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if session_user not in ('postgres','service_role') and coalesce(
      current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='42501';
  end if;
  if p_import_id is null or p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
     or v_action not in ('PREPARE','VALIDATE','AUTHORISE') or cardinality(coalesce(p_action_ids,array[]::text[]))>100 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='22023';
  end if;
  perform set_config('lock_timeout','1500ms',true);
  select * into v_operation from public.import_apply_operations
  where id=p_operation_id and import_id=p_import_id for update;
  if v_operation.id is null or v_operation.request_hash<>lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='40001';
  end if;
  if v_action in ('VALIDATE','AUTHORISE') and (
      v_operation.committed_at_utc is null
      or v_operation.state not in ('SOURCE_COMMITTED_TSFIN_PENDING','COMPLETE')) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='40001';
  end if;
  perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);

  if v_action='PREPARE' then
    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is null
       or current_setting('cloudtms.import_reconciliation_operation_id',true) is distinct from p_operation_id::text
       or current_setting('cloudtms.import_reconciliation_request_hash',true) is distinct from v_operation.request_hash then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_GUARD_REQUIRED' using errcode='55000';
    end if;
    select coalesce(jsonb_agg(u.unit_json order by u.action_id),'[]'::jsonb) into v_units
    from pg_temp.import_review_reconciliation_units_v1 u
    where cardinality(coalesce(p_action_ids,array[]::text[]))=0 or u.action_id=any(p_action_ids);
  else
    v_request_units:=coalesce(v_operation.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb);
    v_applied_units:=coalesce(v_operation.response_json->'reconciliation_units','[]'::jsonb);
    v_policy_units:=coalesce(v_operation.response_json#>'{correction_operation_contract,correction_units}','[]'::jsonb);
    if cardinality(coalesce(p_action_ids,array[]::text[]))>0 then
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_request_units
      from jsonb_array_elements(v_request_units) u where u->>'action_id'=any(p_action_ids);
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_applied_units
      from jsonb_array_elements(v_applied_units) u where u->>'action_id'=any(p_action_ids);
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_policy_units
      from jsonb_array_elements(v_policy_units) u where u->>'action_id'=any(p_action_ids);
    end if;
    if exists(
      select 1 from jsonb_array_elements(v_request_units) request
      where request->>'route' in ('AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and ((select count(*) from jsonb_array_elements(v_applied_units) applied where applied->>'action_id'=request->>'action_id')<>1
          or (select count(*) from jsonb_array_elements(v_policy_units) policy where policy->>'action_id'=request->>'action_id')<>1)
    ) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
    end if;
    select coalesce(jsonb_agg(
      request
      || case when applied is null then '{}'::jsonb else jsonb_build_object(
          'correction_id',applied->>'correction_id','applied_member_ids',applied->'applied_member_ids',
          'reversal_timesheet_id',applied->>'reversal_timesheet_id','replacement_timesheet_id',applied->>'replacement_timesheet_id',
          'parent_timesheet_id',applied->>'parent_timesheet_id','repair_identity_mode',applied->>'repair_identity_mode',
          'applied_result_fingerprint',applied->>'applied_result_fingerprint',
          'reviewed_unit_fingerprint',applied->>'reviewed_unit_fingerprint','applied_reconciliation_fingerprint',applied->>'reconciliation_fingerprint',
          'applied_timesheet_id',applied->>'applied_timesheet_id','rollover_mode',applied->>'rollover_mode',
          'historical_paid_tsfin_id',applied->>'historical_paid_tsfin_id','current_shell_tsfin_id',applied->>'current_shell_tsfin_id',
          'applied_intended_authorisation_action',applied->>'intended_authorisation_action') end
      || case when policy is null then '{}'::jsonb else jsonb_build_object(
          'operation_policy_envelope',policy->'policy_envelope',
          'operation_policy_fingerprint',policy->>'policy_envelope_fingerprint',
          'operation_policy_root_timesheet_id',policy->>'root_timesheet_id',
          'operation_policy_source_row_key',policy->>'source_row_key',
          'operation_policy_source_shift_id',policy->>'source_shift_id') end
      order by request->>'action_id'),'[]'::jsonb)
    into v_units
    from jsonb_array_elements(v_request_units) request
    left join lateral (select u applied from jsonb_array_elements(v_applied_units) u where u->>'action_id'=request->>'action_id' limit 1) a on true
    left join lateral (select u policy from jsonb_array_elements(v_policy_units) u where u->>'action_id'=request->>'action_id' limit 1) p on true;
  end if;
  if jsonb_array_length(v_units)=0 then
    return jsonb_build_object('ok',true,'action',v_action,'idempotent',true,'unit_count',0,'timesheet_ids','[]'::jsonb);
  end if;
  select coalesce(jsonb_agg(u->>'unit_fingerprint' order by u->>'action_id'),'[]'::jsonb)
  into v_unit_fingerprints from jsonb_array_elements(v_units) u;
  if exists(select 1 from jsonb_array_elements(v_units) u
      where u->>'schema_version'<>'IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
        or nullif(u->>'unit_fingerprint','') is null
        or nullif(u->>'source_identity','') is null
        or u->>'route' not in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_NOT_FOUND' using errcode='22023';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1
    from jsonb_array_elements(v_units) u
    left join public.import_review_action_outcomes outcome
      on outcome.operation_id=p_operation_id and outcome.action_id=u->>'action_id'
    where outcome.action_id is null
       or u->>'unit_fingerprint' is distinct from public._import_review_hash_v1(concat_ws('|','unit-v2',
         u->>'action_id',u->>'source_identity',u->>'source_shift_id',u->>'route',u->>'reconciliation_mode',
         u->>'reconciliation_fingerprint',u->>'review_policy_basis_kind',u->>'review_policy_basis_fingerprint',
         outcome.evidence_fingerprint))
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_FINGERPRINT_MISMATCH' using errcode='40001';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1 from jsonb_array_elements(v_units) u
    where u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and (
      nullif(u->>'correction_id','') is null
      or nullif(u->>'reviewed_unit_fingerprint','') is distinct from nullif(u->>'unit_fingerprint','')
      or nullif(u->>'applied_reconciliation_fingerprint','') is distinct from nullif(u->>'reconciliation_fingerprint','')
      or nullif(u->>'operation_policy_source_row_key','') is distinct from nullif(u->>'source_identity','')
      or nullif(u->>'operation_policy_source_shift_id','') is distinct from nullif(u->>'source_shift_id','')
      or nullif(u->>'operation_policy_root_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or jsonb_typeof(u->'applied_member_ids')<>'array' or jsonb_array_length(u->'applied_member_ids')<>2
      or coalesce(u->>'reversal_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(u->>'replacement_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or u->>'reversal_timesheet_id'=u->>'replacement_timesheet_id'
      or not (u->'applied_member_ids' @> jsonb_build_array(u->>'reversal_timesheet_id')
        and u->'applied_member_ids' @> jsonb_build_array(u->>'replacement_timesheet_id'))
      or u->>'applied_result_fingerprint' is distinct from encode(digest(convert_to(jsonb_build_object(
        'correction_id',u->>'correction_id',
        'reversal_timesheet_id',(u->>'reversal_timesheet_id')::uuid,
        'replacement_timesheet_id',(u->>'replacement_timesheet_id')::uuid,
        'M_active_member_ids',u->'applied_member_ids',
        'applied_member_ids',u->'applied_member_ids',
        'parent_timesheet_id',(u->>'parent_timesheet_id')::uuid,
        'repair_identity_mode',u->>'repair_identity_mode',
        'reviewed_unit_fingerprint',u->>'reviewed_unit_fingerprint',
        'reconciliation_fingerprint',u->>'applied_reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
      or jsonb_typeof(u->'operation_policy_envelope')<>'object'
      or nullif(u->>'operation_policy_fingerprint','') is null
      or u#>>'{operation_policy_envelope,envelope_fingerprint}' is distinct from u->>'operation_policy_fingerprint'
      or encode(digest(convert_to(((u->'operation_policy_envelope')-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
        is distinct from u->>'operation_policy_fingerprint'
    )
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1 from jsonb_array_elements(v_units) u
    where u->>'route'='AMEND_PAID_UNINVOICED_SOURCE' and (
      nullif(u->>'reviewed_unit_fingerprint','') is distinct from nullif(u->>'unit_fingerprint','')
      or nullif(u->>'applied_reconciliation_fingerprint','') is distinct from nullif(u->>'reconciliation_fingerprint','')
      or nullif(u->>'applied_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or nullif(u->>'applied_intended_authorisation_action','') is distinct from nullif(u->>'intended_authorisation_action','')
      or coalesce(u->>'rollover_mode','') not in ('CREATED_CURRENT_OPERATION_SHELL','REUSED_COMPLETED_OPERATION_SHELL')
      or coalesce(u->>'historical_paid_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(u->>'current_shell_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or nullif(u->>'operation_policy_source_row_key','') is distinct from nullif(u->>'source_identity','')
      or nullif(u->>'operation_policy_source_shift_id','') is distinct from nullif(u->>'source_shift_id','')
      or nullif(u->>'operation_policy_root_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or jsonb_typeof(u->'operation_policy_envelope')<>'object'
      or nullif(u->>'operation_policy_fingerprint','') is null
      or u#>>'{operation_policy_envelope,envelope_fingerprint}' is distinct from u->>'operation_policy_fingerprint'
      or encode(digest(convert_to(((u->'operation_policy_envelope')-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
        is distinct from u->>'operation_policy_fingerprint'
      or u->>'applied_result_fingerprint' is distinct from encode(digest(convert_to(jsonb_build_object(
        'applied_timesheet_id',(u->>'applied_timesheet_id')::uuid,
        'rollover_mode',u->>'rollover_mode',
        'historical_paid_tsfin_id',(u->>'historical_paid_tsfin_id')::uuid,
        'current_shell_tsfin_id',(u->>'current_shell_tsfin_id')::uuid,
        'intended_authorisation_action',u->>'intended_authorisation_action',
        'reviewed_unit_fingerprint',u->>'reviewed_unit_fingerprint',
        'reconciliation_fingerprint',u->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
    )
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
  end if;

  if v_action='PREPARE' then
    select coalesce(array_agg(distinct x.value::uuid order by x.value::uuid),array[]::uuid[]) into v_target_ids
    from jsonb_array_elements(v_units) u
    cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
    join public.timesheets t on t.timesheet_id=x.value::uuid and t.is_current and t.archived_at_utc is null
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where t.authorised_at_server is not null or tf.authorised_at_utc is not null
       or cw.status='AUTHORISED'::public.contract_week_status_enum;
    if exists(select 1 from jsonb_array_elements(v_units) u
      cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
      join public.timesheets t on t.timesheet_id=x.value::uuid where t.archived_at_utc is not null) then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    if cardinality(v_target_ids)>0 then
      v_capability_token:=encode(gen_random_bytes(32),'hex');
      create temporary table if not exists pg_temp.import_review_lifecycle_capability_v1(
        capability_token text not null,txid bigint not null,operation_id uuid not null,request_hash text not null,
        actor_user_id uuid not null,action text not null,action_id text not null,unit_fingerprint text not null,
        timesheet_id uuid not null,expected_timesheet_id uuid not null,expected_version integer,
        expected_row_signature text,expected_tsfin_id uuid,expected_contract_week_id uuid
      ) on commit drop;
      truncate pg_temp.import_review_lifecycle_capability_v1;
      foreach v_id in array v_target_ids loop
        v_signature:=public.timesheet_lifecycle_signature_v1(v_id,null,false);
        insert into pg_temp.import_review_lifecycle_capability_v1
        select v_capability_token,txid_current(),p_operation_id,v_operation.request_hash,p_actor_user_id,'UNAUTHORISE',
          u->>'action_id',u->>'unit_fingerprint',v_id,v_id,t.version,
          coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature'),
          tf.id,cw.id
        from jsonb_array_elements(v_units) u
        join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value) on x.value::uuid=v_id
        join public.timesheets t on t.timesheet_id=v_id and t.is_current and t.archived_at_utc is null
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id;
      end loop;
      perform set_config('cloudtms.import_reconciliation_capability_token',v_capability_token,true);
      perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
      perform set_config('cloudtms.import_reconciliation_action','UNAUTHORISE',true);
      select jsonb_agg(jsonb_build_object('timesheet_id',x) order by x) into v_items from unnest(v_target_ids) x;
      v_result:=public.timesheet_unauthorise_bulk_atomic(v_items,p_actor_user_id,coalesce(p_now_utc,now()));
      if coalesce((v_result->>'ok')::boolean,false) is not true or coalesce((v_result->>'all_success')::boolean,false) is not true then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_PREPARE_INCOMPLETE' using errcode='55000',detail=coalesce(v_result->>'error_code','bulk unauthorise incomplete');
      end if;
      truncate pg_temp.import_review_lifecycle_capability_v1;
      perform set_config('cloudtms.import_reconciliation_capability_token','',true);
      perform set_config('cloudtms.import_reconciliation_action','',true);
    end if;
    if exists(select 1 from jsonb_array_elements(v_units) u
      cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
      join public.timesheets t on t.timesheet_id=x.value::uuid and t.is_current and t.archived_at_utc is null
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
      where t.authorised_at_server is not null or tf.authorised_at_utc is not null
         or cw.status='AUTHORISED'::public.contract_week_status_enum) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_PREPARE_INCOMPLETE' using errcode='55000';
    end if;
    return jsonb_build_object('ok',true,'action','PREPARE','unit_count',jsonb_array_length(v_units),
      'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[])),'bulk_result',v_result);
  end if;

  for v_unit in select value from jsonb_array_elements(v_units) loop
    perform 1 from public.invoices i where i.id in (
      select x.value::uuid from jsonb_array_elements_text(coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb)) x(value)
    ) order by i.id for update;
    perform 1 from public.invoice_lines il where il.id in (
      select x.value::uuid from jsonb_array_elements_text(coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb)) x(value)
    ) order by il.id for update;
    perform 1 from public.nhsp_shifts s where s.id=(v_unit->>'source_shift_id')::uuid for update;
    if not exists(select 1 from public.nhsp_shifts s where s.id=(v_unit->>'source_shift_id')::uuid
      and s.external_row_key=v_unit->>'source_identity' and s.cancelled_at_utc is null
      and s.source_system::text=v_unit->>'source_system') then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_MISMATCH' using errcode='40001';
    end if;
    perform 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
      and t.is_current and t.archived_at_utc is null order by t.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.is_current and tf.timesheet_id in (
      select t.timesheet_id from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null
    ) order by tf.timesheet_id,tf.id for update;
    perform 1 from public.contract_weeks cw where cw.timesheet_id in (
      select t.timesheet_id from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null
    ) order by cw.id for update;
    select b.balance_json into v_balance
    from public._import_review_effective_invoice_balance_core_v1(p_import_id,jsonb_build_array(jsonb_build_object(
      'source_identity',v_unit->>'source_identity','source_system',v_unit->>'source_system',
      'source_shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity',
      'hr_row_id',v_unit->>'hr_row_id','source_timesheet_id',v_unit->>'source_timesheet_id',
      'candidate_id',v_unit->>'candidate_id','client_id',v_unit->>'client_id','contract_id',v_unit->>'contract_id',
      'week_ending_date',v_unit->>'week_ending_date','invoice_stream',v_unit->>'invoice_stream',
      'authoritative_import_id',p_import_id,'authoritative_schedule_json',v_unit->'A_schedule_json',
      'authoritative_hours',v_unit->'A_hours')),100,512,256,128) b;
    if nullif(v_balance->>'blocking_code','') is not null then
      raise exception 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS' using errcode='55000',detail=v_balance->>'blocking_code';
    end if;
    v_current_invoice_fingerprint:=v_balance->>'effective_invoice_fingerprint';
    if v_current_invoice_fingerprint is distinct from v_unit->>'B_invoice_fingerprint' then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    if v_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') then
      select count(*) into v_member_count from public.timesheets t
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if v_member_count<>2 or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL')
        or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT')
        or exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null
          and not (v_unit->'applied_member_ids' @> jsonb_build_array(t.timesheet_id::text)))
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(v_unit->>'reversal_timesheet_id')::uuid
          and t.correction_id=v_unit->>'correction_id' and t.correction_kind='CHANGED_HOURS_REVERSAL' and t.is_current and t.archived_at_utc is null)
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(v_unit->>'replacement_timesheet_id')::uuid
          and t.correction_id=v_unit->>'correction_id' and t.correction_kind='CHANGED_HOURS_REPLACEMENT' and t.is_current and t.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_MEMBER_SET_MISMATCH' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') and (
            not coalesce(t.is_adjustment,false) or t.adjustment_origin<>'IMPORT_CORRECTION'
            or t.parent_timesheet_id is distinct from (v_unit->>'parent_timesheet_id')::uuid
            or t.contract_id is distinct from (v_unit->>'contract_id')::uuid
            or t.week_ending_date is distinct from (v_unit->>'week_ending_date')::date
            or t.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
            or tf.candidate_id is distinct from (v_unit->>'candidate_id')::uuid
            or tf.client_id is distinct from (v_unit->>'client_id')::uuid
            or (v_unit->>'source_system'='NHSP' and tf.basis<>'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum)
            or (v_unit->>'source_system'='HEALTHROSTER' and tf.basis<>'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum)
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'<>p_operation_id::text
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'<>v_unit->>'unit_fingerprint'
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}'<>v_unit->>'source_identity'
            or jsonb_typeof(t.actual_schedule_json)<>'array'
            or jsonb_array_length(t.actual_schedule_json)<>1
            or not t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
              'shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity'))
            or (select count(*) from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id)<>1
            or exists(select 1 from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id
              and (not coalesce(cw.is_adjustment,false)
                or cw.contract_id is distinct from (v_unit->>'contract_id')::uuid
                or cw.week_ending_date is distinct from (v_unit->>'week_ending_date')::date))
          ))
         or not exists(select 1 from public.timesheets parent_ts
           where parent_ts.timesheet_id=(v_unit->>'parent_timesheet_id')::uuid
             and parent_ts.is_current and parent_ts.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_MISMATCH' using errcode='40001';
      end if;
      if exists(select 1 from public.timesheets t
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (coalesce(public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint','')
            is distinct from coalesce(v_unit->>'operation_policy_fingerprint','')
            or public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)
              is distinct from v_unit->'operation_policy_envelope'))
        or (select count(distinct public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint')
            from public.timesheets t where t.correction_id=v_unit->>'correction_id'
              and t.is_current and t.archived_at_utc is null
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_POLICY_MISMATCH' using errcode='40001';
      end if;
      select h.hours_day,h.hours_night,h.hours_sat,h.hours_sun,h.hours_bh,h.total_hours
      into v_expected_a_day,v_expected_a_night,v_expected_a_sat,v_expected_a_sun,v_expected_a_bh,v_expected_a_total
      from public.timesheets t
      join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      cross join lateral public._wkimp_bucket_hours_from_policy(
        coalesce(tf.policy_snapshot_json,'{}'::jsonb),
        (v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz,
        (v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz,
        coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)
      ) h
      where t.timesheet_id=(v_unit->>'replacement_timesheet_id')::uuid
        and t.correction_id=v_unit->>'correction_id'
        and t.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and t.is_current and t.archived_at_utc is null
      limit 1;
      v_frozen_a_bucket_total:=coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0);
      if v_expected_a_total is null
        or v_expected_a_total is distinct from coalesce((v_unit#>>'{A_hours,total_hours}')::numeric,0)
        or (v_frozen_a_bucket_total<>0 and (
          v_expected_a_day is distinct from coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
          or v_expected_a_night is distinct from coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
          or v_expected_a_sat is distinct from coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
          or v_expected_a_sun is distinct from coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
          or v_expected_a_bh is distinct from coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0)
        )) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (tf.id is null or tf.processing_status not in (
            'PENDING_AUTH'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum,
            'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum))) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null and (
          (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
            (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_unit#>>'{B_standard_schedule_json,0,start_utc}')::timestamptz
            or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_unit#>>'{B_standard_schedule_json,0,end_utc}')::timestamptz
            or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_unit#>>'{B_standard_schedule_json,0,break_mins}')::integer,0)))
          or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
            (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
            or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
            or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)))
        )) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      select count(*) into v_bad_count
      from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and (coalesce(tf.is_stale,true) or coalesce(tf.has_rate_issue,false) or coalesce(tf.has_pay_channel_issue,false));
      if v_bad_count>0 then raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000'; end if;
      select count(*) into v_bad_count
      from public.timesheets t
      join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and case t.correction_kind
          when 'CHANGED_HOURS_REVERSAL' then
            tf.hours_day<>-coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0)
            or tf.hours_night<>-coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)
            or tf.hours_sat<>-coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0)
            or tf.hours_sun<>-coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)
            or tf.hours_bh<>-coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0)
            or tf.total_pay_ex_vat<>-coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
            or tf.total_charge_ex_vat<>-coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)
          when 'CHANGED_HOURS_REPLACEMENT' then
            tf.hours_day<>v_expected_a_day or tf.hours_night<>v_expected_a_night
            or tf.hours_sat<>v_expected_a_sat or tf.hours_sun<>v_expected_a_sun
            or tf.hours_bh<>v_expected_a_bh
          else false
        end;
      if v_bad_count>0 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
    else
      if jsonb_array_length(coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb))<>0
         or jsonb_array_length(coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb))<>0
         or coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)<>0 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      if not exists(select 1 from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.timesheet_id=(v_unit->>'source_timesheet_id')::uuid and t.is_current and t.archived_at_utc is null
          and t.contract_id=(v_unit->>'contract_id')::uuid
          and t.week_ending_date=(v_unit->>'week_ending_date')::date
          and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          and tf.candidate_id=(v_unit->>'candidate_id')::uuid
          and tf.client_id=(v_unit->>'client_id')::uuid
          and ((v_unit->>'source_system'='NHSP' and tf.basis='NHSP'::public.timesheet_fin_basis_enum)
            or (v_unit->>'source_system'='HEALTHROSTER' and tf.basis='HEALTHROSTER'::public.timesheet_fin_basis_enum))
          and jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz=(v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
          and (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz=(v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
          and coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0)=coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
            'shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity'))
          and (v_unit->>'route'<>'AMEND_PAID_UNINVOICED_SOURCE'
            or coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
              tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
              =v_unit->>'operation_policy_fingerprint')
          and not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)
          and tf.processing_status in ('PENDING_AUTH'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum,'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum)
          and tf.hours_day=coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
          and tf.hours_night=coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
          and tf.hours_sat=coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
          and tf.hours_sun=coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
          and tf.hours_bh=coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0)) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000';
      end if;
    end if;
  end loop;

  if v_action='VALIDATE' then
    if not exists(select 1 from public.audit_events ae where ae.action='IMPORT_REVIEW_RECONCILIATION_VALIDATED'
      and ae.after_json->>'operation_id'=p_operation_id::text
      and ae.after_json->>'request_hash'=v_operation.request_hash
      and ae.after_json->'unit_fingerprints'=v_unit_fingerprints) then
      perform public._audit_insert('import_apply_operations',p_operation_id::text,'IMPORT_REVIEW_RECONCILIATION_VALIDATED',null,
        jsonb_build_object('import_id',p_import_id,'operation_id',p_operation_id,'request_hash',v_operation.request_hash,
          'unit_fingerprints',v_unit_fingerprints),
        'IMPORT_REVIEW',p_actor_user_id);
    end if;
    return jsonb_build_object('ok',true,'action','VALIDATE','unit_count',jsonb_array_length(v_units),'idempotent',false);
  end if;

  select coalesce(array_agg(distinct q.timesheet_id order by q.timesheet_id),array[]::uuid[]) into v_target_ids
  from (
    select t.timesheet_id
    from jsonb_array_elements(v_units) u
    join public.timesheets t on u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
      and t.correction_id=u->>'correction_id' and t.is_current and t.archived_at_utc is null
      and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
    where coalesce(u->>'intended_authorisation_action','LEAVE_UNAUTHORISED') in ('AUTHORISE','REAUTHORISE')
    union all
    select t.timesheet_id
    from jsonb_array_elements(v_units) u
    join public.timesheets t on u->>'route' in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE')
      and t.timesheet_id=(u->>'source_timesheet_id')::uuid and t.is_current and t.archived_at_utc is null
    where coalesce(u->>'intended_authorisation_action','LEAVE_UNAUTHORISED') in ('AUTHORISE','REAUTHORISE')
  ) q;
  if exists(
    select 1 from unnest(v_target_ids) x(timesheet_id)
    join public.timesheets t on t.timesheet_id=x.timesheet_id
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where (t.authorised_at_server is not null)::integer
        +(tf.authorised_at_utc is not null)::integer
        +(cw.status='AUTHORISED'::public.contract_week_status_enum)::integer not in (0,3)
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_LIFECYCLE_STATE_INVALID' using errcode='55000';
  end if;
  select coalesce(array_agg(x.timesheet_id order by x.timesheet_id),array[]::uuid[]) into v_pending_target_ids
  from unnest(v_target_ids) x(timesheet_id)
  join public.timesheets t on t.timesheet_id=x.timesheet_id
  join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
  join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
  where t.authorised_at_server is null and tf.authorised_at_utc is null
    and cw.status<>'AUTHORISED'::public.contract_week_status_enum;
  v_all_authorised:=cardinality(v_target_ids)>0 and cardinality(v_pending_target_ids)=0;
  v_any_authorised:=cardinality(v_target_ids)>cardinality(v_pending_target_ids);
  if cardinality(v_pending_target_ids)>0 then
    v_capability_token:=encode(gen_random_bytes(32),'hex');
    create temporary table if not exists pg_temp.import_review_lifecycle_capability_v1(
      capability_token text not null,txid bigint not null,operation_id uuid not null,request_hash text not null,
      actor_user_id uuid not null,action text not null,action_id text not null,unit_fingerprint text not null,
      timesheet_id uuid not null,expected_timesheet_id uuid not null,expected_version integer,
      expected_row_signature text,expected_tsfin_id uuid,expected_contract_week_id uuid
    ) on commit drop;
    truncate pg_temp.import_review_lifecycle_capability_v1;
    foreach v_id in array v_pending_target_ids loop
      v_signature:=public.timesheet_lifecycle_signature_v1(v_id,null,false);
      insert into pg_temp.import_review_lifecycle_capability_v1
      select v_capability_token,txid_current(),p_operation_id,v_operation.request_hash,p_actor_user_id,'AUTHORISE',
        u->>'action_id',u->>'unit_fingerprint',v_id,v_id,t.version,
        coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature'),tf.id,cw.id
      from jsonb_array_elements(v_units) u join public.timesheets t on t.timesheet_id=v_id
        and ((u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and t.correction_id=u->>'correction_id')
          or (u->>'route' in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE') and t.timesheet_id=(u->>'source_timesheet_id')::uuid))
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id;
    end loop;
    perform set_config('cloudtms.import_reconciliation_capability_token',v_capability_token,true);
    perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
    perform set_config('cloudtms.import_reconciliation_action','AUTHORISE',true);
    select jsonb_agg(jsonb_build_object('timesheet_id',x) order by x) into v_items from unnest(v_pending_target_ids) x;
    v_result:=public.timesheet_authorise_bulk_atomic(v_items,p_actor_user_id,coalesce(p_now_utc,now()));
    if coalesce((v_result->>'ok')::boolean,false) is not true or coalesce((v_result->>'all_success')::boolean,false) is not true then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_AUTHORISE_INCOMPLETE' using errcode='55000',detail=coalesce(v_result->>'error_code','bulk authorise incomplete');
    end if;
    truncate pg_temp.import_review_lifecycle_capability_v1;
    perform set_config('cloudtms.import_reconciliation_capability_token','',true);
    perform set_config('cloudtms.import_reconciliation_action','',true);
  end if;
  if exists(
    select 1 from unnest(v_target_ids) x(timesheet_id)
    left join public.timesheets t on t.timesheet_id=x.timesheet_id and t.is_current and t.archived_at_utc is null
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where t.timesheet_id is null or tf.id is null or cw.id is null
      or t.authorised_at_server is null or tf.authorised_at_utc is null
      or cw.status<>'AUTHORISED'::public.contract_week_status_enum
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_AUTHORISE_INCOMPLETE' using errcode='55000';
  end if;
  if not exists(select 1 from public.audit_events ae where ae.action='IMPORT_REVIEW_RECONCILIATION_AUTHORISED'
      and ae.after_json->>'operation_id'=p_operation_id::text and ae.after_json->>'request_hash'=v_operation.request_hash
      and ae.after_json->'unit_fingerprints'=v_unit_fingerprints) then
    perform public._audit_insert('import_apply_operations',p_operation_id::text,'IMPORT_REVIEW_RECONCILIATION_AUTHORISED',null,
      jsonb_build_object('import_id',p_import_id,'operation_id',p_operation_id,'request_hash',v_operation.request_hash,
        'unit_fingerprints',v_unit_fingerprints,'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[]))),
      'IMPORT_REVIEW',p_actor_user_id);
  end if;
  return jsonb_build_object('ok',true,'action','AUTHORISE','unit_count',jsonb_array_length(v_units),
    'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[])),
    'newly_authorised_timesheet_ids',to_jsonb(coalesce(v_pending_target_ids,array[]::uuid[])),
    'idempotent',v_all_authorised,'bulk_result',v_result);
end
$function$;
alter function public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz) owner to postgres;
revoke all on function public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz) from public,anon,authenticated;
grant execute on function public.import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamptz) to service_role;
commit;
