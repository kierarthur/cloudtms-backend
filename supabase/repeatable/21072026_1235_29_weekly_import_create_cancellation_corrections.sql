-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: a92a15a29522.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.weekly_import_create_cancellation_corrections(p_shift_id uuid, p_import_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_import_source_system public.hr_source_enum;
  v_import_client_id uuid;

  -- Shift authority
  v_shift_source_system public.hr_source_enum;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_contract_id uuid;
  v_shift_timesheet_id uuid;
  v_shift_work_date date;
  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_pay_minutes int;
  v_shift_ref_num text;
  v_shift_hr_request_id text;
  v_shift_external_row_key text;
  v_shift_week_ending_date date;
  v_shift_ward text;

  -- Canonical historical chain and preflight authority
  v_chain_scope jsonb;
  v_financial_preflight jsonb;
  v_correction_financials_policy_envelope jsonb;
  v_correction_financials_policy_envelope_fingerprint text;
  v_correction_operation_id uuid;
  v_root_timesheet_id uuid;
  v_latest_positive_timesheet_id uuid;

  -- Historical reversal schedule
  v_anchor_segment jsonb;
  v_reversal_schedule jsonb;
  v_replacement_schedule jsonb := '[]'::jsonb;

  -- Week ending resolution
  v_week_ending_date date;
  v_base_ts_week_ending date;
  v_contract_week_ending_weekday_snapshot int := 0;

  -- Correction identity and member state
  v_correction_id text;
  v_kind text;
  v_member_schedule jsonb;
  v_member_hint jsonb;
  v_member_label_norm text;
  v_member_booking_id text;
  v_member_ts_id uuid;
  v_existing_ts_id uuid;
  v_existing_kind text;
  v_existing_cw_id uuid;
  v_member_cw_id uuid;
  v_base_week_id uuid;
  v_next_additional_seq int;
  v_try int;
  v_pair_changed boolean := false;
  v_pair_count int := 0;
  v_reversal_count int := 0;
  v_replacement_count int := 0;
  v_distinct_parent_count int := 0;

  v_reversal_ts_id uuid;
  v_replacement_ts_id uuid;
  v_created_timesheet_ids uuid[] := array[]::uuid[];

  -- Display / normalized identity
  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;
  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  -- Stable identity hash
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  v_sqlstate text;
  v_err text;
begin
  if p_shift_id is null or p_import_id is null then
    raise exception using message='CANCELLATION_CORRECTION_SCOPE_REQUIRED',errcode='22023',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_SCOPE_REQUIRED','shift_id',p_shift_id,'import_id',p_import_id)::text;
  end if;

  if p_actor_user_id is null then
    raise exception using message='CANCELLATION_CORRECTION_ACTOR_REQUIRED',errcode='22023';
  end if;

  select hi.source_system, hi.client_id
  into v_import_source_system, v_import_client_id
  from public.hr_imports hi
  where hi.id=p_import_id
  limit 1;

  if not found then
    raise exception using message='CANCELLATION_CORRECTION_IMPORT_NOT_FOUND',errcode='P0002',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_IMPORT_NOT_FOUND','import_id',p_import_id)::text;
  end if;

  select
    ns.source_system,
    ns.candidate_id,
    ns.client_id,
    ns.contract_id,
    ns.timesheet_id,
    ns.work_date,
    ns.start_utc,
    ns.end_utc,
    ns.break_mins,
    ns.pay_minutes,
    ns.ref_num,
    ns.hr_request_id,
    ns.external_row_key,
    ns.week_ending_date,
    ns.ward
  into
    v_shift_source_system,
    v_shift_candidate_id,
    v_shift_client_id,
    v_shift_contract_id,
    v_shift_timesheet_id,
    v_shift_work_date,
    v_shift_start_utc,
    v_shift_end_utc,
    v_shift_break_mins,
    v_shift_pay_minutes,
    v_shift_ref_num,
    v_shift_hr_request_id,
    v_shift_external_row_key,
    v_shift_week_ending_date,
    v_shift_ward
  from public.nhsp_shifts ns
  where ns.id=p_shift_id
  for update;

  if not found then
    raise exception using message='CANCELLATION_CORRECTION_SHIFT_NOT_FOUND',errcode='P0002',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_SHIFT_NOT_FOUND','shift_id',p_shift_id)::text;
  end if;

  if v_shift_source_system is distinct from v_import_source_system then
    raise exception using message='CANCELLATION_CORRECTION_SOURCE_MISMATCH',errcode='P0001',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_SOURCE_MISMATCH','import_source_system',v_import_source_system,'shift_source_system',v_shift_source_system)::text;
  end if;

  if v_import_client_id is null
     or v_shift_client_id is distinct from v_import_client_id then
    raise exception using message='CANCELLATION_CORRECTION_CLIENT_MISMATCH',errcode='P0001',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_CLIENT_MISMATCH','import_client_id',v_import_client_id,'shift_client_id',v_shift_client_id)::text;
  end if;

  if v_shift_timesheet_id is null
     or v_shift_contract_id is null
     or v_shift_candidate_id is null
     or v_shift_client_id is null
     or v_shift_work_date is null then
    raise exception using message='CANCELLATION_CORRECTION_SHIFT_SCOPE_INCOMPLETE',errcode='P0001',
      detail=jsonb_build_object(
        'code','CANCELLATION_CORRECTION_SHIFT_SCOPE_INCOMPLETE',
        'shift_id',p_shift_id,
        'timesheet_id',v_shift_timesheet_id,
        'contract_id',v_shift_contract_id,
        'candidate_id',v_shift_candidate_id,
        'client_id',v_shift_client_id,
        'work_date',v_shift_work_date
      )::text;
  end if;

  select public.timesheet_correction_chain_scope_v1(
    v_shift_timesheet_id, true, 32, 100
  ) into v_chain_scope;

  if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
    raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
  end if;

  v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
  v_latest_positive_timesheet_id := coalesce(
    nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
    v_shift_timesheet_id
  );
  v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
    p_import_id,
    v_root_timesheet_id,
    v_shift_external_row_key,
    'CANCELLATION',
    'REVERSAL_ONLY'
  );
  if not exists (
    select 1 from public.nhsp_shifts canonical_cancel
    where canonical_cancel.id=p_shift_id
      and canonical_cancel.cancelled_by_import_id=p_import_id
  ) then
    raise exception 'CANCELLATION_IMPORT_EVIDENCE_REQUIRED' using errcode='P0001';
  end if;
  v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
    v_shift_timesheet_id,
    v_correction_operation_id,
    v_shift_external_row_key,
    'CANCELLATION',
    null::text,
    true,
    32
  );
  v_correction_financials_policy_envelope_fingerprint :=
    v_correction_financials_policy_envelope ->> 'envelope_fingerprint';

  v_financial_preflight := public.import_timesheet_financial_preflight_v1(
    p_timesheet_ids := array[v_shift_timesheet_id]::uuid[],
    p_action := 'IMPORT_CANCELLATION_CORRECTION_CREATE',
    p_actor_user_id := p_actor_user_id,
    p_expected_state_json := jsonb_build_object(
      'chain_fingerprints',jsonb_build_object(v_root_timesheet_id::text,v_chain_scope->>'chain_fingerprint')
    ),
    p_lock_rows := true,
    p_max_scope := 100
  );

  if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
    raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_financial_preflight::text;
  end if;

  -- Resolve the exact historical shift schedule from the effective positive
  -- timesheet first. This prevents a later-mutated source row from becoming the
  -- reversal authority.
  select member_segment.segment
  into v_anchor_segment
  from public.timesheets effective_positive
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(effective_positive.actual_schedule_json)='array'
        then effective_positive.actual_schedule_json
      else '[]'::jsonb
    end
  ) member_segment(segment)
  where effective_positive.timesheet_id=v_latest_positive_timesheet_id
    and (
      nullif(member_segment.segment->>'shift_id','')=p_shift_id::text
      or (
        nullif(v_shift_external_row_key,'') is not null
        and nullif(member_segment.segment->>'external_row_key','')=v_shift_external_row_key
      )
    )
  order by member_segment.segment->>'start_utc',member_segment.segment::text
  limit 1;

  if v_anchor_segment is null then
    select financial_segment.segment
    into v_anchor_segment
    from public.timesheets_financials historical_financial
    cross join lateral jsonb_array_elements(
      case
        when jsonb_typeof(historical_financial.invoice_breakdown_json->'segments')='array'
          then historical_financial.invoice_breakdown_json->'segments'
        else '[]'::jsonb
      end
    ) financial_segment(segment)
    where historical_financial.timesheet_id in (v_latest_positive_timesheet_id,v_root_timesheet_id)
      and (
        nullif(financial_segment.segment->>'nhsp_shift_id','')=p_shift_id::text
        or nullif(financial_segment.segment->>'shift_id','')=p_shift_id::text
        or (
          nullif(v_shift_external_row_key,'') is not null
          and nullif(financial_segment.segment->>'external_row_key','')=v_shift_external_row_key
        )
      )
    order by
      case when historical_financial.locked_by_invoice_id is not null then 0 else 1 end,
      case when historical_financial.paid_at_utc is not null then 0 else 1 end,
      historical_financial.computed_at_utc,
      historical_financial.id
    limit 1;
  end if;

  if v_anchor_segment is null
     or nullif(v_anchor_segment->>'start_utc','') is null
     or nullif(v_anchor_segment->>'end_utc','') is null then
    raise exception using message='CANCELLATION_REVERSAL_SCHEDULE_UNRESOLVED',errcode='P0001',
      detail=jsonb_build_object(
        'code','CANCELLATION_REVERSAL_SCHEDULE_UNRESOLVED',
        'shift_id',p_shift_id,
        'root_timesheet_id',v_root_timesheet_id,
        'latest_positive_timesheet_id',v_latest_positive_timesheet_id
      )::text;
  end if;

  v_reversal_schedule := jsonb_build_array(
    v_anchor_segment || jsonb_build_object(
      'date',coalesce(nullif(v_anchor_segment->>'date',''),v_shift_work_date::text),
      'ward',coalesce(nullif(v_anchor_segment->>'ward',''),nullif(v_shift_ward,'')),
      'shift_id',p_shift_id::text,
      'external_row_key',v_shift_external_row_key,
      'ref_num',coalesce(nullif(v_anchor_segment->>'ref_num',''),nullif(v_shift_ref_num,'')),
      'hr_request_id',coalesce(nullif(v_anchor_segment->>'hr_request_id',''),nullif(v_shift_hr_request_id,'')),
      'import_id',p_import_id::text
    )
  );

  -- Resolve week ending without assuming Sunday.
  select base_timesheet.week_ending_date
  into v_base_ts_week_ending
  from public.timesheets base_timesheet
  where base_timesheet.timesheet_id=v_latest_positive_timesheet_id
  limit 1;

  v_week_ending_date := coalesce(v_base_ts_week_ending,v_shift_week_ending_date);
  if v_week_ending_date is null then
    select coalesce(contract_row.week_ending_weekday_snapshot,0)
    into v_contract_week_ending_weekday_snapshot
    from public.contracts contract_row
    where contract_row.id=v_shift_contract_id
    limit 1;

    v_week_ending_date := (
      v_shift_work_date
      + (((v_contract_week_ending_weekday_snapshot-extract(dow from v_shift_work_date)::int+7)%7))::int
    )::date;
  end if;

  if v_week_ending_date is null then
    raise exception using message='CANCELLATION_CORRECTION_WEEK_UNRESOLVED',errcode='P0001';
  end if;

  -- Stable pair identity. Replays of the same import/shift/root use the same pair.
  v_fnv_s :=
    p_import_id::text||'|'||p_shift_id::text||'|'||v_root_timesheet_id::text||'|'||
    v_latest_positive_timesheet_id::text||'|'||v_correction_financials_policy_envelope_fingerprint;
  v_fnv_h := 2166136261;
  for v_fnv_i in 1..char_length(v_fnv_s) loop
    v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
    v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
  end loop;
  v_fnv_hex := lpad(lower(to_hex(v_fnv_h)),8,'0');
  v_correction_id := 'can:'||p_import_id::text||':'||p_shift_id::text||':'||v_fnv_hex;

  select contract_row.display_site,contract_row.ward_hint,contract_row.role
  into v_contract_display_site,v_contract_ward_hint,v_contract_role
  from public.contracts contract_row
  where contract_row.id=v_shift_contract_id
  limit 1;

  select client_row.name into v_client_name
  from public.clients client_row where client_row.id=v_shift_client_id limit 1;

  select candidate_row.display_name,candidate_row.tms_ref
  into v_candidate_display_name,v_candidate_tms_ref
  from public.candidates candidate_row where candidate_row.id=v_shift_candidate_id limit 1;

  insert into public.contract_weeks(contract_id,week_ending_date,additional_seq)
  values(v_shift_contract_id,v_week_ending_date,0)
  on conflict(contract_id,week_ending_date,additional_seq) do nothing;

  select base_week.id into v_base_week_id
  from public.contract_weeks base_week
  where base_week.contract_id=v_shift_contract_id
    and base_week.week_ending_date=v_week_ending_date
    and base_week.additional_seq=0
  limit 1 for update;

  if v_base_week_id is null then
    raise exception using message='CANCELLATION_CORRECTION_BASE_WEEK_UNRESOLVED',errcode='P0001';
  end if;

  foreach v_kind in array array['CANCELLATION_REVERSAL']::text[] loop
    v_member_ts_id := null;
    v_existing_ts_id := null;
    v_existing_kind := null;
    v_existing_cw_id := null;
    v_member_cw_id := null;

    v_member_schedule := case
      when v_kind='CANCELLATION_REVERSAL' then v_reversal_schedule
      else v_replacement_schedule
    end;

    v_member_hint := jsonb_build_object(
      'import_cancellation',jsonb_build_object(
        'import_id',p_import_id::text,
        'shift_id',p_shift_id::text,
        'source_system',v_shift_source_system::text,
        'external_row_key',v_shift_external_row_key,
        'ref_num',nullif(v_shift_ref_num,''),
        'hr_request_id',nullif(v_shift_hr_request_id,''),
        'correction_id',v_correction_id,
        'correction_kind',v_kind,
        'target','HISTORICAL_REVERSAL'
      ),
      'correction_financials_policy_envelope',v_correction_financials_policy_envelope,
      'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
      'root_timesheet_id',v_root_timesheet_id::text,
      'latest_positive_timesheet_id',v_latest_positive_timesheet_id::text
    );

    v_member_label_norm := regexp_replace(
      lower('weekly-cancel-'||case when v_kind='CANCELLATION_REVERSAL' then 'reversal-' else 'replacement-' end||v_correction_id),
      '[^\w\s\-@&/,:.]','','g'
    );

    v_member_booking_id := 'bk_'||substring(
      encode(extensions.digest(convert_to(
        'scope=WEEKLY|contract_id='||v_shift_contract_id::text||'|week_ending_date='||v_week_ending_date::text||
        '|correction_id='||v_correction_id||'|correction_kind='||v_kind,
        'utf8'),'sha256'),'hex') from 1 for 16
    );

    v_existing_ts_id := null;
    v_existing_kind := null;

    select existing_member.timesheet_id,existing_member.correction_kind
    into v_existing_ts_id,v_existing_kind
    from public.timesheets existing_member
    where existing_member.correction_id=v_correction_id
      and (
        existing_member.correction_kind=v_kind
        or (v_kind='CANCELLATION_REVERSAL' and existing_member.correction_kind='CANCEL_SHIFT_REVERSAL')
      )
    order by
      case when existing_member.correction_kind=v_kind then 0 else 1 end,
      existing_member.is_current desc,
      existing_member.version desc,
      existing_member.timesheet_id
    limit 1 for update;

    if v_existing_ts_id is not null then
      v_member_ts_id := v_existing_ts_id;

      if exists (
        select 1 from public.timesheets desired_member
        where desired_member.timesheet_id=v_existing_ts_id
          and (
            desired_member.actual_schedule_json is distinct from v_member_schedule
            or desired_member.parent_timesheet_id is distinct from v_latest_positive_timesheet_id
            or desired_member.contract_id is distinct from v_shift_contract_id
            or desired_member.week_ending_date is distinct from v_week_ending_date
            or desired_member.correction_kind is distinct from v_kind
            or coalesce(desired_member.candidate_hint_text->>'correction_financials_policy_envelope_fingerprint','')
                 is distinct from v_correction_financials_policy_envelope_fingerprint
          )
      ) then
        if exists (
          select 1 from public.timesheets guarded_member
          left join public.timesheets_financials guarded_financial
            on guarded_financial.timesheet_id=guarded_member.timesheet_id and guarded_financial.is_current=true
          where guarded_member.timesheet_id=v_existing_ts_id
            and (
              guarded_member.authorised_at_server is not null
              or guarded_financial.authorised_at_utc is not null
              or guarded_financial.paid_at_utc is not null
              or guarded_financial.locked_by_invoice_id is not null
              or exists(select 1 from public.invoice_lines guarded_line where guarded_line.timesheet_id=guarded_member.timesheet_id)
            )
        ) then
          raise exception using
            message=case when v_existing_kind='CANCEL_SHIFT_REVERSAL' then 'LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED' else 'CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED' end,
            errcode='P0001',
            detail=jsonb_build_object(
              'code',case when v_existing_kind='CANCEL_SHIFT_REVERSAL' then 'LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED' else 'CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED' end,
              'timesheet_id',v_existing_ts_id,
              'correction_id',v_correction_id,
              'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
            )::text;
        end if;

        update public.timesheets update_member
        set
          booking_id=v_member_booking_id,
          is_current=true,
          status='RECEIVED'::public.timesheet_status_enum,
          occupant_key_norm=lower(coalesce(v_candidate_tms_ref,v_candidate_display_name,v_shift_candidate_id::text)),
          hospital_norm=lower(coalesce(v_contract_display_site,v_client_name,v_shift_client_id::text)),
          ward_norm=lower(coalesce(v_contract_ward_hint,'contract')),
          job_title_norm=lower(coalesce(v_contract_role,'weekly')),
          shift_label_norm=v_member_label_norm,
          week_ending_date=v_week_ending_date,
          contract_id=v_shift_contract_id,
          sheet_scope='WEEKLY'::public.timesheet_scope_enum,
          submission_mode='MANUAL'::public.submission_mode_enum,
          line_type='HOURS'::public.timesheet_line_type_enum,
          manual_pdf_r2_key=null,
          actual_schedule_json=v_member_schedule,
          additional_units_week='{}'::jsonb,
          additional_units_per_day='{}'::jsonb,
          day_references_json=null,
          authorised_at_server=null,
          qr_payload_json=v_member_hint,
          candidate_hint_text=v_member_hint,
          is_adjustment=true,
          parent_timesheet_id=v_latest_positive_timesheet_id,
          correction_id=v_correction_id,
          correction_kind=v_kind,
          adjustment_origin='IMPORT_CANCELLATION',
          updated_at=v_now
        where update_member.timesheet_id=v_existing_ts_id;

        v_pair_changed := true;
      end if;

      select existing_week.id into v_existing_cw_id
      from public.contract_weeks existing_week
      where existing_week.timesheet_id=v_existing_ts_id
        and existing_week.contract_id=v_shift_contract_id
        and existing_week.week_ending_date=v_week_ending_date
      order by existing_week.additional_seq,existing_week.id
      limit 1 for update;

      if v_existing_cw_id is null then
        if exists (
          select 1 from public.timesheets guarded_member
          left join public.timesheets_financials guarded_financial
            on guarded_financial.timesheet_id=guarded_member.timesheet_id and guarded_financial.is_current=true
          where guarded_member.timesheet_id=v_existing_ts_id
            and (
              guarded_member.authorised_at_server is not null
              or guarded_financial.authorised_at_utc is not null
              or guarded_financial.paid_at_utc is not null
              or guarded_financial.locked_by_invoice_id is not null
              or exists(select 1 from public.invoice_lines guarded_line where guarded_line.timesheet_id=guarded_member.timesheet_id)
            )
        ) then
          raise exception using message='CORRECTION_CONTRACT_WEEK_REPAIR_REQUIRED',errcode='P0001',
            detail=jsonb_build_object('code','CORRECTION_CONTRACT_WEEK_REPAIR_REQUIRED','timesheet_id',v_existing_ts_id)::text;
        end if;
      end if;
    else
      v_member_ts_id := null;
    end if;

    if v_existing_cw_id is null then
      perform 1 from public.contract_weeks week_lock
      where week_lock.contract_id=v_shift_contract_id
        and week_lock.week_ending_date=v_week_ending_date
      order by week_lock.id for update;

      v_try := 0;
      loop
        v_try := v_try+1;
        if v_try>10 then
          raise exception using message='CANCELLATION_CORRECTION_SEQUENCE_ALLOCATION_FAILED',errcode='P0001';
        end if;

        select coalesce(max(existing_seq.additional_seq),0)+1
        into v_next_additional_seq
        from public.contract_weeks existing_seq
        where existing_seq.contract_id=v_shift_contract_id
          and existing_seq.week_ending_date=v_week_ending_date;

        begin
          insert into public.contract_weeks(
            contract_id,week_ending_date,additional_seq,is_adjustment,
            submission_mode_snapshot,status,created_at,updated_at,timesheet_id
          )
          values(
            v_shift_contract_id,v_week_ending_date,v_next_additional_seq,true,
            'MANUAL'::public.submission_mode_enum,'SUBMITTED'::public.contract_week_status_enum,
            v_now,v_now,v_member_ts_id
          )
          returning id into v_member_cw_id;
          exit;
        exception when unique_violation then
          v_member_cw_id := null;
        end;
      end loop;
    else
      v_member_cw_id := v_existing_cw_id;
    end if;

    if v_member_ts_id is null then
      insert into public.timesheets(
        booking_id,version,is_current,status,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
        shift_label_norm,week_ending_date,contract_id,sheet_scope,submission_mode,line_type,
        manual_pdf_r2_key,actual_schedule_json,additional_units_week,additional_units_per_day,
        day_references_json,authorised_at_server,qr_payload_json,created_at,updated_at,is_adjustment,
        parent_timesheet_id,candidate_hint_text,correction_id,correction_kind,adjustment_origin
      )
      values(
        v_member_booking_id,1,true,'RECEIVED'::public.timesheet_status_enum,
        lower(coalesce(v_candidate_tms_ref,v_candidate_display_name,v_shift_candidate_id::text)),
        lower(coalesce(v_contract_display_site,v_client_name,v_shift_client_id::text)),
        lower(coalesce(v_contract_ward_hint,'contract')),
        lower(coalesce(v_contract_role,'weekly')),
        v_member_label_norm,v_week_ending_date,v_shift_contract_id,
        'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
        'HOURS'::public.timesheet_line_type_enum,null,v_member_schedule,'{}'::jsonb,'{}'::jsonb,
        null,null,v_member_hint,v_now,v_now,true,v_latest_positive_timesheet_id,v_member_hint,
        v_correction_id,v_kind,'IMPORT_CANCELLATION'
      )
      returning timesheet_id into v_member_ts_id;

      update public.contract_weeks link_week
      set timesheet_id=v_member_ts_id,
          status='SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot='MANUAL'::public.submission_mode_enum,
          is_adjustment=true,
          updated_at=v_now
      where link_week.id=v_member_cw_id;

      v_pair_changed := true;
    elsif v_existing_cw_id is null then
      update public.contract_weeks link_existing_week
      set timesheet_id=v_member_ts_id,
          status='SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot='MANUAL'::public.submission_mode_enum,
          is_adjustment=true,
          updated_at=v_now
      where link_existing_week.id=v_member_cw_id;
    end if;

    if v_kind='CANCELLATION_REVERSAL' then
      v_reversal_ts_id := v_member_ts_id;
    else
      v_replacement_ts_id := v_member_ts_id;
    end if;
  end loop;

  select count(*)::int,count(distinct unit_member.parent_timesheet_id)::int
  into v_pair_count,v_distinct_parent_count
  from public.timesheets unit_member
  where unit_member.correction_id=v_correction_id
    and unit_member.is_current=true
    and unit_member.correction_kind='CANCELLATION_REVERSAL';

  if v_reversal_ts_id is null or v_pair_count<>1 or v_distinct_parent_count<>1 then
    raise exception using message='CANCELLATION_REVERSAL_UNIT_INCOMPLETE',errcode='P0001',
      detail=jsonb_build_object('correction_id',v_correction_id,'member_count',v_pair_count)::text;
  end if;
  if exists (
    select 1 from public.timesheets legacy_replacement
    where legacy_replacement.correction_id=v_correction_id
      and legacy_replacement.correction_kind in ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
  ) then
    raise exception 'LEGACY_ZERO_HOUR_REPLACEMENT_REQUIRES_RECONCILIATION'
      using errcode='P0001',detail=jsonb_build_object('correction_id',v_correction_id)::text;
  end if;
  v_replacement_ts_id:=null;
  v_created_timesheet_ids:=array[v_reversal_ts_id]::uuid[];

  if v_pair_changed then
    perform public.enqueue_ts_financials_priority(
      v_created_timesheet_ids,
      'CONTEXT_CHANGED'::public.ts_fin_reason_enum
    );
  end if;

  perform public._imp_debug_audit(
    p_actor_user_id,
    'WEEKLY_CANCEL_CORRECTION_CREATE_DEBUG',
    jsonb_build_object(
      'import_id',p_import_id::text,
      'shift_id',p_shift_id::text,
      'correction_id',v_correction_id,
      'correction_kind','CANCELLATION_REVERSAL_ONLY',
      'reversal_timesheet_id',v_reversal_ts_id::text,
      'replacement_timesheet_id',v_replacement_ts_id::text,
      'root_timesheet_id',v_root_timesheet_id::text,
      'latest_positive_timesheet_id',v_latest_positive_timesheet_id::text,
      'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
      'week_ending_date',v_week_ending_date::text,
      'pair_changed',v_pair_changed
    ),
    'nhsp_shifts',p_shift_id::text,null,null,null,null
  );

  return jsonb_build_object(
    'import_id',p_import_id,
    'shift_id',p_shift_id,
    'correction_id',v_correction_id,
    'correction_kind','CANCELLATION_REVERSAL_ONLY',
    'reversal_timesheet_id',v_reversal_ts_id,
    'replacement_timesheet_id',v_replacement_ts_id,
    'created_timesheet_ids',to_jsonb(v_created_timesheet_ids),
    'root_timesheet_id',v_root_timesheet_id,
    'latest_positive_timesheet_id',v_latest_positive_timesheet_id,
    'correction_financials_policy_envelope',v_correction_financials_policy_envelope,
    'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
    'chain_fingerprint',v_chain_scope->>'chain_fingerprint',
    'preflight_fingerprint',v_financial_preflight->>'preflight_fingerprint',
    'pair_changed',v_pair_changed
  );

exception when others then
  get stacked diagnostics v_sqlstate=returned_sqlstate,v_err=message_text;
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'WEEKLY_CANCEL_CORRECTION_CREATE_ERROR',
      jsonb_build_object(
        'import_id',case when p_import_id is null then null else p_import_id::text end,
        'shift_id',case when p_shift_id is null then null else p_shift_id::text end,
        'sqlstate',v_sqlstate,
        'error',v_err
      ),
      'nhsp_shifts',case when p_shift_id is null then null else p_shift_id::text end,
      null,null,null,null
    );
  exception when others then
    null;
  end;
  raise;
end;
$function$;
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.weekly_import_create_cancellation_corrections(uuid, uuid, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.weekly_import_create_cancellation_corrections(uuid, uuid, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.weekly_import_create_cancellation_corrections(uuid, uuid, uuid) TO postgres, authenticated, service_role;
