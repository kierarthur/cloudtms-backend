-- Central two-leg correction policy implementation.
--
-- The owner-only builder is called only by import_apply_operation_claim_v1.
-- It proves the import, source shift, root timesheet and client eligibility,
-- freezes both independent leg policies, and returns a fingerprinted envelope.
-- The public resolver never trusts a route label or caller-supplied shape: it
-- reads the exact frozen unit from a durable import operation.

create or replace function public._ctms_correction_financials_policy_build_v2(
  p_timesheet_id uuid,
  p_import_id uuid,
  p_source_row_key text,
  p_correction_action text,
  p_correction_shape text,
  p_operation_id uuid,
  p_request_hash text,
  p_operation_at_utc timestamptz,
  p_lock_rows boolean default false,
  p_max_depth integer default 32
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_action text := upper(btrim(coalesce(p_correction_action, '')));
  v_shape text := upper(btrim(coalesce(p_correction_shape, '')));
  v_source_row_key text := nullif(btrim(coalesce(p_source_row_key, '')), '');
  v_operation_date date;
  v_import public.hr_imports%rowtype;
  v_shift public.nhsp_shifts%rowtype;
  v_root_timesheet_id uuid;
  v_shift_root_timesheet_id uuid;
  v_root_contract_id uuid;
  v_root_fin public.timesheets_financials%rowtype;
  v_client public.clients%rowtype;
  v_settings public.client_settings%rowtype;
  v_defaults public.settings_defaults%rowtype;
  v_contract public.contracts%rowtype;

  v_reversal_setting public.correction_financials_date_basis_enum;
  v_replacement_setting public.correction_financials_date_basis_enum;
  v_reversal_source text;
  v_replacement_source text;

  v_root_paid_date date;
  v_pay_batch_date_count integer := 0;
  v_pay_batch_date date;
  v_pay_batch_refs jsonb := '[]'::jsonb;
  v_root_pay_policy_date date;
  v_root_erni numeric;
  v_root_apply_erni text;
  v_root_pay_vat numeric;

  v_invoice_line_count integer := 0;
  v_invoice_rate_count integer := 0;
  v_invoice_chargeability_count integer := 0;
  v_invoice_stream_count integer := 0;
  v_root_invoice_applied_rate numeric;
  v_root_invoice_source_rate numeric;
  v_root_invoice_chargeable boolean;
  v_root_invoice_policy_date date;
  v_invoice_stream text;
  v_invoice_refs jsonb := '[]'::jsonb;
  v_has_invoice_artifact_without_line boolean := false;
  v_cancellation_identity text;
  v_cancellation_identity_present boolean := false;
  v_cancellation_client_in_scope boolean := false;

  v_leg_name text;
  v_setting public.correction_financials_date_basis_enum;
  v_setting_source text;
  v_window public.settings_finance_windows%rowtype;
  v_window_count integer;
  v_operation_window public.settings_finance_windows%rowtype;
  v_operation_window_count integer;
  v_policy_date date;
  v_erni numeric;
  v_apply_erni text;
  v_pay_source_vat numeric;
  v_pay_applied_vat numeric;
  v_pay_vat_applicable boolean;
  v_pay_vat_chargeable boolean;
  v_pay_method text;
  v_invoice_source_vat numeric;
  v_invoice_applied_vat numeric;
  v_invoice_chargeable boolean;
  v_invoice_policy_date date;
  v_pay_evidence_class text;
  v_invoice_evidence_class text;
  v_pay_evidence_refs jsonb;
  v_invoice_evidence_refs jsonb;
  v_pay_component_evidence jsonb;
  v_erni_from_window boolean;
  v_apply_erni_from_window boolean;
  v_pay_vat_from_window boolean;
  v_tsfin_payload jsonb;
  v_tsfin_policy jsonb;
  v_invoice_payload jsonb;
  v_invoice_policy jsonb;
  v_leg_payload jsonb;
  v_leg jsonb;
  v_reversal jsonb;
  v_replacement jsonb;
  v_expected_roles jsonb;
  v_envelope_payload jsonb;
  v_envelope jsonb;
begin
  if p_timesheet_id is null or p_import_id is null or p_operation_id is null then
    raise exception 'CORRECTION_POLICY_OPERATION_SCOPE_REQUIRED'
      using errcode = '22023';
  end if;
  if p_operation_at_utc is null then
    raise exception 'CORRECTION_POLICY_OPERATION_TIMESTAMP_REQUIRED'
      using errcode = '22023';
  end if;
  if nullif(btrim(coalesce(p_request_hash, '')), '') is null then
    raise exception 'CORRECTION_POLICY_REQUEST_HASH_REQUIRED'
      using errcode = '22023';
  end if;
  if v_source_row_key is null then
    raise exception 'CORRECTION_POLICY_SOURCE_ROW_KEY_REQUIRED'
      using errcode = '22023';
  end if;
  if v_action not in ('CHANGED_HOURS','CANCELLATION') then
    raise exception 'CORRECTION_POLICY_ACTION_INVALID'
      using errcode = '22023',
            detail = jsonb_build_object('correction_action', v_action)::text;
  end if;
  if v_shape not in ('REVERSAL_REPLACEMENT','REVERSAL_ONLY') then
    raise exception 'CORRECTION_POLICY_SHAPE_INVALID'
      using errcode = '22023',
            detail = jsonb_build_object('correction_shape', v_shape)::text;
  end if;
  if v_action = 'CANCELLATION' and v_shape <> 'REVERSAL_ONLY' then
    raise exception 'CORRECTION_POLICY_CANCELLATION_MUST_BE_REVERSAL_ONLY'
      using errcode = '22023';
  end if;
  if v_action = 'CHANGED_HOURS' and v_shape <> 'REVERSAL_REPLACEMENT' then
    raise exception 'CORRECTION_POLICY_CHANGED_HOURS_MUST_REVERSE_AND_REPLACE'
      using errcode = '22023';
  end if;
  if p_max_depth < 1 or p_max_depth > 32 then
    raise exception 'CORRECTION_POLICY_MAX_DEPTH_INVALID'
      using errcode = '22023';
  end if;

  v_operation_date := (p_operation_at_utc at time zone 'Europe/London')::date;

  select hi.* into v_import
  from public.hr_imports hi
  where hi.id = p_import_id;
  if not found then
    raise exception 'CORRECTION_POLICY_IMPORT_NOT_FOUND' using errcode = 'P0002';
  end if;

  if p_lock_rows then
    select ns.* into v_shift
    from public.nhsp_shifts ns
    where ns.external_row_key = v_source_row_key
    for update;
  else
    select ns.* into v_shift
    from public.nhsp_shifts ns
    where ns.external_row_key = v_source_row_key;
  end if;
  if not found then
    raise exception 'CORRECTION_POLICY_SOURCE_SHIFT_NOT_FOUND'
      using errcode = 'P0002',
            detail = jsonb_build_object('source_row_key', v_source_row_key)::text;
  end if;

  if v_shift.source_system is distinct from v_import.source_system then
    raise exception 'CORRECTION_POLICY_SOURCE_SYSTEM_MISMATCH'
      using errcode = 'P0001';
  end if;
  if v_import.client_id is not null
     and v_shift.client_id is distinct from v_import.client_id then
    raise exception 'CORRECTION_POLICY_IMPORT_CLIENT_MISMATCH'
      using errcode = 'P0001';
  end if;
  if v_shift.timesheet_id is null then
    raise exception 'CORRECTION_POLICY_SOURCE_SHIFT_NOT_LINKED'
      using errcode = 'P0001';
  end if;
  if v_action = 'CHANGED_HOURS'
     and v_shift.latest_import_id is distinct from p_import_id then
    raise exception 'CORRECTION_POLICY_CHANGED_HOURS_IMPORT_EVIDENCE_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'source_shift_id', v_shift.id,
              'expected_import_id', p_import_id,
              'actual_latest_import_id', v_shift.latest_import_id
            )::text;
  end if;
  if v_action = 'CANCELLATION' then
    -- Before source commit, prove that the canonical source identity is absent
    -- from this exact import. On replay, require the durable cancellation mark
    -- to belong to this same import. This gives preview/claim a fail-closed
    -- cancellation proof without requiring a mutation before the claim exists.
    if v_shift.cancelled_at_utc is not null then
      if v_shift.cancelled_by_import_id is distinct from p_import_id then
        raise exception 'CORRECTION_POLICY_CANCELLATION_IMPORT_EVIDENCE_MISMATCH'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'expected_import_id', p_import_id,
                  'actual_cancelled_by_import_id', v_shift.cancelled_by_import_id
                )::text;
      end if;
    elsif upper(v_shift.source_system::text) = 'HEALTHROSTER' then
      v_cancellation_identity := nullif(btrim(v_shift.hr_request_id), '');
      if v_cancellation_identity is null then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_IDENTITY_MISSING'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'HR_REQUEST_ID'
                )::text;
      end if;
      select exists (
        select 1
        from public.hr_rows source_row
        where source_row.import_id = p_import_id
          and lower(regexp_replace(btrim(coalesce(
                nullif(source_row.hr_request_id, ''),
                nullif(source_row.payload_json ->> 'request_id', '')
              )), '\s+', ' ', 'g'))
              = lower(regexp_replace(v_cancellation_identity, '\s+', ' ', 'g'))
      ) into v_cancellation_identity_present;
      if v_cancellation_identity_present then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_STILL_PRESENT'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'HR_REQUEST_ID',
                  'import_id', p_import_id
                )::text;
      end if;
    elsif upper(v_shift.source_system::text) = 'NHSP' then
      v_cancellation_identity := nullif(btrim(v_shift.ref_num), '');
      if v_cancellation_identity is null then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_IDENTITY_MISSING'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'REF_NUM'
                )::text;
      end if;
      select exists (
        select 1
        from public.hr_rows source_row
        where source_row.import_id = p_import_id
          and lower(regexp_replace(btrim(coalesce(
                nullif(source_row.payload_json ->> 'ref_num', ''),
                nullif(source_row.payload_json ->> 'Reference', ''),
                nullif(source_row.hr_request_id, '')
              )), '\s+', ' ', 'g'))
              = lower(regexp_replace(v_cancellation_identity, '\s+', ' ', 'g'))
      ) into v_cancellation_identity_present;
      if v_cancellation_identity_present then
        raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_STILL_PRESENT'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'identity_kind', 'REF_NUM',
                  'import_id', p_import_id
                )::text;
      end if;
      select exists (
        select 1
        from public.weekly_import_phase2(
          p_import_id := p_import_id,
          p_system_type := 'NHSP'
        ) import_scope
        where import_scope.client_id = v_shift.client_id
          and import_scope.candidate_id is not null
          and import_scope.work_date is not null
      ) into v_cancellation_client_in_scope;
      if not v_cancellation_client_in_scope then
        raise exception 'CORRECTION_POLICY_CANCELLATION_CLIENT_OUT_OF_SCOPE'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'source_shift_id', v_shift.id,
                  'client_id', v_shift.client_id,
                  'import_id', p_import_id
                )::text;
      end if;
    else
      raise exception 'CORRECTION_POLICY_CANCELLATION_SOURCE_SYSTEM_INVALID'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'source_shift_id', v_shift.id,
                'source_system', v_shift.source_system
              )::text;
    end if;
  end if;
  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts
    where ts.timesheet_id = p_timesheet_id
    union all
    select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
           a.path || parent.timesheet_id,
           parent.timesheet_id = any(a.path)
    from ancestors a
    join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
    where a.parent_timesheet_id is not null
      and not a.cycle
      and a.depth < p_max_depth
  )
  select a.timesheet_id into v_root_timesheet_id
  from ancestors a
  order by a.depth desc
  limit 1;

  if v_root_timesheet_id is null then
    raise exception 'CORRECTION_POLICY_TIMESHEET_NOT_FOUND' using errcode = 'P0002';
  end if;
  if exists (
    with recursive ancestors as (
      select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
             array[ts.timesheet_id]::uuid[] path, false cycle
      from public.timesheets ts where ts.timesheet_id = p_timesheet_id
      union all
      select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
             a.path || parent.timesheet_id,
             parent.timesheet_id = any(a.path)
      from ancestors a
      join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
      where a.parent_timesheet_id is not null and not a.cycle and a.depth < p_max_depth
    ) select 1 from ancestors where cycle
  ) then
    raise exception 'CORRECTION_POLICY_PARENT_CYCLE' using errcode = 'P0001';
  end if;

  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id = v_shift.timesheet_id
    union all
    select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
           a.path || parent.timesheet_id,
           parent.timesheet_id = any(a.path)
    from ancestors a
    join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
    where a.parent_timesheet_id is not null and not a.cycle and a.depth < p_max_depth
  )
  select a.timesheet_id into v_shift_root_timesheet_id
  from ancestors a
  order by a.depth desc
  limit 1;

  if v_shift_root_timesheet_id is distinct from v_root_timesheet_id then
    raise exception 'CORRECTION_POLICY_SOURCE_SHIFT_ROOT_MISMATCH'
      using errcode = 'P0001';
  end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts
    where ts.timesheet_id in (p_timesheet_id, v_shift.timesheet_id, v_root_timesheet_id)
    order by ts.timesheet_id
    for update;
  end if;

  select ts.contract_id into v_root_contract_id
  from public.timesheets ts where ts.timesheet_id = v_root_timesheet_id;
  select c.* into v_contract from public.contracts c where c.id = v_root_contract_id;

  select tf.* into v_root_fin
  from public.timesheets_financials tf
  where tf.timesheet_id = v_root_timesheet_id
  order by (tf.paid_at_utc is not null) desc,
           (tf.authorised_at_utc is not null) desc,
           tf.is_current desc,
           tf.computed_at_utc desc,
           tf.id
  limit 1;

  select c.* into v_client
  from public.clients c
  where c.id = coalesce(v_shift.client_id, v_root_fin.client_id, v_contract.client_id);
  if not found then
    raise exception 'CORRECTION_POLICY_CLIENT_UNRESOLVED' using errcode = 'P0001';
  end if;

  select d.* into v_defaults
  from public.settings_defaults d
  where d.id = 1
  for share;
  if not found then
    raise exception 'CORRECTION_POLICY_GLOBAL_DEFAULTS_MISSING' using errcode = 'P0001';
  end if;

  select cs.* into v_settings
  from public.client_settings cs
  where cs.client_id = v_client.id
    and (cs.effective_from is null or cs.effective_from <= v_operation_date)
  order by cs.effective_from desc nulls last, cs.updated_at desc, cs.id desc
  limit 1;
  if not found then
    raise exception 'CORRECTION_POLICY_CLIENT_SETTINGS_MISSING' using errcode = 'P0001';
  end if;

  if not (
    coalesce(v_settings.is_nhsp, false)
    or (
      coalesce(v_settings.requires_hr, false)
      and coalesce(v_settings.no_timesheet_required, false)
    )
  ) then
    raise exception 'CORRECTION_POLICY_CLIENT_NOT_IMPORT_AUTHORITATIVE'
      using errcode = '22023',
            detail = jsonb_build_object('client_id', v_client.id)::text;
  end if;

  v_reversal_setting := coalesce(
    v_settings.reversal_complete_financials_date,
    v_defaults.reversal_complete_financials_date
  );
  v_replacement_setting := coalesce(
    v_settings.reversal_replacement_financials_date,
    v_defaults.reversal_replacement_financials_date
  );
  v_reversal_source := case
    when v_settings.reversal_complete_financials_date is null then 'GLOBAL'
    else 'CLIENT' end;
  v_replacement_source := case
    when v_settings.reversal_replacement_financials_date is null then 'GLOBAL'
    else 'CLIENT' end;

  if v_root_fin.id is not null then
    v_root_erni := case
      when coalesce(v_root_fin.policy_snapshot_json ->> 'erni_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        then (v_root_fin.policy_snapshot_json ->> 'erni_pct')::numeric
      else null end;
    v_root_apply_erni := nullif(btrim(v_root_fin.policy_snapshot_json ->> 'apply_erni_to'), '');
    v_root_pay_vat := coalesce(
      v_root_fin.pay_vat_rate_pct_snapshot,
      case when coalesce(v_root_fin.policy_snapshot_json ->> 'pay_vat_rate_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        then (v_root_fin.policy_snapshot_json ->> 'pay_vat_rate_pct')::numeric else null end,
      case when coalesce(v_root_fin.policy_snapshot_json ->> 'vat_rate_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        then (v_root_fin.policy_snapshot_json ->> 'vat_rate_pct')::numeric else null end
    );
    v_root_paid_date := case when v_root_fin.paid_at_utc is null then null
      else (v_root_fin.paid_at_utc at time zone 'Europe/London')::date end;
    v_root_pay_policy_date := coalesce(
      case when v_root_fin.authorised_at_utc is null then null
        else (v_root_fin.authorised_at_utc at time zone 'Europe/London')::date end,
      (select (ts.authorised_at_server at time zone 'Europe/London')::date
       from public.timesheets ts where ts.timesheet_id = v_root_timesheet_id),
      v_root_paid_date
    );
  end if;

  select count(distinct pb.authoritative_payment_date) filter (where pb.authoritative_payment_date is not null),
         min(pb.authoritative_payment_date),
         coalesce(jsonb_agg(distinct jsonb_build_object(
           'pay_batch_id', pb.id,
           'pay_batch_item_id', pbi.id,
           'authoritative_payment_date', pb.authoritative_payment_date,
           'status', pb.status,
           'source_snapshot_run_id', pb.source_snapshot_run_id,
           'frozen_component_snapshot_fingerprint', case
             when pbi.frozen_component_snapshot_json is null then null
             else encode(digest(convert_to(pbi.frozen_component_snapshot_json::text, 'UTF8'), 'sha256'), 'hex')
           end,
           'frozen_source_basis_fingerprint', case
             when pbi.frozen_source_basis_json is null then null
             else encode(digest(convert_to(pbi.frozen_source_basis_json::text, 'UTF8'), 'sha256'), 'hex')
           end,
           'frozen_resolution_result_fingerprint', case
             when pbi.frozen_resolution_result_json is null then null
             else encode(digest(convert_to(pbi.frozen_resolution_result_json::text, 'UTF8'), 'sha256'), 'hex')
           end
         )) filter (where pbi.id is not null), '[]'::jsonb)
  into v_pay_batch_date_count, v_pay_batch_date, v_pay_batch_refs
  from public.pay_batch_items pbi
  join public.pay_batch_candidates pbc on pbc.id = pbi.pay_batch_candidate_id
  join public.pay_batches pb on pb.id = pbc.pay_batch_id
  where pbi.timesheet_id = v_root_timesheet_id
    and coalesce(pbi.is_voided, false) = false;

  if v_pay_batch_date_count > 1 then
    raise exception 'CORRECTION_POLICY_ROOT_PAYMENT_DATE_AMBIGUOUS'
      using errcode = 'P0001';
  end if;
  v_root_paid_date := coalesce(v_root_paid_date, v_pay_batch_date);

  select count(*)::integer,
         count(distinct il.vat_rate_pct)::integer,
         min(il.vat_rate_pct),
         count(distinct case
           when lower(coalesce(i.header_snapshot_json ->> 'vat_chargeable', '')) in ('true','false')
             then lower(i.header_snapshot_json ->> 'vat_chargeable')
           else null end)::integer,
         min(case when lower(coalesce(i.header_snapshot_json ->> 'vat_chargeable', '')) = 'true' then 1
                  when lower(coalesce(i.header_snapshot_json ->> 'vat_chargeable', '')) = 'false' then 0 end)::integer = 1,
         min(case when coalesce(i.header_snapshot_json ->> 'applied_vat_rate_pct', '') ~ '^-?[0-9]+([.][0-9]+)?$'
           then (i.header_snapshot_json ->> 'applied_vat_rate_pct')::numeric end),
         min((coalesce(i.issued_at_utc, i.created_at) at time zone 'Europe/London')::date),
         count(distinct case when lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', 'false')) = 'true'
           then 'SELF_BILL' else 'NORMAL' end)::integer,
         min(case when lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', 'false')) = 'true'
           then 'SELF_BILL' else 'NORMAL' end),
         coalesce(jsonb_agg(distinct jsonb_build_object(
           'invoice_id', i.id,
           'invoice_line_id', il.id,
           'invoice_status', i.status,
           'invoice_stream', case when lower(coalesce(i.header_snapshot_json #>> '{meta,self_bill}', 'false')) = 'true'
             then 'SELF_BILL' else 'NORMAL' end,
           'line_vat_rate_pct', il.vat_rate_pct,
           'line_vat_amount', il.vat_amount
         )), '[]'::jsonb)
  into v_invoice_line_count, v_invoice_rate_count, v_root_invoice_applied_rate,
       v_invoice_chargeability_count, v_root_invoice_chargeable,
       v_root_invoice_source_rate, v_root_invoice_policy_date,
       v_invoice_stream_count, v_invoice_stream, v_invoice_refs
  from public.invoice_lines il
  join public.invoices i on i.id = il.invoice_id
  where il.timesheet_id = v_root_timesheet_id;

  if v_invoice_rate_count > 1 then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_VAT_AMBIGUOUS'
      using errcode = 'P0001';
  end if;
  if v_invoice_stream_count > 1 then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_STREAM_AMBIGUOUS'
      using errcode = 'P0001';
  end if;
  if v_invoice_line_count > 0 and v_invoice_chargeability_count <> 1 then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_CHARGEABILITY_UNPROVEN'
      using errcode = 'P0001';
  end if;
  if exists (
    select 1
    from public.invoice_lines il
    join public.invoices i on i.id=il.invoice_id
    where il.timesheet_id=v_root_timesheet_id
      and (
        case
          when coalesce(i.header_snapshot_json->>'applied_vat_rate_pct','') ~ '^-?[0-9]+([.][0-9]+)?$'
            then (i.header_snapshot_json->>'applied_vat_rate_pct')::numeric is distinct from il.vat_rate_pct
          else true
        end
        or (
          lower(i.header_snapshot_json->>'vat_chargeable')='false'
          and il.vat_rate_pct<>0
        )
      )
  ) then
    raise exception 'CORRECTION_POLICY_ROOT_INVOICE_HEADER_LINE_CONFLICT'
      using errcode='P0001';
  end if;
  v_has_invoice_artifact_without_line := v_invoice_line_count = 0 and (
    v_shift.invoice_id is not null
    or v_root_fin.locked_by_invoice_id is not null
    or exists (
      select 1
      from jsonb_array_elements(
        case
          when jsonb_typeof(v_root_fin.invoice_breakdown_json) = 'array'
            then v_root_fin.invoice_breakdown_json
          when jsonb_typeof(v_root_fin.invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_root_fin.invoice_breakdown_json -> 'segments') = 'array'
            then v_root_fin.invoice_breakdown_json -> 'segments'
          else '[]'::jsonb
        end
      ) segment_row
      where nullif(btrim(coalesce(
              segment_row ->> 'invoice_locked_invoice_id',
              segment_row ->> 'invoice_id',
              ''
            )), '') is not null
    )
  );
  v_invoice_stream := coalesce(
    v_invoice_stream,
    case when coalesce(v_contract.self_bill, false) then 'SELF_BILL' else 'NORMAL' end
  );

  v_pay_method := upper(btrim(coalesce(v_root_fin.pay_method, (
    select candidate.pay_method from public.candidates candidate
    where candidate.id = coalesce(v_root_fin.candidate_id, v_shift.candidate_id)
  ), '')));

  for v_leg_name in select unnest(array['reversal','replacement']) loop
    if v_leg_name = 'replacement' and v_shape = 'REVERSAL_ONLY' then
      v_tsfin_payload := jsonb_build_object(
        'applicable', false,
        'evidence_class', 'NOT_APPLICABLE',
        'materialisation_stage', 'TSFIN'
      );
      v_tsfin_policy := v_tsfin_payload || jsonb_build_object(
        'tsfin_policy_fingerprint', encode(extensions.digest(convert_to(v_tsfin_payload::text,'UTF8'),'sha256'::text),'hex')
      );
      v_invoice_payload := jsonb_build_object(
        'applicable', false,
        'evidence_class', 'NOT_APPLICABLE',
        'invoice_stream', v_invoice_stream,
        'materialisation_stage', 'INVOICE_GENERATION',
        'final_invoice_vat_materialised', false
      );
      v_invoice_policy := v_invoice_payload || jsonb_build_object(
        'invoice_policy_fingerprint', encode(extensions.digest(convert_to(v_invoice_payload::text,'UTF8'),'sha256'::text),'hex')
      );
      v_leg_payload := jsonb_build_object(
        'leg', 'REPLACEMENT',
        'applicable', false,
        'setting', 'NOT_APPLICABLE',
        'setting_source', 'NOT_APPLICABLE',
        'tsfin_policy', v_tsfin_policy,
        'invoice_policy', v_invoice_policy
      );
      v_replacement := v_leg_payload || jsonb_build_object(
        'leg_fingerprint', encode(extensions.digest(convert_to(v_leg_payload::text,'UTF8'),'sha256'::text),'hex')
      );
      continue;
    end if;

    v_setting := case when v_leg_name = 'reversal' then v_reversal_setting else v_replacement_setting end;
    v_setting_source := case when v_leg_name = 'reversal' then v_reversal_source else v_replacement_source end;
    v_window := null;
    v_window_count := 0;
    v_operation_window := null;
    v_operation_window_count := 0;
    v_policy_date := null;
    v_erni := null;
    v_apply_erni := null;
    v_pay_source_vat := null;
    v_pay_applied_vat := null;
    v_pay_vat_applicable := null;
    v_pay_vat_chargeable := null;
    v_invoice_source_vat := null;
    v_invoice_applied_vat := null;
    v_invoice_chargeable := null;
    v_invoice_policy_date := null;
    v_pay_evidence_class := null;
    v_invoice_evidence_class := null;
    v_pay_evidence_refs := '{}'::jsonb;
    v_invoice_evidence_refs := '{}'::jsonb;
    v_pay_component_evidence := '{}'::jsonb;
    v_erni_from_window := false;
    v_apply_erni_from_window := false;
    v_pay_vat_from_window := false;

    if v_setting = 'PAID_DATE'::public.correction_financials_date_basis_enum then
      v_policy_date := coalesce(v_root_pay_policy_date, v_root_paid_date);
      v_erni := v_root_erni;
      v_apply_erni := v_root_apply_erni;
      v_pay_source_vat := v_root_pay_vat;
      v_pay_applied_vat := v_root_pay_vat;
      v_pay_evidence_class := 'TSFIN_SNAPSHOT';
      v_pay_evidence_refs := jsonb_build_object(
        'root_tsfin_id', v_root_fin.id,
        'root_timesheet_id', v_root_timesheet_id,
        'pay_batch_snapshots', v_pay_batch_refs
      );

      if v_erni is null or v_apply_erni is null or v_pay_source_vat is null then
        if v_root_paid_date is null then
          raise exception 'CORRECTION_POLICY_PAID_DATE_TSFIN_FALLBACK_UNAVAILABLE'
            using errcode = 'P0001';
        end if;
        select count(*) into v_window_count
        from public.settings_finance_windows fw
        where fw.date_from <= v_root_paid_date
          and (fw.date_to is null or fw.date_to >= v_root_paid_date);
        if v_window_count <> 1 then
          raise exception 'CORRECTION_POLICY_PAID_DATE_WINDOW_NOT_EXACT'
            using errcode = 'P0001',
                  detail = jsonb_build_object('paid_date', v_root_paid_date, 'match_count', v_window_count)::text;
        end if;
        select fw.* into v_window
        from public.settings_finance_windows fw
        where fw.date_from <= v_root_paid_date
          and (fw.date_to is null or fw.date_to >= v_root_paid_date)
        order by fw.date_from desc, fw.id
        limit 1;
        v_policy_date := coalesce(v_policy_date, v_root_paid_date);
        v_erni_from_window := v_erni is null;
        v_apply_erni_from_window := v_apply_erni is null;
        v_pay_vat_from_window := v_pay_source_vat is null;
        v_erni := coalesce(v_erni, v_window.erni_pct);
        v_apply_erni := coalesce(v_apply_erni, v_window.apply_erni_to, 'PAYE_ONLY');
        v_pay_source_vat := coalesce(v_pay_source_vat, v_window.vat_rate_pct);
        v_pay_applied_vat := coalesce(v_pay_applied_vat, v_window.vat_rate_pct);
        v_pay_evidence_class := 'FINANCE_WINDOW_EXACT_MATCH';
        v_pay_evidence_refs := v_pay_evidence_refs || jsonb_build_object(
          'legacy_gap_component_only', true,
          'finance_window_id', v_window.id,
          'finance_window_updated_at', v_window.updated_at,
          'finance_window_date_from', v_window.date_from,
          'finance_window_date_to', v_window.date_to
        );
      end if;
      v_apply_erni := upper(btrim(v_apply_erni));
      v_pay_component_evidence := jsonb_build_object(
        'erni_pct', jsonb_build_object(
          'evidence_class', case when v_erni_from_window
            then 'FINANCE_WINDOW_EXACT_MATCH' else 'TSFIN_SNAPSHOT' end,
          'value', v_erni,
          'root_tsfin_id', case when v_erni_from_window then null else v_root_fin.id end,
          'finance_window_id', case when v_erni_from_window then v_window.id else null end
        ),
        'apply_erni_to', jsonb_build_object(
          'evidence_class', case when v_apply_erni_from_window
            then 'FINANCE_WINDOW_EXACT_MATCH' else 'TSFIN_SNAPSHOT' end,
          'value', v_apply_erni,
          'root_tsfin_id', case when v_apply_erni_from_window then null else v_root_fin.id end,
          'finance_window_id', case when v_apply_erni_from_window then v_window.id else null end
        ),
        'pay_vat_rate_pct', jsonb_build_object(
          'evidence_class', case when v_pay_vat_from_window
            then 'FINANCE_WINDOW_EXACT_MATCH' else 'TSFIN_SNAPSHOT' end,
          'value', v_pay_source_vat,
          'root_tsfin_id', case when v_pay_vat_from_window then null else v_root_fin.id end,
          'finance_window_id', case when v_pay_vat_from_window then v_window.id else null end
        ),
        'pay_vat_applicability', jsonb_build_object(
          'evidence_class', 'TSFIN_SNAPSHOT',
          'pay_method_snapshot', nullif(v_pay_method, ''),
          'rule', 'UMBRELLA_PAY_METHOD_ONLY'
        )
      );
      v_pay_evidence_refs := v_pay_evidence_refs || jsonb_build_object(
        'component_evidence', v_pay_component_evidence
      );

      if v_invoice_line_count > 0 then
        v_invoice_policy_date := v_root_invoice_policy_date;
        v_invoice_chargeable := v_root_invoice_chargeable;
        v_invoice_source_vat := case when v_root_invoice_chargeable then
          coalesce(v_root_invoice_source_rate,v_root_invoice_applied_rate)
          else null end;
        v_invoice_applied_vat := v_root_invoice_applied_rate;
        v_invoice_evidence_class := 'INVOICE_LINE';
        v_invoice_evidence_refs := jsonb_build_object(
          'root_timesheet_id', v_root_timesheet_id,
          'invoice_rows', v_invoice_refs
        );

        if v_root_invoice_chargeable is false then
          if v_root_invoice_policy_date is null then
            raise exception 'CORRECTION_POLICY_ROOT_INVOICE_SOURCE_RATE_UNPROVEN'
              using errcode = 'P0001';
          end if;
          select count(*) into v_window_count
          from public.settings_finance_windows fw
          where fw.date_from <= v_root_invoice_policy_date
            and (fw.date_to is null or fw.date_to >= v_root_invoice_policy_date);
          if v_window_count <> 1 then
            raise exception 'CORRECTION_POLICY_ROOT_INVOICE_WINDOW_NOT_EXACT'
              using errcode = 'P0001';
          end if;
          select fw.* into v_window
          from public.settings_finance_windows fw
          where fw.date_from <= v_root_invoice_policy_date
            and (fw.date_to is null or fw.date_to >= v_root_invoice_policy_date)
          order by fw.date_from desc, fw.id limit 1;
          v_invoice_source_vat := v_window.vat_rate_pct;
          v_invoice_evidence_refs := v_invoice_evidence_refs || jsonb_build_object(
            'non_chargeable_source_rate_evidence_class', 'FINANCE_WINDOW_EXACT_MATCH',
            'source_rate_finance_window_id', v_window.id,
            'source_rate_finance_window_updated_at', v_window.updated_at
          );
        end if;
      elsif v_has_invoice_artifact_without_line then
        -- An invoice identifier/lock without its authoritative line/header is
        -- inconsistent evidence. Never guess historical invoice treatment.
        raise exception 'CORRECTION_POLICY_INVOICE_EVIDENCE_INCOMPLETE'
          using errcode = 'P0001',
                detail = jsonb_build_object(
                  'root_timesheet_id', v_root_timesheet_id,
                  'root_paid_date', v_root_paid_date,
                  'invoice_line_count', v_invoice_line_count,
                  'invoice_artifact_without_line', true,
                  'pay_vat_used_as_invoice_evidence', false,
                  'current_client_state_used_as_historical_invoice_evidence', false
                )::text;
      else
        -- A genuinely never-invoiced root has no historical invoice VAT to
        -- reconstruct. Keep the pay/TSFIN leg on PAID_DATE, but freeze only
        -- the invoice sub-policy from the correction operation window.
        select count(*) into v_operation_window_count
        from public.settings_finance_windows fw
        where fw.date_from <= v_operation_date
          and (fw.date_to is null or fw.date_to >= v_operation_date);
        if v_operation_window_count <> 1 then
          raise exception 'CORRECTION_POLICY_OPERATION_DATE_WINDOW_NOT_EXACT'
            using errcode = 'P0001',
                  detail = jsonb_build_object(
                    'operation_date', v_operation_date,
                    'match_count', v_operation_window_count,
                    'invoice_fallback_reason', 'NO_HISTORICAL_INVOICE'
                  )::text;
        end if;
        select fw.* into v_operation_window
        from public.settings_finance_windows fw
        where fw.date_from <= v_operation_date
          and (fw.date_to is null or fw.date_to >= v_operation_date)
        order by fw.date_from desc, fw.id
        limit 1;
        v_invoice_policy_date := v_operation_date;
        v_invoice_chargeable := v_client.vat_chargeable;
        v_invoice_source_vat := coalesce(
          v_settings.vat_rate_pct,
          v_operation_window.vat_rate_pct
        );
        v_invoice_applied_vat := case when v_invoice_chargeable
          then v_invoice_source_vat else 0 end;
        v_invoice_evidence_class := 'CURRENT_OPERATION_WINDOW';
        v_invoice_evidence_refs := jsonb_build_object(
          'fallback_reason', 'NO_HISTORICAL_INVOICE',
          'requested_leg_setting', 'PAID_DATE',
          'effective_invoice_basis', 'NOW',
          'finance_window_id', v_operation_window.id,
          'finance_window_updated_at', v_operation_window.updated_at,
          'client_id', v_client.id,
          'client_updated_at', v_client.updated_at,
          'client_settings_id', v_settings.id,
          'client_settings_updated_at', v_settings.updated_at,
          'root_timesheet_id', v_root_timesheet_id,
          'root_invoice_line_count', 0,
          'root_invoice_artifact_detected', false,
          'pay_vat_used_as_invoice_evidence', false
        );
      end if;
    else
      v_policy_date := v_operation_date;
      select count(*) into v_window_count
      from public.settings_finance_windows fw
      where fw.date_from <= v_operation_date
        and (fw.date_to is null or fw.date_to >= v_operation_date);
      if v_window_count <> 1 then
        raise exception 'CORRECTION_POLICY_OPERATION_DATE_WINDOW_NOT_EXACT'
          using errcode = 'P0001',
                detail = jsonb_build_object('operation_date', v_operation_date, 'match_count', v_window_count)::text;
      end if;
      select fw.* into v_window
      from public.settings_finance_windows fw
      where fw.date_from <= v_operation_date
        and (fw.date_to is null or fw.date_to >= v_operation_date)
      order by fw.date_from desc, fw.id limit 1;

      v_erni := coalesce(v_settings.erni_pct, v_window.erni_pct);
      v_apply_erni := upper(btrim(coalesce(
        nullif(v_settings.apply_erni_to, ''),
        nullif(v_window.apply_erni_to, ''),
        'PAYE_ONLY'
      )));
      v_pay_source_vat := coalesce(v_settings.vat_rate_pct, v_window.vat_rate_pct);
      v_pay_vat_applicable := v_pay_method like 'UMBRELLA%';
      v_pay_vat_chargeable := coalesce((
        select umbrella.vat_chargeable
        from public.candidates candidate
        join public.umbrellas umbrella on umbrella.id = candidate.umbrella_id
        where candidate.id = coalesce(v_root_fin.candidate_id, v_shift.candidate_id)
      ), false);
      v_pay_applied_vat := case when v_pay_vat_applicable and v_pay_vat_chargeable
        then v_pay_source_vat else 0 end;
      v_pay_evidence_class := 'CURRENT_OPERATION_WINDOW';
      v_pay_evidence_refs := jsonb_build_object(
        'finance_window_id', v_window.id,
        'finance_window_updated_at', v_window.updated_at,
        'client_settings_id', v_settings.id,
        'client_settings_updated_at', v_settings.updated_at,
        'component_evidence', jsonb_build_object(
          'erni_pct', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'value', v_erni,
            'source', case when v_settings.erni_pct is null
              then 'FINANCE_WINDOW' else 'CLIENT_SETTINGS' end
          ),
          'apply_erni_to', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'value', v_apply_erni,
            'source', case when nullif(v_settings.apply_erni_to, '') is null
              then case when nullif(v_window.apply_erni_to, '') is null
                then 'SYSTEM_FALLBACK' else 'FINANCE_WINDOW' end
              else 'CLIENT_SETTINGS' end
          ),
          'pay_vat_rate_pct', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'value', v_pay_source_vat,
            'source', case when v_settings.vat_rate_pct is null
              then 'FINANCE_WINDOW' else 'CLIENT_SETTINGS' end
          ),
          'pay_vat_applicability', jsonb_build_object(
            'evidence_class', 'CURRENT_OPERATION_WINDOW',
            'pay_method_snapshot', nullif(v_pay_method, ''),
            'rule', 'UMBRELLA_PAY_METHOD_ONLY'
          )
        ),
        'erni_economic_eligibility', 'PAYE_ONLY'
      );

      v_invoice_policy_date := v_operation_date;
      v_invoice_chargeable := v_client.vat_chargeable;
      v_invoice_source_vat := coalesce(v_settings.vat_rate_pct, v_window.vat_rate_pct);
      v_invoice_applied_vat := case when v_invoice_chargeable then v_invoice_source_vat else 0 end;
      v_invoice_evidence_class := 'CURRENT_OPERATION_WINDOW';
      v_invoice_evidence_refs := jsonb_build_object(
        'finance_window_id', v_window.id,
        'finance_window_updated_at', v_window.updated_at,
        'client_id', v_client.id,
        'client_updated_at', v_client.updated_at,
        'client_settings_id', v_settings.id,
        'client_settings_updated_at', v_settings.updated_at,
        'pay_vat_used_as_invoice_evidence', false
      );
    end if;

    if v_apply_erni not in ('ALL', 'PAYE_ONLY') then
      raise exception 'CORRECTION_POLICY_APPLY_ERNI_TO_INVALID'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'leg', upper(v_leg_name),
                'apply_erni_to', v_apply_erni,
                'allowed_values', jsonb_build_array('ALL', 'PAYE_ONLY')
              )::text;
    end if;

    v_pay_vat_applicable := coalesce(v_pay_vat_applicable, v_pay_method like 'UMBRELLA%');
    v_pay_vat_chargeable := coalesce(
      v_pay_vat_chargeable,
      v_pay_vat_applicable and coalesce(v_pay_applied_vat, 0) > 0
    );

    if v_erni is null or v_apply_erni is null or v_pay_source_vat is null
       or v_pay_applied_vat is null or v_invoice_source_vat is null
       or v_invoice_applied_vat is null or v_invoice_chargeable is null then
      raise exception 'CORRECTION_POLICY_COMPONENT_UNRESOLVED'
        using errcode = 'P0001',
              detail = jsonb_build_object('leg', upper(v_leg_name), 'setting', v_setting)::text;
    end if;
    if v_pay_evidence_class not in (
      'TSFIN_SNAPSHOT','PAY_BATCH_SNAPSHOT','FINANCE_WINDOW_EXACT_MATCH','CURRENT_OPERATION_WINDOW'
    ) or v_invoice_evidence_class not in (
      'INVOICE_LINE','FINANCE_WINDOW_EXACT_MATCH','CURRENT_OPERATION_WINDOW'
    ) then
      raise exception 'CORRECTION_POLICY_EVIDENCE_CLASS_INVALID' using errcode = 'P0001';
    end if;

    v_tsfin_payload := jsonb_build_object(
      'applicable', true,
      'requested_basis', v_setting,
      'effective_basis', v_setting,
      'pay_policy_date', v_policy_date,
      'erni_pct', v_erni,
      'apply_erni_to', v_apply_erni,
      'pay_method_snapshot', nullif(v_pay_method, ''),
      'pay_vat_applicable', v_pay_vat_applicable,
      'pay_vat_chargeable', v_pay_vat_chargeable,
      'source_pay_vat_rate_pct', v_pay_source_vat,
      'applied_pay_vat_rate_pct', v_pay_applied_vat,
      'evidence_class', v_pay_evidence_class,
      'evidence_refs', v_pay_evidence_refs,
      'materialisation_stage', 'TSFIN'
    );
    v_tsfin_policy := v_tsfin_payload || jsonb_build_object(
      'tsfin_policy_fingerprint', encode(extensions.digest(convert_to(v_tsfin_payload::text,'UTF8'),'sha256'::text),'hex')
    );

    v_invoice_payload := jsonb_build_object(
      'applicable', true,
      'requested_basis', v_setting,
      'effective_basis', case
        when v_invoice_evidence_refs ->> 'fallback_reason' = 'NO_HISTORICAL_INVOICE'
          then 'NOW'
        else v_setting::text
      end,
      'invoice_policy_date', coalesce(v_invoice_policy_date, v_policy_date),
      'invoice_stream', v_invoice_stream,
      'invoice_vat_chargeable', v_invoice_chargeable,
      'source_vat_rate_pct', v_invoice_source_vat,
      'applied_vat_rate_pct', v_invoice_applied_vat,
      'evidence_class', v_invoice_evidence_class,
      'evidence_refs', v_invoice_evidence_refs,
      'materialisation_stage', 'INVOICE_GENERATION',
      'final_invoice_vat_materialised', false
    );
    v_invoice_policy := v_invoice_payload || jsonb_build_object(
      'invoice_policy_fingerprint', encode(extensions.digest(convert_to(v_invoice_payload::text,'UTF8'),'sha256'::text),'hex')
    );

    v_leg_payload := jsonb_build_object(
      'leg', upper(v_leg_name),
      'applicable', true,
      'setting', v_setting,
      'setting_source', v_setting_source,
      'tsfin_policy', v_tsfin_policy,
      'invoice_policy', v_invoice_policy
    );
    v_leg := v_leg_payload || jsonb_build_object(
      'leg_fingerprint', encode(extensions.digest(convert_to(v_leg_payload::text,'UTF8'),'sha256'::text),'hex')
    );
    if v_leg_name = 'reversal' then v_reversal := v_leg; else v_replacement := v_leg; end if;

    v_pay_vat_applicable := null;
    v_pay_vat_chargeable := null;
  end loop;

  v_expected_roles := case when v_shape = 'REVERSAL_ONLY'
    then jsonb_build_array('REVERSAL')
    else jsonb_build_array('REVERSAL','REPLACEMENT') end;

  v_envelope_payload := jsonb_build_object(
    'policy_schema_version', 'IMPORT_CORRECTION_FINANCIALS_POLICY_V2',
    'route_family', 'IMPORT_AUTHORITATIVE',
    'classification', jsonb_build_object(
      'canonical', true,
      'classification_source', 'IMPORT_SOURCE_SHIFT',
      'import_id', p_import_id,
      'source_system', v_import.source_system,
      'source_shift_id', v_shift.id,
      'source_row_key', v_shift.external_row_key,
      'source_shift_timesheet_id', v_shift.timesheet_id,
      'client_eligible_at_operation', true
    ),
    'operation', jsonb_build_object(
      'operation_id', p_operation_id,
      'request_hash', p_request_hash,
      'operation_at_utc', p_operation_at_utc,
      'operation_date_london', v_operation_date,
      'correction_action', v_action
    ),
    'root_timesheet_id', v_root_timesheet_id,
    'correction_chain_id', v_import.source_system::text || ':' || v_shift.id::text,
    'correction_shape', v_shape,
    'expected_member_roles', v_expected_roles,
    'expected_member_count', jsonb_array_length(v_expected_roles),
    'invoice_stream', v_invoice_stream,
    'settings_snapshot', jsonb_build_object(
      'global_settings_id', v_defaults.id,
      'global_settings_updated_at', v_defaults.updated_at,
      'client_settings_id', v_settings.id,
      'client_settings_effective_from', v_settings.effective_from,
      'client_settings_updated_at', v_settings.updated_at,
      'client_id', v_client.id,
      'client_updated_at', v_client.updated_at,
      'reversal_setting', v_reversal_setting,
      'reversal_setting_source', v_reversal_source,
      'replacement_setting', v_replacement_setting,
      'replacement_setting_source', v_replacement_source
    ),
    'root_financial_evidence', jsonb_build_object(
      'root_tsfin_id', v_root_fin.id,
      'root_tsfin_timesheet_id', v_root_fin.timesheet_id,
      'root_tsfin_authorised_at_utc', v_root_fin.authorised_at_utc,
      'root_tsfin_paid_at_utc', v_root_fin.paid_at_utc,
      'root_tsfin_policy_snapshot_fingerprint', case when v_root_fin.id is null then null else
        encode(extensions.digest(convert_to(v_root_fin.policy_snapshot_json::text,'UTF8'),'sha256'::text),'hex') end,
      'pay_batch_snapshots', v_pay_batch_refs,
      'invoice_rows', v_invoice_refs
    ),
    'reversal', v_reversal,
    'replacement', v_replacement
  );
  v_envelope := v_envelope_payload || jsonb_build_object(
    'envelope_fingerprint', encode(extensions.digest(convert_to(v_envelope_payload::text,'UTF8'),'sha256'::text),'hex')
  );
  return v_envelope;
end;
$function$;

create or replace function public._ctms_import_correction_operation_find_v1(
  p_import_id uuid,
  p_root_timesheet_id uuid,
  p_source_row_key text,
  p_correction_action text,
  p_correction_shape text
)
returns uuid
language plpgsql
stable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_ids uuid[];
begin
  select coalesce(array_agg(distinct o.id order by o.id), array[]::uuid[])
  into v_ids
  from public.import_apply_operations o
  cross join lateral jsonb_array_elements(
    case when jsonb_typeof(o.response_json #> '{correction_operation_contract,correction_units}') = 'array'
      then o.response_json #> '{correction_operation_contract,correction_units}' else '[]'::jsonb end
  ) unit
  where o.import_id = p_import_id
    and o.state not in ('BLOCKED','FAILED_BEFORE_COMMIT')
    and unit ->> 'root_timesheet_id' = p_root_timesheet_id::text
    and unit ->> 'source_row_key' = p_source_row_key
    and upper(unit ->> 'correction_action') = upper(p_correction_action)
    and upper(unit ->> 'correction_shape') = upper(p_correction_shape);

  if cardinality(v_ids) <> 1 then
    raise exception 'CORRECTION_POLICY_OPERATION_NOT_UNIQUE'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'import_id', p_import_id,
              'root_timesheet_id', p_root_timesheet_id,
              'source_row_key', p_source_row_key,
              'correction_action', upper(p_correction_action),
              'correction_shape', upper(p_correction_shape),
              'matching_operation_count', cardinality(v_ids)
            )::text;
  end if;
  return v_ids[1];
end;
$function$;

create or replace function public.correction_financials_policy_resolve_v1(
  p_timesheet_id uuid,
  p_operation_id uuid,
  p_source_row_key text,
  p_correction_action text,
  p_expected_envelope_fingerprint text default null,
  p_lock_rows boolean default false,
  p_max_depth integer default 32
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_operation public.import_apply_operations%rowtype;
  v_contract jsonb;
  v_contract_fingerprint text;
  v_root_timesheet_id uuid;
  v_units jsonb;
  v_unit jsonb;
  v_unit_count integer;
  v_envelope jsonb;
  v_envelope_fingerprint text;
  v_source_shift public.nhsp_shifts%rowtype;
begin
  if p_timesheet_id is null or p_operation_id is null
     or nullif(btrim(coalesce(p_source_row_key, '')), '') is null then
    raise exception 'CORRECTION_POLICY_RESOLVER_SCOPE_REQUIRED'
      using errcode = '22023';
  end if;
  if upper(btrim(coalesce(p_correction_action, ''))) not in ('CHANGED_HOURS','CANCELLATION') then
    raise exception 'CORRECTION_POLICY_ACTION_INVALID' using errcode = '22023';
  end if;
  if p_max_depth < 1 or p_max_depth > 32 then
    raise exception 'CORRECTION_POLICY_MAX_DEPTH_INVALID' using errcode = '22023';
  end if;

  if p_lock_rows then
    select o.* into v_operation
    from public.import_apply_operations o
    where o.id = p_operation_id
    for update;
  else
    select o.* into v_operation
    from public.import_apply_operations o
    where o.id = p_operation_id;
  end if;
  if not found then
    raise exception 'CORRECTION_POLICY_OPERATION_NOT_FOUND' using errcode = 'P0002';
  end if;
  if v_operation.state in ('BLOCKED','FAILED_BEFORE_COMMIT') then
    raise exception 'CORRECTION_POLICY_OPERATION_NOT_ACTIVE'
      using errcode = 'P0001', detail = jsonb_build_object('state', v_operation.state)::text;
  end if;

  v_contract := v_operation.response_json -> 'correction_operation_contract';
  if jsonb_typeof(v_contract) <> 'object'
     or v_contract ->> 'schema_version' <> 'IMPORT_CORRECTION_OPERATION_V2'
     or v_contract ->> 'route_family' <> 'IMPORT_AUTHORITATIVE'
     or v_contract ->> 'operation_id' is distinct from p_operation_id::text
     or v_contract ->> 'import_id' is distinct from v_operation.import_id::text
     or v_contract ->> 'request_hash' is distinct from v_operation.request_hash then
    raise exception 'CORRECTION_POLICY_OPERATION_CONTRACT_INVALID'
      using errcode = 'P0001';
  end if;

  v_contract_fingerprint := encode(
    extensions.digest(
      convert_to((v_contract - 'operation_contract_fingerprint')::text, 'UTF8'),
      'sha256'::text
    ), 'hex'
  );
  if nullif(v_contract ->> 'operation_contract_fingerprint', '') is distinct from v_contract_fingerprint then
    raise exception 'CORRECTION_POLICY_OPERATION_CONTRACT_FINGERPRINT_INVALID'
      using errcode = 'P0001';
  end if;

  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id = p_timesheet_id
    union all
    select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
           a.path || parent.timesheet_id,
           parent.timesheet_id = any(a.path)
    from ancestors a
    join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
    where a.parent_timesheet_id is not null and not a.cycle and a.depth < p_max_depth
  )
  select a.timesheet_id into v_root_timesheet_id
  from ancestors a order by a.depth desc limit 1;
  if v_root_timesheet_id is null then
    raise exception 'CORRECTION_POLICY_TIMESHEET_NOT_FOUND' using errcode = 'P0002';
  end if;

  v_units := v_contract -> 'correction_units';
  select count(*)::integer, min(unit::text)::jsonb
  into v_unit_count, v_unit
  from jsonb_array_elements(case when jsonb_typeof(v_units)='array' then v_units else '[]'::jsonb end) unit
  where unit ->> 'root_timesheet_id' = v_root_timesheet_id::text
    and unit ->> 'source_row_key' = p_source_row_key
    and upper(unit ->> 'correction_action') = upper(p_correction_action);

  if v_unit_count <> 1 then
    raise exception 'CORRECTION_POLICY_OPERATION_UNIT_NOT_UNIQUE'
      using errcode = 'P0001',
            detail = jsonb_build_object('matching_unit_count', v_unit_count)::text;
  end if;

  v_envelope := v_unit -> 'policy_envelope';
  if jsonb_typeof(v_envelope) <> 'object'
     or v_envelope #>> '{operation,operation_id}' is distinct from p_operation_id::text
     or v_envelope #>> '{classification,source_row_key}' is distinct from p_source_row_key
     or v_envelope ->> 'root_timesheet_id' is distinct from v_root_timesheet_id::text
     or v_envelope ->> 'correction_shape' is distinct from v_unit ->> 'correction_shape'
     or v_envelope -> 'expected_member_roles' is distinct from v_unit -> 'expected_member_roles'
     or v_envelope ->> 'expected_member_count' is distinct from v_unit ->> 'expected_member_count' then
    raise exception 'CORRECTION_POLICY_OPERATION_ENVELOPE_SCOPE_MISMATCH'
      using errcode = 'P0001';
  end if;

  v_envelope_fingerprint := encode(
    extensions.digest(convert_to((v_envelope - 'envelope_fingerprint')::text,'UTF8'),'sha256'::text),
    'hex'
  );
  if nullif(v_envelope ->> 'envelope_fingerprint', '') is distinct from v_envelope_fingerprint
     or v_unit ->> 'policy_envelope_fingerprint' is distinct from v_envelope_fingerprint then
    raise exception 'CORRECTION_POLICY_ENVELOPE_FINGERPRINT_INVALID'
      using errcode = 'P0001';
  end if;

  if p_lock_rows then
    select ns.* into v_source_shift
    from public.nhsp_shifts ns
    where ns.external_row_key=p_source_row_key
    for update;
  else
    select ns.* into v_source_shift
    from public.nhsp_shifts ns
    where ns.external_row_key=p_source_row_key;
  end if;
  if not found
     or v_source_shift.id::text is distinct from v_envelope#>>'{classification,source_shift_id}'
     or v_source_shift.source_system is distinct from v_operation.source_system then
    raise exception 'CORRECTION_POLICY_LIVE_SOURCE_IDENTITY_MISMATCH'
      using errcode='P0001';
  end if;
  if upper(p_correction_action)='CHANGED_HOURS'
     and v_source_shift.latest_import_id is distinct from v_operation.import_id then
    raise exception 'CORRECTION_POLICY_CHANGED_HOURS_ACTION_NOT_CANONICAL'
      using errcode='P0001';
  end if;
  if upper(p_correction_action)='CANCELLATION'
     and v_source_shift.cancelled_by_import_id is distinct from v_operation.import_id then
    raise exception 'CORRECTION_POLICY_CANCELLATION_ACTION_NOT_CANONICAL'
      using errcode='P0001';
  end if;

  if nullif(btrim(coalesce(p_expected_envelope_fingerprint, '')), '') is not null
     and p_expected_envelope_fingerprint is distinct from v_envelope_fingerprint then
    raise exception 'CORRECTION_POLICY_EXPECTED_FINGERPRINT_MISMATCH'
      using errcode = '40001',
            detail = jsonb_build_object(
              'expected', p_expected_envelope_fingerprint,
              'actual', v_envelope_fingerprint
            )::text;
  end if;
  return v_envelope;
end;
$function$;

comment on function public.correction_financials_policy_resolve_v1(
  uuid, uuid, text, text, text, boolean, integer
) is
  'Returns the frozen two-leg TSFIN/invoice policy for one canonically claimed import correction operation. It does not accept route or shape assertions from the caller.';

revoke all on function public._ctms_correction_financials_policy_build_v2(
  uuid,uuid,text,text,text,uuid,text,timestamptz,boolean,integer
) from public,anon,authenticated,service_role;
revoke all on function public._ctms_import_correction_operation_find_v1(
  uuid,uuid,text,text,text
) from public,anon,authenticated,service_role;
revoke all on function public.correction_financials_policy_resolve_v1(
  uuid,uuid,text,text,text,boolean,integer
) from public,anon,authenticated;
grant execute on function public.correction_financials_policy_resolve_v1(
  uuid,uuid,text,text,text,boolean,integer
) to service_role;
