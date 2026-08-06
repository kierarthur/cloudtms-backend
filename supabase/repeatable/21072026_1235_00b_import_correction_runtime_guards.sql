-- Internal guards used by the direct replacement RPCs.
-- They only activate for import-authoritative correction rows and remain
-- owner-only so they do not create a second public API surface.

create or replace function public._ctms_assert_pay_batch_mutable_v1(
  p_pay_batch_id uuid,
  p_context text default 'IMPORT_CORRECTION'
)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_batch public.pay_batches%rowtype;
  v_transfer_count integer;
  v_export_item_count integer;
  v_reasons text[] := array[]::text[];
  v_status text;
begin
  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_ID_REQUIRED' using errcode = '22023';
  end if;

  -- Preserve the installed behaviour for every ordinary batch. This stricter
  -- guard exists only when the batch actually contains an import-authoritative
  -- correction timesheet.
  if not exists (
    select 1
    from public.pay_batch_candidates pbc
    join public.timesheets_financials tf
      on tf.candidate_id=pbc.candidate_id
    where pbc.pay_batch_id=p_pay_batch_id
      and coalesce((public._ctms_import_correction_classify_v1(tf.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;

  select * into v_batch
  from public.pay_batches
  where id = p_pay_batch_id
  for update;
  if not found then
    raise exception 'PAY_BATCH_NOT_FOUND' using errcode = 'P0001';
  end if;

  v_status := upper(btrim(coalesce(v_batch.status::text, '')));
  if v_status in (
    'AWAITING_AUTHORISATION', 'AWAITING_AUTHORIZATION', 'AUTHORISED',
    'AUTHORIZED', 'APPROVED', 'SUBMITTED', 'EXPORTED', 'EXECUTING',
    'COMPLETED', 'PAID', 'SETTLED', 'CANCELLED', 'CANCELED'
  ) then
    v_reasons := array_append(v_reasons, 'status:' || v_status);
  end if;
  if v_batch.monzo_confirmed_at_utc is not null then v_reasons := array_append(v_reasons, 'monzo_confirmed'); end if;
  if v_batch.executing_started_at_utc is not null then v_reasons := array_append(v_reasons, 'execution_started'); end if;
  if v_batch.completed_at_utc is not null then v_reasons := array_append(v_reasons, 'completed'); end if;
  if v_batch.cancelled_at_utc is not null then v_reasons := array_append(v_reasons, 'cancelled'); end if;
  if coalesce(upper(btrim(v_batch.execution_commit_state::text)), 'NOT_SUBMITTED') <> 'NOT_SUBMITTED'
     or v_batch.execution_commit_ref is not null
     or v_batch.execution_committed_at_utc is not null
     or v_batch.bank_csv_export_json is not null
     or v_batch.execution_intent_json is not null
     or v_batch.settlement_confirmation_json is not null then
    v_reasons := array_append(v_reasons, 'frozen_or_external_evidence');
  end if;

  select count(*)::integer into v_transfer_count
  from public.pay_bank_transfers t where t.pay_batch_id = p_pay_batch_id;
  if v_transfer_count > 0 then v_reasons := array_append(v_reasons, 'bank_transfers'); end if;

  select count(*)::integer into v_export_item_count
  from public.pay_batch_items i
  where i.pay_bank_transfer_id is not null
    and i.pay_batch_candidate_id in (
      select c.id from public.pay_batch_candidates c where c.pay_batch_id = p_pay_batch_id
    );
  if v_export_item_count > 0 then v_reasons := array_append(v_reasons, 'exported_items'); end if;

  if cardinality(v_reasons) > 0 then
    raise exception 'PAY_BATCH_MUTATION_LOCKED_BY_POLICY_X'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'context', p_context, 'pay_batch_id', p_pay_batch_id,
              'status', v_batch.status, 'reasons', to_jsonb(v_reasons)
            )::text;
  end if;
end;
$function$;

create or replace function public._ctms_candidate_correction_residuals_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_exclude_pay_batch_id uuid default null,
  p_context text default 'IMPORT_CORRECTION'
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_result jsonb := '[]'::jsonb;
  v_seen_roots uuid[] := array[]::uuid[];
  v_chain jsonb;
  v_root uuid;
  v_residual jsonb;
  v_target_pay_method text;
  r record;
begin
  if p_candidate_id is null then return v_result; end if;

  -- The correction chain is one economic unit.  Its target channel is the
  -- candidate's current pay method, not whichever historical correction leg
  -- happens to sort first by timesheet UUID.  A PAYE/UMBRELLA change must
  -- therefore enter the existing chain-level resolution workflow once.
  select case
    when upper(btrim(coalesce(candidate_row.pay_method, ''))) = 'PAYE'
      then 'PAYE'
    when upper(btrim(coalesce(candidate_row.pay_method, ''))) = 'UMBRELLA'
      then 'UMBRELLA'
    else null::text
  end
  into v_target_pay_method
  from public.candidates as candidate_row
  where candidate_row.id = p_candidate_id;

  if v_target_pay_method is null then
    raise exception 'CORRECTION_CHAIN_TARGET_PAY_METHOD_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'candidate_id', p_candidate_id::text,
              'context', p_context
            )::text;
  end if;

  for r in
    select distinct tf.timesheet_id
    from public.timesheets_financials tf
    where tf.is_current = true and tf.candidate_id = p_candidate_id
      and coalesce((public._ctms_import_correction_classify_v1(tf.timesheet_id)
        ->> 'is_import_authoritative_correction')::boolean, false)
    order by tf.timesheet_id
    limit 100
  loop
    v_chain := public.timesheet_correction_chain_scope_v1(r.timesheet_id, false, 32, 100);
    if coalesce((v_chain ->> 'valid')::boolean, false) is not true then
      raise exception 'CORRECTION_CHAIN_INVALID_FOR_BANKING_PAY'
        using errcode = 'P0001', detail = v_chain::text;
    end if;
    v_root := nullif(v_chain ->> 'root_timesheet_id', '')::uuid;
    if v_root = any(v_seen_roots) then continue; end if;
    v_seen_roots := array_append(v_seen_roots, v_root);
    v_residual := public.pay_correction_chain_residual_v1(
      r.timesheet_id, p_candidate_id, v_target_pay_method,
      p_session_id, p_exclude_pay_batch_id, 100
    );
    v_result := v_result || jsonb_build_array(v_residual);
  end loop;
  return v_result;
end;
$function$;

create or replace function public._ctms_rewrite_source_build_correction_negative_components_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_scope_timesheet_ids uuid[]
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_rewritten_chain_count integer := 0;
  v_inserted_component_count integer := 0;
begin
  if p_session_id is null
     or p_candidate_id is null
     or to_regclass('pg_temp._tmp_pay_wb_sync_negative_components') is null
     or to_regclass('pg_temp._tmp_pay_wb_sync_rotation_scope') is null then
    return jsonb_build_object(
      'ok', true,
      'rewritten_chain_count', 0,
      'inserted_component_count', 0
    );
  end if;

  for v_residual in
    select value
    from jsonb_array_elements(public._ctms_candidate_correction_residuals_v1(
      p_session_id,
      p_candidate_id,
      null::uuid,
      'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD'
    ))
  loop
    select coalesce(array_agg(value::uuid order by value), array[]::uuid[])
    into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;

    if coalesce(array_length(v_member_ids, 1), 0) = 0
       or (
         coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
         and not (v_member_ids && p_scope_timesheet_ids)
    ) then
      continue;
    end if;

    -- A pay-method mismatch remains visible through the Workbench resolution
    -- surface, but its raw member rows must not remain in the authoritative
    -- negative-component set.  They would otherwise manufacture recovery
    -- authority from an unresolved target amount and fail the metadata gate.
    if coalesce((v_residual->>'draftable')::boolean, false) is not true then
      delete from pg_temp._tmp_pay_wb_sync_negative_components negative_component
      where negative_component.timesheet_id = any(v_member_ids);
      continue;
    end if;

    v_root_id := nullif(v_residual->>'root_timesheet_id', '')::uuid;
    if v_root_id is null then
      raise exception 'CORRECTION_CHAIN_SOURCE_BUILD_ROOT_REQUIRED'
        using errcode='P0001', detail=v_residual::text;
    end if;
    if coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
       and not (v_root_id = any(p_scope_timesheet_ids)) then
      raise exception 'CORRECTION_CHAIN_SOURCE_BUILD_SCOPE_MUST_INCLUDE_ROOT'
        using errcode='P0001',
              detail=jsonb_build_object(
                'root_timesheet_id', v_root_id,
                'member_timesheet_ids', to_jsonb(v_member_ids)
              )::text;
    end if;

    delete from pg_temp._tmp_pay_wb_sync_negative_components negative_component
    where negative_component.timesheet_id = any(v_member_ids);

    if not exists (
      select 1
      from pg_temp._tmp_pay_wb_sync_rotation_scope rotation_scope
      where rotation_scope.requested_timesheet_id = v_root_id
    ) then
      insert into pg_temp._tmp_pay_wb_sync_rotation_scope (
        requested_timesheet_id,
        canonical_timesheet_id,
        family_timesheet_id
      )
      values (v_root_id, v_root_id, v_root_id);
    end if;

    for v_component in
      select value
      from jsonb_array_elements(v_residual->'components')
      where round(
        coalesce(nullif(value->>'target_outstanding_ex_vat', '')::numeric, 0),
        2
      ) < 0
    loop
      insert into pg_temp._tmp_pay_wb_sync_negative_components (
        timesheet_id,
        key_type,
        key_value,
        truth_ex_vat,
        baseline_ex_vat,
        reserved_ex_vat,
        outstanding_ex_vat,
        baseline_signature
      )
      values (
        v_root_id,
        upper(btrim(v_component->>'component_key_type')),
        btrim(v_component->>'component_key_value'),
        round(coalesce(nullif(v_component->>'truth_ex_vat', '')::numeric, 0), 2),
        round(coalesce(nullif(v_component->>'baseline_ex_vat', '')::numeric, 0), 2),
        0,
        round(
          coalesce(nullif(v_component->>'target_outstanding_ex_vat', '')::numeric, 0),
          2
        ),
        v_residual->>'residual_fingerprint'
      );
      v_inserted_component_count := v_inserted_component_count + 1;
    end loop;

    v_rewritten_chain_count := v_rewritten_chain_count + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'rewritten_chain_count', v_rewritten_chain_count,
    'inserted_component_count', v_inserted_component_count
  );
end;
$function$;

create or replace function public._ctms_rewrite_sync_authoritative_correction_negative_components_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_scope_timesheet_ids uuid[]
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_rewritten_chain_count integer := 0;
  v_inserted_component_count integer := 0;
begin
  if p_session_id is null
     or p_candidate_id is null
     or to_regclass('pg_temp.tmp_sync_authoritative_negative_components') is null then
    return jsonb_build_object(
      'ok', true,
      'rewritten_chain_count', 0,
      'inserted_component_count', 0
    );
  end if;

  for v_residual in
    select value
    from jsonb_array_elements(public._ctms_candidate_correction_residuals_v1(
      p_session_id,
      p_candidate_id,
      null::uuid,
      'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW'
    ))
  loop
    select coalesce(array_agg(value::uuid order by value), array[]::uuid[])
    into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;

    if coalesce(array_length(v_member_ids, 1), 0) = 0
       or (
         coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
         and not (v_member_ids && p_scope_timesheet_ids)
    ) then
      continue;
    end if;

    -- Mirror the source-build boundary.  A non-draftable pay-method mismatch
    -- must remain a resolution case, not a raw recovery component.
    if coalesce((v_residual->>'draftable')::boolean, false) is not true then
      delete from pg_temp.tmp_sync_authoritative_negative_components negative_component
      where negative_component.timesheet_id = any(v_member_ids);
      continue;
    end if;

    v_root_id := nullif(v_residual->>'root_timesheet_id', '')::uuid;
    if v_root_id is null then
      raise exception 'CORRECTION_CHAIN_SYNC_ROOT_REQUIRED'
        using errcode='P0001', detail=v_residual::text;
    end if;
    if coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
       and not (v_root_id = any(p_scope_timesheet_ids)) then
      raise exception 'CORRECTION_CHAIN_SYNC_SCOPE_MUST_INCLUDE_ROOT'
        using errcode='P0001',
              detail=jsonb_build_object(
                'root_timesheet_id', v_root_id,
                'member_timesheet_ids', to_jsonb(v_member_ids)
              )::text;
    end if;

    delete from pg_temp.tmp_sync_authoritative_negative_components negative_component
    where negative_component.timesheet_id = any(v_member_ids);

    for v_component in
      select value
      from jsonb_array_elements(v_residual->'components')
      where round(
        coalesce(nullif(value->>'target_outstanding_ex_vat', '')::numeric, 0),
        2
      ) < 0
    loop
      insert into pg_temp.tmp_sync_authoritative_negative_components (
        timesheet_id,
        key_type,
        key_value,
        truth_ex_vat,
        baseline_ex_vat,
        reserved_ex_vat,
        outstanding_ex_vat,
        baseline_signature
      )
      values (
        v_root_id,
        upper(btrim(v_component->>'component_key_type')),
        btrim(v_component->>'component_key_value'),
        round(coalesce(nullif(v_component->>'truth_ex_vat', '')::numeric, 0), 2),
        round(coalesce(nullif(v_component->>'baseline_ex_vat', '')::numeric, 0), 2),
        0,
        round(
          coalesce(nullif(v_component->>'target_outstanding_ex_vat', '')::numeric, 0),
          2
        ),
        v_residual->>'residual_fingerprint'
      );
      v_inserted_component_count := v_inserted_component_count + 1;
    end loop;

    v_rewritten_chain_count := v_rewritten_chain_count + 1;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'rewritten_chain_count', v_rewritten_chain_count,
    'inserted_component_count', v_inserted_component_count
  );
end;
$function$;

create or replace function public._ctms_rewrite_sync_correction_cases_v1(
  p_session_id uuid,
  p_candidate_ids uuid[],
  p_scope_timesheet_ids uuid[]
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_candidate_id uuid;
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_template record;
  v_negative_amount numeric(12,2);
  v_source_original_paid numeric(12,2);
  v_source_corrected_paid numeric(12,2);
  v_linked_shift_date date;
  v_components_json jsonb;
  v_rewritten_count integer := 0;
  v_resolution_pending_count integer := 0;
  v_resolution_pending_member_ids uuid[] := array[]::uuid[];
begin
  if p_session_id is null
     or coalesce(array_length(p_candidate_ids, 1), 0) = 0
     or to_regclass('pg_temp.tmp_sync_timesheet_case_candidates') is null then
    return jsonb_build_object('ok', true, 'rewritten_chain_count', 0);
  end if;

  foreach v_candidate_id in array p_candidate_ids loop
    for v_residual in
      select value
      from jsonb_array_elements(public._ctms_candidate_correction_residuals_v1(
        p_session_id,
        v_candidate_id,
        null::uuid,
        'PAY_SYNC_OVERPAYMENTS_FROM_PREVIEW'
      ))
    loop
      select coalesce(array_agg(value::uuid order by value), array[]::uuid[])
      into v_member_ids
      from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;

      if coalesce(array_length(v_member_ids, 1), 0) = 0 then
        continue;
      end if;
      if coalesce(array_length(p_scope_timesheet_ids, 1), 0) > 0
         and not (v_member_ids && p_scope_timesheet_ids) then
        continue;
      end if;

      v_root_id := nullif(v_residual->>'root_timesheet_id', '')::uuid;
      if v_root_id is null then
        raise exception 'CORRECTION_CHAIN_SYNC_ROOT_REQUIRED'
          using errcode='P0001', detail=v_residual::text;
      end if;

      if exists (
        select 1
        from public.pay_advances finance_case
        join public.pay_advance_reservations reservation
          on reservation.finance_case_id = finance_case.id
        left join public.pay_batch_items batch_item
          on batch_item.id = reservation.pay_batch_item_id
        where finance_case.candidate_id = v_candidate_id
          and finance_case.linked_timesheet_id = any(v_member_ids)
          and upper(btrim(coalesce(reservation.status, ''))) in ('RESERVED', 'COMMITTED')
          and reservation.released_at_utc is null
          and (batch_item.id is null or coalesce(batch_item.is_voided, false) is not true)
      ) then
        if exists (
          select 1
          from public.pay_advances finance_case
          join public.pay_advance_reservations reservation
            on reservation.finance_case_id = finance_case.id
          left join public.pay_batch_items batch_item
            on batch_item.id = reservation.pay_batch_item_id
          where finance_case.candidate_id = v_candidate_id
            and finance_case.linked_timesheet_id = any(v_member_ids)
            and upper(btrim(coalesce(reservation.status, ''))) in ('RESERVED', 'COMMITTED')
            and reservation.released_at_utc is null
            and (batch_item.id is null or coalesce(batch_item.is_voided, false) is not true)
            and not exists (
              select 1
              from public.pay_batch_candidates active_batch_candidate
              join public.pay_batches active_batch
                on active_batch.id = active_batch_candidate.pay_batch_id
              where active_batch_candidate.id = batch_item.pay_batch_candidate_id
                and active_batch_candidate.candidate_id = v_candidate_id
                and active_batch.cancelled_at_utc is null
                and upper(btrim(coalesce(active_batch.status, ''))) in (
                  'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM',
                  'PARTIAL', 'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
                  'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
                )
                and coalesce(
                  batch_item.frozen_component_snapshot_json->>'correction_root_id',
                  batch_item.frozen_resolution_payload_json->>'correction_root_id',
                  ''
                ) = v_root_id::text
            )
        ) then
          raise exception 'CORRECTION_CHAIN_ACTIVE_FINANCE_RESERVATION'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'candidate_id', v_candidate_id::text,
                    'root_timesheet_id', v_root_id::text,
                    'message', 'A correction-chain reservation is not safely covered by its active frozen Banking Pay batch.'
                  )::text;
        end if;

        -- This correction root is already frozen in an active batch.  Remove
        -- its live members from the pre-draft finance-sync workspace so the
        -- refresh cannot recreate, amend or clear the frozen authority.
        delete from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
        where candidate_row.candidate_id = v_candidate_id
          and candidate_row.timesheet_id = any(v_member_ids);

        continue;
      end if;

      if coalesce((v_residual->>'draftable')::boolean,false) is not true then
        if coalesce(v_residual->>'block_code','')
             = 'CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
           and coalesce((v_residual->>'unresolved_count')::integer,0) > 0
           and coalesce((v_residual->>'reservation_overrun_count')::integer,0) = 0
           and coalesce((v_residual->>'component_count')::integer,0) > 0 then
          -- The unresolved chain must remain visible to the Workbench's
          -- existing case-resolution projection, but it must not create,
          -- amend or clear finance authority until the saved resolution is
          -- fresh.  Suppress every member as one coupled economic unit.
          delete from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
          where candidate_row.candidate_id = v_candidate_id
            and candidate_row.timesheet_id = any(v_member_ids);

          select coalesce(array_agg(distinct member_id order by member_id),array[]::uuid[])
          into v_resolution_pending_member_ids
          from unnest(v_resolution_pending_member_ids || v_member_ids) member_id;
          v_resolution_pending_count := v_resolution_pending_count + 1;
          continue;
        end if;

        raise exception 'CORRECTION_RESIDUAL_NOT_READY_FOR_OVERPAYMENT_SYNC'
          using errcode='P0001',detail=v_residual::text;
      end if;

      select candidate_row.*
      into v_template
      from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
      where candidate_row.candidate_id = v_candidate_id
        and candidate_row.timesheet_id = any(v_member_ids)
      order by
        case
          when candidate_row.desired_case_type =
               'OVERPAYMENT'::public.pay_finance_case_type_enum then 0
          else 1
        end,
        case when candidate_row.timesheet_id = v_root_id then 0 else 1 end,
        candidate_row.timesheet_id
      limit 1;

      select
        round(coalesce(sum(abs(nullif(component->>'target_outstanding_ex_vat', '')::numeric)), 0), 2),
        round(coalesce(sum(coalesce(nullif(component->>'baseline_ex_vat', '')::numeric, 0)), 0), 2),
        round(coalesce(sum(coalesce(nullif(component->>'truth_ex_vat', '')::numeric, 0)), 0), 2),
        min(case
          when upper(coalesce(component->>'component_key_type', '')) = 'TS_DAY'
           and coalesce(component->>'component_key_value', '') ~ '^\d{4}-\d{2}-\d{2}$'
          then (component->>'component_key_value')::date
          else null::date
        end),
        coalesce(jsonb_agg(
          jsonb_strip_nulls(jsonb_build_object(
            'candidate_id', v_candidate_id::text,
            'client_id', v_residual->>'client_id',
            'linked_timesheet_id', v_root_id::text,
            'source_family_key', v_residual->>'source_family_key',
            'component_key_type', component->>'component_key_type',
            'component_key_value', component->>'component_key_value',
            'classification', coalesce(component->>'classification', 'TAXABLE_CHANNEL_SENSITIVE'),
            -- The correction-chain resolution has already converted this
            -- amount onto the current target pay channel.  Finance sync must
            -- not offer a second PAYE/umbrella conversion for the same money.
            'source_pay_method', v_residual->>'target_pay_method',
            'current_target_pay_method', v_residual->>'target_pay_method',
            'source_amount', abs(nullif(component->>'target_outstanding_ex_vat', '')::numeric),
            'remaining_source_amount', abs(nullif(component->>'target_outstanding_ex_vat', '')::numeric),
            'overpayment_component_authority', 'PRE_DRAFT_LIVE_TRUTH',
            'source_basis_json', jsonb_strip_nulls(jsonb_build_object(
              'linked_timesheet_id', v_root_id::text,
              'source_family_key', v_residual->>'source_family_key',
              'component_key_type', component->>'component_key_type',
              'component_key_value', component->>'component_key_value',
              'classification', coalesce(component->>'classification', 'TAXABLE_CHANNEL_SENSITIVE'),
              'baseline_ex_vat', component->>'baseline_ex_vat',
              'truth_ex_vat', component->>'truth_ex_vat',
              'correction_chain_fingerprint', v_residual->>'chain_fingerprint',
              'correction_chain_residual_fingerprint', v_residual->>'residual_fingerprint',
              'upstream_correction_pay_method_resolution_applied', true,
              'correction_financials_policy_envelope', component->'correction_financials_policy_envelope',
              'correction_financials_policy_envelope_fingerprint', component->>'correction_financials_policy_envelope_fingerprint'
            )),
            'target_basis_json', jsonb_strip_nulls(jsonb_build_object(
              'current_target_pay_method', v_residual->>'target_pay_method',
              'correction_chain_fingerprint', v_residual->>'chain_fingerprint',
              'correction_chain_residual_fingerprint', v_residual->>'residual_fingerprint'
            ))
          ))
          order by component->>'component_key_type', component->>'component_key_value'
        ), '[]'::jsonb)
      into
        v_negative_amount,
        v_source_original_paid,
        v_source_corrected_paid,
        v_linked_shift_date,
        v_components_json
      from jsonb_array_elements(v_residual->'components') component
      where round(coalesce(nullif(component->>'target_outstanding_ex_vat', '')::numeric, 0), 2) < 0;

      delete from pg_temp.tmp_sync_timesheet_case_candidates candidate_row
      where candidate_row.candidate_id = v_candidate_id
        and candidate_row.timesheet_id = any(v_member_ids);

      if coalesce(v_negative_amount, 0) > 0 then
        -- Correction members are deliberately suppressed once their coupled
        -- residual becomes the sole finance authority.  Therefore a raw
        -- member template may be absent here.  The residual already carries
        -- the authoritative client, target channel, amounts and component
        -- evidence needed to create the single coupled recovery candidate.
        insert into pg_temp.tmp_sync_timesheet_case_candidates (
          candidate_id,
          timesheet_id,
          client_id,
          linked_shift_date,
          corrected_amount_ex,
          baseline_signature,
          candidate_pay_method,
          case_is_blocked,
          needs_lifecycle_tracking,
          overpayment_amount_ex,
          underpayment_amount_ex,
          desired_case_type,
          desired_advance_kind,
          desired_reason,
          source_original_paid_amount,
          source_corrected_paid_amount,
          components_sync_json
        )
        values (
          v_candidate_id,
          v_root_id,
          coalesce(nullif(v_residual->>'client_id', '')::uuid, v_template.client_id),
          v_linked_shift_date,
          v_source_corrected_paid,
          v_residual->>'residual_fingerprint',
          coalesce(nullif(v_residual->>'target_pay_method', ''), v_template.candidate_pay_method),
          false,
          true,
          v_negative_amount,
          0,
          'OVERPAYMENT'::public.pay_finance_case_type_enum,
          'OVERPAYMENT'::public.pay_advance_kind_enum,
          'OVERPAYMENT'::public.pay_advance_reason_enum,
          v_source_original_paid,
          v_source_corrected_paid,
          v_components_json
        );
      end if;

      v_rewritten_count := v_rewritten_count + 1;
    end loop;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'rewritten_chain_count', v_rewritten_count,
    'resolution_pending_chain_count', v_resolution_pending_count,
    'resolution_pending_member_timesheet_ids',
      to_jsonb(v_resolution_pending_member_ids)
  );
end;
$function$;

create or replace function public._ctms_assert_payload_corrections_fresh_v1(
  p_payload jsonb,
  p_context text default 'IMPORT_CORRECTION'
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_id uuid;
  v_chain jsonb;
  v_checked jsonb := '[]'::jsonb;
begin
  foreach v_id in array public._ctms_payload_timesheet_ids_v1(p_payload, 100) loop
    if coalesce((public._ctms_import_correction_classify_v1(v_id)
      ->> 'is_import_authoritative_correction')::boolean, false) then
      v_chain := public.timesheet_correction_chain_scope_v1(v_id, false, 32, 100);
      if coalesce((v_chain ->> 'valid')::boolean, false) is not true then
        raise exception 'CORRECTION_CHAIN_STALE_OR_INVALID'
          using errcode = '40001', detail = jsonb_build_object(
            'context', p_context, 'timesheet_id', v_id, 'chain', v_chain
          )::text;
      end if;
      v_checked := v_checked || jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_id, 'chain_fingerprint', v_chain ->> 'chain_fingerprint'
      ));
    end if;
  end loop;
  return jsonb_build_object('ok', true, 'context', p_context, 'checked', v_checked);
end;
$function$;

create or replace function public._ctms_assert_session_correction_residuals_draftable_v1(
  p_session_id uuid,
  p_selected_preview_row_ids jsonb default null,
  p_context text default 'BANKING_PAY_DRAFT'
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_candidate uuid;
  v_residuals jsonb;
  v_bad jsonb;
  v_carry_bad jsonb;
  v_all jsonb := '[]'::jsonb;
begin
  if p_session_id is null then raise exception 'WORKBENCH_SESSION_ID_REQUIRED' using errcode='22023'; end if;
  for v_candidate in
    with selected_ids as (
      select value::uuid as id
      from jsonb_array_elements_text(coalesce(p_selected_preview_row_ids, '[]'::jsonb)) x(value)
      where value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
    select distinct pr.candidate_id
    from public.banking_pay_workbench_preview_rows pr
    where pr.session_id = p_session_id
      and (p_selected_preview_row_ids is null
        or jsonb_array_length(coalesce(p_selected_preview_row_ids, '[]'::jsonb)) = 0
        or pr.id in (select id from selected_ids))
      and coalesce((public._ctms_import_correction_classify_v1(pr.timesheet_id)
        ->> 'is_import_authoritative_correction')::boolean, false)
  loop
    select jsonb_agg(jsonb_build_object(
      'registration_id',carry_row.id,
      'status',carry_row.status,
      'state_reason_code',carry_row.state_reason_code,
      'canonical_resolution_key',carry_row.canonical_resolution_key
    ) order by carry_row.source_priority,carry_row.created_at_utc)
    into v_carry_bad
    from public.banking_pay_workbench_case_resolution_carry_registrations carry_row
    where carry_row.target_session_id=p_session_id
      and carry_row.candidate_id=v_candidate
      and carry_row.status in ('PENDING','STALE','INCOMPATIBLE');

    if jsonb_array_length(coalesce(v_carry_bad,'[]'::jsonb))>0 then
      raise exception 'CORRECTION_RESOLUTION_CARRY_NOT_DRAFTABLE'
        using errcode='P0001',
              detail=jsonb_build_object(
                'candidate_id',v_candidate,
                'carry_registrations',v_carry_bad
              )::text;
    end if;

    v_residuals := public._ctms_candidate_correction_residuals_v1(
      p_session_id, v_candidate, null, p_context
    );
    select jsonb_agg(x) into v_bad
    from jsonb_array_elements(v_residuals) x
    where coalesce((x ->> 'draftable')::boolean, false) is not true;
    if jsonb_array_length(coalesce(v_bad, '[]'::jsonb)) > 0 then
      raise exception 'CORRECTION_RESIDUAL_NOT_DRAFTABLE'
        using errcode = 'P0001', detail = v_bad::text;
    end if;
    v_all := v_all || v_residuals;
  end loop;
  return jsonb_build_object('ok', true, 'correction_residuals', v_all);
end;
$function$;

create or replace function public._ctms_materialise_candidate_correction_residuals_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_source_build_run_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_suggested_component jsonb;
  v_suggestion_payload jsonb;
  v_suggestion_result jsonb;
  v_suggestion_original_basis numeric;
  v_suggestion_current_basis numeric;
  v_suggestion_current_target_amount numeric;
  v_suggestion_target_per_source numeric;
  v_suggestion_scale numeric;
  v_suggestion_original_units numeric;
  v_suggestion_rebased_units numeric;
  v_suggestion_source_rate numeric;
  v_suggestion_charge_rate numeric;
  v_suggestion_source_charge_ex numeric;
  v_suggestion_target_rate numeric;
  v_suggestion_target_ex numeric;
  v_suggestion_target_vat numeric;
  v_suggestion_target_inc numeric;
  v_suggestion_matching_bucket_count integer := 0;
  v_has_suggested_resolution boolean := false;
  v_member_ids uuid[];
  v_carrier_row_ids uuid[];
  v_root_id uuid;
  v_carrier_row_id uuid;
  v_carrier_has_finance_case boolean;
  v_finance_parent_row_id uuid;
  v_finance_component_json jsonb;
  v_finance_component_due numeric := 0;
  v_chain_in_source_build boolean;
  v_source_pay_method text;
  v_line_key text;
  v_resolution_pending boolean := false;
  v_component_needs_resolution boolean := false;
  v_component_outstanding numeric := 0;
  v_component_source_outstanding numeric := 0;
  v_updated integer := 0;
  v_superseded integer := 0;
  v_row_count integer := 0;
begin
  -- Session replacement registers saved decisions before retiring the source
  -- session. Candidate source build is the first point at which target
  -- correction evidence is authoritative, so consume the candidate's locked
  -- registrations here. Missing evidence remains durably PENDING.
  perform public._pay_workbench_case_resolution_carry_process_candidate_v1(
    p_session_id,
    p_candidate_id,
    p_source_build_run_id,
    coalesce(p_now_utc,now())
  );

  v_residuals := public._ctms_candidate_correction_residuals_v1(
    p_session_id,p_candidate_id,null::uuid,'PAY_WORKBENCH_SOURCE_BUILD'
  );
  for v_residual in select value from jsonb_array_elements(v_residuals) loop
    v_root_id:=nullif(v_residual->>'root_timesheet_id','')::uuid;
    v_resolution_pending:=false;

    if coalesce(v_residual->>'block_code','')
         = 'CORRECTION_CHAIN_RESERVATION_OVERRUN'
       and coalesce((v_residual->>'reservation_overrun_count')::integer,0) > 0
       and v_root_id is not null
       and not exists (
         select 1
         from jsonb_array_elements(
           coalesce(v_residual->'components','[]'::jsonb)
         ) live_component(value)
         where round(coalesce(
           nullif(live_component.value->>'target_outstanding_ex_vat','')::numeric,
           0
         ),2) <> 0
       )
       and exists (
         select 1
         from public.pay_batch_items active_batch_item
         join public.pay_batch_candidates active_batch_candidate
           on active_batch_candidate.id = active_batch_item.pay_batch_candidate_id
         join public.pay_batches active_batch
           on active_batch.id = active_batch_candidate.pay_batch_id
         where active_batch_candidate.candidate_id = p_candidate_id
           and coalesce(active_batch_item.is_voided,false) is not true
           and active_batch.cancelled_at_utc is null
           and upper(btrim(coalesce(active_batch.status,''))) in (
             'DRAFT', 'DRAFT_CREATED', 'READY', 'WAITING_BANK_CONFIRM',
             'PARTIAL', 'FAILED', 'BLOCKED_FUNDS', 'SCHEDULED', 'EXECUTING',
             'AWAITING_AUTHORISATION', 'AUTHORISED_FOR_PAYMENT'
           )
           and coalesce(
             active_batch_item.frozen_component_snapshot_json->>'correction_root_id',
             active_batch_item.frozen_resolution_payload_json->>'correction_root_id',
             ''
           ) = v_root_id::text
       ) then
      -- The correction root is already represented by frozen batch authority.
      -- Do not rebuild a second live carrier while that batch remains active.
      -- This branch is valid only when no live component has a remaining
      -- delta. Raw worked-time aliases from the bounded source build must stop
      -- being current; otherwise the workbench misleadingly asks the user to
      -- resolve amounts that are already frozen in the active draft. Keep
      -- independent expense/additional-code domains current. A later changed
      -- amount cannot enter this branch and remains subject to ordinary
      -- freshness/draft-conflict handling.
      select coalesce(
               array_agg(value::uuid order by value),
               array[]::uuid[]
             )
      into v_member_ids
      from jsonb_array_elements_text(
        coalesce(v_residual->'member_timesheet_ids','[]'::jsonb)
      ) value;

      update public.banking_pay_workbench_candidate_source_lines l
      set status='SUPERSEDED',
          updated_at_utc=coalesce(p_now_utc,now())
      where l.session_id=p_session_id
        and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id
        and l.status='CURRENT'
        and l.timesheet_id=any(v_member_ids)
        and l.section='cases_resolutions'
        and upper(coalesce(l.economic_key_json->>'key_type','')) in (
          'TS_TOTAL','TS_DAY'
        );
      get diagnostics v_row_count = row_count;
      v_superseded:=v_superseded+v_row_count;
      continue;
    end if;

    if coalesce((v_residual->>'draftable')::boolean,false) is not true then
      if coalesce(v_residual->>'block_code','')
           = 'CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED'
         and coalesce((v_residual->>'unresolved_count')::integer,0) > 0
         and coalesce((v_residual->>'reservation_overrun_count')::integer,0) = 0
         and coalesce((v_residual->>'component_count')::integer,0) > 0 then
        -- A previously resolved canonical carrier is valid only for the source
        -- basis against which it was saved.  When current evidence now needs a
        -- fresh PAYE/umbrella decision, retire those generated carrier rows
        -- before retaining the ordinary source rows for the resolver.  Without
        -- this cleanup a settled historical target (for example £43) can remain
        -- selectable beside the new live delta even though the residual has
        -- correctly rejected the old source-basis fingerprint.
        update public.banking_pay_workbench_candidate_source_lines l
        set status='SUPERSEDED',
            updated_at_utc=coalesce(p_now_utc,now())
        where l.session_id=p_session_id
          and l.candidate_id=p_candidate_id
          and l.source_build_run_id=p_source_build_run_id
          and l.status='CURRENT'
          and l.line_key like
                'correction-chain:'||v_root_id::text||':%'
          and nullif(
                btrim(coalesce(
                  l.source_row_json->>'canonical_correction_key',
                  ''
                )),
                ''
              ) is not null;
        get diagnostics v_row_count = row_count;
        v_superseded:=v_superseded+v_row_count;

        -- Keep the chain fail-closed for drafting, but do not leave its raw
        -- member rows as four independent browser decisions.  The component
        -- loop below rewrites exactly one server-owned carrier per canonical
        -- date/total key into Cases / Resolutions and supersedes every alias.
        -- Components whose saved decision is still fresh remain visible as
        -- waiting siblings; only components with resolution_required=true
        -- ask the user for a new PAYE/umbrella decision.
        v_resolution_pending:=true;
      else
        raise exception 'CORRECTION_RESIDUAL_NOT_DRAFTABLE'
          using errcode='P0001',detail=v_residual::text;
      end if;
    end if;
    select coalesce(array_agg(value::uuid order by value),array[]::uuid[]) into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;
    v_carrier_row_ids:=array[]::uuid[];

    -- Targeted source builds deliberately contain only the timesheet family
    -- that dirtied the workbench.  Do not require an unrelated historical
    -- correction chain to be present in that bounded build.  If any member of
    -- the chain is present, the component-level carrier checks below continue
    -- to fail closed exactly as before.
    select exists (
      select 1
      from public.banking_pay_workbench_candidate_source_lines source_line
      where source_line.session_id=p_session_id
        and source_line.candidate_id=p_candidate_id
        and source_line.source_build_run_id=p_source_build_run_id
        and source_line.status='CURRENT'
        and source_line.timesheet_id=any(v_member_ids)
    )
    into v_chain_in_source_build;
    if coalesce(v_chain_in_source_build,false) is not true then
      continue;
    end if;

    for v_component in select value from jsonb_array_elements(v_residual->'components') loop
      v_suggested_component:=null;
      v_suggestion_payload:=null;
      v_suggestion_result:=null;
      v_suggestion_current_basis:=null;
      v_has_suggested_resolution:=false;
      v_component_needs_resolution:=v_resolution_pending
        and coalesce((v_component->>'resolution_required')::boolean,false)
        and coalesce((v_component->>'resolution_complete')::boolean,false) is not true;
      v_component_source_outstanding:=round(coalesce(
        nullif(v_component->>'effective_source_outstanding_ex_vat','')::numeric,
        nullif(v_component->>'truth_ex_vat','')::numeric,
        0
      ),2);
      v_component_outstanding:=case
        when v_component_needs_resolution
          then v_component_source_outstanding
        else round(coalesce(
          nullif(v_component->>'target_outstanding_ex_vat','')::numeric,
          0
        ),2)
      end;

      if v_component_outstanding=0 then
        update public.banking_pay_workbench_candidate_source_lines l
        set status='SUPERSEDED',
            updated_at_utc=coalesce(p_now_utc,now())
        where l.session_id=p_session_id
          and l.candidate_id=p_candidate_id
          and l.source_build_run_id=p_source_build_run_id
          and l.status='CURRENT'
          and l.timesheet_id=any(v_member_ids)
          and upper(coalesce(l.economic_key_json->>'key_type',''))
              =upper(coalesce(v_component->>'component_key_type',''))
          and coalesce(l.economic_key_json->>'key_value','')
              =coalesce(v_component->>'component_key_value','');
        get diagnostics v_row_count = row_count;
        v_superseded:=v_superseded+v_row_count;
        continue;
      end if;

      v_line_key:='correction-chain:'||v_root_id::text||':'||lower(v_component->>'component_key_type')||':'||lower(v_component->>'component_key_value');

      select l.id into v_carrier_row_id
      from public.banking_pay_workbench_candidate_source_lines l
      where l.session_id=p_session_id and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
        and l.timesheet_id=any(v_member_ids)
        and upper(coalesce(l.economic_key_json->>'key_type',''))=upper(coalesce(v_component->>'component_key_type',''))
        and coalesce(l.economic_key_json->>'key_value','')=coalesce(v_component->>'component_key_value','')
      order by
        -- Replays of the same source-build run must retain the carrier that
        -- already owns the canonical correction-chain identity.
        case when l.line_key=v_line_key then 0 else 1 end,
        case
          when v_component_outstanding < 0
           and nullif(btrim(coalesce(l.source_row_json->>'finance_case_id','')),'') is not null then 0
          when v_component_outstanding >= 0
           and nullif(btrim(coalesce(l.source_row_json->>'finance_case_id','')),'') is null then 0
          else 1
        end,
        case when l.timesheet_id=v_root_id then 0 else 1 end,
        l.source_ordinal,
        l.id
      limit 1 for update;

      -- The ordinary finance-case projector exposes a multi-component case as
      -- one TS_TOTAL parent with server-owned component allocations nested in
      -- case_components.  A resolved negative correction requires one exact
      -- finance-backed carrier per dated component.  Clone only the matching
      -- nested component into a provisional child row; the canonical rewrite
      -- below then owns its final identity.  The component's nominal recovery
      -- comes from the already-authoritative correction residual; this only
      -- splits the aggregate presentation and introduces no new calculation.
      if v_carrier_row_id is null
         and v_component_outstanding < 0
         and coalesce(v_resolution_pending,false) is not true then
        v_finance_parent_row_id:=null::uuid;
        v_finance_component_json:=null::jsonb;

        select
          finance_parent.id,
          nested_component.value
        into
          v_finance_parent_row_id,
          v_finance_component_json
        from public.banking_pay_workbench_candidate_source_lines
               finance_parent
        cross join lateral jsonb_array_elements(
          case
            when jsonb_typeof(
                   finance_parent.source_row_json->'case_components'
                 )='array'
              then finance_parent.source_row_json->'case_components'
            else '[]'::jsonb
          end
        ) nested_component(value)
        where finance_parent.session_id=p_session_id
          and finance_parent.candidate_id=p_candidate_id
          and finance_parent.source_build_run_id=p_source_build_run_id
          and finance_parent.status='CURRENT'
          and finance_parent.timesheet_id=any(v_member_ids)
          and upper(coalesce(
                finance_parent.economic_key_json->>'key_type',
                ''
              ))='TS_TOTAL'
          and nullif(
                btrim(coalesce(
                  finance_parent.source_row_json->>'finance_case_id',
                  ''
                )),
                ''
              ) is not null
          and nullif(
                btrim(coalesce(
                  nested_component.value->>'finance_component_id',
                  ''
                )),
                ''
              ) is not null
          and coalesce(
                nested_component.value->>'source_family_key',
                ''
              )=coalesce(v_residual->>'source_family_key','')
          and upper(coalesce(
                nested_component.value->>'component_key_type',
                ''
              ))=upper(coalesce(
                v_component->>'component_key_type',
                ''
              ))
          and coalesce(
                nested_component.value->>'component_key_value',
                ''
              )=coalesce(v_component->>'component_key_value','')
        order by finance_parent.source_ordinal,finance_parent.id
        limit 1
        for update of finance_parent;

        if v_finance_parent_row_id is not null
           and v_finance_component_json is not null then
          v_finance_component_due:=round(abs(v_component_outstanding),2);

          insert into public.banking_pay_workbench_candidate_source_lines (
            id,
            session_id,
            candidate_id,
            session_version,
            source_change_seq,
            source_build_run_id,
            source_ordinal,
            line_key,
            parent_line_key,
            split_suffix,
            timesheet_id,
            section,
            source_row_json,
            economic_key_json,
            contract_json,
            pay_channel_scope,
            refresh_scope_kind,
            status,
            created_at_utc,
            updated_at_utc
          )
          select
            gen_random_uuid(),
            finance_parent.session_id,
            finance_parent.candidate_id,
            finance_parent.session_version,
            finance_parent.source_change_seq,
            finance_parent.source_build_run_id,
            (
              select coalesce(max(existing_line.source_ordinal),0)+1
              from public.banking_pay_workbench_candidate_source_lines
                     existing_line
              where existing_line.session_id=p_session_id
                and existing_line.candidate_id=p_candidate_id
                and existing_line.source_build_run_id=p_source_build_run_id
            ),
            finance_parent.line_key
              ||':component:'
              ||lower(coalesce(
                   v_component->>'component_key_type',
                   ''
                 ))
              ||':'
              ||lower(coalesce(
                   v_component->>'component_key_value',
                   ''
                 )),
            finance_parent.line_key,
            lower(coalesce(
              v_component->>'component_key_type',
              ''
            ))
              ||':'
              ||lower(coalesce(
                   v_component->>'component_key_value',
                   ''
                 )),
            finance_parent.timesheet_id,
            finance_parent.section,
            coalesce(finance_parent.source_row_json,'{}'::jsonb)
              || jsonb_build_object(
                'finance_component_id',
                  v_finance_component_json->>'finance_component_id',
                'component_key_type',
                  v_component->>'component_key_type',
                'component_key_value',
                  v_component->>'component_key_value',
                'case_components',
                  jsonb_build_array(v_finance_component_json),
                'economic_key',
                  coalesce(
                    finance_parent.source_row_json->'economic_key',
                    '{}'::jsonb
                  )
                  || jsonb_build_object(
                    'timesheet_id',finance_parent.timesheet_id::text,
                    'key_type',v_component->>'component_key_type',
                    'key_value',v_component->>'component_key_value'
                  ),
                -- Recovery is initially blocked with zero allocatable value.
                -- The existing headroom revalidator promotes only the amount
                -- supported by retained positive pay.  Keep the full dated
                -- authority separately as the component's nominal due.
                'amount_ex_vat',0,
                'amount_display',0,
                'preview_amount_ex_vat',0,
                'section_amount_ex_vat',0,
                'component_amount_ex_vat',0,
                'preview_component_amount_ex_vat',0,
                'nominal_due_amount_ex_vat',v_finance_component_due,
                'recoverable_this_pay_run_ex_vat',0,
                'preview_contract',
                  coalesce(
                    finance_parent.source_row_json->'preview_contract',
                    '{}'::jsonb
                  )
                  || jsonb_build_object(
                    'amount_ex_vat',0,
                    'selection_amount_ex_vat',0,
                    'key_type',v_component->>'component_key_type',
                    'key_value',v_component->>'component_key_value'
                  )
              ),
            jsonb_build_object(
              'timesheet_id',finance_parent.timesheet_id::text,
              'key_type',v_component->>'component_key_type',
              'key_value',v_component->>'component_key_value'
            ),
            coalesce(finance_parent.contract_json,'{}'::jsonb),
            finance_parent.pay_channel_scope,
            finance_parent.refresh_scope_kind,
            'CURRENT',
            coalesce(p_now_utc,now()),
            coalesce(p_now_utc,now())
          from public.banking_pay_workbench_candidate_source_lines
                 finance_parent
          where finance_parent.id=v_finance_parent_row_id
          on conflict do nothing
          returning id into v_carrier_row_id;

          -- Safe replay can encounter the child after an earlier retry inserted
          -- it.  Re-read the exact current carrier instead of creating another.
          if v_carrier_row_id is null then
            select exact_line.id
            into v_carrier_row_id
            from public.banking_pay_workbench_candidate_source_lines exact_line
            where exact_line.session_id=p_session_id
              and exact_line.candidate_id=p_candidate_id
              and exact_line.source_build_run_id=p_source_build_run_id
              and exact_line.status='CURRENT'
              and exact_line.timesheet_id=any(v_member_ids)
              and upper(coalesce(
                    exact_line.economic_key_json->>'key_type',
                    ''
                  ))=upper(coalesce(
                    v_component->>'component_key_type',
                    ''
                  ))
              and coalesce(
                    exact_line.economic_key_json->>'key_value',
                    ''
                  )=coalesce(v_component->>'component_key_value','')
              and nullif(
                    btrim(coalesce(
                      exact_line.source_row_json->>'finance_case_id',
                      ''
                    )),
                    ''
                  ) is not null
            order by exact_line.source_ordinal,exact_line.id
            limit 1
            for update;
          end if;
        end if;
      end if;

      -- The bounded source builder may expose an unresolved correction member
      -- as one TS_TOTAL row before its saved dated decisions are normalised.
      -- Use one unretained raw member as the dated carrier, then rewrite its
      -- key below from the server-owned residual. A negative component may
      -- use this fallback only while the coupled chain is still pending
      -- resolution: that row is a locked, non-selectable decision surface and
      -- cannot enter draft scope. Once the chain is resolved, a negative
      -- component must use its exact finance-case row.
      if v_carrier_row_id is null
         and (
           v_component_outstanding > 0
           or v_resolution_pending
         ) then
        select l.id into v_carrier_row_id
        from public.banking_pay_workbench_candidate_source_lines l
        where l.session_id=p_session_id
          and l.candidate_id=p_candidate_id
          and l.source_build_run_id=p_source_build_run_id
          and l.status='CURRENT'
          and l.timesheet_id=any(v_member_ids)
          and l.section in ('cases_resolutions','canonical_preview_lines')
          and upper(coalesce(l.economic_key_json->>'key_type',''))='TS_TOTAL'
          and nullif(
            btrim(coalesce(l.source_row_json->>'finance_case_id','')),
            ''
          ) is null
          and not exists (
            select 1
            from unnest(
              coalesce(v_carrier_row_ids,array[]::uuid[])
            ) as retained_carrier(carrier_row_id)
            where retained_carrier.carrier_row_id=l.id
          )
        order by
          case when l.timesheet_id=v_root_id then 0 else 1 end,
          l.source_ordinal,
          l.id
        limit 1
        for update;
      end if;

      if v_carrier_row_id is null then
        -- A chain-wide residual includes settled/unchanged dated components so
        -- the live calculation remains complete. Those zero-outstanding
        -- components legitimately have no current Banking Pay source row.
        -- Only money-bearing components require a carrier and fail closed.
        if v_component_outstanding=0 then
          continue;
        end if;
        raise exception 'CORRECTION_RESIDUAL_SOURCE_COMPONENT_MISSING'
          using errcode='P0001',detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;

      select exists (
        select 1
        from public.banking_pay_workbench_candidate_source_lines carrier_source
        where carrier_source.id=v_carrier_row_id
          and nullif(
            btrim(coalesce(carrier_source.source_row_json->>'finance_case_id','')),
            ''
          ) is not null
      )
      into v_carrier_has_finance_case;

      if v_component_outstanding < 0
         and coalesce(v_resolution_pending,false) is not true
         and coalesce(v_carrier_has_finance_case,false) is not true then
        raise exception 'CORRECTION_CHAIN_OVERPAYMENT_FINANCE_CASE_CARRIER_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;
      if v_component_outstanding > 0
         and nullif(v_component->>'effective_source_outstanding_ex_vat','') is null then
        raise exception 'CORRECTION_CHAIN_SOURCE_OUTSTANDING_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;
      v_source_pay_method:=null;
      if v_component_outstanding > 0 or v_component_needs_resolution then
        select case
                 when count(distinct upper(btrim(source_method.value)))=1
                   then max(upper(btrim(source_method.value)))
                 else null
               end
        into v_source_pay_method
        from jsonb_array_elements_text(
          case
            when jsonb_typeof(v_component->'source_pay_methods')='array'
              then v_component->'source_pay_methods'
            else '[]'::jsonb
          end
        ) as source_method(value)
        where upper(btrim(source_method.value)) in ('PAYE','UMBRELLA');

        if v_source_pay_method is null then
          raise exception 'CORRECTION_CHAIN_SOURCE_PAY_METHOD_REQUIRED'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'source_pay_methods',
                      coalesce(v_component->'source_pay_methods','[]'::jsonb)
                  )::text;
        end if;
      end if;

      v_suggested_component:=null;
      if v_component_needs_resolution then
        -- The canonical residual owns identity and outstanding economics, but
        -- the bounded source row owns the server-generated PAYE/umbrella rate
        -- evidence used by Suggested Rates Review. Preserve only those
        -- suggestion fields while retaining the canonical residual
        -- fingerprint and component identity. Never ask the browser to invent
        -- a rate or reuse a suggestion for another date/component.
        with signed_source_buckets as (
          select
            upper(coalesce(
              delta_component.value->>'bucket_code',
              delta_component.value#>>'{source_basis_json,bucket_code}',
              ''
            )) as bucket_code,
            round(coalesce(sum(
              case
                when coalesce(
                       delta_component.value->>'source_pay_ex_vat',
                       ''
                     ) ~ '^-?[0-9]+([.][0-9]+)?$'
                  then (
                    delta_component.value->>'source_pay_ex_vat'
                  )::numeric
                else 0
              end
            ),0),2) as signed_bucket_source_pay
          from public.banking_pay_workbench_candidate_source_lines
            delta_line
          cross join lateral jsonb_array_elements(
            coalesce(
              delta_line.source_row_json->'case_components',
              '[]'::jsonb
            )
          ) delta_component(value)
          where delta_line.session_id=p_session_id
            and delta_line.candidate_id=p_candidate_id
            and delta_line.source_build_run_id=p_source_build_run_id
            and delta_line.status in ('CURRENT','SUPERSEDED')
            and delta_line.timesheet_id=any(v_member_ids)
            and upper(coalesce(
                  delta_component.value->>'component_key_type',
                  ''
                ))=upper(coalesce(
                  v_component->>'component_key_type',
                  ''
                ))
            and coalesce(
                  delta_component.value->>'component_key_value',
                  ''
                )=coalesce(
                  v_component->>'component_key_value',
                  ''
                )
          group by 1
        ), suggestion_candidates as (
          select
            source_component.value as component_json,
            case when source_line.id=v_carrier_row_id then 0 else 1 end
              as carrier_ordinal,
            case when source_line.status='CURRENT' then 0 else 1 end
              as status_ordinal,
            source_line.source_ordinal,
            source_line.id as source_line_id,
            source_bucket.bucket_code,
            source_bucket.signed_bucket_source_pay
          from public.banking_pay_workbench_candidate_source_lines
            source_line
          cross join lateral jsonb_array_elements(
            coalesce(
              source_line.source_row_json->'case_components',
              '[]'::jsonb
            )
          ) source_component(value)
          join signed_source_buckets source_bucket
            on source_bucket.bucket_code=upper(coalesce(
              source_component.value->>'bucket_code',
              source_component.value#>>'{source_basis_json,bucket_code}',
              ''
            ))
          where source_line.session_id=p_session_id
            and source_line.candidate_id=p_candidate_id
            and source_line.source_build_run_id=p_source_build_run_id
            and source_line.status in ('CURRENT','SUPERSEDED')
            and source_line.timesheet_id=any(v_member_ids)
            and upper(coalesce(
                  source_component.value->>'component_key_type',
                  ''
                ))=upper(coalesce(v_component->>'component_key_type',''))
            and coalesce(
                  source_component.value->>'component_key_value',
                  ''
                )=coalesce(v_component->>'component_key_value','')
            and jsonb_typeof(
                  source_component.value
                    ->'suggested_resolution_payload_json'
                )='object'
            and jsonb_typeof(
                  source_component.value
                    ->'suggested_resolution_result_json'
                )='object'
            and upper(coalesce(
                  source_component.value->>'source_pay_method',
                  source_component.value
                    #>>'{suggested_resolution_result_json,source_pay_method}',
                  ''
                ))=upper(v_source_pay_method)
            and upper(coalesce(
                  source_component.value
                    #>>'{suggested_resolution_payload_json,target_pay_method}',
                  source_component.value
                    #>>'{suggested_resolution_result_json,target_pay_method}',
                  ''
                ))=upper(coalesce(v_component->>'target_pay_method',''))
        ), eligible_suggestion_candidates as (
          select suggestion_candidate.*
          from suggestion_candidates suggestion_candidate
          where abs(suggestion_candidate.signed_bucket_source_pay)>0.005
            and sign(suggestion_candidate.signed_bucket_source_pay)
                =sign(v_component_source_outstanding)
        )
        select
          count(distinct eligible.bucket_code)::integer,
          (
            jsonb_agg(
              eligible.component_json
              order by
                eligible.carrier_ordinal,
                eligible.status_ordinal,
                eligible.source_ordinal,
                eligible.source_line_id
            )->0
          )
        into
          v_suggestion_matching_bucket_count,
          v_suggested_component
        from eligible_suggestion_candidates eligible;

        if v_suggested_component is null then
          raise exception 'CORRECTION_CHAIN_SUGGESTED_RESOLUTION_REQUIRED'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'component_key_type',
                      v_component->>'component_key_type',
                    'component_key_value',
                      v_component->>'component_key_value',
                    'source_pay_method',v_source_pay_method,
                    'target_pay_method',
                      upper(coalesce(v_component->>'target_pay_method',''))
                  )::text;
        end if;
        if v_suggestion_matching_bucket_count<>1 then
          -- A dated correction component can span more than one pay bucket
          -- (for example DAY plus NIGHT). No single historical rate is then
          -- an honest Suggested Rate. Keep the canonical resolution case
          -- available for the established custom-resolution pathway. Preserve
          -- one deterministic row-backed source basis solely so the operator
          -- can enter a replacement rate; do not expose its target suggestion,
          -- guess, average, or let an optional suggestion abort the whole
          -- Workbench source build.
          v_suggestion_current_basis:=round(
            abs(coalesce(v_component_source_outstanding,0)),
            2
          );
          v_suggestion_source_rate:=nullif(
            v_suggested_component->>'source_rate',
            ''
          )::numeric;
          v_suggestion_charge_rate:=nullif(
            v_suggested_component->>'source_charge_rate',
            ''
          )::numeric;
          v_suggestion_rebased_units:=case
            when coalesce(v_suggestion_source_rate,0)>0
              then round(
                v_suggestion_current_basis/v_suggestion_source_rate,
                6
              )
            else null
          end;
          v_suggestion_source_charge_ex:=case
            when v_suggestion_rebased_units is not null
             and v_suggestion_charge_rate is not null
              then round(
                v_suggestion_rebased_units*v_suggestion_charge_rate,
                2
              )
            else null
          end;
          v_suggested_component:=v_suggested_component
            ||jsonb_build_object(
              'source_units',v_suggestion_rebased_units,
              'source_rate',v_suggestion_source_rate,
              'source_charge_rate',v_suggestion_charge_rate,
              'source_pay_ex_vat',v_suggestion_current_basis,
              'source_charge_ex_vat',v_suggestion_source_charge_ex,
              'source_margin_ex_vat',case
                when v_suggestion_source_charge_ex is null then null
                else round(
                  v_suggestion_source_charge_ex
                    -v_suggestion_current_basis,
                  2
                )
              end
            );
        else

        -- A source component can describe the full historical shift while the
        -- canonical correction residual now represents only a smaller unpaid
        -- remainder. Keep the historical source basis/fingerprint as
        -- provenance, but proportionally rebase the actionable suggestion to
        -- the current residual. Copying the old payload unchanged would turn a
        -- £17.39 residual back into the historical £130 target.
        v_suggestion_payload:=coalesce(
          v_suggested_component->'suggested_resolution_payload_json',
          '{}'::jsonb
        );
        v_suggestion_result:=coalesce(
          v_suggested_component->'suggested_resolution_result_json',
          '{}'::jsonb
        );
        v_suggestion_original_basis:=case
          when coalesce(
                 v_suggestion_result
                   ->>'applied_basis_source_amount_ex_vat',
                 v_suggestion_result->>'basis_source_amount_ex_vat',
                 v_suggestion_payload
                   ->>'applied_basis_source_amount_ex_vat',
                 v_suggested_component->>'source_pay_ex_vat',
                 v_suggested_component->>'component_amount_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then abs(coalesce(
              v_suggestion_result
                ->>'applied_basis_source_amount_ex_vat',
              v_suggestion_result->>'basis_source_amount_ex_vat',
              v_suggestion_payload
                ->>'applied_basis_source_amount_ex_vat',
              v_suggested_component->>'source_pay_ex_vat',
              v_suggested_component->>'component_amount_ex_vat'
            )::numeric)
          else null
        end;
        v_suggestion_target_per_source:=case
          when coalesce(
                 v_suggestion_result
                   ->>'target_amount_ex_vat_per_source_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
           and abs(
                 (
                   v_suggestion_result
                     ->>'target_amount_ex_vat_per_source_ex_vat'
                 )::numeric
               ) > 0
            then abs(
              (
                v_suggestion_result
                  ->>'target_amount_ex_vat_per_source_ex_vat'
              )::numeric
            )
          when coalesce(v_suggestion_original_basis,0)>0
           and coalesce(
                 v_suggestion_result->>'target_amount_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
           and abs(
                 (v_suggestion_result->>'target_amount_ex_vat')::numeric
               ) > 0
            then abs(
              (v_suggestion_result->>'target_amount_ex_vat')::numeric
                / v_suggestion_original_basis
            )
          else null
        end;
        -- The signed correction ledger is source-channel authority. Settled
        -- and reserved correction movements are reconciled through their
        -- frozen source reservation amounts, so only the remaining source
        -- residual may be converted to the target channel. Using the full
        -- historical shift units here would turn a £5.22 residual back into
        -- the historical £130 target.
        v_suggestion_current_basis:=round(
          abs(coalesce(v_component_source_outstanding,0)),
          2
        );
        v_suggestion_current_target_amount:=case
          when coalesce(v_suggestion_target_per_source,0)>0
            then round(
              v_suggestion_current_basis
                * v_suggestion_target_per_source,
              2
            )
          else null
        end;
        v_suggestion_original_units:=case
          when coalesce(
                 v_suggestion_result->>'target_units',
                 v_suggestion_payload->>'target_units',
                 v_suggested_component->>'source_units',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then abs(coalesce(
              v_suggestion_result->>'target_units',
              v_suggestion_payload->>'target_units',
              v_suggested_component->>'source_units'
            )::numeric)
          else null
        end;
        v_suggestion_source_rate:=case
          when coalesce(
                 v_suggested_component->>'source_rate',
                 v_suggested_component#>>'{source_basis_json,source_rate}',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then coalesce(
              v_suggested_component->>'source_rate',
              v_suggested_component#>>'{source_basis_json,source_rate}'
            )::numeric
          else null
        end;
        v_suggestion_charge_rate:=case
          when coalesce(
                 v_suggested_component->>'source_charge_rate',
                 v_suggested_component
                   #>>'{source_basis_json,source_charge_rate}',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then coalesce(
              v_suggested_component->>'source_charge_rate',
              v_suggested_component
                #>>'{source_basis_json,source_charge_rate}'
            )::numeric
          else null
        end;

        if coalesce(v_suggestion_original_basis,0)<=0
           or coalesce(v_suggestion_original_units,0)<=0
           or coalesce(v_suggestion_current_target_amount,0)<=0
           or coalesce(v_suggestion_target_per_source,0)<=0
           or coalesce(v_suggestion_current_basis,0)<=0 then
          raise exception 'CORRECTION_CHAIN_SUGGESTED_RESOLUTION_BASIS_INVALID'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'historical_basis_source_amount_ex_vat',
                      v_suggestion_original_basis,
                    'current_basis_source_amount_ex_vat',
                      v_suggestion_current_basis,
                    'current_target_amount_ex_vat',
                      v_suggestion_current_target_amount,
                    'target_amount_ex_vat_per_source_ex_vat',
                      v_suggestion_target_per_source,
                    'historical_source_units',
                      v_suggestion_original_units
                  )::text;
        end if;

        v_suggestion_scale:=
          v_suggestion_current_basis/v_suggestion_original_basis;
        v_suggestion_rebased_units:=round(
          v_suggestion_original_units*v_suggestion_scale,
          6
        );
        v_suggestion_target_rate:=case
          when coalesce(
                 v_suggestion_payload->>'suggested_target_rate',
                 v_suggestion_result->>'replacement_rate',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(coalesce(
              v_suggestion_payload->>'suggested_target_rate',
              v_suggestion_result->>'replacement_rate'
            )::numeric,2)
          else null
        end;
        v_suggestion_target_ex:=case
          when coalesce(
                 v_suggestion_result
                   ->>'target_amount_ex_vat_per_source_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              v_suggestion_current_basis
                * (
                    v_suggestion_result
                      ->>'target_amount_ex_vat_per_source_ex_vat'
                  )::numeric,
              2
            )
          when coalesce(
                 v_suggestion_result->>'target_amount_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              (v_suggestion_result->>'target_amount_ex_vat')::numeric
                * v_suggestion_scale,
              2
            )
          when v_suggestion_target_rate is not null
            then round(
              v_suggestion_rebased_units*v_suggestion_target_rate,
              2
            )
          else null
        end;
        v_suggestion_target_vat:=case
          when coalesce(
                 v_suggestion_result
                   ->>'target_amount_vat_per_source_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              v_suggestion_current_basis
                * (
                    v_suggestion_result
                      ->>'target_amount_vat_per_source_ex_vat'
                  )::numeric,
              2
            )
          when coalesce(
                 v_suggestion_result->>'target_amount_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              (v_suggestion_result->>'target_amount_vat')::numeric
                * v_suggestion_scale,
              2
            )
          else 0
        end;
        -- Inclusive VAT is a derived total.  Re-scaling a historical inclusive
        -- ratio independently can differ by a penny from the separately rounded
        -- ex-VAT and VAT authorities, so always add those two final amounts.
        v_suggestion_target_inc:=round(
          coalesce(v_suggestion_target_ex,0)
            + coalesce(v_suggestion_target_vat,0),
          2
        );
        if v_suggestion_target_rate is null
           and coalesce(v_suggestion_rebased_units,0)<>0
           and v_suggestion_target_ex is not null then
          v_suggestion_target_rate:=round(
            v_suggestion_target_ex/v_suggestion_rebased_units,
            2
          );
        end if;
        if v_suggestion_target_ex is null
           or v_suggestion_target_rate is null then
          raise exception 'CORRECTION_CHAIN_SUGGESTED_RESOLUTION_RESULT_INVALID'
            using errcode='P0001',
                  detail=jsonb_build_object(
                    'canonical_correction_key',
                      v_component->>'canonical_correction_key',
                    'current_basis_source_amount_ex_vat',
                      v_suggestion_current_basis,
                    'rebased_source_units',
                      v_suggestion_rebased_units
                  )::text;
        end if;

        v_suggestion_source_charge_ex:=case
          when v_suggestion_charge_rate is not null
            then round(
              v_suggestion_rebased_units*v_suggestion_charge_rate,
              2
            )
          when coalesce(
                 v_suggestion_result->>'source_charge_ex_vat',
                 ''
               ) ~ '^-?[0-9]+([.][0-9]+)?$'
            then round(
              (v_suggestion_result->>'source_charge_ex_vat')::numeric
                * v_suggestion_scale,
              2
            )
          else null
        end;

        v_suggestion_payload:=jsonb_strip_nulls(
          v_suggestion_payload
          || jsonb_build_object(
            'applied_basis_source_amount_ex_vat',
              v_suggestion_current_basis,
            'source_units',v_suggestion_rebased_units,
            'target_units',v_suggestion_rebased_units,
            'suggested_target_rate',v_suggestion_target_rate,
            'reuse_mode','PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'correction_residual_basis_rebased',true,
            'correction_residual_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint'
          )
        );
        v_suggestion_result:=jsonb_strip_nulls(
          v_suggestion_result
          || jsonb_build_object(
            'basis_source_amount_ex_vat',v_suggestion_current_basis,
            'applied_basis_source_amount_ex_vat',
              v_suggestion_current_basis,
            'source_units',v_suggestion_rebased_units,
            'target_units',v_suggestion_rebased_units,
            'replacement_rate',v_suggestion_target_rate,
            'target_amount_ex_vat',v_suggestion_target_ex,
            'target_amount_vat',v_suggestion_target_vat,
            'target_amount_inc_vat',v_suggestion_target_inc,
            'source_pay_ex_vat',v_suggestion_current_basis,
            'source_charge_ex_vat',v_suggestion_source_charge_ex,
            'source_margin_ex_vat',case
              when v_suggestion_source_charge_ex is null then null
              else round(
                v_suggestion_source_charge_ex
                  - v_suggestion_current_basis,
                2
              )
            end,
            'target_pay_ex_vat',v_suggestion_target_ex,
            'target_charge_ex_vat',v_suggestion_source_charge_ex,
            'target_margin_ex_vat',case
              when v_suggestion_source_charge_ex is null then null
              else round(
                v_suggestion_source_charge_ex-v_suggestion_target_ex,
                2
              )
            end,
            'margin_delta_ex_vat',round(
              v_suggestion_current_basis-v_suggestion_target_ex,
              2
            ),
            'reuse_mode','PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT',
            'correction_residual_basis_rebased',true,
            'correction_residual_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint'
          )
        );
        v_suggested_component:=v_suggested_component
          || jsonb_build_object(
            'source_units',v_suggestion_rebased_units,
            'source_rate',v_suggestion_source_rate,
            'source_charge_rate',v_suggestion_charge_rate,
            'source_pay_ex_vat',v_suggestion_current_basis,
            'source_charge_ex_vat',v_suggestion_source_charge_ex,
            'source_margin_ex_vat',case
              when v_suggestion_source_charge_ex is null then null
              else round(
                v_suggestion_source_charge_ex
                  - v_suggestion_current_basis,
                2
              )
            end,
            'suggested_resolution_payload_json',v_suggestion_payload,
            'suggested_resolution_result_json',v_suggestion_result,
            'suggestion_explanation_text',
              'This suggestion applies the existing PAYE/umbrella conversion to the current correction residual only. Historical full-shift evidence remains unchanged.'
          );
        v_has_suggested_resolution:=true;
        end if;
      end if;

      update public.banking_pay_workbench_candidate_source_lines l
      set section=case
            when v_resolution_pending then 'cases_resolutions'
            when v_component_outstanding>0 then 'canonical_preview_lines'
            else 'blocked_for_pay'
          end,
          timesheet_id=v_root_id,
          line_key=v_line_key,
          parent_line_key='correction-chain:'||v_root_id::text,
          split_suffix=lower(v_component->>'component_key_type')||':'||lower(v_component->>'component_key_value'),
          source_row_json=coalesce(l.source_row_json,'{}'::jsonb)||jsonb_build_object(
            'timesheet_id',v_root_id::text,'real_business_timesheet_id',v_root_id::text,
            'economic_key',coalesce(l.source_row_json->'economic_key','{}'::jsonb)||jsonb_build_object(
              'timesheet_id',v_root_id::text,
              'key_type',v_component->>'component_key_type',
              'key_value',v_component->>'component_key_value'
            ),
            'source_family_key',v_residual->>'source_family_key',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'resolution_identity',
              v_component->>'canonical_correction_key',
            'case_key',v_line_key,
            'linked_timesheet_id',v_root_id::text,
            'correction_identity_version','CORRECTION_CHAIN_V1',
            'correction_root_id',v_residual->>'root_timesheet_id',
            'ordered_member_timesheet_ids',
              v_residual->'ordered_member_timesheet_ids',
            'component_lineage_fingerprint',
              v_component->>'component_lineage_fingerprint',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint',
            'correction_chain_residual',v_residual,
            'correction_chain_component',v_component,
            'case_components',jsonb_build_array(
              v_component||jsonb_build_object(
                'candidate_id',p_candidate_id::text,
                'linked_timesheet_id',v_root_id::text,
                'component_resolution_key',
                  v_component->>'canonical_correction_key',
                'resolution_identity',
                  v_component->>'canonical_correction_key',
                'source_family_key',v_residual->>'source_family_key',
                'component_key_type',v_component->>'component_key_type',
                'component_key_value',v_component->>'component_key_value',
                'source_pay_method',v_source_pay_method,
                'current_target_pay_method',
                  upper(v_component->>'target_pay_method'),
                'requires_resolution',v_component_needs_resolution,
                'needs_resolution',v_component_needs_resolution,
                'is_actionable_resolution_row',v_component_needs_resolution,
                'has_suggested_resolution',v_has_suggested_resolution,
                'bucket_code',case
                  when v_suggested_component is not null
                    then upper(coalesce(
                      v_suggested_component->>'bucket_code',
                      v_suggested_component
                        #>>'{source_basis_json,bucket_code}',
                      ''
                    ))
                  else null
                end,
                'source_units',case
                  when v_suggested_component is not null
                    then nullif(v_suggested_component->>'source_units','')::numeric
                  else null
                end,
                'source_rate',case
                  when v_suggested_component is not null
                    then nullif(v_suggested_component->>'source_rate','')::numeric
                  else null
                end,
                'source_charge_rate',case
                  when v_suggested_component is not null
                    then nullif(v_suggested_component->>'source_charge_rate','')::numeric
                  else null
                end,
                'source_charge_ex_vat',case
                  when v_suggested_component is not null
                    then nullif(
                      v_suggested_component->>'source_charge_ex_vat',
                      ''
                    )::numeric
                  else null
                end,
                'source_margin_ex_vat',case
                  when v_suggested_component is not null
                    then nullif(
                      v_suggested_component->>'source_margin_ex_vat',
                      ''
                    )::numeric
                  else null
                end,
                'source_basis_json',case
                  when v_suggested_component is not null
                    and jsonb_typeof(
                      v_suggested_component->'source_basis_json'
                    )='object'
                    then v_suggested_component->'source_basis_json'
                  else null
                end,
                'suggested_resolution_payload_json',case
                  when v_has_suggested_resolution
                    then v_suggested_component
                      ->'suggested_resolution_payload_json'
                  else null
                end,
                'suggested_resolution_result_json',case
                  when v_has_suggested_resolution
                    then v_suggested_component
                      ->'suggested_resolution_result_json'
                  else null
                end,
                'suggestion_explanation_text',case
                  when v_has_suggested_resolution
                    then nullif(
                      v_suggested_component->>'suggestion_explanation_text',
                      ''
                    )
                  else null
                end,
                'target_pay_ex_vat',case
                  when v_component_needs_resolution then null
                  else v_component_outstanding
                end,
                'component_amount_ex_vat',v_component_outstanding,
                'preview_due_amount_ex_vat',v_component_outstanding,
                'source_pay_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'source_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'source_entitlement_amount_ex_vat',abs((v_component->>'truth_ex_vat')::numeric),
                'source_reservation_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'remaining_source_amount',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end
              )
            ),
            'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
            'raw_correction_member_rows_suppressed',true,
            -- The raw source row was classified before carried decisions were
            -- replayed.  Project an incomplete component into one canonical
            -- Cases / Resolutions row; project the chain into Ready or Blocked
            -- only after every required component is complete. Draft seeding
            -- still revalidates the canonical key and fingerprints
            -- transactionally.
            'target_section',case
              when v_resolution_pending then 'cases_resolutions'
              when v_component_outstanding>0 then 'canonical_preview_lines'
              else 'blocked_for_pay'
            end,
            'section',case
              when v_resolution_pending then 'cases_resolutions'
              when v_component_outstanding>0 then 'canonical_preview_lines'
              else 'blocked_for_pay'
            end,
            'presentation_section',case
              when v_resolution_pending then 'CASES_RESOLUTIONS'
              when v_component_outstanding>0 then 'READY_TO_PAY'
              else 'BLOCKED_FOR_PAY'
            end,
            'draftable',not v_resolution_pending and v_component_outstanding>0,
            'is_ready_for_draft',not v_resolution_pending and v_component_outstanding>0,
            'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0,
            'case_is_blocked',false,
            'case_needs_resolution_now',v_component_needs_resolution,
            'case_needs_resolution',v_component_needs_resolution,
            'is_case_resolution_satisfied',not v_component_needs_resolution,
            'case_resolution_satisfied_now',not v_component_needs_resolution,
            'has_resolved_rate',not v_component_needs_resolution,
            'resolved_rate_family','BUCKETED',
            'resolution_family','BUCKETED',
            'resolution_action_label',case
              when v_has_suggested_resolution then 'Suggested Rate'
              else 'Custom Rate'
            end,
            'selection_allowed',not v_resolution_pending and v_component_outstanding>0,
            'blocked_reason_codes',case
              when v_component_needs_resolution
                then jsonb_build_array('PAY_METHOD_RESOLUTION_REQUIRED')
              when v_resolution_pending
                then jsonb_build_array('LINKED_CORRECTION_RESOLUTION_PENDING')
              when v_component_outstanding>0 then '[]'::jsonb
              else jsonb_build_array('NO_PAY_HEADROOM')
            end,
            'case_resolution_summary',
              coalesce(l.source_row_json->'case_resolution_summary','{}'::jsonb)
              || jsonb_build_object(
                'is_blocked',false,
                'has_resolved_rate',not v_component_needs_resolution,
                'case_needs_resolution',v_component_needs_resolution,
                'case_resolution_satisfied_now',not v_component_needs_resolution,
                'resolved_rate_family','BUCKETED',
                'resolution_family','BUCKETED',
                'resolution_action_label',case
                  when v_has_suggested_resolution then 'Suggested Rate'
                  else 'Custom Rate'
                end,
                'resolved_rate_component_count',case
                  when v_component_needs_resolution then 0
                  else 1
                end,
                'unresolved_taxable_count',case
                  when v_component_needs_resolution then 1
                  else 0
                end,
                'unresolved_taxable_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else 0
                end,
                'blocked_case_amount_ex_vat',0,
                'safe_amount_ex_vat',case
                  when v_resolution_pending then 0
                  else greatest(v_component_outstanding,0)
                end,
                'blocked_reason_codes',case
                  when v_component_needs_resolution
                    then jsonb_build_array('PAY_METHOD_RESOLUTION_REQUIRED')
                  when v_resolution_pending
                    then jsonb_build_array('LINKED_CORRECTION_RESOLUTION_PENDING')
                  when v_component_outstanding>0 then '[]'::jsonb
                  else jsonb_build_array('NO_PAY_HEADROOM')
                end
              ),
            'resolution_badge',case
              when v_component_needs_resolution then 'REQUIRES_RESOLUTION'
              when v_resolution_pending then 'WAITING'
              else 'RESOLVED'
            end,
            'resolution_state',case
              when v_component_needs_resolution then 'RESOLUTION_REQUIRED'
              when v_resolution_pending then 'WAITING_LINKED_RESOLUTION'
              else 'RESOLVED'
            end,
            'policy_x_pre_draft_key_resolved',not v_component_needs_resolution,
            'presentation_reason',case
              when v_component_needs_resolution then 'PAY_METHOD_RESOLUTION_REQUIRED'
              when v_resolution_pending then 'LINKED_CORRECTION_RESOLUTION_PENDING'
              when v_component_outstanding>0 then 'READY_TO_PAY'
              else 'NO_PAY_HEADROOM'
            end
          )
          || case
            when v_component_outstanding < 0
              then jsonb_build_object(
                'amount_ex_vat',0,
                -- Keep the unresolved recovery non-allocatable, but do not
                -- erase the amount whose pay-channel treatment still needs
                -- an operator decision.  Presentation is not draft authority.
                'amount_display',case
                  when v_component_needs_resolution
                    then round(
                      coalesce(
                        v_suggestion_current_basis,
                        abs(v_component_outstanding)
                      ),
                      2
                    )
                  else 0
                end,
                'preview_amount_ex_vat',0,
                'ready_preview_amount_ex_vat',0,
                'section_amount_ex_vat',0,
                'section_amount_display',case
                  when v_component_needs_resolution
                    then round(
                      coalesce(
                        v_suggestion_current_basis,
                        abs(v_component_outstanding)
                      ),
                      2
                    )
                  else 0
                end,
                'component_amount_ex_vat',0,
                'preview_component_amount_ex_vat',0,
                'target_pay_ex_vat',0,
                'nominal_due_amount_ex_vat',
                  round(abs(v_component_outstanding),2),
                'recoverable_this_pay_run_ex_vat',0,
                'preview_contract',
                  coalesce(
                    l.source_row_json->'preview_contract',
                    '{}'::jsonb
                  )
                  || jsonb_build_object(
                    'amount_ex_vat',0,
                    'selection_amount_ex_vat',0,
                    'key_type',v_component->>'component_key_type',
                    'key_value',v_component->>'component_key_value'
                  )
              )
            else jsonb_build_object(
              'amount_ex_vat',v_component_outstanding,
              'amount_display',v_component_outstanding,
              'preview_amount_ex_vat',v_component_outstanding,
              'ready_preview_amount_ex_vat',case
                when v_resolution_pending then 0
                else v_component_outstanding
              end,
              'section_amount_ex_vat',v_component_outstanding,
              'section_amount_display',v_component_outstanding,
              'component_amount_ex_vat',v_component_outstanding,
              'preview_component_amount_ex_vat',v_component_outstanding,
              'source_pay_method',v_source_pay_method,
              'target_pay_method',upper(v_component->>'target_pay_method'),
              'source_pay_ex_vat',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'source_amount_ex_vat',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'source_reservation_amount_ex_vat',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'remaining_source_amount',case
                when v_component_needs_resolution
                  then v_suggestion_current_basis
                else abs(
                  (v_component->>'effective_source_outstanding_ex_vat')::numeric
                )
              end,
              'target_pay_ex_vat',case
                when v_component_needs_resolution then null
                else v_component_outstanding
              end,
              'preview_contract',coalesce(l.source_row_json->'preview_contract','{}'::jsonb)||jsonb_build_object(
                'amount_ex_vat',v_component_outstanding,
                'selection_amount_ex_vat',case
                  when v_resolution_pending then 0
                  else v_component_outstanding
                end,
                'source_entitlement_amount_ex_vat',abs((v_component->>'truth_ex_vat')::numeric),
                'source_reservation_amount_ex_vat',case
                  when v_component_needs_resolution
                    then v_suggestion_current_basis
                  else abs(
                    (v_component->>'effective_source_outstanding_ex_vat')::numeric
                  )
                end,
                'key_type',v_component->>'component_key_type',
                'key_value',v_component->>'component_key_value',
                'line_key',v_line_key,
                'target_section',case
                  when v_resolution_pending then 'cases_resolutions'
                  when v_component_outstanding>0 then 'canonical_preview_lines'
                  else 'blocked_for_pay'
                end,
                'presentation_section',case
                  when v_resolution_pending then 'CASES_RESOLUTIONS'
                  when v_component_outstanding>0 then 'READY_TO_PAY'
                  else 'BLOCKED_FOR_PAY'
                end,
                'draftable',not v_resolution_pending and v_component_outstanding>0,
                'is_ready_for_draft',not v_resolution_pending and v_component_outstanding>0,
                'selection_allowed',not v_resolution_pending and v_component_outstanding>0,
                'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0,
                'reasons',case
                  when v_component_needs_resolution
                    then jsonb_build_array('PAY_METHOD_RESOLUTION_REQUIRED')
                  when v_resolution_pending
                    then jsonb_build_array('LINKED_CORRECTION_RESOLUTION_PENDING')
                  when v_component_outstanding>0 then '[]'::jsonb
                  else jsonb_build_array('NO_PAY_HEADROOM')
                end,
                'reason_count',case
                  when v_resolution_pending then 1
                  when v_component_outstanding>0 then 0
                  else 1
                end
              )
            )
          end,
          economic_key_json=jsonb_build_object(
            'timesheet_id',v_root_id::text,
            'key_type',v_component->>'component_key_type',
            'key_value',v_component->>'component_key_value'
          ),
          contract_json=coalesce(l.contract_json,'{}'::jsonb)||jsonb_build_object(
            'policy_x_authority_scope','PRE_DRAFT_CORRECTION_RESIDUAL',
            'residual_fingerprint',v_residual->>'residual_fingerprint',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'correction_identity_version','CORRECTION_CHAIN_V1',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint'
          ),
          updated_at_utc=coalesce(p_now_utc,now())
      where l.id=v_carrier_row_id;
      v_updated:=v_updated+1;
      v_carrier_row_ids:=array_append(v_carrier_row_ids,v_carrier_row_id);

      update public.banking_pay_workbench_candidate_source_lines l
      set status='SUPERSEDED',updated_at_utc=coalesce(p_now_utc,now())
      where l.session_id=p_session_id and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
        and l.id<>v_carrier_row_id and l.timesheet_id=any(v_member_ids)
        and upper(coalesce(l.economic_key_json->>'key_type',''))=upper(coalesce(v_component->>'component_key_type',''))
        and coalesce(l.economic_key_json->>'key_value','')=coalesce(v_component->>'component_key_value','');
      get diagnostics v_row_count = row_count;
      v_superseded := v_superseded + v_row_count;
    end loop;

    -- Remove the case-level TS_TOTAL recovery parent only when every nested
    -- component belongs to this correction chain and now has its own exact
    -- finance-backed carrier.  Mixed or unrelated finance cases remain intact.
    update public.banking_pay_workbench_candidate_source_lines aggregate_line
    set status='SUPERSEDED',
        updated_at_utc=coalesce(p_now_utc,now())
    where aggregate_line.session_id=p_session_id
      and aggregate_line.candidate_id=p_candidate_id
      and aggregate_line.source_build_run_id=p_source_build_run_id
      and aggregate_line.status='CURRENT'
      and aggregate_line.timesheet_id=any(v_member_ids)
      and upper(coalesce(
            aggregate_line.economic_key_json->>'key_type',
            ''
          ))='TS_TOTAL'
      and nullif(
            btrim(coalesce(
              aggregate_line.source_row_json->>'finance_case_id',
              ''
            )),
            ''
          ) is not null
      and jsonb_typeof(
            aggregate_line.source_row_json->'case_components'
          )='array'
      and jsonb_array_length(
            aggregate_line.source_row_json->'case_components'
          )>0
      and not exists (
        select 1
        from jsonb_array_elements(
          aggregate_line.source_row_json->'case_components'
        ) nested_component(value)
        where coalesce(
                nested_component.value->>'source_family_key',
                ''
              )<>coalesce(v_residual->>'source_family_key','')
           or not exists (
             select 1
             from public.banking_pay_workbench_candidate_source_lines
                    exact_component_line
             where exact_component_line.session_id=p_session_id
               and exact_component_line.candidate_id=p_candidate_id
               and exact_component_line.source_build_run_id
                     =p_source_build_run_id
               and exact_component_line.status='CURRENT'
               and exact_component_line.timesheet_id=any(v_member_ids)
               and upper(coalesce(
                     exact_component_line.economic_key_json->>'key_type',
                     ''
                   ))=upper(coalesce(
                     nested_component.value->>'component_key_type',
                     ''
                   ))
               and coalesce(
                     exact_component_line.economic_key_json->>'key_value',
                     ''
                   )=coalesce(
                     nested_component.value->>'component_key_value',
                     ''
                   )
               and nullif(
                     btrim(coalesce(
                       exact_component_line.source_row_json
                         ->>'finance_case_id',
                       ''
                     )),
                     ''
                   ) is not null
           )
      );
    get diagnostics v_row_count = row_count;
    v_superseded:=v_superseded+v_row_count;

    -- A correction chain is one coupled economic unit. Once its dated
    -- component carriers have been materialised, no raw member row may remain
    -- current merely because it used a broader TS_TOTAL key. Independent
    -- expenses and additional codes retain their own non-TS_TOTAL component
    -- keys and are not affected by this lineage suppression.
    update public.banking_pay_workbench_candidate_source_lines l
    set status='SUPERSEDED',updated_at_utc=coalesce(p_now_utc,now())
    where l.session_id=p_session_id and l.candidate_id=p_candidate_id
      and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
      and l.timesheet_id=any(v_member_ids)
      and l.section in ('cases_resolutions','canonical_preview_lines')
      and upper(coalesce(l.economic_key_json->>'key_type',''))='TS_TOTAL'
      and not exists (
        select 1
        from unnest(coalesce(v_carrier_row_ids,array[]::uuid[])) as retained_carrier(carrier_row_id)
        where retained_carrier.carrier_row_id=l.id
      );
    get diagnostics v_row_count = row_count;
    v_superseded:=v_superseded+v_row_count;
  end loop;
  return jsonb_build_object('ok',true,'residual_count',jsonb_array_length(v_residuals),
    'materialised_component_count',v_updated,'superseded_raw_member_row_count',v_superseded);
end;
$function$;

create or replace function public._ctms_enrich_correction_resolution_payload_v1(
  p_session_id uuid,
  p_payload jsonb
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_candidate uuid;
  v_timesheet uuid;
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
begin
  perform public._ctms_assert_payload_corrections_fresh_v1(v_payload,'PAY_CASE_RESOLUTION');
  v_candidate:=nullif(v_payload->>'candidate_id','')::uuid;
  select id into v_timesheet from unnest(public._ctms_payload_timesheet_ids_v1(v_payload,100)) x(id) limit 1;
  if v_candidate is null or v_timesheet is null then
    return v_payload;
  end if;
  v_residuals:=public._ctms_candidate_correction_residuals_v1(
    p_session_id,v_candidate,null::uuid,'PAY_CASE_RESOLUTION'
  );
  select residual.value
  into v_residual
  from jsonb_array_elements(v_residuals) as residual(value)
  where exists (
    select 1
    from jsonb_array_elements_text(
      coalesce(residual.value->'member_timesheet_ids','[]'::jsonb)
    ) as member(member_id)
    where member.member_id=v_timesheet::text
  )
  limit 1;
  if v_residual is null or jsonb_typeof(v_residual)<>'object' then
    -- A correction root is not itself labelled as a replacement/reversal, but
    -- it is still an authoritative member of its residual chain.  Membership
    -- above is therefore the primary gate.  Preserve ordinary timesheets
    -- unchanged; only a row explicitly classified as an import correction
    -- must fail closed when its residual cannot be found.
    if coalesce((
         public._ctms_import_correction_classify_v1(v_timesheet)
           ->>'is_import_authoritative_correction'
       )::boolean,false) then
      raise exception 'CORRECTION_RESIDUAL_REQUIRED_FOR_CASE_RESOLUTION'
        using errcode='P0001';
    end if;
    return v_payload;
  end if;

  select component.value
  into v_component
  from jsonb_array_elements(
    coalesce(v_residual->'components','[]'::jsonb)
  ) component(value)
  where upper(component.value->>'component_key_type')=upper(coalesce(
      v_payload->>'component_key_type',
      v_payload#>>'{bucket_resolutions,0,component_key_type}',
      v_payload#>>'{bucket_resolutions,0,key_type}',
      ''
    ))
    and component.value->>'component_key_value'=coalesce(
      v_payload->>'component_key_value',
      v_payload#>>'{bucket_resolutions,0,component_key_value}',
      v_payload#>>'{bucket_resolutions,0,key_value}',
      ''
    )
  limit 1;

  if v_component is null or jsonb_typeof(v_component)<>'object' then
    raise exception 'CORRECTION_COMPONENT_REQUIRED_FOR_CASE_RESOLUTION'
      using errcode='P0001',
            detail=jsonb_build_object(
              'candidate_id',v_candidate,
              'timesheet_id',v_timesheet,
              'component_key_type',coalesce(
                v_payload->>'component_key_type',
                v_payload#>>'{bucket_resolutions,0,component_key_type}',
                v_payload#>>'{bucket_resolutions,0,key_type}'
              ),
              'component_key_value',coalesce(
                v_payload->>'component_key_value',
                v_payload#>>'{bucket_resolutions,0,component_key_value}',
                v_payload#>>'{bucket_resolutions,0,key_value}'
              )
            )::text;
  end if;

  return v_payload||jsonb_build_object(
    'resolution_identity_key',v_component->>'canonical_correction_key',
    'resolution_identity_version','CORRECTION_CHAIN_V1',
    'canonical_correction_key',v_component->>'canonical_correction_key',
    'resolution_economic_fingerprint',
      v_component->>'resolution_economic_fingerprint',
    'correction_root_id',v_residual->>'root_timesheet_id',
    'ordered_member_timesheet_ids',
      v_residual->'ordered_member_timesheet_ids',
    'component_lineage_fingerprint',
      v_component->>'component_lineage_fingerprint',
    'source_family_key',v_residual->>'source_family_key',
    'correction_financials_policy_envelope',v_residual->'correction_financials_policy_envelope',
    'correction_financials_policy_envelope_fingerprint',v_residual->>'correction_financials_policy_envelope_fingerprint',
    'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
    'correction_chain_fingerprint',v_residual->>'chain_fingerprint'
  );
end;
$function$;

create or replace function public._ctms_normalise_correction_case_resolutions_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_anchor_timesheet_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_resolution public.banking_pay_workbench_session_case_resolutions%rowtype;
  v_bucket jsonb;
  v_source_amount numeric;
  v_original_source_amount numeric;
  v_original_target_amount numeric;
  v_target_amount numeric;
  v_source_rate numeric;
  v_source_units numeric;
  v_component_timesheet_id uuid;
  v_canonical_resolution_id uuid;
  v_normalised_resolution_id uuid;
  v_alias_deleted_count integer:=0;
  v_alias_deleted_this_component integer:=0;
  v_updated integer:=0;
  v_normalised_resolution_ids jsonb:='[]'::jsonb;
  v_normalised_resolution_identity_keys jsonb:='[]'::jsonb;
begin
  if p_session_id is null
     or p_candidate_id is null
     or p_anchor_timesheet_id is null
     or coalesce((public._ctms_import_correction_classify_v1(
       p_anchor_timesheet_id
     )->>'is_import_authoritative_correction')::boolean,false) is not true then
    return jsonb_build_object('ok',true,'normalised_count',0);
  end if;

  v_residuals:=public._ctms_candidate_correction_residuals_v1(
    p_session_id,p_candidate_id,null::uuid,'PAY_CASE_RESOLUTION_NORMALISE'
  );

  select residual.value
  into v_residual
  from jsonb_array_elements(v_residuals) as residual(value)
  where exists (
    select 1
    from jsonb_array_elements_text(
      coalesce(residual.value->'member_timesheet_ids','[]'::jsonb)
    ) as member(member_id)
    where member.member_id=p_anchor_timesheet_id::text
  )
  limit 1;

  if v_residual is null or jsonb_typeof(v_residual)<>'object' then
    raise exception 'CORRECTION_RESIDUAL_REQUIRED_FOR_CASE_RESOLUTION'
      using errcode='P0001';
  end if;

  select coalesce(array_agg(member_id::uuid order by member_id),array[]::uuid[])
  into v_member_ids
  from jsonb_array_elements_text(
    coalesce(v_residual->'member_timesheet_ids','[]'::jsonb)
  ) as member(member_id);

  for v_component in
    select component.value
    from jsonb_array_elements(
      coalesce(v_residual->'components','[]'::jsonb)
    ) as component(value)
    where coalesce((component.value->>'resolution_required')::boolean,false)
      and round(coalesce(
        nullif(component.value->>'effective_source_outstanding_ex_vat','')::numeric,
        0
      ),2)<>0
    order by component.value->>'component_key_type',
             component.value->>'component_key_value'
  loop
    v_resolution:=null;
    v_canonical_resolution_id:=null;
    v_normalised_resolution_id:=null;
    select resolution_row.*
    into v_resolution
    from public.banking_pay_workbench_session_case_resolutions as resolution_row
    where resolution_row.session_id=p_session_id
      and resolution_row.candidate_id=p_candidate_id
      and resolution_row.resolution_family='BUCKETED'
      and resolution_row.timesheet_id=any(v_member_ids)
      and upper(btrim(coalesce(resolution_row.component_key_type,'')))
          =upper(btrim(coalesce(v_component->>'component_key_type','')))
      and btrim(coalesce(resolution_row.component_key_value,''))
          =btrim(coalesce(v_component->>'component_key_value',''))
    order by resolution_row.updated_at_utc desc,
             resolution_row.created_at_utc desc,
             resolution_row.id desc
    limit 1
    for update;

    if v_resolution.id is null then
      -- A durable carry registered for this exact canonical component is the
      -- authoritative pending decision source.  Do not manufacture a linked
      -- decision from another dated component while that carry is still
      -- waiting to be replayed.  Otherwise the synthetic row wins the unique
      -- session/key constraint and the genuine carried decision is
      -- incorrectly superseded.
      if exists (
        select 1
        from public.banking_pay_workbench_case_resolution_carry_registrations
          as pending_carry
        where pending_carry.target_session_id=p_session_id
          and pending_carry.candidate_id=p_candidate_id
          and pending_carry.status='PENDING'
          and pending_carry.resolution_scope_kind='CORRECTION_COMPONENT'
          and pending_carry.canonical_resolution_key=
            v_component->>'canonical_correction_key'
      ) then
        continue;
      end if;

      -- A correction chain can contain a financially material component that
      -- has no standalone workbench preview row (for example, the historical
      -- carrier day of a paired reversal).  "Resolve linked work" must cover
      -- that component too; otherwise the preview can look complete while
      -- draft seeding correctly rejects the incomplete chain.  Clone only a
      -- decision from the same fingerprinted source family, then bind the new
      -- row to this component's own current source basis below.
      select resolution_row.*
      into v_resolution
      from public.banking_pay_workbench_session_case_resolutions as resolution_row
      where resolution_row.session_id=p_session_id
        and resolution_row.candidate_id=p_candidate_id
        and resolution_row.resolution_family='BUCKETED'
        and resolution_row.timesheet_id=any(v_member_ids)
        and resolution_row.source_family_key=v_residual->>'source_family_key'
      order by resolution_row.updated_at_utc desc,
               resolution_row.created_at_utc desc,
               resolution_row.id desc
      limit 1
      for update;

      if v_resolution.id is null then
        raise exception 'CORRECTION_CHAIN_RESOLUTION_ROW_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object(
                  'session_id',p_session_id,
                  'candidate_id',p_candidate_id,
                  'source_family_key',v_residual->>'source_family_key',
                  'component_key_type',v_component->>'component_key_type',
                  'component_key_value',v_component->>'component_key_value'
                )::text;
      end if;

      v_component_timesheet_id:=nullif(
        btrim(coalesce(v_component->>'carrier_timesheet_id','')),
        ''
      )::uuid;
      if v_component_timesheet_id is null
         or v_component_timesheet_id<>all(v_member_ids) then
        raise exception 'CORRECTION_CHAIN_RESOLUTION_CARRIER_ID_REQUIRED'
          using errcode='P0001',
                detail=jsonb_build_object(
                  'session_id',p_session_id,
                  'candidate_id',p_candidate_id,
                  'source_family_key',v_residual->>'source_family_key',
                  'component_key_type',v_component->>'component_key_type',
                  'component_key_value',v_component->>'component_key_value'
                )::text;
      end if;

      insert into public.banking_pay_workbench_session_case_resolutions (
        session_id,
        candidate_id,
        case_key,
        resolution_family,
        resolution_identity_key,
        timesheet_id,
        source_basis_fingerprint,
        source_family_key,
        bucket_code,
        component_key_type,
        component_key_value,
        payload_json,
        created_at_utc,
        updated_at_utc
      )
      values (
        p_session_id,
        p_candidate_id,
        'timesheet:'||v_component_timesheet_id::text,
        'BUCKETED',
        v_component->>'canonical_correction_key',
        v_component_timesheet_id,
        v_component->>'source_basis_fingerprint',
        v_residual->>'source_family_key',
        v_resolution.bucket_code,
        upper(v_component->>'component_key_type'),
        v_component->>'component_key_value',
        coalesce(v_resolution.payload_json,'{}'::jsonb)
          ||jsonb_build_object(
            'linked_timesheet_id',v_component_timesheet_id::text,
            'timesheet_id',v_component_timesheet_id::text,
            'case_key','timesheet:'||v_component_timesheet_id::text,
            'applied_via_linked_scope',true,
            'source_anchor_case_key',v_resolution.case_key
          ),
        now(),
        now()
      )
      on conflict (session_id,resolution_identity_key)
      do update
      set source_basis_fingerprint=excluded.source_basis_fingerprint,
          source_family_key=excluded.source_family_key,
          component_key_type=excluded.component_key_type,
          component_key_value=excluded.component_key_value,
          payload_json=excluded.payload_json,
          updated_at_utc=now()
      returning public.banking_pay_workbench_session_case_resolutions.*
      into v_resolution;
    end if;

    v_bucket:=coalesce(v_resolution.payload_json#>'{bucket_resolutions,0}','{}'::jsonb);
    select canonical_resolution.id
    into v_canonical_resolution_id
    from public.banking_pay_workbench_session_case_resolutions
      as canonical_resolution
    where canonical_resolution.session_id=p_session_id
      and canonical_resolution.candidate_id=p_candidate_id
      and canonical_resolution.resolution_identity_key=
        v_component->>'canonical_correction_key'
    order by canonical_resolution.updated_at_utc desc,
             canonical_resolution.created_at_utc desc,
             canonical_resolution.id desc
    limit 1
    for update;
    v_normalised_resolution_id:=coalesce(
      v_canonical_resolution_id,
      v_resolution.id
    );
    v_component_timesheet_id:=coalesce(
      nullif(btrim(coalesce(v_component->>'carrier_timesheet_id','')),'')::uuid,
      case
        when btrim(coalesce(v_resolution.case_key,'')) ~*
             '^timesheet:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then substring(btrim(v_resolution.case_key) from 11)::uuid
        else v_resolution.timesheet_id
      end
    );
    if v_component_timesheet_id is null
       or v_component_timesheet_id<>all(v_member_ids) then
      raise exception 'CORRECTION_CHAIN_RESOLUTION_MEMBER_ID_REQUIRED'
        using errcode='P0001',
              detail=jsonb_build_object(
                'session_id',p_session_id,
                'candidate_id',p_candidate_id,
                'case_key',v_resolution.case_key
              )::text;
    end if;
    v_source_amount:=abs(round(
      (v_component->>'effective_source_outstanding_ex_vat')::numeric,2
    ));
    v_original_source_amount:=abs(coalesce(
      nullif(v_bucket->>'source_pay_ex_vat','')::numeric,
      nullif(v_bucket#>>'{saved_resolution_result_json,source_pay_ex_vat}','')::numeric,
      0
    ));
    v_original_target_amount:=abs(coalesce(
      nullif(v_bucket->>'target_pay_ex_vat','')::numeric,
      nullif(v_bucket->>'target_amount_ex_vat','')::numeric,
      nullif(v_bucket#>>'{saved_resolution_result_json,target_pay_ex_vat}','')::numeric,
      nullif(v_bucket#>>'{saved_resolution_result_json,target_amount_ex_vat}','')::numeric,
      0
    ));
    v_source_rate:=abs(coalesce(nullif(v_bucket->>'source_rate','')::numeric,0));

    if v_original_source_amount<=0 or v_original_target_amount<=0 then
      raise exception 'CORRECTION_CHAIN_RESOLUTION_CONVERSION_BASIS_REQUIRED'
        using errcode='P0001',
              detail=jsonb_build_object(
                'session_id',p_session_id,
                'candidate_id',p_candidate_id,
                'component_key_value',v_component->>'component_key_value'
              )::text;
    end if;

    v_target_amount:=round(
      v_source_amount*(v_original_target_amount/v_original_source_amount),2
    );
    v_source_units:=case
      when v_source_rate>0 then round(v_source_amount/v_source_rate,6)
      else round(
        coalesce(nullif(v_bucket->>'source_units','')::numeric,0)
        *(v_source_amount/v_original_source_amount),6
      )
    end;

    v_bucket:=v_bucket
      ||jsonb_build_object(
        'timesheet_id',v_component_timesheet_id::text,
        'source_family_key',v_residual->>'source_family_key',
        'source_basis_fingerprint',v_component->>'source_basis_fingerprint',
        'source_basis_json',
          coalesce(v_bucket->'source_basis_json','{}'::jsonb)
          ||jsonb_build_object(
            'source_family_key',v_residual->>'source_family_key',
            'resolution_identity_key',
              v_component->>'canonical_correction_key',
            'resolution_identity_version','CORRECTION_CHAIN_V1',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint',
            'correction_root_id',v_residual->>'root_timesheet_id',
            'ordered_member_timesheet_ids',
              v_residual->'ordered_member_timesheet_ids',
            'component_lineage_fingerprint',
              v_component->>'component_lineage_fingerprint',
            'root_timesheet_id',v_residual->>'root_timesheet_id',
            'component_key_type',v_component->>'component_key_type',
            'component_key_value',v_component->>'component_key_value',
            'effective_source_outstanding_ex_vat',
              (v_component->>'effective_source_outstanding_ex_vat')::numeric,
            'correction_chain_fingerprint',v_residual->>'chain_fingerprint',
            'correction_chain_residual_fingerprint',
              v_residual->>'residual_fingerprint',
            'correction_financials_policy_envelope_fingerprint',
              v_residual->>'correction_financials_policy_envelope_fingerprint'
          ),
        'source_units',v_source_units,
        'target_units',v_source_units,
        'source_pay_ex_vat',v_source_amount,
        'target_amount_ex_vat',v_target_amount,
        'target_pay_ex_vat',v_target_amount,
        'saved_resolution_result_json',
          coalesce(v_bucket->'saved_resolution_result_json','{}'::jsonb)
          ||jsonb_build_object(
            'source_pay_ex_vat',v_source_amount,
            'target_units',v_source_units,
            'target_amount_ex_vat',v_target_amount,
            'target_pay_ex_vat',v_target_amount
          ),
        'correction_chain_fingerprint',v_residual->>'chain_fingerprint',
        'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
        'correction_financials_policy_envelope_fingerprint',
          v_residual->>'correction_financials_policy_envelope_fingerprint'
      );

    -- A previously frozen batch may have been created from an older decision
    -- carrying this same stable canonical identity.  The frozen batch retains
    -- its own immutable payload, while the open workbench session must reuse
    -- the one canonical pre-draft row for the component.  Updating that row
    -- avoids a unique-key collision and prevents a second active decision for
    -- the same economic component.
    update public.banking_pay_workbench_session_case_resolutions
    set timesheet_id=v_component_timesheet_id,
        resolution_identity_key=
          v_component->>'canonical_correction_key',
        source_basis_fingerprint=v_component->>'source_basis_fingerprint',
        source_family_key=v_residual->>'source_family_key',
        component_key_type=upper(v_component->>'component_key_type'),
        component_key_value=v_component->>'component_key_value',
        payload_json=coalesce(v_resolution.payload_json,'{}'::jsonb)
          ||jsonb_build_object(
            'linked_timesheet_id',v_component_timesheet_id::text,
            'timesheet_id',v_component_timesheet_id::text,
            'source_family_key',v_residual->>'source_family_key',
            'resolution_identity_key',
              v_component->>'canonical_correction_key',
            'resolution_identity_version','CORRECTION_CHAIN_V1',
            'canonical_correction_key',
              v_component->>'canonical_correction_key',
            'resolution_economic_fingerprint',
              v_component->>'resolution_economic_fingerprint',
            'correction_root_id',v_residual->>'root_timesheet_id',
            'ordered_member_timesheet_ids',
              v_residual->'ordered_member_timesheet_ids',
            'component_lineage_fingerprint',
              v_component->>'component_lineage_fingerprint',
            -- Keep the canonical component result available both at the
            -- resolution row boundary and in its detailed bucket.  The
            -- correction residual reader accepts both shapes so decisions
            -- saved before this repeatable was installed remain valid.
            'target_pay_method',v_bucket->>'target_pay_method',
            'target_amount_ex_vat',v_target_amount,
            'target_pay_ex_vat',v_target_amount,
            'saved_resolution_result_json',
              v_bucket->'saved_resolution_result_json',
            'correction_chain_fingerprint',v_residual->>'chain_fingerprint',
            'correction_chain_residual_fingerprint',
              v_residual->>'residual_fingerprint',
            'correction_financials_policy_envelope',
              v_residual->'correction_financials_policy_envelope',
            'correction_financials_policy_envelope_fingerprint',
              v_residual->>'correction_financials_policy_envelope_fingerprint',
            'bucket_resolutions',jsonb_build_array(v_bucket)
          ),
        updated_at_utc=now()
    where id=v_normalised_resolution_id;

    -- Remove only temporary per-member aliases for this exact correction
    -- component.  Independent timesheets, expenses, additional codes and
    -- other component dates remain outside this bounded predicate.
    delete from public.banking_pay_workbench_session_case_resolutions
      as alias_resolution
    where alias_resolution.session_id=p_session_id
      and alias_resolution.candidate_id=p_candidate_id
      and alias_resolution.resolution_family='BUCKETED'
      and alias_resolution.id<>v_normalised_resolution_id
      and alias_resolution.timesheet_id=any(v_member_ids)
      and upper(btrim(coalesce(alias_resolution.component_key_type,'')))=
        upper(btrim(coalesce(v_component->>'component_key_type','')))
      and btrim(coalesce(alias_resolution.component_key_value,''))=
        btrim(coalesce(v_component->>'component_key_value',''));
    get diagnostics v_alias_deleted_this_component=row_count;
    v_alias_deleted_count:=
      v_alias_deleted_count+v_alias_deleted_this_component;
    v_normalised_resolution_ids:=
      v_normalised_resolution_ids
      ||jsonb_build_array(v_normalised_resolution_id::text);
    v_normalised_resolution_identity_keys:=
      v_normalised_resolution_identity_keys
      ||jsonb_build_array(v_component->>'canonical_correction_key');
    v_updated:=v_updated+1;
  end loop;

  return jsonb_build_object(
    'ok',true,
    'normalised_count',v_updated,
    'alias_deleted_count',v_alias_deleted_count,
    'case_resolution_id',
      case
        when jsonb_array_length(v_normalised_resolution_ids)>0
          then v_normalised_resolution_ids->>0
        else null
      end,
    'case_resolution_ids',v_normalised_resolution_ids,
    'resolution_identity_keys',v_normalised_resolution_identity_keys,
    'source_family_key',v_residual->>'source_family_key',
    'root_timesheet_id',v_residual->>'root_timesheet_id'
  );
end;
$function$;

create or replace function public._ctms_clear_correction_chain_snoozes_v1(
  p_snooze_id uuid,
  p_actor_user_id uuid
)
returns integer
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_row public.pay_item_snoozes%rowtype;
  v_chain jsonb;
  v_source_ref text;
  v_count integer:=0;
begin
  select * into v_row from public.pay_item_snoozes where id=p_snooze_id for update;
  if not found or v_row.timesheet_id is null
     or coalesce((public._ctms_import_correction_classify_v1(v_row.timesheet_id)
       ->>'is_import_authoritative_correction')::boolean,false) is not true then return 0; end if;
  v_chain:=public.timesheet_correction_chain_scope_v1(v_row.timesheet_id,false,32,100);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    raise exception 'CORRECTION_CHAIN_INVALID_FOR_SNOOZE_CLEAR' using errcode='P0001',detail=v_chain::text;
  end if;
  v_source_ref:='correction-chain:'||(v_chain->>'root_timesheet_id');
  update public.pay_item_snoozes s set
    cleared_at_utc=coalesce(s.cleared_at_utc,now()),
    cleared_by_user_id=coalesce(s.cleared_by_user_id,p_actor_user_id),
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where s.candidate_id=v_row.candidate_id and s.cleared_at_utc is null and s.cancelled_at_utc is null
    and (s.id=p_snooze_id or s.segment_stable_key=v_source_ref or s.source_ref=v_source_ref);
  get diagnostics v_count=row_count;
  return v_count;
end;
$function$;

create or replace function public._ctms_invoice_week_candidate_ids_v1(
  p_client_id uuid,
  p_invoice_week_start date,
  p_max_members integer default 100
)
returns uuid[]
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v_ids uuid[];
begin
  if p_client_id is null or p_invoice_week_start is null then
    return array[]::uuid[];
  end if;
  if p_max_members < 1 or p_max_members > 100 then
    raise exception 'INVOICE_CORRECTION_SCOPE_LIMIT_INVALID' using errcode='22023';
  end if;
  select coalesce(array_agg(x.timesheet_id order by x.timesheet_id),array[]::uuid[])
  into v_ids
  from (
    select distinct tf.timesheet_id
    from public.timesheets_financials tf
    join public.timesheets ts on ts.timesheet_id=tf.timesheet_id and ts.is_current=true
    where tf.is_current=true
      and tf.client_id=p_client_id
      and tf.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and ts.revoked_at is null
      and coalesce((public._ctms_import_correction_classify_v1(tf.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
      and (
        (ts.week_ending_date::date - 6)=p_invoice_week_start
        or exists (
          select 1
          from jsonb_array_elements(
            case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
              then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end
          ) seg
          where nullif(btrim(coalesce(seg->>'invoice_target_week_start','')),'')::date
            = p_invoice_week_start
        )
      )
    limit p_max_members + 1
  ) x;
  if cardinality(v_ids)>p_max_members then
    raise exception 'INVOICE_CORRECTION_SCOPE_LIMIT_EXCEEDED' using errcode='22023';
  end if;
  return v_ids;
end;
$function$;

create or replace function public._ctms_invoice_payload_has_financial_edit_v1(p_payload jsonb)
returns boolean
language sql
immutable
security definer
set search_path to 'public', 'pg_temp'
as $function$
  select coalesce(p_payload::text, '') ~* '"(lines|line_edits|invoice_lines|timesheet_id|shift_id|source_key|vat_rate_pct|vat_amount|subtotal_ex_vat|total_inc_vat|total_charge_ex_vat|total_pay_ex_vat|margin_ex_vat|hours_day|hours_night|hours_sat|hours_sun|hours_bh|pay_day|pay_night|pay_sat|pay_sun|pay_bh|charge_day|charge_night|charge_sat|charge_sun|charge_bh|amount|rate|remove|delete)"';
$function$;

create or replace function public._ctms_assert_invoice_mutable_draft_v1(
  p_invoice_id uuid,
  p_context text default 'IMPORT_CORRECTION',
  p_lock_row boolean default true
)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v_invoice public.invoices%rowtype;
begin
  if not exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;
  if p_lock_row then
    select * into v_invoice from public.invoices where id=p_invoice_id for update;
  else
    select * into v_invoice from public.invoices where id=p_invoice_id;
  end if;
  if not found then raise exception 'INVOICE_NOT_FOUND' using errcode='P0001'; end if;
  if upper(coalesce(v_invoice.status::text,'')) <> 'DRAFT' or v_invoice.issued_at_utc is not null then
    raise exception 'POLICY_X_FROZEN_INVOICE_NOT_EDITABLE'
      using errcode='P0001', detail=jsonb_build_object('context',p_context,'invoice_id',p_invoice_id,'status',v_invoice.status)::text;
  end if;
end;
$function$;

create or replace function public._ctms_assert_correction_invoice_scope_v1(
  p_timesheet_ids uuid[],
  p_target_invoice_id uuid default null,
  p_actor_user_id uuid default null,
  p_require_complete_selection boolean default false,
  p_require_appendable boolean default false,
  p_lock_rows boolean default false,
  p_context text default 'IMPORT_CORRECTION'
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_input uuid[];
  v_id uuid;
  v_scope jsonb;
  v_members uuid[];
  v_scopes jsonb := '[]'::jsonb;
  v_already boolean;
begin
  select coalesce(array_agg(distinct id order by id), array[]::uuid[]) into v_input
  from unnest(coalesce(p_timesheet_ids,array[]::uuid[])) x(id) where id is not null;
  foreach v_id in array v_input loop
    if coalesce((public._ctms_import_correction_classify_v1(v_id)
      ->> 'is_import_authoritative_correction')::boolean, false) is not true then continue; end if;
    v_scope := public.invoice_correction_pair_scope_v1(
      v_id, p_target_invoice_id, p_actor_user_id, p_lock_rows, 100
    );
    if coalesce((v_scope->>'valid')::boolean,false) is not true then
      raise exception 'INVOICE_CORRECTION_SCOPE_INVALID' using errcode='P0001',detail=v_scope::text;
    end if;
    select coalesce(array_agg(value::uuid order by value),array[]::uuid[]) into v_members
    from jsonb_array_elements_text(v_scope->'pair_timesheet_ids') value;
    v_already := coalesce((v_scope->>'existing_line_member_count')::integer,0)
      = coalesce((v_scope->>'expected_member_count')::integer,0)
      and coalesce((v_scope->>'existing_line_invoice_count')::integer,0)=1;
    if p_require_complete_selection and not v_already and not (v_members <@ v_input) then
      raise exception 'INVOICE_CORRECTION_UNIT_MUST_BE_SELECTED_TOGETHER'
        using errcode='P0001',detail=jsonb_build_object('context',p_context,'selected',to_jsonb(v_input),'required',to_jsonb(v_members))::text;
    end if;
    if p_require_appendable and not v_already
       and coalesce((v_scope->>'target_appendable')::boolean,false) is not true then
      raise exception 'INVOICE_CORRECTION_UNIT_NOT_APPENDABLE' using errcode='P0001',detail=v_scope::text;
    end if;
    v_scopes := v_scopes || jsonb_build_array(v_scope);
  end loop;
  return jsonb_build_object('ok',true,'context',p_context,'scopes',v_scopes);
end;
$function$;

create or replace function public._ctms_assert_invoice_correction_lines_v1(
  p_invoice_id uuid,
  p_actor_user_id uuid default null,
  p_lock_rows boolean default false,
  p_context text default 'IMPORT_CORRECTION'
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare v_id uuid; v_scope jsonb; v_scopes jsonb:='[]'::jsonb;
begin
  for v_id in
    select distinct il.timesheet_id from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  loop
    v_scope := public.invoice_correction_pair_scope_v1(v_id,p_invoice_id,p_actor_user_id,p_lock_rows,100);
    if coalesce((v_scope->>'valid')::boolean,false) is not true
       or (case when upper(coalesce(p_context,''))='INVOICE_APPLY_EDITS_RESULT'
          then coalesce(v_scope->>'placement_state','MALFORMED_PAIR') not in (
            'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE','UNPLACED')
          else coalesce(v_scope->>'placement_state','MALFORMED_PAIR') not in (
            'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES') end) then
      raise exception 'INVOICE_CORRECTION_LINES_NOT_UNIT_SAFE' using errcode='P0001',detail=v_scope::text;
    end if;
    v_scopes:=v_scopes||jsonb_build_array(v_scope);
  end loop;
  return jsonb_build_object('ok',true,'invoice_id',p_invoice_id,'scopes',v_scopes);
end;
$function$;

create or replace function public._ctms_assert_invoice_can_unissue_v1(
  p_invoice_id uuid,
  p_lock_row boolean default true,
  p_context text default 'IMPORT_CORRECTION'
)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v_invoice public.invoices%rowtype; v_credit_count integer;
begin
  if not exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;
  if p_lock_row then select * into v_invoice from public.invoices where id=p_invoice_id for update;
  else select * into v_invoice from public.invoices where id=p_invoice_id; end if;
  if not found then raise exception 'INVOICE_NOT_FOUND' using errcode='P0001'; end if;
  select count(*)::integer into v_credit_count from public.invoices i
  where i.original_invoice_id=p_invoice_id and upper(coalesce(i.type::text,''))='CREDIT_NOTE';
  if v_invoice.paid_at_utc is not null or upper(coalesce(v_invoice.status::text,''))='PAID'
     or v_invoice.credit_note_created_at_utc is not null or v_credit_count>0 then
    raise exception 'INVOICE_UNISSUE_BLOCKED_BY_DOWNSTREAM_AUTHORITY'
      using errcode='P0001',detail=jsonb_build_object('context',p_context,'invoice_id',p_invoice_id)::text;
  end if;
end;
$function$;

revoke all on function public._ctms_assert_pay_batch_mutable_v1(uuid,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_candidate_correction_residuals_v1(uuid,uuid,uuid,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_rewrite_source_build_correction_negative_components_v1(uuid,uuid,uuid[]) from public,anon,authenticated,service_role;
revoke all on function public._ctms_rewrite_sync_authoritative_correction_negative_components_v1(uuid,uuid,uuid[]) from public,anon,authenticated,service_role;
revoke all on function public._ctms_rewrite_sync_correction_cases_v1(uuid,uuid[],uuid[]) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_payload_corrections_fresh_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_session_correction_residuals_draftable_v1(uuid,jsonb,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_materialise_candidate_correction_residuals_v1(uuid,uuid,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public._ctms_enrich_correction_resolution_payload_v1(uuid,jsonb) from public,anon,authenticated,service_role;
revoke all on function public._ctms_normalise_correction_case_resolutions_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public._ctms_clear_correction_chain_snoozes_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public._ctms_invoice_week_candidate_ids_v1(uuid,date,integer) from public,anon,authenticated,service_role;
revoke all on function public._ctms_invoice_payload_has_financial_edit_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_invoice_mutable_draft_v1(uuid,text,boolean) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_correction_invoice_scope_v1(uuid[],uuid,uuid,boolean,boolean,boolean,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_invoice_correction_lines_v1(uuid,uuid,boolean,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_invoice_can_unissue_v1(uuid,boolean,text) from public,anon,authenticated,service_role;
