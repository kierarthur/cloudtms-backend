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
as $function$
declare
  v_result jsonb := '[]'::jsonb;
  v_seen_roots uuid[] := array[]::uuid[];
  v_chain jsonb;
  v_root uuid;
  v_residual jsonb;
  r record;
begin
  if p_candidate_id is null then return v_result; end if;
  for r in
    select distinct tf.timesheet_id,
      case when upper(btrim(coalesce(tf.pay_method, ''))) = 'UMBRELLA' then 'UMBRELLA' else 'PAYE' end as target_pay_method
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
      r.timesheet_id, p_candidate_id, r.target_pay_method,
      p_session_id, p_exclude_pay_batch_id, 100
    );
    v_result := v_result || jsonb_build_array(v_residual);
  end loop;
  return v_result;
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
as $function$
declare
  v_residuals jsonb;
  v_residual jsonb;
  v_component jsonb;
  v_member_ids uuid[];
  v_root_id uuid;
  v_carrier_row_id uuid;
  v_line_key text;
  v_updated integer := 0;
  v_superseded integer := 0;
  v_row_count integer := 0;
begin
  v_residuals := public._ctms_candidate_correction_residuals_v1(
    p_session_id,p_candidate_id,null::uuid,'PAY_WORKBENCH_SOURCE_BUILD'
  );
  for v_residual in select value from jsonb_array_elements(v_residuals) loop
    if coalesce((v_residual->>'draftable')::boolean,false) is not true then
      raise exception 'CORRECTION_RESIDUAL_NOT_DRAFTABLE'
        using errcode='P0001',detail=v_residual::text;
    end if;
    select coalesce(array_agg(value::uuid order by value),array[]::uuid[]) into v_member_ids
    from jsonb_array_elements_text(v_residual->'member_timesheet_ids') value;
    v_root_id:=nullif(v_residual->>'root_timesheet_id','')::uuid;
    for v_component in select value from jsonb_array_elements(v_residual->'components') loop
      select l.id into v_carrier_row_id
      from public.banking_pay_workbench_candidate_source_lines l
      where l.session_id=p_session_id and l.candidate_id=p_candidate_id
        and l.source_build_run_id=p_source_build_run_id and l.status='CURRENT'
        and l.timesheet_id=any(v_member_ids)
        and upper(coalesce(l.economic_key_json->>'key_type',''))=upper(coalesce(v_component->>'component_key_type',''))
        and coalesce(l.economic_key_json->>'key_value','')=coalesce(v_component->>'component_key_value','')
      order by case when l.timesheet_id=v_root_id then 0 else 1 end,l.source_ordinal,l.id
      limit 1 for update;
      if v_carrier_row_id is null then
        raise exception 'CORRECTION_RESIDUAL_SOURCE_COMPONENT_MISSING'
          using errcode='P0001',detail=jsonb_build_object('residual',v_residual,'component',v_component)::text;
      end if;
      v_line_key:='correction-chain:'||v_root_id::text||':'||lower(v_component->>'component_key_type')||':'||lower(v_component->>'component_key_value');

      update public.banking_pay_workbench_candidate_source_lines l
      set timesheet_id=v_root_id,
          line_key=v_line_key,
          parent_line_key='correction-chain:'||v_root_id::text,
          split_suffix=lower(v_component->>'component_key_type')||':'||lower(v_component->>'component_key_value'),
          source_row_json=coalesce(l.source_row_json,'{}'::jsonb)||jsonb_build_object(
            'timesheet_id',v_root_id::text,'real_business_timesheet_id',v_root_id::text,
            'source_family_key',v_residual->>'source_family_key',
            'correction_chain_residual',v_residual,
            'correction_chain_component',v_component,
            'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
            'amount_ex_vat',(v_component->>'target_outstanding_ex_vat')::numeric,
            'preview_amount_ex_vat',(v_component->>'target_outstanding_ex_vat')::numeric,
            'ready_preview_amount_ex_vat',(v_component->>'target_outstanding_ex_vat')::numeric,
            'raw_correction_member_rows_suppressed',true
          ),
          economic_key_json=jsonb_build_object(
            'timesheet_id',v_root_id::text,
            'key_type',v_component->>'component_key_type',
            'key_value',v_component->>'component_key_value'
          ),
          contract_json=coalesce(l.contract_json,'{}'::jsonb)||jsonb_build_object(
            'policy_x_authority_scope','PRE_DRAFT_CORRECTION_RESIDUAL',
            'residual_fingerprint',v_residual->>'residual_fingerprint'
          ),
          updated_at_utc=coalesce(p_now_utc,now())
      where l.id=v_carrier_row_id;
      v_updated:=v_updated+1;

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
begin
  perform public._ctms_assert_payload_corrections_fresh_v1(v_payload,'PAY_CASE_RESOLUTION');
  v_candidate:=nullif(v_payload->>'candidate_id','')::uuid;
  select id into v_timesheet from unnest(public._ctms_payload_timesheet_ids_v1(v_payload,100)) x(id) limit 1;
  if v_candidate is null or v_timesheet is null
     or coalesce((public._ctms_import_correction_classify_v1(v_timesheet)
       ->>'is_import_authoritative_correction')::boolean,false) is not true then
    return v_payload;
  end if;
  v_residuals:=public._ctms_candidate_correction_residuals_v1(
    p_session_id,v_candidate,null::uuid,'PAY_CASE_RESOLUTION'
  );
  v_residual:=v_residuals->0;
  if jsonb_typeof(v_residual)<>'object' then
    raise exception 'CORRECTION_RESIDUAL_REQUIRED_FOR_CASE_RESOLUTION' using errcode='P0001';
  end if;
  return v_payload||jsonb_build_object(
    'source_family_key',coalesce(v_payload->>'source_family_key',v_residual->>'source_family_key'),
    'correction_financials_policy_envelope',v_residual->'correction_financials_policy_envelope',
    'correction_financials_policy_envelope_fingerprint',v_residual->>'correction_financials_policy_envelope_fingerprint',
    'correction_chain_residual_fingerprint',v_residual->>'residual_fingerprint',
    'correction_chain_fingerprint',v_residual->>'chain_fingerprint'
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
       or coalesce((v_scope->>'existing_line_member_count')::integer,0)
          <> coalesce((v_scope->>'expected_member_count')::integer,0)
       or coalesce((v_scope->>'existing_line_invoice_count')::integer,0)<>1 then
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
revoke all on function public._ctms_assert_payload_corrections_fresh_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_session_correction_residuals_draftable_v1(uuid,jsonb,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_materialise_candidate_correction_residuals_v1(uuid,uuid,uuid,timestamptz) from public,anon,authenticated,service_role;
revoke all on function public._ctms_enrich_correction_resolution_payload_v1(uuid,jsonb) from public,anon,authenticated,service_role;
revoke all on function public._ctms_clear_correction_chain_snoozes_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public._ctms_invoice_week_candidate_ids_v1(uuid,date,integer) from public,anon,authenticated,service_role;
revoke all on function public._ctms_invoice_payload_has_financial_edit_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_invoice_mutable_draft_v1(uuid,text,boolean) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_correction_invoice_scope_v1(uuid[],uuid,uuid,boolean,boolean,boolean,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_invoice_correction_lines_v1(uuid,uuid,boolean,text) from public,anon,authenticated,service_role;
revoke all on function public._ctms_assert_invoice_can_unissue_v1(uuid,boolean,text) from public,anon,authenticated,service_role;
