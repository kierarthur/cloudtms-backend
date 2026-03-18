create or replace function public.invoice_enqueue_auto_invoice_ready(p_limit integer default 500)
returns integer
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));

  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_anchor_ymd date := null;
  v_dbg_eligible_ts_count int := null;
  v_dbg_grouped_count int := null;
  v_dbg_inserted_count int := null;
  v_dbg_already_queued_count int := null;
begin
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  with anchor as (
    select (now() at time zone 'Europe/London')::date as anchor_ymd
  ),
  eligible_ts as (
    select
      tf.client_id,
      (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
    from public.timesheets_financials tf
    join public.timesheets t
      on t.timesheet_id = tf.timesheet_id
     and t.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    left join public.contract_weeks cw
      on cw.timesheet_id = tf.timesheet_id
    left join public.contracts c
      on c.id = coalesce(t.contract_id, cw.contract_id)
    left join lateral (
      select
        cs0.auto_invoice_default
      from public.client_settings cs0
      cross join anchor a
      where cs0.client_id = tf.client_id
        and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
      order by cs0.effective_from desc nulls last
      limit 1
    ) cs on true
    where tf.is_current = true
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and t.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'
      and pc.week_ending_date < (select a.anchor_ymd from anchor a)
      and coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
      and not (
        coalesce(vts.hr_validation_required_for_invoice, false)
        and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
        and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
      )
    order by t.updated_at desc nulls last
    limit v_lim
  ),
  grouped as (
    select distinct
      e.client_id,
      e.invoice_week_start
    from eligible_ts e
    where e.client_id is not null
      and e.invoice_week_start is not null
  ),
  ins as (
    insert into public.invoice_jobs_outbox(kind, payload)
    select
      'BY_WEEK'::text as kind,
      jsonb_build_object(
        'client_id', g.client_id::text,
        'invoice_week_start', g.invoice_week_start::text
      ) as payload
    from grouped g
    where not exists (
      select 1
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = g.client_id::text
        and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
    )
    returning 1
  )
  select count(*) into v_ins from ins;

  if v_invoice_debug then
    begin
      with anchor as (
        select (now() at time zone 'Europe/London')::date as anchor_ymd
      ),
      eligible_ts as (
        select
          tf.client_id,
          (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
        from public.timesheets_financials tf
        join public.timesheets t
          on t.timesheet_id = tf.timesheet_id
         and t.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        left join public.contract_weeks cw
          on cw.timesheet_id = tf.timesheet_id
        left join public.contracts c
          on c.id = coalesce(t.contract_id, cw.contract_id)
        left join lateral (
          select cs0.auto_invoice_default
          from public.client_settings cs0
          cross join anchor a
          where cs0.client_id = tf.client_id
            and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
          order by cs0.effective_from desc nulls last
          limit 1
        ) cs on true
        where tf.is_current = true
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and t.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'
          and pc.week_ending_date < (select a.anchor_ymd from anchor a)
          and coalesce(c.auto_invoice, cs.auto_invoice_default, false) = true
          and not (
            coalesce(vts.hr_validation_required_for_invoice, false)
            and vts.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and vts.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )
        order by t.updated_at desc nulls last
        limit v_lim
      ),
      grouped as (
        select distinct e.client_id, e.invoice_week_start
        from eligible_ts e
        where e.client_id is not null
          and e.invoice_week_start is not null
      ),
      already as (
        select count(*)::int as n
        from grouped g
        where exists (
          select 1
          from public.invoice_jobs_outbox o
          where o.kind = 'BY_WEEK'
            and (o.payload->>'client_id') = g.client_id::text
            and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
        )
      )
      select
        (select a.anchor_ymd from anchor a),
        (select count(*)::int from eligible_ts),
        (select count(*)::int from grouped),
        v_ins,
        (select a2.n from already a2)
      into
        v_dbg_anchor_ymd,
        v_dbg_eligible_ts_count,
        v_dbg_grouped_count,
        v_dbg_inserted_count,
        v_dbg_already_queued_count;

      perform public._inv_write_audit(
        null,
        'INVOICE_ENQUEUE_AUTO_READY_DEBUG',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'limit', v_lim,
          'anchor_ymd', v_dbg_anchor_ymd::text,
          'eligible_ts_rows', v_dbg_eligible_ts_count,
          'distinct_groups', v_dbg_grouped_count,
          'inserted_groups', v_dbg_inserted_count,
          'already_queued_groups', v_dbg_already_queued_count
        ),
        'invoice_jobs_outbox',
        ('cron:' || public._inv_iso_utc(v_dbg_run_started)),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_ins;
end;
$function$;

create or replace function public.invoice_enqueue_ready_for_invoice(p_limit integer default 500)
returns integer
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));

  v_invoice_debug boolean := false;
  v_dbg_run_started timestamptz := now();
  v_dbg_anchor_ymd date := null;
  v_dbg_eligible_ts_count int := null;
  v_dbg_grouped_count int := null;
  v_dbg_inserted_count int := null;
  v_dbg_already_queued_count int := null;
begin
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  with anchor as (
    select (now() at time zone 'Europe/London')::date as anchor_ymd
  ),
  eligible_ts as (
    select
      tf.client_id,
      (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
    from public.timesheets_financials tf
    join public.timesheets t
      on t.timesheet_id = tf.timesheet_id
     and t.is_current = true
    join public.v_ts_invoice_precheck pc
      on pc.timesheet_id = tf.timesheet_id
    join public.v_timesheets_summary_base vts
      on vts.timesheet_id = tf.timesheet_id
    left join lateral (
      select
        cs0.auto_invoice_default
      from public.client_settings cs0
      cross join anchor a
      where cs0.client_id = tf.client_id
        and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
      order by cs0.effective_from desc nulls last
      limit 1
    ) cs on true
    where tf.is_current = true
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and t.revoked_at is null
      and upper(coalesce(pc.precheck_status,'')) = 'OK'
      and pc.week_ending_date < (select a.anchor_ymd from anchor a)
      and coalesce(cs.auto_invoice_default, false) = true
      and (
        coalesce(vts.client_hr_validation_required, false) = false
        or vts.validation_status = any(array[
          'VALIDATION_OK'::public.validation_status_enum,
          'OVERRIDDEN'::public.validation_status_enum
        ])
      )
    order by t.updated_at desc nulls last
    limit v_lim
  ),
  grouped as (
    select distinct
      e.client_id,
      e.invoice_week_start
    from eligible_ts e
    where e.client_id is not null
      and e.invoice_week_start is not null
  ),
  ins as (
    insert into public.invoice_jobs_outbox(kind, payload)
    select
      'BY_WEEK'::text as kind,
      jsonb_build_object(
        'client_id', g.client_id::text,
        'invoice_week_start', g.invoice_week_start::text
      ) as payload
    from grouped g
    where not exists (
      select 1
      from public.invoice_jobs_outbox o
      where o.kind = 'BY_WEEK'
        and (o.payload->>'client_id') = g.client_id::text
        and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
    )
    returning 1
  )
  select count(*) into v_ins from ins;

  if v_invoice_debug then
    begin
      with anchor as (
        select (now() at time zone 'Europe/London')::date as anchor_ymd
      ),
      eligible_ts as (
        select
          tf.client_id,
          (pc.week_ending_date::date - interval '6 days')::date as invoice_week_start
        from public.timesheets_financials tf
        join public.timesheets t
          on t.timesheet_id = tf.timesheet_id
         and t.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        join public.v_timesheets_summary_base vts
          on vts.timesheet_id = tf.timesheet_id
        left join lateral (
          select cs0.auto_invoice_default
          from public.client_settings cs0
          cross join anchor a
          where cs0.client_id = tf.client_id
            and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
          order by cs0.effective_from desc nulls last
          limit 1
        ) cs on true
        where tf.is_current = true
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and t.revoked_at is null
          and upper(coalesce(pc.precheck_status,'')) = 'OK'
          and pc.week_ending_date < (select a.anchor_ymd from anchor a)
          and coalesce(cs.auto_invoice_default, false) = true
          and (
            coalesce(vts.client_hr_validation_required, false) = false
            or vts.validation_status = any(array[
              'VALIDATION_OK'::public.validation_status_enum,
              'OVERRIDDEN'::public.validation_status_enum
            ])
          )
        order by t.updated_at desc nulls last
        limit v_lim
      ),
      grouped as (
        select distinct e.client_id, e.invoice_week_start
        from eligible_ts e
        where e.client_id is not null
          and e.invoice_week_start is not null
      ),
      already as (
        select count(*)::int as n
        from grouped g
        where exists (
          select 1
          from public.invoice_jobs_outbox o
          where o.kind = 'BY_WEEK'
            and (o.payload->>'client_id') = g.client_id::text
            and (o.payload->>'invoice_week_start') = g.invoice_week_start::text
        )
      )
      select
        (select a.anchor_ymd from anchor a),
        (select count(*)::int from eligible_ts),
        (select count(*)::int from grouped),
        v_ins,
        (select a2.n from already a2)
      into
        v_dbg_anchor_ymd,
        v_dbg_eligible_ts_count,
        v_dbg_grouped_count,
        v_dbg_inserted_count,
        v_dbg_already_queued_count;

      perform public._inv_write_audit(
        null,
        'INVOICE_ENQUEUE_READY_DEBUG',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_dbg_run_started),
          'run_finished_at_utc', public._inv_iso_utc(now()),
          'limit', v_lim,
          'anchor_ymd', v_dbg_anchor_ymd::text,
          'eligible_ts_rows', v_dbg_eligible_ts_count,
          'distinct_groups', v_dbg_grouped_count,
          'inserted_groups', v_dbg_inserted_count,
          'already_queued_groups', v_dbg_already_queued_count
        ),
        'invoice_jobs_outbox',
        ('cron:' || public._inv_iso_utc(v_dbg_run_started)),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_ins;
end;
$function$;

create or replace function public.mailshot_prepare(p_context_kind text, p_context_ids uuid[], p_output_type text, p_document_template_id uuid, p_actor_user_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_now timestamptz := now();
  v_today_uk date := (now() at time zone 'Europe/London')::date;

  v_context_kind text := lower(btrim(coalesce(p_context_kind,'')));
  v_entity_type text;
  v_output_type text := upper(btrim(coalesce(p_output_type,'')));

  v_template public.document_templates%rowtype;
  v_has_template boolean := false;

  v_selected_keys text[] := '{}'::text[];
  v_to_field_key text := null;
  v_email_type text := null;

  v_rows jsonb := '[]'::jsonb;
  v_prepare_attachment_instructions jsonb := '[]'::jsonb;

  v_ctx record;
  v_ctx_json jsonb;
  v_field_values jsonb;
  v_key text;
  v_path text;
  v_path_arr text[];
  v_val text;

  v_to_value text;
  v_recipient_kind text;
  v_recipient_id uuid;
  v_recipient_display_name text;
  v_candidate_name text;
  v_candidate_display_name text;
  v_client_display_name text;
  v_umbrella_display_name text;

  v_opt_ok boolean;
  v_skip_reason text;

  v_client_settings_json jsonb;
  v_template_content_json jsonb := '{}'::jsonb;
  v_template_attachment_cfg jsonb := '{}'::jsonb;
  v_row_attachment_instructions jsonb := '[]'::jsonb;
  v_attach_timesheet_pdf boolean := false;
  v_attach_invoice_pdf boolean := false;

  v_sms_or_voice boolean := false;

  v_requested_context_count integer := coalesce(array_length(p_context_ids,1),0);
  v_resolved_context_count integer := 0;
  v_returned_row_count integer := 0;
  v_eligible_count integer := 0;
  v_skipped_count integer := 0;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if v_context_kind is null or v_context_kind = '' then
    raise exception 'context_kind required';
  end if;

  if p_context_ids is null or coalesce(array_length(p_context_ids,1),0) = 0 then
    raise exception 'context_ids[] required';
  end if;

  if v_output_type is null or v_output_type = '' then
    raise exception 'output_type required';
  end if;

  if v_output_type not in ('EMAIL','WHATSAPP','SMS','VOICE','WORD','EXCEL') then
    raise exception 'invalid output_type: %', v_output_type;
  end if;

  v_sms_or_voice := (v_output_type in ('SMS','VOICE'));

  v_entity_type :=
    case v_context_kind
      when 'candidates' then 'candidate'
      when 'candidate' then 'candidate'
      when 'clients' then 'client'
      when 'client' then 'client'
      when 'contracts' then 'contract'
      when 'contract' then 'contract'
      when 'timesheets' then 'timesheet'
      when 'timesheet' then 'timesheet'
      when 'invoices' then 'invoice'
      when 'invoice' then 'invoice'
      when 'umbrellas' then 'umbrella'
      when 'umbrella' then 'umbrella'
      else null
    end;

  if v_entity_type is null then
    raise exception 'unsupported context_kind: %', v_context_kind;
  end if;

  if p_document_template_id is not null then
    select dt.*
      into v_template
    from public.document_templates as dt
    where dt.id = p_document_template_id;

    if not found then
      raise exception 'document_template not found: %', p_document_template_id;
    end if;

    v_has_template := true;

    if lower(v_template.entity_type) <> v_entity_type then
      raise exception 'template entity_type % does not match context entity_type %', v_template.entity_type, v_entity_type;
    end if;

    if upper(v_template.output_type) <> v_output_type then
      raise exception 'template output_type % does not match requested %', v_template.output_type, v_output_type;
    end if;

    v_selected_keys := coalesce(v_template.selected_field_keys, '{}'::text[]);
    v_email_type := v_template.email_type;
    v_to_field_key := nullif(btrim(coalesce(v_template.template_content_json->>'to_field_key','')), '');
    v_template_content_json := coalesce(v_template.template_content_json, '{}'::jsonb);
  end if;

  if v_to_field_key is null then
    if v_output_type = 'EMAIL' then
      v_to_field_key :=
        case v_entity_type
          when 'candidate' then 'candidate.email'
          when 'contract' then 'candidate.email'
          when 'timesheet' then 'candidate.email'
          when 'client' then 'client.primary_invoice_email'
          when 'invoice' then 'client.primary_invoice_email'
          when 'umbrella' then 'umbrella.remittance_email'
          else 'candidate.email'
        end;
    else
      v_to_field_key :=
        case v_entity_type
          when 'candidate' then 'candidate.phone'
          when 'contract' then 'candidate.phone'
          when 'timesheet' then 'candidate.phone'
          when 'client' then 'client.contact_mobile'
          when 'invoice' then 'client.contact_mobile'
          when 'umbrella' then null
          else 'candidate.phone'
        end;
    end if;
  end if;

  if jsonb_typeof(v_template_content_json->'mailshot_attachments') = 'object' then
    v_template_attachment_cfg := coalesce(v_template_content_json->'mailshot_attachments', '{}'::jsonb);
  else
    v_template_attachment_cfg := '{}'::jsonb;
  end if;

  v_attach_timesheet_pdf := coalesce((v_template_attachment_cfg->>'attach_authoritative_timesheet_pdf')::boolean, false);
  v_attach_invoice_pdf := coalesce((v_template_attachment_cfg->>'attach_authoritative_invoice_pdf')::boolean, false);

  create temporary table tmp_ctx(
    context_id uuid not null,
    ctx_json jsonb not null
  ) on commit drop;

  if v_entity_type = 'candidate' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      c.id,
      jsonb_build_object(
        'candidate', to_jsonb(c),
        'umbrella', case when u.id is null then null else to_jsonb(u) end,
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.candidates as c
    left join public.umbrellas as u
      on u.id = c.umbrella_id
    where c.id = any(p_context_ids);
  elsif v_entity_type = 'client' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      cl.id,
      jsonb_build_object(
        'client', to_jsonb(cl),
        'client_settings', case when cs.id is null then null else to_jsonb(cs) end,
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.clients as cl
    left join lateral (
      select cs1.*
      from public.client_settings as cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) as cs on true
    where cl.id = any(p_context_ids);
  elsif v_entity_type = 'contract' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      ct.id,
      jsonb_build_object(
        'contract', to_jsonb(ct),
        'candidate', case when c.id is null then null else to_jsonb(c) end,
        'client', case when cl.id is null then null else to_jsonb(cl) end,
        'client_settings', case when cs.id is null then null else to_jsonb(cs) end,
        'umbrella', case when u.id is null then null else to_jsonb(u) end,
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.contracts as ct
    left join public.candidates as c
      on c.id = ct.candidate_id
    left join public.umbrellas as u
      on u.id = c.umbrella_id
    left join public.clients as cl
      on cl.id = ct.client_id
    left join lateral (
      select cs1.*
      from public.client_settings as cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) as cs on true
    where ct.id = any(p_context_ids);
  elsif v_entity_type = 'timesheet' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      ts.timesheet_id,
      jsonb_build_object(
        'timesheet', to_jsonb(ts),
        'contract', case when ct.id is null then null else to_jsonb(ct) end,
        'candidate', case when c.id is null then null else to_jsonb(c) end,
        'client', case when cl.id is null then null else to_jsonb(cl) end,
        'client_settings', case when cs.id is null then null else to_jsonb(cs) end,
        'umbrella', case when u.id is null then null else to_jsonb(u) end,
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.timesheets as ts
    left join public.contracts as ct
      on ct.id = ts.contract_id
    left join public.candidates as c
      on c.id = ct.candidate_id
    left join public.umbrellas as u
      on u.id = c.umbrella_id
    left join public.clients as cl
      on cl.id = ct.client_id
    left join lateral (
      select cs1.*
      from public.client_settings as cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) as cs on true
    where ts.timesheet_id = any(p_context_ids);
  elsif v_entity_type = 'invoice' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      inv.id,
      jsonb_build_object(
        'invoice', to_jsonb(inv),
        'client', case when cl.id is null then null else to_jsonb(cl) end,
        'client_settings', case when cs.id is null then null else to_jsonb(cs) end,
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.invoices as inv
    left join public.clients as cl
      on cl.id = inv.client_id
    left join lateral (
      select cs1.*
      from public.client_settings as cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) as cs on true
    where inv.id = any(p_context_ids);
  elsif v_entity_type = 'umbrella' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      u.id,
      jsonb_build_object(
        'umbrella', to_jsonb(u),
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.umbrellas as u
    where u.id = any(p_context_ids);
  end if;

  select count(*)
    into v_resolved_context_count
  from tmp_ctx as t;

  for v_ctx in
    select t.context_id, t.ctx_json
    from tmp_ctx as t
    order by t.context_id::text
  loop
    v_ctx_json := v_ctx.ctx_json;
    v_field_values := '{}'::jsonb;
    v_row_attachment_instructions := '[]'::jsonb;
    v_candidate_name := null;
    v_recipient_display_name := null;

    if v_selected_keys is not null and array_length(v_selected_keys,1) > 0 then
      foreach v_key in array v_selected_keys
      loop
        if v_key is null or length(btrim(v_key)) = 0 then
          continue;
        end if;

        select coalesce(nullif(btrim(coalesce(f.resolver_spec_json->>'path','')),''), v_key)
          into v_path
        from public.mailshot_fields as f
        where f.field_key = v_key;

        if v_path is null then
          v_path := v_key;
        end if;

        v_path_arr := string_to_array(v_path, '.');

        if v_path_arr is null or array_length(v_path_arr,1) = 0 then
          v_val := null;
        elsif array_length(v_path_arr,1) = 1 then
          v_val := v_ctx_json ->> v_path_arr[1];
        elsif array_length(v_path_arr,1) = 2 then
          v_val := (v_ctx_json -> v_path_arr[1]) ->> v_path_arr[2];
        else
          v_val := v_ctx_json #>> v_path_arr;
        end if;

        v_field_values := v_field_values || jsonb_build_object(v_key, v_val);
      end loop;
    end if;

    v_path := v_to_field_key;
    v_path_arr := string_to_array(v_path, '.');

    if v_path_arr is null or array_length(v_path_arr,1) = 0 then
      v_to_value := null;
      v_recipient_kind := null;
    else
      v_recipient_kind := lower(v_path_arr[1]);
      if array_length(v_path_arr,1) = 1 then
        v_to_value := v_ctx_json ->> v_path_arr[1];
      elsif array_length(v_path_arr,1) = 2 then
        v_to_value := (v_ctx_json -> v_path_arr[1]) ->> v_path_arr[2];
      else
        v_to_value := v_ctx_json #>> v_path_arr;
      end if;
    end if;

    v_to_value := nullif(btrim(coalesce(v_to_value,'')), '');

    v_recipient_id := null;
    if v_recipient_kind = 'candidate' then
      v_recipient_id := nullif((v_ctx_json->'candidate'->>'id'),'')::uuid;
    elsif v_recipient_kind = 'client' then
      v_recipient_id := nullif((v_ctx_json->'client'->>'id'),'')::uuid;
    elsif v_recipient_kind = 'umbrella' then
      v_recipient_id := nullif((v_ctx_json->'umbrella'->>'id'),'')::uuid;
    end if;

    v_candidate_display_name := nullif(btrim(coalesce(v_ctx_json->'candidate'->>'display_name','')), '');
    if v_candidate_display_name is null then
      v_candidate_display_name := nullif(
        btrim(
          concat_ws(
            ' ',
            nullif(btrim(coalesce(v_ctx_json->'candidate'->>'first_name','')), ''),
            nullif(btrim(coalesce(v_ctx_json->'candidate'->>'last_name','')), '')
          )
        ),
        ''
      );
    end if;

    v_client_display_name := nullif(btrim(coalesce(v_ctx_json->'client'->>'name','')), '');
    v_umbrella_display_name := nullif(btrim(coalesce(v_ctx_json->'umbrella'->>'name','')), '');

    v_candidate_name := v_candidate_display_name;

    v_recipient_display_name :=
      case v_recipient_kind
        when 'candidate' then coalesce(v_candidate_display_name, v_to_value)
        when 'client' then coalesce(v_client_display_name, v_to_value)
        when 'umbrella' then coalesce(v_umbrella_display_name, v_to_value)
        else coalesce(v_candidate_display_name, v_client_display_name, v_umbrella_display_name, v_to_value)
      end;

    v_opt_ok := true;
    v_skip_reason := null;

    if v_to_value is null then
      v_opt_ok := false;
      v_skip_reason := 'MISSING_RECIPIENT';
    end if;

    if v_opt_ok = true and v_recipient_kind = 'candidate' then
      if v_output_type = 'EMAIL' then
        v_opt_ok := coalesce((v_ctx_json->'candidate'->>'opt_in_email')::boolean, false);
      elsif v_output_type = 'WHATSAPP' then
        v_opt_ok := coalesce((v_ctx_json->'candidate'->>'opt_in_whatsapp')::boolean, false);
      elsif v_sms_or_voice = true then
        v_opt_ok := coalesce((v_ctx_json->'candidate'->>'opt_in_sms')::boolean, false);
      else
        v_opt_ok := true;
      end if;

      if v_opt_ok = false then
        v_skip_reason := 'OPTOUT';
      end if;
    elsif v_opt_ok = true and v_recipient_kind = 'client' then
      v_client_settings_json := v_ctx_json->'client_settings';

      if v_client_settings_json is null or jsonb_typeof(v_client_settings_json) <> 'object' then
        v_opt_ok := true;
      else
        if v_output_type = 'EMAIL' then
          v_opt_ok := coalesce((v_client_settings_json->>'opt_in_email')::boolean, true);
        elsif v_output_type = 'WHATSAPP' then
          v_opt_ok := coalesce((v_client_settings_json->>'opt_in_whatsapp')::boolean, true);
        elsif v_sms_or_voice = true then
          v_opt_ok := coalesce((v_client_settings_json->>'opt_in_sms')::boolean, true);
        else
          v_opt_ok := true;
        end if;
      end if;

      if v_opt_ok = false then
        v_skip_reason := 'OPTOUT';
      end if;
    end if;

    if v_output_type = 'EMAIL' and v_entity_type = 'timesheet' and v_attach_timesheet_pdf = true then
      v_row_attachment_instructions := v_row_attachment_instructions || jsonb_build_array(
        jsonb_build_object(
          'kind', 'AUTHORITATIVE_TIMESHEET_PDF',
          'context_id', v_ctx.context_id::text,
          'entity_type', v_entity_type,
          'filename', format('Timesheet_%s.pdf', v_ctx.context_id::text),
          'content_type', 'application/pdf'
        )
      );
    elsif v_output_type = 'EMAIL' and v_entity_type = 'invoice' and v_attach_invoice_pdf = true then
      v_row_attachment_instructions := v_row_attachment_instructions || jsonb_build_array(
        jsonb_build_object(
          'kind', 'AUTHORITATIVE_INVOICE_PDF',
          'context_id', v_ctx.context_id::text,
          'entity_type', v_entity_type,
          'filename', format('Invoice_%s.pdf', v_ctx.context_id::text),
          'content_type', 'application/pdf'
        )
      );
    end if;

    v_rows := v_rows || jsonb_build_array(
      jsonb_build_object(
        'context_id', v_ctx.context_id::text,
        'context_kind', v_context_kind,
        'entity_type', v_entity_type,
        'output_type', v_output_type,
        'document_template_id', case when p_document_template_id is null then null else p_document_template_id::text end,
        'to_field_key', v_to_field_key,
        'to', v_to_value,
        'recipient_kind', v_recipient_kind,
        'recipient_id', case when v_recipient_id is null then null else v_recipient_id::text end,
        'recipient_display_name', v_recipient_display_name,
        'candidate_name', v_candidate_name,
        'eligible', v_opt_ok,
        'skip_reason', v_skip_reason,
        'field_values', v_field_values,
        'template_content_json', case when v_has_template then v_template_content_json else '{}'::jsonb end,
        'email_type', v_email_type,
        'attachment_instructions', v_row_attachment_instructions,
        'attachments', v_row_attachment_instructions
      )
    );

    v_returned_row_count := v_returned_row_count + 1;
    if v_opt_ok then
      v_eligible_count := v_eligible_count + 1;
    else
      v_skipped_count := v_skipped_count + 1;
    end if;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'context_kind', v_context_kind,
    'entity_type', v_entity_type,
    'output_type', v_output_type,
    'document_template_id', case when p_document_template_id is null then null else p_document_template_id::text end,
    'selected_field_keys', to_jsonb(v_selected_keys),
    'to_field_key', v_to_field_key,
    'requested_context_count', v_requested_context_count,
    'resolved_context_count', v_resolved_context_count,
    'returned_row_count', v_returned_row_count,
    'eligible_count', v_eligible_count,
    'skipped_count', v_skipped_count,
    'prepare_counts', jsonb_build_object(
      'requested_context_count', v_requested_context_count,
      'resolved_context_count', v_resolved_context_count,
      'returned_row_count', v_returned_row_count,
      'eligible_count', v_eligible_count,
      'skipped_count', v_skipped_count
    ),
    'attachment_instructions', v_prepare_attachment_instructions,
    'attachments', v_prepare_attachment_instructions,
    'rows', v_rows
  );
end;
$function$;

