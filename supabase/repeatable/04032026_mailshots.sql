create or replace function public.document_templates_list(
  p_entity_type text,
  p_output_type text
)
returns table(
  id uuid,
  entity_type text,
  output_type text,
  filename text,
  description text,
  email_type text,
  created_by uuid,
  created_at_utc timestamptz,
  updated_at_utc timestamptz
)
language sql
stable
security definer
set search_path = public
as $$
  select
    dt.id,
    dt.entity_type,
    dt.output_type,
    dt.filename,
    dt.description,
    dt.email_type,
    dt.created_by,
    dt.created_at_utc,
    dt.updated_at_utc
  from public.document_templates dt
  where dt.entity_type = p_entity_type
    and dt.output_type = p_output_type
  order by lower(dt.filename) asc, dt.created_at_utc asc;
$$;

create or replace function public.document_templates_get(
  p_template_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_row public.document_templates%rowtype;
begin
  if p_template_id is null then
    raise exception 'template_id required';
  end if;

  select dt.*
    into v_row
  from public.document_templates dt
  where dt.id = p_template_id;

  if not found then
    raise exception 'document_template not found: %', p_template_id;
  end if;

  return jsonb_build_object(
    'id', v_row.id::text,
    'entity_type', v_row.entity_type,
    'output_type', v_row.output_type,
    'filename', v_row.filename,
    'description', v_row.description,
    'email_type', v_row.email_type,
    'selected_field_keys', to_jsonb(v_row.selected_field_keys),
    'template_content_json', v_row.template_content_json,
    'created_by', case when v_row.created_by is null then null else v_row.created_by::text end,
    'created_at_utc', v_row.created_at_utc::text,
    'updated_at_utc', v_row.updated_at_utc::text
  );
end;
$$;


create or replace function public.document_templates_upsert(
  p_template_id uuid,
  p_entity_type text,
  p_output_type text,
  p_filename text,
  p_description text,
  p_email_type text,
  p_selected_field_keys text[],
  p_template_content_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_id uuid;
  v_existing public.document_templates%rowtype;
  v_row public.document_templates%rowtype;
  v_selected text[] := coalesce(p_selected_field_keys, '{}'::text[]);
  v_content jsonb := coalesce(p_template_content_json, '{}'::jsonb);
  v_output_type_norm text := upper(btrim(coalesce(p_output_type, '')));
  v_email_type_norm text := nullif(lower(btrim(coalesce(p_email_type, ''))), '');
  v_whatsapp_provider text;
  v_whatsapp_template_name text;
  v_whatsapp_param_name text;
  v_whatsapp_message_text text;
  v_sms_voice_message_text text;
  v_word_body_html text;
  v_word_body_text text;
begin
  if p_entity_type is null or length(btrim(p_entity_type)) = 0 then
    raise exception 'entity_type required';
  end if;

  if p_output_type is null or length(btrim(p_output_type)) = 0 then
    raise exception 'output_type required';
  end if;

  if p_filename is null or length(btrim(p_filename)) = 0 then
    raise exception 'filename required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_template_content_json is not null and jsonb_typeof(p_template_content_json) <> 'object' then
    raise exception 'template_content_json must be object';
  end if;

  if v_output_type_norm not in ('EMAIL','WHATSAPP','SMS','VOICE','WORD','EXCEL') then
    raise exception 'invalid output_type: %', v_output_type_norm;
  end if;

  if v_output_type_norm = 'EMAIL' then
    if v_email_type_norm is not null and v_email_type_norm not in ('plain','html') then
      raise exception 'email_type must be plain or html for EMAIL';
    end if;
  end if;

  if v_output_type_norm = 'WHATSAPP' then
    v_whatsapp_provider := coalesce(
      nullif(btrim(coalesce(v_content->'provider_contract'->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->'provider_contract'->>'provider_key','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->>'provider_key','')), '')
    );

    if v_whatsapp_provider is not null and upper(v_whatsapp_provider) <> 'WATI' then
      raise exception 'whatsapp template_content_json provider must be WATI';
    end if;

    v_whatsapp_template_name := coalesce(
      nullif(btrim(coalesce(v_content->'provider_contract'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider_contract'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->>'wati_template_name','')), ''),
      nullif(btrim(coalesce(v_content->>'watiTemplateName','')), ''),
      nullif(btrim(coalesce(v_content->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->>'templateName','')), '')
    );

    if v_whatsapp_template_name is null then
      raise exception 'whatsapp template_content_json missing WATI template_name';
    end if;

    v_whatsapp_param_name := coalesce(
      nullif(btrim(coalesce(v_content->'provider_contract'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider_contract'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->>'wati_param_name','')), ''),
      nullif(btrim(coalesce(v_content->>'watiParamName','')), ''),
      nullif(btrim(coalesce(v_content->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->>'paramName','')), '')
    );

    if v_whatsapp_param_name is null then
      raise exception 'whatsapp template_content_json missing WATI param_name';
    end if;

    v_whatsapp_message_text := nullif(btrim(coalesce(v_content->>'message_text','')), '');

    if v_whatsapp_message_text is null then
      raise exception 'whatsapp template_content_json missing message_text';
    end if;

    v_content := v_content || jsonb_build_object(
      'provider_contract',
      jsonb_build_object(
        'provider', 'WATI',
        'template_name', v_whatsapp_template_name,
        'param_name', v_whatsapp_param_name
      )
    );
  elsif v_output_type_norm in ('SMS','VOICE') then
    v_sms_voice_message_text := nullif(btrim(coalesce(v_content->>'message_text','')), '');

    if v_sms_voice_message_text is null then
      raise exception '% template_content_json missing message_text', v_output_type_norm;
    end if;
  elsif v_output_type_norm = 'WORD' then
    v_word_body_html := nullif(btrim(coalesce(v_content->>'body_html','')), '');
    v_word_body_text := nullif(btrim(coalesce(v_content->>'body_text','')), '');

    if v_word_body_html is null and v_word_body_text is null then
      raise exception 'word template_content_json requires body_html or body_text';
    end if;
  end if;

  if p_template_id is null then
    insert into public.document_templates(
      entity_type,
      output_type,
      filename,
      description,
      email_type,
      selected_field_keys,
      template_content_json,
      created_by,
      created_at_utc,
      updated_at_utc
    )
    values (
      btrim(p_entity_type),
      v_output_type_norm,
      btrim(p_filename),
      p_description,
      v_email_type_norm,
      v_selected,
      v_content,
      p_actor_user_id,
      v_now,
      v_now
    )
    returning id into v_id;
  else
    select dt.*
      into v_existing
    from public.document_templates dt
    where dt.id = p_template_id;

    if not found then
      raise exception 'document_template not found: %', p_template_id;
    end if;

    update public.document_templates dt
    set
      entity_type = btrim(p_entity_type),
      output_type = v_output_type_norm,
      filename = btrim(p_filename),
      description = p_description,
      email_type = v_email_type_norm,
      selected_field_keys = v_selected,
      template_content_json = v_content,
      updated_at_utc = v_now
    where dt.id = p_template_id
    returning dt.id into v_id;
  end if;

  select dt.*
    into v_row
  from public.document_templates dt
  where dt.id = v_id;

  return jsonb_build_object(
    'ok', true,
    'template', jsonb_build_object(
      'id', v_row.id::text,
      'entity_type', v_row.entity_type,
      'output_type', v_row.output_type,
      'filename', v_row.filename,
      'description', v_row.description,
      'email_type', v_row.email_type,
      'selected_field_keys', to_jsonb(v_row.selected_field_keys),
      'template_content_json', v_row.template_content_json,
      'created_by', case when v_row.created_by is null then null else v_row.created_by::text end,
      'created_at_utc', v_row.created_at_utc::text,
      'updated_at_utc', v_row.updated_at_utc::text
    )
  );
end;
$$;

create or replace function public.document_templates_delete(
  p_template_id uuid,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_exists boolean;
  v_mail_outbox_rows int := 0;
  v_comms_outbox_rows int := 0;
  v_mailshot_runs_rows int := 0;
  v_deleted int := 0;
begin
  if p_template_id is null then
    raise exception 'template_id required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  select true
    into v_exists
  from public.document_templates dt
  where dt.id = p_template_id;

  if not found then
    raise exception 'document_template not found: %', p_template_id;
  end if;

  -- To allow deletion while keeping outbox logs indefinitely, we detach references first.
  update public.mail_outbox mo
  set document_template_id = null
  where mo.document_template_id = p_template_id;
  get diagnostics v_mail_outbox_rows = row_count;

  update public.comms_outbox co
  set document_template_id = null
  where co.document_template_id = p_template_id;
  get diagnostics v_comms_outbox_rows = row_count;

  update public.mailshot_runs mr
  set document_template_id = null
  where mr.document_template_id = p_template_id;
  get diagnostics v_mailshot_runs_rows = row_count;

  delete from public.document_templates dt
  where dt.id = p_template_id;
  get diagnostics v_deleted = row_count;

  return jsonb_build_object(
    'ok', (v_deleted = 1),
    'deleted', v_deleted,
    'detached', jsonb_build_object(
      'mail_outbox_rows', v_mail_outbox_rows,
      'comms_outbox_rows', v_comms_outbox_rows,
      'mailshot_runs_rows', v_mailshot_runs_rows
    )
  );
end;
$$;

create or replace function public.document_templates_duplicate(
  p_template_id uuid,
  p_new_filename text,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_src public.document_templates%rowtype;
  v_new_id uuid;
  v_new public.document_templates%rowtype;
begin
  if p_template_id is null then
    raise exception 'template_id required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_new_filename is null or length(btrim(p_new_filename)) = 0 then
    raise exception 'new_filename required';
  end if;

  select dt.*
    into v_src
  from public.document_templates dt
  where dt.id = p_template_id;

  if not found then
    raise exception 'document_template not found: %', p_template_id;
  end if;

  insert into public.document_templates(
    entity_type,
    output_type,
    filename,
    description,
    email_type,
    selected_field_keys,
    template_content_json,
    created_by,
    created_at_utc,
    updated_at_utc
  )
  values (
    v_src.entity_type,
    v_src.output_type,
    btrim(p_new_filename),
    v_src.description,
    v_src.email_type,
    v_src.selected_field_keys,
    v_src.template_content_json,
    p_actor_user_id,
    v_now,
    v_now
  )
  returning id into v_new_id;

  select dt.*
    into v_new
  from public.document_templates dt
  where dt.id = v_new_id;

  return jsonb_build_object(
    'ok', true,
    'template', jsonb_build_object(
      'id', v_new.id::text,
      'entity_type', v_new.entity_type,
      'output_type', v_new.output_type,
      'filename', v_new.filename,
      'description', v_new.description,
      'email_type', v_new.email_type,
      'selected_field_keys', to_jsonb(v_new.selected_field_keys),
      'template_content_json', v_new.template_content_json,
      'created_by', case when v_new.created_by is null then null else v_new.created_by::text end,
      'created_at_utc', v_new.created_at_utc::text,
      'updated_at_utc', v_new.updated_at_utc::text
    )
  );
end;
$$;

create or replace function public.mailshot_fields_list(
  p_entity_type text default null,
  p_include_disabled boolean default false
)
returns table(
  field_id uuid,
  field_key text,
  label_default text,
  label_override text,
  label_effective text,
  enabled_global boolean,
  enabled_local boolean,
  enabled_effective boolean,
  allowed_entity_types text[],
  resolver_spec_json jsonb,
  entity_type text
)
language sql
stable
security definer
set search_path = public
as $$
  select
    f.id as field_id,
    f.field_key,
    f.label_default,
    o.label_override,
    coalesce(o.label_override, f.label_default) as label_effective,
    f.enabled_global,
    coalesce(o.enabled_local, true) as enabled_local,
    (f.enabled_global and coalesce(o.enabled_local, true)) as enabled_effective,
    f.allowed_entity_types,
    f.resolver_spec_json,
    p_entity_type as entity_type
  from public.mailshot_fields f
  left join public.mailshot_field_overrides o
    on o.field_id = f.id
   and (p_entity_type is not null and o.entity_type = p_entity_type)
  where
    (
      p_include_disabled = true
      or (f.enabled_global = true and coalesce(o.enabled_local, true) = true)
    )
    and (
      p_entity_type is null
      or coalesce(array_length(f.allowed_entity_types,1),0) = 0
      or p_entity_type = any(f.allowed_entity_types)
    )
  order by lower(coalesce(o.label_override, f.label_default)) asc, lower(f.field_key) asc;
$$;

create or replace function public.mailshot_fields_upsert_global(
  p_field_id uuid,
  p_field_key text,
  p_label_default text,
  p_enabled_global boolean,
  p_allowed_entity_types text[],
  p_resolver_spec_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_id uuid;
  v_existing_id uuid;
  v_field public.mailshot_fields%rowtype;

  v_key text := nullif(btrim(coalesce(p_field_key,'')), '');
  v_label text := nullif(btrim(coalesce(p_label_default,'')), '');
  v_enabled boolean := coalesce(p_enabled_global, true);
  v_allowed text[] := coalesce(p_allowed_entity_types, '{}'::text[]);
  v_spec jsonb := coalesce(p_resolver_spec_json, '{}'::jsonb);

  v_prefix text;
  v_suffix text;
  v_auto_label text;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_field_id is null and v_key is null then
    raise exception 'field_id or field_key required';
  end if;

  if v_key is not null then
    if position('.' in v_key) = 0 then
      raise exception 'field_key must be namespaced like candidate.email (got: %)', v_key;
    end if;
  end if;

  if v_label is null then
    if v_key is null then
      v_label := 'Field';
    else
      v_prefix := split_part(v_key, '.', 1);
      v_suffix := split_part(v_key, '.', 2);

      v_auto_label :=
        initcap(replace(v_prefix, '_', ' ')) || ' — ' ||
        initcap(replace(v_suffix, '_', ' '));

      v_label := v_auto_label;
    end if;
  end if;

  if p_field_id is not null then
    select f.id into v_existing_id
    from public.mailshot_fields f
    where f.id = p_field_id;

    if not found then
      raise exception 'mailshot_field not found: %', p_field_id;
    end if;

    update public.mailshot_fields f
    set
      field_key = coalesce(v_key, f.field_key),
      label_default = v_label,
      enabled_global = v_enabled,
      allowed_entity_types = v_allowed,
      resolver_spec_json = v_spec,
      updated_at_utc = v_now
    where f.id = p_field_id
    returning f.id into v_id;
  else
    select f.id into v_existing_id
    from public.mailshot_fields f
    where f.field_key = v_key;

    if found then
      update public.mailshot_fields f
      set
        label_default = v_label,
        enabled_global = v_enabled,
        allowed_entity_types = v_allowed,
        resolver_spec_json = v_spec,
        updated_at_utc = v_now
      where f.id = v_existing_id
      returning f.id into v_id;
    else
      insert into public.mailshot_fields(
        field_key,
        label_default,
        enabled_global,
        allowed_entity_types,
        resolver_spec_json,
        created_at_utc,
        updated_at_utc
      )
      values (
        v_key,
        v_label,
        v_enabled,
        v_allowed,
        v_spec,
        v_now,
        v_now
      )
      returning id into v_id;
    end if;
  end if;

  select f.* into v_field
  from public.mailshot_fields f
  where f.id = v_id;

  return jsonb_build_object(
    'ok', true,
    'field', jsonb_build_object(
      'id', v_field.id::text,
      'field_key', v_field.field_key,
      'label_default', v_field.label_default,
      'enabled_global', v_field.enabled_global,
      'allowed_entity_types', to_jsonb(v_field.allowed_entity_types),
      'resolver_spec_json', v_field.resolver_spec_json,
      'created_at_utc', v_field.created_at_utc::text,
      'updated_at_utc', v_field.updated_at_utc::text
    )
  );
end;
$$;



create or replace function public.mailshot_fields_upsert_override(
  p_field_id uuid,
  p_entity_type text,
  p_label_override text,
  p_enabled_local boolean,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_entity text := nullif(btrim(coalesce(p_entity_type,'')), '');
  v_label text := nullif(btrim(coalesce(p_label_override,'')), '');
  v_enabled boolean := coalesce(p_enabled_local, true);

  v_exists uuid;
  v_row public.mailshot_field_overrides%rowtype;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_field_id is null then
    raise exception 'field_id required';
  end if;

  if v_entity is null then
    raise exception 'entity_type required';
  end if;

  perform 1
  from public.mailshot_fields f
  where f.id = p_field_id;

  if not found then
    raise exception 'mailshot_field not found: %', p_field_id;
  end if;

  select o.id
    into v_exists
  from public.mailshot_field_overrides o
  where o.field_id = p_field_id
    and o.entity_type = v_entity;

  if found then
    update public.mailshot_field_overrides o
    set
      label_override = v_label,
      enabled_local = v_enabled
    where o.id = v_exists
    returning o.* into v_row;
  else
    insert into public.mailshot_field_overrides(
      field_id,
      entity_type,
      label_override,
      enabled_local
    )
    values (
      p_field_id,
      v_entity,
      v_label,
      v_enabled
    )
    returning * into v_row;
  end if;

  return jsonb_build_object(
    'ok', true,
    'override', jsonb_build_object(
      'id', v_row.id::text,
      'field_id', v_row.field_id::text,
      'entity_type', v_row.entity_type,
      'label_override', v_row.label_override,
      'enabled_local', v_row.enabled_local
    )
  );
end;
$$;



create or replace function public.mailshot_fields_seed_from_schema(
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_inserted int := 0;
  v_skipped int := 0;
  v_total int := 0;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  -- Curated starter set (keeps admin effort low; admins can prune/rename via UI)
  create temporary table tmp_seed_fields(
    field_key text not null,
    label_default text not null,
    enabled_global boolean not null,
    allowed_entity_types text[] not null,
    resolver_spec_json jsonb not null
  ) on commit drop;

  insert into tmp_seed_fields(field_key,label_default,enabled_global,allowed_entity_types,resolver_spec_json) values
    ('candidate.display_name','Candidate — Display name',true, array['candidate','contract','timesheet']::text[], jsonb_build_object('path','candidate.display_name')),
    ('candidate.first_name','Candidate — First name',true, array['candidate','contract','timesheet']::text[], jsonb_build_object('path','candidate.first_name')),
    ('candidate.last_name','Candidate — Last name',true, array['candidate','contract','timesheet']::text[], jsonb_build_object('path','candidate.last_name')),
    ('candidate.email','Candidate — Email',true, array['candidate','contract','timesheet']::text[], jsonb_build_object('path','candidate.email')),
    ('candidate.phone','Candidate — Phone',true, array['candidate','contract','timesheet']::text[], jsonb_build_object('path','candidate.phone')),

    ('client.name','Client — Name',true, array['client','contract','timesheet','invoice']::text[], jsonb_build_object('path','client.name')),
    ('client.primary_invoice_email','Client — Primary invoice email',true, array['client','invoice']::text[], jsonb_build_object('path','client.primary_invoice_email')),
    ('client.contact_email','Client — Contact email',true, array['client','contract','timesheet','invoice']::text[], jsonb_build_object('path','client.contact_email')),
    ('client.contact_mobile','Client — Contact mobile',true, array['client','contract','timesheet','invoice']::text[], jsonb_build_object('path','client.contact_mobile')),
    ('client.contact_tel','Client — Contact tel',true, array['client','contract','timesheet','invoice']::text[], jsonb_build_object('path','client.contact_tel')),

    ('contract.role','Contract — Role',true, array['contract','timesheet']::text[], jsonb_build_object('path','contract.role')),
    ('contract.band','Contract — Band',true, array['contract','timesheet']::text[], jsonb_build_object('path','contract.band')),
    ('contract.start_date','Contract — Start date',true, array['contract','timesheet']::text[], jsonb_build_object('path','contract.start_date')),
    ('contract.end_date','Contract — End date',true, array['contract','timesheet']::text[], jsonb_build_object('path','contract.end_date')),

    ('timesheet.week_ending_date','Timesheet — Week ending',true, array['timesheet']::text[], jsonb_build_object('path','timesheet.week_ending_date')),
    ('timesheet.booking_id','Timesheet — Booking id',false, array['timesheet']::text[], jsonb_build_object('path','timesheet.booking_id')),

    ('invoice.invoice_no','Invoice — Invoice number',true, array['invoice']::text[], jsonb_build_object('path','invoice.invoice_no')),
    ('invoice.issued_at_utc','Invoice — Issued at',true, array['invoice']::text[], jsonb_build_object('path','invoice.issued_at_utc')),
    ('invoice.total_inc_vat','Invoice — Total inc VAT',true, array['invoice']::text[], jsonb_build_object('path','invoice.total_inc_vat')),

    ('umbrella.name','Umbrella — Name',true, array['umbrella']::text[], jsonb_build_object('path','umbrella.name')),
    ('umbrella.remittance_email','Umbrella — Remittance email',true, array['umbrella']::text[], jsonb_build_object('path','umbrella.remittance_email')),

    ('system.today_ymd','System — Today (UK)',true, array['candidate','client','contract','timesheet','invoice','umbrella']::text[], jsonb_build_object('path','system.today_ymd')),
    ('system.now_utc','System — Now (UTC)',true, array['candidate','client','contract','timesheet','invoice','umbrella']::text[], jsonb_build_object('path','system.now_utc'));

  select count(*) into v_total from tmp_seed_fields;

  insert into public.mailshot_fields(
    field_key,
    label_default,
    enabled_global,
    allowed_entity_types,
    resolver_spec_json,
    created_at_utc,
    updated_at_utc
  )
  select
    s.field_key,
    s.label_default,
    s.enabled_global,
    s.allowed_entity_types,
    s.resolver_spec_json,
    v_now,
    v_now
  from tmp_seed_fields s
  where not exists (
    select 1
    from public.mailshot_fields f
    where f.field_key = s.field_key
  );

  get diagnostics v_inserted = row_count;
  v_skipped := v_total - v_inserted;

  return jsonb_build_object(
    'ok', true,
    'seeded_total', v_total,
    'inserted', v_inserted,
    'skipped_existing', v_skipped
  );
end;
$$;


create or replace function public.mailshot_prepare(
  p_context_kind text,
  p_context_ids uuid[],
  p_output_type text,
  p_document_template_id uuid,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
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

  v_opt_ok boolean;
  v_skip_reason text;

  v_client_settings_json jsonb;

  v_sms_or_voice boolean := false;
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
    from public.document_templates dt
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
    from public.candidates c
    left join public.umbrellas u
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
    from public.clients cl
    left join lateral (
      select cs1.*
      from public.client_settings cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) cs on true
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
    from public.contracts ct
    left join public.candidates c
      on c.id = ct.candidate_id
    left join public.umbrellas u
      on u.id = c.umbrella_id
    left join public.clients cl
      on cl.id = ct.client_id
    left join lateral (
      select cs1.*
      from public.client_settings cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) cs on true
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
    from public.timesheets ts
    left join public.contracts ct
      on ct.id = ts.contract_id
    left join public.candidates c
      on c.id = ct.candidate_id
    left join public.umbrellas u
      on u.id = c.umbrella_id
    left join public.clients cl
      on cl.id = ct.client_id
    left join lateral (
      select cs1.*
      from public.client_settings cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) cs on true
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
    from public.invoices inv
    left join public.clients cl
      on cl.id = inv.client_id
    left join lateral (
      select cs1.*
      from public.client_settings cs1
      where cs1.client_id = cl.id
      order by cs1.effective_from desc
      limit 1
    ) cs on true
    where inv.id = any(p_context_ids);
  elsif v_entity_type = 'umbrella' then
    insert into tmp_ctx(context_id, ctx_json)
    select
      u.id,
      jsonb_build_object(
        'umbrella', to_jsonb(u),
        'system', jsonb_build_object('today_ymd', v_today_uk::text, 'now_utc', v_now::text)
      )
    from public.umbrellas u
    where u.id = any(p_context_ids);
  end if;

  for v_ctx in
    select t.context_id, t.ctx_json
    from tmp_ctx t
    order by t.context_id::text
  loop
    v_ctx_json := v_ctx.ctx_json;
    v_field_values := '{}'::jsonb;

    if v_selected_keys is not null and array_length(v_selected_keys,1) > 0 then
      foreach v_key in array v_selected_keys
      loop
        if v_key is null or length(btrim(v_key)) = 0 then
          continue;
        end if;

        select coalesce(nullif(btrim(coalesce(f.resolver_spec_json->>'path','')),''), v_key)
          into v_path
        from public.mailshot_fields f
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

    -- Resolve TO (always)
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

    -- recipient_id derived from ctx_json
    v_recipient_id := null;
    if v_recipient_kind = 'candidate' then
      v_recipient_id := nullif((v_ctx_json->'candidate'->>'id'),'')::uuid;
    elsif v_recipient_kind = 'client' then
      v_recipient_id := nullif((v_ctx_json->'client'->>'id'),'')::uuid;
    elsif v_recipient_kind = 'umbrella' then
      v_recipient_id := nullif((v_ctx_json->'umbrella'->>'id'),'')::uuid;
    end if;

    -- opt-in checks
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
        -- if no settings row, default allow for clients
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
        'eligible', v_opt_ok,
        'skip_reason', v_skip_reason,
        'field_values', v_field_values,
        'template_content_json', case when v_has_template then v_template.template_content_json else '{}'::jsonb end,
        'email_type', v_email_type
      )
    );
  end loop;

  return jsonb_build_object(
    'ok', true,
    'context_kind', v_context_kind,
    'entity_type', v_entity_type,
    'output_type', v_output_type,
    'document_template_id', case when p_document_template_id is null then null else p_document_template_id::text end,
    'selected_field_keys', to_jsonb(v_selected_keys),
    'to_field_key', v_to_field_key,
    'rows', v_rows
  );
end;
$$;






create or replace function public.mailshot_export(
  p_prepare_json jsonb,
  p_final_edits_json jsonb,
  p_format text,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_output_type text;
  v_format text := upper(btrim(coalesce(p_format,'')));

  v_rows jsonb;
  v_row jsonb;

  v_selected_keys jsonb;

  v_message_tpl text;
  v_body_html_tpl text;
  v_body_text_tpl text;
  v_subject_tpl text;

  v_field_values jsonb;
  v_kv record;

  v_rendered_message text;
  v_rendered_body_html text;
  v_rendered_body_text text;
  v_rendered_subject text;

  v_export_rows jsonb := '[]'::jsonb;

  v_cols jsonb := '[]'::jsonb;

  v_key text;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_prepare_json is null or jsonb_typeof(p_prepare_json) <> 'object' then
    raise exception 'prepare_json object required';
  end if;

  if v_format not in ('XLSX','CSV','WORD') then
    raise exception 'format must be XLSX, CSV, or WORD';
  end if;

  v_output_type := upper(coalesce(p_prepare_json->>'output_type',''));
  v_rows := p_prepare_json->'rows';
  v_selected_keys := p_prepare_json->'selected_field_keys';

  if v_output_type not in ('WORD','EXCEL') then
    raise exception 'mailshot_export is only for WORD/EXCEL output types (got: %)', v_output_type;
  end if;

  if v_rows is null or jsonb_typeof(v_rows) <> 'array' then
    raise exception 'prepare_json.rows must be array';
  end if;

  if v_output_type = 'EXCEL' and v_format not in ('XLSX','CSV') then
    raise exception 'EXCEL export requires XLSX or CSV format';
  end if;

  if v_output_type = 'WORD' and v_format <> 'WORD' then
    raise exception 'WORD export requires WORD format';
  end if;

  -- template sources (edits override template_content_json)
  v_subject_tpl := nullif(coalesce(p_final_edits_json->>'subject',''), '');
  v_body_text_tpl := nullif(coalesce(p_final_edits_json->>'body_text',''), '');
  v_body_html_tpl := nullif(coalesce(p_final_edits_json->>'body_html',''), '');
  v_message_tpl := nullif(coalesce(p_final_edits_json->>'message_text',''), '');

  if v_output_type = 'EXCEL' then
    if v_selected_keys is not null and jsonb_typeof(v_selected_keys) = 'array' then
      v_cols := v_selected_keys;
    else
      v_cols := '[]'::jsonb;
    end if;

    for v_row in
      select value
      from jsonb_array_elements(v_rows)
    loop
      if coalesce((v_row->>'eligible')::boolean,false) = false then
        continue;
      end if;

      v_field_values := coalesce(v_row->'field_values','{}'::jsonb);

      v_export_rows := v_export_rows || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'to', v_row->>'to',
          'fields', v_field_values
        )
      );
    end loop;

    return jsonb_build_object(
      'ok', true,
      'kind', 'EXCEL',
      'format', v_format,
      'columns', v_cols,
      'rows', v_export_rows,
      'generated_at_utc', v_now::text
    );
  end if;

  -- WORD export: render HTML per eligible row (backend will wrap into Word-openable document with page breaks)
  for v_row in
    select value
    from jsonb_array_elements(v_rows)
  loop
    if coalesce((v_row->>'eligible')::boolean,false) = false then
      continue;
    end if;

    v_field_values := coalesce(v_row->'field_values','{}'::jsonb);

    if v_body_html_tpl is null then
      v_body_html_tpl := nullif(coalesce((v_row->'template_content_json'->>'body_html'),''), '');
    end if;

    if v_body_text_tpl is null then
      v_body_text_tpl := nullif(coalesce((v_row->'template_content_json'->>'body_text'),''), '');
    end if;

    v_rendered_body_html := coalesce(v_body_html_tpl, '');
    v_rendered_body_text := coalesce(v_body_text_tpl, '');

    for v_kv in
      select e.key as k, coalesce(e.value,'') as v
      from jsonb_each_text(v_field_values) e
    loop
      v_rendered_body_html := replace(v_rendered_body_html, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_body_text := replace(v_rendered_body_text, '{{' || v_kv.k || '}}', v_kv.v);
    end loop;

    v_export_rows := v_export_rows || jsonb_build_array(
      jsonb_build_object(
        'context_id', v_row->>'context_id',
        'to', v_row->>'to',
        'body_html', nullif(v_rendered_body_html,''),
        'body_text', nullif(v_rendered_body_text,'')
      )
    );
  end loop;

  return jsonb_build_object(
    'ok', true,
    'kind', 'WORD',
    'format', 'WORD',
    'pages', v_export_rows,
    'generated_at_utc', v_now::text
  );
end;
$$;



create or replace function public.outbox_unified_list(
  p_status text,
  p_channel text,
  p_search text,
  p_queue_state text,
  p_limit int,
  p_offset int,
  p_sort_by text default 'created_at_utc',
  p_sort_dir text default 'desc'
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_status text := nullif(upper(btrim(coalesce(p_status,''))), '');
  v_channel text := nullif(upper(btrim(coalesce(p_channel,''))), '');
  v_search text := nullif(btrim(coalesce(p_search,'')), '');
  v_queue_state text := nullif(upper(btrim(coalesce(p_queue_state,''))), '');

  v_limit int := coalesce(p_limit, 50);
  v_offset int := coalesce(p_offset, 0);

  v_sort_by text := lower(btrim(coalesce(p_sort_by, 'created_at_utc')));
  v_sort_dir text := lower(btrim(coalesce(p_sort_dir, 'desc')));

  v_now timestamptz := now();

  v_total bigint := 0;
  v_items jsonb := '[]'::jsonb;
begin
  if v_limit < 1 then v_limit := 1; end if;
  if v_limit > 500 then v_limit := 500; end if;
  if v_offset < 0 then v_offset := 0; end if;

  if v_sort_by not in ('created_at_utc', 'scheduled_for_utc', 'effective_ready_at_utc', 'status', 'channel') then
    v_sort_by := 'created_at_utc';
  end if;

  if v_sort_dir not in ('asc', 'desc') then
    v_sort_dir := 'desc';
  end if;

  if v_queue_state is not null and v_queue_state not in ('SCHEDULED', 'QUEUED', 'SENT', 'DELIVERED', 'READ', 'FAILED') then
    v_queue_state := null;
  end if;

  with base_rows as (
    select
      u.channel,
      u.outbox_id,
      u.outbox_type,
      u.status,
      u.delivery_status,
      u.created_at_utc,
      u.sent_at,
      u.delivered_at,
      u.read_at,
      u.failed_at,
      u.to_address,
      u.subject,
      u.body_text,
      u.reference,
      u.provider_message_id,
      u.last_error,
      u.created_by,
      u.recipient_kind,
      u.recipient_id,
      u.context_kind,
      u.context_id,
      u.mailshot_run_id,
      u.document_template_id,
      u.scheduled_for_utc,
      u.next_attempt_at_utc,
      coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) as effective_ready_at_utc,
      case
        when u.read_at is not null then 'READ'
        when u.delivered_at is not null then 'DELIVERED'
        when u.sent_at is not null then 'SENT'
        when upper(coalesce(u.status,'')) = 'FAILED' or u.failed_at is not null then 'FAILED'
        when upper(coalesce(u.status,'')) = 'QUEUED'
             and coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) > v_now then 'SCHEDULED'
        when upper(coalesce(u.status,'')) = 'QUEUED' then 'QUEUED'
        else upper(coalesce(u.status,''))
      end as queue_state,
      (u.scheduled_for_utc is not null) as is_scheduled
    from public.v_outbox_unified as u
  ),
  filtered as (
    select
      b.channel,
      b.outbox_id,
      b.outbox_type,
      b.status,
      b.delivery_status,
      b.created_at_utc,
      b.sent_at,
      b.delivered_at,
      b.read_at,
      b.failed_at,
      b.to_address,
      b.subject,
      b.body_text,
      b.reference,
      b.provider_message_id,
      b.last_error,
      b.created_by,
      b.recipient_kind,
      b.recipient_id,
      b.context_kind,
      b.context_id,
      b.mailshot_run_id,
      b.document_template_id,
      b.scheduled_for_utc,
      b.next_attempt_at_utc,
      b.effective_ready_at_utc,
      b.queue_state,
      b.is_scheduled
    from base_rows as b
    where
      (v_status is null or upper(coalesce(b.status,'')) = v_status)
      and (v_channel is null or upper(coalesce(b.channel,'')) = v_channel)
      and (v_queue_state is null or b.queue_state = v_queue_state)
      and (
        v_search is null
        or coalesce(b.to_address,'') ilike ('%' || v_search || '%')
        or coalesce(b.subject,'') ilike ('%' || v_search || '%')
        or coalesce(b.body_text,'') ilike ('%' || v_search || '%')
        or coalesce(b.reference,'') ilike ('%' || v_search || '%')
        or coalesce(b.last_error,'') ilike ('%' || v_search || '%')
      )
  ),
  counted as (
    select count(*)::bigint as total_count
    from filtered as f
  ),
  paged as (
    select
      f.channel,
      f.outbox_id,
      f.outbox_type,
      f.status,
      f.delivery_status,
      f.created_at_utc,
      f.sent_at,
      f.delivered_at,
      f.read_at,
      f.failed_at,
      f.to_address,
      f.subject,
      f.body_text,
      f.reference,
      f.provider_message_id,
      f.last_error,
      f.created_by,
      f.recipient_kind,
      f.recipient_id,
      f.context_kind,
      f.context_id,
      f.mailshot_run_id,
      f.document_template_id,
      f.scheduled_for_utc,
      f.next_attempt_at_utc,
      f.effective_ready_at_utc,
      f.queue_state,
      f.is_scheduled
    from filtered as f
    order by
      case when v_sort_by = 'created_at_utc' and v_sort_dir = 'asc' then f.created_at_utc end asc nulls last,
      case when v_sort_by = 'created_at_utc' and v_sort_dir = 'desc' then f.created_at_utc end desc nulls last,

      case when v_sort_by = 'scheduled_for_utc' and v_sort_dir = 'asc' then f.scheduled_for_utc end asc nulls last,
      case when v_sort_by = 'scheduled_for_utc' and v_sort_dir = 'desc' then f.scheduled_for_utc end desc nulls last,

      case when v_sort_by = 'effective_ready_at_utc' and v_sort_dir = 'asc' then f.effective_ready_at_utc end asc nulls last,
      case when v_sort_by = 'effective_ready_at_utc' and v_sort_dir = 'desc' then f.effective_ready_at_utc end desc nulls last,

      case when v_sort_by = 'status' and v_sort_dir = 'asc' then f.queue_state end asc nulls last,
      case when v_sort_by = 'status' and v_sort_dir = 'desc' then f.queue_state end desc nulls last,

      case when v_sort_by = 'channel' and v_sort_dir = 'asc' then f.channel end asc nulls last,
      case when v_sort_by = 'channel' and v_sort_dir = 'desc' then f.channel end desc nulls last,

      f.created_at_utc desc,
      f.outbox_id::text desc
    limit v_limit offset v_offset
  )
  select
    (select c.total_count from counted as c),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'channel', p.channel,
          'outbox_id', p.outbox_id::text,
          'outbox_type', p.outbox_type,
          'status', p.status,
          'delivery_status', p.delivery_status,
          'created_at_utc', p.created_at_utc::text,
          'sent_at', case when p.sent_at is null then null else p.sent_at::text end,
          'delivered_at', case when p.delivered_at is null then null else p.delivered_at::text end,
          'read_at', case when p.read_at is null then null else p.read_at::text end,
          'failed_at', case when p.failed_at is null then null else p.failed_at::text end,
          'to_address', p.to_address,
          'subject', p.subject,
          'body_preview', case
            when p.body_text is null then null
            when char_length(p.body_text) <= 200 then p.body_text
            else left(p.body_text, 200) || '…'
          end,
          'reference', p.reference,
          'provider_message_id', p.provider_message_id,
          'last_error', p.last_error,
          'created_by', case when p.created_by is null then null else p.created_by::text end,
          'recipient_kind', p.recipient_kind,
          'recipient_id', case when p.recipient_id is null then null else p.recipient_id::text end,
          'context_kind', p.context_kind,
          'context_id', case when p.context_id is null then null else p.context_id::text end,
          'mailshot_run_id', case when p.mailshot_run_id is null then null else p.mailshot_run_id::text end,
          'document_template_id', case when p.document_template_id is null then null else p.document_template_id::text end,
          'scheduled_for_utc', case when p.scheduled_for_utc is null then null else p.scheduled_for_utc::text end,
          'next_attempt_at_utc', case when p.next_attempt_at_utc is null then null else p.next_attempt_at_utc::text end,
          'effective_ready_at_utc', case when p.effective_ready_at_utc is null then null else p.effective_ready_at_utc::text end,
          'queue_state', p.queue_state,
          'is_scheduled', p.is_scheduled
        )
        order by
          case when v_sort_by = 'created_at_utc' and v_sort_dir = 'asc' then p.created_at_utc end asc nulls last,
          case when v_sort_by = 'created_at_utc' and v_sort_dir = 'desc' then p.created_at_utc end desc nulls last,

          case when v_sort_by = 'scheduled_for_utc' and v_sort_dir = 'asc' then p.scheduled_for_utc end asc nulls last,
          case when v_sort_by = 'scheduled_for_utc' and v_sort_dir = 'desc' then p.scheduled_for_utc end desc nulls last,

          case when v_sort_by = 'effective_ready_at_utc' and v_sort_dir = 'asc' then p.effective_ready_at_utc end asc nulls last,
          case when v_sort_by = 'effective_ready_at_utc' and v_sort_dir = 'desc' then p.effective_ready_at_utc end desc nulls last,

          case when v_sort_by = 'status' and v_sort_dir = 'asc' then p.queue_state end asc nulls last,
          case when v_sort_by = 'status' and v_sort_dir = 'desc' then p.queue_state end desc nulls last,

          case when v_sort_by = 'channel' and v_sort_dir = 'asc' then p.channel end asc nulls last,
          case when v_sort_by = 'channel' and v_sort_dir = 'desc' then p.channel end desc nulls last,

          p.created_at_utc desc,
          p.outbox_id::text desc
      ),
      '[]'::jsonb
    )
  into v_total, v_items
  from paged as p;

  return jsonb_build_object(
    'ok', true,
    'filters', jsonb_build_object(
      'status', v_status,
      'channel', v_channel,
      'search', v_search,
      'queue_state', v_queue_state
    ),
    'sort', jsonb_build_object(
      'sort_by', v_sort_by,
      'sort_dir', v_sort_dir
    ),
    'limit', v_limit,
    'offset', v_offset,
    'total_count', v_total,
    'items', v_items
  );
end;
$$;

create or replace function public.outbox_unified_get(
  p_channel text,
  p_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_channel text := nullif(upper(btrim(coalesce(p_channel,''))), '');
  v_now timestamptz := now();
  v_row record;
begin
  if v_channel is null then
    raise exception 'channel required';
  end if;

  if p_id is null then
    raise exception 'id required';
  end if;

  select
    u.channel,
    u.outbox_id,
    u.outbox_type,
    u.status,
    u.delivery_status,
    u.created_at_utc,
    u.sent_at,
    u.delivered_at,
    u.read_at,
    u.failed_at,
    u.to_address,
    u.cc,
    u.bcc,
    u.reply_to,
    u.importance,
    u.email_type,
    u.subject,
    u.body_text,
    u.body_html,
    u.attachments,
    u.reference,
    u.provider_message_id,
    u.last_error,
    u.created_by,
    u.recipient_kind,
    u.recipient_id,
    u.context_kind,
    u.context_id,
    u.mailshot_run_id,
    u.document_template_id,
    u.scheduled_for_utc,
    u.next_attempt_at_utc,
    coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) as effective_ready_at_utc,
    case
      when u.read_at is not null then 'READ'
      when u.delivered_at is not null then 'DELIVERED'
      when u.sent_at is not null then 'SENT'
      when upper(coalesce(u.status,'')) = 'FAILED' or u.failed_at is not null then 'FAILED'
      when upper(coalesce(u.status,'')) = 'QUEUED'
           and coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) > v_now then 'SCHEDULED'
      when upper(coalesce(u.status,'')) = 'QUEUED' then 'QUEUED'
      else upper(coalesce(u.status,''))
    end as queue_state,
    (u.scheduled_for_utc is not null) as is_scheduled
  into v_row
  from public.v_outbox_unified as u
  where upper(coalesce(u.channel,'')) = v_channel
    and u.outbox_id = p_id;

  if not found then
    raise exception 'outbox row not found for channel % id %', v_channel, p_id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'row', jsonb_build_object(
      'channel', v_row.channel,
      'outbox_id', v_row.outbox_id::text,
      'outbox_type', v_row.outbox_type,
      'status', v_row.status,
      'delivery_status', v_row.delivery_status,
      'created_at_utc', v_row.created_at_utc::text,
      'sent_at', case when v_row.sent_at is null then null else v_row.sent_at::text end,
      'delivered_at', case when v_row.delivered_at is null then null else v_row.delivered_at::text end,
      'read_at', case when v_row.read_at is null then null else v_row.read_at::text end,
      'failed_at', case when v_row.failed_at is null then null else v_row.failed_at::text end,
      'to_address', v_row.to_address,
      'cc', v_row.cc,
      'bcc', v_row.bcc,
      'reply_to', v_row.reply_to,
      'importance', v_row.importance,
      'email_type', v_row.email_type,
      'subject', v_row.subject,
      'body_text', v_row.body_text,
      'body_html', v_row.body_html,
      'attachments', v_row.attachments,
      'reference', v_row.reference,
      'provider_message_id', v_row.provider_message_id,
      'last_error', v_row.last_error,
      'created_by', case when v_row.created_by is null then null else v_row.created_by::text end,
      'recipient_kind', v_row.recipient_kind,
      'recipient_id', case when v_row.recipient_id is null then null else v_row.recipient_id::text end,
      'context_kind', v_row.context_kind,
      'context_id', case when v_row.context_id is null then null else v_row.context_id::text end,
      'mailshot_run_id', case when v_row.mailshot_run_id is null then null else v_row.mailshot_run_id::text end,
      'document_template_id', case when v_row.document_template_id is null then null else v_row.document_template_id::text end,
      'scheduled_for_utc', case when v_row.scheduled_for_utc is null then null else v_row.scheduled_for_utc::text end,
      'next_attempt_at_utc', case when v_row.next_attempt_at_utc is null then null else v_row.next_attempt_at_utc::text end,
      'effective_ready_at_utc', case when v_row.effective_ready_at_utc is null then null else v_row.effective_ready_at_utc::text end,
      'queue_state', v_row.queue_state,
      'is_scheduled', v_row.is_scheduled
    )
  );
end;
$$;

create or replace function public.comms_by_recipient(
  p_recipient_kind text,
  p_recipient_id uuid,
  p_limit int,
  p_offset int
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_kind text := nullif(lower(btrim(coalesce(p_recipient_kind,''))), '');
  v_limit int := coalesce(p_limit, 50);
  v_offset int := coalesce(p_offset, 0);
  v_now timestamptz := now();

  v_total bigint := 0;
  v_items jsonb := '[]'::jsonb;
begin
  if v_kind is null then
    raise exception 'recipient_kind required';
  end if;

  if p_recipient_id is null then
    raise exception 'recipient_id required';
  end if;

  if v_limit < 1 then v_limit := 1; end if;
  if v_limit > 500 then v_limit := 500; end if;
  if v_offset < 0 then v_offset := 0; end if;

  with base_rows as (
    select
      u.channel,
      u.outbox_id,
      u.outbox_type,
      u.status,
      u.delivery_status,
      u.created_at_utc,
      u.sent_at,
      u.delivered_at,
      u.read_at,
      u.failed_at,
      u.to_address,
      u.subject,
      u.body_text,
      u.reference,
      u.provider_message_id,
      u.last_error,
      u.created_by,
      u.recipient_kind,
      u.recipient_id,
      u.context_kind,
      u.context_id,
      u.mailshot_run_id,
      u.document_template_id,
      u.scheduled_for_utc,
      u.next_attempt_at_utc,
      coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) as effective_ready_at_utc,
      case
        when u.read_at is not null then 'READ'
        when u.delivered_at is not null then 'DELIVERED'
        when u.sent_at is not null then 'SENT'
        when upper(coalesce(u.status,'')) = 'FAILED' or u.failed_at is not null then 'FAILED'
        when upper(coalesce(u.status,'')) = 'QUEUED'
             and coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) > v_now then 'SCHEDULED'
        when upper(coalesce(u.status,'')) = 'QUEUED' then 'QUEUED'
        else upper(coalesce(u.status,''))
      end as queue_state,
      (u.scheduled_for_utc is not null) as is_scheduled
    from public.v_outbox_unified as u
    where lower(coalesce(u.recipient_kind,'')) = v_kind
      and u.recipient_id = p_recipient_id
  ),
  counted as (
    select count(*)::bigint as total_count
    from base_rows as b
  ),
  paged as (
    select
      b.channel,
      b.outbox_id,
      b.outbox_type,
      b.status,
      b.delivery_status,
      b.created_at_utc,
      b.sent_at,
      b.delivered_at,
      b.read_at,
      b.failed_at,
      b.to_address,
      b.subject,
      b.body_text,
      b.reference,
      b.provider_message_id,
      b.last_error,
      b.created_by,
      b.recipient_kind,
      b.recipient_id,
      b.context_kind,
      b.context_id,
      b.mailshot_run_id,
      b.document_template_id,
      b.scheduled_for_utc,
      b.next_attempt_at_utc,
      b.effective_ready_at_utc,
      b.queue_state,
      b.is_scheduled
    from base_rows as b
    order by b.created_at_utc desc, b.outbox_id::text desc
    limit v_limit offset v_offset
  )
  select
    (select c.total_count from counted as c),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'channel', p.channel,
          'outbox_id', p.outbox_id::text,
          'outbox_type', p.outbox_type,
          'status', p.status,
          'delivery_status', p.delivery_status,
          'created_at_utc', p.created_at_utc::text,
          'sent_at', case when p.sent_at is null then null else p.sent_at::text end,
          'delivered_at', case when p.delivered_at is null then null else p.delivered_at::text end,
          'read_at', case when p.read_at is null then null else p.read_at::text end,
          'failed_at', case when p.failed_at is null then null else p.failed_at::text end,
          'to_address', p.to_address,
          'subject', p.subject,
          'body_preview', case
            when p.body_text is null then null
            when char_length(p.body_text) <= 200 then p.body_text
            else left(p.body_text, 200) || '…'
          end,
          'reference', p.reference,
          'provider_message_id', p.provider_message_id,
          'last_error', p.last_error,
          'created_by', case when p.created_by is null then null else p.created_by::text end,
          'recipient_kind', p.recipient_kind,
          'recipient_id', case when p.recipient_id is null then null else p.recipient_id::text end,
          'context_kind', p.context_kind,
          'context_id', case when p.context_id is null then null else p.context_id::text end,
          'mailshot_run_id', case when p.mailshot_run_id is null then null else p.mailshot_run_id::text end,
          'document_template_id', case when p.document_template_id is null then null else p.document_template_id::text end,
          'scheduled_for_utc', case when p.scheduled_for_utc is null then null else p.scheduled_for_utc::text end,
          'next_attempt_at_utc', case when p.next_attempt_at_utc is null then null else p.next_attempt_at_utc::text end,
          'effective_ready_at_utc', case when p.effective_ready_at_utc is null then null else p.effective_ready_at_utc::text end,
          'queue_state', p.queue_state,
          'is_scheduled', p.is_scheduled
        )
        order by p.created_at_utc desc, p.outbox_id::text desc
      ),
      '[]'::jsonb
    )
  into v_total, v_items
  from paged as p;

  return jsonb_build_object(
    'ok', true,
    'recipient_kind', v_kind,
    'recipient_id', p_recipient_id::text,
    'limit', v_limit,
    'offset', v_offset,
    'total_count', v_total,
    'items', v_items
  );
end;
$$;



create or replace function public.outbox_unified_delete(
  p_channel text,
  p_id uuid,
  p_actor_user_id uuid,
  p_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_channel text := nullif(upper(btrim(coalesce(p_channel,''))), '');
  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
  v_now timestamptz := now();

  v_mail_row public.mail_outbox%rowtype;
  v_comms_row public.comms_outbox%rowtype;

  v_row_channel text;
  v_row_id_text text;
  v_row_status text;
  v_row_to text;
  v_row_mailshot_run_id_text text;
  v_row_recipient_kind text;
  v_row_recipient_id_text text;
  v_row_context_kind text;
  v_row_context_id_text text;

  v_block_reason text := null;
  v_deleted boolean := false;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if v_channel is null then
    raise exception 'channel required';
  end if;

  if p_id is null then
    raise exception 'id required';
  end if;

  if v_channel not in ('EMAIL','WHATSAPP','SMS','VOICE') then
    raise exception 'unsupported channel %', v_channel;
  end if;

  if v_channel = 'EMAIL' then
    select mo.*
    into v_mail_row
    from public.mail_outbox as mo
    where mo.id = p_id
    for update;

    if not found then
      return jsonb_build_object(
        'ok', true,
        'deleted', false,
        'reason', 'NOT_FOUND',
        'row', jsonb_build_object(
          'channel', v_channel,
          'outbox_id', p_id::text
        )
      );
    end if;

    v_row_channel := 'EMAIL';
    v_row_id_text := v_mail_row.id::text;
    v_row_status := v_mail_row.status::text;
    v_row_to := v_mail_row."to";
    v_row_mailshot_run_id_text := case when v_mail_row.mailshot_run_id is null then null else v_mail_row.mailshot_run_id::text end;
    v_row_recipient_kind := v_mail_row.recipient_kind;
    v_row_recipient_id_text := case when v_mail_row.recipient_id is null then null else v_mail_row.recipient_id::text end;
    v_row_context_kind := v_mail_row.context_kind;
    v_row_context_id_text := case when v_mail_row.context_id is null then null else v_mail_row.context_id::text end;

    if v_mail_row.read_at is not null then
      v_block_reason := 'ALREADY_READ';
    elsif v_mail_row.delivered_at is not null then
      v_block_reason := 'ALREADY_DELIVERED';
    elsif v_mail_row.sent_at is not null then
      v_block_reason := 'ALREADY_SENT';
    elsif upper(coalesce(v_mail_row.status::text,'')) not in ('QUEUED','FAILED') then
      v_block_reason := 'INVALID_STATUS';
    end if;

    if v_block_reason is null then
      perform public._audit_insert(
        'outbox',
        v_mail_row.id::text,
        'OUTBOX_DELETE',
        jsonb_build_object(
          'channel', 'EMAIL',
          'outbox_id', v_mail_row.id::text,
          'status', v_mail_row.status::text,
          'to_address', v_mail_row."to",
          'mailshot_run_id', case when v_mail_row.mailshot_run_id is null then null else v_mail_row.mailshot_run_id::text end,
          'recipient_kind', v_mail_row.recipient_kind,
          'recipient_id', case when v_mail_row.recipient_id is null then null else v_mail_row.recipient_id::text end,
          'context_kind', v_mail_row.context_kind,
          'context_id', case when v_mail_row.context_id is null then null else v_mail_row.context_id::text end
        ),
        null,
        v_reason,
        p_actor_user_id
      );

      delete from public.mail_outbox as mo
      where mo.id = v_mail_row.id;

      v_deleted := true;
    end if;

  else
    select co.*
    into v_comms_row
    from public.comms_outbox as co
    where co.id = p_id
    for update;

    if not found then
      return jsonb_build_object(
        'ok', true,
        'deleted', false,
        'reason', 'NOT_FOUND',
        'row', jsonb_build_object(
          'channel', v_channel,
          'outbox_id', p_id::text
        )
      );
    end if;

    if upper(coalesce(v_comms_row.channel,'')) <> v_channel then
      return jsonb_build_object(
        'ok', true,
        'deleted', false,
        'reason', 'CHANNEL_MISMATCH',
        'row', jsonb_build_object(
          'channel', v_comms_row.channel,
          'outbox_id', v_comms_row.id::text
        )
      );
    end if;

    v_row_channel := v_comms_row.channel;
    v_row_id_text := v_comms_row.id::text;
    v_row_status := v_comms_row.status;
    v_row_to := v_comms_row.to_address;
    v_row_mailshot_run_id_text := case when v_comms_row.mailshot_run_id is null then null else v_comms_row.mailshot_run_id::text end;
    v_row_recipient_kind := v_comms_row.recipient_kind;
    v_row_recipient_id_text := case when v_comms_row.recipient_id is null then null else v_comms_row.recipient_id::text end;
    v_row_context_kind := v_comms_row.context_kind;
    v_row_context_id_text := case when v_comms_row.context_id is null then null else v_comms_row.context_id::text end;

    if v_comms_row.read_at is not null then
      v_block_reason := 'ALREADY_READ';
    elsif v_comms_row.delivered_at is not null then
      v_block_reason := 'ALREADY_DELIVERED';
    elsif v_comms_row.sent_at is not null then
      v_block_reason := 'ALREADY_SENT';
    elsif upper(coalesce(v_comms_row.status,'')) not in ('QUEUED','FAILED') then
      v_block_reason := 'INVALID_STATUS';
    end if;

    if v_block_reason is null then
      perform public._audit_insert(
        'outbox',
        v_comms_row.id::text,
        'OUTBOX_DELETE',
        jsonb_build_object(
          'channel', v_comms_row.channel,
          'outbox_id', v_comms_row.id::text,
          'status', v_comms_row.status,
          'to_address', v_comms_row.to_address,
          'mailshot_run_id', case when v_comms_row.mailshot_run_id is null then null else v_comms_row.mailshot_run_id::text end,
          'recipient_kind', v_comms_row.recipient_kind,
          'recipient_id', case when v_comms_row.recipient_id is null then null else v_comms_row.recipient_id::text end,
          'context_kind', v_comms_row.context_kind,
          'context_id', case when v_comms_row.context_id is null then null else v_comms_row.context_id::text end
        ),
        null,
        v_reason,
        p_actor_user_id
      );

      delete from public.comms_outbox as co
      where co.id = v_comms_row.id;

      v_deleted := true;
    end if;
  end if;

  return jsonb_build_object(
    'ok', true,
    'deleted', v_deleted,
    'deleted_at_utc', case when v_deleted then v_now::text else null end,
    'reason', case when v_deleted then null else v_block_reason end,
    'row', jsonb_build_object(
      'channel', v_row_channel,
      'outbox_id', v_row_id_text,
      'status', v_row_status,
      'to_address', v_row_to,
      'mailshot_run_id', v_row_mailshot_run_id_text,
      'recipient_kind', v_row_recipient_kind,
      'recipient_id', v_row_recipient_id_text,
      'context_kind', v_row_context_kind,
      'context_id', v_row_context_id_text
    )
  );
end;
$$;


create or replace function public.outbox_unified_delete_many(
  p_items jsonb,
  p_actor_user_id uuid,
  p_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
  v_item jsonb;
  v_channel text;
  v_id_text text;
  v_id uuid;

  v_result jsonb;
  v_results jsonb := '[]'::jsonb;

  v_requested int := 0;
  v_deleted int := 0;
  v_blocked_already_sent int := 0;
  v_blocked_invalid_status int := 0;
  v_not_found int := 0;
  v_other_blocked int := 0;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_items is null or jsonb_typeof(p_items) <> 'array' then
    raise exception 'items array required';
  end if;

  for v_item in
    select jbe.value
    from jsonb_array_elements(p_items) as jbe(value)
  loop
    v_requested := v_requested + 1;

    v_channel := nullif(upper(btrim(coalesce(v_item->>'channel',''))), '');
    v_id_text := nullif(btrim(coalesce(v_item->>'id','')), '');

    if v_channel is null or v_id_text is null then
      v_results := v_results || jsonb_build_array(
        jsonb_build_object(
          'ok', true,
          'deleted', false,
          'reason', 'INVALID_ITEM',
          'row', jsonb_build_object(
            'channel', v_channel,
            'outbox_id', v_id_text
          )
        )
      );
      v_other_blocked := v_other_blocked + 1;
      continue;
    end if;

    begin
      v_id := v_id_text::uuid;
    exception
      when others then
        v_results := v_results || jsonb_build_array(
          jsonb_build_object(
            'ok', true,
            'deleted', false,
            'reason', 'INVALID_ID',
            'row', jsonb_build_object(
              'channel', v_channel,
              'outbox_id', v_id_text
            )
          )
        );
        v_other_blocked := v_other_blocked + 1;
        continue;
    end;

    v_result := public.outbox_unified_delete(
      v_channel,
      v_id,
      p_actor_user_id,
      v_reason
    );

    v_results := v_results || jsonb_build_array(v_result);

    if coalesce((v_result->>'deleted')::boolean, false) = true then
      v_deleted := v_deleted + 1;
    else
      case upper(coalesce(v_result->>'reason',''))
        when 'NOT_FOUND' then
          v_not_found := v_not_found + 1;
        when 'ALREADY_SENT', 'ALREADY_DELIVERED', 'ALREADY_READ' then
          v_blocked_already_sent := v_blocked_already_sent + 1;
        when 'INVALID_STATUS' then
          v_blocked_invalid_status := v_blocked_invalid_status + 1;
        else
          v_other_blocked := v_other_blocked + 1;
      end case;
    end if;
  end loop;

  perform public._audit_insert(
    'outbox_batch',
    null,
    'OUTBOX_DELETE_MANY',
    jsonb_build_object(
      'requested', v_requested
    ),
    jsonb_build_object(
      'requested', v_requested,
      'deleted', v_deleted,
      'blocked_already_sent', v_blocked_already_sent,
      'blocked_invalid_status', v_blocked_invalid_status,
      'not_found', v_not_found,
      'other_blocked', v_other_blocked
    ),
    v_reason,
    p_actor_user_id
  );

  return jsonb_build_object(
    'ok', true,
    'requested', v_requested,
    'deleted', v_deleted,
    'blocked_already_sent', v_blocked_already_sent,
    'blocked_invalid_status', v_blocked_invalid_status,
    'not_found', v_not_found,
    'other_blocked', v_other_blocked,
    'results', v_results
  );
end;
$$;

create or replace function public.mailshot_run_cancel_pending(
  p_mailshot_run_id uuid,
  p_actor_user_id uuid,
  p_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
  v_run public.mailshot_runs%rowtype;

  v_total_rows int := 0;
  v_deleted_pending int := 0;
  v_skipped_already_sent int := 0;

  v_deleted_mail_ids jsonb := '[]'::jsonb;
  v_deleted_comms_ids jsonb := '[]'::jsonb;

  v_mail_row public.mail_outbox%rowtype;
  v_comms_row public.comms_outbox%rowtype;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_mailshot_run_id is null then
    raise exception 'mailshot_run_id required';
  end if;

  select mr.*
  into v_run
  from public.mailshot_runs as mr
  where mr.id = p_mailshot_run_id
  for update;

  if not found then
    raise exception 'mailshot_run not found';
  end if;

  for v_mail_row in
    select mo.*
    from public.mail_outbox as mo
    where mo.mailshot_run_id = p_mailshot_run_id
    order by mo.created_at_utc asc, mo.id::text asc
    for update
  loop
    v_total_rows := v_total_rows + 1;

    if v_mail_row.read_at is not null
       or v_mail_row.delivered_at is not null
       or v_mail_row.sent_at is not null then
      v_skipped_already_sent := v_skipped_already_sent + 1;
      continue;
    end if;

    if upper(coalesce(v_mail_row.status::text,'')) not in ('QUEUED','FAILED') then
      v_skipped_already_sent := v_skipped_already_sent + 1;
      continue;
    end if;

    perform public._audit_insert(
      'outbox',
      v_mail_row.id::text,
      'MAILSHOT_RUN_CANCEL_PENDING_DELETE',
      jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'mailshot_run_id', p_mailshot_run_id::text,
        'status', v_mail_row.status::text,
        'to_address', v_mail_row."to"
      ),
      null,
      v_reason,
      p_actor_user_id
    );

    delete from public.mail_outbox as mo
    where mo.id = v_mail_row.id;

    v_deleted_pending := v_deleted_pending + 1;
    v_deleted_mail_ids := v_deleted_mail_ids || jsonb_build_array(v_mail_row.id::text);
  end loop;

  for v_comms_row in
    select co.*
    from public.comms_outbox as co
    where co.mailshot_run_id = p_mailshot_run_id
    order by co.created_at_utc asc, co.id::text asc
    for update
  loop
    v_total_rows := v_total_rows + 1;

    if v_comms_row.read_at is not null
       or v_comms_row.delivered_at is not null
       or v_comms_row.sent_at is not null then
      v_skipped_already_sent := v_skipped_already_sent + 1;
      continue;
    end if;

    if upper(coalesce(v_comms_row.status,'')) not in ('QUEUED','FAILED') then
      v_skipped_already_sent := v_skipped_already_sent + 1;
      continue;
    end if;

    perform public._audit_insert(
      'outbox',
      v_comms_row.id::text,
      'MAILSHOT_RUN_CANCEL_PENDING_DELETE',
      jsonb_build_object(
        'channel', v_comms_row.channel,
        'outbox_id', v_comms_row.id::text,
        'mailshot_run_id', p_mailshot_run_id::text,
        'status', v_comms_row.status,
        'to_address', v_comms_row.to_address
      ),
      null,
      v_reason,
      p_actor_user_id
    );

    delete from public.comms_outbox as co
    where co.id = v_comms_row.id;

    v_deleted_pending := v_deleted_pending + 1;
    v_deleted_comms_ids := v_deleted_comms_ids || jsonb_build_array(v_comms_row.id::text);
  end loop;

  perform public._audit_insert(
    'mailshot_run',
    p_mailshot_run_id::text,
    'MAILSHOT_RUN_CANCEL_PENDING',
    jsonb_build_object(
      'mailshot_run_id', p_mailshot_run_id::text,
      'total_rows', v_total_rows
    ),
    jsonb_build_object(
      'mailshot_run_id', p_mailshot_run_id::text,
      'total_rows', v_total_rows,
      'deleted_pending', v_deleted_pending,
      'skipped_already_sent', v_skipped_already_sent,
      'deleted_mail_outbox_ids', v_deleted_mail_ids,
      'deleted_comms_outbox_ids', v_deleted_comms_ids
    ),
    v_reason,
    p_actor_user_id
  );

  return jsonb_build_object(
    'ok', true,
    'mailshot_run_id', p_mailshot_run_id::text,
    'total_rows', v_total_rows,
    'deleted_pending', v_deleted_pending,
    'skipped_already_sent', v_skipped_already_sent,
    'deleted_mail_outbox_ids', v_deleted_mail_ids,
    'deleted_comms_outbox_ids', v_deleted_comms_ids
  );
end;
$$;

create or replace function public.mailshot_run_delete_if_unsent(
  p_mailshot_run_id uuid,
  p_actor_user_id uuid,
  p_reason text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_reason text := nullif(btrim(coalesce(p_reason,'')), '');
  v_run public.mailshot_runs%rowtype;

  v_total_rows int := 0;
  v_mail_row_count int := 0;
  v_comms_row_count int := 0;

  v_blocking_mail_sent int := 0;
  v_blocking_mail_delivered int := 0;
  v_blocking_mail_read int := 0;

  v_blocking_comms_sent int := 0;
  v_blocking_comms_delivered int := 0;
  v_blocking_comms_read int := 0;

  v_blocking_total int := 0;

  v_deleted_mail_ids jsonb := '[]'::jsonb;
  v_deleted_comms_ids jsonb := '[]'::jsonb;

  v_mail_row public.mail_outbox%rowtype;
  v_comms_row public.comms_outbox%rowtype;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_mailshot_run_id is null then
    raise exception 'mailshot_run_id required';
  end if;

  select mr.*
  into v_run
  from public.mailshot_runs as mr
  where mr.id = p_mailshot_run_id
  for update;

  if not found then
    raise exception 'mailshot_run not found';
  end if;

  select count(*)::int
  into v_mail_row_count
  from public.mail_outbox as mo
  where mo.mailshot_run_id = p_mailshot_run_id;

  select count(*)::int
  into v_comms_row_count
  from public.comms_outbox as co
  where co.mailshot_run_id = p_mailshot_run_id;

  v_total_rows := coalesce(v_mail_row_count, 0) + coalesce(v_comms_row_count, 0);

  select count(*)::int
  into v_blocking_mail_sent
  from public.mail_outbox as mo
  where mo.mailshot_run_id = p_mailshot_run_id
    and mo.sent_at is not null;

  select count(*)::int
  into v_blocking_mail_delivered
  from public.mail_outbox as mo
  where mo.mailshot_run_id = p_mailshot_run_id
    and mo.delivered_at is not null;

  select count(*)::int
  into v_blocking_mail_read
  from public.mail_outbox as mo
  where mo.mailshot_run_id = p_mailshot_run_id
    and mo.read_at is not null;

  select count(*)::int
  into v_blocking_comms_sent
  from public.comms_outbox as co
  where co.mailshot_run_id = p_mailshot_run_id
    and co.sent_at is not null;

  select count(*)::int
  into v_blocking_comms_delivered
  from public.comms_outbox as co
  where co.mailshot_run_id = p_mailshot_run_id
    and co.delivered_at is not null;

  select count(*)::int
  into v_blocking_comms_read
  from public.comms_outbox as co
  where co.mailshot_run_id = p_mailshot_run_id
    and co.read_at is not null;

  v_blocking_total :=
      coalesce(v_blocking_mail_sent, 0)
    + coalesce(v_blocking_mail_delivered, 0)
    + coalesce(v_blocking_mail_read, 0)
    + coalesce(v_blocking_comms_sent, 0)
    + coalesce(v_blocking_comms_delivered, 0)
    + coalesce(v_blocking_comms_read, 0);

  if v_blocking_total > 0 then
    return jsonb_build_object(
      'ok', true,
      'deleted', false,
      'reason', 'RUN_ALREADY_PARTIALLY_SENT',
      'mailshot_run_id', p_mailshot_run_id::text,
      'counts', jsonb_build_object(
        'total_rows', v_total_rows,
        'mail_rows', v_mail_row_count,
        'comms_rows', v_comms_row_count,
        'blocking_mail_sent', v_blocking_mail_sent,
        'blocking_mail_delivered', v_blocking_mail_delivered,
        'blocking_mail_read', v_blocking_mail_read,
        'blocking_comms_sent', v_blocking_comms_sent,
        'blocking_comms_delivered', v_blocking_comms_delivered,
        'blocking_comms_read', v_blocking_comms_read,
        'blocking_total', v_blocking_total
      )
    );
  end if;

  for v_mail_row in
    select mo.*
    from public.mail_outbox as mo
    where mo.mailshot_run_id = p_mailshot_run_id
    order by mo.created_at_utc asc, mo.id::text asc
    for update
  loop
    perform public._audit_insert(
      'outbox',
      v_mail_row.id::text,
      'MAILSHOT_RUN_DELETE_IF_UNSENT_CHILD_DELETE',
      jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', v_mail_row.status::text,
        'to_address', v_mail_row."to",
        'mailshot_run_id', p_mailshot_run_id::text
      ),
      null,
      v_reason,
      p_actor_user_id
    );

    delete from public.mail_outbox as mo
    where mo.id = v_mail_row.id;

    v_deleted_mail_ids := v_deleted_mail_ids || jsonb_build_array(v_mail_row.id::text);
  end loop;

  for v_comms_row in
    select co.*
    from public.comms_outbox as co
    where co.mailshot_run_id = p_mailshot_run_id
    order by co.created_at_utc asc, co.id::text asc
    for update
  loop
    perform public._audit_insert(
      'outbox',
      v_comms_row.id::text,
      'MAILSHOT_RUN_DELETE_IF_UNSENT_CHILD_DELETE',
      jsonb_build_object(
        'channel', v_comms_row.channel,
        'outbox_id', v_comms_row.id::text,
        'status', v_comms_row.status,
        'to_address', v_comms_row.to_address,
        'mailshot_run_id', p_mailshot_run_id::text
      ),
      null,
      v_reason,
      p_actor_user_id
    );

    delete from public.comms_outbox as co
    where co.id = v_comms_row.id;

    v_deleted_comms_ids := v_deleted_comms_ids || jsonb_build_array(v_comms_row.id::text);
  end loop;

  perform public._audit_insert(
    'mailshot_run',
    p_mailshot_run_id::text,
    'MAILSHOT_RUN_DELETE_IF_UNSENT',
    jsonb_build_object(
      'mailshot_run_id', p_mailshot_run_id::text,
      'context_kind', v_run.context_kind,
      'output_type', v_run.output_type,
      'document_template_id', case when v_run.document_template_id is null then null else v_run.document_template_id::text end,
      'created_by', case when v_run.created_by is null then null else v_run.created_by::text end,
      'created_at_utc', v_run.created_at_utc::text,
      'delivery_timing_json', v_run.delivery_timing_json
    ),
    jsonb_build_object(
      'deleted', true,
      'deleted_mail_outbox_ids', v_deleted_mail_ids,
      'deleted_comms_outbox_ids', v_deleted_comms_ids,
      'mail_row_count', v_mail_row_count,
      'comms_row_count', v_comms_row_count,
      'total_rows', v_total_rows
    ),
    v_reason,
    p_actor_user_id
  );

  delete from public.mailshot_runs as mr
  where mr.id = p_mailshot_run_id;

  return jsonb_build_object(
    'ok', true,
    'deleted', true,
    'mailshot_run_id', p_mailshot_run_id::text,
    'counts', jsonb_build_object(
      'total_rows', v_total_rows,
      'mail_rows', v_mail_row_count,
      'comms_rows', v_comms_row_count,
      'blocking_total', 0
    ),
    'deleted_mail_outbox_ids', v_deleted_mail_ids,
    'deleted_comms_outbox_ids', v_deleted_comms_ids
  );
end;
$$;


create or replace function public.outbox_unified_retry(
  p_channel text,
  p_id uuid,
  p_actor_user_id uuid,
  p_retry_at_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_channel text := nullif(upper(btrim(coalesce(p_channel,''))), '');
  v_retry_at_utc timestamptz := coalesce(p_retry_at_utc, now());
  v_reason text := 'RETRY';

  v_mail_row public.mail_outbox%rowtype;
  v_comms_row public.comms_outbox%rowtype;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if v_channel is null then
    raise exception 'channel required';
  end if;

  if p_id is null then
    raise exception 'id required';
  end if;

  if v_channel not in ('EMAIL','WHATSAPP','SMS','VOICE') then
    raise exception 'unsupported channel %', v_channel;
  end if;

  if v_channel = 'EMAIL' then
    select mo.*
    into v_mail_row
    from public.mail_outbox as mo
    where mo.id = p_id
    for update;

    if not found then
      raise exception 'outbox row not found';
    end if;

    if v_mail_row.read_at is not null
       or v_mail_row.delivered_at is not null
       or v_mail_row.sent_at is not null then
      raise exception 'outbox row is not retryable because it has already been sent';
    end if;

    if upper(coalesce(v_mail_row.status::text,'')) not in ('QUEUED','FAILED') then
      raise exception 'outbox row status is not retryable';
    end if;

    perform public._audit_insert(
      'outbox',
      v_mail_row.id::text,
      'OUTBOX_RETRY',
      jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', v_mail_row.status::text,
        'next_attempt_at_utc', case when v_mail_row.next_attempt_at_utc is null then null else v_mail_row.next_attempt_at_utc::text end,
        'scheduled_for_utc', case when v_mail_row.scheduled_for_utc is null then null else v_mail_row.scheduled_for_utc::text end,
        'provider_message_id', v_mail_row.provider_message_id,
        'provider_status', v_mail_row.provider_status,
        'last_error', v_mail_row.last_error
      ),
      jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', 'QUEUED',
        'next_attempt_at_utc', v_retry_at_utc::text,
        'scheduled_for_utc', case when v_mail_row.scheduled_for_utc is null then null else v_mail_row.scheduled_for_utc::text end,
        'provider_message_id', null,
        'provider_status', null,
        'last_error', null
      ),
      v_reason,
      p_actor_user_id
    );

    update public.mail_outbox as mo
    set status = 'QUEUED'::public.mail_status_enum,
        sent_at = null,
        delivered_at = null,
        read_at = null,
        failed_at = null,
        last_error = null,
        provider_message_id = null,
        provider_status = null,
        next_attempt_at_utc = v_retry_at_utc
    where mo.id = v_mail_row.id;

    select mo.*
    into v_mail_row
    from public.mail_outbox as mo
    where mo.id = p_id;

    return jsonb_build_object(
      'ok', true,
      'row', jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', v_mail_row.status::text,
        'to_address', v_mail_row."to",
        'mailshot_run_id', case when v_mail_row.mailshot_run_id is null then null else v_mail_row.mailshot_run_id::text end,
        'recipient_kind', v_mail_row.recipient_kind,
        'recipient_id', case when v_mail_row.recipient_id is null then null else v_mail_row.recipient_id::text end,
        'context_kind', v_mail_row.context_kind,
        'context_id', case when v_mail_row.context_id is null then null else v_mail_row.context_id::text end,
        'scheduled_for_utc', case when v_mail_row.scheduled_for_utc is null then null else v_mail_row.scheduled_for_utc::text end,
        'next_attempt_at_utc', case when v_mail_row.next_attempt_at_utc is null then null else v_mail_row.next_attempt_at_utc::text end
      )
    );
  end if;

  select co.*
  into v_comms_row
  from public.comms_outbox as co
  where co.id = p_id
  for update;

  if not found then
    raise exception 'outbox row not found';
  end if;

  if upper(coalesce(v_comms_row.channel,'')) <> v_channel then
    raise exception 'channel mismatch';
  end if;

  if v_comms_row.read_at is not null
     or v_comms_row.delivered_at is not null
     or v_comms_row.sent_at is not null then
    raise exception 'outbox row is not retryable because it has already been sent';
  end if;

  if upper(coalesce(v_comms_row.status,'')) not in ('QUEUED','FAILED') then
    raise exception 'outbox row status is not retryable';
  end if;

  perform public._audit_insert(
    'outbox',
    v_comms_row.id::text,
    'OUTBOX_RETRY',
    jsonb_build_object(
      'channel', v_comms_row.channel,
      'outbox_id', v_comms_row.id::text,
      'status', v_comms_row.status,
      'next_attempt_at_utc', case when v_comms_row.next_attempt_at_utc is null then null else v_comms_row.next_attempt_at_utc::text end,
      'scheduled_for_utc', case when v_comms_row.scheduled_for_utc is null then null else v_comms_row.scheduled_for_utc::text end,
      'provider_key', v_comms_row.provider_key,
      'provider_message_id', v_comms_row.provider_message_id,
      'last_error', v_comms_row.last_error
    ),
    jsonb_build_object(
      'channel', v_comms_row.channel,
      'outbox_id', v_comms_row.id::text,
      'status', 'QUEUED',
      'next_attempt_at_utc', v_retry_at_utc::text,
      'scheduled_for_utc', case when v_comms_row.scheduled_for_utc is null then null else v_comms_row.scheduled_for_utc::text end,
      'provider_key', 'AUTO',
      'provider_message_id', null,
      'last_error', null
    ),
    v_reason,
    p_actor_user_id
  );

  update public.comms_outbox as co
  set status = 'QUEUED',
      provider_key = 'AUTO',
      provider_message_id = null,
      provider_response_json = '{}'::jsonb,
      last_error = null,
      sent_at = null,
      delivered_at = null,
      read_at = null,
      failed_at = null,
      next_attempt_at_utc = v_retry_at_utc
  where co.id = v_comms_row.id;

  select co.*
  into v_comms_row
  from public.comms_outbox as co
  where co.id = p_id;

  return jsonb_build_object(
    'ok', true,
    'row', jsonb_build_object(
      'channel', v_comms_row.channel,
      'outbox_id', v_comms_row.id::text,
      'status', v_comms_row.status,
      'to_address', v_comms_row.to_address,
      'mailshot_run_id', case when v_comms_row.mailshot_run_id is null then null else v_comms_row.mailshot_run_id::text end,
      'recipient_kind', v_comms_row.recipient_kind,
      'recipient_id', case when v_comms_row.recipient_id is null then null else v_comms_row.recipient_id::text end,
      'context_kind', v_comms_row.context_kind,
      'context_id', case when v_comms_row.context_id is null then null else v_comms_row.context_id::text end,
      'scheduled_for_utc', case when v_comms_row.scheduled_for_utc is null then null else v_comms_row.scheduled_for_utc::text end,
      'next_attempt_at_utc', case when v_comms_row.next_attempt_at_utc is null then null else v_comms_row.next_attempt_at_utc::text end
    )
  );
end;
$$;



create or replace function public.outbox_unified_reschedule(
  p_channel text,
  p_id uuid,
  p_actor_user_id uuid,
  p_scheduled_for_utc timestamptz
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_channel text := nullif(upper(btrim(coalesce(p_channel,''))), '');
  v_target_utc timestamptz := p_scheduled_for_utc;
  v_reason text := 'RESCHEDULE';

  v_mail_row public.mail_outbox%rowtype;
  v_comms_row public.comms_outbox%rowtype;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if v_channel is null then
    raise exception 'channel required';
  end if;

  if p_id is null then
    raise exception 'id required';
  end if;

  if v_target_utc is null then
    raise exception 'scheduled_for_utc required';
  end if;

  if v_channel not in ('EMAIL','WHATSAPP','SMS','VOICE') then
    raise exception 'unsupported channel %', v_channel;
  end if;

  if v_channel = 'EMAIL' then
    select mo.*
    into v_mail_row
    from public.mail_outbox as mo
    where mo.id = p_id
    for update;

    if not found then
      raise exception 'outbox row not found';
    end if;

    if v_mail_row.read_at is not null
       or v_mail_row.delivered_at is not null
       or v_mail_row.sent_at is not null then
      raise exception 'outbox row is not reschedulable because it has already been sent';
    end if;

    if upper(coalesce(v_mail_row.status::text,'')) not in ('QUEUED','FAILED') then
      raise exception 'outbox row status is not reschedulable';
    end if;

    perform public._audit_insert(
      'outbox',
      v_mail_row.id::text,
      'OUTBOX_RESCHEDULE',
      jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', v_mail_row.status::text,
        'scheduled_for_utc', case when v_mail_row.scheduled_for_utc is null then null else v_mail_row.scheduled_for_utc::text end,
        'next_attempt_at_utc', case when v_mail_row.next_attempt_at_utc is null then null else v_mail_row.next_attempt_at_utc::text end,
        'provider_message_id', v_mail_row.provider_message_id,
        'provider_status', v_mail_row.provider_status,
        'last_error', v_mail_row.last_error
      ),
      jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', 'QUEUED',
        'scheduled_for_utc', v_target_utc::text,
        'next_attempt_at_utc', v_target_utc::text,
        'provider_message_id', null,
        'provider_status', null,
        'last_error', null
      ),
      v_reason,
      p_actor_user_id
    );

    update public.mail_outbox as mo
    set status = 'QUEUED'::public.mail_status_enum,
        sent_at = null,
        delivered_at = null,
        read_at = null,
        failed_at = null,
        last_error = null,
        provider_message_id = null,
        provider_status = null,
        scheduled_for_utc = v_target_utc,
        next_attempt_at_utc = v_target_utc
    where mo.id = v_mail_row.id;

    select mo.*
    into v_mail_row
    from public.mail_outbox as mo
    where mo.id = p_id;

    return jsonb_build_object(
      'ok', true,
      'row', jsonb_build_object(
        'channel', 'EMAIL',
        'outbox_id', v_mail_row.id::text,
        'status', v_mail_row.status::text,
        'to_address', v_mail_row."to",
        'mailshot_run_id', case when v_mail_row.mailshot_run_id is null then null else v_mail_row.mailshot_run_id::text end,
        'recipient_kind', v_mail_row.recipient_kind,
        'recipient_id', case when v_mail_row.recipient_id is null then null else v_mail_row.recipient_id::text end,
        'context_kind', v_mail_row.context_kind,
        'context_id', case when v_mail_row.context_id is null then null else v_mail_row.context_id::text end,
        'scheduled_for_utc', case when v_mail_row.scheduled_for_utc is null then null else v_mail_row.scheduled_for_utc::text end,
        'next_attempt_at_utc', case when v_mail_row.next_attempt_at_utc is null then null else v_mail_row.next_attempt_at_utc::text end
      )
    );
  end if;

  select co.*
  into v_comms_row
  from public.comms_outbox as co
  where co.id = p_id
  for update;

  if not found then
    raise exception 'outbox row not found';
  end if;

  if upper(coalesce(v_comms_row.channel,'')) <> v_channel then
    raise exception 'channel mismatch';
  end if;

  if v_comms_row.read_at is not null
     or v_comms_row.delivered_at is not null
     or v_comms_row.sent_at is not null then
    raise exception 'outbox row is not reschedulable because it has already been sent';
  end if;

  if upper(coalesce(v_comms_row.status,'')) not in ('QUEUED','FAILED') then
    raise exception 'outbox row status is not reschedulable';
  end if;

  perform public._audit_insert(
    'outbox',
    v_comms_row.id::text,
    'OUTBOX_RESCHEDULE',
    jsonb_build_object(
      'channel', v_comms_row.channel,
      'outbox_id', v_comms_row.id::text,
      'status', v_comms_row.status,
      'scheduled_for_utc', case when v_comms_row.scheduled_for_utc is null then null else v_comms_row.scheduled_for_utc::text end,
      'next_attempt_at_utc', case when v_comms_row.next_attempt_at_utc is null then null else v_comms_row.next_attempt_at_utc::text end,
      'provider_key', v_comms_row.provider_key,
      'provider_message_id', v_comms_row.provider_message_id,
      'last_error', v_comms_row.last_error
    ),
    jsonb_build_object(
      'channel', v_comms_row.channel,
      'outbox_id', v_comms_row.id::text,
      'status', 'QUEUED',
      'scheduled_for_utc', v_target_utc::text,
      'next_attempt_at_utc', v_target_utc::text,
      'provider_key', 'AUTO',
      'provider_message_id', null,
      'last_error', null
    ),
    v_reason,
    p_actor_user_id
  );

  update public.comms_outbox as co
  set status = 'QUEUED',
      provider_key = 'AUTO',
      provider_message_id = null,
      provider_response_json = '{}'::jsonb,
      last_error = null,
      sent_at = null,
      delivered_at = null,
      read_at = null,
      failed_at = null,
      scheduled_for_utc = v_target_utc,
      next_attempt_at_utc = v_target_utc
  where co.id = v_comms_row.id;

  select co.*
  into v_comms_row
  from public.comms_outbox as co
  where co.id = p_id;

  return jsonb_build_object(
    'ok', true,
    'row', jsonb_build_object(
      'channel', v_comms_row.channel,
      'outbox_id', v_comms_row.id::text,
      'status', v_comms_row.status,
      'to_address', v_comms_row.to_address,
      'mailshot_run_id', case when v_comms_row.mailshot_run_id is null then null else v_comms_row.mailshot_run_id::text end,
      'recipient_kind', v_comms_row.recipient_kind,
      'recipient_id', case when v_comms_row.recipient_id is null then null else v_comms_row.recipient_id::text end,
      'context_kind', v_comms_row.context_kind,
      'context_id', case when v_comms_row.context_id is null then null else v_comms_row.context_id::text end,
      'scheduled_for_utc', case when v_comms_row.scheduled_for_utc is null then null else v_comms_row.scheduled_for_utc::text end,
      'next_attempt_at_utc', case when v_comms_row.next_attempt_at_utc is null then null else v_comms_row.next_attempt_at_utc::text end
    )
  );
end;
$$;


create or replace function public.mailshot_enqueue(
  p_prepare_json jsonb,
  p_final_edits_json jsonb,
  p_delivery_timing_json jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  v_context_kind text;
  v_entity_type text;
  v_output_type text;

  v_template_id uuid;
  v_to_field_key text;

  v_rows jsonb;
  v_row jsonb;

  v_run_id uuid;

  v_queued int := 0;
  v_skipped int := 0;
  v_failed int := 0;

  v_skip_list jsonb := '[]'::jsonb;

  v_subject_tpl_override text;
  v_body_text_tpl_override text;
  v_body_html_tpl_override text;
  v_message_tpl_override text;

  v_cc_override text;
  v_bcc_override text;
  v_reply_to_override text;
  v_importance_override text;
  v_email_type_override text;

  v_attachments jsonb;

  v_to text;
  v_recipient_kind text;
  v_recipient_id uuid;
  v_context_id uuid;

  v_field_values jsonb;
  v_kv record;

  v_rendered_subject text;
  v_rendered_body_text text;
  v_rendered_body_html text;
  v_rendered_message text;

  v_ref text;

  v_sms_max int := 1000;
  v_voice_max int := 1200;
  v_whatsapp_max int := 600;

  v_cfg jsonb;
  v_cfg_wati jsonb;
  v_cfg_clicksend jsonb;
  v_cfg_scheduling jsonb;

  v_provider_key text;

  v_sanitised boolean := false;
  v_truncated boolean := false;
  v_original_len int := 0;

  v_row_subject_tpl text;
  v_row_body_text_tpl text;
  v_row_body_html_tpl text;
  v_row_message_tpl text;
  v_row_cc text;
  v_row_bcc text;
  v_row_reply_to text;
  v_row_importance text;
  v_row_email_type text;
  v_row_template_content jsonb := '{}'::jsonb;
  v_wati_template_name text;
  v_wati_param_name text;

  v_delivery_timing_json jsonb := '{}'::jsonb;
  v_delivery_mode text := 'NOW';
  v_requested_timezone text := null;
  v_requested_local_text text := null;
  v_relative_minutes int := null;

  v_scheduled_for_utc timestamptz := null;
  v_next_attempt_at_utc timestamptz := null;

  v_max_future_days int := 365;
  v_allow_past_grace_minutes int := 15;
  v_scheduled_text text;
begin
  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_prepare_json is null or jsonb_typeof(p_prepare_json) <> 'object' then
    raise exception 'prepare_json object required';
  end if;

  if p_final_edits_json is not null and jsonb_typeof(p_final_edits_json) <> 'object' then
    raise exception 'final_edits_json object required';
  end if;

  if p_delivery_timing_json is not null and jsonb_typeof(p_delivery_timing_json) <> 'object' then
    raise exception 'delivery_timing_json object required';
  end if;

  v_context_kind := lower(coalesce(p_prepare_json->>'context_kind',''));
  v_entity_type := lower(coalesce(p_prepare_json->>'entity_type',''));
  v_output_type := upper(coalesce(p_prepare_json->>'output_type',''));

  if v_context_kind = '' or v_entity_type = '' or v_output_type = '' then
    raise exception 'prepare_json missing context_kind/entity_type/output_type';
  end if;

  v_rows := p_prepare_json->'rows';
  if v_rows is null or jsonb_typeof(v_rows) <> 'array' then
    raise exception 'prepare_json.rows must be array';
  end if;

  v_to_field_key := nullif(btrim(coalesce(p_prepare_json->>'to_field_key','')), '');

  v_template_id := null;
  if nullif(coalesce(p_prepare_json->>'document_template_id',''),'') is not null then
    v_template_id := (p_prepare_json->>'document_template_id')::uuid;
  end if;

  select sd.comms_adaptors_json
  into v_cfg
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_cfg is not null and jsonb_typeof(v_cfg) = 'object' then
    v_cfg_wati := v_cfg->'wati';
    v_cfg_clicksend := v_cfg->'clicksend';
    v_cfg_scheduling := v_cfg->'scheduling';

    if v_cfg_wati is not null and jsonb_typeof(v_cfg_wati) = 'object' then
      v_whatsapp_max := coalesce(nullif((v_cfg_wati->>'whatsapp_max_chars')::int, 0), v_whatsapp_max);
    end if;

    if v_cfg_clicksend is not null and jsonb_typeof(v_cfg_clicksend) = 'object' then
      v_sms_max := coalesce(nullif((v_cfg_clicksend->>'sms_max_chars')::int, 0), v_sms_max);
      v_voice_max := coalesce(nullif((v_cfg_clicksend->>'voice_max_chars')::int, 0), v_voice_max);
    end if;

    if v_cfg_scheduling is not null and jsonb_typeof(v_cfg_scheduling) = 'object' then
      v_max_future_days := coalesce(nullif((v_cfg_scheduling->>'max_future_days')::int, 0), v_max_future_days);
      v_allow_past_grace_minutes := coalesce(nullif((v_cfg_scheduling->>'allow_past_grace_minutes')::int, 0), v_allow_past_grace_minutes);
    end if;
  end if;

  if p_delivery_timing_json is null or p_delivery_timing_json = '{}'::jsonb then
    v_delivery_mode := 'NOW';
  else
    v_delivery_mode := upper(btrim(coalesce(p_delivery_timing_json->>'mode','NOW')));
    if v_delivery_mode = '' then
      v_delivery_mode := 'NOW';
    end if;
  end if;

  if v_delivery_mode = 'RELATIVE_DELAY' then
    v_delivery_mode := 'AFTER_DELAY';
  end if;

  if v_delivery_mode not in ('NOW','AT_TIME','AFTER_DELAY') then
    raise exception 'delivery_timing_json.mode must be NOW, AT_TIME or AFTER_DELAY';
  end if;

  v_requested_timezone := nullif(btrim(coalesce(p_delivery_timing_json->>'requested_timezone','')), '');
  v_requested_local_text := nullif(btrim(coalesce(p_delivery_timing_json->>'requested_local_text','')), '');

  if nullif(coalesce(p_delivery_timing_json->>'relative_minutes',''),'') is not null then
    v_relative_minutes := (p_delivery_timing_json->>'relative_minutes')::int;
  else
    v_relative_minutes := null;
  end if;

  if v_delivery_mode = 'AT_TIME' then
    v_scheduled_text := nullif(btrim(coalesce(p_delivery_timing_json->>'scheduled_for_utc','')), '');
    if v_scheduled_text is null then
      raise exception 'delivery_timing_json.scheduled_for_utc required when mode=AT_TIME';
    end if;

    begin
      v_scheduled_for_utc := v_scheduled_text::timestamptz;
    exception
      when others then
        raise exception 'delivery_timing_json.scheduled_for_utc invalid';
    end;

    if v_scheduled_for_utc > (v_now + make_interval(days => v_max_future_days)) then
      raise exception 'delivery_timing_json.scheduled_for_utc exceeds max_future_days';
    end if;

    if v_scheduled_for_utc < (v_now - make_interval(mins => v_allow_past_grace_minutes)) then
      raise exception 'delivery_timing_json.scheduled_for_utc too far in past';
    end if;

    if v_scheduled_for_utc <= v_now then
      v_next_attempt_at_utc := v_now;
    else
      v_next_attempt_at_utc := v_scheduled_for_utc;
    end if;
  elsif v_delivery_mode = 'AFTER_DELAY' then
    v_scheduled_text := nullif(btrim(coalesce(p_delivery_timing_json->>'scheduled_for_utc','')), '');

    if v_scheduled_text is not null then
      begin
        v_scheduled_for_utc := v_scheduled_text::timestamptz;
      exception
        when others then
          raise exception 'delivery_timing_json.scheduled_for_utc invalid';
      end;
    else
      if v_relative_minutes is null then
        raise exception 'delivery_timing_json.relative_minutes required when mode=AFTER_DELAY';
      end if;

      v_scheduled_for_utc := v_now + make_interval(mins => v_relative_minutes);
    end if;

    if v_scheduled_for_utc > (v_now + make_interval(days => v_max_future_days)) then
      raise exception 'delivery_timing_json.scheduled_for_utc exceeds max_future_days';
    end if;

    if v_scheduled_for_utc < (v_now - make_interval(mins => v_allow_past_grace_minutes)) then
      raise exception 'delivery_timing_json.scheduled_for_utc too far in past';
    end if;

    if v_scheduled_for_utc <= v_now then
      v_next_attempt_at_utc := v_now;
    else
      v_next_attempt_at_utc := v_scheduled_for_utc;
    end if;
  else
    v_scheduled_for_utc := null;
    v_next_attempt_at_utc := null;
  end if;

  v_delivery_timing_json := jsonb_build_object(
    'mode', v_delivery_mode,
    'scheduled_for_utc', case when v_scheduled_for_utc is null then null else to_jsonb(v_scheduled_for_utc) end,
    'requested_timezone', to_jsonb(v_requested_timezone),
    'requested_local_text', to_jsonb(v_requested_local_text),
    'relative_minutes', to_jsonb(v_relative_minutes)
  );

  v_subject_tpl_override := nullif(coalesce(p_final_edits_json->>'subject',''), '');
  v_body_text_tpl_override := nullif(coalesce(p_final_edits_json->>'body_text',''), '');
  v_body_html_tpl_override := nullif(coalesce(p_final_edits_json->>'body_html',''), '');
  v_message_tpl_override := nullif(coalesce(p_final_edits_json->>'message_text',''), '');

  v_cc_override := nullif(coalesce(p_final_edits_json->>'cc',''), '');
  v_bcc_override := nullif(coalesce(p_final_edits_json->>'bcc',''), '');
  v_reply_to_override := nullif(coalesce(p_final_edits_json->>'reply_to',''), '');
  v_importance_override := nullif(coalesce(p_final_edits_json->>'importance',''), '');
  v_email_type_override := nullif(coalesce(p_final_edits_json->>'email_type',''), '');

  v_attachments := coalesce(p_final_edits_json->'attachments', '[]'::jsonb);

  insert into public.mailshot_runs(
    context_kind,
    output_type,
    document_template_id,
    created_by,
    created_at_utc,
    selection_json,
    result_json,
    delivery_timing_json
  )
  values (
    v_context_kind,
    v_output_type,
    v_template_id,
    p_actor_user_id,
    v_now,
    p_prepare_json,
    '{}'::jsonb,
    v_delivery_timing_json
  )
  returning id into v_run_id;

  for v_row in
    select jbe.value
    from jsonb_array_elements(v_rows) as jbe(value)
  loop
    if coalesce((v_row->>'eligible')::boolean, false) = false then
      v_skipped := v_skipped + 1;
      v_skip_list := v_skip_list || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'recipient_kind', v_row->>'recipient_kind',
          'recipient_id', v_row->>'recipient_id',
          'reason', coalesce(v_row->>'skip_reason', 'NOT_ELIGIBLE')
        )
      );
      continue;
    end if;

    v_to := nullif(btrim(coalesce(v_row->>'to','')), '');

    if v_to is null then
      v_skipped := v_skipped + 1;
      v_skip_list := v_skip_list || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'recipient_kind', v_row->>'recipient_kind',
          'recipient_id', v_row->>'recipient_id',
          'reason', 'MISSING_TO'
        )
      );
      continue;
    end if;

    v_recipient_kind := nullif(btrim(coalesce(v_row->>'recipient_kind','')), '');

    v_recipient_id := null;
    if nullif(coalesce(v_row->>'recipient_id',''),'') is not null then
      v_recipient_id := (v_row->>'recipient_id')::uuid;
    end if;

    v_context_id := null;
    if nullif(coalesce(v_row->>'context_id',''),'') is not null then
      v_context_id := (v_row->>'context_id')::uuid;
    end if;

    v_field_values := coalesce(v_row->'field_values', '{}'::jsonb);
    v_row_template_content := coalesce(v_row->'template_content_json', '{}'::jsonb);

    v_row_subject_tpl := coalesce(
      v_subject_tpl_override,
      nullif(coalesce(v_row_template_content->>'subject',''), '')
    );

    v_row_body_text_tpl := coalesce(
      v_body_text_tpl_override,
      nullif(coalesce(v_row_template_content->>'body_text',''), '')
    );

    v_row_body_html_tpl := coalesce(
      v_body_html_tpl_override,
      nullif(coalesce(v_row_template_content->>'body_html',''), '')
    );

    v_row_message_tpl := coalesce(
      v_message_tpl_override,
      nullif(coalesce(v_row_template_content->>'message_text',''), '')
    );

    v_row_cc := coalesce(
      v_cc_override,
      nullif(coalesce(v_row_template_content->>'cc',''), '')
    );

    v_row_bcc := coalesce(
      v_bcc_override,
      nullif(coalesce(v_row_template_content->>'bcc',''), '')
    );

    v_row_reply_to := coalesce(
      v_reply_to_override,
      nullif(coalesce(v_row_template_content->>'reply_to',''), '')
    );

    v_row_importance := coalesce(
      v_importance_override,
      nullif(coalesce(v_row_template_content->>'importance',''), ''),
      'Normal'
    );

    v_row_email_type := coalesce(
      v_email_type_override,
      nullif(coalesce(v_row_template_content->>'email_type',''), ''),
      nullif(btrim(coalesce(v_row->>'email_type','')), ''),
      'plain'
    );

    v_rendered_subject := coalesce(v_row_subject_tpl, '');
    v_rendered_body_text := coalesce(v_row_body_text_tpl, '');
    v_rendered_body_html := coalesce(v_row_body_html_tpl, '');
    v_rendered_message := coalesce(v_row_message_tpl, '');

    for v_kv in
      select
        jet.key as k,
        coalesce(jet.value, '') as v
      from jsonb_each_text(v_field_values) as jet(key, value)
    loop
      v_rendered_subject := replace(v_rendered_subject, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_body_text := replace(v_rendered_body_text, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_body_html := replace(v_rendered_body_html, '{{' || v_kv.k || '}}', v_kv.v);
      v_rendered_message := replace(v_rendered_message, '{{' || v_kv.k || '}}', v_kv.v);
    end loop;

    if v_output_type = 'EMAIL' then
      v_ref := 'mailshot:' || v_run_id::text || ':' || coalesce(v_context_id::text, '') || ':' || md5(coalesce(v_to, ''));

      insert into public.mail_outbox(
        type,
        "to",
        cc,
        bcc,
        reply_to,
        importance,
        email_type,
        subject,
        body_text,
        body_html,
        attachments,
        status,
        reference,
        created_at_utc,
        created_by,
        recipient_kind,
        recipient_id,
        context_kind,
        context_id,
        mailshot_run_id,
        document_template_id,
        scheduled_for_utc,
        next_attempt_at_utc
      )
      values (
        'MAILSHOT_EMAIL'::text,
        v_to,
        v_row_cc,
        v_row_bcc,
        v_row_reply_to,
        v_row_importance,
        v_row_email_type,
        v_rendered_subject,
        nullif(v_rendered_body_text, ''),
        nullif(v_rendered_body_html, ''),
        v_attachments,
        'QUEUED'::public.mail_status_enum,
        v_ref,
        v_now,
        p_actor_user_id,
        v_recipient_kind,
        v_recipient_id,
        v_context_kind,
        v_context_id,
        v_run_id,
        v_template_id,
        v_scheduled_for_utc,
        v_next_attempt_at_utc
      );

      v_queued := v_queued + 1;

    elsif v_output_type in ('WHATSAPP','SMS','VOICE') then
      v_original_len := char_length(v_rendered_message);
      v_sanitised := false;
      v_truncated := false;
      v_wati_template_name := null;
      v_wati_param_name := null;

      if v_output_type = 'WHATSAPP' then
        v_wati_template_name := coalesce(
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'templateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'wati_template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'watiTemplateName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'template_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'templateName','')), '')
        );

        v_wati_param_name := coalesce(
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider_contract'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'wati'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'whatsapp'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->'provider'->>'paramName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'wati_param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'watiParamName','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'param_name','')), ''),
          nullif(btrim(coalesce(v_row_template_content->>'paramName','')), '')
        );

        if v_wati_template_name is null then
          v_skipped := v_skipped + 1;
          v_skip_list := v_skip_list || jsonb_build_array(
            jsonb_build_object(
              'context_id', v_row->>'context_id',
              'recipient_kind', v_row->>'recipient_kind',
              'recipient_id', v_row->>'recipient_id',
              'reason', 'WATI_TEMPLATE_NAME_MISSING'
            )
          );
          continue;
        end if;

        if v_wati_param_name is null then
          v_skipped := v_skipped + 1;
          v_skip_list := v_skip_list || jsonb_build_array(
            jsonb_build_object(
              'context_id', v_row->>'context_id',
              'recipient_kind', v_row->>'recipient_kind',
              'recipient_id', v_row->>'recipient_id',
              'reason', 'WATI_PARAM_NAME_MISSING'
            )
          );
          continue;
        end if;

        v_rendered_message := regexp_replace(v_rendered_message, E'[\\r\\n\\t]+', ' ', 'g');
        v_rendered_message := regexp_replace(v_rendered_message, '[^A-Za-z ,]+', '', 'g');
        v_rendered_message := regexp_replace(v_rendered_message, E'\\s+', ' ', 'g');
        v_rendered_message := btrim(v_rendered_message);

        if char_length(v_rendered_message) <> v_original_len then
          v_sanitised := true;
        end if;

        if char_length(v_rendered_message) > v_whatsapp_max then
          v_rendered_message := left(v_rendered_message, v_whatsapp_max);
          v_truncated := true;
        end if;
      elsif v_output_type = 'SMS' then
        if char_length(v_rendered_message) > v_sms_max then
          v_rendered_message := left(v_rendered_message, v_sms_max);
          v_truncated := true;
        end if;
      else
        if char_length(v_rendered_message) > v_voice_max then
          v_rendered_message := left(v_rendered_message, v_voice_max);
          v_truncated := true;
        end if;
      end if;

      if nullif(v_rendered_message, '') is null then
        v_skipped := v_skipped + 1;
        v_skip_list := v_skip_list || jsonb_build_array(
          jsonb_build_object(
            'context_id', v_row->>'context_id',
            'recipient_kind', v_row->>'recipient_kind',
            'recipient_id', v_row->>'recipient_id',
            'reason', 'EMPTY_MESSAGE_AFTER_SANITIZE'
          )
        );
        continue;
      end if;

      v_provider_key := 'AUTO';

      insert into public.comms_outbox(
        channel,
        status,
        to_address,
        message_text,
        provider_key,
        provider_message_id,
        provider_payload_json,
        provider_response_json,
        last_error,
        created_at_utc,
        sent_at,
        delivered_at,
        read_at,
        failed_at,
        created_by,
        recipient_kind,
        recipient_id,
        context_kind,
        context_id,
        mailshot_run_id,
        document_template_id,
        scheduled_for_utc,
        next_attempt_at_utc
      )
      values (
        v_output_type,
        'QUEUED',
        v_to,
        v_rendered_message,
        v_provider_key,
        null,
        jsonb_build_object(
          'original_len', v_original_len,
          'was_sanitised', v_sanitised,
          'was_truncated', v_truncated
        ) ||
        case
          when v_output_type = 'WHATSAPP' then
            jsonb_build_object(
              'provider_contract',
              jsonb_build_object(
                'provider', 'WATI',
                'template_name', v_wati_template_name,
                'param_name', v_wati_param_name
              )
            )
          else
            '{}'::jsonb
        end,
        '{}'::jsonb,
        null,
        v_now,
        null,
        null,
        null,
        null,
        p_actor_user_id,
        v_recipient_kind,
        v_recipient_id,
        v_context_kind,
        v_context_id,
        v_run_id,
        v_template_id,
        v_scheduled_for_utc,
        v_next_attempt_at_utc
      );

      v_queued := v_queued + 1;
    else
      v_skipped := v_skipped + 1;
      v_skip_list := v_skip_list || jsonb_build_array(
        jsonb_build_object(
          'context_id', v_row->>'context_id',
          'recipient_kind', v_row->>'recipient_kind',
          'recipient_id', v_row->>'recipient_id',
          'reason', 'UNSUPPORTED_OUTPUT_TYPE'
        )
      );
    end if;
  end loop;

  update public.mailshot_runs as mr
  set result_json = jsonb_build_object(
    'queued', v_queued,
    'skipped', v_skipped,
    'failed', v_failed,
    'skips', v_skip_list,
    'delivery_timing', v_delivery_timing_json
  )
  where mr.id = v_run_id;

  return jsonb_build_object(
    'ok', true,
    'mailshot_run_id', v_run_id::text,
    'queued', v_queued,
    'skipped', v_skipped,
    'failed', v_failed,
    'skips', v_skip_list,
    'delivery_timing', v_delivery_timing_json
  );
end;
$$;



