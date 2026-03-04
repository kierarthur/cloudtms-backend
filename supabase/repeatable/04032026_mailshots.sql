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
      btrim(p_output_type),
      btrim(p_filename),
      p_description,
      p_email_type,
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
      output_type = btrim(p_output_type),
      filename = btrim(p_filename),
      description = p_description,
      email_type = p_email_type,
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
