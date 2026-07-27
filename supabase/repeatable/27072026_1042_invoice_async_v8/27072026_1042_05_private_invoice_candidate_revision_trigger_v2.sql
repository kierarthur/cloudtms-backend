create or replace function private._invoice_jsonb_pick_v2(
  p_value jsonb,
  p_keys text[]
) returns jsonb
language sql
immutable
security invoker
set search_path to 'public','private','extensions','pg_temp'
as $function$
  select coalesce(
    jsonb_object_agg(e.key, e.value order by e.key),
    '{}'::jsonb
  )
  from jsonb_each(coalesce(p_value, '{}'::jsonb)) e
  where e.key = any(coalesce(p_keys, array[]::text[]));
$function$;

alter function private._invoice_jsonb_pick_v2(jsonb,text[]) owner to postgres;
revoke all on function private._invoice_jsonb_pick_v2(jsonb,text[])
  from public, anon, authenticated;
grant execute on function private._invoice_jsonb_pick_v2(jsonb,text[])
  to service_role;

create or replace function private._invoice_candidate_revision_trigger_v2()
returns trigger
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_generate boolean := coalesce(tg_argv[0], 'false')::boolean;
  v_issue boolean := coalesce(tg_argv[1], 'false')::boolean;
  v_fields text[] := coalesce(tg_argv[2:array_length(tg_argv, 1)], array[]::text[]);
  v_identity_fields text[];
  v_compare_fields text[];
  v_changed boolean := false;
  v_qualifier text := 'true';
  v_sql text;
begin
  if tg_level <> 'STATEMENT' then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_CANDIDATE_REVISION_TRIGGER_MUST_BE_STATEMENT_LEVEL';
  end if;

  if tg_table_name = 'invoice_operations' then
    -- Selection roots and their manifest bookkeeping are not candidate work.
    v_qualifier :=
      $$coalesce(entity_type, '') <> 'INVOICE_BATCH'$$;
  elsif tg_table_name = 'invoice_operation_chunks' then
    -- Never let an expander, an uncommitted carrier, or an outcome-only
    -- root-owned carrier invalidate the snapshot that is constructing it.
    v_qualifier :=
      $$coalesce(payload_json->>'is_selection_expander','false') <> 'true'
        and not (is_manifest_member and not manifest_committed)
        and not (is_manifest_member and coalesce(entity_type,'') = 'OPERATION')$$;
  end if;

  select coalesce(array_agg(a.attname order by key_column.ordinality), array[]::text[])
  into v_identity_fields
  from pg_index i
  cross join lateral unnest(i.indkey) with ordinality key_column(attnum, ordinality)
  join pg_attribute a
    on a.attrelid = i.indrelid
   and a.attnum = key_column.attnum
  where i.indrelid = tg_relid
    and i.indisprimary;

  if cardinality(v_identity_fields) = 0 then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_CANDIDATE_REVISION_PRIMARY_KEY_REQUIRED';
  end if;

  select array_agg(field_name order by field_name)
  into v_compare_fields
  from (
    select distinct field_name
    from unnest(v_identity_fields || v_fields) field_name
  ) compared;

  if tg_op = 'INSERT' then
    v_sql := format(
      'select exists(select 1 from new_rows where %s)',
      v_qualifier
    );
    execute v_sql into v_changed;
  elsif tg_op = 'DELETE' then
    v_sql := format(
      'select exists(select 1 from old_rows where %s)',
      v_qualifier
    );
    execute v_sql into v_changed;
  elsif tg_op = 'UPDATE' then
    if tg_table_name = 'invoice_operations' then
      select exists (
        select 1
        from old_rows o
        join new_rows n on n.id = o.id
        where coalesce(o.entity_type, '') <> 'INVOICE_BATCH'
          and coalesce(n.entity_type, '') <> 'INVOICE_BATCH'
          and jsonb_build_object(
            'id', o.id,
            'parent_operation_id', o.parent_operation_id,
            'operation_type', o.operation_type,
            'entity_type', o.entity_type,
            'entity_id', o.entity_id,
            'status', o.status,
            'phase', o.phase,
            'source_revision', o.source_revision,
            'template_version', o.template_version,
            'control_version', o.control_version,
            'manifest_generation', o.manifest_generation,
            'manifest_committed', o.manifest_committed,
            'release_complete', o.release_complete,
            'legal_issue_state',
              o.result_json->'legal_issue_state',
            'delivery_state', o.result_json->'delivery_state',
            'document_version_id',
              o.result_json->'document_version_id',
            'issued_document_version_id',
              o.result_json->'issued_document_version_id',
            'error_code', o.error_json->'code'
          ) is distinct from jsonb_build_object(
            'id', n.id,
            'parent_operation_id', n.parent_operation_id,
            'operation_type', n.operation_type,
            'entity_type', n.entity_type,
            'entity_id', n.entity_id,
            'status', n.status,
            'phase', n.phase,
            'source_revision', n.source_revision,
            'template_version', n.template_version,
            'control_version', n.control_version,
            'manifest_generation', n.manifest_generation,
            'manifest_committed', n.manifest_committed,
            'release_complete', n.release_complete,
            'legal_issue_state',
              n.result_json->'legal_issue_state',
            'delivery_state', n.result_json->'delivery_state',
            'document_version_id',
              n.result_json->'document_version_id',
            'issued_document_version_id',
              n.result_json->'issued_document_version_id',
            'error_code', n.error_json->'code'
          )
      ) into v_changed;
    elsif tg_table_name = 'invoice_operation_chunks' then
      select exists (
        select 1
        from old_rows o
        join new_rows n on n.id = o.id
        where coalesce(
                o.payload_json->>'is_selection_expander',
                'false'
              ) <> 'true'
          and coalesce(
                n.payload_json->>'is_selection_expander',
                'false'
              ) <> 'true'
          and not (o.is_manifest_member and not o.manifest_committed)
          and not (n.is_manifest_member and not n.manifest_committed)
          and not (
            o.is_manifest_member
            and coalesce(o.entity_type, '') = 'OPERATION'
          )
          and not (
            n.is_manifest_member
            and coalesce(n.entity_type, '') = 'OPERATION'
          )
          and jsonb_build_object(
            'id', o.id,
            'operation_id', o.operation_id,
            'chunk_type', o.chunk_type,
            'entity_type', o.entity_type,
            'entity_id', o.entity_id,
            'document_version_id', o.document_version_id,
            'document_asset_id', o.document_asset_id,
            'input_document_version_id', o.input_document_version_id,
            'status', o.status,
            'phase', o.phase,
            'replaced_by_chunk_id', o.replaced_by_chunk_id,
            'manifest_generation', o.manifest_generation,
            'is_manifest_member', o.is_manifest_member,
            'manifest_committed', o.manifest_committed,
            'result_visible', o.result_visible,
            'selection_key', o.selection_key,
            'result_category', o.result_category,
            'blocked_for_sending',
              o.payload_json->'blocked_for_sending',
            'row_kind', o.payload_json->'row_kind',
            'source_revision', o.payload_json->'source_revision',
            'document_result_version_id',
              o.result_json->'document_version_id',
            'issued_document_version_id',
              o.result_json->'issued_document_version_id',
            'legal_issue_state',
              o.result_json->'legal_issue_state',
            'delivery_state', o.result_json->'delivery_state',
            'error_code', o.error_json->'code'
          ) is distinct from jsonb_build_object(
            'id', n.id,
            'operation_id', n.operation_id,
            'chunk_type', n.chunk_type,
            'entity_type', n.entity_type,
            'entity_id', n.entity_id,
            'document_version_id', n.document_version_id,
            'document_asset_id', n.document_asset_id,
            'input_document_version_id', n.input_document_version_id,
            'status', n.status,
            'phase', n.phase,
            'replaced_by_chunk_id', n.replaced_by_chunk_id,
            'manifest_generation', n.manifest_generation,
            'is_manifest_member', n.is_manifest_member,
            'manifest_committed', n.manifest_committed,
            'result_visible', n.result_visible,
            'selection_key', n.selection_key,
            'result_category', n.result_category,
            'blocked_for_sending',
              n.payload_json->'blocked_for_sending',
            'row_kind', n.payload_json->'row_kind',
            'source_revision', n.payload_json->'source_revision',
            'document_result_version_id',
              n.result_json->'document_version_id',
            'issued_document_version_id',
              n.result_json->'issued_document_version_id',
            'legal_issue_state',
              n.result_json->'legal_issue_state',
            'delivery_state', n.result_json->'delivery_state',
            'error_code', n.error_json->'code'
          )
      ) into v_changed;
    else
      v_sql := format($sql$
        select exists (
          (
            select private._invoice_jsonb_pick_v2(to_jsonb(o), $1)
            from old_rows o
            where %1$s
            except all
            select private._invoice_jsonb_pick_v2(to_jsonb(n), $1)
            from new_rows n
            where %1$s
          )
          union all
          (
            select private._invoice_jsonb_pick_v2(to_jsonb(n), $1)
            from new_rows n
            where %1$s
            except all
            select private._invoice_jsonb_pick_v2(to_jsonb(o), $1)
            from old_rows o
            where %1$s
          )
        )
      $sql$, v_qualifier);
      execute v_sql into v_changed using v_compare_fields;
    end if;
  end if;

  if v_changed then
    perform private._invoice_candidate_snapshot_bump_v2(
      v_generate,
      v_issue,
      tg_op,
      tg_table_schema || '.' || tg_table_name,
      statement_timestamp()
    );
  end if;

  return null;
end;
$function$;

alter function private._invoice_candidate_revision_trigger_v2()
  owner to postgres;
revoke all on function private._invoice_candidate_revision_trigger_v2()
  from public, anon, authenticated;
