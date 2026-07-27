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
    execute v_sql into v_changed using v_fields;
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
