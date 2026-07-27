create or replace function private._invoice_result_page_revision_trigger_v2()
returns trigger
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
begin
  if tg_op = 'INSERT' then
    with roots as materialized (
      select distinct n.operation_id
      from new_rows n
      where n.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
        and n.result_visible
    ),
    stamps as materialized (
      select r.operation_id,
        nextval('public.invoice_operation_change_seq'::regclass) stamp
      from roots r
    )
    update public.invoice_operations o
    set result_page_revision = s.stamp,
        change_seq = greatest(o.change_seq, s.stamp),
        updated_at_utc = statement_timestamp()
    from stamps s
    where o.id = s.operation_id;
  elsif tg_op = 'DELETE' then
    with roots as materialized (
      select distinct o.operation_id
      from old_rows o
      where o.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
        and o.result_visible
    ),
    stamps as materialized (
      select r.operation_id,
        nextval('public.invoice_operation_change_seq'::regclass) stamp
      from roots r
    )
    update public.invoice_operations op
    set result_page_revision = s.stamp,
        change_seq = greatest(op.change_seq, s.stamp),
        updated_at_utc = statement_timestamp()
    from stamps s
    where op.id = s.operation_id;
  else
    with roots as materialized (
      select distinct coalesce(n.operation_id, o.operation_id) operation_id
      from old_rows o
      full join new_rows n on n.id = o.id
      where coalesce(n.chunk_type, o.chunk_type) in ('GENERATION_GROUP','ISSUE_INVOICE')
        and (
          n.result_visible is distinct from o.result_visible
          or n.selection_key is distinct from o.selection_key
          or n.result_category is distinct from o.result_category
          or n.document_version_id is distinct from o.document_version_id
          or n.replaced_by_chunk_id is distinct from o.replaced_by_chunk_id
          or n.manifest_generation is distinct from o.manifest_generation
        )
        and (coalesce(n.result_visible, false) or coalesce(o.result_visible, false))
    ),
    stamps as materialized (
      select r.operation_id,
        nextval('public.invoice_operation_change_seq'::regclass) stamp
      from roots r
      where r.operation_id is not null
    )
    update public.invoice_operations op
    set result_page_revision = s.stamp,
        change_seq = greatest(op.change_seq, s.stamp),
        updated_at_utc = statement_timestamp()
    from stamps s
    where op.id = s.operation_id;
  end if;

  return null;
end;
$function$;

alter function private._invoice_result_page_revision_trigger_v2()
  owner to postgres;
revoke all on function private._invoice_result_page_revision_trigger_v2()
  from public, anon, authenticated;
