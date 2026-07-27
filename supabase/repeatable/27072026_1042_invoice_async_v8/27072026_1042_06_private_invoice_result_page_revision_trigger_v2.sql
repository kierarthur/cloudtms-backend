create or replace function private._invoice_result_page_revision_trigger_v2()
returns trigger
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_root_id uuid;
  v_stamp bigint;
begin
  if tg_op = 'INSERT' then
    for v_root_id in
      select distinct n.operation_id
      from new_rows n
      where n.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
        and n.result_visible
        and n.operation_id is not null
      order by n.operation_id
    loop
      perform 1
      from public.invoice_operations o
      where o.id = v_root_id
      for update;

      v_stamp := nextval(
        'public.invoice_operation_change_seq'::regclass
      );

      update public.invoice_operations o
      set result_page_revision = greatest(
            o.result_page_revision,
            v_stamp
          ),
          change_seq = greatest(o.change_seq, v_stamp),
          updated_at_utc = statement_timestamp()
      where o.id = v_root_id;
    end loop;
  elsif tg_op = 'DELETE' then
    for v_root_id in
      select distinct prior.operation_id
      from old_rows prior
      where prior.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
        and prior.result_visible
        and prior.operation_id is not null
      order by prior.operation_id
    loop
      perform 1
      from public.invoice_operations o
      where o.id = v_root_id
      for update;

      v_stamp := nextval(
        'public.invoice_operation_change_seq'::regclass
      );

      update public.invoice_operations o
      set result_page_revision = greatest(
            o.result_page_revision,
            v_stamp
          ),
          change_seq = greatest(o.change_seq, v_stamp),
          updated_at_utc = statement_timestamp()
      where o.id = v_root_id;
    end loop;
  else
    for v_root_id in
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
        and coalesce(n.operation_id, o.operation_id) is not null
      order by coalesce(n.operation_id, o.operation_id)
    loop
      perform 1
      from public.invoice_operations root_operation
      where root_operation.id = v_root_id
      for update;

      v_stamp := nextval(
        'public.invoice_operation_change_seq'::regclass
      );

      update public.invoice_operations root_operation
      set result_page_revision = greatest(
            root_operation.result_page_revision,
            v_stamp
          ),
          change_seq = greatest(root_operation.change_seq, v_stamp),
          updated_at_utc = statement_timestamp()
      where root_operation.id = v_root_id;
    end loop;
  end if;

  return null;
end;
$function$;

alter function private._invoice_result_page_revision_trigger_v2()
  owner to postgres;
revoke all on function private._invoice_result_page_revision_trigger_v2()
  from public, anon, authenticated;
