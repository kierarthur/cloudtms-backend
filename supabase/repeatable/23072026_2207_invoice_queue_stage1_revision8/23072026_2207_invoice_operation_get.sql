-- CloudTMS Invoice Async V8/V2 operation read authority.
drop function if exists public.invoice_operation_get(
  uuid[],uuid,text
);

create or replace function public.invoice_operation_get(
  p_operation_ids uuid[],
  p_actor_user_id uuid,
  p_mode text default 'PROGRESS',
  p_page_request jsonb default null
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_service boolean := coalesce(auth.role(),'')='service_role';
  v_role text;
  v_operations jsonb;
  v_category text;
  v_after_selection_key text;
  v_after_chunk_id uuid;
  v_limit integer;
  v_root_id uuid;
  v_root_manifest_generation integer;
  v_root_result_page_revision bigint;
  v_expected_result_page_revision bigint;
  v_result_page jsonb;
begin
  if cardinality(coalesce(p_operation_ids,array[]::uuid[]))<1
     or cardinality(p_operation_ids)>100 then
    raise exception using
      errcode='22023',
      message='p_operation_ids must contain 1..100 IDs';
  end if;

  if not v_service
     and (
       auth.uid() is null
       or auth.uid() is distinct from p_actor_user_id
     ) then
    raise exception using
      errcode='42501',
      message='Authenticated actor mismatch';
  end if;

  select lower(btrim(coalesce(u.role,'')))
  into v_role
  from public.tms_users u
  where u.id=p_actor_user_id
    and u.is_active;

  if not found and not v_service then
    raise exception using errcode='42501', message='Active actor required';
  end if;

  if p_page_request is null then
    return private._invoice_operation_get_core_v8(
      p_operation_ids,
      p_actor_user_id,
      p_mode
    );
  end if;

  if jsonb_typeof(p_page_request) is distinct from 'object' then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_PAGE_REQUEST_INVALID';
  end if;

  select
    root.id,
    root.manifest_generation,
    root.result_page_revision
  into
    v_root_id,
    v_root_manifest_generation,
    v_root_result_page_revision
  from unnest(p_operation_ids) requested(id)
  join public.invoice_operations root on root.id=requested.id
  where (
      v_service
      or v_role='admin'
      or root.actor_user_id=p_actor_user_id
    )
    and root.entity_type='INVOICE_BATCH'
    and root.input_json->>'contract_version'
      ='INVOICE_BATCH_SELECTION_ROOT_V2'
  order by root.id
  limit 1;

  if v_root_id is null
     or (
       select count(*)
       from unnest(p_operation_ids) requested(id)
       join public.invoice_operations root on root.id=requested.id
       where (
           v_service
           or v_role='admin'
           or root.actor_user_id=p_actor_user_id
         )
         and root.entity_type='INVOICE_BATCH'
         and root.input_json->>'contract_version'
           ='INVOICE_BATCH_SELECTION_ROOT_V2'
     )<>1 then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_ROOT_INVALID';
  end if;

  if coalesce(p_page_request->>'result_page_revision','')
    !~ '^[0-9]{1,18}$' then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_CURSOR_INVALID';
  end if;
  v_expected_result_page_revision :=
    (p_page_request->>'result_page_revision')::bigint;

  if v_expected_result_page_revision
     is distinct from v_root_result_page_revision then
    raise exception using
      errcode='40001',
      message='OPERATION_RESULT_CURSOR_STALE';
  end if;

  v_category := upper(coalesce(
    nullif(p_page_request->>'category',''),
    'ALL'
  ));
  if v_category not in (
    'ALL','READY','COMPLETED','IN_PROGRESS',
    'GENERATED','REGENERATED','ISSUED','ISSUED_SEND_BLOCKED',
    'ALREADY_ACTIVE','BLOCKED','FAILED','CHANGED'
  ) then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_CATEGORY_INVALID';
  end if;

  v_after_selection_key := nullif(btrim(coalesce(
    p_page_request->>'after_selection_key',
    p_page_request#>>'{cursor,after_selection_key}',
    ''
  )),'');

  if nullif(coalesce(
       p_page_request->>'after_chunk_id',
       p_page_request#>>'{cursor,after_chunk_id}',
       ''
     ),'') is not null then
    if not pg_input_is_valid(coalesce(
      p_page_request->>'after_chunk_id',
      p_page_request#>>'{cursor,after_chunk_id}'
    ),'uuid') then
      raise exception using
        errcode='22023',
        message='OPERATION_RESULT_CURSOR_INVALID';
    end if;
    v_after_chunk_id := coalesce(
      p_page_request->>'after_chunk_id',
      p_page_request#>>'{cursor,after_chunk_id}'
    )::uuid;
  end if;

  if (v_after_selection_key is null)
     is distinct from (v_after_chunk_id is null) then
    raise exception using
      errcode='22023',
      message='OPERATION_RESULT_CURSOR_INVALID';
  end if;

  if v_after_chunk_id is not null
     and not exists (
       select 1
       from public.invoice_operation_chunks anchor
       where anchor.id=v_after_chunk_id
         and anchor.operation_id=v_root_id
         and anchor.manifest_generation=v_root_manifest_generation
         and anchor.result_visible
         and anchor.selection_key=v_after_selection_key
         and anchor.replaced_by_chunk_id is null
     ) then
    raise exception using
      errcode='40001',
      message='OPERATION_RESULT_CURSOR_STALE';
  end if;

  v_limit := case
    when coalesce(p_page_request->>'limit','') ~ '^[1-9][0-9]{0,8}$'
      then greatest(1,least((p_page_request->>'limit')::integer,100))
    else 100
  end;

  v_operations := private._invoice_operation_get_core_v8(
    p_operation_ids,
    p_actor_user_id,
    p_mode
  );

  with
  current_carriers as materialized (
    select carrier.*
    from public.invoice_operation_chunks carrier
    where carrier.operation_id=v_root_id
      and carrier.manifest_generation=v_root_manifest_generation
      and carrier.result_visible
      and carrier.selection_key is not null
      and carrier.replaced_by_chunk_id is null
      and carrier.chunk_type in ('GENERATION_GROUP','ISSUE_INVOICE')
  ),
  matching as materialized (
    select carrier.*
    from current_carriers carrier
    where v_category='ALL'
       or carrier.result_category=v_category
       or (
         v_category='READY'
         and carrier.status='COMPLETE'
       )
       or (
         v_category='COMPLETED'
         and carrier.result_category in (
           'GENERATED','REGENERATED','ISSUED',
           'ISSUED_SEND_BLOCKED','ALREADY_ACTIVE'
         )
       )
       or (
         v_category='IN_PROGRESS'
         and carrier.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT')
       )
  ),
  cursor_filtered as materialized (
    select carrier.*
    from matching carrier
    where v_after_selection_key is null
       or (
         carrier.selection_key,
         carrier.id
       )>(
         v_after_selection_key,
         v_after_chunk_id
       )
  ),
  page as materialized (
    select carrier.*
    from cursor_filtered carrier
    order by carrier.selection_key,carrier.id
    limit v_limit+1
  ),
  visible as materialized (
    select carrier.*
    from page carrier
    order by carrier.selection_key,carrier.id
    limit v_limit
  ),
  enriched as materialized (
    select
      carrier.*,
      case
        when carrier.entity_type='INVOICE' then carrier.entity_id
        when pg_input_is_valid(
          coalesce(carrier.payload_json->>'invoice_id',''),
          'uuid'
        ) then (carrier.payload_json->>'invoice_id')::uuid
      end invoice_id
    from visible carrier
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_RESULT_PAGE_V2',
    'root_operation_id',v_root_id,
    'result_page_revision',v_root_result_page_revision,
    'category',v_category,
    'rows',coalesce((
      select jsonb_agg(jsonb_build_object(
        'selection_key',row.selection_key,
        'chunk_id',row.id,
        'result_category',row.result_category,
        'entity_type',row.entity_type,
        'entity_id',row.entity_id,
        'invoice_id',row.invoice_id,
        'invoice_number',coalesce(
          row.payload_json->>'invoice_number',
          invoice.invoice_no
        ),
        'client_name',coalesce(
          row.payload_json->>'client_name',
          client.name
        ),
        'candidate_display',row.payload_json->>'candidate_display',
        'week_ending_display',row.payload_json->>'week_ending_display',
        'currency',coalesce(row.payload_json->>'currency','GBP'),
        'total_ex_vat',row.payload_json->'total_ex_vat',
        'total_inc_vat',row.payload_json->'total_inc_vat',
        'row_kind',row.payload_json->>'row_kind',
        'status',row.status,
        'phase',row.phase,
        'badge_codes',coalesce(
          row.result_json->'badge_codes',
          row.payload_json->'action_blocker_codes',
          row.payload_json->'issue_blocker_codes',
          case
            when row.error_json ? 'code'
              then jsonb_build_array(row.error_json->>'code')
          end,
          '[]'::jsonb
        ),
        'error_code',row.error_json->>'code',
        'document_version_id',coalesce(
          row.result_json->>'issued_document_version_id',
          row.result_json->>'document_version_id',
          row.document_version_id::text
        ),
        'can_view',coalesce(
          row.result_json->>'issued_document_version_id',
          row.result_json->>'document_version_id',
          row.document_version_id::text
        ) is not null
          and row.status='COMPLETE',
        'blocked_for_sending',coalesce(
          (row.payload_json->>'blocked_for_sending')::boolean,
          false
        )
      ) order by row.selection_key,row.id)
      from enriched row
      left join public.invoices invoice on invoice.id=row.invoice_id
      left join public.clients client on client.id=invoice.client_id
    ),'[]'::jsonb),
    'has_more',(select count(*) from page)>v_limit,
    'next_cursor_values',case
      when (select count(*) from page)>v_limit then (
        select jsonb_build_object(
          'after_selection_key',row.selection_key,
          'after_chunk_id',row.id
        )
        from visible row
        order by row.selection_key desc,row.id desc
        limit 1
      )
      else null
    end,
    'total_count',(select count(*) from matching),
    'limit',v_limit
  )
  into v_result_page;

  return jsonb_build_object(
    'operations',coalesce(v_operations,'[]'::jsonb),
    'result_page',v_result_page
  );
end;
$function$;

alter function public.invoice_operation_get(
  uuid[],uuid,text,jsonb
) owner to postgres;
revoke all on function public.invoice_operation_get(
  uuid[],uuid,text,jsonb
) from public,anon;
grant execute on function public.invoice_operation_get(
  uuid[],uuid,text,jsonb
) to authenticated,service_role;
