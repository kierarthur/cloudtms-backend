create or replace function private._invoice_current_chunk_ids_v2(
  p_chunk_ids uuid[],
  p_limit integer default 500
)
returns table(
  requested_chunk_id uuid,
  current_chunk_id uuid,
  replacement_chain_status text,
  replacement_chain_error jsonb
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_limit integer := greatest(1, least(coalesce(p_limit, 500), 500));
begin
  if p_chunk_ids is null
     or cardinality(p_chunk_ids) = 0
     or exists (select 1 from unnest(p_chunk_ids) value where value is null) then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_CURRENT_CHUNK_IDS_INVALID';
  end if;

  if cardinality(p_chunk_ids) > v_limit then
    raise exception using
      errcode = '22023',
      message = 'INVOICE_CURRENT_CHUNK_ID_LIMIT_EXCEEDED';
  end if;

  return query
  with recursive
  requested as materialized (
    select distinct value requested_id
    from unnest(p_chunk_ids) value
  ),
  chain as (
    select
      r.requested_id,
      c.id chunk_id,
      c.replaced_by_chunk_id,
      array[c.id] path,
      1 depth,
      false cycle
    from requested r
    left join public.invoice_operation_chunks c on c.id = r.requested_id

    union all

    select
      ch.requested_id,
      next_chunk.id,
      next_chunk.replaced_by_chunk_id,
      ch.path || next_chunk.id,
      ch.depth + 1,
      next_chunk.id = any(ch.path)
    from chain ch
    join public.invoice_operation_chunks next_chunk
      on next_chunk.id = ch.replaced_by_chunk_id
    where ch.replaced_by_chunk_id is not null
      and not ch.cycle
      and ch.depth < 64
  ),
  terminal as materialized (
    select distinct on (ch.requested_id)
      ch.requested_id,
      ch.chunk_id,
      ch.replaced_by_chunk_id,
      ch.depth,
      ch.cycle,
      ch.path
    from chain ch
    order by ch.requested_id, ch.depth desc
  )
  select
    r.requested_id,
    case
      when t.chunk_id is not null
       and t.replaced_by_chunk_id is null
       and not t.cycle
       and t.depth < 64
      then t.chunk_id
    end,
    case
      when t.chunk_id is null then 'MISSING'
      when t.cycle then 'CYCLE'
      when t.depth >= 64 and t.replaced_by_chunk_id is not null then 'DEPTH_EXCEEDED'
      when t.replaced_by_chunk_id is not null then 'MISSING_LINK'
      else 'CURRENT'
    end,
    case
      when t.chunk_id is null then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_SOURCE_MISSING',
        'requested_chunk_id', r.requested_id
      )
      when t.cycle then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_CYCLE',
        'requested_chunk_id', r.requested_id,
        'depth', t.depth
      )
      when t.depth >= 64 and t.replaced_by_chunk_id is not null then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_DEPTH_EXCEEDED',
        'requested_chunk_id', r.requested_id,
        'maximum_depth', 64
      )
      when t.replaced_by_chunk_id is not null then jsonb_build_object(
        'code', 'REPLACEMENT_CHAIN_LINK_MISSING',
        'requested_chunk_id', r.requested_id,
        'missing_chunk_id', t.replaced_by_chunk_id
      )
    end
  from requested r
  left join terminal t on t.requested_id = r.requested_id
  order by r.requested_id;
end;
$function$;

alter function private._invoice_current_chunk_ids_v2(uuid[],integer)
  owner to postgres;
revoke all on function private._invoice_current_chunk_ids_v2(uuid[],integer)
  from public, anon, authenticated;
grant execute on function private._invoice_current_chunk_ids_v2(uuid[],integer)
  to service_role;
