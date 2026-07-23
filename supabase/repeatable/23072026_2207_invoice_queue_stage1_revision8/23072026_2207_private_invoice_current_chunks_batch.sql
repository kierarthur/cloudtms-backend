create or replace function private._invoice_current_chunks_batch(
  p_operation_ids uuid[] default null,
  p_document_version_ids uuid[] default null,
  p_asset_ids uuid[] default null,
  p_limit integer default 10000
) returns table(
  logical_slot_key text,
  current_chunk_id uuid,
  operation_id uuid,
  chunk_type text,
  level_no integer,
  sequence_no integer,
  work_key text,
  plan_generation integer,
  entity_type text,
  entity_id uuid,
  document_version_id uuid,
  document_asset_id uuid,
  input_document_version_id uuid,
  current_status text,
  current_phase text,
  replacement_chain_status text,
  replacement_chain_error jsonb
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_operation_count integer:=cardinality(coalesce(p_operation_ids,array[]::uuid[]));
  v_document_count integer:=cardinality(coalesce(p_document_version_ids,array[]::uuid[]));
  v_asset_count integer:=cardinality(coalesce(p_asset_ids,array[]::uuid[]));
  v_limit integer:=greatest(1,least(coalesce(p_limit,10000),10000));
  v_row_count integer;
begin
  if v_operation_count+v_document_count+v_asset_count<1 then
    raise exception using errcode='22023',
      message='At least one current-chunk scope is required';
  end if;
  if v_operation_count>500 or v_document_count>500 or v_asset_count>500
     or v_operation_count+v_document_count+v_asset_count>500 then
    raise exception using errcode='22023',
      message='Current-chunk scope is limited to 500 identifiers';
  end if;

  with requested_operations as materialized (
    select distinct x.id
    from unnest(coalesce(p_operation_ids,array[]::uuid[])) x(id)
    where x.id is not null
    union
    select distinct c.operation_id
    from public.invoice_operation_chunks c
    where c.document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.input_document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.document_asset_id=any(coalesce(p_asset_ids,array[]::uuid[]))
  )
  select count(*)::integer into v_row_count
  from (
    select 1
    from public.invoice_operation_chunks c
    join requested_operations r on r.id=c.operation_id
    limit v_limit+1
  ) bounded;

  if v_row_count>v_limit then
    raise exception using errcode='54000',
      message='CURRENT_CHUNK_SCOPE_TOO_LARGE';
  end if;

  return query
  with recursive requested_operations as materialized (
    select distinct x.id
    from unnest(coalesce(p_operation_ids,array[]::uuid[])) x(id)
    where x.id is not null
    union
    select distinct c.operation_id
    from public.invoice_operation_chunks c
    where c.document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.input_document_version_id=any(coalesce(
        p_document_version_ids,array[]::uuid[]))
       or c.document_asset_id=any(coalesce(p_asset_ids,array[]::uuid[]))
  ),
  raw as materialized (
    select c.*,
      encode(digest(concat_ws('|',
        c.operation_id::text,c.chunk_type,c.level_no::text,c.sequence_no::text,
        coalesce(c.entity_type,'~'),coalesce(c.entity_id::text,'~'),
        coalesce(c.document_version_id::text,'~'),
        coalesce(c.document_asset_id::text,'~'),
        coalesce(c.input_document_version_id::text,'~')),'sha256'),'hex')
        slot_key
    from public.invoice_operation_chunks c
    join requested_operations r on r.id=c.operation_id
  ),
  incoming as materialized (
    select r.replaced_by_chunk_id,count(*)::integer predecessor_count
    from raw r
    where r.replaced_by_chunk_id is not null
    group by r.replaced_by_chunk_id
  ),
  edge_validation as materialized (
    select old.id old_chunk_id,old.slot_key,
      case
        when old.replacement_required and old.replaced_by_chunk_id is null
          then 'REPLACEMENT_REQUIRED_MISSING'
        when old.replaced_by_chunk_id is null then null
        when old.status<>'SUPERSEDED' then 'REPLACEMENT_SOURCE_NOT_SUPERSEDED'
        when not old.replacement_required then 'REPLACEMENT_LINK_NOT_REQUIRED'
        when replacement.id is null then 'REPLACEMENT_MISSING'
        when replacement.operation_id<>old.operation_id
          then 'REPLACEMENT_CROSS_OPERATION'
        when replacement.slot_key<>old.slot_key then 'REPLACEMENT_CROSS_SLOT'
        when replacement.plan_generation<=old.plan_generation
          then 'REPLACEMENT_GENERATION_NOT_HIGHER'
        when coalesce(i.predecessor_count,0)>1
          then 'REPLACEMENT_MULTIPLE_PREDECESSORS'
        else null
      end error_code
    from raw old
    left join raw replacement on replacement.id=old.replaced_by_chunk_id
    left join incoming i on i.replaced_by_chunk_id=old.replaced_by_chunk_id
  ),
  walk(origin_chunk_id,current_chunk_id,slot_key,path,depth,cycle) as (
    select r.id,r.id,r.slot_key,array[r.id]::uuid[],0,false
    from raw r
    union all
    select w.origin_chunk_id,next_chunk.id,w.slot_key,
      w.path||next_chunk.id,w.depth+1,next_chunk.id=any(w.path)
    from walk w
    join raw current_chunk on current_chunk.id=w.current_chunk_id
    join raw next_chunk on next_chunk.id=current_chunk.replaced_by_chunk_id
    where not w.cycle and w.depth<64
  ),
  walk_validation as materialized (
    select w.slot_key,
      bool_or(w.cycle) has_cycle,
      bool_or(w.depth=64 and current_chunk.replaced_by_chunk_id is not null)
        too_deep
    from walk w
    join raw current_chunk on current_chunk.id=w.current_chunk_id
    group by w.slot_key
  ),
  leaves as materialized (
    select r.slot_key,count(*)::integer leaf_count,
      (array_agg(r.id order by r.id))[1] leaf_id
    from raw r
    where r.replaced_by_chunk_id is null
    group by r.slot_key
  ),
  slot_errors as materialized (
    select r.slot_key,
      coalesce(l.leaf_count,0) leaf_count,
      l.leaf_id,
      coalesce(w.has_cycle,false) has_cycle,
      coalesce(w.too_deep,false) too_deep,
      array_remove(array_agg(distinct e.error_code order by e.error_code),null)
        edge_errors
    from (select distinct slot_key from raw) r
    left join leaves l on l.slot_key=r.slot_key
    left join walk_validation w on w.slot_key=r.slot_key
    left join edge_validation e on e.slot_key=r.slot_key
    group by r.slot_key,l.leaf_count,l.leaf_id,w.has_cycle,w.too_deep
  ),
  classified as materialized (
    select s.*,
      case
        when s.has_cycle then 'REPLACEMENT_CYCLE'
        when s.too_deep then 'REPLACEMENT_CHAIN_TOO_DEEP'
        when cardinality(s.edge_errors)>0 then s.edge_errors[1]
        when s.leaf_count=0 then 'REPLACEMENT_CURRENT_LEAF_MISSING'
        when s.leaf_count>1 then 'REPLACEMENT_MULTIPLE_CURRENT_LEAVES'
        else null
      end primary_error
    from slot_errors s
  )
  select c.slot_key,
    case when c.leaf_count=1 then leaf.id end,
    coalesce(leaf.operation_id,slot.operation_id),
    coalesce(leaf.chunk_type,slot.chunk_type),
    coalesce(leaf.level_no,slot.level_no),
    coalesce(leaf.sequence_no,slot.sequence_no),
    leaf.work_key,leaf.plan_generation,
    coalesce(leaf.entity_type,slot.entity_type),
    coalesce(leaf.entity_id,slot.entity_id),
    coalesce(leaf.document_version_id,slot.document_version_id),
    coalesce(leaf.document_asset_id,slot.document_asset_id),
    coalesce(leaf.input_document_version_id,slot.input_document_version_id),
    leaf.status,leaf.phase,
    case when c.primary_error is null then 'VALID' else 'INVALID' end,
    case when c.primary_error is null then null
      else jsonb_build_object(
        'code',c.primary_error,
        'logical_slot_key',c.slot_key,
        'leaf_count',c.leaf_count,
        'edge_errors',to_jsonb(c.edge_errors))
    end
  from classified c
  join lateral (
    select r.* from raw r where r.slot_key=c.slot_key order by r.id limit 1
  ) slot on true
  left join raw leaf on leaf.id=c.leaf_id
  order by coalesce(leaf.operation_id,slot.operation_id),
    coalesce(leaf.chunk_type,slot.chunk_type),
    coalesce(leaf.level_no,slot.level_no),
    coalesce(leaf.sequence_no,slot.sequence_no),c.slot_key;
end;
$function$;

revoke all on function private._invoice_current_chunks_batch(
  uuid[],uuid[],uuid[],integer) from public,anon,authenticated;
grant execute on function private._invoice_current_chunks_batch(
  uuid[],uuid[],uuid[],integer) to service_role;
