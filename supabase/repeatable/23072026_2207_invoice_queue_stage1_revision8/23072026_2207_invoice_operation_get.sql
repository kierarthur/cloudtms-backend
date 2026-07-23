create or replace function public.invoice_operation_get(
  p_operation_ids uuid[],
  p_actor_user_id uuid,
  p_mode text default 'PROGRESS'
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_mode_input text:=upper(btrim(coalesce(p_mode,'PROGRESS')));
  v_mode text;
  v_descendant_offset integer:=0;
  v_chunk_offset integer:=0;
  v_role text;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_result jsonb;
begin
  if v_mode_input='PROGRESS' then
    v_mode:='PROGRESS';
  elsif v_mode_input='DETAIL' then
    v_mode:='DETAIL';
  elsif v_mode_input~'^DETAIL:[0-9]{1,6}$' then
    v_mode:='DETAIL';
    v_descendant_offset:=split_part(v_mode_input,':',2)::integer;
  elsif v_mode_input~'^DETAIL:[0-9]{1,6}:[0-9]{1,6}$' then
    v_mode:='DETAIL';
    v_descendant_offset:=split_part(v_mode_input,':',2)::integer;
    v_chunk_offset:=split_part(v_mode_input,':',3)::integer;
  else
    raise exception
      'p_mode must be PROGRESS, DETAIL, DETAIL:<descendant offset>, or DETAIL:<descendant offset>:<chunk offset>';
  end if;
  if cardinality(coalesce(p_operation_ids,array[]::uuid[]))<1
    or cardinality(p_operation_ids)>100 then
    raise exception 'p_operation_ids must contain 1..100 IDs';
  end if;
  if not v_service and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Authenticated actor mismatch';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_role
  from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if not found and not v_service then
    raise exception using errcode='42501',message='Active actor required';
  end if;
  if v_mode='DETAIL' and not v_service and coalesce(v_role,'')<>'admin' then
    raise exception using errcode='42501',message='Administrator permission required for DETAIL';
  end if;

  with recursive requested as materialized (
    select id,min(ordinality) ordinality
    from unnest(p_operation_ids) with ordinality x(id,ordinality)
    group by id
  ),
  authorised as materialized (
    select o.*,r.ordinality
    from requested r join public.invoice_operations o on o.id=r.id
    where v_service or v_role='admin' or o.actor_user_id=p_actor_user_id
  ),
  descendants(root_id,parent_id,id,operation_type,entity_type,entity_id,status,phase,
      progress_json,result_json,error_json,change_seq,updated_at_utc,
      created_at_utc,depth,path)
      as materialized (
    select p.id,ch.parent_operation_id,ch.id,ch.operation_type,ch.entity_type,ch.entity_id,
      ch.status,ch.phase,ch.progress_json,ch.result_json,ch.error_json,ch.change_seq,
      ch.updated_at_utc,ch.created_at_utc,1,array[p.id,ch.id]::uuid[]
    from authorised p
    join public.invoice_operations ch on ch.parent_operation_id=p.id
    union all
    select d.root_id,ch.parent_operation_id,ch.id,ch.operation_type,ch.entity_type,ch.entity_id,
      ch.status,ch.phase,ch.progress_json,ch.result_json,ch.error_json,ch.change_seq,
      ch.updated_at_utc,ch.created_at_utc,d.depth+1,d.path||ch.id
    from descendants d
    join public.invoice_operations ch on ch.parent_operation_id=d.id
    where not ch.id=any(d.path)
  ),
  scope_operation_ids as materialized (
    select a.id operation_id from authorised a
    union
    select d.id from descendants d
  ),
  scope_ranked as materialized (
    select s.operation_id,row_number() over(order by s.operation_id) rn
    from scope_operation_ids s
  ),
  scope_pages as materialized (
    select ((s.rn-1)/100)::integer page_no,
      array_agg(s.operation_id order by s.operation_id) operation_ids
    from scope_ranked s
    group by ((s.rn-1)/100)::integer
  ),
  current_slots as materialized (
    select slot.*
    from scope_pages p
    cross join lateral private._invoice_current_chunks_batch(
      p.operation_ids,null,null,10000) slot
  ),
  current_chunks as materialized (
    select slot.logical_slot_key,
      coalesce(c.id,slot.current_chunk_id) id,slot.operation_id,
      slot.chunk_type,slot.level_no,slot.sequence_no,slot.work_key,
      slot.plan_generation,slot.entity_type,slot.entity_id,
      slot.document_version_id,slot.document_asset_id,
      slot.input_document_version_id,
      case when slot.replacement_chain_status='INVALID'
        then 'BLOCKED' else c.status end status,
      case when slot.replacement_chain_status='INVALID'
        then 'REPLACEMENT_VALIDATION' else c.phase end phase,
      c.attempt_count,c.max_attempts,c.payload_json,
      c.progress_json,c.result_json,
      case when slot.replacement_chain_status='INVALID'
        then slot.replacement_chain_error else c.error_json end error_json,
      c.created_at_utc,c.updated_at_utc
    from current_slots slot
    left join public.invoice_operation_chunks c
      on c.id=slot.current_chunk_id
  ),
  child_rows as materialized (
    select *
    from(
      select d.*,row_number() over(partition by d.root_id
        order by d.depth,d.created_at_utc,d.id) rn
      from descendants d
    ) bounded
    where rn>v_descendant_offset
      and rn<=v_descendant_offset+200
  ),
  expected_purpose as materialized (
    select a.id operation_id,
      case
        when a.operation_type='ISSUE_INVOICES' then 'FINAL_ISSUE'
        when a.entity_type='TIMESHEET' then 'TIMESHEET'
        when upper(coalesce(a.input_json->>'purpose','')) in(
          'DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET')
          then upper(a.input_json->>'purpose')
        when a.entity_type='INVOICE' and exists(
          select 1 from public.invoices i
          where i.id=a.entity_id and i.status in('ISSUED','PAID'))
          then 'FINAL_ISSUE'
        else 'DRAFT_PREVIEW'
      end purpose
    from authorised a
  ),
  issue_document_ids as materialized (
    select distinct a.id operation_id,
      case when coalesce(c.payload_json->>'final_document_version_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(c.payload_json->>'final_document_version_id')::uuid end document_version_id
    from authorised a
    join current_chunks c
      on c.operation_id=a.id and c.chunk_type='ISSUE_INVOICE'
  ),
  candidate_documents as materialized (
    select a.id root_operation_id,v.*
    from authorised a join public.invoice_document_versions v on v.operation_id=a.id
    union all
    select cr.root_id root_operation_id,v.*
    from descendants cr join public.invoice_document_versions v on v.operation_id=cr.id
    union all
    select x.operation_id root_operation_id,v.*
    from issue_document_ids x join public.invoice_document_versions v on v.id=x.document_version_id
    union all
    select a.id root_operation_id,v.*
    from authorised a join public.invoices i
      on a.entity_type='INVOICE' and i.id=a.entity_id
    join public.invoice_document_versions v
      on v.id in(i.preview_document_version_id,i.issued_document_version_id)
    union all
    select a.id root_operation_id,v.*
    from authorised a join public.timesheets t
      on a.entity_type='TIMESHEET' and t.timesheet_id=a.entity_id and t.is_current
    join public.invoice_document_versions v on v.id=t.current_document_version_id
  ),
  selected_documents as materialized (
    select distinct on(c.root_operation_id) c.*
    from candidate_documents c
    join expected_purpose expected on expected.operation_id=c.root_operation_id
    where c.purpose=expected.purpose
    order by c.root_operation_id,
      case status when 'READY' then 0 when 'VERIFYING' then 1
        when 'ASSEMBLING' then 2 when 'RENDERING' then 3 else 4 end,
      created_at_utc desc,id desc
  ),
  descendant_summary as materialized (
    select a.id root_id,count(d.id)::integer total_descendants,
      greatest(a.change_seq,coalesce(max(d.change_seq),0))::bigint
        effective_change_seq,
      count(d.id) filter(where d.operation_type='BUILD_DOCUMENT')::integer
        document_operation_count,
      count(d.id) filter(where d.operation_type='DELIVER_INVOICES')::integer
        delivery_operation_count
    from authorised a
    left join descendants d on d.root_id=a.id
    group by a.id,a.change_seq
  ),
  operation_rows as materialized (
    select a.*,d.id active_document_version_id,d.entity_type document_entity_type,
      d.entity_id document_entity_id,d.purpose document_purpose,
      d.status document_status,d.r2_key,d.sha256,d.size_bytes,d.page_count,
      ds.total_descendants,
      least(greatest(ds.total_descendants-v_descendant_offset,0),200)::integer
        returned_descendants,
      ds.total_descendants>v_descendant_offset+200 results_truncated,
      case when ds.total_descendants>v_descendant_offset+200
        then 'DETAIL:'||(v_descendant_offset+200)::text end
        continuation_token,
      ds.effective_change_seq,ds.document_operation_count,
      ds.delivery_operation_count,
      (select count(*)::integer from current_chunks z
        where z.operation_id=a.id) total_chunks,
      least(greatest((select count(*)::integer from current_chunks z
        where z.operation_id=a.id)-v_chunk_offset,0),250)::integer
        returned_chunks,
      (select count(*) from current_chunks z
        where z.operation_id=a.id)>v_chunk_offset+250 chunks_truncated,
      case when (select count(*) from current_chunks z
          where z.operation_id=a.id)>v_chunk_offset+250
        then 'DETAIL:'||v_descendant_offset::text||':'||
          (v_chunk_offset+250)::text end chunk_continuation_token,
      case when v_mode='DETAIL' then(
        select coalesce(jsonb_agg(jsonb_build_object(
          'chunk_id',q.id,'chunk_type',q.chunk_type,'phase',q.phase,'status',q.status,
          'entity_type',q.entity_type,'entity_id',q.entity_id,
          'attempt_count',q.attempt_count,'max_attempts',q.max_attempts,
          'progress',q.progress_json,'result',q.result_json,'error',q.error_json)
          order by q.created_at_utc,q.id),'[]'::jsonb)
        from(
          select * from current_chunks z
          where z.operation_id=a.id
          order by z.created_at_utc,z.id
          offset v_chunk_offset limit 250
        ) q
      ) end chunks,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'operation_id',cr.id,'operation_type',cr.operation_type,
          'entity_type',cr.entity_type,'entity_id',cr.entity_id,
          'status',cr.status,'phase',cr.phase,
          'progress',case when v_mode='DETAIL' then cr.progress_json else
            jsonb_build_object('status_message',cr.progress_json->>'status_message') end,
          'result',case when v_mode='DETAIL' and cr.status='COMPLETE'
            then cr.result_json end,
          'error_code',cr.error_json->>'code','change_seq',cr.change_seq,'updated_at_utc',cr.updated_at_utc)
          order by cr.updated_at_utc,cr.id)
        from child_rows cr where cr.root_id=a.id
      ),'[]'::jsonb) children,
      jsonb_build_object(
        'created_invoice_ids',coalesce((
          select jsonb_agg(to_jsonb(b.value) order by b.value)
          from(
            select distinct e.value
            from current_chunks c
            cross join lateral jsonb_array_elements_text(
              case when jsonb_typeof(c.result_json->'invoice_ids')='array'
                then c.result_json->'invoice_ids'
                when c.result_json?'invoice_id'
                  then jsonb_build_array(c.result_json->>'invoice_id')
                else '[]'::jsonb end) e(value)
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
            order by e.value limit 500
          ) b),'[]'::jsonb),
        'document_operation_ids',coalesce((
          select jsonb_agg(to_jsonb(b.id) order by b.id)
          from(
            select cr.id from descendants cr
            where cr.root_id=a.id and cr.operation_type='BUILD_DOCUMENT'
            order by cr.id limit 500
          ) b),'[]'::jsonb),
        'delivery_operation_ids',coalesce((
          select jsonb_agg(to_jsonb(b.id) order by b.id)
          from(
            select cr.id from descendants cr
            where cr.root_id=a.id and cr.operation_type='DELIVER_INVOICES'
            order by cr.id limit 500
          ) b),'[]'::jsonb),
        'issue_results',coalesce((
          select jsonb_agg(jsonb_build_object(
            'chunk_id',b.id,'invoice_id',b.entity_id,
            'status',b.status,'phase',b.phase,
            'document_version_id',coalesce(
              b.result_json->>'issued_document_version_id',
              b.result_json->>'document_version_id'),
            'document_operation_id',b.result_json->>'document_operation_id',
            'delivery_operation_id',b.result_json->>'delivery_operation_id',
            'error_code',b.error_json->>'code') order by b.entity_id,b.id)
          from(
            select c.* from current_chunks c
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
              and c.chunk_type='ISSUE_INVOICE'
            order by c.entity_id,c.id limit 500
          ) b),'[]'::jsonb),
        'mail_outbox_ids',coalesce((
          select jsonb_agg(to_jsonb(b.value) order by b.value)
          from(
            select distinct m.value
            from current_chunks c
            cross join lateral jsonb_array_elements_text(
              case when jsonb_typeof(c.result_json->'mail_outbox_ids')='array'
                then c.result_json->'mail_outbox_ids' else '[]'::jsonb end)
              m(value)
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
            order by m.value limit 500
          ) b),'[]'::jsonb),
        'blocked_or_failed_entities',coalesce((
          select jsonb_agg(jsonb_build_object(
            'chunk_id',b.id,'entity_type',b.entity_type,
            'entity_id',b.entity_id,'status',b.status,
            'error_code',b.error_json->>'code') order by b.id)
          from(
            select c.* from current_chunks c
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
              and c.status in('BLOCKED','FAILED','DEAD_LETTER')
            order by c.id limit 500
          ) b),'[]'::jsonb),
        'invoice_ids_page',coalesce(a.result_json->'invoice_ids_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false)),
        'document_operations_page',coalesce(
          a.result_json->'document_operations_page',
          jsonb_build_object('total_count',ds.document_operation_count,
            'returned_count',least(ds.document_operation_count,500),
            'truncated',ds.document_operation_count>500)),
        'delivery_operations_page',coalesce(
          a.result_json->'delivery_operations_page',
          jsonb_build_object('total_count',ds.delivery_operation_count,
            'returned_count',least(ds.delivery_operation_count,500),
            'truncated',ds.delivery_operation_count>500)),
        'issue_outcomes_page',coalesce(a.result_json->'issue_outcomes_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false)),
        'mail_ids_page',coalesce(a.result_json->'mail_ids_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false)),
        'blocked_entities_page',coalesce(
          a.result_json->'blocked_entities_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false))
      ) aggregate_result
    from authorised a
    join descendant_summary ds on ds.root_id=a.id
    left join selected_documents d on d.root_operation_id=a.id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'operation_id',r.id,'parent_operation_id',r.parent_operation_id,
    'operation_type',r.operation_type,'entity_type',r.entity_type,'entity_id',r.entity_id,
    'status',r.status,'phase',r.phase,'priority',r.priority,
    'source_revision',r.source_revision,'template_version',r.template_version,
    'total_units',r.total_units,'completed_units',r.completed_units,'failed_units',r.failed_units,
    'progress',r.progress_json,
    'result',case when v_mode='DETAIL' or r.status='COMPLETE'
      then coalesce(r.result_json,'{}'::jsonb)||r.aggregate_result end,
    'error_code',r.error_json->>'code',
    'error_summary',coalesce(r.error_json->>'summary',r.error_json->>'message'),
    'requires_user_action',r.requires_user_action,
    'can_retry',r.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'),
    'can_cancel',r.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and not exists(
        select 1 from current_chunks ic
        join public.invoices i on i.id=ic.entity_id
        where ic.operation_id in(
            select r.id
            union all select cr.id from descendants cr where cr.root_id=r.id)
          and ic.chunk_type='ISSUE_INVOICE'
          and i.status in('ISSUED','PAID')),
    'change_seq',r.change_seq,
    'effective_change_seq',r.effective_change_seq,
    'total_descendants',r.total_descendants,
    'descendant_page_offset',v_descendant_offset,
    'returned_descendants',r.returned_descendants,
    'results_truncated',r.results_truncated,
    'continuation_token',r.continuation_token,
    'total_chunks',r.total_chunks,
    'returned_chunks',r.returned_chunks,
    'chunks_truncated',r.chunks_truncated,
    'chunk_page_offset',v_chunk_offset,
    'chunk_continuation_token',r.chunk_continuation_token,
    'legal_issue_state',coalesce(
      r.result_json->>'legal_issue_status',
      case when r.operation_type='ISSUE_INVOICES' then 'IN_PROGRESS' end),
    'delivery_state',coalesce(
      r.result_json->>'delivery_status',
      case when r.delivery_operation_count>0 then 'IN_PROGRESS'
        else 'NOT_REQUESTED' end),
    'active_document_version_id',r.active_document_version_id,
    'document_purpose',r.document_purpose,'document_status',r.document_status,
    'ready_key',case when r.document_status='READY' then r.r2_key end,
    'artifact',case when v_mode='DETAIL' and r.document_status='READY'
      then jsonb_build_object('sha256',r.sha256,'size_bytes',r.size_bytes,
        'page_count',r.page_count) end,
    'children',r.children,'chunks',r.chunks,
    'created_at_utc',r.created_at_utc,'updated_at_utc',r.updated_at_utc,
    'completed_at_utc',r.completed_at_utc
  ) order by r.ordinality),'[]'::jsonb) into v_result
  from operation_rows r;

  return v_result;
end;
$function$;

revoke all on function public.invoice_operation_get(uuid[],uuid,text) from public,anon;
grant execute on function public.invoice_operation_get(uuid[],uuid,text)
  to authenticated,service_role;
