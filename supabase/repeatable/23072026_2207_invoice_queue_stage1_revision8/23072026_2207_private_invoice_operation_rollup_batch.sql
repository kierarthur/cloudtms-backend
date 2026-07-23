create or replace function private._invoice_operation_rollup_batch(
  p_operation_ids uuid[],
  p_now_utc timestamptz default now(),
  p_propagate_ancestors boolean default true
) returns table(
  operation_id uuid,
  status text,
  phase text,
  total_units integer,
  completed_units integer,
  failed_units integer,
  blocked_required_count integer,
  requires_user_action boolean,
  change_seq bigint
)
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
begin
  return query
  with recursive requested as materialized (
    select distinct x.id
    from unnest(coalesce(p_operation_ids,array[]::uuid[])) x(id)
    where x.id is not null
  ),
  scope(id,depth) as (
    select r.id,0 from requested r
    union
    select o.parent_operation_id,s.depth+1
    from scope s join public.invoice_operations o on o.id=s.id
    where p_propagate_ancestors and o.parent_operation_id is not null
      and s.depth<16
  ),
  targets as materialized (
    select distinct id from scope where id is not null
  ),
  ancestor_bumps as materialized (
    select distinct id from scope where id is not null and depth>0
  ),
  target_ranked as materialized (
    select t.id,
      row_number() over(order by t.id) operation_no
    from targets t
  ),
  target_pages as materialized (
    select ((t.operation_no-1)/500)::integer page_no,
      array_agg(t.id order by t.id) operation_ids
    from target_ranked t
    group by ((t.operation_no-1)/500)::integer
  ),
  current_slots as materialized (
    select cc.*
    from target_pages a
    cross join lateral private._invoice_current_chunks_batch(
      a.operation_ids,null,null,10000) cc
  ),
  current_chunks as materialized (
    select s.logical_slot_key,
      coalesce(c.id,s.current_chunk_id) id,s.operation_id,s.chunk_type,
      s.level_no,s.sequence_no,s.work_key,s.plan_generation,
      s.entity_type,s.entity_id,s.document_version_id,s.document_asset_id,
      s.input_document_version_id,
      case when s.replacement_chain_status='INVALID'
        then 'BLOCKED' else c.status end status,
      case when s.replacement_chain_status='INVALID'
        then 'REPLACEMENT_VALIDATION' else c.phase end phase,
      c.payload_json,c.progress_json,c.result_json,
      case when s.replacement_chain_status='INVALID'
        then s.replacement_chain_error else c.error_json end error_json,
      coalesce(c.updated_at_utc,v_now) updated_at_utc,
      s.replacement_chain_status,s.replacement_chain_error
    from current_slots s
    left join public.invoice_operation_chunks c on c.id=s.current_chunk_id
  ),
  counts as materialized (
    select t.id operation_id,count(c.logical_slot_key)::integer total_units,
      count(*) filter(where c.status='QUEUED')::integer queued_units,
      count(*) filter(where c.status='RUNNING')::integer running_units,
      count(*) filter(where c.status='WAITING')::integer waiting_units,
      count(*) filter(where c.status='RETRY_WAIT')::integer retry_units,
      count(*) filter(where c.status='BLOCKED')::integer blocked_units,
      count(*) filter(where c.status='COMPLETE')::integer complete_units,
      count(*) filter(where c.status='FAILED')::integer failed_only_units,
      count(*) filter(where c.status='DEAD_LETTER')::integer dead_units,
      count(*) filter(where c.status='CANCELLED')::integer cancelled_units,
      count(*) filter(where c.status='SUPERSEDED')::integer superseded_units,
      count(*) filter(where c.replacement_chain_status='INVALID')::integer
        missing_replacement_units,
      (count(*) filter(where c.status in(
        'FAILED','DEAD_LETTER','BLOCKED'))
       +count(*) filter(where c.replacement_chain_status='INVALID'
          and c.status<>'BLOCKED'))::integer failed_units,
      (array_agg(c.phase order by
        case c.status when 'RUNNING' then 0 when 'QUEUED' then 1
          when 'RETRY_WAIT' then 2 when 'BLOCKED' then 3 when 'WAITING' then 4
          else 5 end,c.updated_at_utc desc,c.id)
        filter(where c.id is not null))[1] current_phase,
      coalesce(jsonb_agg(jsonb_build_object(
        'chunk_id',c.id,'chunk_type',c.chunk_type,'entity_type',c.entity_type,
        'entity_id',c.entity_id,'code',c.error_json->>'code',
        'replacement_chain_status',c.replacement_chain_status)
        order by c.updated_at_utc desc,c.id)
        filter(where c.status in('BLOCKED','FAILED','DEAD_LETTER')),
        '[]'::jsonb) blocker_summary
    from targets t
    left join current_chunks c on c.operation_id=t.id
    group by t.id
  ),
  derived as materialized (
    select c.*,
      case
        when o.status in('CANCELLED','SUPERSEDED') then o.status
        when c.missing_replacement_units>0 then 'BLOCKED'
        when c.running_units>0 then 'RUNNING'
        when c.queued_units>0 then 'QUEUED'
        when c.retry_units>0 then 'RETRY_WAIT'
        when c.blocked_units>0 then 'BLOCKED'
        when c.waiting_units>0 then 'WAITING'
        when c.dead_units>0 then 'DEAD_LETTER'
        when c.failed_only_units>0 then 'FAILED'
        when c.total_units>0 and c.complete_units=c.total_units then 'COMPLETE'
        when c.total_units>0 and c.cancelled_units=c.total_units then 'CANCELLED'
        when c.total_units>0 and c.superseded_units=c.total_units then 'SUPERSEDED'
        when c.cancelled_units>0 then 'BLOCKED'
        else o.status
      end derived_status,
      c.blocked_units>0 or c.dead_units>0 or c.failed_only_units>0
        or c.missing_replacement_units>0
        requires_action
    from counts c join public.invoice_operations o on o.id=c.operation_id
  ),
  child_rows as materialized (
    select parent.id operation_id,child.id child_id,
      child.operation_type,child.status,child.phase,child.change_seq,
      child.entity_id,child.error_json->>'code' error_code,
      row_number() over(partition by parent.id
        order by child.created_at_utc,child.id) child_no,
      count(*) over(partition by parent.id)::integer child_total
    from public.invoice_operations parent
    join targets t on t.id=parent.id
    join public.invoice_operations child
      on child.parent_operation_id=parent.id
  ),
  child_results as materialized (
    select t.id operation_id,
      coalesce(jsonb_agg(jsonb_build_object(
        'operation_id',child.child_id,
        'operation_type',child.operation_type,
        'status',child.status,'phase',child.phase,
        'change_seq',child.change_seq,
        'entity_id',child.entity_id,
        'error_code',child.error_code)
        order by child.child_no)
        filter(where child.child_no<=200),'[]'::jsonb) children,
      coalesce(max(child.child_total),0)::integer child_total,
      least(coalesce(max(child.child_total),0),200)::integer child_returned,
      coalesce(max(child.child_total),0)>200 children_truncated,
      coalesce(max(child.change_seq),0) descendant_change_seq
    from targets t
    left join child_rows child on child.operation_id=t.id
    group by t.id
  ),
  descendant_tree(root_operation_id,operation_id,depth,path) as (
    select t.id,t.id,0,array[t.id]::uuid[] from targets t
    union all
    select d.root_operation_id,o.id,d.depth+1,d.path||o.id
    from descendant_tree d
    join public.invoice_operations o
      on o.parent_operation_id=d.operation_id
    where not o.id=any(d.path)
  ),
  descendant_ranked as materialized (
    select d.*,
      row_number() over(
        partition by d.root_operation_id
        order by d.depth,d.operation_id) descendant_no
    from descendant_tree d
  ),
  descendant_scope as materialized (
    select d.root_operation_id,d.operation_id,d.depth,d.descendant_no
    from descendant_ranked d
  ),
  descendant_operation_ids as materialized (
    select d.operation_id,
      row_number() over(order by d.operation_id) operation_no
    from (
      select distinct scoped.operation_id
      from descendant_scope scoped
    ) d
  ),
  descendant_operation_pages as materialized (
    select ((d.operation_no-1)/500)::integer page_no,
      array_agg(d.operation_id order by d.operation_id) operation_ids
    from descendant_operation_ids d
    group by ((d.operation_no-1)/500)::integer
  ),
  descendant_current_slots as materialized (
    select cc.*
    from descendant_operation_pages a
    cross join lateral private._invoice_current_chunks_batch(
      a.operation_ids,null,null,10000) cc
  ),
  descendant_chunks as materialized (
    select d.root_operation_id,
      coalesce(c.id,s.current_chunk_id) id,s.operation_id,s.chunk_type,
      s.entity_type,s.entity_id,
      case when s.replacement_chain_status='INVALID'
        then 'BLOCKED' else c.status end status,
      case when s.replacement_chain_status='INVALID'
        then 'REPLACEMENT_VALIDATION' else c.phase end phase,
      c.result_json,
      case when s.replacement_chain_status='INVALID'
        then s.replacement_chain_error else c.error_json end error_json,
      coalesce(c.updated_at_utc,v_now) updated_at_utc
    from descendant_scope d
    join descendant_current_slots s on s.operation_id=d.operation_id
    left join public.invoice_operation_chunks c on c.id=s.current_chunk_id
  ),
  invoice_result_rows as materialized (
    select distinct c.root_operation_id,x.value::uuid invoice_id
    from descendant_chunks c
    cross join lateral jsonb_array_elements_text(
      case when jsonb_typeof(c.result_json->'invoice_ids')='array'
        then c.result_json->'invoice_ids' else '[]'::jsonb end) x(value)
    where x.value~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  invoice_results as materialized (
    select r.root_operation_id,
      jsonb_agg(to_jsonb(r.invoice_id) order by r.invoice_id) invoice_ids
    from (
      select x.*,row_number() over(partition by x.root_operation_id
        order by x.invoice_id) result_no
      from invoice_result_rows x
    ) r
    where r.result_no<=500
    group by r.root_operation_id
  ),
  document_result_rows as materialized (
    select distinct d.root_operation_id,o.id document_operation_id
    from descendant_scope d
    join public.invoice_operations o on o.id=d.operation_id
    where o.operation_type='BUILD_DOCUMENT'
    union
    select distinct c.root_operation_id,
      (c.result_json->>'document_operation_id')::uuid
    from descendant_chunks c
    where coalesce(c.result_json->>'document_operation_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  document_results as materialized (
    select r.root_operation_id,
      jsonb_agg(to_jsonb(r.document_operation_id)
        order by r.document_operation_id) document_operation_ids
    from (
      select x.*,row_number() over(partition by x.root_operation_id
        order by x.document_operation_id) result_no
      from document_result_rows x
    ) r
    where r.result_no<=500
    group by r.root_operation_id
  ),
  delivery_operation_result_rows as materialized (
    select distinct d.root_operation_id,o.id delivery_operation_id
    from descendant_scope d
    join public.invoice_operations o on o.id=d.operation_id
    where o.operation_type='DELIVER_INVOICES'
  ),
  delivery_operation_results as materialized (
    select r.root_operation_id,
      jsonb_agg(to_jsonb(r.delivery_operation_id)
        order by r.delivery_operation_id) delivery_operation_ids
    from(
      select x.*,row_number() over(partition by x.root_operation_id
        order by x.delivery_operation_id) result_no
      from delivery_operation_result_rows x
    ) r
    where r.result_no<=500
    group by r.root_operation_id
  ),  mail_result_rows as materialized (
    select distinct c.root_operation_id,x.value::uuid mail_outbox_id
    from descendant_chunks c
    cross join lateral jsonb_array_elements_text(
      case when jsonb_typeof(c.result_json->'mail_outbox_ids')='array'
        then c.result_json->'mail_outbox_ids' else '[]'::jsonb end) x(value)
    where x.value~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    union
    select distinct c.root_operation_id,
      (c.result_json->>'mail_outbox_id')::uuid
    from descendant_chunks c
    where coalesce(c.result_json->>'mail_outbox_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  mail_results as materialized (
    select r.root_operation_id,
      jsonb_agg(to_jsonb(r.mail_outbox_id)
        order by r.mail_outbox_id) mail_outbox_ids
    from (
      select x.*,row_number() over(partition by x.root_operation_id
        order by x.mail_outbox_id) result_no
      from mail_result_rows x
    ) r
    where r.result_no<=500
    group by r.root_operation_id
  ),
  issue_result_rows as materialized (
    select c.root_operation_id,c.entity_id invoice_id,
      c.status,c.phase,c.result_json,c.error_json,c.updated_at_utc,c.id
    from descendant_chunks c
    where c.chunk_type='ISSUE_INVOICE'
      and c.entity_type='INVOICE' and c.entity_id is not null
  ),
  issue_results as materialized (
    select r.root_operation_id,
      jsonb_agg(jsonb_build_object(
        'chunk_id',r.id,'invoice_id',r.invoice_id,
        'status',r.status,'phase',r.phase,
        'document_version_id',coalesce(
          r.result_json->>'issued_document_version_id',
          r.result_json->>'document_version_id'),
        'document_operation_id',r.result_json->>'document_operation_id',
        'delivery_operation_id',r.result_json->>'delivery_operation_id',
        'error_code',r.error_json->>'code')
        order by r.updated_at_utc,r.id) issue_outcomes
    from (
      select x.*,row_number() over(partition by x.root_operation_id
        order by x.updated_at_utc,x.id) result_no
      from issue_result_rows x
    ) r
    where r.result_no<=500
    group by r.root_operation_id
  ),
  descendant_blocked as materialized (
    select c.root_operation_id,
      jsonb_agg(jsonb_build_object(
        'chunk_id',c.id,'chunk_type',c.chunk_type,
        'entity_type',c.entity_type,'entity_id',c.entity_id,
        'status',c.status,'code',c.error_json->>'code')
        order by c.updated_at_utc desc,c.id)
        filter(where c.status in('BLOCKED','FAILED','DEAD_LETTER'))
        blocked_entities
    from (
      select x.*,row_number() over(partition by x.root_operation_id
        order by x.updated_at_utc desc,x.id) result_no
      from descendant_chunks x
      where x.status in('BLOCKED','FAILED','DEAD_LETTER')
    ) c
    where c.result_no<=500
    group by c.root_operation_id
  ),
  category_counts as materialized (
    select t.id root_operation_id,
      (select count(*)::integer from invoice_result_rows r
        where r.root_operation_id=t.id) invoice_total,
      (select count(*)::integer from document_result_rows r
        where r.root_operation_id=t.id) document_total,
      (select count(*)::integer from issue_result_rows r
        where r.root_operation_id=t.id) issue_total,
      (select count(*)::integer from delivery_operation_result_rows r
        where r.root_operation_id=t.id) delivery_operation_total,
      (select count(*)::integer from mail_result_rows r
        where r.root_operation_id=t.id) mail_total,
      (select count(*)::integer from descendant_chunks r
        where r.root_operation_id=t.id
          and r.status in('BLOCKED','FAILED','DEAD_LETTER')) blocked_total
    from targets t
  ),
  terminal_counts as materialized (
    select t.id root_operation_id,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE')::integer
        issue_total,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status='COMPLETE')::integer issue_complete,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT'))::integer
        issue_active,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status='BLOCKED')::integer issue_blocked,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status='FAILED')::integer issue_failed,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status='DEAD_LETTER')::integer issue_dead,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status='CANCELLED')::integer issue_cancelled,
      count(*) filter(where c.chunk_type='ISSUE_INVOICE'
        and c.status='SUPERSEDED')::integer issue_superseded,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE')::integer
        delivery_total,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status='COMPLETE')::integer delivery_complete,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT'))::integer
        delivery_active,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status='BLOCKED')::integer delivery_blocked,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status='FAILED')::integer delivery_failed,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status='DEAD_LETTER')::integer delivery_dead,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status='CANCELLED')::integer delivery_cancelled,
      count(*) filter(where c.chunk_type='DELIVERY_PREPARE'
        and c.status='SUPERSEDED')::integer delivery_superseded
    from targets t
    left join descendant_chunks c on c.root_operation_id=t.id
    group by t.id
  ),
  result_aggregates as materialized (
    select t.id operation_id,
      coalesce(i.invoice_ids,'[]'::jsonb) invoice_ids,
      coalesce(d.document_operation_ids,'[]'::jsonb)
        document_operation_ids,
      coalesce(delivery_ops.delivery_operation_ids,'[]'::jsonb)
        delivery_operation_ids,
      coalesce(m.mail_outbox_ids,'[]'::jsonb) mail_outbox_ids,
      coalesce(ir.issue_outcomes,'[]'::jsonb) issue_outcomes,
      coalesce(b.blocked_entities,'[]'::jsonb) blocked_entities,
      jsonb_build_object(
        'total_count',cc.invoice_total,
        'returned_count',least(cc.invoice_total,500),
        'truncated',cc.invoice_total>500) invoice_ids_page,
      jsonb_build_object(
        'total_count',cc.document_total,
        'returned_count',least(cc.document_total,500),
        'truncated',cc.document_total>500) document_operations_page,
      jsonb_build_object(
        'total_count',cc.issue_total,
        'returned_count',least(cc.issue_total,500),
        'truncated',cc.issue_total>500) issue_outcomes_page,
      jsonb_build_object(
        'total_count',cc.delivery_operation_total,
        'returned_count',least(cc.delivery_operation_total,500),
        'truncated',cc.delivery_operation_total>500)
        delivery_operations_page,
      jsonb_build_object(
        'total_count',cc.mail_total,
        'returned_count',least(cc.mail_total,500),
        'truncated',cc.mail_total>500) mail_ids_page,
      jsonb_build_object(
        'total_count',cc.blocked_total,
        'returned_count',least(cc.blocked_total,500),
        'truncated',cc.blocked_total>500) blocked_entities_page,
      (select count(*)::integer from descendant_scope ds
        where ds.root_operation_id=t.id) total_descendant_operations,
      least(500,(select count(*)::integer from descendant_scope ds
        where ds.root_operation_id=t.id)) returned_descendant_operations,
      (select count(*)>500 from descendant_scope ds
        where ds.root_operation_id=t.id) results_truncated,
      case when (select count(*)>500 from descendant_scope ds
          where ds.root_operation_id=t.id)
        then encode(digest(concat_ws('|',t.id::text,'DESCENDANTS','500'),
          'sha256'),'hex')
      end continuation_token,
      case
        when tc.issue_total=0 then 'NOT_REQUESTED'
        when tc.issue_complete=tc.issue_total then 'ISSUED'
        when tc.issue_cancelled=tc.issue_total then 'CANCELLED'
        when tc.issue_superseded=tc.issue_total then 'SUPERSEDED'
        when tc.issue_dead=tc.issue_total then 'DEAD_LETTER'
        when tc.issue_failed=tc.issue_total then 'FAILED'
        when tc.issue_complete>0 and tc.issue_active>0
          and tc.issue_blocked+tc.issue_failed+tc.issue_dead
            +tc.issue_cancelled+tc.issue_superseded=0
          then 'PARTIAL_IN_PROGRESS'
        when tc.issue_blocked+tc.issue_failed+tc.issue_dead
            +tc.issue_cancelled+tc.issue_superseded>0
          then 'PARTIAL_OR_BLOCKED'
        else 'IN_PROGRESS' end legal_issue_status,
      case
        when tc.delivery_total=0 then 'NOT_REQUESTED'
        when tc.delivery_complete=tc.delivery_total then 'PREPARED'
        when tc.delivery_cancelled=tc.delivery_total then 'CANCELLED'
        when tc.delivery_superseded=tc.delivery_total then 'SUPERSEDED'
        when tc.delivery_dead=tc.delivery_total then 'DEAD_LETTER'
        when tc.delivery_failed=tc.delivery_total then 'FAILED'
        when tc.delivery_blocked+tc.delivery_failed+tc.delivery_dead
            +tc.delivery_cancelled+tc.delivery_superseded>0
          then 'PARTIAL_OR_BLOCKED'
        else 'IN_PROGRESS' end delivery_status
    from targets t
    left join invoice_results i on i.root_operation_id=t.id
    left join document_results d on d.root_operation_id=t.id
    left join delivery_operation_results delivery_ops
      on delivery_ops.root_operation_id=t.id
    left join mail_results m on m.root_operation_id=t.id
    left join issue_results ir on ir.root_operation_id=t.id
    left join descendant_blocked b on b.root_operation_id=t.id
    join category_counts cc on cc.root_operation_id=t.id
    join terminal_counts tc on tc.root_operation_id=t.id
  ),
  updated as materialized (
    update public.invoice_operations o
    set status=d.derived_status,
      phase=case when d.missing_replacement_units>0
        then 'BLOCKED' else coalesce(d.current_phase,o.phase) end,
      total_units=d.total_units,chunk_count=d.total_units,
      completed_units=d.complete_units,
      failed_units=d.failed_units,
      requires_user_action=d.requires_action,
      progress_json=(coalesce(o.progress_json,'{}'::jsonb)
        ||jsonb_build_object(
          'total_units',d.total_units,'completed_units',d.complete_units,
          'failed_units',d.failed_units,
          'blocked_required_count',
            d.blocked_units+d.missing_replacement_units,
          'requires_user_action',d.requires_action,
          'child_operations',jsonb_build_object(
            'rows',cr.children,
            'total_count',cr.child_total,
            'returned_count',cr.child_returned,
            'truncated',cr.children_truncated))),
      result_json=coalesce(o.result_json,'{}'::jsonb)
        ||jsonb_build_object(
          'invoice_ids',ra.invoice_ids,
          'document_operation_ids',ra.document_operation_ids,
          'delivery_operation_ids',ra.delivery_operation_ids,
          'mail_outbox_ids',ra.mail_outbox_ids,
          'issue_outcomes',ra.issue_outcomes,
          'blocked_entities',ra.blocked_entities,
          'invoice_ids_page',ra.invoice_ids_page,
          'document_operations_page',ra.document_operations_page,
          'issue_outcomes_page',ra.issue_outcomes_page,
          'delivery_operations_page',ra.delivery_operations_page,
          'mail_ids_page',ra.mail_ids_page,
          'blocked_entities_page',ra.blocked_entities_page,
          'total_descendant_operations',ra.total_descendant_operations,
          'returned_descendant_operations',ra.returned_descendant_operations,
          'results_truncated',ra.results_truncated,
          'continuation_token',ra.continuation_token,
          'legal_issue_status',ra.legal_issue_status,
          'delivery_status',ra.delivery_status),
      error_json=case
        when d.missing_replacement_units>0 then jsonb_build_object(
          'code','MISSING_REPLACEMENT_WORK',
          'count',d.missing_replacement_units,
          'blocked_entities',d.blocker_summary)
        when d.failed_units>0 then coalesce(o.error_json,'{}'::jsonb)
          ||jsonb_build_object('blocked_entities',d.blocker_summary)
        else o.error_json end,
      started_at_utc=case when d.derived_status='RUNNING'
        then coalesce(o.started_at_utc,v_now) else o.started_at_utc end,
      completed_at_utc=case when d.derived_status='COMPLETE'
        then coalesce(o.completed_at_utc,v_now)
        when d.derived_status not in('CANCELLED','SUPERSEDED') then null
        else o.completed_at_utc end,
      failed_at_utc=case when d.derived_status in('FAILED','DEAD_LETTER')
        then coalesce(o.failed_at_utc,v_now)
        when d.derived_status not in('CANCELLED','SUPERSEDED') then null
        else o.failed_at_utc end,
      updated_at_utc=v_now,
      change_seq=case when
        o.status is distinct from d.derived_status
        or o.phase is distinct from case when d.missing_replacement_units>0
          then 'BLOCKED' else coalesce(d.current_phase,o.phase) end
        or o.total_units is distinct from d.total_units
        or o.completed_units is distinct from d.complete_units
        or o.failed_units is distinct from d.failed_units
        or o.requires_user_action is distinct from d.requires_action
        or exists(select 1 from ancestor_bumps ab where ab.id=o.id)
        or cr.descendant_change_seq>o.change_seq
        then nextval('public.invoice_operation_change_seq')
        else o.change_seq end
    from derived d
    join child_results cr on cr.operation_id=d.operation_id
    join result_aggregates ra on ra.operation_id=d.operation_id
    where o.id=d.operation_id
    returning o.id,o.status,o.phase,o.total_units,o.completed_units,
      o.failed_units,
      case when coalesce(o.progress_json->>'blocked_required_count','')
          ~'^[0-9]{1,9}$'
        then(o.progress_json->>'blocked_required_count')::integer else 0 end
        blocked_required_count,
      o.requires_user_action,o.change_seq
  )
  select u.id,u.status,u.phase,u.total_units,u.completed_units,u.failed_units,
    u.blocked_required_count,u.requires_user_action,u.change_seq
  from updated u
  order by u.id;
end;
$function$;

revoke all on function private._invoice_operation_rollup_batch(
  uuid[],timestamptz,boolean) from public,anon,authenticated;
grant execute on function private._invoice_operation_rollup_batch(
  uuid[],timestamptz,boolean) to service_role;
