-- CloudTMS Invoice Async V8/V2 operation rollup authority.
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
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
begin
  return query
  with recursive
  core_rows as materialized (
    select *
    from private._invoice_operation_rollup_core_v8(
      p_operation_ids,
      v_now,
      p_propagate_ancestors
    )
  ),
  roots as materialized (
    select distinct o.id operation_id
    from core_rows core
    join public.invoice_operations o on o.id=core.operation_id
    where o.operation_type in ('GENERATE_INVOICES','ISSUE_INVOICES')
      and o.entity_type='INVOICE_BATCH'
      and o.input_json->>'contract_version'
        ='INVOICE_BATCH_SELECTION_ROOT_V2'
  ),
  descendants(descendant_root_operation_id,operation_id,depth,path) as (
    select r.operation_id,r.operation_id,0,array[r.operation_id]::uuid[]
    from roots r
    union all
    select
      d.descendant_root_operation_id,
      child.id,
      d.depth+1,
      d.path||child.id
    from descendants d
    join public.invoice_operations child
      on child.parent_operation_id=d.operation_id
    where d.depth<16
      and not child.id=any(d.path)
  ),
  refreshed_documents as materialized (
    update public.invoice_operation_chunks carrier
    set
      status=case
        when child.status='COMPLETE' then 'COMPLETE'
        when child.status in ('BLOCKED','FAILED','DEAD_LETTER')
          then child.status
        else carrier.status
      end,
      phase=case
        when child.status='COMPLETE' then 'COMPLETE'
        when child.status in ('BLOCKED','FAILED','DEAD_LETTER')
          then child.status
        else carrier.phase
      end,
      document_version_id=coalesce(
        ready_version.id,
        carrier.document_version_id
      ),
      result_json=coalesce(carrier.result_json,'{}'::jsonb)
        || jsonb_build_object(
          'result_category',case
            when child.status='COMPLETE' then 'REGENERATED'
            when child.status='BLOCKED' then 'BLOCKED'
            when child.status in ('FAILED','DEAD_LETTER') then 'FAILED'
            else 'IN_PROGRESS'
          end,
          'document_operation_id',child.id,
          'document_version_id',coalesce(
            ready_version.id,
            carrier.document_version_id
          )
        ),
      completed_at_utc=case
        when child.status in ('COMPLETE','BLOCKED','FAILED','DEAD_LETTER')
          then coalesce(carrier.completed_at_utc,v_now)
        else carrier.completed_at_utc
      end,
      updated_at_utc=v_now
    from public.invoice_operations child
    left join lateral (
      select version.id
      from public.invoice_document_versions version
      where version.operation_id=child.id
        and version.entity_type='INVOICE'
        and version.purpose='DRAFT_PREVIEW'
        and version.status='READY'
      order by version.ready_at_utc desc nulls last,
        version.created_at_utc desc,
        version.id desc
      limit 1
    ) ready_version on true
    where carrier.operation_id in (select root.operation_id from roots root)
      and carrier.is_manifest_member
      and carrier.phase='WAITING_DOCUMENT'
      and carrier.status='WAITING'
      and pg_input_is_valid(
        coalesce(carrier.result_json->>'document_operation_id',''),
        'uuid'
      )
      and child.id=(carrier.result_json->>'document_operation_id')::uuid
      and child.status in ('COMPLETE','BLOCKED','FAILED','DEAD_LETTER')
    returning carrier.id
  ),
  normalised_carriers as materialized (
    update public.invoice_operation_chunks carrier
    set
      result_json=coalesce(carrier.result_json,'{}'::jsonb)
        || jsonb_build_object(
          'result_category',case
            when carrier.status='COMPLETE'
             and carrier.result_category in (
               'EXCLUDED','ALREADY_ACTIVE','REGENERATED',
               'ISSUED','ISSUED_SEND_BLOCKED'
             ) then carrier.result_category
            when carrier.status='COMPLETE'
             and carrier.chunk_type='GENERATION_GROUP'
             and coalesce(
               carrier.payload_json->>'row_kind',
               'CREATE_INVOICE'
             )='CREATE_INVOICE' then 'GENERATED'
            when carrier.status='COMPLETE'
             and carrier.chunk_type='GENERATION_GROUP'
              then 'REGENERATED'
            when carrier.status='COMPLETE'
             and carrier.chunk_type='ISSUE_INVOICE'
             and coalesce(
               (carrier.payload_json->>'blocked_for_sending')::boolean,
               false
             ) then 'ISSUED_SEND_BLOCKED'
            when carrier.status='COMPLETE'
             and carrier.chunk_type='ISSUE_INVOICE' then 'ISSUED'
            when carrier.status='BLOCKED' then 'BLOCKED'
            when carrier.status='SUPERSEDED' then 'CHANGED'
            when carrier.status in ('FAILED','DEAD_LETTER') then 'FAILED'
            else 'IN_PROGRESS'
          end
        ),
      updated_at_utc=v_now
    where carrier.operation_id in (select root.operation_id from roots root)
      and carrier.is_manifest_member
      and carrier.manifest_committed
      and carrier.result_visible
      and carrier.result_category is distinct from case
        when carrier.status='COMPLETE'
         and carrier.result_category in (
           'EXCLUDED','ALREADY_ACTIVE','REGENERATED',
           'ISSUED','ISSUED_SEND_BLOCKED'
         ) then carrier.result_category
        when carrier.status='COMPLETE'
         and carrier.chunk_type='GENERATION_GROUP'
         and coalesce(
           carrier.payload_json->>'row_kind',
           'CREATE_INVOICE'
         )='CREATE_INVOICE' then 'GENERATED'
        when carrier.status='COMPLETE'
         and carrier.chunk_type='GENERATION_GROUP' then 'REGENERATED'
        when carrier.status='COMPLETE'
         and carrier.chunk_type='ISSUE_INVOICE'
         and coalesce(
           (carrier.payload_json->>'blocked_for_sending')::boolean,
           false
         ) then 'ISSUED_SEND_BLOCKED'
        when carrier.status='COMPLETE'
         and carrier.chunk_type='ISSUE_INVOICE' then 'ISSUED'
        when carrier.status='BLOCKED' then 'BLOCKED'
        when carrier.status='SUPERSEDED' then 'CHANGED'
        when carrier.status in ('FAILED','DEAD_LETTER') then 'FAILED'
        else 'IN_PROGRESS'
      end
    returning carrier.id
  ),
  carriers as materialized (
    select
      o.id batch_root_id,
      o.operation_type,
      o.manifest_committed root_manifest_committed,
      o.release_complete,
      o.progress_json root_progress_json,
      c.id,
      c.status,
      c.phase,
      c.payload_json,
      c.result_json,
      c.result_category,
      c.manifest_committed,
      c.result_visible
    from public.invoice_operations o
    join roots r on r.operation_id=o.id
    left join public.invoice_operation_chunks c
      on c.operation_id=o.id
     and c.is_manifest_member
     and c.manifest_generation=o.manifest_generation
     and c.replaced_by_chunk_id is null
    where (select count(*) from refreshed_documents)>=0
      and (select count(*) from normalised_carriers)>=0
  ),
  carrier_counts as materialized (
    select
      c.batch_root_id,
      min(c.operation_type) operation_type,
      bool_or(c.root_manifest_committed) manifest_committed,
      bool_or(c.release_complete) release_complete,
      coalesce(max(case
        when c.operation_type='GENERATE_INVOICES'
         and coalesce(
           c.root_progress_json->>'candidate_total',
           ''
         )~'^[0-9]+$'
          then (c.root_progress_json->>'candidate_total')::integer
        when c.operation_type='ISSUE_INVOICES'
         and coalesce(
           c.root_progress_json->>'invoice_total',
           ''
         )~'^[0-9]+$'
          then (c.root_progress_json->>'invoice_total')::integer
      end),0) candidate_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
      )::integer selected_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
      )::integer expanded_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
          and c.manifest_committed
          and c.status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT'
          )
      )::integer queued_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
          and not c.manifest_committed
      )::integer release_pending_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
          and c.manifest_committed
          and coalesce(c.result_category,'') not in(
            'ALREADY_ACTIVE','BLOCKED','CHANGED','MISSING','FAILED'
          )
      )::integer released_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
          and c.result_category='ALREADY_ACTIVE'
      )::integer release_conflict_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
          and c.result_category in(
            'BLOCKED','CHANGED','MISSING','FAILED'
          )
      )::integer release_blocked_total,
      count(c.id) filter (
        where c.result_category='GENERATED'
      )::integer generated_total,
      count(c.id) filter (
        where c.result_category='REGENERATED'
      )::integer regenerated_total,
      count(c.id) filter (
        where c.result_category in ('ISSUED','ISSUED_SEND_BLOCKED')
      )::integer issued_total,
      count(c.id) filter (
        where c.result_category='ISSUED_SEND_BLOCKED'
      )::integer issued_send_blocked_total,
      count(c.id) filter (
        where c.result_category='ALREADY_ACTIVE'
      )::integer already_active_total,
      count(c.id) filter (
        where c.result_category='BLOCKED'
      )::integer blocked_total,
      count(c.id) filter (
        where c.result_category='CHANGED'
      )::integer changed_total,
      count(c.id) filter (
        where c.result_category='FAILED'
      )::integer failed_total,
      count(c.id) filter (
        where c.result_category='MISSING'
           or c.payload_json->>'manifest_outcome'='MISSING'
      )::integer missing_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='EXCLUDED'
      )::integer excluded_total,
      count(c.id) filter (
        where c.payload_json->>'manifest_outcome'='SELECTED'
          and c.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      )::integer in_progress_total
    from carriers c
    group by c.batch_root_id
  ),
  delivery_counts as materialized (
    select
      d.descendant_root_operation_id as batch_root_id,
      count(c.id) filter (
        where c.chunk_type='DELIVERY_PREPARE'
          and c.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      )::integer delivery_pending_total,
      count(c.id) filter (
        where c.chunk_type='DELIVERY_PREPARE'
          and c.status='COMPLETE'
      )::integer delivery_complete_total,
      count(c.id) filter (
        where c.chunk_type='DELIVERY_PREPARE'
          and c.status in ('BLOCKED','FAILED','DEAD_LETTER')
      )::integer delivery_blocked_total
    from descendants d
    left join public.invoice_operation_chunks c
      on c.operation_id=d.operation_id
     and c.replaced_by_chunk_id is null
    group by d.descendant_root_operation_id
  ),
  progress as materialized (
    select
      jsonb_build_object(
        'contract_version','INVOICE_BATCH_PROGRESS_V2',
        'manifest_generation',root.manifest_generation,
        'manifest_status',coalesce(
          nullif(root.progress_json->>'manifest_status',''),
          case
            when root.release_complete then 'RELEASE_COMPLETE'
            when root.manifest_committed then 'RELEASE_MANIFEST'
            else 'BUILDING'
          end
        ),
        'manifest_committed',counts.manifest_committed,
        'expected_scan_total',case
          when coalesce(
            root.progress_json->>'expected_scan_total',
            ''
          ) ~ '^[0-9]+$'
            then (root.progress_json->>'expected_scan_total')::integer
          else counts.candidate_total
        end,
        'scanned_total',case
          when coalesce(
            root.progress_json->>'scanned_total',
            ''
          ) ~ '^[0-9]+$'
            then (root.progress_json->>'scanned_total')::integer
          else counts.expanded_total
        end,
        'selection_expansion_pending',not counts.manifest_committed,
        'release_pending_total',case
          when coalesce(
            root.progress_json->>'release_pending_total',
            ''
          ) ~ '^[0-9]+$'
            then (root.progress_json->>'release_pending_total')::integer
          else counts.release_pending_total
        end,
        'released_total',case
          when coalesce(
            root.progress_json->>'released_total',
            ''
          ) ~ '^[0-9]+$'
            then (root.progress_json->>'released_total')::integer
          else counts.released_total
        end,
        'release_conflict_total',case
          when coalesce(
            root.progress_json->>'release_conflict_total',
            ''
          ) ~ '^[0-9]+$'
            then (root.progress_json->>'release_conflict_total')::integer
          else counts.release_conflict_total
        end,
        'release_blocked_total',case
          when coalesce(
            root.progress_json->>'release_blocked_total',
            ''
          ) ~ '^[0-9]+$'
            then (root.progress_json->>'release_blocked_total')::integer
          else counts.release_blocked_total
        end,
        'release_complete',counts.release_complete,
        'committed_at_utc',root.progress_json->'committed_at_utc',
        'superseded_manifest_generation',
          root.progress_json->'superseded_manifest_generation',
        'candidate_total',case
          when counts.operation_type='GENERATE_INVOICES'
            then counts.candidate_total
          else 0
        end,
        'invoice_total',case
          when counts.operation_type='ISSUE_INVOICES'
            then counts.candidate_total
          else 0
        end,
        'selected_total',counts.selected_total,
        'expanded_total',counts.expanded_total,
        'queued_total',counts.queued_total,
        'generated_total',counts.generated_total,
        'regenerated_total',counts.regenerated_total,
        'issued_total',counts.issued_total,
        'issued_send_blocked_total',counts.issued_send_blocked_total,
        'already_active_total',counts.already_active_total,
        'blocked_total',counts.blocked_total,
        'changed_total',counts.changed_total,
        'failed_total',counts.failed_total,
        'excluded_total',counts.excluded_total,
        'missing_total',counts.missing_total,
        'in_progress_total',counts.in_progress_total,
        'delivery_pending_total',coalesce(delivery.delivery_pending_total,0),
        'delivery_complete_total',coalesce(delivery.delivery_complete_total,0),
        'delivery_blocked_total',coalesce(delivery.delivery_blocked_total,0),
        'final',counts.manifest_committed
          and counts.release_complete
          and coalesce(
            case
              when coalesce(
                root.progress_json->>'release_pending_total',
                ''
              ) ~ '^[0-9]+$'
                then (
                  root.progress_json->>'release_pending_total'
                )::integer
              else 0
            end,
            0
          )=0
          and counts.in_progress_total=0
          and coalesce(delivery.delivery_pending_total,0)=0
      ) progress_json,
      counts.*
    from carrier_counts counts
    join public.invoice_operations root
      on root.id=counts.batch_root_id
    left join delivery_counts delivery
      on delivery.batch_root_id=counts.batch_root_id
  ),
  updated_roots as materialized (
    update public.invoice_operations root
    set
      status=case
        when root.status='CANCELLED' then root.status
        when root.status='BLOCKED'
         and root.error_json->>'code'
          ='BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
          then root.status
        when not p.manifest_committed
          or not p.release_complete then 'QUEUED'
        when coalesce(
          (p.progress_json->>'release_pending_total')::integer,
          0
        )>0 then 'QUEUED'
        when p.in_progress_total>0
          or coalesce((p.progress_json->>'delivery_pending_total')::integer,0)>0
          then 'RUNNING'
        else 'COMPLETE'
      end,
      phase=case
        when root.status='CANCELLED' then root.phase
        when root.status='BLOCKED'
         and root.error_json->>'code'
          ='BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
          then root.phase
        when not p.manifest_committed then 'BUILD_MANIFEST'
        when not p.release_complete then 'RELEASE_MANIFEST'
        when coalesce(
          (p.progress_json->>'release_pending_total')::integer,
          0
        )>0 then 'RELEASE_MANIFEST'
        when p.in_progress_total>0
          or coalesce((p.progress_json->>'delivery_pending_total')::integer,0)>0
          then 'BUSINESS_WORK'
        else 'TERMINAL'
      end,
      total_units=greatest(p.candidate_total,1),
      completed_units=greatest(
        p.candidate_total-p.in_progress_total,
        0
      ),
      failed_units=p.failed_total,
      progress_json=coalesce(root.progress_json,'{}'::jsonb)
        || p.progress_json,
      result_json=coalesce(root.result_json,'{}'::jsonb)
        || jsonb_build_object('batch_progress',p.progress_json),
      requires_user_action=(
        p.failed_total+p.blocked_total+p.changed_total>0
      ),
      completed_at_utc=case
        when p.manifest_committed
         and p.release_complete
         and coalesce(
           (p.progress_json->>'release_pending_total')::integer,
           0
         )=0
         and p.in_progress_total=0
         and coalesce(
           (p.progress_json->>'delivery_pending_total')::integer,
           0
         )=0 then coalesce(root.completed_at_utc,v_now)
        else null
      end,
      updated_at_utc=case
        when coalesce(root.progress_json,'{}'::jsonb)
          is distinct from (
            coalesce(root.progress_json,'{}'::jsonb)
            || p.progress_json
          ) then v_now
        else root.updated_at_utc
      end,
      change_seq=case
        when coalesce(root.progress_json,'{}'::jsonb)
          is distinct from (
            coalesce(root.progress_json,'{}'::jsonb)
            || p.progress_json
          )
          or root.status is distinct from case
            when root.status='CANCELLED' then root.status
            when root.status='BLOCKED'
             and root.error_json->>'code'
              ='BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
              then root.status
            when not p.manifest_committed
              or not p.release_complete then 'QUEUED'
            when coalesce(
              (p.progress_json->>'release_pending_total')::integer,
              0
            )>0 then 'QUEUED'
            when p.in_progress_total>0
              or coalesce(
                (p.progress_json->>'delivery_pending_total')::integer,
                0
              )>0 then 'RUNNING'
            else 'COMPLETE'
          end
          then nextval('public.invoice_operation_change_seq')
        else root.change_seq
      end
    from progress p
    where root.id=p.batch_root_id
    returning root.id
  )
  select
    operation.id,
    operation.status,
    operation.phase,
    operation.total_units,
    operation.completed_units,
    operation.failed_units,
    case
      when coalesce(
        operation.progress_json->>'blocked_required_count',
        ''
      ) ~ '^[0-9]{1,9}$'
        then (operation.progress_json->>'blocked_required_count')::integer
      else 0
    end,
    operation.requires_user_action,
    operation.change_seq
  from core_rows core
  join public.invoice_operations operation on operation.id=core.operation_id
  where (select count(*) from updated_roots)>=0
  order by operation.id;
end;
$function$;

alter function private._invoice_operation_rollup_batch(
  uuid[],timestamptz,boolean
) owner to postgres;
revoke all on function private._invoice_operation_rollup_batch(
  uuid[],timestamptz,boolean
) from public,anon,authenticated;
grant execute on function private._invoice_operation_rollup_batch(
  uuid[],timestamptz,boolean
) to service_role;
