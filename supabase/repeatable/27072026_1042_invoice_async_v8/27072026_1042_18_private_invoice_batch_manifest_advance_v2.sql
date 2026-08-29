create or replace function private._invoice_batch_manifest_advance_v2(
  p_claims jsonb,
  p_action text,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_action text := upper(btrim(coalesce(p_action,'')));
  v_chunk_type text;
  v_result jsonb := '[]'::jsonb;
  v_expander record;
  v_page jsonb;
  v_page_rows jsonb;
  v_has_more boolean;
  v_next_cursor jsonb;
  v_row_count integer;
  v_manifest_generation integer;
  v_release_ids uuid[];
  v_remaining integer;
  v_requests jsonb;
  v_ensure jsonb;
begin
  if v_action not in ('GENERATE','ISSUE') then
    raise exception using
      errcode='22023',
      message='INVOICE_BATCH_MANIFEST_ACTION_INVALID';
  end if;

  if jsonb_typeof(coalesce(p_claims,'null'::jsonb)) is distinct from 'array'
     or jsonb_array_length(p_claims) < 1
     or jsonb_array_length(p_claims) > 100 then
    raise exception using
      errcode='22023',
      message='p_claims must contain 1..100 claims';
  end if;

  v_chunk_type := case
    when v_action='GENERATE' then 'GENERATION_GROUP'
    else 'ISSUE_INVOICE'
  end;

  for v_expander in
    select
      c.*,
      o.actor_user_id,
      o.control_version root_control_version,
      o.manifest_generation root_manifest_generation,
      o.input_json root_input_json
    from jsonb_array_elements(p_claims) claim
    join public.invoice_operation_chunks c
      on c.id = case
        when pg_input_is_valid(coalesce(claim->>'chunk_id',''),'uuid')
          then (claim->>'chunk_id')::uuid
      end
    join public.invoice_operations o on o.id=c.operation_id
    where c.chunk_type=v_chunk_type
      and c.phase in ('BUILD_MANIFEST','RELEASE_MANIFEST')
      and coalesce(c.payload_json->>'is_selection_expander','false')
        in ('true','t','1','yes','on')
    order by c.id
  loop
    v_manifest_generation := greatest(
      1,
      coalesce(v_expander.root_manifest_generation,0),
      case
        when coalesce(v_expander.payload_json->>'manifest_generation','')
          ~ '^[1-9][0-9]{0,8}$'
          then (v_expander.payload_json->>'manifest_generation')::integer
        else 1
      end
    );

    if v_expander.phase='BUILD_MANIFEST' then
      begin
        v_page := case
          when v_action='GENERATE' then
            private._invoice_batch_generate_candidate_rows_v2(
              (case
                when jsonb_typeof(v_expander.payload_json->'query')='object'
                  then v_expander.payload_json->'query'
                else '{}'::jsonb
              end)
              || jsonb_build_object(
                'contract_version','INVOICE_BATCH_QUERY_V2',
                'action',v_action,
                'mode','EXPAND_SELECTION',
                'page_size',250,
                'cursor',case
                  when jsonb_typeof(v_expander.payload_json->'cursor')='object'
                    then v_expander.payload_json->'cursor'
                  else '{}'::jsonb
                end,
                'selection',coalesce(
                  v_expander.payload_json#>'{selection_contract,selection}',
                  v_expander.payload_json#>'{query,selection}'
                )
              ),
              v_now
            )
          else
            private._invoice_batch_issue_candidate_rows_v2(
              (case
                when jsonb_typeof(v_expander.payload_json->'query')='object'
                  then v_expander.payload_json->'query'
                else '{}'::jsonb
              end)
              || jsonb_build_object(
                'contract_version','INVOICE_BATCH_QUERY_V2',
                'action',v_action,
                'mode','EXPAND_SELECTION',
                'page_size',250,
                'cursor',case
                  when jsonb_typeof(v_expander.payload_json->'cursor')='object'
                    then v_expander.payload_json->'cursor'
                  else '{}'::jsonb
                end,
                'selection',coalesce(
                  v_expander.payload_json#>'{selection_contract,selection}',
                  v_expander.payload_json#>'{query,selection}'
                )
              ),
              v_now
            )
        end;
      exception
        when serialization_failure then
          update public.invoice_operation_chunks c
          set
            status='SUPERSEDED',
            phase='SOURCE_CHANGED',
            manifest_committed=true,
            result_visible=(
              coalesce(c.payload_json->>'manifest_outcome','') <> 'EXCLUDED'
            ),
            result_json=coalesce(c.result_json,'{}'::jsonb)
              || jsonb_build_object(
                'result_category','CHANGED',
                'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
              ),
            completed_at_utc=v_now,
            updated_at_utc=v_now
          where c.operation_id=v_expander.operation_id
            and c.is_manifest_member
            and c.manifest_generation=v_manifest_generation
            and not c.manifest_committed;

          update public.invoice_operation_chunks c
          set
            status='BLOCKED',
            phase='SOURCE_CHANGED',
            error_json=jsonb_build_object(
              'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
            ),
            completed_at_utc=v_now,
            lease_owner=null,
            lease_token=null,
            lease_expires_at_utc=null,
            updated_at_utc=v_now
          where c.id=v_expander.id;

          update public.invoice_operations o
          set
            status='BLOCKED',
            phase='SOURCE_CHANGED',
            manifest_committed=false,
            release_complete=false,
            requires_user_action=true,
            error_json=jsonb_build_object(
              'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
            ),
            progress_json=coalesce(o.progress_json,'{}'::jsonb)
              || jsonb_build_object(
                'selection_expansion_pending',false,
                'manifest_committed',false,
                'manifest_status','SUPERSEDED',
                'release_pending_total',0,
                'release_complete',false,
                'superseded_manifest_generation',
                  v_manifest_generation,
                'status_message','Candidate data changed; reload the selection'
              ),
            updated_at_utc=v_now,
            change_seq=nextval('public.invoice_operation_change_seq')
          where o.id=v_expander.operation_id;

          v_result := v_result || jsonb_build_array(jsonb_build_object(
            'chunk_id',v_expander.id,
            'status','BLOCKED',
            'phase','SOURCE_CHANGED',
            'error',jsonb_build_object(
              'code','BATCH_SNAPSHOT_CHANGED_DURING_EXPANSION'
            )
          ));
          continue;
      end;

      v_page_rows := case
        when jsonb_typeof(v_page->'rows')='array' then v_page->'rows'
        else '[]'::jsonb
      end;
      v_row_count := jsonb_array_length(v_page_rows);
      v_has_more := coalesce(
        (v_page#>>'{page,has_more}')::boolean,
        false
      );
      v_next_cursor := case
        when jsonb_typeof(v_page#>'{page,next_cursor_values}')='object'
          then v_page#>'{page,next_cursor_values}'
        else '{}'::jsonb
      end;

      insert into public.invoice_operation_chunks(
        operation_id,
        chunk_type,
        phase,
        work_key,
        sequence_no,
        level_no,
        entity_type,
        entity_id,
        status,
        priority,
        run_after_utc,
        payload_json,
        progress_json,
        result_json,
        operation_control_version,
        manifest_generation,
        is_manifest_member,
        manifest_committed,
        result_visible,
        created_at_utc,
        updated_at_utc
      )
      select
        v_expander.operation_id,
        v_chunk_type,
        'AWAITING_MANIFEST_COMMIT',
        private._invoice_batch_hash_v2(jsonb_build_object(
          'root_operation_id',v_expander.operation_id,
          'manifest_generation',v_manifest_generation,
          'selection_key',row_json->>'selection_key',
          'source_revision',coalesce(
            row_json->>'source_revision',
            row_json->>'document_revision'
          ),
          'outcome',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'SELECTED'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end
        )),
        coalesce(
          case
            when coalesce(v_expander.payload_json->>'scanned','')
              ~ '^[0-9]{1,9}$'
              then (v_expander.payload_json->>'scanned')::integer
            else 0
          end,
          0
        ) + row_item.ordinality::integer,
        0,
        'OPERATION',
        v_expander.operation_id,
        'WAITING',
        greatest(
          coalesce(v_expander.priority,case when v_action='ISSUE' then 850 else 600 end),
          case when v_action='ISSUE' then 850 else 600 end
        ),
        v_now,
        (
          case
            when v_action='GENERATE'
             and jsonb_typeof(row_json->'command_payload')='object'
              then row_json->'command_payload'
            else '{}'::jsonb
          end
        )
        || jsonb_build_object(
          'contract_version','INVOICE_BATCH_MANIFEST_CARRIER_V2',
          'selection_key',row_json->>'selection_key',
          'group_key',row_json->>'group_key',
          'manifest_generation',v_manifest_generation,
          'is_manifest_member',true,
          'manifest_outcome',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'SELECTED'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end,
          'result_category',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'IN_PROGRESS'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end,
          'row_kind',row_json->>'row_kind',
          'source_revision',coalesce(
            row_json->>'source_revision',
            row_json->>'document_revision'
          ),
          'expected_revision',coalesce(
            row_json->>'document_revision',
            row_json->>'source_revision'
          ),
          'invoice_id',row_json->>'invoice_id',
          'invoice_number',row_json->>'invoice_number',
          'client_id',row_json->>'client_id',
          'client_name',row_json->>'client_name',
          'candidate_display',row_json->>'candidate_display',
          'week_ending_display',row_json->>'week_ending_display',
          'currency',coalesce(row_json->>'currency','GBP'),
          'total_ex_vat',row_json->'total_ex_vat',
          'total_inc_vat',row_json->'total_inc_vat',
          'action_blocker_codes',coalesce(
            row_json->'action_blocker_codes',
            row_json->'issue_blocker_codes',
            '[]'::jsonb
          ),
          'delivery_blocker_codes',coalesce(
            row_json->'delivery_blocker_codes',
            '[]'::jsonb
          ),
          'blocked_for_sending',coalesce(
            row_json->'blocked_for_sending',
            'false'::jsonb
          ),
          'allow_early',coalesce(
            v_expander.payload_json#>'{query,filters,allow_early}',
            'false'::jsonb
          ),
          'deliver',coalesce(
            v_expander.payload_json->'deliver',
            'false'::jsonb
          ),
          'command_token',v_expander.root_input_json->>'command_token',
          'delivery_request_token',
            v_expander.payload_json->>'delivery_request_token',
          'delivery_intent',coalesce(
            v_expander.payload_json->'delivery_intent',
            '{}'::jsonb
          ),
          'parent_expander_chunk_id',v_expander.id
        ),
        jsonb_build_object(
          'contract_version','INVOICE_BATCH_PROGRESS_V2',
          'status_message','Waiting for manifest commit'
        ),
        jsonb_build_object(
          'result_category',case
            when coalesce((row_json->>'selectable')::boolean,false)
             and coalesce((row_json->>'selected')::boolean,false)
              then 'IN_PROGRESS'
            when coalesce((row_json->>'selectable')::boolean,false)
              then 'EXCLUDED'
            when coalesce(row_json->>'row_status','')='IN_PROGRESS'
              then 'ALREADY_ACTIVE'
            else 'BLOCKED'
          end,
          'badge_codes',coalesce(
            row_json->'action_blocker_codes',
            row_json->'issue_blocker_codes',
            '[]'::jsonb
          )
        ),
        v_expander.root_control_version,
        v_manifest_generation,
        true,
        false,
        false,
        v_now,
        v_now
      from jsonb_array_elements(v_page_rows)
        with ordinality row_item(row_json,ordinality)
      where nullif(row_json->>'selection_key','') is not null
      on conflict do nothing;

      update public.invoice_operation_chunks c
      set
        status='QUEUED',
        phase=case
          when v_has_more then 'BUILD_MANIFEST'
          else 'RELEASE_MANIFEST'
        end,
        run_after_utc=v_now,
        payload_json=coalesce(c.payload_json,'{}'::jsonb)
          || jsonb_build_object(
            'cursor',case when v_has_more then v_next_cursor else '{}'::jsonb end,
            'scanned',coalesce(
              case
                when coalesce(c.payload_json->>'scanned','') ~ '^[0-9]{1,9}$'
                  then (c.payload_json->>'scanned')::integer
                else 0
              end,
              0
            ) + v_row_count,
            'last_candidate_page',v_page->'page',
            'completed',not v_has_more
          ),
        progress_json=jsonb_build_object(
          'contract_version','INVOICE_BATCH_PROGRESS_V2',
          'status_message',case
            when v_has_more then 'Building selection manifest'
            else 'Manifest committed; releasing work'
          end
        ),
        lease_owner=null,
        lease_token=null,
        lease_expires_at_utc=null,
        updated_at_utc=v_now
      where c.id=v_expander.id;

      if not v_has_more then
        update public.invoice_operation_chunks member
        set
          phase='AWAITING_RELEASE',
          updated_at_utc=v_now
        where member.operation_id=v_expander.operation_id
          and member.is_manifest_member
          and member.manifest_generation=v_manifest_generation
          and not member.manifest_committed
          and member.phase='AWAITING_MANIFEST_COMMIT';
      end if;

      update public.invoice_operations o
      set
        status='QUEUED',
        phase=case
          when v_has_more then 'BUILD_MANIFEST'
          else 'RELEASE_MANIFEST'
        end,
        manifest_committed=not v_has_more,
        release_complete=false,
        progress_json=coalesce(o.progress_json,'{}'::jsonb)
          || jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message',case
              when v_has_more then 'Building selection manifest'
              else 'Manifest committed; releasing work'
            end,
            'selection_expansion_pending',v_has_more,
            'manifest_committed',not v_has_more,
            'manifest_generation',v_manifest_generation,
            'manifest_status',case
              when v_has_more then 'BUILDING'
              else 'COMMITTED'
            end,
            'expected_scan_total',coalesce(
              case
                when coalesce(
                  o.progress_json->>'expected_scan_total',
                  ''
                ) ~ '^[0-9]+$'
                  then (
                    o.progress_json->>'expected_scan_total'
                  )::integer
              end,
              (
                select count(*)::integer
                from public.invoice_operation_chunks member
                where member.operation_id=o.id
                  and member.is_manifest_member
                  and member.manifest_generation=v_manifest_generation
              )
            ),
            'scanned_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
            ),
            'selected_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='SELECTED'
            ),
            'excluded_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='EXCLUDED'
            ),
            'blocked_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='BLOCKED'
            ),
            'already_active_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'
                  ='ALREADY_ACTIVE'
            ),
            'changed_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='CHANGED'
            ),
            'missing_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='MISSING'
            ),
            'release_pending_total',case
              when v_has_more then 0
              else (
                select count(*)::integer
                from public.invoice_operation_chunks member
                where member.operation_id=o.id
                  and member.is_manifest_member
                  and member.manifest_generation=v_manifest_generation
                  and member.payload_json->>'manifest_outcome'='SELECTED'
                  and not member.manifest_committed
              )
            end,
            'released_total',0,
            'release_conflict_total',0,
            'release_blocked_total',0,
            'release_complete',false,
            'committed_at_utc',case
              when v_has_more then o.progress_json->'committed_at_utc'
              else to_jsonb(v_now)
            end,
            'superseded_manifest_generation',
              o.progress_json->'superseded_manifest_generation',
            'expanded_total',(
              select count(*)::integer
              from public.invoice_operation_chunks member
              where member.operation_id=o.id
                and member.is_manifest_member
                and member.manifest_generation=v_manifest_generation
                and member.payload_json->>'manifest_outcome'='SELECTED'
            )
          ),
        chunk_count=(
          select count(*)::integer
          from public.invoice_operation_chunks member
          where member.operation_id=o.id
        ),
        updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
      where o.id=v_expander.operation_id;

      v_result := v_result || jsonb_build_array(jsonb_build_object(
        'chunk_id',v_expander.id,
        'status','QUEUED',
        'phase',case
          when v_has_more then 'BUILD_MANIFEST'
          else 'RELEASE_MANIFEST'
        end,
        'result',jsonb_build_object(
          'manifest_committed',not v_has_more,
          'page',v_page->'page'
        )
      ));
      continue;
    end if;

    select coalesce(array_agg(release_member.id order by
      release_member.selection_key,
      release_member.id
    ),array[]::uuid[])
    into v_release_ids
    from (
      select c.id,c.selection_key
      from public.invoice_operation_chunks c
      where c.operation_id=v_expander.operation_id
        and c.is_manifest_member
        and c.manifest_generation=v_manifest_generation
        and not c.manifest_committed
      order by c.selection_key,c.id
      for update skip locked
      limit 250
    ) release_member;

    if cardinality(v_release_ids) > 0 then
      update public.invoice_operation_chunks c
      set
        manifest_committed=true,
        status=case c.payload_json->>'manifest_outcome'
          when 'EXCLUDED' then 'COMPLETE'
          when 'BLOCKED' then 'BLOCKED'
          when 'ALREADY_ACTIVE' then 'COMPLETE'
          else c.status
        end,
        phase=case c.payload_json->>'manifest_outcome'
          when 'EXCLUDED' then 'EXCLUDED'
          when 'BLOCKED' then 'BLOCKED'
          when 'ALREADY_ACTIVE' then 'ALREADY_ACTIVE'
          else c.phase
        end,
        result_visible=(
          c.payload_json->>'manifest_outcome' <> 'EXCLUDED'
        ),
        completed_at_utc=case
          when c.payload_json->>'manifest_outcome'
            in ('EXCLUDED','BLOCKED','ALREADY_ACTIVE')
            then v_now
          else c.completed_at_utc
        end,
        updated_at_utc=v_now
      where c.id=any(v_release_ids)
        and c.payload_json->>'manifest_outcome' <> 'SELECTED';

      if v_action='GENERATE' then
        update public.invoice_operation_chunks c
        set
          manifest_committed=true,
          entity_type='CLIENT',
          entity_id=case
            when pg_input_is_valid(coalesce(c.payload_json->>'client_id',''),'uuid')
              then (c.payload_json->>'client_id')::uuid
          end,
          status='QUEUED',
          phase='VALIDATE_SOURCES',
          result_visible=true,
          run_after_utc=v_now,
          progress_json=jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message','Queued for invoice generation'
          ),
          updated_at_utc=v_now
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and coalesce(c.payload_json->>'row_kind','CREATE_INVOICE')
            ='CREATE_INVOICE';

        select coalesce(jsonb_agg(jsonb_build_object(
          'request_key',c.selection_key,
          'invoice_id',c.payload_json->>'invoice_id',
          'document_revision',c.payload_json->>'expected_revision',
          'purpose','DRAFT_PREVIEW',
          'priority',greatest(c.priority,550),
          'parent_operation_id',c.operation_id,
          'actor_user_id',v_expander.actor_user_id,
          'template_version','invoice-professional-v2'
        ) order by c.selection_key),'[]'::jsonb)
        into v_requests
        from public.invoice_operation_chunks c
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and coalesce(c.payload_json->>'row_kind','CREATE_INVOICE')
            <> 'CREATE_INVOICE';

        if jsonb_array_length(v_requests) > 0 then
          v_ensure := private._invoice_document_operation_ensure_batch(
            v_requests,
            v_now
          );

          update public.invoice_operation_chunks c
          set
            manifest_committed=true,
            status=case
              when result_row->>'status'='READY' then 'COMPLETE'
              when coalesce((result_row->>'ok')::boolean,false) then 'WAITING'
              when result_row->>'code' in ('SOURCE_CHANGED','INVOICE_NOT_FOUND')
                then 'SUPERSEDED'
              else 'BLOCKED'
            end,
            phase=case
              when result_row->>'status'='READY' then 'COMPLETE'
              when coalesce((result_row->>'ok')::boolean,false)
                then 'WAITING_DOCUMENT'
              when result_row->>'code'='SOURCE_CHANGED'
                then 'SOURCE_CHANGED'
              when result_row->>'code'='INVOICE_NOT_FOUND'
                then 'SOURCE_MISSING'
              else 'BLOCKED'
            end,
            result_visible=true,
            document_version_id=case
              when pg_input_is_valid(
                coalesce(result_row->>'document_version_id',''),
                'uuid'
              ) then (result_row->>'document_version_id')::uuid
              else c.document_version_id
            end,
            result_json=coalesce(c.result_json,'{}'::jsonb)
              || jsonb_build_object(
                'result_category',case
                  when result_row->>'status'='READY' then 'REGENERATED'
                  when coalesce((result_row->>'ok')::boolean,false)
                    then 'IN_PROGRESS'
                  when result_row->>'code'='SOURCE_CHANGED'
                    then 'CHANGED'
                  when result_row->>'code'='INVOICE_NOT_FOUND'
                    then 'MISSING'
                  else 'BLOCKED'
                end,
                'document_operation_id',result_row->>'operation_id',
                'document_version_id',result_row->>'document_version_id',
                'code',result_row->>'code'
              ),
            completed_at_utc=case
              when result_row->>'status'='READY'
                or not coalesce((result_row->>'ok')::boolean,false)
                then v_now
              else null
            end,
            updated_at_utc=v_now
          from jsonb_array_elements(
            coalesce(v_ensure->'results','[]'::jsonb)
          ) result_item(result_row)
          where c.id=any(v_release_ids)
            and c.selection_key=result_row->>'request_key';
        end if;
      else
        perform i.id
        from public.invoices i
        where i.id in (
          select (c.payload_json->>'invoice_id')::uuid
          from public.invoice_operation_chunks c
          where c.id=any(v_release_ids)
            and c.payload_json->>'manifest_outcome'='SELECTED'
            and pg_input_is_valid(
              coalesce(c.payload_json->>'invoice_id',''),
              'uuid'
            )
        )
        order by i.id
        for update;

        update public.invoice_operation_chunks c
        set
          manifest_committed=true,
          status='COMPLETE',
          phase='ALREADY_ACTIVE',
          result_visible=true,
          result_json=coalesce(c.result_json,'{}'::jsonb)
            || jsonb_build_object(
              'result_category','ALREADY_ACTIVE',
              'code','ALREADY_ACTIVE'
            ),
          completed_at_utc=v_now,
          updated_at_utc=v_now
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and exists (
            select 1
            from public.invoice_operation_chunks active
            join public.invoice_operations active_operation
              on active_operation.id=active.operation_id
            where active.chunk_type='ISSUE_INVOICE'
              and active.entity_type='INVOICE'
              and active.entity_id=(c.payload_json->>'invoice_id')::uuid
              and active.status in (
                'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
              )
              and active.id<>c.id
              and active.replaced_by_chunk_id is null
              and active_operation.status
                in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
          );

        update public.invoice_operation_chunks c
        set
          manifest_committed=true,
          entity_type='INVOICE',
          entity_id=(c.payload_json->>'invoice_id')::uuid,
          status='QUEUED',
          phase='VALIDATE',
          result_visible=true,
          run_after_utc=v_now,
          payload_json=c.payload_json || jsonb_build_object(
            'request_key',c.selection_key,
            'evaluation_date',(v_now at time zone 'Europe/London')::date,
            'frozen_issue_at_utc',v_now
          ),
          progress_json=jsonb_build_object(
            'contract_version','INVOICE_BATCH_PROGRESS_V2',
            'status_message','Queued for legal issue'
          ),
          updated_at_utc=v_now
        where c.id=any(v_release_ids)
          and c.payload_json->>'manifest_outcome'='SELECTED'
          and c.status='WAITING';
      end if;
    end if;

    select count(*)::integer
    into v_remaining
    from public.invoice_operation_chunks c
    where c.operation_id=v_expander.operation_id
      and c.is_manifest_member
      and c.manifest_generation=v_manifest_generation
      and not c.manifest_committed;

    update public.invoice_operation_chunks c
    set
      status=case when v_remaining=0 then 'COMPLETE' else 'QUEUED' end,
      phase=case when v_remaining=0 then 'RELEASE_COMPLETE' else 'RELEASE_MANIFEST' end,
      run_after_utc=case when v_remaining=0 then c.run_after_utc else v_now end,
      completed_at_utc=case when v_remaining=0 then v_now else null end,
      payload_json=coalesce(c.payload_json,'{}'::jsonb)
        || jsonb_build_object(
          'release_remaining',v_remaining,
          'release_complete',v_remaining=0
        ),
      progress_json=jsonb_build_object(
        'contract_version','INVOICE_BATCH_PROGRESS_V2',
        'status_message',case
          when v_remaining=0 then 'Manifest release complete'
          else 'Releasing manifest work'
        end
      ),
      lease_owner=null,
      lease_token=null,
      lease_expires_at_utc=null,
      updated_at_utc=v_now
    where c.id=v_expander.id;

    update public.invoice_operations o
    set
      status='QUEUED',
      phase=case when v_remaining=0 then 'BUSINESS_WORK' else 'RELEASE_MANIFEST' end,
      manifest_committed=true,
      release_complete=v_remaining=0,
      progress_json=coalesce(o.progress_json,'{}'::jsonb)
        || jsonb_build_object(
          'contract_version','INVOICE_BATCH_PROGRESS_V2',
          'selection_expansion_pending',false,
          'manifest_committed',true,
          'manifest_generation',v_manifest_generation,
          'manifest_status',case
            when v_remaining=0 then 'RELEASE_COMPLETE'
            else 'RELEASE_MANIFEST'
          end,
          'release_pending_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and not member.manifest_committed
          ),
          'released_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and member.manifest_committed
              and coalesce(member.result_category,'') not in(
                'ALREADY_ACTIVE','BLOCKED','CHANGED','MISSING','FAILED'
              )
          ),
          'release_conflict_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and member.result_category='ALREADY_ACTIVE'
          ),
          'release_blocked_total',(
            select count(*)::integer
            from public.invoice_operation_chunks member
            where member.operation_id=o.id
              and member.is_manifest_member
              and member.manifest_generation=v_manifest_generation
              and member.payload_json->>'manifest_outcome'='SELECTED'
              and member.result_category in(
                'BLOCKED','CHANGED','MISSING','FAILED'
              )
          ),
          'release_complete',v_remaining=0,
          'status_message',case
            when v_remaining=0 then 'Processing selected work'
            else 'Releasing manifest work'
          end
        ),
      updated_at_utc=v_now,
      change_seq=nextval('public.invoice_operation_change_seq')
    where o.id=v_expander.operation_id;

    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'chunk_id',v_expander.id,
      'status',case when v_remaining=0 then 'COMPLETE' else 'QUEUED' end,
      'phase',case
        when v_remaining=0 then 'RELEASE_COMPLETE'
        else 'RELEASE_MANIFEST'
      end,
      'result',jsonb_build_object(
        'released_count',cardinality(v_release_ids),
        'remaining_count',v_remaining,
        'release_complete',v_remaining=0
      )
    ));
  end loop;

  return v_result;
end;
$function$;

alter function private._invoice_batch_manifest_advance_v2(
  jsonb,text,timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_manifest_advance_v2(
  jsonb,text,timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_manifest_advance_v2(
  jsonb,text,timestamptz
) to service_role;
