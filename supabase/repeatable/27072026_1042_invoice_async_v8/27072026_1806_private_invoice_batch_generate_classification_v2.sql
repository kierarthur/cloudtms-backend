create or replace function private._invoice_batch_generate_classification_v2(
  p_allow_early boolean default false,
  p_scope_keys text[] default null,
  p_now_utc timestamptz default now()
) returns table(
  selection_key text,
  candidate_json jsonb
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_scope_keys text[];
begin
  if p_scope_keys is not null then
    if cardinality(p_scope_keys)>250 then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_LIMIT_EXCEEDED';
    end if;
    if exists(
      select 1
      from unnest(p_scope_keys) value
      where value is null
        or btrim(value)=''
        or length(btrim(value))>512
    ) then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_INVALID';
    end if;
    select coalesce(array_agg(value order by value),'{}'::text[])
    into v_scope_keys
    from(
      select distinct btrim(raw_value) value
      from unnest(p_scope_keys) raw_value
    ) deduplicated;
  end if;

  return query
  with
  anchor as materialized (
    select
      coalesce(p_now_utc,statement_timestamp()) evaluation_utc,
      (
        coalesce(p_now_utc,statement_timestamp())
        at time zone 'Europe/London'
      )::date today
  ),
  source_candidates as materialized (
    select distinct financial.timesheet_id
    from public.timesheets_financials financial
    join public.timesheets timesheet
      on timesheet.timesheet_id=financial.timesheet_id
     and timesheet.is_current
     and timesheet.revoked_at is null
    where financial.is_current
      and financial.client_id is not null
    order by financial.timesheet_id
  ),
  command as materialized (
    select jsonb_build_array(jsonb_build_object(
      'command_type','GENERATE_SELECTED',
      'source_ids',coalesce(
        jsonb_agg(source.timesheet_id order by source.timesheet_id),
        '[]'::jsonb
      ),
      'allow_early',coalesce(p_allow_early,false),
      'scope_keys',case
        when v_scope_keys is null then null
        else to_jsonb(v_scope_keys)
      end
    )) commands
    from source_candidates source
  ),
  groups as materialized (
    select resolved.*
    from command
    cross join anchor
    cross join lateral private._invoice_generation_resolve_command_groups(
      command.commands,
      null,
      anchor.evaluation_utc
    ) resolved
    where v_scope_keys is null
       or resolved.group_key=any(v_scope_keys)
  ),
  group_sources as materialized (
    select
      group_row.*,
      member.value member,
      case
        when coalesce(member.value->>'source_id','')~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
          then (member.value->>'source_id')::uuid
      end source_id,
      case
        when coalesce(
          member.value->>'related_timesheet_id',
          member.value->>'source_id',
          ''
        )~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
          then coalesce(
            member.value->>'related_timesheet_id',
            member.value->>'source_id'
          )::uuid
      end timesheet_id,
      nullif(member.value->>'segment_id','') segment_id
    from groups group_row
    cross join lateral jsonb_array_elements(
      group_row.canonical_source_members
    ) member(value)
  ),
  vat_policy as materialized (
    select policy.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',source.member->>'source_member_key',
        'source_type',source.member->>'source_type',
        'source_id',source.source_id,
        'timesheet_id',source.member->>'related_timesheet_id',
        'segment_id',source.segment_id,
        'effective_date',source.effective_settings_date
      ) order by
        source.group_key,
        source.member->>'source_member_key'
      )
      from group_sources source
    ),'[]'::jsonb)) policy
  ),
  reference_policy as materialized (
    select policy.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',source.member->>'source_member_key',
        'source_type',source.member->>'source_type',
        'source_id',source.source_id,
        'related_timesheet_id',source.timesheet_id,
        'segment_id',source.segment_id,
        'target_invoice_week',source.target_invoice_week,
        'invoice_stream',source.invoice_stream,
        'consolidation_mode',source.consolidation_mode
      ) order by
        source.group_key,
        source.member->>'source_member_key'
      )
      from group_sources source
    ),'[]'::jsonb)) policy
  ),
  correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generate-candidate:'||group_row.group_key,
      'scope_key',group_row.group_key,
      'validation_purpose','CANDIDATE_GENERATION',
      'expected_client_id',group_row.client_id,
      'expected_contract_id',case
        when cardinality(group_row.contract_ids)=1
          then group_row.contract_ids[1]
      end,
      'natural_source_week',case
        when cardinality(group_row.natural_source_weeks)=1
          then group_row.natural_source_weeks[1]
      end,
      'target_invoice_week',group_row.target_invoice_week,
      'expected_invoice_stream',group_row.invoice_stream,
      'planned_members',coalesce((
        select jsonb_agg(jsonb_build_object(
          'timesheet_id',source.timesheet_id,
          'source_type',source.member->>'source_type',
          'source_id',source.source_id,
          'source_member_key',source.member->>'source_member_key',
          'segment_id',source.segment_id,
          'target_invoice_week',source.target_invoice_week,
          'vat_rate_pct',vat.vat_rate
        ) order by source.member->>'source_member_key')
        from group_sources source
        left join vat_policy vat
          on vat.source_member_key=
            source.member->>'source_member_key'
        where source.group_key=group_row.group_key
      ),'[]'::jsonb)
    ) order by group_row.group_key),'[]'::jsonb) scopes
    from groups group_row
  ),
  correction_validation as materialized (
    select validation.*
    from correction_scopes scope
    cross join lateral private._invoice_correction_validate_batch(
      scope.scopes,
      (select today from anchor)
    ) validation
  ),
  correction_group_results as materialized (
    select
      validation.scope_key group_key,
      validation.valid,
      validation.blocker_code,
      validation.blocker_codes,
      validation.detail_json details
    from correction_validation validation
  ),
  independent_member_blockers as materialized (
    select
      source.group_key,
      source.member->>'source_member_key' source_member_key,
      blocker.code blocker_code,
      blocker.ordinality blocker_order
    from group_sources source
    left join public.timesheets timesheet
      on timesheet.timesheet_id=source.timesheet_id
     and timesheet.is_current
    left join public.timesheets_financials financial
      on financial.timesheet_id=source.timesheet_id
     and financial.is_current
    left join public.v_ts_invoice_precheck precheck
      on precheck.timesheet_id=source.timesheet_id
    left join reference_policy reference
      on reference.source_member_key=
        source.member->>'source_member_key'
    left join vat_policy vat
      on vat.source_member_key=source.member->>'source_member_key'
    cross join lateral unnest(array_remove(array[
      case
        when timesheet.timesheet_id is null
          then 'TIMESHEET_NOT_CURRENT'
      end,
      case
        when financial.timesheet_id is null
          then 'CURRENT_FINANCIALS_MISSING'
      end,
      case
        when coalesce(financial.is_stale,true)
          then 'FINANCIALS_STALE'
      end,
      case
        when coalesce(financial.processing_status::text,'')
          <>'READY_FOR_INVOICE'
          then 'NOT_READY_FOR_INVOICE'
      end,
      case
        when coalesce(financial.has_rate_issue,false)
          then 'RATE_MISSING'
      end,
      case
        when coalesce(financial.has_pay_channel_issue,false)
          then 'PAY_CHANNEL_MISSING'
      end,
      case
        when financial.locked_by_invoice_id is not null
          then 'SOURCE_ALREADY_LOCKED'
      end,
      case
        when upper(coalesce(timesheet.submission_mode::text,''))='QR'
         and (
           nullif(timesheet.qr_signed_hash,'') is null
           or timesheet.qr_signed_at_utc is null
         )
          then 'QR_TIMESHEET_UNSIGNED'
      end,
      case
        when coalesce(precheck.require_reference_to_invoice,false)
         and coalesce(financial.total_hours,0)>0
         and coalesce(reference.reference_ready,false) is not true
          then coalesce(reference.blocker_code,'MISSING_REFERENCE')
      end,
      case
        when (
          coalesce(financial.mileage_pay_ex_vat,0)<>0
          or coalesce(financial.mileage_charge_ex_vat,0)<>0
        )
        and not exists(
          select 1
          from public.timesheet_evidence evidence
          where evidence.timesheet_id=source.timesheet_id
            and upper(coalesce(evidence.kind,''))='MILEAGE'
            and nullif(evidence.storage_key,'') is not null
        )
          then 'MISSING_MILEAGE_EVIDENCE'
      end,
      case
        when (
          coalesce(financial.expenses_pay_ex_vat,0)<>0
          or coalesce(financial.expenses_charge_ex_vat,0)<>0
          or coalesce(financial.travel_pay_ex_vat,0)<>0
          or coalesce(financial.travel_charge_ex_vat,0)<>0
          or coalesce(financial.accommodation_pay_ex_vat,0)<>0
          or coalesce(financial.accommodation_charge_ex_vat,0)<>0
        )
        and not exists(
          select 1
          from public.timesheet_evidence evidence
          where evidence.timesheet_id=source.timesheet_id
            and upper(coalesce(evidence.kind,'')) in(
              'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES'
            )
            and nullif(evidence.storage_key,'') is not null
        )
          then 'MISSING_EXPENSE_EVIDENCE'
      end,
      case
        when exists(
          select 1
          from public.timesheet_evidence evidence
          join public.invoice_document_assets asset
            on asset.id=evidence.document_asset_id
          where evidence.timesheet_id=source.timesheet_id
            and asset.status in(
              'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'
            )
        )
          then 'REQUIRED_ASSET_PERMANENT_FAILURE'
      end,
      case
        when coalesce(vat.valid,false) is not true
          then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED')
      end,
      case
        when exists(
          select 1
          from public.invoice_lines line
          join public.invoices invoice on invoice.id=line.invoice_id
          where line.timesheet_id=source.timesheet_id
            and invoice.status in('DRAFT','ISSUED','ON_HOLD')
            and coalesce(
              financial.invoice_breakdown_json->>'mode',
              ''
            )<>'SEGMENTS'
        )
          then 'SOURCE_ALREADY_INVOICED'
      end
    ],null)) with ordinality blocker(code,ordinality)
  ),
  independent_group_blockers as materialized (
    select
      blocker.group_key,
      (
        array_agg(
          blocker.blocker_code
          order by blocker.source_member_key,blocker.blocker_order
        )
      )[1] blocker_code,
      jsonb_build_object(
        'code',(
          array_agg(
            blocker.blocker_code
            order by blocker.source_member_key,blocker.blocker_order
          )
        )[1],
        'sources',jsonb_agg(jsonb_build_object(
          'source_member_key',blocker.source_member_key,
          'code',blocker.blocker_code
        ) order by blocker.source_member_key,blocker.blocker_order)
      ) blocker_detail
    from independent_member_blockers blocker
    group by blocker.group_key
  ),
  selected_member_totals as materialized (
    select
      source.group_key,
      source.member->>'source_member_key' source_member_key,
      source.timesheet_id,
      source.segment_id,
      round(case
        when source.segment_id is not null then coalesce((
          select sum(case
            when coalesce(segment.value->>'charge_amount','')~
              '^[+-]?[0-9]+([.][0-9]+)?$'
              then (segment.value->>'charge_amount')::numeric
            when coalesce(segment.value->>'charge_ex_vat','')~
              '^[+-]?[0-9]+([.][0-9]+)?$'
              then (segment.value->>'charge_ex_vat')::numeric
            else 0
          end)
          from jsonb_array_elements(
            case
              when jsonb_typeof(
                financial.invoice_breakdown_json->'segments'
              )='array'
                then financial.invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) segment(value)
          where coalesce(
            nullif(segment.value->>'segment_id',''),
            left(
              encode(
                digest(segment.value::text,'sha256'),
                'hex'
              ),
              24
            )
          )=source.segment_id
        ),0)
        else coalesce(financial.total_charge_ex_vat,0)
      end,2) total_charge_ex_vat,
      round(case
        when source.segment_id is not null then coalesce((
          select sum(
            case
              when coalesce(segment.value->>'hours_day','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_day')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_night','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_night')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_sat','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_sat')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_sun','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_sun')::numeric
              else 0
            end
            +case
              when coalesce(segment.value->>'hours_bh','')~
                '^[+-]?[0-9]+([.][0-9]+)?$'
                then (segment.value->>'hours_bh')::numeric
              else 0
            end
          )
          from jsonb_array_elements(
            case
              when jsonb_typeof(
                financial.invoice_breakdown_json->'segments'
              )='array'
                then financial.invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) segment(value)
          where coalesce(
            nullif(segment.value->>'segment_id',''),
            left(
              encode(
                digest(segment.value::text,'sha256'),
                'hex'
              ),
              24
            )
          )=source.segment_id
        ),0)
        else coalesce(financial.total_hours,0)
      end,2) total_hours
    from group_sources source
    join public.timesheets_financials financial
      on financial.timesheet_id=source.timesheet_id
     and financial.is_current
  ),
  generation_totals as materialized (
    select
      source.group_key,
      round(coalesce(sum(total.total_charge_ex_vat),0),2)
        total_ex_vat,
      round(coalesce(sum(round(
        total.total_charge_ex_vat*coalesce(vat.vat_rate,0)/100,
        2
      )),0),2) vat_amount,
      round(coalesce(sum(total.total_hours),0),2) total_hours
    from group_sources source
    join selected_member_totals total
      on total.group_key=source.group_key
     and total.source_member_key=
       source.member->>'source_member_key'
    left join vat_policy vat
      on vat.source_member_key=source.member->>'source_member_key'
    group by source.group_key
  ),
  timesheet_totals as materialized (
    select
      total.group_key,
      total.timesheet_id,
      round(sum(total.total_charge_ex_vat),2) total_charge_ex_vat,
      round(sum(total.total_hours),2) total_hours
    from selected_member_totals total
    group by total.group_key,total.timesheet_id
  ),
  source_display as materialized (
    select distinct
      source.group_key,
      financial.timesheet_id,
      financial.client_id,
      financial.candidate_id,
      timesheet.week_ending_date,
      timesheet.submission_mode,
      financial.basis,
      total.total_charge_ex_vat,
      total.total_hours,
      summary.client_name,
      summary.candidate_name,
      summary.validation_status,
      coalesce(
        summary.hr_validation_required_for_invoice,
        false
      ) hr_validation_required_for_invoice,
      coalesce(
        summary.hr_validation_required_for_invoice,
        false
      )
      and (
        summary.validation_status is null
        or summary.validation_status<>all(array[
          'VALIDATION_OK'::public.validation_status_enum,
          'OVERRIDDEN'::public.validation_status_enum
        ])
      ) blocked_by_hr_validation,
      precheck.precheck_status,
      exists(
        select 1
        from public.timesheet_evidence evidence
        left join public.invoice_document_assets asset
          on asset.id=evidence.document_asset_id
        where evidence.timesheet_id=financial.timesheet_id
          and evidence.processing_state not in('SUPERSEDED')
          and (asset.id is null or asset.status<>'READY')
      ) unready_evidence_asset,
      exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='TIMESHEET'
          and version.entity_id=financial.timesheet_id
          and version.purpose='TIMESHEET'
          and version.source_revision=timesheet.document_revision::text
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      ) timesheet_document_ready,
      timesheet.document_revision,
      timesheet.document_state,
      timesheet.current_document_version_id
    from group_sources source
    join public.timesheets_financials financial
      on financial.timesheet_id=source.timesheet_id
     and financial.is_current
    join public.timesheets timesheet
      on timesheet.timesheet_id=financial.timesheet_id
     and timesheet.is_current
    join timesheet_totals total
      on total.group_key=source.group_key
     and total.timesheet_id=source.timesheet_id
    join public.v_ts_invoice_precheck precheck
      on precheck.timesheet_id=financial.timesheet_id
    left join public.v_timesheets_summary_base summary
      on summary.timesheet_id=financial.timesheet_id
  ),
  active_exact as materialized (
    select distinct on(
      chunk.payload_json->>'group_key',
      chunk.payload_json->>'source_revision'
    )
      chunk.payload_json->>'group_key' group_key,
      operation.id operation_id,
      operation.status,
      chunk.payload_json->>'source_revision' source_revision,
      operation.progress_json,
      operation.error_json,
      operation.updated_at_utc
    from public.invoice_operation_chunks chunk
    join public.invoice_operations operation
      on operation.id=chunk.operation_id
    where chunk.chunk_type='GENERATION_GROUP'
      and chunk.status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
      )
      and operation.status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
      )
      and coalesce(
        chunk.payload_json->>'is_selection_expander',
        'false'
      )<>'true'
      and (not chunk.is_manifest_member or chunk.manifest_committed)
      and (
        not chunk.is_manifest_member
        or coalesce(chunk.entity_type,'')<>'OPERATION'
      )
    order by
      chunk.payload_json->>'group_key',
      chunk.payload_json->>'source_revision',
      operation.priority desc,
      operation.created_at_utc desc
  ),
  group_classification as materialized (
    select
      group_row.client_id,
      group_row.group_key,
      group_row.target_invoice_week,
      group_row.consolidation_mode,
      group_row.invoice_stream,
      group_row.source_revision_hash,
      group_row.canonical_source_ids,
      group_row.canonical_source_members,
      totals.total_ex_vat,
      totals.vat_amount,
      totals.total_hours,
      coalesce(jsonb_agg(
        distinct to_jsonb(display.candidate_id)
        order by to_jsonb(display.candidate_id)
      )
        filter(where display.candidate_id is not null),'[]'::jsonb)
        candidate_ids,
      coalesce(jsonb_agg(
        distinct to_jsonb(display.candidate_name)
        order by to_jsonb(display.candidate_name)
      )
        filter(where nullif(display.candidate_name,'') is not null),
        '[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(display.week_ending_date)
        order by to_jsonb(display.week_ending_date))
        filter(where display.week_ending_date is not null),
        '[]'::jsonb) week_ending_dates,
      max(display.week_ending_date) week_ending_date,
      case
        when group_row.blocker_code is not null
          then group_row.blocker_code
        when independent.blocker_code is not null
          then independent.blocker_code
        when coalesce(correction.valid,true) is not true
          then coalesce(
            correction.blocker_code,
            'INVOICE_CORRECTION_UNIT_INVALID'
          )
        when bool_or(display.blocked_by_hr_validation)
          then 'HR_VALIDATION_BLOCKED'
        when not coalesce(p_allow_early,false)
         and exists(
           select 1
           from jsonb_array_elements(
             group_row.canonical_source_members
           ) early(value)
           where case
             when pg_input_is_valid(
               coalesce(early.value->>'target_invoice_week',''),
               'date'
             )
               then (
                 early.value->>'target_invoice_week'
               )::date+6 >= (select today from anchor)
             else false
           end
         )
          then 'EARLY_GENERATION_NOT_ALLOWED'
      end blocker_code,
      case
        when group_row.blocker_code is not null
          then group_row.blocker_detail
        when independent.blocker_code is not null
          then independent.blocker_detail
        when coalesce(correction.valid,true) is not true
          then jsonb_build_object(
            'code',coalesce(
              correction.blocker_code,
              'INVOICE_CORRECTION_UNIT_INVALID'
            ),
            'correction_validation',correction.details
          )
        when bool_or(display.blocked_by_hr_validation)
          then jsonb_build_object(
            'code','HR_VALIDATION_BLOCKED',
            'sources',coalesce(
              jsonb_agg(display.timesheet_id order by display.timesheet_id)
                filter(where display.blocked_by_hr_validation),
              '[]'::jsonb
            )
          )
        when not coalesce(p_allow_early,false)
         and exists(
           select 1
           from jsonb_array_elements(
             group_row.canonical_source_members
           ) early(value)
           where case
             when pg_input_is_valid(
               coalesce(early.value->>'target_invoice_week',''),
               'date'
             )
               then (
                 early.value->>'target_invoice_week'
               )::date+6 >= (select today from anchor)
             else false
           end
         )
          then jsonb_build_object(
            'code','EARLY_GENERATION_NOT_ALLOWED'
          )
      end blocker_detail,
      active.operation_id active_operation_id,
      active.status active_status,
      active.progress_json active_progress,
      active.error_json active_error,
      coalesce(correction.details,'[]'::jsonb)
        correction_validation,
      jsonb_agg(jsonb_build_object(
        'timesheet_id',display.timesheet_id,
        'candidate_name',display.candidate_name,
        'week_ending_date',display.week_ending_date,
        'total_charge_ex_vat',display.total_charge_ex_vat,
        'total_hours',display.total_hours,
        'basis',display.basis::text,
        'submission_mode',coalesce(display.submission_mode::text,''),
        'validation_status',coalesce(display.validation_status::text,''),
        'hr_validation_required_for_invoice',
          display.hr_validation_required_for_invoice,
        'blocked_by_hr_validation',display.blocked_by_hr_validation,
        'precheck_status',coalesce(display.precheck_status::text,''),
        'document_revision',display.document_revision,
        'document_state',display.document_state,
        'current_document_version_id',
          display.current_document_version_id,
        'timesheet_document_ready',
          display.timesheet_document_ready,
        'unready_evidence_asset',display.unready_evidence_asset
      ) order by
        display.candidate_name nulls last,
        display.timesheet_id
      ) timesheet_projection
    from groups group_row
    join generation_totals totals
      on totals.group_key=group_row.group_key
    join source_display display
      on display.group_key=group_row.group_key
    left join independent_group_blockers independent
      on independent.group_key=group_row.group_key
    left join correction_group_results correction
      on correction.group_key=group_row.group_key
    left join active_exact active
      on active.group_key=group_row.group_key
     and active.source_revision=group_row.source_revision_hash
    group by
      group_row.client_id,
      group_row.group_key,
      group_row.target_invoice_week,
      group_row.consolidation_mode,
      group_row.invoice_stream,
      group_row.source_revision_hash,
      group_row.canonical_source_ids,
      group_row.canonical_source_members,
      group_row.blocker_code,
      group_row.blocker_detail,
      totals.total_ex_vat,
      totals.vat_amount,
      totals.total_hours,
      independent.blocker_code,
      independent.blocker_detail,
      correction.valid,
      correction.blocker_code,
      correction.details,
      active.operation_id,
      active.status,
      active.progress_json,
      active.error_json
  ),
  create_rows as materialized (
    select
      'generate:'||classification.group_key selection_key,
      jsonb_build_object(
        'selection_key','generate:'||classification.group_key,
        'row_kind','CREATE_INVOICE',
        'scope_key',classification.group_key,
        'invoice_id',null,
        'client_id',classification.client_id,
        'client_name',client.name,
        'candidate_ids',classification.candidate_ids,
        'candidate_names',classification.candidate_names,
        'candidate_display',case
          when jsonb_array_length(classification.candidate_names)=1
            then classification.candidate_names->>0
          when jsonb_array_length(classification.candidate_names)>1
            then 'Multiple candidates ('||
              jsonb_array_length(classification.candidate_names)::text||
              ')'
          else 'Unknown candidate'
        end,
        'week_ending_dates',classification.week_ending_dates,
        'week_ending_date',classification.week_ending_date,
        'currency','GBP',
        'invoice_stream',upper(coalesce(
          nullif(classification.invoice_stream,''),
          'NORMAL'
        )),
        'total_ex_vat',round(classification.total_ex_vat,2),
        'vat_amount',round(classification.vat_amount,2),
        'total_inc_vat',round(
          classification.total_ex_vat+classification.vat_amount,
          2
        ),
        'generation_state','NOT_GENERATED',
        'primary_blocker_code',classification.blocker_code,
        'action_blocker_codes',coalesce((
          select jsonb_agg(to_jsonb(code) order by first_order,code)
          from (
            select code,min(code_order) first_order
            from (
              select
                1 code_order,
                nullif(classification.blocker_code,'') code
              union all
              select
                2,
                nullif(classification.blocker_detail->>'code','')
              union all
              select
                100+source.ordinality::integer,
                nullif(source.value->>'code','')
              from jsonb_array_elements(
                case
                  when jsonb_typeof(
                    classification.blocker_detail->'sources'
                  )='array'
                    then classification.blocker_detail->'sources'
                  else '[]'::jsonb
                end
              ) with ordinality source(value,ordinality)
            ) raw_code
            where code is not null
              and code not in(
                'EARLY_GENERATION_NOT_ALLOWED',
                'SOURCE_ALREADY_INVOICED'
              )
            group by code
          ) code_row
        ),'[]'::jsonb),
        'informational_codes',case
          when classification.active_status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then jsonb_build_array('GENERATING')
          else '[]'::jsonb
        end,
        'is_active',classification.active_status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
        ),
        'active_operation_id',classification.active_operation_id,
        'active_operation_status',classification.active_status,
        'source_revision',classification.source_revision_hash,
        'document_revision',null,
        'command_payload',jsonb_build_object(
          'command_type','GENERATE_SELECTED',
          'canonical_source_ids',
            to_jsonb(classification.canonical_source_ids),
          'canonical_source_members',
            classification.canonical_source_members,
          'group_key',classification.group_key,
          'source_revision',classification.source_revision_hash,
          'target_invoice_week',classification.target_invoice_week,
          'consolidation_mode',classification.consolidation_mode,
          'invoice_stream',classification.invoice_stream,
          'correction_validation',
            classification.correction_validation,
          'allow_early',coalesce(p_allow_early,false)
        ),
        'is_early',coalesce(
          classification.week_ending_date
            >=(select today from anchor),
          false
        ),
        'selectable',
          jsonb_array_length(coalesce((
            select jsonb_agg(to_jsonb(code))
            from (
              select distinct code
              from (
                select nullif(classification.blocker_code,'') code
                union all
                select nullif(
                  classification.blocker_detail->>'code',
                  ''
                )
              ) code_source
              where code is not null
                and code not in(
                  'EARLY_GENERATION_NOT_ALLOWED',
                  'SOURCE_ALREADY_INVOICED'
                )
            ) blocking
          ),'[]'::jsonb))=0
          and classification.active_status is null,
        'row_status',case
          when classification.blocker_code is not null
           and classification.blocker_code not in(
             'EARLY_GENERATION_NOT_ALLOWED',
             'SOURCE_ALREADY_INVOICED'
           )
            then 'BLOCKED'
          when classification.active_status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then 'IN_PROGRESS'
          else 'READY'
        end,
        '_private',jsonb_build_object(
          'canonical_source_ids',
            to_jsonb(classification.canonical_source_ids),
          'canonical_source_members',
            classification.canonical_source_members,
          'target_invoice_week',classification.target_invoice_week,
          'consolidation_mode',classification.consolidation_mode,
          'correction_validation',
            classification.correction_validation,
          'blocker_detail',classification.blocker_detail,
          'total_hours',classification.total_hours,
          'timesheets',classification.timesheet_projection,
          'active_progress',classification.active_progress,
          'active_error',classification.active_error
        )
      ) candidate_json
    from group_classification classification
    join public.clients client on client.id=classification.client_id
    where coalesce(classification.blocker_code,'')
      not in('SOURCE_ALREADY_INVOICED','EARLY_GENERATION_NOT_ALLOWED')
  ),
  stale_invoice_timesheets as materialized (
    select
      line.invoice_id,
      coalesce(jsonb_agg(
        distinct to_jsonb(summary.candidate_id)
        order by to_jsonb(summary.candidate_id)
      )
        filter(where summary.candidate_id is not null),'[]'::jsonb)
        candidate_ids,
      coalesce(jsonb_agg(
        distinct to_jsonb(summary.candidate_name)
        order by to_jsonb(summary.candidate_name)
      )
        filter(where nullif(summary.candidate_name,'') is not null),
        '[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(
        distinct to_jsonb(summary.week_ending_date)
        order by to_jsonb(summary.week_ending_date)
      )
        filter(where summary.week_ending_date is not null),
        '[]'::jsonb) week_ending_dates,
      min(summary.week_ending_date) min_week_ending,
      max(summary.week_ending_date) max_week_ending
    from public.invoice_lines line
    left join public.v_timesheets_summary_base summary
      on summary.timesheet_id=line.timesheet_id
    group by line.invoice_id
  ),
  stale_rows as materialized (
    select
      'invoice:'||invoice.id::text selection_key,
      jsonb_build_object(
        'selection_key','invoice:'||invoice.id::text,
        'row_kind',case
          when coalesce(invoice.document_state,'')='FAILED'
            then 'RETRY_GENERATION'
          else 'REGENERATE_DRAFT'
        end,
        'scope_key',invoice.id::text,
        'invoice_id',invoice.id,
        'client_id',invoice.client_id,
        'client_name',client.name,
        'candidate_ids',coalesce(
          timesheet.candidate_ids,
          '[]'::jsonb
        ),
        'candidate_names',coalesce(
          timesheet.candidate_names,
          '[]'::jsonb
        ),
        'candidate_display',case
          when jsonb_array_length(coalesce(
            timesheet.candidate_names,
            '[]'::jsonb
          ))=1
            then timesheet.candidate_names->>0
          when jsonb_array_length(coalesce(
            timesheet.candidate_names,
            '[]'::jsonb
          ))>1
            then 'Multiple candidates ('||
              jsonb_array_length(timesheet.candidate_names)::text||
              ')'
          else 'Unknown candidate'
        end,
        'week_ending_dates',coalesce(
          timesheet.week_ending_dates,
          '[]'::jsonb
        ),
        'week_ending_date',coalesce(
          timesheet.min_week_ending,
          case
            when pg_input_is_valid(
              invoice.header_snapshot_json#>>'{meta,invoice_week_start}',
              'date'
            )
              then (
                invoice.header_snapshot_json#>>
                  '{meta,invoice_week_start}'
              )::date+6
          end
        ),
        'currency',coalesce(
          nullif(
            invoice.header_snapshot_json#>>'{meta,currency}',
            ''
          ),
          nullif(invoice.header_snapshot_json->>'currency',''),
          'GBP'
        ),
        'invoice_stream',upper(coalesce(
          nullif(
            invoice.header_snapshot_json#>>'{meta,invoice_stream}',
            ''
          ),
          nullif(invoice.header_snapshot_json->>'invoice_stream',''),
          case
            when lower(coalesce(
              invoice.header_snapshot_json#>>'{meta,self_bill}',
              invoice.header_snapshot_json->>'self_bill',
              'false'
            )) in('true','t','1','yes')
              then 'SELF_BILL'
          end,
          'NORMAL'
        )),
        'total_ex_vat',round(coalesce(invoice.subtotal_ex_vat,0),2),
        'vat_amount',round(coalesce(invoice.vat_amount,0),2),
        'total_inc_vat',round(coalesce(
          invoice.total_inc_vat,
          coalesce(invoice.subtotal_ex_vat,0)
            +coalesce(invoice.vat_amount,0)
        ),2),
        'generation_state',case
          when coalesce(invoice.document_state,'')='FAILED'
            then 'FAILED'
          when exists(
            select 1
            from public.invoice_document_versions version
            where version.entity_type='INVOICE'
              and version.entity_id=invoice.id
              and version.purpose='DRAFT_PREVIEW'
          )
            then 'STALE'
          else 'NOT_GENERATED'
        end,
        'primary_blocker_code',null,
        'action_blocker_codes','[]'::jsonb,
        'informational_codes',case
          when active.status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then jsonb_build_array('GENERATING')
          when exists(
            select 1
            from public.invoice_document_versions version
            where version.entity_type='INVOICE'
              and version.entity_id=invoice.id
              and version.purpose='DRAFT_PREVIEW'
          )
            then jsonb_build_array('STALE')
          else jsonb_build_array('NOT_GENERATED')
        end,
        'is_active',active.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
        ),
        'active_operation_id',active.operation_id,
        'active_operation_status',active.status,
        'source_revision',invoice.document_revision::text,
        'document_revision',invoice.document_revision::text,
        'command_payload',jsonb_build_object(
          'command_type','VIEW_INVOICE_DOCUMENT',
          'invoice_id',invoice.id,
          'purpose','DRAFT_PREVIEW',
          'expected_revision',invoice.document_revision,
          'source_revision',invoice.document_revision::text
        ),
        'is_early',coalesce(
          timesheet.max_week_ending,
          case
            when pg_input_is_valid(
              invoice.header_snapshot_json#>>
                '{meta,invoice_week_start}',
              'date'
            )
              then (
                invoice.header_snapshot_json#>>
                  '{meta,invoice_week_start}'
              )::date+6
          end
        ) >= (select today from anchor),
        'selectable',active.operation_id is null,
        'row_status',case
          when active.status in(
            'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
          )
            then 'IN_PROGRESS'
          when coalesce(invoice.document_state,'')='FAILED'
            then 'FAILED'
          when exists(
            select 1
            from public.invoice_document_versions version
            where version.entity_type='INVOICE'
              and version.entity_id=invoice.id
              and version.purpose='DRAFT_PREVIEW'
          )
            then 'STALE'
          else 'READY'
        end,
        '_private','{}'::jsonb
      ) candidate_json
    from public.invoices invoice
    join public.clients client on client.id=invoice.client_id
    left join stale_invoice_timesheets timesheet
      on timesheet.invoice_id=invoice.id
    left join lateral (
      select
        operation.id operation_id,
        operation.status
      from public.invoice_operations operation
      where operation.operation_type='BUILD_DOCUMENT'
        and operation.entity_type='INVOICE'
        and operation.entity_id=invoice.id
        and operation.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'
        )
      order by
        operation.created_at_utc desc,
        operation.id desc
      limit 1
    ) active on true
    where invoice.type::text='INVOICE'
      and invoice.status::text in('DRAFT','ON_HOLD')
      and coalesce(invoice.document_revision,0)>0
      and (
        v_scope_keys is null
        or invoice.id::text=any(v_scope_keys)
      )
      and not exists(
        select 1
        from public.invoice_document_versions version
        where version.entity_type='INVOICE'
          and version.entity_id=invoice.id
          and version.purpose='DRAFT_PREVIEW'
          and version.source_revision=invoice.document_revision::text
          and version.template_version='invoice-professional-v2'
          and version.status='READY'
          and version.r2_key is not null
          and version.sha256~'^[0-9a-f]{64}$'
          and coalesce(version.size_bytes,0)>0
          and coalesce(version.page_count,0)>0
      )
  )
  select create_row.selection_key,create_row.candidate_json
  from create_rows create_row
  union all
  select stale_row.selection_key,stale_row.candidate_json
  from stale_rows stale_row
  order by selection_key;
end;
$function$;

alter function private._invoice_batch_generate_classification_v2(
  boolean,text[],timestamptz
) owner to postgres;
revoke all on function private._invoice_batch_generate_classification_v2(
  boolean,text[],timestamptz
) from public,anon,authenticated;
grant execute on function private._invoice_batch_generate_classification_v2(
  boolean,text[],timestamptz
) to service_role;
