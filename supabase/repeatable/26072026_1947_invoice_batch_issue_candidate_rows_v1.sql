create or replace function private._invoice_batch_issue_candidate_rows_v1(
  p_query jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_selection jsonb;
  v_page_size integer;
  v_after_key text;
  v_allow_early boolean;
  v_display_mode text;
  v_legacy jsonb;
  v_result jsonb;
begin
  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if coalesce(v_query->>'contract_version','INVOICE_BATCH_QUERY_V1') <> 'INVOICE_BATCH_QUERY_V1' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  v_mode := upper(coalesce(nullif(v_query->>'mode',''),'PAGE'));
  if v_mode not in ('PAGE','FACETS','EXPAND_SELECTION','EXPLICIT_KEYS') then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_filters := case when jsonb_typeof(v_query->'filters') = 'object' then v_query->'filters' else '{}'::jsonb end;
  v_sort := case when jsonb_typeof(v_query->'sort') = 'object' then v_query->'sort' else '{}'::jsonb end;
  v_selection := case when jsonb_typeof(v_query->'selection') = 'object' then v_query->'selection' else jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V1',
    'mode','IMPLICIT_ALL',
    'default_selected',true,
    'rules','[]'::jsonb
  ) end;

  perform 1 from private._invoice_batch_selection_rules_v1(v_selection) limit 1;

  v_allow_early := lower(coalesce(v_query->>'allow_early',v_filters->>'allow_early','false')) in ('true','t','1','yes','on');
  v_display_mode := upper(coalesce(nullif(v_query->>'display_mode',''),nullif(v_filters->>'display_mode',''),'ALL'));
  if v_display_mode not in ('ALL','READY','BLOCKED') then
    raise exception using errcode='22023', message='INVOICE_BATCH_DISPLAY_MODE_INVALID';
  end if;

  if upper(coalesce(v_sort->>'group_preset','WEEK_CLIENT_CANDIDATE')) not in (
    'WEEK_CLIENT_CANDIDATE','CLIENT_WEEK_CANDIDATE','CANDIDATE_WEEK_CLIENT','STATUS_WEEK_CLIENT'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_GROUP_PRESET_INVALID';
  end if;

  if upper(coalesce(v_sort->>'sort_key','WEEK_ENDING_DATE')) not in (
    'WEEK_ENDING_DATE','CLIENT_NAME','CANDIDATE_NAME','TOTAL_EX_VAT','TOTAL_INC_VAT','STATUS','INVOICE_NUMBER'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SORT_KEY_INVALID';
  end if;

  v_page_size := case
    when coalesce(v_query->>'page_size','') ~ '^[1-9][0-9]{0,8}$'
      then greatest(1,least((v_query->>'page_size')::integer,100))
    else 100
  end;
  v_after_key := nullif(coalesce(v_query#>>'{cursor,after_selection_key}',v_query#>>'{cursor,last_selection_key}',v_query->>'after_selection_key'), '');

  -- Always ask the legacy authority for the wider set; this helper applies the
  -- locked Batch early visibility rule itself so early rows are invisible when
  -- allow_early=false instead of being shown as blocked.
  v_legacy := public.invoice_batch_issue_candidates(true, 20000);

  with
  params as materialized (
    select
      v_mode mode,
      v_allow_early allow_early,
      v_display_mode display_mode,
      v_page_size page_size,
      v_after_key after_key,
      lower(nullif(btrim(coalesce(v_filters->>'search','')),'')) search_text,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_from',''),'date') then (v_filters->>'week_ending_from')::date end week_ending_from,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_to',''),'date') then (v_filters->>'week_ending_to')::date end week_ending_to,
      (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date today,
      case when jsonb_typeof(v_filters->'client_ids')='array' then v_filters->'client_ids' else '[]'::jsonb end client_ids,
      case when jsonb_typeof(v_filters->'candidate_ids')='array' then v_filters->'candidate_ids' else '[]'::jsonb end candidate_ids,
      case when jsonb_typeof(v_filters->'week_endings')='array' then v_filters->'week_endings' else '[]'::jsonb end week_endings,
      case when jsonb_typeof(v_filters->'status_codes')='array' then v_filters->'status_codes' else '[]'::jsonb end status_codes,
      case when jsonb_typeof(v_filters->'blocker_codes')='array' then v_filters->'blocker_codes' else '[]'::jsonb end blocker_codes,
      coalesce(nullif(upper(v_sort->>'group_preset'),''),'WEEK_CLIENT_CANDIDATE') group_preset,
      coalesce(nullif(upper(v_sort->>'sort_key'),''),'WEEK_ENDING_DATE') sort_key,
      case when upper(coalesce(v_sort->>'sort_direction','ASC'))='DESC' then 'DESC' else 'ASC' end sort_direction,
      case when pg_input_is_valid(
        nullif(coalesce(v_query#>>'{cursor,after_sort_date}',v_query#>>'{cursor,last_sort_date}',''),''),
        'date'
      )
        then coalesce(v_query#>>'{cursor,after_sort_date}',v_query#>>'{cursor,last_sort_date}')::date end after_sort_date,
      nullif(coalesce(v_query#>>'{cursor,after_sort_text}',v_query#>>'{cursor,last_sort_text}',''),'') after_sort_text,
      case when coalesce(v_query#>>'{cursor,after_sort_numeric}',v_query#>>'{cursor,last_sort_numeric}','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then coalesce(v_query#>>'{cursor,after_sort_numeric}',v_query#>>'{cursor,last_sort_numeric}')::numeric end after_sort_numeric
  ),
  selection_rules as materialized (
    select * from private._invoice_batch_selection_rules_v1(v_selection)
  ),
  legacy_clients as materialized (
    select client.value client_json
    from jsonb_array_elements(case when jsonb_typeof(v_legacy)='array' then v_legacy else '[]'::jsonb end) client(value)
  ),
  legacy_weeks as materialized (
    select client_json,
      client_json->>'client_id' client_id_text,
      client_json->>'client_name' client_name,
      week.value week_json
    from legacy_clients
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(client_json->'weeks')='array' then client_json->'weeks' else '[]'::jsonb end
    ) week(value)
  ),
  invoice_rows_raw as materialized (
    select
      'invoice:'||coalesce(inv.value->>'invoice_id','') selection_key,
      case when pg_input_is_valid(inv.value->>'invoice_id','uuid') then (inv.value->>'invoice_id')::uuid end invoice_id,
      inv.value->>'invoice_no' invoice_number,
      case when pg_input_is_valid(lw.client_id_text,'uuid') then lw.client_id_text::uuid end client_id,
      lw.client_name,
      case when pg_input_is_valid(inv.value->>'document_revision','int8') then (inv.value->>'document_revision')::bigint end document_revision,
      case when pg_input_is_valid(lw.week_json->>'week_ending_date','date') then (lw.week_json->>'week_ending_date')::date end week_ending_date,
      case when pg_input_is_valid(lw.week_json->>'week_ending_date','date')
        then (lw.week_json->>'week_ending_date')::date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early,
      coalesce(nullif(inv.value->>'currency',''),'GBP') currency,
      case when coalesce(inv.value->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'subtotal_ex_vat')::numeric,2) else 0 end total_ex_vat,
      case when coalesce(inv.value->>'vat_amount','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'vat_amount')::numeric,2) else 0 end vat_amount,
      case when coalesce(inv.value->>'total_inc_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$' then round((inv.value->>'total_inc_vat')::numeric,2) else 0 end total_inc_vat,
      upper(coalesce(inv.value->>'preview_document_state','')) preview_document_state,
      upper(coalesce(inv.value->>'status','')) invoice_status,
      coalesce(inv.value->'stable_blocker_codes','[]'::jsonb) hard_blocker_codes,
      coalesce(inv.value->'document_dependency_codes','[]'::jsonb) document_dependency_codes,
      coalesce(inv.value->'delivery_blocker_codes','[]'::jsonb) delivery_blocker_codes,
      coalesce(inv.value->'recipient_routing_warnings','[]'::jsonb) warning_codes,
      coalesce(inv.value->>'can_issue_only','false') in ('true','t','1','yes','on') can_issue_only,
      coalesce(inv.value->>'can_issue_and_deliver','false') in ('true','t','1','yes','on') can_issue_and_deliver,
      inv.value->'validation_detail' validation_detail,
      inv.value->'support_readiness' support_readiness,
      inv.value->>'active_issue_operation_id' active_issue_operation_id_text,
      inv.value->'active_issue_operation' active_issue_operation,
      inv.value->>'active_document_operation_id' active_document_operation_id_text,
      inv.value->'last_issue_error' last_issue_error,
      inv.value->'last_document_error' last_document_error
    from legacy_weeks lw
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(lw.week_json->'invoices')='array' then lw.week_json->'invoices' else '[]'::jsonb end
    ) inv(value)
  ),
  candidate_names as materialized (
    select r.invoice_id,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_name)) filter(where nullif(s.candidate_name,'') is not null),'[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(s.week_ending_date)) filter(where s.week_ending_date is not null),'[]'::jsonb) week_ending_dates
    from invoice_rows_raw r
    left join public.invoice_lines l on l.invoice_id=r.invoice_id
    left join public.v_timesheets_summary_base s on s.timesheet_id=l.timesheet_id
    group by r.invoice_id
  ),
  rows_with_state as materialized (
    select r.*,
      coalesce(cn.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(cn.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(cn.candidate_names,'[]'::jsonb))=1
        then cn.candidate_names->>0
        when jsonb_array_length(coalesce(cn.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(cn.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      case when jsonb_array_length(coalesce(cn.week_ending_dates,'[]'::jsonb))>0 then cn.week_ending_dates else jsonb_build_array(r.week_ending_date) end week_ending_dates,
      case
        when exists (
          select 1 from public.invoice_document_versions v
          where v.entity_type='INVOICE'
            and v.entity_id=r.invoice_id
            and v.purpose='DRAFT_PREVIEW'
            and v.source_revision=r.document_revision::text
            and v.template_version='invoice-professional-v2'
            and v.status='READY'
            and v.r2_key is not null
            and v.sha256 ~ '^[0-9a-f]{64}$'
            and coalesce(v.size_bytes,0)>0
            and coalesce(v.page_count,0)>0
        ) then 'FRESH'
        when exists (
          select 1 from public.invoice_document_versions prior_ready
          where prior_ready.entity_type='INVOICE'
            and prior_ready.entity_id=r.invoice_id
            and prior_ready.purpose='DRAFT_PREVIEW'
            and prior_ready.template_version='invoice-professional-v2'
            and prior_ready.status='READY'
            and prior_ready.r2_key is not null
            and prior_ready.sha256 ~ '^[0-9a-f]{64}$'
            and coalesce(prior_ready.size_bytes,0)>0
            and coalesce(prior_ready.page_count,0)>0
        ) then case
          when nullif(r.active_document_operation_id_text,'') is not null then 'ACTIVE'
          when r.preview_document_state in ('FAILED') or r.last_document_error is not null then 'FAILED'
          else 'STALE'
        end
        else 'NEVER_GENERATED'
      end generated_state,
      nullif(r.active_issue_operation_id_text,'') is not null is_active_issue,
      coalesce((jsonb_array_length(r.delivery_blocker_codes)>0 and r.can_issue_only and not r.can_issue_and_deliver),false) blocked_for_sending
    from invoice_rows_raw r
    left join candidate_names cn on cn.invoice_id=r.invoice_id
  ),
  candidate_rows as materialized (
    select r.*,
      (case when r.generated_state='STALE' then jsonb_build_array('STALE') else '[]'::jsonb end)
      || (case when r.generated_state='FAILED' then jsonb_build_array('FAILED_RENDER') else '[]'::jsonb end)
      || (case when r.generated_state='ACTIVE' then jsonb_build_array('GENERATING') else '[]'::jsonb end)
      || coalesce(r.hard_blocker_codes,'[]'::jsonb)
      || coalesce(r.document_dependency_codes,'[]'::jsonb) issue_blocker_codes,
      case when r.blocked_for_sending then jsonb_build_array('BLOCKED_FOR_SENDING') else '[]'::jsonb end
        || coalesce(r.warning_codes,'[]'::jsonb) informational_codes,
      (r.generated_state='FRESH'
        and r.can_issue_only
        and jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))=0
        and jsonb_array_length(coalesce(r.document_dependency_codes,'[]'::jsonb))=0
        and not r.is_active_issue) selectable,
      case when r.is_active_issue then 'IN_PROGRESS'
           when r.generated_state='STALE' then 'STALE'
           when r.generated_state='FAILED' then 'FAILED'
           when r.generated_state='ACTIVE' then 'IN_PROGRESS'
           when not r.can_issue_only or jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))>0
             or jsonb_array_length(coalesce(r.document_dependency_codes,'[]'::jsonb))>0 then 'BLOCKED'
           else 'READY' end row_status
    from rows_with_state r
    where r.generated_state <> 'NEVER_GENERATED'
      and r.invoice_status in ('DRAFT','ON_HOLD')
      and nullif(r.invoice_number,'') is not null
  ),
  filtered_rows as materialized (
    select r.*
    from candidate_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (p.display_mode='ALL'
        or (p.display_mode='READY' and r.selectable)
        or (p.display_mode='BLOCKED' and (r.row_status in ('BLOCKED','STALE','FAILED') or r.blocked_for_sending)))
      and (p.search_text is null
        or lower(coalesce(r.invoice_number,'')||' '||coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.invoice_id::text,'')) like '%'||p.search_text||'%')
      and (p.week_ending_from is null or r.week_ending_date >= p.week_ending_from)
      and (p.week_ending_to is null or r.week_ending_date <= p.week_ending_to)
      and (jsonb_array_length(p.client_ids)=0 or r.client_id::text in (select value from jsonb_array_elements_text(p.client_ids)))
      and (jsonb_array_length(p.candidate_ids)=0 or exists (
        select 1 from jsonb_array_elements_text(r.candidate_ids) row_candidate(id)
        where row_candidate.id in (select value from jsonb_array_elements_text(p.candidate_ids))
      ))
      and (jsonb_array_length(p.week_endings)=0 or r.week_ending_date::text in (select value from jsonb_array_elements_text(p.week_endings)))
      and (jsonb_array_length(p.status_codes)=0 or r.row_status in (select upper(value) from jsonb_array_elements_text(p.status_codes)))
      and (jsonb_array_length(p.blocker_codes)=0 or exists (
        select 1 from jsonb_array_elements_text(coalesce(r.issue_blocker_codes,'[]'::jsonb) || coalesce(r.delivery_blocker_codes,'[]'::jsonb) || coalesce(r.informational_codes,'[]'::jsonb)) badge(code)
        where badge.code in (select upper(value) from jsonb_array_elements_text(p.blocker_codes))
      ))
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then coalesce(lower(fr.row_status), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='INVOICE_NUMBER' then coalesce(lower(fr.invoice_number), case when p.sort_direction='DESC' then '' else repeat('~',100) end) end sort_text_key,
      case when p.sort_key='TOTAL_EX_VAT' then coalesce(fr.total_ex_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end)
           when p.sort_key='TOTAL_INC_VAT' then coalesce(fr.total_inc_vat, case when p.sort_direction='DESC' then -999999999999999999::numeric else 999999999999999999::numeric end) end sort_numeric_key
    from filtered_rows fr cross join params p
  ),
  cursor_filtered_rows as materialized (
    select sr.*
    from sortable_rows sr
    cross join params p
    where p.after_key is null
       or (
         p.sort_key='WEEK_ENDING_DATE'
         and p.after_sort_date is not null
         and ((p.sort_direction='ASC' and (sr.sort_date_key > p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_date_key < p.after_sort_date or (sr.sort_date_key=p.after_sort_date and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER')
         and p.after_sort_text is not null
         and ((p.sort_direction='ASC' and (sr.sort_text_key > p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_text_key < p.after_sort_text or (sr.sort_text_key=p.after_sort_text and sr.selection_key>p.after_key))))
       )
       or (
         p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT')
         and p.after_sort_numeric is not null
         and ((p.sort_direction='ASC' and (sr.sort_numeric_key > p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key)))
           or (p.sort_direction='DESC' and (sr.sort_numeric_key < p.after_sort_numeric or (sr.sort_numeric_key=p.after_sort_numeric and sr.selection_key>p.after_key))))
       )
       or (
         ((p.sort_key='WEEK_ENDING_DATE' and p.after_sort_date is null)
           or (p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.after_sort_text is null)
           or (p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.after_sort_numeric is null))
         and sr.selection_key > p.after_key
       )
  ),
  selected_rows as materialized (
    select fr.*,
      coalesce((
        select sr.action
        from selection_rules sr
        where (sr.selector_type='ROW' and sr.selection_key=fr.selection_key)
           or (sr.selector_type='WEEK' and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='CLIENT' and sr.client_id=fr.client_id)
           or (sr.selector_type='CANDIDATE' and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='WEEK_CLIENT' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='WEEK_CLIENT_CANDIDATE' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
        order by sr.rule_sequence desc
        limit 1
      ),'INCLUDE') last_selection_action
    from cursor_filtered_rows fr
  ),
  expanded_rows as materialized (
    select * from selected_rows
    where selectable and last_selection_action <> 'EXCLUDE'
  ),
  candidate_page_source as materialized (
    select * from expanded_rows where v_mode='EXPAND_SELECTION'
    union all
    select * from selected_rows where v_mode<>'EXPAND_SELECTION'
  ),
  ordered_page_rows as materialized (
    select src.*,
      row_number() over (order by
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='ASC' then src.sort_date_key end asc nulls last,
        case when p.sort_key='WEEK_ENDING_DATE' and p.sort_direction='DESC' then src.sort_date_key end desc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.sort_direction='ASC' then src.sort_text_key end asc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS','INVOICE_NUMBER') and p.sort_direction='DESC' then src.sort_text_key end desc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='ASC' then src.sort_numeric_key end asc nulls last,
        case when p.sort_key in ('TOTAL_EX_VAT','TOTAL_INC_VAT') and p.sort_direction='DESC' then src.sort_numeric_key end desc nulls last,
        src.selection_key asc
      ) page_ordinal
    from candidate_page_source src cross join params p
  ),
  page_rows as materialized (
    select * from ordered_page_rows
    where page_ordinal <= (select page_size + 1 from params)
  ),
  visible_rows as materialized (
    select * from page_rows
    where page_ordinal <= (select page_size from params)
  ),
  totals as materialized (
    select
      count(*)::integer all_count,
      count(*) filter(where selectable)::integer ready_count,
      count(*) filter(where not selectable)::integer blocked_count,
      count(*) filter(where blocked_for_sending)::integer blocked_for_sending_count,
      count(*) filter(where generated_state='STALE')::integer stale_count,
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count
    from filtered_rows
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'invoice_id',invoice_id,
      'invoice_number',invoice_number,
      'document_revision',document_revision,
      'client_id',client_id,
      'client_name',client_name,
      'candidate_ids',candidate_ids,
      'candidate_names',candidate_names,
      'candidate_display',candidate_display,
      'week_ending_dates',week_ending_dates,
      'week_ending_date',week_ending_date,
      'week_ending_display',case when jsonb_array_length(week_ending_dates)>1 then 'Multiple weeks' else to_char(week_ending_date,'DD/MM/YYYY') end,
      'currency',currency,
      'total_ex_vat',total_ex_vat,
      'vat_amount',vat_amount,
      'total_inc_vat',total_inc_vat,
      'generated_state',generated_state,
      'row_status',row_status,
      'is_early',is_early,
      'selectable',selectable,
      'selected',selectable and last_selection_action <> 'EXCLUDE',
      'issue_blocker_codes',coalesce(issue_blocker_codes,'[]'::jsonb),
      'delivery_blocker_codes',coalesce(delivery_blocker_codes,'[]'::jsonb),
      'informational_codes',coalesce(informational_codes,'[]'::jsonb),
      'blocked_for_sending',blocked_for_sending,
      'can_issue_only',can_issue_only,
      'can_issue_and_deliver',can_issue_and_deliver,
      'active_issue_operation_id',active_issue_operation_id_text,
      'active_issue_status',active_issue_operation->>'status',
      'active_issue_operation',active_issue_operation,
      'active_document_operation_id',active_document_operation_id_text,
      'validation_detail',validation_detail,
      'support_readiness',support_readiness,
      'sort_tuple',jsonb_build_object(
        'sort_date',case when sort_date_key is not null then sort_date_key::text end,
        'sort_text',sort_text_key,
        'sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end,
        'selection_key',selection_key
      )
    ) order by page_ordinal),'[]'::jsonb) rows
    from visible_rows
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_CANDIDATES_V1',
    'action','ISSUE',
    'mode',v_mode,
    'snapshot_at_utc',coalesce(v_query->>'snapshot_at_utc',p_now_utc::text),
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',encode(digest(coalesce(v_filters,'{}'::jsonb)::text || '|' || coalesce(v_sort,'{}'::jsonb)::text || '|ISSUE','sha256'),'hex'),
    'rows',(select rows from row_json),
    'page',jsonb_build_object(
      'page_size',v_page_size,
      'has_more',(select count(*) from page_rows)>v_page_size,
      'next_cursor_values',case when (select count(*) from page_rows)>v_page_size then (
        select jsonb_build_object(
          'after_selection_key',selection_key,
          'after_sort_date',case when sort_date_key is not null then sort_date_key::text end,
          'after_sort_text',sort_text_key,
          'after_sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end
        )
        from visible_rows
        order by page_ordinal desc
        limit 1
      ) else null end
    ),
    'totals',jsonb_build_object(
      'all',(select all_count from totals),
      'ready',(select ready_count from totals),
      'blocked',(select blocked_count from totals),
      'blocked_for_sending',(select blocked_for_sending_count from totals),
      'stale',(select stale_count from totals),
      'in_progress',(select in_progress_count from totals)
    ),
    'facets',jsonb_build_object(),
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  return v_result;
end;
$function$;

revoke all on function private._invoice_batch_issue_candidate_rows_v1(jsonb,timestamptz) from public, anon, authenticated;
grant execute on function private._invoice_batch_issue_candidate_rows_v1(jsonb,timestamptz) to service_role;
