create or replace function private._invoice_batch_generate_candidate_rows_v1(
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
  v_fetch_limit integer;
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

  -- Validate the selection ledger up-front even when the current mode only pages rows.
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
    'WEEK_ENDING_DATE','CLIENT_NAME','CANDIDATE_NAME','TOTAL_EX_VAT','TOTAL_INC_VAT','STATUS'
  ) then
    raise exception using errcode='22023', message='INVOICE_BATCH_SORT_KEY_INVALID';
  end if;

  v_page_size := case
    when coalesce(v_query->>'page_size','') ~ '^[1-9][0-9]{0,8}$'
      then greatest(1,least((v_query->>'page_size')::integer,100))
    else 100
  end;
  v_after_key := nullif(coalesce(v_query#>>'{cursor,after_selection_key}',v_query#>>'{cursor,last_selection_key}',v_query->>'after_selection_key'), '');
  v_fetch_limit := case when v_mode = 'PAGE' then 20000 else 20000 end;

  -- Reuse the existing canonical candidate authority for the underlying grouping/economics.
  -- The public candidate RPC must keep its legacy NULL-p_query branch when it is later extended
  -- to call this helper, otherwise this helper would recurse.
  -- Always ask the legacy authority for the wider set; this helper applies the
  -- locked Batch early visibility rule itself so early rows are invisible when
  -- allow_early=false instead of being shown as blocked.
  v_legacy := public.invoice_batch_generate_candidates(true, v_fetch_limit, null::text[]);

  with
  params as materialized (
    select
      v_mode mode,
      v_allow_early allow_early,
      v_display_mode display_mode,
      v_page_size page_size,
      v_after_key after_key,
      lower(nullif(btrim(coalesce(v_filters->>'search','')),'')) search_text,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_from',''),'date')
        then (v_filters->>'week_ending_from')::date end week_ending_from,
      case when pg_input_is_valid(nullif(v_filters->>'week_ending_to',''),'date')
        then (v_filters->>'week_ending_to')::date end week_ending_to,
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
  legacy_groups as materialized (
    select
      client_json,
      client_json->>'client_id' client_id_text,
      client_json->>'client_name' client_name,
      grp.value group_json
    from legacy_clients
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(client_json->'groups')='array' then client_json->'groups' else '[]'::jsonb end
    ) grp(value)
  ),
  generation_timesheets as materialized (
    select
      lg.group_json->>'group_key' group_key,
      t.value timesheet_json,
      case when pg_input_is_valid(t.value->>'timesheet_id','uuid') then (t.value->>'timesheet_id')::uuid end timesheet_id,
      t.value->>'candidate_name' candidate_name,
      case when pg_input_is_valid(t.value->>'week_ending_date','date') then (t.value->>'week_ending_date')::date end week_ending_date,
      case when coalesce(t.value->>'total_charge_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (t.value->>'total_charge_ex_vat')::numeric else 0 end total_ex_vat
    from legacy_groups lg
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(lg.group_json->'timesheets')='array' then lg.group_json->'timesheets' else '[]'::jsonb end
    ) t(value)
  ),
  generation_candidate_ids as materialized (
    select gt.group_key,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(gt.candidate_name)) filter(where nullif(gt.candidate_name,'') is not null),'[]'::jsonb) candidate_names
    from generation_timesheets gt
    left join public.v_timesheets_summary_base s on s.timesheet_id=gt.timesheet_id
    group by gt.group_key
  ),
  generation_vat_members as materialized (
    select distinct on (lg.group_json->>'group_key', gt.timesheet_id)
      lg.group_json->>'group_key' group_key,
      gt.timesheet_id,
      coalesce(gt.total_ex_vat,0) total_ex_vat,
      coalesce(vat.vat_rate,0) vat_rate
    from legacy_groups lg
    left join lateral jsonb_array_elements(
      case when jsonb_typeof(lg.group_json->'canonical_source_members')='array'
        then lg.group_json->'canonical_source_members' else '[]'::jsonb end
    ) member(value) on true
    left join generation_timesheets gt
      on gt.group_key=lg.group_json->>'group_key'
     and gt.timesheet_id=case when pg_input_is_valid(coalesce(member.value->>'related_timesheet_id',member.value->>'source_id',''),'uuid')
       then coalesce(member.value->>'related_timesheet_id',member.value->>'source_id')::uuid end
    left join lateral private._invoice_generation_vat_policy_batch(jsonb_build_array(jsonb_build_object(
      'source_member_key',coalesce(nullif(member.value->>'source_member_key',''), lg.group_json->>'group_key'||':'||coalesce(gt.timesheet_id::text,'')),
      'source_type',member.value->>'source_type',
      'source_id',member.value->>'source_id',
      'timesheet_id',coalesce(member.value->>'related_timesheet_id',member.value->>'source_id'),
      'segment_id',nullif(member.value->>'segment_id',''),
      'effective_date',member.value->>'effective_settings_date'
    ))) vat on true
    where gt.timesheet_id is not null
    order by lg.group_json->>'group_key', gt.timesheet_id, coalesce(vat.vat_rate,0) desc
  ),
  generation_vat as materialized (
    select group_key,
      round(coalesce(sum(total_ex_vat * vat_rate / 100.0),0),2) vat_amount
    from generation_vat_members
    group by group_key
  ),
  create_rows as materialized (
    select
      'generate:'||coalesce(lg.group_json->>'group_key','') selection_key,
      'CREATE_INVOICE' row_kind,
      lg.group_json->>'group_key' scope_key,
      null::uuid invoice_id,
      case when pg_input_is_valid(lg.client_id_text,'uuid') then lg.client_id_text::uuid end client_id,
      lg.client_name,
      coalesce(gci.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(gci.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(gci.candidate_names,'[]'::jsonb))=1
        then gci.candidate_names->>0
        when jsonb_array_length(coalesce(gci.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(gci.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      coalesce((
        select jsonb_agg(distinct to_jsonb(gt.week_ending_date) order by to_jsonb(gt.week_ending_date))
        from generation_timesheets gt where gt.group_key=lg.group_json->>'group_key' and gt.week_ending_date is not null
      ),'[]'::jsonb) week_ending_dates,
      case when pg_input_is_valid(lg.group_json->>'week_ending_date','date') then (lg.group_json->>'week_ending_date')::date end week_ending_date,
      coalesce(nullif(lg.group_json#>>'{command_payload,currency}',''),'GBP') currency,
      case when coalesce(lg.group_json->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then round((lg.group_json->>'subtotal_ex_vat')::numeric,2) else 0 end total_ex_vat,
      coalesce(gv.vat_amount,0) vat_amount,
      round((case when coalesce(lg.group_json->>'subtotal_ex_vat','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (lg.group_json->>'subtotal_ex_vat')::numeric else 0 end) + coalesce(gv.vat_amount,0),2) total_inc_vat,
      'NOT_GENERATED' generation_state,
      coalesce(lg.group_json->>'blocker_code','') primary_blocker_code,
      coalesce((
        select jsonb_agg(to_jsonb(blocker_code) order by blocker_order, blocker_code)
        from (
          select blocker_code, min(blocker_order) blocker_order
          from (
            select 1 blocker_order, nullif(lg.group_json->>'blocker_code','') blocker_code
            union all
            select 2, nullif(lg.group_json#>>'{blocker_detail,code}','')
            union all
            select 10 + arr.ordinality::integer, nullif(arr.value,'')
            from jsonb_array_elements_text(
              case when jsonb_typeof(lg.group_json->'blocker_codes')='array'
                then lg.group_json->'blocker_codes' else '[]'::jsonb end
            ) with ordinality arr(value, ordinality)
            union all
            select 100 + src.ordinality::integer, nullif(src.value->>'code','')
            from jsonb_array_elements(
              case when jsonb_typeof(lg.group_json#>'{blocker_detail,sources}')='array'
                then lg.group_json#>'{blocker_detail,sources}' else '[]'::jsonb end
            ) with ordinality src(value, ordinality)
          ) raw_codes
          where blocker_code is not null
            and blocker_code not in ('EARLY_GENERATION_NOT_ALLOWED','SOURCE_ALREADY_INVOICED')
          group by blocker_code
        ) blocker_rows
      ),'[]'::jsonb) action_blocker_codes,
      case when coalesce(lg.group_json->>'active_generation_status','') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        then jsonb_build_array('GENERATING') else '[]'::jsonb end informational_codes,
      (coalesce(lg.group_json->>'active_generation_status','') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')) is_active,
      lg.group_json->>'active_generation_operation_id' active_operation_id_text,
      lg.group_json->>'active_generation_status' active_operation_status,
      lg.group_json->>'canonical_source_revision' source_revision,
      null::text document_revision,
      lg.group_json->'command_payload' command_payload,
      case when pg_input_is_valid(lg.group_json->>'week_ending_date','date')
        then (lg.group_json->>'week_ending_date')::date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early
    from legacy_groups lg
    left join generation_candidate_ids gci on gci.group_key=lg.group_json->>'group_key'
    left join generation_vat gv on gv.group_key=lg.group_json->>'group_key'
    where coalesce(lg.group_json->>'group_key','')<>''
      and coalesce(lg.group_json->>'blocker_code','') not in ('SOURCE_ALREADY_INVOICED','EARLY_GENERATION_NOT_ALLOWED')
  ),
  stale_invoice_timesheets as materialized (
    select l.invoice_id,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_id)) filter(where s.candidate_id is not null),'[]'::jsonb) candidate_ids,
      coalesce(jsonb_agg(distinct to_jsonb(s.candidate_name)) filter(where nullif(s.candidate_name,'') is not null),'[]'::jsonb) candidate_names,
      coalesce(jsonb_agg(distinct to_jsonb(s.week_ending_date)) filter(where s.week_ending_date is not null),'[]'::jsonb) week_ending_dates,
      min(s.week_ending_date) min_week_ending,
      max(s.week_ending_date) max_week_ending
    from public.invoice_lines l
    left join public.v_timesheets_summary_base s on s.timesheet_id=l.timesheet_id
    group by l.invoice_id
  ),
  stale_rows as materialized (
    select
      'invoice:'||i.id::text selection_key,
      case when coalesce(i.document_state,'')='FAILED' then 'RETRY_GENERATION' else 'REGENERATE_DRAFT' end row_kind,
      i.id::text scope_key,
      i.id invoice_id,
      i.client_id,
      c.name client_name,
      coalesce(sit.candidate_ids,'[]'::jsonb) candidate_ids,
      coalesce(sit.candidate_names,'[]'::jsonb) candidate_names,
      case when jsonb_array_length(coalesce(sit.candidate_names,'[]'::jsonb))=1
        then sit.candidate_names->>0
        when jsonb_array_length(coalesce(sit.candidate_names,'[]'::jsonb))>1
        then 'Multiple candidates ('||jsonb_array_length(sit.candidate_names)::text||')'
        else 'Unknown candidate' end candidate_display,
      coalesce(sit.week_ending_dates,'[]'::jsonb) week_ending_dates,
      coalesce(sit.min_week_ending, case when pg_input_is_valid(i.header_snapshot_json#>>'{meta,invoice_week_start}','date') then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date + 6 end) week_ending_date,
      coalesce(nullif(i.header_snapshot_json#>>'{meta,currency}',''), nullif(i.header_snapshot_json->>'currency',''), 'GBP') currency,
      round(coalesce(i.subtotal_ex_vat,0),2) total_ex_vat,
      round(coalesce(i.vat_amount,0),2) vat_amount,
      round(coalesce(i.total_inc_vat,coalesce(i.subtotal_ex_vat,0)+coalesce(i.vat_amount,0)),2) total_inc_vat,
      case
        when coalesce(i.document_state,'')='FAILED' then 'FAILED'
        when exists (
          select 1 from public.invoice_document_versions prior_state
          where prior_state.entity_type='INVOICE'
            and prior_state.entity_id=i.id
            and prior_state.purpose='DRAFT_PREVIEW'
        ) then 'STALE'
        else 'NOT_GENERATED'
      end generation_state,
      null::text primary_blocker_code,
      '[]'::jsonb action_blocker_codes,
      case when coalesce(active.status,'') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        then jsonb_build_array('GENERATING')
        when exists (
          select 1 from public.invoice_document_versions prior_state
          where prior_state.entity_type='INVOICE'
            and prior_state.entity_id=i.id
            and prior_state.purpose='DRAFT_PREVIEW'
        ) then jsonb_build_array('STALE')
        else jsonb_build_array('NOT_GENERATED') end informational_codes,
      (coalesce(active.status,'') in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')) is_active,
      active.operation_id::text active_operation_id_text,
      active.status active_operation_status,
      i.document_revision::text source_revision,
      i.document_revision::text document_revision,
      jsonb_build_object(
        'command_type','VIEW_INVOICE_DOCUMENT',
        'invoice_id',i.id,
        'purpose','DRAFT_PREVIEW',
        'expected_revision',i.document_revision,
        'source_revision',i.document_revision::text
      ) command_payload,
      coalesce(
        sit.max_week_ending,
        case when pg_input_is_valid(i.header_snapshot_json#>>'{meta,invoice_week_start}','date')
          then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date + 6 end
      ) >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date is_early
    from public.invoices i
    join public.clients c on c.id=i.client_id
    left join stale_invoice_timesheets sit on sit.invoice_id=i.id
    left join lateral (
      select o.id operation_id,o.status
      from public.invoice_operations o
      where o.operation_type='BUILD_DOCUMENT'
        and o.entity_type='INVOICE'
        and o.entity_id=i.id
        and o.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      order by o.created_at_utc desc,o.id desc
      limit 1
    ) active on true
    where i.type::text='INVOICE'
      and i.status::text in ('DRAFT','ON_HOLD')
      and coalesce(i.document_revision,0)>0
      and not exists (
        select 1
        from public.invoice_document_versions v
        where v.entity_type='INVOICE'
          and v.entity_id=i.id
          and v.purpose='DRAFT_PREVIEW'
          and v.source_revision=i.document_revision::text
          and v.template_version='invoice-professional-v2'
          and v.status='READY'
          and v.r2_key is not null
          and v.sha256 ~ '^[0-9a-f]{64}$'
          and coalesce(v.size_bytes,0)>0
          and coalesce(v.page_count,0)>0
      )

  ),
  all_rows_raw as materialized (
    select * from create_rows
    union all
    select * from stale_rows
  ),
  all_rows as materialized (
    select r.*,
      (jsonb_array_length(coalesce(r.action_blocker_codes,'[]'::jsonb))=0 and not r.is_active) selectable,
      case when jsonb_array_length(coalesce(r.action_blocker_codes,'[]'::jsonb))>0 then 'BLOCKED'
           when r.is_active then 'IN_PROGRESS'
           when r.generation_state='STALE' then 'STALE'
           when r.generation_state='FAILED' then 'FAILED'
           else 'READY' end row_status
    from all_rows_raw r
  ),
  filtered_rows as materialized (
    select r.*
    from all_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (p.display_mode='ALL'
        or (p.display_mode='READY' and r.selectable)
        or (p.display_mode='BLOCKED' and row_status='BLOCKED'))
      and (p.search_text is null
        or lower(coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.scope_key,'')) like '%'||p.search_text||'%')
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
        select 1 from jsonb_array_elements_text(coalesce(r.action_blocker_codes,'[]'::jsonb) || coalesce(r.informational_codes,'[]'::jsonb)) badge(code)
        where badge.code in (select upper(value) from jsonb_array_elements_text(p.blocker_codes))
      ))
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then coalesce(lower(fr.row_status), case when p.sort_direction='DESC' then '' else repeat('~',100) end) end sort_text_key,
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
         p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS')
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
           or (p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.after_sort_text is null)
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
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.sort_direction='ASC' then src.sort_text_key end asc nulls last,
        case when p.sort_key in ('CLIENT_NAME','CANDIDATE_NAME','STATUS') and p.sort_direction='DESC' then src.sort_text_key end desc nulls last,
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
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count,
      count(*) filter(where generation_state='NOT_GENERATED')::integer not_generated_count,
      count(*) filter(where generation_state='STALE')::integer stale_count,
      count(*) filter(where generation_state='FAILED')::integer failed_retryable_count
    from filtered_rows
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'row_kind',row_kind,
      'scope_key',scope_key,
      'invoice_id',invoice_id,
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
      'generation_state',generation_state,
      'row_status',row_status,
      'is_early',is_early,
      'selectable',selectable,
      'selected',selectable and last_selection_action <> 'EXCLUDE',
      'action_blocker_codes',coalesce(action_blocker_codes,'[]'::jsonb),
      'informational_codes',coalesce(informational_codes,'[]'::jsonb),
      'active_operation_id',active_operation_id_text,
      'active_operation_status',active_operation_status,
      'source_revision',source_revision,
      'document_revision',document_revision,
      'command_payload',command_payload,
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
    'action','GENERATE',
    'mode',v_mode,
    'snapshot_at_utc',coalesce(v_query->>'snapshot_at_utc',p_now_utc::text),
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',encode(digest(coalesce(v_filters,'{}'::jsonb)::text || '|' || coalesce(v_sort,'{}'::jsonb)::text || '|GENERATE','sha256'),'hex'),
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
      'in_progress',(select in_progress_count from totals),
      'not_generated',(select not_generated_count from totals),
      'stale',(select stale_count from totals),
      'failed_retryable',(select failed_retryable_count from totals)
    ),
    'facets',jsonb_build_object(),
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  return v_result;
end;
$function$;

revoke all on function private._invoice_batch_generate_candidate_rows_v1(jsonb,timestamptz) from public, anon, authenticated;
grant execute on function private._invoice_batch_generate_candidate_rows_v1(jsonb,timestamptz) to service_role;
