
create or replace function private._invoice_batch_issue_candidate_rows_v2(
  p_query jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_mode text;
  v_filters jsonb;
  v_sort jsonb;
  v_selection jsonb;
  v_selection_keys jsonb;
  v_expected_source_revisions jsonb;
  v_page_size integer;
  v_after_key text;
  v_allow_early boolean;
  v_display_mode text;
  v_input_snapshot jsonb;
  v_snapshot jsonb;
  v_snapshot_after jsonb;
  v_filter_hash text;
  v_query_hash text;
  v_selection_hash text;
  v_result jsonb;
begin
  v_query := private._invoice_batch_query_validate_v2(v_query, 'ISSUE');

  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if coalesce(v_query->>'contract_version','INVOICE_BATCH_QUERY_V2') <> 'INVOICE_BATCH_QUERY_V2' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_CONTRACT_INVALID';
  end if;

  v_mode := upper(coalesce(nullif(v_query->>'mode',''),'PAGE'));
  if v_mode not in ('PAGE','FACETS','SUMMARY','EXPAND_SELECTION','EXPLICIT_KEYS') then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_MODE_INVALID';
  end if;

  v_selection_keys := coalesce(v_query->'selection_keys','[]'::jsonb);
  v_expected_source_revisions := coalesce(
    v_query->'expected_source_revisions',
    '{}'::jsonb
  );
  if v_mode='EXPLICIT_KEYS' then
    if jsonb_typeof(v_selection_keys) is distinct from 'array'
       or jsonb_array_length(v_selection_keys) < 1
       or jsonb_array_length(v_selection_keys) > 100
       or jsonb_typeof(v_expected_source_revisions) is distinct from 'object'
       or exists (
         select 1
         from jsonb_array_elements(v_selection_keys) with ordinality key_item(value,ordinality)
         where jsonb_typeof(key_item.value) is distinct from 'string'
            or nullif(btrim(key_item.value #>> '{}'),'') is null
            or length(btrim(key_item.value #>> '{}')) > 512
       )
       or (
         select count(*) from jsonb_array_elements_text(v_selection_keys)
       ) <> (
         select count(distinct value)
         from jsonb_array_elements_text(v_selection_keys) explicit_key(value)
       )
       or exists (
         select 1
         from jsonb_array_elements_text(v_selection_keys) explicit_key(value)
         where nullif(v_expected_source_revisions->>explicit_key.value,'') is null
       ) then
      raise exception using
        errcode='22023',
        message='INVOICE_BATCH_EXPLICIT_KEYS_INVALID';
    end if;
  end if;

  v_filters := case when jsonb_typeof(v_query->'filters') = 'object' then v_query->'filters' else '{}'::jsonb end;
  v_sort := case when jsonb_typeof(v_query->'sort') = 'object' then v_query->'sort' else '{}'::jsonb end;
  v_selection := case when jsonb_typeof(v_query->'selection') = 'object' then v_query->'selection' else jsonb_build_object(
    'contract_version','INVOICE_BATCH_SELECTION_V2',
    'mode','IMPLICIT_ALL',
    'default_selected',true,
    'rules','[]'::jsonb
  ) end;

  perform 1 from private._invoice_batch_selection_rules_v2(v_selection) limit 1;

  v_allow_early := coalesce((v_filters->>'allow_early')::boolean,false);
  v_display_mode := upper(coalesce(
    nullif(v_filters->>'display_mode',''),
    'ALL'
  ));
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

  -- V8 Issue totals, selection summaries, status ordering and cursors always
  -- use the shared authoritative scalar classifier.
  -- PAGE, EXPAND_SELECTION and EXPLICIT_KEYS always obtain their bounded
  -- keyset first. The complete classifier remains the scalar authority for
  -- exact filtering, totals and group state, but it must never cause the
  -- rich presentation hydrator to receive an unrestricted invoice set.

  v_page_size := case
    when v_mode = 'EXPAND_SELECTION'
      then (v_query->>'page_size')::integer
    when v_mode = 'PAGE'
      then (v_query->>'page_size')::integer
    else 100
  end;
  v_after_key := nullif(v_query#>>'{cursor,after_selection_key}', '');

  v_input_snapshot := v_query->'snapshot';
  if v_input_snapshot is null
     or jsonb_typeof(v_input_snapshot) = 'null' then
    if v_mode <> 'PAGE' or v_after_key is not null then
      raise exception using errcode='22023', message='BATCH_SNAPSHOT_REQUIRED';
    end if;
    v_snapshot := private._invoice_candidate_snapshot_get_v2(
      'ISSUE',
      coalesce(p_now_utc,now())
    );
  else
    v_snapshot := private._invoice_candidate_snapshot_verify_v2(
      'ISSUE',
      v_input_snapshot,
      coalesce(p_now_utc,now())
    );
  end if;

  v_filter_hash := private._invoice_batch_hash_v2(jsonb_build_object(
    'action','ISSUE',
    'filters',v_filters,
    'sort',v_sort
  ));
  v_query_hash := private._invoice_batch_hash_v2(jsonb_build_object(
    'contract_version','INVOICE_BATCH_QUERY_V2',
    'action','ISSUE',
    'filters',v_filters,
    'sort',v_sort,
    'snapshot',jsonb_build_object(
      'contract_version',v_snapshot->>'contract_version',
      'action',v_snapshot->>'action',
      'at_utc',v_snapshot->>'at_utc',
      'revision',v_snapshot->>'revision',
      'expires_at_utc',v_snapshot->>'expires_at_utc',
      'key_id',v_snapshot->>'key_id'
    )
  ));
  v_selection_hash := private._invoice_batch_hash_v2(v_selection);

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
      case when jsonb_typeof(v_filters->'invoice_streams')='array' then v_filters->'invoice_streams' else '[]'::jsonb end invoice_streams,
      coalesce(nullif(upper(v_sort->>'group_preset'),''),'WEEK_CLIENT_CANDIDATE') group_preset,
      coalesce(nullif(upper(v_sort->>'sort_key'),''),'WEEK_ENDING_DATE') sort_key,
      case when upper(coalesce(v_sort->>'sort_direction','ASC'))='DESC' then 'DESC' else 'ASC' end sort_direction,
      case when pg_input_is_valid(
        nullif(coalesce(v_query#>>'{cursor,after_sort_date}',''),''),
        'date'
      )
        then (v_query#>>'{cursor,after_sort_date}')::date end after_sort_date,
      nullif(coalesce(v_query#>>'{cursor,after_sort_text}',''),'') after_sort_text,
      case when coalesce(v_query#>>'{cursor,after_sort_numeric}','') ~ '^[+-]?[0-9]+([.][0-9]+)?$'
        then (v_query#>>'{cursor,after_sort_numeric}')::numeric end after_sort_numeric,
      lower(nullif(btrim(coalesce(v_query#>>'{facet_request,search}','')),'')) facet_search,
      case when coalesce(v_query#>>'{facet_request,limit_per_kind}','') ~ '^[1-9][0-9]{0,2}$'
        then least((v_query#>>'{facet_request,limit_per_kind}')::integer,100)
        else 100 end facet_limit,
      case when jsonb_typeof(v_query#>'{facet_request,kinds}')='array'
        then v_query#>'{facet_request,kinds}'
        else '["CLIENTS","CANDIDATES","WEEK_ENDINGS","STATUSES","BLOCKERS"]'::jsonb end facet_kinds,
      lower(nullif(v_query#>>'{facet_request,cursors,clients,after_label}','')) facet_client_after_label,
      nullif(v_query#>>'{facet_request,cursors,clients,after_id}','') facet_client_after_id,
      lower(nullif(v_query#>>'{facet_request,cursors,candidates,after_label}','')) facet_candidate_after_label,
      nullif(v_query#>>'{facet_request,cursors,candidates,after_id}','') facet_candidate_after_id,
      case when pg_input_is_valid(
        coalesce(v_query#>>'{facet_request,cursors,week_endings,after_value}',''),
        'date'
      ) then (v_query#>>'{facet_request,cursors,week_endings,after_value}')::date end facet_week_after_value,
      nullif(v_query#>>'{facet_request,cursors,statuses,after_code}','') facet_status_after_code,
      nullif(v_query#>>'{facet_request,cursors,blockers,after_code}','') facet_blocker_after_code
  ),
  selection_rules as materialized (
    select * from private._invoice_batch_selection_rules_v2(v_selection)
  ),
  candidate_keys as materialized (
    select key_row.*
    from (
      select 1
      where v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
    ) gate
    cross join lateral private._invoice_batch_issue_candidate_key_rows_v2(
      v_query,
      coalesce(p_now_utc,now())
    ) key_row
  ),
  issue_scope_request as materialized (
    select
      case
        when v_mode in ('FACETS','SUMMARY') then '{}'::uuid[]
        else coalesce(array_agg(k.invoice_id order by k.page_ordinal)
          filter (where k.page_ordinal<=v_page_size),'{}'::uuid[])
      end invoice_ids
    from candidate_keys k
  ),
  source_weeks as materialized (
    select
      '{}'::jsonb client_json,
      source.client_id::text client_id_text,
      source.client_name,
      source.invoice_week_start,
      source.week_ending_date,
      source.invoice_json
    from issue_scope_request request
    cross join lateral private._invoice_batch_issue_source_rows_for_ids_v2(
      request.invoice_ids,
      true,
      coalesce(p_now_utc,now())
    ) source
    where v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
      and cardinality(request.invoice_ids)>0
  ),
  invoice_rows_raw as materialized (
    select
      'invoice:'||coalesce(inv.value->>'invoice_id','') selection_key,


      case when pg_input_is_valid(inv.value->>'invoice_id','uuid') then (inv.value->>'invoice_id')::uuid end invoice_id,
      inv.value->>'invoice_no' invoice_number,
      case when pg_input_is_valid(lw.client_id_text,'uuid') then lw.client_id_text::uuid end client_id,
      lw.client_name,
      case when pg_input_is_valid(inv.value->>'document_revision','int8') then (inv.value->>'document_revision')::bigint end document_revision,
      lw.week_ending_date,
      case when lw.week_ending_date is not null
        then lw.week_ending_date >= (coalesce(p_now_utc,now()) at time zone 'Europe/London')::date
        else false end is_early,
      coalesce(nullif(inv.value->>'currency',''),'GBP') currency,
      upper(coalesce(nullif(inv.value->>'invoice_stream',''),'NORMAL')) invoice_stream,
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
    from source_weeks lw
    cross join lateral (select lw.invoice_json value) inv
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
      coalesce(r.hard_blocker_codes,'[]'::jsonb) issue_blocker_codes,
      case when r.blocked_for_sending then jsonb_build_array('BLOCKED_FOR_SENDING') else '[]'::jsonb end
        || coalesce(r.document_dependency_codes,'[]'::jsonb)
        || coalesce(r.warning_codes,'[]'::jsonb) informational_codes,
      (r.can_issue_only
        and jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))=0
        and not r.is_active_issue) selectable,
      case when r.is_active_issue then 'IN_PROGRESS'
           when not r.can_issue_only or jsonb_array_length(coalesce(r.hard_blocker_codes,'[]'::jsonb))>0
             then 'BLOCKED'
           else 'READY' end row_status
    from rows_with_state r
    where r.invoice_status in ('DRAFT','ON_HOLD')
      and nullif(r.invoice_number,'') is not null
  ),
  classification_source as materialized (
    -- PAGE-family requests reuse the exact candidate payload that the keyset
    -- helper already classified. FACETS/SUMMARY do not invoke the keyset
    -- helper, so they retain one direct full-scope classification pass.
    select key_row.selection_key,key_row.candidate_json
    from candidate_keys key_row
    where key_row.page_ordinal<=v_page_size
    union all
    select candidate.selection_key,candidate.candidate_json
    from (
      select 1
      where v_mode in ('FACETS','SUMMARY')
    ) gate
    cross join lateral private._invoice_batch_issue_classification_v2(
      true,
      null,
      coalesce(p_now_utc,statement_timestamp())
    ) candidate
  ),
  authoritative_rows as materialized (
    select
      candidate.candidate_json->>'selection_key' selection_key,
      (candidate.candidate_json->>'invoice_id')::uuid invoice_id,
      candidate.candidate_json->>'invoice_number' invoice_number,
      (candidate.candidate_json->>'client_id')::uuid client_id,
      candidate.candidate_json->>'client_name' client_name,
      (candidate.candidate_json->>'document_revision')::bigint
        document_revision,
      case
        when pg_input_is_valid(
          coalesce(candidate.candidate_json->>'week_ending_date',''),
          'date'
        )
          then (candidate.candidate_json->>'week_ending_date')::date
      end week_ending_date,
      coalesce(
        (candidate.candidate_json->>'is_early')::boolean,
        false
      ) is_early,
      coalesce(
        nullif(candidate.candidate_json->>'currency',''),
        'GBP'
      ) currency,
      upper(coalesce(
        nullif(candidate.candidate_json->>'invoice_stream',''),
        'NORMAL'
      )) invoice_stream,
      coalesce(
        (candidate.candidate_json->>'total_ex_vat')::numeric,
        0
      ) total_ex_vat,
      coalesce(
        (candidate.candidate_json->>'vat_amount')::numeric,
        0
      ) vat_amount,
      coalesce(
        (candidate.candidate_json->>'total_inc_vat')::numeric,
        0
      ) total_inc_vat,
      candidate.candidate_json->>'preview_document_state'
        preview_document_state,
      candidate.candidate_json->>'invoice_status' invoice_status,
      coalesce(
        candidate.candidate_json->'hard_blocker_codes',
        '[]'::jsonb
      ) hard_blocker_codes,
      coalesce(
        candidate.candidate_json->'document_dependency_codes',
        '[]'::jsonb
      ) document_dependency_codes,
      coalesce(
        candidate.candidate_json->'delivery_blocker_codes',
        '[]'::jsonb
      ) delivery_blocker_codes,
      coalesce(
        candidate.candidate_json->'warning_codes',
        '[]'::jsonb
      ) warning_codes,
      coalesce(
        (candidate.candidate_json->>'can_issue_only')::boolean,
        false
      ) can_issue_only,
      coalesce(
        (candidate.candidate_json->>'can_issue_and_deliver')::boolean,
        false
      ) can_issue_and_deliver,
      candidate.candidate_json->'validation_detail'
        validation_detail,
      candidate.candidate_json->'support_readiness'
        support_readiness,
      candidate.candidate_json->>'active_issue_operation_id'
        active_issue_operation_id_text,
      candidate.candidate_json->'active_issue_operation'
        active_issue_operation,
      candidate.candidate_json->>'active_document_operation_id'
        active_document_operation_id_text,
      candidate.candidate_json->'last_issue_error'
        last_issue_error,
      candidate.candidate_json->'last_document_error'
        last_document_error,
      coalesce(
        candidate.candidate_json->'candidate_ids',
        '[]'::jsonb
      ) candidate_ids,
      coalesce(
        candidate.candidate_json->'candidate_names',
        '[]'::jsonb
      ) candidate_names,
      candidate.candidate_json->>'candidate_display'
        candidate_display,
      coalesce(
        candidate.candidate_json->'week_ending_dates',
        '[]'::jsonb
      ) week_ending_dates,
      candidate.candidate_json->>'generated_state'
        generated_state,
      coalesce(
        (candidate.candidate_json->>'blocked_for_sending')::boolean,
        false
      ) blocked_for_sending,
      coalesce(
        candidate.candidate_json->'issue_blocker_codes',
        '[]'::jsonb
      ) issue_blocker_codes,
      coalesce(
        candidate.candidate_json->'informational_codes',
        '[]'::jsonb
      ) informational_codes,
      coalesce(
        (candidate.candidate_json->>'selectable')::boolean,
        false
      ) selectable,
      candidate.candidate_json->>'row_status' row_status
    from classification_source candidate
  ),
  filter_match_rows as materialized (
    select
      r.*,
      (
        jsonb_array_length(p.client_ids)=0
        or r.client_id::text in (
          select value from jsonb_array_elements_text(p.client_ids)
        )
      ) client_filter_match,
      (
        jsonb_array_length(p.candidate_ids)=0
        or exists (
          select 1
          from jsonb_array_elements_text(r.candidate_ids) row_candidate(id)
          where row_candidate.id in (
            select value from jsonb_array_elements_text(p.candidate_ids)
          )
        )
      ) candidate_filter_match,
      (
        jsonb_array_length(p.week_endings)=0
        or r.week_ending_date::text in (
          select value from jsonb_array_elements_text(p.week_endings)
        )
      ) week_filter_match,
      (
        jsonb_array_length(p.status_codes)=0
        or r.row_status in (
          select upper(value)
          from jsonb_array_elements_text(p.status_codes)
        )
      ) status_filter_match,
      (
        jsonb_array_length(p.blocker_codes)=0
        or exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(r.issue_blocker_codes,'[]'::jsonb)
            || coalesce(r.delivery_blocker_codes,'[]'::jsonb)
            || coalesce(r.informational_codes,'[]'::jsonb)
          ) badge(code)
          where badge.code in (
            select upper(value)
            from jsonb_array_elements_text(p.blocker_codes)
          )
        )
      ) blocker_filter_match
    from authoritative_rows r
    cross join params p
    where (p.allow_early or not coalesce(r.is_early,false))
      and (
        jsonb_array_length(p.invoice_streams)=0
        or r.invoice_stream in (
          select upper(value)
          from jsonb_array_elements_text(p.invoice_streams)
        )
      )
      and (
        v_mode <> 'EXPLICIT_KEYS'
        or r.selection_key in (
          select value
          from jsonb_array_elements_text(v_selection_keys)
        )
      )
      and (p.search_text is null


        or lower(coalesce(r.invoice_number,'')||' '||coalesce(r.client_name,'')||' '||coalesce(r.candidate_display,'')||' '||coalesce(r.invoice_id::text,'')) like '%'||p.search_text||'%')
      and (p.week_ending_from is null or r.week_ending_date >= p.week_ending_from)
      and (p.week_ending_to is null or r.week_ending_date <= p.week_ending_to)
  ),
  scope_rows as materialized (
    select r.*
    from filter_match_rows r
    where r.client_filter_match
      and r.candidate_filter_match
      and r.week_filter_match
      and r.status_filter_match
      and r.blocker_filter_match
  ),
  facet_client_rows as materialized (
    select r.* from filter_match_rows r
    where r.candidate_filter_match and r.week_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_candidate_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.week_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_week_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.status_filter_match and r.blocker_filter_match
  ),
  facet_status_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.week_filter_match and r.blocker_filter_match
  ),
  facet_blocker_rows as materialized (
    select r.* from filter_match_rows r
    where r.client_filter_match and r.candidate_filter_match
      and r.week_filter_match and r.status_filter_match
  ),
  facet_client_values_base as materialized (
    select
      client_id,
      min(coalesce(nullif(client_name,''),client_id::text)) label,
      count(*)::integer row_count
    from facet_client_rows
    where client_id is not null
    group by client_id
  ),
  facet_client_values as materialized (
    select b.*,
      row_number() over(order by lower(b.label),b.client_id) facet_ordinal,
      count(*) over() facet_total
    from facet_client_values_base b
    cross join params p
    where (p.facet_search is null
        or lower(b.label) like '%'||p.facet_search||'%'
        or b.client_id::text like p.facet_search||'%')
      and (p.facet_client_after_label is null
        or (lower(b.label),b.client_id::text)>
           (p.facet_client_after_label,coalesce(p.facet_client_after_id,'')))
  ),
  facet_candidate_values_base as materialized (
    select
      candidate.value candidate_id,
      min(coalesce(
        nullif(r.candidate_names->>(candidate.ordinality::integer-1),''),
        candidate.value
      )) label,
      count(distinct r.selection_key)::integer row_count
    from facet_candidate_rows r
    cross join lateral jsonb_array_elements_text(
      coalesce(r.candidate_ids,'[]'::jsonb)
    ) with ordinality candidate(value,ordinality)
    group by candidate.value
  ),
  facet_candidate_values as materialized (
    select b.*,
      row_number() over(order by lower(b.label),b.candidate_id) facet_ordinal,
      count(*) over() facet_total
    from facet_candidate_values_base b
    cross join params p
    where (p.facet_search is null
        or lower(b.label) like '%'||p.facet_search||'%'
        or b.candidate_id like p.facet_search||'%')
      and (p.facet_candidate_after_label is null
        or (lower(b.label),b.candidate_id)>
           (p.facet_candidate_after_label,coalesce(p.facet_candidate_after_id,'')))
  ),
  facet_week_values as materialized (
    select b.*,
      row_number() over(order by b.week_ending_date desc) facet_ordinal,
      count(*) over() facet_total
    from (
      select week_ending_date,count(*)::integer row_count
      from facet_week_rows
      where week_ending_date is not null
      group by week_ending_date
    ) b
    cross join params p
    where (p.facet_search is null
        or to_char(b.week_ending_date,'DD/MM/YYYY') like '%'||p.facet_search||'%'
        or b.week_ending_date::text like '%'||p.facet_search||'%')
      and (p.facet_week_after_value is null
        or b.week_ending_date<p.facet_week_after_value)
  ),
  facet_status_values as materialized (
    select b.*,
      row_number() over(order by b.row_status) facet_ordinal,
      count(*) over() facet_total
    from (
      select row_status,count(*)::integer row_count
      from facet_status_rows
      group by row_status
    ) b
    cross join params p
    where (p.facet_search is null
        or lower(b.row_status) like '%'||p.facet_search||'%'
        or lower(replace(b.row_status,'_',' ')) like '%'||p.facet_search||'%')
      and (p.facet_status_after_code is null
        or b.row_status>p.facet_status_after_code)
  ),
  facet_blocker_values as materialized (
    select b.*,
      row_number() over(order by b.code) facet_ordinal,
      count(*) over() facet_total
    from (
      select badge.code,count(distinct r.selection_key)::integer row_count
      from facet_blocker_rows r
      cross join lateral jsonb_array_elements_text(
        coalesce(r.issue_blocker_codes,'[]'::jsonb)
        || coalesce(r.delivery_blocker_codes,'[]'::jsonb)
        || coalesce(r.informational_codes,'[]'::jsonb)
      ) badge(code)
      group by badge.code
    ) b
    cross join params p
    where (p.facet_search is null
        or lower(b.code) like '%'||p.facet_search||'%'
        or lower(replace(b.code,'_',' ')) like '%'||p.facet_search||'%')
      and (p.facet_blocker_after_code is null
        or b.code>p.facet_blocker_after_code)
  ),
  grouped_scope_rows as materialized (
    select
      r.*,
      private._invoice_batch_hash_v2(jsonb_build_object(
        'action','ISSUE',
        'group_preset',p.group_preset,
        'status_code',case when p.group_preset='STATUS_WEEK_CLIENT' then r.row_status end,
        'week_ending_date',r.week_ending_date,
        'client_id',r.client_id,
        'candidate_ids',case
          when p.group_preset='STATUS_WEEK_CLIENT' then '[]'::jsonb
          else coalesce(r.candidate_ids,'[]'::jsonb)
        end
      )) group_key
    from scope_rows r
    cross join params p
  ),
  selection_scope_rows as materialized (
    select
      fr.*,
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
           or (sr.selector_type='STATUS' and sr.status_code=fr.row_status)
           or (sr.selector_type='WEEK_CLIENT' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='WEEK_CLIENT_CANDIDATE' and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id and exists (
             select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
             where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
           ))
           or (sr.selector_type='STATUS_WEEK' and sr.status_code=fr.row_status and sr.week_ending_date=fr.week_ending_date)
           or (sr.selector_type='STATUS_WEEK_CLIENT' and sr.status_code=fr.row_status and sr.week_ending_date=fr.week_ending_date and sr.client_id=fr.client_id)
           or (sr.selector_type='DIMENSION_GROUP'
             and (sr.week_ending_date is null or sr.week_ending_date=fr.week_ending_date)
             and (sr.client_id is null or sr.client_id=fr.client_id)
             and (sr.status_code is null or sr.status_code=fr.row_status)
             and (sr.candidate_id is null or exists (
               select 1 from jsonb_array_elements_text(fr.candidate_ids) cid(value)
               where pg_input_is_valid(cid.value,'uuid') and cid.value::uuid=sr.candidate_id
             )))
        order by sr.rule_sequence desc
        limit 1
      ),'INCLUDE') last_selection_action
    from grouped_scope_rows fr
  ),
  filtered_rows as materialized (
    select r.*
    from selection_scope_rows r
    cross join params p
    where (
      v_mode='EXPAND_SELECTION'
      or p.display_mode='ALL'
      or (p.display_mode='READY' and r.selectable)
      or (
        p.display_mode='BLOCKED'
        and (
          r.row_status in ('BLOCKED','STALE','FAILED')
          or r.blocked_for_sending
        )
      )
    )
      and (
        v_mode<>'EXPAND_SELECTION'
        or (
          r.selectable
          and r.last_selection_action<>'EXCLUDE'
        )
      )
  ),
  sortable_rows as materialized (
    select fr.*,
      case when p.sort_key='WEEK_ENDING_DATE' then coalesce(fr.week_ending_date, case when p.sort_direction='DESC' then date '0001-01-01' else date '9999-12-31' end) end sort_date_key,
      case when p.sort_key='CLIENT_NAME' then coalesce(lower(fr.client_name), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='CANDIDATE_NAME' then coalesce(lower(fr.candidate_display), case when p.sort_direction='DESC' then '' else repeat('~',100) end)
           when p.sort_key='STATUS' then
             lpad((case
               when fr.row_status='READY' and not fr.blocked_for_sending then 10
               when fr.row_status='READY' and fr.blocked_for_sending then 20
               when fr.row_status='STALE' then 30
               when fr.row_status='IN_PROGRESS' then 40
               when fr.row_status='FAILED' then 50
               else 60
             end)::text,3,'0')||'|'||lower(coalesce(fr.row_status,'BLOCKED'))
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
    select * from cursor_filtered_rows
  ),
  candidate_page_source as materialized (
    select * from selected_rows where v_mode='EXPAND_SELECTION'
    union all
    select * from selected_rows where v_mode in('PAGE','EXPLICIT_KEYS')
  ),
  ordered_page_rows as materialized (
    select src.*,
      row_number() over (order by
        case when v_mode='EXPAND_SELECTION' then src.selection_key end asc,
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
  visible_invoice_request as materialized (
    select array_agg(invoice_id order by invoice_id) invoice_ids
    from visible_rows
    where invoice_id is not null
  ),
  nonempty_visible_invoice_request as materialized (
    select invoice_ids
    from visible_invoice_request
    where cardinality(invoice_ids)>0
  ),
  visible_support as materialized (
    select
      (source.invoice_json->>'invoice_id')::uuid support_invoice_id,
      source.invoice_json
    from nonempty_visible_invoice_request request
    cross join lateral private._invoice_batch_issue_source_rows_for_ids_v2(
      request.invoice_ids,
      true,
      coalesce(p_now_utc,statement_timestamp())
    ) source
  ),
  totals as materialized (
    select
      count(*)::integer all_count,
      count(*) filter(where selectable)::integer ready_count,
      count(*) filter(where selectable and last_selection_action <> 'EXCLUDE')::integer selected_count,
      count(*) filter(where not selectable)::integer blocked_count,
      count(*) filter(where blocked_for_sending)::integer blocked_for_sending_count,
      count(*) filter(where generated_state='STALE')::integer stale_count,
      count(*) filter(where row_status='IN_PROGRESS')::integer in_progress_count
    from selection_scope_rows
  ),
  display_totals as materialized (
    select count(*)::integer display_count
    from filtered_rows
  ),
  visible_group_keys as materialized (
    select distinct group_key
    from visible_rows
    where v_mode = 'PAGE'
  ),
  page_group_rollup as materialized (
    select
      r.group_key,
      min(r.week_ending_date) week_ending_date,
      (array_agg(r.client_id order by r.selection_key))[1] client_id,
      min(r.row_status) row_status,
      (array_agg(r.candidate_ids order by r.selection_key))[1] candidate_ids,
      count(*)::integer row_total,
      count(*) filter (where r.selectable)::integer eligible_total,
      count(*) filter (
        where r.selectable and r.last_selection_action <> 'EXCLUDE'
      )::integer selected_total,
      count(*) filter (where v.group_key is not null)::integer visible_total
    from selection_scope_rows r
    join visible_group_keys g on g.group_key = r.group_key
    left join visible_rows v
      on v.group_key = r.group_key
     and v.selection_key = r.selection_key
    group by r.group_key
  ),
  page_group_selection_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'group_key',g.group_key,
      'selector',case
        when p.group_preset='STATUS_WEEK_CLIENT' then jsonb_build_object(
          'type','STATUS_WEEK_CLIENT',
          'status_code',g.row_status,
          'week_ending_date',g.week_ending_date,
          'client_id',g.client_id
        )
        when jsonb_array_length(coalesce(g.candidate_ids,'[]'::jsonb))=1
          then jsonb_build_object(
            'type','WEEK_CLIENT_CANDIDATE',
            'week_ending_date',g.week_ending_date,
            'client_id',g.client_id,
            'candidate_id',g.candidate_ids->>0
          )
        else jsonb_build_object(
          'type','WEEK_CLIENT',
          'week_ending_date',g.week_ending_date,
          'client_id',g.client_id
        )
      end,
      'eligible_total',g.eligible_total,
      'selected_total',g.selected_total,
      'state',case
        when g.eligible_total = 0 then 'DISABLED'
        when g.selected_total = 0 then 'UNCHECKED'
        when g.selected_total = g.eligible_total then 'CHECKED'
        else 'INDETERMINATE'
      end,
      'has_hidden_override',
        g.visible_total < g.row_total
        and g.selected_total not in (0,g.eligible_total)
    ) order by g.group_key),'[]'::jsonb) groups
    from page_group_rollup g
    cross join params p
  ),
  requested_group_selectors as materialized (
    select
      selector.ordinality::integer request_ordinal,
      selector.value selector,
      upper(selector.value->>'type') selector_type,
      nullif(btrim(selector.value->>'selection_key'),'') selection_key,
      case when selector.value ? 'week_ending_date'
        then (selector.value->>'week_ending_date')::date end week_ending_date,
      case when selector.value ? 'client_id'
        then (selector.value->>'client_id')::uuid end client_id,
      case when selector.value ? 'candidate_id'
        then (selector.value->>'candidate_id')::uuid end candidate_id,
      nullif(upper(btrim(selector.value->>'status_code')),'') status_code
    from jsonb_array_elements(coalesce(v_query->'group_selectors','[]'::jsonb))
      with ordinality selector(value, ordinality)
    where v_mode = 'SUMMARY'
  ),
  requested_group_members as materialized (
    select
      requested.request_ordinal,
      requested.selector,
      row_scope.*
    from requested_group_selectors requested
    join selection_scope_rows row_scope on row_scope.selectable
      and (
        (requested.selector_type='ROW'
          and requested.selection_key=row_scope.selection_key)
        or (requested.selector_type='WEEK'
          and requested.week_ending_date=row_scope.week_ending_date)
        or (requested.selector_type='CLIENT'
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='CANDIDATE' and exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(row_scope.candidate_ids,'[]'::jsonb)
          ) candidate(value)
          where pg_input_is_valid(candidate.value,'uuid')
            and candidate.value::uuid=requested.candidate_id
        ))
        or (requested.selector_type='STATUS'
          and requested.status_code=row_scope.row_status)
        or (requested.selector_type='WEEK_CLIENT'
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='WEEK_CLIENT_CANDIDATE'
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id
          and exists (
            select 1
            from jsonb_array_elements_text(
              coalesce(row_scope.candidate_ids,'[]'::jsonb)
            ) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=requested.candidate_id
          ))
        or (requested.selector_type='STATUS_WEEK'
          and requested.status_code=row_scope.row_status
          and requested.week_ending_date=row_scope.week_ending_date)
        or (requested.selector_type='STATUS_WEEK_CLIENT'
          and requested.status_code=row_scope.row_status
          and requested.week_ending_date=row_scope.week_ending_date
          and requested.client_id=row_scope.client_id)
        or (requested.selector_type='DIMENSION_GROUP'
          and (requested.week_ending_date is null or requested.week_ending_date=row_scope.week_ending_date)
          and (requested.client_id is null or requested.client_id=row_scope.client_id)
          and (requested.status_code is null or requested.status_code=row_scope.row_status)
          and (requested.candidate_id is null or exists (
            select 1 from jsonb_array_elements_text(row_scope.candidate_ids) candidate(value)
            where pg_input_is_valid(candidate.value,'uuid')
              and candidate.value::uuid=requested.candidate_id
          )))
      )
  ),
  requested_group_base as materialized (
    select
      requested.*,
      coalesce((
        select rule.action
        from selection_rules rule
        where exists (
          select 1
          from requested_group_members member
          where member.request_ordinal=requested.request_ordinal
        )
          and not exists (
            select 1
            from requested_group_members member
            where member.request_ordinal=requested.request_ordinal
              and (
                (rule.selector_type='ROW'
                  and rule.selection_key=member.selection_key)
                or (rule.selector_type='WEEK'
                  and rule.week_ending_date=member.week_ending_date)
                or (rule.selector_type='CLIENT'
                  and rule.client_id=member.client_id)
                or (rule.selector_type='CANDIDATE' and exists (
                  select 1
                  from jsonb_array_elements_text(
                    coalesce(member.candidate_ids,'[]'::jsonb)
                  ) candidate(value)
                  where pg_input_is_valid(candidate.value,'uuid')
                    and candidate.value::uuid=rule.candidate_id
                ))
                or (rule.selector_type='STATUS'
                  and rule.status_code=member.row_status)
                or (rule.selector_type='WEEK_CLIENT'
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id)
                or (rule.selector_type='WEEK_CLIENT_CANDIDATE'
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id
                  and exists (
                    select 1
                    from jsonb_array_elements_text(
                      coalesce(member.candidate_ids,'[]'::jsonb)
                    ) candidate(value)
                    where pg_input_is_valid(candidate.value,'uuid')
                      and candidate.value::uuid=rule.candidate_id
                  ))
                or (rule.selector_type='STATUS_WEEK'
                  and rule.status_code=member.row_status
                  and rule.week_ending_date=member.week_ending_date)
                or (rule.selector_type='STATUS_WEEK_CLIENT'
                  and rule.status_code=member.row_status
                  and rule.week_ending_date=member.week_ending_date
                  and rule.client_id=member.client_id)
                or (rule.selector_type='DIMENSION_GROUP'
                  and (rule.week_ending_date is null or rule.week_ending_date=member.week_ending_date)
                  and (rule.client_id is null or rule.client_id=member.client_id)
                  and (rule.status_code is null or rule.status_code=member.row_status)
                  and (rule.candidate_id is null or exists (
                    select 1 from jsonb_array_elements_text(member.candidate_ids) candidate(value)
                    where pg_input_is_valid(candidate.value,'uuid')
                      and candidate.value::uuid=rule.candidate_id
                  )))
              ) is not true
          )
        order by rule.rule_sequence desc
        limit 1
      ),'INCLUDE') base_action
    from requested_group_selectors requested
  ),
  requested_group_rollup as materialized (
    select
      requested.request_ordinal,
      requested.selector,
      case when count(distinct member.group_key)=1
        then min(member.group_key) end group_key,
      count(member.selection_key)::integer eligible_total,
      count(member.selection_key) filter (
        where member.last_selection_action <> 'EXCLUDE'
      )::integer selected_total,
      coalesce(bool_or(
        member.last_selection_action is distinct from requested.base_action
      ) filter (where member.selection_key is not null),false)
        has_hidden_override
    from requested_group_base requested
    left join requested_group_members member
      on member.request_ordinal=requested.request_ordinal
    group by
      requested.request_ordinal,
      requested.selector,
      requested.base_action
  ),
  summary_group_selection_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selector',rollup.selector,
      'group_key',rollup.group_key,
      'eligible_total',rollup.eligible_total,
      'selected_total',rollup.selected_total,
      'state',case
        when rollup.eligible_total=0 then 'DISABLED'
        when rollup.selected_total=0 then 'UNCHECKED'
        when rollup.selected_total=rollup.eligible_total
          and not rollup.has_hidden_override then 'CHECKED'
        else 'INDETERMINATE'
      end,
      'has_hidden_override',rollup.has_hidden_override
    ) order by rollup.request_ordinal),'[]'::jsonb) groups
    from requested_group_rollup rollup
  ),
  group_selection_json as materialized (
    select case
      when v_mode='SUMMARY' then summary_groups.groups
      else page_groups.groups
    end groups
    from page_group_selection_json page_groups
    cross join summary_group_selection_json summary_groups
  ),
  facet_json as materialized (
    select jsonb_strip_nulls(jsonb_build_object(
      'clients',case when p.facet_kinds ? 'CLIENTS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'id',f.client_id,'label',f.label,'count',f.row_count
        ) order by f.facet_ordinal) from facet_client_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_client_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_client_values f),false) then (
          select jsonb_build_object('after_label',lower(f.label),'after_id',f.client_id)
          from facet_client_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'candidates',case when p.facet_kinds ? 'CANDIDATES' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'id',f.candidate_id,'label',f.label,'count',f.row_count
        ) order by f.facet_ordinal) from facet_candidate_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_candidate_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_candidate_values f),false) then (
          select jsonb_build_object('after_label',lower(f.label),'after_id',f.candidate_id)
          from facet_candidate_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'week_endings',case when p.facet_kinds ? 'WEEK_ENDINGS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'value',f.week_ending_date,'label',to_char(f.week_ending_date,'DD/MM/YYYY'),
          'count',f.row_count
        ) order by f.facet_ordinal) from facet_week_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_week_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_week_values f),false) then (
          select jsonb_build_object('after_value',f.week_ending_date)
          from facet_week_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'statuses',case when p.facet_kinds ? 'STATUSES' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'code',f.row_status,'label',initcap(replace(lower(f.row_status),'_',' ')),
          'count',f.row_count
        ) order by f.facet_ordinal) from facet_status_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_status_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_status_values f),false) then (
          select jsonb_build_object('after_code',f.row_status)
          from facet_status_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end,
      'blockers',case when p.facet_kinds ? 'BLOCKERS' then jsonb_build_object(
        'items',coalesce((select jsonb_agg(jsonb_build_object(
          'code',f.code,'count',f.row_count
        ) order by f.facet_ordinal) from facet_blocker_values f
          where f.facet_ordinal<=p.facet_limit),'[]'::jsonb),
        'has_more',coalesce((select max(f.facet_total)>p.facet_limit
          from facet_blocker_values f),false),
        'next_cursor_values',case when coalesce((select max(f.facet_total)>p.facet_limit
          from facet_blocker_values f),false) then (
          select jsonb_build_object('after_code',f.code)
          from facet_blocker_values f where f.facet_ordinal=p.facet_limit
        ) end
      ) end
    )) facets
    from params p
  ),
  row_json as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'selection_key',selection_key,
      'group_key',group_key,
      'invoice_id',invoice_id,
      'invoice_number',invoice_number,
      'source_revision',document_revision::text,
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
      'invoice_stream',invoice_stream,
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
      'support_readiness',coalesce(
        support.invoice_json->'support_readiness',
        visible_rows.support_readiness
      ),
      'sort_tuple',jsonb_build_object(
        'sort_date',case when sort_date_key is not null then sort_date_key::text end,
        'sort_text',sort_text_key,
        'sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end,
        'selection_key',selection_key
      )
    ) order by page_ordinal),'[]'::jsonb) rows
    from visible_rows
    left join visible_support support
      on support.support_invoice_id=visible_rows.invoice_id
  )
  select jsonb_build_object(
    'contract_version','INVOICE_BATCH_CANDIDATES_V2',
    'action','ISSUE',
    'mode',v_mode,
    'snapshot',v_snapshot,
    'normalised_filter',v_filters,
    'normalised_sort',v_sort,
    'filter_hash',v_filter_hash,
    'query_hash',v_query_hash,
    'selection_hash',v_selection_hash,
    'rows',(select rows from row_json),
    'page',jsonb_build_object(
      'page_size',v_page_size,
      'returned_count',(select count(*) from visible_rows),
      'total_count',case
        when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
          then coalesce((select max(k.full_scope_count) from candidate_keys k),0)
        else (select display_count from display_totals)
      end,
      'has_more',case
        when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
          then exists (
            select 1
            from candidate_keys k
            where k.page_ordinal>v_page_size
          )
        else (select count(*) from page_rows)>v_page_size
      end,
      'next_cursor_values',case when (
        case
          when v_mode in ('PAGE','EXPAND_SELECTION','EXPLICIT_KEYS')
            then exists (
              select 1
              from candidate_keys k
              where k.page_ordinal>v_page_size
            )
          else (select count(*) from page_rows)>v_page_size
        end
      ) then (
        select case when v_mode='EXPAND_SELECTION'
          then jsonb_build_object('after_selection_key',selection_key)
          else jsonb_build_object(
            'after_selection_key',selection_key,
            'after_sort_date',case when sort_date_key is not null then sort_date_key::text end,
            'after_sort_text',sort_text_key,
            'after_sort_numeric',case when sort_numeric_key is not null then sort_numeric_key::text end
          )
        end
        from (
          select
            selection_key,
            sort_date_key,
            sort_text_key,
            sort_numeric_key,
            page_ordinal
          from candidate_keys
          where page_ordinal<=v_page_size
        ) cursor_source
        order by page_ordinal desc
        limit 1
      ) else null end
    ),
    'totals',jsonb_build_object(
      'all',(select all_count from totals),
      'filtered_total',(select all_count from totals),
      'display_total',(select display_count from display_totals),
      'eligible_total',(select ready_count from totals),
      'selected_total',(select selected_count from totals),
      'excluded_total',(select ready_count-selected_count from totals),
      'ready',(select ready_count from totals),
      'blocked',(select blocked_count from totals),
      'blocked_total',(select blocked_count from totals),
      'blocked_for_sending',(select blocked_for_sending_count from totals),
      'stale',(select stale_count from totals),
      'in_progress',(select in_progress_count from totals)
    ),
    'selection_summary',jsonb_build_object(
      'eligible_total',(select ready_count from totals),
      'selected_total',(select selected_count from totals),
      'excluded_total',(select ready_count-selected_count from totals),
      'blocked_total',(select blocked_count from totals),
      'exact',v_mode in ('FACETS','SUMMARY')
    ),
    'group_selection',(select groups from group_selection_json),
    'facets',case
      when v_mode='FACETS' then (select facets from facet_json)
      else jsonb_build_object()
    end,
    'selection_seed',jsonb_build_object('mode','IMPLICIT_ALL','default_selected',true)
  ) into v_result;

  if v_mode='SUMMARY'
     and coalesce((v_result#>>'{totals,filtered_total}')::integer,0)>25000 then
    raise exception using
      errcode='54000',
      message='BATCH_SUMMARY_SCOPE_TOO_LARGE';
  end if;

  if v_mode='EXPLICIT_KEYS' and (
    jsonb_array_length(coalesce(v_result->'rows','[]'::jsonb))
      <> jsonb_array_length(v_selection_keys)
    or exists (
      select 1
      from jsonb_array_elements(v_result->'rows') row_item(row_json)
      where coalesce(row_json->>'source_revision','')
        is distinct from coalesce(
          v_expected_source_revisions->>(row_json->>'selection_key'),
          ''
        )
    )
  ) then
    raise exception using
      errcode='40001',
      message='BATCH_SOURCE_CHANGED';
  end if;

  v_snapshot_after := private._invoice_candidate_snapshot_verify_v2(
    'ISSUE',
    v_snapshot,
    coalesce(p_now_utc,now())
  );
  if v_snapshot_after->>'revision' <> v_snapshot->>'revision' then
    raise exception using errcode='40001', message='BATCH_SNAPSHOT_CHANGED';
  end if;

  return v_result;
end;
$function$;

alter function private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamptz) owner to postgres;
revoke all on function private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamptz) from public, anon, authenticated;
grant execute on function private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamptz) to service_role;
