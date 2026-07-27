create or replace function private._invoice_generation_resolve_command_groups(
  p_commands jsonb,
  p_actor_user_id uuid,
  p_effective_at_utc timestamptz
) returns table(
  command_no integer,
  command_type text,
  group_key text,
  canonical_source_ids uuid[],
  canonical_source_members jsonb,
  source_types text[],
  client_id uuid,
  contract_ids uuid[],
  target_invoice_week date,
  natural_source_weeks date[],
  consolidation_mode text,
  invoice_stream text,
  self_bill boolean,
  automatic boolean,
  source_origin text,
  allow_early boolean,
  effective_settings_date date,
  source_revision_hash text,
  idempotency_components jsonb,
  blocker_code text,
  blocker_detail jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with raw_commands as materialized (
  select e.ordinality::integer command_no,e.value command_json,
    upper(btrim(coalesce(e.value->>'command_type',e.value->>'type',''))) command_type
  from jsonb_array_elements(
    case when jsonb_typeof(p_commands)='array' then p_commands else '[]'::jsonb end
  ) with ordinality e(value,ordinality)
),
generation_commands as materialized (
  select r.*,
    case
      when r.command_type='GENERATE_NHSP'
        and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
        then r.command_json->'nhsp_shift_ids'
      when jsonb_typeof(r.command_json->'source_ids')='array'
        then r.command_json->'source_ids'
      when jsonb_typeof(r.command_json->'canonical_source_ids')='array'
        then r.command_json->'canonical_source_ids'
      when jsonb_typeof(r.command_json->'timesheet_ids')='array'
        then r.command_json->'timesheet_ids'
      when r.command_type='GENERATE_CREDIT_NOTE'
        and jsonb_typeof(r.command_json->'invoice_ids')='array'
        then r.command_json->'invoice_ids'
      when r.command_type='GENERATE_CREDIT_NOTE'
        and coalesce(r.command_json->>'base_invoice_id','')<>''
        then jsonb_build_array(r.command_json->>'base_invoice_id')
      when r.command_type='GENERATE_CREDIT_NOTE'
        and coalesce(r.command_json->>'invoice_id','')<>''
        then jsonb_build_array(r.command_json->>'invoice_id')
      else '[]'::jsonb
    end requested_ids,
    case when lower(coalesce(r.command_json->>'allow_early','false')) in('true','false')
      then(r.command_json->>'allow_early')::boolean else false end allow_early,
    r.command_type='GENERATE_AUTO' automatic
  from raw_commands r
  where r.command_type in(
    'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS','GENERATE_EXPENSES',
    'GENERATE_NHSP','GENERATE_CREDIT_NOTE','GENERATE_AUTO')
),
raw_ids as materialized (
  select c.command_no,c.command_type,c.command_json,c.allow_early,c.automatic,
    x.ordinality::integer source_ordinal,x.value source_id_text,
    x.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      valid_uuid
  from generation_commands c
  cross join lateral jsonb_array_elements_text(c.requested_ids)
    with ordinality x(value,ordinality)
),
command_shape as materialized (
  select c.command_no,c.command_type,c.command_json,c.allow_early,c.automatic,
    jsonb_array_length(c.requested_ids) requested_count,
    count(r.source_id_text) filter(where r.valid_uuid)::integer valid_uuid_count,
    count(r.source_id_text) filter(where not r.valid_uuid)::integer invalid_uuid_count,
    coalesce(jsonb_agg(r.source_id_text order by r.source_ordinal)
      filter(where not r.valid_uuid),'[]'::jsonb) invalid_values
  from generation_commands c left join raw_ids r on r.command_no=c.command_no
  group by c.command_no,c.command_type,c.command_json,c.allow_early,c.automatic,c.requested_ids
),
resolved_ids as materialized (
  select distinct r.command_no,r.command_type,r.command_json,r.allow_early,r.automatic,
    case when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
      then s.timesheet_id else r.source_id_text::uuid end timesheet_id,
    case when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
      then 'NHSP_SHIFT' else 'TIMESHEET' end requested_source_type,
    case when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
      then r.source_id_text::uuid else r.source_id_text::uuid end requested_source_id
  from raw_ids r
  left join public.nhsp_shifts s
    on r.command_type='GENERATE_NHSP'
      and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
      and s.id=r.source_id_text::uuid
  where r.valid_uuid
),
base_sources as materialized (
  select r.*,ts.week_ending_date::date natural_week_ending,
    (ts.week_ending_date::date-6) natural_week_start,
    ts.version timesheet_version,ts.updated_at timesheet_updated_at,
    ts.is_current timesheet_is_current,ts.revoked_at,ts.is_adjustment,ts.adjustment_origin,
    ts.parent_timesheet_id,
    tf.id tsfin_id,tf.timesheet_version tsfin_timesheet_version,
    tf.updated_at tsfin_updated_at,tf.is_current tsfin_is_current,tf.is_stale,
    tf.processing_status::text processing_status,tf.locked_by_invoice_id,
    tf.client_id,tf.candidate_id,tf.basis::text basis,tf.invoice_breakdown_json,
    tf.additional_units_json,
    coalesce(ts.contract_id,cw.contract_id) contract_id,
    coalesce(parent_ts.contract_id,parent_cw.contract_id) parent_contract_id,
    coalesce(parent_contract.self_bill,contract.self_bill,false) self_bill,
    case
      when upper(coalesce(tf.basis::text,'')) like 'NHSP%' then 'NHSP'
      when upper(coalesce(tf.basis::text,'')) like 'HEALTHROSTER%' then 'HEALTHROSTER'
      when coalesce(parent_contract.self_bill,contract.self_bill,false) then 'SELF_BILL'
      else 'NORMAL'
    end invoice_stream,
    case
      when upper(coalesce(tf.basis::text,'')) like 'NHSP%' then 'NHSP'
      when upper(coalesce(tf.basis::text,'')) like 'HEALTHROSTER%' then 'HEALTHROSTER'
      when upper(coalesce(tf.basis::text,'')) like '%EXPENSE%' then 'EXPENSE'
      when coalesce(ts.is_adjustment,false) then 'ADJUSTMENT'
      else 'TIMESHEET'
    end resolved_source_type
  from resolved_ids r
  left join public.timesheets ts on ts.timesheet_id=r.timesheet_id
  left join public.timesheets_financials tf
    on tf.timesheet_id=r.timesheet_id and tf.is_current
  left join lateral (
    select w.contract_id from public.contract_weeks w
    where w.timesheet_id=r.timesheet_id
    order by w.updated_at desc nulls last,w.created_at desc nulls last,w.id desc limit 1
  ) cw on true
  left join public.contracts contract on contract.id=coalesce(ts.contract_id,cw.contract_id)
  left join public.timesheets parent_ts
    on parent_ts.timesheet_id=ts.parent_timesheet_id and parent_ts.is_current
  left join lateral (
    select w.contract_id from public.contract_weeks w
    where w.timesheet_id=parent_ts.timesheet_id
    order by w.updated_at desc nulls last,w.created_at desc nulls last,w.id desc limit 1
  ) parent_cw on true
  left join public.contracts parent_contract
    on parent_contract.id=coalesce(parent_ts.contract_id,parent_cw.contract_id)
),
expanded_sources as materialized (
  select b.*,
    case
      when seg.segment_json is not null
        and pg_input_is_valid(
          coalesce(seg.segment_json->>'invoice_target_week_start',''),'date')
        then(seg.segment_json->>'invoice_target_week_start')::date
      when pg_input_is_valid(
          coalesce(b.command_json->>'target_invoice_week',''),'date')
        then(b.command_json->>'target_invoice_week')::date
      else b.natural_week_start end economic_target_week,
    seg.segment_json,seg.segment_ordinal,
    seg.segment_json->>'segment_id' segment_id,
    case
      when b.requested_source_type='NHSP_SHIFT' and b.timesheet_id is null
        then 'NHSP_SHIFT_TIMESHEET_REQUIRED'
      when b.timesheet_id is null then 'SOURCE_NOT_RESOLVED'
      when b.tsfin_id is null then 'CURRENT_FINANCIALS_MISSING'
      when b.timesheet_is_current is not true or b.revoked_at is not null
        then 'TIMESHEET_NOT_CURRENT'
      when b.tsfin_is_current is not true then 'CURRENT_FINANCIALS_MISSING'
      when b.is_stale then 'FINANCIALS_STALE'
      when b.processing_status<>'READY_FOR_INVOICE' then 'NOT_READY_FOR_INVOICE'
      when b.client_id is null then 'CLIENT_UNRESOLVED'
      when b.locked_by_invoice_id is not null
        and coalesce(b.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
        then 'SOURCE_ALREADY_LOCKED'
      when seg.segment_json is not null
        and nullif(btrim(coalesce(
          seg.segment_json->>'invoice_target_week_start','')),'') is not null
        and not pg_input_is_valid(
          seg.segment_json->>'invoice_target_week_start','date')
        then 'MALFORMED_SEGMENT_TARGET_WEEK'
      when seg.segment_json is not null and exists(
        select 1
        from (values
          (seg.segment_json->>'hours_day'),(seg.segment_json->>'hours_night'),
          (seg.segment_json->>'hours_sat'),(seg.segment_json->>'hours_sun'),
          (seg.segment_json->>'hours_bh'),(seg.segment_json->>'pay_amount'),
          (seg.segment_json->>'pay_ex_vat'),
          (seg.segment_json->>'charge_amount'),
          (seg.segment_json->>'charge_ex_vat')) n(value)
        where nullif(btrim(n.value),'') is not null
          and n.value!~'^[+-]?[0-9]+([.][0-9]+)?$')
        then 'MALFORMED_SEGMENT_FINANCIAL'
      when jsonb_typeof(b.additional_units_json)='object' and exists(
        select 1
        from jsonb_each(b.additional_units_json) a(key,value)
        where jsonb_typeof(a.value)<>'object'
          or exists(
            select 1
            from (values
              (a.value->>'unit_count'),(a.value->>'pay_rate'),
              (a.value->>'charge_rate'),(a.value->>'pay_ex_vat'),
              (a.value->>'charge_ex_vat')) n(value)
            where nullif(btrim(n.value),'') is not null
              and not pg_input_is_valid(n.value,'numeric'))
          or exists(
            select 1
            from jsonb_each_text(case
              when jsonb_typeof(a.value->'days')='object'
                then a.value->'days' else '{}'::jsonb end) d(key,value)
            where nullif(btrim(d.value),'') is not null
              and not pg_input_is_valid(d.value,'numeric')))
        then 'MALFORMED_ADDITIONAL_RATE'
      when seg.segment_json is not null
        and nullif(seg.segment_json->>'invoice_locked_invoice_id','') is not null
        then 'SEGMENT_ALREADY_LOCKED'
    end source_blocker
  from base_sources b
  left join lateral (
    select x.value segment_json,x.ordinality::integer segment_ordinal
    from jsonb_array_elements(
      case when jsonb_typeof(b.invoice_breakdown_json->'segments')='array'
        then b.invoice_breakdown_json->'segments' else '[]'::jsonb end
    ) with ordinality x(value,ordinality)
    where coalesce(b.invoice_breakdown_json->>'mode','')='SEGMENTS'
      and jsonb_typeof(x.value)='object'
    union all
    select null::jsonb,0
    where coalesce(b.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
      or jsonb_array_length(case
        when jsonb_typeof(b.invoice_breakdown_json->'segments')='array'
          then b.invoice_breakdown_json->'segments' else '[]'::jsonb end)=0
  ) seg on true
  where case
    when coalesce(b.command_json->>'target_invoice_week','')='' then true
    when not pg_input_is_valid(
      b.command_json->>'target_invoice_week','date') then true
    else coalesce(
      case when seg.segment_json is not null
          and pg_input_is_valid(
            coalesce(seg.segment_json->>'invoice_target_week_start',''),'date')
        then(seg.segment_json->>'invoice_target_week_start')::date end,
      b.natural_week_start)=(b.command_json->>'target_invoice_week')::date
    end
),
settings_resolved as materialized (
  select e.*,
    upper(coalesce(nullif(e.command_json->>'consolidation_mode',''),
      cs.invoice_consolidation_mode::text,'NONE')) consolidation_mode,
    coalesce(e.economic_target_week,e.natural_week_start,
      (p_effective_at_utc at time zone 'Europe/London')::date) settings_date
  from expanded_sources e
  left join lateral (
    select s.invoice_consolidation_mode
    from public.client_settings s
    where s.client_id=e.client_id
      and(s.effective_from is null
        or s.effective_from<=coalesce(e.economic_target_week,e.natural_week_start,
          (p_effective_at_utc at time zone 'Europe/London')::date))
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
source_rows_base as materialized (
  select s.*,
    encode(digest(concat_ws('|',
      s.resolved_source_type,s.timesheet_id::text,
      coalesce(s.segment_id,'WHOLE'),s.economic_target_week::text),
      'sha256'),'hex') source_member_key,
    encode(digest(concat_ws('|',
      s.timesheet_id::text,s.tsfin_id::text,s.tsfin_timesheet_version::text,
      s.tsfin_updated_at::text,s.timesheet_version::text,s.timesheet_updated_at::text,
      coalesce(s.invoice_breakdown_json::text,''),coalesce(s.segment_id,'WHOLE'),
      coalesce(s.segment_json::text,''),coalesce(s.segment_json->>'segment_id',''),
      coalesce(s.segment_json->>'invoice_target_week_start',''),
      coalesce(s.segment_json->>'invoice_locked_invoice_id',''),
      coalesce(s.segment_json->>'ref_num',''),
      coalesce(s.segment_json->>'charge_amount',''),
      coalesce(s.segment_json->>'pay_amount',''),
      coalesce(s.economic_target_week::text,''),s.invoice_stream,s.consolidation_mode),
      'sha256'),'hex') row_revision
  from settings_resolved s
),
pre_reference_groups as materialized (
  select
    s.command_no,
    s.client_id,
    s.invoice_stream,
    s.consolidation_mode,
    case
      when s.consolidation_mode='NONE' then s.timesheet_id::text
      when s.consolidation_mode='BY_WEEK'
        then s.economic_target_week::text
      else 'ANY_WEEK'
    end grouping_value,
    encode(digest(concat_ws(
      '|',
      s.command_type,
      s.client_id::text,
      s.invoice_stream,
      s.consolidation_mode,
      case
        when s.consolidation_mode='NONE' then s.timesheet_id::text
        when s.consolidation_mode='BY_WEEK'
          then s.economic_target_week::text
        else 'ANY_WEEK'
      end,
      encode(digest(string_agg(concat_ws(
        ':',
        s.timesheet_id::text,
        coalesce(s.segment_id,'WHOLE'),
        s.economic_target_week::text
      ),'|' order by
        s.timesheet_id,
        coalesce(s.segment_id,'WHOLE')
      ),'sha256'),'hex')
    ),'sha256'),'hex') group_key
  from source_rows_base s
  group by
    s.command_no,
    s.command_type,
    s.client_id,
    s.invoice_stream,
    s.consolidation_mode,
    case
      when s.consolidation_mode='NONE' then s.timesheet_id::text
      when s.consolidation_mode='BY_WEEK'
        then s.economic_target_week::text
      else 'ANY_WEEK'
    end
),
source_rows_scoped as materialized (
  select source_row.*
  from source_rows_base source_row
  join pre_reference_groups candidate_group
    on candidate_group.command_no=source_row.command_no
   and candidate_group.client_id=source_row.client_id
   and candidate_group.invoice_stream=source_row.invoice_stream
   and candidate_group.consolidation_mode=
       source_row.consolidation_mode
   and candidate_group.grouping_value is not distinct from case
     when source_row.consolidation_mode='NONE'
       then source_row.timesheet_id::text
     when source_row.consolidation_mode='BY_WEEK'
       then source_row.economic_target_week::text
     else 'ANY_WEEK'
   end
  join generation_commands command
    on command.command_no=source_row.command_no
  where jsonb_typeof(command.command_json->'scope_keys')
          is distinct from 'array'
     or jsonb_array_length(
          case
            when jsonb_typeof(
              command.command_json->'scope_keys'
            )='array'
              then command.command_json->'scope_keys'
            else '[]'::jsonb
          end
        )=0
     or exists (
       select 1
       from jsonb_array_elements_text(
         case
           when jsonb_typeof(
             command.command_json->'scope_keys'
           )='array'
             then command.command_json->'scope_keys'
           else '[]'::jsonb
         end
       ) requested_scope(scope_key)
       where requested_scope.scope_key=candidate_group.group_key
     )
),
reference_results as materialized (
  select distinct on (r.source_member_key) r.*
  from private._invoice_source_reference_validate_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',b.source_member_key,
      'source_type',b.resolved_source_type,
      'source_id',b.timesheet_id,
      'related_timesheet_id',b.timesheet_id,
      'segment_id',b.segment_id,
      'target_invoice_week',b.economic_target_week,
      'invoice_stream',b.invoice_stream,
      'consolidation_mode',b.consolidation_mode,
      'row_revision',b.row_revision)
      order by b.command_no,b.timesheet_id,b.segment_ordinal)
    from source_rows_scoped b
  ),'[]'::jsonb)) r
  order by r.source_member_key
),
source_rows as materialized (
  select b.*,
    coalesce(b.source_blocker,r.blocker_code) canonical_blocker,
    r.reference_hash,r.current_revision canonical_row_revision,
    r.detail_json reference_detail
  from source_rows_scoped b
  left join reference_results r
    on r.source_member_key=b.source_member_key
),
grouped as materialized (
  select s.command_no,s.command_type,
    encode(digest(concat_ws('|',s.command_type,s.client_id::text,s.invoice_stream,
      s.consolidation_mode,
      case
        when s.consolidation_mode='NONE' then s.timesheet_id::text
        when s.consolidation_mode='BY_WEEK' then s.economic_target_week::text
        else 'ANY_WEEK' end,
      encode(digest(string_agg(concat_ws(':',s.timesheet_id::text,
        coalesce(s.segment_id,'WHOLE'),s.economic_target_week::text)
        ,'|' order by s.timesheet_id,coalesce(s.segment_id,'WHOLE')),'sha256'),'hex')),
      'sha256'),'hex') group_key,
    array_agg(distinct s.timesheet_id order by s.timesheet_id)
      filter(where s.timesheet_id is not null) canonical_source_ids,
    jsonb_agg(jsonb_build_object(
      'source_member_key',s.source_member_key,
      'source_type',s.resolved_source_type,
      'source_id',s.timesheet_id,
      'related_timesheet_id',s.timesheet_id,
      'requested_source_type',s.requested_source_type,
      'requested_source_id',s.requested_source_id,
      'natural_week_start',s.natural_week_start,
      'target_invoice_week',s.economic_target_week,
      'segment_id',s.segment_id,
      'segment_ordinal',s.segment_ordinal,
      'row_revision',coalesce(s.canonical_row_revision,s.row_revision))
      order by s.economic_target_week,s.timesheet_id,s.segment_ordinal)
      canonical_source_members,
    array_agg(distinct s.resolved_source_type order by s.resolved_source_type)
      source_types,
    s.client_id,
    array_agg(distinct coalesce(s.parent_contract_id,s.contract_id)
      order by coalesce(s.parent_contract_id,s.contract_id))
      filter(where coalesce(s.parent_contract_id,s.contract_id) is not null) contract_ids,
    case when s.consolidation_mode='ANY_WEEK' then null::date
      else min(s.economic_target_week) end target_invoice_week,
    array_agg(distinct s.natural_week_start order by s.natural_week_start)
      filter(where s.natural_week_start is not null) natural_source_weeks,
    s.consolidation_mode,s.invoice_stream,bool_or(s.self_bill) self_bill,
    bool_or(s.automatic) automatic,
    case when bool_or(s.command_type='GENERATE_AUTO') then 'AUTOMATIC'
      when bool_or(s.requested_source_type='NHSP_SHIFT') then 'NHSP_SELECTION'
      else 'MANUAL_SELECTION' end source_origin,
    bool_and(s.allow_early) allow_early,
    min(s.settings_date) effective_settings_date,
    encode(digest(string_agg(s.timesheet_id::text||':'||
      coalesce(s.canonical_row_revision,s.row_revision),
      '|' order by s.timesheet_id,s.segment_ordinal),'sha256'),'hex') source_revision_hash,
    coalesce(min(s.canonical_blocker),null) blocker_code,
    coalesce(jsonb_agg(jsonb_build_object(
      'source_id',s.timesheet_id,'segment_id',s.segment_id,
      'code',s.canonical_blocker,
      'reference_hash',s.reference_hash,
      'reference_detail',s.reference_detail)
      order by s.timesheet_id,s.segment_ordinal)
      filter(where s.canonical_blocker is not null),'[]'::jsonb) blocker_sources
  from source_rows s
  group by s.command_no,s.command_type,s.client_id,s.consolidation_mode,s.invoice_stream,
    case
      when s.consolidation_mode='NONE' then s.timesheet_id::text
      when s.consolidation_mode='BY_WEEK' then s.economic_target_week::text
      else 'ANY_WEEK' end
),
normal_results as (
  select g.command_no,g.command_type,g.group_key,g.canonical_source_ids,
    g.canonical_source_members,g.source_types,g.client_id,g.contract_ids,
    g.target_invoice_week,g.natural_source_weeks,g.consolidation_mode,
    g.invoice_stream,g.self_bill,g.automatic,g.source_origin,g.allow_early,
    g.effective_settings_date,g.source_revision_hash,
    jsonb_build_object(
      'command_type',g.command_type,'group_key',g.group_key,
      'canonical_source_ids',to_jsonb(g.canonical_source_ids),
      'canonical_source_members',g.canonical_source_members,
      'client_id',g.client_id,'contract_ids',to_jsonb(g.contract_ids),
      'target_invoice_week',g.target_invoice_week,
      'natural_source_weeks',to_jsonb(g.natural_source_weeks),
      'consolidation_mode',g.consolidation_mode,'invoice_stream',g.invoice_stream,
      'self_bill',g.self_bill,'automatic',g.automatic,
      'source_origin',g.source_origin,'allow_early',g.allow_early,
      'effective_settings_date',g.effective_settings_date,
      'source_revision_hash',g.source_revision_hash) idempotency_components,
    g.blocker_code,
    case when g.blocker_code is null then null
      else jsonb_build_object('code',g.blocker_code,'sources',g.blocker_sources) end blocker_detail
  from grouped g
),
shape_errors as (
  select s.command_no,s.command_type,
    encode(digest('INVALID|'||s.command_no||'|'||s.command_type,'sha256'),'hex') group_key,
    array[]::uuid[] canonical_source_ids,'[]'::jsonb canonical_source_members,
    array[]::text[] source_types,null::uuid client_id,array[]::uuid[] contract_ids,
    null::date target_invoice_week,array[]::date[] natural_source_weeks,
    upper(coalesce(nullif(s.command_json->>'consolidation_mode',''),'NONE')) consolidation_mode,
    'UNKNOWN'::text invoice_stream,false self_bill,s.automatic,
    case when s.automatic then 'AUTOMATIC' else 'MANUAL_SELECTION' end source_origin,
    s.allow_early,
    (p_effective_at_utc at time zone 'Europe/London')::date effective_settings_date,
    encode(digest('INVALID','sha256'),'hex') source_revision_hash,
    jsonb_build_object('command_type',s.command_type,'invalid',true) idempotency_components,
    case
      when s.requested_count=0 then 'SOURCE_IDS_REQUIRED'
      when s.invalid_uuid_count>0 then 'MALFORMED_SOURCE_ID'
      when s.valid_uuid_count<>s.requested_count then 'MIXED_VALID_INVALID_SOURCE_IDS'
      when not exists(select 1 from resolved_ids r where r.command_no=s.command_no
        and r.timesheet_id is not null) then 'SOURCE_NOT_RESOLVED'
      when exists(
        select 1 from resolved_ids r
        left join public.timesheets t on t.timesheet_id=r.timesheet_id
        where r.command_no=s.command_no
          and(r.timesheet_id is null or t.timesheet_id is null)
      ) then 'SOURCE_NOT_RESOLVED'
    end blocker_code,
    jsonb_build_object(
      'requested_count',s.requested_count,'valid_uuid_count',s.valid_uuid_count,
      'invalid_uuid_count',s.invalid_uuid_count,'invalid_values',s.invalid_values) blocker_detail
  from command_shape s
  where s.requested_count=0 or s.invalid_uuid_count>0
    or s.valid_uuid_count<>s.requested_count
    or not exists(select 1 from resolved_ids r where r.command_no=s.command_no
      and r.timesheet_id is not null)
    or exists(
      select 1 from resolved_ids r
      left join public.timesheets t on t.timesheet_id=r.timesheet_id
      where r.command_no=s.command_no
        and(r.timesheet_id is null or t.timesheet_id is null)
    )
),
credit_results as (
  select s.command_no,s.command_type,
    encode(digest('CREDIT|'||i.id||'|'||
      coalesce(s.command_json->>'credit_reason','')||'|'||
      coalesce(s.command_json->>'command_token',''),'sha256'),'hex') group_key,
    array[i.id] canonical_source_ids,
    jsonb_build_array(jsonb_build_object(
      'source_type','INVOICE','source_id',i.id,'source_revision',
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),
        'sha256'),'hex'))) canonical_source_members,
    array['INVOICE']::text[] source_types,i.client_id,array[]::uuid[] contract_ids,
    null::date target_invoice_week,array[]::date[] natural_source_weeks,
    'CREDIT_NOTE'::text consolidation_mode,'CREDIT_NOTE'::text invoice_stream,
    false self_bill,false automatic,'MANUAL_SELECTION'::text source_origin,
    true allow_early,(i.updated_at at time zone 'Europe/London')::date effective_settings_date,
    encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
      i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),
      'sha256'),'hex') source_revision_hash,
    jsonb_build_object('command_type',s.command_type,'source_invoice_id',i.id,
      'source_revision',encode(digest(concat_ws('|',i.id::text,i.updated_at::text,
        i.status::text,i.subtotal_ex_vat::text,i.vat_amount::text,
        i.total_inc_vat::text),'sha256'),'hex'),
      'credit_reason',s.command_json->>'credit_reason',
      'command_token',s.command_json->>'command_token') idempotency_components,
    case
      when i.id is null then 'INVOICE_NOT_FOUND'
      when i.type::text<>'INVOICE' then 'CREDIT_SOURCE_NOT_INVOICE'
      when i.status::text not in('ISSUED','PAID') then 'CREDIT_SOURCE_NOT_ISSUED'
      when exists(select 1 from public.invoices c
        where c.original_invoice_id=i.id and c.type='CREDIT_NOTE'
          and c.status in('DRAFT','ISSUED','PAID')) then 'CREDIT_ALREADY_EXISTS'
    end blocker_code,
    null::jsonb blocker_detail
  from command_shape s
  join raw_ids r on r.command_no=s.command_no and r.valid_uuid
  left join public.invoices i on i.id=r.source_id_text::uuid
  where s.command_type='GENERATE_CREDIT_NOTE'
    and s.requested_count>0 and s.invalid_uuid_count=0
)
select * from normal_results
where command_type<>'GENERATE_CREDIT_NOTE'
  and not exists(select 1 from shape_errors e where e.command_no=normal_results.command_no)
union all
select * from shape_errors
union all
select * from credit_results
order by command_no,group_key;
$function$;

revoke all on function private._invoice_generation_resolve_command_groups(
  jsonb,uuid,timestamptz) from public,anon,authenticated;
grant execute on function private._invoice_generation_resolve_command_groups(
  jsonb,uuid,timestamptz) to service_role;
