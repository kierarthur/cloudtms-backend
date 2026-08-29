drop function if exists private._invoice_correction_validate_batch(uuid[],date);
drop function if exists private._invoice_correction_validate_batch(uuid[],date,jsonb);
drop function if exists private._invoice_correction_validate_batch(jsonb,date);
create or replace function private._invoice_correction_validate_batch(
  p_scopes jsonb,
  p_evaluation_date date
) returns table(
  request_key text,
  scope_key text,
  invoice_id uuid,
  timesheet_id uuid,
  correction_classification text,
  correction_id text,
  correction_ids text[],
  required_member_ids uuid[],
  missing_member_ids uuid[],
  conflicting_invoice_ids uuid[],
  balanced boolean,
  valid boolean,
  blocker_code text,
  blocker_codes text[],
  detail_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with recursive scope_input as materialized (
  select e.ordinality::integer input_ordinal,e.value scope_json
  from jsonb_array_elements(case when jsonb_typeof(p_scopes)='array'
    then p_scopes else '[]'::jsonb end) with ordinality e(value,ordinality)
),
scopes as materialized (
  select
    coalesce(nullif(btrim(scope_json->>'request_key'),''),
      'correction:'||input_ordinal::text) request_key,
    coalesce(nullif(btrim(scope_json->>'scope_key'),''),
      nullif(btrim(scope_json->>'request_key'),''),
      'scope:'||input_ordinal::text) external_scope_key,
    coalesce(nullif(btrim(scope_json->>'request_key'),''),
      'correction:'||input_ordinal::text)||E'\\x1f'||
    coalesce(nullif(btrim(scope_json->>'scope_key'),''),
      nullif(btrim(scope_json->>'request_key'),''),
      'scope:'||input_ordinal::text) scope_key,
    case when coalesce(scope_json->>'invoice_id',
      scope_json->>'target_invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then coalesce(scope_json->>'invoice_id',
        scope_json->>'target_invoice_id')::uuid end invoice_id,
    upper(coalesce(nullif(btrim(scope_json->>'validation_purpose'),''),
      'VALIDATE')) validation_purpose,
    case when coalesce(scope_json->>'expected_client_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(scope_json->>'expected_client_id')::uuid end expected_client_id,
    case when coalesce(scope_json->>'expected_contract_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(scope_json->>'expected_contract_id')::uuid end expected_contract_id,
    case when coalesce(scope_json->>'natural_source_week','')
      ~'^\\d{4}-\\d{2}-\\d{2}$'
      then(scope_json->>'natural_source_week')::date end natural_source_week,
    case when coalesce(scope_json->>'target_invoice_week','')
      ~'^\\d{4}-\\d{2}-\\d{2}$'
      then(scope_json->>'target_invoice_week')::date end target_invoice_week,
    nullif(upper(btrim(scope_json->>'expected_invoice_stream')),'')
      expected_invoice_stream,
    case when coalesce(scope_json->>'expected_vat_rate_pct','')
      ~'^[+-]?[0-9]+([.][0-9]+)?$'
      then(scope_json->>'expected_vat_rate_pct')::numeric end
      expected_vat_rate_pct,
    case when jsonb_typeof(scope_json->'planned_members')='array'
      then scope_json->'planned_members' else '[]'::jsonb end planned_members,
    scope_json
  from scope_input
),
planned_lines as materialized (
  select distinct s.scope_key,s.invoice_id,
    case when coalesce(x.value->>'timesheet_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x.value->>'timesheet_id')::uuid end timesheet_id,
    case when coalesce(x.value->>'vat_rate_pct','')
      ~'^[+-]?[0-9]+([.][0-9]+)?$'
      then(x.value->>'vat_rate_pct')::numeric
      else s.expected_vat_rate_pct end vat_rate_pct,
    nullif(btrim(coalesce(x.value->>'segment_id',
      x.value->>'segment_key')),'') segment_id
  from scopes s
  cross join lateral jsonb_array_elements(s.planned_members) x(value)
  where coalesce(x.value->>'timesheet_id','')~*
    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
),
targets as materialized (
  select distinct s.invoice_id,s.scope_key,l.timesheet_id
  from scopes s
  join public.invoice_lines l on l.invoice_id=s.invoice_id
  where s.invoice_id is not null and l.timesheet_id is not null
  union
  select p.invoice_id,p.scope_key,p.timesheet_id
  from planned_lines p
  where p.timesheet_id is not null
),
seed_units as materialized (
  select distinct t.correction_id,
    coalesce(t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope')
      #>>'{operation,operation_id}' correction_operation_id,
    coalesce(t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope')
      ->>'correction_chain_id' correction_chain_id
  from targets q
  join public.timesheets t on t.timesheet_id=q.timesheet_id
  left join public.timesheets_financials f
    on f.timesheet_id=t.timesheet_id and f.is_current
),all_current as materialized (
  select t.timesheet_id,t.correction_id,
    upper(btrim(coalesce(t.correction_kind,''))) correction_kind,
    upper(btrim(coalesce(t.adjustment_origin,''))) adjustment_origin,
    t.parent_timesheet_id,t.is_current,t.status::text timesheet_status,
    t.contract_id,t.week_ending_date,
    coalesce(f.client_id,c.client_id) client_id,
    f.id financial_id,f.processing_status::text processing_status,
    f.basis::text basis,f.total_charge_ex_vat,f.is_stale,f.stale_reason,
    f.locked_by_invoice_id,f.invoice_breakdown_json,
    f.policy_snapshot_json,f.rate_source_refs_json,
    coalesce(
      t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope') envelope
  from public.timesheets t
  left join public.timesheets_financials f
    on f.timesheet_id=t.timesheet_id and f.is_current
  left join public.contracts c on c.id=t.contract_id
  where exists(select 1 from targets q where q.timesheet_id=t.timesheet_id)
     or(t.correction_id is not null and exists(
       select 1 from seed_units u where u.correction_id=t.correction_id))
     or exists(
       select 1 from seed_units u
       where u.correction_operation_id is not null
         and u.correction_chain_id is not null
         and u.correction_operation_id=coalesce(
           t.candidate_hint_text->'correction_financials_policy_envelope',
           f.policy_snapshot_json->'correction_financials_policy_envelope',
           f.rate_source_refs_json->'correction_financials_policy_envelope')
             #>>'{operation,operation_id}'
         and u.correction_chain_id=coalesce(
           t.candidate_hint_text->'correction_financials_policy_envelope',
           f.policy_snapshot_json->'correction_financials_policy_envelope',
           f.rate_source_refs_json->'correction_financials_policy_envelope')
             ->>'correction_chain_id')
),
classified as materialized (
  select a.*,
    case
      when jsonb_typeof(a.envelope)='object'
      then encode(digest(convert_to(
        (a.envelope-'envelope_fingerprint')::text,'UTF8'),
        'sha256'),'hex')
    end recomputed_fingerprint,
    cs.is_nhsp,cs.autoprocess_hr,cs.no_timesheet_required,
    (a.adjustment_origin in(
        'IMPORT_CORRECTION','IMPORT_CANCELLATION',
        'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
        'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION')
      or a.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
        'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT'))
      import_declared,
    coalesce(
      a.adjustment_origin in(
        'IMPORT_CORRECTION','IMPORT_CANCELLATION',
        'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
        'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION')
      and a.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
        'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT')
      and jsonb_typeof(a.envelope)='object'
      and a.envelope->>'policy_schema_version'=
        'IMPORT_CORRECTION_FINANCIALS_POLICY_V2'
      and a.envelope->>'route_family'='IMPORT_AUTHORITATIVE'
      and lower(coalesce(a.envelope#>>'{classification,canonical}','false'))
        in('true','t','1','yes')
      and nullif(a.envelope#>>'{operation,operation_id}','') is not null
      and nullif(a.envelope->>'correction_chain_id','') is not null
      and a.envelope->>'envelope_fingerprint'=
        case
          when jsonb_typeof(a.envelope)='object'
          then encode(digest(convert_to(
            (a.envelope-'envelope_fingerprint')::text,'UTF8'),
            'sha256'),'hex')
        end
      and(
        (a.correction_kind like 'CHANGED_HOURS_%'
          and a.envelope#>>'{operation,correction_action}'='CHANGED_HOURS')
        or
        (a.correction_kind like 'CANCELLATION_%'
          and a.envelope#>>'{operation,correction_action}'='CANCELLATION')),
      false) authoritative
  from all_current a
  left join lateral (
    select s.is_nhsp,s.autoprocess_hr,s.no_timesheet_required
    from public.client_settings s
    where s.client_id=a.client_id
      and(s.effective_from is null
        or s.effective_from<=p_evaluation_date)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
policy_checked as materialized (
  select c.*,
    c.envelope#>>'{operation,operation_id}' correction_operation_id,
    c.envelope->>'correction_chain_id' correction_chain_id,
    c.envelope->>'root_timesheet_id' frozen_root_timesheet_id,
    upper(btrim(coalesce(c.envelope->>'correction_shape','')))
      correction_shape,
    case when coalesce(c.envelope->>'expected_member_count','')~'^[0-9]+$'
      then(c.envelope->>'expected_member_count')::integer end
      expected_member_count,
    c.envelope->'expected_member_roles' expected_member_roles,
    case
      when c.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
        then 'REVERSAL'
      when c.correction_kind in(
        'CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
        then 'REPLACEMENT'
      else 'INVALID'
    end correction_role,
    case
      when c.correction_kind in(
        'CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
        then c.envelope->'reversal'
      when c.correction_kind in(
        'CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
        then c.envelope->'replacement'
    end correction_leg
  from classified c
),
policy_validated as materialized (
  select p.*,
    case when jsonb_typeof(p.correction_leg)='object'
      then encode(digest(convert_to(
        (p.correction_leg-'leg_fingerprint')::text,'UTF8'),'sha256'),'hex')
    end recomputed_leg_fingerprint,
    case when jsonb_typeof(p.correction_leg->'tsfin_policy')='object'
      then encode(digest(convert_to(
        ((p.correction_leg->'tsfin_policy')-'tsfin_policy_fingerprint')::text,
        'UTF8'),'sha256'),'hex')
    end recomputed_tsfin_policy_fingerprint,
    case when jsonb_typeof(p.correction_leg->'invoice_policy')='object'
      then encode(digest(convert_to(
        ((p.correction_leg->'invoice_policy')-'invoice_policy_fingerprint')::text,
        'UTF8'),'sha256'),'hex')
    end recomputed_invoice_policy_fingerprint,
    case when upper(coalesce(p.basis,'')) in(
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',
      'HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end
      current_invoice_stream
  from policy_checked p
),
policy_final as materialized (
  select p.*,
    (
      not p.import_declared
      or(
        jsonb_typeof(p.envelope)='object'
        and nullif(p.envelope->>'envelope_fingerprint','') is not null
        and p.envelope->>'envelope_fingerprint'=p.recomputed_fingerprint
        and p.correction_shape in('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
        and p.expected_member_count between 1 and 100
        and jsonb_typeof(p.expected_member_roles)='array'
        and jsonb_array_length(p.expected_member_roles)=p.expected_member_count
        and nullif(p.correction_operation_id,'') is not null
        and nullif(p.correction_chain_id,'') is not null
        and jsonb_typeof(p.correction_leg)='object'
        and p.correction_leg->>'leg_fingerprint'
          =p.recomputed_leg_fingerprint
        and p.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
          =p.recomputed_tsfin_policy_fingerprint
        and p.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
          =p.recomputed_invoice_policy_fingerprint
        and p.correction_leg#>>'{invoice_policy,invoice_stream}'
          =p.envelope->>'invoice_stream'
        and p.current_invoice_stream=p.envelope->>'invoice_stream'
        and coalesce(
          p.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          p.policy_snapshot_json#>>
            '{correction_financials_policy_envelope,envelope_fingerprint}',
          p.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint')
          is not distinct from p.envelope->>'envelope_fingerprint'
        and coalesce(
          p.policy_snapshot_json->>'correction_leg_fingerprint',
          p.rate_source_refs_json->>'correction_leg_fingerprint')
          is not distinct from p.correction_leg->>'leg_fingerprint'
        and coalesce(
          p.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',
          p.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
          is not distinct from
            p.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
        and coalesce(
          p.policy_snapshot_json->>'correction_invoice_policy_fingerprint',
          p.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
          is not distinct from
            p.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
        and p.policy_snapshot_json->'correction_invoice_policy'
          is not distinct from p.correction_leg->'invoice_policy'
        and upper(btrim(coalesce(
          p.policy_snapshot_json->>'correction_invoice_stream','')))
          is not distinct from p.envelope->>'invoice_stream'
      )
    ) frozen_policy_valid
  from policy_validated p
),
target_classes as materialized (
  select t.invoice_id,t.scope_key,t.timesheet_id,c.correction_id,
    c.correction_kind,c.import_declared,c.authoritative,c.envelope,c.client_id,c.contract_id,
    c.week_ending_date,c.basis,c.correction_operation_id,
    c.correction_chain_id
  from targets t
  join policy_final c on c.timesheet_id=t.timesheet_id
),
members as materialized (
  select tc.invoice_id,tc.scope_key,tc.timesheet_id target_timesheet_id,
    tc.correction_id,tc.correction_kind target_kind,
    tc.envelope target_envelope,
    m.timesheet_id,m.correction_kind,m.client_id,m.contract_id,
    m.week_ending_date,m.basis,m.financial_id,m.processing_status,
    m.is_stale,m.stale_reason,m.locked_by_invoice_id,
    m.invoice_breakdown_json,
    m.envelope,m.recomputed_fingerprint,m.is_nhsp,m.autoprocess_hr,
    m.no_timesheet_required,m.import_declared,m.authoritative,
    m.frozen_policy_valid,m.adjustment_origin,m.timesheet_status,
    m.policy_snapshot_json,m.rate_source_refs_json,
    m.recomputed_leg_fingerprint,m.recomputed_tsfin_policy_fingerprint,
    m.recomputed_invoice_policy_fingerprint,m.correction_role,m.correction_shape,m.expected_member_count,
    m.expected_member_roles,m.correction_operation_id,
    m.correction_chain_id,m.frozen_root_timesheet_id,
    m.current_invoice_stream,m.correction_leg
  from target_classes tc
  join policy_final m
    on m.timesheet_id=tc.timesheet_id
      or(
        tc.authoritative
        and m.authoritative
        and m.correction_operation_id=tc.correction_operation_id
        and m.correction_chain_id=tc.correction_chain_id)
      or(
        not tc.authoritative
        and tc.correction_id is not null
        and m.correction_id=tc.correction_id)
),
member_ancestors as (
  select m.scope_key,m.target_timesheet_id,m.timesheet_id member_timesheet_id,
    t.timesheet_id ancestor_id,t.parent_timesheet_id,0 depth,
    array[t.timesheet_id]::uuid[] path,false cycle
  from members m
  join public.timesheets t on t.timesheet_id=m.timesheet_id
  union all
  select a.scope_key,a.target_timesheet_id,a.member_timesheet_id,
    p.timesheet_id,p.parent_timesheet_id,a.depth+1,
    a.path||p.timesheet_id,p.timesheet_id=any(a.path)
  from member_ancestors a
  join public.timesheets p on p.timesheet_id=a.parent_timesheet_id
  where a.parent_timesheet_id is not null
    and not a.cycle and a.depth<32
),
member_roots as materialized (
  select distinct on(
      a.scope_key,a.target_timesheet_id,a.member_timesheet_id)
    a.scope_key,a.target_timesheet_id,a.member_timesheet_id,
    a.ancestor_id root_timesheet_id,a.cycle,
    a.depth=32 and a.parent_timesheet_id is not null depth_exceeded
  from member_ancestors a
  order by a.scope_key,a.target_timesheet_id,a.member_timesheet_id,
    a.depth desc,a.ancestor_id
),
rollup as materialized (
  select m.invoice_id,m.scope_key,m.target_timesheet_id timesheet_id,
    m.correction_id,
    bool_or(m.import_declared) import_declared,
    bool_or(m.authoritative) authoritative,
    array_agg(distinct m.timesheet_id order by m.timesheet_id) required_ids,
    count(*) filter(where m.financial_id is not null
      and upper(coalesce(m.processing_status,''))='READY_FOR_INVOICE') ready_count,
    count(*) filter(where m.financial_id is null) missing_financial_count,
    count(*) filter(where coalesce(m.is_stale,false)) stale_financial_count,
    count(*) filter(where m.financial_id is not null
      and upper(coalesce(m.processing_status,''))<>'READY_FOR_INVOICE')
      not_ready_financial_count,
    count(*) filter(where m.locked_by_invoice_id is not null
      and(m.invoice_id is null
        or m.locked_by_invoice_id<>m.invoice_id)) whole_lock_conflict_count,
    count(*) filter(where exists(
      select 1
      from jsonb_array_elements(case
        when jsonb_typeof(m.invoice_breakdown_json->'segments')='array'
          then m.invoice_breakdown_json->'segments'
        else '[]'::jsonb end) seg(value)
      where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is not null
        and(m.invoice_id is null
          or seg.value->>'invoice_locked_invoice_id'<>m.invoice_id::text)
        and(
          not exists(
            select 1 from planned_lines selected
            where selected.scope_key=m.scope_key
              and selected.timesheet_id=m.timesheet_id
              and selected.segment_id is not null)
          or seg.value->>'segment_id' in(
            select selected.segment_id from planned_lines selected
            where selected.scope_key=m.scope_key
              and selected.timesheet_id=m.timesheet_id
              and selected.segment_id is not null)))
    )
      segment_lock_conflict_count,
    count(distinct m.timesheet_id) member_count,
    count(distinct m.client_id) client_count,
    (min(m.client_id::text))::uuid member_client_id,
    count(distinct m.contract_id) contract_count,
    (min(m.contract_id::text))::uuid member_contract_id,
    count(distinct m.week_ending_date) week_count,
    min(m.week_ending_date) member_week,
    count(distinct case when upper(coalesce(m.basis,'')) in(
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',
      'HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)
      stream_count,
    min(case when m.import_declared then m.envelope->>'invoice_stream'
      else m.current_invoice_stream end) expected_invoice_stream,
    count(*) filter(where m.correction_kind like '%_REVERSAL') reversal_count,
    count(*) filter(where m.correction_kind like '%_REPLACEMENT')
      replacement_count,
    bool_or(m.correction_kind like 'CHANGED_HOURS_%') changed_hours,
    bool_or(m.correction_kind like 'CANCELLATION_%') cancellation,
    bool_and(m.frozen_policy_valid) envelope_valid,
    count(*) filter(where m.import_declared
      and jsonb_typeof(m.envelope) is distinct from 'object')
      missing_envelope_count,
    count(*) filter(where m.import_declared
      and coalesce(m.envelope->>'policy_schema_version','')<>
        'IMPORT_CORRECTION_FINANCIALS_POLICY_V2') invalid_schema_count,
    count(*) filter(where m.import_declared
      and coalesce(m.envelope->>'route_family','')<>'IMPORT_AUTHORITATIVE')
      invalid_route_count,
    count(*) filter(where m.import_declared and(
      lower(coalesce(m.envelope#>>'{classification,canonical}','false'))
        not in('true','t','1','yes')
      or lower(coalesce(
        m.envelope#>>'{classification,client_eligible_at_operation}',
        'false')) not in('true','t','1','yes')
      or nullif(btrim(m.envelope#>>'{classification,source_system}'),'')
        is null)) invalid_classification_count,
    count(*) filter(where m.import_declared
      and nullif(m.envelope#>>'{operation,operation_id}','') is null)
      missing_operation_count,
    count(*) filter(where m.import_declared
      and nullif(m.envelope->>'correction_chain_id','') is null)
      missing_chain_count,
    count(*) filter(where m.import_declared and(
      (m.correction_kind like 'CHANGED_HOURS_%'
        and coalesce(m.envelope#>>'{operation,correction_action}','')<>
          'CHANGED_HOURS')
      or(m.correction_kind like 'CANCELLATION_%'
        and coalesce(m.envelope#>>'{operation,correction_action}','')<>
          'CANCELLATION'))) invalid_action_count,
    count(*) filter(where m.import_declared and(
      nullif(m.envelope->>'envelope_fingerprint','') is null
      or m.envelope->>'envelope_fingerprint'
        is distinct from m.recomputed_fingerprint)) envelope_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared
      and jsonb_typeof(m.correction_leg) is distinct from 'object')
      missing_leg_count,
    count(*) filter(where m.import_declared and(
      nullif(m.correction_leg->>'leg_fingerprint','') is null
      or m.correction_leg->>'leg_fingerprint'
        is distinct from m.recomputed_leg_fingerprint))
      leg_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      jsonb_typeof(m.correction_leg->'tsfin_policy') is distinct from 'object'
      or m.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
        is distinct from m.recomputed_tsfin_policy_fingerprint))
      tsfin_policy_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      jsonb_typeof(m.correction_leg->'invoice_policy')
        is distinct from 'object'
      or m.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
        is distinct from m.recomputed_invoice_policy_fingerprint))
      invoice_policy_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      m.correction_leg#>>'{invoice_policy,invoice_stream}'
        is distinct from m.envelope->>'invoice_stream'
      or m.current_invoice_stream
        is distinct from m.envelope->>'invoice_stream'))
      policy_stream_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
      m.policy_snapshot_json#>>
        '{correction_financials_policy_envelope,envelope_fingerprint}',
      m.rate_source_refs_json->>
        'correction_financials_policy_envelope_fingerprint')
      is distinct from m.envelope->>'envelope_fingerprint')
      current_envelope_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_leg_fingerprint',
      m.rate_source_refs_json->>'correction_leg_fingerprint')
      is distinct from m.correction_leg->>'leg_fingerprint')
      current_leg_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',
      m.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
      is distinct from
        m.correction_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}')
      current_tsfin_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and coalesce(
      m.policy_snapshot_json->>'correction_invoice_policy_fingerprint',
      m.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
      is distinct from
        m.correction_leg#>>'{invoice_policy,invoice_policy_fingerprint}')
      current_invoice_fingerprint_mismatch_count,
    count(*) filter(where m.import_declared and(
      m.policy_snapshot_json->'correction_tsfin_policy'
        is distinct from m.correction_leg->'tsfin_policy'
      or m.policy_snapshot_json->'correction_invoice_policy'
        is distinct from m.correction_leg->'invoice_policy'))
      frozen_subpolicy_drift_count,
    count(distinct m.correction_operation_id)
      filter(where m.import_declared) correction_operation_count,
    count(distinct m.correction_chain_id)
      filter(where m.import_declared) correction_chain_count,
    count(distinct m.frozen_root_timesheet_id)
      filter(where m.import_declared) frozen_root_count,
    count(distinct mr.root_timesheet_id)
      filter(where m.import_declared) actual_root_count,
    bool_or(coalesce(mr.cycle,false)) chain_cycle,
    bool_or(coalesce(mr.depth_exceeded,false)) chain_depth_exceeded,
    bool_and(not m.import_declared
      or m.frozen_root_timesheet_id=mr.root_timesheet_id::text)
      frozen_root_matches,
    count(distinct m.envelope->>'envelope_fingerprint')
      filter(where m.import_declared) envelope_count,
    max(m.expected_member_count)
      filter(where m.import_declared) expected_member_count,
    count(distinct m.expected_member_count)
      filter(where m.import_declared) expected_count_variants,
    count(distinct m.correction_shape)
      filter(where m.import_declared) shape_variants,
    min(m.correction_shape)
      filter(where m.import_declared) correction_shape,
    coalesce(jsonb_agg(m.correction_role order by
      case m.correction_role when 'REVERSAL' then 1
        when 'REPLACEMENT' then 2 else 3 end,m.timesheet_id)
      filter(where m.import_declared),'[]'::jsonb) actual_member_roles,
    (min(m.expected_member_roles::text)
      filter(where m.import_declared))::jsonb expected_member_roles
  from members m
  left join member_roots mr on mr.scope_key=m.scope_key
    and mr.target_timesheet_id=m.target_timesheet_id
    and mr.member_timesheet_id=m.timesheet_id
  group by m.invoice_id,m.scope_key,m.target_timesheet_id,m.correction_id
),
candidate_lines as materialized (
  select distinct r.scope_key,r.invoice_id target_invoice_id,
    l.invoice_id line_invoice_id,l.timesheet_id,l.vat_rate_pct,
    false planned
  from rollup r
  join members m on m.scope_key=r.scope_key
    and m.target_timesheet_id=r.timesheet_id
  join public.invoice_lines l on l.timesheet_id=m.timesheet_id
  union all
  select p.scope_key,p.invoice_id,p.invoice_id,p.timesheet_id,p.vat_rate_pct,
    true
  from planned_lines p
  where not exists(
    select 1 from public.invoice_lines l
    where l.invoice_id=p.invoice_id and l.timesheet_id=p.timesheet_id)
),
line_scope as materialized (
  select r.invoice_id,r.scope_key,r.timesheet_id,
    array_agg(distinct l.timesheet_id order by l.timesheet_id)
      filter(where l.timesheet_id is not null and(
        l.planned or(
          r.invoice_id is not null and l.line_invoice_id=r.invoice_id)))
      present_ids,
    array_agg(distinct l.line_invoice_id order by l.line_invoice_id)
      filter(where not l.planned and(
        r.invoice_id is null or l.line_invoice_id<>r.invoice_id))
      conflicting_ids,
    count(distinct l.timesheet_id) filter(where l.timesheet_id is not null and(
      l.planned or(
        r.invoice_id is not null and l.line_invoice_id=r.invoice_id)))
      present_count,
    count(*) filter(where l.timesheet_id is not null and(
        l.planned or(
          r.invoice_id is not null and l.line_invoice_id=r.invoice_id))
      and coalesce(l.vat_rate_pct,-999999) is distinct from
        case when coalesce(
          m.correction_leg#>>'{invoice_policy,applied_vat_rate_pct}','')
          ~'^[+-]?[0-9]+([.][0-9]+)?$'
          then (m.correction_leg#>>
            '{invoice_policy,applied_vat_rate_pct}')::numeric
          else coalesce(l.vat_rate_pct,-999999) end) vat_mismatch_count
  from rollup r
  join members m on m.scope_key=r.scope_key
    and m.target_timesheet_id=r.timesheet_id
  left join candidate_lines l on l.scope_key=r.scope_key
    and l.timesheet_id=m.timesheet_id
  group by r.invoice_id,r.scope_key,r.timesheet_id
),
final as materialized (
  select r.*,coalesce(ls.present_ids,array[]::uuid[]) present_ids,
    coalesce(ls.conflicting_ids,array[]::uuid[]) conflicting_ids,
    coalesce(ls.present_count,0) present_count,
    coalesce(ls.vat_mismatch_count,0) vat_mismatch_count,
    array(select x from unnest(r.required_ids) x
      where not x=any(coalesce(ls.present_ids,array[]::uuid[]))) missing_ids,
    ti.id is not null target_exists,
    ti.status::text target_status,
    ti.issued_at_utc target_issued_at_utc,
    ti.client_id target_client_id,
    case when lower(coalesce(
      ti.header_snapshot_json#>>'{meta,self_bill}','false'))
      in('true','t','1','yes') then 'SELF_BILL' else 'NORMAL' end
      target_invoice_stream,
    coalesce(pair_scope.scope_json,'{}'::jsonb) pair_scope_json,
    (
      not r.import_declared
      or(
        r.correction_operation_count=1
        and r.correction_chain_count=1
        and r.frozen_root_count=1
        and r.actual_root_count=1
        and r.frozen_root_matches
        and not r.chain_cycle
        and not r.chain_depth_exceeded
        and r.envelope_count=1
        and r.expected_count_variants=1
        and r.shape_variants=1
        and r.expected_member_count=r.member_count
        and r.actual_member_roles=r.expected_member_roles
        and r.reversal_count=1
        and r.replacement_count=case
          when r.correction_shape='REVERSAL_ONLY' then 0 else 1 end
        and r.correction_shape in('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
      )
    ) pair_balanced
  from rollup r
  left join line_scope ls
    on ls.invoice_id is not distinct from r.invoice_id
   and ls.scope_key=r.scope_key
   and ls.timesheet_id=r.timesheet_id
  left join public.invoices ti on ti.id=r.invoice_id
  left join lateral (
    select public.invoice_correction_pair_scope_v1(
      r.timesheet_id,null::uuid,null::uuid,false,100) scope_json
    where r.import_declared
  ) pair_scope on true
),
member_results as materialized (
  select f.*,s.request_key,s.external_scope_key,s.validation_purpose,
    s.expected_client_id,s.expected_contract_id,s.natural_source_week,
    s.target_invoice_week,s.expected_invoice_stream scope_expected_stream,
    s.expected_vat_rate_pct,
    case when f.import_declared then 'IMPORT_AUTHORITATIVE'
      when f.correction_id is not null then 'NON_AUTHORITATIVE_CORRECTION'
      else 'ORDINARY' end correction_classification,
    b.blocker_codes,
    jsonb_build_object(
      'actual_member_count',f.member_count,
      'existing_line_member_count',f.present_count,
      'ready_count',f.ready_count,
      'missing_financial_count',f.missing_financial_count,
      'stale_financial_count',f.stale_financial_count,
      'not_ready_financial_count',f.not_ready_financial_count,
      'whole_lock_conflict_count',f.whole_lock_conflict_count,
      'segment_lock_conflict_count',f.segment_lock_conflict_count,
      'reversal_count',f.reversal_count,
      'replacement_count',f.replacement_count,
      'correction_shape',f.correction_shape,
      'expected_member_count',f.expected_member_count,
      'expected_member_roles',f.expected_member_roles,
      'actual_member_roles',f.actual_member_roles,
      'correction_operation_count',f.correction_operation_count,
      'correction_chain_count',f.correction_chain_count,
      'frozen_root_count',f.frozen_root_count,
      'actual_root_count',f.actual_root_count,
      'frozen_root_matches',f.frozen_root_matches,
      'chain_cycle',f.chain_cycle,
      'chain_depth_exceeded',f.chain_depth_exceeded,
      'expected_invoice_stream',f.expected_invoice_stream,
      'target_invoice_stream',case when f.invoice_id is null
        then null else f.target_invoice_stream end,
      'member_client_id',f.member_client_id,
      'member_contract_id',f.member_contract_id,
      'member_week',f.member_week,
      'target_client_id',f.target_client_id,
      'target_status',f.target_status,
      'vat_mismatch_count',f.vat_mismatch_count,
      'policy_failure_counts',jsonb_build_object(
        'missing_envelope',f.missing_envelope_count,
        'invalid_schema',f.invalid_schema_count,
        'invalid_route',f.invalid_route_count,
        'invalid_classification',f.invalid_classification_count,
        'missing_operation',f.missing_operation_count,
        'missing_chain',f.missing_chain_count,
        'invalid_action',f.invalid_action_count,
        'envelope_fingerprint',f.envelope_fingerprint_mismatch_count,
        'missing_leg',f.missing_leg_count,
        'leg_fingerprint',f.leg_fingerprint_mismatch_count,
        'tsfin_policy_fingerprint',f.tsfin_policy_fingerprint_mismatch_count,
        'invoice_policy_fingerprint',f.invoice_policy_fingerprint_mismatch_count,
        'policy_stream',f.policy_stream_mismatch_count,
        'current_envelope_fingerprint',
          f.current_envelope_fingerprint_mismatch_count,
        'current_leg_fingerprint',f.current_leg_fingerprint_mismatch_count,
        'current_tsfin_fingerprint',f.current_tsfin_fingerprint_mismatch_count,
        'current_invoice_fingerprint',
          f.current_invoice_fingerprint_mismatch_count,
        'frozen_subpolicy_drift',f.frozen_subpolicy_drift_count)) detail_json
  from final f
  join scopes s on s.scope_key=f.scope_key
  cross join lateral (
    select coalesce(array_agg(code order by ordinal)
      filter(where code is not null),array[]::text[]) blocker_codes
    from unnest(array[
      case when p_evaluation_date is null
        then 'EVALUATION_DATE_REQUIRED' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and not f.target_exists
        then 'INVOICE_CORRECTION_TARGET_NOT_FOUND' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and(upper(coalesce(f.target_status,''))<>'DRAFT'
          or f.target_issued_at_utc is not null)
        then 'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and f.target_client_id is distinct from f.member_client_id
        then 'INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH' end,
      case when s.expected_client_id is not null
        and s.expected_client_id is distinct from f.member_client_id
        then 'INVOICE_CORRECTION_CLIENT_MISMATCH' end,
      case when s.expected_contract_id is not null
        and s.expected_contract_id is distinct from f.member_contract_id
        then 'INVOICE_CORRECTION_CONTRACT_MISMATCH' end,
      case when s.natural_source_week is not null
        and s.natural_source_week is distinct from f.member_week
        then 'INVOICE_CORRECTION_WEEK_MISMATCH' end,
      case when s.expected_invoice_stream is not null
        and s.expected_invoice_stream is distinct from f.expected_invoice_stream
        then 'INVOICE_CORRECTION_STREAM_MISMATCH' end,
      case when f.invoice_id is not null
        and s.validation_purpose not in('CREDIT_SOURCE','RECONCILE')
        and f.target_invoice_stream is distinct from f.expected_invoice_stream
        then 'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH' end,
      case when f.import_declared and f.missing_envelope_count>0
        then 'INVOICE_CORRECTION_ENVELOPE_MISSING' end,
      case when f.import_declared and f.invalid_schema_count>0
        then 'INVOICE_CORRECTION_ENVELOPE_SCHEMA_INVALID' end,
      case when f.import_declared and f.invalid_route_count>0
        then 'INVOICE_CORRECTION_ROUTE_FAMILY_INVALID' end,
      case when f.import_declared and f.invalid_classification_count>0
        then 'INVOICE_CORRECTION_CLASSIFICATION_INVALID' end,
      case when f.import_declared and f.missing_operation_count>0
        then 'INVOICE_CORRECTION_OPERATION_IDENTITY_INVALID' end,
      case when f.import_declared and f.missing_chain_count>0
        then 'INVOICE_CORRECTION_CHAIN_IDENTITY_INVALID' end,
      case when f.import_declared and f.invalid_action_count>0
        then 'INVOICE_CORRECTION_ACTION_INVALID' end,
      case when f.import_declared and f.envelope_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_ENVELOPE_FINGERPRINT_MISMATCH' end,
      case when f.import_declared and f.envelope_count<>1
        then 'INVOICE_CORRECTION_MEMBER_ENVELOPE_MISMATCH' end,
      case when f.import_declared and f.missing_leg_count>0
        then 'INVOICE_CORRECTION_LEG_MISSING' end,
      case when f.import_declared and f.leg_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_LEG_FINGERPRINT_MISMATCH' end,
      case when f.import_declared
        and f.tsfin_policy_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_TSFIN_POLICY_FINGERPRINT_MISMATCH' end,
      case when f.import_declared
        and f.invoice_policy_fingerprint_mismatch_count>0
        then 'INVOICE_CORRECTION_INVOICE_POLICY_FINGERPRINT_MISMATCH' end,
      case when f.import_declared and f.policy_stream_mismatch_count>0
        then 'INVOICE_CORRECTION_STREAM_MISMATCH' end,
      case when f.import_declared
        and(f.current_envelope_fingerprint_mismatch_count>0
          or f.current_leg_fingerprint_mismatch_count>0
          or f.current_tsfin_fingerprint_mismatch_count>0
          or f.current_invoice_fingerprint_mismatch_count>0)
        then 'INVOICE_CORRECTION_CURRENT_POLICY_FINGERPRINT_MISMATCH' end,
      case when f.import_declared and f.frozen_subpolicy_drift_count>0
        then 'INVOICE_CORRECTION_FROZEN_POLICY_DRIFT' end,
      case when f.chain_cycle then 'INVOICE_CORRECTION_CHAIN_CYCLE' end,
      case when f.chain_depth_exceeded
        then 'INVOICE_CORRECTION_CHAIN_DEPTH_EXCEEDED' end,
      case when f.member_count>100
        then 'INVOICE_CORRECTION_MEMBER_LIMIT_EXCEEDED' end,
      case when f.import_declared and not f.frozen_root_matches
        then 'INVOICE_CORRECTION_ENVELOPE_ROOT_MISMATCH' end,
      case when f.missing_financial_count>0
        then 'INVOICE_CORRECTION_TSFIN_MISSING' end,
      case when f.stale_financial_count>0
        then 'INVOICE_CORRECTION_TSFIN_STALE' end,
      case when f.not_ready_financial_count>0
        or f.ready_count<>f.member_count
        then 'INVOICE_CORRECTION_TSFIN_NOT_READY' end,
      case when f.client_count<>1
        then 'INVOICE_CORRECTION_CLIENT_MISMATCH' end,
      case when f.contract_count<>1
        then 'INVOICE_CORRECTION_CONTRACT_MISMATCH' end,
      case when f.week_count<>1
        then 'INVOICE_CORRECTION_WEEK_MISMATCH' end,
      case when f.stream_count<>1
        then 'INVOICE_CORRECTION_STREAM_MISMATCH' end,
      case when f.whole_lock_conflict_count>0
        then 'INVOICE_CORRECTION_SOURCE_LOCK_CONFLICT' end,
      case when f.segment_lock_conflict_count>0
        then 'INVOICE_CORRECTION_SEGMENT_LOCK_CONFLICT' end,
      case when f.pair_scope_json->>'placement_state'='INCOMPLETE_MOVE'
        then 'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE' end,
      case when cardinality(f.conflicting_ids)>0
        and not (
          coalesce((f.pair_scope_json->>'valid')::boolean,false)
          and f.pair_scope_json->>'placement_state' in ('COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE')
        )
        then 'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES' end,
      case when (cardinality(f.missing_ids)>0
        and not (
          coalesce((f.pair_scope_json->>'valid')::boolean,false)
          and f.pair_scope_json->>'placement_state' in ('COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE')
        ))
        or(f.import_declared
          and coalesce(f.expected_member_count,0)>f.member_count
          and f.pair_scope_json->>'placement_state'<>'INCOMPLETE_MOVE')
        then 'INVOICE_CORRECTION_MEMBER_MISSING' end,
      case when f.vat_mismatch_count>0
        then 'INVOICE_CORRECTION_VAT_POLICY_MISMATCH' end,
      case when f.import_declared and not f.pair_balanced
        then 'INVOICE_CORRECTION_UNIT_INVALID' end
    ]::text[]) with ordinality u(code,ordinal)
  ) b
),
scope_blockers as materialized (
  select s.scope_key,coalesce(array_agg(x.code order by x.first_ordinal)
      filter(where x.code is not null),array[]::text[]) blocker_codes
  from scopes s
  left join lateral (
    select u.code,min(u.ordinal)::integer first_ordinal
    from member_results m
    cross join lateral unnest(m.blocker_codes) with ordinality u(code,ordinal)
    where m.scope_key=s.scope_key
    group by u.code
  ) x on true
  group by s.scope_key
),
scope_summary as materialized (
  select s.request_key,s.external_scope_key scope_key,s.invoice_id,
    case when count(distinct m.timesheet_id)=1 then (min(m.timesheet_id::text))::uuid end
      timesheet_id,
    case when bool_or(coalesce(m.import_declared,false))
      then 'IMPORT_AUTHORITATIVE'
      when bool_or(m.correction_id is not null)
      then 'NON_AUTHORITATIVE_CORRECTION'
      else 'ORDINARY' end correction_classification,
    case when count(distinct m.correction_id)=1 then min(m.correction_id) end
      correction_id,
    coalesce(array_agg(distinct m.correction_id order by m.correction_id)
      filter(where m.correction_id is not null),array[]::text[])
      correction_ids,
    coalesce(array(select distinct x from member_results mr
      cross join lateral unnest(mr.required_ids) q(x)
      where mr.scope_key=s.scope_key order by x),array[]::uuid[])
      required_member_ids,
    coalesce(array(select distinct x from member_results mr
      cross join lateral unnest(mr.missing_ids) q(x)
      where mr.scope_key=s.scope_key order by x),array[]::uuid[])
      missing_member_ids,
    coalesce(array(select distinct x from member_results mr
      cross join lateral unnest(mr.conflicting_ids) q(x)
      where mr.scope_key=s.scope_key order by x),array[]::uuid[])
      conflicting_invoice_ids,
    coalesce(bool_and(m.pair_balanced),true) balanced,
    sb.blocker_codes,
    jsonb_build_object(
      'validation_purpose',s.validation_purpose,
      'evaluation_date',p_evaluation_date,
      'natural_source_week',s.natural_source_week,
      'target_invoice_week',s.target_invoice_week,
      'unit_count',count(distinct(m.correction_id,m.timesheet_id))
        filter(where m.correction_id is not null),
      'units',coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',m.timesheet_id,
        'correction_id',m.correction_id,
        'classification',m.correction_classification,
        'required_member_ids',m.required_ids,
        'missing_member_ids',m.missing_ids,
        'conflicting_invoice_ids',m.conflicting_ids,
        'balanced',m.pair_balanced,
        'blocker_codes',m.blocker_codes,
        'detail',m.detail_json)
        order by m.correction_id nulls last,m.timesheet_id)
        filter(where m.timesheet_id is not null),'[]'::jsonb)) detail_json
  from scopes s
  left join member_results m on m.scope_key=s.scope_key
  join scope_blockers sb on sb.scope_key=s.scope_key
  group by s.request_key,s.external_scope_key,s.scope_key,s.invoice_id,
    s.validation_purpose,s.natural_source_week,s.target_invoice_week,
    sb.blocker_codes
)
select request_key,scope_key,invoice_id,timesheet_id,
  correction_classification,correction_id,correction_ids,
  required_member_ids,missing_member_ids,conflicting_invoice_ids,
  balanced,cardinality(blocker_codes)=0 valid,
  blocker_codes[1] blocker_code,blocker_codes,detail_json
from scope_summary
order by request_key,scope_key;
$function$;

revoke all on function private._invoice_correction_validate_batch(jsonb,date)
  from public,anon,authenticated;
grant execute on function private._invoice_correction_validate_batch(jsonb,date)
  to service_role;
