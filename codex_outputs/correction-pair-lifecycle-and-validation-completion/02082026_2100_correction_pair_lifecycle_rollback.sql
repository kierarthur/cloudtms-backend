-- TEST-only rollback captured immediately before the 02/08/2026 correction-pair lifecycle installation.
-- Restores the exact pre-install definitions; removes the newly introduced preview function.
-- Generated from pg_get_functiondef on test-cloudtms. Do not run against production.

begin;

-- private._invoice_correction_validate_batch(p_scopes jsonb, p_evaluation_date date) pre-install MD5 68e66b7e18cde8a668c592ff225ab0b2
CREATE OR REPLACE FUNCTION private._invoice_correction_validate_batch(p_scopes jsonb, p_evaluation_date date)
 RETURNS TABLE(request_key text, scope_key text, invoice_id uuid, timesheet_id uuid, correction_classification text, correction_id text, correction_ids text[], required_member_ids uuid[], missing_member_ids uuid[], conflicting_invoice_ids uuid[], balanced boolean, valid boolean, blocker_code text, blocker_codes text[], detail_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
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
      case when cardinality(f.conflicting_ids)>0
        then 'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES' end,
      case when cardinality(f.missing_ids)>0
        or(f.import_declared
          and coalesce(f.expected_member_count,0)>f.member_count)
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

-- public._ctms_assert_invoice_correction_lines_v1(p_invoice_id uuid, p_actor_user_id uuid, p_lock_rows boolean, p_context text) pre-install MD5 142d02024c2c01d9090818846e0efa55
CREATE OR REPLACE FUNCTION public._ctms_assert_invoice_correction_lines_v1(p_invoice_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_lock_rows boolean DEFAULT false, p_context text DEFAULT 'IMPORT_CORRECTION'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_id uuid; v_scope jsonb; v_scopes jsonb:='[]'::jsonb;
begin
  for v_id in
    select distinct il.timesheet_id from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  loop
    v_scope := public.invoice_correction_pair_scope_v1(v_id,p_invoice_id,p_actor_user_id,p_lock_rows,100);
    if coalesce((v_scope->>'valid')::boolean,false) is not true
       or coalesce((v_scope->>'existing_line_member_count')::integer,0)
          <> coalesce((v_scope->>'expected_member_count')::integer,0)
       or coalesce((v_scope->>'existing_line_invoice_count')::integer,0)<>1 then
      raise exception 'INVOICE_CORRECTION_LINES_NOT_UNIT_SAFE' using errcode='P0001',detail=v_scope::text;
    end if;
    v_scopes:=v_scopes||jsonb_build_array(v_scope);
  end loop;
  return jsonb_build_object('ok',true,'invoice_id',p_invoice_id,'scopes',v_scopes);
end;
$function$;

-- public._ctms_expand_lifecycle_items_v1(p_items jsonb, p_action text, p_actor_user_id uuid, p_max_members integer) pre-install MD5 79f1a7b9fcc4b31add4843a91df7f6b3
CREATE OR REPLACE FUNCTION public._ctms_expand_lifecycle_items_v1(p_items jsonb, p_action text, p_actor_user_id uuid, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_result jsonb := '[]'::jsonb;
  v_seen_correction_ids text[] := array[]::text[];
  v_item jsonb;
  v_id_text text;
  v_id uuid;
  v_class jsonb;
  v_transition jsonb;
  v_correction_id text;
begin
  if v_action not in ('AUTHORISE', 'UNAUTHORISE') then
    raise exception 'CORRECTION_LIFECYCLE_ACTION_INVALID' using errcode = '22023';
  end if;
  if p_actor_user_id is null then
    raise exception 'CORRECTION_LIFECYCLE_ACTOR_REQUIRED' using errcode = '22023';
  end if;
  if jsonb_typeof(coalesce(p_items, '[]'::jsonb)) <> 'array'
     or jsonb_array_length(coalesce(p_items, '[]'::jsonb)) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_ITEMS_INVALID' using errcode = '22023';
  end if;

  for v_item in
    select value from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
  loop
    v_id_text := nullif(btrim(coalesce(
      v_item ->> 'timesheet_id', v_item ->> 'timesheetId',
      v_item ->> 'current_timesheet_id', v_item ->> 'currentTimesheetId',
      v_item ->> 'requested_timesheet_id', v_item ->> 'requestedTimesheetId',
      case when coalesce(v_item ->> 'row_key', '') like 'timesheet:%'
        then substring(v_item ->> 'row_key' from 11) end,
      ''
    )), '');

    if v_id_text is null
       or v_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
      v_result := v_result || jsonb_build_array(v_item);
      continue;
    end if;

    v_id := v_id_text::uuid;
    v_class := public._ctms_import_correction_classify_v1(v_id);
    if coalesce((v_class ->> 'is_import_authoritative_correction')::boolean, false) is not true then
      v_result := v_result || jsonb_build_array(v_item);
      continue;
    end if;

    v_correction_id := nullif(v_class ->> 'correction_id', '');
    if v_correction_id = any(v_seen_correction_ids) then
      continue;
    end if;

    v_transition := public.timesheet_correction_pair_transition_v1(
      v_id, v_action, p_actor_user_id, null::uuid, null::text, true, p_max_members
    );
    if coalesce((v_transition ->> 'valid')::boolean, false) is not true
       or coalesce((v_transition ->> 'action_ready')::boolean, false) is not true then
      raise exception 'CORRECTION_UNIT_LIFECYCLE_TRANSITION_BLOCKED'
        using errcode = 'P0001', detail = v_transition::text;
    end if;

    v_seen_correction_ids := array_append(v_seen_correction_ids, v_correction_id);
    v_result := v_result || coalesce(v_transition -> 'transition_items', '[]'::jsonb);
  end loop;

  if jsonb_array_length(v_result) > p_max_members then
    raise exception 'CORRECTION_LIFECYCLE_EXPANSION_TOO_LARGE' using errcode = '22023';
  end if;
  return v_result;
end;
$function$;

-- public._import_review_action_catalog_core_v1(p_import_id uuid, p_preview_generation integer, p_max_actions integer) pre-install MD5 1e36c2a336d5a925c479fde70a0aa942
CREATE OR REPLACE FUNCTION public._import_review_action_catalog_core_v1(p_import_id uuid, p_preview_generation integer, p_max_actions integer DEFAULT 5000)
 RETURNS TABLE(action_id text, action_kind text, action_category text, target_key text, source_identity text, hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid, contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean, default_selected boolean, blocking boolean, summary_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_count integer; v_weekly_preview jsonb;
begin
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>5000 then
    raise exception 'IMPORT_REVIEW_ACTION_CATALOG_INPUT_INVALID' using errcode='22023';
  end if;

  create temporary table if not exists pg_temp.import_review_catalog_v1 (
    action_id text, action_kind text, action_category text, target_key text, source_identity text,
    hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid,
    contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean,
    default_selected boolean, blocking boolean, summary_json jsonb
  ) on commit drop;
  truncate pg_temp.import_review_catalog_v1;

  insert into pg_temp.import_review_catalog_v1
  with import_row as (
    select hi.* from public.hr_imports hi where hi.id=p_import_id
  ), raw as (
    select r.*, i.source_system::text as source_system, upper(coalesce(i.import_scope,'')) as import_scope,
      i.client_id as import_client_id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,'')) as staff_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),'') as staff_key,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),nullif(r.unit_raw,''),nullif(r.unit_hint,'')) as client_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),'') as client_key,
      lower(btrim(coalesce(nullif(r.assignment_grade_norm,''),r.payload_json->>'grade_raw',r.payload_json->>'Request_Grade',''))) as grade_key,
      coalesce(nullif(r.external_row_key,''),'hr-row:'||r.id::text) as source_row_key
    from public.hr_rows r join import_row i on true where r.import_id=p_import_id
    order by r.id limit 501
  ), mapped as (
    select raw.*,
      coalesce(c_alias.id,c_map.candidate_id,c_exact.candidate_id) as resolved_candidate_id,
      coalesce(raw.import_client_id,ch.client_id,c_client.client_id) as resolved_client_id
    from raw
    left join lateral (
      select c.id from public.candidates c
      where c.nhsp_hr_name_aliases is not null and raw.staff_key is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[raw.staff_key]::text[])
      order by c.id limit 1
    ) c_alias on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(coalesce(raw.staff_label,''))),raw.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) c_map on c_alias.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as candidate_id
      from public.candidates c where c.active and raw.staff_key is not null
        and (regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=raw.staff_key
          or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=raw.staff_key)
    ) c_exact on c_alias.id is null and c_map.candidate_id is null
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where raw.client_key is not null and ch.hospital_name_norm @> to_jsonb(array[raw.client_key]::text[])
      order by ch.id limit 1
    ) ch on raw.import_client_id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as client_id
      from public.clients c where raw.client_key is not null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=raw.client_key
    ) c_client on raw.import_client_id is null and ch.client_id is null
  ), weekly_phase as materialized (
    -- weekly_import_phase2 remains the single authority for assignment-code
    -- mapping precedence and contract choice.  The review catalogue consumes
    -- its answer rather than maintaining a second resolver.
    select w.*
    from import_row i
    cross join lateral public.weekly_import_phase2(
      p_import_id,
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end
    ) w
    where not (upper(i.source_system::text)='HEALTHROSTER_DAILY'
      or upper(coalesce(i.import_scope,'')) like '%DAILY%')
  ), classified as (
    select m.*,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
        then case when rtsx.contract_id is not null then 1 else 0 end
        else con.contract_count end as contract_count,
      case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end as resolved_contract_id,
      wp.action as weekly_resolution_action,wp.reason as weekly_resolution_reason,
      wp.incoming_code as weekly_incoming_code,
      wp.week_ending_date as resolved_week_ending_date,
      wm.has_weekly_mapping,wm.mapping_evidence as weekly_mapping_evidence,
      dgm.mapping_count as daily_mapping_count,dgm.mapping_id as daily_mapping_id,
      dgm.role_code as daily_mapped_role,dgm.band_norm as daily_mapped_band,
      dgm.updated_at as daily_mapping_updated_at,(coalesce(dgm.mapping_count,0)=1) as has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,tsx.timesheet_evidence_hash,
      dtsx.submitted_timesheet_count as daily_submitted_timesheet_count,
      dtsx.submitted_timesheet_evidence_hash as daily_submitted_timesheet_evidence_hash,
      tsx.timesheet_contract_ids,dcon.contract_ids as eligible_contract_ids,dcon.contract_evidence_hash,
      cr.route_eligible as contract_route_eligible,cr.rate_complete as contract_rate_complete,
      cr.import_authoritative,cr.authority_mode,cr.authority_fingerprint,
      cr.rate_evidence as contract_rate_evidence,
      wopts.options as weekly_contract_options,dopts.options as daily_role_options,
      res.resolved_timesheet_id as stored_timesheet_id,res.status as resolution_status,
      coalesce(case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id) as resolved_timesheet_id,
      nss.id as existing_shift_id,nss.timesheet_id as existing_shift_timesheet_id,
      nss.start_utc as existing_shift_start_utc,nss.end_utc as existing_shift_end_utc,
      nss.break_mins as existing_shift_break_minutes,nss.pay_minutes as existing_shift_paid_minutes,
      nss.assignment_code as existing_shift_role,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then true else false end as is_daily
    from mapped m
    left join weekly_phase wp on wp.hr_row_id=m.id
    left join lateral (
      select count(*)::integer contract_count,
             case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id
      from public.contracts c
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
    ) con on true
    left join lateral (
      select count(*)::integer mapping_count,
        (array_agg(gm.id order by gm.updated_at desc,gm.id))[1] mapping_id,
        (array_agg(gm.role_code order by gm.updated_at desc,gm.id))[1] role_code,
        (array_agg(gm.band_norm order by gm.updated_at desc,gm.id))[1] band_norm,
        (array_agg(gm.updated_at order by gm.updated_at desc,gm.id))[1] updated_at
      from public.hr_daily_grade_role_mappings gm
      where gm.client_id=m.resolved_client_id and gm.incoming_grade_norm=m.grade_key and gm.active
    ) dgm on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer contract_count,
        case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id,
        array_agg(c.id order by c.id) contract_ids,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',c.id,c.updated_at,c.role,c.band,a.authority_fingerprint),',' order by c.id),'')) contract_evidence_hash
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1('HR_DAILY',c.id,c.client_id,m.date_local) a
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        and coalesce(dgm.mapping_count,0)=1 and a.route_eligible
        and lower(btrim(coalesce(c.role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(c.band,'')))=lower(btrim(dgm.band_norm)))
    ) dcon on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer submitted_timesheet_count,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,
          t.worked_end_iso,t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,
          t.tsfin_role,t.tsfin_band,ts.contract_id,ts.updated_at),',' order by t.timesheet_id),''))
          submitted_timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
    ) dtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      with candidates as (
        select abm.*,
          case when abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id then 3
            when abm.candidate_id=m.resolved_candidate_id and abm.client_id is null then 2
            when abm.candidate_id is null and abm.client_id=m.resolved_client_id then 1 else 0 end specificity
        from public.assignment_band_mappings abm
        where abm.active and upper(btrim(abm.system_type))=
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end
          and lower(btrim(abm.incoming_code))=m.grade_key
          and ((abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id=m.resolved_candidate_id and abm.client_id is null)
            or (abm.candidate_id is null and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id is null and abm.client_id is null))
      ), chosen as (select * from candidates where specificity=(select max(specificity) from candidates))
      select exists(select 1 from chosen) has_weekly_mapping,
        public._import_review_hash_v1(coalesce((select string_agg(concat_ws('|',id,updated_at,target_contract_id,band_match_pattern),',' order by id)
          from chosen),'')) mapping_evidence
    ) wm on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select count(*)::integer timesheet_count,
             array_agg(t.timesheet_id order by t.worked_start_iso,t.timesheet_id) timesheet_ids,
             array_agg(ts.contract_id order by t.worked_start_iso,t.timesheet_id) timesheet_contract_ids,
             case when count(*)=1 then (array_agg(t.timesheet_id order by t.timesheet_id))[1] end auto_timesheet_id,
             public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,t.worked_end_iso,
               t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,t.tsfin_role,t.tsfin_band,
               ts.contract_id,ts.updated_at),',' order by t.timesheet_id),'')) timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
        and coalesce(dgm.mapping_count,0)=1
        and lower(btrim(coalesce(t.tsfin_role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(t.tsfin_band,'')))=lower(btrim(dgm.band_norm)))
    ) tsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.import_review_daily_timesheet_resolutions res
      on res.import_id=p_import_id and res.hr_row_id=m.id and res.status in ('CURRENT','APPLIED')
    left join lateral (
      select ts.contract_id
      from public.timesheets ts
      where ts.timesheet_id=coalesce(
        case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id)
        and ts.is_current and ts.revoked_at is null
      order by ts.updated_at desc limit 1
    ) rtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select a.route_eligible,a.import_authoritative,a.authority_mode,a.authority_fingerprint,
        (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else
            (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete,
        public._import_review_hash_v1(concat_ws('|',c.id,c.updated_at,c.start_date,c.end_date,c.role,c.band,
          c.pay_method_snapshot,c.rates_json,c.overrideclientsettings,c.is_nhsp,c.autoprocess_hr,c.requires_hr,
          c.no_timesheet_required,a.client_settings_id,a.client_settings_updated_at,
          a.effective_is_nhsp,a.effective_autoprocess_hr,a.effective_requires_hr,
          a.effective_no_timesheet_required,a.authority_fingerprint)) rate_evidence
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1(
        case when upper(m.source_system)='NHSP' then 'NHSP'
          when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then 'HR_DAILY'
          else 'HR_WEEKLY' end,c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
      where c.id=case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end
    ) cr on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','contract:'||o.id::text,'contract_id',o.id,'candidate_id',o.candidate_id,'client_id',o.client_id,
        'role',o.role,'band',o.band,'site',o.display_site,'start_date',o.start_date,'end_date',o.end_date,
        'source_route_eligible',coalesce(o.route_eligible,false),'rate_complete',coalesce(o.rate_complete,false),
        'authority_mode',o.authority_mode,
        -- Choosing a contract records the server-approved assignment mapping;
        -- it does not apply the import or grant financial authority.  An
        -- authoritative contract with incomplete rates must therefore remain
        -- selectable here and will still be blocked by the refreshed action
        -- catalogue before final application.
        'selectable',coalesce(o.route_eligible,false),
        'disabled_reason_code',case when not coalesce(o.route_eligible,false) then 'CONTRACT_NOT_ELIGIBLE' end,
        'display_label',concat_ws(' · ',nullif(o.role,''),nullif(o.band,''),nullif(o.display_site,''),
          to_char(o.start_date,'DD Mon YYYY')||' to '||coalesce(to_char(o.end_date,'DD Mon YYYY'),'open ended'))
      ) order by lower(coalesce(o.role,'')),lower(coalesce(o.band,'')),o.start_date desc,o.id),'[]'::jsonb) options
      from (
        select c.*,a.route_eligible,a.import_authoritative,a.authority_mode,
          (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete
        from public.contracts c
        cross join lateral public._import_review_effective_authority_core_v1(
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end,
          c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
        where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
          and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        order by c.start_date desc,c.id limit 25
      ) o
    ) wopts on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','daily-role:'||public._import_review_hash_v1(lower(concat_ws('|',o.role,o.band))),
        'role_code',o.role,'band_norm',o.band,'selectable',true,
        'display_label',concat_ws(' · ',nullif(o.role,''),coalesce(nullif(o.band,''),'No band'))
      ) order by lower(o.role),lower(coalesce(o.band,''))),'[]'::jsonb) options
      from (
        select distinct t.tsfin_role role,t.tsfin_band band
        from public.v_timesheets_daily_match t
        where t.candidate_id=m.resolved_candidate_id
          and t.client_id=m.resolved_client_id
          and t.sheet_scope::text='DAILY'
          and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
          and nullif(btrim(t.tsfin_role),'') is not null
        order by t.tsfin_role,t.tsfin_band
        limit 25
      ) o
    ) dopts on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,ts.tsfin_role,ts.tsfin_band,
      coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id) as authoritative_target_timesheet_id,
      public._import_review_timesheet_has_calculated_expenses_core_v1(
        coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
      ) as authoritative_timesheet_has_calculated_expenses,
      mutable_replacement.timesheet_id as mutable_replacement_timesheet_id,
      mutable_replacement.protection as mutable_replacement_protection,
      source_timesheet.authorised_at_server as source_authorised_at_server,
      source_tf.authorised_at_utc as source_tsfin_authorised_at_utc,
      source_tf.policy_snapshot_json as source_policy_snapshot_json,
      source_tf.basis::text as source_tsfin_basis,
      authoritative_hours.hours_day as authoritative_hours_day,
      authoritative_hours.hours_night as authoritative_hours_night,
      authoritative_hours.hours_sat as authoritative_hours_sat,
      authoritative_hours.hours_sun as authoritative_hours_sun,
      authoritative_hours.hours_bh as authoritative_hours_bh,
      authoritative_hours.total_hours as authoritative_total_hours,
      coalesce((auto_authorise.value->>'effective_value')::boolean,false) as effective_auto_authorise,
      public._import_review_timesheet_protection_core_v1(coalesce(
        c.resolved_timesheet_id,c.existing_shift_timesheet_id,base_week.timesheet_id
      )) as protection
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
    left join lateral (
      select cw.timesheet_id
      from public.contract_weeks cw
      where not c.is_daily
        and coalesce(c.import_authoritative,false)
        and cw.contract_id=c.resolved_contract_id
        and cw.week_ending_date=coalesce(
          c.resolved_week_ending_date,
          c.date_local + ((7-extract(dow from c.date_local)::integer)%7)
        )
        and cw.is_adjustment=false
        and coalesce(cw.additional_seq,0)=0
      order by cw.id
      limit 1
    ) base_week on true
    left join public.timesheets source_timesheet
      on source_timesheet.timesheet_id=coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
    left join public.timesheets_financials source_tf
      on source_tf.timesheet_id=source_timesheet.timesheet_id and source_tf.is_current=true
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(source_tf.policy_snapshot_json,'{}'::jsonb),
      (c.payload_json->>'start_utc')::timestamptz,
      (c.payload_json->>'end_utc')::timestamptz,
      coalesce((c.payload_json->>'actual_break_mins')::integer,
        (c.payload_json->>'actual_break_minutes')::integer,
        (c.payload_json->>'break_mins')::integer,
        (c.payload_json->>'break_minutes')::integer,0)
    ) authoritative_hours on not c.is_daily and coalesce(c.import_authoritative,false)
      and c.existing_shift_id is not null
    left join lateral (
      select case
        when not c.is_daily
          and coalesce(c.import_authoritative,false)
          and c.resolved_client_id is not null
          and c.resolved_contract_id is not null
        then public.import_auto_authorise_policy_resolve_v1(
          case when upper(c.source_system)='NHSP' then 'NHSP'::public.hr_source_enum else 'HEALTHROSTER'::public.hr_source_enum end,
          c.resolved_client_id,c.resolved_contract_id,false
        )
        else null::jsonb
      end as value
    ) auto_authorise on true
    left join lateral (
      select replacement_candidate.timesheet_id,replacement_candidate.protection
      from (
        select
          replacement_timesheet.timesheet_id,
          replacement_timesheet.updated_at,
          replacement_timesheet.created_at,
          public._import_review_timesheet_protection_core_v1(
            replacement_timesheet.timesheet_id
          ) as protection
        from public.timesheets replacement_timesheet
        where not c.is_daily
          and c.existing_shift_id is not null
          and replacement_timesheet.is_adjustment is true
          and replacement_timesheet.is_current is true
          and replacement_timesheet.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and jsonb_typeof(replacement_timesheet.actual_schedule_json)='array'
          and replacement_timesheet.actual_schedule_json @> jsonb_build_array(
            jsonb_build_object(
              'shift_id',c.existing_shift_id::text,
              'external_row_key',c.source_row_key
            )
          )
      ) replacement_candidate
      where coalesce(
          (replacement_candidate.protection->>'paid')::boolean,
          false
        ) is false
        and coalesce(
          (replacement_candidate.protection->>'invoice_locked')::boolean,
          false
        ) is false
      order by
        replacement_candidate.updated_at desc nulls last,
        replacement_candidate.created_at desc nulls last
      limit 1
    ) mutable_replacement on true
  ), reconciliation_source_rows as (
    select
      f.*,
      ((row_number() over (order by f.source_row_key) - 1) / 100)::integer as reconciliation_batch
    from facts f
    where not f.is_daily and coalesce(f.import_authoritative,false) and f.existing_shift_id is not null
  ), reconciliation_inputs as (
    select coalesce(jsonb_agg(jsonb_build_object(
      'source_identity',f.source_row_key,
      'source_system',case when upper(f.source_system)='NHSP' then 'NHSP' else 'HEALTHROSTER' end,
      'source_shift_id',f.existing_shift_id,
      'external_row_key',f.source_row_key,
      'hr_row_id',f.id,
      'source_timesheet_id',coalesce(f.existing_shift_timesheet_id,f.authoritative_target_timesheet_id),
      'candidate_id',f.resolved_candidate_id,'client_id',f.resolved_client_id,'contract_id',f.resolved_contract_id,
      'week_ending_date',coalesce(f.resolved_week_ending_date,f.date_local+((7-extract(dow from f.date_local)::integer)%7)),
      'invoice_stream',case when upper(coalesce(f.source_tsfin_basis,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end,
      'authoritative_import_id',p_import_id,
      'authoritative_schedule_json',jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'date',f.date_local,'start_utc',f.payload_json->>'start_utc','end_utc',f.payload_json->>'end_utc',
        'break_mins',coalesce((f.payload_json->>'actual_break_mins')::integer,(f.payload_json->>'actual_break_minutes')::integer,
          (f.payload_json->>'break_mins')::integer,(f.payload_json->>'break_minutes')::integer,0),
        'shift_id',f.existing_shift_id,'external_row_key',f.source_row_key,'import_id',p_import_id,
        'ref_num',coalesce(f.hr_request_id,f.payload_json->>'ref_num',f.payload_json->>'reference_number')
      ))),
      'authoritative_hours',jsonb_build_object(
        'hours_day',coalesce(f.authoritative_hours_day,0),'hours_night',coalesce(f.authoritative_hours_night,0),
        'hours_sat',coalesce(f.authoritative_hours_sat,0),'hours_sun',coalesce(f.authoritative_hours_sun,0),
        'hours_bh',coalesce(f.authoritative_hours_bh,0),'total_hours',coalesce(f.authoritative_total_hours,f.hours_worked,0)
      )
    ) order by f.source_row_key),'[]'::jsonb) items
    from reconciliation_source_rows f
    group by f.reconciliation_batch
  ), reconciliation_balances as materialized (
    select b.source_identity,b.balance_json
    from reconciliation_inputs i
    cross join lateral public._import_review_effective_invoice_balance_core_v1(
      p_import_id,i.items,100,512,256,128
    ) b
  ), evidenced as (
    select c.*,
      rb.balance_json as reconciliation_balance,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.daily_submitted_timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        c.authoritative_target_timesheet_id,c.authoritative_timesheet_has_calculated_expenses,
        c.mutable_replacement_timesheet_id,coalesce(c.mutable_replacement_protection::text,''),
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,coalesce(rb.balance_json::text,''),
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
    left join reconciliation_balances rb on rb.source_identity=c.source_row_key
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when not f.is_daily and coalesce(f.weekly_resolution_action,'')<>'OK' then 'ADVISORY'
        when not f.is_daily and coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when not f.is_daily and not coalesce(f.contract_route_eligible,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when not coalesce(f.import_authoritative,false) then 'NO_ACTION'
        when not coalesce(f.contract_rate_complete,false) then 'ADVISORY'
        when coalesce(f.authoritative_timesheet_has_calculated_expenses,false) then 'ADVISORY'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when coalesce((f.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
          then 'APPLY_AMENDMENT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or ((f.payload_json->>'break_mins') is not null
            and (f.payload_json->>'break_mins')::integer is distinct from
              coalesce((select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id),0))
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from evidenced f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY'
             or nullif(m.reconciliation_balance->>'blocking_code','') is not null
             or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
        or nullif(m.reconciliation_balance->>'blocking_code','') is not null
        or coalesce((m.protection->>'active_pay_draft')::boolean,false)) blocking,
      jsonb_strip_nulls(jsonb_build_object(
        'reason_code',case
          when m.resolved_candidate_id is null then 'CANDIDATE_UNRESOLVED'
          when m.resolved_client_id is null then 'CLIENT_UNRESOLVED'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT' then 'CONTRACT_MISSING'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and coalesce(m.weekly_resolution_action,'')<>'OK' then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when not m.is_daily and not coalesce(m.contract_route_eligible,false) then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and not coalesce(m.contract_rate_complete,false) then 'CONTRACT_RATES_INCOMPLETE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'DAILY_TIMESHEET_NOT_SUBMITTED'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when nullif(m.reconciliation_balance->>'blocking_code','') is not null
            then m.reconciliation_balance->>'blocking_code'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'existing_shift_id',m.existing_shift_id,
        'invoice_stream',m.reconciliation_balance->>'invoice_stream',
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
        'amendment_route',case
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'AMEND_EXISTING_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'effective_hours_net_is_positive')::boolean,false)
            and coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,false)
            then 'CREATE_REVERSAL_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.protection->>'paid')::boolean,false)
            then 'AMEND_PAID_UNINVOICED_SOURCE'
          when m.action_kind='APPLY_AMENDMENT' then 'AMEND_SOURCE'
          else null
        end,
        'reconciliation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'FROZEN_INVOICE_BALANCE'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_SOURCE'
          else null end,
        'mutable_replacement_timesheet_id',coalesce(
          (select x.value::uuid from jsonb_array_elements_text(coalesce(m.reconciliation_balance->'active_mutable_member_ids','[]'::jsonb)) x(value)
            join public.timesheets mutable_ts on mutable_ts.timesheet_id=x.value::uuid and mutable_ts.correction_kind='CHANGED_HOURS_REPLACEMENT' limit 1),
          m.mutable_replacement_timesheet_id),
        'correction_id',m.reconciliation_balance->>'active_mutable_correction_id',
        'reviewed_existing_correction_id',m.reconciliation_balance->>'reviewed_existing_correction_id',
        'repair_identity_mode',m.reconciliation_balance->>'repair_identity_mode',
        'physically_missing_mutable_roles',coalesce(m.reconciliation_balance->'physically_missing_mutable_roles','[]'::jsonb),
        'archived_ignored_roles',coalesce(m.reconciliation_balance->'archived_history_roles','[]'::jsonb),
        'reversal_repair_required',coalesce((m.reconciliation_balance->>'reversal_repair_required')::boolean,false),
        'replacement_repair_required',coalesce((m.reconciliation_balance->>'replacement_repair_required')::boolean,false),
        'correction_generation_required',coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
          and not coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false),
        'standard_representable',coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,true),
        'B_hours',m.reconciliation_balance->'B_hours','B_financials',m.reconciliation_balance->'B_financials',
        'B_standard_schedule_json',m.reconciliation_balance->'B_standard_schedule_json',
        'B_policy_fingerprint',m.reconciliation_balance->>'B_policy_fingerprint',
        'review_policy_basis_kind','IMPORT_AUTHORITATIVE_WEEKLY_V1',
        'review_policy_basis_fingerprint',public._import_review_hash_v1(concat_ws('|','review-policy-basis-v1',
          m.reconciliation_balance->>'source_scope_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
          m.reconciliation_balance->>'role_evidence_fingerprint',m.authority_fingerprint,
          m.reconciliation_balance->>'B_policy_fingerprint',m.reconciliation_balance->>'invoice_stream')),
        'effective_invoice_ids',m.reconciliation_balance->'effective_invoice_ids',
        'effective_invoice_line_ids',m.reconciliation_balance->'effective_invoice_line_ids',
        'M_hours',m.reconciliation_balance->'M_hours','M_existing_financials',m.reconciliation_balance->'M_existing_financials',
        'A_hours',m.reconciliation_balance->'A_hours','A_schedule_json',m.reconciliation_balance->'A_schedule_json',
        'effective_invoice_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
        'mutable_generation_fingerprint',m.reconciliation_balance->>'active_mutable_fingerprint',
        'authoritative_evidence_fingerprint',m.reconciliation_balance->>'A_evidence_fingerprint',
        'reconciliation_fingerprint',m.reconciliation_balance->>'reconciliation_fingerprint',
        'source_scope_fingerprint',m.reconciliation_balance->>'source_scope_fingerprint'
      ) || jsonb_build_object(
        'archived_timesheet_ids',m.reconciliation_balance->'archived_timesheet_ids',
        'archived_history_timesheet_ids',m.reconciliation_balance->'archived_history_timesheet_ids',
        'archived_history_roles',m.reconciliation_balance->'archived_history_roles',
        'historical_missing_timesheet_ids',m.reconciliation_balance->'historical_missing_timesheet_ids',
        'active_mutable_member_ids',m.reconciliation_balance->'active_mutable_member_ids',
        'missing_mutable_roles',m.reconciliation_balance->'active_mutable_missing_roles',
        'active_mutable_parent_timesheet_id',m.reconciliation_balance->>'active_mutable_parent_timesheet_id',
        'pre_apply_authorised',m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null,
        'effective_auto_authorise',m.effective_auto_authorise,
        'intended_authorisation_action',case
          when m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null then 'REAUTHORISE'
          when m.effective_auto_authorise then 'AUTHORISE' else 'LEAVE_UNAUTHORISED' end,
        'financial_validation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'CORRECTION_NEGATIVE_MUST_REVERSE_FROZEN_B_AND_POSITIVE_TSFIN_DEFINES_A'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_TSFIN_DEFINES_A' end,
        'candidate_name',m.staff_label,'client_name',m.client_label,'work_date',m.date_local,
        'week_ending_date',m.date_local + ((7-extract(dow from m.date_local)::integer)%7),
        'start_time',m.start_time_local,'end_time',m.end_time_local,
        'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
          (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
        'hours_worked',m.hours_worked,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.start_time_local,'end',m.end_time_local,
          'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
            (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
          'worked_hours',m.hours_worked,'worked_minutes',case when m.hours_worked is null then null else round(m.hours_worked*60) end,
          'reference',m.hr_request_id,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),'grade',m.grade_key)),
        'current_evidence',case when m.is_daily and m.resolved_timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'start',m.worked_start_iso,'end',m.worked_end_iso,
          'break_minutes',m.ts_break_minutes,'elapsed_minutes',m.worked_minutes,
          'worked_minutes',greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0),
          'worked_hours',round(greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0)/60.0,2),
          'reference',m.reference_number,'role',m.tsfin_role,'band',m.tsfin_band,'timesheet_id',m.resolved_timesheet_id))
          when not m.is_daily and m.existing_shift_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.existing_shift_start_utc,'end',m.existing_shift_end_utc,
          'break_minutes',m.existing_shift_break_minutes,'worked_minutes',m.existing_shift_paid_minutes,
          'role',m.existing_shift_role,'timesheet_id',m.existing_shift_timesheet_id,'shift_id',m.existing_shift_id)) end,
        'difference_codes',to_jsonb(array_remove(array[
          case when m.existing_shift_id is null and not m.is_daily then 'NEW_SHIFT'::text end,
          case when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_SELECTION_REQUIRED'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.start_time_local is distinct from
            (m.worked_start_iso at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.end_time_local is distinct from
            (m.worked_end_iso at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.ts_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0))>1
            then 'WORKED_HOURS'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.start_time_local is distinct from
            (m.existing_shift_start_utc at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.end_time_local is distinct from
            (m.existing_shift_end_utc at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.existing_shift_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-m.existing_shift_paid_minutes)>1 then 'WORKED_HOURS'::text end,
          case when not m.is_daily and coalesce((m.reconciliation_balance->>'financial_position_requires_amendment')::boolean,false)
            then 'FINANCIAL_POSITION'::text end
        ],null)),
        'outcome_label',case
          when not m.is_daily and not coalesce(m.import_authoritative,false) then 'Validate candidate timesheet'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'Request timesheet from candidate'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'Candidate timesheet states they did not work this shift'
          when not m.is_daily and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'Timesheet occupied by expenses'
          when m.action_kind='INCLUDE_SHIFT' then 'TMS will add shift'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'TMS will repair current correction generation'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'TMS will create correction generation'
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.protection->>'paid')::boolean,false)
            then 'TMS will amend paid uninvoiced shift'
          when m.action_kind='APPLY_AMENDMENT' then 'TMS will amend shift'
          when m.action_kind='APPLY_CANCELLATION' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift' else 'TMS will cancel shift' end
          when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' when m.action_kind='NO_ACTION' then 'No action required'
          else 'Resolve before continuing' end,
        'resolution_kind',case
          when m.resolved_candidate_id is null then 'CANDIDATE_LINK'
          when m.resolved_client_id is null then 'CLIENT_LINK'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'DAILY_GRADE_ROLE'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'WEEKLY_ASSIGNMENT_CONTRACT'
          when m.is_daily and m.resolved_timesheet_id is null and coalesce(m.timesheet_count,0)>0 then 'DAILY_EXISTING_TIMESHEET' end,
        'resolution_options',case
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then m.daily_role_options
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH' then m.weekly_contract_options
          else '[]'::jsonb end,
        'mapping_evidence',case when m.is_daily then jsonb_strip_nulls(jsonb_build_object(
          'mapping_id',m.daily_mapping_id,'updated_at',m.daily_mapping_updated_at,'role',m.daily_mapped_role,'band',m.daily_mapped_band))
          else jsonb_strip_nulls(jsonb_build_object('mapping_fingerprint',m.weekly_mapping_evidence,
            'resolution_action',m.weekly_resolution_action,'resolution_reason',m.weekly_resolution_reason)) end,
        'timesheet_options',case when m.is_daily then to_jsonb(coalesce(m.timesheet_ids,array[]::uuid[])) else null end,
        'occupied_timesheet_id',case when coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
          then m.authoritative_target_timesheet_id end,
        'protection',m.protection
      )) summary_json
    from main_actions m
  )
  select * from rendered;

  -- Daily mismatch/query actions are independent of the evidence association.
  insert into pg_temp.import_review_catalog_v1
  with r as (
    select h.*,d.resolved_timesheet_id as timesheet_id,t.candidate_id,t.client_id,t.worked_start_iso,t.worked_end_iso,
      t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status::text,
      c.id contract_id,public._import_review_timesheet_protection_core_v1(d.resolved_timesheet_id) protection
    from public.hr_rows h
    join public.hr_imports i on i.id=h.import_id
    join public.import_review_daily_timesheet_resolutions d on d.import_id=h.import_id and d.hr_row_id=h.id and d.status in ('CURRENT','APPLIED')
    join public.v_timesheets_daily_match t on t.timesheet_id=d.resolved_timesheet_id
    left join public.contracts c on c.id=(select ts.contract_id from public.timesheets ts where ts.timesheet_id=t.timesheet_id)
    where h.import_id=p_import_id and (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
    order by h.id limit 501
  ), mismatch as (
    select r.*,
      case
        when r.hours_worked is not null and r.worked_minutes is not null
          and abs((r.hours_worked*60)-greatest(r.worked_minutes-coalesce(r.break_minutes,0),0))>1
          then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_evidence_supplied')::boolean,false)
          and (r.payload_json->>'break_mins')::integer is distinct from coalesce(r.break_minutes,0)
          then 'BREAK_MINUTES_MISMATCH'
      end reason_code
    from r
  ), issues as (
    select m.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY',m.reason_code,m.timesheet_id,m.hr_request_id,
      lower(coalesce(m.staff_norm,'')),m.date_local,m.start_time_local,m.end_time_local,m.hours_worked,m.worked_minutes)) issue_fingerprint,
      lower(btrim(case when coalesce(m.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=m.contract_id),false)
        then (select c.ts_queries_alt_email_address from public.contracts c where c.id=m.contract_id)
        else (select c.ts_queries_email from public.clients c where c.id=m.client_id) end)) route_email
    from mismatch m where m.reason_code is not null
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,i.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||i.issue_fingerprint,i.issue_fingerprint,i.id,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v1',i.issue_fingerprint,i.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select concat_ws('|',c.updated_at,c.ts_queries_alt_email_address) from public.contracts c where c.id=i.contract_id)
        else (select concat_ws('|',c.rev,c.updated_at,c.ts_queries_email) from public.clients c where c.id=i.client_id) end)),
    not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    e.id is null and not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    false,
    jsonb_build_object('reason_code',i.reason_code,'issue_fingerprint',i.issue_fingerprint,'work_date',i.date_local,
      'candidate_name',i.staff_raw,'timesheet_id',i.timesheet_id,'recipient_scope_key',
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||i.contract_id::text else 'CLIENT_DEFAULT:'||i.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(i.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_alt_email_address,''))),c.updated_at)) from public.contracts c where c.id=i.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_email,''))),c.rev,c.updated_at)) from public.clients c where c.id=i.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(i.route_email,'')) not between 3 and 320 or position('@' in coalesce(i.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((i.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',i.protection)
  from issues i left join public.hr_issue_emails e on e.issue_fingerprint=i.issue_fingerprint;

  -- Weekly validation-only issues use the installed comparison engine, but
  -- normalise every user choice into the same server-owned decision catalogue.
  if exists(select 1 from public.hr_imports i where i.id=p_import_id
      and i.source_system='HEALTHROSTER'::public.hr_source_enum
      and upper(coalesce(i.import_scope,'HR_WEEKLY')) not like '%DAILY%') then
    v_weekly_preview:=public.hr_weekly_validation_preview(p_import_id);

    -- Validation-only Weekly evidence has two distinct, server-proven states.
    -- Neither state is an instruction to mutate CloudTMS financial records.
    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), eligible_validation_groups as (
      select d.candidate_id,d.summary_json->>'week_ending_date' week_ending_date
      from pg_temp.import_review_catalog_v1 d
      where d.candidate_id is not null
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      group by d.candidate_id,d.summary_json->>'week_ending_date'
      having bool_and(d.action_kind='NO_ACTION' and not d.blocking)
    ), missing_timesheets as (
      select p.row_json,p.timesheet_id,p.candidate_id,
        d.hr_row_id shift_hr_row_id,d.client_id shift_client_id,
        d.contract_id shift_contract_id,d.source_identity shift_source_identity,
        d.evidence_fingerprint shift_evidence_fingerprint,d.summary_json shift_summary_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      join pg_temp.import_review_catalog_v1 d on d.candidate_id=p.candidate_id
        and d.summary_json->>'week_ending_date'=p.row_json->>'week_ending_date'
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and d.action_kind='NO_ACTION' and not d.blocking
      where p.row_json->>'overall_status'='MISSING_TIMESHEET'
    ), omitted_shifts as (
      select p.*,cx.value comparison_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null and cx.value->>'match_status'='HR_ONLY'
    ), confirmed_exceptions as (
      select p.*,cx.value exception_json
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_TIMESHEET_NOT_SUBMITTED',m.shift_hr_row_id)),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-timesheet-not-submitted',m.shift_hr_row_id),
      m.shift_source_identity,
      m.shift_hr_row_id,null::uuid,null::uuid,m.shift_client_id,m.candidate_id,m.shift_contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-timesheet-not-submitted-v2',
        m.shift_evidence_fingerprint,m.row_json::text)),
      false,false,true,
      jsonb_strip_nulls(m.shift_summary_json||jsonb_build_object(
        'reason_code','WEEKLY_TIMESHEET_NOT_SUBMITTED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',m.row_json->>'candidate_name','week_ending_date',m.row_json->>'week_ending_date',
        'difference_codes',jsonb_build_array('TIMESHEET_NOT_SUBMITTED'),
        'outcome_label','Request timesheet from candidate'))
    from missing_timesheets m
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',o.comparison_json->>'hr_row_id')),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-candidate-did-not-work',o.comparison_json->>'hr_row_id'),
      concat_ws('|',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      nullif(o.comparison_json->>'hr_row_id','')::uuid,o.timesheet_id,null::uuid,o.client_id,o.candidate_id,o.contract_id,null::uuid,
      o.comparison_json->>'exception_evidence_fingerprint',
      false,false,true,
      jsonb_build_object(
        'reason_code','WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',o.row_json->>'candidate_name','week_ending_date',o.row_json->>'week_ending_date',
        'work_date',o.comparison_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',o.comparison_json->>'work_date','start',o.comparison_json->>'healthroster_start',
          'end',o.comparison_json->>'healthroster_end',
          'break_minutes',nullif(o.comparison_json->>'healthroster_break_mins','')::integer,
          'reference',o.comparison_json->>'ref_after')),
        'current_evidence',jsonb_build_object('timesheet_id',o.timesheet_id),
        'difference_codes',jsonb_build_array('HR_ONLY'),
        'outcome_label','Confirm candidate did not work this shift')
    from omitted_shifts o
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_CANDIDATE_DID_NOT_WORK',c.exception_json->>'hr_row_id')),
      'NO_ACTION','NO_ACTION',
      concat_ws(':','weekly-candidate-did-not-work',c.exception_json->>'hr_row_id'),
      concat_ws('|',c.timesheet_id,c.exception_json->>'work_date',
        c.exception_json->>'healthroster_start',c.exception_json->>'healthroster_end'),
      nullif(c.exception_json->>'hr_row_id','')::uuid,c.timesheet_id,null::uuid,c.client_id,c.candidate_id,c.contract_id,null::uuid,
      c.exception_json->>'evidence_fingerprint',
      false,false,false,
      jsonb_build_object(
        'reason_code','CANDIDATE_DID_NOT_WORK_CONFIRMED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'resolution_kind','WEEKLY_CANDIDATE_DID_NOT_WORK',
        'candidate_name',c.row_json->>'candidate_name','week_ending_date',c.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(c.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'confirmed_exception_count',coalesce(nullif(c.row_json->>'confirmed_exception_count','')::integer,0),
        'work_date',c.exception_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',c.exception_json->>'work_date','start',c.exception_json->>'healthroster_start',
          'end',c.exception_json->>'healthroster_end',
          'break_minutes',nullif(c.exception_json->>'healthroster_break_mins','')::integer,
          'reference',c.exception_json->>'reference')),
        'current_evidence',jsonb_build_object('timesheet_id',c.timesheet_id),
        'difference_codes',jsonb_build_array('CONFIRMED_EXCEPTION'),
        'outcome_label','Passed with confirmed exception')
    from confirmed_exceptions c;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id,
        nullif(r.value->>'issue_fingerprint','') issue_fingerprint
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), email_filtered as (
      select p.*,
        coalesce((select jsonb_agg(cx.value order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where (
            coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
          )),'[]'::jsonb) email_comparisons,
        coalesce((select jsonb_agg(day_json.value order by day_json.value->>'date')
          from jsonb_array_elements(coalesce(p.row_json->'days','[]'::jsonb)) day_json(value)
          where exists (
            select 1
            from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
            where cx.value->>'work_date'=day_json.value->>'date'
              and (
                coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
                or coalesce((cx.value->>'ref_changed')::boolean,false)
              )
          )),'[]'::jsonb) email_days,
        coalesce((select jsonb_agg(to_jsonb(fr.value))
          from jsonb_array_elements_text(coalesce(p.row_json->'failure_reasons','[]'::jsonb)) fr(value)
          where fr.value<>'HealthRoster has a shift not present on the timesheet.'),'[]'::jsonb) email_failure_reasons
      from preview_rows p
    ), routed as (
      select p.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_WEEKLY','validation-email-v2',
          p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text)) email_issue_fingerprint,
        c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from email_filtered p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=public._import_review_hash_v1(concat_ws('|',
        'HEALTHROSTER_WEEKLY','validation-email-v2',p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text))
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
        and exists (
          select 1 from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
        )
    ), email_actions as (
      select r.*,
        public._import_review_hash_v1(concat_ws('|','query-route-v1',r.recipient_scope_key,r.recipient_email,
          case when r.recipient_scope='CONTRACT_OVERRIDE' then r.contract_updated_at::text
            else concat_ws('|',r.client_rev,r.client_updated_at) end)) route_fingerprint,
        length(coalesce(r.recipient_email,'')) between 3 and 320
          and r.recipient_email~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' valid_email
      from routed r
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.email_issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.email_issue_fingerprint,a.email_issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v2',a.timesheet_id,
        a.row_json->>'candidate_name',a.row_json->>'week_ending_date',
        a.email_comparisons::text,a.email_days::text,a.email_failure_reasons::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.email_issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
        'validation_total_shift_count',jsonb_array_length(coalesce(a.row_json->'comparisons','[]'::jsonb))
          + coalesce(nullif(a.row_json->>'confirmed_exception_count','')::integer,0),
        'validation_difference_count',jsonb_array_length(a.email_comparisons),
      'failure_reasons',a.email_failure_reasons,
        'days',a.email_days,'comparisons',a.email_comparisons,
        'evidence_rows',coalesce((
          select jsonb_agg(jsonb_build_object(
            'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'healthroster_start',
              'end',cx.value->>'healthroster_end','break_minutes',nullif(cx.value->>'healthroster_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'hr_minutes','')::integer,'reference',cx.value->>'ref_after')),
            'current_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'timesheet_start',
              'end',cx.value->>'timesheet_end','break_minutes',nullif(cx.value->>'timesheet_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'ts_minutes','')::integer,'reference',cx.value->>'ref_before')),
            'difference_codes',to_jsonb(array_remove(array[
              case when coalesce(cx.value->>'match_status','MATCH')<>'MATCH' then cx.value->>'match_status' end,
              case when coalesce((cx.value->>'ref_changed')::boolean,false) then 'REFERENCE' end,
              case when coalesce(day_json.value->>'day_status','OK')<>'OK' then 'WORKED_HOURS' end
            ],null))
          ) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(a.email_comparisons) cx(value)
          left join lateral (select d.value from jsonb_array_elements(coalesce(a.row_json->'days','[]'::jsonb)) d(value)
            where d.value->>'date'=cx.value->>'work_date' limit 1) day_json on true
        ),'[]'::jsonb),
        'outcome_label',case when a.issue_id is null then 'Request amend shift' else 'Request amend shift reminder' end,
        'recipient_scope_key',a.recipient_scope_key,'recipient_route_fingerprint',a.route_fingerprint,
        'delivery_history_status',coalesce(a.delivery_history_status,'NEW'),'sent_count',coalesce(a.sent_count,0),
        'default_excluded_reason',case when a.issue_id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
          when not a.valid_email then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
          when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
        'protection',a.protection)
    from email_actions a;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
      where nullif(r.value->>'timesheet_id','') is not null
    ), invalidations as (
      select p.*,cx.value comparison_json,nullif(btrim(cx.value->>'comparison_key'),'') comparison_key,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where coalesce((cx.value->>'is_destructive_invalidation')::boolean,false)
        and exists(select 1 from public.hr_imports hi where hi.id=p_import_id
          and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'))
        and nullif(btrim(cx.value->>'comparison_key'),'') is not null
        and nullif(btrim(cx.value->>'ref_before'),'') is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',i.timesheet_id,i.comparison_key)),
      'INVALIDATE_REFERENCE','PENDING','timesheet:'||i.timesheet_id::text||':'||i.comparison_key,
      i.comparison_key,null::uuid,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-reference-invalidation-v1',i.timesheet_id,i.comparison_json::text,i.protection::text)),
      not coalesce((i.protection->>'protected')::boolean,false),false,false,
      jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT',
        'candidate_name',i.row_json->>'candidate_name','week_ending_date',i.row_json->>'week_ending_date',
        'timesheet_id',i.timesheet_id,'comparison_key',i.comparison_key,'comparison',i.comparison_json,
        'protection',i.protection,'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
    from invalidations i;
  end if;

  -- Complete Daily coverage also exposes existing timesheets that are absent
  -- from the file.  Missing rows are query-email candidates; reference
  -- invalidation is a separate, explicit, default-off decision.
  insert into pg_temp.import_review_catalog_v1
  with i as (
    select * from public.hr_imports where id=p_import_id
  ), missing as (
    select t.*,ts.contract_id,c.first_name,c.last_name,cl.name as client_name,
      public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection,
      lower(btrim(case when coalesce(ts.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=ts.contract_id),false)
        then (select ct.ts_queries_alt_email_address from public.contracts ct where ct.id=ts.contract_id)
        else cl.ts_queries_email end)) route_email,
      public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY','MISSING_FROM_IMPORT',
        t.timesheet_id,t.candidate_id,t.client_id,(t.worked_start_iso at time zone 'Europe/London')::date,
        coalesce(t.reference_number,''))) issue_fingerprint
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join public.candidates c on c.id=t.candidate_id
    join public.clients cl on cl.id=t.client_id
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (
        select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED')
      )
    order by t.timesheet_id limit 501
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,m.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||m.issue_fingerprint,m.issue_fingerprint,null::uuid,m.timesheet_id,null::uuid,
    m.client_id,m.candidate_id,m.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','missing-daily-email-v1',m.issue_fingerprint,m.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(m.contract_id is not null and (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select concat_ws('|',ct.updated_at,ct.ts_queries_alt_email_address) from public.contracts ct where ct.id=m.contract_id)
        else (select concat_ws('|',cl.rev,cl.updated_at,cl.ts_queries_email) from public.clients cl where cl.id=m.client_id) end)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,
    e.id is null and not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,false,
    jsonb_build_object('reason_code','MISSING_FROM_IMPORT','issue_fingerprint',m.issue_fingerprint,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,
      'week_ending_date',(m.worked_start_iso at time zone 'Europe/London')::date
        + ((7-extract(dow from (m.worked_start_iso at time zone 'Europe/London')::date)::integer)%7),
      'candidate_name',btrim(concat_ws(' ',m.first_name,m.last_name)),'client_name',m.client_name,
      'timesheet_id',m.timesheet_id,'reference_number',m.reference_number,
      'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,
      'break_minutes',m.break_minutes,'role',m.tsfin_role,
      'recipient_scope_key',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||m.contract_id::text else 'CLIENT_DEFAULT:'||m.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||ct.id::text,
          lower(btrim(coalesce(ct.ts_queries_alt_email_address,''))),ct.updated_at)) from public.contracts ct where ct.id=m.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||cl.id::text,
          lower(btrim(coalesce(cl.ts_queries_email,''))),cl.rev,cl.updated_at)) from public.clients cl where cl.id=m.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(m.route_email,'')) not between 3 and 320 or position('@' in coalesce(m.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',m.protection)
  from missing m left join public.hr_issue_emails e on e.issue_fingerprint=m.issue_fingerprint;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists(select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'MARK_VALIDATION_ERROR',m.timesheet_id)),
    'MARK_VALIDATION_ERROR','READY','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-validation-v1',m.timesheet_id,m.worked_start_iso,
      m.worked_end_iso,m.break_minutes,m.worked_minutes,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false)
      then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_IMPORT' end,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,'break_minutes',m.break_minutes,
      'hours_worked',m.worked_minutes/60.0,'role',m.tsfin_role,'protection',m.protection)
  from missing m;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and nullif(btrim(t.reference_number),'') is not null
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',m.timesheet_id)),
    'INVALIDATE_REFERENCE','PENDING','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-reference-v1',m.timesheet_id,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'protected')::boolean,false),false,false,
    jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT',
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'protection',m.protection,
      'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
  from missing m;

  -- Omitted existing shifts are proposed only inside immutable complete coverage.
  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select s.*,public._import_review_timesheet_protection_core_v1(s.timesheet_id) protection
    from public.nhsp_shifts s
    join i on true
    cross join lateral public._import_review_effective_authority_core_v1(
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
      s.contract_id,s.client_id,coalesce(s.week_ending_date,s.work_date)) authority
    where i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and s.source_system=i.source_system
      and authority.import_authoritative
      and s.cancelled_at_utc is null
      and s.work_date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or s.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=s.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=s.candidate_id))
      and not exists (select 1 from public.hr_rows h where h.import_id=i.id and h.external_row_key=s.external_row_key)
    order by s.id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'APPLY_CANCELLATION',m.id)),
    'APPLY_CANCELLATION','READY','shift:'||m.id::text,m.external_row_key,null::uuid,m.timesheet_id,m.id,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-shift-v1',m.id,m.updated_at,m.timesheet_id,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_COMPLETE_IMPORT' end,
      'work_date',m.work_date,'week_ending_date',m.week_ending_date,'candidate_id',m.candidate_id,'client_id',m.client_id,
      'start_time',m.start_utc,'end_time',m.end_utc,'break_minutes',m.break_mins,'role',m.assignment_code,'protection',m.protection)
  from missing m;

  -- Query emails can be committed only with one exact, complete, current
  -- timesheet PDF.  Invoice-linked validation records are never eligible for
  -- validation.  The same evidence fingerprint is frozen into the decision so
  -- document or invoice lifecycle movement makes a reviewed action stale.
  with evidence as materialized (
    select c.action_id,public._import_review_query_evidence_core_v1(c.timesheet_id) evidence_json
    from pg_temp.import_review_catalog_v1 c
    where c.timesheet_id is not null
      and (
        c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
        or c.summary_json->>'authority_mode'='VALIDATION_ONLY'
        or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
      )
  )
  update pg_temp.import_review_catalog_v1 c
  set evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','query-evidence-decision-v1',
        c.evidence_fingerprint,e.evidence_json->>'evidence_fingerprint')),
      action_category=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then 'PENDING'
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then 'PENDING'
        else c.action_category end,
      selectable=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.selectable end,
      default_selected=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then false
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then false
        else c.default_selected end,
      blocking=case
        when c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null then true
        when (
          c.summary_json->>'authority_mode'='VALIDATION_ONLY'
          or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
        ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED' then true
        else c.blocking end,
      summary_json=c.summary_json||jsonb_build_object(
        'attachment_evidence',e.evidence_json,
        'attachment_fingerprint',e.evidence_json->>'evidence_fingerprint'
      )||case
        when (
          c.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
          and nullif(e.evidence_json->>'reason_code','') is not null
        ) or (
          (
            c.summary_json->>'authority_mode'='VALIDATION_ONLY'
            or coalesce(nullif(c.summary_json->>'is_daily','')::boolean,false)
          ) and e.evidence_json->>'reason_code'='TIMESHEET_PRESENT_BUT_INVOICED'
        ) then jsonb_build_object(
          'reason_code',e.evidence_json->>'reason_code',
          'default_excluded_reason',e.evidence_json->>'reason_code',
          'outcome_label',case e.evidence_json->>'reason_code'
            when 'TIMESHEET_PRESENT_BUT_INVOICED' then 'Timesheet present but invoiced'
            when 'TIMESHEET_EVIDENCE_PREPARING' then 'Preparing timesheet evidence'
            else 'Timesheet evidence incomplete' end
        ) else '{}'::jsonb end
  from evidence e
  where e.action_id=c.action_id;

  -- Daily validation is atomic per Daily timesheet.  An email, document hold
  -- or invoice blocker for that record prevents only that record from entering
  -- validation and TSFIN work.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','daily-validation-held-v1',
        current_row.evidence_fingerprint,current_row.timesheet_id)),
      summary_json=current_row.summary_json||jsonb_build_object(
        'daily_validation_held',true,
        'validation_hold_label','Validation held: resolve this Daily timesheet first'
      )
  where current_row.action_kind='NO_ACTION'
    and coalesce(nullif(current_row.summary_json->>'is_daily','')::boolean,false)
    and current_row.timesheet_id is not null
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.timesheet_id=current_row.timesheet_id
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  -- Weekly validation is all-or-nothing per candidate/client/contract/week.
  -- One mismatch, unresolved exception, missing attachment or invoice blocker
  -- holds the whole Weekly timesheet while leaving the actual issue visible.
  update pg_temp.import_review_catalog_v1 current_row
  set selectable=false,
      default_selected=false,
      evidence_fingerprint=public._import_review_hash_v1(concat_ws('|','weekly-validation-held-v2',
        current_row.evidence_fingerprint,current_row.candidate_id,current_row.client_id,
        current_row.contract_id,current_row.summary_json->>'week_ending_date')),
      summary_json=current_row.summary_json||jsonb_build_object(
        'weekly_validation_held',true,
        'weekly_validation_badge_code','WEEKLY_VALIDATION_HELD',
        'weekly_validation_badge_label','Validation held: resolve outstanding shift',
        'validation_hold_label','Validation held: one or more shifts still require action'
      )
  where current_row.action_kind='NO_ACTION'
    and current_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and current_row.summary_json->>'source_route' not like '%DAILY%'
    and exists (
      select 1 from pg_temp.import_review_catalog_v1 hold
      where hold.candidate_id=current_row.candidate_id
        and hold.client_id=current_row.client_id
        and hold.contract_id is not distinct from current_row.contract_id
        and hold.summary_json->>'week_ending_date'=current_row.summary_json->>'week_ending_date'
        and hold.action_id<>current_row.action_id
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    );

  select count(*) into v_count from pg_temp.import_review_catalog_v1;
  if v_count>p_max_actions then
    raise exception 'IMPORT_REVIEW_ACTION_LIMIT_EXCEEDED' using errcode='54000',
      detail=jsonb_build_object('count',v_count,'max',p_max_actions)::text;
  end if;

  return query select c.action_id,c.action_kind,c.action_category,c.target_key,c.source_identity,
    c.hr_row_id,c.timesheet_id,c.shift_id,c.client_id,c.candidate_id,c.contract_id,c.issue_id,
    c.evidence_fingerprint,c.selectable,c.default_selected,c.blocking,c.summary_json
  from pg_temp.import_review_catalog_v1 c order by c.action_id;
end
$function$;

-- public._import_review_effective_invoice_balance_core_v1(p_import_id uuid, p_source_items jsonb, p_max_sources integer, p_max_invoice_lines_per_source integer, p_max_audit_rows_per_source integer, p_max_operations_per_source integer) pre-install MD5 444aea7e217ac419877a358349533d36
CREATE OR REPLACE FUNCTION public._import_review_effective_invoice_balance_core_v1(p_import_id uuid, p_source_items jsonb, p_max_sources integer DEFAULT 100, p_max_invoice_lines_per_source integer DEFAULT 512, p_max_audit_rows_per_source integer DEFAULT 256, p_max_operations_per_source integer DEFAULT 128)
 RETURNS TABLE(source_identity text, balance_json jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_item jsonb;
  v_source_identity text;
  v_source_system text;
  v_external_row_key text;
  v_invoice_stream text;
  v_source_shift_id uuid;
  v_source_timesheet_id uuid;
  v_hr_row_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_authoritative_import_id uuid;
  v_a_schedule jsonb;
  v_a_hours jsonb;
  v_a_fingerprint text;
  v_scope_fingerprint text;
  v_hist_ids uuid[]:=array[]::uuid[];
  v_audit_ids uuid[]:=array[]::uuid[];
  v_archived_ids uuid[]:=array[]::uuid[];
  v_active_ids uuid[]:=array[]::uuid[];
  v_missing_ids uuid[]:=array[]::uuid[];
  v_import_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
  v_effective_invoice_ids uuid[]:=array[]::uuid[];
  v_effective_line_ids uuid[]:=array[]::uuid[];
  v_credit_line_ids uuid[]:=array[]::uuid[];
  v_line_count integer:=0;
  v_audit_count integer:=0;
  v_operation_count integer:=0;
  v_operation_evidence jsonb:='[]'::jsonb;
  v_operation_member_ids uuid[]:=array[]::uuid[];
  v_member_supersession_map jsonb:='[]'::jsonb;
  v_member_supersession_conflict boolean:=false;
  v_operation_evidence_conflict boolean:=false;
  v_operation_in_progress boolean:=false;
  v_transition_operation_id uuid:=case
    when coalesce(current_setting('cloudtms.import_reconciliation_operation_id',true),'')
      ~*'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
      then current_setting('cloudtms.import_reconciliation_operation_id',true)::uuid
    else null end;
  v_member_role_map jsonb:='[]'::jsonb;
  v_member_role_conflict boolean:=false;
  v_effective_component_count integer:=0;
  v_b_day numeric:=0;
  v_b_night numeric:=0;
  v_b_sat numeric:=0;
  v_b_sun numeric:=0;
  v_b_bh numeric:=0;
  v_b_pay numeric:=0;
  v_b_charge numeric:=0;
  v_b_margin numeric:=0;
  v_component_day numeric:=0;
  v_component_night numeric:=0;
  v_component_sat numeric:=0;
  v_component_sun numeric:=0;
  v_component_bh numeric:=0;
  v_component_pay numeric:=0;
  v_component_charge numeric:=0;
  v_component_margin numeric:=0;
  v_b_schedule jsonb:='[]'::jsonb;
  v_candidate_schedule jsonb:='[]'::jsonb;
  v_candidate_hours jsonb:='{}'::jsonb;
  v_schedule_candidates jsonb:='[]'::jsonb;
  v_candidate_policy_fingerprint text;
  v_b_policy_fingerprint text;
  v_terminal_generation_id text;
  v_terminal_positive_timesheet_id uuid;
  v_terminal_positive_member_count integer:=0;
  v_terminal_frozen_candidate jsonb;
  v_terminal_frozen_candidate_count integer:=0;
  v_terminal_frozen_schedule_variant_count integer:=0;
  v_terminal_frozen_policy_variant_count integer:=0;
  v_terminal_operation jsonb;
  v_terminal_operation_schedule jsonb:='[]'::jsonb;
  v_terminal_operation_hours jsonb:='{}'::jsonb;
  v_terminal_operation_policy_fingerprint text;
  v_terminal_operation_id uuid;
  v_terminal_policy_snapshot jsonb:='{}'::jsonb;
  v_terminal_policy_snapshot_fingerprint text;
  v_terminal_derived_day numeric:=0;
  v_terminal_derived_night numeric:=0;
  v_terminal_derived_sat numeric:=0;
  v_terminal_derived_sun numeric:=0;
  v_terminal_derived_bh numeric:=0;
  v_terminal_derived_total numeric:=0;
  v_terminal_frozen_matches_b boolean:=false;
  v_terminal_operation_matches_b boolean:=false;
  v_terminal_schedule_authority_conflict boolean:=false;
  v_terminal_policy_authority_conflict boolean:=false;
  v_b_standard_schedule_authority text:='NONE';
  v_b_standard_schedule_authority_timesheet_id uuid;
  v_b_standard_schedule_authority_correction_id text;
  v_b_standard_schedule_authority_operation_id uuid;
  v_b_standard_schedule_authority_policy_fingerprint text;
  v_b_standard_schedule_authority_fingerprint text;
  v_b_standard_schedule_authority_diagnostic text;
  v_effective_fingerprint text;
  v_line_evidence jsonb:='[]'::jsonb;
  v_ignored_nonhours_line_ids uuid[]:=array[]::uuid[];
  v_generation_role_evidence jsonb:='[]'::jsonb;
  v_fully_invoiced_generation_ids text[]:=array[]::text[];
  v_partial_generation_ids text[]:=array[]::text[];
  v_mutable_generation_ids text[]:=array[]::text[];
  v_archived_history_roles jsonb:='[]'::jsonb;
  v_role_evidence_conflicts jsonb:='[]'::jsonb;
  v_role_evidence_fingerprint text;
  v_repair_identity_mode text;
  v_reversal_repair_required boolean:=false;
  v_replacement_repair_required boolean:=false;
  v_line record;
  v_original_line public.invoice_lines%rowtype;
  v_original_invoice public.invoices%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_original_tf public.timesheets_financials%rowtype;
  v_seg jsonb;
  v_original_seg jsonb;
  v_line_type text;
  v_original_line_type text;
  v_is_weekly_hours boolean:=false;
  v_is_separable_nonhours boolean:=false;
  v_original_line_id uuid;
  v_seg_count integer:=0;
  v_matching_seg_count integer:=0;
  v_original_seg_count integer:=0;
  v_original_matching_seg_count integer:=0;
  v_single_source boolean:=false;
  v_line_scope_proven boolean:=false;
  v_operation_member_scope_proven boolean:=false;
  v_component_timesheet_id uuid;
  v_component_correction_id text;
  v_component_correction_kind text;
  v_scope_unprovable boolean:=false;
  v_credit_ambiguous boolean:=false;
  v_stream_conflict boolean:=false;
  v_archived_invoice_conflict boolean:=false;
  v_partial_invoice_state boolean:=false;
  v_active_invoice_activity boolean:=false;
  v_role_partial_invoice_state boolean:=false;
  v_role_active_invoice_activity boolean:=false;
  v_role_scope_unprovable boolean:=false;
  v_paid_mutable_state boolean:=false;
  v_mutable_correction_id text;
  v_mutable_member_ids uuid[]:=array[]::uuid[];
  v_mutable_missing_roles text[]:=array[]::text[];
  v_mutable_fingerprint text;
  v_mutable_parent_id uuid;
  v_m_day numeric:=0;
  v_m_night numeric:=0;
  v_m_sat numeric:=0;
  v_m_sun numeric:=0;
  v_m_bh numeric:=0;
  v_m_pay numeric:=0;
  v_m_charge numeric:=0;
  v_m_margin numeric:=0;
  v_m_financials_complete boolean:=true;
  v_b_standard_representable boolean:=false;
  v_b_hours_zero boolean:=false;
  v_b_money_zero boolean:=false;
  v_effective_zero boolean:=false;
  v_current_source_safe boolean:=false;
  v_current_source_safety_reason text;
  v_current_source_count integer:=0;
  v_current_source_invoice_lined boolean:=false;
  v_current_source_paid boolean:=false;
  v_current_source_unlocked boolean:=false;
  v_current_source_fresh boolean:=false;
  v_current_source_segment_unlocked boolean:=false;
  v_current_source_contract_week_safe boolean:=false;
  v_current_source_invoice_operation_clear boolean:=false;
  v_source_protection jsonb:='{}'::jsonb;
  v_financial_position_requires_amendment boolean:=false;
  v_blocking_code text;
  v_reconciliation_fingerprint text;
  v_uuid_re constant text:='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
begin
  if p_import_id is null or jsonb_typeof(coalesce(p_source_items,'null'::jsonb))<>'array' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_INPUT_INVALID' using errcode='22023';
  end if;
  if p_max_sources not between 1 and 100
     or p_max_invoice_lines_per_source not between 1 and 512
     or p_max_audit_rows_per_source not between 1 and 256
     or p_max_operations_per_source not between 1 and 128 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_BOUND_INVALID' using errcode='22023';
  end if;
  if jsonb_array_length(p_source_items)>p_max_sources then
    raise exception 'IMPORT_REVIEW_SOURCE_LIMIT_EXCEEDED' using errcode='54000';
  end if;
  if exists (
    select 1 from jsonb_array_elements(p_source_items) s(value)
    group by nullif(btrim(s.value->>'source_identity'),'') having count(*)>1
  ) then
    raise exception 'IMPORT_REVIEW_SOURCE_IDENTITY_DUPLICATE' using errcode='22023';
  end if;

  for v_item in select s.value from jsonb_array_elements(p_source_items) s(value)
  loop
    if jsonb_typeof(v_item)<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023';
    end if;
    v_source_identity:=nullif(btrim(v_item->>'source_identity'),'');
    v_source_system:=upper(btrim(coalesce(v_item->>'source_system','')));
    v_external_row_key:=nullif(btrim(v_item->>'external_row_key'),'');
    v_invoice_stream:=upper(btrim(coalesce(v_item->>'invoice_stream','')));
    if v_source_identity is null or v_source_system not in ('NHSP','HEALTHROSTER')
       or v_external_row_key is null or v_invoice_stream not in ('NORMAL','SELF_BILL')
       or coalesce(v_item->>'source_shift_id','')!~*v_uuid_re
       or coalesce(v_item->>'hr_row_id','')!~*v_uuid_re
       or coalesce(v_item->>'source_timesheet_id','')!~*v_uuid_re
       or coalesce(v_item->>'candidate_id','')!~*v_uuid_re
       or coalesce(v_item->>'client_id','')!~*v_uuid_re
       or coalesce(v_item->>'contract_id','')!~*v_uuid_re
       or coalesce(v_item->>'authoritative_import_id','')!~*v_uuid_re
       or coalesce(v_item->>'week_ending_date','')!~'^\d{4}-\d{2}-\d{2}$'
       or jsonb_typeof(v_item->'authoritative_schedule_json')<>'array'
       or jsonb_array_length(v_item->'authoritative_schedule_json')<>1
       or jsonb_typeof(v_item->'authoritative_hours')<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=coalesce(v_source_identity,'missing source_identity');
    end if;
    v_source_shift_id:=(v_item->>'source_shift_id')::uuid;
    v_hr_row_id:=(v_item->>'hr_row_id')::uuid;
    v_source_timesheet_id:=(v_item->>'source_timesheet_id')::uuid;
    v_candidate_id:=(v_item->>'candidate_id')::uuid;
    v_client_id:=(v_item->>'client_id')::uuid;
    v_contract_id:=(v_item->>'contract_id')::uuid;
    v_authoritative_import_id:=(v_item->>'authoritative_import_id')::uuid;
    v_week_ending_date:=(v_item->>'week_ending_date')::date;
    v_a_schedule:=v_item->'authoritative_schedule_json';
    v_a_hours:=jsonb_build_object(
      'hours_day',coalesce((v_item#>>'{authoritative_hours,hours_day}')::numeric,0),
      'hours_night',coalesce((v_item#>>'{authoritative_hours,hours_night}')::numeric,0),
      'hours_sat',coalesce((v_item#>>'{authoritative_hours,hours_sat}')::numeric,0),
      'hours_sun',coalesce((v_item#>>'{authoritative_hours,hours_sun}')::numeric,0),
      'hours_bh',coalesce((v_item#>>'{authoritative_hours,hours_bh}')::numeric,0),
      'total_hours',coalesce((v_item#>>'{authoritative_hours,total_hours}')::numeric,0)
    );
    v_a_fingerprint:=encode(digest(convert_to(concat_ws('|','A-v1',v_source_identity,v_authoritative_import_id,v_a_schedule::text,v_a_hours::text),'UTF8'),'sha256'),'hex');
    v_scope_fingerprint:=encode(digest(convert_to(concat_ws('|','source-scope-v1',v_source_identity,v_source_system,v_source_shift_id,v_external_row_key,v_source_timesheet_id,v_candidate_id,v_client_id,v_contract_id,v_week_ending_date,v_invoice_stream),'UTF8'),'sha256'),'hex');

    perform 1 from public.hr_rows r
    where r.id=v_hr_row_id and r.import_id=p_import_id and r.external_row_key=v_external_row_key;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;
    perform 1 from public.nhsp_shifts s
    where s.id=v_source_shift_id and s.external_row_key=v_external_row_key
      and upper(s.source_system::text)=v_source_system
      and s.candidate_id=v_candidate_id and s.client_id=v_client_id
      and s.contract_id=v_contract_id and s.week_ending_date=v_week_ending_date;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;

    select count(*)::integer into v_audit_count
    from public.audit_events ae
    where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
      and (ae.after_json->>'shift_id'=v_source_shift_id::text
        or ae.after_json->>'external_row_key'=v_external_row_key);
    if v_audit_count>p_max_audit_rows_per_source then
      raise exception 'IMPORT_REVIEW_AUDIT_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    select coalesce(array_agg(distinct candidate_id order by candidate_id),array[]::uuid[])
    into v_audit_ids
    from (
      select candidate_id
      from public.audit_events ae
      cross join lateral unnest(array[
        case when ae.object_type='timesheets' then ae.object_id_text end,
        ae.after_json->>'timesheet_id',
        ae.after_json->>'reversal_timesheet_id',
        ae.after_json->>'replacement_timesheet_id',
        ae.after_json->>'counterpart_timesheet_id'
      ]) raw(candidate_text)
      cross join lateral (select case when raw.candidate_text~*v_uuid_re then raw.candidate_text::uuid end candidate_id) parsed
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
        and parsed.candidate_id is not null
    ) candidates;

    select coalesce(array_agg(distinct import_id order by import_id),array[]::uuid[])
    into v_import_ids
    from (
      select p_import_id import_id
      union all select v_authoritative_import_id
      union all select s.latest_import_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all
      select case when raw.import_text~*v_uuid_re then raw.import_text::uuid end
      from public.audit_events ae
      cross join lateral unnest(array[
        ae.after_json->>'import_id',ae.after_json->>'trigger_import_id',ae.after_json->>'evidence_import_id'
      ]) raw(import_text)
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
    ) imports where import_id is not null;
    -- A later authoritative import replaces latest_import_id, so historical
    -- operation identity is discovered both through known imports and through
    -- the immutable decision/outcome link for this exact source shift.
    select coalesce(array_agg(distinct operation_id order by operation_id),array[]::uuid[])
    into v_operation_ids
    from (
      select op.id operation_id
      from public.import_apply_operations op
      where op.import_id=any(v_import_ids)
      union all
      select outcome.operation_id
      from public.import_review_decisions decision
      join public.import_review_action_outcomes outcome on outcome.action_id=decision.action_id
      where decision.shift_id=v_source_shift_id and outcome.shift_id=v_source_shift_id
        and decision.source_identity=v_source_identity and outcome.source_identity=v_source_identity
        and decision.candidate_id=v_candidate_id and outcome.candidate_id=v_candidate_id
        and decision.client_id=v_client_id and outcome.client_id=v_client_id
        and decision.contract_id is not distinct from v_contract_id
        and outcome.contract_id is not distinct from v_contract_id
        and decision.action_kind='APPLY_AMENDMENT' and outcome.action_kind='APPLY_AMENDMENT'
    ) operation_candidates
    where operation_id is not null;
    v_operation_count:=cardinality(v_operation_ids);
    if v_operation_count>p_max_operations_per_source then
      raise exception 'IMPORT_REVIEW_OPERATION_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    -- A completed reconciliation operation is durable identity evidence, not
    -- economic evidence.  Validate every request/applied/policy triple before
    -- any invoice-line scope is built so physically deleted members remain
    -- discoverable without allowing an operation to contribute money twice.
    with matching_requests as (
      select op.id operation_id,op.state::text operation_state,op.committed_at_utc,op.finalised_at_utc,
        op.response_json,request_unit
      from public.import_apply_operations op
      cross join lateral jsonb_array_elements(coalesce(op.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb)) request_unit
      where op.id=any(v_operation_ids)
        and (request_unit->>'source_identity'=v_source_identity
          or request_unit->>'source_shift_id'=v_source_shift_id::text)
    ), triples as (
      select mr.*,
        (select count(*) from jsonb_array_elements(coalesce(mr.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb)) candidate
          where candidate->>'action_id'=mr.request_unit->>'action_id'
            and (candidate->>'source_identity'=v_source_identity or candidate->>'source_shift_id'=v_source_shift_id::text)) request_count,
        applied_match.applied_unit,applied_match.applied_count,
        policy_match.policy_unit,policy_match.policy_count,
        outcome_match.action_outcome,outcome_match.outcome_count,
        mr.response_json->'correction_operation_contract' operation_contract
      from matching_requests mr
      left join lateral (
        select min(applied::text)::jsonb applied_unit,count(*)::integer applied_count
        from jsonb_array_elements(coalesce(mr.response_json->'reconciliation_units','[]'::jsonb)) applied
        where applied->>'action_id'=mr.request_unit->>'action_id'
      ) applied_match on true
      left join lateral (
        select min(policy::text)::jsonb policy_unit,count(*)::integer policy_count
        from jsonb_array_elements(coalesce(mr.response_json#>'{correction_operation_contract,correction_units}','[]'::jsonb)) policy
        where policy->>'action_id'=mr.request_unit->>'action_id'
      ) policy_match on true
      left join lateral (
        select min(to_jsonb(outcome)::text)::jsonb action_outcome,count(*)::integer outcome_count
        from public.import_review_action_outcomes outcome
        where outcome.operation_id=mr.operation_id
          and outcome.action_id=mr.request_unit->>'action_id'
      ) outcome_match on true
    ), evaluated as (
      select t.*,
        case when t.operation_state='COMPLETE'
          and t.committed_at_utc is not null and t.finalised_at_utc is not null
          and t.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          and t.request_count=1 and t.applied_count=1 and t.policy_count=1 and t.outcome_count=1
          and t.request_unit->>'action_id'=t.applied_unit->>'action_id'
          and t.request_unit->>'action_id'=t.policy_unit->>'action_id'
          and t.request_unit->>'source_identity'=v_source_identity
          and t.applied_unit->>'source_identity'=v_source_identity
          and t.policy_unit->>'source_row_key'=v_source_identity
          and t.request_unit->>'source_system'=v_source_system
          and t.applied_unit->>'source_system'=v_source_system
          and t.request_unit->>'source_shift_id'=v_source_shift_id::text
          and t.applied_unit->>'source_shift_id'=v_source_shift_id::text
          and t.policy_unit->>'source_shift_id'=v_source_shift_id::text
          and t.request_unit->>'source_timesheet_id'=v_source_timesheet_id::text
          and t.policy_unit->>'root_timesheet_id'=v_source_timesheet_id::text
          and t.request_unit->>'candidate_id'=v_candidate_id::text
          and t.request_unit->>'client_id'=v_client_id::text
          and t.request_unit->>'contract_id'=v_contract_id::text
          and t.request_unit->>'week_ending_date'=v_week_ending_date::text
          and t.request_unit->>'invoice_stream'=v_invoice_stream
          and t.request_unit->>'source_scope_fingerprint'=v_scope_fingerprint
          and t.action_outcome->>'action_kind'='APPLY_AMENDMENT'
          and t.action_outcome->>'source_identity'=v_source_identity
          and t.action_outcome->>'shift_id'=v_source_shift_id::text
          and t.action_outcome->>'candidate_id'=v_candidate_id::text
          and t.action_outcome->>'client_id'=v_client_id::text
          and t.action_outcome->>'contract_id'=v_contract_id::text
          and nullif(t.action_outcome->>'evidence_fingerprint','') is not null
          and nullif(t.request_unit->>'unit_fingerprint','') is not null
          and t.request_unit->>'unit_fingerprint'=encode(digest(convert_to(concat_ws('|','unit-v2',
            t.request_unit->>'action_id',t.request_unit->>'source_identity',t.request_unit->>'source_shift_id',
            t.request_unit->>'route',t.request_unit->>'reconciliation_mode',
            t.request_unit->>'reconciliation_fingerprint',t.request_unit->>'review_policy_basis_kind',
            t.request_unit->>'review_policy_basis_fingerprint',t.action_outcome->>'evidence_fingerprint'),'UTF8'),'sha256'),'hex')
          and t.applied_unit->>'reviewed_unit_fingerprint'=t.request_unit->>'unit_fingerprint'
          and t.applied_unit->>'reconciliation_fingerprint'=t.request_unit->>'reconciliation_fingerprint'
          and case
            when t.request_unit->>'route'='CREATE_REVERSAL_REPLACEMENT' then
              coalesce(t.request_unit->>'repair_identity_mode','') in ('','CREATE_NEW_GENERATION')
              and t.applied_unit->>'repair_identity_mode'='CREATE_NEW_GENERATION'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is null
            when t.request_unit->>'route'='AMEND_EXISTING_REPLACEMENT' then
              t.request_unit->>'repair_identity_mode' in ('RETAIN_EXISTING_CORRECTION_ID','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED')
              and t.applied_unit->>'repair_identity_mode'=t.request_unit->>'repair_identity_mode'
              and nullif(t.request_unit->>'reviewed_existing_correction_id','') is not null
              and case
                when t.request_unit->>'repair_identity_mode'='RETAIN_EXISTING_CORRECTION_ID' then
                  t.applied_unit->>'correction_id'=t.request_unit->>'reviewed_existing_correction_id'
                else
                  t.request_unit->>'reviewed_existing_correction_id'<>t.applied_unit->>'correction_id'
                  and jsonb_typeof(t.request_unit->'reviewed_existing_member_ids')='array'
                  and jsonb_array_length(t.request_unit->'reviewed_existing_member_ids') between 1 and 2
                  and exists(select 1 from jsonb_array_elements_text(t.request_unit->'reviewed_existing_member_ids') reviewed(member_id)
                    where reviewed.member_id in (t.applied_unit->>'reversal_timesheet_id',t.applied_unit->>'replacement_timesheet_id'))
              end
            else false
          end
          and nullif(t.applied_unit->>'correction_id','') is not null
          and coalesce(t.applied_unit->>'reversal_timesheet_id','')~*v_uuid_re
          and coalesce(t.applied_unit->>'replacement_timesheet_id','')~*v_uuid_re
          and coalesce(t.applied_unit->>'parent_timesheet_id','')~*v_uuid_re
          and t.applied_unit->>'reversal_timesheet_id'<>t.applied_unit->>'replacement_timesheet_id'
          and jsonb_typeof(t.applied_unit->'applied_member_ids')='array'
          and jsonb_array_length(t.applied_unit->'applied_member_ids')=2
          and not exists(select 1 from jsonb_array_elements_text(t.applied_unit->'applied_member_ids') member(value)
            where member.value!~*v_uuid_re)
          and t.applied_unit->'applied_member_ids' @> jsonb_build_array(t.applied_unit->>'reversal_timesheet_id')
          and t.applied_unit->'applied_member_ids' @> jsonb_build_array(t.applied_unit->>'replacement_timesheet_id')
          and jsonb_typeof(t.policy_unit->'policy_envelope')='object'
          and nullif(t.policy_unit->>'policy_envelope_fingerprint','') is not null
          and t.policy_unit#>>'{policy_envelope,envelope_fingerprint}'=t.policy_unit->>'policy_envelope_fingerprint'
          and encode(digest(convert_to(((t.policy_unit->'policy_envelope')-'envelope_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
            =t.policy_unit->>'policy_envelope_fingerprint'
          and jsonb_typeof(t.operation_contract)='object'
          and nullif(t.operation_contract->>'operation_contract_fingerprint','') is not null
          and encode(digest(convert_to((t.operation_contract-'operation_contract_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
            =t.operation_contract->>'operation_contract_fingerprint'
          and case when coalesce(t.applied_unit->>'reversal_timesheet_id','')~*v_uuid_re
              and coalesce(t.applied_unit->>'replacement_timesheet_id','')~*v_uuid_re
              and coalesce(t.applied_unit->>'parent_timesheet_id','')~*v_uuid_re
            then t.applied_unit->>'applied_result_fingerprint'=encode(digest(convert_to(jsonb_build_object(
              'correction_id',t.applied_unit->>'correction_id',
              'reversal_timesheet_id',(t.applied_unit->>'reversal_timesheet_id')::uuid,
              'replacement_timesheet_id',(t.applied_unit->>'replacement_timesheet_id')::uuid,
              'M_active_member_ids',t.applied_unit->'applied_member_ids',
              'applied_member_ids',t.applied_unit->'applied_member_ids',
              'parent_timesheet_id',(t.applied_unit->>'parent_timesheet_id')::uuid,
              'repair_identity_mode',t.applied_unit->>'repair_identity_mode',
              'reviewed_unit_fingerprint',t.applied_unit->>'reviewed_unit_fingerprint',
              'reconciliation_fingerprint',t.applied_unit->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
            else false end
          then true else false end valid_historical_authority
      from triples t
    )
    select
      coalesce(jsonb_agg(jsonb_build_object(
        'operation_id',e.operation_id,'action_id',e.request_unit->>'action_id',
        'evidence_at',e.finalised_at_utc,'source_identity',e.request_unit->>'source_identity',
        'source_shift_id',e.request_unit->>'source_shift_id','source_timesheet_id',e.request_unit->>'source_timesheet_id',
        'correction_id',e.applied_unit->>'correction_id',
        'reversal_timesheet_id',e.applied_unit->>'reversal_timesheet_id',
        'replacement_timesheet_id',e.applied_unit->>'replacement_timesheet_id',
        'applied_member_ids',e.applied_unit->'applied_member_ids',
        'parent_timesheet_id',e.applied_unit->>'parent_timesheet_id',
        'route',e.request_unit->>'route',
        'invoice_stream',e.request_unit->>'invoice_stream',
        'source_scope_fingerprint',e.request_unit->>'source_scope_fingerprint',
        'reviewed_existing_correction_id',e.request_unit->>'reviewed_existing_correction_id',
        'reviewed_existing_member_ids',coalesce(e.request_unit->'reviewed_existing_member_ids','[]'::jsonb),
        'request_repair_identity_mode',e.request_unit->>'repair_identity_mode',
        'repair_identity_mode',e.applied_unit->>'repair_identity_mode',
        'reviewed_unit_fingerprint',e.request_unit->>'unit_fingerprint',
        'action_evidence_fingerprint',e.action_outcome->>'evidence_fingerprint',
        'reconciliation_fingerprint',e.request_unit->>'reconciliation_fingerprint',
        'B_standard_schedule_json',coalesce(e.request_unit->'B_standard_schedule_json','[]'::jsonb),
        'B_hours',coalesce(e.request_unit->'B_hours','{}'::jsonb),
        'A_schedule_json',coalesce(e.request_unit->'A_schedule_json','[]'::jsonb),
        'A_hours',coalesce(e.request_unit->'A_hours','{}'::jsonb),
        'policy_envelope',e.policy_unit->'policy_envelope',
        'policy_envelope_fingerprint',e.policy_unit->>'policy_envelope_fingerprint'
      ) order by e.finalised_at_utc,e.operation_id,e.request_unit->>'action_id')
        filter(where e.valid_historical_authority),'[]'::jsonb),
      coalesce(bool_or(e.operation_state='COMPLETE'
        and e.request_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and not e.valid_historical_authority),false),
      coalesce(bool_or(e.operation_state in ('SOURCE_COMMITTED_TSFIN_PENDING','FINANCIALISED_PENDING_FINALISATION')
        and e.committed_at_utc is not null
        and e.operation_id is distinct from v_transition_operation_id),false)
    into v_operation_evidence,v_operation_evidence_conflict,v_operation_in_progress
    from evaluated e;

    -- A surviving member can be re-keyed more than once as successive archived
    -- siblings occupy prior correction identities.  Canonicalise the complete
    -- bounded chain (C1 -> C2 -> C3), retain every earlier assignment as audit
    -- history, and fail closed on cycles or branches.
    with recursive direct_edges as (
      select role.member_id::uuid member_timesheet_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id' superseded_correction_id,
        unit->>'correction_id' canonical_correction_id,
        array_agg(distinct (unit->>'operation_id')::uuid order by (unit->>'operation_id')::uuid) operation_ids
      from jsonb_array_elements(v_operation_evidence) unit
      cross join lateral (values
        ('CHANGED_HOURS_REVERSAL'::text,unit->>'reversal_timesheet_id'),
        ('CHANGED_HOURS_REPLACEMENT'::text,unit->>'replacement_timesheet_id')
      ) role(correction_kind,member_id)
      where unit->>'route'='AMEND_EXISTING_REPLACEMENT'
        and unit->>'repair_identity_mode'='FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED'
        and nullif(unit->>'reviewed_existing_correction_id','') is not null
        and unit->>'reviewed_existing_correction_id'<>unit->>'correction_id'
        and role.member_id~*v_uuid_re
        and coalesce(unit->'reviewed_existing_member_ids','[]'::jsonb) @> jsonb_build_array(role.member_id)
        and not exists(
          select 1
          from public.invoice_lines historical_line
          join public.invoices historical_invoice on historical_invoice.id=historical_line.invoice_id
          where (historical_line.timesheet_id=role.member_id::uuid
              or historical_line.meta_json->>'timesheet_id'=role.member_id)
            and historical_line.created_at<=coalesce((unit->>'evidence_at')::timestamptz,'infinity'::timestamptz)
            and upper(coalesce(historical_line.meta_json->>'line_type',''))
              !~ '^(EXPENSE(_.*)?|MILEAGE|TRAVEL|ACCOMMODATION|REIMBURSEMENT|ADDITION)$'
            and (
              historical_invoice.status::text='DRAFT'
              or historical_invoice.issued_at_utc is null
              or historical_invoice.active_document_operation_id is not null
              or historical_invoice.active_issue_operation_id is not null
              or upper(coalesce(historical_invoice.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED')
              or (historical_invoice.status::text in ('ISSUED','PAID','ON_HOLD')
                and historical_invoice.issued_at_utc is not null)
            ))
      group by role.member_id,role.correction_kind,
        unit->>'reviewed_existing_correction_id',unit->>'correction_id'
    ), walk as (
      select edge.member_timesheet_id,edge.correction_kind,
        edge.superseded_correction_id root_correction_id,
        edge.canonical_correction_id reached_correction_id,
        array[edge.superseded_correction_id,edge.canonical_correction_id]::text[] path,
        edge.operation_ids,1 depth,
        edge.canonical_correction_id=edge.superseded_correction_id cycle
      from direct_edges edge
      union all
      select walk.member_timesheet_id,walk.correction_kind,walk.root_correction_id,
        edge.canonical_correction_id,walk.path||edge.canonical_correction_id,
        walk.operation_ids||edge.operation_ids,walk.depth+1,
        edge.canonical_correction_id=any(walk.path)
      from walk
      join direct_edges edge
        on edge.member_timesheet_id=walk.member_timesheet_id
       and edge.correction_kind=walk.correction_kind
       and edge.superseded_correction_id=walk.reached_correction_id
      where not walk.cycle and walk.depth<p_max_operations_per_source
    ), closure as (
      select distinct on (member_timesheet_id,correction_kind,root_correction_id,reached_correction_id)
        member_timesheet_id,correction_kind,root_correction_id,reached_correction_id,
        operation_ids,depth,path
      from walk
      where not cycle and root_correction_id<>reached_correction_id
      order by member_timesheet_id,correction_kind,root_correction_id,reached_correction_id,depth desc,operation_ids::text
    ), conflicts as (
      select exists(
        select 1 from direct_edges edge
        group by edge.member_timesheet_id,edge.correction_kind,edge.superseded_correction_id
        having count(distinct edge.canonical_correction_id)<>1
      )
      or exists(select 1 from walk where cycle)
      or exists(
        select 1 from walk
        where depth=p_max_operations_per_source
          and exists(select 1 from direct_edges edge
            where edge.member_timesheet_id=walk.member_timesheet_id
              and edge.correction_kind=walk.correction_kind
              and edge.superseded_correction_id=walk.reached_correction_id)
      )
      or exists(
        select 1
        from walk terminal
        where not terminal.cycle and not exists(select 1 from direct_edges edge
          where edge.member_timesheet_id=terminal.member_timesheet_id
            and edge.correction_kind=terminal.correction_kind
            and edge.superseded_correction_id=terminal.reached_correction_id)
        group by terminal.member_timesheet_id,terminal.correction_kind,terminal.root_correction_id
        having count(distinct terminal.reached_correction_id)<>1
      ) as has_conflict
    )
    select coalesce((select jsonb_agg(jsonb_build_object(
        'operation_id',entry.operation_ids[cardinality(entry.operation_ids)],
        'operation_ids',to_jsonb(entry.operation_ids),
        'member_timesheet_id',entry.member_timesheet_id,
        'correction_kind',entry.correction_kind,
        'superseded_correction_id',entry.root_correction_id,
        'canonical_correction_id',entry.reached_correction_id,
        'supersession_depth',entry.depth,
        'supersession_path',to_jsonb(entry.path)
      ) order by entry.member_timesheet_id,entry.correction_kind,
        entry.root_correction_id,entry.depth,entry.reached_correction_id)
      from closure entry),'[]'::jsonb),
      coalesce((select has_conflict from conflicts),false)
    into v_member_supersession_map,v_member_supersession_conflict;
    v_operation_evidence_conflict:=v_operation_evidence_conflict or v_member_supersession_conflict;

    select coalesce(array_agg(distinct member_id order by member_id),array[]::uuid[])
    into v_operation_member_ids
    from (
      select (unit->>'reversal_timesheet_id')::uuid member_id
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select (unit->>'replacement_timesheet_id')::uuid
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select member.value::uuid
      from jsonb_array_elements(v_operation_evidence) unit
      cross join lateral jsonb_array_elements_text(unit->'applied_member_ids') member(value)
    ) ids;

    select coalesce(array_agg(distinct timesheet_id order by timesheet_id),array[]::uuid[])
    into v_hist_ids
    from (
      select v_source_timesheet_id timesheet_id
      union all select s.timesheet_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all select unnest(v_audit_ids)
      union all select unnest(v_operation_member_ids)
      union all
      select t.timesheet_id
      from public.timesheets t
      join public.timesheets_financials tf_scope on tf_scope.timesheet_id=t.timesheet_id
      where tf_scope.candidate_id=v_candidate_id and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
        and (
          (jsonb_typeof(t.actual_schedule_json)='array' and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key)))
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_shift_id}'=v_source_shift_id::text
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_row_key}'=v_external_row_key
        )
    ) ids where timesheet_id is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_archived_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.archived_at_utc is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_active_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.is_current and t.archived_at_utc is null;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_missing_ids
    from (select distinct unnest(v_audit_ids||v_operation_member_ids) x) missing
    where not exists(select 1 from public.timesheets t where t.timesheet_id=missing.x);

    -- Canonical source/role ownership is established once and then reused by
    -- both invoice balance and generation classification.  Higher-authority
    -- evidence may fill a missing identity but contradictory identities fail
    -- closed instead of being resolved by arbitrary precedence.
    with evidence as (
      select (unit->>'reversal_timesheet_id')::uuid timesheet_id,unit->>'correction_id' correction_id,
        'CHANGED_HOURS_REVERSAL'::text correction_kind,'COMPLETED_OPERATION'::text evidence_source,
        1 priority,true operation_proven
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select (unit->>'replacement_timesheet_id')::uuid,unit->>'correction_id',
        'CHANGED_HOURS_REPLACEMENT','COMPLETED_OPERATION',1,true
      from jsonb_array_elements(v_operation_evidence) unit
      union all
      select raw.member_id,ae.after_json->>'correction_id','CHANGED_HOURS_REVERSAL','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'reversal_timesheet_id'~*v_uuid_re
        then (ae.after_json->>'reversal_timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select raw.member_id,ae.after_json->>'correction_id','CHANGED_HOURS_REPLACEMENT','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'replacement_timesheet_id'~*v_uuid_re
        then (ae.after_json->>'replacement_timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select raw.member_id,ae.after_json->>'correction_id',ae.after_json->>'correction_kind','CORRECTION_AUDIT',2,false
      from public.audit_events ae
      cross join lateral (select case when ae.after_json->>'timesheet_id'~*v_uuid_re
        then (ae.after_json->>'timesheet_id')::uuid end member_id) raw
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and ae.after_json->>'correction_kind' in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        and nullif(ae.after_json->>'correction_id','') is not null and raw.member_id is not null
      union all
      select t.timesheet_id,t.correction_id,t.correction_kind::text,'LIVE_ROW',4,false
      from public.timesheets t
      where t.timesheet_id=any(v_hist_ids) and nullif(t.correction_id,'') is not null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
    ), conflicts as (
      select distinct left_evidence.timesheet_id
      from evidence left_evidence
      join evidence right_evidence on right_evidence.timesheet_id=left_evidence.timesheet_id
        and (right_evidence.correction_id,right_evidence.correction_kind)
          is distinct from (left_evidence.correction_id,left_evidence.correction_kind)
      where left_evidence.correction_kind<>right_evidence.correction_kind
        or (left_evidence.correction_id<>right_evidence.correction_id
          and not exists(
            select 1 from jsonb_array_elements(v_member_supersession_map) edge
            where edge->>'member_timesheet_id'=left_evidence.timesheet_id::text
              and edge->>'correction_kind'=left_evidence.correction_kind
              and ((edge->>'superseded_correction_id'=left_evidence.correction_id
                    and edge->>'canonical_correction_id'=right_evidence.correction_id)
                or (edge->>'superseded_correction_id'=right_evidence.correction_id
                    and edge->>'canonical_correction_id'=left_evidence.correction_id))))
    ), canonical as (
      select distinct on (e.timesheet_id) e.*
      from evidence e
      where not exists(select 1 from conflicts c where c.timesheet_id=e.timesheet_id)
        and not exists(
          select 1 from jsonb_array_elements(v_member_supersession_map) edge
          where edge->>'member_timesheet_id'=e.timesheet_id::text
            and edge->>'correction_kind'=e.correction_kind
            and edge->>'superseded_correction_id'=e.correction_id
            and edge->>'canonical_correction_id'<>e.correction_id)
      order by e.timesheet_id,e.priority,e.correction_id,e.correction_kind
    )
    select
      coalesce((select jsonb_agg(jsonb_build_object(
        'timesheet_id',c.timesheet_id,'correction_id',c.correction_id,'correction_kind',c.correction_kind,
        'evidence_source',c.evidence_source,'operation_proven',c.operation_proven,
        'source_system',v_source_system,'source_identity',v_source_identity,'source_shift_id',v_source_shift_id
      ) order by c.timesheet_id) from canonical c),'[]'::jsonb),
      exists(select 1 from conflicts)
    into v_member_role_map,v_member_role_conflict;

    v_effective_invoice_ids:=array[]::uuid[];
    v_effective_line_ids:=array[]::uuid[];
    v_credit_line_ids:=array[]::uuid[];
    v_effective_component_count:=0;
    v_b_day:=0; v_b_night:=0; v_b_sat:=0; v_b_sun:=0; v_b_bh:=0;
    v_b_pay:=0; v_b_charge:=0; v_b_margin:=0;
    v_b_schedule:='[]'::jsonb; v_candidate_schedule:='[]'::jsonb; v_candidate_hours:='{}'::jsonb;
    v_schedule_candidates:='[]'::jsonb; v_candidate_policy_fingerprint:=null; v_b_policy_fingerprint:=null;
    v_terminal_generation_id:=null; v_terminal_positive_timesheet_id:=null; v_terminal_positive_member_count:=0;
    v_terminal_frozen_candidate:=null; v_terminal_frozen_candidate_count:=0;
    v_terminal_frozen_schedule_variant_count:=0; v_terminal_frozen_policy_variant_count:=0;
    v_terminal_operation:=null; v_terminal_operation_schedule:='[]'::jsonb; v_terminal_operation_hours:='{}'::jsonb;
    v_terminal_operation_policy_fingerprint:=null; v_terminal_operation_id:=null;
    v_terminal_policy_snapshot:='{}'::jsonb; v_terminal_policy_snapshot_fingerprint:=null;
    v_terminal_derived_day:=0; v_terminal_derived_night:=0; v_terminal_derived_sat:=0;
    v_terminal_derived_sun:=0; v_terminal_derived_bh:=0; v_terminal_derived_total:=0;
    v_terminal_frozen_matches_b:=false; v_terminal_operation_matches_b:=false;
    v_terminal_schedule_authority_conflict:=false; v_terminal_policy_authority_conflict:=false;
    v_b_standard_schedule_authority:='NONE'; v_b_standard_schedule_authority_timesheet_id:=null;
    v_b_standard_schedule_authority_correction_id:=null; v_b_standard_schedule_authority_operation_id:=null;
    v_b_standard_schedule_authority_policy_fingerprint:=null; v_b_standard_schedule_authority_fingerprint:=null;
    v_b_standard_schedule_authority_diagnostic:=null;
    v_line_evidence:='[]'::jsonb;
    v_ignored_nonhours_line_ids:=array[]::uuid[];
    v_generation_role_evidence:='[]'::jsonb;
    v_fully_invoiced_generation_ids:=array[]::text[];
    v_partial_generation_ids:=array[]::text[];
    v_mutable_generation_ids:=array[]::text[];
    v_archived_history_roles:='[]'::jsonb;
    v_role_evidence_conflicts:='[]'::jsonb;
    v_repair_identity_mode:=null;
    v_reversal_repair_required:=false;
    v_replacement_repair_required:=false;
    v_role_partial_invoice_state:=false;
    v_role_active_invoice_activity:=false;
    v_role_scope_unprovable:=false;
    v_scope_unprovable:=false; v_credit_ambiguous:=false; v_stream_conflict:=false;
    v_archived_invoice_conflict:=false; v_active_invoice_activity:=false;
    v_financial_position_requires_amendment:=false;

    with directly_scoped as (
      select il.id
      from public.invoice_lines il
      where il.timesheet_id=any(v_hist_ids)
        or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
          then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
    ), scoped as (
      select il.id
      from public.invoice_lines il where il.id in(select id from directly_scoped)
      union
      select credit.id
      from public.invoice_lines credit
      where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
        and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
    )
    select count(*)::integer into v_line_count from scoped;
    if v_line_count>p_max_invoice_lines_per_source then
      raise exception 'IMPORT_REVIEW_INVOICE_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    for v_line in
      with directly_scoped as (
        select il.id
        from public.invoice_lines il
        where il.timesheet_id=any(v_hist_ids)
          or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
            then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
      ), scoped as (
        select il.id from public.invoice_lines il where il.id in(select id from directly_scoped)
        union
        select credit.id from public.invoice_lines credit
        where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
          and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
      )
      select il.*,i.type::text invoice_type,i.status::text invoice_status,i.issued_at_utc,
        i.client_id invoice_client_id,i.original_invoice_id,
        i.active_document_operation_id,i.active_issue_operation_id,i.issue_state
      from scoped s join public.invoice_lines il on il.id=s.id join public.invoices i on i.id=il.invoice_id
      order by i.issued_at_utc nulls last,il.id
    loop
      -- Archived rows are audit-only.  They cannot contribute to the current
      -- source balance or make an otherwise repairable generation block.
      if v_line.timesheet_id=any(v_archived_ids)
         or (coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
           and (v_line.meta_json->>'timesheet_id')::uuid=any(v_archived_ids)) then
        continue;
      end if;

      v_tf:=null; v_seg:=null; v_seg_count:=0; v_matching_seg_count:=0;
      if coalesce(v_line.meta_json->>'tsfin_id','')~*v_uuid_re then
        select tf.* into v_tf from public.timesheets_financials tf where tf.id=(v_line.meta_json->>'tsfin_id')::uuid;
      elsif v_line.timesheet_id is not null then
        select tf.* into v_tf from public.timesheets_financials tf
        where tf.timesheet_id=v_line.timesheet_id
        order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
      end if;
      if v_tf.id is not null then
        select count(*)::integer,
          count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key)::integer,
          (array_agg(seg order by case when seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text then 0 else 1 end)
            filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key))[1]
        into v_seg_count,v_matching_seg_count,v_seg
        from jsonb_array_elements(case when jsonb_typeof(v_tf.invoice_breakdown_json->'segments')='array' then v_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
      end if;

      v_original_line:=null;
      v_original_invoice:=null;
      v_original_tf:=null;
      v_original_seg:=null;
      v_original_seg_count:=0;
      v_original_matching_seg_count:=0;
      v_original_line_id:=null;
      if v_line.invoice_type='CREDIT_NOTE' then
        if coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')~*v_uuid_re then
          v_original_line_id:=coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id')::uuid;
          select original.* into v_original_line from public.invoice_lines original where original.id=v_original_line_id;
        end if;
        if v_original_line.id is null then
          v_scope_unprovable:=true;
          continue;
        end if;
        select original_invoice.* into v_original_invoice
        from public.invoices original_invoice
        where original_invoice.id=v_original_line.invoice_id;
        -- A credit is admissible only when the header, original physical line,
        -- client and exact source identity all prove one lineage.  The line's
        -- signed values are never trusted in isolation.
        if v_original_invoice.id is null
          or v_line.original_invoice_id is distinct from v_original_line.invoice_id
          or v_line.meta_json->>'original_invoice_line_id' is distinct from v_original_line.id::text
          or v_line.invoice_client_id is distinct from v_original_invoice.client_id
          or not (
            coalesce(v_line.timesheet_id::text,
              case when coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
                then v_line.meta_json->>'timesheet_id' end)
              is not distinct from
            coalesce(v_original_line.timesheet_id::text,
              case when coalesce(v_original_line.meta_json->>'timesheet_id','')~*v_uuid_re
                then v_original_line.meta_json->>'timesheet_id' end)
            or (
              exists(select 1 from jsonb_array_elements(v_member_role_map) credit_member
                where credit_member->>'timesheet_id'=coalesce(v_line.timesheet_id::text,v_line.meta_json->>'timesheet_id')
                  and credit_member->>'source_identity'=v_source_identity)
              and exists(select 1 from jsonb_array_elements(v_member_role_map) original_member
                where original_member->>'timesheet_id'=coalesce(v_original_line.timesheet_id::text,v_original_line.meta_json->>'timesheet_id')
                  and original_member->>'source_identity'=v_source_identity)
            )
          ) then
          v_scope_unprovable:=true;
          continue;
        end if;
        if coalesce(v_original_line.meta_json->>'tsfin_id','')~*v_uuid_re then
          select tf.* into v_original_tf from public.timesheets_financials tf where tf.id=(v_original_line.meta_json->>'tsfin_id')::uuid;
        elsif v_original_line.timesheet_id is not null then
          select tf.* into v_original_tf from public.timesheets_financials tf
          where tf.timesheet_id=v_original_line.timesheet_id
          order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
        end if;
        if v_original_tf.id is not null then
          select count(*)::integer,
            count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text
              or seg->>'shift_id'=v_source_shift_id::text
              or seg->>'external_row_key'=v_external_row_key)::integer,
            (array_agg(seg order by seg::text) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text
              or seg->>'shift_id'=v_source_shift_id::text
              or seg->>'external_row_key'=v_external_row_key))[1]
          into v_original_seg_count,v_original_matching_seg_count,v_original_seg
          from jsonb_array_elements(case when jsonb_typeof(v_original_tf.invoice_breakdown_json->'segments')='array'
            then v_original_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
        end if;
        -- The credit writer preserves the original hour buckets and writes one
        -- exact signed monetary mirror.  Reject partial or contradictory credit
        -- shapes before allocating any source component.
        if coalesce(v_line.hours_day,0)<>coalesce(v_original_line.hours_day,0)
          or coalesce(v_line.hours_night,0)<>coalesce(v_original_line.hours_night,0)
          or coalesce(v_line.hours_sat,0)<>coalesce(v_original_line.hours_sat,0)
          or coalesce(v_line.hours_sun,0)<>coalesce(v_original_line.hours_sun,0)
          or coalesce(v_line.hours_bh,0)<>coalesce(v_original_line.hours_bh,0)
          or round(coalesce(v_line.total_pay_ex_vat,0),2)<>-round(coalesce(v_original_line.total_pay_ex_vat,0),2)
          or round(coalesce(v_line.total_charge_ex_vat,0),2)<>-round(coalesce(v_original_line.total_charge_ex_vat,0),2)
          or round(coalesce(v_line.margin_ex_vat,v_line.total_charge_ex_vat-v_line.total_pay_ex_vat,0),2)
            <>-round(coalesce(v_original_line.margin_ex_vat,v_original_line.total_charge_ex_vat-v_original_line.total_pay_ex_vat,0),2)
          or (v_original_seg_count>0 and v_original_matching_seg_count<>1)
          or (v_original_seg_count>1 and (v_original_seg is null
            or nullif(v_original_seg->>'pay_amount','') is null
            or nullif(v_original_seg->>'charge_amount','') is null)) then
          v_scope_unprovable:=true;
          continue;
        end if;
      end if;

      v_line_type:=upper(nullif(btrim(coalesce(v_line.meta_json->>'line_type','')),''));
      v_original_line_type:=upper(nullif(btrim(coalesce(v_original_line.meta_json->>'line_type','')),''));
      v_is_separable_nonhours:=coalesce(case when v_line.invoice_type='CREDIT_NOTE' then v_original_line_type else v_line_type end,'')
        ~ '^(EXPENSE(_.*)?|MILEAGE|TRAVEL|ACCOMMODATION|REIMBURSEMENT|ADDITION)$';
      if v_is_separable_nonhours then
        v_ignored_nonhours_line_ids:=array_append(v_ignored_nonhours_line_ids,v_line.id);
        continue;
      end if;
      v_is_weekly_hours:=coalesce(case when v_line.invoice_type='CREDIT_NOTE' then v_original_line_type else v_line_type end,'')='HOURS_WEEKLY';
      if not v_is_weekly_hours then
        -- Legacy lines are acceptable only when a single frozen source segment
        -- proves the exact Weekly component for this shift.
        v_is_weekly_hours:=case when v_line.invoice_type='CREDIT_NOTE'
          then v_original_seg is not null
          else v_matching_seg_count=1 end;
      end if;
      if not v_is_weekly_hours then
        v_scope_unprovable:=true;
        continue;
      end if;

      v_component_timesheet_id:=case
        when v_line.invoice_type='CREDIT_NOTE' then coalesce(
          v_original_line.timesheet_id,
          case when coalesce(v_original_line.meta_json->>'timesheet_id','')~*v_uuid_re
            then (v_original_line.meta_json->>'timesheet_id')::uuid end)
        else coalesce(v_line.timesheet_id,
          case when coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re
            then (v_line.meta_json->>'timesheet_id')::uuid end)
        end;
      v_component_correction_id:=null;
      v_component_correction_kind:=null;
      v_operation_member_scope_proven:=false;
      select member->>'correction_id',member->>'correction_kind',
        coalesce((member->>'operation_proven')::boolean,false)
      into v_component_correction_id,v_component_correction_kind,v_operation_member_scope_proven
      from jsonb_array_elements(v_member_role_map) member
      where member->>'timesheet_id'=v_component_timesheet_id::text
      limit 1;

      if v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null
         or v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED') then
        v_active_invoice_activity:=true;
      end if;
      if v_line.invoice_type='CREDIT_NOTE'
         and v_line.invoice_status in ('ISSUED','PAID','ON_HOLD') and v_line.issued_at_utc is not null and (
        select count(*) from public.invoice_lines other_credit
        join public.invoices other_credit_invoice on other_credit_invoice.id=other_credit.invoice_id
        where coalesce(other_credit.meta_json->>'original_invoice_line_id',other_credit.meta_json->>'credit_of_line_id','')
          =coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')
          and other_credit_invoice.type='CREDIT_NOTE' and other_credit_invoice.status in ('ISSUED','PAID','ON_HOLD')
          and other_credit_invoice.issued_at_utc is not null
      )>1 then
        v_credit_ambiguous:=true;
      end if;
      v_single_source:=v_matching_seg_count=1 and v_seg_count=1;
      if not v_single_source and v_line.timesheet_id is not null then
        select jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key))
        into v_single_source from public.timesheets t where t.timesheet_id=v_line.timesheet_id;
        v_single_source:=coalesce(v_single_source,false);
      end if;
      v_line_scope_proven:=case when v_line.invoice_type='CREDIT_NOTE'
        then v_original_seg is not null
          or coalesce(v_original_line.timesheet_id=any(v_hist_ids),false)
          or (v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY')
        else v_single_source or v_matching_seg_count=1
          or (v_operation_member_scope_proven and v_line_type='HOURS_WEEKLY') end;
      if not v_line_scope_proven or (v_line.invoice_type='CREDIT_NOTE' and v_original_seg is null
          and not coalesce(v_original_line.timesheet_id=any(v_hist_ids),false)
          and not (v_operation_member_scope_proven and v_original_line_type='HOURS_WEEKLY')) then
        v_scope_unprovable:=true;
        continue;
      end if;
      if (v_line.invoice_type<>'CREDIT_NOTE' and v_tf.id is not null
            and (case when upper(coalesce(v_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream)
         or (v_line.invoice_type='CREDIT_NOTE' and v_original_tf.id is not null
            and (case when upper(coalesce(v_original_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream) then
        v_stream_conflict:=true;
        continue;
      end if;

      if v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null
         or v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED') then
        v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
          'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
          'economic_state','PENDING','timesheet_id',v_component_timesheet_id,
          'correction_id',v_component_correction_id,'correction_kind',v_component_correction_kind));
        continue;
      end if;

      if v_line.invoice_type='CREDIT_NOTE' then
        -- Hours are the negative of the exact original frozen component.  A
        -- multi-source credit receives the matching source segment's money,
        -- never the whole aggregate line's money.
        if v_original_seg is not null then
          v_component_day:=-coalesce((v_original_seg->>'hours_day')::numeric,0);
          v_component_night:=-coalesce((v_original_seg->>'hours_night')::numeric,0);
          v_component_sat:=-coalesce((v_original_seg->>'hours_sat')::numeric,0);
          v_component_sun:=-coalesce((v_original_seg->>'hours_sun')::numeric,0);
          v_component_bh:=-coalesce((v_original_seg->>'hours_bh')::numeric,0);
        else
          v_component_day:=-coalesce(v_original_line.hours_day,0);
          v_component_night:=-coalesce(v_original_line.hours_night,0);
          v_component_sat:=-coalesce(v_original_line.hours_sat,0);
          v_component_sun:=-coalesce(v_original_line.hours_sun,0);
          v_component_bh:=-coalesce(v_original_line.hours_bh,0);
        end if;
        if v_original_seg_count>1 then
          v_component_pay:=-coalesce((v_original_seg->>'pay_amount')::numeric,0);
          v_component_charge:=-coalesce((v_original_seg->>'charge_amount')::numeric,0);
          v_component_margin:=v_component_charge-v_component_pay;
        else
          v_component_pay:=coalesce(v_line.total_pay_ex_vat,0);
          v_component_charge:=coalesce(v_line.total_charge_ex_vat,0);
          v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
        end if;
      elsif v_single_source or v_operation_member_scope_proven then
        v_component_day:=coalesce(v_line.hours_day,0); v_component_night:=coalesce(v_line.hours_night,0);
        v_component_sat:=coalesce(v_line.hours_sat,0); v_component_sun:=coalesce(v_line.hours_sun,0); v_component_bh:=coalesce(v_line.hours_bh,0);
        v_component_pay:=coalesce(v_line.total_pay_ex_vat,0); v_component_charge:=coalesce(v_line.total_charge_ex_vat,0); v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
      else
        v_component_day:=coalesce((v_seg->>'hours_day')::numeric,0); v_component_night:=coalesce((v_seg->>'hours_night')::numeric,0);
        v_component_sat:=coalesce((v_seg->>'hours_sat')::numeric,0); v_component_sun:=coalesce((v_seg->>'hours_sun')::numeric,0); v_component_bh:=coalesce((v_seg->>'hours_bh')::numeric,0);
        v_component_pay:=coalesce((v_seg->>'pay_amount')::numeric,0); v_component_charge:=coalesce((v_seg->>'charge_amount')::numeric,0); v_component_margin:=v_component_charge-v_component_pay;
      end if;
      v_b_day:=v_b_day+v_component_day; v_b_night:=v_b_night+v_component_night; v_b_sat:=v_b_sat+v_component_sat; v_b_sun:=v_b_sun+v_component_sun; v_b_bh:=v_b_bh+v_component_bh;
      v_b_pay:=v_b_pay+v_component_pay; v_b_charge:=v_b_charge+v_component_charge; v_b_margin:=v_b_margin+v_component_margin;
      v_effective_component_count:=v_effective_component_count+1;
      v_effective_invoice_ids:=array_append(v_effective_invoice_ids,v_line.invoice_id);
      v_effective_line_ids:=array_append(v_effective_line_ids,v_line.id);
      if v_line.invoice_type='CREDIT_NOTE' then v_credit_line_ids:=array_append(v_credit_line_ids,v_line.id); end if;
      v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
        'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
        'economic_state','EFFECTIVE','timesheet_id',v_component_timesheet_id,
        'correction_id',v_component_correction_id,'correction_kind',v_component_correction_kind,
        'hours',jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh),
        'pay_ex_vat',v_component_pay,'charge_ex_vat',v_component_charge,'margin_ex_vat',v_component_margin));
      if v_seg is not null and (v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh)>0 then
        v_candidate_schedule:=jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce(v_seg->>'date',(v_a_schedule->0)->>'date'),
          'start_utc',v_seg->>'start_utc','end_utc',v_seg->>'end_utc',
          'break_mins',coalesce((v_seg->>'break_mins')::integer,0),
          'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key,
          'import_id',coalesce(v_seg->>'import_id',v_authoritative_import_id::text),
          'ref_num',coalesce(v_seg->>'ref_num',v_seg->>'reference_number',(v_a_schedule->0)->>'ref_num')
        )));
        v_candidate_hours:=jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh,'total_hours',v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh);
        v_candidate_policy_fingerprint:=case
          when v_component_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') then
            case when jsonb_typeof(v_tf.policy_snapshot_json->'correction_financials_policy_envelope')='object'
                and nullif(coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                  v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'),'') is not null
                and v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
                  =coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                    v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
                and encode(digest(convert_to(((v_tf.policy_snapshot_json->'correction_financials_policy_envelope')
                    -'envelope_fingerprint'::text)::text,'UTF8'),'sha256'),'hex')
                  =coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                    v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
              then coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
                v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}') end
          else coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
            v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
            encode(digest(convert_to(coalesce(v_tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex')) end;
        v_schedule_candidates:=v_schedule_candidates||jsonb_build_array(jsonb_build_object(
          'timesheet_id',v_component_timesheet_id,'correction_id',v_component_correction_id,
          'correction_kind',v_component_correction_kind,'invoice_line_id',v_line.id,
          'schedule_json',v_candidate_schedule,'hours',v_candidate_hours,
          'policy_fingerprint',v_candidate_policy_fingerprint));
      end if;
    end loop;

    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_invoice_ids from unnest(v_effective_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_line_ids from unnest(v_effective_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_credit_line_ids from unnest(v_credit_line_ids) x;
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v1',v_source_identity,v_line_evidence::text,v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin),'UTF8'),'sha256'),'hex');

    -- Classify each generation from the same admitted, signed Weekly-hours
    -- component ledger used for B.  Raw invoice-line existence is never a
    -- second financial authority, and archived identities are audit-only.
    with correction_seed as (
      select correction_id,max(evidence_at) evidence_at
      from (
        select member->>'correction_id' correction_id,
          coalesce((select max(coalesce(t.updated_at,t.created_at)) from public.timesheets t
            where t.timesheet_id=(member->>'timesheet_id')::uuid),'-infinity'::timestamptz) evidence_at
        from jsonb_array_elements(v_member_role_map) member
        union all
        select unit->>'correction_id',coalesce((unit->>'evidence_at')::timestamptz,'-infinity'::timestamptz)
        from jsonb_array_elements(v_operation_evidence) unit
      ) seeded where nullif(correction_id,'') is not null group by correction_id
    ), roles as (
      select seed.correction_id,seed.evidence_at,role
      from correction_seed seed
      cross join lateral unnest(array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT']) role
    ), role_state as (
      select r.correction_id,r.evidence_at,r.role,
        coalesce((select array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid)
          from jsonb_array_elements(v_member_role_map) member
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role),array[]::uuid[]) member_ids,
        coalesce((select array_agg(distinct t.timesheet_id order by t.timesheet_id)
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null),array[]::uuid[]) active_ids,
        coalesce((select array_agg(distinct t.timesheet_id order by t.timesheet_id)
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.archived_at_utc is not null),array[]::uuid[]) archived_ids,
        coalesce((select array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid)
          from jsonb_array_elements(v_member_role_map) member
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and coalesce((member->>'operation_proven')::boolean,false)
            and not exists(select 1 from public.timesheets t where t.timesheet_id=(member->>'timesheet_id')::uuid)),array[]::uuid[]) operation_missing_ids,
        exists(select 1 from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE') has_effective_history,
        exists(select 1 from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='PENDING')
          or exists(select 1
            from jsonb_array_elements(v_member_role_map) member
            join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
            left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
            left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
            where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
              and t.is_current and t.archived_at_utc is null
              and (tf.locked_by_invoice_id is not null or upper(coalesce(cw.status::text,''))='INVOICED'
                or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
                  then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
                  where nullif(seg->>'invoice_locked_invoice_id','') is not null))) pending_invoice,
        exists(select 1
          from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null and tf.paid_at_utc is not null) paid,
        (select count(*) from jsonb_array_elements(v_member_role_map) member
          join public.timesheets t on t.timesheet_id=(member->>'timesheet_id')::uuid
          where member->>'correction_id'=r.correction_id and member->>'correction_kind'=r.role
            and t.is_current and t.archived_at_utc is null)>1 active_duplicate,
        (select count(distinct component->>'timesheet_id') from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state' in ('EFFECTIVE','PENDING'))>1 economic_member_duplicate,
        coalesce((select sum(coalesce((component#>>'{hours,hours_day}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_day,
        coalesce((select sum(coalesce((component#>>'{hours,hours_night}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_night,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sat}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_sat,
        coalesce((select sum(coalesce((component#>>'{hours,hours_sun}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_sun,
        coalesce((select sum(coalesce((component#>>'{hours,hours_bh}')::numeric,0))
          from jsonb_array_elements(v_line_evidence) component
          where component->>'correction_id'=r.correction_id and component->>'correction_kind'=r.role
            and component->>'economic_state'='EFFECTIVE'),0) net_bh
      from roles r
    ), generation_state as (
      select correction_id,max(evidence_at) evidence_at,
        count(*) filter(where cardinality(member_ids)>0) proven_roles,
        count(*) filter(where has_effective_history) effective_roles,
        count(*) filter(where pending_invoice) pending_roles,
        count(*) filter(where cardinality(active_ids)>0) active_role_count,
        count(*) filter(where cardinality(operation_missing_ids)>0) missing_operation_role_count,
        count(*) filter(where cardinality(active_ids)=0 and cardinality(archived_ids)>0
          and cardinality(operation_missing_ids)=0) archived_only_role_count,
        bool_or(paid) paid,bool_or(active_duplicate) active_duplicate,
        bool_or(economic_member_duplicate) economic_member_duplicate,
        jsonb_agg(jsonb_build_object(
          'role',role,'member_ids',to_jsonb(member_ids),'active_member_ids',to_jsonb(active_ids),
          'archived_member_ids',to_jsonb(archived_ids),'operation_proven_missing_member_ids',to_jsonb(operation_missing_ids),
          'has_effective_history',has_effective_history,'effective_state',case
            when has_effective_history and net_day+net_night+net_sat+net_sun+net_bh=0 then 'SETTLED_ZERO_HISTORY'
            when has_effective_history then 'EFFECTIVE_HISTORY'
            when pending_invoice then 'PENDING_INVOICE'
            when cardinality(active_ids)>0 then 'ACTIVE_MUTABLE'
            when cardinality(operation_missing_ids)>0 then 'PHYSICALLY_MISSING_MUTABLE'
            when cardinality(archived_ids)>0 then 'ARCHIVED_AUDIT_ONLY'
            else 'UNPROVABLE' end,
          'signed_net_hours',jsonb_build_object('hours_day',net_day,'hours_night',net_night,
            'hours_sat',net_sat,'hours_sun',net_sun,'hours_bh',net_bh,
            'total_hours',net_day+net_night+net_sat+net_sun+net_bh),
          'pending_invoice',pending_invoice,'paid',paid,'economic_member_duplicate',economic_member_duplicate
        ) order by role) role_evidence
      from role_state group by correction_id
    )
    select
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',g.correction_id,'state',case
          when g.active_duplicate or g.economic_member_duplicate then 'UNPROVABLE'
          when g.effective_roles=2 then 'FULLY_INVOICED'
          when g.effective_roles=1 and g.proven_roles=2 then 'PARTIALLY_INVOICED'
          when g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
            and (g.active_role_count>0 or g.missing_operation_role_count=2) then 'MUTABLE'
          when g.effective_roles=0 and g.pending_roles=0 and g.active_role_count=0
            and g.archived_only_role_count=2 then 'ARCHIVED_AUDIT_ONLY'
          else 'UNPROVABLE' end,
          'roles',g.role_evidence) order by g.evidence_at,g.correction_id) from generation_state g),'[]'::jsonb),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=2 and not g.active_duplicate and not g.economic_member_duplicate),array[]::text[]),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=1 and g.proven_roles=2 and not g.active_duplicate and not g.economic_member_duplicate),array[]::text[]),
      coalesce((select array_agg(g.correction_id order by g.evidence_at,g.correction_id) from generation_state g
        where g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
          and not g.active_duplicate and not g.economic_member_duplicate
          and (g.active_role_count>0 or g.missing_operation_role_count=2)),array[]::text[]),
      (select g.correction_id from generation_state g
        where g.effective_roles=0 and g.pending_roles=0 and g.proven_roles=2 and not g.paid
          and not g.active_duplicate and not g.economic_member_duplicate
          and (g.active_role_count>0 or g.missing_operation_role_count=2)
        order by g.evidence_at desc,g.correction_id desc limit 1),
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',r.correction_id,'role',r.role,
        'timesheet_ids',to_jsonb(r.archived_ids)) order by r.correction_id,r.role)
        from role_state r where cardinality(r.archived_ids)>0),'[]'::jsonb),
      coalesce((select jsonb_agg(jsonb_build_object('correction_id',g.correction_id,'reason',case
          when g.active_duplicate then 'ACTIVE_ROLE_DUPLICATE'
          when g.economic_member_duplicate then 'DUPLICATE_EFFECTIVE_ROLE_WITHOUT_REPAIR_LINEAGE'
          else 'ROLE_IDENTITY_UNPROVABLE' end) order by g.evidence_at,g.correction_id)
        from generation_state g where g.active_duplicate or g.economic_member_duplicate
          or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0))),'[]'::jsonb),
      exists(select 1 from generation_state g where g.effective_roles=1 and g.proven_roles=2
        and not g.active_duplicate and not g.economic_member_duplicate),
      -- A fully issued generation legitimately retains TSFIN/invoice locks.
      -- Those locks are immutable history, not active invoice activity.  Keep
      -- blocking only while at least one expected role is not yet effective.
      exists(select 1 from generation_state g where g.pending_roles>0 and g.effective_roles<2),
      exists(select 1 from generation_state g where g.active_duplicate or g.economic_member_duplicate
        or (g.proven_roles<2 and (g.effective_roles>0 or g.pending_roles>0)))
    into v_generation_role_evidence,v_fully_invoiced_generation_ids,v_partial_generation_ids,v_mutable_generation_ids,
      v_mutable_correction_id,v_archived_history_roles,v_role_evidence_conflicts,v_role_partial_invoice_state,
      v_role_active_invoice_activity,v_role_scope_unprovable;

    v_partial_invoice_state:=v_role_partial_invoice_state;
    v_active_invoice_activity:=v_active_invoice_activity or v_role_active_invoice_activity or v_operation_in_progress;
    v_scope_unprovable:=v_scope_unprovable or v_role_scope_unprovable
      or v_operation_evidence_conflict or v_member_role_conflict;

    if v_mutable_correction_id is not null then
      v_repair_identity_mode:=case when exists(select 1 from public.timesheets archived
        where archived.correction_id=v_mutable_correction_id and archived.archived_at_utc is not null
          and archived.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))
        then 'FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED' else 'RETAIN_EXISTING_CORRECTION_ID' end;
    end if;
    v_mutable_member_ids:=array[]::uuid[]; v_mutable_missing_roles:=array[]::text[];
    v_mutable_parent_id:=null; v_m_day:=0; v_m_night:=0; v_m_sat:=0; v_m_sun:=0; v_m_bh:=0;
    v_m_pay:=0; v_m_charge:=0; v_m_margin:=0; v_m_financials_complete:=true; v_paid_mutable_state:=false;
    if v_mutable_correction_id is not null then
      select coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[]),
        (array_agg(t.parent_timesheet_id order by t.created_at,t.timesheet_id))[1],
        coalesce(sum(tf.hours_day),0),coalesce(sum(tf.hours_night),0),coalesce(sum(tf.hours_sat),0),coalesce(sum(tf.hours_sun),0),coalesce(sum(tf.hours_bh),0),
        coalesce(sum(tf.total_pay_ex_vat),0),coalesce(sum(tf.total_charge_ex_vat),0),coalesce(sum(tf.margin_ex_vat),0),
        count(*)=count(tf.id) and bool_and(not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),
        bool_or(tf.paid_at_utc is not null)
      into v_mutable_member_ids,v_mutable_parent_id,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin,v_m_financials_complete,v_paid_mutable_state
      from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REVERSAL');
      end if;
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REPLACEMENT');
      end if;
    end if;
    v_mutable_fingerprint:=encode(digest(convert_to(concat_ws('|','mutable-v1',v_mutable_correction_id,v_mutable_member_ids::text,v_mutable_missing_roles::text,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin),'UTF8'),'sha256'),'hex');

    -- A schedule is valid only when it belongs to the exact terminal positive
    -- member.  An older positive may have the same buckets while representing
    -- different work times, so mere candidate presence or bucket equality is
    -- never provenance authority.
    if cardinality(v_fully_invoiced_generation_ids)>0 then
      v_terminal_generation_id:=v_fully_invoiced_generation_ids[cardinality(v_fully_invoiced_generation_ids)];
      select count(distinct (member->>'timesheet_id')::uuid),
        (array_agg(distinct (member->>'timesheet_id')::uuid order by (member->>'timesheet_id')::uuid))[1]
      into v_terminal_positive_member_count,v_terminal_positive_timesheet_id
      from jsonb_array_elements(v_member_role_map) member
      where member->>'correction_id'=v_terminal_generation_id
        and member->>'correction_kind'='CHANGED_HOURS_REPLACEMENT';
      if v_terminal_positive_member_count<>1 then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POSITIVE_MEMBER_UNPROVABLE';
      end if;
    elsif (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 then
      v_terminal_positive_timesheet_id:=v_source_timesheet_id;
      v_terminal_positive_member_count:=case when v_source_timesheet_id is null then 0 else 1 end;
    end if;

    if v_terminal_positive_timesheet_id is not null then
      select count(*)::integer,
        count(distinct candidate->'schedule_json'::text)::integer,
        count(distinct coalesce(candidate->>'policy_fingerprint','<NULL>'))::integer
      into v_terminal_frozen_candidate_count,v_terminal_frozen_schedule_variant_count,
        v_terminal_frozen_policy_variant_count
      from jsonb_array_elements(v_schedule_candidates) candidate
      where candidate->>'timesheet_id'=v_terminal_positive_timesheet_id::text
        and (v_terminal_generation_id is null or (
          candidate->>'correction_id'=v_terminal_generation_id
          and candidate->>'correction_kind'='CHANGED_HOURS_REPLACEMENT'));

      select candidate
      into v_terminal_frozen_candidate
      from jsonb_array_elements(v_schedule_candidates) candidate
      where candidate->>'timesheet_id'=v_terminal_positive_timesheet_id::text
        and (v_terminal_generation_id is null or (
          candidate->>'correction_id'=v_terminal_generation_id
          and candidate->>'correction_kind'='CHANGED_HOURS_REPLACEMENT'))
      order by candidate->>'invoice_line_id'
      limit 1;
    end if;

    if v_terminal_generation_id is not null and v_terminal_positive_timesheet_id is not null then
      select unit
      into v_terminal_operation
      from jsonb_array_elements(v_operation_evidence) unit
      where unit->>'correction_id'=v_terminal_generation_id
        and unit->>'replacement_timesheet_id'=v_terminal_positive_timesheet_id::text
      order by (unit->>'evidence_at')::timestamptz desc,unit->>'operation_id' desc
      limit 1;
    end if;

    v_candidate_schedule:=coalesce(v_terminal_frozen_candidate->'schedule_json','[]'::jsonb);
    v_candidate_hours:=coalesce(v_terminal_frozen_candidate->'hours','{}'::jsonb);
    v_candidate_policy_fingerprint:=nullif(v_terminal_frozen_candidate->>'policy_fingerprint','');
    v_terminal_operation_schedule:=coalesce(v_terminal_operation->'A_schedule_json','[]'::jsonb);
    v_terminal_operation_hours:=coalesce(v_terminal_operation->'A_hours','{}'::jsonb);
    v_terminal_operation_policy_fingerprint:=nullif(v_terminal_operation->>'policy_envelope_fingerprint','');
    v_terminal_operation_id:=case when coalesce(v_terminal_operation->>'operation_id','')~*v_uuid_re
      then (v_terminal_operation->>'operation_id')::uuid end;

    -- Historical request envelopes created before bucketed A-hours were
    -- populated can legitimately contain total_hours with zeroed buckets.
    -- Recover the buckets only from the exact terminal replacement member:
    -- its validated completed-operation schedule plus its matching frozen
    -- correction policy.  Never borrow a surviving older positive schedule.
    if v_terminal_positive_timesheet_id is not null and v_terminal_operation is not null then
      select tf.policy_snapshot_json,
        coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
      into v_terminal_policy_snapshot,v_terminal_policy_snapshot_fingerprint
      from public.timesheets_financials tf
      where tf.timesheet_id=v_terminal_positive_timesheet_id
        and tf.is_current
        and jsonb_typeof(tf.policy_snapshot_json->'correction_financials_policy_envelope')='object'
        and coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
          tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
          =v_terminal_operation_policy_fingerprint
        and tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
          =v_terminal_operation_policy_fingerprint
        -- jsonb considers numerically equivalent values (for example 1 and
        -- 1.0) equal even though their text encodings hash differently.  The
        -- completed operation envelope was already independently re-attested;
        -- require the terminal TSFIN copy to be semantically identical and to
        -- carry that exact validated fingerprint.
        and tf.policy_snapshot_json->'correction_financials_policy_envelope'
          =v_terminal_operation->'policy_envelope'
      order by tf.computed_at_utc desc nulls last,tf.id desc
      limit 1;

      if v_terminal_policy_snapshot_fingerprint=v_terminal_operation_policy_fingerprint
        and jsonb_typeof(v_terminal_operation_schedule)='array'
        and jsonb_array_length(v_terminal_operation_schedule)=1
        and nullif(coalesce((v_terminal_operation_schedule->0)->>'start_utc',
          (v_terminal_operation_schedule->0)->>'start'),'') is not null
        and nullif(coalesce((v_terminal_operation_schedule->0)->>'end_utc',
          (v_terminal_operation_schedule->0)->>'end'),'') is not null then
        begin
          select bucket.hours_day,bucket.hours_night,bucket.hours_sat,bucket.hours_sun,
            bucket.hours_bh,bucket.total_hours
          into v_terminal_derived_day,v_terminal_derived_night,v_terminal_derived_sat,
            v_terminal_derived_sun,v_terminal_derived_bh,v_terminal_derived_total
          from public._wkimp_bucket_hours_from_policy(
            v_terminal_policy_snapshot,
            coalesce((v_terminal_operation_schedule->0)->>'start_utc',
              (v_terminal_operation_schedule->0)->>'start')::timestamptz,
            coalesce((v_terminal_operation_schedule->0)->>'end_utc',
              (v_terminal_operation_schedule->0)->>'end')::timestamptz,
            coalesce(nullif(coalesce((v_terminal_operation_schedule->0)->>'break_mins',
              (v_terminal_operation_schedule->0)->>'break_minutes'),'')::integer,0)
          ) bucket;
          if v_terminal_derived_total=
            coalesce((v_terminal_operation_hours->>'total_hours')::numeric,v_terminal_derived_total) then
            v_terminal_operation_hours:=jsonb_build_object(
              'hours_day',v_terminal_derived_day,'hours_night',v_terminal_derived_night,
              'hours_sat',v_terminal_derived_sat,'hours_sun',v_terminal_derived_sun,
              'hours_bh',v_terminal_derived_bh,'total_hours',v_terminal_derived_total);
          end if;
        exception when others then
          v_b_standard_schedule_authority_diagnostic:='TERMINAL_OPERATION_SCHEDULE_BUCKET_DERIVATION_FAILED';
        end;
      end if;
    end if;

    v_terminal_frozen_matches_b:=v_terminal_frozen_candidate_count>0
      and jsonb_array_length(v_candidate_schedule)=1
      and coalesce((v_candidate_hours->>'hours_day')::numeric,0)=v_b_day
      and coalesce((v_candidate_hours->>'hours_night')::numeric,0)=v_b_night
      and coalesce((v_candidate_hours->>'hours_sat')::numeric,0)=v_b_sat
      and coalesce((v_candidate_hours->>'hours_sun')::numeric,0)=v_b_sun
      and coalesce((v_candidate_hours->>'hours_bh')::numeric,0)=v_b_bh;
    v_terminal_operation_matches_b:=v_terminal_operation is not null
      and jsonb_typeof(v_terminal_operation_schedule)='array'
      and jsonb_array_length(v_terminal_operation_schedule)=1
      and coalesce((v_terminal_operation_hours->>'hours_day')::numeric,0)=v_b_day
      and coalesce((v_terminal_operation_hours->>'hours_night')::numeric,0)=v_b_night
      and coalesce((v_terminal_operation_hours->>'hours_sat')::numeric,0)=v_b_sat
      and coalesce((v_terminal_operation_hours->>'hours_sun')::numeric,0)=v_b_sun
      and coalesce((v_terminal_operation_hours->>'hours_bh')::numeric,0)=v_b_bh;

    v_terminal_schedule_authority_conflict:=v_terminal_frozen_schedule_variant_count>1
      or (v_terminal_frozen_candidate_count>0 and v_terminal_operation is not null and (
        not v_terminal_frozen_matches_b or not v_terminal_operation_matches_b
        or jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce((v_candidate_schedule->0)->>'date',(v_candidate_schedule->0)->>'work_date'),
          'start_utc',coalesce((v_candidate_schedule->0)->>'start_utc',(v_candidate_schedule->0)->>'start'),
          'end_utc',coalesce((v_candidate_schedule->0)->>'end_utc',(v_candidate_schedule->0)->>'end'),
          'break_mins',coalesce((v_candidate_schedule->0)->>'break_mins',(v_candidate_schedule->0)->>'break_minutes','0'),
          'shift_id',(v_candidate_schedule->0)->>'shift_id',
          'external_row_key',(v_candidate_schedule->0)->>'external_row_key'))
          is distinct from
        jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce((v_terminal_operation_schedule->0)->>'date',(v_terminal_operation_schedule->0)->>'work_date'),
          'start_utc',coalesce((v_terminal_operation_schedule->0)->>'start_utc',(v_terminal_operation_schedule->0)->>'start'),
          'end_utc',coalesce((v_terminal_operation_schedule->0)->>'end_utc',(v_terminal_operation_schedule->0)->>'end'),
          'break_mins',coalesce((v_terminal_operation_schedule->0)->>'break_mins',(v_terminal_operation_schedule->0)->>'break_minutes','0'),
          'shift_id',(v_terminal_operation_schedule->0)->>'shift_id',
          'external_row_key',(v_terminal_operation_schedule->0)->>'external_row_key'))));
    v_terminal_policy_authority_conflict:=v_terminal_frozen_policy_variant_count>1
      or (v_terminal_frozen_candidate_count>0 and v_terminal_operation is not null
        and v_candidate_policy_fingerprint is distinct from v_terminal_operation_policy_fingerprint);

    if v_terminal_schedule_authority_conflict then
      v_scope_unprovable:=true;
      v_b_standard_schedule_authority_diagnostic:='TERMINAL_SCHEDULE_AUTHORITY_CONFLICT';
    elsif v_terminal_policy_authority_conflict then
      v_scope_unprovable:=true;
      v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_CONFLICT';
    elsif v_terminal_frozen_matches_b then
      if v_candidate_policy_fingerprint is null then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_MISSING';
      else
        v_b_schedule:=v_candidate_schedule;
        v_b_policy_fingerprint:=v_candidate_policy_fingerprint;
        v_b_standard_schedule_authority:=case when v_terminal_generation_id is null
          then 'ORIGINAL_SOURCE_FROZEN_SEGMENT' else 'TERMINAL_REPLACEMENT_FROZEN_SEGMENT' end;
        v_b_standard_schedule_authority_timesheet_id:=v_terminal_positive_timesheet_id;
        v_b_standard_schedule_authority_correction_id:=v_terminal_generation_id;
        v_b_standard_schedule_authority_operation_id:=v_terminal_operation_id;
        v_b_standard_schedule_authority_policy_fingerprint:=v_candidate_policy_fingerprint;
      end if;
    elsif v_terminal_operation_matches_b then
      if v_terminal_operation_policy_fingerprint is null then
        v_scope_unprovable:=true;
        v_b_standard_schedule_authority_diagnostic:='TERMINAL_POLICY_AUTHORITY_MISSING';
      else
        v_b_schedule:=v_terminal_operation_schedule;
        v_b_policy_fingerprint:=v_terminal_operation_policy_fingerprint;
        v_b_standard_schedule_authority:='TERMINAL_COMPLETED_OPERATION_A_SCHEDULE';
        v_b_standard_schedule_authority_timesheet_id:=v_terminal_positive_timesheet_id;
        v_b_standard_schedule_authority_correction_id:=v_terminal_generation_id;
        v_b_standard_schedule_authority_operation_id:=v_terminal_operation_id;
        v_b_standard_schedule_authority_policy_fingerprint:=v_terminal_operation_policy_fingerprint;
      end if;
    end if;

    v_b_hours_zero:=v_b_day=0 and v_b_night=0 and v_b_sat=0 and v_b_sun=0 and v_b_bh=0;
    v_b_money_zero:=round(v_b_pay,2)=0 and round(v_b_charge,2)=0 and round(v_b_margin,2)=0;
    if v_b_hours_zero and v_b_money_zero then
      v_b_schedule:='[]'::jsonb;
      v_b_policy_fingerprint:=null;
      v_b_standard_schedule_authority:='NONE';
      v_b_standard_schedule_authority_timesheet_id:=null;
      v_b_standard_schedule_authority_correction_id:=null;
      v_b_standard_schedule_authority_operation_id:=null;
      v_b_standard_schedule_authority_policy_fingerprint:=null;
    end if;
    v_b_standard_representable:=(v_b_hours_zero and v_b_money_zero)
      or (v_b_day>=0 and v_b_night>=0 and v_b_sat>=0 and v_b_sun>=0 and v_b_bh>=0
        and v_b_standard_schedule_authority<>'NONE' and jsonb_array_length(v_b_schedule)=1);
    v_b_standard_schedule_authority_fingerprint:=encode(digest(convert_to(jsonb_build_object(
      'contract','B-standard-schedule-authority-v1','source_scope_fingerprint',v_scope_fingerprint,
      'authority',v_b_standard_schedule_authority,'timesheet_id',v_b_standard_schedule_authority_timesheet_id,
      'correction_id',v_b_standard_schedule_authority_correction_id,
      'operation_id',v_b_standard_schedule_authority_operation_id,
      'schedule_json',v_b_schedule,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,
        'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'policy_fingerprint',v_b_standard_schedule_authority_policy_fingerprint)::text,'UTF8'),'sha256'),'hex');

    v_role_evidence_fingerprint:=encode(digest(convert_to(concat_ws('|','role-evidence-v4',
      v_operation_evidence::text,v_member_supersession_map::text,v_member_role_map::text,v_generation_role_evidence::text,
      v_archived_history_roles::text,v_role_evidence_conflicts::text,v_b_standard_schedule_authority_fingerprint),'UTF8'),'sha256'),'hex');
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v4',v_source_identity,
      v_line_evidence::text,v_ignored_nonhours_line_ids::text,v_role_evidence_fingerprint,
      v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin,
      v_b_standard_schedule_authority_fingerprint),'UTF8'),'sha256'),'hex');

    if v_mutable_correction_id is not null then
      v_reversal_repair_required:=not exists(select 1 from public.timesheets t
        where t.correction_id=v_mutable_correction_id and t.correction_kind='CHANGED_HOURS_REVERSAL'
          and t.is_current and t.archived_at_utc is null
          and t.actual_schedule_json is not distinct from v_b_schedule);
      v_replacement_repair_required:=not exists(select 1 from public.timesheets t
        where t.correction_id=v_mutable_correction_id and t.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and t.is_current and t.archived_at_utc is null
          and t.actual_schedule_json is not distinct from v_a_schedule);
    end if;

    v_effective_zero:=v_b_hours_zero and v_b_money_zero;
    v_source_protection:=public._import_review_timesheet_protection_core_v1(v_source_timesheet_id);
    select count(*)::integer
    into v_current_source_count
    from public.timesheets t
    where t.timesheet_id=v_source_timesheet_id and t.is_current and t.archived_at_utc is null
      and coalesce(t.correction_kind::text,'') not in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
      and jsonb_typeof(t.actual_schedule_json)='array'
      and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
        'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key));
    v_current_source_invoice_lined:=exists(select 1 from public.invoice_lines il
      where il.timesheet_id=v_source_timesheet_id
        or il.meta_json->>'timesheet_id'=v_source_timesheet_id::text);
    select coalesce(bool_or(tf.paid_at_utc is not null),false),
      coalesce(bool_and(tf.locked_by_invoice_id is null),false),
      count(*)=1 and coalesce(bool_and(not coalesce(tf.is_stale,true)
        and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),false),
      count(*)=1 and coalesce(bool_and(not exists(
        select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
        where nullif(seg->>'invoice_locked_invoice_id','') is not null)),false)
    into v_current_source_paid,v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked
    from public.timesheets_financials tf
    where tf.timesheet_id=v_source_timesheet_id and tf.is_current
      and tf.candidate_id=v_candidate_id;
    select count(*)=1 and bool_and(upper(coalesce(cw.status::text,'')) not in ('INVOICED','CANCELLED'))
    into v_current_source_contract_week_safe
    from public.contract_weeks cw
    where cw.timesheet_id=v_source_timesheet_id and cw.contract_id=v_contract_id
      and cw.week_ending_date=v_week_ending_date;
    v_current_source_contract_week_safe:=coalesce(v_current_source_contract_week_safe,false);
    v_current_source_invoice_operation_clear:=not exists(
      select 1
      from public.invoice_lines il
      join public.invoices i on i.id=il.invoice_id
      where (il.timesheet_id=v_source_timesheet_id or il.meta_json->>'timesheet_id'=v_source_timesheet_id::text)
        and (i.active_document_operation_id is not null or i.active_issue_operation_id is not null
          or upper(coalesce(i.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED'))
    );
    v_current_source_safe:=v_current_source_count=1
      and not v_current_source_invoice_lined
      and not v_current_source_paid
      and v_current_source_unlocked and v_current_source_fresh and v_current_source_segment_unlocked
      and v_current_source_contract_week_safe and v_current_source_invoice_operation_clear
      and not coalesce((v_source_protection->>'paid')::boolean,false)
      and not coalesce((v_source_protection->>'invoice_locked')::boolean,false)
      and not coalesce((v_source_protection->>'active_pay_draft')::boolean,false);
    v_current_source_safety_reason:=case
      when v_current_source_safe then 'SAFE_CURRENT_ORDINARY_SOURCE'
      when v_current_source_paid and v_current_source_invoice_lined then 'CURRENT_SOURCE_PAID_AND_INVOICE_LINED'
      when v_current_source_invoice_lined then 'CURRENT_SOURCE_INVOICE_LINED_AFTER_EFFECTIVE_ZERO'
      when v_current_source_paid then 'CURRENT_SOURCE_PAID_AFTER_EFFECTIVE_ZERO'
      when v_current_source_count<>1 then 'NO_EXACT_CURRENT_ORDINARY_SOURCE'
      when not v_current_source_unlocked then 'CURRENT_SOURCE_INVOICE_LOCKED'
      when not v_current_source_fresh then 'CURRENT_SOURCE_TSFIN_NOT_FRESH'
      when not v_current_source_segment_unlocked then 'CURRENT_SOURCE_SEGMENT_LOCKED'
      when not v_current_source_invoice_operation_clear then 'CURRENT_SOURCE_INVOICE_OPERATION_ACTIVE'
      when not v_current_source_contract_week_safe then 'CURRENT_SOURCE_CONTRACT_WEEK_UNSAFE'
      when coalesce((v_source_protection->>'active_pay_draft')::boolean,false) then 'CURRENT_SOURCE_ACTIVE_PAY_DRAFT'
      else 'CURRENT_SOURCE_LIFECYCLE_UNSAFE' end;

    -- The authoritative decision is economic whenever an invoiced position or
    -- a mutable correction generation exists.  The live operational source row
    -- is not the financial authority in that case.  Compare every rate bucket,
    -- using B + M for an active mutable generation and B otherwise.
    v_financial_position_requires_amendment:=case
      when v_mutable_correction_id is not null then
        v_b_day+v_m_day is distinct from coalesce((v_a_hours->>'hours_day')::numeric,0)
        or v_b_night+v_m_night is distinct from coalesce((v_a_hours->>'hours_night')::numeric,0)
        or v_b_sat+v_m_sat is distinct from coalesce((v_a_hours->>'hours_sat')::numeric,0)
        or v_b_sun+v_m_sun is distinct from coalesce((v_a_hours->>'hours_sun')::numeric,0)
        or v_b_bh+v_m_bh is distinct from coalesce((v_a_hours->>'hours_bh')::numeric,0)
      when v_effective_component_count>0 and not v_effective_zero then
        v_b_day is distinct from coalesce((v_a_hours->>'hours_day')::numeric,0)
        or v_b_night is distinct from coalesce((v_a_hours->>'hours_night')::numeric,0)
        or v_b_sat is distinct from coalesce((v_a_hours->>'hours_sat')::numeric,0)
        or v_b_sun is distinct from coalesce((v_a_hours->>'hours_sun')::numeric,0)
        or v_b_bh is distinct from coalesce((v_a_hours->>'hours_bh')::numeric,0)
      else false
    end;

    v_blocking_code:=case
      when v_partial_invoice_state then 'IMPORT_REVIEW_CORRECTION_GENERATION_PARTIALLY_INVOICED'
      when v_active_invoice_activity then 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS'
      when v_credit_ambiguous then 'IMPORT_REVIEW_EFFECTIVE_CREDIT_AMBIGUOUS'
      when v_scope_unprovable or v_stream_conflict then 'IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE'
      when v_paid_mutable_state then 'IMPORT_REVIEW_PAID_MUTABLE_GENERATION_ROLLOVER_UNAVAILABLE'
      when v_b_hours_zero and not v_b_money_zero then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      when v_effective_zero and v_effective_component_count>0 and v_mutable_correction_id is null
        and coalesce((v_a_hours->>'total_hours')::numeric,0)>0 and not v_current_source_safe
        then 'IMPORT_REVIEW_EFFECTIVE_ZERO_NO_ACTIVE_SOURCE'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0 then 'IMPORT_REVIEW_INVOICE_STATE_UNSUPPORTED'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 and not v_b_standard_representable then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      else null end;
    v_reconciliation_fingerprint:=encode(digest(convert_to(concat_ws('|','reconciliation-v4',v_scope_fingerprint,v_operation_ids::text,v_member_supersession_map::text,
      v_effective_fingerprint,v_mutable_fingerprint,v_a_fingerprint,v_blocking_code,v_b_policy_fingerprint,
      v_b_standard_schedule_authority,v_b_standard_schedule_authority_timesheet_id,
      v_b_standard_schedule_authority_correction_id,v_b_standard_schedule_authority_operation_id,
      v_b_standard_schedule_authority_policy_fingerprint,v_b_standard_schedule_authority_fingerprint,
      v_current_source_safe,v_current_source_safety_reason,v_current_source_invoice_lined,v_current_source_paid,
      v_current_source_unlocked,v_current_source_fresh,v_current_source_segment_unlocked,
      v_current_source_contract_week_safe,v_current_source_invoice_operation_clear,v_b_hours_zero,v_b_money_zero,
      v_financial_position_requires_amendment),'UTF8'),'sha256'),'hex');

    source_identity:=v_source_identity;
    balance_json:=jsonb_build_object(
      'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_BALANCE_V1',
      'source_identity',v_source_identity,'source_system',v_source_system,'source_shift_id',v_source_shift_id,
      'external_row_key',v_external_row_key,'source_timesheet_id',v_source_timesheet_id,
      'candidate_id',v_candidate_id,'client_id',v_client_id,'contract_id',v_contract_id,
      'week_ending_date',v_week_ending_date,'invoice_stream',v_invoice_stream,
      'source_scope_fingerprint',v_scope_fingerprint,
      'archived_timesheet_ids',to_jsonb(v_archived_ids),'archived_history_timesheet_ids',to_jsonb(v_archived_ids),
      'archived_history_roles',v_archived_history_roles,'active_timesheet_ids',to_jsonb(v_active_ids),
      'historical_missing_timesheet_ids',to_jsonb(v_missing_ids),
      'effective_invoice_ids',to_jsonb(v_effective_invoice_ids),'effective_invoice_line_ids',to_jsonb(v_effective_line_ids),
      'effective_credit_line_ids',to_jsonb(v_credit_line_ids),'effective_invoice_component_count',v_effective_component_count,
      'effective_hours_component_count',v_effective_component_count,
      'ignored_nonhours_invoice_line_ids',to_jsonb(v_ignored_nonhours_line_ids)
    ) || jsonb_build_object(
      'generation_role_evidence',v_generation_role_evidence,
      'validated_completed_operation_evidence_count',jsonb_array_length(v_operation_evidence),
      'validated_completed_operation_evidence_fingerprint',encode(digest(convert_to(v_operation_evidence::text,'UTF8'),'sha256'),'hex'),
      'correction_member_supersession_lineage',v_member_supersession_map,
      'fully_invoiced_generation_ids',to_jsonb(v_fully_invoiced_generation_ids),
      'partial_generation_ids',to_jsonb(v_partial_generation_ids),
      'mutable_generation_ids',to_jsonb(v_mutable_generation_ids),
      'role_evidence_conflicts',v_role_evidence_conflicts,
      'role_evidence_fingerprint',v_role_evidence_fingerprint,
      'effective_invoice_fingerprint',v_effective_fingerprint,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'effective_hours_net_is_zero',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)=0,
      'effective_money_net_is_zero',v_b_money_zero,
      'effective_position_net_is_zero',v_effective_zero,
      'effective_hours_net_is_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
      'effective_hours_net_is_negative',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)<0,
      'B_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_standard_schedule_json',v_b_schedule,'B_policy_fingerprint',v_b_policy_fingerprint,'B_standard_representable',v_b_standard_representable,
      'B_standard_schedule_authority',v_b_standard_schedule_authority,
      'B_standard_schedule_authority_timesheet_id',v_b_standard_schedule_authority_timesheet_id,
      'B_standard_schedule_authority_correction_id',v_b_standard_schedule_authority_correction_id,
      'B_standard_schedule_authority_operation_id',v_b_standard_schedule_authority_operation_id,
      'B_standard_schedule_authority_policy_fingerprint',v_b_standard_schedule_authority_policy_fingerprint,
      'B_standard_schedule_authority_fingerprint',v_b_standard_schedule_authority_fingerprint,
      'B_standard_schedule_authority_diagnostic',v_b_standard_schedule_authority_diagnostic,
      'active_mutable_generation',v_mutable_correction_id is not null,'active_mutable_member_ids',to_jsonb(v_mutable_member_ids),
      'active_mutable_missing_roles',to_jsonb(v_mutable_missing_roles),'active_mutable_correction_id',v_mutable_correction_id,
      'physically_missing_mutable_roles',to_jsonb(v_mutable_missing_roles),
      'reviewed_existing_correction_id',v_mutable_correction_id,'repair_identity_mode',v_repair_identity_mode,
      'reversal_repair_required',v_reversal_repair_required,'replacement_repair_required',v_replacement_repair_required
    ) || jsonb_build_object(
      'active_mutable_parent_timesheet_id',v_mutable_parent_id,'active_mutable_fingerprint',v_mutable_fingerprint,
      'M_hours',jsonb_build_object('hours_day',v_m_day,'hours_night',v_m_night,'hours_sat',v_m_sat,'hours_sun',v_m_sun,'hours_bh',v_m_bh,'total_hours',v_m_day+v_m_night+v_m_sat+v_m_sun+v_m_bh),
      'M_existing_financials',jsonb_build_object('pay_ex_vat',v_m_pay,'charge_ex_vat',v_m_charge,'margin_ex_vat',v_m_margin),'M_financials_complete',v_m_financials_complete,
      'A_schedule_json',v_a_schedule,'A_hours',v_a_hours,'A_evidence_fingerprint',v_a_fingerprint,
      'partial_invoice_state',v_partial_invoice_state,'active_invoice_activity',v_active_invoice_activity,
      'archived_active_conflict',false,'archived_invoice_conflict',false,
      'paid_mutable_state',v_paid_mutable_state,
      'current_source_safe_for_effective_zero_amendment',v_current_source_safe,
      'effective_zero_source_safety_reason',v_current_source_safety_reason,
      'current_source_invoice_lined',v_current_source_invoice_lined,'current_source_paid',v_current_source_paid,
      'financial_position_requires_amendment',v_financial_position_requires_amendment,
      'recommended_route_inputs',jsonb_build_object('B_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,
        'has_mutable_generation',v_mutable_correction_id is not null,'source_timesheet_active',v_source_timesheet_id=any(v_active_ids),
        'current_source_safe_for_effective_zero_amendment',v_current_source_safe),
      'blocking_code',v_blocking_code,'reconciliation_fingerprint',v_reconciliation_fingerprint
    );
    return next;
  end loop;
end
$function$;

-- public.invoice_apply_edits(p_invoice_id uuid, p_payload jsonb, p_actor_user_id uuid) pre-install MD5 3564c3c9421c96f36fab3f31c7c768cb
CREATE OR REPLACE FUNCTION public.invoice_apply_edits(p_invoice_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;


-- =====================================================
-- DEBUG (invoice_debug): single audit row per RPC call
-- =====================================================
v_invoice_debug boolean := false;
v_dbg_started_at timestamptz := now();
v_dbg_steps jsonb := '[]'::jsonb;
v_dbg_sqlstate text := null;
v_dbg_error text := null;
v_dbg_stats jsonb := '{}'::jsonb;


v_dbg_lines_deleted int := 0;
v_dbg_timesheets_unlocked int := 0;
v_dbg_seg_add_refs int := 0;
v_dbg_seg_remove_refs int := 0;
v_dbg_seg_tsfins int := 0;
v_dbg_seg_timesheets_rebuilt int := 0;
v_dbg_seg_timesheets_removed int := 0;
v_dbg_add_timesheets_found int := 0;
v_dbg_add_timesheets_skipped int := 0;

v_rc int := 0;

  v_inv record;
  v_week_start date;
  v_week_end date;

  v_remove_ids uuid[];
  v_add_ts_ids uuid[];

-- segment move payload (explicit segment add/remove)
v_remove_seg_refs jsonb;
v_add_seg_refs jsonb;
v_has_seg_ops boolean := false;
v_refresh_hr_cache boolean := false;
v_seg_tsfin_ids uuid[] := array[]::uuid[];
v_seg_ts_ids uuid[] := array[]::uuid[];
v_seg_refs_to_lock jsonb := '[]'::jsonb;
v_ref jsonb;
v_tsfin_id uuid;
v_seg_id text;
v_has_additional boolean;
v_has_expense_or_mileage boolean;

  -- reference updates (refs-to-issue)
  v_reference_updates jsonb;
  v_refupd jsonb;
  v_refupd_ts_id uuid;
  v_refupd_count int := 0;
  v_refupd_applied int := 0;
  v_refupd_set_refnum boolean;
  v_refupd_set_dayrefs boolean;
  v_refupd_set_sched boolean;
  v_refupd_dayrefs jsonb;
  v_refupd_sched jsonb;
  v_refupd_refnum text;

  -- timesheet hospital / ward source edits
  v_location_updates jsonb;
  v_location_update jsonb;
  v_location_ts_id uuid;
  v_location_count int := 0;
  v_location_applied int := 0;
  v_location_set_hospital boolean;
  v_location_set_ward boolean;
  v_location_hospital_norm text;
  v_location_ward_norm text;
  v_location_ts_ids uuid[] := array[]::uuid[];
  -- Source-edit contract state. Reference and location inputs are merged into
  -- one desired row per timesheet, then written by one statement so the
  -- statement-level invalidation trigger advances each source exactly once.
  v_source_updates_map jsonb := '{}'::jsonb;
  v_source_update jsonb;
  v_source_edit_key text;
  v_source_expected_revision bigint;
  v_source_changed_ts_ids uuid[] := array[]::uuid[];
  v_source_changed_revisions jsonb := '[]'::jsonb;
  v_source_edit_requested boolean := false;
  v_allowed_reference_fields text[]:=array[
    'timesheet_id','expected_document_revision','reference_number',
    'day_references_json','actual_schedule_json'];
  v_allowed_location_fields text[]:=array[
    'timesheet_id','expected_document_revision','hospital_norm','ward_norm'];
  v_allowed_reference_segment_fields text[]:=array[
    'segment_id','date','start_utc','end_utc','start','end','ref_num',
    'source_system'];
  v_source_unsupported_field text;
  v_source_financial_id uuid;
  v_source_mode text;
  v_source_ib jsonb;
  v_source_payload_schedule jsonb;
  v_source_canonical_schedule jsonb;
  v_source_new_segments jsonb;
  v_source_segment_updates jsonb;
  v_source_segment_reference_changed boolean;
  v_source_timesheet_row_changed boolean;
  v_source_match_count integer;
  v_source_match_segment jsonb;
  v_source_payload_segment jsonb;
  v_source_current_segment jsonb;
  v_source_payload_ord bigint;
  v_source_start timestamptz;
  v_source_end timestamptz;
  v_source_marker_rows jsonb:='{}'::jsonb;
  v_source_marker jsonb:='{}'::jsonb;
  v_source_edit_preexisting_preview boolean := false;
  v_source_edit_invoice_replacement_required boolean := false;
  v_pre_edit_invoice_revision bigint;
  v_pre_edit_preview_document_version_id uuid;
  v_pre_edit_active_document_operation_id uuid;
  v_expected_command_count int := 0;
  v_operation_result jsonb;
  v_operation_id uuid;
  v_document_version_id uuid;
  v_command_no int;
  v_command_type text;
  v_operation_status text;
  v_operation_input jsonb;
  v_validated_operations jsonb := '[]'::jsonb;
  v_document_commands jsonb := '[]'::jsonb;
  v_started_operations jsonb := '[]'::jsonb;

  -- reference update side-effects (meta refresh / segment ref sync)
  v_refupd_ts_ids uuid[] := array[]::uuid[];
  v_refupd_ts_ids_distinct uuid[] := array[]::uuid[];
  v_refupd_meta_rows_updated int := 0;
  v_refupd_tsfin_rows_updated int := 0;
  v_refupd_tsfin_segments_updated int := 0;
  v_refupd_invoice_fallback_invalidated boolean := false;

  -- temp vars for reference-derived meta and optional tsfin segment ref sync
  v_ref_ts_id uuid;
  v_ref_ts record;
  v_ref_schedule_refs jsonb := '[]'::jsonb;
  v_ref_schedule_refs_distinct jsonb := '[]'::jsonb;
  v_ref_sched_map jsonb := '{}'::jsonb;
  v_ref_seen_keys jsonb := '{}'::jsonb;
  v_ref_sched_key text;
  v_ref_sched_ref text;
  v_ref_tsfin_id uuid;
  v_ref_ib jsonb;
  v_ref_new_segments jsonb := '[]'::jsonb;
  v_ref_seg_obj jsonb;
  v_ref_seg_start text;
  v_ref_seg_end text;
    v_ref_seg_id text;
v_ref_seg_cur_ref text;
  v_ref_seg_new_ref text;
  v_ref_seg_has_update boolean := false;
  v_ref_seg_updates_this_ts int := 0;
  v_ref_seg_matches_this_ts int := 0;

  -- audit (history) accumulators (NOT debug-only)
  v_hist_adj jsonb := '[]'::jsonb;
  v_hist_seg_add jsonb := '[]'::jsonb;
  v_hist_seg_remove jsonb := '[]'::jsonb;
  v_hist_lines_removed jsonb := '[]'::jsonb;
  v_hist_add_ts jsonb := '[]'::jsonb;

  -- contract week status touch set
  v_cw_ts_ids uuid[] := array[]::uuid[];

  v_ts_ids_touched uuid[] := array[]::uuid[];
  -- FIX: timesheets fully removed from this invoice (no remaining invoice_lines) should be unlocked
  v_ts_ids_fully_removed uuid[] := array[]::uuid[];

  v_vat_chargeable boolean := true;
  v_vat_rate numeric := 0;

  -- adjustments
  adj jsonb;
  v_adj_token text;
  v_adj_desc text;
  v_adj_ex numeric;
  v_adj_vat numeric;
  v_adj_inc numeric;
  v_adj_source_key text;
  v_meta jsonb;

  -- timesheet loop
  tsid uuid;
  snap record;
  ts record;
  pc record;
  contract_id uuid;
  v_client_daily_calc boolean := false;
  v_contract_override boolean := false;
  v_contract_daily_calc boolean;
  v_contract_bucket_labels jsonb;
  c_daily_calc boolean := false;
  c_bucket_labels jsonb := null;
  c_role text := null;
  c_display_site text := null;
  c_ward_hint text := null;

  -- segment filtering
  v_seg jsonb;
  segments jsonb := '[]'::jsonb;
  seg_target date;
  seg_date text;
  natural_start date;
  seg_locked text;
  seg_ref text;

  -- aggregation for weekly line
  h_day numeric; h_night numeric; h_sat numeric; h_sun numeric; h_bh numeric;
  pay_ex numeric; chg_ex numeric; margin_ex numeric;
  vat_amt numeric; inc_amt numeric;
  line_desc text;
  v_source_key text;
  v_exact_timesheet_document_r2_key text;

  -- daily aggregation record
  r_day record;

  -- segment refs for locking
  seg_refs jsonb := '[]'::jsonb;

  -- additional units
  kv record;
  ex jsonb;
  code text;
  unit_count numeric;
  bucket_name text;
  unit_name text;

  -- expenses notes
  v_note_travel text;
  v_note_accom text;
  v_note_other text;

  -- recompute totals
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  -- header meta counters (keep header_snapshot_json.meta in sync with current invoice state)
  v_hdr_ts_count_lines int := 0;
  v_hdr_ts_count_seglocks int := 0;
  v_hdr_seg_locked_count int := 0;
  v_hdr_meta_timesheet_count int := 0;
  v_hdr_meta_segment_count int := 0;

  v_manifest jsonb;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_actor_role text;
begin

  if not v_service
     and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',
      message='AUTHENTICATED_ACTOR_MISMATCH';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_actor_role
  from public.tms_users u
  where u.id=p_actor_user_id and u.is_active;
  if(not found or v_actor_role<>'admin') and not v_service then
    raise exception using errcode='42501',
      message='INVOICE_ADMINISTRATOR_PERMISSION_REQUIRED';
  end if;

  perform public._ctms_assert_invoice_mutable_draft_v1(
    p_invoice_id,'INVOICE_APPLY_EDITS',true
  );
  if public._ctms_invoice_payload_has_financial_edit_v1(p_payload)
     and exists (
       select 1 from public.invoice_lines il
       where il.invoice_id=p_invoice_id and il.timesheet_id is not null
         and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
           ->>'is_import_authoritative_correction')::boolean,false)
     ) then
    raise exception 'IMPORT_CORRECTION_INVOICE_FINANCIAL_EDIT_FORBIDDEN'
      using errcode='P0001',detail=jsonb_build_object('invoice_id',p_invoice_id)::text;
  end if;
-- Load invoice_debug flag (safe even if column not yet present)
begin
  select coalesce(sd.invoice_debug, false)
  into v_invoice_debug
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;
exception when undefined_column then
  v_invoice_debug := false;
end;

if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'at_utc', public._inv_iso_utc(v_dbg_started_at),
      'invoice_id', p_invoice_id::text
    )
  );
end if;

  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  for update
  limit 1;

  if not found then
    raise exception 'Invoice not found';
  end if;

  -- Editable gate: DRAFT/ON_HOLD and unpaid
  if v_inv.status::text not in ('DRAFT','ON_HOLD') then
    raise exception 'Invoice is not editable (status=%)', v_inv.status::text;
  end if;

  if v_inv.paid_at_utc is not null then
    raise exception 'Invoice is not editable (already paid)';
  end if;



if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_loaded',
      'status', coalesce(v_inv.status::text,''),
      'paid_at_utc', case when v_inv.paid_at_utc is null then null else public._inv_iso_utc(v_inv.paid_at_utc) end
    )
  );
end if;

  -- Require invoice_week_start in header_snapshot_json.meta
  if v_inv.header_snapshot_json is null
     or btrim(coalesce(v_inv.header_snapshot_json #>> '{meta,invoice_week_start}','')) = ''
     or (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}') !~ '^\d{4}-\d{2}-\d{2}$'
  then
    raise exception 'Invoice header_snapshot_json.meta.invoice_week_start is required for edits';
  end if;

  v_week_start := (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}')::date;
  v_week_end := (v_week_start + interval '6 days')::date;


if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_week_loaded',
      'invoice_week_start', v_week_start::text,
      'invoice_week_end', v_week_end::text
    )
  );
end if;

  -- Load effective client setting daily_calc_of_invoices (used when contract.overrideclientsettings=false)
  begin
    select coalesce(cs0.daily_calc_of_invoices,false)
    into v_client_daily_calc
    from public.client_settings cs0
    where cs0.client_id = v_inv.client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;
  exception when others then
    v_client_daily_calc := false;
  end;

  -- VAT settings from invoice snapshot
  if jsonb_typeof(v_inv.header_snapshot_json->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_inv.header_snapshot_json->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := true;
  end if;

  if (v_inv.header_snapshot_json ? 'applied_vat_rate_pct') then
    begin
      v_vat_rate := (v_inv.header_snapshot_json->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_vat_rate := 0;
    end;
  else
    v_vat_rate := 0;
  end if;

  if v_vat_chargeable = false then
    v_vat_rate := 0;
  end if;

  -- Parse payload arrays
  v_remove_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_invoice_line_ids') then
    select array_agg((x)::uuid)
    into v_remove_ids
    from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  v_add_ts_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_timesheet_ids') then
    select array_agg((x)::uuid)
    into v_add_ts_ids
    from jsonb_array_elements_text(coalesce(p_payload->'add_timesheet_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;


-- Parse segment move payloads (tsfin_id + segment_id)
v_remove_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_segment_refs') then
  v_remove_seg_refs := coalesce(p_payload->'remove_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_remove_seg_refs) <> 'array' then
    v_remove_seg_refs := '[]'::jsonb;
  end if;
end if;

v_add_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_segment_refs') then
  v_add_seg_refs := coalesce(p_payload->'add_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_add_seg_refs) <> 'array' then
    v_add_seg_refs := '[]'::jsonb;
  end if;
end if;



-- Parse and strictly validate source edits. The expected source revision is
-- mandatory because values displayed by an older modal must never overwrite a
-- newer timesheet revision.
v_reference_updates := null;
if p_payload is not null and jsonb_typeof(p_payload)='object'
   and p_payload ? 'reference_updates' then
  v_reference_updates:=p_payload->'reference_updates';
  if jsonb_typeof(v_reference_updates)<>'array' then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
      detail=jsonb_build_object('field','reference_updates','reason','ARRAY_REQUIRED')::text;
  end if;
  v_refupd_count:=jsonb_array_length(v_reference_updates);
  for v_refupd in select value from jsonb_array_elements(v_reference_updates) value loop
    if v_refupd is not null and jsonb_typeof(v_refupd)='object' then
      select min(field_name) into v_source_unsupported_field
      from jsonb_object_keys(v_refupd) field_name
      where not (field_name=any(v_allowed_reference_fields));
      if v_source_unsupported_field is not null then
        raise exception using errcode='22023',
          message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
          detail=jsonb_build_object(
            'field','reference_updates','reason','UNSUPPORTED_FIELD',
            'unsupported_field',v_source_unsupported_field)::text;
      end if;
    end if;
    if v_refupd is null or jsonb_typeof(v_refupd)<>'object'
       or nullif(btrim(coalesce(v_refupd->>'timesheet_id','')),'') is null
       or coalesce(v_refupd->>'timesheet_id','') !~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
       or coalesce(v_refupd->>'expected_document_revision','') !~ '^[1-9][0-9]*$'
       or not (
         v_refupd ? 'reference_number'
         or v_refupd ? 'day_references_json'
         or v_refupd ? 'actual_schedule_json'
       )
       or (
         v_refupd ? 'reference_number'
         and jsonb_typeof(v_refupd->'reference_number') not in('string','null')
       )
       or (
         v_refupd ? 'day_references_json'
         and jsonb_typeof(v_refupd->'day_references_json') not in('object','null')
       )
       or (
         v_refupd ? 'actual_schedule_json'
         and jsonb_typeof(v_refupd->'actual_schedule_json') not in('array','null')
       )
    then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
        detail=jsonb_build_object('field','reference_updates','reason','INVALID_ITEM')::text;
    end if;
    v_source_edit_key:=lower(v_refupd->>'timesheet_id');
    if v_source_updates_map ? v_source_edit_key then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_DUPLICATE_TIMESHEET',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'field','reference_updates')::text;
    end if;
    v_source_updates_map:=jsonb_set(
      v_source_updates_map,array[v_source_edit_key],
      jsonb_build_object(
        'timesheet_id',v_source_edit_key,
        'expected_document_revision',(v_refupd->>'expected_document_revision')::bigint,
        'has_reference_number',v_refupd ? 'reference_number',
        'reference_number',case when v_refupd ? 'reference_number'
          then to_jsonb(nullif(btrim(coalesce(v_refupd->>'reference_number','')),''))
          else 'null'::jsonb end,
        'has_day_references_json',v_refupd ? 'day_references_json',
        'day_references_json',case
          when not (v_refupd ? 'day_references_json') then 'null'::jsonb
          when jsonb_typeof(v_refupd->'day_references_json')='null' then 'null'::jsonb
          else v_refupd->'day_references_json' end,
        'has_actual_schedule_json',v_refupd ? 'actual_schedule_json',
        'actual_schedule_json',case
          when not (v_refupd ? 'actual_schedule_json') then 'null'::jsonb
          when jsonb_typeof(v_refupd->'actual_schedule_json')='null' then 'null'::jsonb
          else v_refupd->'actual_schedule_json' end,
        'has_hospital_norm',false,
        'has_ward_norm',false),
      true);
  end loop;
end if;

-- Hospital/ward edits merge into the same desired source row. A source may
-- appear once in each array, but both items must carry the same revision.
v_location_updates:=null;
if p_payload is not null and jsonb_typeof(p_payload)='object'
   and p_payload ? 'timesheet_location_updates' then
  v_location_updates:=p_payload->'timesheet_location_updates';
  if jsonb_typeof(v_location_updates)<>'array' then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
      detail=jsonb_build_object('field','timesheet_location_updates','reason','ARRAY_REQUIRED')::text;
  end if;
  v_location_count:=jsonb_array_length(v_location_updates);
  for v_location_update in select value from jsonb_array_elements(v_location_updates) value loop
    if v_location_update is not null and jsonb_typeof(v_location_update)='object' then
      select min(field_name) into v_source_unsupported_field
      from jsonb_object_keys(v_location_update) field_name
      where not (field_name=any(v_allowed_location_fields));
      if v_source_unsupported_field is not null then
        raise exception using errcode='22023',
          message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
          detail=jsonb_build_object(
            'field','timesheet_location_updates','reason','UNSUPPORTED_FIELD',
            'unsupported_field',v_source_unsupported_field)::text;
      end if;
    end if;
    if v_location_update is null or jsonb_typeof(v_location_update)<>'object'
       or nullif(btrim(coalesce(v_location_update->>'timesheet_id','')),'') is null
       or coalesce(v_location_update->>'timesheet_id','') !~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
       or coalesce(v_location_update->>'expected_document_revision','') !~ '^[1-9][0-9]*$'
       or not (v_location_update ? 'hospital_norm' or v_location_update ? 'ward_norm')
       or (
         v_location_update ? 'hospital_norm'
         and jsonb_typeof(v_location_update->'hospital_norm') not in('string','null')
       )
       or (
         v_location_update ? 'ward_norm'
         and jsonb_typeof(v_location_update->'ward_norm') not in('string','null')
       )
    then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
        detail=jsonb_build_object('field','timesheet_location_updates','reason','INVALID_ITEM')::text;
    end if;
    v_source_edit_key:=lower(v_location_update->>'timesheet_id');
    v_source_expected_revision:=(v_location_update->>'expected_document_revision')::bigint;
    if v_source_updates_map ? v_source_edit_key
       and (
         coalesce((v_source_updates_map#>>
           array[v_source_edit_key,'has_hospital_norm'])::boolean,false)
         or coalesce((v_source_updates_map#>>
           array[v_source_edit_key,'has_ward_norm'])::boolean,false)
       ) then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_DUPLICATE_TIMESHEET',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'field','timesheet_location_updates')::text;
    end if;
    if v_source_updates_map ? v_source_edit_key
       and (v_source_updates_map#>>array[v_source_edit_key,'expected_document_revision'])::bigint
           is distinct from v_source_expected_revision then
      raise exception using errcode='40001',
        message='INVOICE_SOURCE_EDIT_STALE_REVISION',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'reason','EXPECTED_REVISION_CONFLICT')::text;
    end if;
    v_source_update:=coalesce(v_source_updates_map->v_source_edit_key,
      jsonb_build_object(
        'timesheet_id',v_source_edit_key,
        'expected_document_revision',v_source_expected_revision,
        'has_reference_number',false,
        'has_day_references_json',false,
        'has_actual_schedule_json',false));
    v_source_update:=v_source_update||jsonb_build_object(
      'has_hospital_norm',v_location_update ? 'hospital_norm',
      'hospital_norm',case when v_location_update ? 'hospital_norm'
        then to_jsonb(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'hospital_norm','')),
          '[[:space:]]+',' ','g')))
        else 'null'::jsonb end,
      'has_ward_norm',v_location_update ? 'ward_norm',
      'ward_norm',case when v_location_update ? 'ward_norm'
        then to_jsonb(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'ward_norm','')),
          '[[:space:]]+',' ','g')))
        else 'null'::jsonb end);
    v_source_updates_map:=jsonb_set(v_source_updates_map,array[v_source_edit_key],v_source_update,true);
  end loop;
end if;
v_source_edit_requested:=v_source_updates_map<>'{}'::jsonb;
v_has_seg_ops :=
  (v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0)
  or (v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0);

  v_refresh_hr_cache := (coalesce(array_length(v_add_ts_ids,1),0) > 0) or coalesce(v_has_seg_ops,false);

  if v_invoice_debug then
    v_dbg_stats := v_dbg_stats || jsonb_build_object(
      'remove_invoice_line_ids_count', coalesce(array_length(v_remove_ids,1),0),
      'add_timesheet_ids_count', coalesce(array_length(v_add_ts_ids,1),0),
      'add_adjustments_count', case when p_payload is not null and jsonb_typeof(p_payload)='object' and (p_payload ? 'add_adjustments') and jsonb_typeof(p_payload->'add_adjustments')='array' then jsonb_array_length(p_payload->'add_adjustments') else 0 end,
      'remove_segment_refs_count', case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end,
      'add_segment_refs_count', case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end,
      'timesheet_location_updates_count',v_location_count,
      'has_segment_ops', v_has_seg_ops
    );
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','payload_parsed','stats',v_dbg_stats));
  end if;

  if v_source_edit_requested then
    -- Source edits are a narrower authority than ordinary draft edits.
    if v_inv.issued_at_utc is not null then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_ISSUED';
    end if;
    if v_inv.paid_at_utc is not null then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_PAID';
    end if;
    if v_inv.active_issue_operation_id is not null
       or upper(coalesce(v_inv.issue_state,'')) in
          ('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
       or exists(
         select 1
         from public.invoice_operation_chunks c
         where c.chunk_type='ISSUE_INVOICE'
           and c.entity_type='INVOICE'
           and c.entity_id=p_invoice_id
           and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
       )
    then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_ISSUE_IN_PROGRESS';
    end if;

    v_pre_edit_invoice_revision:=v_inv.document_revision;
    v_pre_edit_preview_document_version_id:=v_inv.preview_document_version_id;
    v_pre_edit_active_document_operation_id:=v_inv.active_document_operation_id;

    -- Only an exact current-revision V8 preview is authoritative. Legacy PDF
    -- summary fields do not independently force or suppress replacement.
    select exists(
      select 1
      from public.invoice_document_versions dv
      left join public.invoice_operations op on op.id=dv.operation_id
      where dv.entity_type='INVOICE'
        and dv.entity_id=p_invoice_id
        and dv.purpose='DRAFT_PREVIEW'
        and dv.source_revision=v_pre_edit_invoice_revision::text
        and (
          (dv.status='READY'
            and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
            and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
            and coalesce(dv.size_bytes,0)>0
            and coalesce(dv.page_count,0)>0)
          or (
            dv.id=v_pre_edit_preview_document_version_id
            and dv.status in(
              'PLANNING','WAITING_FOR_INPUTS','RENDERING',
              'ASSEMBLING','VERIFYING','READY'))
          or (
            dv.operation_id=v_pre_edit_active_document_operation_id
            and op.operation_type='BUILD_DOCUMENT'
            and op.entity_type='INVOICE'
            and op.entity_id=p_invoice_id
            and op.source_revision=v_pre_edit_invoice_revision::text
            and coalesce(op.input_json->>'purpose','')='DRAFT_PREVIEW'
            and op.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
        )
    ) into v_source_edit_preexisting_preview;

    -- Clear any caller-controlled or earlier transaction-local value before
    -- establishing this function's exact-once invalidation marker.
    perform set_config('cloudtms.invoice_source_edit_marker','{}',true);

    -- Lock every target and its current financial snapshot in deterministic
    -- order before validating ownership, revisions or segment identities.
    for v_ref_ts in
      select t.*
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired on true
      where t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      order by t.timesheet_id
      for update of t
    loop
      null;
    end loop;
    for v_ref_ts in
      select tf.id
      from public.timesheets_financials tf
      join jsonb_each(v_source_updates_map) desired
        on tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where tf.is_current
      order by tf.timesheet_id,tf.id
      for update of tf
    loop
      null;
    end loop;

    if (
      select count(*)
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where t.is_current
    )<>(select count(*) from jsonb_each(v_source_updates_map)) then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_SOURCE_NOT_CURRENT';
    end if;

    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      join public.timesheets t
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where t.document_revision is distinct from
        (desired.value->>'expected_document_revision')::bigint
    ) then
      raise exception using errcode='40001',
        message='INVOICE_SOURCE_EDIT_STALE_REVISION';
    end if;

    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where not exists(
        select 1
        from public.invoice_lines owned_line
        where owned_line.invoice_id=p_invoice_id
          and (
            owned_line.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              owned_line.timesheet_id is null
              and coalesce(owned_line.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (owned_line.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
    ) then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_SOURCE_NOT_OWNED';
    end if;

    -- Preserve the established import-authoritative correction lock across
    -- both supported source carriers.
    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where coalesce((
        public._ctms_import_correction_classify_v1(
          (desired.value->>'timesheet_id')::uuid)
          ->>'is_import_authoritative_correction')::boolean,false)
    ) then
      raise exception using errcode='55000',
        message='IMPORT_AUTHORITATIVE_CORRECTION_SOURCE_EDIT_FORBIDDEN';
    end if;

    -- Reject a mixed command that edits a source while removing every carrier
    -- for that source in the same Save.
    if coalesce(array_length(v_remove_ids,1),0)>0 and exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where exists(
        select 1
        from public.invoice_lines carrier
        where carrier.invoice_id=p_invoice_id
          and (
            carrier.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              carrier.timesheet_id is null
              and coalesce(carrier.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (carrier.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
      and not exists(
        select 1
        from public.invoice_lines retained
        where retained.invoice_id=p_invoice_id
          and not (retained.id=any(v_remove_ids))
          and (
            retained.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              retained.timesheet_id is null
              and coalesce(retained.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (retained.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
    ) then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND';
    end if;

    -- Classify each source from the locked current financial snapshot. A
    -- SEGMENTS payload is an editable reference projection only: it can never
    -- replace timesheets.actual_schedule_json or any financial segment object.
    for v_ref_ts in
      select t.*,desired.value desired_value,
        tf.id financial_id,tf.invoice_breakdown_json financial_breakdown
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      left join lateral(
        select f.id,f.invoice_breakdown_json
        from public.timesheets_financials f
        where f.timesheet_id=t.timesheet_id and f.is_current
        order by f.updated_at desc nulls last,f.created_at desc nulls last,f.id desc
        limit 1
      ) tf on true
      where t.is_current
      order by t.timesheet_id
    loop
      v_source_update:=v_ref_ts.desired_value;
      v_source_financial_id:=v_ref_ts.financial_id;
      v_source_ib:=v_ref_ts.financial_breakdown;
      v_source_mode:=upper(coalesce(v_source_ib->>'mode',''));
      v_source_payload_schedule:=v_source_update->'actual_schedule_json';
      v_source_canonical_schedule:=v_ref_ts.actual_schedule_json;
      v_source_new_segments:=null;
      v_source_segment_updates:='[]'::jsonb;
      v_source_segment_reference_changed:=false;

      if (v_source_update->>'has_actual_schedule_json')::boolean then
        if jsonb_typeof(v_source_payload_schedule)<>'array' then
          raise exception using errcode='22023',
            message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
            detail=jsonb_build_object(
              'field','actual_schedule_json','reason','ARRAY_REQUIRED',
              'timesheet_id',v_ref_ts.timesheet_id)::text;
        end if;

        if v_source_mode='SEGMENTS' then
          if v_source_financial_id is null
             or jsonb_typeof(v_source_ib->'segments')<>'array' then
            raise exception using errcode='55000',
              message='INVOICE_SOURCE_EDIT_SOURCE_NOT_CURRENT';
          end if;

          v_ref_seen_keys:='{}'::jsonb;
          for v_source_payload_segment,v_source_payload_ord in
            select value,ord
            from jsonb_array_elements(v_source_payload_schedule)
              with ordinality payload(value,ord)
          loop
            if jsonb_typeof(v_source_payload_segment)<>'object' then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','INVALID_SEGMENT',
                  'ordinal',v_source_payload_ord)::text;
            end if;
            select min(field_name) into v_source_unsupported_field
            from jsonb_object_keys(v_source_payload_segment) field_name
            where not (field_name=any(v_allowed_reference_segment_fields));
            if v_source_unsupported_field is not null then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','UNSUPPORTED_FIELD',
                  'unsupported_field',v_source_unsupported_field,
                  'ordinal',v_source_payload_ord)::text;
            end if;
            if not (v_source_payload_segment ? 'ref_num') then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','REFERENCE_REQUIRED',
                  'ordinal',v_source_payload_ord)::text;
            end if;

            v_ref_seg_id:=nullif(btrim(coalesce(
              v_source_payload_segment->>'segment_id','')),'');
            v_source_match_count:=0;
            v_source_match_segment:=null;
            if v_ref_seg_id is not null then
              select count(*),min(seg.value::text)::jsonb
              into v_source_match_count,v_source_match_segment
              from jsonb_array_elements(v_source_ib->'segments') seg(value)
              where nullif(btrim(coalesce(seg.value->>'segment_id','')),'')=v_ref_seg_id
                and nullif(btrim(coalesce(
                  seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text;
            end if;

            if v_source_match_count=0 then
              begin
                v_source_start:=nullif(btrim(coalesce(
                  v_source_payload_segment->>'start_utc',
                  v_source_payload_segment->>'start','')),'')::timestamptz;
                v_source_end:=nullif(btrim(coalesce(
                  v_source_payload_segment->>'end_utc',
                  v_source_payload_segment->>'end','')),'')::timestamptz;
              exception when others then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                  detail=jsonb_build_object(
                    'field','actual_schedule_json','reason','INVALID_SEGMENT_TIME',
                    'ordinal',v_source_payload_ord)::text;
              end;
              if v_source_start is null or v_source_end is null then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                  detail=jsonb_build_object(
                    'field','actual_schedule_json','reason','SEGMENT_IDENTITY_REQUIRED',
                    'ordinal',v_source_payload_ord)::text;
              end if;
              select count(*),min(seg.value::text)::jsonb
              into v_source_match_count,v_source_match_segment
              from jsonb_array_elements(v_source_ib->'segments') seg(value)
              where nullif(btrim(coalesce(
                    seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
                and nullif(btrim(coalesce(
                    seg.value->>'start_utc',seg.value->>'start','')),'')::timestamptz
                    =v_source_start
                and nullif(btrim(coalesce(
                    seg.value->>'end_utc',seg.value->>'end','')),'')::timestamptz
                    =v_source_end;
            end if;

            if v_source_match_count<>1 or v_source_match_segment is null then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts.timesheet_id,
                  'ordinal',v_source_payload_ord,
                  'match_count',v_source_match_count)::text;
            end if;

            v_ref_sched_key:=coalesce(
              'SID:'||nullif(btrim(coalesce(
                v_source_match_segment->>'segment_id','')),''),
              'SE:'||coalesce(v_source_match_segment->>'start_utc',
                v_source_match_segment->>'start','')||'|'||
                coalesce(v_source_match_segment->>'end_utc',
                  v_source_match_segment->>'end',''));
            if v_ref_seen_keys ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts.timesheet_id,
                  'reason','DUPLICATE_IDENTITY')::text;
            end if;
            v_ref_seen_keys:=jsonb_set(
              v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
            v_ref_seg_new_ref:=nullif(btrim(coalesce(
              v_source_payload_segment->>'ref_num','')),'');
            v_source_segment_reference_changed:=
              v_source_segment_reference_changed or (
                nullif(btrim(coalesce(
                  v_source_match_segment->>'ref_num','')),'')
                is distinct from v_ref_seg_new_ref);
            v_source_segment_updates:=v_source_segment_updates||
              jsonb_build_array(jsonb_build_object(
                'segment_id',nullif(btrim(coalesce(
                  v_source_match_segment->>'segment_id','')),''),
                'start_identity',coalesce(v_source_match_segment->>'start_utc',
                  v_source_match_segment->>'start'),
                'end_identity',coalesce(v_source_match_segment->>'end_utc',
                  v_source_match_segment->>'end'),
                'ref_num',v_ref_seg_new_ref));
          end loop;

          v_source_new_segments:='[]'::jsonb;
          for v_source_current_segment in
            select value
            from jsonb_array_elements(v_source_ib->'segments')
              with ordinality current_segment(value,ord)
            order by ord
          loop
            v_source_payload_segment:=null;
            select update_item.value into v_source_payload_segment
            from jsonb_array_elements(v_source_segment_updates) update_item(value)
            where (
              nullif(update_item.value->>'segment_id','') is not null
              and nullif(btrim(coalesce(
                v_source_current_segment->>'segment_id','')),'')
                  =nullif(update_item.value->>'segment_id','')
            ) or (
              nullif(update_item.value->>'segment_id','') is null
              and coalesce(v_source_current_segment->>'start_utc',
                    v_source_current_segment->>'start')
                  is not distinct from update_item.value->>'start_identity'
              and coalesce(v_source_current_segment->>'end_utc',
                    v_source_current_segment->>'end')
                  is not distinct from update_item.value->>'end_identity'
            )
            limit 1;
            if v_source_payload_segment is not null then
              v_source_current_segment:=jsonb_set(
                v_source_current_segment,'{ref_num}',
                coalesce(v_source_payload_segment->'ref_num','null'::jsonb),true);
            end if;
            v_source_new_segments:=v_source_new_segments||
              jsonb_build_array(v_source_current_segment);
          end loop;
        else
          v_source_canonical_schedule:=coalesce(
            v_ref_ts.actual_schedule_json,'[]'::jsonb);
          if jsonb_typeof(v_source_canonical_schedule)<>'array'
             or jsonb_array_length(v_source_payload_schedule)
                <>jsonb_array_length(v_source_canonical_schedule) then
            raise exception using errcode='22023',
              message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
              detail=jsonb_build_object(
                'field','actual_schedule_json','reason','STRUCTURE_MISMATCH',
                'timesheet_id',v_ref_ts.timesheet_id)::text;
          end if;
          v_ref_new_segments:='[]'::jsonb;
          for v_source_payload_segment,v_source_current_segment,v_source_payload_ord in
            select p.value,c.value,p.ord
            from jsonb_array_elements(v_source_payload_schedule)
              with ordinality p(value,ord)
            join jsonb_array_elements(v_source_canonical_schedule)
              with ordinality c(value,ord) using(ord)
            order by p.ord
          loop
            if jsonb_typeof(v_source_payload_segment)<>'object'
               or jsonb_typeof(v_source_current_segment)<>'object'
               or not (v_source_payload_segment ? 'ref_num')
               or (v_source_payload_segment-'ref_num') is distinct from
                  (v_source_current_segment-'ref_num') then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','STRUCTURE_MISMATCH',
                  'ordinal',v_source_payload_ord)::text;
            end if;
            v_ref_new_segments:=v_ref_new_segments||jsonb_build_array(
              jsonb_set(v_source_current_segment,'{ref_num}',
                coalesce(v_source_payload_segment->'ref_num','null'::jsonb),true));
          end loop;
          v_source_canonical_schedule:=v_ref_new_segments;
        end if;
      end if;

      v_source_timesheet_row_changed:=(
        ((v_source_update->>'has_reference_number')::boolean
          and v_ref_ts.reference_number is distinct from
            nullif(btrim(coalesce(v_source_update->>'reference_number','')),''))
        or ((v_source_update->>'has_day_references_json')::boolean
          and v_ref_ts.day_references_json is distinct from
            case when jsonb_typeof(v_source_update->'day_references_json')='null'
              then null else v_source_update->'day_references_json' end)
        or ((v_source_update->>'has_actual_schedule_json')::boolean
          and v_source_mode<>'SEGMENTS'
          and v_ref_ts.actual_schedule_json is distinct from
            v_source_canonical_schedule)
        or ((v_source_update->>'has_hospital_norm')::boolean
          and lower(regexp_replace(
                btrim(coalesce(v_ref_ts.hospital_norm,'')),
                '[[:space:]]+',' ','g')) is distinct from
            coalesce(v_source_update->>'hospital_norm',''))
        or ((v_source_update->>'has_ward_norm')::boolean
          and lower(regexp_replace(
                btrim(coalesce(v_ref_ts.ward_norm,'')),
                '[[:space:]]+',' ','g')) is distinct from
            coalesce(v_source_update->>'ward_norm',''))
      );
      v_source_update:=v_source_update||jsonb_build_object(
        'source_mode',v_source_mode,
        'financial_id',v_source_financial_id,
        'canonical_actual_schedule_json',v_source_canonical_schedule,
        'new_financial_segments',v_source_new_segments,
        'segment_updates',v_source_segment_updates,
        'segment_reference_changed',v_source_segment_reference_changed,
        'timesheet_row_changed',v_source_timesheet_row_changed);
      v_source_updates_map:=jsonb_set(
        v_source_updates_map,array[v_ref_ts.timesheet_id::text],
        v_source_update,true);
    end loop;

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_source_changed_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_row_changed')::boolean
       or (desired.value->>'segment_reference_changed')::boolean;

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_refupd_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_id')::uuid=any(v_source_changed_ts_ids)
      and (
        (desired.value->>'has_reference_number')::boolean
        or (desired.value->>'has_day_references_json')::boolean
        or (
          (desired.value->>'has_actual_schedule_json')::boolean
          and (
            (desired.value->>'timesheet_row_changed')::boolean
            or (desired.value->>'segment_reference_changed')::boolean
          )
        )
      );

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_location_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_row_changed')::boolean
      and (
        (desired.value->>'has_hospital_norm')::boolean
        or (desired.value->>'has_ward_norm')::boolean
      );

    if exists(
      select 1 from jsonb_each(v_source_updates_map) desired
      where (desired.value->>'timesheet_row_changed')::boolean
    ) then
      with desired as materialized(
        select (value->>'timesheet_id')::uuid as timesheet_id,value
        from jsonb_each(v_source_updates_map)
      )
      update public.timesheets t
      set updated_at=v_now,
          reference_number=case when (d.value->>'has_reference_number')::boolean
            then nullif(btrim(coalesce(d.value->>'reference_number','')),'')
            else t.reference_number end,
          day_references_json=case when (d.value->>'has_day_references_json')::boolean
            then case when jsonb_typeof(d.value->'day_references_json')='null'
              then null else d.value->'day_references_json' end
            else t.day_references_json end,
          actual_schedule_json=case
            when (d.value->>'has_actual_schedule_json')::boolean
              and d.value->>'source_mode'<>'SEGMENTS'
            then d.value->'canonical_actual_schedule_json'
            else t.actual_schedule_json end,
          hospital_norm=case when (d.value->>'has_hospital_norm')::boolean
            then coalesce(d.value->>'hospital_norm','') else t.hospital_norm end,
          ward_norm=case when (d.value->>'has_ward_norm')::boolean
            then coalesce(d.value->>'ward_norm','') else t.ward_norm end
      from desired d
      where t.timesheet_id=d.timesheet_id
        and (d.value->>'timesheet_row_changed')::boolean
        and t.is_current;
    end if;

    v_refupd_applied:=cardinality(v_refupd_ts_ids);
    v_location_applied:=cardinality(v_location_ts_ids);

    -- Marker rows exist only where the timesheet statement has already advanced
    -- the source revision and the following SEGMENTS ref update would otherwise
    -- invalidate the same source a second time.
    select coalesce(jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object('expected_revision',t.document_revision)),
      '{}'::jsonb)
    into v_source_marker_rows
    from jsonb_each(v_source_updates_map) desired
    join public.timesheets t
      on t.timesheet_id=(desired.value->>'timesheet_id')::uuid and t.is_current
    where (desired.value->>'timesheet_row_changed')::boolean
      and (desired.value->>'segment_reference_changed')::boolean
      and t.document_revision=
        (desired.value->>'expected_document_revision')::bigint+1;
    if (select count(*) from jsonb_each(v_source_marker_rows))<>(
      select count(*) from jsonb_each(v_source_updates_map) desired
      where (desired.value->>'timesheet_row_changed')::boolean
        and (desired.value->>'segment_reference_changed')::boolean
    ) then
      raise exception using errcode='55000',
        message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
        detail=jsonb_build_object(
          'reason','SOURCE_REVISION_ADVANCE_NOT_EXACT')::text;
    end if;
    v_source_marker:=jsonb_build_object(
      'txid',txid_current()::text,'rows',v_source_marker_rows);
    perform set_config(
      'cloudtms.invoice_source_edit_marker',v_source_marker::text,true);

    -- The legacy per-array loops below remain structurally compatible for old
    -- payload code, but the strict merged statement above is now the sole
    -- execution path for accepted source edits.
    v_reference_updates:=null;
    v_location_updates:=null;
  end if;





  -- 0) Apply reference updates to timesheets (does NOT recompute TSFIN; it updates the source timesheet refs)
  if v_reference_updates is not null and jsonb_typeof(v_reference_updates)='array' and jsonb_array_length(v_reference_updates) > 0 then
    for v_refupd in
      select value from jsonb_array_elements(v_reference_updates) value
    loop
      if v_refupd is null or jsonb_typeof(v_refupd) <> 'object' then
        continue;
      end if;

      if nullif(btrim(coalesce(v_refupd->>'timesheet_id','')), '') is null then
        continue;
      end if;

      v_refupd_ts_id := (v_refupd->>'timesheet_id')::uuid;

      v_refupd_set_refnum := (v_refupd ? 'reference_number');
      v_refupd_set_dayrefs := (v_refupd ? 'day_references_json');
      v_refupd_set_sched := (v_refupd ? 'actual_schedule_json');

      v_refupd_refnum := null;
      if v_refupd_set_refnum then
        v_refupd_refnum := nullif(btrim(coalesce(v_refupd->>'reference_number','')), '');
      end if;

      v_refupd_dayrefs := null;
      if v_refupd_set_dayrefs then
        v_refupd_dayrefs := v_refupd->'day_references_json';
        if v_refupd_dayrefs is not null and jsonb_typeof(v_refupd_dayrefs) = 'null' then
          v_refupd_dayrefs := null;
        end if;
      end if;

      v_refupd_sched := null;
      if v_refupd_set_sched then
        v_refupd_sched := v_refupd->'actual_schedule_json';
        if v_refupd_sched is not null and jsonb_typeof(v_refupd_sched) = 'null' then
          v_refupd_sched := null;
        end if;
      end if;

      update public.timesheets tsu
      set
        updated_at = v_now,
        reference_number = case when v_refupd_set_refnum then v_refupd_refnum else tsu.reference_number end,
        day_references_json = case when v_refupd_set_dayrefs then v_refupd_dayrefs else tsu.day_references_json end,
        actual_schedule_json = case when v_refupd_set_sched then v_refupd_sched else tsu.actual_schedule_json end
      where tsu.timesheet_id = v_refupd_ts_id
        and tsu.is_current = true
        and exists(
          select 1
          from public.invoice_lines owned_line
          where owned_line.invoice_id=p_invoice_id
            and (
              owned_line.timesheet_id=tsu.timesheet_id
              or (
                owned_line.timesheet_id is null
                and coalesce(owned_line.meta_json->>'timesheet_id','')
                  ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                and (owned_line.meta_json->>'timesheet_id')::uuid=tsu.timesheet_id
              )
            )
        )
        and (
          (v_refupd_set_refnum and tsu.reference_number is distinct from v_refupd_refnum)
          or (v_refupd_set_dayrefs and tsu.day_references_json is distinct from v_refupd_dayrefs)
          or (v_refupd_set_sched and tsu.actual_schedule_json is distinct from v_refupd_sched)
        );

      get diagnostics v_rc = row_count;
      if coalesce(v_rc,0) > 0 then
        v_refupd_applied := v_refupd_applied + 1;
        v_refupd_ts_ids := v_refupd_ts_ids || v_refupd_ts_id;
      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object('step','reference_updates_applied','count_requested',v_refupd_count,'count_applied',v_refupd_applied)
      );
    end if;
  end if;

  -- 0a) Apply only material hospital/ward changes to current timesheets that
  -- are owned by this invoice. Identical canonical values perform no UPDATE,
  -- so they cannot dirty documents or queue replacement work.
  if v_location_updates is not null
     and jsonb_typeof(v_location_updates)='array'
     and jsonb_array_length(v_location_updates)>0 then
    for v_location_update in
      select value from jsonb_array_elements(v_location_updates) value
    loop
      if v_location_update is null
         or jsonb_typeof(v_location_update)<>'object'
         or nullif(btrim(coalesce(v_location_update->>'timesheet_id','')),'') is null then
        continue;
      end if;

      v_location_ts_id := (v_location_update->>'timesheet_id')::uuid;
      v_location_set_hospital := v_location_update ? 'hospital_norm';
      v_location_set_ward := v_location_update ? 'ward_norm';
      if not v_location_set_hospital and not v_location_set_ward then
        continue;
      end if;

      v_location_hospital_norm := null;
      if v_location_set_hospital then
        v_location_hospital_norm := nullif(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'hospital_norm','')),
          '[[:space:]]+',' ','g')), '');
      end if;
      v_location_ward_norm := null;
      if v_location_set_ward then
        v_location_ward_norm := nullif(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'ward_norm','')),
          '[[:space:]]+',' ','g')), '');
      end if;

      update public.timesheets tsu
      set updated_at=v_now,
          hospital_norm=case when v_location_set_hospital
            then v_location_hospital_norm else tsu.hospital_norm end,
          ward_norm=case when v_location_set_ward
            then v_location_ward_norm else tsu.ward_norm end
      where tsu.timesheet_id=v_location_ts_id
        and tsu.is_current=true
        and exists(
          select 1
          from public.invoice_lines owned_line
          where owned_line.invoice_id=p_invoice_id
            and (
              owned_line.timesheet_id=tsu.timesheet_id
              or (
                owned_line.timesheet_id is null
                and coalesce(owned_line.meta_json->>'timesheet_id','')
                  ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                and (owned_line.meta_json->>'timesheet_id')::uuid=tsu.timesheet_id
              )
            )
        )
        and (
          (v_location_set_hospital
            and tsu.hospital_norm is distinct from v_location_hospital_norm)
          or (v_location_set_ward
            and tsu.ward_norm is distinct from v_location_ward_norm)
        );

      get diagnostics v_rc=row_count;
      if coalesce(v_rc,0)>0 then
        v_location_applied:=v_location_applied+1;
        v_location_ts_ids:=v_location_ts_ids||v_location_ts_id;
      end if;
    end loop;

    if v_invoice_debug then
      v_dbg_steps:=v_dbg_steps||jsonb_build_array(jsonb_build_object(
        'step','timesheet_location_updates_applied',
        'count_requested',v_location_count,
        'count_applied',v_location_applied));
    end if;
  end if;
  -- 0b) After reference updates: refresh invoice_lines meta (ts_reference_number / schedule_ref_nums) and
  -- synchronise only the approved ref_num fields into the locked SEGMENTS
  -- financial projection. The full financial objects and their order are
  -- preserved from the locked current snapshot.
  if v_refupd_ts_ids is not null and coalesce(array_length(v_refupd_ts_ids,1),0) > 0 then

    with desired as materialized(
      select value
      from jsonb_each(v_source_updates_map)
      where (value->>'segment_reference_changed')::boolean
    )
    update public.timesheets_financials tf
    set invoice_breakdown_json=jsonb_set(
      coalesce(tf.invoice_breakdown_json,'{}'::jsonb),
      '{segments}',d.value->'new_financial_segments',true)
    from desired d
    where tf.id=(d.value->>'financial_id')::uuid
      and tf.is_current
      and tf.timesheet_id=(d.value->>'timesheet_id')::uuid
      and upper(coalesce(tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
      and tf.invoice_breakdown_json->'segments'
        is distinct from d.value->'new_financial_segments';
    get diagnostics v_refupd_tsfin_rows_updated=row_count;
    select coalesce(sum(jsonb_array_length(
      coalesce(value->'segment_updates','[]'::jsonb))),0)::integer
    into v_refupd_tsfin_segments_updated
    from jsonb_each(v_source_updates_map)
    where (value->>'segment_reference_changed')::boolean;

    -- The marker has served its single synchronisation statement. Clear it so
    -- no later statement in this transaction can inherit suppression authority.
    perform set_config('cloudtms.invoice_source_edit_marker','{}',true);

    select array_agg(distinct x)
    into v_refupd_ts_ids_distinct
    from unnest(v_refupd_ts_ids) x
    where x is not null;

    v_refupd_ts_ids_distinct := coalesce(v_refupd_ts_ids_distinct, array[]::uuid[]);

    foreach v_ref_ts_id in array v_refupd_ts_ids_distinct loop
      if v_ref_ts_id is null then
        continue;
      end if;

      select tsu.*
      into v_ref_ts
      from public.timesheets tsu
      where tsu.timesheet_id = v_ref_ts_id
        and tsu.is_current = true
      limit 1;

      if not found then
        continue;
      end if;
      v_source_update:=v_source_updates_map->v_ref_ts_id::text;
      v_source_mode:=upper(coalesce(v_source_update->>'source_mode',''));
      v_source_canonical_schedule:=case when v_source_mode='SEGMENTS'
        then coalesce(v_source_update->'new_financial_segments','[]'::jsonb)
        else coalesce(v_ref_ts.actual_schedule_json,'[]'::jsonb) end;

      -- Build the cache from the exact authority used by this invoice. SEGMENTS
      -- refs come from invoice-owned financial segments; other modes use the
      -- structurally preserved timesheet schedule.
      v_ref_schedule_refs := '[]'::jsonb;

      if nullif(btrim(coalesce(v_ref_ts.reference_number,'')), '') is not null then
        v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(v_ref_ts.reference_number),'')));
      end if;

      if v_ref_ts.day_references_json is not null and jsonb_typeof(v_ref_ts.day_references_json) = 'object' then
        for kv in
          select key as k, value as v
          from jsonb_each_text(v_ref_ts.day_references_json)
        loop
          if nullif(btrim(coalesce(kv.v,'')), '') is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(kv.v),'')));
          end if;
        end loop;
      end if;

      if jsonb_typeof(v_source_canonical_schedule)='array' then
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_source_canonical_schedule) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;
          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          if v_ref_sched_ref is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(v_ref_sched_ref));
          end if;
        end loop;
      end if;

      select coalesce(jsonb_agg(to_jsonb(x) order by x), '[]'::jsonb)
      into v_ref_schedule_refs_distinct
      from (
        select distinct btrim(t.x) as x
        from jsonb_array_elements_text(coalesce(v_ref_schedule_refs,'[]'::jsonb)) as t(x)
        where nullif(btrim(coalesce(t.x,'')), '') is not null
      ) q;

      -- Refresh meta_json on all invoice lines for this timesheet
      update public.invoice_lines ilu
      set meta_json = coalesce(ilu.meta_json, '{}'::jsonb) || jsonb_build_object(
        'ts_reference_number', nullif(btrim(coalesce(v_ref_ts.reference_number,'')), ''),
        'schedule_ref_nums', coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb),
        'schedule_ref_count', jsonb_array_length(coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb))
      )
      where ilu.invoice_id = p_invoice_id
        and (
          ilu.timesheet_id=v_ref_ts_id
          or (
            ilu.timesheet_id is null
            and coalesce(ilu.meta_json->>'timesheet_id','') ~
              '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            and (ilu.meta_json->>'timesheet_id')::uuid=v_ref_ts_id
          )
        )
        and (
          ilu.meta_json->'ts_reference_number' is distinct from
            coalesce(to_jsonb(nullif(btrim(coalesce(v_ref_ts.reference_number,'')),'')),'null'::jsonb)
          or ilu.meta_json->'schedule_ref_nums' is distinct from
            coalesce(v_ref_schedule_refs_distinct,'[]'::jsonb)
          or ilu.meta_json->'schedule_ref_count' is distinct from
            to_jsonb(jsonb_array_length(coalesce(v_ref_schedule_refs_distinct,'[]'::jsonb)))
        );

      get diagnostics v_rc = row_count;
      v_refupd_meta_rows_updated := v_refupd_meta_rows_updated + coalesce(v_rc,0);

      -- Best-effort sync: update tsfin.invoice_breakdown_json segments[].ref_num by matching start/end fields
      v_ref_seg_updates_this_ts := 0;
      v_ref_seg_matches_this_ts := 0;
      v_ref_tsfin_id := null;
      v_ref_ib := null;

        select tfu.id, tfu.invoice_breakdown_json
      into v_ref_tsfin_id, v_ref_ib
      from public.timesheets_financials tfu
      where tfu.timesheet_id = v_ref_ts_id
        and tfu.is_current = true
      limit 1
      for update;


      if v_ref_tsfin_id is not null
         and v_source_mode<>'SEGMENTS'
         and v_ref_ib is not null
         and jsonb_typeof(v_ref_ib) = 'object'
         and upper(coalesce(v_ref_ib->>'mode','')) = 'SEGMENTS'
         and jsonb_typeof(v_ref_ib->'segments') = 'array'
         and v_ref_ts.actual_schedule_json is not null
         and jsonb_typeof(v_ref_ts.actual_schedule_json) = 'array'
       then
         -- Build a map of segment_id (preferred) or start|end -> ref_num from actual_schedule_json
        v_ref_sched_map := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ts.actual_schedule_json) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;

          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_sched_map ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                  'side','TIMESHEET_SCHEDULE')::text;
            end if;
            v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
            continue;
          end if;

          v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
          v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');
          if v_ref_seg_start is null or v_ref_seg_end is null then
            continue;
          end if;

          v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
          if v_ref_sched_map ? v_ref_sched_key then
            raise exception using errcode='22023',
              message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
              detail=jsonb_build_object(
                'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                'side','TIMESHEET_SCHEDULE')::text;
          end if;
          v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
        end loop;

        v_ref_new_segments := '[]'::jsonb;
        v_ref_seen_keys := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ib->'segments') value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
            continue;
          end if;

          v_ref_seg_new_ref := null;
          v_ref_seg_has_update := false;

          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_seen_keys ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                  'side','FINANCIAL_SEGMENTS')::text;
            end if;
            v_ref_seen_keys:=jsonb_set(v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
            if v_ref_sched_map ? v_ref_sched_key then
              v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
              v_ref_seg_has_update := true;
              v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
            end if;
          end if;

          if not v_ref_seg_has_update then
            v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
            v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');

            if v_ref_seg_start is not null and v_ref_seg_end is not null then
              v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
              if v_ref_seen_keys ? v_ref_sched_key then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                  detail=jsonb_build_object(
                    'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                    'side','FINANCIAL_SEGMENTS')::text;
              end if;
              v_ref_seen_keys:=jsonb_set(v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
              if v_ref_sched_map ? v_ref_sched_key then
                v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
                v_ref_seg_has_update := true;
                v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
              end if;
            end if;
          end if;

          if v_ref_seg_has_update then
            v_ref_seg_cur_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
            if v_ref_seg_cur_ref is distinct from v_ref_seg_new_ref then
              v_ref_seg_obj := jsonb_set(
                v_ref_seg_obj,
                '{ref_num}',
                case when v_ref_seg_new_ref is null then 'null'::jsonb else to_jsonb(v_ref_seg_new_ref) end,
                true
              );
              v_ref_seg_updates_this_ts := v_ref_seg_updates_this_ts + 1;
            end if;
          end if;

          v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
        end loop;



        if v_ref_sched_map is not null
           and jsonb_typeof(v_ref_sched_map) = 'object'
           and v_ref_sched_map <> '{}'::jsonb
           and coalesce(jsonb_array_length(coalesce(v_ref_ib->'segments','[]'::jsonb)),0) > 0
           and coalesce(v_ref_seg_matches_this_ts,0) = 0
        then
          raise exception 'SEGMENTS reference sync failed: no segments matched schedule keys (timesheet_id=% tsfin_id=%)', v_ref_ts_id, v_ref_tsfin_id;
        end if;

        if v_ref_seg_updates_this_ts > 0 then
              update public.timesheets_financials tfu2
          set invoice_breakdown_json = jsonb_set(coalesce(tfu2.invoice_breakdown_json, '{}'::jsonb), '{segments}', v_ref_new_segments, true)
          where tfu2.id = v_ref_tsfin_id
            and tfu2.is_current = true;


          get diagnostics v_rc = row_count;
          if coalesce(v_rc,0) > 0 then
            v_refupd_tsfin_rows_updated := v_refupd_tsfin_rows_updated + 1;
            v_refupd_tsfin_segments_updated := v_refupd_tsfin_segments_updated + v_ref_seg_updates_this_ts;
          end if;
        end if;

      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','reference_updates_meta_refreshed',
          'timesheets_count', coalesce(array_length(v_refupd_ts_ids_distinct,1),0),
          'invoice_line_rows_updated', v_refupd_meta_rows_updated,
          'tsfin_rows_updated', v_refupd_tsfin_rows_updated,
          'tsfin_segments_refnum_updated', v_refupd_tsfin_segments_updated
        )
      );
    end if;
  end if;

  -- 1) Removals (by invoice_line_id)
  if v_remove_ids is not null and coalesce(array_length(v_remove_ids,1),0) > 0 then
    -- collect timesheet_ids touched (any)
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_ts_ids_touched
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids);

    -- record removed lines for history
    v_hist_lines_removed := coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb);

    -- Only unlock TSFIN when HOURS lines were removed (prevents accidental unlock when deleting only expenses/other lines)
    -- IMPORTANT: compute BEFORE deletion because we match on the removed invoice_line ids.
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_cw_ts_ids
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids)
      and upper(coalesce(l.meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);



    if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      v_refresh_hr_cache := true;
    end if;
delete from public.invoice_lines
    where invoice_id = p_invoice_id
      and id = any(v_remove_ids);
    get diagnostics v_rc = row_count;
    v_dbg_lines_deleted := v_dbg_lines_deleted + coalesce(v_rc,0);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','lines_removed',
          'rows_deleted', coalesce(v_rc,0),
          'timesheets_touched', coalesce(array_length(v_ts_ids_touched,1),0),
          'timesheets_to_unlock_count', coalesce(array_length(v_cw_ts_ids,1),0)
        )
      );
    end if;

    -- FIX: if a touched timesheet now has NO remaining invoice_lines on this invoice,
    -- unlock it even if the removed lines were expenses/mileage/additional (expense-only/SEGMENTS-empty case).
    v_ts_ids_fully_removed := array[]::uuid[];
    select array_agg(distinct x)
    into v_ts_ids_fully_removed
    from unnest(coalesce(v_ts_ids_touched, array[]::uuid[])) x
    where x is not null
      and not exists (
        select 1
        from public.invoice_lines l2
        where l2.invoice_id = p_invoice_id
          and l2.timesheet_id = x
      );

    v_ts_ids_fully_removed := coalesce(v_ts_ids_fully_removed, array[]::uuid[]);

    if coalesce(array_length(v_ts_ids_fully_removed,1),0) > 0 then
      -- unlock any segments locked to this invoice (safe no-op for SEGMENTS-empty)
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_ts_ids_fully_removed);

      -- clear whole-timesheet lock if it was set for this invoice (SEGMENTS-empty / non-segments / pseudo segment_id null locks)
      update public.timesheets_financials tfu_lock
      set
        locked_by_invoice_id = null,
        locked_at_utc = null,
        updated_at = v_now
      where tfu_lock.is_current = true
        and tfu_lock.timesheet_id = any(v_ts_ids_fully_removed)
        and tfu_lock.locked_by_invoice_id = p_invoice_id;

      get diagnostics v_rc = row_count;
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(v_rc,0);

      v_refresh_hr_cache := true;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','timesheets_unlocked_after_full_removal',
            'timesheets_fully_removed_count', coalesce(array_length(v_ts_ids_fully_removed,1),0),
            'tsfin_rows_unlocked_count', coalesce(v_rc,0)
          )
        );
      end if;
    end if;

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

    if v_cw_ts_ids is not null and coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_cw_ts_ids);
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(array_length(v_cw_ts_ids,1),0);

      -- Cleanup: if no segments remain on THIS invoice for a touched timesheet, remove ALL remaining invoice lines for that timesheet
      foreach tsid in array v_cw_ts_ids loop
        if tsid is null then continue; end if;

        -- detect if any segments are still locked to THIS invoice
        select tf.*
        into snap
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.timesheet_id = tsid
        limit 1;

        if not found then
          continue;
        end if;

        segments := '[]'::jsonb;
        if snap.invoice_breakdown_json is not null
           and jsonb_typeof(snap.invoice_breakdown_json)='object'
           and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
           and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
        then
          for v_seg in
            select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
          loop
            if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
              continue;
            end if;
            seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
            if seg_locked = p_invoice_id::text then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end loop;

          if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        else
          -- Non-segments: if snapshot is no longer locked to this invoice, remove all remaining invoice lines for this timesheet
          if snap.locked_by_invoice_id is null then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        end if;
      end loop;
    end if;

    -- History event (always)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_LINES_REMOVED',
      null,
      jsonb_build_object('remove_invoice_line_ids', v_hist_lines_removed, 'timesheet_ids_touched', coalesce(to_jsonb(v_ts_ids_touched), '[]'::jsonb)),
      null,
      p_actor_user_id
    );

  end if;


  -- 1b) Segment edits (SEGMENTS mode only)
-- NOTE: Segment moves are NOT allowed when additional rates OR expenses/mileage exist on the TSFIN snapshot.
-- Payload contract uses tsfin_id + segment_id.
if v_has_seg_ops then

  v_dbg_seg_add_refs := case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end;
  v_dbg_seg_remove_refs := case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','segment_ops_start',
        'add_segment_refs_count', v_dbg_seg_add_refs,
        'remove_segment_refs_count', v_dbg_seg_remove_refs
      )
    );
  end if;

  -- Collect distinct tsfin_ids involved in segment ops
  select array_agg(distinct (x->>'tsfin_id')::uuid)
  into v_seg_tsfin_ids
  from (
    select value as x
    from jsonb_array_elements(coalesce(v_remove_seg_refs,'[]'::jsonb))
    union all
    select value as x
    from jsonb_array_elements(coalesce(v_add_seg_refs,'[]'::jsonb))
  ) u
  where jsonb_typeof(x) = 'object'
    and nullif(btrim(coalesce(x->>'tsfin_id','')), '') is not null;

  v_seg_tsfin_ids := coalesce(v_seg_tsfin_ids, array[]::uuid[]);

  v_dbg_seg_tsfins := coalesce(array_length(v_seg_tsfin_ids,1),0);
  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','segment_ops_targets','tsfin_count',v_dbg_seg_tsfins));
  end if;

  if coalesce(array_length(v_seg_tsfin_ids,1),0) > 0 then

    -- Validate that each snapshot is current SEGMENTS mode and has NO additional/expenses/mileage
    foreach v_tsfin_id in array v_seg_tsfin_ids loop
      select *
      into snap
      from public.timesheets_financials tf
      where tf.id = v_tsfin_id
      limit 1;

      if not found then
        raise exception 'Segment edit refers to unknown tsfin_id %', v_tsfin_id;
      end if;

      if snap.is_current is not true then
        raise exception 'Segment edit requires current tsfin snapshot (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.client_id is distinct from v_inv.client_id then
        raise exception 'Segment edit timesheet client mismatch (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.invoice_breakdown_json is null
         or jsonb_typeof(snap.invoice_breakdown_json) <> 'object'
         or upper(coalesce(snap.invoice_breakdown_json->>'mode','')) <> 'SEGMENTS'
         or jsonb_typeof(snap.invoice_breakdown_json->'segments') <> 'array'
      then
        raise exception 'Segment edit is only supported for SEGMENTS timesheets (tsfin_id=%)', v_tsfin_id;
      end if;

      v_has_additional :=
        public._inv_round2(coalesce(snap.additional_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.additional_charge_ex_vat,0)) <> 0
        or (snap.additional_units_json is not null and jsonb_typeof(snap.additional_units_json)='object' and snap.additional_units_json <> '{}'::jsonb);

      v_has_expense_or_mileage :=
        public._inv_round2(coalesce(snap.expenses_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.expenses_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_units,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_charge_ex_vat,0)) <> 0;

      if v_has_additional then
        raise exception 'Segments cannot be moved when additional rates exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;

      if v_has_expense_or_mileage then
        raise exception 'Segments cannot be moved when expenses or mileage exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;
    end loop;

    -- Validate and apply add_segment_refs (lock selected segments)
    v_seg_refs_to_lock := '[]'::jsonb;
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      for v_ref in
        select value from jsonb_array_elements(v_add_seg_refs) value
      loop
        if v_ref is null or jsonb_typeof(v_ref) <> 'object' then
          continue;
        end if;

        if nullif(btrim(coalesce(v_ref->>'tsfin_id','')), '') is null then
          continue;
        end if;

        v_tsfin_id := (v_ref->>'tsfin_id')::uuid;
        v_seg_id := nullif(btrim(coalesce(v_ref->>'segment_id','')), '');

        if v_seg_id is null then
          raise exception 'add_segment_refs requires segment_id (tsfin_id=%)', v_tsfin_id;
        end if;

          -- Load snapshot + timesheet + precheck (must be OK)
        select
          tf.*,
          tsr.sheet_scope::text as sheet_scope,
          coalesce(tsr.submission_mode::text,'') as submission_mode,
          tsr.day_references_json,
          tsr.actual_schedule_json,
          tsr.week_ending_date,
          cw.contract_id
        into snap
        from public.timesheets_financials tf
        join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
        left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
        join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
        where tf.id = v_tsfin_id
          and tf.is_current = true
          and tf.client_id = v_inv.client_id
          and upper(coalesce(pcv.precheck_status,''))
            in ('OK','BLOCK_NO_PDF')
        limit 1;

        if not found then
          raise exception 'Segment add failed eligibility (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        natural_start := (snap.week_ending_date::date - 6);

        -- Locate the segment object
        v_seg := null;
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;
          if nullif(btrim(coalesce(v_seg->>'segment_id','')), '') = v_seg_id then
            exit;
          end if;
          v_seg := null;
        end loop;

        if v_seg is null then
          raise exception 'Segment not found (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
        if seg_locked is not null then
          raise exception 'Segment already invoiced (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
        seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

        -- segment-level ref gating if required
        select * into pc from public.v_ts_invoice_precheck where timesheet_id = snap.timesheet_id limit 1;
        if pc.require_reference_to_invoice is true and seg_ref = '' then
          raise exception 'Segment missing reference number (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
        end if;

        -- Week eligibility
        if seg_target is null or seg_target = natural_start then
          if v_week_start <> natural_start then
            raise exception 'Segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        else
          if v_week_start <> seg_target then
            raise exception 'Delayed segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
          if seg_target > v_anchor_ymd then
            raise exception 'Delayed segment cannot be invoiced early (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        end if;

        v_seg_refs_to_lock := v_seg_refs_to_lock || jsonb_build_array(
          jsonb_build_object(
            'tsfin_id', v_tsfin_id::text,
            'segment_id', v_seg_id
          )
        );
      end loop;

      if jsonb_typeof(v_seg_refs_to_lock) = 'array' and jsonb_array_length(v_seg_refs_to_lock) > 0 then
        perform public._inv_lock_segments_for_invoice(p_invoice_id, v_seg_refs_to_lock);
      end if;
    end if;

      -- Apply remove_segment_refs (unlock selected segments on THIS invoice)
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._inv_unlock_segment_refs_for_invoice(p_invoice_id, v_remove_seg_refs::jsonb, p_actor_user_id);
    end if;



    -- History: segment ops (always)
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_ADDED',
        null,
        jsonb_build_object('add_segment_refs', v_add_seg_refs),
        null,
        p_actor_user_id
      );
    end if;
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_REMOVED',
        null,
        jsonb_build_object('remove_segment_refs', v_remove_seg_refs),
        null,
        p_actor_user_id
      );
    end if;

    -- Rebuild HOURS lines for touched timesheets on this invoice
    select array_agg(distinct tf.timesheet_id)
    into v_seg_ts_ids
    from public.timesheets_financials tf
    where tf.id = any(v_seg_tsfin_ids);

    v_seg_ts_ids := coalesce(v_seg_ts_ids, array[]::uuid[]);

    foreach tsid in array v_seg_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + contract (no READY_FOR_INVOICE restriction; this is an invoice edit)
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',tsid::text));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',tsid::text,'tsfin_id',snap.id::text));
      end if;

      select v.r2_key
      into v_exact_timesheet_document_r2_key
      from public.timesheets t
      join public.invoice_document_versions v
        on v.entity_type='TIMESHEET'
       and v.entity_id=t.timesheet_id
       and v.purpose='TIMESHEET'
       and v.source_revision=t.document_revision::text
       and v.status='READY'
       and nullif(v.r2_key,'') is not null
       and nullif(v.sha256,'') is not null
       and coalesce(v.size_bytes,0)>0
       and coalesce(v.page_count,0)>0
      where t.timesheet_id=tsid and t.is_current
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1;


      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_display_site := null;

      if contract_id is not null then
        select
          coalesce(overrideclientsettings,false),
          daily_calc_of_invoices,
          bucket_labels_json,
          nullif(btrim(coalesce(display_site,'')), '')
        into
          v_contract_override, v_contract_daily_calc, v_contract_bucket_labels, c_display_site
        from public.contracts
        where id = contract_id
        limit 1;

        c_daily_calc := case when v_contract_override then coalesce(v_contract_daily_calc,false) else v_client_daily_calc end;
        c_bucket_labels := case when v_contract_override then v_contract_bucket_labels else null end;
      end if;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      -- Build segment set locked to THIS invoice
      segments := '[]'::jsonb;
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked = p_invoice_id::text then
            segments := segments || jsonb_build_array(v_seg);
          end if;
        end loop;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        -- If no segments remain on this invoice for this timesheet, remove ALL invoice lines for that timesheet
        delete from public.invoice_lines
        where invoice_id = p_invoice_id
          and timesheet_id = tsid;
        v_dbg_seg_timesheets_removed := v_dbg_seg_timesheets_removed + 1;
        continue;
      end if;

      v_dbg_seg_timesheets_rebuilt := v_dbg_seg_timesheets_rebuilt + 1;

      -- Replace HOURS lines for this timesheet on this invoice
      delete from public.invoice_lines
      where invoice_id = p_invoice_id
        and timesheet_id = tsid
        and upper(coalesce(meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');
        -- HOURS lines
        if c_daily_calc then
          -- Daily: group by segment.date
          for r_day in
            with rows as (
              select
                nullif(btrim(coalesce(seg_el->>'date','')), '') as ymd,
                coalesce((seg_el->>'hours_day')::numeric,0)   as h_day,
                coalesce((seg_el->>'hours_night')::numeric,0) as h_night,
                coalesce((seg_el->>'hours_sat')::numeric,0)   as h_sat,
                coalesce((seg_el->>'hours_sun')::numeric,0)   as h_sun,
                coalesce((seg_el->>'hours_bh')::numeric,0)    as h_bh,
                coalesce((seg_el->>'pay_amount')::numeric,0)  as pay_ex,
                coalesce((seg_el->>'charge_amount')::numeric,0) as chg_ex
              from jsonb_array_elements(segments) seg_el
            ),
            agg as (
              select
                ymd,
                sum(rows.h_day)::numeric as hours_day,
                sum(rows.h_night)::numeric as hours_night,
                sum(rows.h_sat)::numeric as hours_sat,
                sum(rows.h_sun)::numeric as hours_sun,
                sum(rows.h_bh)::numeric as hours_bh,
                sum(rows.pay_ex)::numeric as pay_ex,
                sum(rows.chg_ex)::numeric as chg_ex
              from rows
              where ymd is not null and ymd ~ '^\d{4}-\d{2}-\d{2}$'
              group by ymd
            )
            select * from agg order by ymd
          loop
          chg_ex := public._inv_round2(r_day.chg_ex);
if chg_ex = 0 then continue; end if;

if (coalesce(r_day.hours_day,0)+coalesce(r_day.hours_night,0)+coalesce(r_day.hours_sat,0)+coalesce(r_day.hours_sun,0)+coalesce(r_day.hours_bh,0)) = 0 then
  continue;
end if;


            pay_ex := public._inv_round2(r_day.pay_ex);
            margin_ex := public._inv_round2(chg_ex - pay_ex);
            vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
            inc_amt := public._inv_round2(chg_ex + vat_amt);

            line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
                         ' – '|| r_day.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');

            v_meta := jsonb_build_object(
              'line_type','HOURS_DAILY',
              'timesheet_id', tsid::text,
              'tsfin_id', snap.id::text,
              'candidate_display', coalesce(nullif(btrim(coalesce(c_display_site,'')),''), null),
              'role', c_role,
              'hospital', c_display_site,
              'ward', c_ward_hint,
              'week_ending_date', snap.week_ending_date::text,
              'date', r_day.ymd,
              'bucket_labels', c_bucket_labels
            );

            v_source_key := 'TS:' || tsid::text || ':HOURS:' || r_day.ymd;

            insert into public.invoice_lines(
              invoice_id, timesheet_id, booking_id, description,
              hours_day, hours_night, hours_sat, hours_sun, hours_bh,
              pay_day, pay_night, pay_sat, pay_sun, pay_bh,
              charge_day, charge_night, charge_sat, charge_sun, charge_bh,
              total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
              vat_rate_pct, vat_amount, total_inc_vat,
              paper_ts_r2_key, meta_json, source_key
            )
            values (
              p_invoice_id, tsid, snap.booking_id, line_desc,
              public._inv_round2(r_day.hours_day), public._inv_round2(r_day.hours_night), public._inv_round2(r_day.hours_sat), public._inv_round2(r_day.hours_sun), public._inv_round2(r_day.hours_bh),
              null,null,null,null,null,
              null,null,null,null,null,
              pay_ex, chg_ex, margin_ex,
              v_vat_rate, vat_amt, inc_amt,
              v_exact_timesheet_document_r2_key,
              v_meta,
              v_source_key
            )
            on conflict (invoice_id, source_key) do nothing;
          end loop;
        else
          -- Weekly hours line
          select
            public._inv_round2(coalesce(sum((seg_el->>'hours_day')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_night')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sat')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sun')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_bh')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'pay_amount')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'charge_amount')::numeric),0))
          into h_day, h_night, h_sat, h_sun, h_bh, pay_ex, chg_ex
          from jsonb_array_elements(segments) seg_el;

       if chg_ex <> 0 and (coalesce(h_day,0)+coalesce(h_night,0)+coalesce(h_sat,0)+coalesce(h_sun,0)+coalesce(h_bh,0)) <> 0 then
  margin_ex := public._inv_round2(chg_ex - pay_ex);
  vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
  inc_amt := public._inv_round2(chg_ex + vat_amt);

  line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
               ' – W/E '|| coalesce(snap.week_ending_date::text,'');

  v_meta := jsonb_build_object(
    'line_type','HOURS_WEEKLY',
    'timesheet_id', tsid::text,
    'tsfin_id', snap.id::text,
    'week_ending_date', snap.week_ending_date::text,
    'bucket_labels', c_bucket_labels
  );

  v_source_key := 'TS:' || tsid::text || ':HOURS:WEEK';

  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  values (
    p_invoice_id, tsid, snap.booking_id, line_desc,
    h_day, h_night, h_sat, h_sun, h_bh,
    null,null,null,null,null,
    null,null,null,null,null,
    pay_ex, chg_ex, margin_ex,
    v_vat_rate, vat_amt, inc_amt,
    v_exact_timesheet_document_r2_key,
    v_meta,
    v_source_key
  )
  on conflict (invoice_id, source_key) do nothing;
end if;

        end if;

        -- Additional rates


    end loop;
  end if;
end if;

  -- 2) Add adjustments
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_adjustments') then
    if jsonb_typeof(p_payload->'add_adjustments') = 'array' then
      for adj in
        select value from jsonb_array_elements(p_payload->'add_adjustments') value
      loop
        if adj is null or jsonb_typeof(adj) <> 'object' then
          continue;
        end if;

        v_adj_token := nullif(btrim(coalesce(adj->>'client_token','')), '');
        v_adj_desc  := nullif(btrim(coalesce(adj->>'description','')), '');
        begin
          v_adj_ex := (adj->>'amount_ex_vat')::numeric;
        exception when others then
          v_adj_ex := null;
        end;

        if v_adj_token is null or v_adj_desc is null or v_adj_ex is null then
          continue;
        end if;

        v_adj_vat := public._inv_round2(v_adj_ex * v_vat_rate / 100);
        if v_vat_rate = 0 then v_adj_vat := 0; end if;
        v_adj_inc := public._inv_round2(v_adj_ex + v_adj_vat);

        v_adj_source_key := 'ADJ:' || v_adj_token;

        v_meta := jsonb_build_object(
          'line_type','ADJUSTMENT',
          'client_token', v_adj_token,
          'description', v_adj_desc,
          'amount_ex_vat', public._inv_round2(v_adj_ex),
          'vat_rate_pct', v_vat_rate,
          'vat_chargeable', v_vat_chargeable
        );

        v_hist_adj := v_hist_adj || jsonb_build_array(v_meta);

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, null, null, v_adj_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          0, public._inv_round2(v_adj_ex), public._inv_round2(v_adj_ex),
          v_vat_rate, v_adj_vat, v_adj_inc,
          null,
          v_meta,
          v_adj_source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end loop;
    end if;
  end if;

  -- History: adjustments added (always)
  if jsonb_typeof(v_hist_adj)='array' and jsonb_array_length(v_hist_adj) > 0 then
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_ADJUSTMENTS_ADDED',
      null,
      jsonb_build_object('adjustments', v_hist_adj),
      null,
      p_actor_user_id
    );
  end if;


  -- 3) Add timesheets (full parity: hours + additional + expenses + mileage), for THIS invoice week_start
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    foreach tsid in array v_add_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + precheck
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
        and tf.locked_by_invoice_id is null
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and upper(coalesce(pcv.precheck_status,''))
          in ('OK','BLOCK_NO_PDF')
        and tf.client_id = v_inv.client_id
        and (
          pcv.require_reference_to_invoice is not true
          or public._inv_timesheet_has_invoice_reference(
                tsr.sheet_scope::text,
                coalesce(tsr.submission_mode::text,''),
                tsr.reference_number,
                tsr.day_references_json,
                tsr.actual_schedule_json
             )
        )
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',coalesce(tsid::text,'')));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',coalesce(tsid::text,''),'tsfin_id',coalesce(snap.id::text,'')));
      end if;

      select v.r2_key
      into v_exact_timesheet_document_r2_key
      from public.timesheets t
      join public.invoice_document_versions v
        on v.entity_type='TIMESHEET'
       and v.entity_id=t.timesheet_id
       and v.purpose='TIMESHEET'
       and v.source_revision=t.document_revision::text
       and v.status='READY'
       and nullif(v.r2_key,'') is not null
       and nullif(v.sha256,'') is not null
       and coalesce(v.size_bytes,0)>0
       and coalesce(v.page_count,0)>0
      where t.timesheet_id=tsid and t.is_current
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1;
      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_role := null;
      c_display_site := null;
      c_ward_hint := null;

      if contract_id is not null then
        select
          coalesce(overrideclientsettings,false),
          daily_calc_of_invoices,
          bucket_labels_json,
          nullif(btrim(coalesce(role,'')), ''),
          nullif(btrim(coalesce(display_site,'')), ''),
          nullif(btrim(coalesce(ward_hint,'')), '')
        into
          v_contract_override, v_contract_daily_calc, v_contract_bucket_labels, c_role, c_display_site, c_ward_hint
        from public.contracts
        where id = contract_id
        limit 1;

        c_daily_calc := case when v_contract_override then coalesce(v_contract_daily_calc,false) else v_client_daily_calc end;
        c_bucket_labels := case when v_contract_override then v_contract_bucket_labels else null end;
      end if;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      natural_start := (snap.week_ending_date::date - 6);

      segments := '[]'::jsonb;

      -- Build invoiceable segment set for THIS invoice week_start
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked is not null then
            continue; -- already invoiced
          end if;

          seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
          seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

          -- segment-level ref gating if required
          select * into pc from public.v_ts_invoice_precheck where timesheet_id = tsid limit 1;
          if pc.require_reference_to_invoice is true and seg_ref = '' then
            continue;
          end if;

          -- delayed detection
          if seg_target is null or seg_target = natural_start then
            -- not delayed: belongs to natural week only
            if v_week_start <> natural_start then
              continue;
            end if;
            -- allow early is implicit for invoice edits (invoice is already being edited)
            segments := segments || jsonb_build_array(v_seg);
          else
            -- delayed: only if this invoice week matches target AND target has arrived (no early invoicing for delayed)
            if v_week_start = seg_target and seg_target <= v_anchor_ymd then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end if;
        end loop;
      else
        -- Non-segments: include only when invoice week matches natural week
        if v_week_start = natural_start then
          segments := jsonb_build_array(
            jsonb_build_object(
              'segment_id', null,
              'date', coalesce(snap.week_ending_date::text, v_week_start::text),
              'hours_day', public._inv_round2(coalesce(snap.hours_day,0)),
              'hours_night', public._inv_round2(coalesce(snap.hours_night,0)),
              'hours_sat', public._inv_round2(coalesce(snap.hours_sat,0)),
              'hours_sun', public._inv_round2(coalesce(snap.hours_sun,0)),
              'hours_bh', public._inv_round2(coalesce(snap.hours_bh,0)),
              'pay_amount', public._inv_round2(
                coalesce(snap.total_pay_ex_vat,0)
                - coalesce(snap.additional_pay_ex_vat,0)
                - coalesce(snap.expenses_pay_ex_vat,0)
                - coalesce(snap.mileage_pay_ex_vat,0)
              ),
              'charge_amount', public._inv_round2(
                coalesce(snap.total_charge_ex_vat,0)
                - coalesce(snap.additional_charge_ex_vat,0)
                - coalesce(snap.expenses_charge_ex_vat,0)
                - coalesce(snap.mileage_charge_ex_vat,0)
              ),
              'ref_num', coalesce(snap.reference_number,'')
            )
          );
        end if;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        continue;
      end if;

      -- Build the exact invoice-line economics from the frozen current financial snapshot.
      -- This mirrors Invoice Async V8 generation and never derives economics from mutable live rows.
      select p.vat_rate
      into v_vat_rate
      from private._invoice_generation_vat_policy_batch(jsonb_build_array(jsonb_build_object(
        'source_member_key','invoice-edit:'||tsid::text,
        'source_type','TIMESHEET',
        'source_id',tsid,
        'timesheet_id',tsid,
        'effective_date',snap.week_ending_date
      ))) p
      where p.source_member_key='invoice-edit:'||tsid::text
        and p.valid
        and p.vat_rate is not null
      limit 1;

      if not found then
        raise exception 'INVOICE_EDIT_VAT_POLICY_UNRESOLVED'
          using errcode='P0001',detail=jsonb_build_object(
            'invoice_id',p_invoice_id,'timesheet_id',tsid)::text;
      end if;

      insert into public.invoice_lines(
        invoice_id,timesheet_id,booking_id,description,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh,
        pay_day,pay_night,pay_sat,pay_sun,pay_bh,
        charge_day,charge_night,charge_sat,charge_sun,charge_bh,
        total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
        vat_rate_pct,vat_amount,total_inc_vat,
        paper_ts_r2_key,meta_json,source_key
      )
      with base as materialized (
        select coalesce(
          (select nullif(btrim(coalesce(cd.display_name,'')),'')
             from public.candidates cd where cd.id=snap.candidate_id limit 1),
          'TS '||tsid::text
        ) candidate_display
      ),
      segment_rows as materialized (
        select
          case when left(coalesce(s.value->>'date',''),10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then left(s.value->>'date',10) end work_date,
          case when pg_input_is_valid(s.value->>'hours_day','numeric') then (s.value->>'hours_day')::numeric else 0 end h_day,
          case when pg_input_is_valid(s.value->>'hours_night','numeric') then (s.value->>'hours_night')::numeric else 0 end h_night,
          case when pg_input_is_valid(s.value->>'hours_sat','numeric') then (s.value->>'hours_sat')::numeric else 0 end h_sat,
          case when pg_input_is_valid(s.value->>'hours_sun','numeric') then (s.value->>'hours_sun')::numeric else 0 end h_sun,
          case when pg_input_is_valid(s.value->>'hours_bh','numeric') then (s.value->>'hours_bh')::numeric else 0 end h_bh,
          case
            when pg_input_is_valid(s.value->>'pay_amount','numeric') then (s.value->>'pay_amount')::numeric
            when pg_input_is_valid(s.value->>'pay_ex_vat','numeric') then (s.value->>'pay_ex_vat')::numeric
            else 0 end pay_ex,
          case
            when pg_input_is_valid(s.value->>'charge_amount','numeric') then (s.value->>'charge_amount')::numeric
            when pg_input_is_valid(s.value->>'charge_ex_vat','numeric') then (s.value->>'charge_ex_vat')::numeric
            else 0 end charge_ex
        from jsonb_array_elements(segments) s(value)
      ),
      hour_daily as materialized (
        select s.work_date,sum(s.h_day) h_day,sum(s.h_night) h_night,sum(s.h_sat) h_sat,
          sum(s.h_sun) h_sun,sum(s.h_bh) h_bh,sum(s.pay_ex) pay_ex,sum(s.charge_ex) charge_ex
        from segment_rows s
        where c_daily_calc and s.work_date is not null
        group by s.work_date
      ),
      hour_weekly as materialized (
        select sum(s.h_day) h_day,sum(s.h_night) h_night,sum(s.h_sat) h_sat,
          sum(s.h_sun) h_sun,sum(s.h_bh) h_bh,sum(s.pay_ex) pay_ex,sum(s.charge_ex) charge_ex
        from segment_rows s
        having not c_daily_calc or not exists(select 1 from segment_rows d where d.work_date is not null)
      ),
      additional_source as materialized (
        select upper(a.key) code,a.value unit
        from jsonb_each(case when jsonb_typeof(snap.additional_units_json)='object'
          then snap.additional_units_json else '{}'::jsonb end) a
        where jsonb_typeof(a.value)='object' and btrim(a.key)<>''
      ),
      additional_daily as materialized (
        select a.code,left(d.key,10) work_date,
          case when pg_input_is_valid(d.value,'numeric') then d.value::numeric else 0 end units,
          case when pg_input_is_valid(a.unit->>'pay_rate','numeric') then (a.unit->>'pay_rate')::numeric else 0 end pay_rate,
          case when pg_input_is_valid(a.unit->>'charge_rate','numeric') then (a.unit->>'charge_rate')::numeric else 0 end charge_rate,
          coalesce(nullif(a.unit->>'bucket_name',''),a.code) bucket_name,
          coalesce(nullif(a.unit->>'unit_name',''),'units') unit_name,
          a.unit->'frequency' frequency
        from additional_source a
        cross join lateral jsonb_each_text(case when jsonb_typeof(a.unit->'days')='object'
          then a.unit->'days' else '{}'::jsonb end) d
        where c_daily_calc
          and left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          and pg_input_is_valid(d.value,'numeric')
          and d.value::numeric<>0
          and exists(select 1 from segment_rows where work_date is not null)
      ),
      additional_weekly as materialized (
        select a.code,
          case when pg_input_is_valid(a.unit->>'unit_count','numeric') then (a.unit->>'unit_count')::numeric else 0 end units,
          case when pg_input_is_valid(a.unit->>'pay_rate','numeric') then (a.unit->>'pay_rate')::numeric else 0 end pay_rate,
          case when pg_input_is_valid(a.unit->>'charge_rate','numeric') then (a.unit->>'charge_rate')::numeric else 0 end charge_rate,
          case when pg_input_is_valid(a.unit->>'pay_ex_vat','numeric') then (a.unit->>'pay_ex_vat')::numeric else 0 end pay_ex,
          case when pg_input_is_valid(a.unit->>'charge_ex_vat','numeric') then (a.unit->>'charge_ex_vat')::numeric else 0 end charge_ex,
          coalesce(nullif(a.unit->>'bucket_name',''),a.code) bucket_name,
          coalesce(nullif(a.unit->>'unit_name',''),'units') unit_name,
          a.unit->'frequency' frequency
        from additional_source a
        where not(c_daily_calc
          and exists(select 1 from jsonb_each_text(case when jsonb_typeof(a.unit->'days')='object'
            then a.unit->'days' else '{}'::jsonb end) d
            where left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(d.value,'numeric') and d.value::numeric<>0))
          and (
            (case when pg_input_is_valid(a.unit->>'pay_ex_vat','numeric') then (a.unit->>'pay_ex_vat')::numeric else 0 end)<>0
            or (case when pg_input_is_valid(a.unit->>'charge_ex_vat','numeric') then (a.unit->>'charge_ex_vat')::numeric else 0 end)<>0
          )
      ),
      expense_lines as materialized (
        select e.code,e.pay_ex,e.charge_ex
        from (values
          ('TRAVEL',coalesce(snap.travel_pay_ex_vat,0),coalesce(snap.travel_charge_ex_vat,0)),
          ('ACCOMMODATION',coalesce(snap.accommodation_pay_ex_vat,0),coalesce(snap.accommodation_charge_ex_vat,0)),
          ('OTHER',coalesce(snap.other_pay_ex_vat,0),coalesce(snap.other_charge_ex_vat,0)),
          ('EXPENSES_FALLBACK',
            case when coalesce(snap.travel_pay_ex_vat,0)+coalesce(snap.accommodation_pay_ex_vat,0)+coalesce(snap.other_pay_ex_vat,0)=0
              then coalesce(snap.expenses_pay_ex_vat,0) else 0 end,
            case when coalesce(snap.travel_charge_ex_vat,0)+coalesce(snap.accommodation_charge_ex_vat,0)+coalesce(snap.other_charge_ex_vat,0)=0
              then coalesce(snap.expenses_charge_ex_vat,0) else 0 end),
          ('MILEAGE',coalesce(snap.mileage_pay_ex_vat,0),coalesce(snap.mileage_charge_ex_vat,0))
        ) e(code,pay_ex,charge_ex)
        where e.pay_ex<>0 or e.charge_ex<>0
      ),
      line_union as materialized (
        select b.candidate_display||' - '||h.work_date||' - W/E '||coalesce(snap.week_ending_date::text,'' ) description,
          h.h_day,h.h_night,h.h_sat,h.h_sun,h.h_bh,h.pay_ex,h.charge_ex,
          'HOURS_DAILY' line_type,'TS:'||tsid::text||':HOURS:'||h.work_date source_key,
          jsonb_build_object('date',h.work_date) detail
        from hour_daily h cross join base b
        union all
        select b.candidate_display||' - W/E '||coalesce(snap.week_ending_date::text,''),
          h.h_day,h.h_night,h.h_sat,h.h_sun,h.h_bh,h.pay_ex,h.charge_ex,
          'HOURS_WEEKLY','TS:'||tsid::text||':HOURS:WEEK','{}'::jsonb
        from hour_weekly h cross join base b
        union all
        select b.candidate_display||' - '||a.bucket_name||' - '||a.work_date||' - '||a.units||' '||a.unit_name,
          0,0,0,0,0,round(a.units*a.pay_rate,2),round(a.units*a.charge_rate,2),
          'ADDITIONAL_RATE_DAILY','TS:'||tsid::text||':ADD:'||a.code||':'||a.work_date,
          jsonb_build_object('date',a.work_date,'bucket',jsonb_build_object(
            'code',a.code,'bucket_name',a.bucket_name,'unit_name',a.unit_name,'frequency',a.frequency),
            'units',jsonb_build_object('unit_count',a.units,'pay_rate',a.pay_rate,'charge_rate',a.charge_rate))
        from additional_daily a cross join base b
        union all
        select b.candidate_display||' - '||a.bucket_name||' - '||a.units||' '||a.unit_name,
          0,0,0,0,0,a.pay_ex,a.charge_ex,
          'ADDITIONAL_RATE','TS:'||tsid::text||':ADD:'||a.code||':WEEK',
          jsonb_build_object('bucket',jsonb_build_object(
            'code',a.code,'bucket_name',a.bucket_name,'unit_name',a.unit_name,'frequency',a.frequency),
            'units',jsonb_build_object('unit_count',a.units,'pay_rate',a.pay_rate,'charge_rate',a.charge_rate))
        from additional_weekly a cross join base b
        union all
        select case when e.code='MILEAGE' then 'Mileage - '||coalesce(snap.mileage_units,0)||' miles (W/E '||coalesce(snap.week_ending_date::text,'')||')'
          when e.code='EXPENSES_FALLBACK' then 'Expenses (W/E '||coalesce(snap.week_ending_date::text,'')||')'
          else initcap(replace(e.code,'_',' '))||' expenses (W/E '||coalesce(snap.week_ending_date::text,'')||')' end,
          0,0,0,0,0,e.pay_ex,e.charge_ex,
          case when e.code='MILEAGE' then 'MILEAGE'
            when e.code='EXPENSES_FALLBACK' then 'EXPENSES_TOTAL'
            else 'EXPENSE_'||e.code end,
          case when e.code='MILEAGE' then 'TS:'||tsid::text||':MILEAGE'
            when e.code='EXPENSES_FALLBACK' then 'TS:'||tsid::text||':EXP:TOTAL'
            else 'TS:'||tsid::text||':EXP:'||e.code end,
          jsonb_build_object('expense',case when e.code='MILEAGE' then jsonb_build_object(
            'category','MILEAGE','mileage_units',snap.mileage_units,
            'pay_rate',snap.mileage_pay_rate,'charge_rate',snap.mileage_charge_rate,
            'evidence_r2_key',snap.mileage_evidence_r2_key,'evidence_manifest',snap.mileage_evidence_manifest)
            else jsonb_build_object('category',e.code,'note',snap.expenses_description,
              'evidence_r2_key',snap.expenses_evidence_r2_key,'evidence_manifest',snap.expenses_evidence_manifest) end)
        from expense_lines e
      )
      select p_invoice_id,tsid,snap.booking_id,l.description,
        round(l.h_day,2),round(l.h_night,2),round(l.h_sat,2),round(l.h_sun,2),round(l.h_bh,2),
        null,null,null,null,null,
        snap.charge_day,snap.charge_night,snap.charge_sat,snap.charge_sun,snap.charge_bh,
        round(l.pay_ex,2),round(l.charge_ex,2),round(l.charge_ex-l.pay_ex,2),v_vat_rate,
        round(l.charge_ex*v_vat_rate/100,2),round(l.charge_ex+(l.charge_ex*v_vat_rate/100),2),
        v_exact_timesheet_document_r2_key,
        jsonb_build_object('line_type',l.line_type,'timesheet_id',tsid,'tsfin_id',snap.id,
          'candidate_display',l.description,'week_ending_date',snap.week_ending_date,
          'role',c_role,'hospital',c_display_site,'ward',c_ward_hint,'bucket_labels',c_bucket_labels,
          'totals',jsonb_build_object('line_pay_ex_vat',round(l.pay_ex,2),
            'line_charge_ex_vat',round(l.charge_ex,2),'margin_ex_vat',round(l.charge_ex-l.pay_ex,2),
            'vat_rate_pct',v_vat_rate,'vat_amount',round(l.charge_ex*v_vat_rate/100,2),
            'total_inc_vat',round(l.charge_ex+(l.charge_ex*v_vat_rate/100),2)))||l.detail,
        l.source_key
      from line_union l
      on conflict(invoice_id,source_key) do nothing;
      -- Build segment refs to lock
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
      then
        for v_seg in
          select value from jsonb_array_elements(segments) value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_refs := seg_refs || jsonb_build_array(
            jsonb_build_object(
              'tsfin_id', snap.id::text,
              'segment_id', nullif(btrim(coalesce(v_seg->>'segment_id','')), '')
            )
          );
        end loop;
      else
        -- lock whole for non-segments
        seg_refs := seg_refs || jsonb_build_array(
          jsonb_build_object('tsfin_id', snap.id::text, 'segment_id', null)
        );
      end if;
    end loop;

    -- Apply segment locks for the exact selected source rows.
    if jsonb_typeof(seg_refs) = 'array' and jsonb_array_length(seg_refs) > 0 then
      perform public._inv_lock_segments_for_invoice(p_invoice_id, seg_refs);
    end if;

    -- History: timesheets added (always; includes requested ids)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_TIMESHEETS_ADDED',
      null,
      jsonb_build_object('add_timesheet_ids', coalesce(to_jsonb(v_add_ts_ids), '[]'::jsonb)),
      null,
      p_actor_user_id
    );
  end if;

  -- 3c) Contract week status: set INVOICED only when timesheet is FULLY invoiced (segment-aware), and revert INVOICED -> AUTHORISED if no longer fully invoiced.
  -- Touch set = union of: add_timesheet_ids, line-removal touched (hours), segment-op touched.
  v_cw_ts_ids := array[]::uuid[];
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_add_ts_ids;
  end if;
  if v_ts_ids_touched is not null and coalesce(array_length(v_ts_ids_touched,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_ts_ids_touched;
  end if;
  if v_seg_ts_ids is not null and coalesce(array_length(v_seg_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_seg_ts_ids;
  end if;

  -- de-dup
  select array_agg(distinct x)
  into v_cw_ts_ids
  from unnest(coalesce(v_cw_ts_ids, array[]::uuid[])) x
  where x is not null;

  v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

  if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
    with src as (
      select
        cw.timesheet_id,
        cw.status as cw_status,
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json,
        (
          case
            when tf.invoice_breakdown_json is not null
             and jsonb_typeof(tf.invoice_breakdown_json)='object'
             and coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS'
             and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
             and jsonb_array_length(tf.invoice_breakdown_json->'segments') > 0
            then
              not exists (
                select 1
                from jsonb_array_elements(tf.invoice_breakdown_json->'segments') s(seg)
                where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is null
              )
            else
              (tf.locked_by_invoice_id is not null)
          end
        ) as fully_invoiced
      from public.contract_weeks cw
      join public.timesheets_financials tf
        on tf.is_current = true
       and tf.timesheet_id = cw.timesheet_id
      where cw.timesheet_id = any(v_cw_ts_ids)
    )
    update public.contract_weeks cw
    set status = case
      when src.fully_invoiced then 'INVOICED'::public.contract_week_status_enum
      when cw.status = 'INVOICED'::public.contract_week_status_enum then 'AUTHORISED'::public.contract_week_status_enum
      else cw.status
    end
    from src
    where cw.timesheet_id = src.timesheet_id;
  end if;


  -- 4) Recompute invoice totals from invoice_lines and clear PDF key
  select
    public._inv_round2(coalesce(sum(coalesce(l.total_charge_ex_vat,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.vat_amount,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.total_inc_vat,0)),0))
  into v_new_ex, v_new_vat, v_new_inc
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;


  perform public.invoice_recompute_totals(p_invoice_id);

  -- Recompute header_snapshot_json.meta counters to avoid stale values after edits
  select count(distinct l.timesheet_id)
  into v_hdr_ts_count_lines
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  select count(*)
  into v_hdr_seg_locked_count
  from public.timesheets_financials tf
  cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
  where tf.is_current = true
    and coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text;

  select count(distinct tf.timesheet_id)
  into v_hdr_ts_count_seglocks
  from public.timesheets_financials tf
  where tf.is_current = true
    and exists (
      select 1
      from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
      where coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text
    );

  if coalesce(v_hdr_seg_locked_count,0) > 0 then
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_seglocks,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_seg_locked_count,0);
  else
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_lines,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_meta_timesheet_count,0);
  end if;

  -- The statement triggers are the sole document/issue invalidation authority.
  -- This update changes only the authoritative business snapshot counters.
  update public.invoices invu
  set header_snapshot_json=jsonb_set(jsonb_set(coalesce(invu.header_snapshot_json,'{}'::jsonb),
      '{meta,timesheet_count}',to_jsonb(v_hdr_meta_timesheet_count),true),
      '{meta,segment_count}',to_jsonb(v_hdr_meta_segment_count),true),
    updated_at=now()
  where invu.id=p_invoice_id;

-- Refresh invoice-level NHSP/HR cache after timesheet/segment changes
  if coalesce(v_refresh_hr_cache,false) = true then
    perform 1
    from public.invoice_source_rows_collect(p_invoice_id, true) sr
    limit 1;
  end if;

  -- Revalidate ownership after every line and segment move in this Save. A
  -- source edit must not queue work for a source that was detached later in the
  -- same transaction, and every edited SEGMENTS identity must still be locked
  -- to this exact invoice.
  if cardinality(v_source_changed_ts_ids)>0 and exists(
    select 1
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_id')::uuid=any(v_source_changed_ts_ids)
      and not exists(
        select 1 from public.invoice_lines carrier
        where carrier.invoice_id=p_invoice_id
          and (
            carrier.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              carrier.timesheet_id is null
              and coalesce(carrier.meta_json->>'timesheet_id','')~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (carrier.meta_json->>'timesheet_id')::uuid=
                (desired.value->>'timesheet_id')::uuid
            )
          )
      )
      and not exists(
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(
          coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
        where tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
          and tf.is_current
          and nullif(btrim(coalesce(
            seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
      )
  ) then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND',
      detail=jsonb_build_object('reason','POST_EDIT_SOURCE_DETACHED')::text;
  end if;

  if cardinality(v_source_changed_ts_ids)>0 and exists(
    select 1
    from jsonb_each(v_source_updates_map) desired
    cross join lateral jsonb_array_elements(
      coalesce(desired.value->'segment_updates','[]'::jsonb)) requested(value)
    where desired.value->>'source_mode'='SEGMENTS'
      and not exists(
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(
          coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
        where tf.id=(desired.value->>'financial_id')::uuid
          and tf.is_current
          and tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
          and nullif(btrim(coalesce(
            seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
          and (
            (
              nullif(requested.value->>'segment_id','') is not null
              and nullif(btrim(coalesce(seg.value->>'segment_id','')),'')
                =nullif(requested.value->>'segment_id','')
            ) or (
              nullif(requested.value->>'segment_id','') is null
              and coalesce(seg.value->>'start_utc',seg.value->>'start')
                is not distinct from requested.value->>'start_identity'
              and coalesce(seg.value->>'end_utc',seg.value->>'end')
                is not distinct from requested.value->>'end_identity'
            )
          )
      )
  ) then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND',
      detail=jsonb_build_object('reason','POST_EDIT_SEGMENT_DETACHED')::text;
  end if;

  -- A material source edit always starts or reuses the exact replacement
  -- timesheet work. Replacement of an existing invoice preview is derived from
  -- the locked pre-edit V8 state; browser flags cannot suppress it.
  if cardinality(v_source_changed_ts_ids)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'command_type','VIEW_TIMESHEET_DOCUMENT',
      'timesheet_id',changed_id,
      'purpose','TIMESHEET',
      'priority_reason','SOURCE_EDIT_REPLACEMENT',
      'template_version','timesheet-professional-v2')
      order by changed_id),'[]'::jsonb)
    into v_document_commands
    from unnest(v_source_changed_ts_ids) changed_id;

    v_source_edit_invoice_replacement_required:=
      v_source_edit_preexisting_preview
      or lower(coalesce(p_payload->>'request_preview','false'))
           in('true','1','yes','on');
    if v_source_edit_invoice_replacement_required then
      v_document_commands:=v_document_commands||jsonb_build_array(
        jsonb_build_object(
          'command_type','VIEW_INVOICE_DOCUMENT',
          'invoice_id',p_invoice_id,
          'purpose','DRAFT_PREVIEW',
          'priority_reason',case when v_source_edit_preexisting_preview
            then 'SOURCE_EDIT_REPLACEMENT' else 'VIEW_NOW' end,
          'template_version','invoice-professional-v2'));
    end if;

    v_expected_command_count:=jsonb_array_length(v_document_commands);
    if v_expected_command_count>1000 then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_TOO_MANY_REPLACEMENTS';
    end if;

    v_started_operations:=public.invoice_operation_start_batch(
      v_document_commands,p_actor_user_id,v_now);

    if jsonb_typeof(v_started_operations)<>'array'
       or jsonb_array_length(v_started_operations)<>v_expected_command_count then
      raise exception using errcode='55000',
        message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
        detail=jsonb_build_object(
          'reason','RESULT_COUNT',
          'expected',v_expected_command_count,
          'actual',case when jsonb_typeof(v_started_operations)='array'
            then jsonb_array_length(v_started_operations) else null end)::text;
    end if;

    for v_command_no in 1..v_expected_command_count loop
      select value into v_operation_result
      from jsonb_array_elements(v_started_operations) value
      where coalesce(value->>'command_no','')~'^[1-9][0-9]*$'
        and (value->>'command_no')::int=v_command_no;

      if not found or (
        select count(*)
        from jsonb_array_elements(v_started_operations) value
        where coalesce(value->>'command_no','')~'^[1-9][0-9]*$'
          and (value->>'command_no')::int=v_command_no
      )<>1 then
        raise exception using errcode='55000',
          message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
          detail=jsonb_build_object('reason','COMMAND_NO','command_no',v_command_no)::text;
      end if;

      v_source_update:=v_document_commands->(v_command_no-1);
      v_command_type:=v_source_update->>'command_type';
      if v_operation_result->>'command_type' is distinct from v_command_type
         or lower(coalesce(v_operation_result->>'accepted','false'))
              not in('true','1','yes','on')
         or lower(coalesce(v_operation_result->>'blocked','false'))
              in('true','1','yes','on') then
        raise exception using errcode='55000',
          message=case
            when lower(coalesce(v_operation_result->>'blocked','false'))
                   in('true','1','yes','on')
              then 'SOURCE_EDIT_REPLACEMENT_BLOCKED'
            else 'SOURCE_EDIT_REPLACEMENT_REJECTED' end,
          detail=jsonb_build_object(
            'command_no',v_command_no,
            'terminal_error',v_operation_result->'terminal_error')::text;
      end if;

      v_operation_status:=upper(coalesce(v_operation_result->>'status',''));
      v_operation_id:=case when coalesce(v_operation_result->>'operation_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (v_operation_result->>'operation_id')::uuid end;
      v_document_version_id:=case when coalesce(v_operation_result->>'document_version_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (v_operation_result->>'document_version_id')::uuid end;

      if v_operation_status='READY' then
        if v_document_version_id is null or v_operation_id is not null
           or not exists(
             select 1
             from public.invoice_document_versions dv
             where dv.id=v_document_version_id
               and dv.entity_type=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'INVOICE' end
               and dv.entity_id=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (v_source_update->>'timesheet_id')::uuid else p_invoice_id end
               and dv.purpose=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'DRAFT_PREVIEW' end
               and dv.source_revision=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (
                   select t.document_revision::text from public.timesheets t
                   where t.timesheet_id=(v_source_update->>'timesheet_id')::uuid
                     and t.is_current)
                 else (
                   select i.document_revision::text from public.invoices i
                   where i.id=p_invoice_id)
                 end
               and dv.template_version=v_source_update->>'template_version'
               and dv.status='READY'
               and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
               and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
               and coalesce(dv.size_bytes,0)>0
               and coalesce(dv.page_count,0)>0
           ) then
          raise exception using errcode='55000',
            message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
            detail=jsonb_build_object(
              'reason','READY_IDENTITY','command_no',v_command_no)::text;
        end if;
      else
        if v_operation_id is null
           or v_operation_status not in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           or not exists(
             select 1
             from public.invoice_operations op
             where op.id=v_operation_id
               and op.operation_type='BUILD_DOCUMENT'
               and op.entity_type=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'INVOICE' end
               and op.entity_id=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (v_source_update->>'timesheet_id')::uuid else p_invoice_id end
               and op.source_revision=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (
                   select t.document_revision::text from public.timesheets t
                   where t.timesheet_id=(v_source_update->>'timesheet_id')::uuid
                     and t.is_current)
                 else (
                   select i.document_revision::text from public.invoices i
                   where i.id=p_invoice_id)
                 end
               and op.template_version=v_source_update->>'template_version'
               and coalesce(op.input_json->>'purpose','')=
                 case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                   then 'TIMESHEET' else 'DRAFT_PREVIEW' end
               and op.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           ) then
          raise exception using errcode='55000',
            message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
            detail=jsonb_build_object(
              'reason','ACTIVE_IDENTITY','command_no',v_command_no)::text;
        end if;
      end if;

      v_validated_operations:=v_validated_operations||jsonb_build_array(v_operation_result);
    end loop;

    select coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,
      'document_revision',t.document_revision)
      order by t.timesheet_id),'[]'::jsonb)
    into v_source_changed_revisions
    from public.timesheets t
    where t.timesheet_id=any(v_source_changed_ts_ids) and t.is_current;
  end if;

-- Return compact state only; never build a manifest or PDF here.
  select jsonb_build_object('invoice_id',i.id,'status',i.status,
    'subtotal_ex_vat',i.subtotal_ex_vat,'vat_amount',i.vat_amount,'total_inc_vat',i.total_inc_vat,
    'document_revision',i.document_revision,'document_state',i.document_state,
    'preview_document_version_id',i.preview_document_version_id,
    'active_document_operation_id',i.active_document_operation_id,
    'issue_state',i.issue_state,'active_issue_operation_id',i.active_issue_operation_id,
    'reference_updates_applied',v_refupd_applied,
    'timesheet_location_updates_applied',v_location_applied,
    'timesheet_source_changed',
      cardinality(v_source_changed_ts_ids)>0,
    'source_edit_queue_contract',case when v_source_edit_requested
      then 'INVOICE_SOURCE_EDIT_QUEUE_V1' else null end,
    'changed_timesheet_ids',coalesce(to_jsonb(v_source_changed_ts_ids),'[]'::jsonb),
    'changed_timesheet_revisions',coalesce(v_source_changed_revisions,'[]'::jsonb),
    'source_edit_preexisting_preview',v_source_edit_preexisting_preview,
    'invoice_replacement_required',v_source_edit_invoice_replacement_required,
    'accepted_operations',coalesce(v_validated_operations,'[]'::jsonb),
    'document_queue_requested',jsonb_array_length(v_document_commands)>0)
  into v_manifest from public.invoices i where i.id=p_invoice_id;

  if v_invoice_debug then
    begin
      -- attach finish marker (avoid extra heavy queries here)
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'invoice_total_charge_ex_vat', v_new_ex,
          'invoice_vat_amount', v_new_vat,
          'invoice_total_inc_vat', v_new_inc
        )
      );

      v_dbg_stats := v_dbg_stats || jsonb_build_object(
        'lines_deleted', v_dbg_lines_deleted,
        'timesheets_unlocked_via_line_removal', v_dbg_timesheets_unlocked,
        'segment_add_refs', v_dbg_seg_add_refs,
        'segment_remove_refs', v_dbg_seg_remove_refs,
        'segment_tsfin_count', v_dbg_seg_tsfins,
        'segment_timesheets_rebuilt', v_dbg_seg_timesheets_rebuilt,
        'segment_timesheets_removed', v_dbg_seg_timesheets_removed,
        'add_timesheets_found', v_dbg_add_timesheets_found,
        'add_timesheets_skipped', v_dbg_add_timesheets_skipped
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_DEBUG',
        jsonb_build_object(
          'invoice_id', p_invoice_id::text,
          'week_start', v_week_start::text,
          'week_end', v_week_end::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,false,'INVOICE_APPLY_EDITS_RESULT'
  );

  return v_manifest;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        coalesce(p_invoice_id::text,''),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- public.invoice_correction_pair_scope_v1(p_timesheet_id uuid, p_target_invoice_id uuid, p_actor_user_id uuid, p_lock_rows boolean, p_max_members integer) pre-install MD5 ec4f3729311c3dd687169596ec0d99c1
CREATE OR REPLACE FUNCTION public.invoice_correction_pair_scope_v1(p_timesheet_id uuid, p_target_invoice_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_lock_rows boolean DEFAULT true, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_chain jsonb;
  v_unit jsonb;
  v_envelope jsonb;
  v_ids uuid[]:=array[]::uuid[];
  v_expected_count integer;
  v_ready_count integer:=0;
  v_line_member_count integer:=0;
  v_line_invoice_count integer:=0;
  v_client_count integer:=0;
  v_contract_count integer:=0;
  v_week_count integer:=0;
  v_stream_count integer:=0;
  v_target public.invoices%rowtype;
  v_rows jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  r record;
  v_leg jsonb;
  v_policy_ready boolean;
  v_expected_stream text;
  v_current_stream text;
  v_target_stream text;
  v_line_policy_mismatch_count integer:=0;
  v_compatibility_mode text;
  v_operation public.import_apply_operations%rowtype;
  v_operation_unit jsonb;
  v_operation_unit_count integer:=0;
  v_balance jsonb;
  v_balance_row_count integer:=0;
  v_timesheet public.timesheets%rowtype;
  v_pair_correction_id text;
  v_pair_parent_id uuid;
  v_pair_operation_id uuid;
  v_pair_unit_fingerprint text;
  v_pair_source_identity text;
  v_pair_envelope_count integer:=0;
  v_pair_parent_count integer:=0;
  v_pair_member_count integer:=0;
  v_pair_reversal_count integer:=0;
  v_pair_replacement_count integer:=0;
  v_missing_ids uuid[]:=array[]::uuid[];
  v_recomputed_unit_fingerprint text;
begin
  if p_timesheet_id is null then raise exception 'INVOICE_CORRECTION_TIMESHEET_ID_REQUIRED' using errcode='22023'; end if;
  if p_max_members<1 or p_max_members>100 then raise exception 'INVOICE_CORRECTION_MEMBER_LIMIT_INVALID' using errcode='22023'; end if;
  if p_actor_user_id is not null then
    perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
    if not found then raise exception 'INVOICE_CORRECTION_ACTOR_INVALID' using errcode='42501'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    -- The ordinary whole-chain validator remains authoritative.  This narrowly
    -- gated fallback exists only for a current pair committed by the reviewed
    -- authoritative-import reconciliation path when an older issued row was
    -- physically removed but its frozen invoice evidence remains provable.
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current and archived_at_utc is null;
    if v_timesheet.timesheet_id is null
       or not coalesce(v_timesheet.is_adjustment,false)
       or upper(coalesce(v_timesheet.adjustment_origin,''))<>'IMPORT_CORRECTION'
       or v_timesheet.correction_kind not in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
       or v_timesheet.correction_id is null
       or coalesce(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    v_pair_correction_id:=v_timesheet.correction_id;
    v_pair_operation_id:=(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}')::uuid;
    v_pair_unit_fingerprint:=nullif(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}','');
    v_pair_source_identity:=nullif(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}','');

    select * into v_operation from public.import_apply_operations o
    where o.id=v_pair_operation_id and o.state='COMPLETE' and o.committed_at_utc is not null;
    if v_operation.id is null then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    select count(*)::integer,min(u::text)::jsonb into v_operation_unit_count,v_operation_unit
    from jsonb_array_elements(case when jsonb_typeof(v_operation.response_json->'reconciliation_units')='array'
      then v_operation.response_json->'reconciliation_units' else '[]'::jsonb end) u
    where u->>'schema_version'='IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
      and u->>'correction_id'=v_pair_correction_id
      and u->>'source_identity'=v_pair_source_identity
      and u->>'unit_fingerprint'=v_pair_unit_fingerprint
      and u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT');
    if v_operation_unit_count<>1 then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;

    select public._import_review_hash_v1(concat_ws('|','unit-v1',
      v_operation_unit->>'action_id',v_operation_unit->>'source_identity',v_operation_unit->>'route',
      v_operation_unit->>'reconciliation_fingerprint',outcome.evidence_fingerprint))
    into v_recomputed_unit_fingerprint
    from public.import_review_action_outcomes outcome
    where outcome.operation_id=v_operation.id and outcome.action_id=v_operation_unit->>'action_id';
    if v_recomputed_unit_fingerprint is distinct from v_operation_unit->>'unit_fingerprint' then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    select count(*)::integer,
      count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')::integer,
      count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')::integer,
      count(distinct t.parent_timesheet_id)::integer,
      min(t.parent_timesheet_id),
      count(distinct coalesce(
        t.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
        tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
      ))::integer,
      coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[])
    into v_pair_member_count,v_pair_reversal_count,v_pair_replacement_count,
      v_pair_parent_count,v_pair_parent_id,v_pair_envelope_count,v_ids
    from public.timesheets t
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.correction_id=v_pair_correction_id and t.is_current and t.archived_at_utc is null
      and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and t.adjustment_origin='IMPORT_CORRECTION'
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'=v_operation.id::text
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'=v_pair_unit_fingerprint
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}'=v_pair_source_identity;
    if v_pair_member_count<>2 or v_pair_reversal_count<>1 or v_pair_replacement_count<>1
       or v_pair_parent_count<>1 or v_pair_parent_id is null or v_pair_envelope_count<>1
       or v_pair_parent_id is distinct from (v_operation_unit->>'parent_timesheet_id')::uuid
       or not exists(select 1 from public.timesheets parent_ts where parent_ts.timesheet_id=v_pair_parent_id
          and parent_ts.is_current and parent_ts.archived_at_utc is null) then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    if exists(select 1 from public.timesheets t left join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id=any(v_ids) and (
        t.contract_id is distinct from (v_operation_unit->>'contract_id')::uuid
        or t.week_ending_date is distinct from (v_operation_unit->>'week_ending_date')::date
        or tf.candidate_id is distinct from (v_operation_unit->>'candidate_id')::uuid
        or tf.client_id is distinct from (v_operation_unit->>'client_id')::uuid
        or jsonb_typeof(t.actual_schedule_json)<>'array'
        or jsonb_array_length(t.actual_schedule_json)<>1
        or not t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
          'shift_id',v_operation_unit->>'source_shift_id','external_row_key',v_operation_unit->>'source_identity'))
        or (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
          (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_operation_unit#>>'{B_standard_schedule_json,0,start_utc}')::timestamptz
          or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_operation_unit#>>'{B_standard_schedule_json,0,end_utc}')::timestamptz
          or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_operation_unit#>>'{B_standard_schedule_json,0,break_mins}')::integer,0)))
        or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
          (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_operation_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
          or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_operation_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
          or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_operation_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)))
      )) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    select coalesce(array_agg(x.value::uuid order by x.value::uuid),array[]::uuid[]) into v_missing_ids
    from jsonb_array_elements_text(coalesce(v_operation_unit->'historical_missing_timesheet_ids','[]'::jsonb)) x(value);
    if cardinality(v_missing_ids)=0
       or exists(select 1 from unnest(v_missing_ids) missing_id
          where exists(select 1 from public.timesheets t where t.timesheet_id=missing_id)
             or not exists(select 1 from public.invoice_lines il join public.invoices i on i.id=il.invoice_id
               where (il.timesheet_id=missing_id or il.meta_json->>'timesheet_id'=missing_id::text)
                 and i.status in ('ISSUED','PAID','ON_HOLD') and i.issued_at_utc is not null)
             or not exists(select 1 from public.audit_events ae where ae.object_type='timesheets'
               and ae.object_id_text=missing_id::text
               and ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')))
       or exists(select 1 from jsonb_array_elements(coalesce(v_chain->'errors','[]'::jsonb)) e
          where e->>'code'<>'CORRECTION_UNIT_INVALID') then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;

    select count(*)::integer,min(b.balance_json::text)::jsonb into v_balance_row_count,v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      v_operation.import_id,
      jsonb_build_array(jsonb_build_object(
        'source_identity',v_operation_unit->>'source_identity','source_system',v_operation_unit->>'source_system',
        'source_shift_id',v_operation_unit->>'source_shift_id','external_row_key',v_operation_unit->>'source_identity',
        'hr_row_id',v_operation_unit->>'hr_row_id','source_timesheet_id',v_operation_unit->>'source_timesheet_id',
        'candidate_id',v_operation_unit->>'candidate_id','client_id',v_operation_unit->>'client_id',
        'contract_id',v_operation_unit->>'contract_id','week_ending_date',v_operation_unit->>'week_ending_date',
        'invoice_stream',v_operation_unit->>'invoice_stream','authoritative_import_id',v_operation.import_id,
        'authoritative_schedule_json',v_operation_unit->'A_schedule_json','authoritative_hours',v_operation_unit->'A_hours'
      )),1,512,256,128
    ) b;
    if v_balance_row_count<>1 or nullif(v_balance->>'blocking_code','') is not null
       or v_balance->>'effective_invoice_fingerprint' is distinct from v_operation_unit->>'B_invoice_fingerprint'
       or v_balance->'historical_missing_timesheet_ids' is distinct from to_jsonb(v_missing_ids) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;
    if exists(select 1 from public.timesheets t join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id=any(v_ids) and (
        coalesce(tf.is_stale,true) or coalesce(tf.has_rate_issue,false) or coalesce(tf.has_pay_channel_issue,false)
        or (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
          tf.hours_day<>-coalesce((v_operation_unit#>>'{B_hours,hours_day}')::numeric,0)
          or tf.hours_night<>-coalesce((v_operation_unit#>>'{B_hours,hours_night}')::numeric,0)
          or tf.hours_sat<>-coalesce((v_operation_unit#>>'{B_hours,hours_sat}')::numeric,0)
          or tf.hours_sun<>-coalesce((v_operation_unit#>>'{B_hours,hours_sun}')::numeric,0)
          or tf.hours_bh<>-coalesce((v_operation_unit#>>'{B_hours,hours_bh}')::numeric,0)
          or tf.total_pay_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
          or tf.total_charge_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)
          or tf.margin_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)))
        or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
          tf.hours_day<>coalesce((v_operation_unit#>>'{A_hours,hours_day}')::numeric,0)
          or tf.hours_night<>coalesce((v_operation_unit#>>'{A_hours,hours_night}')::numeric,0)
          or tf.hours_sat<>coalesce((v_operation_unit#>>'{A_hours,hours_sat}')::numeric,0)
          or tf.hours_sun<>coalesce((v_operation_unit#>>'{A_hours,hours_sun}')::numeric,0)
          or tf.hours_bh<>coalesce((v_operation_unit#>>'{A_hours,hours_bh}')::numeric,0)))
      )) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    v_envelope:=public._ctms_correction_policy_envelope_read_v1(p_timesheet_id);
    v_unit:=jsonb_build_object('valid',true,'correction_id',v_pair_correction_id,
      'correction_shape','REVERSAL_REPLACEMENT','expected_member_count',2,
      'member_ids',to_jsonb(v_ids),'policy_envelope',v_envelope);
    v_chain:=jsonb_build_object('root_timesheet_id',v_envelope->>'root_timesheet_id');
    v_compatibility_mode:='IMPORT_AUTHORITATIVE_RECONCILIATION_V1';
  else
    v_unit:=v_chain->'requested_correction_unit';
  end if;
  if jsonb_typeof(v_unit)<>'object' or coalesce((v_unit->>'valid')::boolean,false) is not true then
    raise exception 'INVOICE_CORRECTION_UNIT_INVALID' using errcode='P0001';
  end if;
  v_envelope:=v_unit->'policy_envelope';
  v_expected_stream:=upper(btrim(coalesce(v_envelope->>'invoice_stream','')));
  if v_expected_stream not in ('NORMAL','SELF_BILL') then
    raise exception 'INVOICE_CORRECTION_FROZEN_STREAM_INVALID' using errcode='P0001';
  end if;
  v_expected_count:=(v_unit->>'expected_member_count')::integer;
  select coalesce(array_agg(value::uuid order by value::text),array[]::uuid[])
  into v_ids from jsonb_array_elements_text(v_unit->'member_ids');
  if cardinality(v_ids)<>v_expected_count then raise exception 'INVOICE_CORRECTION_MEMBER_COUNT_MISMATCH' using errcode='P0001'; end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts where ts.timesheet_id=any(v_ids) order by ts.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.timesheet_id=any(v_ids) and tf.is_current=true
      order by tf.timesheet_id,tf.id for update;
  end if;

  if p_target_invoice_id is not null then
    if p_lock_rows then select * into v_target from public.invoices where id=p_target_invoice_id for update;
    else select * into v_target from public.invoices where id=p_target_invoice_id; end if;
    if not found then raise exception 'INVOICE_CORRECTION_TARGET_NOT_FOUND' using errcode='P0002'; end if;
    if upper(coalesce(v_target.status::text,''))<>'DRAFT' or v_target.issued_at_utc is not null then
      raise exception 'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE' using errcode='P0001',
        detail=jsonb_build_object('invoice_id',p_target_invoice_id,'status',v_target.status,'issued_at_utc',v_target.issued_at_utc)::text;
    end if;
    v_target_stream:=case
      when lower(coalesce(v_target.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
        then 'SELF_BILL' else 'NORMAL' end;
    if v_target_stream is distinct from v_expected_stream then
      v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
        'code','INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
        'expected_stream',v_expected_stream,
        'target_stream',v_target_stream
      ));
    end if;
  end if;

  for r in
    select ts.timesheet_id,ts.correction_kind,ts.contract_id,ts.week_ending_date,
      tf.id tsfin_id,tf.client_id,tf.basis,tf.processing_status,tf.is_stale,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      c.self_bill
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contracts c on c.id=ts.contract_id
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_current_stream:=case
      when upper(coalesce(r.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end;
    v_policy_ready:=
      coalesce(r.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
               r.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
               r.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint')
        is not distinct from v_envelope->>'envelope_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_leg_fingerprint',r.rate_source_refs_json->>'correction_leg_fingerprint')
        is not distinct from v_leg->>'leg_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',r.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
        is not distinct from v_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
      and coalesce(r.policy_snapshot_json->>'correction_invoice_policy_fingerprint',r.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
        is not distinct from v_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
      and r.policy_snapshot_json->'correction_invoice_policy'
        is not distinct from v_leg->'invoice_policy'
      and upper(btrim(coalesce(r.policy_snapshot_json->>'correction_invoice_stream','')))
        is not distinct from v_expected_stream
      and upper(btrim(coalesce(v_leg#>>'{invoice_policy,invoice_stream}','')))
        is not distinct from v_expected_stream
      and v_current_stream is not distinct from v_expected_stream;
    if r.tsfin_id is not null and not coalesce(r.is_stale,false)
       and r.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
       and v_policy_ready then v_ready_count:=v_ready_count+1; end if;
    if not v_policy_ready then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_POLICY_NOT_FROZEN','timesheet_id',r.timesheet_id)); end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'correction_kind',r.correction_kind,'tsfin_id',r.tsfin_id,
      'client_id',r.client_id,'contract_id',r.contract_id,'week_ending_date',r.week_ending_date,
       'invoice_stream',v_expected_stream,'current_contract_stream',v_current_stream,
       'processing_status',r.processing_status,'policy_ready',v_policy_ready,
       'invoice_vat_chargeable',v_leg#>'{invoice_policy,invoice_vat_chargeable}',
       'invoice_vat_rate_pct',v_leg#>'{invoice_policy,applied_vat_rate_pct}',
       'invoice_policy_fingerprint',v_leg#>>'{invoice_policy,invoice_policy_fingerprint}',
       'leg_fingerprint',v_leg->>'leg_fingerprint'));
  end loop;

  select count(distinct tf.client_id),count(distinct ts.contract_id),count(distinct ts.week_ending_date),
    count(distinct case
      when upper(coalesce(tf.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end)
  into v_client_count,v_contract_count,v_week_count,v_stream_count
  from public.timesheets ts
  join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  where ts.timesheet_id=any(v_ids);

  select count(distinct il.timesheet_id),count(distinct il.invoice_id)
  into v_line_member_count,v_line_invoice_count
  from public.invoice_lines il where il.timesheet_id=any(v_ids);

  select count(*)::integer into v_line_policy_mismatch_count
  from public.invoice_lines il
  cross join lateral (
    select public._ctms_correction_policy_leg_read_v1(il.timesheet_id) leg
  ) expected
  where il.timesheet_id=any(v_ids)
    and (p_target_invoice_id is null or il.invoice_id=p_target_invoice_id)
    and il.vat_rate_pct is distinct from
      (expected.leg#>>'{invoice_policy,applied_vat_rate_pct}')::numeric;

  if v_ready_count<>v_expected_count then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TSFIN_NOT_READY','ready_count',v_ready_count)); end if;
  if v_client_count<>1 or v_contract_count<>1 or v_week_count<>1 or v_stream_count<>1 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_SCOPE_MIXED'));
  end if;
  if exists (
    select 1
    from public.timesheets ts
    join public.timesheets_financials tf
      on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    where ts.timesheet_id=any(v_ids)
      and (case
        when upper(coalesce(tf.basis::text,'')) in (
          'NHSP','NHSP_ADJUSTMENT',
          'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
        ) then 'SELF_BILL'
        else 'NORMAL'
      end)
        is distinct from v_expected_stream
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_FROZEN_STREAM_DRIFT',
      'expected_stream',v_expected_stream
    ));
  end if;
  if v_line_member_count not in (0,v_expected_count) or (v_line_member_count=v_expected_count and v_line_invoice_count<>1) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_UNIT_SPLIT'));
  end if;
  if v_line_policy_mismatch_count<>0 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_LINE_VAT_POLICY_MISMATCH',
      'mismatching_line_count',v_line_policy_mismatch_count
    ));
  end if;
  if p_target_invoice_id is not null and v_target.client_id is distinct from (select tf.client_id from public.timesheets_financials tf where tf.timesheet_id=v_ids[1] and tf.is_current=true) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH'));
  end if;

  return jsonb_build_object(
    'ok',true,'valid',jsonb_array_length(v_errors)=0,'root_timesheet_id',v_chain->>'root_timesheet_id',
    'correction_id',v_unit->>'correction_id','correction_shape',v_unit->>'correction_shape',
    'expected_member_count',v_expected_count,'pair_timesheet_ids',to_jsonb(v_ids),
    'target_invoice_id',p_target_invoice_id,'target_appendable',p_target_invoice_id is null or jsonb_array_length(v_errors)=0,
    'correction_financials_policy_envelope',v_envelope,
    'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
    'invoice_stream',v_expected_stream,
    'pair_rows',v_rows,'ready_count',v_ready_count,'existing_line_member_count',v_line_member_count,
    'existing_line_invoice_count',v_line_invoice_count,
    'line_policy_mismatch_count',v_line_policy_mismatch_count,'errors',v_errors,
    'compatibility_mode',v_compatibility_mode);
end;
$function$;

-- public.timesheet_archive_state_v1(p_timesheet_id uuid) pre-install MD5 24e61743268e2f748fe29ce72a080117
CREATE OR REPLACE FUNCTION public.timesheet_archive_state_v1(p_timesheet_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_requested public.timesheets%ROWTYPE;
  v_current public.timesheets%ROWTYPE;
  v_actor_display text := NULL;
  v_actor_role text := NULL;
  v_signature_payload jsonb := '{}'::jsonb;
  v_signature text := NULL;
  v_tools_stage text := NULL;
  v_summary_stage text := NULL;
  v_processing_status text := NULL;
  v_paid_at_utc timestamptz := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_invoice_segments_locked integer := 0;
  v_contract_week_id uuid := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;

  SELECT t.*
    INTO v_requested
  FROM public.timesheets AS t
  WHERE t.timesheet_id = p_timesheet_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'error_code', 'TARGET_NOT_FOUND');
  END IF;

  SELECT t.*
    INTO v_current
  FROM public.timesheets AS t
  WHERE t.booking_id = v_requested.booking_id
    AND t.is_current = true
  ORDER BY t.version DESC NULLS LAST,
           t.updated_at DESC NULLS LAST,
           t.created_at DESC NULLS LAST,
           t.timesheet_id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'requested_timesheet_id', p_timesheet_id
    );
  END IF;

  SELECT cw.id
    INTO v_contract_week_id
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current.timesheet_id
  ORDER BY cw.id
  LIMIT 1;

  IF v_current.archived_by_user_id IS NOT NULL THEN
    SELECT
      COALESCE(
        NULLIF(BTRIM(actor.display_name), ''),
        NULLIF(BTRIM(actor.email), ''),
        'a CloudTMS administrator'
      ),
      actor.role
    INTO v_actor_display, v_actor_role
    FROM public.tms_users AS actor
    WHERE actor.id = v_current.archived_by_user_id;
  END IF;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    v_contract_week_id,
    false
  );
  v_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  SELECT
    source_row.tools_stage,
    source_row.summary_stage,
    source_row.processing_status::text,
    source_row.paid_at_utc,
    source_row.locked_by_invoice_id,
    COALESCE(source_row.invoice_segments_locked, 0)
  INTO
    v_tools_stage,
    v_summary_stage,
    v_processing_status,
    v_paid_at_utc,
    v_locked_by_invoice_id,
    v_invoice_segments_locked
  FROM public.bulk_timesheet_workbench_row_source_v1(
    jsonb_build_object('timesheet_ids', jsonb_build_array(v_current.timesheet_id))
  ) AS source_row
  WHERE source_row.timesheet_id = v_current.timesheet_id
  LIMIT 1;

  RETURN jsonb_build_object(
    'ok', true,
    'requested_timesheet_id', p_timesheet_id,
    'current_timesheet_id', v_current.timesheet_id,
    'was_stale', v_current.timesheet_id IS DISTINCT FROM p_timesheet_id,
    'is_archived', v_current.archived_at_utc IS NOT NULL,
    'archived_at_utc', v_current.archived_at_utc,
    'archived_by_user_id', v_current.archived_by_user_id,
    'archived_by_display', CASE
      WHEN v_current.archived_at_utc IS NULL THEN NULL
      ELSE COALESCE(v_actor_display, 'a CloudTMS administrator')
    END,
    'archived_by_role', CASE WHEN v_current.archived_at_utc IS NULL THEN NULL ELSE v_actor_role END,
    'archived_reason_code', v_current.archived_reason_code,
    'is_current', v_current.is_current,
    'contract_id', v_current.contract_id,
    'contract_week_id', v_contract_week_id,
    'booking_id', v_current.booking_id,
    'authorised_at_server', v_current.authorised_at_server,
    'tools_stage', COALESCE(v_tools_stage, CASE WHEN v_current.archived_at_utc IS NOT NULL THEN 'ARCHIVED' ELSE NULL END),
    'summary_stage', v_summary_stage,
    'processing_status', v_processing_status,
    'paid_at_utc', v_paid_at_utc,
    'locked_by_invoice_id', v_locked_by_invoice_id,
    'invoice_segments_locked', v_invoice_segments_locked,
    'backend_row_signature', v_signature,
    'row_signature', v_signature
  );
END;
$function$;

-- public.timesheet_archive_transition_v1(p_timesheet_id uuid, p_action text, p_removal_kind text, p_actor_user_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text, p_now_utc timestamp with time zone) pre-install MD5 71ebaec6fee639cf5008850529da9738
CREATE OR REPLACE FUNCTION public.timesheet_archive_transition_v1(p_timesheet_id uuid, p_action text, p_removal_kind text DEFAULT 'STANDARD_DELETE'::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_action text := UPPER(NULLIF(BTRIM(COALESCE(p_action, '')), ''));
  v_kind text := UPPER(NULLIF(BTRIM(COALESCE(p_removal_kind, 'STANDARD_DELETE')), ''));
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_preview jsonb := '{}'::jsonb;
  v_recheck jsonb := '{}'::jsonb;
  v_decision text := NULL;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_recheck_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_advance jsonb := '{}'::jsonb;
  v_archive_before jsonb := '{}'::jsonb;
  v_target_count integer := 0;
  v_archived_count integer := 0;
  v_unarchived_count integer := 0;
  v_already_archived_count integer := 0;
  v_actor_display text := NULL;
  v_actor_role text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_primary_contract_week_id uuid := NULL;
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_archive_capability_token uuid := NULL;
  v_remaining_capability_count integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '1500ms', true);

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_TIMESHEET_ID_REQUIRED',
      DETAIL = jsonb_build_object(
        'requested_timesheet_id', p_timesheet_id,
        'message', 'The current Timesheet identity is required. Refresh before continuing.'
      )::text;
  END IF;
  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object(
        'requested_timesheet_id', p_timesheet_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'message', 'The current Timesheet lifecycle signature is required. Refresh before continuing.'
      )::text;
  END IF;
  IF v_action IS NULL OR v_action NOT IN ('ARCHIVE', 'UNARCHIVE') THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_ARCHIVE_ACTION';
  END IF;
  IF v_kind IS NULL OR v_kind NOT IN (
    'STANDARD_DELETE',
    'WEEKLY_CHAIN_DELETE_PARENT',
    'WEEKLY_MANUAL_ADJUSTMENT_DELETE'
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_REMOVAL_KIND';
  END IF;

  SELECT
    COALESCE(
      NULLIF(BTRIM(actor.display_name), ''),
      NULLIF(BTRIM(actor.email), ''),
      'a CloudTMS administrator'
    ),
    actor.role
  INTO v_actor_display, v_actor_role
  FROM public.tms_users AS actor
  WHERE actor.id = p_actor_user_id
    AND actor.is_active = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  END IF;

  IF v_kind = 'STANDARD_DELETE' THEN
    v_preview := public.timesheet_standard_delete_preview_v1(
      p_timesheet_id,
      p_actor_user_id,
      p_expected_timesheet_id,
      p_expected_row_signature
    );

    -- A caller may enter through the ordinary Delete/Unarchive route without
    -- knowing that the installed target resolver owns a weekly unit.  Follow
    -- the authoritative preview redirect rather than archiving only one row.
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_preview -> 'blockers', '[]'::jsonb)) AS blocker(value)
      WHERE blocker.value ->> 'code' = 'WEEKLY_CHAIN_PREVIEW_REQUIRED'
    ) THEN
      v_kind := 'WEEKLY_CHAIN_DELETE_PARENT';
      v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
    ELSIF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_preview -> 'blockers', '[]'::jsonb)) AS blocker(value)
      WHERE blocker.value ->> 'code' = 'WEEKLY_MANUAL_ADJUSTMENT_PREVIEW_REQUIRED'
    ) THEN
      v_kind := 'WEEKLY_MANUAL_ADJUSTMENT_DELETE';
      v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
    END IF;
  ELSIF v_kind = 'WEEKLY_CHAIN_DELETE_PARENT' THEN
    v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  ELSE
    v_preview := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_timesheet_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_preview -> 'timesheet_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_preview -> 'contract_week_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  IF COALESCE(array_length(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'REMOVAL_UNIT_EMPTY',
      'preview', v_preview
    );
  END IF;

  IF array_length(v_timesheet_ids, 1) > 32 THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_TOO_LARGE';
  END IF;

  -- Lock the bounded unit in a deterministic order.  FK-backed writers that
  -- create new financial history cannot acquire a conflicting key-share lock
  -- until this transition commits or rolls back.
  PERFORM 1
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids)
  ORDER BY t.timesheet_id
  FOR UPDATE;

  SELECT COUNT(*)
    INTO v_target_count
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids);

  IF v_target_count <> array_length(v_timesheet_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_CHANGED';
  END IF;

  PERFORM 1
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_timesheet_ids)
    AND tf.is_current = true
  ORDER BY tf.timesheet_id, tf.id
  FOR UPDATE;

  PERFORM 1
  FROM public.contract_weeks AS cw
  WHERE cw.id = ANY(v_contract_week_ids)
  ORDER BY cw.id
  FOR UPDATE;



  IF v_kind = 'STANDARD_DELETE' THEN
    v_recheck := public.timesheet_standard_delete_preview_v1(
      p_timesheet_id,
      p_actor_user_id,
      p_expected_timesheet_id,
      p_expected_row_signature
    );
  ELSIF v_kind = 'WEEKLY_CHAIN_DELETE_PARENT' THEN
    v_recheck := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  ELSE
    v_recheck := public.timesheet_weekly_manual_adjustment_delete_preview(p_timesheet_id, p_actor_user_id);
  END IF;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_recheck_timesheet_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'timesheet_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  SELECT COALESCE(array_agg(DISTINCT id ORDER BY id), ARRAY[]::uuid[])
    INTO v_recheck_contract_week_ids
  FROM (
    SELECT value::uuid AS id
    FROM jsonb_array_elements_text(COALESCE(v_recheck -> 'contract_week_ids', '[]'::jsonb)) AS values(value)
  ) AS parsed;

  IF v_recheck_timesheet_ids IS DISTINCT FROM v_timesheet_ids
     OR v_recheck_contract_week_ids IS DISTINCT FROM v_contract_week_ids THEN
    RAISE EXCEPTION USING MESSAGE = 'REMOVAL_UNIT_CHANGED';
  END IF;

  v_current_timesheet_id := NULLIF(
    BTRIM(COALESCE(v_recheck ->> 'current_timesheet_id', '')),
    ''
  )::uuid;

  IF v_current_timesheet_id IS NULL
     OR NOT (v_current_timesheet_id = ANY(v_timesheet_ids)) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  SELECT
    COUNT(*) FILTER (WHERE t.archived_at_utc IS NOT NULL),
    COALESCE(jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object(
        'archived_at_utc', t.archived_at_utc,
        'archived_by_user_id', t.archived_by_user_id,
        'archived_reason_code', t.archived_reason_code
      )
    ), '{}'::jsonb)
  INTO v_already_archived_count, v_archive_before
  FROM public.timesheets AS t
  WHERE t.timesheet_id = ANY(v_timesheet_ids);

  IF v_action = 'ARCHIVE'
     AND v_already_archived_count = array_length(v_timesheet_ids, 1) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'ARCHIVE',
      'already_archived', true,
      'decision', 'ARCHIVED',
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
    );
  END IF;

  IF v_action = 'UNARCHIVE' AND v_already_archived_count = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'UNARCHIVE',
      'already_unarchived', true,
      'decision', 'ACTIVE',
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'advance_restored', false,
      'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
    );
  END IF;

  IF v_already_archived_count > 0
     AND v_already_archived_count <> array_length(v_timesheet_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'ARCHIVE_UNIT_PARTIAL_STATE';
  END IF;

  -- Idempotent already-completed outcomes above deliberately precede the
  -- stale guards so a retry after an unknown response can reconcile safely
  -- without replaying any Archive, Unarchive, Advance or audit mutation.
  IF p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'EXPECTED_TIMESHEET_MISMATCH',
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  v_primary_contract_week_id := v_contract_week_ids[1];
  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current_timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'ROW_SIGNATURE_UNAVAILABLE',
      'current_timesheet_id', v_current_timesheet_id,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  IF v_current_row_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_row_signature,
      'current_timesheet_id', v_current_timesheet_id,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids)
    );
  END IF;

  IF v_action = 'ARCHIVE' THEN
    v_decision := COALESCE(v_recheck ->> 'decision', 'BLOCKED');

    IF v_decision = 'PERMANENT_DELETE' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'action', 'ARCHIVE',
        'decision', 'PERMANENT_DELETE',
        'error_code', 'ARCHIVE_NOT_REQUIRED',
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids)
      );
    END IF;

    IF v_decision <> 'ARCHIVE_REQUIRED' THEN
      RETURN jsonb_build_object(
        'ok', false,
        'action', 'ARCHIVE',
        'decision', 'BLOCKED',
        'error_code', 'ARCHIVE_BLOCKED',
        'blockers', COALESCE(v_recheck -> 'blockers', v_recheck -> 'blocked_reasons', '[]'::jsonb),
        'timesheet_ids', to_jsonb(v_timesheet_ids),
        'contract_week_ids', to_jsonb(v_contract_week_ids)
      );
    END IF;

    v_advance := COALESCE(v_recheck -> 'advance', '{}'::jsonb);

    -- Archive is metadata-only. Active, clearable, consumed and historical
    -- Advance rows remain unchanged and continue to be reported by the preview.
    v_archive_capability_token := gen_random_uuid();

    INSERT INTO public.timesheet_archive_transition_capability (
      capability_token,
      backend_pid,
      transaction_id,
      timesheet_id,
      action
    )
    SELECT
      v_archive_capability_token,
      pg_backend_pid(),
      txid_current(),
      target_id,
      'ARCHIVE'
    FROM unnest(v_timesheet_ids) AS targets(target_id);

    PERFORM set_config(
      'cloudtms.archive_transition_capability',
      v_archive_capability_token::text,
      true
    );

    UPDATE public.timesheets AS t
       SET archived_at_utc = v_now,
           archived_by_user_id = p_actor_user_id,
           archived_reason_code = 'FINANCIAL_HISTORY_PREVENTED_DELETE',
           updated_at = v_now
     WHERE t.timesheet_id = ANY(v_timesheet_ids)
       AND t.archived_at_utc IS NULL;
    GET DIAGNOSTICS v_archived_count = ROW_COUNT;

    PERFORM set_config('cloudtms.archive_transition_capability', '', true);

    SELECT COUNT(*)
      INTO v_remaining_capability_count
    FROM public.timesheet_archive_transition_capability AS capability
    WHERE capability.capability_token = v_archive_capability_token
      AND capability.backend_pid = pg_backend_pid()
      AND capability.transaction_id = txid_current();

    IF v_remaining_capability_count <> 0 THEN
      RAISE EXCEPTION USING
        MESSAGE = 'ARCHIVE_CAPABILITY_NOT_CONSUMED',
        DETAIL = jsonb_build_object(
          'remaining_capabilities', v_remaining_capability_count,
          'target_count', array_length(v_timesheet_ids, 1)
        )::text;
    END IF;

    IF v_archived_count <> array_length(v_timesheet_ids, 1) THEN
      RAISE EXCEPTION USING MESSAGE = 'ARCHIVE_COUNT_MISMATCH';
    END IF;

    INSERT INTO public.audit_events (
      ts_utc,
      actor_user_id,
      actor_display,
      actor_role_at_time,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason
    )
    SELECT
      v_now,
      p_actor_user_id,
      v_actor_display,
      v_actor_role,
      'timesheets',
      target_id::text,
      'TIMESHEET_ARCHIVED',
      COALESCE(v_archive_before -> target_id::text, jsonb_build_object('archived_at_utc', NULL)),
      jsonb_build_object(
        'archived_at_utc', v_now,
        'archived_by_user_id', p_actor_user_id,
        'archived_reason_code', 'FINANCIAL_HISTORY_PREVENTED_DELETE',
        'target_unit_ids', to_jsonb(v_timesheet_ids),
        'active_advance_detected', COALESCE((v_advance ->> 'active')::boolean, false),
        'advance_unchanged', true,
        'related_pay_batch_id', v_advance -> 'related_pay_batch_id',
        'related_pay_batch_status', v_advance -> 'related_pay_batch_status'
      ),
      'FINANCIAL_HISTORY_PREVENTED_DELETE'
    FROM unnest(v_timesheet_ids) AS targets(target_id);

    RETURN jsonb_build_object(
      'ok', true,
      'action', 'ARCHIVE',
      'decision', 'ARCHIVED',
      'already_archived', false,
      'archived_count', v_archived_count,
      'timesheet_ids', to_jsonb(v_timesheet_ids),
      'contract_week_ids', to_jsonb(v_contract_week_ids),
      'advance', v_advance || jsonb_build_object(
        'unchanged', true
      ),
      'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
    );
  END IF;

  -- UNARCHIVE is metadata-only. It does not change Advance, authorisation,
  -- processing, TSFIN, pay-state, batch, invoice or contract data.
  v_archive_capability_token := gen_random_uuid();

  INSERT INTO public.timesheet_archive_transition_capability (
    capability_token,
    backend_pid,
    transaction_id,
    timesheet_id,
    action
  )
  SELECT
    v_archive_capability_token,
    pg_backend_pid(),
    txid_current(),
    target_id,
    'UNARCHIVE'
  FROM unnest(v_timesheet_ids) AS targets(target_id);

  PERFORM set_config(
    'cloudtms.archive_transition_capability',
    v_archive_capability_token::text,
    true
  );

  UPDATE public.timesheets AS t
     SET archived_at_utc = NULL,
         archived_by_user_id = NULL,
         archived_reason_code = NULL,
         updated_at = v_now
   WHERE t.timesheet_id = ANY(v_timesheet_ids)
     AND t.archived_at_utc IS NOT NULL;
  GET DIAGNOSTICS v_unarchived_count = ROW_COUNT;

  PERFORM set_config('cloudtms.archive_transition_capability', '', true);

  SELECT COUNT(*)
    INTO v_remaining_capability_count
  FROM public.timesheet_archive_transition_capability AS capability
  WHERE capability.capability_token = v_archive_capability_token
    AND capability.backend_pid = pg_backend_pid()
    AND capability.transaction_id = txid_current();

  IF v_remaining_capability_count <> 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ARCHIVE_CAPABILITY_NOT_CONSUMED',
      DETAIL = jsonb_build_object(
        'remaining_capabilities', v_remaining_capability_count,
        'target_count', array_length(v_timesheet_ids, 1)
      )::text;
  END IF;

  IF v_unarchived_count <> array_length(v_timesheet_ids, 1) THEN
    RAISE EXCEPTION USING MESSAGE = 'UNARCHIVE_COUNT_MISMATCH';
  END IF;

  INSERT INTO public.audit_events (
    ts_utc,
    actor_user_id,
    actor_display,
    actor_role_at_time,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  )
  SELECT
    v_now,
    p_actor_user_id,
    v_actor_display,
    v_actor_role,
    'timesheets',
    target_id::text,
    'TIMESHEET_UNARCHIVED',
    COALESCE(v_archive_before -> target_id::text, jsonb_build_object('archived_at_utc', true)),
    jsonb_build_object(
      'archived_at_utc', NULL,
      'archived_by_user_id', NULL,
      'archived_reason_code', NULL,
      'target_unit_ids', to_jsonb(v_timesheet_ids),
      'advance_restored', false,
      'advance_unchanged', true
    ),
    'USER_CONFIRMED_UNARCHIVE'
  FROM unnest(v_timesheet_ids) AS targets(target_id);

  RETURN jsonb_build_object(
    'ok', true,
    'action', 'UNARCHIVE',
    'decision', 'ACTIVE',
    'already_unarchived', false,
    'unarchived_count', v_unarchived_count,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'advance_restored', false,
    'advance_unchanged', true,
    'archive_state', public.timesheet_archive_state_v1(p_timesheet_id)
  );
EXCEPTION
  WHEN lock_not_available OR deadlock_detected THEN
    RETURN jsonb_build_object(
      'ok', false,
      'action', v_action,
      'decision', 'BLOCKED',
      'error_code', 'LOCK_TIMEOUT',
      'message', 'The timesheet removal unit is currently being changed. Refresh and try again.'
    );
END;
$function$;

-- public.timesheet_authorise_bulk_atomic(p_items jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone) pre-install MD5 1ad2faae318d955350b5d1d80b2203ba
CREATE OR REPLACE FUNCTION public.timesheet_authorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_capability_items jsonb := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  IF to_regclass('pg_temp.import_review_lifecycle_capability_v1') IS NOT NULL
     AND nullif(current_setting('cloudtms.import_reconciliation_capability_token',true),'') IS NOT NULL THEN
    IF coalesce(current_setting('request.jwt.claim.role',true),
         nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','') <> 'service_role' THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_INVALID' USING ERRCODE='42501';
    END IF;
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',c.timesheet_id,'expected_timesheet_id',c.expected_timesheet_id,
      'expected_row_signature',c.expected_row_signature) ORDER BY c.timesheet_id),'[]'::jsonb)
    INTO v_capability_items
    FROM pg_temp.import_review_lifecycle_capability_v1 c
    WHERE c.capability_token=current_setting('cloudtms.import_reconciliation_capability_token',true)
      AND c.txid=txid_current() AND c.actor_user_id=p_actor_user_id AND c.action='AUTHORISE';
    IF jsonb_array_length(v_capability_items)=0 OR
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_items_array) x)
       IS DISTINCT FROM
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_capability_items) x) THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_ITEM_SET_MISMATCH' USING ERRCODE='22023';
    END IF;
    v_items_array:=v_capability_items;
  ELSE
    v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'AUTHORISE', p_actor_user_id, 100);
  END IF;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_results;

  CREATE TEMP TABLE timesheet_authorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_authorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.qr_status AS current_qr_status,
    cur_ts.qr_token AS current_qr_token,
    cur_ts.qr_generated_at AS current_qr_generated_at,
    cur_ts.qr_scanned_at AS current_qr_scanned_at,
    cur_ts.sheet_scope AS current_sheet_scope,
    cur_ts.contract_id AS current_contract_id,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.basis AS tsfin_basis,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    COALESCE(summary_row.client_requires_hr, contract_row.requires_hr, false) AS client_requires_hr,
    COALESCE(summary_row.hr_validation_required_for_invoice, contract_row.requires_hr, false) AS hr_validation_required_for_invoice,
    COALESCE(summary_row.validation_status_text, NULL::text) AS summary_validation_status,
    cw.id AS contract_week_id,
    cw.status AS contract_week_status,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock,
    COALESCE(validation_state.validation_ok, false) AS validation_ok
  FROM pg_temp.timesheet_authorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT c.requires_hr
    FROM public.contracts AS c
    WHERE c.id = cur_ts.contract_id
    LIMIT 1
  ) AS contract_row ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(vts.client_requires_hr, false) AS client_requires_hr,
      COALESCE(vts.hr_validation_required_for_invoice, false) AS hr_validation_required_for_invoice,
      CASE
        WHEN vts.validation_status IS NULL THEN NULL::text
        ELSE UPPER(vts.validation_status::text)
      END AS validation_status_text
    FROM public.v_timesheets_summary_base AS vts
    WHERE vts.timesheet_id = cur_ts.timesheet_id
    LIMIT 1
  ) AS summary_row ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw_sel.timesheet_id
           AND cw_ts.booking_id = cur_ts.booking_id
       )
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END,
             cw_sel.updated_at DESC NULLS LAST,
             cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON true
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true
  LEFT JOIN LATERAL (
    SELECT COALESCE(UPPER(COALESCE(summary_row.validation_status_text, tv.status::text)) IN ('VALIDATION_OK', 'OVERRIDDEN'), false) AS validation_ok
    FROM public.timesheet_validations AS tv
    WHERE tv.timesheet_id = cur_ts.timesheet_id
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1
  ) AS validation_state ON true;

  CREATE TEMP TABLE timesheet_authorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'INVOICED'::public.contract_week_status_enum THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'CANCELLED'::public.contract_week_status_enum THEN 'CONTRACT_WEEK_NOT_AUTHORISABLE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'AUTHORISED'::public.contract_week_status_enum THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NOT NULL OR state_rows.tsfin_authorised_at_utc IS NOT NULL THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.tsfin_processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum OR (state_rows.current_qr_status = 'PENDING'::public.timesheet_qr_status_enum AND NULLIF(BTRIM(COALESCE(state_rows.current_qr_token, '')), '') IS NOT NULL AND state_rows.current_qr_generated_at IS NOT NULL AND state_rows.current_qr_scanned_at IS NULL) THEN 'AWAITING_SIGNED_QR'
      WHEN state_rows.tsfin_processing_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN 'AUTHORISE_NOT_ALLOWED'
      ELSE NULL::text
    END AS failure_code,
    CASE
      WHEN state_rows.tsfin_basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.hr_validation_required_for_invoice, false) AND NOT COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END AS new_processing_status
  FROM pg_temp.timesheet_authorise_bulk_state AS state_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = p_actor_user_id,
           authorised_at_utc = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_authorise:' || v_now::text,
    'TIMESHEET_BULK_AUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_AUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_authorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'AUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_authorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'AUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', -v_success_count, 'authorised_eligible', v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;

-- public.timesheet_standard_delete_preview_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text) pre-install MD5 747036ef0a743bdc1eb8905e848b2cc3
CREATE OR REPLACE FUNCTION public.timesheet_standard_delete_preview_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_requested public.timesheets%ROWTYPE;
  v_current public.timesheets%ROWTYPE;
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_nhsp_shift_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_preserved_source_contract_week_ids uuid[] := ARRAY[]::uuid[];
  v_primary_contract_week_id uuid := NULL;
  v_history jsonb := '{}'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_decision text := 'BLOCKED';
  v_signature_payload jsonb := '{}'::jsonb;
  v_current_signature text := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ID_REQUIRED';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_USER_ID_REQUIRED';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor
    WHERE actor.id = p_actor_user_id
      AND actor.is_active = true
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'ACTOR_NOT_FOUND_OR_INACTIVE';
  END IF;

  SELECT t.*
    INTO v_requested
  FROM public.timesheets AS t
  WHERE t.timesheet_id = p_timesheet_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'eligible', false,
      'blockers', jsonb_build_array(jsonb_build_object('code', 'TARGET_NOT_FOUND')),
      'blocked_reasons', jsonb_build_array(jsonb_build_object('code', 'TARGET_NOT_FOUND')),
      'timesheet_ids', '[]'::jsonb,
      'contract_week_ids', '[]'::jsonb,
      'nhsp_shift_ids', '[]'::jsonb,
      'preserved_source_timesheet_ids', '[]'::jsonb,
      'preserved_source_contract_week_ids', '[]'::jsonb,
      'retention_reasons', '[]'::jsonb,
      'advance', jsonb_build_object(
        'active', false,
        'clearable', false,
        'consumed', false,
        'historical', false
      )
    );
  END IF;

  SELECT t.*
    INTO v_current
  FROM public.timesheets AS t
  WHERE t.booking_id = v_requested.booking_id
    AND t.is_current = true
  ORDER BY t.version DESC NULLS LAST,
           t.updated_at DESC NULLS LAST,
           t.created_at DESC NULLS LAST,
           t.timesheet_id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', true,
      'kind', 'STANDARD_DELETE',
      'decision', 'BLOCKED',
      'eligible', false,
      'requested_timesheet_id', p_timesheet_id,
      'blockers', jsonb_build_array(jsonb_build_object('code', 'CURRENT_TIMESHEET_NOT_FOUND')),
      'blocked_reasons', jsonb_build_array(jsonb_build_object('code', 'CURRENT_TIMESHEET_NOT_FOUND')),
      'timesheet_ids', '[]'::jsonb,
      'contract_week_ids', '[]'::jsonb,
      'nhsp_shift_ids', '[]'::jsonb,
      'preserved_source_timesheet_ids', '[]'::jsonb,
      'preserved_source_contract_week_ids', '[]'::jsonb,
      'retention_reasons', '[]'::jsonb,
      'advance', jsonb_build_object(
        'active', false,
        'clearable', false,
        'consumed', false,
        'historical', false
      )
    );
  END IF;

  -- The installed standard-delete path removes the authoritative current row only.
  -- Weekly parent/manual paths resolve their own larger units in their existing previews.
  v_timesheet_ids := ARRAY[v_current.timesheet_id]::uuid[];

  SELECT COALESCE(array_agg(DISTINCT cw.id ORDER BY cw.id), ARRAY[]::uuid[])
    INTO v_contract_week_ids
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current.timesheet_id;

  v_primary_contract_week_id := v_contract_week_ids[1];

  SELECT COALESCE(array_agg(DISTINCT nhsp_shift.id ORDER BY nhsp_shift.id), ARRAY[]::uuid[])
    INTO v_nhsp_shift_ids
  FROM public.nhsp_shifts AS nhsp_shift
  WHERE nhsp_shift.timesheet_id = ANY(v_timesheet_ids);

  SELECT COALESCE(array_agg(DISTINCT source_contract_week.id ORDER BY source_contract_week.id), ARRAY[]::uuid[])
    INTO v_preserved_source_contract_week_ids
  FROM public.contract_weeks AS target_contract_week
  JOIN public.contract_weeks AS source_contract_week
    ON source_contract_week.contract_id = target_contract_week.contract_id
   AND source_contract_week.week_ending_date = target_contract_week.week_ending_date
   AND COALESCE(source_contract_week.additional_seq, 0) = 0
   AND COALESCE(source_contract_week.is_adjustment, false) = false
  WHERE target_contract_week.id = ANY(v_contract_week_ids)
    AND source_contract_week.id <> ALL(v_contract_week_ids);

  SELECT COALESCE(array_agg(DISTINCT preserved_row.preserved_timesheet_id ORDER BY preserved_row.preserved_timesheet_id), ARRAY[]::uuid[])
    INTO v_preserved_source_timesheet_ids
  FROM (
    SELECT source_contract_week.timesheet_id AS preserved_timesheet_id
    FROM public.contract_weeks AS source_contract_week
    WHERE source_contract_week.id = ANY(v_preserved_source_contract_week_ids)
      AND source_contract_week.timesheet_id IS NOT NULL

    UNION

    SELECT target_timesheet.parent_timesheet_id AS preserved_timesheet_id
    FROM public.timesheets AS target_timesheet
    WHERE target_timesheet.timesheet_id = ANY(v_timesheet_ids)
      AND target_timesheet.parent_timesheet_id IS NOT NULL
  ) AS preserved_row
  WHERE preserved_row.preserved_timesheet_id IS NOT NULL
    AND preserved_row.preserved_timesheet_id <> ALL(v_timesheet_ids);

  IF p_expected_timesheet_id IS NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_TIMESHEET_ID_REQUIRED',
      'current_timesheet_id', v_current.timesheet_id,
      'message', 'The current Timesheet identity is required. Refresh before continuing.'
    ));
  ELSIF p_expected_timesheet_id IS DISTINCT FROM v_current.timesheet_id THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_TIMESHEET_MISMATCH',
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current.timesheet_id
    ));
  END IF;

  v_signature_payload := public.timesheet_lifecycle_guard_signature_v1(
    v_current.timesheet_id,
    v_primary_contract_week_id,
    false
  );
  v_current_signature := NULLIF(BTRIM(COALESCE(
    v_signature_payload ->> 'backend_row_signature',
    v_signature_payload ->> 'row_signature',
    v_signature_payload ->> 'signature',
    ''
  )), '');

  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      'current_row_signature', v_current_signature,
      'message', 'The current Timesheet lifecycle signature is required. Refresh before continuing.'
    ));
  ELSIF v_current_signature IS DISTINCT FROM BTRIM(p_expected_row_signature) THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'ROW_SIGNATURE_MISMATCH',
      'expected_row_signature', BTRIM(p_expected_row_signature),
      'current_row_signature', v_current_signature
    ));
  END IF;

  IF v_current.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    IF COALESCE(v_current.is_adjustment, false)
       OR EXISTS (
         SELECT 1
         FROM public.contract_weeks AS cw
         WHERE cw.timesheet_id = v_current.timesheet_id
           AND (COALESCE(cw.is_adjustment, false) OR COALESCE(cw.additional_seq, 0) > 0)
       ) THEN
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'WEEKLY_MANUAL_ADJUSTMENT_PREVIEW_REQUIRED'
      ));
    ELSE
      v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
        'code', 'WEEKLY_CHAIN_PREVIEW_REQUIRED'
      ));
    END IF;
  END IF;

  IF UPPER(COALESCE(v_current.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
     OR NULLIF(BTRIM(COALESCE(v_current.correction_kind, '')), '') IS NOT NULL
     OR v_current.correction_id IS NOT NULL THEN
    v_blockers := v_blockers || jsonb_build_array(jsonb_build_object(
      'code', 'IMPORT_DERIVED_CHILD',
      'message', 'Import-derived children must be handled through the parent-chain removal path.'
    ));
  END IF;

  v_history := public.timesheet_removal_financial_history_v1(
    v_timesheet_ids,
    ARRAY[v_current.booking_id]::text[],
    v_contract_week_ids
  );

  v_blockers := v_blockers || COALESCE(v_history -> 'blockers', '[]'::jsonb);

  IF jsonb_array_length(v_blockers) > 0 THEN
    v_decision := 'BLOCKED';
  ELSIF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    v_decision := 'ARCHIVE_REQUIRED';
  ELSE
    v_decision := 'PERMANENT_DELETE';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'kind', 'STANDARD_DELETE',
    'decision', v_decision,
    'eligible', v_decision = 'PERMANENT_DELETE',
    'requested_timesheet_id', p_timesheet_id,
    'current_timesheet_id', v_current.timesheet_id,
    'was_stale', v_current.timesheet_id IS DISTINCT FROM p_timesheet_id,
    'booking_id', v_current.booking_id,
    'timesheet_ids', to_jsonb(v_timesheet_ids),
    'contract_week_ids', to_jsonb(v_contract_week_ids),
    'nhsp_shift_ids', to_jsonb(v_nhsp_shift_ids),
    'preserved_source_timesheet_ids', to_jsonb(v_preserved_source_timesheet_ids),
    'preserved_source_contract_week_ids', to_jsonb(v_preserved_source_contract_week_ids),
    'blockers', v_blockers,
    'blocked_reasons', v_blockers,
    'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
    'advance', COALESCE(v_history -> 'advance', '{}'::jsonb),
    'current_row_signature', v_current_signature
  );
END;
$function$;

-- public.timesheet_unauthorise_bulk_atomic(p_items jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone) pre-install MD5 acf45c24bfd1e2fdffc6a9afbdfe529b
CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_capability_items jsonb := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  IF to_regclass('pg_temp.import_review_lifecycle_capability_v1') IS NOT NULL
     AND nullif(current_setting('cloudtms.import_reconciliation_capability_token',true),'') IS NOT NULL THEN
    IF coalesce(current_setting('request.jwt.claim.role',true),
         nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','') <> 'service_role' THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_INVALID' USING ERRCODE='42501';
    END IF;
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',c.timesheet_id,'expected_timesheet_id',c.expected_timesheet_id,
      'expected_row_signature',c.expected_row_signature) ORDER BY c.timesheet_id),'[]'::jsonb)
    INTO v_capability_items
    FROM pg_temp.import_review_lifecycle_capability_v1 c
    WHERE c.capability_token=current_setting('cloudtms.import_reconciliation_capability_token',true)
      AND c.txid=txid_current() AND c.actor_user_id=p_actor_user_id AND c.action='UNAUTHORISE';
    IF jsonb_array_length(v_capability_items)=0 OR
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_items_array) x)
       IS DISTINCT FROM
       (SELECT array_agg(distinct nullif(x->>'timesheet_id','')::uuid ORDER BY nullif(x->>'timesheet_id','')::uuid)
          FROM jsonb_array_elements(v_capability_items) x) THEN
      RAISE EXCEPTION 'IMPORT_REVIEW_LIFECYCLE_CAPABILITY_ITEM_SET_MISMATCH' USING ERRCODE='22023';
    END IF;
    v_items_array:=v_capability_items;
  ELSE
    v_items_array := public._ctms_expand_lifecycle_items_v1(v_items_array, 'UNAUTHORISE', p_actor_user_id, 100);
  END IF;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_results;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_unauthorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.sheet_scope AS current_sheet_scope,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    cw.id AS contract_week_id,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock
  FROM pg_temp.timesheet_unauthorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (SELECT 1 FROM public.timesheets AS cw_ts WHERE cw_ts.timesheet_id = cw_sel.timesheet_id AND cw_ts.booking_id = cur_ts.booking_id)
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END, cw_sel.updated_at DESC NULLS LAST, cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON cur_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NULL AND state_rows.tsfin_authorised_at_utc IS NULL THEN 'ALREADY_UNAUTHORISED'
      ELSE NULL::text
    END AS failure_code,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum AS new_processing_status
  FROM pg_temp.timesheet_unauthorise_bulk_state AS state_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = NULL,
           authorised_at_utc = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET timesheet_id = work_rows.current_timesheet_id,
           status = 'SUBMITTED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_unauthorise:' || v_now::text,
    'TIMESHEET_BULK_UNAUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_UNAUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_unauthorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'UNAUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'UNAUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', v_success_count, 'authorised_eligible', -v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;

drop function if exists public.timesheet_correction_pair_lifecycle_preview_v1(jsonb,text,uuid,integer);

commit;

