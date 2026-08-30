-- LEGACY_UPGRADE-only routine-definition and ACL convergence for the
-- exact current managed-TEST contract. Definitions are sourced read-only from
-- current TEST; only the five exact absent private legacy owners are added.
-- Every ACL is reset to one audited provider-neutral role set.

begin;
set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_routine_contract_prestate$
declare
  v_count integer;
begin
  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname in ('public','private');
  -- The contract owns 1,151 application routines; 219 provider/release helper
  -- routines are intentionally outside that exported application contract.
  if v_count<>1370 then
    raise exception 'LEGACY_ROUTINE_COUNT_DRIFT:%',v_count;
  end if;
end
$legacy_routine_contract_prestate$;

-- SOURCE private._invoice_generation_advance_batch_legacy_20260726(p_claims jsonb, p_now_utc timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_generation_advance_batch_legacy_20260726(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_result jsonb := '[]'::jsonb;
  v_part jsonb;
begin
  -- VALIDATE_SOURCES: no invoice/header/line writes are permitted in this block.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='VALIDATE_SOURCES'
  ),
  member_values as materialized (
    select c.id chunk_id,c.operation_id,c.payload_json,
      m.value member,m.ordinality
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
          and jsonb_array_length(c.payload_json->'canonical_source_members')>0
        then c.payload_json->'canonical_source_members'
        else coalesce((
          select jsonb_agg(jsonb_build_object(
            'source_type','TIMESHEET','source_id',s.value,
            'related_timesheet_id',s.value) order by s.ordinality)
          from jsonb_array_elements_text(coalesce(
            c.payload_json->'canonical_source_ids',
            c.payload_json->'source_ids','[]'::jsonb))
            with ordinality s(value,ordinality)
        ),'[]'::jsonb) end) with ordinality m(value,ordinality)
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  members as materialized (
    select m.chunk_id,m.operation_id,m.payload_json,m.member,
      upper(coalesce(m.member->>'source_type','TIMESHEET')) source_type,
      case when coalesce(m.member->>'source_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(m.member->>'source_id')::uuid end source_id,
      case when coalesce(m.member->>'related_timesheet_id',
          m.member->>'source_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then coalesce(m.member->>'related_timesheet_id',
          m.member->>'source_id')::uuid end timesheet_id,
      nullif(btrim(m.member->>'segment_id'),'') segment_id,
      coalesce(nullif(btrim(m.member->>'source_member_key'),''),
        encode(digest(concat_ws('|',
          upper(coalesce(m.member->>'source_type','TIMESHEET')),
          coalesce(m.member->>'source_id',''),
          coalesce(m.member->>'related_timesheet_id',''),
          coalesce(m.member->>'segment_id','WHOLE'),
          coalesce(m.member->>'target_invoice_week','')),'sha256'),'hex'))
        source_member_key,
      nullif(btrim(coalesce(m.member->>'row_revision',
        m.member->>'source_revision','')),'') expected_revision,
      m.ordinality
    from member_values m
  ),
  reference_eval as materialized (
    select distinct on (r.source_member_key) r.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,'source_id',m.source_id,
        'related_timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'target_invoice_week',m.member->>'target_invoice_week',
        'invoice_stream',m.payload_json->>'invoice_stream',
        'consolidation_mode',m.payload_json->>'consolidation_mode',
        'source_revision',m.expected_revision)
        order by m.chunk_id,m.ordinality)
      from members m
    ),'[]'::jsonb)) r
    order by r.source_member_key
  ),
  vat_eval as materialized (
    select distinct on (v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,'source_id',m.source_id,
        'timesheet_id',m.timesheet_id,'segment_id',m.segment_id,
        'effective_date',m.payload_json->>'effective_settings_date')
        order by m.chunk_id,m.ordinality)
      from members m
    ),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-validate:'||scope.chunk_id::text,
      'scope_key',scope.chunk_id::text,
      'validation_purpose','GENERATION_VALIDATE',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.chunk_id),'[]'::jsonb) scopes
    from(
      select m.chunk_id,min(m.payload_json::text)::jsonb payload_json,
        jsonb_agg(jsonb_build_object(
          'timesheet_id',m.timesheet_id,
          'source_type',m.source_type,
          'source_id',m.source_id,
          'source_member_key',m.source_member_key,
          'segment_id',m.segment_id,
          'target_invoice_week',m.payload_json->>'target_invoice_week',
          'vat_rate_pct',v.vat_rate)
          order by m.ordinality) planned_members
      from members m
      left join vat_eval v on v.source_member_key=m.source_member_key
      group by m.chunk_id
    ) scope
  ),
  correction_eval as materialized (
    select r.*
    from correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),  source_eval as materialized (
    select m.chunk_id,m.operation_id,m.timesheet_id,m.segment_id,
      m.source_member_key,m.ordinality,
      tf.id tsfin_id,
      encode(digest(concat_ws('|',tf.id::text,tf.timesheet_version::text,
        tf.updated_at::text,ts.version::text,ts.updated_at::text,
        coalesce(tf.invoice_breakdown_json::text,'')),'sha256'),'hex') row_revision,
      array_remove(array[
        case when ts.timesheet_id is null then 'TIMESHEET_NOT_FOUND' end,
        case when ts.timesheet_id is not null and (not ts.is_current or ts.revoked_at is not null) then 'TIMESHEET_NOT_CURRENT' end,
        case when tf.id is null then 'CURRENT_FINANCIALS_MISSING' end,
        case when tf.id is not null and tf.is_stale then 'FINANCIALS_STALE' end,
        case when tf.id is not null and tf.processing_status::text<>'READY_FOR_INVOICE' then 'NOT_READY_FOR_INVOICE' end,
        case when tf.id is not null and tf.has_rate_issue then 'RATE_MISSING' end,
        case when tf.id is not null and tf.has_pay_channel_issue then 'PAY_CHANNEL_MISSING' end,
        case when tf.id is not null and tf.client_id is null then 'CLIENT_UNRESOLVED' end,
        case when tf.id is not null and tf.locked_by_invoice_id is not null then 'SOURCE_ALREADY_LOCKED' end,
        case when ts.authorised_at_server is null
          then 'TIMESHEET_NOT_AUTHORISED' end,
        case when upper(coalesce(ts.submission_mode::text,''))='QR'
          and(nullif(ts.qr_signed_hash,'') is null
            or ts.qr_signed_at_utc is null)
          then 'QR_TIMESHEET_UNSIGNED' end,
        case when pc.require_reference_to_invoice
          and coalesce(ref.reference_ready,false) is not true
          then coalesce(ref.blocker_code,'MISSING_REFERENCE') end,
        case when coalesce(tf.mileage_pay_ex_vat,0)<>0
            or coalesce(tf.mileage_charge_ex_vat,0)<>0
          then case when not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null)
            then 'MISSING_MILEAGE_EVIDENCE' end end,
        case when(coalesce(tf.mileage_pay_ex_vat,0)<>0
            or coalesce(tf.mileage_charge_ex_vat,0)<>0)
          and exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null
              and e.document_asset_id is null)
          then 'MILEAGE_ASSET_NOT_REGISTERED' end,
        case when coalesce(tf.expenses_pay_ex_vat,0)<>0
            or coalesce(tf.expenses_charge_ex_vat,0)<>0
            or coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0
            or coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0
          then case when not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null)
            then 'MISSING_EXPENSE_EVIDENCE' end end,
        case when(
            coalesce(tf.expenses_pay_ex_vat,0)<>0
            or coalesce(tf.expenses_charge_ex_vat,0)<>0
            or coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0
            or coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null
              and e.document_asset_id is null)
          then 'EXPENSE_ASSET_NOT_REGISTERED' end,
        case when(coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='TRAVEL'
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_TRAVEL_EVIDENCE' end,
        case when(coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='ACCOMMODATION'
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_ACCOMMODATION_EVIDENCE' end,
        case when(coalesce(tf.other_pay_ex_vat,0)<>0
            or coalesce(tf.other_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_OTHER_EXPENSE_EVIDENCE' end,
        case when exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
            where e.timesheet_id=m.timesheet_id
              and a.status in(
                'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
          then 'REQUIRED_ASSET_PERMANENT_FAILURE' end,
        case when coalesce(vat.valid,false) is not true
          then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED') end,
        case when corr.valid is false
          then coalesce(corr.blocker_code,'CORRECTION_VALIDATION_FAILED') end,
        case when coalesce(vs.hr_validation_required_for_invoice,false)
          and upper(coalesce(tf.hr_crosscheck_status,'')) not in(
            'OK','PASS','PASSED','MATCHED','NOT_REQUIRED')
          then 'HEALTHROSTER_VALIDATION_REQUIRED' end,
        case when coalesce(vs.client_is_nhsp,false)
          and coalesce(vs.nhsp_shift_count,0)>0
          and coalesce(vs.nhsp_shift_included_count,0)=0
          then 'NHSP_SOURCE_NOT_READY' end,
        case when exists(select 1 from public.invoice_lines l join public.invoices i on i.id=l.invoice_id
                         where l.timesheet_id=m.timesheet_id and i.status in ('DRAFT','ISSUED','ON_HOLD'))
             and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS' then 'SOURCE_ALREADY_INVOICED' end
      ],null)::text[] blockers
    from members m
    left join public.timesheets ts on ts.timesheet_id=m.timesheet_id
    left join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=m.timesheet_id
    left join public.v_timesheets_summary_base vs
      on vs.timesheet_id=m.timesheet_id
    left join reference_eval ref on ref.source_member_key=m.source_member_key
    left join vat_eval vat on vat.source_member_key=m.source_member_key
    left join correction_eval corr
      on corr.scope_key=m.chunk_id::text
  ),
  resolver_inputs as materialized (
    select c.id chunk_id,c.payload_json,
      row_number() over(order by c.id)::integer command_no
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  canonical_now as materialized (
    select ri.chunk_id,r.group_key,r.source_revision_hash,
      r.blocker_code,r.blocker_detail
    from private._invoice_generation_resolve_command_groups(
      (select coalesce(jsonb_agg(ri.payload_json order by ri.command_no),
        '[]'::jsonb) from resolver_inputs ri),null,v_now) r
    join resolver_inputs ri on ri.command_no=r.command_no
    where r.group_key=ri.payload_json->>'group_key'
  ),
  per_chunk as materialized (
    select c.id chunk_id,
      n.source_revision_hash current_revision,
      coalesce(jsonb_agg(jsonb_build_object('source_id',se.timesheet_id,
        'segment_id',se.segment_id,
        'codes',to_jsonb(se.blockers)) order by se.ordinality)
        filter(where cardinality(se.blockers)>0),'[]'::jsonb)
        ||case when n.blocker_code is null then '[]'::jsonb
          else jsonb_build_array(coalesce(n.blocker_detail,
            jsonb_build_object('code',n.blocker_code))) end blockers,
      count(se.timesheet_id)::integer source_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join source_eval se on se.chunk_id=c.id
    left join canonical_now n on n.chunk_id=c.id
    group by c.id,n.source_revision_hash,n.blocker_code,n.blocker_detail
  ),
  updated as (
    update public.invoice_operation_chunks c
    set phase=case
          when p.source_count=0 then 'BLOCKED'
          when jsonb_array_length(p.blockers)>0 then 'BLOCKED'
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then 'SUPERSEDED'
          else 'PLAN' end,
        status=case
          when p.source_count=0 or jsonb_array_length(p.blockers)>0 then 'BLOCKED'
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then 'SUPERSEDED'
          else 'QUEUED' end,
        payload_json=c.payload_json||jsonb_build_object('source_revision',p.current_revision,'source_count',p.source_count),
        progress_json=jsonb_build_object('status_message',
          case when p.source_count=0 then 'No sources resolved'
               when jsonb_array_length(p.blockers)>0 then 'Source validation blocked'
               when nullif(c.payload_json->>'source_revision','') is not null
                and c.payload_json->>'source_revision'<>p.current_revision then 'Source changed'
               else 'Sources validated' end,'source_count',p.source_count),
        error_json=case
          when p.source_count=0 then jsonb_build_object('code','NO_SOURCES','sources','[]'::jsonb)
          when jsonb_array_length(p.blockers)>0 then jsonb_build_object('code','SOURCE_VALIDATION_BLOCKED','sources',p.blockers)
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then jsonb_build_object('code','SOURCE_CHANGED')
          else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from per_chunk p where c.id=p.chunk_id
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]'::jsonb)
  into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Credit-note validation is deliberately separate from normal timesheet sources.
  with claim_ids as (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='VALIDATE_SOURCES'
  ),
  credit_eval as (
    select c.id chunk_id,i.id invoice_id,
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),'sha256'),'hex') revision,
      case when i.id is null then 'INVOICE_NOT_FOUND'
           when i.type::text<>'INVOICE' then 'CREDIT_SOURCE_NOT_INVOICE'
           when i.status::text not in ('ISSUED','PAID') then 'CREDIT_SOURCE_NOT_ISSUED'
           when exists(select 1 from public.invoices cn where cn.original_invoice_id=i.id and cn.type='CREDIT_NOTE'
                       and cn.status in('DRAFT','ISSUED','PAID')) then 'CREDIT_ALREADY_EXISTS'
           else null end blocker
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join public.invoices i on i.id=c.entity_id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
  ),
  updated as (
    update public.invoice_operation_chunks c
    set phase=case when e.blocker is null then 'PLAN' else 'BLOCKED' end,
        status=case when e.blocker is null then 'QUEUED' else 'BLOCKED' end,
        payload_json=c.payload_json||jsonb_build_object('source_revision',e.revision,'source_invoice_id',e.invoice_id),
        progress_json=jsonb_build_object('status_message',case when e.blocker is null then 'Credit source validated' else 'Credit source blocked' end),
        error_json=case when e.blocker is null then null else jsonb_build_object('code',e.blocker,'invoice_id',c.entity_id) end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from credit_eval e where c.id=e.chunk_id
    returning c.id,c.status,c.phase,c.error_json
  )
  select v_result||coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]'::jsonb)
  into v_result from updated;

  -- PLAN: compact financial/settings summary only; no complete line arrays.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='PLAN'
  ),
  members as materialized (
    select c.id chunk_id,c.payload_json,
      case when pg_input_is_valid(s.value,'uuid')
        then s.value::uuid end timesheet_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    cross join lateral jsonb_array_elements_text(coalesce(
      c.payload_json->'canonical_source_ids',c.payload_json->'source_ids','[]'::jsonb)) s(value)
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
      and pg_input_is_valid(s.value,'uuid')
  ),
  values_by_chunk as materialized (
    select m.chunk_id,count(*)::integer source_count,
      round(sum(coalesce(tf.total_pay_ex_vat,0)),2) expected_pay_ex_vat,
      round(sum(coalesce(tf.total_charge_ex_vat,0)),2) expected_charge_ex_vat,
      (array_agg(tf.client_id order by tf.client_id))[1] client_id,
      jsonb_build_object(
        'client_id',(array_agg(tf.client_id order by tf.client_id))[1],'consolidation_mode',max(c.payload_json->>'consolidation_mode'),
        'stream',max(c.payload_json->>'invoice_stream'),
        'invoice_week_start',max(c.payload_json->>'target_invoice_week'),
        'client',jsonb_build_object('name',max(cl.name),'vat_chargeable',bool_and(cl.vat_chargeable),
          'payment_terms_days',max(cl.payment_terms_days),'primary_invoice_email',max(cl.primary_invoice_email)),
        'attach_policy',jsonb_build_object(
          'hr_attach_to_invoice',coalesce(bool_or(cs.hr_attach_to_invoice),bool_or(sd.hr_attach_to_invoice),true),
          'ts_attach_to_invoice',coalesce(bool_or(cs.ts_attach_to_invoice),bool_or(sd.ts_attach_to_invoice),true),
          'requires_hr',coalesce(bool_or(cs.requires_hr),false))
      ) settings_snapshot
    from members m join public.invoice_operation_chunks c on c.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.clients cl on cl.id=tf.client_id
    left join lateral (
      select s.* from public.client_settings s where s.client_id=tf.client_id
       and (s.effective_from is null or s.effective_from<=coalesce(
      case when c.payload_json->>'effective_settings_date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          and pg_input_is_valid(c.payload_json->>'effective_settings_date','date')
           then (c.payload_json->>'effective_settings_date')::date end,
         (v_now at time zone 'Europe/London')::date))
      order by s.effective_from desc nulls last,s.updated_at desc nulls last,
        s.created_at desc nulls last,s.id desc
      limit 1
    ) cs on true
    cross join public.settings_defaults sd
    where sd.id=1
    group by m.chunk_id
  ),
  plans as materialized (
    select c.id chunk_id,coalesce(
      case when pg_input_is_valid(
        nullif(c.payload_json->>'planned_invoice_id',''),'uuid')
        then(c.payload_json->>'planned_invoice_id')::uuid end,
      case when c.payload_json->>'consolidation_mode' in ('BY_WEEK','ANY_WEEK') then (
        select i.id from public.invoices i
        where i.client_id=v.client_id and i.status='DRAFT' and i.type='INVOICE'
          and coalesce(i.header_snapshot_json#>>'{meta,consolidation_mode}','NONE')=c.payload_json->>'consolidation_mode'
          and coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false')=
              case when c.payload_json->>'invoice_stream'='SELF_BILL' then 'true' else 'false' end
          and (c.payload_json->>'consolidation_mode'='ANY_WEEK'
               or i.header_snapshot_json#>>'{meta,invoice_week_start}'=c.payload_json->>'target_invoice_week')
        order by i.created_at desc limit 1
      ) end,gen_random_uuid()) planned_invoice_id,
      v.source_count,v.expected_charge_ex_vat,v.expected_pay_ex_vat,v.client_id,
      v.settings_snapshot,encode(digest(v.settings_snapshot::text,'sha256'),'hex') settings_hash
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join values_by_chunk v on v.chunk_id=c.id
  ),
  credit_plans as materialized (
    select c.id chunk_id,gen_random_uuid() planned_invoice_id,1 source_count,
      -i.subtotal_ex_vat expected_charge_ex_vat,-coalesce(sum(l.total_pay_ex_vat),0) expected_pay_ex_vat,
      i.client_id,jsonb_build_object('source_invoice_id',i.id,'source_invoice_no',i.invoice_no,
        'client_id',i.client_id,'credit_note',true) settings_snapshot,
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text),'sha256'),'hex') settings_hash
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i on i.id=c.entity_id
    left join public.invoice_lines l on l.invoice_id=i.id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
    group by c.id,i.id
  ),
  all_plans as materialized (select * from plans union all select * from credit_plans),
  updated as (
    update public.invoice_operation_chunks c
    set phase='COMMIT',status='QUEUED',
      payload_json=c.payload_json||jsonb_build_object('plan',jsonb_build_object(
        'planned_invoice_id',p.planned_invoice_id,'intended_invoice_count',1,
        'source_count',p.source_count,'expected_pay_ex_vat',p.expected_pay_ex_vat,
        'expected_charge_ex_vat',p.expected_charge_ex_vat,'settings_snapshot',p.settings_snapshot,
        'settings_hash',p.settings_hash,'source_revision',c.payload_json->>'source_revision')),
      progress_json=jsonb_build_object('status_message','Generation plan ready','source_count',p.source_count),
      error_json=null,lease_owner=null,lease_token=null,
      lease_expires_at_utc=null,updated_at_utc=v_now
    from all_plans p where c.id=p.chunk_id
    returning c.id,c.status,c.phase,c.payload_json->'plan' plan
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'plan',plan)),'[]'::jsonb)
    into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- COMMIT: revalidate immediately, then headers, lines, locks, documents and audit set-wise.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='COMMIT'
  ),
  normal_chunks as materialized (
    select c.*,case when pg_input_is_valid(
        c.payload_json#>>'{plan,planned_invoice_id}','uuid')
      then(c.payload_json#>>'{plan,planned_invoice_id}')::uuid end planned_invoice_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  members as materialized (
    select c.id chunk_id,c.operation_id,c.planned_invoice_id,c.payload_json,
      upper(coalesce(s.value->>'source_type','TIMESHEET')) source_type,
      case when pg_input_is_valid(s.value->>'source_id','uuid')
        then(s.value->>'source_id')::uuid end source_id,
      case when pg_input_is_valid(coalesce(
          s.value->>'related_timesheet_id',s.value->>'source_id'),'uuid')
        then coalesce(s.value->>'related_timesheet_id',
          s.value->>'source_id')::uuid end timesheet_id,
      nullif(btrim(s.value->>'segment_id'),'') segment_id,
      coalesce(nullif(btrim(s.value->>'source_member_key'),''),
        encode(digest(concat_ws('|',
          upper(coalesce(s.value->>'source_type','TIMESHEET')),
          coalesce(s.value->>'source_id',''),
          coalesce(s.value->>'related_timesheet_id',''),
          coalesce(s.value->>'segment_id','WHOLE'),
          coalesce(s.value->>'target_invoice_week','')),'sha256'),'hex'))
        source_member_key
    from normal_chunks c
    cross join lateral jsonb_array_elements(case
      when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
        and jsonb_array_length(c.payload_json->'canonical_source_members')>0
      then c.payload_json->'canonical_source_members'
      else coalesce((
        select jsonb_agg(jsonb_build_object(
          'source_type','TIMESHEET','source_id',ids.value,
          'related_timesheet_id',ids.value) order by ids.ordinality)
        from jsonb_array_elements_text(coalesce(
          c.payload_json->'canonical_source_ids',
          c.payload_json->'source_ids','[]'::jsonb))
          with ordinality ids(value,ordinality)
      ),'[]'::jsonb) end) s(value)
    where pg_input_is_valid(coalesce(
      s.value->>'related_timesheet_id',s.value->>'source_id'),'uuid')
  ),
  selected_segments as materialized (
    select c.id chunk_id,
      (m.value->>'source_id')::uuid timesheet_id,
      array_agg(distinct m.value->>'segment_id'
        order by m.value->>'segment_id')
        filter(where nullif(m.value->>'segment_id','') is not null) segment_ids
    from normal_chunks c
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
        then c.payload_json->'canonical_source_members' else '[]'::jsonb end)
      m(value)
    where coalesce(m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    group by c.id,(m.value->>'source_id')::uuid
  ),
  commit_reference_eval as materialized (
    select distinct on(r.source_member_key) r.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,
        'source_id',m.source_id,
        'related_timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'target_invoice_week',m.payload_json->>'target_invoice_week',
        'invoice_stream',m.payload_json->>'invoice_stream',
        'consolidation_mode',m.payload_json->>'consolidation_mode')
        order by m.chunk_id,m.source_member_key)
      from members m),'[]'::jsonb)) r
    order by r.source_member_key
  ),
  commit_vat_eval as materialized (
    select distinct on(v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,
        'source_id',m.source_id,
        'timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'effective_date',m.payload_json->>'effective_settings_date')
        order by m.chunk_id,m.source_member_key)
      from members m),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  commit_correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-commit:'||scope.planned_invoice_id::text,
      'scope_key',scope.planned_invoice_id::text,
      'invoice_id',case when exists(select 1 from public.invoices existing
        where existing.id=scope.planned_invoice_id)
        then scope.planned_invoice_id end,
      'validation_purpose','GENERATION_COMMIT',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.planned_invoice_id),'[]'::jsonb) scopes
    from(
      select m.planned_invoice_id,min(m.payload_json::text)::jsonb payload_json,
        jsonb_agg(jsonb_build_object(
          'timesheet_id',m.timesheet_id,
          'source_type',m.source_type,
          'source_id',m.source_id,
          'source_member_key',m.source_member_key,
          'segment_id',m.segment_id,
          'target_invoice_week',m.payload_json->>'target_invoice_week',
          'vat_rate_pct',v.vat_rate)
          order by m.source_member_key) planned_members
      from members m
      left join commit_vat_eval v
        on v.source_member_key=m.source_member_key
      group by m.planned_invoice_id
    ) scope
  ),
  commit_correction_eval as materialized (
    select r.*
    from commit_correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),  source_still_valid as materialized (
    select m.chunk_id,
      bool_and(tf.processing_status='READY_FOR_INVOICE' and not tf.is_stale
        and (tf.locked_by_invoice_id is null
          or coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS')
        and ts.authorised_at_server is not null
        and not(upper(coalesce(ts.submission_mode::text,''))='QR'
          and(nullif(ts.qr_signed_hash,'') is null
            or ts.qr_signed_at_utc is null))
        and(not coalesce(pc.require_reference_to_invoice,false)
          or coalesce(ref.reference_ready,false))
        and coalesce(vat.valid,false)
        and coalesce(corr.valid,true)
        and not(coalesce(vs.hr_validation_required_for_invoice,false)
          and upper(coalesce(tf.hr_crosscheck_status,'')) not in(
            'OK','PASS','PASSED','MATCHED','NOT_REQUIRED'))
        and not(coalesce(vs.client_is_nhsp,false)
          and coalesce(vs.nhsp_shift_count,0)>0
          and coalesce(vs.nhsp_shift_included_count,0)=0)
        and not((coalesce(tf.mileage_pay_ex_vat,0)<>0
              or coalesce(tf.mileage_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.travel_pay_ex_vat,0)<>0
              or coalesce(tf.travel_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='TRAVEL'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.accommodation_pay_ex_vat,0)<>0
              or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='ACCOMMODATION'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.expenses_pay_ex_vat,0)<>0
              or coalesce(tf.expenses_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.other_pay_ex_vat,0)<>0
              or coalesce(tf.other_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null))
        and not exists(
          select 1
          from public.timesheet_evidence e
          left join public.invoice_document_assets a
            on a.id=e.document_asset_id
          where e.timesheet_id=m.timesheet_id
            and(
              (upper(coalesce(e.kind,''))='MILEAGE'
                and(coalesce(tf.mileage_pay_ex_vat,0)<>0
                  or coalesce(tf.mileage_charge_ex_vat,0)<>0))
              or(upper(coalesce(e.kind,'')) in(
                  'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
                and(coalesce(tf.expenses_pay_ex_vat,0)<>0
                  or coalesce(tf.expenses_charge_ex_vat,0)<>0
                  or coalesce(tf.travel_pay_ex_vat,0)<>0
                  or coalesce(tf.travel_charge_ex_vat,0)<>0
                  or coalesce(tf.accommodation_pay_ex_vat,0)<>0
                  or coalesce(tf.accommodation_charge_ex_vat,0)<>0)))
            and(e.document_asset_id is null
              or a.status in('UNSUPPORTED','CORRUPT','MISSING',
                'FAILED','SUPERSEDED')))
        and not exists(
          select 1
          from public.invoice_lines l
          join public.invoices existing on existing.id=l.invoice_id
          where l.timesheet_id=m.timesheet_id
            and existing.id<>m.planned_invoice_id
            and existing.status in('DRAFT','ISSUED','ON_HOLD')
            and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS')
      ) still_valid
    from members m join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.timesheets ts on ts.timesheet_id=m.timesheet_id and ts.is_current and ts.revoked_at is null
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=m.timesheet_id
    left join public.v_timesheets_summary_base vs
      on vs.timesheet_id=m.timesheet_id
    left join commit_reference_eval ref
      on ref.source_member_key=m.source_member_key
    left join commit_vat_eval vat
      on vat.source_member_key=m.source_member_key
    left join commit_correction_eval corr
      on corr.scope_key=m.planned_invoice_id::text
    group by m.chunk_id
  ),
  commit_resolver_inputs as materialized (
    select c.id chunk_id,c.payload_json,
      row_number() over(order by c.id)::integer command_no
    from normal_chunks c
  ),
  commit_resolver_results as materialized (
    select ri.chunk_id,r.source_revision_hash,r.blocker_code
    from private._invoice_generation_resolve_command_groups(
      (select coalesce(jsonb_agg(ri.payload_json order by ri.command_no),
        '[]'::jsonb) from commit_resolver_inputs ri),null,v_now) r
    join commit_resolver_inputs ri on ri.command_no=r.command_no
    where r.group_key=ri.payload_json->>'group_key'
  ),
  revision_check as materialized (
    select c.id chunk_id,r.source_revision_hash current_revision,
      s.still_valid and r.blocker_code is null still_valid
    from normal_chunks c
    join source_still_valid s on s.chunk_id=c.id
    join commit_resolver_results r on r.chunk_id=c.id
  ),
  rejected as (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code','SOURCE_CHANGED_BEFORE_COMMIT'),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from revision_check r
    where c.id=r.chunk_id and (not r.still_valid or r.current_revision<>c.payload_json->>'source_revision')
    returning c.id
  ),
  valid_chunks as materialized (
    select c.* from normal_chunks c join revision_check r on r.chunk_id=c.id
    where r.still_valid and r.current_revision=c.payload_json->>'source_revision'
      and not exists(select 1 from rejected x where x.id=c.id)
  ),
  header_source as materialized (
    select vc.id chunk_id,vc.planned_invoice_id,(array_agg(tf.client_id order by tf.client_id))[1] client_id,
      max(cl.name) client_name,max(cl.invoice_address) client_invoice_address,
      max(cl.primary_invoice_email) primary_invoice_email,bool_and(cl.vat_chargeable) vat_chargeable,
      max(cl.payment_terms_days) payment_terms_days,
      max(sd.agency_name) agency_name,max(sd.agency_logo) agency_logo,
      max(sd.registered_address) registered_address,max(sd.company_reg_number) company_reg_number,
      max(sd.bank_name) bank_name,max(sd.bank_sort_code) bank_sort_code,
      max(sd.bank_account_number) bank_account_number,max(sd.vat_registration_number) vat_registration_number,
      count(*)::integer source_count
    from valid_chunks vc join members m on m.chunk_id=vc.id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.clients cl on cl.id=tf.client_id cross join public.settings_defaults sd
    where sd.id=1 group by vc.id,vc.planned_invoice_id
  ),
  source_rows_base as materialized (
    select m.chunk_id,m.planned_invoice_id,m.payload_json,
      m.source_member_key,m.source_type,m.source_id,m.segment_id,tf.*,ts.booking_id,
      ts.week_ending_date,ts.reference_number,ts.sheet_scope,ts.submission_mode,ts.day_references_json,
      ts.actual_schedule_json,coalesce(ts.contract_id,cw.contract_id) contract_id,
      coalesce(cd.display_name,'Candidate '||left(coalesce(tf.candidate_id::text,m.timesheet_id::text),8)) candidate_display,
      coalesce(ct.daily_calc_of_invoices,false) daily_calc_of_invoices,
      coalesce(ct.bucket_labels_json,
        jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH'))
        bucket_labels_json,
      ct.role contract_role,ct.display_site contract_display_site,
      ct.ward_hint contract_ward_hint,
      null::numeric ordinary_vat_rate,
      case when ts.sheet_scope::text='WEEKLY'
          and ts.submission_mode::text='MANUAL'
          and jsonb_typeof(ts.actual_schedule_json)='array'
        then coalesce((
          select jsonb_agg(jsonb_build_object(
            'date',e.value->>'date','start',e.value->>'start',
            'end',e.value->>'end','start_utc',e.value->>'start_utc',
            'end_utc',e.value->>'end_utc','ref_num',e.value->>'ref_num')
            order by e.ordinality)
          from jsonb_array_elements(ts.actual_schedule_json)
            with ordinality e(value,ordinality)
          where nullif(btrim(e.value->>'start'),'') is not null
            and nullif(btrim(e.value->>'end'),'') is not null
        ),'[]'::jsonb)
        else '[]'::jsonb end schedule_refs
    from members m join valid_chunks vc on vc.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.timesheets ts on ts.timesheet_id=m.timesheet_id and ts.is_current
    left join lateral (
      select w.contract_id
      from public.contract_weeks w
      where w.timesheet_id=m.timesheet_id
      order by w.updated_at desc nulls last,w.created_at desc nulls last,w.id desc
      limit 1
    ) cw on true
    left join public.contracts ct on ct.id=coalesce(ts.contract_id,cw.contract_id)
    left join public.candidates cd on cd.id=tf.candidate_id
  ),
  vat_policy as materialized (
    select distinct on (v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',s.source_member_key,
        'source_type',s.source_type,'source_id',s.source_id,
        'timesheet_id',s.timesheet_id,'segment_id',s.segment_id,
        'ordinary_rate',s.ordinary_vat_rate,
        'effective_date',coalesce(
          case when s.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                s.payload_json->>'effective_settings_date','date')
            then(s.payload_json->>'effective_settings_date')::date end,
          s.week_ending_date::date))
        order by s.chunk_id,s.timesheet_id)
      from source_rows_base s
    ),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  source_rows as materialized (
    select s.*,v.vat_rate
    from source_rows_base s
    join vat_policy v on v.source_member_key=s.source_member_key and v.valid
  ),
  segment_lock_targets_pre as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'invoice_id',s.planned_invoice_id,
      'timesheet_id',s.timesheet_id,
      'segment_ids',to_jsonb(p.segment_ids),
      'expected_financial_revision',encode(digest(jsonb_build_object(
        'financial_id',s.id,
        'timesheet_version',s.timesheet_version,
        'updated_at',s.updated_at,
        'basis',s.basis,
        'invoice_breakdown_json',s.invoice_breakdown_json
      )::text,'sha256'),'hex'))
      order by s.planned_invoice_id,s.timesheet_id),'[]'::jsonb) targets
    from source_rows s
    join selected_segments p
      on p.chunk_id=s.chunk_id and p.timesheet_id=s.timesheet_id
    where cardinality(coalesce(p.segment_ids,array[]::text[]))>0
  ),
  segment_lock_authority_pre as materialized (
    select r.*
    from segment_lock_targets_pre t
    cross join lateral private._invoice_segment_lock_batch(t.targets,v_now) r
  ),
  segment_lock_failures_pre as materialized (
    select * from segment_lock_authority_pre where not success
  ),
  segment_entries as materialized (
    select s.*,seg.ordinality,
      coalesce(nullif(seg.value->>'segment_id',''),
        left(encode(digest(seg.value::text,'sha256'),'hex'),24)) segment_id,
      case when left(coalesce(seg.value->>'date',''),10)
        ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        then left(seg.value->>'date',10) end work_date,
      case when coalesce(seg.value->>'hours_day','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_day')::numeric
        else 0 end segment_hours_day,
      case when coalesce(seg.value->>'hours_night','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_night')::numeric
        else 0 end segment_hours_night,
      case when coalesce(seg.value->>'hours_sat','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_sat')::numeric
        else 0 end segment_hours_sat,
      case when coalesce(seg.value->>'hours_sun','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_sun')::numeric
        else 0 end segment_hours_sun,
      case when coalesce(seg.value->>'hours_bh','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_bh')::numeric
        else 0 end segment_hours_bh,
      case
        when coalesce(seg.value->>'pay_amount','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'pay_amount')::numeric
        when coalesce(seg.value->>'pay_ex_vat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'pay_ex_vat')::numeric
        else 0 end segment_pay_ex,
      case
        when coalesce(seg.value->>'charge_amount','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'charge_amount')::numeric
        when coalesce(seg.value->>'charge_ex_vat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'charge_ex_vat')::numeric
        else 0 end segment_charge_ex
    from source_rows s
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)
      with ordinality seg(value,ordinality)
    where upper(coalesce(s.invoice_breakdown_json->>'mode',''))='SEGMENTS'
      and nullif(seg.value->>'invoice_locked_invoice_id','') is null
      and exists(
        select 1 from selected_segments picked
        where picked.chunk_id=s.chunk_id
          and picked.timesheet_id=s.timesheet_id
          and coalesce(nullif(seg.value->>'segment_id',''),
            left(encode(digest(seg.value::text,'sha256'),'hex'),24))
            =any(coalesce(picked.segment_ids,array[]::text[])))
  ),
  segment_daily_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,s.work_date,
      sum(s.segment_hours_day) h_day,sum(s.segment_hours_night) h_night,
      sum(s.segment_hours_sat) h_sat,sum(s.segment_hours_sun) h_sun,
      sum(s.segment_hours_bh) h_bh,sum(s.segment_pay_ex) pay_ex,
      sum(s.segment_charge_ex) charge_ex,max(s.vat_rate) vat_rate,
      max(s.charge_day) charge_day,max(s.charge_night) charge_night,
      max(s.charge_sat) charge_sat,max(s.charge_sun) charge_sun,
      max(s.charge_bh) charge_bh,
      max(s.timesheet_version) timesheet_version,
      (array_agg(s.id order by s.id))[1] tsfin_id,
      max(s.contract_role) contract_role,
      max(s.contract_display_site) contract_display_site,
      max(s.contract_ward_hint) contract_ward_hint,
      (array_agg(s.bucket_labels_json order by s.ordinality))[1]
        bucket_labels_json
    from segment_entries s
    where s.daily_calc_of_invoices and s.work_date is not null
    group by s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,s.work_date
  ),
  segment_weekly_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,
      sum(s.segment_hours_day) h_day,sum(s.segment_hours_night) h_night,
      sum(s.segment_hours_sat) h_sat,sum(s.segment_hours_sun) h_sun,
      sum(s.segment_hours_bh) h_bh,sum(s.segment_pay_ex) pay_ex,
      sum(s.segment_charge_ex) charge_ex,max(s.vat_rate) vat_rate,
      max(s.charge_day) charge_day,max(s.charge_night) charge_night,
      max(s.charge_sat) charge_sat,max(s.charge_sun) charge_sun,
      max(s.charge_bh) charge_bh,
      max(s.timesheet_version) timesheet_version,
      (array_agg(s.id order by s.id))[1] tsfin_id,
      max(s.contract_role) contract_role,
      max(s.contract_display_site) contract_display_site,
      max(s.contract_ward_hint) contract_ward_hint,
      (array_agg(s.bucket_labels_json order by s.ordinality))[1]
        bucket_labels_json
    from segment_entries s
    where not s.daily_calc_of_invoices
       or not exists(select 1 from segment_entries d
         where d.chunk_id=s.chunk_id and d.timesheet_id=s.timesheet_id
           and d.daily_calc_of_invoices and d.work_date is not null)
    group by s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date
  ),
  nonsegment_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      coalesce(s.hours_day,0) h_day,coalesce(s.hours_night,0) h_night,coalesce(s.hours_sat,0) h_sat,
      coalesce(s.hours_sun,0) h_sun,coalesce(s.hours_bh,0) h_bh,
      round(coalesce(s.total_pay_ex_vat,0)-coalesce(s.additional_pay_ex_vat,0)
        -coalesce(s.expenses_pay_ex_vat,0)-coalesce(s.mileage_pay_ex_vat,0),2) pay_ex,
      round(coalesce(s.total_charge_ex_vat,0)-coalesce(s.additional_charge_ex_vat,0)
        -coalesce(s.expenses_charge_ex_vat,0)-coalesce(s.mileage_charge_ex_vat,0),2) charge_ex,
      s.vat_rate,s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,
      s.charge_bh,s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    where upper(coalesce(s.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
       or jsonb_array_length(case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
                                  then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)=0
  ),
  additional_daily_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,upper(a.key) code,
      left(d.key,10) work_date,
      case when pg_input_is_valid(d.value,'numeric')
        then d.value::numeric else 0 end units,
      case when pg_input_is_valid(a.value->>'pay_rate','numeric')
        then(a.value->>'pay_rate')::numeric else 0 end pay_rate,
      case when pg_input_is_valid(a.value->>'charge_rate','numeric')
        then(a.value->>'charge_rate')::numeric else 0 end charge_rate,
      round((case when pg_input_is_valid(d.value,'numeric')
          then d.value::numeric else 0 end)*
        (case when pg_input_is_valid(a.value->>'pay_rate','numeric')
          then(a.value->>'pay_rate')::numeric else 0 end),2) pay_ex,
      round((case when pg_input_is_valid(d.value,'numeric')
          then d.value::numeric else 0 end)*
        (case when pg_input_is_valid(a.value->>'charge_rate','numeric')
          then(a.value->>'charge_rate')::numeric else 0 end),2) charge_ex,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) bucket_name,
      coalesce(nullif(a.value->>'unit_name',''),'units') unit_name,
      a.value->'frequency' frequency,s.vat_rate,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    cross join lateral jsonb_each(
      case when jsonb_typeof(s.additional_units_json)='object'
        then s.additional_units_json else '{}'::jsonb end) a
    cross join lateral jsonb_each_text(
      case when jsonb_typeof(a.value->'days')='object'
        then a.value->'days' else '{}'::jsonb end) d
    where s.daily_calc_of_invoices
      and left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      and exists(select 1 from segment_entries se
        where se.chunk_id=s.chunk_id and se.timesheet_id=s.timesheet_id
          and se.work_date is not null)
  ),
  additional_weekly_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      upper(a.key) code,
      case when pg_input_is_valid(a.value->>'unit_count','numeric')
        then(a.value->>'unit_count')::numeric else 0 end units,
      case when pg_input_is_valid(a.value->>'pay_rate','numeric')
        then(a.value->>'pay_rate')::numeric else 0 end pay_rate,
      case when pg_input_is_valid(a.value->>'charge_rate','numeric')
        then(a.value->>'charge_rate')::numeric else 0 end charge_rate,
      case when pg_input_is_valid(a.value->>'pay_ex_vat','numeric')
        then(a.value->>'pay_ex_vat')::numeric else 0 end pay_ex,
      case when pg_input_is_valid(a.value->>'charge_ex_vat','numeric')
        then(a.value->>'charge_ex_vat')::numeric else 0 end charge_ex,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) bucket_name,
      coalesce(nullif(a.value->>'unit_name',''),'units') unit_name,
      a.value->'frequency' frequency,s.vat_rate,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s cross join lateral jsonb_each(
      case when jsonb_typeof(s.additional_units_json)='object'
        then s.additional_units_json else '{}'::jsonb end) a
    where jsonb_typeof(a.value)='object'
      and ((case when pg_input_is_valid(a.value->>'pay_ex_vat','numeric')
              then(a.value->>'pay_ex_vat')::numeric else 0 end)<>0
        or (case when pg_input_is_valid(a.value->>'charge_ex_vat','numeric')
              then(a.value->>'charge_ex_vat')::numeric else 0 end)<>0)
      and not(s.daily_calc_of_invoices
        and jsonb_typeof(a.value->'days')='object'
        and exists(select 1 from jsonb_each_text(
          case when jsonb_typeof(a.value->'days')='object'
            then a.value->'days' else '{}'::jsonb end) d
          where left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'))
  ),
  expense_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      e.code,e.pay_ex,e.charge_ex,s.vat_rate,s.expenses_description,
      s.expenses_evidence_r2_key,s.expenses_evidence_manifest,
      s.mileage_units,s.mileage_pay_rate,s.mileage_charge_rate,
      s.mileage_evidence_r2_key,s.mileage_evidence_manifest,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    cross join lateral (
      values
       ('TRAVEL',coalesce(s.travel_pay_ex_vat,0),coalesce(s.travel_charge_ex_vat,0)),
       ('ACCOMMODATION',coalesce(s.accommodation_pay_ex_vat,0),coalesce(s.accommodation_charge_ex_vat,0)),
       ('OTHER',coalesce(s.other_pay_ex_vat,0),coalesce(s.other_charge_ex_vat,0)),
       ('EXPENSES_FALLBACK',
          case when coalesce(s.travel_pay_ex_vat,0)+coalesce(s.accommodation_pay_ex_vat,0)+coalesce(s.other_pay_ex_vat,0)=0 then coalesce(s.expenses_pay_ex_vat,0) else 0 end,
          case when coalesce(s.travel_charge_ex_vat,0)+coalesce(s.accommodation_charge_ex_vat,0)+coalesce(s.other_charge_ex_vat,0)=0 then coalesce(s.expenses_charge_ex_vat,0) else 0 end),
       ('MILEAGE',coalesce(s.mileage_pay_ex_vat,0),coalesce(s.mileage_charge_ex_vat,0))
    ) e(code,pay_ex,charge_ex)
    where e.pay_ex<>0 or e.charge_ex<>0
  ),
  line_union as materialized (
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||work_date description,h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric pay_day,null::numeric pay_night,null::numeric pay_sat,
      null::numeric pay_sun,null::numeric pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_DAILY' line_type,
      'TS:'||timesheet_id||':HOURS:'||work_date source_key,
      jsonb_build_object('date',work_date,'timesheet_version',timesheet_version,
        'tsfin_id',tsfin_id,'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json) detail
    from segment_daily_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - W/E '||week_ending_date, h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_WEEKLY','TS:'||timesheet_id||':HOURS:WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json)
    from segment_weekly_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - W/E '||week_ending_date, h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_WEEKLY','TS:'||timesheet_id||':HOURS:WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json)
    from nonsegment_lines
    where pay_ex<>0 or charge_ex<>0 or h_day+h_night+h_sat+h_sun+h_bh<>0
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||bucket_name||' - '||work_date||' - '||
        units||' '||unit_name,0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'ADDITIONAL_RATE_DAILY',
      'TS:'||timesheet_id||':ADD:'||code||':'||work_date,
      jsonb_build_object('date',work_date,'timesheet_version',timesheet_version,
        'tsfin_id',tsfin_id,'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'bucket',jsonb_build_object('code',code,'bucket_name',bucket_name,
          'unit_name',unit_name,'frequency',frequency),
        'units',jsonb_build_object('unit_count',units,'pay_rate',pay_rate,
          'charge_rate',charge_rate))
    from additional_daily_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||bucket_name||' - '||units||' '||unit_name,0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'ADDITIONAL_RATE',
      'TS:'||timesheet_id||':ADD:'||code||':WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'bucket',jsonb_build_object('code',code,'bucket_name',bucket_name,
          'unit_name',unit_name,'frequency',frequency),
        'units',jsonb_build_object('unit_count',units,'pay_rate',pay_rate,
          'charge_rate',charge_rate))
    from additional_weekly_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      case when code='MILEAGE' then 'Mileage - '||coalesce(mileage_units,0)||
          ' miles (W/E '||week_ending_date||')'
        else initcap(replace(code,'_',' '))||' (W/E '||week_ending_date||')' end,
      0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,
      case when code='MILEAGE' then 'MILEAGE'
        when code='EXPENSES_FALLBACK' then 'EXPENSES_TOTAL'
        else 'EXPENSE_'||code end,
      case when code='MILEAGE' then 'TS:'||timesheet_id||':MILEAGE'
        when code='EXPENSES_FALLBACK' then 'TS:'||timesheet_id||':EXP:TOTAL'
        else 'TS:'||timesheet_id||':EXP:'||code end,
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'expense',case when code='MILEAGE' then jsonb_build_object(
          'category','MILEAGE','mileage_units',mileage_units,
          'pay_rate',mileage_pay_rate,'charge_rate',mileage_charge_rate,
          'evidence_r2_key',mileage_evidence_r2_key,
          'evidence_manifest',mileage_evidence_manifest)
        else jsonb_build_object('category',code,'note',expenses_description,
          'evidence_r2_key',expenses_evidence_r2_key,
          'evidence_manifest',expenses_evidence_manifest) end)
    from expense_lines
  ),
  correction_scopes_pre as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-prewrite:'||scope.planned_invoice_id::text,
      'scope_key',scope.planned_invoice_id::text,
      'invoice_id',case when exists(select 1 from public.invoices existing
        where existing.id=scope.planned_invoice_id)
        then scope.planned_invoice_id end,
      'validation_purpose','GENERATION_PREWRITE',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.planned_invoice_id),'[]'::jsonb) scopes
    from(
      select l.planned_invoice_id,min(vc.payload_json::text)::jsonb payload_json,
        jsonb_agg(distinct jsonb_build_object(
          'timesheet_id',l.timesheet_id,
          'vat_rate_pct',l.vat_rate))
          filter(where l.timesheet_id is not null) planned_members
      from line_union l
      join valid_chunks vc on vc.id=l.chunk_id
      group by l.planned_invoice_id
    ) scope
  ),
  correction_validation_pre as materialized (
    select r.*
    from correction_scopes_pre s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),
  correction_failures_pre as materialized (
    select scope_key,blocker_code,detail_json detail
    from correction_validation_pre
    where not valid
  ),  write_eligible_chunks as materialized (
    select vc.*
    from valid_chunks vc
    where not exists(
      select 1 from correction_failures_pre f
      where f.scope_key=vc.planned_invoice_id::text)
  ),
  line_totals as materialized (
    select l.planned_invoice_id,
      round(sum(round(l.charge_ex,2)),2) subtotal_ex_vat,
      round(sum(round(l.charge_ex*l.vat_rate/100,2)),2) vat_amount,
      round(sum(round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2)),2) total_inc_vat
    from line_union l
    join write_eligible_chunks w on w.id=l.chunk_id
    group by l.planned_invoice_id
  ),
  existing_target_headers as materialized (
    select vc.id chunk_id,i.id invoice_id
    from write_eligible_chunks vc
    join header_source h on h.chunk_id=vc.id
    join public.invoices i on i.id=vc.planned_invoice_id
      and i.client_id=h.client_id and i.type='INVOICE' and i.status='DRAFT'
    where not exists(
      select 1 from segment_lock_failures_pre f
      where f.invoice_id=vc.planned_invoice_id)
  ),
  inserted_headers as (
    insert into public.invoices(id,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
      header_snapshot_json,document_revision,document_state,created_at,updated_at)
    select h.planned_invoice_id,h.client_id,'DRAFT',v_now,
      t.subtotal_ex_vat,t.vat_amount,t.total_inc_vat,
      jsonb_build_object('client_id',h.client_id,'client_name',h.client_name,
        'client_invoice_address',h.client_invoice_address,'client_primary_invoice_email',h.primary_invoice_email,
        'agency_name',h.agency_name,'agency_logo',h.agency_logo,'registered_address',h.registered_address,
        'company_reg_number',h.company_reg_number,'company_registration_number',h.company_reg_number,
        'vat_chargeable',h.vat_chargeable,'payment_terms_days',h.payment_terms_days,
        'issued_at_utc',null,'due_at_utc',null,
        'totals',jsonb_build_object(
          'subtotal_ex_vat',t.subtotal_ex_vat,
          'vat_amount',t.vat_amount,'total_inc_vat',t.total_inc_vat),
        'bank',jsonb_build_object('name',h.bank_name,'sort_code',h.bank_sort_code,'account_number',h.bank_account_number),
        'vat_registration_number',h.vat_registration_number,
        'meta',jsonb_build_object('source','INVOICE_OPERATION_QUEUE',
          'invoice_week_start',vc.payload_json->>'target_invoice_week',
          'consolidation_mode',vc.payload_json->>'consolidation_mode',
          'self_bill',vc.payload_json->>'invoice_stream'='SELF_BILL','timesheet_count',h.source_count),
        'attach_policy',vc.payload_json#>'{plan,settings_snapshot,attach_policy}'),
      1,'STALE',v_now,v_now
    from header_source h join write_eligible_chunks vc on vc.id=h.chunk_id
    join line_totals t on t.planned_invoice_id=h.planned_invoice_id
    where not exists(
      select 1 from segment_lock_failures_pre f
      where f.invoice_id=h.planned_invoice_id)
    on conflict(id) do nothing returning id
  ),
  target_headers as materialized (
    select vc.id chunk_id,h.id invoice_id
    from inserted_headers h
    join write_eligible_chunks vc on vc.planned_invoice_id=h.id
    union all
    select e.chunk_id,e.invoice_id
    from existing_target_headers e
  ),
  inserted_lines as (
    insert into public.invoice_lines(
      invoice_id,timesheet_id,booking_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
      paper_ts_r2_key,meta_json,source_key
    )
    select l.planned_invoice_id,l.timesheet_id,l.booking_id,l.description,
      round(l.h_day,2),round(l.h_night,2),round(l.h_sat,2),round(l.h_sun,2),round(l.h_bh,2),
      l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
      l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,
      round(l.pay_ex,2),round(l.charge_ex,2),round(l.charge_ex-l.pay_ex,2),l.vat_rate,
      round(l.charge_ex*l.vat_rate/100,2),round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2),
      null,jsonb_build_object('line_type',l.line_type,'timesheet_id',l.timesheet_id,
        'candidate_display',l.description,'week_ending_date',
        (select s.week_ending_date from source_rows s where s.timesheet_id=l.timesheet_id limit 1),
        'schedule_references',
        (select s.schedule_refs from source_rows s where s.timesheet_id=l.timesheet_id limit 1),
        'totals',jsonb_build_object('line_pay_ex_vat',round(l.pay_ex,2),
          'line_charge_ex_vat',round(l.charge_ex,2),'margin_ex_vat',round(l.charge_ex-l.pay_ex,2),
          'vat_rate_pct',l.vat_rate,'vat_amount',round(l.charge_ex*l.vat_rate/100,2),
          'total_inc_vat',round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2)))||l.detail,l.source_key
    from line_union l join target_headers h on h.invoice_id=l.planned_invoice_id
    on conflict(invoice_id,source_key) do nothing
    returning invoice_id,timesheet_id,total_charge_ex_vat,vat_amount,total_inc_vat
  ),
  all_target_lines as materialized (
    select l.invoice_id,l.total_charge_ex_vat,l.vat_amount,l.total_inc_vat
    from public.invoice_lines l
    where l.invoice_id in(select invoice_id from target_headers)
    union all
    select l.invoice_id,l.total_charge_ex_vat,l.vat_amount,l.total_inc_vat
    from inserted_lines l
  ),
  all_line_totals as materialized (
    select l.invoice_id,count(*)::integer line_count,
      round(sum(l.total_charge_ex_vat),2) subtotal_ex_vat,
      round(sum(l.vat_amount),2) vat_amount,
      round(sum(l.total_inc_vat),2) total_inc_vat
    from all_target_lines l
    group by l.invoice_id
  ),
  updated_header_totals as (
    update public.invoices i
    set subtotal_ex_vat=t.subtotal_ex_vat,vat_amount=t.vat_amount,
      total_inc_vat=t.total_inc_vat,document_state='STALE',
      header_snapshot_json=coalesce(i.header_snapshot_json,'{}'::jsonb)
        ||jsonb_build_object('totals',jsonb_build_object(
          'subtotal_ex_vat',t.subtotal_ex_vat,'vat_amount',t.vat_amount,
          'total_inc_vat',t.total_inc_vat)),
      updated_at=v_now
    from all_line_totals t where i.id=t.invoice_id
    returning i.id
  ),
  segment_lock_targets as materialized (
    select targets from segment_lock_targets_pre
  ),
  segment_lock_authority as materialized (
    select * from segment_lock_authority_pre
  ),
  segment_lock_failures as materialized (
    select * from segment_lock_authority where not success
  ),
  whole_lock as (
    update public.timesheets_financials tf
    set locked_by_invoice_id=vc.planned_invoice_id,locked_at_utc=v_now,updated_at=v_now
    from members m join write_eligible_chunks vc on vc.id=m.chunk_id
    cross join (select count(*) applied_count from segment_lock_authority) segment_application
    where tf.timesheet_id=m.timesheet_id and tf.is_current
      and (coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
        or not exists(select 1 from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) e
                      where nullif(e->>'invoice_locked_invoice_id','') is null))
    returning tf.timesheet_id
  ),
  week_lock as (
    update public.contract_weeks cw set status='INVOICED',updated_at=v_now
    where cw.timesheet_id in(select timesheet_id from members)
      and exists(select 1 from public.timesheets_financials tf where tf.timesheet_id=cw.timesheet_id
        and tf.is_current and (tf.locked_by_invoice_id is not null or
          not exists(select 1 from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) e
                     where nullif(e->>'invoice_locked_invoice_id','') is null)))
    returning cw.id
  ),
  hr_sources as (
    insert into public.invoice_hr_source_rows(invoice_id,source_system,import_id,header_columns,rows_json,header_rows)
    select distinct vc.planned_invoice_id,
      case when tf.basis::text='NHSP' then 'NHSP' else 'HEALTHROSTER' end,
      tf.nhsp_import_id,'[]'::jsonb,
      case when jsonb_typeof(tf.external_source_rows_json)='array' then tf.external_source_rows_json else '[]'::jsonb end,
      '[]'::jsonb
    from members m join write_eligible_chunks vc on vc.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    where tf.nhsp_import_id is not null
    on conflict(invoice_id,source_system,import_id) do update set rows_json=excluded.rows_json
    returning invoice_id
  ),
  source_segments as materialized (
    select distinct s.planned_invoice_id,
      substr(x.value->>'segment_id',6)::uuid shift_id
    from source_rows s
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end) x(value)
    where left(x.value->>'segment_id',5)='nhsp:'
      and substr(x.value->>'segment_id',6) ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (
        upper(coalesce(x.value->>'source_system',''))='NHSP'
        or (upper(coalesce(x.value->>'source_system',''))='HEALTHROSTER'
          and coalesce((s.payload_json#>>'{plan,settings_snapshot,attach_policy,requires_hr}')::boolean,false)
          and coalesce((s.payload_json#>>'{plan,settings_snapshot,attach_policy,hr_attach_to_invoice}')::boolean,true)))
  ),
  source_imports as materialized (
    select s.planned_invoice_id,upper(coalesce(n.source_system::text,'UNKNOWN')) source_system,
      n.latest_import_id import_id,jsonb_agg(distinct n.external_row_key) row_keys
    from source_segments s join public.nhsp_shifts n on n.id=s.shift_id
    where n.latest_import_id is not null and n.external_row_key is not null
    group by s.planned_invoice_id,upper(coalesce(n.source_system::text,'UNKNOWN')),n.latest_import_id
  ),
  authoritative_hr_sources as (
    insert into public.invoice_hr_source_rows(
      invoice_id,source_system,import_id,header_columns,rows_json)
    select g.planned_invoice_id,g.source_system,g.import_id,
      case when jsonb_typeof(i.parse_summary_json->'header_columns')='array'
        then i.parse_summary_json->'header_columns' else '[]'::jsonb end,
      coalesce((select jsonb_agg(r.payload_json order by r.id)
        from public.hr_rows r where r.import_id=g.import_id
          and r.external_row_key in(select jsonb_array_elements_text(g.row_keys))),
        '[]'::jsonb)
    from source_imports g join public.hr_imports i on i.id=g.import_id
    on conflict(invoice_id,source_system,import_id) do update
      set header_columns=excluded.header_columns,rows_json=excluded.rows_json
    returning invoice_id
  ),
  audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,after_json,reason)
    select v_now,o.actor_user_id,u.display_name,u.role,'invoices',vc.planned_invoice_id::text,
      case when exists(select 1 from inserted_headers h
        where h.id=vc.planned_invoice_id)
        then 'INVOICE_CREATED' else 'INVOICE_DRAFT_APPENDED' end,
      jsonb_build_object('invoice_id',vc.planned_invoice_id,
        'source_ids',coalesce(vc.payload_json->'canonical_source_ids',
          vc.payload_json->'source_ids','[]'::jsonb),
        'source_revision',vc.payload_json->>'source_revision',
        'operation_id',vc.operation_id),'INVOICE_OPERATION_QUEUE'
    from write_eligible_chunks vc join public.invoice_operations o on o.id=vc.operation_id
    left join public.tms_users u on u.id=o.actor_user_id
    returning id
  ),
  correction_failures as materialized (
    select scope_key::uuid invoice_id,blocker_code,detail
    from correction_failures_pre
    where scope_key~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  update public.invoice_operation_chunks c
    set status=case when exists(select 1 from all_line_totals l
                               where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then 'QUEUED' else 'BLOCKED' end,
      phase=case when exists(select 1 from all_line_totals l
                            where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then 'QUEUE_DOCUMENT' else 'BLOCKED' end,
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(vc.planned_invoice_id),
        'source_revision',vc.payload_json->>'source_revision'),
      error_json=case
        when not exists(select 1 from all_line_totals l
          where l.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code','NO_INVOICE_LINES_CREATED')
        when exists(select 1 from segment_lock_failures sf
          where sf.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code','SEGMENT_LOCK_FAILED','detail',(
            select jsonb_agg(to_jsonb(sf))
            from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id))
        when exists(select 1 from correction_failures cf
          where cf.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code',(
            select cf.blocker_code from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id))
        else null end,
      completed_at_utc=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from valid_chunks vc
    cross join (select count(*) applied_count from segment_lock_authority) segment_application
    cross join (select count(*) cached_count from authoritative_hr_sources) source_cache_application
    where c.id=vc.id
  ;
  select coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,
      'result',c.result_json,'error',c.error_json)),'[]'::jsonb)
    into v_part
  from jsonb_array_elements(p_claims) x
  join public.invoice_operation_chunks c
    on c.id=case when coalesce(x->>'chunk_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x->>'chunk_id')::uuid end
  where x->>'phase'='COMMIT'
    and coalesce(c.payload_json->>'command_type','')<>'GENERATE_CREDIT_NOTE';
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Credit-note COMMIT: exact negative line clone and source unlock; document creation stays asynchronous.
  execute $q$
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements($1) x where x->>'phase'='COMMIT'
  ),
  credit_revisions as materialized (
    select c.*,i.id source_invoice_id,(c.payload_json#>>'{plan,planned_invoice_id}')::uuid credit_note_id
      ,encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),
        'sha256'),'hex') current_revision
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i on i.id=c.entity_id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
      and i.type='INVOICE' and i.status in ('ISSUED','PAID')
  ),
  credit_correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','credit-source:'||c.source_invoice_id::text,
      'scope_key',c.source_invoice_id::text,
      'invoice_id',c.source_invoice_id,
      'validation_purpose','CREDIT_SOURCE')
      order by c.source_invoice_id),'[]'::jsonb) scopes
    from(select distinct source_invoice_id from credit_revisions) c
  ),
  credit_correction_failures as materialized (
    select r.invoice_id source_invoice_id,r.blocker_code,r.detail_json
    from credit_correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,($2 at time zone 'Europe/London')::date) r
    where not r.valid
  ),
  credit_rejected as (
    update public.invoice_operation_chunks q
    set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code',case
        when c.current_revision<>c.payload_json->>'source_revision'
          then 'CREDIT_SOURCE_CHANGED_BEFORE_COMMIT'
        else coalesce((select f.blocker_code
          from credit_correction_failures f
          where f.source_invoice_id=c.source_invoice_id
          order by f.blocker_code limit 1),'CREDIT_CORRECTION_VALIDATION_FAILED')
        end),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=$2
    from credit_revisions c
    where q.id=c.id and(
      c.current_revision<>c.payload_json->>'source_revision'
      or exists(select 1 from credit_correction_failures f
        where f.source_invoice_id=c.source_invoice_id))
    returning q.id
  ),
  credits as materialized (
    select c.* from credit_revisions c
    where c.current_revision=c.payload_json->>'source_revision'
      and not exists(select 1 from credit_rejected r where r.id=c.id)
  ),
  inserted_credit as (
    insert into public.invoices(id,type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
      original_invoice_id,notes,header_snapshot_json,document_revision,document_state,created_at,updated_at)
    select c.credit_note_id,'CREDIT_NOTE',i.client_id,'DRAFT',$2,
      -i.subtotal_ex_vat,-i.vat_amount,-i.total_inc_vat,i.id,
      c.payload_json->>'credit_reason',
      i.header_snapshot_json||jsonb_build_object(
        'credit_source_invoice_id',i.id,
        'credit_reason',c.payload_json->>'credit_reason'),1,'STALE',$2,$2
    from credits c join public.invoices i on i.id=c.source_invoice_id
    on conflict(id) do nothing returning id
  ),
  cloned_lines as (
    insert into public.invoice_lines(invoice_id,timesheet_id,booking_id,description,
      hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,total_pay_ex_vat,total_charge_ex_vat,
      margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,paper_ts_r2_key,meta_json,source_key)
    select c.credit_note_id,l.timesheet_id,l.booking_id,
      'CREDIT NOTE – '||coalesce(l.description,''),
      l.hours_day,l.hours_night,l.hours_sat,l.hours_sun,l.hours_bh,
      l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
      l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,
      -l.total_pay_ex_vat,-l.total_charge_ex_vat,-l.margin_ex_vat,l.vat_rate_pct,
      -l.vat_amount,-l.total_inc_vat,l.paper_ts_r2_key,
      coalesce(l.meta_json,'{}'::jsonb)||jsonb_build_object(
        'credit_note',true,'original_invoice_id',c.source_invoice_id,
        'original_invoice_line_id',l.id,'credit_of_line_id',l.id,'line_type',
        'CREDIT_'||coalesce(l.meta_json->>'line_type','LINE')),
      'CN:'||c.credit_note_id||':LINE:'||l.id
    from credits c join public.invoice_lines l on l.invoice_id=c.source_invoice_id
    on conflict(invoice_id,source_key) do nothing returning invoice_id
  ),
  source_mark as (
    update public.invoices i set credit_note_created_at_utc=$2,updated_at=$2
    where i.id in(select source_invoice_id from credits) returning i.id
  ),
  unlocks as (
    update public.timesheets_financials tf
    set locked_by_invoice_id=null,locked_at_utc=null,
      unlocked_by_credit_note_id=c.credit_note_id,is_stale=true,
      stale_reason='UNLOCKED_BY_CREDIT',updated_at=$2
    from credits c
    where tf.locked_by_invoice_id=c.source_invoice_id and tf.is_current returning tf.timesheet_id
  ),
  segment_unlocks as (
    update public.timesheets_financials tf
    set invoice_breakdown_json=jsonb_set(tf.invoice_breakdown_json,'{segments}',
      (select jsonb_agg(case
        when e.value->>'invoice_locked_invoice_id'=c.source_invoice_id::text
          then (e.value-'invoice_locked_invoice_id'-'invoice_locked_at_utc')
        else e.value end order by e.ordinality)
       from jsonb_array_elements(tf.invoice_breakdown_json->'segments')
         with ordinality e(value,ordinality)),true),
      unlocked_by_credit_note_id=c.credit_note_id,is_stale=true,
      stale_reason='UNLOCKED_BY_CREDIT',updated_at=$2
    from credits c
    where tf.is_current and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
      and exists(select 1 from jsonb_array_elements(tf.invoice_breakdown_json->'segments') e
        where e->>'invoice_locked_invoice_id'=c.source_invoice_id::text)
    returning tf.timesheet_id
  ),
  recompute_outbox as (
    insert into public.ts_financials_outbox(
      timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at)
    select distinct u.timesheet_id,'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,$2,null,$2
    from (
      select timesheet_id from unlocks
      union all select timesheet_id from segment_unlocks
    ) u
    on conflict on constraint uq_tsfin_outbox do nothing
    returning timesheet_id
  ),
  credit_audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,
      actor_role_at_time,object_type,object_id_text,action,after_json,reason)
    select $2,o.actor_user_id,u.display_name,u.role,'invoices',
      c.credit_note_id::text,'CREDIT_NOTE_CREATED',
      jsonb_build_object('credit_note_id',c.credit_note_id,
        'original_invoice_id',c.source_invoice_id,
        'credit_reason',c.payload_json->>'credit_reason',
        'command_token',c.payload_json->>'command_token',
        'subtotal_ex_vat',-i.subtotal_ex_vat,'vat_amount',-i.vat_amount,
        'total_inc_vat',-i.total_inc_vat,
        'financial_recompute_enqueued',true),
      'INVOICE_OPERATION_QUEUE'
    from credits c join public.invoices i on i.id=c.source_invoice_id
    join public.invoice_operations o on o.id=c.operation_id
    left join public.tms_users u on u.id=o.actor_user_id
    returning id
  ),
  completed as (
    update public.invoice_operation_chunks q
    set status='QUEUED',
      phase='QUEUE_DOCUMENT',
      error_json=null,
      completed_at_utc=null,lease_owner=null,lease_token=null,
      lease_expires_at_utc=null,updated_at_utc=$2,
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(c.credit_note_id),
        'credit_note_id',c.credit_note_id,'source_invoice_id',c.source_invoice_id)
    from credits c
    cross join (select count(*) outbox_count from recompute_outbox) outbox_application
    cross join (select count(*) audit_count from credit_audits) audit_application
    where q.id=c.id
    returning q.id,q.status,q.phase,q.result_json,q.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json)),'[]'::jsonb)
  from completed
  $q$
  into v_part using p_claims,v_now;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Document work is queued in a separate SQL statement so source-invalidation
  -- statement triggers cannot supersede the freshly-created document operation.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='QUEUE_DOCUMENT'
  ),
  queued_sources as materialized (
    select c.id chunk_id,c.operation_id parent_operation_id,
      (c.payload_json#>>'{plan,planned_invoice_id}')::uuid planned_invoice_id,
      c.payload_json->>'source_revision' source_revision,
      c.payload_json->>'command_type' command_type,
      c.payload_json->>'command_token' command_token,
      c.payload_json->>'credit_reason' credit_reason,
      i.document_revision::text document_revision
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i
      on i.id=(c.payload_json#>>'{plan,planned_invoice_id}')::uuid
  ),
  existing_doc_versions as materialized (
    select distinct on(q.chunk_id) q.chunk_id,v.id document_version_id,
      v.operation_id,v.status
    from queued_sources q
    join public.invoice_document_versions v
      on v.entity_type='INVOICE'
     and v.entity_id=q.planned_invoice_id
     and v.purpose='DRAFT_PREVIEW'
     and v.source_revision=q.document_revision
     and v.template_version='invoice-professional-v1'
     and v.status in(
       'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
       'VERIFYING','READY')
    where q.command_type<>'GENERATE_CREDIT_NOTE'
    order by q.chunk_id,case when v.status='READY' then 0 else 1 end,
      v.created_at_utc desc,v.id desc
  ),
  doc_ops as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,created_at_utc,updated_at_utc)
    select q.parent_operation_id,'BUILD_DOCUMENT','INVOICE',q.planned_invoice_id,o.actor_user_id,
      encode(digest('DRAFT_PREVIEW|'||q.planned_invoice_id||'|'||q.document_revision||
        '|invoice-professional-v1','sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',550,q.document_revision,'invoice-professional-v1',
      jsonb_build_object('invoice_id',q.planned_invoice_id,'purpose','DRAFT_PREVIEW'),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,
      nextval('public.invoice_operation_change_seq'),v_now,v_now
    from queued_sources q join public.invoice_operations o on o.id=q.parent_operation_id
    where q.command_type<>'GENERATE_CREDIT_NOTE'
      and not exists(
        select 1 from existing_doc_versions e where e.chunk_id=q.chunk_id)
    on conflict(idempotency_key) where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,excluded.priority),updated_at_utc=v_now
    returning *
  ),
  selected_doc_ops as materialized (
    select q.chunk_id,q.planned_invoice_id,q.document_revision source_revision,
      d.id operation_id,d.control_version,null::uuid existing_version_id,
      null::text existing_status
    from queued_sources q join doc_ops d on d.entity_id=q.planned_invoice_id
    union all
    select q.chunk_id,q.planned_invoice_id,q.document_revision,
      e.operation_id,o.control_version,e.document_version_id,e.status
    from queued_sources q
    join existing_doc_versions e on e.chunk_id=q.chunk_id
    join public.invoice_operations o on o.id=e.operation_id
  ),
  doc_versions as materialized (
    insert into public.invoice_document_versions(entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select 'INVOICE',d.planned_invoice_id,'DRAFT_PREVIEW',d.operation_id,d.source_revision,
      'invoice-professional-v1','PLANNING','{}',encode(digest('{}','sha256'),'hex'),
      '[]',encode(digest('[]','sha256'),'hex'),v_now
    from selected_doc_ops d
    where d.existing_version_id is null
    on conflict(entity_type,entity_id,purpose,source_revision,template_version)
      where purpose in('DRAFT_PREVIEW','TIMESHEET')
        and status in ('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning id,entity_id,operation_id
  ),
  all_doc_versions as materialized (
    select d.chunk_id,d.planned_invoice_id,d.operation_id,d.control_version,
      d.source_revision,
      coalesce(d.existing_version_id,v.id) document_version_id,
      coalesce(d.existing_status,'PLANNING') document_status
    from selected_doc_ops d
    left join doc_versions v
      on v.entity_id=d.planned_invoice_id and v.operation_id=d.operation_id
    where d.existing_version_id is not null or v.id is not null
  ),
  doc_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select d.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',d.document_version_id::text,
        d.source_revision,'invoice-professional-v1','1'),'sha256'),'hex'),
      0,'INVOICE',d.planned_invoice_id,d.document_version_id,
      'QUEUED',550,v_now,jsonb_build_object('purpose','DRAFT_PREVIEW'),d.control_version,v_now,v_now
    from all_doc_versions d
    where d.document_status<>'READY'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning operation_id
  ),
  invoice_ptrs as (
    update public.invoices i set preview_document_version_id=d.document_version_id,
      active_document_operation_id=case when d.document_status='READY'
        then null else d.operation_id end,
      document_state=case when d.document_status='READY'
        then 'READY' else 'QUEUED' end,updated_at=v_now
    from all_doc_versions d where i.id=d.planned_invoice_id returning i.id
  ),
  credit_issue_ops as materialized (
    insert into public.invoice_operations(
      parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,
      input_json,config_json,progress_json,total_units,chunk_count,
      control_version,change_seq,created_at_utc,updated_at_utc)
    select q.parent_operation_id,'ISSUE_INVOICES','INVOICE_BATCH',null,
      o.actor_user_id,
      encode(digest('CREDIT_ISSUE|'||q.planned_invoice_id||'|'||
        q.document_revision||'|'||coalesce(q.command_token,''),'sha256'),'hex'),
      'QUEUED','VALIDATE',850,q.document_revision,'invoice-professional-v1',
      jsonb_build_object(
        'invoice_ids',jsonb_build_array(q.planned_invoice_id),
        'credit_note',true,'credit_reason',q.credit_reason,
        'command_token',q.command_token,'deliver',false),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    from queued_sources q
    join public.invoice_operations o on o.id=q.parent_operation_id
    where q.command_type='GENERATE_CREDIT_NOTE'
    on conflict(idempotency_key)
      where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,excluded.priority),
      updated_at_utc=v_now
    returning *
  ),
  selected_credit_issue as materialized (
    select q.chunk_id,q.planned_invoice_id,q.document_revision,
      q.command_token,q.credit_reason,o.id operation_id,o.control_version
    from queued_sources q
    join credit_issue_ops o on o.parent_operation_id=q.parent_operation_id
      and o.operation_type='ISSUE_INVOICES'
    where q.command_type='GENERATE_CREDIT_NOTE'
  ),
  credit_issue_chunks as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,status,
      priority,run_after_utc,payload_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select s.operation_id,'ISSUE_INVOICE','VALIDATE',
      encode(digest(concat_ws('|','ISSUE_INVOICE',s.planned_invoice_id::text,
        s.document_revision,s.command_token),'sha256'),'hex'),
      0,'INVOICE',
      s.planned_invoice_id,'QUEUED',850,v_now,
      jsonb_build_object(
        'invoice_id',s.planned_invoice_id,
        'source_revision',s.document_revision,
        'allow_early',true,'deliver',false,
        'command_token',s.command_token,
        'credit_note',true,'credit_reason',s.credit_reason),
      s.control_version,v_now,v_now
    from selected_credit_issue s
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning operation_id
  ),
  credit_issue_ptrs as (
    update public.invoices i
    set active_issue_operation_id=s.operation_id,issue_state='VALIDATING',
      updated_at=v_now
    from selected_credit_issue s
    where i.id=s.planned_invoice_id
    returning i.id
  ),
  child_results as materialized (
    select d.chunk_id,d.operation_id document_operation_id,
      d.document_version_id,null::uuid issue_operation_id
    from all_doc_versions d
    union all
    select s.chunk_id,null::uuid,null::uuid,s.operation_id
    from selected_credit_issue s
  )
  update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=coalesce(c.result_json,'{}'::jsonb)||jsonb_build_object(
        'document_operation_id',d.document_operation_id,
        'document_version_id',d.document_version_id,
        'issue_operation_id',d.issue_operation_id)
    from child_results d where c.id=d.chunk_id
  ;
  select coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,
      'result',c.result_json,'error',c.error_json)),'[]'::jsonb)
    into v_part
  from jsonb_array_elements(p_claims) x
  join public.invoice_operation_chunks c
    on c.id=case when coalesce(x->>'chunk_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x->>'chunk_id')::uuid end
  where x->>'phase'='QUEUE_DOCUMENT';
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  return coalesce(v_result,'[]'::jsonb);
end;
$function$
;

-- SOURCE private._invoice_issue_advance_batch_legacy_20260726(p_claims jsonb, p_now_utc timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_issue_advance_batch_legacy_20260726(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb:='[]'::jsonb;
  v_part jsonb;
begin
  if p_claims is null or jsonb_typeof(p_claims) is distinct from 'array' then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;
  if jsonb_array_length(p_claims) < 1 or jsonb_array_length(p_claims) > 100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  -- VALIDATE preserves stable issue blocker codes and makes no legal transition.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then (x->>'chunk_id')::uuid end chunk_id
    from jsonb_array_elements(p_claims) x
    where x->>'phase'='VALIDATE'
  ),
  requests as materialized (
    select c.id chunk_id,c.operation_id,c.entity_id invoice_id,
      case when pg_input_is_valid(
          nullif(c.payload_json->>'evaluation_date',''),'date')
        then(c.payload_json->>'evaluation_date')::date end
        evaluation_date,
      jsonb_build_object(
        'request_key',c.id::text,
        'invoice_id',c.entity_id,
        'operation_id',c.operation_id,
        'expected_revision',c.payload_json->'source_revision',
        'allow_early',coalesce(c.payload_json->'allow_early','false'::jsonb),
        'deliver',coalesce(c.payload_json->'deliver','false'::jsonb),
        'recipient_set',coalesce(c.payload_json#>'{delivery_intent,recipient_set}',
          '[]'::jsonb),
        'cc',coalesce(c.payload_json#>'{delivery_intent,cc}','[]'::jsonb),
        'bcc',coalesce(c.payload_json#>'{delivery_intent,bcc}','[]'::jsonb)
      ) request_json
    from ids x
    join public.invoice_operation_chunks c on c.id=x.chunk_id
  ),
  validation_groups as materialized (
    select r.evaluation_date,
      jsonb_agg(r.request_json order by r.chunk_id) request_json
    from requests r
    group by r.evaluation_date
  ),
  shared_validation as materialized (
    select v.*
    from validation_groups g
    cross join lateral private._invoice_issue_validate_batch(
      g.request_json,g.evaluation_date) v
  ),
  eval as materialized (
    select r.chunk_id,r.operation_id,r.invoice_id,
      array(select jsonb_array_elements_text(
        coalesce(v.hard_blocker_codes,'[]'::jsonb))) blockers
    from requests r
    left join shared_validation v
      on v.request_key=r.chunk_id::text and v.invoice_id=r.invoice_id
  ),
  updated as (
    update public.invoice_operation_chunks c set
      phase=case when cardinality(e.blockers)=0 then 'FREEZE' else 'BLOCKED' end,
      status=case when cardinality(e.blockers)=0 then 'QUEUED' else 'BLOCKED' end,
      error_json=case when cardinality(e.blockers)=0 then null else
        jsonb_build_object('code',e.blockers[1],'blocker_codes',to_jsonb(e.blockers),'invoice_id',e.invoice_id) end,
      progress_json=jsonb_build_object('status_message',
        case when cardinality(e.blockers)=0 then 'Issue validation passed' else 'Issue blocked' end,
        'blocker_codes',to_jsonb(e.blockers)),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from eval e where c.id=e.chunk_id returning c.id,c.status,c.phase,c.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]')
    into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]');

  -- FREEZE: immutable legal/render snapshot; invoice deliberately remains DRAFT.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x->>'chunk_id')::uuid end chunk_id from jsonb_array_elements(p_claims) x where x->>'phase'='FREEZE'
  ),
  freeze_route_requests as materialized (
    select c.id chunk_id,
      case when pg_input_is_valid(
          nullif(c.payload_json->>'evaluation_date',''),'date')
        then(c.payload_json->>'evaluation_date')::date end
        evaluation_date,
      jsonb_build_object(
      'request_key',c.id::text,
      'invoice_id',c.entity_id,
      'recipient_set',coalesce(
        c.payload_json#>'{delivery_intent,recipient_set}','[]'::jsonb),
      'cc',coalesce(c.payload_json#>'{delivery_intent,cc}','[]'::jsonb),
      'bcc',coalesce(c.payload_json#>'{delivery_intent,bcc}','[]'::jsonb),
      'delivery_policy',coalesce(
        c.payload_json#>>'{delivery_intent,delivery_policy}','ATTACH'),
      'template_version',coalesce(
        c.payload_json#>>'{delivery_intent,template_version}',
        'invoice-delivery-v1')
    ) request_json
    from ids x join public.invoice_operation_chunks c on c.id=x.chunk_id
  ),
  freeze_route_groups as materialized (
    select q.evaluation_date,
      jsonb_agg(q.request_json order by q.chunk_id) request_json
    from freeze_route_requests q
    group by q.evaluation_date
  ),
  frozen_routes as materialized (
    select r.*
    from freeze_route_groups g
    cross join lateral private._invoice_delivery_routes_batch(
      g.request_json,g.evaluation_date) r
  ),
  freeze_seed as materialized (
    select c.id chunk_id,c.operation_id,c.entity_id invoice_id,c.payload_json,
      i.document_revision,i.invoice_no,i.client_id,
      frozen_clock.issue_at_utc,
      frozen_clock.issue_at_utc tax_point_utc,
      frozen_clock.issue_at_utc+make_interval(days=>coalesce(
        case when pg_input_is_valid(nullif(i.header_snapshot_json->>'payment_terms_days',''),'integer')
          then greatest(0,least(3650,(i.header_snapshot_json->>'payment_terms_days')::integer)) end,
        cl.payment_terms_days,30)) due_at_utc,
      rr.request_json routing_request,
      jsonb_build_object(
        'request_key',dr.request_key,
        'to',coalesce(dr.canonical_to,'[]'::jsonb),
        'cc',coalesce(dr.canonical_cc,'[]'::jsonb),
        'bcc',coalesce(dr.canonical_bcc,'[]'::jsonb),
        'recipient_set_hash',dr.recipient_set_hash,
        'route_policy_hash',dr.route_policy_hash,
        'route_source',dr.route_source,
        'do_not_send',dr.do_not_send,
        'delivery_suppressed',dr.delivery_suppressed,
        'suppression_reason',dr.suppression_reason,
        'client_settings_id',dr.client_settings_id,
        'contract_settings_ids',dr.contract_settings_ids,
        'effective_date',dr.effective_date,
        'client_id',dr.client_id,
        'invoice_group_identity',dr.invoice_group_identity,
        'self_bill',dr.self_bill,
        'grouping_identity',dr.grouping_identity,
        'template_version',coalesce(c.payload_json#>>'{delivery_intent,template_version}','invoice-delivery-v1'),
        'delivery_policy',upper(coalesce(c.payload_json#>>'{delivery_intent,delivery_policy}','ATTACH')),
        'warning_codes',coalesce(dr.warning_codes,'[]'::jsonb),
        'blocker_codes',coalesce(dr.blocker_codes,'[]'::jsonb),
        'evaluated_date',dr.effective_date
      ) delivery_route,
      coalesce(nullif(c.payload_json->>'delivery_request_token',''),
        'ISSUE:'||coalesce(c.payload_json->>'command_token',c.id::text)) delivery_request_token,
      coalesce(c.payload_json->'deliver','false'::jsonb) deliver
    from ids x
    join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoices i on i.id=c.entity_id and i.status='DRAFT'
    join public.clients cl on cl.id=i.client_id
    cross join lateral (
      select case when pg_input_is_valid(nullif(c.payload_json->>'frozen_issue_at_utc',''),'timestamptz')
        then (c.payload_json->>'frozen_issue_at_utc')::timestamptz else v_now end issue_at_utc
    ) frozen_clock
    left join frozen_routes dr on dr.request_key=c.id::text and dr.invoice_id=i.id
    left join freeze_route_requests rr on rr.chunk_id=c.id
  ),
  presentation_requests as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
        'request_key',fs.chunk_id::text,
        'entity_type','INVOICE',
        'entity_id',fs.invoice_id,
        'purpose','FINAL_ISSUE',
        'template_version','invoice-professional-v1',
        'issue_at_utc',fs.issue_at_utc,
        'tax_point_utc',fs.tax_point_utc,
        'due_at_utc',fs.due_at_utc
      ) order by fs.chunk_id),'[]'::jsonb) requests
    from freeze_seed fs
  ),
  presentation_batch as materialized (
    select p.*
    from (
      select pr.requests
      from presentation_requests pr
      where jsonb_typeof(pr.requests)='array'
        and jsonb_array_length(pr.requests)>0
    ) pr
    cross join lateral private._invoice_presentation_snapshot_batch(
      pr.requests,v_now) p
  ),
  frozen as materialized (
    select fs.chunk_id,fs.operation_id,fs.invoice_id,fs.payload_json,
      fs.document_revision,fs.invoice_no,fs.client_id,
      (
        pb.snapshot_json
        || jsonb_build_object(
          'snapshot_schema_version','FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5',
          'issue_date_utc',fs.issue_at_utc,
          'tax_point_utc',fs.tax_point_utc,
          'due_date_utc',fs.due_at_utc,
          'routing_request',coalesce(fs.routing_request,'{}'::jsonb),
          'delivery_route',coalesce(fs.delivery_route,'{}'::jsonb),
          'delivery_intent',coalesce(fs.payload_json->'delivery_intent','{}'::jsonb),
          'delivery_request_token',fs.delivery_request_token,
          'deliver',fs.deliver,
          'presentation_model_hash',coalesce(pb.presentation_model->>'presentation_model_hash',pb.snapshot_json->>'presentation_model_hash',encode(digest(coalesce(pb.presentation_model,'{}'::jsonb)::text,'sha256'),'hex'))
        )
      ) snapshot,
      pb.valid presentation_valid,
      pb.error_code presentation_error_code,
      pb.error_detail presentation_error_detail
    from freeze_seed fs
    left join presentation_batch pb on pb.request_key=fs.chunk_id::text
  ),
  existing_versions as materialized (
    select f.*,v.id existing_document_version_id,v.operation_id doc_operation_id,
      o.control_version doc_control_version,v.status document_status,
      v.snapshot_hash existing_snapshot_hash
    from frozen f join public.invoice_document_versions v
      on coalesce(f.presentation_valid,false) is true
      and v.entity_type='INVOICE' and v.entity_id=f.invoice_id
      and v.purpose='FINAL_ISSUE'
      and v.snapshot_hash=encode(digest(f.snapshot::text,'sha256'),'hex')
      and v.template_version='invoice-professional-v1'
      and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','READY')
    join public.invoice_operations o on o.id=v.operation_id
  ),
  doc_ops as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,created_at_utc,updated_at_utc)
    select f.operation_id,'BUILD_DOCUMENT','INVOICE',f.invoice_id,o.actor_user_id,
      encode(digest('FINAL_ISSUE|'||f.invoice_id||'|'||
        encode(digest(f.snapshot::text,'sha256'),'hex')||
        '|invoice-professional-v1','sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',850,f.document_revision::text,'invoice-professional-v1',
      jsonb_build_object('invoice_id',f.invoice_id,'purpose','FINAL_ISSUE'),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,
      nextval('public.invoice_operation_change_seq'),v_now,v_now
    from frozen f join public.invoice_operations o on o.id=f.operation_id
    where coalesce(f.presentation_valid,false) is true
      and not exists(select 1 from existing_versions e where e.chunk_id=f.chunk_id)
    on conflict(idempotency_key) where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,850),updated_at_utc=v_now
    returning *
  ),
  selected as materialized (
    select f.chunk_id,f.operation_id,f.invoice_id,f.payload_json,f.document_revision,
      f.invoice_no,f.client_id,f.snapshot,d.id doc_operation_id,d.control_version doc_control_version,
      null::uuid existing_document_version_id,null::text document_status,
      null::text existing_snapshot_hash
    from frozen f join doc_ops d on d.entity_id=f.invoice_id
    where coalesce(f.presentation_valid,false) is true
    union all
    select f.chunk_id,f.operation_id,f.invoice_id,f.payload_json,f.document_revision,
      f.invoice_no,f.client_id,f.snapshot,e.doc_operation_id,e.doc_control_version,
      e.existing_document_version_id,e.document_status,e.existing_snapshot_hash
    from frozen f join existing_versions e on e.chunk_id=f.chunk_id
    where coalesce(f.presentation_valid,false) is true
  ),
  presentation_blocked as (
    update public.invoice_operation_chunks c set
      status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
      error_json=jsonb_build_object(
        'code',coalesce(f.presentation_error_code,'FINAL_ISSUE_PRESENTATION_INVALID'),
        'detail',coalesce(f.presentation_error_detail,'{}'::jsonb),
        'invoice_id',f.invoice_id),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from frozen f
    where c.id=f.chunk_id and coalesce(f.presentation_valid,false) is not true
    returning c.id,c.status,c.phase,c.document_version_id
  ),
  versions as materialized (
    insert into public.invoice_document_versions(entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select 'INVOICE',s.invoice_id,'FINAL_ISSUE',s.doc_operation_id,s.document_revision::text,
      'invoice-professional-v1','PLANNING',s.snapshot,encode(digest(s.snapshot::text,'sha256'),'hex'),
      '[]',encode(digest('[]','sha256'),'hex'),v_now
    from selected s
    where s.existing_document_version_id is null
    on conflict(entity_type,entity_id,purpose,snapshot_hash,template_version)
      where purpose='FINAL_ISSUE'
        and status in ('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning id,entity_id,operation_id,snapshot_hash
  ),
  exact_versions as materialized (
    select s.*,coalesce(s.existing_document_version_id,v.id) document_version_id,
      coalesce(s.existing_snapshot_hash,v.snapshot_hash) snapshot_hash,
      coalesce(s.document_status,'PLANNING') exact_document_status
    from selected s left join versions v
      on v.entity_id=s.invoice_id and v.operation_id=s.doc_operation_id
  ),
  doc_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select s.doc_operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',
        s.document_version_id::text,s.document_revision::text,
        'invoice-professional-v1','1'),'sha256'),'hex'),
      0,'INVOICE',s.invoice_id,s.document_version_id,
      'QUEUED',850,v_now,jsonb_build_object('purpose','FINAL_ISSUE'),s.doc_control_version,v_now,v_now
    from exact_versions s
    where s.exact_document_status<>'READY'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning id
  ),
  invoice_state as (
    update public.invoices i set issue_state='PREPARING_DOCUMENT',
      active_issue_operation_id=s.operation_id,
      active_document_operation_id=case when s.exact_document_status='READY'
        then i.active_document_operation_id else s.doc_operation_id end,
      document_state=case when s.exact_document_status='READY'
        then i.document_state else 'PREPARING' end,updated_at=v_now
    from exact_versions s where i.id=s.invoice_id and i.status='DRAFT' returning i.id
  ),
  advanced as (
    update public.invoice_operation_chunks c set
      phase=case when s.exact_document_status='READY' then 'FINALISE' else 'WAIT_DOCUMENT' end,
      status=case when s.exact_document_status='READY' then 'QUEUED' else 'WAITING' end,
      document_version_id=s.document_version_id,
      payload_json=c.payload_json||jsonb_build_object('frozen_document_revision',s.document_revision,
        'document_version_id',s.document_version_id,'document_operation_id',s.doc_operation_id,
        'snapshot_hash',s.snapshot_hash,
        'routing_request',s.snapshot->'routing_request',
        'frozen_delivery_route',s.snapshot->'delivery_route',
        'delivery_request_token',s.snapshot->>'delivery_request_token'),
      progress_json=jsonb_build_object('status_message',
        case when s.exact_document_status='READY'
          then 'Exact final document is ready' else 'Final issue document is preparing' end),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from exact_versions s where c.id=s.chunk_id
    returning c.id,c.status,c.phase,c.document_version_id
  ),
  freeze_outcomes as (
    select id,status,phase,document_version_id from advanced
    union all
    select id,status,phase,document_version_id from presentation_blocked
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,
    'document_version_id',document_version_id)),'[]') into v_part from freeze_outcomes;
  v_result:=v_result||coalesce(v_part,'[]');

  -- WAIT_DOCUMENT observes only the exact row and never R2.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x->>'chunk_id')::uuid end chunk_id from jsonb_array_elements(p_claims) x where x->>'phase'='WAIT_DOCUMENT'
  ),
  updated as (
    update public.invoice_operation_chunks c set
      phase=case when v.status='READY' then 'FINALISE'
                 when v.status in ('FAILED','SUPERSEDED','CANCELLED') then 'BLOCKED'
                 else 'WAIT_DOCUMENT' end,
      status=case when v.status='READY' then 'QUEUED'
                  when v.status in ('FAILED','SUPERSEDED','CANCELLED') then 'BLOCKED'
                  else 'WAITING' end,
      error_json=case when v.status in ('FAILED','SUPERSEDED','CANCELLED')
        then jsonb_build_object('code','FINAL_DOCUMENT_'||v.status,'document_version_id',v.id,'detail',v.error_json)
        else null end,run_after_utc=case when v.status='READY' then v_now else c.run_after_utc end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from ids x,public.invoice_document_versions v
    where c.id=x.chunk_id and v.id=c.document_version_id
    returning c.id,c.status,c.phase,c.error_json
  ),
  missing as (
    update public.invoice_operation_chunks c set
      phase='BLOCKED',status='BLOCKED',
      error_json=jsonb_build_object(
        'code','FINAL_DOCUMENT_VERSION_MISSING',
        'document_version_id',c.document_version_id),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from ids x
    where c.id=x.chunk_id
      and not exists(
        select 1 from public.invoice_document_versions v
        where v.id=c.document_version_id)
    returning c.id,c.status,c.phase,c.error_json
  ),
  outcomes as (
    select * from updated
    union all
    select * from missing
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]')
    into v_part from outcomes;
  v_result:=v_result||coalesce(v_part,'[]');

  -- FINALISE is the only legal issue transition and demands the exact verified version.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x->>'chunk_id')::uuid end chunk_id from jsonb_array_elements(p_claims) x where x->>'phase'='FINALISE'
  ),
  eligible as materialized (
    select c.*,i.document_revision,v.id final_document_version_id,v.r2_key,v.sha256,v.size_bytes,v.page_count,
      v.verified_at_utc,
      case when pg_input_is_valid(
        nullif(v.snapshot_json->>'issue_date_utc',''),'timestamptz')
        then (v.snapshot_json->>'issue_date_utc')::timestamptz
        else v_now end issue_at,
      case when pg_input_is_valid(
        nullif(v.snapshot_json->>'tax_point_utc',''),'timestamptz')
        then (v.snapshot_json->>'tax_point_utc')::timestamptz end tax_point_at,
      case when pg_input_is_valid(
        nullif(v.snapshot_json->>'due_date_utc',''),'timestamptz')
        then (v.snapshot_json->>'due_date_utc')::timestamptz end due_at,
      v.snapshot_hash
    from ids x join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoices i on i.id=c.entity_id and i.status='DRAFT'
      and i.issue_state='PREPARING_DOCUMENT'
      and i.active_issue_operation_id=c.operation_id
    join public.invoice_document_versions v on v.id=c.document_version_id and v.purpose='FINAL_ISSUE'
      and v.entity_type='INVOICE' and v.entity_id=c.entity_id
      and v.status='READY' and v.verified_at_utc is not null and v.r2_key is not null
      and v.sha256 is not null and v.size_bytes>0 and v.page_count>0
    where case when pg_input_is_valid(
        nullif(c.payload_json->>'frozen_document_revision',''),'bigint')
      then(c.payload_json->>'frozen_document_revision')::bigint=i.document_revision
      else false end
      and v.source_revision=i.document_revision::text
      and c.payload_json->>'snapshot_hash'=v.snapshot_hash
  ),
  issued as (
    update public.invoices i set status='ISSUED',status_date_utc=v_now,
      issued_at_utc=e.issue_at,due_at_utc=e.due_at,
      on_hold_reason=null,issued_document_version_id=e.final_document_version_id,
      invoice_pdf_r2_key=e.r2_key,invoice_pdf_generated_at_utc=e.verified_at_utc,
      issue_state='ISSUED',document_state='READY',active_issue_operation_id=null,
      active_document_operation_id=case
        when pg_input_is_valid(
          nullif(e.payload_json->>'document_operation_id',''),'uuid')
          then case when i.active_document_operation_id=
              (e.payload_json->>'document_operation_id')::uuid
            then null else i.active_document_operation_id end
        else i.active_document_operation_id end,updated_at=v_now
    from eligible e where i.id=e.entity_id returning i.id
  ),
  audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,after_json,reason)
    select v_now,o.actor_user_id,u.display_name,u.role,'invoice',e.entity_id::text,'INVOICE_ISSUED',
      jsonb_build_object('invoice_id',e.entity_id,'document_version_id',e.final_document_version_id,
        'issue_at_utc',e.issue_at,'tax_point_utc',e.tax_point_at,'due_at_utc',e.due_at,
        'sha256',e.sha256,'size_bytes',e.size_bytes,'page_count',e.page_count),'INVOICE_OPERATION_QUEUE'
    from eligible e join public.invoice_operations o on o.id=e.operation_id
    left join public.tms_users u on u.id=o.actor_user_id returning id
  ),
  advanced as (
    update public.invoice_operation_chunks c set
      phase=case when lower(coalesce(c.payload_json->>'deliver','false'))
        in('true','t','1','yes') then 'QUEUE_DELIVERY' else 'COMPLETE' end,
      status=case when lower(coalesce(c.payload_json->>'deliver','false'))
        in('true','t','1','yes') then 'QUEUED' else 'COMPLETE' end,
      completed_at_utc=case when lower(coalesce(
        c.payload_json->>'deliver','false')) in('true','t','1','yes')
        then null else v_now end,
      result_json=jsonb_build_object('invoice_id',e.entity_id,'document_version_id',e.final_document_version_id,
        'issue_at_utc',e.issue_at,'tax_point_utc',e.tax_point_at,'due_at_utc',e.due_at,
        'r2_key',e.r2_key,'sha256',e.sha256,'size_bytes',e.size_bytes,'page_count',e.page_count),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from eligible e where c.id=e.id
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  ineligible as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','ISSUE_FINALISE_OWNERSHIP_OR_DOCUMENT_MISMATCH',
          'invoice_id',c.entity_id,
          'document_version_id',c.document_version_id),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from ids x
    where c.id=x.chunk_id
      and not exists(select 1 from eligible e where e.id=c.id)
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  outcomes as (
    select * from advanced
    union all
    select * from ineligible
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json)),'[]')
    into v_part from outcomes;
  v_result:=v_result||coalesce(v_part,'[]');

  -- QUEUE_DELIVERY durably delegates to the single delivery-routing authority.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then (x->>'chunk_id')::uuid end chunk_id
    from jsonb_array_elements(p_claims) x
    where x->>'phase'='QUEUE_DELIVERY'
  ),
  delivery_specs as materialized (
    select c.id chunk_id,c.operation_id parent_operation_id,c.entity_id invoice_id,
      o.actor_user_id,i.issued_document_version_id,
      coalesce(c.payload_json->'delivery_intent','{}'::jsonb) delivery_intent,
      coalesce(c.payload_json->'routing_request','{}'::jsonb)
        routing_request,
      coalesce(c.payload_json->'frozen_delivery_route','{}'::jsonb)
        frozen_delivery_route,
      coalesce(nullif(c.payload_json->>'delivery_request_token',''),
        'ISSUE:'||coalesce(c.payload_json->>'command_token',c.id::text))
        delivery_request_token,
      nullif(c.payload_json#>>'{frozen_delivery_route,template_version}','')
        template_version,
      nullif(c.payload_json#>>'{frozen_delivery_route,recipient_set_hash}','')
        recipient_set_hash,
      nullif(c.payload_json#>>'{frozen_delivery_route,route_policy_hash}','')
        route_policy_hash,
      coalesce(c.payload_json#>'{frozen_delivery_route,to}','[]'::jsonb)
        canonical_to,
      coalesce(c.payload_json#>'{frozen_delivery_route,cc}','[]'::jsonb)
        canonical_cc,
      coalesce(c.payload_json#>'{frozen_delivery_route,bcc}','[]'::jsonb)
        canonical_bcc,
      lower(coalesce(
        c.payload_json#>>'{frozen_delivery_route,do_not_send}','false'))
        in('true','t','1','yes') do_not_send,
      lower(coalesce(
        c.payload_json#>>'{frozen_delivery_route,delivery_suppressed}','false'))
        in('true','t','1','yes') delivery_suppressed,
      c.payload_json#>>'{frozen_delivery_route,route_source}' route_source,
      coalesce(c.payload_json#>'{frozen_delivery_route,warning_codes}',
        '[]'::jsonb) route_warnings,
      coalesce(c.payload_json#>'{frozen_delivery_route,blocker_codes}',
        '[]'::jsonb) route_blockers,
      c.payload_json#>>'{frozen_delivery_route,route_policy_hash}' is not null
        and c.payload_json#>>'{frozen_delivery_route,recipient_set_hash}'
          is not null
        and c.payload_json#>>'{frozen_delivery_route,template_version}'
          is not null
        and upper(coalesce(
          c.payload_json#>>'{frozen_delivery_route,delivery_policy}',''))
          in('ATTACH','SPLIT','SECURE_LINK')
        and lower(coalesce(
          c.payload_json#>>'{frozen_delivery_route,do_not_send}','false'))
          not in('true','t','1','yes')
        and lower(coalesce(
          c.payload_json#>>'{frozen_delivery_route,delivery_suppressed}','false'))
          not in('true','t','1','yes')
        and (case
          when jsonb_typeof(c.payload_json#>'{frozen_delivery_route,blocker_codes}')='array'
            then jsonb_array_length(c.payload_json#>'{frozen_delivery_route,blocker_codes}')
          when c.payload_json#>'{frozen_delivery_route,blocker_codes}' is null then 0
          else 1
        end)=0
        frozen_route_usable,
      coalesce(case when coalesce(c.payload_json#>>'{delivery_intent,part_number}','') ~
        '^[0-9]+$' then(c.payload_json#>>'{delivery_intent,part_number}')::integer end,1)
        part_number,
      case when upper(coalesce(
          c.payload_json#>>'{delivery_intent,delivery_policy}','')) in(
            'ATTACH','SPLIT','SECURE_LINK')
        then upper(c.payload_json#>>'{delivery_intent,delivery_policy}')
        when upper(coalesce(
          c.payload_json#>>'{frozen_delivery_route,delivery_policy}','')) in(
            'ATTACH','SPLIT','SECURE_LINK')
        then upper(c.payload_json#>>'{frozen_delivery_route,delivery_policy}')
      end delivery_policy
    from ids x join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    join public.invoices i on i.id=c.entity_id and i.status='ISSUED'
      and i.issued_document_version_id=c.document_version_id
  ),
  delivery_groups as materialized (
    select d.parent_operation_id,
      (array_agg(d.actor_user_id order by d.chunk_id))[1] actor_user_id,
      (array_agg(d.template_version order by d.chunk_id))[1] template_version,
      encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
        d.route_policy_hash),'||' order by d.invoice_id),'sha256'),'hex')
        route_policy_hash,
      (array_agg(d.delivery_policy order by d.chunk_id))[1] delivery_policy,
      array_agg(d.invoice_id order by d.invoice_id) invoice_ids,
      array_agg(d.issued_document_version_id order by d.invoice_id)
        issued_document_version_ids,
      array_agg(d.delivery_request_token order by d.invoice_id)
        delivery_request_tokens,
      count(*)::integer unit_count,
      encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
        d.issued_document_version_id::text,d.delivery_request_token),
        '||' order by d.invoice_id),'sha256'),'hex') group_request_hash,
      encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
        d.issued_document_version_id::text,d.delivery_request_token,
        d.route_policy_hash),
        '||' order by d.invoice_id),'sha256'),'hex') delivery_revision,
      encode(digest(concat_ws('|','DELIVER_INVOICES',d.parent_operation_id::text,
        encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
          d.issued_document_version_id::text,d.delivery_request_token),
          '||' order by d.invoice_id),'sha256'),'hex')),'sha256'),'hex')
        delivery_key
    from delivery_specs d
    group by d.parent_operation_id
  ),
  existing_delivery as materialized (
    select g.parent_operation_id,o.id operation_id,o.control_version,o.status
    from delivery_groups g join lateral(
      select x.* from public.invoice_operations x
      where x.idempotency_key=g.delivery_key
        and x.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      order by(x.status='COMPLETE') desc,x.created_at_utc desc limit 1
    ) o on true
  ),
  inserted_delivery as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,
      entity_type,entity_id,actor_user_id,idempotency_key,status,phase,priority,
      source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,requires_user_action,
      error_json,created_at_utc,updated_at_utc)
    select g.parent_operation_id,'DELIVER_INVOICES','INVOICE_BATCH',null,
      g.actor_user_id,g.delivery_key,'QUEUED','PREPARE',700,
      g.delivery_revision,g.template_version,
      jsonb_build_object('invoice_ids',to_jsonb(g.invoice_ids),
        'issued_document_version_ids',to_jsonb(g.issued_document_version_ids),
        'template_version',g.template_version,
        'delivery_request_tokens',to_jsonb(g.delivery_request_tokens),
        'group_request_hash',g.group_request_hash,
        'route_policy_hash',g.route_policy_hash,
        'delivery_policy',g.delivery_policy),
      jsonb_build_object(
        'delivery_request_tokens',to_jsonb(g.delivery_request_tokens),
        'group_request_hash',g.group_request_hash,
        'route_policy_hash',g.route_policy_hash,
        'processor_policy',parent.config_json->'processor_policy'),
      jsonb_build_object('status_message','Batch delivery preparation queued'),
      g.unit_count,g.unit_count,1,nextval('public.invoice_operation_change_seq'),
      false,null,v_now,v_now
    from delivery_groups g
    join public.invoice_operations parent on parent.id=g.parent_operation_id
    where not exists(
      select 1 from existing_delivery e
      where e.parent_operation_id=g.parent_operation_id)
    on conflict do nothing
    returning *
  ),
  selected_delivery_groups as materialized (
    select g.*,coalesce(e.operation_id,n.id) delivery_operation_id,
      coalesce(e.control_version,n.control_version) delivery_control_version,
      coalesce(e.status,n.status) delivery_status
    from delivery_groups g
    left join existing_delivery e
      on e.parent_operation_id=g.parent_operation_id
    left join inserted_delivery n on n.idempotency_key=g.delivery_key
  ),
  selected_delivery as materialized (
    select d.*,g.delivery_operation_id,g.delivery_control_version,
      g.delivery_status
    from delivery_specs d
    join selected_delivery_groups g
      on g.parent_operation_id=d.parent_operation_id
  ),
  delivery_chunk_specs as materialized (
    select d.*,gen_random_uuid() delivery_chunk_id
    from selected_delivery d
    where d.delivery_status<>'COMPLETE'
  ),
  delivery_chunks as (
    insert into public.invoice_operation_chunks(id,operation_id,chunk_type,phase,
      work_key,sequence_no,entity_type,entity_id,document_version_id,status,priority,
      run_after_utc,payload_json,error_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select d.delivery_chunk_id,d.delivery_operation_id,'DELIVERY_PREPARE',
      case when d.frozen_route_usable then 'PREPARE' else 'BLOCKED' end,
      encode(digest(concat_ws('|','DELIVERY_PREPARE',
        d.issued_document_version_id::text,d.route_policy_hash,
        d.template_version,d.part_number::text,d.delivery_request_token),
        'sha256'),'hex'),
      row_number() over(partition by d.delivery_operation_id
        order by d.invoice_id)::integer-1,
      'INVOICE',d.invoice_id,d.issued_document_version_id,
      case when d.frozen_route_usable then 'QUEUED' else 'BLOCKED' end,
      700,v_now,
      jsonb_build_object('request_key',d.delivery_chunk_id::text,
        'invoice_id',d.invoice_id,
        'issued_document_version_id',d.issued_document_version_id,
        'recipient_set',d.canonical_to,'cc',d.canonical_cc,'bcc',d.canonical_bcc,
        'recipient_set_hash',d.recipient_set_hash,'template_version',d.template_version,
        'route_policy_hash',d.route_policy_hash,
        'delivery_part_number',d.part_number,'do_not_send',d.do_not_send,
        'delivery_suppressed',d.delivery_suppressed,
        'delivery_policy',d.delivery_policy,
        'route_source',d.route_source,
        'delivery_request_token',d.delivery_request_token,
        'routing_request',d.routing_request,
        'frozen_delivery_route',d.frozen_delivery_route),
      case when d.frozen_route_usable then null else jsonb_build_object(
        'code','FROZEN_DELIVERY_ROUTE_UNUSABLE',
        'frozen_route',d.frozen_delivery_route,
        'route_blockers',d.route_blockers) end,
      d.delivery_control_version,v_now,v_now
    from delivery_chunk_specs d
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning id
  ),
  completed_delivery_request as (
    update public.invoice_operation_chunks c set status='COMPLETE',phase='COMPLETE',
      completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=coalesce(c.result_json,'{}')||jsonb_build_object(
        'delivery_operation_id',d.delivery_operation_id,
        'delivery_status',case when d.frozen_route_usable
          then d.delivery_status else 'BLOCKED' end,
        'delivery_requested',true,
        'delivery_request_token',d.delivery_request_token,
        'delivery_blocker',case when d.frozen_route_usable then null
          else jsonb_build_object('code','FROZEN_DELIVERY_ROUTE_UNUSABLE',
            'frozen_route',d.frozen_delivery_route,
            'route_blockers',d.route_blockers) end)
    from selected_delivery d where c.id=d.chunk_id
    returning c.id,c.status,c.phase,c.result_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,
    'phase',phase,'result',result_json)),'[]') into v_part
  from completed_delivery_request;
  v_result:=v_result||coalesce(v_part,'[]');

  return coalesce(v_result,'[]'::jsonb);
end;
$function$
;

-- SOURCE private._invoice_operation_get_legacy_20260726(p_operation_ids uuid[], p_actor_user_id uuid, p_mode text)
CREATE OR REPLACE FUNCTION private._invoice_operation_get_legacy_20260726(p_operation_ids uuid[], p_actor_user_id uuid, p_mode text DEFAULT 'PROGRESS'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_mode_input text:=upper(btrim(coalesce(p_mode,'PROGRESS')));
  v_mode text;
  v_descendant_offset integer:=0;
  v_chunk_offset integer:=0;
  v_role text;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_result jsonb;
begin
  if v_mode_input='PROGRESS' then
    v_mode:='PROGRESS';
  elsif v_mode_input='DETAIL' then
    v_mode:='DETAIL';
  elsif v_mode_input~'^DETAIL:[0-9]{1,6}$' then
    v_mode:='DETAIL';
    v_descendant_offset:=split_part(v_mode_input,':',2)::integer;
  elsif v_mode_input~'^DETAIL:[0-9]{1,6}:[0-9]{1,6}$' then
    v_mode:='DETAIL';
    v_descendant_offset:=split_part(v_mode_input,':',2)::integer;
    v_chunk_offset:=split_part(v_mode_input,':',3)::integer;
  else
    raise exception
      'p_mode must be PROGRESS, DETAIL, DETAIL:<descendant offset>, or DETAIL:<descendant offset>:<chunk offset>';
  end if;
  if cardinality(coalesce(p_operation_ids,array[]::uuid[]))<1
    or cardinality(p_operation_ids)>100 then
    raise exception 'p_operation_ids must contain 1..100 IDs';
  end if;
  if not v_service and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Authenticated actor mismatch';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_role
  from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if not found and not v_service then
    raise exception using errcode='42501',message='Active actor required';
  end if;
  if v_mode='DETAIL' and not v_service and coalesce(v_role,'')<>'admin' then
    raise exception using errcode='42501',message='Administrator permission required for DETAIL';
  end if;

  with recursive requested as materialized (
    select id,min(ordinality) ordinality
    from unnest(p_operation_ids) with ordinality x(id,ordinality)
    group by id
  ),
  authorised as materialized (
    select o.*,r.ordinality
    from requested r join public.invoice_operations o on o.id=r.id
    where v_service or v_role='admin' or o.actor_user_id=p_actor_user_id
  ),
  descendants(root_id,parent_id,id,operation_type,entity_type,entity_id,status,phase,
      progress_json,result_json,error_json,change_seq,updated_at_utc,
      created_at_utc,depth,path)
      as materialized (
    select p.id,ch.parent_operation_id,ch.id,ch.operation_type,ch.entity_type,ch.entity_id,
      ch.status,ch.phase,ch.progress_json,ch.result_json,ch.error_json,ch.change_seq,
      ch.updated_at_utc,ch.created_at_utc,1,array[p.id,ch.id]::uuid[]
    from authorised p
    join public.invoice_operations ch on ch.parent_operation_id=p.id
    union all
    select d.root_id,ch.parent_operation_id,ch.id,ch.operation_type,ch.entity_type,ch.entity_id,
      ch.status,ch.phase,ch.progress_json,ch.result_json,ch.error_json,ch.change_seq,
      ch.updated_at_utc,ch.created_at_utc,d.depth+1,d.path||ch.id
    from descendants d
    join public.invoice_operations ch on ch.parent_operation_id=d.id
    where not ch.id=any(d.path)
  ),
  scope_operation_ids as materialized (
    select a.id operation_id from authorised a
    union
    select d.id from descendants d
  ),
  scope_ranked as materialized (
    select s.operation_id,row_number() over(order by s.operation_id) rn
    from scope_operation_ids s
  ),
  scope_pages as materialized (
    select ((s.rn-1)/100)::integer page_no,
      array_agg(s.operation_id order by s.operation_id) operation_ids
    from scope_ranked s
    group by ((s.rn-1)/100)::integer
  ),
  current_slots as materialized (
    select slot.*
    from scope_pages p
    cross join lateral private._invoice_current_chunks_batch(
      p.operation_ids,null,null,10000) slot
  ),
  current_chunks as materialized (
    select slot.logical_slot_key,
      coalesce(c.id,slot.current_chunk_id) id,slot.operation_id,
      slot.chunk_type,slot.level_no,slot.sequence_no,slot.work_key,
      slot.plan_generation,slot.entity_type,slot.entity_id,
      slot.document_version_id,slot.document_asset_id,
      slot.input_document_version_id,
      case when slot.replacement_chain_status='INVALID'
        then 'BLOCKED' else c.status end status,
      case when slot.replacement_chain_status='INVALID'
        then 'REPLACEMENT_VALIDATION' else c.phase end phase,
      c.attempt_count,c.max_attempts,c.payload_json,
      c.progress_json,c.result_json,
      case when slot.replacement_chain_status='INVALID'
        then slot.replacement_chain_error else c.error_json end error_json,
      c.created_at_utc,c.updated_at_utc
    from current_slots slot
    left join public.invoice_operation_chunks c
      on c.id=slot.current_chunk_id
  ),
  child_rows as materialized (
    select *
    from(
      select d.*,row_number() over(partition by d.root_id
        order by d.depth,d.created_at_utc,d.id) rn
      from descendants d
    ) bounded
    where rn>v_descendant_offset
      and rn<=v_descendant_offset+200
  ),
  expected_purpose as materialized (
    select a.id operation_id,
      case
        when a.operation_type='ISSUE_INVOICES' then 'FINAL_ISSUE'
        when a.entity_type='TIMESHEET' then 'TIMESHEET'
        when upper(coalesce(a.input_json->>'purpose','')) in(
          'DRAFT_PREVIEW','FINAL_ISSUE','TIMESHEET')
          then upper(a.input_json->>'purpose')
        when a.entity_type='INVOICE' and exists(
          select 1 from public.invoices i
          where i.id=a.entity_id and i.status in('ISSUED','PAID'))
          then 'FINAL_ISSUE'
        else 'DRAFT_PREVIEW'
      end purpose
    from authorised a
  ),
  issue_document_ids as materialized (
    select distinct a.id operation_id,
      case when coalesce(c.payload_json->>'final_document_version_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(c.payload_json->>'final_document_version_id')::uuid end document_version_id
    from authorised a
    join current_chunks c
      on c.operation_id=a.id and c.chunk_type='ISSUE_INVOICE'
  ),
  candidate_documents as materialized (
    select a.id root_operation_id,v.*
    from authorised a join public.invoice_document_versions v on v.operation_id=a.id
    union all
    select cr.root_id root_operation_id,v.*
    from descendants cr join public.invoice_document_versions v on v.operation_id=cr.id
    union all
    select x.operation_id root_operation_id,v.*
    from issue_document_ids x join public.invoice_document_versions v on v.id=x.document_version_id
    union all
    select a.id root_operation_id,v.*
    from authorised a join public.invoices i
      on a.entity_type='INVOICE' and i.id=a.entity_id
    join public.invoice_document_versions v
      on v.id in(i.preview_document_version_id,i.issued_document_version_id)
    union all
    select a.id root_operation_id,v.*
    from authorised a join public.timesheets t
      on a.entity_type='TIMESHEET' and t.timesheet_id=a.entity_id and t.is_current
    join public.invoice_document_versions v on v.id=t.current_document_version_id
  ),
  selected_documents as materialized (
    select distinct on(c.root_operation_id) c.*
    from candidate_documents c
    join expected_purpose expected on expected.operation_id=c.root_operation_id
    where c.purpose=expected.purpose
    order by c.root_operation_id,
      case status when 'READY' then 0 when 'VERIFYING' then 1
        when 'ASSEMBLING' then 2 when 'RENDERING' then 3 else 4 end,
      created_at_utc desc,id desc
  ),
  descendant_summary as materialized (
    select a.id root_id,count(d.id)::integer total_descendants,
      greatest(a.change_seq,coalesce(max(d.change_seq),0))::bigint
        effective_change_seq,
      count(d.id) filter(where d.operation_type='BUILD_DOCUMENT')::integer
        document_operation_count,
      count(d.id) filter(where d.operation_type='DELIVER_INVOICES')::integer
        delivery_operation_count
    from authorised a
    left join descendants d on d.root_id=a.id
    group by a.id,a.change_seq
  ),
  operation_rows as materialized (
    select a.*,d.id active_document_version_id,d.entity_type document_entity_type,
      d.entity_id document_entity_id,d.purpose document_purpose,
      d.status document_status,d.r2_key,d.sha256,d.size_bytes,d.page_count,
      ds.total_descendants,
      least(greatest(ds.total_descendants-v_descendant_offset,0),200)::integer
        returned_descendants,
      ds.total_descendants>v_descendant_offset+200 results_truncated,
      case when ds.total_descendants>v_descendant_offset+200
        then 'DETAIL:'||(v_descendant_offset+200)::text end
        continuation_token,
      ds.effective_change_seq,ds.document_operation_count,
      ds.delivery_operation_count,
      (select count(*)::integer from current_chunks z
        where z.operation_id=a.id) total_chunks,
      least(greatest((select count(*)::integer from current_chunks z
        where z.operation_id=a.id)-v_chunk_offset,0),250)::integer
        returned_chunks,
      (select count(*) from current_chunks z
        where z.operation_id=a.id)>v_chunk_offset+250 chunks_truncated,
      case when (select count(*) from current_chunks z
          where z.operation_id=a.id)>v_chunk_offset+250
        then 'DETAIL:'||v_descendant_offset::text||':'||
          (v_chunk_offset+250)::text end chunk_continuation_token,
      case when v_mode='DETAIL' then(
        select coalesce(jsonb_agg(jsonb_build_object(
          'chunk_id',q.id,'chunk_type',q.chunk_type,'phase',q.phase,'status',q.status,
          'entity_type',q.entity_type,'entity_id',q.entity_id,
          'attempt_count',q.attempt_count,'max_attempts',q.max_attempts,
          'progress',q.progress_json,'result',q.result_json,'error',q.error_json)
          order by q.created_at_utc,q.id),'[]'::jsonb)
        from(
          select * from current_chunks z
          where z.operation_id=a.id
          order by z.created_at_utc,z.id
          offset v_chunk_offset limit 250
        ) q
      ) end chunks,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'operation_id',cr.id,'operation_type',cr.operation_type,
          'entity_type',cr.entity_type,'entity_id',cr.entity_id,
          'status',cr.status,'phase',cr.phase,
          'progress',case when v_mode='DETAIL' then cr.progress_json else
            jsonb_build_object('status_message',cr.progress_json->>'status_message') end,
          'result',case when v_mode='DETAIL' and cr.status='COMPLETE'
            then cr.result_json end,
          'error_code',cr.error_json->>'code','change_seq',cr.change_seq,'updated_at_utc',cr.updated_at_utc)
          order by cr.updated_at_utc,cr.id)
        from child_rows cr where cr.root_id=a.id
      ),'[]'::jsonb) children,
      jsonb_build_object(
        'created_invoice_ids',coalesce((
          select jsonb_agg(to_jsonb(b.value) order by b.value)
          from(
            select distinct e.value
            from current_chunks c
            cross join lateral jsonb_array_elements_text(
              case when jsonb_typeof(c.result_json->'invoice_ids')='array'
                then c.result_json->'invoice_ids'
                when c.result_json?'invoice_id'
                  then jsonb_build_array(c.result_json->>'invoice_id')
                else '[]'::jsonb end) e(value)
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
            order by e.value limit 500
          ) b),'[]'::jsonb),
        'document_operation_ids',coalesce((
          select jsonb_agg(to_jsonb(b.id) order by b.id)
          from(
            select cr.id from descendants cr
            where cr.root_id=a.id and cr.operation_type='BUILD_DOCUMENT'
            order by cr.id limit 500
          ) b),'[]'::jsonb),
        'delivery_operation_ids',coalesce((
          select jsonb_agg(to_jsonb(b.id) order by b.id)
          from(
            select cr.id from descendants cr
            where cr.root_id=a.id and cr.operation_type='DELIVER_INVOICES'
            order by cr.id limit 500
          ) b),'[]'::jsonb),
        'issue_results',coalesce((
          select jsonb_agg(jsonb_build_object(
            'chunk_id',b.id,'invoice_id',b.entity_id,
            'status',b.status,'phase',b.phase,
            'document_version_id',coalesce(
              b.result_json->>'issued_document_version_id',
              b.result_json->>'document_version_id'),
            'document_operation_id',b.result_json->>'document_operation_id',
            'delivery_operation_id',b.result_json->>'delivery_operation_id',
            'error_code',b.error_json->>'code') order by b.entity_id,b.id)
          from(
            select c.* from current_chunks c
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
              and c.chunk_type='ISSUE_INVOICE'
            order by c.entity_id,c.id limit 500
          ) b),'[]'::jsonb),
        'mail_outbox_ids',coalesce((
          select jsonb_agg(to_jsonb(b.value) order by b.value)
          from(
            select distinct m.value
            from current_chunks c
            cross join lateral jsonb_array_elements_text(
              case when jsonb_typeof(c.result_json->'mail_outbox_ids')='array'
                then c.result_json->'mail_outbox_ids' else '[]'::jsonb end)
              m(value)
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
            order by m.value limit 500
          ) b),'[]'::jsonb),
        'blocked_or_failed_entities',coalesce((
          select jsonb_agg(jsonb_build_object(
            'chunk_id',b.id,'entity_type',b.entity_type,
            'entity_id',b.entity_id,'status',b.status,
            'error_code',b.error_json->>'code') order by b.id)
          from(
            select c.* from current_chunks c
            where c.operation_id in(
              select a.id union all
              select cr.id from descendants cr where cr.root_id=a.id)
              and c.status in('BLOCKED','FAILED','DEAD_LETTER')
            order by c.id limit 500
          ) b),'[]'::jsonb),
        'invoice_ids_page',coalesce(a.result_json->'invoice_ids_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false)),
        'document_operations_page',coalesce(
          a.result_json->'document_operations_page',
          jsonb_build_object('total_count',ds.document_operation_count,
            'returned_count',least(ds.document_operation_count,500),
            'truncated',ds.document_operation_count>500)),
        'delivery_operations_page',coalesce(
          a.result_json->'delivery_operations_page',
          jsonb_build_object('total_count',ds.delivery_operation_count,
            'returned_count',least(ds.delivery_operation_count,500),
            'truncated',ds.delivery_operation_count>500)),
        'issue_outcomes_page',coalesce(a.result_json->'issue_outcomes_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false)),
        'mail_ids_page',coalesce(a.result_json->'mail_ids_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false)),
        'blocked_entities_page',coalesce(
          a.result_json->'blocked_entities_page',
          jsonb_build_object('total_count',0,'returned_count',0,
            'truncated',false))
      ) aggregate_result
    from authorised a
    join descendant_summary ds on ds.root_id=a.id
    left join selected_documents d on d.root_operation_id=a.id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'operation_id',r.id,'parent_operation_id',r.parent_operation_id,
    'operation_type',r.operation_type,'entity_type',r.entity_type,'entity_id',r.entity_id,
    'status',r.status,'phase',r.phase,'priority',r.priority,
    'source_revision',r.source_revision,'template_version',r.template_version,
    'total_units',r.total_units,'completed_units',r.completed_units,'failed_units',r.failed_units,
    'progress',r.progress_json,
    'result',case when v_mode='DETAIL' or r.status='COMPLETE'
      then coalesce(r.result_json,'{}'::jsonb)||r.aggregate_result end,
    'error_code',r.error_json->>'code',
    'error_summary',coalesce(r.error_json->>'summary',r.error_json->>'message'),
    'requires_user_action',r.requires_user_action,
    'can_retry',r.status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT'),
    'can_cancel',r.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      and not exists(
        select 1 from current_chunks ic
        join public.invoices i on i.id=ic.entity_id
        where ic.operation_id in(
            select r.id
            union all select cr.id from descendants cr where cr.root_id=r.id)
          and ic.chunk_type='ISSUE_INVOICE'
          and i.status in('ISSUED','PAID')),
    'change_seq',r.change_seq,
    'effective_change_seq',r.effective_change_seq,
    'total_descendants',r.total_descendants,
    'descendant_page_offset',v_descendant_offset,
    'returned_descendants',r.returned_descendants,
    'results_truncated',r.results_truncated,
    'continuation_token',r.continuation_token,
    'total_chunks',r.total_chunks,
    'returned_chunks',r.returned_chunks,
    'chunks_truncated',r.chunks_truncated,
    'chunk_page_offset',v_chunk_offset,
    'chunk_continuation_token',r.chunk_continuation_token,
    'legal_issue_state',coalesce(
      r.result_json->>'legal_issue_status',
      case when r.operation_type='ISSUE_INVOICES' then 'IN_PROGRESS' end),
    'delivery_state',coalesce(
      r.result_json->>'delivery_status',
      case when r.delivery_operation_count>0 then 'IN_PROGRESS'
        else 'NOT_REQUESTED' end),
    'active_document_version_id',r.active_document_version_id,
    'document_purpose',r.document_purpose,'document_status',r.document_status,
    'ready_key',case when r.document_status='READY' then r.r2_key end,
    'artifact',case when v_mode='DETAIL' and r.document_status='READY'
      then jsonb_build_object('sha256',r.sha256,'size_bytes',r.size_bytes,
        'page_count',r.page_count) end,
    'children',r.children,'chunks',r.chunks,
    'created_at_utc',r.created_at_utc,'updated_at_utc',r.updated_at_utc,
    'completed_at_utc',r.completed_at_utc
  ) order by r.ordinality),'[]'::jsonb) into v_result
  from operation_rows r;

  return v_result;
end;
$function$
;

-- SOURCE private._invoice_operation_rollup_batch_legacy_20260726(p_operation_ids uuid[], p_now_utc timestamp with time zone, p_propagate_ancestors boolean)
CREATE OR REPLACE FUNCTION private._invoice_operation_rollup_batch_legacy_20260726(p_operation_ids uuid[], p_now_utc timestamp with time zone DEFAULT now(), p_propagate_ancestors boolean DEFAULT true)
 RETURNS TABLE(operation_id uuid, status text, phase text, total_units integer, completed_units integer, failed_units integer, blocked_required_count integer, requires_user_action boolean, change_seq bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
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
$function$
;

-- SOURCE private._invoice_operation_start_batch_legacy_20260726(p_commands jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_operation_start_batch_legacy_20260726(p_commands jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_jwt_role text:=coalesce(
    nullif(current_setting('request.jwt.claim.role',true),''),
    auth.jwt()->>'role','');
  v_auth_user uuid:=auth.uid();
  v_result jsonb;
begin
  if jsonb_typeof(p_commands)<>'array'
     or jsonb_array_length(p_commands)<1
     or jsonb_array_length(p_commands)>1000 then
    raise exception using errcode='22023',
      message='p_commands must be a JSON array containing 1..1000 commands';
  end if;

  if p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
       where u.id=p_actor_user_id and u.is_active and lower(u.role)='admin')
     or(v_jwt_role<>'service_role' and v_auth_user is distinct from p_actor_user_id) then
    raise exception using errcode='42501',
      message='Active administrator actor and matching authenticated/service caller required';
  end if;

  with raw as materialized (
    select e.ordinality::integer command_no,e.value command_json,
      upper(btrim(coalesce(e.value->>'command_type',e.value->>'type',''))) command_type
    from jsonb_array_elements(p_commands) with ordinality e(value,ordinality)
  ),
  command_ids as materialized (
    select r.command_no,r.command_type,x.ordinality::integer item_no,x.value id_text,
      x.value ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        valid_uuid
    from raw r
    cross join lateral jsonb_array_elements_text(
      case
        when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
          then r.command_json->'nhsp_shift_ids'
        when r.command_type in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS',
          'GENERATE_EXPENSES','GENERATE_NHSP','GENERATE_AUTO')
          and jsonb_typeof(r.command_json->'source_ids')='array'
          then r.command_json->'source_ids'
        when r.command_type in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS',
          'GENERATE_EXPENSES','GENERATE_NHSP','GENERATE_AUTO')
          and jsonb_typeof(r.command_json->'timesheet_ids')='array'
          then r.command_json->'timesheet_ids'
        when r.command_type in('GENERATE_CREDIT_NOTE','ISSUE_INVOICES','DELIVER_INVOICES')
          and jsonb_typeof(r.command_json->'invoice_ids')='array'
          then r.command_json->'invoice_ids'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and coalesce(r.command_json->>'base_invoice_id','')<>'' then
          jsonb_build_array(r.command_json->>'base_invoice_id')
        when r.command_type in(
          'GENERATE_CREDIT_NOTE','VIEW_INVOICE_DOCUMENT','DELIVER_INVOICES')
          and coalesce(r.command_json->>'invoice_id','')<>'' then
          jsonb_build_array(r.command_json->>'invoice_id')
        when r.command_type='VIEW_TIMESHEET_DOCUMENT'
          and coalesce(r.command_json->>'timesheet_id','')<>'' then
          jsonb_build_array(r.command_json->>'timesheet_id')
        else '[]'::jsonb
      end
    ) with ordinality x(value,ordinality)
  ),
  canonical_ids as materialized (
    select command_no,array_agg(distinct id_text::uuid order by id_text::uuid) ids
    from command_ids where valid_uuid group by command_no
  ),
  generation_groups as materialized (
    select *
    from private._invoice_generation_resolve_command_groups(
      p_commands,p_actor_user_id,v_now)
  ),
  view_facts as materialized (
    select r.command_no,r.command_type,
      case when r.command_type='VIEW_INVOICE_DOCUMENT' then 'INVOICE' else 'TIMESHEET' end entity_type,
      (array_agg(ci.id_text::uuid order by ci.item_no))[1] entity_id,
      case when r.command_type='VIEW_INVOICE_DOCUMENT'
        then i.document_revision else t.document_revision end document_revision,
      case when r.command_type='VIEW_INVOICE_DOCUMENT'
        then 'DRAFT_PREVIEW' else 'TIMESHEET' end purpose,
      coalesce(nullif(r.command_json->>'template_version',''),
        case when r.command_type='VIEW_INVOICE_DOCUMENT'
          then 'invoice-professional-v1' else 'timesheet-professional-v1' end) template_version
    from raw r
    left join command_ids ci on ci.command_no=r.command_no and ci.valid_uuid
    left join public.invoices i
      on r.command_type='VIEW_INVOICE_DOCUMENT' and i.id=ci.id_text::uuid
    left join public.timesheets t
      on r.command_type='VIEW_TIMESHEET_DOCUMENT' and t.timesheet_id=ci.id_text::uuid
    where r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
    group by r.command_no,r.command_type,i.document_revision,t.document_revision,r.command_json
  ),
  issue_facts as materialized (
    select r.command_no,
      array_agg(i.id order by i.id) invoice_ids,
      encode(digest(string_agg(i.id||':'||i.document_revision,
        '|' order by i.id),'sha256'),'hex') invoice_revision_hash,
      count(*)::integer resolved_count
    from raw r join canonical_ids ci on ci.command_no=r.command_no
    cross join unnest(ci.ids) invoice_id
    join public.invoices i on i.id=invoice_id
    where r.command_type='ISSUE_INVOICES'
    group by r.command_no
  ),
  delivery_facts as materialized (
    select r.command_no,
      array_agg(i.id order by i.id) invoice_ids,
      array_agg(i.issued_document_version_id order by i.id) document_version_ids,
      encode(digest(string_agg(concat_ws(':',i.id::text,
        i.issued_document_version_id::text,v.sha256,v.size_bytes::text,v.page_count::text),
        '|' order by i.id),'sha256'),'hex') delivery_revision,
      count(*)::integer resolved_count
    from raw r join canonical_ids ci on ci.command_no=r.command_no
    cross join unnest(ci.ids) invoice_id
    join public.invoices i on i.id=invoice_id
    join public.invoice_document_versions v
      on v.id=i.issued_document_version_id and v.status='READY'
      and v.purpose='FINAL_ISSUE' and v.entity_type='INVOICE' and v.entity_id=i.id
    where r.command_type='DELIVER_INVOICES' and i.status in('ISSUED','PAID')
    group by r.command_no
  ),
  recipient_sets as materialized (
    select r.command_no,
      coalesce((select jsonb_agg(email order by email)
        from(
          select distinct lower(btrim(e.value)) email
          from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'recipient_set')='array'
              then r.command_json->'recipient_set' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ) q),'[]'::jsonb) canonical_recipients,
      coalesce((select jsonb_agg(email order by email)
        from(
          select distinct lower(btrim(e.value)) email
          from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'cc')='array'
              then r.command_json->'cc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ) q),'[]'::jsonb) canonical_cc,
      coalesce((select jsonb_agg(email order by email)
        from(
          select distinct lower(btrim(e.value)) email
          from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'bcc')='array'
              then r.command_json->'bcc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ) q),'[]'::jsonb) canonical_bcc,
      (select count(*)::integer
        from(
          select value from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'recipient_set')='array'
              then r.command_json->'recipient_set' else '[]'::jsonb end)
          union all
          select value from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'cc')='array'
              then r.command_json->'cc' else '[]'::jsonb end)
          union all
          select value from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'bcc')='array'
              then r.command_json->'bcc' else '[]'::jsonb end)
        ) e
        where nullif(btrim(e.value),'') is not null
          and btrim(e.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
        invalid_count
    from raw r
    where r.command_type='DELIVER_INVOICES'
  ),
  delivery_route_requests as materialized (
    select r.command_no,i.invoice_id,jsonb_build_object(
      'request_key',r.command_no::text||':'||i.invoice_id::text,
      'invoice_id',i.invoice_id,
      'recipient_set',rs.canonical_recipients,
      'cc',rs.canonical_cc,
      'bcc',rs.canonical_bcc,
      'delivery_policy',upper(coalesce(
        nullif(r.command_json->>'delivery_policy',''),'ATTACH')),
      'template_version',coalesce(
        nullif(r.command_json->>'delivery_template_version',''),
        'invoice-delivery-v1')) request_json
    from raw r
    join delivery_facts f on f.command_no=r.command_no
    join recipient_sets rs on rs.command_no=r.command_no
    cross join unnest(f.invoice_ids) i(invoice_id)
    where r.command_type='DELIVER_INVOICES'
  ),
  delivery_routes as materialized (
    select q.command_no,d.*
    from private._invoice_delivery_routes_batch(coalesce((
      select jsonb_agg(q.request_json order by q.command_no,q.invoice_id)
      from delivery_route_requests q),'[]'::jsonb),
      (v_now at time zone 'Europe/London')::date) d
    join delivery_route_requests q
      on q.request_json->>'request_key'=d.request_key
      and q.invoice_id=d.invoice_id
  ),
  delivery_route_groups as materialized (
    select command_no,
      encode(digest(string_agg(invoice_id::text||':'||route_policy_hash,
        '|' order by invoice_id),'sha256'),'hex') route_set_hash,
      jsonb_agg(jsonb_build_object(
        'invoice_id',invoice_id,'to',canonical_to,'cc',canonical_cc,
        'bcc',canonical_bcc,'recipient_set_hash',recipient_set_hash,
        'route_policy_hash',route_policy_hash,
        'route_source',route_source,'do_not_send',do_not_send,
        'warning_codes',warning_codes,'blocker_codes',blocker_codes,
        'evaluated_date',(v_now at time zone 'Europe/London')::date)
        order by invoice_id) frozen_routes
    from delivery_routes
    group by command_no
  ),
  issue_delivery_intents as materialized (
    select r.command_no,
      (intent.intent_json-'recipient_set'-'cc'-'bcc')
        ||jsonb_build_object(
          'recipient_set',recipients.canonical,
          'cc',cc.canonical,'bcc',bcc.canonical) canonical_intent,
      recipients.requested_count+cc.requested_count+bcc.requested_count requested_count,
      recipients.valid_count+cc.valid_count+bcc.valid_count valid_count
    from raw r
    cross join lateral(
      select case when jsonb_typeof(r.command_json->'delivery_intent')='object'
        then r.command_json->'delivery_intent' else '{}'::jsonb end intent_json
    ) intent
    cross join lateral(
      select coalesce((select jsonb_agg(email order by email)
          from(
            select distinct lower(btrim(e.value)) email
            from jsonb_array_elements_text(
              case when jsonb_typeof(intent.intent_json->'recipient_set')='array'
                then intent.intent_json->'recipient_set' else '[]'::jsonb end) e(value)
            where nullif(btrim(e.value),'') is not null
              and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
          ) valid),'[]'::jsonb) canonical,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'recipient_set')='array'
              then intent.intent_json->'recipient_set' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null) requested_count,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'recipient_set')='array'
              then intent.intent_json->'recipient_set' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
          valid_count
    ) recipients
    cross join lateral(
      select coalesce((select jsonb_agg(email order by email)
          from(
            select distinct lower(btrim(e.value)) email
            from jsonb_array_elements_text(
              case when jsonb_typeof(intent.intent_json->'cc')='array'
                then intent.intent_json->'cc' else '[]'::jsonb end) e(value)
            where nullif(btrim(e.value),'') is not null
              and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
          ) valid),'[]'::jsonb) canonical,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'cc')='array'
              then intent.intent_json->'cc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null) requested_count,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'cc')='array'
              then intent.intent_json->'cc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
          valid_count
    ) cc
    cross join lateral(
      select coalesce((select jsonb_agg(email order by email)
          from(
            select distinct lower(btrim(e.value)) email
            from jsonb_array_elements_text(
              case when jsonb_typeof(intent.intent_json->'bcc')='array'
                then intent.intent_json->'bcc' else '[]'::jsonb end) e(value)
            where nullif(btrim(e.value),'') is not null
              and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
          ) valid),'[]'::jsonb) canonical,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'bcc')='array'
              then intent.intent_json->'bcc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null) requested_count,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'bcc')='array'
              then intent.intent_json->'bcc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
          valid_count
    ) bcc
    where r.command_type='ISSUE_INVOICES'
  ),
  asset_specs as materialized (
    select r.command_no,r.command_json,
      upper(btrim(coalesce(r.command_json->>'source_kind',''))) source_kind,
      case when coalesce(r.command_json->>'source_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(r.command_json->>'source_id')::uuid end source_id,
      nullif(btrim(r.command_json->>'source_revision'),'') source_revision,
      nullif(btrim(r.command_json->>'original_r2_key'),'') original_r2_key,
      nullif(btrim(r.command_json->>'original_filename'),'') original_filename,
      nullif(btrim(r.command_json->>'declared_media_type'),'') declared_media_type,
      case when coalesce(r.command_json->>'rotation_degrees','') ~ '^(0|90|180|270)$'
        then(r.command_json->>'rotation_degrees')::integer end rotation_degrees,
      r.command_json?'rotation_degrees' rotation_supplied
    from raw r where r.command_type='PREPARE_ASSET'
  ),
  issue_entity_locks as materialized (
    select pg_advisory_xact_lock(
      hashtextextended('ISSUE_INVOICE|'||ci.id_text,0))
    from command_ids ci join raw r on r.command_no=ci.command_no
    where r.command_type='ISSUE_INVOICES' and ci.valid_uuid
    order by ci.id_text
  ),
  command_validation as materialized (
    select r.command_no,r.command_type,
      case
        when jsonb_typeof(r.command_json)<>'object'
          then 'COMMAND_MUST_BE_OBJECT'
        when r.command_type not in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS','GENERATE_EXPENSES',
          'GENERATE_NHSP','GENERATE_CREDIT_NOTE','GENERATE_AUTO',
          'VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT','PREPARE_ASSET',
          'ISSUE_INVOICES','DELIVER_INVOICES','RECONCILE')
          then 'UNSUPPORTED_COMMAND_TYPE'
        when r.command_type in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS',
          'GENERATE_EXPENSES','GENERATE_AUTO')
          and not(
            jsonb_typeof(r.command_json->'source_ids')='array'
            or jsonb_typeof(r.command_json->'timesheet_ids')='array')
          then 'GENERATION_SOURCE_ARRAY_REQUIRED'
        when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')<>'array'
          then 'NHSP_SHIFT_ARRAY_REQUIRED'
        when r.command_type like 'GENERATE_%'
          and r.command_type<>'GENERATE_CREDIT_NOTE'
          and r.command_json?'consolidation_mode'
          and upper(coalesce(r.command_json->>'consolidation_mode','')) not in(
            'NONE','BY_WEEK','ANY_WEEK')
          then 'INVALID_CONSOLIDATION_MODE'
        when r.command_type like 'GENERATE_%'
          and r.command_type<>'GENERATE_CREDIT_NOTE'
          and r.command_json?'allow_early'
          and jsonb_typeof(r.command_json->'allow_early')<>'boolean'
          then 'ALLOW_EARLY_MUST_BE_BOOLEAN'
        when r.command_type like 'GENERATE_%'
          and r.command_type<>'GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(
            r.command_json->>'target_invoice_week','')),'') is not null
          and not pg_input_is_valid(
            r.command_json->>'target_invoice_week','date')
          then 'INVALID_TARGET_INVOICE_WEEK'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(r.command_json->>'base_invoice_id','')),'') is null
          then 'BASE_INVOICE_ID_REQUIRED'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and(select count(*) from command_ids x
            where x.command_no=r.command_no)<>1
          then 'EXACTLY_ONE_BASE_INVOICE_REQUIRED'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(r.command_json->>'credit_reason','')),'') is null
          then 'CREDIT_REASON_REQUIRED'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(r.command_json->>'command_token','')),'') is null
          then 'CREDIT_COMMAND_TOKEN_REQUIRED'
        when exists(select 1 from command_ids x
          where x.command_no=r.command_no and not x.valid_uuid)
          then 'MALFORMED_UUID'
        when r.command_type like 'GENERATE_%' and not exists(
          select 1 from generation_groups g where g.command_no=r.command_no)
          then 'GENERATION_SOURCES_REQUIRED'
        when r.command_type like 'GENERATE_%'
          and (select count(*) from generation_groups g
            where g.command_no=r.command_no)>500
          then 'OPERATION_SCOPE_TOO_LARGE'
        when r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
          and(select count(*) from command_ids x where x.command_no=r.command_no)<>1
          then 'EXACTLY_ONE_DOCUMENT_ENTITY_REQUIRED'
        when r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
          and nullif(btrim(coalesce(r.command_json->>'priority_reason','')),'') is null
          then 'DOCUMENT_PRIORITY_REASON_REQUIRED'
        when r.command_type='VIEW_INVOICE_DOCUMENT'
          and upper(coalesce(nullif(r.command_json->>'purpose',''),
            'DRAFT_PREVIEW'))<>'DRAFT_PREVIEW'
          then 'INVALID_INVOICE_DOCUMENT_PURPOSE'
        when r.command_type='VIEW_TIMESHEET_DOCUMENT'
          and upper(coalesce(nullif(r.command_json->>'purpose',''),
            'TIMESHEET'))<>'TIMESHEET'
          then 'INVALID_TIMESHEET_DOCUMENT_PURPOSE'
        when r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
          and not exists(select 1 from view_facts v
            where v.command_no=r.command_no and v.document_revision is not null)
          then 'DOCUMENT_ENTITY_NOT_FOUND'
        when r.command_type='PREPARE_ASSET' and not exists(
          select 1 from asset_specs a
          where a.command_no=r.command_no
            and a.source_kind in(
              'MANUAL_TIMESHEET','TIMESHEET_EVIDENCE','OTHER_EVIDENCE',
              'HR_REPORT','NHSP_REPORT','HIGHER_RATE_SUPPORT')
            and a.source_id is not null and a.source_revision is not null
            and a.original_r2_key is not null and a.original_filename is not null
            and a.declared_media_type is not null
            and(not a.rotation_supplied or a.rotation_degrees is not null))
          then 'INVALID_ASSET_SOURCE_METADATA'
        when r.command_type='PREPARE_ASSET'
          and r.command_json?'rotation_degrees'
          and coalesce(r.command_json->>'rotation_degrees','')!~'^(0|90|180|270)$'
          then 'INVALID_ASSET_ROTATION'
        when r.command_type='ISSUE_INVOICES'
          and(jsonb_typeof(r.command_json->'invoice_ids')<>'array'
            or coalesce(cardinality((select ids from canonical_ids x
              where x.command_no=r.command_no)),0)=0
            or coalesce((select resolved_count from issue_facts f
              where f.command_no=r.command_no),0)<>
              coalesce(cardinality((select ids from canonical_ids x
                where x.command_no=r.command_no)),0))
          then 'ISSUE_INVOICES_NOT_RESOLVED'
        when r.command_type='ISSUE_INVOICES'
          and (r.command_json?'allow_early'
            and jsonb_typeof(r.command_json->'allow_early')<>'boolean'
            or r.command_json?'deliver'
            and jsonb_typeof(r.command_json->'deliver')<>'boolean')
          then 'ISSUE_FLAGS_MUST_BE_BOOLEAN'
        when r.command_type='ISSUE_INVOICES'
          and coalesce(nullif(r.command_json->>'command_token',''),'')=''
          then 'ISSUE_COMMAND_TOKEN_REQUIRED'
        when r.command_type='ISSUE_INVOICES'
          and coalesce(cardinality((select ids from canonical_ids x
            where x.command_no=r.command_no)),0)>500
          then 'OPERATION_SCOPE_TOO_LARGE'
        when r.command_type='ISSUE_INVOICES'
          and r.command_json?'delivery_intent'
          and jsonb_typeof(r.command_json->'delivery_intent')<>'object'
          then 'ISSUE_DELIVERY_INTENT_MUST_BE_OBJECT'
        when r.command_type='ISSUE_INVOICES'
          and jsonb_typeof(r.command_json->'delivery_intent')='object'
          and (
            r.command_json->'delivery_intent'?'recipient_set'
              and jsonb_typeof(r.command_json->'delivery_intent'->'recipient_set')<>'array'
            or r.command_json->'delivery_intent'?'cc'
              and jsonb_typeof(r.command_json->'delivery_intent'->'cc')<>'array'
            or r.command_json->'delivery_intent'?'bcc'
              and jsonb_typeof(r.command_json->'delivery_intent'->'bcc')<>'array')
          then 'ISSUE_DELIVERY_RECIPIENT_SET_MUST_BE_ARRAY'
        when r.command_type='ISSUE_INVOICES'
          and exists(
            select 1 from issue_delivery_intents intent
            where intent.command_no=r.command_no
              and intent.requested_count<>intent.valid_count)
          then 'ISSUE_DELIVERY_RECIPIENT_SET_INVALID'
        when r.command_type='ISSUE_INVOICES' and exists(
          select 1 from command_ids x
          join public.invoice_operation_chunks c
            on c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
            and c.entity_id=x.id_text::uuid
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
          where x.command_no=r.command_no and x.valid_uuid)
          and not exists(
            select 1
            from public.invoice_operations exact
            join issue_facts f on f.command_no=r.command_no
            join issue_delivery_intents intent on intent.command_no=r.command_no
            where exact.idempotency_key=encode(digest(concat_ws('|',
              'ISSUE_INVOICES',f.invoice_revision_hash,
              coalesce(r.command_json->>'allow_early','false'),
              coalesce(r.command_json->>'deliver','false'),
              coalesce(r.command_json->>'command_token',''),
              encode(digest(intent.canonical_intent::text,'sha256'),'hex')),
              'sha256'),'hex')
              and exact.status in(
                'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
          then 'ACTIVE_ISSUE_CONFLICT'
        when r.command_type='DELIVER_INVOICES'
          and not(
            jsonb_typeof(r.command_json->'invoice_ids')='array'
            or nullif(btrim(coalesce(r.command_json->>'invoice_id','')),'') is not null)
          then 'DELIVERY_INVOICE_IDS_REQUIRED'
        when r.command_type='DELIVER_INVOICES'
          and nullif(btrim(coalesce(
            r.command_json->>'delivery_request_token','')),'') is null
          then 'DELIVERY_REQUEST_TOKEN_REQUIRED'
        when r.command_type='DELIVER_INVOICES'
          and coalesce(cardinality((select ids from canonical_ids x
            where x.command_no=r.command_no)),0)>500
          then 'OPERATION_SCOPE_TOO_LARGE'
        when r.command_type='DELIVER_INVOICES'
          and r.command_json?'recipient_set'
          and jsonb_typeof(r.command_json->'recipient_set')<>'array'
          then 'DELIVERY_RECIPIENT_SET_MUST_BE_ARRAY'
        when r.command_type='DELIVER_INVOICES'
          and ((r.command_json?'cc' and jsonb_typeof(r.command_json->'cc')<>'array')
            or(r.command_json?'bcc' and jsonb_typeof(r.command_json->'bcc')<>'array'))
          then 'DELIVERY_CC_BCC_MUST_BE_ARRAY'
        when r.command_type='DELIVER_INVOICES'
          and exists(select 1 from recipient_sets rs
            where rs.command_no=r.command_no and rs.invalid_count>0)
          then 'DELIVERY_RECIPIENT_SET_INVALID'
        when r.command_type='DELIVER_INVOICES'
          and r.command_json?'delivery_part_number'
          and coalesce(r.command_json->>'delivery_part_number','') !~ '^[1-9][0-9]*$'
          then 'INVALID_DELIVERY_PART_NUMBER'
        when r.command_type='DELIVER_INVOICES'
          and r.command_json?'delivery_policy'
          and upper(coalesce(r.command_json->>'delivery_policy',''))
            not in('ATTACH','SPLIT','SECURE_LINK')
          then 'INVALID_DELIVERY_POLICY'
        when r.command_type='DELIVER_INVOICES'
          and(coalesce((select resolved_count from delivery_facts f
              where f.command_no=r.command_no),0)<>
              coalesce(cardinality((select ids from canonical_ids x
                where x.command_no=r.command_no)),0))
          then 'ISSUED_DOCUMENT_NOT_READY'
        when r.command_type='RECONCILE'
          and not(r.command_json?'operation_ids'
            or r.command_json?'older_than_seconds') then 'RECONCILE_SCOPE_REQUIRED'
        when r.command_type='RECONCILE'
          and r.command_json?'operation_ids'
          and jsonb_typeof(r.command_json->'operation_ids')<>'array'
          then 'RECONCILE_OPERATION_IDS_MUST_BE_ARRAY'
        when r.command_type='RECONCILE'
          and jsonb_typeof(r.command_json->'operation_ids')='array'
          and jsonb_array_length(r.command_json->'operation_ids')>500
          then 'RECONCILE_OPERATION_SCOPE_TOO_LARGE'
        when r.command_type='RECONCILE'
          and r.command_json?'older_than_seconds'
          and coalesce(r.command_json->>'older_than_seconds','') !~ '^[0-9]{1,6}$'
          then 'RECONCILE_AGE_MUST_BE_INTEGER_SECONDS'
      end error_code,
      case when r.command_type like 'GENERATE_%' then (
        select jsonb_agg(jsonb_build_object(
          'group_key',g.group_key,'code',g.blocker_code,'detail',g.blocker_detail)
          order by g.group_key)
        from generation_groups g
        where g.command_no=r.command_no and g.blocker_code is not null)
      end error_detail
    from raw r cross join(select count(*) from issue_entity_locks) lock_barrier
  ),
  valid_commands as materialized (
    select r.* from raw r join command_validation v using(command_no,command_type)
    where v.error_code is null
  ),
  inserted_assets as materialized (
    insert into public.invoice_document_assets(
      source_kind,source_id,source_revision,original_r2_key,original_filename,
      declared_media_type,orientation_degrees,status,normalised_manifest_json,
      created_at_utc,updated_at_utc)
    select a.source_kind,a.source_id,a.source_revision,a.original_r2_key,
      a.original_filename,a.declared_media_type,a.rotation_degrees,
      'DISCOVERED','[]'::jsonb,v_now,v_now
    from asset_specs a
    join valid_commands v on v.command_no=a.command_no
    on conflict(source_kind,source_id,source_revision,original_r2_key) do nothing
    returning *
  ),
  assets as materialized (
    select a.command_no,inserted.*
    from asset_specs a
    join valid_commands v on v.command_no=a.command_no
    join inserted_assets inserted
      on inserted.source_kind=a.source_kind
      and inserted.source_id=a.source_id
      and inserted.source_revision=a.source_revision
      and inserted.original_r2_key=a.original_r2_key
    union all
    select a.command_no,registered.*
    from asset_specs a
    join valid_commands v on v.command_no=a.command_no
    join public.invoice_document_assets registered
      on registered.source_kind=a.source_kind
      and registered.source_id=a.source_id
      and registered.source_revision=a.source_revision
      and registered.original_r2_key=a.original_r2_key
    where not exists(
      select 1 from inserted_assets inserted
      where inserted.source_kind=a.source_kind
        and inserted.source_id=a.source_id
        and inserted.source_revision=a.source_revision
        and inserted.original_r2_key=a.original_r2_key)
  ),
  asset_workflow_state as materialized (
    select a.*,
      exists(
        select 1
        from public.invoice_operation_chunks c
        join public.invoice_operations o on o.id=c.operation_id
        where c.document_asset_id=a.id
          and c.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
          and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
          and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      ) has_active_work
    from assets a
  ),
  operation_specs as materialized (
    select v.command_no,v.command_type,v.command_json,
      'GENERATE_INVOICES'::text operation_type,
      case when count(*)=1 then 'CLIENT' else 'INVOICE_BATCH' end entity_type,
      case when count(*)=1 then(array_agg(g.client_id order by g.client_id))[1]
        else null::uuid end entity_id,
      encode(digest(string_agg(g.source_revision_hash,'|' order by g.group_key),
        'sha256'),'hex') source_revision,
      null::text template_version,
      case when v.command_type='GENERATE_AUTO' then 200 else 600 end priority,
      count(*)::integer unit_count,
      jsonb_build_object('generation_groups',
        jsonb_agg(g.idempotency_components order by g.group_key)) canonical_input,
      encode(digest('GENERATE_INVOICES|'||
        string_agg(g.idempotency_components::text,'|' order by g.group_key),
        'sha256'),'hex') idempotency_key
    from valid_commands v join generation_groups g on g.command_no=v.command_no
    where v.command_type like 'GENERATE_%'
    group by v.command_no,v.command_type,v.command_json
    union all
    select v.command_no,v.command_type,v.command_json,'BUILD_DOCUMENT',
      f.entity_type,f.entity_id,f.document_revision::text,f.template_version,1000,1,
      jsonb_build_object('entity_type',f.entity_type,'entity_id',f.entity_id,
        'purpose',f.purpose,'document_revision',f.document_revision,
        'template_version',f.template_version),
      encode(digest(concat_ws('|','BUILD_DOCUMENT',f.entity_type,f.entity_id::text,
        f.purpose,f.document_revision::text,f.template_version),'sha256'),'hex')
    from valid_commands v join view_facts f on f.command_no=v.command_no
    where v.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
      and not exists(
        select 1 from public.invoice_document_versions ready
        where ready.entity_type=f.entity_type and ready.entity_id=f.entity_id
          and ready.purpose=f.purpose
          and ready.source_revision=f.document_revision::text
          and ready.template_version=f.template_version and ready.status='READY')
    union all
    select v.command_no,v.command_type,v.command_json,'PREPARE_ASSET',
      'DOCUMENT_ASSET',a.id,a.source_revision,null,550,1,
      jsonb_build_object('document_asset_id',a.id,'source_kind',a.source_kind,
        'source_id',a.source_id,'source_revision',a.source_revision,
        'original_r2_key',a.original_r2_key),
      encode(digest(concat_ws('|','PREPARE_ASSET',a.source_kind,a.source_id::text,
        a.source_revision,a.original_r2_key),'sha256'),'hex')
    from valid_commands v join asset_workflow_state a
      on a.command_no=v.command_no
    where v.command_type='PREPARE_ASSET'
      and a.status in('DISCOVERED','INSPECTING','NORMALISING')
      and not(a.status in('INSPECTING','NORMALISING')
        and not a.has_active_work)
    union all
    select v.command_no,v.command_type,v.command_json,'ISSUE_INVOICES',
      'INVOICE_BATCH',null,f.invoice_revision_hash,null,850,
      cardinality(f.invoice_ids),
      jsonb_build_object('invoice_ids',to_jsonb(f.invoice_ids),
        'invoice_revision_hash',f.invoice_revision_hash,
        'allow_early',coalesce(v.command_json->'allow_early','false'::jsonb),
        'deliver',coalesce(v.command_json->'deliver','false'::jsonb),
        'command_token',v.command_json->>'command_token',
        'delivery_request_token','ISSUE:'||(v.command_json->>'command_token'),
        'delivery_intent',intent.canonical_intent),
      encode(digest(concat_ws('|','ISSUE_INVOICES',f.invoice_revision_hash,
        coalesce(v.command_json->>'allow_early','false'),
        coalesce(v.command_json->>'deliver','false'),
        coalesce(v.command_json->>'command_token',''),
        encode(digest(intent.canonical_intent::text,'sha256'),'hex')),
        'sha256'),'hex')
    from valid_commands v join issue_facts f on f.command_no=v.command_no
    join issue_delivery_intents intent on intent.command_no=v.command_no
    where v.command_type='ISSUE_INVOICES'
    union all
    select v.command_no,v.command_type,v.command_json,'DELIVER_INVOICES',
      'INVOICE_BATCH',null,f.delivery_revision,
      coalesce(nullif(v.command_json->>'delivery_template_version',''),
        'invoice-delivery-v1'),400,cardinality(f.invoice_ids),
      jsonb_build_object('invoice_ids',to_jsonb(f.invoice_ids),
        'issued_document_version_ids',to_jsonb(f.document_version_ids),
        'delivery_revision',f.delivery_revision,
        'recipient_set',rs.canonical_recipients,
        'recipient_set_hash',drg.route_set_hash,
        'frozen_delivery_routes',drg.frozen_routes,
        'template_version',coalesce(nullif(v.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        'delivery_part_number',coalesce(v.command_json->'delivery_part_number','1'::jsonb),
        'delivery_request_token',v.command_json->>'delivery_request_token'),
      encode(digest(concat_ws('|','DELIVER_INVOICES',f.delivery_revision,
        drg.route_set_hash,
        coalesce(nullif(v.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        coalesce(v.command_json->>'delivery_part_number','1'),
        v.command_json->>'delivery_request_token'),'sha256'),'hex')
    from valid_commands v join delivery_facts f on f.command_no=v.command_no
    join recipient_sets rs on rs.command_no=v.command_no
    join delivery_route_groups drg on drg.command_no=v.command_no
    where v.command_type='DELIVER_INVOICES'
    union all
    select v.command_no,v.command_type,v.command_json,'RECONCILE_INVOICE_WORK',
      'SYSTEM',null,
      encode(digest(coalesce(v.command_json->'operation_ids','[]')::text||'|'||
        coalesce(v.command_json->>'older_than_seconds',''),'sha256'),'hex'),
      null,50,1,
      jsonb_build_object(
        'operation_ids',coalesce(v.command_json->'operation_ids','[]'::jsonb),
        'older_than_seconds',least(604800,greatest(30,case
          when coalesce(v.command_json->>'older_than_seconds','') ~ '^[0-9]{1,6}$'
            then(v.command_json->>'older_than_seconds')::integer else 300 end)),
        'max_rows',least(500,greatest(1,case
          when coalesce(v.command_json->>'max_rows','') ~ '^[0-9]{1,6}$'
            then(v.command_json->>'max_rows')::integer else 100 end))),
      encode(digest('RECONCILE|'||coalesce(v.command_json->'operation_ids','[]')::text||
        '|'||coalesce(v.command_json->>'older_than_seconds',''),'sha256'),'hex')
    from valid_commands v where v.command_type='RECONCILE'
  ),
  locks as materialized (
    select pg_advisory_xact_lock(hashtextextended(x.lock_key,0))
    from(
      select idempotency_key lock_key from operation_specs
      union
      select 'ISSUE_INVOICE|'||unnest(f.invoice_ids)::text
      from issue_facts f join valid_commands v on v.command_no=f.command_no
      where v.command_type='ISSUE_INVOICES'
    ) x order by x.lock_key
  ),
  existing_before as materialized (
    select s.command_no,o.*
    from operation_specs s
    join lateral (
      select candidate.*
      from public.invoice_operations candidate
      where candidate.idempotency_key=s.idempotency_key
        and candidate.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      order by(candidate.status='COMPLETE') desc,candidate.created_at_utc desc
      limit 1
    ) o on true
  ),
  inserted_operations as materialized (
    insert into public.invoice_operations(
      operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
      status,phase,priority,source_revision,template_version,input_json,
      config_json,progress_json,total_units,chunk_count,control_version,
      change_seq,created_at_utc,updated_at_utc)
    select s.operation_type,s.entity_type,s.entity_id,p_actor_user_id,
      s.idempotency_key,'QUEUED','SUBMITTED',s.priority,s.source_revision,
      s.template_version,s.canonical_input,
      jsonb_build_object(
        'command_type',s.command_type,
        'processor_policy',private._invoice_processor_limits()),
      jsonb_build_object('status_message','Accepted','total_units',s.unit_count,
        'completed_units',0,'failed_units',0),
      s.unit_count,s.unit_count,1,nextval('public.invoice_operation_change_seq'),
      v_now,v_now
    from operation_specs s cross join(select count(*) from locks) lock_barrier
    where not exists(select 1 from existing_before e where e.command_no=s.command_no)
    on conflict do nothing
    returning *
  ),
  chosen as materialized (
    select s.*,coalesce(i.id,e.id) operation_id,
      coalesce(i.status,e.status) operation_status,
      coalesce(i.phase,e.phase) operation_phase,
      coalesce(i.control_version,e.control_version) control_version,
      coalesce(i.change_seq,e.change_seq) change_seq,
      coalesce(i.priority,e.priority,s.priority) current_priority,
      i.id is not null created,e.id is not null reused
    from operation_specs s
    left join inserted_operations i on i.idempotency_key=s.idempotency_key
    left join existing_before e on e.command_no=s.command_no
  ),
  raised as materialized (
    update public.invoice_operations o
    set priority=greatest(o.priority,c.priority),updated_at_utc=v_now,
        change_seq=case when o.priority<c.priority
          then nextval('public.invoice_operation_change_seq') else o.change_seq end
    from chosen c
    where c.reused and o.id=c.operation_id
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning o.id
  ),
  existing_document_versions as materialized (
    select c.command_no,c.operation_id,v.*
    from chosen c join view_facts f on f.command_no=c.command_no
    join public.invoice_document_versions v
      on v.entity_type=f.entity_type and v.entity_id=f.entity_id
      and v.purpose=f.purpose and v.source_revision=f.document_revision::text
      and v.template_version=f.template_version
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
  ),
  document_versions as materialized (
    insert into public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,template_version,
      status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select f.entity_type,f.entity_id,f.purpose,c.operation_id,
      f.document_revision::text,f.template_version,'PLANNING','{}'::jsonb,
      encode(digest('{}','sha256'),'hex'),'[]'::jsonb,
      encode(digest('[]','sha256'),'hex'),v_now
    from chosen c join view_facts f on f.command_no=c.command_no
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
      and c.operation_status<>'COMPLETE'
    on conflict(entity_type,entity_id,purpose,source_revision,template_version)
      where purpose in('DRAFT_PREVIEW','TIMESHEET') and status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning *
  ),
  all_document_versions as materialized (
    select c.command_no,c.operation_id,v.*
    from chosen c join view_facts f on f.command_no=c.command_no
    join document_versions v
      on v.entity_type=f.entity_type and v.entity_id=f.entity_id
      and v.purpose=f.purpose and v.source_revision=f.document_revision::text
      and v.template_version=f.template_version
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
    union all
    select e.* from existing_document_versions e
  ),
  chunk_specs as materialized (
    select c.operation_id,'GENERATION_GROUP'::text chunk_type,
      'VALIDATE_SOURCES'::text phase,
      encode(digest(concat_ws('|','GENERATION_GROUP',g.group_key,
        g.source_revision_hash,c.command_type,
        coalesce(g.idempotency_components->>'consolidation_mode',''),
        coalesce(g.idempotency_components->>'invoice_stream',''),'1'),
        'sha256'),'hex') work_key,
      row_number() over(partition by c.operation_id order by g.group_key)::integer-1 sequence_no,
      'CLIENT'::text entity_type,g.client_id entity_id,null::uuid document_version_id,
      null::uuid document_asset_id,
      g.idempotency_components||jsonb_build_object(
        'canonical_source_ids',to_jsonb(g.canonical_source_ids),
        'canonical_source_members',g.canonical_source_members,
        'source_types',to_jsonb(g.source_types),
        'source_revision',g.source_revision_hash,
        'resolver_blocker_code',g.blocker_code,
        'resolver_blocker_detail',g.blocker_detail) payload_json,
      c.priority,c.control_version
    from chosen c join generation_groups g on g.command_no=c.command_no
    where c.command_type like 'GENERATE_%' and c.operation_status<>'COMPLETE'
    union all
    select c.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',v.id::text,
        f.document_revision::text,f.template_version,'1'),'sha256'),'hex'),
      0,f.entity_type,f.entity_id,
      v.id,null,jsonb_build_object('purpose',f.purpose,
        'source_revision',f.document_revision::text,
        'template_version',f.template_version),c.priority,c.control_version
    from chosen c join view_facts f on f.command_no=c.command_no
    join all_document_versions v on v.command_no=c.command_no
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
      and c.operation_status<>'COMPLETE' and v.status<>'READY'
    union all
    select c.operation_id,'ASSET_INSPECT','INSPECT',
      encode(digest(concat_ws('|','ASSET_INSPECT',a.id::text,a.source_revision,
        private._invoice_processor_limits()->>'policy_version'),'sha256'),'hex'),
      0,'DOCUMENT_ASSET',a.id,
      null,a.id,jsonb_build_object('source_revision',a.source_revision,
        'source_kind',a.source_kind,'source_id',a.source_id,
        'rotation_degrees',a.orientation_degrees),c.priority,c.control_version
    from chosen c join assets a on a.command_no=c.command_no
    where c.command_type='PREPARE_ASSET' and c.operation_status<>'COMPLETE'
      and a.status not in('READY','INSPECTING','NORMALISING')
    union all
    select c.operation_id,'ISSUE_INVOICE','VALIDATE',
      encode(digest(concat_ws('|','ISSUE_INVOICE',i.id::text,
        i.document_revision::text,c.command_json->>'command_token'),'sha256'),'hex'),
      row_number() over(partition by c.operation_id order by i.id)::integer-1,
      'INVOICE',i.id,null,null,
      jsonb_build_object(
        'request_key',c.command_no::text||':'||i.id::text,
        'invoice_id',i.id,'source_revision',i.document_revision,
        'evaluation_date',(v_now at time zone 'Europe/London')::date,
        'frozen_issue_at_utc',v_now,
        'allow_early',coalesce(c.command_json->'allow_early','false'::jsonb),
        'deliver',coalesce(c.command_json->'deliver','false'::jsonb),
        'command_token',c.command_json->>'command_token',
        'delivery_request_token','ISSUE:'||(c.command_json->>'command_token'),
        'delivery_intent',intent.canonical_intent),
      c.priority,c.control_version
    from chosen c join issue_facts f on f.command_no=c.command_no
    join issue_delivery_intents intent on intent.command_no=c.command_no
    cross join unnest(f.invoice_ids) invoice_id
    join public.invoices i on i.id=invoice_id
    where c.command_type='ISSUE_INVOICES' and c.operation_status<>'COMPLETE'
    union all
    select c.operation_id,'DELIVERY_PREPARE','PREPARE',
      encode(digest(concat_ws('|','DELIVERY_PREPARE',
        i.issued_document_version_id::text,dr.route_policy_hash,
        coalesce(nullif(c.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        coalesce(c.command_json->>'delivery_part_number','1'),
        c.command_json->>'delivery_request_token'),'sha256'),'hex'),
      row_number() over(partition by c.operation_id order by i.id)::integer-1,
      'INVOICE',i.id,i.issued_document_version_id,null,
      jsonb_build_object(
        'invoice_id',i.id,'issued_document_version_id',i.issued_document_version_id,
        'recipient_set',dr.canonical_to,'cc',dr.canonical_cc,
        'bcc',dr.canonical_bcc,
        'recipient_set_hash',dr.recipient_set_hash,
        'route_policy_hash',dr.route_policy_hash,
        'frozen_delivery_route',jsonb_build_object(
          'request_key',dr.request_key,
          'to',dr.canonical_to,'cc',dr.canonical_cc,'bcc',dr.canonical_bcc,
          'recipient_set_hash',dr.recipient_set_hash,
          'route_policy_hash',dr.route_policy_hash,
          'route_source',dr.route_source,'do_not_send',dr.do_not_send,
          'warning_codes',dr.warning_codes,'blocker_codes',dr.blocker_codes,
          'evaluated_date',(v_now at time zone 'Europe/London')::date),
        'template_version',coalesce(nullif(c.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        'delivery_policy',upper(coalesce(
          nullif(c.command_json->>'delivery_policy',''),'ATTACH')),
        'delivery_part_number',coalesce(c.command_json->'delivery_part_number','1'::jsonb),
        'delivery_request_token',encode(digest(concat_ws('|',
          'DELIVERY_REQUEST',c.command_json->>'delivery_request_token',
          i.id::text,i.issued_document_version_id::text),'sha256'),'hex'),
        'user_command_token',c.command_json->>'delivery_request_token'),
      c.priority,c.control_version
    from chosen c join delivery_facts f on f.command_no=c.command_no
    join recipient_sets rs on rs.command_no=c.command_no
    cross join unnest(f.invoice_ids) invoice_id
    join public.invoices i on i.id=invoice_id
    join delivery_routes dr on dr.command_no=c.command_no
      and dr.request_key=c.command_no::text||':'||i.id::text
      and dr.invoice_id=i.id
    where c.command_type='DELIVER_INVOICES' and c.operation_status<>'COMPLETE'
    union all
    select c.operation_id,'RECONCILE','RECONCILE',
      encode(digest(concat_ws('|','RECONCILE',c.canonical_input::text,'1'),
        'sha256'),'hex'),
      0,'SYSTEM',null,null,null,
      c.canonical_input,c.priority,c.control_version
    from chosen c where c.command_type='RECONCILE' and c.operation_status<>'COMPLETE'
  ),
  inserted_chunks as materialized (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,document_asset_id,status,priority,run_after_utc,
      payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select c.operation_id,c.chunk_type,c.phase,c.work_key,c.sequence_no,c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,'QUEUED',c.priority,v_now,
      c.payload_json,c.control_version,v_now,v_now
    from chunk_specs c
    on conflict do nothing
    returning *
  ),
  asset_operation_links as (
    update public.invoice_document_assets a
    set operation_id=c.operation_id,
        status=case when a.status='DISCOVERED' then 'INSPECTING' else a.status end,
        updated_at_utc=v_now
    from chosen c join assets x on x.command_no=c.command_no
    where c.command_type='PREPARE_ASSET' and a.id=x.id and a.status<>'READY'
    returning a.id
  ),
  entity_pointers as (
    update public.invoices i
    set active_document_operation_id=case
          when c.command_type='VIEW_INVOICE_DOCUMENT' then c.operation_id
          else i.active_document_operation_id end,
        document_state=case when c.command_type='VIEW_INVOICE_DOCUMENT'
          then 'QUEUED' else i.document_state end,
        active_issue_operation_id=case when c.command_type='ISSUE_INVOICES'
          then c.operation_id else i.active_issue_operation_id end,
        issue_state=case when c.command_type='ISSUE_INVOICES'
          then 'VALIDATING' else i.issue_state end,
        updated_at=v_now
    from chosen c
    where(c.command_type='VIEW_INVOICE_DOCUMENT' and i.id=c.entity_id)
      or(c.command_type='ISSUE_INVOICES' and exists(
        select 1
        from issue_facts f
        where f.command_no=c.command_no
          and i.id=any(f.invoice_ids)))
    returning i.id
  ),
  timesheet_pointers as (
    update public.timesheets t
    set active_document_operation_id=c.operation_id,document_state='QUEUED',
        updated_at=v_now
    from chosen c
    where c.command_type='VIEW_TIMESHEET_DOCUMENT' and t.timesheet_id=c.entity_id
    returning t.timesheet_id
  ),
  refreshed_counts as (
    update public.invoice_operations o
    set total_units=greatest(o.total_units,q.n),
        chunk_count=greatest(o.chunk_count,q.n),
        progress_json=coalesce(o.progress_json,'{}'::jsonb)||jsonb_build_object(
          'status_message','Queued',
          'total_units',greatest(o.total_units,q.n)),
        updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    from(
      select intended.operation_id,count(*)::integer n
      from chunk_specs intended
      cross join(
        select count(*) from inserted_chunks
      ) ensure_chunk_insert
      group by intended.operation_id
    ) q
    where o.id=q.operation_id and o.status<>'COMPLETE'
    returning o.id,o.chunk_count n
  ),
  ready_results as (
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',null,'status','READY',
      'document_version_id',v.id,'r2_key',v.r2_key,
      'created',false,'reused_active',false,'reused_ready',true,
      'priority_raised',false,'blocked',false,'terminal_error',null) result
    from raw r join view_facts f on f.command_no=r.command_no
    join public.invoice_document_versions v
      on v.entity_type=f.entity_type and v.entity_id=f.entity_id
      and v.purpose=f.purpose and v.source_revision=f.document_revision::text
      and v.template_version=f.template_version and v.status='READY'
    where r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
    union all
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',null,'status','READY',
      'document_asset_id',a.id,'created',false,'reused_active',false,
      'reused_ready',true,'priority_raised',false,'blocked',false,
      'terminal_error',null) result
    from raw r join assets a on a.command_no=r.command_no
    where r.command_type='PREPARE_ASSET' and a.status='READY'
    union all
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',a.operation_id,'status',a.status,
      'document_asset_id',a.id,'created',false,'reused_active',false,
      'reused_ready',false,'priority_raised',false,'blocked',true,
      'retry_required',true,'terminal_error',coalesce(a.error_json,
        jsonb_build_object('code','ASSET_'||a.status))) result
    from raw r join assets a on a.command_no=r.command_no
    where r.command_type='PREPARE_ASSET'
      and a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
    union all
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',a.operation_id,'status','BLOCKED',
      'document_asset_id',a.id,'created',false,'reused_active',false,
      'reused_ready',false,'priority_raised',false,'blocked',true,
      'retry_required',true,'reconcile_available',true,
      'terminal_error',jsonb_build_object(
        'code','ASSET_WORKFLOW_INCONSISTENT',
        'asset_status',a.status,'document_asset_id',a.id)) result
    from raw r join asset_workflow_state a on a.command_no=r.command_no
    where r.command_type='PREPARE_ASSET'
      and a.status in('INSPECTING','NORMALISING')
      and not a.has_active_work
  ),
  operation_results as (
    select c.command_no,jsonb_build_object(
      'command_no',c.command_no,'command_type',c.command_type,'accepted',true,
      'operation_id',c.operation_id,'operation_type',c.operation_type,
      'status',coalesce(o.status,c.operation_status),
      'phase',coalesce(o.phase,c.operation_phase),
      'source_revision',c.source_revision,
      'change_seq',coalesce(o.change_seq,c.change_seq),
      'created',c.created,
      'reused_active',c.reused and coalesce(o.status,c.operation_status)<>'COMPLETE',
      'reused_ready',c.reused and coalesce(o.status,c.operation_status)='COMPLETE',
      'priority_raised',c.reused and c.priority>c.current_priority,
      'blocked',coalesce(o.status,c.operation_status)='BLOCKED',
      'terminal_error',null,
      'chunk_count',greatest(c.unit_count,(select count(*)
        from chunk_specs intended
        where intended.operation_id=c.operation_id)))
      result
    from chosen c
    left join public.invoice_operations o on o.id=c.operation_id
    left join refreshed_counts rc on rc.id=c.operation_id
  ),
  error_results as (
    select v.command_no,jsonb_build_object(
      'command_no',v.command_no,'command_type',v.command_type,
      'accepted',false,'created',false,'reused_active',false,
      'reused_ready',false,'priority_raised',false,
      'blocked',false,
      'terminal_error',jsonb_build_object('code',v.error_code,
        'detail',v.error_detail),
      'error',jsonb_build_object('code',v.error_code,'detail',v.error_detail)) result
    from command_validation v where v.error_code is not null
  ),
  combined as (
    select * from ready_results
    union all
    select o.* from operation_results o
    where not exists(select 1 from ready_results r where r.command_no=o.command_no)
    union all
    select * from error_results
  )
  select coalesce(jsonb_agg(result order by command_no),'[]'::jsonb)
  into v_result from combined;

  return v_result;
end;
$function$
;

-- SOURCE private._invoice_batch_issue_candidates_legacy_20260726(p_allow_early boolean, p_limit integer)
CREATE OR REPLACE FUNCTION private._invoice_batch_issue_candidates_legacy_20260726(p_allow_early boolean DEFAULT false, p_limit integer DEFAULT 2000)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with
anchor as materialized (
  select (now() at time zone 'Europe/London')::date today,
    greatest(1,least(coalesce(p_limit,2000),20000)) row_limit
),
base as materialized (
  select i.*,c.name client_name,c.primary_invoice_email,
    case when coalesce(i.header_snapshot_json#>>'{meta,invoice_week_start}','')
      ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      then (i.header_snapshot_json#>>'{meta,invoice_week_start}')::date end
      invoice_week_start,
    lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
      i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
      is_self_bill
  from public.invoices i
  join public.clients c on c.id=i.client_id
  where i.type::text='INVOICE' and i.status::text in('DRAFT','ON_HOLD')
  order by i.created_at desc nulls last,i.id
  limit (select row_limit from anchor)
),
validation_requests as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','candidate:'||b.id::text,
    'invoice_id',b.id,'expected_revision',b.document_revision,
    'allow_early',coalesce(p_allow_early,false),'deliver',true)
    order by b.id),'[]'::jsonb) commands
  from base b
),
validations as materialized (
  select v.*
  from validation_requests r
  cross join lateral private._invoice_issue_validate_batch(
    r.commands,(select today from anchor)) v
),
source_timesheets as materialized (
  select distinct l.invoice_id,l.timesheet_id
  from public.invoice_lines l
  join base b on b.id=l.invoice_id
  where l.timesheet_id is not null
),
timesheet_support as materialized (
  select s.invoice_id,s.timesheet_id,t.submission_mode,
    coalesce(pc.effective_ts_attach_to_invoice,true)
      and not coalesce(summary.client_no_timesheet_required,false)
      and not coalesce(summary.client_is_nhsp,false) required,
    ma.status manual_asset_state,
    coalesce(ma.normalised_page_count,0) manual_asset_pages,
    dv.status timesheet_document_state,
    coalesce(dv.page_count,0) timesheet_document_pages,
    case when dv.status<>'READY' then dv.operation_id end
      active_timesheet_document_operation_id,
    upper(coalesce(t.submission_mode::text,'')) in('MANUAL','QR') is_manual
  from source_timesheets s
  left join public.timesheets t
    on t.timesheet_id=s.timesheet_id and t.is_current
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=s.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join lateral (
    select ev.document_asset_id
    from public.timesheet_evidence ev
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=ev.document_asset_id
    where ev.timesheet_id=t.timesheet_id
      and upper(coalesce(ev.kind,''))='TIMESHEET'
      and coalesce(ev.processing_state,'')<>'SUPERSEDED'
    order by(ev.document_asset_id=t.manual_document_asset_id) desc,
      (candidate_asset.status='READY') desc,
      ev.created_at desc nulls last,ev.id desc
    limit 1
  ) manual_source on true
  left join public.invoice_document_assets ma
    on ma.id=coalesce(t.manual_document_asset_id,
      manual_source.document_asset_id)
  left join lateral (
    select v.*
    from public.invoice_document_versions v
    where v.entity_type='TIMESHEET' and v.entity_id=t.timesheet_id
      and v.purpose='TIMESHEET'
      and v.source_revision=t.document_revision::text
      and v.template_version='timesheet-professional-v1'
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
        'VERIFYING','READY','FAILED','SUPERSEDED','CANCELLED')
    order by
      (v.status='READY') desc,
      (v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING')) desc,
      v.created_at_utc desc,v.id desc
    limit 1
  ) dv on true
),
timesheet_support_agg as materialized (
  select b.id invoice_id,
    count(*) filter(where t.timesheet_id is not null and t.required
      and t.is_manual)::integer manual_count,
    count(*) filter(where t.timesheet_id is not null and t.required
      and not t.is_manual)::integer electronic_count,
    count(*) filter(where t.timesheet_id is not null and t.required
      and coalesce(t.timesheet_document_state,'NOT_READY')<>'READY')::integer
      timesheet_not_ready_count,
    coalesce(sum(t.timesheet_document_pages)
      filter(where t.required),0)::integer timesheet_pages,
    coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,
      'required',t.required,
      'submission_mode',coalesce(t.submission_mode::text,''),
      'manual_asset_state',t.manual_asset_state,
      'manual_asset_pages',t.manual_asset_pages,
      'timesheet_document_state',t.timesheet_document_state,
      'timesheet_document_pages',t.timesheet_document_pages,
      'active_timesheet_document_operation_id',
        t.active_timesheet_document_operation_id)
      order by t.timesheet_id)
      filter(where t.timesheet_id is not null),'[]'::jsonb)
      timesheet_support_rows
  from base b
  left join timesheet_support t on t.invoice_id=b.id
  group by b.id
),
evidence_economics as materialized (
  select l.invoice_id,l.timesheet_id,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) in(
        'EXPENSE_MILEAGE','MILEAGE')
      or coalesce(l.source_key,'') like '%:MILEAGE') mileage_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%TRAVEL%') travel_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%ACCOMMODATION%') accommodation_required,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) like 'EXPENSE_%'
      and upper(coalesce(l.meta_json->>'line_type','')) not in(
        'EXPENSE_MILEAGE','EXPENSE_TRAVEL','EXPENSE_ACCOMMODATION'))
      general_expense_required
  from public.invoice_lines l
  join base b on b.id=l.invoice_id
  where l.timesheet_id is not null
  group by l.invoice_id,l.timesheet_id
),
evidence_rows as materialized (
  select distinct s.invoice_id,e.id evidence_id,e.timesheet_id,
    upper(coalesce(e.kind,'')) kind,e.document_asset_id,a.status,
    coalesce(a.normalised_page_count,0) pages,
    case
      when upper(coalesce(e.kind,''))='TIMESHEET'
        then coalesce(pc.effective_ts_attach_to_invoice,true)
          and not coalesce(summary.client_no_timesheet_required,false)
          and not coalesce(summary.client_is_nhsp,false)
      when upper(coalesce(e.kind,''))='MILEAGE'
        then coalesce(econ.mileage_required,false)
      when upper(coalesce(e.kind,''))='TRAVEL'
        then coalesce(econ.travel_required,false)
      when upper(coalesce(e.kind,''))='ACCOMMODATION'
        then coalesce(econ.accommodation_required,false)
      when upper(coalesce(e.kind,'')) in('OTHER','EXPENSE','EXPENSES')
        then coalesce(econ.general_expense_required,false)
      else false
    end required
  from source_timesheets s
  join public.timesheet_evidence e on e.timesheet_id=s.timesheet_id
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=s.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join evidence_economics econ
    on econ.invoice_id=s.invoice_id and econ.timesheet_id=s.timesheet_id
  left join public.invoice_document_assets a on a.id=e.document_asset_id
),
evidence_agg as materialized (
  select b.id invoice_id,
    count(e.evidence_id) filter(where e.required)::integer evidence_count,
    count(*) filter(where e.required and e.evidence_id is not null
      and e.document_asset_id is null)::integer unregistered_count,
    count(*) filter(where e.required and e.evidence_id is not null
      and e.document_asset_id is not null
      and coalesce(e.status,'DISCOVERED') not in(
        'READY','UNSUPPORTED','CORRUPT','MISSING','FAILED'))::integer not_ready_count,
    count(*) filter(where e.required
      and e.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED'))::integer failed_count,
    coalesce(sum(e.pages) filter(where e.required),0)::integer evidence_pages
  from base b left join evidence_rows e on e.invoice_id=b.id
  group by b.id
),
hr_support as materialized (
  select b.id invoice_id,
    count(h.source_system) filter(
      where upper(coalesce(h.source_system,''))='HEALTHROSTER')::integer
      healthroster_count,
    count(h.source_system) filter(
      where upper(coalesce(h.source_system,''))='NHSP')::integer nhsp_count
  from base b
  left join public.invoice_hr_source_rows h on h.invoice_id=b.id
  group by b.id
),
line_flags as materialized (
  select b.id invoice_id,count(l.id)::integer line_count,
    coalesce(bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%HIGHER_RATE%'),false) higher_rate_required
  from base b left join public.invoice_lines l on l.invoice_id=b.id
  group by b.id
),
active_issue as materialized (
  select distinct on(c.entity_id) c.entity_id invoice_id,c.operation_id,
    c.id chunk_id,c.status,c.phase,c.progress_json,c.error_json,o.change_seq
  from public.invoice_operation_chunks c
  join public.invoice_operations o on o.id=c.operation_id
  join base b on b.id=c.entity_id
  where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
  order by c.entity_id,c.updated_at_utc desc,c.id desc
),
evaluated as materialized (
  select b.*,b.invoice_week_start+6 week_ending_date,
    coalesce(v.hard_blocker_codes,'[]'::jsonb) blocker_codes,
    coalesce(v.warning_codes,'[]'::jsonb) routing_warnings,
    coalesce(v.document_dependency_codes,'[]'::jsonb)
      document_dependency_codes,
    coalesce(v.delivery_blocker_codes,'[]'::jsonb)
      delivery_blocker_codes,
    coalesce(v.can_issue_only,false) can_issue_only,
    coalesce(v.can_issue_and_deliver,false) can_issue_and_deliver,
    v.detail_json validation_detail,
    v.route_policy_result->'canonical_to' recipient,
    ts.*,ev.*,hr.*,lf.*,
    ai.operation_id active_issue_operation_id_resolved,
    ai.chunk_id active_issue_chunk_id,ai.status active_issue_status,
    ai.phase active_issue_phase,ai.progress_json active_issue_progress,
    ai.error_json active_issue_error,ai.change_seq active_issue_change_seq
  from base b
  left join validations v
    on v.request_key='candidate:'||b.id::text and v.invoice_id=b.id
  join timesheet_support_agg ts on ts.invoice_id=b.id
  join evidence_agg ev on ev.invoice_id=b.id
  join hr_support hr on hr.invoice_id=b.id
  join line_flags lf on lf.invoice_id=b.id
  left join active_issue ai on ai.invoice_id=b.id
),
weeks as materialized (
  select e.client_id,max(e.client_name) client_name,e.invoice_week_start,
    e.week_ending_date,round(sum(e.subtotal_ex_vat),2) subtotal_ex_vat_sum,
    round(sum(e.total_inc_vat),2) total_inc_vat_sum,
    jsonb_agg(jsonb_build_object(
      'invoice_id',e.id,'invoice_no',e.invoice_no,'status',e.status,
      'on_hold_reason',e.on_hold_reason,
      'subtotal_ex_vat',round(e.subtotal_ex_vat,2),
      'vat_amount',round(e.vat_amount,2),
      'total_inc_vat',round(e.total_inc_vat,2),
      'is_self_bill',e.is_self_bill,'do_not_send',e.do_not_send,
      'document_revision',e.document_revision,
      'preview_document_state',e.document_state,
      'stable_blocker_codes',e.blocker_codes,
      'document_dependency_codes',e.document_dependency_codes,
      'delivery_blocker_codes',e.delivery_blocker_codes,
      'can_issue_only',e.can_issue_only,
      'can_issue_and_deliver',e.can_issue_and_deliver,
      'validation_detail',e.validation_detail,
      'estimated_supporting_page_count',
        e.evidence_pages+e.timesheet_pages+e.healthroster_count+e.nhsp_count,
      'support_readiness',jsonb_build_object(
        'manual_timesheet_count',e.manual_count,
        'electronic_timesheet_count',e.electronic_count,
        'timesheet_not_ready_count',e.timesheet_not_ready_count,
        'timesheets',e.timesheet_support_rows,
        'evidence_count',e.evidence_count,
        'unregistered_asset_count',e.unregistered_count,
        'not_ready_asset_count',e.not_ready_count,
        'failed_asset_count',e.failed_count,
        'healthroster_count',e.healthroster_count,
        'nhsp_count',e.nhsp_count,
        'higher_rate_required',e.higher_rate_required),
      'recipient_ready',not exists(
        select 1 from jsonb_array_elements_text(
          e.delivery_blocker_codes) code(value)
          where code.value in('MISSING_RECIPIENT','CONTRACT_MANUAL_EMAIL_MISSING',
            'CLIENT_MANUAL_EMAIL_MISSING','CONTRACT_MANUAL_EMAIL_CONFLICT',
            'INVALID_TO_RECIPIENT','INVALID_CC_RECIPIENT',
            'INVALID_BCC_RECIPIENT')),
      'recipient',e.recipient,
      'recipient_routing_warnings',e.routing_warnings,
      'active_issue_operation_id',e.active_issue_operation_id_resolved,
      'active_issue_operation',case when e.active_issue_operation_id_resolved is not null
        then jsonb_build_object(
          'id',e.active_issue_operation_id_resolved,
          'chunk_id',e.active_issue_chunk_id,'status',e.active_issue_status,
          'phase',e.active_issue_phase,'progress',e.active_issue_progress,
          'error',e.active_issue_error,'change_seq',e.active_issue_change_seq) end,
      'active_document_operation_id',e.active_document_operation_id,
      'last_issue_error',e.active_issue_error,
      'last_document_error',e.last_document_error_json)
      order by e.status desc,e.invoice_no nulls last,e.id) invoices
  from evaluated e
  group by e.client_id,e.invoice_week_start,e.week_ending_date
),
clients as (
  select w.client_id,max(w.client_name) client_name,
    jsonb_agg(jsonb_build_object(
      'invoice_week_start',w.invoice_week_start,
      'week_ending_date',w.week_ending_date,
      'subtotal_ex_vat_sum',w.subtotal_ex_vat_sum,
      'total_inc_vat_sum',w.total_inc_vat_sum,
      'invoices',w.invoices)
      order by w.week_ending_date desc nulls last) weeks
  from weeks w group by w.client_id
)
select coalesce(jsonb_agg(jsonb_build_object(
  'client_id',c.client_id,'client_name',c.client_name,'weeks',c.weeks)
  order by c.client_name nulls last,c.client_id),'[]'::jsonb)
from clients c;
$function$
;

-- SOURCE private._invoice_delivery_routes_batch(p_requests jsonb, p_evaluation_date date)
CREATE OR REPLACE FUNCTION private._invoice_delivery_routes_batch(p_requests jsonb, p_evaluation_date date)
 RETURNS TABLE(request_key text, invoice_id uuid, canonical_to jsonb, canonical_cc jsonb, canonical_bcc jsonb, invalid_to_count integer, invalid_cc_count integer, invalid_bcc_count integer, recipient_set_hash text, route_policy_hash text, route_source text, client_settings_id uuid, contract_settings_ids jsonb, effective_date date, client_id uuid, invoice_group_identity text, self_bill boolean, do_not_send boolean, delivery_suppressed boolean, suppression_reason text, warning_codes jsonb, warning_details jsonb, blocker_codes jsonb, blocker_details jsonb, grouping_identity text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with raw as materialized (
  select x.ordinality::integer request_no,x.value request_json,
    nullif(btrim(coalesce(x.value->>'request_key','')),'') request_key,
    case when coalesce(x.value->>'invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x.value->>'invoice_id')::uuid end invoice_id,
    case when jsonb_typeof(x.value->'recipient_set')='array'
      then x.value->'recipient_set' else '[]'::jsonb end requested_to,
    case when jsonb_typeof(x.value->'cc')='array'
      then x.value->'cc' else '[]'::jsonb end requested_cc,
    case when jsonb_typeof(x.value->'bcc')='array'
      then x.value->'bcc' else '[]'::jsonb end requested_bcc,
    upper(coalesce(nullif(btrim(x.value->>'delivery_policy'),''),
      'ATTACH')) delivery_policy,
    coalesce(nullif(btrim(x.value->>'template_version'),''),
      'invoice-delivery-v1') template_version
  from jsonb_array_elements(case when jsonb_typeof(p_requests)='array'
    then p_requests else '[]'::jsonb end)
    with ordinality x(value,ordinality)
  where jsonb_typeof(x.value)='object'
),
request_counts as materialized (
  select r.request_key,count(*)::integer request_key_count
  from raw r
  where r.request_key is not null
  group by r.request_key
),
facts as materialized (
  select r.*,coalesce(rc.request_key_count,0) request_key_count,
    i.client_id,
    coalesce(nullif(i.header_snapshot_json#>>'{meta,invoice_week_start}',''),
      nullif(i.header_snapshot_json->>'invoice_week_start',''),'NO_WEEK')
      invoice_week_identity,
    coalesce(i.do_not_send,false) invoice_do_not_send,
    lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
      i.header_snapshot_json->>'self_bill','false'))
      in('true','t','1','yes') invoice_self_bill,
    nullif(btrim(coalesce(i.header_snapshot_json->>
      'client_primary_invoice_email',cl.primary_invoice_email,'')),'')
      primary_email,
    cs.id client_settings_id,cs.effective_from client_settings_effective_from,
    cs.send_manual_invoices_to_different_email client_alt_enabled,
    nullif(btrim(cs.manual_invoices_alt_email_address),'') client_alt_email,
    cs.self_bill_no_invoices_sent
  from raw r
  left join request_counts rc on rc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join public.clients cl on cl.id=i.client_id
  left join lateral (
    select s.id,s.effective_from,s.send_manual_invoices_to_different_email,
      s.manual_invoices_alt_email_address,s.self_bill_no_invoices_sent
    from public.client_settings s
    where p_evaluation_date is not null
      and s.client_id=i.client_id
      and(s.effective_from is null or s.effective_from<=p_evaluation_date)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
line_routes as materialized (
  select f.request_no,f.request_key,f.invoice_id,l.timesheet_id,
    l.timesheet_id is not null and ts.timesheet_id is null
      missing_current_timesheet,
    (
      (coalesce(ts.is_adjustment,false) or coalesce(cw.is_adjustment,false))
      and(
        upper(coalesce(ts.submission_mode::text,'')) in('MANUAL','QR')
        or nullif(btrim(coalesce(ts.qr_status::text,'')),'') is not null
        or nullif(btrim(coalesce(ts.qr_token::text,'')),'') is not null)
      and not(
        coalesce(ts.is_adjustment,false)
        and(
          left(upper(coalesce(ts.adjustment_origin::text,'')),7)='IMPORT_'
          or ts.correction_id is not null
          or nullif(btrim(coalesce(ts.correction_kind::text,'')),'') is not null))
    ) manual_adjustment,
    coalesce(ts.contract_id,cw.contract_id) contract_id
  from facts f
  join public.invoice_lines l on l.invoice_id=f.invoice_id
  left join public.timesheets ts
    on ts.timesheet_id=l.timesheet_id and ts.is_current
  left join lateral (
    select coalesce(bool_or(coalesce(w.is_adjustment,false)),false)
        is_adjustment,
      (array_agg(w.contract_id order by w.updated_at desc nulls last,
        w.created_at desc nulls last,w.id desc)
        filter(where w.contract_id is not null))[1] contract_id
    from public.contract_weeks w
    where w.timesheet_id=l.timesheet_id
  ) cw on true
  where l.timesheet_id is not null
),
contract_routes as materialized (
  select lr.*,ct.id is null contract_missing,
    coalesce(ct.overrideclientsettings,false)
      and coalesce(ct.send_manual_invoices_to_different_email,false)
      override_enabled,
    nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'')
      alt_email,
    lower(nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'') )
      contract_alt_policy_email
  from line_routes lr
  left join public.contracts ct on ct.id=lr.contract_id
),
route_rollup as materialized (
  select f.request_no,f.request_key,f.invoice_id,
    coalesce(bool_or(cr.missing_current_timesheet),false)
      missing_current_timesheet,
    coalesce(bool_or(cr.manual_adjustment),false) has_manual_adjustment,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_id is null),false)
      missing_contract,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_missing),false)
      contract_data_missing,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled),false)
      has_contract_override,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled
      and cr.alt_email is null),false) contract_alt_missing,
    count(distinct lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_count,
    min(lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_email,
    coalesce(jsonb_agg(distinct jsonb_build_object(
      'contract_id',cr.contract_id,
      'override_client_settings',cr.override_enabled,
      'manual_invoice_alternate_email',cr.contract_alt_policy_email,
      'contract_missing',cr.contract_missing)
      order by jsonb_build_object(
        'contract_id',cr.contract_id,
        'override_client_settings',cr.override_enabled,
        'manual_invoice_alternate_email',cr.contract_alt_policy_email,
        'contract_missing',cr.contract_missing))
      filter(where cr.manual_adjustment and cr.contract_id is not null),
      '[]'::jsonb) contract_setting_identities
  from facts f
  left join contract_routes cr
    on cr.request_no=f.request_no and cr.invoice_id=f.invoice_id
  group by f.request_no,f.request_key,f.invoice_id
),
chosen as materialized (
  select f.*,rr.missing_current_timesheet,rr.has_manual_adjustment,
    rr.missing_contract,rr.contract_data_missing,rr.has_contract_override,
    rr.contract_alt_missing,rr.contract_alt_count,rr.contract_alt_email,
    rr.contract_setting_identities,
    case
      when jsonb_array_length(f.requested_to)>0 then 'REQUESTED'
      when rr.contract_alt_count=1 then 'CONTRACT_MANUAL_ALTERNATE'
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then 'CLIENT_MANUAL_ALTERNATE'
      else 'CLIENT_PRIMARY'
    end route_source,
    case
      when jsonb_array_length(f.requested_to)>0 then f.requested_to
      when rr.contract_alt_count=1 then jsonb_build_array(rr.contract_alt_email)
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then jsonb_build_array(f.client_alt_email)
      else jsonb_build_array(f.primary_email)
    end selected_to,
    f.invoice_do_not_send
      or(f.invoice_self_bill and coalesce(f.self_bill_no_invoices_sent,true))
      delivery_suppressed,
    case
      when f.invoice_do_not_send then 'DO_NOT_SEND'
      when f.invoice_self_bill and coalesce(f.self_bill_no_invoices_sent,true)
        then 'SELF_BILL_SUPPRESSED'
    end suppression_reason
  from facts f
  left join route_rollup rr on rr.request_no=f.request_no
),
canonical as materialized (
  select c.*,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) to_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) cc_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) bcc_json,
    (select count(*)::integer
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_to,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_cc,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_bcc
  from chosen c
),
classified as materialized (
  select c.*,
    array_remove(array[
      case when c.missing_current_timesheet then
        'EMAIL_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.missing_contract then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_data_missing then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_alt_missing then
        'CONTRACT_MANUAL_EMAIL_MISSING' end,
      case when c.has_manual_adjustment and c.contract_alt_count>1 then
        'CONTRACT_MANUAL_EMAIL_CONFLICT' end,
      case when c.has_manual_adjustment and not c.has_contract_override
        and coalesce(c.client_alt_enabled,false) and c.client_alt_email is null
        then 'CLIENT_MANUAL_EMAIL_MISSING' end
    ],null)::text[] warnings,
    array_remove(array[
      case when c.request_key is null then 'REQUEST_KEY_REQUIRED' end,
      case when c.request_key_count>1 then 'REQUEST_KEY_DUPLICATE' end,
      case when c.invoice_id is null then 'INVOICE_ID_INVALID' end,
      case when c.client_id is null and c.invoice_id is not null
        then 'INVOICE_NOT_FOUND' end,
      case when p_evaluation_date is null then 'EVALUATION_DATE_REQUIRED' end,
      case when c.delivery_policy not in('ATTACH','SPLIT','SECURE_LINK')
        then 'DELIVERY_POLICY_INVALID' end,
      case when not c.delivery_suppressed and jsonb_array_length(c.to_json)=0
        then 'MISSING_RECIPIENT' end,
      case when c.invalid_to>0 then 'INVALID_TO_RECIPIENT' end,
      case when c.invalid_cc>0 then 'INVALID_CC_RECIPIENT' end,
      case when c.invalid_bcc>0 then 'INVALID_BCC_RECIPIENT' end
    ],null)::text[] blockers
  from canonical c
),
hashed as materialized (
  select c.*,
    encode(digest(jsonb_build_object(
      'to',case when c.delivery_suppressed then '[]'::jsonb else c.to_json end,
      'cc',case when c.delivery_suppressed then '[]'::jsonb else c.cc_json end,
      'bcc',case when c.delivery_suppressed then '[]'::jsonb else c.bcc_json end
    )::text,'sha256'),'hex') calculated_recipient_set_hash
  from classified c
),
policy_hashed as materialized (
  select h.*,
    encode(digest(jsonb_build_object(
      'policy_version','INVOICE_DELIVERY_ROUTE_V5',
      'client_id',h.client_id,
      'invoice_week_identity',h.invoice_week_identity,
      'recipient_set_hash',h.calculated_recipient_set_hash,
      'to',case when h.delivery_suppressed then '[]'::jsonb else h.to_json end,
      'cc',case when h.delivery_suppressed then '[]'::jsonb else h.cc_json end,
      'bcc',case when h.delivery_suppressed then '[]'::jsonb else h.bcc_json end,
      'route_source',h.route_source,
      'client_settings_id',h.client_settings_id,
      'client_settings_effective_from',h.client_settings_effective_from,
      'contract_settings',h.contract_setting_identities,
      'self_bill',h.invoice_self_bill,
      'do_not_send',h.invoice_do_not_send,
      'delivery_suppressed',h.delivery_suppressed,
      'suppression_reason',h.suppression_reason,
      'warnings',to_jsonb(h.warnings),'blockers',to_jsonb(h.blockers),
      'template_version',h.template_version,
      'delivery_policy',h.delivery_policy
    )::text,'sha256'),'hex') calculated_route_policy_hash
  from hashed h
)
select p.request_key,p.invoice_id,
  case when p.delivery_suppressed then '[]'::jsonb else p.to_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.cc_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.bcc_json end,
  p.invalid_to,p.invalid_cc,p.invalid_bcc,p.calculated_recipient_set_hash,
  p.calculated_route_policy_hash,p.route_source,p.client_settings_id,
  p.contract_setting_identities,p_evaluation_date,p.client_id,
  p.invoice_week_identity,p.invoice_self_bill,p.invoice_do_not_send,
  p.delivery_suppressed,p.suppression_reason,to_jsonb(p.warnings),
  jsonb_build_object(
    'missing_current_timesheet',p.missing_current_timesheet,
    'manual_adjustment',p.has_manual_adjustment,
    'contract_override_count',p.contract_alt_count),
  to_jsonb(p.blockers),
  jsonb_build_object(
    'invalid_to_count',p.invalid_to,'invalid_cc_count',p.invalid_cc,
    'invalid_bcc_count',p.invalid_bcc),
  p.calculated_route_policy_hash
from policy_hashed p
order by p.request_key nulls first,p.invoice_id nulls first,p.request_no;
$function$
;

-- SOURCE private._invoice_generation_advance_core_v8(p_claims jsonb, p_now_utc timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_generation_advance_core_v8(p_claims jsonb, p_now_utc timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_result jsonb := '[]'::jsonb;
  v_part jsonb;
begin
  -- VALIDATE_SOURCES: no invoice/header/line writes are permitted in this block.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='VALIDATE_SOURCES'
  ),
  member_values as materialized (
    select c.id chunk_id,c.operation_id,c.payload_json,
      m.value member,m.ordinality
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
          and jsonb_array_length(c.payload_json->'canonical_source_members')>0
        then c.payload_json->'canonical_source_members'
        else coalesce((
          select jsonb_agg(jsonb_build_object(
            'source_type','TIMESHEET','source_id',s.value,
            'related_timesheet_id',s.value) order by s.ordinality)
          from jsonb_array_elements_text(coalesce(
            c.payload_json->'canonical_source_ids',
            c.payload_json->'source_ids','[]'::jsonb))
            with ordinality s(value,ordinality)
        ),'[]'::jsonb) end) with ordinality m(value,ordinality)
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  members as materialized (
    select m.chunk_id,m.operation_id,m.payload_json,m.member,
      upper(coalesce(m.member->>'source_type','TIMESHEET')) source_type,
      case when coalesce(m.member->>'source_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(m.member->>'source_id')::uuid end source_id,
      case when coalesce(m.member->>'related_timesheet_id',
          m.member->>'source_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then coalesce(m.member->>'related_timesheet_id',
          m.member->>'source_id')::uuid end timesheet_id,
      nullif(btrim(m.member->>'segment_id'),'') segment_id,
      coalesce(nullif(btrim(m.member->>'source_member_key'),''),
        encode(digest(concat_ws('|',
          upper(coalesce(m.member->>'source_type','TIMESHEET')),
          coalesce(m.member->>'source_id',''),
          coalesce(m.member->>'related_timesheet_id',''),
          coalesce(m.member->>'segment_id','WHOLE'),
          coalesce(m.member->>'target_invoice_week','')),'sha256'),'hex'))
        source_member_key,
      nullif(btrim(coalesce(m.member->>'row_revision',
        m.member->>'source_revision','')),'') expected_revision,
      m.ordinality
    from member_values m
  ),
  reference_eval as materialized (
    select distinct on (r.source_member_key) r.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,'source_id',m.source_id,
        'related_timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'target_invoice_week',m.member->>'target_invoice_week',
        'invoice_stream',m.payload_json->>'invoice_stream',
        'consolidation_mode',m.payload_json->>'consolidation_mode',
        'source_revision',m.expected_revision)
        order by m.chunk_id,m.ordinality)
      from members m
    ),'[]'::jsonb)) r
    order by r.source_member_key
  ),
  vat_eval as materialized (
    select distinct on (v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,'source_id',m.source_id,
        'timesheet_id',m.timesheet_id,'segment_id',m.segment_id,
        'effective_date',coalesce(
          case when m.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                m.payload_json->>'effective_settings_date','date')
            then m.payload_json->>'effective_settings_date' end,
          (select ts_vat.week_ending_date::text
             from public.timesheets ts_vat
            where ts_vat.timesheet_id=m.timesheet_id
              and ts_vat.is_current
            limit 1)))
        order by m.chunk_id,m.ordinality)
      from members m
    ),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-validate:'||scope.chunk_id::text,
      'scope_key',scope.chunk_id::text,
      'validation_purpose','GENERATION_VALIDATE',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.chunk_id),'[]'::jsonb) scopes
    from(
      select m.chunk_id,min(m.payload_json::text)::jsonb payload_json,
        jsonb_agg(jsonb_build_object(
          'timesheet_id',m.timesheet_id,
          'source_type',m.source_type,
          'source_id',m.source_id,
          'source_member_key',m.source_member_key,
          'segment_id',m.segment_id,
          'target_invoice_week',m.payload_json->>'target_invoice_week',
          'vat_rate_pct',v.vat_rate)
          order by m.ordinality) planned_members
      from members m
      left join vat_eval v on v.source_member_key=m.source_member_key
      group by m.chunk_id
    ) scope
  ),
  correction_eval as materialized (
    select r.*
    from correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),  source_eval as materialized (
    select m.chunk_id,m.operation_id,m.timesheet_id,m.segment_id,
      m.source_member_key,m.ordinality,
      tf.id tsfin_id,
      encode(digest(concat_ws('|',tf.id::text,tf.timesheet_version::text,
        tf.updated_at::text,ts.version::text,ts.updated_at::text,
        coalesce(tf.invoice_breakdown_json::text,'')),'sha256'),'hex') row_revision,
      array_remove(array[
        case when ts.timesheet_id is null then 'TIMESHEET_NOT_FOUND' end,
        case when ts.timesheet_id is not null and (not ts.is_current or ts.revoked_at is not null) then 'TIMESHEET_NOT_CURRENT' end,
        case when tf.id is null then 'CURRENT_FINANCIALS_MISSING' end,
        case when tf.id is not null and tf.is_stale then 'FINANCIALS_STALE' end,
        case when tf.id is not null and tf.processing_status::text<>'READY_FOR_INVOICE' then 'NOT_READY_FOR_INVOICE' end,
        case when tf.id is not null and tf.has_rate_issue then 'RATE_MISSING' end,
        case when tf.id is not null and tf.has_pay_channel_issue then 'PAY_CHANNEL_MISSING' end,
        case when tf.id is not null and tf.client_id is null then 'CLIENT_UNRESOLVED' end,
        case when tf.id is not null and tf.locked_by_invoice_id is not null then 'SOURCE_ALREADY_LOCKED' end,
        case when ts.authorised_at_server is null
          then 'TIMESHEET_NOT_AUTHORISED' end,
        case when upper(coalesce(ts.submission_mode::text,''))='QR'
          and(nullif(ts.qr_signed_hash,'') is null
            or ts.qr_signed_at_utc is null)
          then 'QR_TIMESHEET_UNSIGNED' end,
        case when pc.require_reference_to_invoice
          and coalesce(ref.reference_ready,false) is not true
          then coalesce(ref.blocker_code,'MISSING_REFERENCE') end,
        case when coalesce(tf.mileage_pay_ex_vat,0)<>0
            or coalesce(tf.mileage_charge_ex_vat,0)<>0
          then case when not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null)
            then 'MISSING_MILEAGE_EVIDENCE' end end,
        case when(coalesce(tf.mileage_pay_ex_vat,0)<>0
            or coalesce(tf.mileage_charge_ex_vat,0)<>0)
          and exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null
              and e.document_asset_id is null)
          then 'MILEAGE_ASSET_NOT_REGISTERED' end,
        case when coalesce(tf.expenses_pay_ex_vat,0)<>0
            or coalesce(tf.expenses_charge_ex_vat,0)<>0
            or coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0
            or coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0
          then case when not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null)
            then 'MISSING_EXPENSE_EVIDENCE' end end,
        case when(
            coalesce(tf.expenses_pay_ex_vat,0)<>0
            or coalesce(tf.expenses_charge_ex_vat,0)<>0
            or coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0
            or coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null
              and e.document_asset_id is null)
          then 'EXPENSE_ASSET_NOT_REGISTERED' end,
        case when(coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='TRAVEL'
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_TRAVEL_EVIDENCE' end,
        case when(coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='ACCOMMODATION'
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_ACCOMMODATION_EVIDENCE' end,
        case when(coalesce(tf.other_pay_ex_vat,0)<>0
            or coalesce(tf.other_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_OTHER_EXPENSE_EVIDENCE' end,
        case when exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
            where e.timesheet_id=m.timesheet_id
              and a.status in(
                'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
          then 'REQUIRED_ASSET_PERMANENT_FAILURE' end,
        case when coalesce(vat.valid,false) is not true
          then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED') end,
        case when corr.valid is false
          then coalesce(corr.blocker_code,'CORRECTION_VALIDATION_FAILED') end,
        case when coalesce(vs.hr_validation_required_for_invoice,false)
          and upper(coalesce(tf.hr_crosscheck_status,'')) not in(
            'OK','PASS','PASSED','MATCHED','NOT_REQUIRED')
          then 'HEALTHROSTER_VALIDATION_REQUIRED' end,
        case when coalesce(vs.client_is_nhsp,false)
          and coalesce(vs.nhsp_shift_count,0)>0
          and not exists(
            select 1
            from public.nhsp_shifts ns_ready
            where ns_ready.timesheet_id=m.timesheet_id
              and ns_ready.invoice_status='PENDING'
              and ns_ready.invoice_id is null
              and ns_ready.cancelled_at_utc is null)
          then 'NHSP_SOURCE_NOT_READY' end,
        case when exists(select 1 from public.invoice_lines l join public.invoices i on i.id=l.invoice_id
                         where l.timesheet_id=m.timesheet_id and i.status in ('DRAFT','ISSUED','ON_HOLD'))
             and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS' then 'SOURCE_ALREADY_INVOICED' end
      ],null)::text[] blockers
    from members m
    left join public.timesheets ts on ts.timesheet_id=m.timesheet_id
    left join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=m.timesheet_id
    left join public.v_timesheets_summary_base vs
      on vs.timesheet_id=m.timesheet_id
    left join reference_eval ref on ref.source_member_key=m.source_member_key
    left join vat_eval vat on vat.source_member_key=m.source_member_key
    left join correction_eval corr
      on corr.scope_key=m.chunk_id::text
  ),
  resolver_inputs as materialized (
    select c.id chunk_id,c.payload_json,
      row_number() over(order by c.id)::integer command_no
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  canonical_now as materialized (
    select ri.chunk_id,r.group_key,r.source_revision_hash,
      r.blocker_code,r.blocker_detail
    from private._invoice_generation_resolve_command_groups(
      (select coalesce(jsonb_agg(ri.payload_json order by ri.command_no),
        '[]'::jsonb) from resolver_inputs ri),null,v_now) r
    join resolver_inputs ri on ri.command_no=r.command_no
    where r.group_key=case
      when left(coalesce(ri.payload_json->>'selection_key',''),9)='generate:'
        then substr(ri.payload_json->>'selection_key',10)
      else ri.payload_json->>'group_key' end
  ),
  per_chunk as materialized (
    select c.id chunk_id,
      n.source_revision_hash current_revision,
      coalesce(jsonb_agg(jsonb_build_object('source_id',se.timesheet_id,
        'segment_id',se.segment_id,
        'codes',to_jsonb(se.blockers)) order by se.ordinality)
        filter(where cardinality(se.blockers)>0),'[]'::jsonb)
        ||case when n.blocker_code is null then '[]'::jsonb
          else jsonb_build_array(coalesce(n.blocker_detail,
            jsonb_build_object('code',n.blocker_code))) end blockers,
      count(se.timesheet_id)::integer source_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join source_eval se on se.chunk_id=c.id
    left join canonical_now n on n.chunk_id=c.id
    group by c.id,n.source_revision_hash,n.blocker_code,n.blocker_detail
  ),
  updated as (
    update public.invoice_operation_chunks c
    set phase=case
          when p.source_count=0 then 'BLOCKED'
          when jsonb_array_length(p.blockers)>0 then 'BLOCKED'
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then 'SUPERSEDED'
          else 'PLAN' end,
        status=case
          when p.source_count=0 or jsonb_array_length(p.blockers)>0 then 'BLOCKED'
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then 'SUPERSEDED'
          else 'QUEUED' end,
        payload_json=c.payload_json||jsonb_build_object('source_revision',p.current_revision,'source_count',p.source_count),
        progress_json=jsonb_build_object('status_message',
          case when p.source_count=0 then 'No sources resolved'
               when jsonb_array_length(p.blockers)>0 then 'Source validation blocked'
               when nullif(c.payload_json->>'source_revision','') is not null
                and c.payload_json->>'source_revision'<>p.current_revision then 'Source changed'
               else 'Sources validated' end,'source_count',p.source_count),
        error_json=case
          when p.source_count=0 then jsonb_build_object('code','NO_SOURCES','sources','[]'::jsonb)
          when jsonb_array_length(p.blockers)>0 then jsonb_build_object('code','SOURCE_VALIDATION_BLOCKED','sources',p.blockers)
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then jsonb_build_object('code','SOURCE_CHANGED')
          else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from per_chunk p where c.id=p.chunk_id
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]'::jsonb)
  into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Credit-note validation is deliberately separate from normal timesheet sources.
  with claim_ids as (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='VALIDATE_SOURCES'
  ),
  credit_eval as (
    select c.id chunk_id,i.id invoice_id,
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),'sha256'),'hex') revision,
      case when i.id is null then 'INVOICE_NOT_FOUND'
           when i.type::text<>'INVOICE' then 'CREDIT_SOURCE_NOT_INVOICE'
           when i.status::text not in ('ISSUED','PAID') then 'CREDIT_SOURCE_NOT_ISSUED'
           when exists(select 1 from public.invoices cn where cn.original_invoice_id=i.id and cn.type='CREDIT_NOTE'
                       and cn.status in('DRAFT','ISSUED','PAID')) then 'CREDIT_ALREADY_EXISTS'
           else null end blocker
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join public.invoices i on i.id=c.entity_id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
  ),
  updated as (
    update public.invoice_operation_chunks c
    set phase=case when e.blocker is null then 'PLAN' else 'BLOCKED' end,
        status=case when e.blocker is null then 'QUEUED' else 'BLOCKED' end,
        payload_json=c.payload_json||jsonb_build_object('source_revision',e.revision,'source_invoice_id',e.invoice_id),
        progress_json=jsonb_build_object('status_message',case when e.blocker is null then 'Credit source validated' else 'Credit source blocked' end),
        error_json=case when e.blocker is null then null else jsonb_build_object('code',e.blocker,'invoice_id',c.entity_id) end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from credit_eval e where c.id=e.chunk_id
    returning c.id,c.status,c.phase,c.error_json
  )
  select v_result||coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]'::jsonb)
  into v_result from updated;

  -- PLAN: compact financial/settings summary only; no complete line arrays.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='PLAN'
  ),
  members as materialized (
    select c.id chunk_id,c.payload_json,
      case when pg_input_is_valid(s.value,'uuid')
        then s.value::uuid end timesheet_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    cross join lateral jsonb_array_elements_text(coalesce(
      c.payload_json->'canonical_source_ids',c.payload_json->'source_ids','[]'::jsonb)) s(value)
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
      and pg_input_is_valid(s.value,'uuid')
  ),
  values_by_chunk as materialized (
    select m.chunk_id,count(*)::integer source_count,
      round(sum(coalesce(tf.total_pay_ex_vat,0)),2) expected_pay_ex_vat,
      round(sum(coalesce(tf.total_charge_ex_vat,0)),2) expected_charge_ex_vat,
      (array_agg(tf.client_id order by tf.client_id))[1] client_id,
      jsonb_build_object(
        'client_id',(array_agg(tf.client_id order by tf.client_id))[1],'consolidation_mode',max(c.payload_json->>'consolidation_mode'),
        'stream',max(c.payload_json->>'invoice_stream'),
        'invoice_week_start',max(c.payload_json->>'target_invoice_week'),
        'client',jsonb_build_object('name',max(cl.name),'vat_chargeable',bool_and(cl.vat_chargeable),
          'payment_terms_days',max(cl.payment_terms_days),'primary_invoice_email',max(cl.primary_invoice_email)),
        'attach_policy',jsonb_build_object(
          'hr_attach_to_invoice',coalesce(bool_or(cs.hr_attach_to_invoice),bool_or(sd.hr_attach_to_invoice),true),
          'ts_attach_to_invoice',coalesce(bool_or(cs.ts_attach_to_invoice),bool_or(sd.ts_attach_to_invoice),true),
          'requires_hr',coalesce(bool_or(cs.requires_hr),false))
      ) settings_snapshot
    from members m join public.invoice_operation_chunks c on c.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.clients cl on cl.id=tf.client_id
    left join lateral (
      select s.* from public.client_settings s where s.client_id=tf.client_id
       and (s.effective_from is null or s.effective_from<=coalesce(
      case when c.payload_json->>'effective_settings_date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          and pg_input_is_valid(c.payload_json->>'effective_settings_date','date')
           then (c.payload_json->>'effective_settings_date')::date end,
         (v_now at time zone 'Europe/London')::date))
      order by s.effective_from desc nulls last,s.updated_at desc nulls last,
        s.created_at desc nulls last,s.id desc
      limit 1
    ) cs on true
    cross join public.settings_defaults sd
    where sd.id=1
    group by m.chunk_id
  ),
  plans as materialized (
    select c.id chunk_id,coalesce(
      case when pg_input_is_valid(
        nullif(c.payload_json->>'planned_invoice_id',''),'uuid')
        then(c.payload_json->>'planned_invoice_id')::uuid end,
      case when c.payload_json->>'consolidation_mode' in ('BY_WEEK','ANY_WEEK') then (
        select i.id from public.invoices i
        where i.client_id=v.client_id and i.status='DRAFT' and i.type='INVOICE'
          and coalesce(i.header_snapshot_json#>>'{meta,consolidation_mode}','NONE')=c.payload_json->>'consolidation_mode'
          and coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false')=
              case when c.payload_json->>'invoice_stream'='SELF_BILL' then 'true' else 'false' end
          and (c.payload_json->>'consolidation_mode'='ANY_WEEK'
               or i.header_snapshot_json#>>'{meta,invoice_week_start}'=c.payload_json->>'target_invoice_week')
        order by i.created_at desc limit 1
      ) end,gen_random_uuid()) planned_invoice_id,
      v.source_count,v.expected_charge_ex_vat,v.expected_pay_ex_vat,v.client_id,
      v.settings_snapshot,encode(digest(v.settings_snapshot::text,'sha256'),'hex') settings_hash
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join values_by_chunk v on v.chunk_id=c.id
  ),
  credit_plans as materialized (
    select c.id chunk_id,gen_random_uuid() planned_invoice_id,1 source_count,
      -i.subtotal_ex_vat expected_charge_ex_vat,-coalesce(sum(l.total_pay_ex_vat),0) expected_pay_ex_vat,
      i.client_id,jsonb_build_object('source_invoice_id',i.id,'source_invoice_no',i.invoice_no,
        'client_id',i.client_id,'credit_note',true) settings_snapshot,
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text),'sha256'),'hex') settings_hash
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i on i.id=c.entity_id
    left join public.invoice_lines l on l.invoice_id=i.id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
    group by c.id,i.id
  ),
  all_plans as materialized (select * from plans union all select * from credit_plans),
  updated as (
    update public.invoice_operation_chunks c
    set phase='COMMIT',status='QUEUED',
      payload_json=c.payload_json||jsonb_build_object('plan',jsonb_build_object(
        'planned_invoice_id',p.planned_invoice_id,'intended_invoice_count',1,
        'source_count',p.source_count,'expected_pay_ex_vat',p.expected_pay_ex_vat,
        'expected_charge_ex_vat',p.expected_charge_ex_vat,'settings_snapshot',p.settings_snapshot,
        'settings_hash',p.settings_hash,'source_revision',c.payload_json->>'source_revision')),
      progress_json=jsonb_build_object('status_message','Generation plan ready','source_count',p.source_count),
      error_json=null,lease_owner=null,lease_token=null,
      lease_expires_at_utc=null,updated_at_utc=v_now
    from all_plans p where c.id=p.chunk_id
    returning c.id,c.status,c.phase,c.payload_json->'plan' plan
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'plan',plan)),'[]'::jsonb)
    into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- COMMIT: revalidate immediately, then headers, lines, locks, documents and audit set-wise.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='COMMIT'
  ),
  normal_chunks as materialized (
    select c.*,case when pg_input_is_valid(
        c.payload_json#>>'{plan,planned_invoice_id}','uuid')
      then(c.payload_json#>>'{plan,planned_invoice_id}')::uuid end planned_invoice_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  members as materialized (
    select c.id chunk_id,c.operation_id,c.planned_invoice_id,c.payload_json,
      upper(coalesce(s.value->>'source_type','TIMESHEET')) source_type,
      case when pg_input_is_valid(s.value->>'source_id','uuid')
        then(s.value->>'source_id')::uuid end source_id,
      case when pg_input_is_valid(coalesce(
          s.value->>'related_timesheet_id',s.value->>'source_id'),'uuid')
        then coalesce(s.value->>'related_timesheet_id',
          s.value->>'source_id')::uuid end timesheet_id,
      nullif(btrim(s.value->>'segment_id'),'') segment_id,
      coalesce(nullif(btrim(s.value->>'source_member_key'),''),
        encode(digest(concat_ws('|',
          upper(coalesce(s.value->>'source_type','TIMESHEET')),
          coalesce(s.value->>'source_id',''),
          coalesce(s.value->>'related_timesheet_id',''),
          coalesce(s.value->>'segment_id','WHOLE'),
          coalesce(s.value->>'target_invoice_week','')),'sha256'),'hex'))
        source_member_key
    from normal_chunks c
    cross join lateral jsonb_array_elements(case
      when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
        and jsonb_array_length(c.payload_json->'canonical_source_members')>0
      then c.payload_json->'canonical_source_members'
      else coalesce((
        select jsonb_agg(jsonb_build_object(
          'source_type','TIMESHEET','source_id',ids.value,
          'related_timesheet_id',ids.value) order by ids.ordinality)
        from jsonb_array_elements_text(coalesce(
          c.payload_json->'canonical_source_ids',
          c.payload_json->'source_ids','[]'::jsonb))
          with ordinality ids(value,ordinality)
      ),'[]'::jsonb) end) s(value)
    where pg_input_is_valid(coalesce(
      s.value->>'related_timesheet_id',s.value->>'source_id'),'uuid')
  ),
  selected_segments as materialized (
    select c.id chunk_id,
      (m.value->>'source_id')::uuid timesheet_id,
      array_agg(distinct m.value->>'segment_id'
        order by m.value->>'segment_id')
        filter(where nullif(m.value->>'segment_id','') is not null) segment_ids
    from normal_chunks c
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
        then c.payload_json->'canonical_source_members' else '[]'::jsonb end)
      m(value)
    where coalesce(m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    group by c.id,(m.value->>'source_id')::uuid
  ),
  commit_reference_eval as materialized (
    select distinct on(r.source_member_key) r.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,
        'source_id',m.source_id,
        'related_timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'target_invoice_week',m.payload_json->>'target_invoice_week',
        'invoice_stream',m.payload_json->>'invoice_stream',
        'consolidation_mode',m.payload_json->>'consolidation_mode')
        order by m.chunk_id,m.source_member_key)
      from members m),'[]'::jsonb)) r
    order by r.source_member_key
  ),
  commit_vat_eval as materialized (
    select distinct on(v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,
        'source_id',m.source_id,
        'timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'effective_date',coalesce(
          case when m.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                m.payload_json->>'effective_settings_date','date')
            then m.payload_json->>'effective_settings_date' end,
          (select ts_vat.week_ending_date::text
             from public.timesheets ts_vat
            where ts_vat.timesheet_id=m.timesheet_id
              and ts_vat.is_current
            limit 1)))
        order by m.chunk_id,m.source_member_key)
      from members m),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  commit_correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-commit:'||scope.planned_invoice_id::text,
      'scope_key',scope.planned_invoice_id::text,
      'invoice_id',case when exists(select 1 from public.invoices existing
        where existing.id=scope.planned_invoice_id)
        then scope.planned_invoice_id end,
      'validation_purpose','GENERATION_COMMIT',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.planned_invoice_id),'[]'::jsonb) scopes
    from(
      select m.planned_invoice_id,min(m.payload_json::text)::jsonb payload_json,
        jsonb_agg(jsonb_build_object(
          'timesheet_id',m.timesheet_id,
          'source_type',m.source_type,
          'source_id',m.source_id,
          'source_member_key',m.source_member_key,
          'segment_id',m.segment_id,
          'target_invoice_week',m.payload_json->>'target_invoice_week',
          'vat_rate_pct',v.vat_rate)
          order by m.source_member_key) planned_members
      from members m
      left join commit_vat_eval v
        on v.source_member_key=m.source_member_key
      group by m.planned_invoice_id
    ) scope
  ),
  commit_correction_eval as materialized (
    select r.*
    from commit_correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),  source_still_valid as materialized (
    select m.chunk_id,
      bool_and(tf.processing_status='READY_FOR_INVOICE' and not tf.is_stale
        and (tf.locked_by_invoice_id is null
          or coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS')
        and ts.authorised_at_server is not null
        and not(upper(coalesce(ts.submission_mode::text,''))='QR'
          and(nullif(ts.qr_signed_hash,'') is null
            or ts.qr_signed_at_utc is null))
        and(not coalesce(pc.require_reference_to_invoice,false)
          or coalesce(ref.reference_ready,false))
        and coalesce(vat.valid,false)
        and coalesce(corr.valid,true)
        and not(coalesce(vs.hr_validation_required_for_invoice,false)
          and upper(coalesce(tf.hr_crosscheck_status,'')) not in(
            'OK','PASS','PASSED','MATCHED','NOT_REQUIRED'))
        and not(coalesce(vs.client_is_nhsp,false)
          and coalesce(vs.nhsp_shift_count,0)>0
          and not exists(
            select 1
            from public.nhsp_shifts ns_ready
            where ns_ready.timesheet_id=m.timesheet_id
              and ns_ready.invoice_status='PENDING'
              and ns_ready.invoice_id is null
              and ns_ready.cancelled_at_utc is null))
        and not((coalesce(tf.mileage_pay_ex_vat,0)<>0
              or coalesce(tf.mileage_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.travel_pay_ex_vat,0)<>0
              or coalesce(tf.travel_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='TRAVEL'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.accommodation_pay_ex_vat,0)<>0
              or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='ACCOMMODATION'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.expenses_pay_ex_vat,0)<>0
              or coalesce(tf.expenses_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.other_pay_ex_vat,0)<>0
              or coalesce(tf.other_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null))
        and not exists(
          select 1
          from public.timesheet_evidence e
          left join public.invoice_document_assets a
            on a.id=e.document_asset_id
          where e.timesheet_id=m.timesheet_id
            and(
              (upper(coalesce(e.kind,''))='MILEAGE'
                and(coalesce(tf.mileage_pay_ex_vat,0)<>0
                  or coalesce(tf.mileage_charge_ex_vat,0)<>0))
              or(upper(coalesce(e.kind,'')) in(
                  'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
                and(coalesce(tf.expenses_pay_ex_vat,0)<>0
                  or coalesce(tf.expenses_charge_ex_vat,0)<>0
                  or coalesce(tf.travel_pay_ex_vat,0)<>0
                  or coalesce(tf.travel_charge_ex_vat,0)<>0
                  or coalesce(tf.accommodation_pay_ex_vat,0)<>0
                  or coalesce(tf.accommodation_charge_ex_vat,0)<>0)))
            and(e.document_asset_id is null
              or a.status in('UNSUPPORTED','CORRUPT','MISSING',
                'FAILED','SUPERSEDED')))
        and not exists(
          select 1
          from public.invoice_lines l
          join public.invoices existing on existing.id=l.invoice_id
          where l.timesheet_id=m.timesheet_id
            and existing.id<>m.planned_invoice_id
            and existing.status in('DRAFT','ISSUED','ON_HOLD')
            and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS')
      ) still_valid
    from members m join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.timesheets ts on ts.timesheet_id=m.timesheet_id and ts.is_current and ts.revoked_at is null
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=m.timesheet_id
    left join public.v_timesheets_summary_base vs
      on vs.timesheet_id=m.timesheet_id
    left join commit_reference_eval ref
      on ref.source_member_key=m.source_member_key
    left join commit_vat_eval vat
      on vat.source_member_key=m.source_member_key
    left join commit_correction_eval corr
      on corr.scope_key=m.planned_invoice_id::text
    group by m.chunk_id
  ),
  commit_resolver_inputs as materialized (
    select c.id chunk_id,c.payload_json,
      row_number() over(order by c.id)::integer command_no
    from normal_chunks c
  ),
  commit_resolver_results as materialized (
    select ri.chunk_id,r.source_revision_hash,r.blocker_code
    from private._invoice_generation_resolve_command_groups(
      (select coalesce(jsonb_agg(ri.payload_json order by ri.command_no),
        '[]'::jsonb) from commit_resolver_inputs ri),null,v_now) r
    join commit_resolver_inputs ri on ri.command_no=r.command_no
    where r.group_key=case
      when left(coalesce(ri.payload_json->>'selection_key',''),9)='generate:'
        then substr(ri.payload_json->>'selection_key',10)
      else ri.payload_json->>'group_key' end
  ),
  revision_check as materialized (
    select c.id chunk_id,r.source_revision_hash current_revision,
      s.still_valid and r.blocker_code is null still_valid
    from normal_chunks c
    join source_still_valid s on s.chunk_id=c.id
    join commit_resolver_results r on r.chunk_id=c.id
  ),
  rejected as (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code','SOURCE_CHANGED_BEFORE_COMMIT'),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from revision_check r
    where c.id=r.chunk_id and (not r.still_valid or r.current_revision<>c.payload_json->>'source_revision')
    returning c.id
  ),
  valid_chunks as materialized (
    select c.* from normal_chunks c join revision_check r on r.chunk_id=c.id
    where r.still_valid and r.current_revision=c.payload_json->>'source_revision'
      and not exists(select 1 from rejected x where x.id=c.id)
  ),
  header_source as materialized (
    select vc.id chunk_id,vc.planned_invoice_id,(array_agg(tf.client_id order by tf.client_id))[1] client_id,
      max(cl.name) client_name,max(cl.invoice_address) client_invoice_address,
      max(cl.primary_invoice_email) primary_invoice_email,bool_and(cl.vat_chargeable) vat_chargeable,
      max(cl.payment_terms_days) payment_terms_days,
      max(sd.agency_name) agency_name,max(sd.agency_logo) agency_logo,
      max(sd.registered_address) registered_address,max(sd.company_reg_number) company_reg_number,
      max(sd.bank_name) bank_name,max(sd.bank_sort_code) bank_sort_code,
      max(sd.bank_account_number) bank_account_number,max(sd.vat_registration_number) vat_registration_number,
      count(*)::integer source_count
    from valid_chunks vc join members m on m.chunk_id=vc.id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.clients cl on cl.id=tf.client_id cross join public.settings_defaults sd
    where sd.id=1 group by vc.id,vc.planned_invoice_id
  ),
  source_rows_base as materialized (
    select m.chunk_id,m.planned_invoice_id,m.payload_json,
      m.source_member_key,m.source_type,m.source_id,m.segment_id,tf.*,ts.booking_id,
      ts.week_ending_date,ts.reference_number,ts.sheet_scope,ts.submission_mode,ts.day_references_json,
      ts.actual_schedule_json,coalesce(ts.contract_id,cw.contract_id) contract_id,
      coalesce(cd.display_name,'Candidate '||left(coalesce(tf.candidate_id::text,m.timesheet_id::text),8)) candidate_display,
      coalesce(ct.daily_calc_of_invoices,false) daily_calc_of_invoices,
      coalesce(ct.bucket_labels_json,
        jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH'))
        bucket_labels_json,
      ct.role contract_role,ct.display_site contract_display_site,
      ct.ward_hint contract_ward_hint,
      null::numeric ordinary_vat_rate,
      case when ts.sheet_scope::text='WEEKLY'
          and ts.submission_mode::text='MANUAL'
          and jsonb_typeof(ts.actual_schedule_json)='array'
        then coalesce((
          select jsonb_agg(jsonb_build_object(
            'date',e.value->>'date','start',e.value->>'start',
            'end',e.value->>'end','start_utc',e.value->>'start_utc',
            'end_utc',e.value->>'end_utc','ref_num',e.value->>'ref_num')
            order by e.ordinality)
          from jsonb_array_elements(ts.actual_schedule_json)
            with ordinality e(value,ordinality)
          where nullif(btrim(e.value->>'start'),'') is not null
            and nullif(btrim(e.value->>'end'),'') is not null
        ),'[]'::jsonb)
        else '[]'::jsonb end schedule_refs
    from members m join valid_chunks vc on vc.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.timesheets ts on ts.timesheet_id=m.timesheet_id and ts.is_current
    left join lateral (
      select w.contract_id
      from public.contract_weeks w
      where w.timesheet_id=m.timesheet_id
      order by w.updated_at desc nulls last,w.created_at desc nulls last,w.id desc
      limit 1
    ) cw on true
    left join public.contracts ct on ct.id=coalesce(ts.contract_id,cw.contract_id)
    left join public.candidates cd on cd.id=tf.candidate_id
  ),
  vat_policy as materialized (
    select distinct on (v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',s.source_member_key,
        'source_type',s.source_type,'source_id',s.source_id,
        'timesheet_id',s.timesheet_id,'segment_id',s.segment_id,
        'ordinary_rate',s.ordinary_vat_rate,
        'effective_date',coalesce(
          case when s.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                s.payload_json->>'effective_settings_date','date')
            then(s.payload_json->>'effective_settings_date')::date end,
          s.week_ending_date::date))
        order by s.chunk_id,s.timesheet_id)
      from source_rows_base s
    ),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  source_rows as materialized (
    select s.*,v.vat_rate
    from source_rows_base s
    join vat_policy v on v.source_member_key=s.source_member_key and v.valid
  ),
  source_timesheet_ancestry(
    planned_invoice_id,
    root_timesheet_id,
    source_timesheet_id,
    ancestry_path,
    ancestry_depth
  ) as materialized (
    select distinct s.planned_invoice_id,s.timesheet_id,s.timesheet_id,
      array[s.timesheet_id]::uuid[],0
    from source_rows s
    union all
    select a.planned_invoice_id,a.root_timesheet_id,t.parent_timesheet_id,
      a.ancestry_path||t.parent_timesheet_id,a.ancestry_depth+1
    from source_timesheet_ancestry a
    join public.timesheets t
      on t.timesheet_id=a.source_timesheet_id
     and t.is_current
    where t.parent_timesheet_id is not null
      and not(t.parent_timesheet_id=any(a.ancestry_path))
      and a.ancestry_depth<32
  ),
  adjustment_segment_refs as materialized (
    select distinct s.planned_invoice_id,s.timesheet_id root_timesheet_id,
      coalesce(
        (s.payload_json#>>'{plan,settings_snapshot,attach_policy,hr_attach_to_invoice}')::boolean,
        true
      ) healthroster_attach_allowed,
      nullif(btrim(seg.value->>'ref_num'),'') ref_num,
      case when pg_input_is_valid(
          seg.value->>'start_utc','timestamp with time zone')
        then(seg.value->>'start_utc')::timestamptz end start_utc,
      case when pg_input_is_valid(
          seg.value->>'end_utc','timestamp with time zone')
        then(seg.value->>'end_utc')::timestamptz end end_utc,
      lower(coalesce(seg.value->>'is_reversal','false')) in(
        'true','t','1','yes') is_reversal
    from source_rows s
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)
      seg(value)
    where s.basis::text in('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT')
  ),
  segment_lock_targets_pre as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'invoice_id',s.planned_invoice_id,
      'timesheet_id',s.timesheet_id,
      'segment_ids',to_jsonb(p.segment_ids),
      'expected_financial_revision',encode(digest(jsonb_build_object(
        'financial_id',s.id,
        'timesheet_version',s.timesheet_version,
        'updated_at',s.updated_at,
        'basis',s.basis,
        'invoice_breakdown_json',s.invoice_breakdown_json
      )::text,'sha256'),'hex'))
      order by s.planned_invoice_id,s.timesheet_id),'[]'::jsonb) targets
    from source_rows s
    join selected_segments p
      on p.chunk_id=s.chunk_id and p.timesheet_id=s.timesheet_id
    where cardinality(coalesce(p.segment_ids,array[]::text[]))>0
      and exists(
        select 1
        from public.invoices existing_header
        where existing_header.id=s.planned_invoice_id)
  ),
  segment_lock_authority_pre as materialized (
    select r.*
    from segment_lock_targets_pre t
    cross join lateral private._invoice_segment_lock_batch(t.targets,v_now) r
  ),
  segment_lock_failures_pre as materialized (
    select * from segment_lock_authority_pre where not success
  ),
  segment_entries as materialized (
    select s.*,seg.ordinality,
      coalesce(nullif(seg.value->>'segment_id',''),
        left(encode(digest(seg.value::text,'sha256'),'hex'),24)) segment_id,
      case when left(coalesce(seg.value->>'date',''),10)
        ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        then left(seg.value->>'date',10) end work_date,
      case when coalesce(seg.value->>'hours_day','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_day')::numeric
        else 0 end segment_hours_day,
      case when coalesce(seg.value->>'hours_night','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_night')::numeric
        else 0 end segment_hours_night,
      case when coalesce(seg.value->>'hours_sat','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_sat')::numeric
        else 0 end segment_hours_sat,
      case when coalesce(seg.value->>'hours_sun','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_sun')::numeric
        else 0 end segment_hours_sun,
      case when coalesce(seg.value->>'hours_bh','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_bh')::numeric
        else 0 end segment_hours_bh,
      case
        when coalesce(seg.value->>'pay_amount','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'pay_amount')::numeric
        when coalesce(seg.value->>'pay_ex_vat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'pay_ex_vat')::numeric
        else 0 end segment_pay_ex,
      case
        when coalesce(seg.value->>'charge_amount','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'charge_amount')::numeric
        when coalesce(seg.value->>'charge_ex_vat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'charge_ex_vat')::numeric
        else 0 end segment_charge_ex
    from source_rows s
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)
      with ordinality seg(value,ordinality)
    where upper(coalesce(s.invoice_breakdown_json->>'mode',''))='SEGMENTS'
      and nullif(seg.value->>'invoice_locked_invoice_id','') is null
      and exists(
        select 1 from selected_segments picked
        where picked.chunk_id=s.chunk_id
          and picked.timesheet_id=s.timesheet_id
          and coalesce(nullif(seg.value->>'segment_id',''),
            left(encode(digest(seg.value::text,'sha256'),'hex'),24))
            =any(coalesce(picked.segment_ids,array[]::text[])))
  ),
  segment_daily_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,s.work_date,
      sum(s.segment_hours_day) h_day,sum(s.segment_hours_night) h_night,
      sum(s.segment_hours_sat) h_sat,sum(s.segment_hours_sun) h_sun,
      sum(s.segment_hours_bh) h_bh,sum(s.segment_pay_ex) pay_ex,
      sum(s.segment_charge_ex) charge_ex,max(s.vat_rate) vat_rate,
      max(s.charge_day) charge_day,max(s.charge_night) charge_night,
      max(s.charge_sat) charge_sat,max(s.charge_sun) charge_sun,
      max(s.charge_bh) charge_bh,
      max(s.timesheet_version) timesheet_version,
      (array_agg(s.id order by s.id))[1] tsfin_id,
      max(s.contract_role) contract_role,
      max(s.contract_display_site) contract_display_site,
      max(s.contract_ward_hint) contract_ward_hint,
      (array_agg(s.bucket_labels_json order by s.ordinality))[1]
        bucket_labels_json
    from segment_entries s
    where s.daily_calc_of_invoices and s.work_date is not null
    group by s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,s.work_date
  ),
  segment_weekly_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,
      sum(s.segment_hours_day) h_day,sum(s.segment_hours_night) h_night,
      sum(s.segment_hours_sat) h_sat,sum(s.segment_hours_sun) h_sun,
      sum(s.segment_hours_bh) h_bh,sum(s.segment_pay_ex) pay_ex,
      sum(s.segment_charge_ex) charge_ex,max(s.vat_rate) vat_rate,
      max(s.charge_day) charge_day,max(s.charge_night) charge_night,
      max(s.charge_sat) charge_sat,max(s.charge_sun) charge_sun,
      max(s.charge_bh) charge_bh,
      max(s.timesheet_version) timesheet_version,
      (array_agg(s.id order by s.id))[1] tsfin_id,
      max(s.contract_role) contract_role,
      max(s.contract_display_site) contract_display_site,
      max(s.contract_ward_hint) contract_ward_hint,
      (array_agg(s.bucket_labels_json order by s.ordinality))[1]
        bucket_labels_json
    from segment_entries s
    where not s.daily_calc_of_invoices
       or not exists(select 1 from segment_entries d
         where d.chunk_id=s.chunk_id and d.timesheet_id=s.timesheet_id
           and d.daily_calc_of_invoices and d.work_date is not null)
    group by s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date
  ),
  nonsegment_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      coalesce(s.hours_day,0) h_day,coalesce(s.hours_night,0) h_night,coalesce(s.hours_sat,0) h_sat,
      coalesce(s.hours_sun,0) h_sun,coalesce(s.hours_bh,0) h_bh,
      round(coalesce(s.total_pay_ex_vat,0)-coalesce(s.additional_pay_ex_vat,0)
        -coalesce(s.expenses_pay_ex_vat,0)-coalesce(s.mileage_pay_ex_vat,0),2) pay_ex,
      round(coalesce(s.total_charge_ex_vat,0)-coalesce(s.additional_charge_ex_vat,0)
        -coalesce(s.expenses_charge_ex_vat,0)-coalesce(s.mileage_charge_ex_vat,0),2) charge_ex,
      s.vat_rate,s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,
      s.charge_bh,s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    where upper(coalesce(s.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
       or jsonb_array_length(case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
                                  then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)=0
  ),
  additional_daily_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,upper(a.key) code,
      left(d.key,10) work_date,
      case when pg_input_is_valid(d.value,'numeric')
        then d.value::numeric else 0 end units,
      case when pg_input_is_valid(a.value->>'pay_rate','numeric')
        then(a.value->>'pay_rate')::numeric else 0 end pay_rate,
      case when pg_input_is_valid(a.value->>'charge_rate','numeric')
        then(a.value->>'charge_rate')::numeric else 0 end charge_rate,
      round((case when pg_input_is_valid(d.value,'numeric')
          then d.value::numeric else 0 end)*
        (case when pg_input_is_valid(a.value->>'pay_rate','numeric')
          then(a.value->>'pay_rate')::numeric else 0 end),2) pay_ex,
      round((case when pg_input_is_valid(d.value,'numeric')
          then d.value::numeric else 0 end)*
        (case when pg_input_is_valid(a.value->>'charge_rate','numeric')
          then(a.value->>'charge_rate')::numeric else 0 end),2) charge_ex,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) bucket_name,
      coalesce(nullif(a.value->>'unit_name',''),'units') unit_name,
      a.value->'frequency' frequency,s.vat_rate,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    cross join lateral jsonb_each(
      case when jsonb_typeof(s.additional_units_json)='object'
        then s.additional_units_json else '{}'::jsonb end) a
    cross join lateral jsonb_each_text(
      case when jsonb_typeof(a.value->'days')='object'
        then a.value->'days' else '{}'::jsonb end) d
    where s.daily_calc_of_invoices
      and left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      and exists(select 1 from segment_entries se
        where se.chunk_id=s.chunk_id and se.timesheet_id=s.timesheet_id
          and se.work_date is not null)
  ),
  additional_weekly_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      upper(a.key) code,
      case when pg_input_is_valid(a.value->>'unit_count','numeric')
        then(a.value->>'unit_count')::numeric else 0 end units,
      case when pg_input_is_valid(a.value->>'pay_rate','numeric')
        then(a.value->>'pay_rate')::numeric else 0 end pay_rate,
      case when pg_input_is_valid(a.value->>'charge_rate','numeric')
        then(a.value->>'charge_rate')::numeric else 0 end charge_rate,
      case when pg_input_is_valid(a.value->>'pay_ex_vat','numeric')
        then(a.value->>'pay_ex_vat')::numeric else 0 end pay_ex,
      case when pg_input_is_valid(a.value->>'charge_ex_vat','numeric')
        then(a.value->>'charge_ex_vat')::numeric else 0 end charge_ex,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) bucket_name,
      coalesce(nullif(a.value->>'unit_name',''),'units') unit_name,
      a.value->'frequency' frequency,s.vat_rate,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s cross join lateral jsonb_each(
      case when jsonb_typeof(s.additional_units_json)='object'
        then s.additional_units_json else '{}'::jsonb end) a
    where jsonb_typeof(a.value)='object'
      and ((case when pg_input_is_valid(a.value->>'pay_ex_vat','numeric')
              then(a.value->>'pay_ex_vat')::numeric else 0 end)<>0
        or (case when pg_input_is_valid(a.value->>'charge_ex_vat','numeric')
              then(a.value->>'charge_ex_vat')::numeric else 0 end)<>0)
      and not(s.daily_calc_of_invoices
        and jsonb_typeof(a.value->'days')='object'
        and exists(select 1 from jsonb_each_text(
          case when jsonb_typeof(a.value->'days')='object'
            then a.value->'days' else '{}'::jsonb end) d
          where left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'))
  ),
  expense_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      e.code,e.pay_ex,e.charge_ex,s.vat_rate,s.expenses_description,
      s.expenses_evidence_r2_key,s.expenses_evidence_manifest,
      s.mileage_units,s.mileage_pay_rate,s.mileage_charge_rate,
      s.mileage_evidence_r2_key,s.mileage_evidence_manifest,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    cross join lateral (
      values
       ('TRAVEL',coalesce(s.travel_pay_ex_vat,0),coalesce(s.travel_charge_ex_vat,0)),
       ('ACCOMMODATION',coalesce(s.accommodation_pay_ex_vat,0),coalesce(s.accommodation_charge_ex_vat,0)),
       ('OTHER',coalesce(s.other_pay_ex_vat,0),coalesce(s.other_charge_ex_vat,0)),
       ('EXPENSES_FALLBACK',
          case when coalesce(s.travel_pay_ex_vat,0)+coalesce(s.accommodation_pay_ex_vat,0)+coalesce(s.other_pay_ex_vat,0)=0 then coalesce(s.expenses_pay_ex_vat,0) else 0 end,
          case when coalesce(s.travel_charge_ex_vat,0)+coalesce(s.accommodation_charge_ex_vat,0)+coalesce(s.other_charge_ex_vat,0)=0 then coalesce(s.expenses_charge_ex_vat,0) else 0 end),
       ('MILEAGE',coalesce(s.mileage_pay_ex_vat,0),coalesce(s.mileage_charge_ex_vat,0))
    ) e(code,pay_ex,charge_ex)
    where e.pay_ex<>0 or e.charge_ex<>0
  ),
  line_union as materialized (
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||work_date description,h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric pay_day,null::numeric pay_night,null::numeric pay_sat,
      null::numeric pay_sun,null::numeric pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_DAILY' line_type,
      'TS:'||timesheet_id||':HOURS:'||work_date source_key,
      jsonb_build_object('date',work_date,'timesheet_version',timesheet_version,
        'tsfin_id',tsfin_id,'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json) detail
    from segment_daily_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - W/E '||week_ending_date, h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_WEEKLY','TS:'||timesheet_id||':HOURS:WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json)
    from segment_weekly_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - W/E '||week_ending_date, h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_WEEKLY','TS:'||timesheet_id||':HOURS:WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json)
    from nonsegment_lines
    where pay_ex<>0 or charge_ex<>0 or h_day+h_night+h_sat+h_sun+h_bh<>0
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||bucket_name||' - '||work_date||' - '||
        units||' '||unit_name,0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'ADDITIONAL_RATE_DAILY',
      'TS:'||timesheet_id||':ADD:'||code||':'||work_date,
      jsonb_build_object('date',work_date,'timesheet_version',timesheet_version,
        'tsfin_id',tsfin_id,'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'bucket',jsonb_build_object('code',code,'bucket_name',bucket_name,
          'unit_name',unit_name,'frequency',frequency),
        'units',jsonb_build_object('unit_count',units,'pay_rate',pay_rate,
          'charge_rate',charge_rate))
    from additional_daily_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||bucket_name||' - '||units||' '||unit_name,0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'ADDITIONAL_RATE',
      'TS:'||timesheet_id||':ADD:'||code||':WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'bucket',jsonb_build_object('code',code,'bucket_name',bucket_name,
          'unit_name',unit_name,'frequency',frequency),
        'units',jsonb_build_object('unit_count',units,'pay_rate',pay_rate,
          'charge_rate',charge_rate))
    from additional_weekly_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      case when code='MILEAGE' then 'Mileage - '||coalesce(mileage_units,0)||
          ' miles (W/E '||week_ending_date||')'
        else initcap(replace(code,'_',' '))||' (W/E '||week_ending_date||')' end,
      0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,
      case when code='MILEAGE' then 'MILEAGE'
        when code='EXPENSES_FALLBACK' then 'EXPENSES_TOTAL'
        else 'EXPENSE_'||code end,
      case when code='MILEAGE' then 'TS:'||timesheet_id||':MILEAGE'
        when code='EXPENSES_FALLBACK' then 'TS:'||timesheet_id||':EXP:TOTAL'
        else 'TS:'||timesheet_id||':EXP:'||code end,
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'expense',case when code='MILEAGE' then jsonb_build_object(
          'category','MILEAGE','mileage_units',mileage_units,
          'pay_rate',mileage_pay_rate,'charge_rate',mileage_charge_rate,
          'evidence_r2_key',mileage_evidence_r2_key,
          'evidence_manifest',mileage_evidence_manifest)
        else jsonb_build_object('category',code,'note',expenses_description,
          'evidence_r2_key',expenses_evidence_r2_key,
          'evidence_manifest',expenses_evidence_manifest) end)
    from expense_lines
  ),
  correction_scopes_pre as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-prewrite:'||scope.planned_invoice_id::text,
      'scope_key',scope.planned_invoice_id::text,
      'invoice_id',case when exists(select 1 from public.invoices existing
        where existing.id=scope.planned_invoice_id)
        then scope.planned_invoice_id end,
      'validation_purpose','GENERATION_PREWRITE',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.planned_invoice_id),'[]'::jsonb) scopes
    from(
      select l.planned_invoice_id,min(vc.payload_json::text)::jsonb payload_json,
        jsonb_agg(distinct jsonb_build_object(
          'timesheet_id',l.timesheet_id,
          'vat_rate_pct',l.vat_rate))
          filter(where l.timesheet_id is not null) planned_members
      from line_union l
      join valid_chunks vc on vc.id=l.chunk_id
      group by l.planned_invoice_id
    ) scope
  ),
  correction_validation_pre as materialized (
    select r.*
    from correction_scopes_pre s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),
  correction_failures_pre as materialized (
    select scope_key,blocker_code,detail_json detail
    from correction_validation_pre
    where not valid
  ),  write_eligible_chunks as materialized (
    select vc.*
    from valid_chunks vc
    where not exists(
      select 1 from correction_failures_pre f
      where f.scope_key=vc.planned_invoice_id::text)
  ),
  line_totals as materialized (
    select l.planned_invoice_id,
      round(sum(round(l.charge_ex,2)),2) subtotal_ex_vat,
      round(sum(round(l.charge_ex*l.vat_rate/100,2)),2) vat_amount,
      round(sum(round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2)),2) total_inc_vat
    from line_union l
    join write_eligible_chunks w on w.id=l.chunk_id
    group by l.planned_invoice_id
  ),
  existing_target_headers as materialized (
    select vc.id chunk_id,i.id invoice_id
    from write_eligible_chunks vc
    join header_source h on h.chunk_id=vc.id
    join public.invoices i on i.id=vc.planned_invoice_id
      and i.client_id=h.client_id and i.type='INVOICE' and i.status='DRAFT'
    where not exists(
      select 1 from segment_lock_failures_pre f
      where f.invoice_id=vc.planned_invoice_id)
  ),
  inserted_headers as (
    insert into public.invoices(id,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
      header_snapshot_json,document_revision,document_state,created_at,updated_at)
    select h.planned_invoice_id,h.client_id,'DRAFT',v_now,
      t.subtotal_ex_vat,t.vat_amount,t.total_inc_vat,
      jsonb_build_object('client_id',h.client_id,'client_name',h.client_name,
        'client_invoice_address',h.client_invoice_address,'client_primary_invoice_email',h.primary_invoice_email,
        'agency_name',h.agency_name,'agency_logo',h.agency_logo,'registered_address',h.registered_address,
        'company_reg_number',h.company_reg_number,'company_registration_number',h.company_reg_number,
        'vat_chargeable',h.vat_chargeable,'payment_terms_days',h.payment_terms_days,
        'issued_at_utc',null,'due_at_utc',null,
        'totals',jsonb_build_object(
          'subtotal_ex_vat',t.subtotal_ex_vat,
          'vat_amount',t.vat_amount,'total_inc_vat',t.total_inc_vat),
        'bank',jsonb_build_object('name',h.bank_name,'sort_code',h.bank_sort_code,'account_number',h.bank_account_number),
        'vat_registration_number',h.vat_registration_number,
        'meta',jsonb_build_object('source','INVOICE_OPERATION_QUEUE',
          'invoice_week_start',vc.payload_json->>'target_invoice_week',
          'consolidation_mode',vc.payload_json->>'consolidation_mode',
          'self_bill',vc.payload_json->>'invoice_stream'='SELF_BILL','timesheet_count',h.source_count),
        'attach_policy',vc.payload_json#>'{plan,settings_snapshot,attach_policy}'),
      1,'STALE',v_now,v_now
    from header_source h join write_eligible_chunks vc on vc.id=h.chunk_id
    join line_totals t on t.planned_invoice_id=h.planned_invoice_id
    where not exists(
      select 1 from segment_lock_failures_pre f
      where f.invoice_id=h.planned_invoice_id)
    on conflict(id) do nothing returning id
  ),
  target_headers as materialized (
    select e.chunk_id,e.invoice_id
    from existing_target_headers e
    where not exists(
      select 1
      from inserted_headers h
      where h.id=e.invoice_id)
  ),
  deferred_new_headers as (
    update public.invoice_operation_chunks c
    set status='QUEUED',
      phase='COMMIT',
      progress_json=coalesce(c.progress_json,'{}'::jsonb)
        ||jsonb_build_object('status_message',
          'Invoice header created; applying authoritative source ownership'),
      error_json=null,
      lease_owner=null,
      lease_token=null,
      lease_expires_at_utc=null,
      updated_at_utc=v_now
    from inserted_headers h
    join write_eligible_chunks vc on vc.planned_invoice_id=h.id
    where c.id=vc.id
    returning c.id
  ),
  inserted_lines as (
    insert into public.invoice_lines(
      invoice_id,timesheet_id,booking_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
      paper_ts_r2_key,meta_json,source_key
    )
    select l.planned_invoice_id,l.timesheet_id,l.booking_id,l.description,
      round(l.h_day,2),round(l.h_night,2),round(l.h_sat,2),round(l.h_sun,2),round(l.h_bh,2),
      l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
      l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,
      round(l.pay_ex,2),round(l.charge_ex,2),round(l.charge_ex-l.pay_ex,2),l.vat_rate,
      round(l.charge_ex*l.vat_rate/100,2),round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2),
      null,jsonb_build_object('line_type',l.line_type,'timesheet_id',l.timesheet_id,
        'candidate_display',l.description,'week_ending_date',
        (select s.week_ending_date from source_rows s where s.timesheet_id=l.timesheet_id limit 1),
        'schedule_references',
        (select s.schedule_refs from source_rows s where s.timesheet_id=l.timesheet_id limit 1),
        'totals',jsonb_build_object('line_pay_ex_vat',round(l.pay_ex,2),
          'line_charge_ex_vat',round(l.charge_ex,2),'margin_ex_vat',round(l.charge_ex-l.pay_ex,2),
          'vat_rate_pct',l.vat_rate,'vat_amount',round(l.charge_ex*l.vat_rate/100,2),
          'total_inc_vat',round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2)))||l.detail,l.source_key
    from line_union l join target_headers h on h.invoice_id=l.planned_invoice_id
    on conflict(invoice_id,source_key) do nothing
    returning invoice_id,timesheet_id,total_charge_ex_vat,vat_amount,total_inc_vat
  ),
  all_target_lines as materialized (
    select l.invoice_id,l.total_charge_ex_vat,l.vat_amount,l.total_inc_vat
    from public.invoice_lines l
    where l.invoice_id in(select invoice_id from target_headers)
    union all
    select l.invoice_id,l.total_charge_ex_vat,l.vat_amount,l.total_inc_vat
    from inserted_lines l
  ),
  all_line_totals as materialized (
    select l.invoice_id,count(*)::integer line_count,
      round(sum(l.total_charge_ex_vat),2) subtotal_ex_vat,
      round(sum(l.vat_amount),2) vat_amount,
      round(sum(l.total_inc_vat),2) total_inc_vat
    from all_target_lines l
    group by l.invoice_id
  ),
  updated_header_totals as (
    update public.invoices i
    set subtotal_ex_vat=t.subtotal_ex_vat,vat_amount=t.vat_amount,
      total_inc_vat=t.total_inc_vat,document_state='STALE',
      header_snapshot_json=coalesce(i.header_snapshot_json,'{}'::jsonb)
        ||jsonb_build_object('totals',jsonb_build_object(
          'subtotal_ex_vat',t.subtotal_ex_vat,'vat_amount',t.vat_amount,
          'total_inc_vat',t.total_inc_vat)),
      updated_at=v_now
    from all_line_totals t where i.id=t.invoice_id
    returning i.id
  ),
  segment_lock_targets as materialized (
    select targets from segment_lock_targets_pre
  ),
  segment_lock_authority as materialized (
    select * from segment_lock_authority_pre
  ),
  segment_lock_failures as materialized (
    select * from segment_lock_authority where not success
  ),
  whole_lock as (
    update public.timesheets_financials tf
    set locked_by_invoice_id=vc.planned_invoice_id,locked_at_utc=v_now,updated_at=v_now
    from members m join write_eligible_chunks vc on vc.id=m.chunk_id
    join target_headers h
      on h.chunk_id=vc.id and h.invoice_id=vc.planned_invoice_id
    cross join (select count(*) applied_count from segment_lock_authority) segment_application
    where tf.timesheet_id=m.timesheet_id and tf.is_current
      and (coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
        or not exists(select 1 from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) e
                      where nullif(e->>'invoice_locked_invoice_id','') is null))
    returning tf.timesheet_id
  ),
  week_lock as (
    update public.contract_weeks cw set status='INVOICED',updated_at=v_now
    where cw.timesheet_id in(
      select m.timesheet_id
      from members m
      join target_headers h on h.chunk_id=m.chunk_id)
      and exists(select 1 from public.timesheets_financials tf where tf.timesheet_id=cw.timesheet_id
        and tf.is_current and (tf.locked_by_invoice_id is not null or
          not exists(select 1 from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) e
                     where nullif(e->>'invoice_locked_invoice_id','') is null)))
    returning cw.id
  ),
  nhsp_shift_inclusion as (
    update public.nhsp_shifts ns
    set invoice_status='INCLUDED',
      invoice_id=h.invoice_id,
      updated_at=v_now
    from source_rows s
    join target_headers h
      on h.chunk_id=s.chunk_id and h.invoice_id=s.planned_invoice_id
    where ns.timesheet_id=s.timesheet_id
      and ns.invoice_status='PENDING'
      and ns.invoice_id is null
      and ns.cancelled_at_utc is null
      and exists(
        select 1
        from jsonb_array_elements(
          case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
            then s.invoice_breakdown_json->'segments'
            else '[]'::jsonb end) segment(value)
        where coalesce(
            nullif(segment.value->>'nhsp_shift_id',''),
            nullif(segment.value->>'shift_id',''))=ns.id::text)
    returning ns.id
  ),
  hr_sources as (
    insert into public.invoice_hr_source_rows(invoice_id,source_system,import_id,header_columns,rows_json,header_rows)
    select distinct h.invoice_id,
      case when tf.basis::text in('NHSP','NHSP_ADJUSTMENT')
        then 'NHSP' else 'HEALTHROSTER' end,
      tf.nhsp_import_id,'[]'::jsonb,
      case when jsonb_typeof(tf.external_source_rows_json)='array' then tf.external_source_rows_json else '[]'::jsonb end,
      '[]'::jsonb
    from members m
    join target_headers h on h.chunk_id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    where tf.nhsp_import_id is not null
    on conflict(invoice_id,source_system,import_id) do update set rows_json=excluded.rows_json
    returning invoice_id
  ),
  source_segments as materialized (
    select distinct h.invoice_id planned_invoice_id,
      coalesce(
        case when coalesce(x.value->>'nhsp_shift_id','')~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(x.value->>'nhsp_shift_id')::uuid end,
        case when coalesce(x.value->>'shift_id','')~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(x.value->>'shift_id')::uuid end,
        case when substr(coalesce(x.value->>'segment_id',''),6)~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then substr(x.value->>'segment_id',6)::uuid end) shift_id,
      case
        when s.basis::text in('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT')
         and (
           lower(coalesce(x.value->>'is_reversal','false')) in(
             'true','t','1','yes')
           or coalesce(s.total_hours,0)<0
         ) then 'REVERSAL'
        when s.basis::text in('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT')
          then 'CORRECTED_HOURS'
        else 'SOURCE'
      end evidence_role_key
    from source_rows s
    join target_headers h
      on h.chunk_id=s.chunk_id and h.invoice_id=s.planned_invoice_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end) x(value)
    where coalesce(
        nullif(x.value->>'nhsp_shift_id',''),
        nullif(x.value->>'shift_id',''),
        case when left(x.value->>'segment_id',5)='nhsp:'
          then substr(x.value->>'segment_id',6) end)~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (
        upper(coalesce(x.value->>'source_system',''))='NHSP'
        or (upper(coalesce(x.value->>'source_system',''))='HEALTHROSTER'
          and coalesce((s.payload_json#>>'{plan,settings_snapshot,attach_policy,hr_attach_to_invoice}')::boolean,true)))
    union
    select distinct r.planned_invoice_id,n.id shift_id,
      case when r.is_reversal then 'REVERSAL'
        else 'CORRECTED_HOURS' end evidence_role_key
    from adjustment_segment_refs r
    join source_timesheet_ancestry a
      on a.planned_invoice_id=r.planned_invoice_id
     and a.root_timesheet_id=r.root_timesheet_id
     and a.ancestry_depth>0
    join public.nhsp_shifts n
      on n.timesheet_id=a.source_timesheet_id
     and n.latest_import_id is not null
     and n.external_row_key is not null
     and (
       upper(coalesce(n.source_system::text,''))='NHSP'
       or (
         upper(coalesce(n.source_system::text,''))='HEALTHROSTER'
         and r.healthroster_attach_allowed
       )
     )
     and (
       (r.ref_num is not null
         and btrim(coalesce(n.ref_num,''))=r.ref_num)
       or (
         r.ref_num is null
         and r.start_utc is not null
         and r.end_utc is not null
         and n.start_utc=r.start_utc
         and n.end_utc=r.end_utc
       )
     )
  ),
  source_import_rows as materialized (
    select distinct s.planned_invoice_id,
      upper(coalesce(n.source_system::text,'UNKNOWN')) source_system,
      n.latest_import_id import_id,n.external_row_key,s.evidence_role_key
    from source_segments s
    join public.nhsp_shifts n on n.id=s.shift_id
    where n.latest_import_id is not null
      and n.external_row_key is not null
  ),
  source_imports as materialized (
    select s.planned_invoice_id,s.source_system,s.import_id,
      jsonb_agg(jsonb_build_object(
        'external_row_key',s.external_row_key,
        'evidence_role_key',s.evidence_role_key)
        order by s.external_row_key,s.evidence_role_key) row_occurrences
    from source_import_rows s
    group by s.planned_invoice_id,s.source_system,s.import_id
  ),
  authoritative_hr_sources as (
    insert into public.invoice_hr_source_rows(
      invoice_id,source_system,import_id,header_columns,rows_json)
    select g.planned_invoice_id,g.source_system,g.import_id,
      case when jsonb_typeof(i.parse_summary_json->'header_columns')='array'
        then i.parse_summary_json->'header_columns' else '[]'::jsonb end,
      coalesce((select jsonb_agg(
          r.payload_json
            ||jsonb_build_object(
              'evidence_role',case occurrence.value->>'evidence_role_key'
                when 'REVERSAL' then case when g.source_system='NHSP'
                  then 'NHSP Reversal' else 'HealthRoster Reversal' end
                when 'CORRECTED_HOURS' then case when g.source_system='NHSP'
                  then 'NHSP Corrected Hours' else 'HealthRoster Corrected Hours' end
                else case when g.source_system='NHSP'
                  then 'NHSP Shift' else 'HealthRoster Shift' end
              end)
            ||case when occurrence.value->>'evidence_role_key'='REVERSAL'
              then jsonb_build_object('reversal_state','REVERSED')
              else '{}'::jsonb end
          order by r.id,occurrence.value->>'evidence_role_key')
        from jsonb_array_elements(g.row_occurrences) occurrence(value)
        join public.hr_rows r
          on r.import_id=g.import_id
         and r.external_row_key=occurrence.value->>'external_row_key'),
        '[]'::jsonb)
    from source_imports g join public.hr_imports i on i.id=g.import_id
    on conflict(invoice_id,source_system,import_id) do update
      set header_columns=excluded.header_columns,rows_json=excluded.rows_json
    returning invoice_id
  ),
  audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,after_json,reason)
    select v_now,o.actor_user_id,u.display_name,u.role,'invoices',vc.planned_invoice_id::text,
      case when exists(select 1 from inserted_headers h
        where h.id=vc.planned_invoice_id)
        then 'INVOICE_CREATED' else 'INVOICE_DRAFT_APPENDED' end,
      jsonb_build_object('invoice_id',vc.planned_invoice_id,
        'source_ids',coalesce(vc.payload_json->'canonical_source_ids',
          vc.payload_json->'source_ids','[]'::jsonb),
        'source_revision',vc.payload_json->>'source_revision',
        'operation_id',vc.operation_id),'INVOICE_OPERATION_QUEUE'
    from write_eligible_chunks vc
    join target_headers h
      on h.chunk_id=vc.id and h.invoice_id=vc.planned_invoice_id
    join public.invoice_operations o on o.id=vc.operation_id
    left join public.tms_users u on u.id=o.actor_user_id
    returning id
  ),
  correction_failures as materialized (
    select scope_key::uuid invoice_id,blocker_code,detail
    from correction_failures_pre
    where scope_key~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  update public.invoice_operation_chunks c
    set status=case when exists(select 1 from all_line_totals l
                               where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then 'COMPLETE' else 'BLOCKED' end,
      phase=case when exists(select 1 from all_line_totals l
                            where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then 'COMPLETE' else 'BLOCKED' end,
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(vc.planned_invoice_id),
        'source_revision',vc.payload_json->>'source_revision'),
      error_json=case
        when not exists(select 1 from all_line_totals l
          where l.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code','NO_INVOICE_LINES_CREATED')
        when exists(select 1 from segment_lock_failures sf
          where sf.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code','SEGMENT_LOCK_FAILED','detail',(
            select jsonb_agg(to_jsonb(sf))
            from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id))
        when exists(select 1 from correction_failures cf
          where cf.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code',(
            select cf.blocker_code from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id))
        else null end,
      completed_at_utc=case
        when exists(select 1 from all_line_totals l
          where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then v_now else null end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from valid_chunks vc
    join target_headers h
      on h.chunk_id=vc.id and h.invoice_id=vc.planned_invoice_id
    cross join (select count(*) applied_count from segment_lock_authority) segment_application
    cross join (select count(*) cached_count from authoritative_hr_sources) source_cache_application
    where c.id=vc.id
  ;
  select coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,
      'result',c.result_json,'error',c.error_json)),'[]'::jsonb)
    into v_part
  from jsonb_array_elements(p_claims) x
  join public.invoice_operation_chunks c
    on c.id=case when coalesce(x->>'chunk_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x->>'chunk_id')::uuid end
  where x->>'phase'='COMMIT'
    and coalesce(c.payload_json->>'command_type','')<>'GENERATE_CREDIT_NOTE';
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Credit-note COMMIT: exact negative line clone and source unlock; document creation stays asynchronous.
  execute $q$
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements($1) x where x->>'phase'='COMMIT'
  ),
  credit_revisions as materialized (
    select c.*,i.id source_invoice_id,(c.payload_json#>>'{plan,planned_invoice_id}')::uuid credit_note_id
      ,encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),
        'sha256'),'hex') current_revision
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i on i.id=c.entity_id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
      and i.type='INVOICE' and i.status in ('ISSUED','PAID')
  ),
  credit_correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','credit-source:'||c.source_invoice_id::text,
      'scope_key',c.source_invoice_id::text,
      'invoice_id',c.source_invoice_id,
      'validation_purpose','CREDIT_SOURCE')
      order by c.source_invoice_id),'[]'::jsonb) scopes
    from(select distinct source_invoice_id from credit_revisions) c
  ),
  credit_correction_failures as materialized (
    select r.invoice_id source_invoice_id,r.blocker_code,r.detail_json
    from credit_correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,($2 at time zone 'Europe/London')::date) r
    where not r.valid
  ),
  credit_rejected as (
    update public.invoice_operation_chunks q
    set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code',case
        when c.current_revision<>c.payload_json->>'source_revision'
          then 'CREDIT_SOURCE_CHANGED_BEFORE_COMMIT'
        else coalesce((select f.blocker_code
          from credit_correction_failures f
          where f.source_invoice_id=c.source_invoice_id
          order by f.blocker_code limit 1),'CREDIT_CORRECTION_VALIDATION_FAILED')
        end),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=$2
    from credit_revisions c
    where q.id=c.id and(
      c.current_revision<>c.payload_json->>'source_revision'
      or exists(select 1 from credit_correction_failures f
        where f.source_invoice_id=c.source_invoice_id))
    returning q.id
  ),
  credits as materialized (
    select c.* from credit_revisions c
    where c.current_revision=c.payload_json->>'source_revision'
      and not exists(select 1 from credit_rejected r where r.id=c.id)
  ),
  inserted_credit as (
    insert into public.invoices(id,type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
      original_invoice_id,notes,header_snapshot_json,document_revision,document_state,created_at,updated_at)
    select c.credit_note_id,'CREDIT_NOTE',i.client_id,'DRAFT',$2,
      -i.subtotal_ex_vat,-i.vat_amount,-i.total_inc_vat,i.id,
      c.payload_json->>'credit_reason',
      i.header_snapshot_json||jsonb_build_object(
        'credit_source_invoice_id',i.id,
        'credit_reason',c.payload_json->>'credit_reason'),1,'STALE',$2,$2
    from credits c join public.invoices i on i.id=c.source_invoice_id
    on conflict(id) do nothing returning id
  ),
  cloned_lines as (
    insert into public.invoice_lines(invoice_id,timesheet_id,booking_id,description,
      hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,total_pay_ex_vat,total_charge_ex_vat,
      margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,paper_ts_r2_key,meta_json,source_key)
    select c.credit_note_id,l.timesheet_id,l.booking_id,
      'CREDIT NOTE – '||coalesce(l.description,''),
      l.hours_day,l.hours_night,l.hours_sat,l.hours_sun,l.hours_bh,
      l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
      l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,
      -l.total_pay_ex_vat,-l.total_charge_ex_vat,-l.margin_ex_vat,l.vat_rate_pct,
      -l.vat_amount,-l.total_inc_vat,l.paper_ts_r2_key,
      coalesce(l.meta_json,'{}'::jsonb)||jsonb_build_object(
        'credit_note',true,'original_invoice_id',c.source_invoice_id,
        'original_invoice_line_id',l.id,'credit_of_line_id',l.id,'line_type',
        'CREDIT_'||coalesce(l.meta_json->>'line_type','LINE')),
      'CN:'||c.credit_note_id||':LINE:'||l.id
    from credits c join public.invoice_lines l on l.invoice_id=c.source_invoice_id
    on conflict(invoice_id,source_key) do nothing returning invoice_id
  ),
  source_mark as (
    update public.invoices i set credit_note_created_at_utc=$2,updated_at=$2
    where i.id in(select source_invoice_id from credits) returning i.id
  ),
  unlocks as (
    update public.timesheets_financials tf
    set locked_by_invoice_id=null,locked_at_utc=null,
      unlocked_by_credit_note_id=c.credit_note_id,is_stale=true,
      stale_reason='UNLOCKED_BY_CREDIT',updated_at=$2
    from credits c
    where tf.locked_by_invoice_id=c.source_invoice_id and tf.is_current returning tf.timesheet_id
  ),
  segment_unlocks as (
    update public.timesheets_financials tf
    set invoice_breakdown_json=jsonb_set(tf.invoice_breakdown_json,'{segments}',
      (select jsonb_agg(case
        when e.value->>'invoice_locked_invoice_id'=c.source_invoice_id::text
          then (e.value-'invoice_locked_invoice_id'-'invoice_locked_at_utc')
        else e.value end order by e.ordinality)
       from jsonb_array_elements(tf.invoice_breakdown_json->'segments')
         with ordinality e(value,ordinality)),true),
      unlocked_by_credit_note_id=c.credit_note_id,is_stale=true,
      stale_reason='UNLOCKED_BY_CREDIT',updated_at=$2
    from credits c
    where tf.is_current and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
      and exists(select 1 from jsonb_array_elements(tf.invoice_breakdown_json->'segments') e
        where e->>'invoice_locked_invoice_id'=c.source_invoice_id::text)
    returning tf.timesheet_id
  ),
  recompute_outbox as (
    insert into public.ts_financials_outbox(
      timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at)
    select distinct u.timesheet_id,'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,$2,null,$2
    from (
      select timesheet_id from unlocks
      union all select timesheet_id from segment_unlocks
    ) u
    on conflict on constraint uq_tsfin_outbox do nothing
    returning timesheet_id
  ),
  credit_audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,
      actor_role_at_time,object_type,object_id_text,action,after_json,reason)
    select $2,o.actor_user_id,u.display_name,u.role,'invoices',
      c.credit_note_id::text,'CREDIT_NOTE_CREATED',
      jsonb_build_object('credit_note_id',c.credit_note_id,
        'original_invoice_id',c.source_invoice_id,
        'credit_reason',c.payload_json->>'credit_reason',
        'command_token',c.payload_json->>'command_token',
        'subtotal_ex_vat',-i.subtotal_ex_vat,'vat_amount',-i.vat_amount,
        'total_inc_vat',-i.total_inc_vat,
        'financial_recompute_enqueued',true),
      'INVOICE_OPERATION_QUEUE'
    from credits c join public.invoices i on i.id=c.source_invoice_id
    join public.invoice_operations o on o.id=c.operation_id
    left join public.tms_users u on u.id=o.actor_user_id
    returning id
  ),
  completed as (
    update public.invoice_operation_chunks q
    set status='QUEUED',
      phase='QUEUE_DOCUMENT',
      error_json=null,
      completed_at_utc=null,lease_owner=null,lease_token=null,
      lease_expires_at_utc=null,updated_at_utc=$2,
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(c.credit_note_id),
        'credit_note_id',c.credit_note_id,'source_invoice_id',c.source_invoice_id)
    from credits c
    cross join (select count(*) outbox_count from recompute_outbox) outbox_application
    cross join (select count(*) audit_count from credit_audits) audit_application
    where q.id=c.id
    returning q.id,q.status,q.phase,q.result_json,q.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json)),'[]'::jsonb)
  from completed
  $q$
  into v_part using p_claims,v_now;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Document work is queued in a separate SQL statement so source-invalidation
  -- statement triggers cannot supersede the freshly-created document operation.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='QUEUE_DOCUMENT'
  ),
  queued_sources as materialized (
    select c.id chunk_id,c.operation_id parent_operation_id,
      (c.payload_json#>>'{plan,planned_invoice_id}')::uuid planned_invoice_id,
      c.payload_json->>'source_revision' source_revision,
      c.payload_json->>'command_type' command_type,
      c.payload_json->>'command_token' command_token,
      c.payload_json->>'credit_reason' credit_reason,
      i.document_revision::text document_revision
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i
      on i.id=(c.payload_json#>>'{plan,planned_invoice_id}')::uuid
  ),
  existing_doc_versions as materialized (
    select distinct on(q.chunk_id) q.chunk_id,v.id document_version_id,
      v.operation_id,v.status
    from queued_sources q
    join public.invoice_document_versions v
      on v.entity_type='INVOICE'
     and v.entity_id=q.planned_invoice_id
     and v.purpose='DRAFT_PREVIEW'
     and v.source_revision=q.document_revision
     and v.template_version='invoice-professional-v2'
     and v.status in(
       'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
       'VERIFYING','READY')
    where q.command_type<>'GENERATE_CREDIT_NOTE'
    order by q.chunk_id,case when v.status='READY' then 0 else 1 end,
      v.created_at_utc desc,v.id desc
  ),
  doc_ops as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,created_at_utc,updated_at_utc)
    select q.parent_operation_id,'BUILD_DOCUMENT','INVOICE',q.planned_invoice_id,o.actor_user_id,
      encode(digest('DRAFT_PREVIEW|'||q.planned_invoice_id||'|'||q.document_revision||
        '|invoice-professional-v2','sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',550,q.document_revision,'invoice-professional-v2',
      jsonb_build_object('invoice_id',q.planned_invoice_id,'purpose','DRAFT_PREVIEW'),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,
      nextval('public.invoice_operation_change_seq'),v_now,v_now
    from queued_sources q join public.invoice_operations o on o.id=q.parent_operation_id
    where q.command_type<>'GENERATE_CREDIT_NOTE'
      and not exists(
        select 1 from existing_doc_versions e where e.chunk_id=q.chunk_id)
    on conflict(idempotency_key) where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,excluded.priority),updated_at_utc=v_now
    returning *
  ),
  selected_doc_ops as materialized (
    select q.chunk_id,q.planned_invoice_id,q.document_revision source_revision,
      d.id operation_id,d.control_version,null::uuid existing_version_id,
      null::text existing_status
    from queued_sources q join doc_ops d on d.entity_id=q.planned_invoice_id
    union all
    select q.chunk_id,q.planned_invoice_id,q.document_revision,
      e.operation_id,o.control_version,e.document_version_id,e.status
    from queued_sources q
    join existing_doc_versions e on e.chunk_id=q.chunk_id
    join public.invoice_operations o on o.id=e.operation_id
  ),
  doc_versions as materialized (
    insert into public.invoice_document_versions(entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select 'INVOICE',d.planned_invoice_id,'DRAFT_PREVIEW',d.operation_id,d.source_revision,
      'invoice-professional-v2','PLANNING','{}',encode(digest('{}','sha256'),'hex'),
      '[]',encode(digest('[]','sha256'),'hex'),v_now
    from selected_doc_ops d
    where d.existing_version_id is null
    on conflict(entity_type,entity_id,purpose,source_revision,template_version)
      where purpose in('DRAFT_PREVIEW','TIMESHEET')
        and status in ('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning id,entity_id,operation_id
  ),
  all_doc_versions as materialized (
    select d.chunk_id,d.planned_invoice_id,d.operation_id,d.control_version,
      d.source_revision,
      coalesce(d.existing_version_id,v.id) document_version_id,
      coalesce(d.existing_status,'PLANNING') document_status
    from selected_doc_ops d
    left join doc_versions v
      on v.entity_id=d.planned_invoice_id and v.operation_id=d.operation_id
    where d.existing_version_id is not null or v.id is not null
  ),
  doc_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select d.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',d.document_version_id::text,
        d.source_revision,'invoice-professional-v2','1'),'sha256'),'hex'),
      0,'INVOICE',d.planned_invoice_id,d.document_version_id,
      'QUEUED',550,v_now,jsonb_build_object('purpose','DRAFT_PREVIEW'),d.control_version,v_now,v_now
    from all_doc_versions d
    where d.document_status<>'READY'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning operation_id
  ),
  invoice_ptrs as (
    update public.invoices i set preview_document_version_id=d.document_version_id,
      active_document_operation_id=case when d.document_status='READY'
        then null else d.operation_id end,
      document_state=case when d.document_status='READY'
        then 'READY' else 'QUEUED' end,updated_at=v_now
    from all_doc_versions d where i.id=d.planned_invoice_id returning i.id
  ),
  credit_issue_ops as materialized (
    insert into public.invoice_operations(
      parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,
      input_json,config_json,progress_json,total_units,chunk_count,
      control_version,change_seq,created_at_utc,updated_at_utc)
    select q.parent_operation_id,'ISSUE_INVOICES','INVOICE_BATCH',null,
      o.actor_user_id,
      encode(digest('CREDIT_ISSUE|'||q.planned_invoice_id||'|'||
        q.document_revision||'|'||coalesce(q.command_token,''),'sha256'),'hex'),
      'QUEUED','VALIDATE',850,q.document_revision,'invoice-professional-v2',
      jsonb_build_object(
        'invoice_ids',jsonb_build_array(q.planned_invoice_id),
        'credit_note',true,'credit_reason',q.credit_reason,
        'command_token',q.command_token,'deliver',false),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    from queued_sources q
    join public.invoice_operations o on o.id=q.parent_operation_id
    where q.command_type='GENERATE_CREDIT_NOTE'
    on conflict(idempotency_key)
      where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,excluded.priority),
      updated_at_utc=v_now
    returning *
  ),
  selected_credit_issue as materialized (
    select q.chunk_id,q.planned_invoice_id,q.document_revision,
      q.command_token,q.credit_reason,o.id operation_id,o.control_version
    from queued_sources q
    join credit_issue_ops o on o.parent_operation_id=q.parent_operation_id
      and o.operation_type='ISSUE_INVOICES'
    where q.command_type='GENERATE_CREDIT_NOTE'
  ),
  credit_issue_chunks as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,status,
      priority,run_after_utc,payload_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select s.operation_id,'ISSUE_INVOICE','VALIDATE',
      encode(digest(concat_ws('|','ISSUE_INVOICE',s.planned_invoice_id::text,
        s.document_revision,s.command_token),'sha256'),'hex'),
      0,'INVOICE',
      s.planned_invoice_id,'QUEUED',850,v_now,
      jsonb_build_object(
        'invoice_id',s.planned_invoice_id,
        'source_revision',s.document_revision,
        'allow_early',true,'deliver',false,
        'command_token',s.command_token,
        'credit_note',true,'credit_reason',s.credit_reason),
      s.control_version,v_now,v_now
    from selected_credit_issue s
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning operation_id
  ),
  credit_issue_ptrs as (
    update public.invoices i
    set active_issue_operation_id=s.operation_id,issue_state='VALIDATING',
      updated_at=v_now
    from selected_credit_issue s
    where i.id=s.planned_invoice_id
    returning i.id
  ),
  child_results as materialized (
    select d.chunk_id,d.operation_id document_operation_id,
      d.document_version_id,null::uuid issue_operation_id
    from all_doc_versions d
    union all
    select s.chunk_id,null::uuid,null::uuid,s.operation_id
    from selected_credit_issue s
  )
  update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=coalesce(c.result_json,'{}'::jsonb)||jsonb_build_object(
        'document_operation_id',d.document_operation_id,
        'document_version_id',d.document_version_id,
        'issue_operation_id',d.issue_operation_id)
    from child_results d where c.id=d.chunk_id
  ;
  select coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,
      'result',c.result_json,'error',c.error_json)),'[]'::jsonb)
    into v_part
  from jsonb_array_elements(p_claims) x
  join public.invoice_operation_chunks c
    on c.id=case when coalesce(x->>'chunk_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x->>'chunk_id')::uuid end
  where x->>'phase'='QUEUE_DOCUMENT';
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  return coalesce(v_result,'[]'::jsonb);
end;
$function$
;

-- SOURCE private._invoice_generation_resolve_command_groups(p_commands jsonb, p_actor_user_id uuid, p_effective_at_utc timestamp with time zone)
CREATE OR REPLACE FUNCTION private._invoice_generation_resolve_command_groups(p_commands jsonb, p_actor_user_id uuid, p_effective_at_utc timestamp with time zone)
 RETURNS TABLE(command_no integer, command_type text, group_key text, canonical_source_ids uuid[], canonical_source_members jsonb, source_types text[], client_id uuid, contract_ids uuid[], target_invoice_week date, natural_source_weeks date[], consolidation_mode text, invoice_stream text, self_bill boolean, automatic boolean, source_origin text, allow_early boolean, effective_settings_date date, source_revision_hash text, idempotency_components jsonb, blocker_code text, blocker_detail jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
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
    ts.parent_timesheet_id,ts.correction_id,ts.correction_kind::text correction_kind,
    tf.id tsfin_id,tf.timesheet_version tsfin_timesheet_version,
    tf.updated_at tsfin_updated_at,tf.is_current tsfin_is_current,tf.is_stale,
    tf.processing_status::text processing_status,tf.locked_by_invoice_id,
    tf.client_id,tf.candidate_id,tf.basis::text basis,tf.total_hours,
    tf.invoice_breakdown_json,
    tf.additional_units_json,
    coalesce(
      ts.candidate_hint_text->'correction_financials_policy_envelope',
      tf.policy_snapshot_json->'correction_financials_policy_envelope',
      tf.rate_source_refs_json->'correction_financials_policy_envelope'
    ) correction_envelope,
    coalesce(ts.contract_id,cw.contract_id) contract_id,
    coalesce(parent_ts.contract_id,parent_cw.contract_id) parent_contract_id,
    coalesce(parent_contract.self_bill,contract.self_bill,false) self_bill,
    case
      when upper(coalesce(tf.basis::text,'')) in(
        'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL',
        'HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      when coalesce(parent_contract.self_bill,contract.self_bill,false)
        then 'SELF_BILL'
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
    case
      when s.consolidation_mode='NONE'
        and coalesce(s.is_adjustment,false)
        and upper(btrim(coalesce(s.adjustment_origin,'')))='IMPORT_CORRECTION'
        and nullif(btrim(coalesce(s.correction_id,'')),'') is not null
        and upper(btrim(coalesce(s.correction_kind,''))) in(
          'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        and jsonb_typeof(s.correction_envelope)='object'
        and s.correction_envelope->>'policy_schema_version'=
          'IMPORT_CORRECTION_FINANCIALS_POLICY_V2'
        and s.correction_envelope->>'route_family'='IMPORT_AUTHORITATIVE'
        and lower(coalesce(
          s.correction_envelope#>>'{classification,canonical}','false'))
          in('true','t','1','yes')
        and s.correction_envelope->>'correction_shape'=
          'REVERSAL_REPLACEMENT'
        and s.correction_envelope->>'expected_member_count'='2'
        and nullif(
          s.correction_envelope#>>'{operation,operation_id}','') is not null
        and nullif(
          s.correction_envelope->>'correction_chain_id','') is not null
        and s.correction_envelope#>>'{operation,correction_action}'=
          'CHANGED_HOURS'
        and s.correction_envelope->>'envelope_fingerprint'=
          encode(digest(convert_to(
            (s.correction_envelope-'envelope_fingerprint')::text,'UTF8'),
            'sha256'),'hex')
      then 'IMPORT_AUTHORITATIVE_CORRECTION:'||s.correction_id
      else s.timesheet_id::text
    end atomic_grouping_value,
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
      when s.consolidation_mode='NONE' then s.atomic_grouping_value
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
        when s.consolidation_mode='NONE' then s.atomic_grouping_value
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
      when s.consolidation_mode='NONE' then s.atomic_grouping_value
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
       then source_row.atomic_grouping_value
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
    case
      when b.source_blocker is not null then b.source_blocker
      when r.blocker_code='INVOICE_REFERENCE_REQUIRED'
       and not (
         coalesce(precheck.require_reference_to_invoice,false)
         and coalesce(b.total_hours,0)>0
       ) then null
      else r.blocker_code
    end canonical_blocker,
    r.reference_hash,r.current_revision canonical_row_revision,
    r.detail_json reference_detail
  from source_rows_scoped b
  left join reference_results r
    on r.source_member_key=b.source_member_key
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=b.timesheet_id
),
grouped as materialized (
  select s.command_no,s.command_type,
    encode(digest(concat_ws('|',s.command_type,s.client_id::text,s.invoice_stream,
      s.consolidation_mode,
      case
        when s.consolidation_mode='NONE' then s.atomic_grouping_value
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
      when s.consolidation_mode='NONE' then s.atomic_grouping_value
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
$function$
;

-- SOURCE private._invoice_issue_validate_batch(p_requests jsonb, p_evaluation_date date)
CREATE OR REPLACE FUNCTION private._invoice_issue_validate_batch(p_requests jsonb, p_evaluation_date date)
 RETURNS TABLE(request_key text, invoice_id uuid, hard_blocker_codes jsonb, document_dependency_codes jsonb, delivery_blocker_codes jsonb, warning_codes jsonb, can_issue_only boolean, can_issue_and_deliver boolean, route_policy_result jsonb, detail_json jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with raw as materialized (
  select x.ordinality::integer request_no,
      nullif(btrim(coalesce(x.value->>'request_key','')),'') request_key,
      case when coalesce(x.value->>'invoice_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (x.value->>'invoice_id')::uuid end invoice_id,
      case when coalesce(x.value->>'operation_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (x.value->>'operation_id')::uuid end operation_id,
      case when jsonb_typeof(x.value->'expected_revision')='number'
        and coalesce(x.value->>'expected_revision','')~'^[0-9]+$'
        then (x.value->>'expected_revision')::bigint end expected_revision,
      case when jsonb_typeof(x.value->'allow_early')='boolean'
        then(x.value->>'allow_early')::boolean else false end allow_early,
      case when jsonb_typeof(x.value->'deliver')='boolean'
        then(x.value->>'deliver')::boolean else false end deliver,
      case when jsonb_typeof(x.value->'recipient_set')='array'
        then x.value->'recipient_set' else '[]'::jsonb end recipient_set,
      case when jsonb_typeof(x.value->'cc')='array'
        then x.value->'cc' else '[]'::jsonb end cc,
      case when jsonb_typeof(x.value->'bcc')='array'
        then x.value->'bcc' else '[]'::jsonb end bcc,
      jsonb_typeof(x.value->'expected_revision')='number'
        and coalesce(x.value->>'expected_revision','')~'^[0-9]+$'
        expected_revision_well_formed,
      (not (x.value ? 'allow_early')
        or jsonb_typeof(x.value->'allow_early')='boolean')
        allow_early_well_formed,
      (not (x.value ? 'deliver')
        or jsonb_typeof(x.value->'deliver')='boolean')
        deliver_well_formed
    from jsonb_array_elements(
      case when jsonb_typeof(p_requests)='array' then p_requests else '[]'::jsonb end)
      with ordinality x(value,ordinality)
    where jsonb_typeof(x.value)='object'
),
invoice_scope as materialized (
  select distinct r.invoice_id
  from raw r
  where r.invoice_id is not null
),
request_counts as materialized (
  select r.request_key,count(*)::integer request_key_count
  from raw r where r.request_key is not null group by r.request_key
),
invoice_ids as materialized (
  select coalesce(array_agg(r.invoice_id order by r.invoice_id),array[]::uuid[]) ids
  from invoice_scope r
),
references_batch as materialized (
  select r.*
  from invoice_ids i
  cross join lateral private._invoice_reference_rows_batch(i.ids) r
),
correction_scopes as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key',r.request_key,
    'scope_key',r.request_key,
    'invoice_id',r.invoice_id,
    'validation_purpose','ISSUE',
    'expected_client_id',i.client_id,
    'expected_invoice_stream',case when lower(coalesce(
      i.header_snapshot_json#>>'{meta,self_bill}','false'))
      in('true','t','1','yes') then 'SELF_BILL' else 'NORMAL' end,
    'planned_members',coalesce((select jsonb_agg(jsonb_build_object(
      'timesheet_id',l.timesheet_id,
      'vat_rate_pct',l.vat_rate_pct)
      order by l.timesheet_id,l.id)
      from public.invoice_lines l
      where l.invoice_id=r.invoice_id and l.timesheet_id is not null),
      '[]'::jsonb)) order by r.request_no),'[]'::jsonb) scopes
  from raw r
  left join public.invoices i on i.id=r.invoice_id
  where r.invoice_id is not null
),
corrections as materialized (
  select c.*
  from correction_scopes s
  cross join lateral private._invoice_correction_validate_batch(
    s.scopes,p_evaluation_date) c
),
routes as materialized (
  select d.*
  from private._invoice_delivery_routes_batch(
    (select coalesce(jsonb_agg(jsonb_build_object(
      'request_key',r.request_key,'invoice_id',r.invoice_id,
      'recipient_set',r.recipient_set,
      'cc',r.cc,'bcc',r.bcc)),'[]'::jsonb) from raw r),
    p_evaluation_date) d
),
line_totals as materialized (
  select r.invoice_id,count(l.id) line_count,
    round(coalesce(sum(l.total_charge_ex_vat),0),2) net,
    round(coalesce(sum(l.vat_amount),0),2) vat,
    round(coalesce(sum(l.total_inc_vat),0),2) gross,
    count(*) filter(where upper(coalesce(l.meta_json->>'line_type',''))
      like '%HIGHER_RATE%'
      and not exists(
        select 1 from public.invoice_hr_source_rows hs
        where hs.invoice_id=r.invoice_id
          and hs.source_system in('HEALTHROSTER','NHSP')))
      missing_higher_rate
  from invoice_scope r
  left join public.invoice_lines l on l.invoice_id=r.invoice_id
  group by r.invoice_id
),
source_requirements as materialized (
  select l.invoice_id,l.timesheet_id,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) like 'HOURS%'
      or upper(coalesce(l.meta_json->>'line_type',''))
        like 'ADDITIONAL_RATE%') reference_required,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) in(
        'EXPENSE_MILEAGE','MILEAGE')
      or coalesce(l.source_key,'') like '%:MILEAGE') mileage_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%TRAVEL%') travel_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%ACCOMMODATION%') accommodation_required,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) in(
        'EXPENSES_TOTAL','EXPENSE_TOTAL','OTHER_EXPENSE','EXPENSE_OTHER')
      or(
        upper(coalesce(l.meta_json->>'line_type','')) like 'EXPENSE_%'
        and upper(coalesce(l.meta_json->>'line_type','')) not in(
          'EXPENSE_MILEAGE','EXPENSE_TRAVEL','EXPENSE_ACCOMMODATION')))
      general_expense_required
  from public.invoice_lines l
  join invoice_scope s on s.invoice_id=l.invoice_id
  where l.timesheet_id is not null
  group by l.invoice_id,l.timesheet_id
),
timesheet_state as materialized (
  select s.invoice_id,s.timesheet_id,s.reference_required,
    s.mileage_required,s.travel_required,s.accommodation_required,
    s.general_expense_required,
    t.timesheet_id is not null source_exists,
    upper(coalesce(t.submission_mode::text,'')) submission_mode,
    coalesce(pc.effective_ts_attach_to_invoice,true)
      and not coalesce(summary.client_no_timesheet_required,false)
      and not coalesce(summary.client_is_nhsp,false) document_required,
    coalesce(pc.issue_missing_reference,false) issue_missing_reference,
    manual_source.evidence_id manual_source_evidence_id,
    coalesce(t.manual_document_asset_id,manual_source.document_asset_id)
      manual_document_asset_id,
    manual_asset.status manual_asset_status,
    manual_asset.operation_id manual_asset_operation_id,
    manual_asset_op.status manual_asset_operation_status,
    v.id timesheet_document_version_id,
    v.status timesheet_document_status,
    v.operation_id timesheet_document_operation_id,
    vop.status timesheet_document_operation_status,
    t.document_revision timesheet_document_revision,
    upper(coalesce(t.submission_mode::text,''))='QR'
      and(nullif(t.qr_signed_hash,'') is null
        or t.qr_signed_at_utc is null) qr_unsigned
  from source_requirements s
  left join public.timesheets t
    on t.timesheet_id=s.timesheet_id and t.is_current
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=s.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join lateral (
    select ev.id evidence_id,ev.document_asset_id
    from public.timesheet_evidence ev
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=ev.document_asset_id
    where ev.timesheet_id=t.timesheet_id
      and upper(coalesce(ev.kind,''))='TIMESHEET'
      and coalesce(ev.processing_state,'')<>'SUPERSEDED'
    order by(ev.document_asset_id=t.manual_document_asset_id) desc,
      (candidate_asset.status='READY') desc,
      ev.created_at desc nulls last,ev.id desc
    limit 1
  ) manual_source on true
  left join public.invoice_document_assets manual_asset
    on manual_asset.id=coalesce(t.manual_document_asset_id,
      manual_source.document_asset_id)
  left join public.invoice_operations manual_asset_op
    on manual_asset_op.id=manual_asset.operation_id
  left join lateral (
    select dv.*
    from public.invoice_document_versions dv
    where dv.entity_type='TIMESHEET' and dv.entity_id=t.timesheet_id
      and dv.purpose='TIMESHEET'
      and dv.source_revision=t.document_revision::text
      and dv.template_version='timesheet-professional-v1'
      and dv.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
        'VERIFYING','READY','FAILED','SUPERSEDED','CANCELLED')
    order by
      (dv.status='READY') desc,
      (dv.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING')) desc,
      dv.created_at_utc desc,dv.id desc
    limit 1
  ) v on true
  left join public.invoice_operations vop on vop.id=v.operation_id
),
timesheet_readiness as materialized (
  select t.*,
    case
      when not t.document_required then 'NOT_REQUIRED'
      when not t.source_exists then 'SOURCE_MISSING'
      when t.qr_unsigned then 'QR_UNSIGNED'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_document_asset_id is null then 'MANUAL_SOURCE_MISSING'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_document_asset_id is not null
        and t.manual_asset_status is null then 'ASSET_NOT_REGISTERED'
      when t.submission_mode in('MANUAL','QR') and(
        t.manual_asset_status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        or t.manual_asset_operation_status='DEAD_LETTER')
        then 'ASSET_PERMANENT_FAILURE'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
        and coalesce(t.manual_asset_operation_status,'') not in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        then 'ASSET_WORKFLOW_MISSING'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
        then 'ASSET_IN_PROGRESS'
      when t.timesheet_document_status='READY' then 'DOCUMENT_READY'
      when t.timesheet_document_status in('FAILED','SUPERSEDED','CANCELLED')
        and coalesce(t.timesheet_document_operation_status,'') not in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        then 'DOCUMENT_PERMANENT_FAILURE'
      when t.timesheet_document_status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
        or t.timesheet_document_operation_status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        then 'DOCUMENT_IN_PROGRESS'
      else 'DOCUMENT_CREATABLE'
    end readiness_classification
  from timesheet_state t
),
timesheet_readiness_json as materialized (
  select t.invoice_id,jsonb_agg(jsonb_build_object(
    'timesheet_id',t.timesheet_id,
    'required',t.document_required,
    'submission_mode',t.submission_mode,
    'readiness_classification',t.readiness_classification,
    'manual_source_evidence_id',t.manual_source_evidence_id,
    'manual_document_asset_id',t.manual_document_asset_id,
    'manual_asset_status',t.manual_asset_status,
    'manual_asset_operation_id',t.manual_asset_operation_id,
    'manual_asset_operation_status',t.manual_asset_operation_status,
    'timesheet_document_revision',t.timesheet_document_revision,
    'timesheet_document_version_id',t.timesheet_document_version_id,
    'timesheet_document_status',t.timesheet_document_status,
    'timesheet_document_operation_id',t.timesheet_document_operation_id,
    'timesheet_document_operation_status',t.timesheet_document_operation_status,
    'qr_unsigned',t.qr_unsigned)
    order by t.timesheet_id) readiness_rows
  from timesheet_readiness t
  group by t.invoice_id
),timesheet_checks as materialized (
  select i.invoice_id,
    count(*) filter(where t.document_required and not t.source_exists)
      missing_timesheet,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_document_asset_id is null) missing_manual_source,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_document_asset_id is not null
      and t.manual_asset_status is null) missing_asset_registration,
    count(*) filter(where t.document_required and t.qr_unsigned)
      unsigned_qr_source,
    count(*) filter(where t.reference_required
      and t.issue_missing_reference) missing_reference,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and(
        t.manual_asset_status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        or t.manual_asset_operation_status='DEAD_LETTER'))
      permanently_failed_required_asset,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
      and coalesce(t.manual_asset_operation_status,'') not in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      missing_asset_workflow,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
      and t.manual_asset_operation_status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      queueable_required_asset,
    count(*) filter(where t.document_required and t.source_exists
      and t.timesheet_document_status in(
        'FAILED','SUPERSEDED','CANCELLED')
      and coalesce(t.timesheet_document_operation_status,'') not in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      permanently_failed_timesheet_document,
    count(*) filter(where t.document_required and t.source_exists
      and coalesce(t.timesheet_document_status,'')<>'READY'
      and(
        t.timesheet_document_status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING')
        or t.timesheet_document_operation_status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        or(
          t.timesheet_document_version_id is null
          and t.submission_mode not in('MANUAL','QR'))
        or(
          t.timesheet_document_version_id is null
          and t.submission_mode in('MANUAL','QR')
          and t.manual_asset_status='READY')
      )) queueable_timesheet_document
  from invoice_scope i
  left join timesheet_readiness t on t.invoice_id=i.invoice_id
  group by i.invoice_id
),
import_source_requirements as materialized (
  select distinct s.invoice_id,s.timesheet_id,
    case
      when coalesce(summary.client_is_nhsp,false)
        or upper(coalesce(summary.route_type,'')) like '%NHSP%'
        or upper(coalesce(financial.basis::text,'')) like 'NHSP%'
        then 'NHSP'
      when upper(coalesce(summary.route_type,'')) like '%HEALTHROSTER%'
        or upper(coalesce(financial.basis::text,'')) in(
          'HEALTHROSTER','HEALTHROSTER_ADJUSTMENT','HR_WEEKLY','HR_DAILY')
        then 'HEALTHROSTER'
    end source_system,
    financial.nhsp_import_id import_id
  from source_requirements s
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join public.timesheets_financials financial
    on financial.timesheet_id=s.timesheet_id and financial.is_current
  join public.invoices invoice_policy on invoice_policy.id=s.invoice_id
  where (
      coalesce(summary.client_is_nhsp,false)
      or upper(coalesce(summary.route_type,'')) like '%NHSP%'
      or upper(coalesce(financial.basis::text,'')) like 'NHSP%'
    )
    or (
      (
        upper(coalesce(summary.route_type,'')) like '%HEALTHROSTER%'
        or upper(coalesce(financial.basis::text,'')) in(
          'HEALTHROSTER','HEALTHROSTER_ADJUSTMENT','HR_WEEKLY','HR_DAILY')
      )
      and coalesce(
        (invoice_policy.header_snapshot_json#>>'{attach_policy,hr_attach_to_invoice}')::boolean,
        true
      )
    )
),
import_source_checks as materialized (
  select i.invoice_id,
    count(*) filter(where r.source_system is not null and not exists(
      select 1
      from public.invoice_hr_source_rows source
      where source.invoice_id=r.invoice_id
        and upper(coalesce(source.source_system,''))=r.source_system
        and(r.import_id is null or source.import_id=r.import_id)
        and jsonb_typeof(source.rows_json)='array'
        and jsonb_array_length(source.rows_json)>0
    )) missing_import_source
  from invoice_scope i
  left join import_source_requirements r on r.invoice_id=i.invoice_id
  group by i.invoice_id
),
evidence_requirements as materialized (
  select s.invoice_id,s.timesheet_id,'MILEAGE'::text requirement
  from source_requirements s where s.mileage_required
  union all
  select s.invoice_id,s.timesheet_id,'TRAVEL'
  from source_requirements s where s.travel_required
  union all
  select s.invoice_id,s.timesheet_id,'ACCOMMODATION'
  from source_requirements s where s.accommodation_required
  union all
  select s.invoice_id,s.timesheet_id,'GENERAL_EXPENSE'
  from source_requirements s where s.general_expense_required
),
evidence_state as materialized (
  select r.*,e.id evidence_id,e.document_asset_id,
    a.status asset_status,op.status asset_operation_status
  from evidence_requirements r
  left join lateral (
    select ev.*
    from public.timesheet_evidence ev
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=ev.document_asset_id
    where ev.timesheet_id=r.timesheet_id
      and(
        upper(coalesce(ev.kind,''))=r.requirement
        or r.requirement='GENERAL_EXPENSE'
          and upper(coalesce(ev.kind,'')) in('OTHER','EXPENSE','EXPENSES'))
    order by
      (candidate_asset.status='READY') desc,
      (candidate_asset.status in(
        'DISCOVERED','INSPECTING','NORMALISING')) desc,
      ev.created_at desc nulls last,ev.id desc
    limit 1
  ) e on true
  left join public.invoice_document_assets a on a.id=e.document_asset_id
  left join public.invoice_operations op on op.id=a.operation_id
),
evidence_checks as materialized (
  select i.invoice_id,
    count(*) filter(where e.requirement='MILEAGE'
      and e.evidence_id is null) missing_mileage,
    count(*) filter(where e.requirement<>'MILEAGE'
      and e.evidence_id is null) missing_expense,
    count(*) filter(where e.evidence_id is not null
      and(e.document_asset_id is null or e.asset_status is null))
      missing_asset_registration,
    count(*) filter(where e.asset_status in(
      'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
      or e.asset_operation_status='DEAD_LETTER')
      permanently_failed_required_asset,
    count(*) filter(where e.asset_status in(
      'DISCOVERED','INSPECTING','NORMALISING')
      and e.asset_operation_status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      queueable_required_asset,
    count(*) filter(where e.asset_status in(
      'DISCOVERED','INSPECTING','NORMALISING')
      and coalesce(e.asset_operation_status,'') not in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      missing_asset_workflow
  from invoice_scope i
  left join evidence_state e on e.invoice_id=i.invoice_id
  group by i.invoice_id
),
reference_checks as materialized (
  select r.invoice_id,
    count(*) filter(where
      coalesce(precheck.reference_number_required_to_issue_invoice,false)
      and nullif(btrim(ref.current_reference),'') is null)
      missing_required_reference
  from invoice_scope r
  left join references_batch ref on ref.invoice_id=r.invoice_id
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=ref.timesheet_id
  group by r.invoice_id
),
correction_checks as materialized (
  select r.invoice_id,
    count(*) filter(where not coalesce(c.valid,true)) invalid_correction,
    (array_agg(c.blocker_code order by c.request_key) filter(where not coalesce(c.valid,true)))[1]
      correction_blocker
  from invoice_scope r
  left join corrections c on c.invoice_id=r.invoice_id
  group by r.invoice_id
),
facts as materialized (
  select r.request_no,r.request_key,r.operation_id,r.expected_revision,
    r.allow_early,r.deliver,r.expected_revision_well_formed,
    r.allow_early_well_formed,r.deliver_well_formed,
    r.invoice_id,
    r.recipient_set,r.cc,r.bcc,
    coalesce(rkc.request_key_count,0) request_key_count,
    i.status invoice_status,i.document_revision,i.on_hold_reason,
    i.subtotal_ex_vat,i.vat_amount,i.total_inc_vat,i.header_snapshot_json,
    lt.line_count,lt.net,lt.vat,lt.gross,
    ec.missing_mileage,ec.missing_expense,lt.missing_higher_rate,
    isc.missing_import_source,
    tc.missing_timesheet,tc.missing_manual_source,tc.unsigned_qr_source,
    tc.missing_reference,
    coalesce(tc.missing_asset_registration,0)
      +coalesce(ec.missing_asset_registration,0) missing_asset_registration,
    coalesce(tc.missing_asset_workflow,0)
      +coalesce(ec.missing_asset_workflow,0) missing_asset_workflow,
    coalesce(tc.permanently_failed_required_asset,0)
      +coalesce(ec.permanently_failed_required_asset,0)
      permanently_failed_required_asset,
    coalesce(tc.queueable_required_asset,0)
      +coalesce(ec.queueable_required_asset,0) queueable_required_asset,
    tc.permanently_failed_timesheet_document,
    tc.queueable_timesheet_document,rc.missing_required_reference,
    cc.invalid_correction,cc.correction_blocker,
    tr.readiness_rows timesheet_readiness,
    dr.canonical_to,dr.canonical_cc,dr.canonical_bcc,
    dr.recipient_set_hash,dr.grouping_identity,
    dr.route_policy_hash,
    dr.route_source,dr.warning_codes route_warnings,
    dr.blocker_codes route_blockers,dr.do_not_send,
    dr.delivery_suppressed,
    exists(
      select 1 from public.invoice_operation_chunks oc
      join public.invoice_operations other_operation
        on other_operation.id=oc.operation_id
      where oc.chunk_type='ISSUE_INVOICE' and oc.entity_type='INVOICE'
        and oc.entity_id=r.invoice_id
        and(r.operation_id is null or oc.operation_id<>r.operation_id)
        and oc.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and other_operation.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
      conflicting_issue
  from raw r
  left join request_counts rkc on rkc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join line_totals lt on lt.invoice_id=r.invoice_id
  left join timesheet_checks tc on tc.invoice_id=r.invoice_id
  left join import_source_checks isc on isc.invoice_id=r.invoice_id
  left join evidence_checks ec on ec.invoice_id=r.invoice_id
  left join reference_checks rc on rc.invoice_id=r.invoice_id
  left join correction_checks cc on cc.invoice_id=r.invoice_id
  left join timesheet_readiness_json tr on tr.invoice_id=r.invoice_id
  left join routes dr
    on dr.request_key=r.request_key and dr.invoice_id=r.invoice_id
),
classified as materialized (
  select f.*,
    array_remove(array[
      case when f.request_key is null then 'REQUEST_KEY_REQUIRED' end,
      case when f.request_key_count>1 then 'REQUEST_KEY_DUPLICATE' end,
      case when p_evaluation_date is null then 'EVALUATION_DATE_REQUIRED' end,
      case when f.invoice_status is null then 'INVOICE_NOT_FOUND' end,
      case when not f.expected_revision_well_formed
        then 'EXPECTED_REVISION_INVALID' end,
      case when not f.allow_early_well_formed
        then 'ALLOW_EARLY_INVALID' end,
      case when not f.deliver_well_formed
        then 'DELIVERY_INTENT_INVALID' end,
      case when f.invoice_status='ON_HOLD' or f.on_hold_reason is not null
        then 'INVOICE_ON_HOLD' end,
      case when f.invoice_status is not null and f.invoice_status<>'DRAFT'
        then 'INVOICE_NOT_DRAFT' end,
      case when f.expected_revision is not null
        and f.expected_revision<>f.document_revision
        then 'SOURCE_REVISION_CHANGED' end,
      case when coalesce(f.line_count,0)=0
        or round(coalesce(f.subtotal_ex_vat,0),2)<>coalesce(f.net,0)
        or round(coalesce(f.vat_amount,0),2)<>coalesce(f.vat,0)
        or round(coalesce(f.total_inc_vat,0),2)<>coalesce(f.gross,0)
        then 'INVALID_TOTALS' end,
      case when coalesce(f.missing_required_reference,0)>0
        or coalesce(f.missing_reference,0)>0 then 'MISSING_REFERENCE' end,
      case when coalesce(f.missing_timesheet,0)>0 then 'MISSING_TIMESHEET' end,
      case when coalesce(f.missing_manual_source,0)>0
        then 'MANUAL_TIMESHEET_SOURCE_MISSING' end,
      case when coalesce(f.unsigned_qr_source,0)>0
        then 'QR_TIMESHEET_UNSIGNED' end,
      case when coalesce(f.missing_asset_registration,0)>0
        then 'ASSET_NOT_REGISTERED' end,
      case when coalesce(f.missing_asset_workflow,0)>0
        then 'ASSET_WORKFLOW_MISSING' end,
      case when coalesce(f.permanently_failed_required_asset,0)>0
        then 'REQUIRED_ASSET_FAILED' end,
      case when coalesce(f.permanently_failed_timesheet_document,0)>0
        then 'TIMESHEET_DOCUMENT_FAILED' end,
      case when coalesce(f.missing_mileage,0)>0
        then 'MISSING_MILEAGE_EVIDENCE' end,
      case when coalesce(f.missing_expense,0)>0
        then 'MISSING_EXPENSE_EVIDENCE' end,
      case when coalesce(f.missing_higher_rate,0)>0
        then 'MISSING_HIGHER_RATE_SUPPORT' end,
      case when coalesce(f.missing_import_source,0)>0
        then 'MISSING_IMPORT_SOURCE_EVIDENCE' end,
      case when coalesce(f.invalid_correction,0)>0
        then coalesce(f.correction_blocker,'CORRECTION_LINES_NOT_UNIT_SAFE') end,
      case when not f.allow_early
        and coalesce(
          case when coalesce(f.header_snapshot_json#>>'{meta,invoice_week_start}','')
            ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then (f.header_snapshot_json#>>'{meta,invoice_week_start}')::date+6
          end,p_evaluation_date-1)
          >=p_evaluation_date
        then 'EARLY_ISSUE_NOT_ALLOWED' end,
      case when f.conflicting_issue then 'CONFLICTING_ISSUE_OPERATION' end
    ],null)::text[] blockers
  from facts f
)
select c.request_key,c.invoice_id,
  to_jsonb(c.blockers),
  (case when coalesce(c.queueable_timesheet_document,0)>0
      then jsonb_build_array('TIMESHEET_DOCUMENT_NOT_READY')
    else '[]'::jsonb end)
  ||(case when coalesce(c.queueable_required_asset,0)>0
      then jsonb_build_array('REQUIRED_ASSET_NOT_READY')
    else '[]'::jsonb end),
  case when c.deliver
    then coalesce(c.route_blockers,'[]'::jsonb) else '[]'::jsonb end,
  coalesce(c.route_warnings,'[]'::jsonb),
  cardinality(c.blockers)=0,
  cardinality(c.blockers)=0
    and(not c.deliver or jsonb_array_length(
      coalesce(c.route_blockers,'[]'::jsonb))=0),
  jsonb_build_object(
    'request_key',c.request_key,
    'invoice_id',c.invoice_id,
    'route_policy_hash',c.route_policy_hash,
    'recipient_set_hash',c.recipient_set_hash,
    'grouping_identity',c.grouping_identity,
    'route_source',c.route_source,
    'do_not_send',c.do_not_send,
    'delivery_suppressed',c.delivery_suppressed,
    'canonical_to',c.canonical_to,
    'canonical_cc',c.canonical_cc,
    'canonical_bcc',c.canonical_bcc,
    'warnings',coalesce(c.route_warnings,'[]'::jsonb),
    'blockers',coalesce(c.route_blockers,'[]'::jsonb)),
  jsonb_build_object(
    'request_key',c.request_key,
    'evaluation_date',p_evaluation_date,
    'expected_revision',c.expected_revision,
    'current_revision',c.document_revision,
    'net_expected',c.subtotal_ex_vat,'net_lines',c.net,
    'vat_expected',c.vat_amount,'vat_lines',c.vat,
    'gross_expected',c.total_inc_vat,'gross_lines',c.gross,
    'hard_blockers',to_jsonb(c.blockers),
    'timesheet_readiness',coalesce(c.timesheet_readiness,'[]'::jsonb),
    'document_dependency_codes',
      (case when coalesce(c.queueable_timesheet_document,0)>0
          then jsonb_build_array('TIMESHEET_DOCUMENT_NOT_READY')
        else '[]'::jsonb end)
      ||(case when coalesce(c.queueable_required_asset,0)>0
          then jsonb_build_array('REQUIRED_ASSET_NOT_READY')
        else '[]'::jsonb end),
    'queueable_asset_count',coalesce(c.queueable_required_asset,0),
    'missing_import_source_count',coalesce(c.missing_import_source,0),
    'missing_asset_workflow_count',coalesce(c.missing_asset_workflow,0),
    'queueable_timesheet_document_count',
      coalesce(c.queueable_timesheet_document,0),
    'delivery_blockers',case when c.deliver
      then coalesce(c.route_blockers,'[]'::jsonb) else '[]'::jsonb end,
    'can_issue_only',cardinality(c.blockers)=0,
    'can_issue_and_deliver',cardinality(c.blockers)=0
      and(not c.deliver or jsonb_array_length(
        coalesce(c.route_blockers,'[]'::jsonb))=0),
    'recipient_set_hash',c.recipient_set_hash,
    'route_policy_hash',c.route_policy_hash,
    'grouping_identity',c.grouping_identity,
    'recipient_to',c.canonical_to,
    'recipient_cc',c.canonical_cc,
    'recipient_bcc',c.canonical_bcc,
    'route_source',c.route_source,
    'do_not_send',c.do_not_send,
    'delivery_suppressed',c.delivery_suppressed)
from classified c
order by c.request_no;
$function$
;

-- SOURCE public.cloudtms_data_api_mfa_gate()
CREATE OR REPLACE FUNCTION public.cloudtms_data_api_mfa_gate()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_jwt_raw text;
  v_jwt jsonb := '{}'::jsonb;
  v_role text := '';
  v_aal text := 'aal1';
  v_sub text := '';
begin
  v_jwt_raw := coalesce(
    nullif(current_setting('request.jwt', true), ''),
    nullif(current_setting('request.jwt.claims', true), ''),
    '{}'
  );

  begin
    v_jwt := v_jwt_raw::jsonb;
  exception when others then
    v_jwt := '{}'::jsonb;
  end;

  v_role := coalesce(nullif(v_jwt->>'role', ''), current_user, '');
  v_aal := coalesce(nullif(v_jwt->>'aal', ''), 'aal1');
  v_sub := coalesce(nullif(v_jwt->>'sub', ''), '');

  -- Backend/system path must not be blocked.
  if v_role = 'service_role' or current_user = 'service_role' then
    return;
  end if;

  -- Normal MFA-complete Supabase user.
  if v_role = 'authenticated' and v_aal = 'aal2' then
    return;
  end if;

  -- Known TMS/Codex/system user exemption only.
  if v_role = 'authenticated'
     and v_sub = '42f1f62c-7d11-437e-85b0-7b135be865e3' then
    return;
  end if;

  if v_role = 'anon' or v_role = '' or v_jwt = '{}'::jsonb then
    raise sqlstate 'PGRST'
      using
        message = json_build_object(
          'code', 'AUTH_REQUIRED',
          'message', 'Authentication required',
          'details', 'CloudTMS Data API/RPC access requires a logged-in user.',
          'hint', 'Login before calling the CloudTMS Data API/RPC.'
        )::text,
        detail = json_build_object(
          'status', 401,
          'status_text', 'Unauthorized'
        )::text;
  end if;

  raise sqlstate 'PGRST'
    using
      message = json_build_object(
        'code', 'MFA_REQUIRED',
        'message', 'MFA required',
        'details', 'CloudTMS Data API/RPC access requires an aal2 MFA-complete session.',
        'hint', 'Complete MFA before calling the CloudTMS Data API/RPC.'
      )::text,
      detail = json_build_object(
        'status', 403,
        'status_text', 'Forbidden'
      )::text;
end;
$function$
;

-- SOURCE public.id_ledger_list(p_limit integer, p_offset integer, p_status text[], p_client_id uuid, p_search text, p_only_reportable boolean)
CREATE OR REPLACE FUNCTION public.id_ledger_list(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_status text[] DEFAULT NULL::text[], p_client_id uuid DEFAULT NULL::uuid, p_search text DEFAULT NULL::text, p_only_reportable boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_limit int := greatest(1, least(coalesce(p_limit, 50), 500));
  v_offset int := greatest(coalesce(p_offset, 0), 0);

  v_statuses text[] := null;
  v_search text := nullif(btrim(coalesce(p_search, '')), '');

  v_total_count int := 0;
  v_rows jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  -- Normalise status filter to uppercase trimmed values (ignore blanks)
  if p_status is not null then
    select
      array_agg(upper(btrim(x)) order by upper(btrim(x)))
    into v_statuses
    from unnest(p_status) as x
    where nullif(btrim(coalesce(x, '')), '') is not null;

    if v_statuses is not null and array_length(v_statuses, 1) = 0 then
      v_statuses := null;
    end if;
  end if;

  with base as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      l.current_ex_vat,
      l.current_vat,
      l.current_inc_vat,

      l.last_reported_ex_vat,
      l.last_reported_vat,
      l.last_reported_inc_vat,

      l.updated_at_utc,

      i.client_id as client_id,
      c.name as client_name,

      i.issued_at_utc,
      i.due_at_utc,
      i.paid_at_utc,
      i.status_date_utc,
      i.credit_note_created_at_utc,

      coalesce(nullif(btrim(coalesce(l.invoice_number, '')), ''), i.invoice_no) as effective_invoice_number,
      coalesce(nullif(btrim(coalesce(l.invoice_status, '')), ''), i.status::text) as effective_invoice_status,
      coalesce(nullif(btrim(coalesce(l.invoice_type, '')), ''), i.type::text) as effective_invoice_type,

      coalesce(i.issued_at_utc, i.status_date_utc, l.updated_at_utc) as sort_ts
    from public.id_invoice_ledger l
    left join public.invoices i
      on i.id = l.invoice_id
    left join public.clients c
      on c.id = i.client_id
  ),
  calc as (
    select
      b.*,

      (upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD') as is_on_hold,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_ex_vat, 0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_vat, 0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_inc_vat, 0)::numeric(12,2)
      end) as reportable_current_inc_vat
    from base b
  ),
  filtered as (
    select
      c.*,

      (c.reportable_current_ex_vat - coalesce(c.last_reported_ex_vat, 0)::numeric(12,2))::numeric(12,2) as delta_ex_vat,
      (c.reportable_current_vat - coalesce(c.last_reported_vat, 0)::numeric(12,2))::numeric(12,2) as delta_vat,
      (c.reportable_current_inc_vat - coalesce(c.last_reported_inc_vat, 0)::numeric(12,2))::numeric(12,2) as delta_inc_vat,

      (case when c.is_on_hold then 'NON_REPORTABLE' else 'REPORTABLE' end) as line_kind,
      (case when c.is_on_hold then 'ON_HOLD' else null end) as non_reportable_reason
    from calc c
    where
      (v_statuses is null or upper(coalesce(c.effective_invoice_status, '')) = any(v_statuses))
      and (p_client_id is null or c.client_id = p_client_id)
      and (
        v_search is null
        or coalesce(c.effective_invoice_number, '') ilike ('%' || v_search || '%')
        or coalesce(c.client_name, '') ilike ('%' || v_search || '%')
      )
      and (
        coalesce(p_only_reportable, false) = false
        or upper(coalesce(c.effective_invoice_status, '')) <> 'ON_HOLD'
      )
  ),
  total as (
    select count(*)::int as total_count
    from filtered f
  ),
  page as (
    select
      f.*
    from filtered f
    order by
      f.sort_ts desc nulls last,
      nullif(btrim(coalesce(f.effective_invoice_number, '')), '') desc nulls last,
      f.invoice_id desc
    limit v_limit offset v_offset
  )
  select
    coalesce((select t.total_count from total t), 0),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', p.invoice_id::text,
          'invoice_number', p.effective_invoice_number,
          'invoice_status', p.effective_invoice_status,
          'invoice_type', p.effective_invoice_type,

          'client_id', case when p.client_id is null then null else p.client_id::text end,
          'client_name', p.client_name,

          'issued_at_utc', p.issued_at_utc,
          'due_at_utc', p.due_at_utc,
          'paid_at_utc', p.paid_at_utc,
          'status_date_utc', p.status_date_utc,
          'credit_note_created_at_utc', p.credit_note_created_at_utc,

          'updated_at_utc', p.updated_at_utc,

          'current_ex_vat', coalesce(p.current_ex_vat, 0)::numeric(12,2),
          'current_vat', coalesce(p.current_vat, 0)::numeric(12,2),
          'current_inc_vat', coalesce(p.current_inc_vat, 0)::numeric(12,2),

          'last_reported_ex_vat', coalesce(p.last_reported_ex_vat, 0)::numeric(12,2),
          'last_reported_vat', coalesce(p.last_reported_vat, 0)::numeric(12,2),
          'last_reported_inc_vat', coalesce(p.last_reported_inc_vat, 0)::numeric(12,2),

          'reportable_current_ex_vat', p.reportable_current_ex_vat,
          'reportable_current_vat', p.reportable_current_vat,
          'reportable_current_inc_vat', p.reportable_current_inc_vat,

          'delta_ex_vat', p.delta_ex_vat,
          'delta_vat', p.delta_vat,
          'delta_inc_vat', p.delta_inc_vat,

          'line_kind', p.line_kind,
          'non_reportable_reason', p.non_reportable_reason
        )
        order by
          p.sort_ts desc nulls last,
          nullif(btrim(coalesce(p.effective_invoice_number, '')), '') desc nulls last,
          p.invoice_id desc
      ),
      '[]'::jsonb
    )
  into v_total_count, v_rows
  from page p;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total_count,
    'limit', v_limit,
    'offset', v_offset,
    'rows', v_rows
  );
end;
$function$
;

-- SOURCE public.pay_loans_snoozes_list(p_candidate_id uuid, p_client_id uuid, p_include_paid_off boolean, p_include_cleared_snoozes boolean)
CREATE OR REPLACE FUNCTION public.pay_loans_snoozes_list(p_candidate_id uuid DEFAULT NULL::uuid, p_client_id uuid DEFAULT NULL::uuid, p_include_paid_off boolean DEFAULT true, p_include_cleared_snoozes boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_today_uk date := (now() at time zone 'Europe/London')::date;
  v_finance_cases jsonb := '[]'::jsonb;
  v_timesheet_snoozes jsonb := '[]'::jsonb;
  v_summary jsonb := '{}'::jsonb;
begin
  with finance_rows as (
    select
      vfcr.finance_case_id,
      vfcr.case_type,
      vfcr.candidate_id,
      vfcr.candidate_tms_ref,
      vfcr.candidate_display_name,
      vfcr.pay_method,
      vfcr.client_id,
      vfcr.client_name,
      vfcr.status,
      vfcr.payout_status,
      vfcr.payout_pay_batch_id,
      vfcr.payout_transfer_id,
      round(coalesce(vfcr.original_amount, 0), 2) as original_amount,
      round(coalesce(vfcr.outstanding_amount, 0), 2) as outstanding_amount,
      round(coalesce(vfcr.weekly_due, 0), 2) as weekly_due,
      vfcr.weeks_total,
      vfcr.start_week_start,
      vfcr.next_due_week_start,
      vfcr.schedule_json,
      vfcr.adjustment_comment,
      vfcr.notes,
      vfcr.linked_timesheet_id,
      vfcr.linked_shift_date,
      vfcr.source_original_paid_amount,
      vfcr.source_corrected_paid_amount,
      vfcr.minimum_earnings_threshold,
      vfcr.take_home_floor_override,
      vfcr.written_off_at_utc,
      vfcr.write_off_reason,
      vfcr.cleared_at_utc,
      vfcr.active_reserved_amount,
      vfcr.reserved_amount,
      vfcr.committed_amount,
      vfcr.settled_amount,
      vfcr.released_amount,
      vfcr.active_reservation_count,
      vfcr.latest_recovery_pay_batch_id,
      vfcr.latest_recovery_pay_date,
      vfcr.latest_remittance_sent_at_utc,
      vfcr.latest_remittance_trigger_status,
      vfcr.last_remittance_error,
      vfcr.active_snooze_id,
      vfcr.active_snooze_kind,
      vfcr.active_snooze_until_date,
      vfcr.active_snooze_note,
      vfcr.active_snooze_created_at_utc,
      vfcr.active_snooze_updated_at_utc,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then false
        else coalesce(vfcr.is_mixed_case, false)
      end as is_mixed_case,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then 0
        else coalesce(vfcr.open_taxable_count, 0)
      end as open_taxable_count,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then 0
        else coalesce(vfcr.open_reimbursement_count, 0)
      end as open_reimbursement_count,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then 0
        else coalesce(vfcr.unresolved_taxable_count, 0)
      end as unresolved_taxable_count,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then 0
        else coalesce(vfcr.stale_count, 0)
      end as stale_count,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then jsonb_build_object(
          'open_taxable_count', 0,
          'open_reimbursement_count', 0,
          'unresolved_taxable_count', 0,
          'stale_count', 0,
          'is_mixed_case', false
        )
        else coalesce(vfcr.component_resolution_summary_json, '{}'::jsonb)
      end as component_resolution_summary_json,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then 'Payment Advance'
        when vfcr.case_type = 'OVERPAYMENT' then 'Overpayment'
        when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then 'Manual Debt Adjustment'
        when vfcr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then 'Manual Credit Adjustment'
        when vfcr.case_type = 'UNDERPAYMENT' then 'Underpayment'
        else vfcr.case_type::text
      end as admin_label,
      case
        when vfcr.active_snooze_id is null then 'NOT_SNOOZED'
        when vfcr.active_snooze_until_date is null then 'INDEFINITE_SNOOZE'
        else 'DATED_SNOOZE'
      end as snooze_state,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then 'READY'
        when coalesce(vfcr.unresolved_taxable_count, 0) > 0 and coalesce(vfcr.stale_count, 0) > 0 then 'BLOCKED_STALE_TAXABLE_COMPONENTS'
        when coalesce(vfcr.unresolved_taxable_count, 0) > 0 then 'BLOCKED_UNRESOLVED_TAXABLE_COMPONENTS'
        else 'READY'
      end as blocked_state,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then null
        when coalesce(vfcr.unresolved_taxable_count, 0) > 0 and coalesce(vfcr.stale_count, 0) > 0 then 'One or more taxable components are stale and must be re-resolved before the case can proceed.'
        when coalesce(vfcr.unresolved_taxable_count, 0) > 0 then 'One or more taxable components are unresolved and must be resolved before the case can proceed.'
        else null
      end as blocked_reason,
      case
        when vfcr.case_type in ('PAYMENT_ADVANCE', 'MANUAL_DEBT_ADJUSTMENT')
         and round(coalesce(vfcr.outstanding_amount, 0), 2) > 0
         and vfcr.written_off_at_utc is null
        then true else false
      end as can_restructure,
      case
        when vfcr.case_type in ('PAYMENT_ADVANCE', 'OVERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT')
         and vfcr.status = 'ACTIVE'
         and round(coalesce(vfcr.outstanding_amount, 0), 2) > 0
         and vfcr.written_off_at_utc is null
        then true else false
      end as can_pause,
      case
        when vfcr.case_type in ('PAYMENT_ADVANCE', 'OVERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT')
         and vfcr.status = 'PAUSED'
         and round(coalesce(vfcr.outstanding_amount, 0), 2) > 0
         and vfcr.written_off_at_utc is null
        then true else false
      end as can_resume,
      case
        when vfcr.case_type in ('PAYMENT_ADVANCE', 'OVERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT')
         and round(coalesce(vfcr.outstanding_amount, 0), 2) > 0
         and vfcr.written_off_at_utc is null
        then true else false
      end as can_write_off,
      case
        when vfcr.case_type = 'PAYMENT_ADVANCE' then coalesce(vfcr.payout_status, 'PENDING') in ('PENDING', 'CANCELLED')
        when vfcr.case_type = 'MANUAL_CREDIT_ADJUSTMENT' then coalesce(vfcr.payout_status, 'PENDING') in ('PENDING', 'CANCELLED')
        when vfcr.case_type = 'MANUAL_DEBT_ADJUSTMENT' then true
        else false
      end as can_edit,
      case when vfcr.active_snooze_id is not null then true else false end as can_unsnooze
    from public.v_finance_cases_register vfcr
    where (p_candidate_id is null or vfcr.candidate_id = p_candidate_id)
      and (p_client_id is null or vfcr.client_id = p_client_id)
      and (
        p_include_paid_off = true
        or vfcr.case_type = 'MANUAL_CREDIT_ADJUSTMENT'
        or (vfcr.status <> 'PAID_OFF' and coalesce(vfcr.outstanding_amount, 0) > 0)
      )
  ),
  finance_payload_rows as (
    select
      fr.candidate_display_name,
      fr.admin_label,
      fr.finance_case_id,
      jsonb_strip_nulls(
        jsonb_build_object(
          'finance_case_id', fr.finance_case_id::text,
          'case_type', fr.case_type::text,
          'admin_label', fr.admin_label,
          'candidate_id', fr.candidate_id::text,
          'candidate_tms_ref', fr.candidate_tms_ref,
          'candidate_display_name', fr.candidate_display_name,
          'pay_method', fr.pay_method,
          'client_id', case when fr.client_id is null then null else fr.client_id::text end,
          'client_name', fr.client_name,
          'status', fr.status::text,
          'payout_status', case when fr.payout_status is null then null else fr.payout_status::text end,
          'blocked_state', fr.blocked_state,
          'blocked_reason', fr.blocked_reason
        )
        ||
        jsonb_build_object(
          'payout_pay_batch_id', case when fr.payout_pay_batch_id is null then null else fr.payout_pay_batch_id::text end,
          'payout_transfer_id', case when fr.payout_transfer_id is null then null else fr.payout_transfer_id::text end,
          'original_amount', fr.original_amount,
          'outstanding_amount', fr.outstanding_amount,
          'weekly_due', fr.weekly_due,
          'weeks_total', fr.weeks_total,
          'start_week_start', case when fr.start_week_start is null then null else fr.start_week_start::text end,
          'next_due_week_start', case when fr.next_due_week_start is null then null else fr.next_due_week_start::text end,
          'schedule_json', coalesce(fr.schedule_json, '[]'::jsonb),
          'adjustment_comment', fr.adjustment_comment,
          'notes', fr.notes,
          'linked_timesheet_id', case when fr.linked_timesheet_id is null then null else fr.linked_timesheet_id::text end,
          'linked_shift_date', case when fr.linked_shift_date is null then null else fr.linked_shift_date::text end
        )
        ||
        jsonb_build_object(
          'source_original_paid_amount', fr.source_original_paid_amount,
          'source_corrected_paid_amount', fr.source_corrected_paid_amount,
          'minimum_earnings_threshold', fr.minimum_earnings_threshold,
          'take_home_floor_override', fr.take_home_floor_override,
          'written_off_at_utc', case when fr.written_off_at_utc is null then null else fr.written_off_at_utc::text end,
          'write_off_reason', fr.write_off_reason,
          'cleared_at_utc', case when fr.cleared_at_utc is null then null else fr.cleared_at_utc::text end,
          'active_reserved_amount', coalesce(fr.active_reserved_amount, 0),
          'reserved_amount', coalesce(fr.reserved_amount, 0),
          'committed_amount', coalesce(fr.committed_amount, 0),
          'settled_amount', coalesce(fr.settled_amount, 0),
          'released_amount', coalesce(fr.released_amount, 0),
          'active_reservation_count', coalesce(fr.active_reservation_count, 0)
        )
        ||
        jsonb_build_object(
          'latest_recovery_pay_batch_id', case when fr.latest_recovery_pay_batch_id is null then null else fr.latest_recovery_pay_batch_id::text end,
          'latest_recovery_pay_date', case when fr.latest_recovery_pay_date is null then null else fr.latest_recovery_pay_date::text end,
          'latest_remittance_sent_at_utc', case when fr.latest_remittance_sent_at_utc is null then null else fr.latest_remittance_sent_at_utc::text end,
          'latest_remittance_trigger_status', fr.latest_remittance_trigger_status,
          'last_remittance_error', fr.last_remittance_error,
          'is_mixed_case', fr.is_mixed_case,
          'open_taxable_count', fr.open_taxable_count,
          'open_reimbursement_count', fr.open_reimbursement_count,
          'unresolved_taxable_count', fr.unresolved_taxable_count,
          'stale_count', fr.stale_count,
          'component_resolution_summary_json', fr.component_resolution_summary_json,
          'snooze', case
            when fr.active_snooze_id is null then null
            else jsonb_build_object(
              'snooze_id', fr.active_snooze_id::text,
              'snooze_kind', fr.active_snooze_kind,
              'snooze_until_date', case when fr.active_snooze_until_date is null then null else fr.active_snooze_until_date::text end,
              'snooze_note', fr.active_snooze_note,
              'snooze_created_at_utc', case when fr.active_snooze_created_at_utc is null then null else fr.active_snooze_created_at_utc::text end,
              'snooze_updated_at_utc', case when fr.active_snooze_updated_at_utc is null then null else fr.active_snooze_updated_at_utc::text end,
              'snooze_state', fr.snooze_state
            )
          end,
          'action_flags', jsonb_build_object(
            'can_restructure', fr.can_restructure,
            'can_pause', fr.can_pause,
            'can_resume', fr.can_resume,
            'can_write_off', fr.can_write_off,
            'can_edit', fr.can_edit,
            'can_unsnooze', fr.can_unsnooze
          )
        )
      ) as row_json
    from finance_rows fr
  ),
  finance_json as (
    select
      coalesce(
        jsonb_agg(
          fpr.row_json
          order by fpr.candidate_display_name asc, fpr.admin_label asc, fpr.finance_case_id asc
        ),
        '[]'::jsonb
      ) as payload
    from finance_payload_rows fpr
  ),
  pay_item_snooze_source as (
    select
      pis.id as snooze_id,
      pis.candidate_id,
      c.tms_ref as candidate_tms_ref,
      c.display_name as candidate_display_name,
      pis.timesheet_id as stored_timesheet_id,
      coalesce(pis.booking_id, ts_stored.booking_id) as booking_id,
      pis.segment_id as stored_segment_id,
      coalesce(nullif(btrim(coalesce(pis.segment_stable_key, '')), ''), nullif(btrim(coalesce(pis.segment_id, '')), '')) as segment_stable_key,
      upper(coalesce(pis.snooze_kind, '')) as snooze_kind,
      pis.snooze_until_date,
      pis.note,
      pis.created_at_utc,
      pis.updated_at_utc,
      pis.cleared_at_utc,
      pis.created_by_user_id,
      pis.updated_by_user_id
    from public.pay_item_snoozes pis
    join public.candidates c
      on c.id = pis.candidate_id
    left join public.timesheets ts_stored
      on ts_stored.timesheet_id = pis.timesheet_id
    where pis.source_ref is null
      and (p_candidate_id is null or pis.candidate_id = p_candidate_id)
      and (
        coalesce(pis.booking_id, ts_stored.booking_id) is not null
        or pis.timesheet_id is not null
      )
  ),
  current_timesheet_context as (
    select distinct
      pss.booking_id,
      ts_current.timesheet_id as current_timesheet_id,
      tf_current.client_id as client_id,
      cl_current.name as client_name,
      tf_current.role as role,
      tf_current.band as band,
      ts_current.week_ending_date as week_ending_date,
      ts_current.reference_number as reference_number,
      tf_current.invoice_breakdown_json as invoice_breakdown_json,
      round(coalesce(tf_current.total_pay_ex_vat, 0), 2) as total_pay_ex_vat
    from pay_item_snooze_source pss
    join public.timesheets ts_current
      on ts_current.booking_id = pss.booking_id
     and ts_current.is_current = true
    left join public.timesheets_financials tf_current
      on tf_current.timesheet_id = ts_current.timesheet_id
     and tf_current.is_current = true
    left join public.clients cl_current
      on cl_current.id = tf_current.client_id
  ),
  current_timesheet_segments_actual as (
    select
      ctc.current_timesheet_id,
      ctc.booking_id,
      nullif(btrim(coalesce(seg_item.value->>'segment_id', '')), '') as segment_id,
      coalesce(
        nullif(btrim(coalesce(seg_item.value->>'segment_stable_key', '')), ''),
        nullif(btrim(coalesce(seg_item.value->>'segment_id', '')), ''),
        nullif(btrim(coalesce(seg_item.value->>'segment_key', '')), ''),
        nullif(btrim(coalesce(seg_item.value->>'date', '')), ''),
        nullif(btrim(coalesce(seg_item.value->>'ref_num', '')), '')
      ) as segment_stable_key,
      nullif(btrim(coalesce(seg_item.value->>'date', '')), '') as work_date,
      coalesce(nullif(btrim(coalesce(seg_item.value->>'client_name', '')), ''), ctc.client_name) as client_name,
      coalesce(nullif(btrim(coalesce(seg_item.value->>'role', '')), ''), ctc.role) as role,
      coalesce(nullif(btrim(coalesce(seg_item.value->>'band', '')), ''), ctc.band) as band,
      coalesce(
        nullif(btrim(coalesce(seg_item.value->>'start', '')), ''),
        nullif(btrim(coalesce(seg_item.value->>'start_hhmm', '')), '')
      ) as start_time,
      coalesce(
        nullif(btrim(coalesce(seg_item.value->>'end', '')), ''),
        nullif(btrim(coalesce(seg_item.value->>'end_hhmm', '')), '')
      ) as finish_time,
      nullif(btrim(coalesce(seg_item.value->>'break_start', '')), '') as break_start,
      nullif(btrim(coalesce(seg_item.value->>'break_end', '')), '') as break_end,
      coalesce(
        nullif(seg_item.value->>'break_mins', '')::numeric,
        nullif(seg_item.value->>'break_minutes', '')::numeric
      ) as break_mins,
      round(coalesce(nullif(seg_item.value->>'pay_amount', '')::numeric, 0), 2) as pay_amount_ex_vat,
      nullif(btrim(coalesce(seg_item.value->>'ref_num', '')), '') as ref_num
    from current_timesheet_context ctc
    join lateral jsonb_array_elements(
      case
        when ctc.invoice_breakdown_json is not null
         and jsonb_typeof(ctc.invoice_breakdown_json) = 'object'
         and jsonb_typeof(ctc.invoice_breakdown_json->'segments') = 'array'
        then ctc.invoice_breakdown_json->'segments'
        else '[]'::jsonb
      end
    ) as seg_item(value)
      on true
    where seg_item.value is not null
      and jsonb_typeof(seg_item.value) = 'object'
  ),
  current_timesheet_segments as (
    select
      ctsa.current_timesheet_id,
      ctsa.booking_id,
      ctsa.segment_id,
      ctsa.segment_stable_key,
      ctsa.work_date,
      ctsa.client_name,
      ctsa.role,
      ctsa.band,
      ctsa.start_time,
      ctsa.finish_time,
      ctsa.break_start,
      ctsa.break_end,
      ctsa.break_mins,
      ctsa.pay_amount_ex_vat,
      ctsa.ref_num
    from current_timesheet_segments_actual ctsa

    union all

    select
      ctc.current_timesheet_id,
      ctc.booking_id,
      null::text as segment_id,
      ('timesheet:' || coalesce(ctc.booking_id, ctc.current_timesheet_id::text)) as segment_stable_key,
      null::text as work_date,
      ctc.client_name,
      ctc.role,
      ctc.band,
      null::text as start_time,
      null::text as finish_time,
      null::text as break_start,
      null::text as break_end,
      null::numeric as break_mins,
      round(coalesce(ctc.total_pay_ex_vat, 0), 2) as pay_amount_ex_vat,
      ctc.reference_number as ref_num
    from current_timesheet_context ctc
    where not exists (
      select 1
      from current_timesheet_segments_actual ctsa
      where ctsa.current_timesheet_id = ctc.current_timesheet_id
    )
  ),
  current_timesheet_segment_groups as (
    select
      cts.current_timesheet_id,
      count(*)::int as segment_count,
      round(coalesce(sum(cts.pay_amount_ex_vat), 0), 2) as total_segment_pay_ex_vat,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'segment_id', cts.segment_id,
            'segment_stable_key', cts.segment_stable_key,
            'date', cts.work_date,
            'client_name', cts.client_name,
            'role', cts.role,
            'band', cts.band,
            'start', cts.start_time,
            'finish', cts.finish_time,
            'break_start', cts.break_start,
            'break_end', cts.break_end,
            'break_mins', cts.break_mins,
            'pay_amount_ex_vat', cts.pay_amount_ex_vat,
            'ref_num', cts.ref_num
          )
          order by cts.work_date asc nulls last, cts.start_time asc nulls last, cts.segment_stable_key asc
        ),
        '[]'::jsonb
      ) as segment_rows_json
    from current_timesheet_segments cts
    group by cts.current_timesheet_id
  ),
  timesheet_snooze_rows_raw as (
    select
      pss.snooze_id,
      pss.candidate_id,
      pss.candidate_tms_ref,
      pss.candidate_display_name,
      pss.stored_timesheet_id,
      pss.booking_id,
      pss.stored_segment_id,
      pss.segment_stable_key,
      pss.snooze_kind,
      pss.snooze_until_date,
      pss.note,
      pss.created_at_utc,
      pss.updated_at_utc,
      pss.cleared_at_utc,
      pss.created_by_user_id,
      pss.updated_by_user_id,
      case when pss.segment_stable_key is null then 'WHOLE_TIMESHEET' else 'SEGMENT' end as row_kind,
      ctc.current_timesheet_id,
      ctc.client_id as current_client_id,
      ctc.client_name as current_client_name,
      ctc.role as current_role,
      ctc.band as current_band,
      ctc.week_ending_date as current_week_ending_date,
      ctc.reference_number as current_reference_number,
      cts.segment_id as current_segment_id,
      cts.segment_stable_key as current_segment_stable_key,
      cts.work_date as current_segment_date,
      cts.client_name as current_segment_client_name,
      cts.role as current_segment_role,
      cts.band as current_segment_band,
      cts.start_time as current_segment_start,
      cts.finish_time as current_segment_finish,
      cts.break_start as current_segment_break_start,
      cts.break_end as current_segment_break_end,
      cts.break_mins as current_segment_break_mins,
      cts.pay_amount_ex_vat as current_segment_pay_amount_ex_vat,
      cts.ref_num as current_segment_ref_num,
      ctsg.segment_count,
      ctsg.total_segment_pay_ex_vat,
      ctsg.segment_rows_json
    from pay_item_snooze_source pss
    left join current_timesheet_context ctc
      on ctc.booking_id = pss.booking_id
    left join current_timesheet_segments cts
      on cts.current_timesheet_id = ctc.current_timesheet_id
     and pss.segment_stable_key is not null
     and (
       cts.segment_stable_key = pss.segment_stable_key
       or (
         pss.stored_segment_id is not null
         and cts.segment_id = pss.stored_segment_id
       )
     )
    left join current_timesheet_segment_groups ctsg
      on ctsg.current_timesheet_id = ctc.current_timesheet_id
  ),
  timesheet_snooze_rows_lifecycle as (
    select
      tsrr.*,
      case
        when tsrr.cleared_at_utc is not null then 'CLEARED'
        when tsrr.current_timesheet_id is null then 'CLEARED_DELETED_TIMESHEET'
        when tsrr.row_kind = 'SEGMENT' and tsrr.current_segment_stable_key is null then 'CLEARED_DELETED_SEGMENT'
        when tsrr.snooze_until_date is not null and tsrr.snooze_until_date < v_today_uk then 'EXPIRED'
        else 'ACTIVE'
      end as lifecycle_state,
      case
        when tsrr.cleared_at_utc is not null then tsrr.cleared_at_utc
        when tsrr.current_timesheet_id is null then tsrr.updated_at_utc
        when tsrr.row_kind = 'SEGMENT' and tsrr.current_segment_stable_key is null then tsrr.updated_at_utc
        when tsrr.snooze_until_date is not null and tsrr.snooze_until_date < v_today_uk then tsrr.updated_at_utc
        else null::timestamptz
      end as effective_cleared_at_utc,
      case
        when tsrr.cleared_at_utc is not null then 'USER_CLEARED'
        when tsrr.current_timesheet_id is null then 'DELETED_TIMESHEET'
        when tsrr.row_kind = 'SEGMENT' and tsrr.current_segment_stable_key is null then 'DELETED_SEGMENT'
        when tsrr.snooze_until_date is not null and tsrr.snooze_until_date < v_today_uk then 'EXPIRED'
        else null::text
      end as clear_reason
    from timesheet_snooze_rows_raw tsrr
  ),
  timesheet_snooze_rows_filtered as (
    select
      tsrl.snooze_id,
      tsrl.candidate_id,
      tsrl.candidate_tms_ref,
      tsrl.candidate_display_name,
      case when tsrl.current_client_id is null then null else tsrl.current_client_id::text end as client_id_text,
      tsrl.current_client_name as client_name,
      coalesce(tsrl.current_timesheet_id, tsrl.stored_timesheet_id) as display_timesheet_id,
      tsrl.current_timesheet_id,
      tsrl.stored_timesheet_id,
      tsrl.booking_id,
      case when tsrl.row_kind = 'SEGMENT' then coalesce(tsrl.current_segment_id, tsrl.stored_segment_id) else null end as display_segment_id,
      tsrl.stored_segment_id,
      tsrl.segment_stable_key,
      tsrl.row_kind,
      tsrl.snooze_kind,
      tsrl.snooze_until_date,
      tsrl.note,
      tsrl.created_at_utc,
      tsrl.updated_at_utc,
      tsrl.effective_cleared_at_utc,
      tsrl.lifecycle_state,
      tsrl.clear_reason,
      case
        when tsrl.lifecycle_state = 'ACTIVE' and tsrl.snooze_until_date is null then 'INDEFINITE_SNOOZE'
        when tsrl.lifecycle_state = 'ACTIVE' and tsrl.snooze_until_date is not null then 'DATED_SNOOZE'
        when tsrl.lifecycle_state = 'EXPIRED' then 'EXPIRED'
        else 'CLEARED'
      end as snooze_state,
      case
        when tsrl.row_kind = 'WHOLE_TIMESHEET' then tsrl.current_week_ending_date
        else null::date
      end as week_ending_date,
      case
        when tsrl.row_kind = 'WHOLE_TIMESHEET' then tsrl.current_reference_number
        else tsrl.current_segment_ref_num
      end as reference_number,
      case
        when tsrl.row_kind = 'WHOLE_TIMESHEET' then round(coalesce(tsrl.total_segment_pay_ex_vat, 0), 2)
        else round(coalesce(tsrl.current_segment_pay_amount_ex_vat, 0), 2)
      end as pay_amount_ex_vat,
      case
        when tsrl.row_kind = 'WHOLE_TIMESHEET' then coalesce(tsrl.segment_count, 0)
        when tsrl.current_segment_stable_key is not null then 1
        else 0
      end as segment_count,
      case
        when tsrl.row_kind = 'WHOLE_TIMESHEET' then coalesce(tsrl.segment_rows_json, '[]'::jsonb)
        when tsrl.current_segment_stable_key is not null then jsonb_build_array(
          jsonb_build_object(
            'segment_id', tsrl.current_segment_id,
            'segment_stable_key', tsrl.current_segment_stable_key,
            'date', tsrl.current_segment_date,
            'client_name', tsrl.current_segment_client_name,
            'role', tsrl.current_segment_role,
            'band', tsrl.current_segment_band,
            'start', tsrl.current_segment_start,
            'finish', tsrl.current_segment_finish,
            'break_start', tsrl.current_segment_break_start,
            'break_end', tsrl.current_segment_break_end,
            'break_mins', tsrl.current_segment_break_mins,
            'pay_amount_ex_vat', tsrl.current_segment_pay_amount_ex_vat,
            'ref_num', tsrl.current_segment_ref_num
          )
        )
        else '[]'::jsonb
      end as segment_rows_json
    from timesheet_snooze_rows_lifecycle tsrl
    where (p_client_id is null or tsrl.current_client_id = p_client_id)
      and (
        tsrl.lifecycle_state = 'ACTIVE'
        or (p_include_cleared_snoozes = true and tsrl.lifecycle_state <> 'ACTIVE')
      )
  ),
  timesheet_snoozes_json as (
    select
      coalesce(
        jsonb_agg(
          jsonb_strip_nulls(
            jsonb_build_object(
              'snooze_id', tsf.snooze_id::text,
              'row_kind', tsf.row_kind,
              'lifecycle_state', tsf.lifecycle_state,
              'clear_reason', tsf.clear_reason,
              'candidate_id', tsf.candidate_id::text,
              'candidate_tms_ref', tsf.candidate_tms_ref,
              'candidate_display_name', tsf.candidate_display_name,
              'client_id', tsf.client_id_text,
              'client_name', tsf.client_name,
              'timesheet_id', case when tsf.display_timesheet_id is null then null else tsf.display_timesheet_id::text end,
              'current_timesheet_id', case when tsf.current_timesheet_id is null then null else tsf.current_timesheet_id::text end,
              'original_timesheet_id', case when tsf.stored_timesheet_id is null then null else tsf.stored_timesheet_id::text end,
              'booking_id', tsf.booking_id,
              'segment_id', tsf.display_segment_id,
              'original_segment_id', tsf.stored_segment_id,
              'segment_stable_key', tsf.segment_stable_key,
              'week_ending_date', case when tsf.week_ending_date is null then null else tsf.week_ending_date::text end,
              'reference_number', tsf.reference_number,
              'snooze_kind', tsf.snooze_kind,
              'snooze_until_date', case when tsf.snooze_until_date is null then null else tsf.snooze_until_date::text end,
              'note', tsf.note,
              'created_at_utc', case when tsf.created_at_utc is null then null else tsf.created_at_utc::text end,
              'updated_at_utc', case when tsf.updated_at_utc is null then null else tsf.updated_at_utc::text end,
              'cleared_at_utc', case when tsf.effective_cleared_at_utc is null then null else tsf.effective_cleared_at_utc::text end,
              'snooze_state', tsf.snooze_state,
              'pay_amount_ex_vat', tsf.pay_amount_ex_vat,
              'segment_count', tsf.segment_count,
              'segment_rows', tsf.segment_rows_json,
              'action_flags', jsonb_build_object(
                'can_unsnooze', (tsf.lifecycle_state = 'ACTIVE'),
                'can_change_to_indefinite', (tsf.lifecycle_state = 'ACTIVE' and tsf.snooze_until_date is not null),
                'can_change_to_dated', (tsf.lifecycle_state = 'ACTIVE' and tsf.snooze_until_date is null),
                'can_amend_date', (tsf.lifecycle_state = 'ACTIVE' and tsf.snooze_until_date is not null)
              )
            )
          )
          order by tsf.candidate_display_name asc, tsf.row_kind asc, tsf.week_ending_date asc nulls last, tsf.display_timesheet_id asc nulls last, tsf.segment_stable_key asc nulls last, tsf.snooze_id asc
        ),
        '[]'::jsonb
      ) as payload
    from timesheet_snooze_rows_filtered tsf
  ),
  summary_data as (
    select
      jsonb_build_object(
        'payment_advances_active_count', count(*) filter (where fr.case_type = 'PAYMENT_ADVANCE' and fr.status in ('ACTIVE', 'PAUSED') and fr.outstanding_amount > 0),
        'overpayments_active_count', count(*) filter (where fr.case_type = 'OVERPAYMENT' and fr.status in ('ACTIVE', 'PAUSED') and fr.outstanding_amount > 0),
        'underpayments_active_count', count(*) filter (where fr.case_type = 'UNDERPAYMENT' and fr.status in ('ACTIVE', 'PAUSED') and fr.outstanding_amount > 0),
        'manual_debt_adjustments_active_count', count(*) filter (where fr.case_type = 'MANUAL_DEBT_ADJUSTMENT' and fr.status in ('ACTIVE', 'PAUSED') and fr.outstanding_amount > 0),
        'manual_credit_adjustments_count', count(*) filter (where fr.case_type = 'MANUAL_CREDIT_ADJUSTMENT'),
        'mixed_finance_cases_count', count(*) filter (where fr.is_mixed_case),
        'unresolved_finance_cases_count', count(*) filter (where fr.unresolved_taxable_count > 0),
        'stale_finance_cases_count', count(*) filter (where fr.stale_count > 0),
        'finance_cases_with_active_snooze_count', count(*) filter (where fr.active_snooze_id is not null),
        'timesheet_snoozes_count', (select count(*) from timesheet_snooze_rows_filtered)
      ) as payload
    from finance_rows fr
  )
  select
    coalesce((select fj.payload from finance_json fj), '[]'::jsonb),
    coalesce((select tsj.payload from timesheet_snoozes_json tsj), '[]'::jsonb),
    coalesce((select sd.payload from summary_data sd), '{}'::jsonb)
  into v_finance_cases, v_timesheet_snoozes, v_summary;

  return jsonb_build_object(
    'ok', true,
    'filters', jsonb_build_object(
      'candidate_id', case when p_candidate_id is null then null else p_candidate_id::text end,
      'client_id', case when p_client_id is null then null else p_client_id::text end,
      'include_paid_off', p_include_paid_off,
      'include_cleared_snoozes', p_include_cleared_snoozes
    ),
    'summary', v_summary,
    'finance_cases', v_finance_cases,
    'timesheet_snoozes', v_timesheet_snoozes
  );
end;
$function$
;

-- SOURCE public.pay_payment_advance_create(p_candidate_id uuid, p_principal_amount numeric, p_weekly_due numeric, p_weeks_total integer, p_start_week_start date, p_actor_user_id uuid, p_note text, p_minimum_earnings_threshold numeric, p_take_home_floor_override numeric)
CREATE OR REPLACE FUNCTION public.pay_payment_advance_create(p_candidate_id uuid, p_principal_amount numeric, p_weekly_due numeric, p_weeks_total integer, p_start_week_start date, p_actor_user_id uuid, p_note text DEFAULT NULL::text, p_minimum_earnings_threshold numeric DEFAULT NULL::numeric, p_take_home_floor_override numeric DEFAULT NULL::numeric)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_now_utc timestamptz := now();
  v_pay_date date := (now() at time zone 'Europe/London')::date;

  v_settings record;
  v_candidate record;

  v_schedule_json jsonb := '[]'::jsonb;
  v_next_due date := null;

  v_finance_case_id uuid := null;
  v_pay_batch_id uuid := null;
  v_pay_batch_candidate_id uuid := null;
  v_pay_batch_item_id uuid := null;
  v_finance_component_id uuid := null;

  v_warnings jsonb := '[]'::jsonb;

  v_provider text := null;
  v_env text := null;
  v_need_name_check boolean := false;
  v_requires_payee_map boolean := false;

  v_bnc_status text := null;
  v_bnc_has_override boolean := false;
  v_bpm_present boolean := false;

  v_required_weeks int := 0;
  v_pay_channel text := null;
  v_paye_treatment text := 'NONE';

  v_principal_amount_norm numeric(12,2) := 0;
  v_weekly_due_norm numeric(12,2) := null;
  v_weeks_total_norm int := null;
  v_schedule_input_mode text := null;
  v_has_weekly_due_input boolean := false;
  v_has_weeks_total_input boolean := false;

  v_source_family_key text := null;
  v_component_source_basis_json jsonb := '{}'::jsonb;
  v_component_snapshot_json jsonb := '{}'::jsonb;
  v_component_summary_json jsonb := '{}'::jsonb;
begin
  if p_candidate_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'CANDIDATE_ID_REQUIRED',
      'message', 'pay_payment_advance_create: candidate_id is required'
    )::text;
  end if;

  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_payment_advance_create: actor_user_id is required'
    )::text;
  end if;

  if p_principal_amount is null or round(p_principal_amount,2) <= 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'PRINCIPAL_INVALID',
      'message', 'pay_payment_advance_create: principal_amount must be > 0'
    )::text;
  end if;

  if p_start_week_start is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'START_WEEK_START_REQUIRED',
      'message', 'pay_payment_advance_create: start_week_start is required'
    )::text;
  end if;

  if public._pay_week_start_monday(p_start_week_start) <> p_start_week_start then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'START_WEEK_START_NOT_MONDAY',
      'message', 'pay_payment_advance_create: start_week_start must be a Monday (week start)',
      'start_week_start', p_start_week_start::text
    )::text;
  end if;

  if p_minimum_earnings_threshold is not null and round(p_minimum_earnings_threshold,2) < 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'MINIMUM_EARNINGS_THRESHOLD_INVALID',
      'message', 'pay_payment_advance_create: minimum_earnings_threshold must be >= 0'
    )::text;
  end if;

  if p_take_home_floor_override is not null and round(p_take_home_floor_override,2) < 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'TAKE_HOME_FLOOR_INVALID',
      'message', 'pay_payment_advance_create: take_home_floor_override must be >= 0'
    )::text;
  end if;

  v_principal_amount_norm := round(p_principal_amount,2);
  v_has_weekly_due_input := (p_weekly_due is not null and round(p_weekly_due,2) > 0);
  v_has_weeks_total_input := (p_weeks_total is not null and p_weeks_total >= 1);

  if not v_has_weekly_due_input and not v_has_weeks_total_input then
    if p_weekly_due is not null and round(p_weekly_due,2) <= 0 then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_PAYMENT_ADVANCE_CREATE',
        'code', 'WEEKLY_DUE_INVALID',
        'message', 'pay_payment_advance_create: weekly_due must be > 0 when provided'
      )::text;
    end if;

    if p_weeks_total is not null and p_weeks_total < 1 then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_PAYMENT_ADVANCE_CREATE',
        'code', 'WEEKS_TOTAL_INVALID',
        'message', 'pay_payment_advance_create: weeks_total must be >= 1 when provided'
      )::text;
    end if;

    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'REPAYMENT_INPUT_REQUIRED',
      'message', 'pay_payment_advance_create: supply either weekly_due or weeks_total'
    )::text;
  end if;

  if v_has_weekly_due_input and v_has_weeks_total_input then
    v_schedule_input_mode := 'LEGACY_BOTH';
    v_weekly_due_norm := round(p_weekly_due,2);
    v_weeks_total_norm := p_weeks_total;
  elsif v_has_weeks_total_input then
    v_schedule_input_mode := 'BY_WEEKS';
    v_weeks_total_norm := p_weeks_total;
    v_weekly_due_norm := round((ceil((v_principal_amount_norm / v_weeks_total_norm) * 100) / 100), 2);
  else
    v_schedule_input_mode := 'BY_WEEKLY_DUE';
    v_weekly_due_norm := round(p_weekly_due,2);
    v_weeks_total_norm := greatest(ceil(v_principal_amount_norm / v_weekly_due_norm)::int, 1);
  end if;

  if v_weekly_due_norm is null or v_weekly_due_norm <= 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'WEEKLY_DUE_INVALID',
      'message', 'pay_payment_advance_create: normalized weekly_due must be > 0'
    )::text;
  end if;

  if v_weeks_total_norm is null or v_weeks_total_norm < 1 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'WEEKS_TOTAL_INVALID',
      'message', 'pay_payment_advance_create: normalized weeks_total must be >= 1'
    )::text;
  end if;

  v_required_weeks := greatest(ceil(v_principal_amount_norm / v_weekly_due_norm)::int, 1);
  if v_weeks_total_norm < v_required_weeks then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'WEEKS_TOTAL_TOO_SHORT',
      'message', 'pay_payment_advance_create: repayment schedule does not cover the principal amount',
      'required_weeks', v_required_weeks,
      'weeks_total', v_weeks_total_norm
    )::text;
  end if;

  select
    sd.banking_system,
    sd.external_paye_system,
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_name_check
  into v_settings
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;

  if v_settings.banking_system is null or v_settings.external_paye_system is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'SETTINGS_DEFAULTS_MISSING',
      'message', 'pay_payment_advance_create: settings_defaults missing required banking defaults (id=1)'
    )::text;
  end if;

  select
    c.id,
    c.active,
    c.tms_ref,
    c.display_name,
    c.first_name,
    c.last_name,
    c.account_holder,
    c.sort_code,
    c.account_number,
    c.bank_details_hash,
    c.umbrella_id,
    upper(coalesce(c.pay_method,'')) as pay_method
  into v_candidate
  from public.candidates c
  where c.id = p_candidate_id
  limit 1;

  if v_candidate.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'CANDIDATE_NOT_FOUND',
      'message', 'pay_payment_advance_create: candidate not found',
      'candidate_id', p_candidate_id::text
    )::text;
  end if;

  if coalesce(v_candidate.active,false) = false then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'CANDIDATE_INACTIVE',
      'message', 'pay_payment_advance_create: candidate is not active',
      'candidate_id', p_candidate_id::text
    )::text;
  end if;

  if v_candidate.pay_method not in ('PAYE','UMBRELLA') then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_CREATE',
      'code', 'PAY_METHOD_INVALID',
      'message', 'pay_payment_advance_create: candidate pay_method must be PAYE or UMBRELLA',
      'candidate_id', p_candidate_id::text,
      'pay_method', v_candidate.pay_method
    )::text;
  end if;

  v_pay_channel := v_candidate.pay_method;

  v_provider := upper(btrim(coalesce(v_settings.rail_provider_default,'CSV')));
  v_env := upper(btrim(coalesce(v_settings.rail_env_default,'PROD')));
  v_need_name_check := (coalesce(v_settings.rail_supports_name_check,false) = true) and (v_provider <> 'CSV');
  v_requires_payee_map := (v_provider <> 'CSV');

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'week_start', (p_start_week_start + (gs.i * 7))::date,
          'amount', round(
            -least(
              v_weekly_due_norm,
              greatest(v_principal_amount_norm - (v_weekly_due_norm * gs.i), 0)
            ),
            2
          )
        )
        order by (p_start_week_start + (gs.i * 7))::date asc
      ),
      '[]'::jsonb
    )
  into v_schedule_json
  from generate_series(0, greatest(v_weeks_total_norm,1) - 1) as gs(i);

  select min(x.week_start)
  into v_next_due
  from (
    select
      (p_start_week_start + (gs2.i * 7))::date as week_start,
      round(
        -least(
          v_weekly_due_norm,
          greatest(v_principal_amount_norm - (v_weekly_due_norm * gs2.i), 0)
        ),
        2
      )::numeric as amt
    from generate_series(0, greatest(v_weeks_total_norm,1) - 1) as gs2(i)
  ) x
  where x.amt < 0;

  insert into public.pay_advances(
    candidate_id,
    client_id,
    reason,
    original_amount,
    outstanding_amount,
    linked_shift_date,
    schedule_json,
    next_due_week_start,
    status,
    best_guess_hours,
    notes,
    created_at,
    created_by,
    updated_at,
    advance_kind,
    linked_timesheet_id,
    baseline_signature,
    payout_status,
    payout_pay_batch_id,
    payout_transfer_id,
    weekly_due,
    weeks_total,
    start_week_start,
    case_type,
    adjustment_comment,
    source_original_paid_amount,
    source_corrected_paid_amount,
    minimum_earnings_threshold,
    take_home_floor_override,
    written_off_at_utc,
    written_off_by_user_id,
    write_off_reason,
    cleared_at_utc,
    cleared_by_user_id
  )
  values (
    p_candidate_id,
    null::uuid,
    'LOAN'::public.pay_advance_reason_enum,
    v_principal_amount_norm,
    v_principal_amount_norm,
    null::date,
    coalesce(v_schedule_json,'[]'::jsonb),
    v_next_due,
    'ACTIVE'::public.pay_advance_status_enum,
    null::jsonb,
    nullif(btrim(coalesce(p_note,'')), ''),
    v_now_utc,
    p_actor_user_id,
    v_now_utc,
    'LOAN'::public.pay_advance_kind_enum,
    null::uuid,
    null::text,
    'PENDING'::public.pay_advance_payout_status_enum,
    null::uuid,
    null::uuid,
    v_weekly_due_norm,
    v_weeks_total_norm,
    p_start_week_start,
    'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum,
    null::text,
    null::numeric,
    null::numeric,
    case when p_minimum_earnings_threshold is null then null else round(p_minimum_earnings_threshold,2) end,
    case when p_take_home_floor_override is null then null else round(p_take_home_floor_override,2) end,
    null::timestamptz,
    null::uuid,
    null::text,
    null::timestamptz,
    null::uuid
  )
  returning id into v_finance_case_id;

  v_source_family_key := 'case:' || v_finance_case_id::text;
  v_component_source_basis_json := jsonb_strip_nulls(
    jsonb_build_object(
      'case_type', 'PAYMENT_ADVANCE',
      'advance_kind', 'LOAN',
      'principal_amount', v_principal_amount_norm,
      'weekly_due', v_weekly_due_norm,
      'weeks_total', v_weeks_total_norm,
      'start_week_start', p_start_week_start::text,
      'next_due_week_start', case when v_next_due is null then null else v_next_due::text end,
      'schedule_json', coalesce(v_schedule_json, '[]'::jsonb),
      'minimum_earnings_threshold', case when p_minimum_earnings_threshold is null then null else round(p_minimum_earnings_threshold,2) end,
      'take_home_floor_override', case when p_take_home_floor_override is null then null else round(p_take_home_floor_override,2) end,
      'note', nullif(btrim(coalesce(p_note,'')), '')
    )
  );

  insert into public.pay_finance_case_components (
    finance_case_id,
    candidate_id,
    client_id,
    linked_timesheet_id,
    source_family_key,
    component_key_type,
    component_key_value,
    classification,
    source_pay_method,
    source_basis_json,
    source_amount,
    remaining_source_amount,
    saved_target_pay_method,
    saved_resolution_mode,
    saved_resolution_payload_json,
    saved_resolution_result_json,
    resolution_fingerprint,
    is_resolution_stale,
    stale_reason,
    allocation_priority_group,
    allocation_priority_order,
    created_at_utc,
    updated_at_utc,
    resolved_at_utc,
    closed_at_utc
  )
  values (
    v_finance_case_id,
    p_candidate_id,
    null::uuid,
    null::uuid,
    v_source_family_key,
    'CASE_TOTAL',
    'TOTAL',
    'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
    v_candidate.pay_method,
    v_component_source_basis_json,
    v_principal_amount_norm,
    v_principal_amount_norm,
    null,
    null,
    null,
    null,
    null,
    false,
    null,
    0,
    0,
    v_now_utc,
    v_now_utc,
    null,
    null
  )
  returning id into v_finance_component_id;

  v_component_snapshot_json := jsonb_build_object(
    'finance_component_id', v_finance_component_id::text,
    'finance_case_id', v_finance_case_id::text,
    'source_family_key', v_source_family_key,
    'component_key_type', 'CASE_TOTAL',
    'component_key_value', 'TOTAL',
    'classification', 'NET_PAY_FIXED_RECOVERY',
    'source_pay_method', v_candidate.pay_method,
    'target_pay_method', v_candidate.pay_method,
    'source_basis_json', v_component_source_basis_json,
    'source_amount', v_principal_amount_norm,
    'remaining_source_amount', v_principal_amount_norm,
    'current_component_fingerprint', public.pay_finance_component_fingerprint(
      p_source_family_key => v_source_family_key,
      p_component_key_type => 'CASE_TOTAL',
      p_component_key_value => 'TOTAL',
      p_classification => 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
      p_source_pay_method => v_candidate.pay_method,
      p_current_target_pay_method => v_candidate.pay_method,
      p_source_basis_json => v_component_source_basis_json,
      p_source_amount => v_principal_amount_norm,
      p_relevant_erni_pct => null,
      p_target_basis_json => jsonb_build_object('current_target_pay_method', v_candidate.pay_method)
    )
  );

  insert into public.pay_batches(
    pay_date,
    created_at_utc,
    created_by_user_id,
    status,
    banking_system_snapshot,
    external_paye_system_snapshot,
    rail_provider_snapshot,
    rail_env_snapshot,
    batch_kind_fixed
  )
  values (
    v_pay_date,
    v_now_utc,
    p_actor_user_id,
    'DRAFT',
    v_settings.banking_system,
    v_settings.external_paye_system,
    v_settings.rail_provider_default,
    v_settings.rail_env_default,
    'LOANS'
  )
  returning id into v_pay_batch_id;

  update public.pay_advances pa
  set payout_pay_batch_id = v_pay_batch_id,
      updated_at = v_now_utc
  where pa.id = v_finance_case_id;

  insert into public.pay_batch_candidates(
    pay_batch_id,
    candidate_id,
    candidate_tms_ref,
    candidate_display_name,
    paye_state,
    mismatch_settlement_choice,
    gross_preview,
    net_bank_amount,
    debt_created,
    loan_repayment_taken,
    overpayment_recovery_taken,
    awaiting_net_amount,
    updated_at
  )
  values (
    v_pay_batch_id,
    p_candidate_id,
    v_candidate.tms_ref,
    v_candidate.display_name,
    null,
    null,
    v_principal_amount_norm,
    v_principal_amount_norm,
    0,
    0,
    0,
    false,
    v_now_utc
  )
  returning id into v_pay_batch_candidate_id;

  insert into public.pay_batch_items(
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    bank_reference,
    pay_bank_transfer_id,
    repayment_week_start,
    is_voided,
    is_mismatch,
    created_at,
    updated_at,
    finance_case_id,
    reservation_id,
    paye_treatment,
    finance_component_id,
    frozen_component_snapshot_json,
    frozen_component_key_type,
    frozen_component_key_value,
    frozen_component_classification,
    frozen_source_basis_json,
    frozen_source_pay_method,
    frozen_target_pay_method,
    frozen_resolution_mode,
    frozen_resolution_payload_json,
    frozen_resolution_result_json,
    frozen_source_amount,
    frozen_target_amount_ex_vat,
    frozen_target_amount_vat,
    frozen_target_amount_inc_vat
  )
  values (
    v_pay_batch_candidate_id,
    'LOAN_PAYOUT',
    null::uuid,
    null::text,
    ('advance:' || v_finance_case_id::text),
    'Payment Advance',
    v_principal_amount_norm,
    0,
    v_principal_amount_norm,
    v_pay_channel,
    case when v_pay_channel = 'UMBRELLA' then v_candidate.umbrella_id else null::uuid end,
    null::text,
    null::uuid,
    null::date,
    false,
    false,
    v_now_utc,
    v_now_utc,
    v_finance_case_id,
    null::uuid,
    v_paye_treatment,
    v_finance_component_id,
    v_component_snapshot_json,
    'CASE_TOTAL',
    'TOTAL',
    'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
    v_component_source_basis_json,
    v_candidate.pay_method,
    v_candidate.pay_method,
    null,
    null,
    null,
    v_principal_amount_norm,
    v_principal_amount_norm,
    0,
    v_principal_amount_norm
  )
  returning id into v_pay_batch_item_id;

  insert into public.pay_batch_item_breakdowns(
    pay_batch_item_id,
    line_kind,
    bucket_code,
    unit_name,
    units,
    rate,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    meta_json
  )
  values (
    v_pay_batch_item_id,
    'LOAN_PAYOUT',
    null,
    'Payment Advance',
    null::numeric,
    null::numeric,
    v_principal_amount_norm,
    0,
    v_principal_amount_norm,
    jsonb_build_object(
      'case_type', 'PAYMENT_ADVANCE',
      'worker_label', 'Payment Advance'
    )
  );

  insert into public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    before_json,
    after_json,
    reason,
    note
  )
  values (
    v_finance_case_id,
    null,
    'CREATED',
    v_now_utc,
    p_actor_user_id,
    v_pay_batch_id,
    null::jsonb,
    jsonb_build_object(
      'case_type', 'PAYMENT_ADVANCE',
      'original_amount', v_principal_amount_norm,
      'outstanding_amount', v_principal_amount_norm,
      'weekly_due', v_weekly_due_norm,
      'weeks_total', v_weeks_total_norm,
      'start_week_start', p_start_week_start::text,
      'next_due_week_start', case when v_next_due is null then null else v_next_due::text end,
      'payout_pay_batch_id', v_pay_batch_id::text,
      'payout_status', 'PENDING',
      'minimum_earnings_threshold', case when p_minimum_earnings_threshold is null then null else round(p_minimum_earnings_threshold,2) end,
      'take_home_floor_override', case when p_take_home_floor_override is null then null else round(p_take_home_floor_override,2) end,
      'schedule_input_mode', v_schedule_input_mode
    ),
    null::text,
    'Payment Advance created'
  );

  insert into public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    before_json,
    after_json,
    reason,
    note
  )
  values (
    v_finance_case_id,
    v_finance_component_id,
    'COMPONENT_CREATED',
    v_now_utc,
    p_actor_user_id,
    v_pay_batch_id,
    null::jsonb,
    v_component_snapshot_json,
    'PAYMENT_ADVANCE_COMPONENT_CREATE',
    'Created net-fixed finance component for Payment Advance principal'
  );

  if v_candidate.bank_details_hash is null or btrim(coalesce(v_candidate.bank_details_hash,'')) = '' then
    v_warnings := v_warnings || jsonb_build_array(
      jsonb_build_object(
        'code', 'BLOCKED_BANK_DETAILS',
        'message', 'Candidate bank details are missing; batch can be created but will be blocked at prepare/schedule until bank details are present.',
        'candidate_id', p_candidate_id::text
      )
    );
  else
    if v_need_name_check = true then
      select
        coalesce(bnc.status, 'UNVERIFIED') as status,
        (bnc.override_reason is not null and bnc.override_hash = v_candidate.bank_details_hash) as has_override
      into
        v_bnc_status,
        v_bnc_has_override
      from public.bank_name_checks bnc
      where bnc.rail_provider = v_settings.rail_provider_default
        and bnc.rail_env = v_settings.rail_env_default
        and bnc.entity_kind = 'CANDIDATE'
        and bnc.entity_id = p_candidate_id
        and bnc.bank_details_hash = v_candidate.bank_details_hash
      limit 1;

      if coalesce(v_bnc_status,'UNVERIFIED') <> 'PASS' and coalesce(v_bnc_has_override,false) = false then
        v_warnings := v_warnings || jsonb_build_array(
          jsonb_build_object(
            'code', 'BLOCKED_NAME_CHECK',
            'message', 'Name check has not passed (or override missing) for candidate bank details; scheduling/execution may be blocked until resolved.',
            'candidate_id', p_candidate_id::text,
            'rail_provider', v_settings.rail_provider_default,
            'rail_env', v_settings.rail_env_default
          )
        );
      end if;
    end if;

    if v_requires_payee_map = true then
      select (bpm.payee_id is not null) as present
      into v_bpm_present
      from public.bank_payee_map bpm
      where bpm.rail_provider = v_settings.rail_provider_default
        and bpm.rail_env = v_settings.rail_env_default
        and bpm.entity_kind = 'CANDIDATE'
        and bpm.entity_id = p_candidate_id
        and bpm.bank_details_hash = v_candidate.bank_details_hash
      limit 1;

      if coalesce(v_bpm_present,false) = false then
        v_warnings := v_warnings || jsonb_build_array(
          jsonb_build_object(
            'code', 'BLOCKED_NO_PAYEE_MAP',
            'message', 'Payee map is missing for candidate bank details on this rail; scheduling/execution may be blocked until payee mapping exists.',
            'candidate_id', p_candidate_id::text,
            'rail_provider', v_settings.rail_provider_default,
            'rail_env', v_settings.rail_env_default
          )
        );
      end if;
    end if;
  end if;

  v_component_summary_json := jsonb_build_object(
    'finance_component_id', v_finance_component_id::text,
    'source_family_key', v_source_family_key,
    'component_key_type', 'CASE_TOTAL',
    'component_key_value', 'TOTAL',
    'classification', 'NET_PAY_FIXED_RECOVERY',
    'source_pay_method', v_candidate.pay_method,
    'source_amount', v_principal_amount_norm,
    'remaining_source_amount', v_principal_amount_norm,
    'saved_target_pay_method', null,
    'saved_resolution_mode', null,
    'is_resolution_stale', false,
    'stale_reason', null,
    'source_basis_json', v_component_source_basis_json
  );

  return jsonb_build_object(
    'ok', true,
    'finance_case_id', v_finance_case_id::text,
    'advance_id', v_finance_case_id::text,
    'pay_batch_id', v_pay_batch_id::text,
    'pay_date', v_pay_date::text,
    'batch_kind_fixed', 'LOANS',
    'case_type', 'PAYMENT_ADVANCE',
    'worker_label', 'Payment Advance',
    'repayment_label', 'Payment Advance Repayment',
    'schedule_input_mode', v_schedule_input_mode,
    'principal_amount', v_principal_amount_norm,
    'weekly_due', v_weekly_due_norm,
    'weeks_total', v_weeks_total_norm,
    'start_week_start', p_start_week_start::text,
    'next_due_week_start', case when v_next_due is null then null else v_next_due::text end,
    'minimum_earnings_threshold', case when p_minimum_earnings_threshold is null then null else round(p_minimum_earnings_threshold,2) end,
    'take_home_floor_override', case when p_take_home_floor_override is null then null else round(p_take_home_floor_override,2) end,
    'schedule_json', coalesce(v_schedule_json,'[]'::jsonb),
    'finance_component_id', v_finance_component_id::text,
    'component_summary', v_component_summary_json,
    'warnings', v_warnings
  );
end;
$function$
;

-- SOURCE public.pay_payment_advance_update(p_finance_case_id uuid, p_actor_user_id uuid, p_principal_amount numeric, p_weekly_due numeric, p_weeks_total integer, p_start_week_start date, p_note text, p_minimum_earnings_threshold numeric, p_take_home_floor_override numeric)
CREATE OR REPLACE FUNCTION public.pay_payment_advance_update(p_finance_case_id uuid, p_actor_user_id uuid, p_principal_amount numeric DEFAULT NULL::numeric, p_weekly_due numeric DEFAULT NULL::numeric, p_weeks_total integer DEFAULT NULL::integer, p_start_week_start date DEFAULT NULL::date, p_note text DEFAULT NULL::text, p_minimum_earnings_threshold numeric DEFAULT NULL::numeric, p_take_home_floor_override numeric DEFAULT NULL::numeric)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_now_utc timestamptz := now();
  v_case public.pay_advances%rowtype;
  v_batch public.pay_batches%rowtype;
  v_has_batch boolean := false;
  v_is_committed boolean := false;

  v_new_principal numeric(12,2);
  v_new_weekly_due numeric(12,2);
  v_new_weeks_total int;
  v_new_start_week_start date;
  v_new_notes text;
  v_new_minimum_earnings_threshold numeric(12,2);
  v_new_take_home_floor_override numeric(12,2);
  v_new_schedule_json jsonb := '[]'::jsonb;
  v_new_next_due date := null;
  v_required_weeks int := 0;

  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
  v_can_edit_payout_details boolean := false;
  v_has_principal_change boolean := false;
  v_batch_item_id uuid := null;
  v_pay_batch_candidate_id uuid := null;

  v_schedule_base_amount numeric(12,2) := 0;
  v_schedule_input_mode text := null;
  v_has_weekly_due_input boolean := false;
  v_has_weeks_total_input boolean := false;

  v_component public.pay_finance_case_components%rowtype;
  v_component_after public.pay_finance_case_components%rowtype;
  v_finance_component_id uuid := null;
  v_component_before_json jsonb := '{}'::jsonb;
  v_component_after_json jsonb := '{}'::jsonb;
  v_component_source_basis_json jsonb := '{}'::jsonb;
  v_component_snapshot_json jsonb := '{}'::jsonb;
  v_component_summary_json jsonb := '{}'::jsonb;
  v_component_has_saved_resolution boolean := false;
  v_component_mark_stale boolean := false;
  v_component_stale_reason text := null;
  v_component_remaining_amount numeric(12,2) := 0;
  v_source_family_key text := null;

  v_candidate_pay_method text := null;
begin
  if p_finance_case_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'FINANCE_CASE_ID_REQUIRED',
      'message', 'pay_payment_advance_update: finance_case_id is required'
    )::text;
  end if;

  if p_actor_user_id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_payment_advance_update: actor_user_id is required'
    )::text;
  end if;

  select pa.*
  into v_case
  from public.pay_advances pa
  where pa.id = p_finance_case_id
    and pa.case_type = 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
  for update;

  if v_case.id is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'FINANCE_CASE_NOT_FOUND',
      'message', 'pay_payment_advance_update: Payment Advance not found',
      'finance_case_id', p_finance_case_id::text
    )::text;
  end if;

  select upper(coalesce(c.pay_method,''))
  into v_candidate_pay_method
  from public.candidates c
  where c.id = v_case.candidate_id
  limit 1;

  if v_case.payout_pay_batch_id is not null then
    select pb.*
    into v_batch
    from public.pay_batches pb
    where pb.id = v_case.payout_pay_batch_id
    limit 1;

    v_has_batch := v_batch.id is not null;
  end if;

  if v_has_batch then
    v_is_committed := (
      coalesce(v_batch.authoritative_payment_date is not null, false)
      or upper(coalesce(v_batch.status,'')) in ('SCHEDULED','AWAITING_AUTHORISATION','AUTHORISED_FOR_PAYMENT','EXECUTING','SETTLED','COMPLETED','PARTIAL')
    );
  else
    v_is_committed := false;
  end if;

  v_can_edit_payout_details := (v_is_committed = false);

  v_new_principal := coalesce(round(p_principal_amount,2), round(coalesce(v_case.original_amount,0),2));
  v_new_notes := case when p_note is null then v_case.notes else nullif(btrim(p_note), '') end;
  v_new_minimum_earnings_threshold := case when p_minimum_earnings_threshold is null then v_case.minimum_earnings_threshold else round(p_minimum_earnings_threshold,2) end;
  v_new_take_home_floor_override := case when p_take_home_floor_override is null then v_case.take_home_floor_override else round(p_take_home_floor_override,2) end;

  if v_new_minimum_earnings_threshold is not null and v_new_minimum_earnings_threshold < 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'MINIMUM_EARNINGS_THRESHOLD_INVALID',
      'message', 'pay_payment_advance_update: minimum_earnings_threshold must be >= 0'
    )::text;
  end if;

  if v_new_take_home_floor_override is not null and v_new_take_home_floor_override < 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'TAKE_HOME_FLOOR_INVALID',
      'message', 'pay_payment_advance_update: take_home_floor_override must be >= 0'
    )::text;
  end if;

  v_has_principal_change := round(v_new_principal,2) <> round(coalesce(v_case.original_amount,0),2);

  if v_is_committed and v_has_principal_change then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'PAYOUT_ALREADY_COMMITTED',
      'message', 'pay_payment_advance_update: payout details can only be edited before commit; after commit only future repayment terms may be changed',
      'finance_case_id', p_finance_case_id::text,
      'pay_batch_id', case when v_batch.id is null then null else v_batch.id::text end
    )::text;
  end if;

  if v_can_edit_payout_details then
    if v_new_principal <= 0 then
      raise exception '%', jsonb_build_object(
        'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
        'code', 'PRINCIPAL_INVALID',
        'message', 'pay_payment_advance_update: principal_amount must be > 0'
      )::text;
    end if;
    v_schedule_base_amount := v_new_principal;
  else
    v_schedule_base_amount := round(coalesce(v_case.outstanding_amount,0),2);
  end if;

  if p_start_week_start is not null and public._pay_week_start_monday(p_start_week_start) <> p_start_week_start then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'START_WEEK_START_NOT_MONDAY',
      'message', 'pay_payment_advance_update: start_week_start must be a Monday (week start)',
      'start_week_start', p_start_week_start::text
    )::text;
  end if;

  v_has_weekly_due_input := (p_weekly_due is not null and round(p_weekly_due,2) > 0);
  v_has_weeks_total_input := (p_weeks_total is not null and p_weeks_total >= 1);

  if p_weekly_due is not null and round(p_weekly_due,2) <= 0 and not v_has_weeks_total_input then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'WEEKLY_DUE_INVALID',
      'message', 'pay_payment_advance_update: weekly_due must be > 0 when provided'
    )::text;
  end if;

  if p_weeks_total is not null and p_weeks_total < 1 and not v_has_weekly_due_input then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'WEEKS_TOTAL_INVALID',
      'message', 'pay_payment_advance_update: weeks_total must be >= 1 when provided'
    )::text;
  end if;

  if not v_has_weekly_due_input and not v_has_weeks_total_input then
    v_schedule_input_mode := 'UNCHANGED';
    v_new_weekly_due := round(coalesce(v_case.weekly_due,0),2);
    v_new_weeks_total := v_case.weeks_total;
  elsif v_has_weekly_due_input and v_has_weeks_total_input then
    v_schedule_input_mode := 'LEGACY_BOTH';
    v_new_weekly_due := round(p_weekly_due,2);
    v_new_weeks_total := p_weeks_total;
  elsif v_has_weeks_total_input then
    v_schedule_input_mode := 'BY_WEEKS';
    v_new_weeks_total := p_weeks_total;
    v_new_weekly_due := round((ceil((v_schedule_base_amount / v_new_weeks_total) * 100) / 100), 2);
  else
    v_schedule_input_mode := 'BY_WEEKLY_DUE';
    v_new_weekly_due := round(p_weekly_due,2);
    v_new_weeks_total := greatest(ceil(v_schedule_base_amount / v_new_weekly_due)::int, 1);
  end if;

  if v_new_weekly_due is null or v_new_weekly_due <= 0 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'WEEKLY_DUE_INVALID',
      'message', 'pay_payment_advance_update: normalized weekly_due must be > 0'
    )::text;
  end if;

  if v_new_weeks_total is null or v_new_weeks_total < 1 then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'WEEKS_TOTAL_INVALID',
      'message', 'pay_payment_advance_update: normalized weeks_total must be >= 1'
    )::text;
  end if;

  if p_start_week_start is not null then
    v_new_start_week_start := p_start_week_start;
  elsif v_case.next_due_week_start is not null then
    v_new_start_week_start := v_case.next_due_week_start;
  else
    v_new_start_week_start := v_case.start_week_start;
  end if;

  if v_new_start_week_start is null then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'START_WEEK_START_REQUIRED',
      'message', 'pay_payment_advance_update: start_week_start is required'
    )::text;
  end if;

  if public._pay_week_start_monday(v_new_start_week_start) <> v_new_start_week_start then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'START_WEEK_START_NOT_MONDAY',
      'message', 'pay_payment_advance_update: start_week_start must be a Monday (week start)',
      'start_week_start', v_new_start_week_start::text
    )::text;
  end if;

  v_required_weeks := greatest(ceil(v_schedule_base_amount / v_new_weekly_due)::int, 1);
  if v_new_weeks_total < v_required_weeks then
    raise exception '%', jsonb_build_object(
      'error', 'PAY_PAYMENT_ADVANCE_UPDATE',
      'code', 'WEEKS_TOTAL_TOO_SHORT',
      'message', 'pay_payment_advance_update: repayment schedule does not cover the remaining amount',
      'required_weeks', v_required_weeks,
      'weeks_total', v_new_weeks_total
    )::text;
  end if;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'week_start', (v_new_start_week_start + (gs.i * 7))::date,
          'amount', round(
            -least(
              v_new_weekly_due,
              greatest(v_schedule_base_amount - (v_new_weekly_due * gs.i), 0)
            ),
            2
          )
        )
        order by (v_new_start_week_start + (gs.i * 7))::date asc
      ),
      '[]'::jsonb
    )
  into v_new_schedule_json
  from generate_series(0, greatest(v_new_weeks_total,1) - 1) as gs(i);

  select min(x.week_start)
  into v_new_next_due
  from (
    select
      (v_new_start_week_start + (gs2.i * 7))::date as week_start,
      round(
        -least(
          v_new_weekly_due,
          greatest(v_schedule_base_amount - (v_new_weekly_due * gs2.i), 0)
        ),
        2
      )::numeric as amt
    from generate_series(0, greatest(v_new_weeks_total,1) - 1) as gs2(i)
  ) x
  where x.amt < 0;

  v_source_family_key := 'case:' || p_finance_case_id::text;

  select pfc.*
  into v_component
  from public.pay_finance_case_components pfc
  where pfc.finance_case_id = p_finance_case_id
    and pfc.source_family_key = v_source_family_key
    and pfc.component_key_type = 'CASE_TOTAL'
    and pfc.component_key_value = 'TOTAL'
    and pfc.closed_at_utc is null
  order by pfc.updated_at_utc desc, pfc.created_at_utc desc, pfc.id desc
  limit 1
  for update;

  v_component_source_basis_json := jsonb_strip_nulls(
    jsonb_build_object(
      'case_type', 'PAYMENT_ADVANCE',
      'advance_kind', 'LOAN',
      'principal_amount', v_new_principal,
      'weekly_due', v_new_weekly_due,
      'weeks_total', v_new_weeks_total,
      'start_week_start', v_new_start_week_start::text,
      'next_due_week_start', case when v_new_next_due is null then null else v_new_next_due::text end,
      'schedule_json', coalesce(v_new_schedule_json, '[]'::jsonb),
      'minimum_earnings_threshold', v_new_minimum_earnings_threshold,
      'take_home_floor_override', v_new_take_home_floor_override,
      'note', v_new_notes
    )
  );

  if v_component.id is null then
    insert into public.pay_finance_case_components (
      finance_case_id,
      candidate_id,
      client_id,
      linked_timesheet_id,
      source_family_key,
      component_key_type,
      component_key_value,
      classification,
      source_pay_method,
      source_basis_json,
      source_amount,
      remaining_source_amount,
      saved_target_pay_method,
      saved_resolution_mode,
      saved_resolution_payload_json,
      saved_resolution_result_json,
      resolution_fingerprint,
      is_resolution_stale,
      stale_reason,
      allocation_priority_group,
      allocation_priority_order,
      created_at_utc,
      updated_at_utc,
      resolved_at_utc,
      closed_at_utc
    )
    values (
      p_finance_case_id,
      v_case.candidate_id,
      v_case.client_id,
      v_case.linked_timesheet_id,
      v_source_family_key,
      'CASE_TOTAL',
      'TOTAL',
      'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
      v_candidate_pay_method,
      v_component_source_basis_json,
      v_new_principal,
      case when v_can_edit_payout_details then v_new_principal else round(coalesce(v_case.outstanding_amount,0),2) end,
      null,
      null,
      null,
      null,
      null,
      false,
      null,
      0,
      0,
      v_now_utc,
      v_now_utc,
      null,
      null
    )
    returning * into v_component;
  end if;

  v_component_has_saved_resolution := false;

  if v_can_edit_payout_details then
    v_component_remaining_amount := v_new_principal;
  else
    v_component_remaining_amount := round(coalesce(v_case.outstanding_amount,0),2);
  end if;

  v_component_mark_stale := false;
  v_component_stale_reason := null;

  v_component_before_json := jsonb_build_object(
    'finance_component_id', v_component.id::text,
    'classification', v_component.classification::text,
    'source_pay_method', v_component.source_pay_method,
    'source_amount', round(coalesce(v_component.source_amount,0),2),
    'remaining_source_amount', round(coalesce(v_component.remaining_source_amount,0),2),
    'saved_target_pay_method', v_component.saved_target_pay_method,
    'saved_resolution_mode', case when v_component.saved_resolution_mode is null then null else v_component.saved_resolution_mode::text end,
    'is_resolution_stale', v_component.is_resolution_stale,
    'stale_reason', v_component.stale_reason,
    'source_basis_json', v_component.source_basis_json
  );

  update public.pay_finance_case_components pfc
  set
    candidate_id = v_case.candidate_id,
    client_id = v_case.client_id,
    linked_timesheet_id = v_case.linked_timesheet_id,
    classification = 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
    source_pay_method = v_candidate_pay_method,
    source_basis_json = v_component_source_basis_json,
    source_amount = v_new_principal,
    remaining_source_amount = v_component_remaining_amount,
    saved_target_pay_method = null,
    saved_resolution_mode = null,
    saved_resolution_payload_json = null,
    saved_resolution_result_json = null,
    resolution_fingerprint = null,
    is_resolution_stale = false,
    stale_reason = null,
    allocation_priority_group = 0,
    allocation_priority_order = 0,
    updated_at_utc = v_now_utc
  where pfc.id = v_component.id
  returning * into v_component_after;

  v_finance_component_id := v_component_after.id;

  update public.pay_advances pa
  set
    original_amount = case when v_can_edit_payout_details then v_new_principal else pa.original_amount end,
    outstanding_amount = case when v_can_edit_payout_details then v_new_principal else pa.outstanding_amount end,
    schedule_json = coalesce(v_new_schedule_json,'[]'::jsonb),
    next_due_week_start = v_new_next_due,
    notes = v_new_notes,
    weekly_due = v_new_weekly_due,
    weeks_total = v_new_weeks_total,
    start_week_start = v_new_start_week_start,
    minimum_earnings_threshold = v_new_minimum_earnings_threshold,
    take_home_floor_override = v_new_take_home_floor_override,
    updated_at = v_now_utc
  where pa.id = p_finance_case_id;

  if v_can_edit_payout_details and v_has_batch then
    select pbc.id
    into v_pay_batch_candidate_id
    from public.pay_batch_candidates pbc
    where pbc.pay_batch_id = v_batch.id
      and pbc.candidate_id = v_case.candidate_id
    limit 1;

    select pbi.id
    into v_batch_item_id
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc2
      on pbc2.id = pbi.pay_batch_candidate_id
    where pbc2.pay_batch_id = v_batch.id
      and pbc2.candidate_id = v_case.candidate_id
      and pbi.source_ref = ('advance:' || p_finance_case_id::text)
      and pbi.item_type = 'LOAN_PAYOUT'
      and pbi.is_voided = false
    limit 1;

    if v_pay_batch_candidate_id is not null then
      update public.pay_batch_candidates pbc
      set gross_preview = v_new_principal,
          net_bank_amount = v_new_principal,
          updated_at = v_now_utc
      where pbc.id = v_pay_batch_candidate_id;
    end if;

    v_component_snapshot_json := jsonb_build_object(
      'finance_component_id', v_finance_component_id::text,
      'finance_case_id', p_finance_case_id::text,
      'source_family_key', v_source_family_key,
      'component_key_type', 'CASE_TOTAL',
      'component_key_value', 'TOTAL',
      'classification', 'NET_PAY_FIXED_RECOVERY',
      'source_pay_method', v_candidate_pay_method,
      'target_pay_method', v_candidate_pay_method,
      'source_basis_json', v_component_source_basis_json,
      'source_amount', v_new_principal,
      'remaining_source_amount', v_component_remaining_amount,
      'saved_target_pay_method', v_component_after.saved_target_pay_method,
      'saved_resolution_mode', case when v_component_after.saved_resolution_mode is null then null else v_component_after.saved_resolution_mode::text end,
      'saved_resolution_payload_json', v_component_after.saved_resolution_payload_json,
      'saved_resolution_result_json', v_component_after.saved_resolution_result_json,
      'current_component_fingerprint', public.pay_finance_component_fingerprint(
        p_source_family_key => v_source_family_key,
        p_component_key_type => 'CASE_TOTAL',
        p_component_key_value => 'TOTAL',
        p_classification => 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
        p_source_pay_method => v_candidate_pay_method,
        p_current_target_pay_method => v_candidate_pay_method,
        p_source_basis_json => v_component_source_basis_json,
        p_source_amount => v_new_principal,
        p_relevant_erni_pct => null,
        p_target_basis_json => jsonb_build_object('current_target_pay_method', v_candidate_pay_method)
      )
    );

    if v_batch_item_id is not null then
      update public.pay_batch_items pbi
      set description = 'Payment Advance',
          amount_ex_vat = v_new_principal,
          amount_vat = 0,
          amount_inc_vat = v_new_principal,
          updated_at = v_now_utc,
          finance_case_id = p_finance_case_id,
          reservation_id = null,
          paye_treatment = 'NONE',
          finance_component_id = v_finance_component_id,
          frozen_component_snapshot_json = v_component_snapshot_json,
          frozen_component_key_type = 'CASE_TOTAL',
          frozen_component_key_value = 'TOTAL',
          frozen_component_classification = 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum,
          frozen_source_basis_json = v_component_source_basis_json,
          frozen_source_pay_method = v_candidate_pay_method,
          frozen_target_pay_method = v_candidate_pay_method,
          frozen_resolution_mode = v_component_after.saved_resolution_mode,
          frozen_resolution_payload_json = v_component_after.saved_resolution_payload_json,
          frozen_resolution_result_json = v_component_after.saved_resolution_result_json,
          frozen_source_amount = v_new_principal,
          frozen_target_amount_ex_vat = v_new_principal,
          frozen_target_amount_vat = 0,
          frozen_target_amount_inc_vat = v_new_principal
      where pbi.id = v_batch_item_id;

      update public.pay_batch_item_breakdowns pbib
      set unit_name = 'Payment Advance',
          amount_ex_vat = v_new_principal,
          amount_vat = 0,
          amount_inc_vat = v_new_principal,
          meta_json = jsonb_build_object(
            'case_type', 'PAYMENT_ADVANCE',
            'worker_label', 'Payment Advance'
          )
      where pbib.pay_batch_item_id = v_batch_item_id;
    end if;
  end if;

  select pa.*
  into v_case
  from public.pay_advances pa
  where pa.id = p_finance_case_id;

  v_after_json := jsonb_build_object(
    'original_amount', round(coalesce(v_case.original_amount,0),2),
    'outstanding_amount', round(coalesce(v_case.outstanding_amount,0),2),
    'weekly_due', round(coalesce(v_case.weekly_due,0),2),
    'weeks_total', v_case.weeks_total,
    'start_week_start', case when v_case.start_week_start is null then null else v_case.start_week_start::text end,
    'next_due_week_start', case when v_case.next_due_week_start is null then null else v_case.next_due_week_start::text end,
    'notes', v_case.notes,
    'minimum_earnings_threshold', v_case.minimum_earnings_threshold,
    'take_home_floor_override', v_case.take_home_floor_override,
    'schedule_input_mode', v_schedule_input_mode
  );

  v_component_after_json := jsonb_build_object(
    'finance_component_id', v_component_after.id::text,
    'classification', v_component_after.classification::text,
    'source_pay_method', v_component_after.source_pay_method,
    'source_amount', round(coalesce(v_component_after.source_amount,0),2),
    'remaining_source_amount', round(coalesce(v_component_after.remaining_source_amount,0),2),
    'saved_target_pay_method', v_component_after.saved_target_pay_method,
    'saved_resolution_mode', case when v_component_after.saved_resolution_mode is null then null else v_component_after.saved_resolution_mode::text end,
    'is_resolution_stale', v_component_after.is_resolution_stale,
    'stale_reason', v_component_after.stale_reason,
    'source_basis_json', v_component_after.source_basis_json
  );

  insert into public.pay_finance_case_events(
    finance_case_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    before_json,
    after_json,
    reason,
    note
  )
  values (
    p_finance_case_id,
    'UPDATED',
    v_now_utc,
    p_actor_user_id,
    case when v_batch.id is null then null else v_batch.id end,
    v_before_json,
    v_after_json,
    null::text,
    case when v_can_edit_payout_details then 'Payment Advance payout details updated before commit' else 'Payment Advance future repayment terms updated after commit' end
  );

  insert into public.pay_finance_case_events(
    finance_case_id,
    finance_component_id,
    event_type,
    event_at_utc,
    actor_user_id,
    pay_batch_id,
    before_json,
    after_json,
    reason,
    note
  )
  values (
    p_finance_case_id,
    v_finance_component_id,
    'COMPONENT_UPDATED',
    v_now_utc,
    p_actor_user_id,
    case when v_batch.id is null then null else v_batch.id end,
    v_component_before_json,
    v_component_after_json,
    'PAYMENT_ADVANCE_COMPONENT_UPDATED',
    'Updated Payment Advance net-fixed component state'
  );

  v_component_summary_json := jsonb_build_object(
    'finance_component_id', v_finance_component_id::text,
    'source_family_key', v_source_family_key,
    'component_key_type', 'CASE_TOTAL',
    'component_key_value', 'TOTAL',
    'classification', 'NET_PAY_FIXED_RECOVERY',
    'source_pay_method', v_component_after.source_pay_method,
    'source_amount', round(coalesce(v_component_after.source_amount,0),2),
    'remaining_source_amount', round(coalesce(v_component_after.remaining_source_amount,0),2),
    'saved_target_pay_method', v_component_after.saved_target_pay_method,
    'saved_resolution_mode', case when v_component_after.saved_resolution_mode is null then null else v_component_after.saved_resolution_mode::text end,
    'is_resolution_stale', v_component_after.is_resolution_stale,
    'stale_reason', v_component_after.stale_reason,
    'source_basis_json', v_component_after.source_basis_json
  );

  return jsonb_build_object(
    'ok', true,
    'finance_case_id', p_finance_case_id::text,
    'advance_id', p_finance_case_id::text,
    'case_type', 'PAYMENT_ADVANCE',
    'worker_label', 'Payment Advance',
    'repayment_label', 'Payment Advance Repayment',
    'payout_editable', v_can_edit_payout_details,
    'pay_batch_id', case when v_batch.id is null then null else v_batch.id::text end,
    'schedule_input_mode', v_schedule_input_mode,
    'principal_amount', round(coalesce(v_case.original_amount,0),2),
    'outstanding_amount', round(coalesce(v_case.outstanding_amount,0),2),
    'weekly_due', round(coalesce(v_case.weekly_due,0),2),
    'weeks_total', v_case.weeks_total,
    'start_week_start', case when v_case.start_week_start is null then null else v_case.start_week_start::text end,
    'next_due_week_start', case when v_case.next_due_week_start is null then null else v_case.next_due_week_start::text end,
    'minimum_earnings_threshold', v_case.minimum_earnings_threshold,
    'take_home_floor_override', v_case.take_home_floor_override,
    'schedule_json', coalesce(v_case.schedule_json,'[]'::jsonb),
    'finance_component_id', v_finance_component_id::text,
    'component_summary', v_component_summary_json
  );
end;
$function$
;

do $legacy_routine_acl_convergence$
declare
  v_identity text;
  v_signature pg_catalog.regprocedure;
  v_mode text;
  v_targets text[];
  v_count integer;
begin
  for v_mode,v_targets in
    select mode,targets
    from (values
      ('OWNER_ONLY',array[
      'private._candidate_dataset_overlay_v1(p_payload jsonb)',
      'private._candidate_draft_totals_guard_v1(p_contract_week_id uuid, p_totals_json jsonb)',
      'private._candidate_office_context_overlay_v1(p_payload jsonb)',
      'private._candidate_week_schedule_from_template_v1(p_template jsonb, p_week_ending_date date, p_contract_start date, p_contract_end date)',
      'private._candidate_weekly_final_state_guard_v1(p_contract_week_id uuid, p_timesheet_id uuid, p_timesheet_create_json jsonb, p_timesheet_patch_json jsonb, p_tsfin_snapshot_json jsonb)'
      ]::text[]),
      ('AUTH_SERVICE',array[
      'public._bank_hash(p_sort_code text, p_account_number text, p_account_holder text)',
      'public._inv_collect_weekly_manual_schedule_refs(p_sheet_scope text, p_submission_mode text, p_actual_schedule_json jsonb)',
      'public._inv_day_refs_has_any(day_refs jsonb)',
      'public._inv_iso_utc(ts timestamp with time zone)',
      'public._inv_lock_all_segments_json(p_ib jsonb, p_invoice_id uuid)',
      'public._inv_round2(n numeric)',
      'public._inv_timesheet_has_invoice_reference(p_sheet_scope text, p_submission_mode text, p_reference_number text, p_day_references_json jsonb, p_actual_schedule_json jsonb)',
      'public._inv_weekly_manual_schedule_has_complete_refs(schedule jsonb)',
      'public._pay_convert_paye_to_umbrella(p_paye_ex numeric, p_erni_pct numeric, p_vat_rate_pct numeric, p_vat_chargeable boolean)',
      'public._pay_convert_umbrella_to_paye_ex(p_umbrella_ex numeric, p_erni_pct numeric)',
      'public._pay_csv_parse_line(p_line text)',
      'public._pay_csv_trim_field(p_field text)',
      'public._pay_finance_protected_recovery_allocate(p_recovery_rows jsonb, p_run_earnings_headroom numeric, p_run_take_home_headroom numeric, p_default_take_home_floor numeric)',
      'public._pay_pct_to_frac(p_pct numeric)',
      'public._pay_pct_to_mult(p_pct numeric)',
      'public._pay_umbrella_vat_calc(p_ex numeric, p_vat_rate_pct numeric, p_vat_chargeable boolean)',
      'public._pay_week_start_monday(p_date date)',
      'public._pay_workbench_dirty_payload_merge(p_existing jsonb, p_incoming jsonb)',
      'public._pay_workbench_merge_targeted_scope_payload(p_existing jsonb, p_incoming jsonb)',
      'public._tsfin_invalid_segment_count(invoice_breakdown_json jsonb)',
      'public._wkimp_bucket_hours_from_policy(p_policy jsonb, p_start_utc timestamp with time zone, p_end_utc timestamp with time zone, p_break_mins integer)',
      'public._wkimp_hhmm_to_min(p text)',
      'public._wkimp_overlap_intersection2(a0 integer, b0 integer, ws1 integer, we1 integer, ws2 integer, we2 integer)',
      'public._wkimp_overlap_intersection3(a0 integer, b0 integer, ws1 integer, we1 integer, ws2 integer, we2 integer, ws3 integer, we3 integer)',
      'public._wkimp_overlap_range(a0 integer, b0 integer, s0 integer, e0 integer)',
      'public._wkimp_overlap_window(a0 integer, b0 integer, ws integer, we integer)',
      'public._wkimp_win_parts(ws integer, we integer)',
      'public.calendar_candidate_contracts_range(candidate_id uuid, from_date date, to_date date)',
      'public.calendar_candidate_day_feed(candidate_id uuid, from_date date, to_date date)',
      'public.calendar_contract_day_feed(contract_id uuid, from_date date, to_date date)',
      'public.client_picker_search(p_query text, p_limit integer, p_offset integer, p_nhsp_only boolean, p_hr_auto_only boolean)',
      'public.comms_outbox_claim_ready_batch(p_channel text, p_limit integer, p_attempt_lease_token text, p_lease_minutes integer)',
      'public.enqueue_ts_financials(_timesheet_id uuid, _reason ts_fin_reason_enum)',
      'public.enqueue_ts_financials_priority(_timesheet_ids uuid[], _reason ts_fin_reason_enum)',
      'public.enqueue_tsfin_for_hospital_norm(p_hospital_norm text, p_reason ts_fin_reason_enum, p_priority boolean, p_limit integer)',
      'public.enqueue_tsfin_for_occ_key(p_occ_key_norm text, p_reason ts_fin_reason_enum, p_priority boolean, p_limit integer)',
      'public.hr_autoprocess_apply_phase1(import_id uuid, selected_group_ids text[], p_skip_external_row_keys text[], p_force_overwrite_external_row_keys text[])',
      'public.hr_weekly_preview_mappings_phase1(p_import_id uuid)',
      'public.invoice_no_next()',
      'public.invpdf_dequeue_batch_ids(p_limit integer)',
      'public.invpdf_work_fail_bulk(p_rows jsonb)',
      'public.invpdf_work_success_bulk(p_ids uuid[])',
      'public.nhsp_apply_import_phase1(p_import_id uuid, p_selected_group_ids text[], p_skip_external_row_keys text[], p_force_overwrite_external_row_keys text[])',
      'public.nhsp_preview_mappings_phase1(p_import_id uuid)',
      'public.pay_workbench_compact_job_result_json(p_result_json jsonb)',
      'public.pay_workbench_delta_refresh_family_key(p_session_id uuid, p_candidate_id uuid, p_payload_json jsonb, p_fallback_session_version bigint, p_fallback_projection_mode text, p_fallback_projection_class text, p_fallback_refresh_scope_kind text)',
      'public.pay_workbench_session_compact_progress_json(p_progress_json jsonb, p_keep_active_jobs boolean)',
      'public.rpc_smoke_test()',
      'public.timesheet_pdf_load_context_batch(p_timesheet_ids uuid[])',
      'public.trg_invoices_set_invoice_no()',
      'public.trg_tsfin_client_hospitals_wakeup()',
      'public.trg_tsfin_finance_windows_erni_wakeup()',
      'public.trg_tsfin_finance_windows_erni_wakeup_all()',
      'public.tsfin_dequeue_batch_ids(p_limit integer)',
      'public.tsfin_dequeue_specific(p_timesheet_ids uuid[], p_limit integer)',
      'public.tsfin_load_nhsp_shifts_batch(p_timesheet_ids uuid[])',
      'public.tsfin_load_weekly_context_batch(p_timesheet_ids uuid[])',
      'public.tsfin_mark_revoked(p_timesheet_id uuid)',
      'public.tsfin_prepare_write(p_timesheet_id uuid)',
      'public.tsfin_resolve_rates_batch(p_items jsonb)',
      'public.tsfin_work_fail(p_id uuid, p_error text)',
      'public.tsfin_work_success(p_id uuid)',
      'public.tsfin_work_success_bulk(p_ids uuid[])',
      'public.tspdf_dequeue_batch_ids(p_limit integer)',
      'public.tspdf_enqueue_ready_for_invoice(p_limit integer)',
      'public.tspdf_work_fail_bulk(p_rows jsonb)',
      'public.tspdf_work_success_bulk(p_ids uuid[])',
      'public.weekly_import_apply_phase2(p_import_id uuid, p_system_type text)',
      'public.weekly_import_phase2(p_import_id uuid, p_system_type text)'
      ]::text[]),
      ('OWNER_SERVICE',array[
      'public._pay_batch_item_breakdown_kind_guard_v1()',
      'private._invoice_generation_advance_batch_legacy_20260726(p_claims jsonb, p_now_utc timestamp with time zone)',
      'private._invoice_issue_advance_batch_legacy_20260726(p_claims jsonb, p_now_utc timestamp with time zone)',
      'private._invoice_operation_get_legacy_20260726(p_operation_ids uuid[], p_actor_user_id uuid, p_mode text)',
      'private._invoice_operation_rollup_batch_legacy_20260726(p_operation_ids uuid[], p_now_utc timestamp with time zone, p_propagate_ancestors boolean)',
      'private._invoice_operation_start_batch_legacy_20260726(p_commands jsonb, p_actor_user_id uuid, p_now_utc timestamp with time zone)'
      ]::text[]),
      ('ANON_AUTH_SERVICE',array[
      'public.cloudtms_jsonb_storage_keys_v1(p_document jsonb, p_max_depth integer)'
      ]::text[]),
      ('PUBLIC_ANON_AUTH_SERVICE',array[
      'public.settings_finance_list()',
      'public.settings_finance_pick(p_date date)'
      ]::text[])
    ) as role_sets(mode,targets)
  loop
    foreach v_identity in array v_targets
    loop
      select p.oid::pg_catalog.regprocedure into v_signature
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid=p.pronamespace
      where n.nspname=pg_catalog.split_part(v_identity,'.',1)
        and p.proname||'('||pg_catalog.pg_get_function_identity_arguments(p.oid)||')'
          =substring(v_identity from position('.' in v_identity)+1);
      if v_signature is null then
        raise exception 'LEGACY_ROUTINE_ACL_TARGET_MISSING:%',v_identity;
      end if;

      execute pg_catalog.format(
        'revoke all privileges on function %s from PUBLIC, current_user, service_role, anon, authenticated, authenticator, supabase_admin',
        v_signature
      );
      execute pg_catalog.format('grant execute on function %s to current_user',v_signature);
      if v_mode in ('AUTH_SERVICE','OWNER_SERVICE','ANON_AUTH_SERVICE','PUBLIC_ANON_AUTH_SERVICE') then
        execute pg_catalog.format('grant execute on function %s to service_role',v_signature);
      end if;
      if v_mode in ('AUTH_SERVICE','ANON_AUTH_SERVICE','PUBLIC_ANON_AUTH_SERVICE') then
        execute pg_catalog.format('grant execute on function %s to authenticated',v_signature);
      end if;
      if v_mode in ('ANON_AUTH_SERVICE','PUBLIC_ANON_AUTH_SERVICE') then
        execute pg_catalog.format('grant execute on function %s to anon',v_signature);
      end if;
      if v_mode='PUBLIC_ANON_AUTH_SERVICE' then
        execute pg_catalog.format('grant execute on function %s to PUBLIC',v_signature);
      end if;
    end loop;
  end loop;

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname in ('public','private');
  if v_count<>1375 then
    raise exception 'LEGACY_ROUTINE_CONVERGENCE_COUNT_FAILED:%',v_count;
  end if;
end
$legacy_routine_acl_convergence$;

commit;
