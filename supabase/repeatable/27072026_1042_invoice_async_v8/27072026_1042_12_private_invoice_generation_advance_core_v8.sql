create or replace function private._invoice_generation_advance_core_v8(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
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
$function$;

alter function private._invoice_generation_advance_core_v8(jsonb,timestamptz) owner to postgres;
revoke all on function private._invoice_generation_advance_core_v8(jsonb,timestamptz) from public,anon,authenticated;
grant execute on function private._invoice_generation_advance_core_v8(jsonb,timestamptz) to service_role;
