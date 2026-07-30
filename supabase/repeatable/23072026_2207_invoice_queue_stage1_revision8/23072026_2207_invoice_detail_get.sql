create or replace function public.invoice_detail_get(
  p_invoice_id uuid,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_role text;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_result jsonb;
begin
  if p_invoice_id is null then
    raise exception using errcode='22023',message='invoice_id is required';
  end if;
  if not v_service
     and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Authenticated actor mismatch';
  end if;

  select lower(btrim(coalesce(u.role,''))) into v_role
  from public.tms_users u
  where u.id=p_actor_user_id and u.is_active;
  if(not found or v_role<>'admin') and not v_service then
    raise exception using errcode='42501',
      message='Invoice administrator permission required';
  end if;

  if not exists(select 1 from public.invoices i where i.id=p_invoice_id) then
    return jsonb_build_object('ok',false,'error_code','INVOICE_NOT_FOUND');
  end if;

  with
  inv as materialized (
    select i.* from public.invoices i where i.id=p_invoice_id
  ),
  lines as materialized (
    select l.*,td.r2_key exact_timesheet_document_r2_key,
      pc.precheck_status,pc.has_timesheet_evidence_pdf
    from public.invoice_lines l
    left join public.timesheets t
      on t.timesheet_id=l.timesheet_id and t.is_current
    left join lateral (
      select v.r2_key
      from public.invoice_document_versions v
      where v.entity_type='TIMESHEET'
        and v.entity_id=t.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=t.document_revision::text
        and v.status='READY'
        and nullif(v.r2_key,'') is not null
        and nullif(v.sha256,'') is not null
        and coalesce(v.size_bytes,0)>0
        and coalesce(v.page_count,0)>0
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1
    ) td on true
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=l.timesheet_id
    where l.invoice_id=p_invoice_id
  ),
  source_timesheets as materialized (
    select distinct x.timesheet_id
    from (
      select l.timesheet_id from lines l where l.timesheet_id is not null
      union
      select case when coalesce(l.meta_json->>'timesheet_id','')~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (l.meta_json->>'timesheet_id')::uuid end
      from lines l
      where l.timesheet_id is null
    ) x
    where x.timesheet_id is not null
  ),
  references_batch as materialized (
    select r.*
    from private._invoice_reference_rows_batch(array[p_invoice_id]) r
  ),
  reference_rows as materialized (
    select coalesce(jsonb_agg(
      jsonb_build_object(
        'row_key',r.row_key,'timesheet_id',r.timesheet_id,
        'candidate_id',coalesce(ct.candidate_id,tf.candidate_id),
         'candidate_display',coalesce(nullif(btrim(cc.display_name),''),
           nullif(btrim(cf.display_name),'')),
         'week_ending_date',t.week_ending_date,
         'document_revision',t.document_revision,
         'sheet_scope',r.sheet_scope,'submission_mode',r.submission_mode,
        'ref_target',r.ref_target,'segment_id',r.segment_id,
        'day_ymd',r.day_ymd,'start_utc',r.start_utc,'end_utc',r.end_utc,
        'current_reference',r.current_reference,'is_required',r.is_required)
      order by r.timesheet_id,r.day_ymd nulls last,r.start_utc nulls last,
        r.segment_id nulls last),'[]'::jsonb) rows
    from references_batch r
    left join public.timesheets t
      on t.timesheet_id=r.timesheet_id and t.is_current
    left join public.contracts ct on ct.id=t.contract_id
    left join public.timesheets_financials tf
      on tf.timesheet_id=r.timesheet_id and tf.is_current
    left join public.candidates cc on cc.id=ct.candidate_id
    left join public.candidates cf on cf.id=tf.candidate_id
  ),
  current_financials as materialized (
    select distinct on (tf.timesheet_id) tf.*
    from public.timesheets_financials tf
    join source_timesheets s on s.timesheet_id=tf.timesheet_id
    where tf.is_current
    order by tf.timesheet_id,tf.updated_at desc nulls last,tf.created_at desc nulls last,tf.id desc
  ),
  reference_sources as materialized (
    select coalesce(jsonb_object_agg(t.timesheet_id::text,
       jsonb_build_object(
         'candidate_id',coalesce(ct.candidate_id,f.candidate_id),
         'candidate_display',coalesce(nullif(btrim(cc.display_name),''),
           nullif(btrim(cf.display_name),'')),
         'week_ending_date',t.week_ending_date,
         'document_revision',t.document_revision,
         'reference_number',t.reference_number,
         'hospital_norm',t.hospital_norm,
         'ward_norm',t.ward_norm,
         'source_mode',upper(coalesce(f.invoice_breakdown_json->>'mode','')),
         'day_references_json',t.day_references_json,
        'actual_schedule_json',case
          when upper(coalesce(f.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            and jsonb_typeof(f.invoice_breakdown_json->'segments')='array'
          then coalesce((
            select jsonb_agg(jsonb_build_object(
              'segment_id',seg.value->>'segment_id',
              'date',seg.value->>'date',
              'start_utc',seg.value->>'start_utc',
              'end_utc',seg.value->>'end_utc',
              'start',seg.value->>'start_utc',
              'end',seg.value->>'end_utc',
              'ref_num',seg.value->>'ref_num',
              'source_system',seg.value->>'source_system')
              order by coalesce(seg.value->>'date',''),
                coalesce(seg.value->>'start_utc',''),
                coalesce(seg.value->>'segment_id',''))
            from jsonb_array_elements(f.invoice_breakdown_json->'segments') seg(value)
            where nullif(btrim(coalesce(
              seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
          ),'[]'::jsonb)
          when jsonb_typeof(t.actual_schedule_json)='array'
            then t.actual_schedule_json
          else '[]'::jsonb end)
      order by t.timesheet_id),'{}'::jsonb) rows
     from source_timesheets s
     join public.timesheets t on t.timesheet_id=s.timesheet_id and t.is_current
     left join current_financials f on f.timesheet_id=t.timesheet_id
     left join public.contracts ct on ct.id=t.contract_id
     left join public.candidates cc on cc.id=ct.candidate_id
     left join public.candidates cf on cf.id=f.candidate_id
   ),
  line_totals as materialized (
    select count(*)::integer line_count,
      round(coalesce(sum(l.total_charge_ex_vat),0),2) net,
      round(coalesce(sum(l.vat_amount),0),2) vat,
      round(coalesce(sum(l.total_inc_vat),0),2) gross
    from lines l
  ),
  timesheet_readiness as materialized (
    select t.timesheet_id,t.submission_mode,t.document_state,
      t.current_document_version_id,t.manual_document_asset_id,
      summary.client_no_timesheet_required,summary.client_is_nhsp,
      case
        when coalesce(summary.client_no_timesheet_required,false)
          or coalesce(summary.client_is_nhsp,false) then true
        else dv.status='READY'
      end ready,
      case
        when coalesce(summary.client_no_timesheet_required,false) then 'NOT_REQUIRED'
        when coalesce(summary.client_is_nhsp,false) then 'NHSP_SUPPORT'
        when upper(coalesce(t.submission_mode::text,'')) in('MANUAL','QR')
          then 'MANUAL_TIMESHEET'
        else 'ELECTRONIC_TIMESHEET'
      end support_type,
      coalesce(dv.page_count,0) pages
    from source_timesheets s
    left join public.timesheets t
      on t.timesheet_id=s.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base summary
      on summary.timesheet_id=s.timesheet_id
    left join lateral (
      select v.status,v.page_count
      from public.invoice_document_versions v
      where v.entity_type='TIMESHEET'
        and v.entity_id=t.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=t.document_revision::text
        and v.status='READY'
        and nullif(v.r2_key,'') is not null
        and nullif(v.sha256,'') is not null
        and coalesce(v.size_bytes,0)>0
        and coalesce(v.page_count,0)>0
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1
    ) dv on true
  ),
  evidence_rows as materialized (
    select e.id,e.timesheet_id,e.kind,e.display_name,e.storage_key,e.created_at,
      e.document_asset_id,e.processing_state,e.processing_error_json,
      a.status asset_status,a.normalised_r2_key,a.normalised_sha256,
      a.normalised_size_bytes,a.normalised_page_count,
      case when e.document_asset_id is null then 'NOT_REGISTERED'
        when a.status='READY' then 'READY'
        when a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED') then 'FAILED'
        else 'NOT_READY' end readiness
    from public.timesheet_evidence e
    join source_timesheets s on s.timesheet_id=e.timesheet_id
    left join public.invoice_document_assets a on a.id=e.document_asset_id
  ),
  segment_stats as materialized (
    select f.timesheet_id,f.id tsfin_id,
      coalesce(jsonb_agg(seg.value order by coalesce(seg.value->>'date',''),
        coalesce(seg.value->>'segment_id','')) filter(
          where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'')
            =p_invoice_id::text),'[]'::jsonb) invoiced_segments,
      count(*) filter(where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is null)
        ::integer uninvoiced_segment_count,
      count(*) filter(where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is not null
        and nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'')<>p_invoice_id::text)
        ::integer locked_elsewhere_segment_count
    from current_financials f
    cross join lateral jsonb_array_elements(
      case when upper(coalesce(f.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          and jsonb_typeof(f.invoice_breakdown_json->'segments')='array'
        then f.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg(value)
    group by f.timesheet_id,f.id
  ),
  segment_projection as materialized (
    select coalesce(jsonb_object_agg(s.timesheet_id::text,jsonb_build_object(
      'tsfin_id',s.tsfin_id,'invoiced_segments',s.invoiced_segments,
      'uninvoiced_segment_count',s.uninvoiced_segment_count,
      'locked_elsewhere_segment_count',s.locked_elsewhere_segment_count)
      order by s.timesheet_id),'{}'::jsonb) rows
    from segment_stats s
  ),
  history_audit as materialized (
    select jsonb_build_object(
      'kind','AUDIT','id',ae.id,'ts_utc',ae.ts_utc,
      'actor_user_id',ae.actor_user_id,
      'actor_display',coalesce(ae.actor_display,u.display_name,u.email,'CloudTMS server'),
      'actor_role_at_time',coalesce(ae.actor_role_at_time,u.role,'system'),
      'action',ae.action,'reason',ae.reason,'object_type',ae.object_type,
      'object_id_text',ae.object_id_text,'before_json',ae.before_json,
      'after_json',ae.after_json,'correlation_id',ae.correlation_id) row_json,
      ae.ts_utc
    from public.audit_events ae
    left join public.tms_users u on u.id=ae.actor_user_id
    where ae.object_type in('invoice','invoices')
      and ae.object_id_text=p_invoice_id::text
    order by ae.ts_utc desc,ae.id desc limit 500
  ),
  invoice_mail as materialized (
    select m.*,jsonb_build_object(
      'kind','EMAIL','mail_outbox_id',m.id,'ts_utc',m.created_at_utc,
      'status',m.status,'to',m."to",'cc',m.cc,'subject',m.subject,
      'reference',m.reference,'sent_at',m.sent_at,'failed_at',m.failed_at) row_json
    from public.mail_outbox m
    where upper(coalesce(m.type,''))='INVOICE'
      and jsonb_typeof(m.attachments)='array'
      and exists(
        select 1 from jsonb_array_elements(m.attachments) a(value)
        where a.value->>'invoice_id'=p_invoice_id::text
          or a.value->>'document_version_id'=(
            select i.issued_document_version_id::text from inv i))
    order by m.created_at_utc desc,m.id desc limit 200
  ),
  history_projection as materialized (
    select coalesce(jsonb_agg(x.row_json order by x.ts_utc desc),'[]'::jsonb) rows
    from (
      select h.row_json,h.ts_utc from history_audit h
      union all
      select m.row_json,m.created_at_utc from invoice_mail m
    ) x
  ),
  current_documents as materialized (
    select
      coalesce((
        select jsonb_build_object(
          'id',v.id,'operation_id',v.operation_id,'purpose',v.purpose,
          'source_revision',v.source_revision,'template_version',v.template_version,
          'status',v.status,'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count,
          'created_at_utc',v.created_at_utc,'ready_at_utc',v.ready_at_utc,
          'verified_at_utc',v.verified_at_utc,'error',v.error_json)
        from public.invoice_document_versions v
        join inv i on i.preview_document_version_id=v.id
        where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
          and v.purpose='DRAFT_PREVIEW'),'null'::jsonb) preview,
      coalesce((
        select jsonb_build_object(
          'id',v.id,'operation_id',v.operation_id,'purpose',v.purpose,
          'source_revision',v.source_revision,'template_version',v.template_version,
          'status',v.status,'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count,
          'created_at_utc',v.created_at_utc,'ready_at_utc',v.ready_at_utc,
          'verified_at_utc',v.verified_at_utc,'error',v.error_json)
        from public.invoice_document_versions v
        join inv i on i.issued_document_version_id=v.id
        where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
          and v.purpose='FINAL_ISSUE'),'null'::jsonb) issued,
      coalesce((
        select jsonb_build_object(
          'id',v.id,'operation_id',v.operation_id,'purpose',v.purpose,
          'source_revision',v.source_revision,'template_version',v.template_version,
          'status',v.status,'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count,
          'created_at_utc',v.created_at_utc,'ready_at_utc',v.ready_at_utc,
          'verified_at_utc',v.verified_at_utc,'error',v.error_json)
        from public.invoice_document_versions v
        where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
        order by v.created_at_utc desc,v.id desc limit 1),'null'::jsonb) latest
  ),
  active_document as materialized (
    select o.id operation_id,o.status,o.phase,o.progress_json,o.error_json,o.change_seq
    from public.invoice_operations o
    join inv i on i.active_document_operation_id=o.id
  ),
  active_issue as materialized (
    select c.operation_id,c.id chunk_id,c.status,c.phase,c.progress_json,c.error_json,
      o.change_seq
    from public.invoice_operation_chunks c
    join public.invoice_operations o on o.id=c.operation_id
    where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
      and c.entity_id=p_invoice_id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
     order by c.updated_at_utc desc,c.id desc limit 1
  ),
  source_edit_authority as materialized (
    select
      (
        i.status in('DRAFT','ON_HOLD')
        and i.issued_at_utc is null
        and i.paid_at_utc is null
        and i.active_issue_operation_id is null
        and upper(coalesce(i.issue_state,'')) not in(
          'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
        and not exists(select 1 from active_issue)
        and not exists(
          select 1
          from source_timesheets s
          where coalesce((
            public._ctms_import_correction_classify_v1(s.timesheet_id)
              ->>'is_import_authoritative_correction')::boolean,false)
        )
      ) can_edit_source,
      coalesce((
        select jsonb_agg(code order by ordinal)
        from (
          values
            (1,case when i.status not in('DRAFT','ON_HOLD')
              then 'INVOICE_SOURCE_EDIT_STATUS_FORBIDDEN' end),
            (2,case when i.issued_at_utc is not null
              then 'INVOICE_SOURCE_EDIT_ISSUED' end),
            (3,case when i.paid_at_utc is not null
              then 'INVOICE_SOURCE_EDIT_PAID' end),
            (4,case when i.active_issue_operation_id is not null
              or upper(coalesce(i.issue_state,'')) in(
                'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
              or exists(select 1 from active_issue)
              then 'INVOICE_SOURCE_EDIT_ISSUE_IN_PROGRESS' end),
            (5,case when exists(
              select 1
              from source_timesheets s
              where coalesce((
                public._ctms_import_correction_classify_v1(s.timesheet_id)
                  ->>'is_import_authoritative_correction')::boolean,false))
              then 'IMPORT_AUTHORITATIVE_CORRECTION_SOURCE_EDIT_FORBIDDEN' end)
        ) blockers(ordinal,code)
        where code is not null
      ),'[]'::jsonb) source_edit_blocker_codes,
      exists(
        select 1
        from public.invoice_document_versions dv
        left join public.invoice_operations op on op.id=dv.operation_id
        where dv.entity_type='INVOICE'
          and dv.entity_id=i.id
          and dv.purpose='DRAFT_PREVIEW'
          and dv.source_revision=i.document_revision::text
          and (
            (dv.status='READY'
              and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
              and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
              and coalesce(dv.size_bytes,0)>0
              and coalesce(dv.page_count,0)>0)
            or (
              dv.id=i.preview_document_version_id
              and dv.status in(
                'PLANNING','WAITING_FOR_INPUTS','RENDERING',
                'ASSEMBLING','VERIFYING','READY'))
            or (
              dv.operation_id=i.active_document_operation_id
              and op.operation_type='BUILD_DOCUMENT'
              and op.entity_type='INVOICE'
              and op.entity_id=i.id
              and op.source_revision=i.document_revision::text
              and coalesce(op.input_json->>'purpose','')='DRAFT_PREVIEW'
              and op.status in(
                'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
          )
      ) source_edit_will_replace_preview
    from inv i
  ),
  issue_validation as materialized (
    select v.*
    from inv i
    cross join lateral private._invoice_issue_validate_batch(
      jsonb_build_array(jsonb_build_object(
        'request_key','detail:'||i.id::text,
        'invoice_id',i.id,'expected_revision',i.document_revision,
        'allow_early',false,'deliver',true)),
      (now() at time zone 'Europe/London')::date) v
  ),
  support_summary as materialized (
    select
      count(*) filter(where support_type='ELECTRONIC_TIMESHEET')::integer
        electronic_timesheet_count,
      count(*) filter(where support_type='ELECTRONIC_TIMESHEET'
        and not coalesce(ready,false))::integer electronic_timesheet_not_ready_count,
      count(*) filter(where support_type='MANUAL_TIMESHEET')::integer
        manual_timesheet_count,
      count(*) filter(where support_type='MANUAL_TIMESHEET'
        and not coalesce(ready,false))::integer manual_timesheet_not_ready_count,
      coalesce(sum(pages),0)::integer estimated_timesheet_pages
    from timesheet_readiness
  ),
  evidence_summary as materialized (
    select count(*)::integer evidence_count,
      count(*) filter(where readiness='NOT_REGISTERED')::integer evidence_unregistered_count,
      count(*) filter(where readiness='NOT_READY')::integer evidence_not_ready_count,
      count(*) filter(where readiness='FAILED')::integer evidence_failed_count,
      coalesce(sum(normalised_page_count),0)::integer estimated_evidence_pages
    from evidence_rows
  ),
  projections as materialized (
    select
      (select coalesce(jsonb_agg(
        jsonb_build_object(
          'id',l.id,'invoice_id',l.invoice_id,'timesheet_id',l.timesheet_id,
          'booking_id',l.booking_id,'description',l.description,
          'hours_day',l.hours_day,'hours_night',l.hours_night,
          'hours_sat',l.hours_sat,'hours_sun',l.hours_sun,'hours_bh',l.hours_bh,
          'pay_day',l.pay_day,'pay_night',l.pay_night,'pay_sat',l.pay_sat,
          'pay_sun',l.pay_sun,'pay_bh',l.pay_bh,
          'charge_day',l.charge_day,'charge_night',l.charge_night,
          'charge_sat',l.charge_sat,'charge_sun',l.charge_sun,
          'charge_bh',l.charge_bh,'total_pay_ex_vat',l.total_pay_ex_vat,
          'total_charge_ex_vat',l.total_charge_ex_vat,
          'margin_ex_vat',l.margin_ex_vat,'vat_rate_pct',l.vat_rate_pct,
          'vat_amount',l.vat_amount,'total_inc_vat',l.total_inc_vat,
          'created_at',l.created_at,'paper_ts_r2_key',l.exact_timesheet_document_r2_key,
          'meta_json',l.meta_json,'source_key',l.source_key,
          'precheck_status',l.precheck_status,
          'has_timesheet_evidence_pdf',l.has_timesheet_evidence_pdf,
          'paper_ts_r2_key',l.exact_timesheet_document_r2_key,
          'is_adjustment',l.timesheet_id is null
            or upper(coalesce(l.meta_json->>'line_type',''))='ADJUSTMENT',
          'line_type_norm',upper(coalesce(l.meta_json->>'line_type','')))
        order by l.created_at,l.id),'[]'::jsonb) from lines l) line_rows,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
        'display_name',e.display_name,'storage_key',e.storage_key,
        'created_at',e.created_at,'document_asset_id',e.document_asset_id,
        'processing_state',e.processing_state,
        'processing_error',e.processing_error_json,'asset_status',e.asset_status,
        'normalised_r2_key',e.normalised_r2_key,
        'normalised_sha256',e.normalised_sha256,
        'normalised_size_bytes',e.normalised_size_bytes,
        'normalised_page_count',e.normalised_page_count,
        'readiness',e.readiness) order by e.created_at,e.id),
        '[]'::jsonb) from evidence_rows e) evidence,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
        'display_name',e.display_name,'storage_key',e.storage_key,
        'created_at',e.created_at,'document_asset_id',e.document_asset_id,
        'processing_state',e.processing_state,'asset_status',e.asset_status,
        'normalised_r2_key',e.normalised_r2_key,
        'normalised_sha256',e.normalised_sha256,
        'normalised_size_bytes',e.normalised_size_bytes,
        'normalised_page_count',e.normalised_page_count,
        'readiness',e.readiness) order by e.created_at,e.id),
        '[]'::jsonb) from evidence_rows e
        where upper(coalesce(e.kind,''))='TIMESHEET') timesheet_evidence,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
        'display_name',e.display_name,'storage_key',e.storage_key,
        'created_at',e.created_at,'document_asset_id',e.document_asset_id,
        'processing_state',e.processing_state,'asset_status',e.asset_status,
        'normalised_r2_key',e.normalised_r2_key,
        'normalised_sha256',e.normalised_sha256,
        'normalised_size_bytes',e.normalised_size_bytes,
        'normalised_page_count',e.normalised_page_count,
        'readiness',e.readiness) order by e.created_at,e.id),
        '[]'::jsonb) from evidence_rows e
        where upper(coalesce(e.kind,''))<>'TIMESHEET') evidence_other,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'invoice_id',h.invoice_id,'source_system',h.source_system,
        'import_id',h.import_id,'header_rows',h.header_rows,
        'header_columns',h.header_columns,'rows_json',h.rows_json)
        order by h.source_system,h.import_id),
        '[]'::jsonb) from public.invoice_hr_source_rows h
        where h.invoice_id=p_invoice_id) hr_rows,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'tsfin_id',f.id,'timesheet_id',f.timesheet_id,
        'external_source_rows_json',f.external_source_rows_json,
        'mileage_units',f.mileage_units,'mileage_pay_rate',f.mileage_pay_rate,
        'mileage_charge_rate',f.mileage_charge_rate)
        order by f.timesheet_id),'[]'::jsonb) from current_financials f) tsfin_rows,
      (select coalesce(jsonb_object_agg(f.timesheet_id::text,f.id::text
        order by f.timesheet_id),'{}'::jsonb) from current_financials f) tsfin_map
  ),
  assembled as materialized (
    select i.*,lt.*,rr.rows reference_rows,rs.rows reference_sources,
      sp.rows segments,p.*,hp.rows history_rows,ss.*,es.*,
      sea.can_edit_source,sea.source_edit_blocker_codes,
      sea.source_edit_will_replace_preview,
      cd.preview,cd.issued,cd.latest,
      ad.operation_id document_operation_id,ad.status document_operation_status,
      ad.phase document_operation_phase,ad.progress_json document_operation_progress,
      ad.error_json document_operation_error,ad.change_seq document_change_seq,
      ai.operation_id issue_operation_id,ai.chunk_id issue_chunk_id,
      ai.status issue_status,ai.phase issue_phase,ai.progress_json issue_progress,
      ai.error_json issue_error,ai.change_seq issue_change_seq,
      iv.hard_blocker_codes blocker_codes,iv.document_dependency_codes,
      iv.delivery_blocker_codes,iv.warning_codes,iv.can_issue_only,
      iv.can_issue_and_deliver,iv.route_policy_result,iv.detail_json
    from inv i cross join line_totals lt cross join reference_rows rr
    cross join reference_sources rs cross join segment_projection sp
    cross join projections p cross join history_projection hp
    cross join support_summary ss cross join evidence_summary es
    cross join source_edit_authority sea
    cross join current_documents cd
    left join active_document ad on true left join active_issue ai on true
    left join issue_validation iv on true
  ),
  allowlisted_snapshot as materialized (
    select a.*,
      jsonb_strip_nulls(jsonb_build_object(
        'agency_name',a.header_snapshot_json->'agency_name',
        'agency_logo',a.header_snapshot_json->'agency_logo',
        'agency_logo_url',a.header_snapshot_json->'agency_logo_url',
        'registered_address',a.header_snapshot_json->'registered_address',
        'company_reg_number',a.header_snapshot_json->'company_reg_number',
        'company_registration_number',
          a.header_snapshot_json->'company_registration_number',
        'vat_registration_number',
          a.header_snapshot_json->'vat_registration_number',
        'client_id',a.header_snapshot_json->'client_id',
        'client_name',a.header_snapshot_json->'client_name',
        'client_invoice_address',
          a.header_snapshot_json->'client_invoice_address',
        'client_primary_invoice_email',
          a.header_snapshot_json->'client_primary_invoice_email',
        'issued_at_utc',a.header_snapshot_json->'issued_at_utc',
        'due_at_utc',a.header_snapshot_json->'due_at_utc',
        'payment_terms_days',a.header_snapshot_json->'payment_terms_days',
        'vat_chargeable',a.header_snapshot_json->'vat_chargeable',
        'applied_vat_rate_pct',
          a.header_snapshot_json->'applied_vat_rate_pct',
        'hide_bank_footer',a.header_snapshot_json->'hide_bank_footer',
        'bank',jsonb_strip_nulls(jsonb_build_object(
          'name',a.header_snapshot_json#>'{bank,name}',
          'sort_code',a.header_snapshot_json#>'{bank,sort_code}',
          'account_number',a.header_snapshot_json#>'{bank,account_number}')),
        'stationery_key',a.header_snapshot_json->'stationery_key',
        'stationery_margins_mm',
          a.header_snapshot_json->'stationery_margins_mm',
        'attach_policy',jsonb_strip_nulls(jsonb_build_object(
          'ts_attach_to_invoice',
            a.header_snapshot_json#>'{attach_policy,ts_attach_to_invoice}',
          'hr_attach_to_invoice',
            a.header_snapshot_json#>'{attach_policy,hr_attach_to_invoice}',
          'requires_hr',
            a.header_snapshot_json#>'{attach_policy,requires_hr}')),
        'meta',jsonb_strip_nulls(jsonb_build_object(
          'consolidation_mode',
            a.header_snapshot_json#>'{meta,consolidation_mode}',
          'invoice_week_start',
            a.header_snapshot_json#>'{meta,invoice_week_start}',
          'segment_count',a.header_snapshot_json#>'{meta,segment_count}',
          'self_bill',a.header_snapshot_json#>'{meta,self_bill}',
          'source',a.header_snapshot_json#>'{meta,source}',
          'timesheet_count',
            a.header_snapshot_json#>'{meta,timesheet_count}'))
      )) business_header_snapshot
    from assembled a
  )
  select jsonb_build_object(
    'ok',true,
    'source_edit_queue_contract','INVOICE_SOURCE_EDIT_QUEUE_V1',
    'invoice',jsonb_build_object(
      'id',a.id,'type',a.type,'invoice_no',a.invoice_no,'client_id',a.client_id,
      'issued_at_utc',a.issued_at_utc,'due_at_utc',a.due_at_utc,
      'paid_at_utc',a.paid_at_utc,'status',a.status,
      'status_date_utc',a.status_date_utc,'subtotal_ex_vat',a.subtotal_ex_vat,
      'vat_amount',a.vat_amount,'total_inc_vat',a.total_inc_vat,
      'original_invoice_id',a.original_invoice_id,'notes',a.notes,
      'created_at',a.created_at,'updated_at',a.updated_at,
      'invoice_pdf_r2_key',a.invoice_pdf_r2_key,
      'invoice_pdf_generated_at_utc',a.invoice_pdf_generated_at_utc,
      'header_snapshot_json',a.business_header_snapshot,
      'on_hold_reason',a.on_hold_reason,'do_not_send',a.do_not_send,
      'credit_note_created_at_utc',a.credit_note_created_at_utc,
      'document_revision',a.document_revision,'document_state',a.document_state,
      'preview_document_version_id',a.preview_document_version_id,
      'issued_document_version_id',a.issued_document_version_id,
      'active_document_operation_id',a.active_document_operation_id,
      'issue_state',a.issue_state,
      'active_issue_operation_id',a.active_issue_operation_id,
      'last_document_error_json',a.last_document_error_json),
    'items',a.line_rows,'lines',a.line_rows,
    'header_snapshot_json',coalesce(a.business_header_snapshot,'{}'::jsonb),
    'attach_policy',a.business_header_snapshot->'attach_policy',
    'evidence',a.evidence,'timesheet_evidence',a.timesheet_evidence,
    'evidence_other',a.evidence_other,
    'hr_source_rows_cache',a.hr_rows,
    'tsfin_external_source_rows',a.tsfin_rows,
    'segments_on_invoice_by_timesheet',a.segments,
    'segments_by_timesheet',a.segments,
    'history',a.history_rows,
    'tsfin_id_by_timesheet_id',a.tsfin_map,
    'reference_rows',a.reference_rows,
    'timesheet_reference_sources_by_id',a.reference_sources,
    'email_summary',jsonb_build_object(
      'emailed_once',(select count(*)>0 from invoice_mail),
      'email_count',(select count(*) from invoice_mail),
      'last_email_at_utc',(select max(created_at_utc) from invoice_mail)),
    'financial_validation',jsonb_build_object(
      'stored_net',round(coalesce(a.subtotal_ex_vat,0),2),
      'calculated_net',a.net,'stored_vat',round(coalesce(a.vat_amount,0),2),
      'calculated_vat',a.vat,'stored_gross',round(coalesce(a.total_inc_vat,0),2),
      'calculated_gross',a.gross),
    'document_readiness',jsonb_build_object(
      'revision',a.document_revision,'state',a.document_state,
      'preview',a.preview,'issued',a.issued,'latest',a.latest,
      'operation_id',a.document_operation_id,'operation_status',a.document_operation_status,
      'operation_phase',a.document_operation_phase,'operation_progress',a.document_operation_progress,
      'operation_error',coalesce(a.document_operation_error,a.last_document_error_json),
      'change_seq',a.document_change_seq,
      'electronic_timesheet_count',a.electronic_timesheet_count,
      'electronic_timesheet_not_ready_count',a.electronic_timesheet_not_ready_count,
      'manual_timesheet_count',a.manual_timesheet_count,
      'manual_timesheet_not_ready_count',a.manual_timesheet_not_ready_count,
      'evidence_count',a.evidence_count,
      'evidence_unregistered_count',a.evidence_unregistered_count,
      'evidence_not_ready_count',a.evidence_not_ready_count,
      'evidence_failed_count',a.evidence_failed_count,
      'healthroster_support_count',(select count(*) from public.invoice_hr_source_rows h
        where h.invoice_id=p_invoice_id and upper(coalesce(h.source_system,''))='HEALTHROSTER'),
      'nhsp_support_count',(select count(*) from public.invoice_hr_source_rows h
        where h.invoice_id=p_invoice_id and upper(coalesce(h.source_system,''))='NHSP'),
      'higher_rate_support_count',(select count(*) from lines l
        where upper(coalesce(l.meta_json->>'line_type','')) like '%HIGHER_RATE%'),
      'estimated_supporting_pages',a.estimated_timesheet_pages+a.estimated_evidence_pages),
    'issue',jsonb_build_object(
      'state',a.issue_state,'operation_id',a.issue_operation_id,
      'chunk_id',a.issue_chunk_id,'status',a.issue_status,'phase',a.issue_phase,
      'progress',a.issue_progress,'error',a.issue_error,'change_seq',a.issue_change_seq,
      'validation_detail',a.detail_json,
      'document_dependencies',coalesce(a.document_dependency_codes,'[]'::jsonb),
      'delivery_blockers',coalesce(a.delivery_blocker_codes,'[]'::jsonb),
      'route_policy',coalesce(a.route_policy_result,'{}'::jsonb),
      'warnings',coalesce(a.warning_codes,'[]'::jsonb)),
    'blocker_codes',coalesce(a.blocker_codes,'[]'::jsonb),
    'source_edit_blocker_codes',coalesce(a.source_edit_blocker_codes,'[]'::jsonb),
    'source_edit_will_replace_preview',coalesce(a.source_edit_will_replace_preview,false),
    'actions',jsonb_build_object(
      'can_edit',a.status='DRAFT' and a.issue_operation_id is null,
      'can_edit_source',coalesce(a.can_edit_source,false),
      'can_issue',coalesce(a.can_issue_only,false)
        and a.issue_operation_id is null,
      'can_issue_only',coalesce(a.can_issue_only,false)
        and a.issue_operation_id is null,
      'can_issue_and_deliver',coalesce(a.can_issue_and_deliver,false)
        and a.issue_operation_id is null,
      'can_unissue',a.status='ISSUED',
      'can_retry_document',a.document_state='FAILED',
      'can_retry_issue',coalesce(
        a.issue_status in('BLOCKED','FAILED','DEAD_LETTER'),false))
  ) into v_result
  from allowlisted_snapshot a;

  return v_result;
end;
$function$;

revoke all on function public.invoice_detail_get(uuid,uuid) from public,anon;
grant execute on function public.invoice_detail_get(uuid,uuid)
  to authenticated,service_role;
