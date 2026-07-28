create or replace function private._invoice_document_advance_batch_v6_downstream(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb:='[]'::jsonb;
  v_part jsonb;
begin
  -- BUILD_MANIFEST creates metadata and bounded dependency work only.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='BUILD_MANIFEST'
  ),
  base as materialized (
    select c.*,o.source_revision,o.template_version,
      coalesce(c.payload_json->>'purpose',
        case when c.entity_type='TIMESHEET' then 'TIMESHEET' else 'DRAFT_PREVIEW' end) purpose
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
  ),
  reference_scope as materialized (
    select r.*
    from private._invoice_reference_rows_batch((
      select coalesce(array_agg(distinct b.entity_id),
        array[]::uuid[])
      from base b where b.entity_type='INVOICE'
    )) r
  ),
  legacy_version_seed as materialized (
    select b.*,v.id version_id,
      case when b.purpose='FINAL_ISSUE' then v.snapshot_json
        when b.entity_type='INVOICE' then
          jsonb_build_object(
            'snapshot_schema_version','INVOICE_PRESENTATION_SNAPSHOT_V4',
            'presentation_model',(select jsonb_build_object(
              'document_type',case when i.type='CREDIT_NOTE' then 'CREDIT_NOTE'
                when lower(coalesce(i.header_snapshot_json->>'self_bill','false'))='true' then 'SELF_BILL_INVOICE'
                else 'INVOICE' end,
              'invoice_number',i.invoice_no,
              'issue_date',i.issued_at_utc,
              'preview_date',case when i.status='DRAFT' then v_now else null end,
              'tax_point',i.header_snapshot_json->'tax_point_utc',
              'due_date',i.due_at_utc,
              'currency',coalesce(i.header_snapshot_json->'currency','"GBP"'::jsonb),
              'po_reference',i.header_snapshot_json->'po_reference',
              'payment_terms_days',i.header_snapshot_json->'payment_terms_days',
              'payment_terms_text',i.header_snapshot_json->'payment_terms_text',
              'supplier_legal_name',i.header_snapshot_json->'agency_name',
              'supplier_trading_name',i.header_snapshot_json->'agency_trading_name',
              'supplier_registered_address',i.header_snapshot_json->'registered_address',
              'company_registration_number',i.header_snapshot_json->'company_reg_number',
              'vat_registration_number',i.header_snapshot_json->'vat_registration_number',
              'supplier_contact',i.header_snapshot_json->'supplier_contact',
              'bank_name',i.header_snapshot_json->'bank_name',
              'bank_sort_code',i.header_snapshot_json->'bank_sort_code',
              'bank_account_number',i.header_snapshot_json->'bank_account_number',
              'client_legal_name',i.header_snapshot_json->'client_name',
              'client_billing_address',i.header_snapshot_json->'client_invoice_address',
              'client_reference',i.header_snapshot_json->'client_reference',
              'net_total',i.subtotal_ex_vat,'vat_total',i.vat_amount,'gross_total',i.total_inc_vat,
              'amount_paid',i.header_snapshot_json->'amount_paid',
              'amount_credited',i.header_snapshot_json->'amount_credited',
              'amount_outstanding',i.header_snapshot_json->'amount_outstanding',
              'vat_breakdown',i.header_snapshot_json->'vat_breakdown',
              'credit_note',jsonb_build_object('original_invoice_id',i.original_invoice_id,
                'original_invoice_number',i.header_snapshot_json->'original_invoice_number',
                'original_invoice_date',i.header_snapshot_json->'original_invoice_date',
                'reason',i.header_snapshot_json->'credit_reason'),
              'self_bill_wording',i.header_snapshot_json->'self_bill_wording',
              'legal_wording',i.header_snapshot_json->'legal_wording',
              'attachment_policy',i.header_snapshot_json->'attachment_policy',
              'branding_asset_identity',i.header_snapshot_json->'agency_logo',
              'template_version',b.template_version,'locale','en-GB',
              'page_geometry','A4_PORTRAIT_210X297MM')
              from public.invoices i where i.id=b.entity_id),
            'invoice',(select jsonb_build_object(
              'id',i.id,'type',i.type,'status',i.status,'invoice_no',i.invoice_no,
              'client_id',i.client_id,'status_date_utc',i.status_date_utc,
              'issued_at_utc',i.issued_at_utc,'due_at_utc',i.due_at_utc,
              'subtotal_ex_vat',i.subtotal_ex_vat,'vat_amount',i.vat_amount,
              'total_inc_vat',i.total_inc_vat,'original_invoice_id',i.original_invoice_id,
              'notes',i.notes,'header',i.header_snapshot_json,
              'document_revision',i.document_revision)
              from public.invoices i where i.id=b.entity_id),
            'lines',coalesce((select jsonb_agg(jsonb_build_object(
              'id',l.id,'timesheet_id',l.timesheet_id,'booking_id',l.booking_id,
              'description',l.description,'hours_day',l.hours_day,
              'hours_night',l.hours_night,'hours_sat',l.hours_sat,
              'hours_sun',l.hours_sun,'hours_bh',l.hours_bh,
              'pay_day',l.pay_day,'pay_night',l.pay_night,'pay_sat',l.pay_sat,
              'pay_sun',l.pay_sun,'pay_bh',l.pay_bh,
              'charge_day',l.charge_day,'charge_night',l.charge_night,
              'charge_sat',l.charge_sat,'charge_sun',l.charge_sun,
              'charge_bh',l.charge_bh,'total_pay_ex_vat',l.total_pay_ex_vat,
              'total_charge_ex_vat',l.total_charge_ex_vat,
              'margin_ex_vat',l.margin_ex_vat,'vat_rate_pct',l.vat_rate_pct,
              'vat_amount',l.vat_amount,'total_inc_vat',l.total_inc_vat,
              'source_key',l.source_key,'business_meta',l.meta_json)
              order by l.created_at,l.id)
              from public.invoice_lines l where l.invoice_id=b.entity_id),'[]'::jsonb),
            'timesheet_sources',coalesce((select jsonb_agg(jsonb_build_object(
              'timesheet_id',src.timesheet_id,
              'submission_mode',src.submission_mode,
              'document_revision',src.document_revision,
              'manual_document_asset_id',src.manual_document_asset_id,
              'client_is_nhsp',src.client_is_nhsp,
              'no_timesheet_required',src.no_timesheet_required,
              'attach_timesheet',src.attach_timesheet,
              'render_model',src.render_model) order by src.timesheet_id)
              from (
                select distinct t.timesheet_id,
                  t.submission_mode::text submission_mode,
                  t.document_revision,t.manual_document_asset_id,
                  coalesce(vs.client_is_nhsp,false) client_is_nhsp,
                  coalesce(vs.client_no_timesheet_required,false)
                    no_timesheet_required,
                  coalesce(pc.effective_ts_attach_to_invoice,true)
                    attach_timesheet,
                  jsonb_build_object(
                    'timesheet_id',t.timesheet_id,
                    'booking_id',t.booking_id,
                    'contract_id',t.contract_id,
                    'candidate_display',t.occupant_key_norm,
                    'hospital',t.hospital_norm,'ward',t.ward_norm,
                    'job_title',t.job_title_norm,'band',t.band,
                    'shift_label',t.shift_label_norm,
                    'week_ending_date',t.week_ending_date,
                    'reference_number',t.reference_number,
                    'sheet_scope',t.sheet_scope,
                    'submission_mode',t.submission_mode,
                    'scheduled_start_iso',t.scheduled_start_iso,
                    'scheduled_end_iso',t.scheduled_end_iso,
                    'worked_start_iso',t.worked_start_iso,
                    'worked_end_iso',t.worked_end_iso,
                    'break_start_iso',t.break_start_iso,
                    'break_end_iso',t.break_end_iso,
                    'break_minutes',t.break_minutes,
                    'worked_minutes',t.worked_minutes,
                    'day_references',t.day_references_json,
                    'actual_schedule',t.actual_schedule_json,
                    'additional_units_week',t.additional_units_week,
                    'additional_units_per_day',t.additional_units_per_day,
                    'authorisation',jsonb_build_object(
                      'name',t.auth_name,'job_title',t.auth_job_title,
                      'authorised_at_utc',t.authorised_at_server),
                    'signatures',jsonb_build_object(
                      'nurse_r2_key',t.r2_nurse_key,
                      'authorisation_r2_key',t.r2_auth_key,
                      'nurse_sha256',t.img_sha256_nurse,
                      'authorisation_sha256',t.img_sha256_auth),
                    'qr',jsonb_build_object(
                      'status',t.qr_status,'signed_hash',t.qr_signed_hash,
                      'signed_at_utc',t.qr_signed_at_utc,
                      'immutable_r2_key',t.qr_r2_key),
                    'template_version','timesheet-professional-v1',
                    'version',t.version,
                    'document_revision',t.document_revision,
                    'financials',jsonb_build_object(
                      'id',f.id,'timesheet_version',f.timesheet_version,
                      'basis',f.basis,'hours_day',f.hours_day,
                      'hours_night',f.hours_night,'hours_sat',f.hours_sat,
                      'hours_sun',f.hours_sun,'hours_bh',f.hours_bh,
                      'total_pay_ex_vat',f.total_pay_ex_vat,
                      'total_charge_ex_vat',f.total_charge_ex_vat,
                      'invoice_breakdown',f.invoice_breakdown_json))
                    render_model
                from public.invoice_lines il
                join public.timesheets t
                  on t.timesheet_id=il.timesheet_id and t.is_current
                left join public.timesheets_financials f
                  on f.timesheet_id=t.timesheet_id and f.is_current
                left join public.v_timesheets_summary_base vs
                  on vs.timesheet_id=t.timesheet_id
                left join public.v_ts_invoice_precheck pc
                  on pc.timesheet_id=t.timesheet_id
                where il.invoice_id=b.entity_id and il.timesheet_id is not null
              ) src),'[]'::jsonb),
            'references',coalesce((select jsonb_agg(jsonb_build_object(
              'timesheet_id',r.timesheet_id,'sheet_scope',r.sheet_scope,
              'submission_mode',r.submission_mode,'ref_target',r.ref_target,
              'segment_id',r.segment_id,'day_ymd',r.day_ymd,
              'current_reference',r.current_reference,'is_required',r.is_required,
              'row_key',r.row_key) order by r.row_key)
              from reference_scope r where r.invoice_id=b.entity_id),'[]'::jsonb),
            'supporting_sources',coalesce((select jsonb_agg(jsonb_build_object(
              'source_system',s.source_system,'import_id',s.import_id,
              'header_rows',s.header_rows,'header_columns',s.header_columns,
              'rows',s.rows_json) order by s.source_system,s.import_id)
              from public.invoice_hr_source_rows s
              where s.invoice_id=b.entity_id),'[]'::jsonb))
        else
          jsonb_build_object(
            'snapshot_schema_version','TIMESHEET_PRESENTATION_SNAPSHOT_V4',
            'timesheet',(select jsonb_build_object(
              'timesheet_id',t.timesheet_id,'booking_id',t.booking_id,
              'contract_id',t.contract_id,
              'candidate_display',t.occupant_key_norm,
              'hospital',t.hospital_norm,'ward',t.ward_norm,
              'job_title',t.job_title_norm,'band',t.band,
              'shift_label',t.shift_label_norm,
              'week_ending_date',t.week_ending_date,'reference_number',t.reference_number,
              'sheet_scope',t.sheet_scope,'submission_mode',t.submission_mode,
              'scheduled_start_iso',t.scheduled_start_iso,
              'scheduled_end_iso',t.scheduled_end_iso,
              'worked_start_iso',t.worked_start_iso,
              'worked_end_iso',t.worked_end_iso,
              'break_start_iso',t.break_start_iso,
              'break_end_iso',t.break_end_iso,
              'break_minutes',t.break_minutes,'worked_minutes',t.worked_minutes,
              'day_references',t.day_references_json,
              'actual_schedule',t.actual_schedule_json,
              'additional_units_week',t.additional_units_week,
              'additional_units_per_day',t.additional_units_per_day,
              'authorisation',jsonb_build_object(
                'name',t.auth_name,'job_title',t.auth_job_title,
                'authorised_at_utc',t.authorised_at_server),
              'signatures',jsonb_build_object(
                'nurse_r2_key',t.r2_nurse_key,
                'authorisation_r2_key',t.r2_auth_key,
                'nurse_sha256',t.img_sha256_nurse,
                'authorisation_sha256',t.img_sha256_auth),
              'qr',jsonb_build_object(
                'status',t.qr_status,'signed_hash',t.qr_signed_hash,
                'signed_at_utc',t.qr_signed_at_utc,
                'immutable_r2_key',t.qr_r2_key),
              'template_version','timesheet-professional-v1',
              'version',t.version,'document_revision',t.document_revision)
              from public.timesheets t
              where t.timesheet_id=b.entity_id and t.is_current),
            'financials',(select jsonb_build_object(
              'id',f.id,'timesheet_version',f.timesheet_version,'basis',f.basis,
              'hours_day',f.hours_day,'hours_night',f.hours_night,
              'hours_sat',f.hours_sat,'hours_sun',f.hours_sun,'hours_bh',f.hours_bh,
              'total_pay_ex_vat',f.total_pay_ex_vat,
              'total_charge_ex_vat',f.total_charge_ex_vat,
              'invoice_breakdown',f.invoice_breakdown_json)
              from public.timesheets_financials f
              where f.timesheet_id=b.entity_id and f.is_current))
        end
      snapshot_json
    from base b
    join public.invoice_document_versions v
      on v.id=b.document_version_id and v.operation_id=b.operation_id
      and v.entity_type=b.entity_type and v.entity_id=b.entity_id
      and v.purpose=b.purpose
  ),
  presentation_batch as materialized (
    select p.*
    from private._invoice_presentation_snapshot_batch(
      (select coalesce(jsonb_agg(jsonb_build_object(
        'request_key',s.id::text,
        'entity_type',s.entity_type,
        'entity_id',s.entity_id,
        'purpose',s.purpose,
        'template_version',s.template_version
      ) order by s.id),'[]'::jsonb)
      from legacy_version_seed s
      where s.purpose<>'FINAL_ISSUE'),
      v_now
    ) p
  ),
  version_seed as materialized (
    select s.*,
      case when s.purpose='FINAL_ISSUE' then s.snapshot_json
        else p.snapshot_json end snapshot_json_v5,
      p.snapshot_hash snapshot_hash_v5,
      p.valid presentation_valid,
      p.error_code presentation_error_code,
      p.error_detail presentation_error_detail
    from legacy_version_seed s
    left join presentation_batch p on p.request_key=s.id::text
  ),
  missing_versions as materialized (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        error_json=jsonb_build_object(
          'code',coalesce(s.presentation_error_code,'DOCUMENT_VERSION_REQUIRED'),
          'document_version_id',c.document_version_id,
          'detail',coalesce(s.presentation_error_detail,'{}'::jsonb)),
        updated_at_utc=v_now
    from base b
    left join version_seed s on s.id=b.id
    where c.id=b.id and (s.id is null
      or (s.purpose<>'FINAL_ISSUE' and coalesce(s.presentation_valid,false)=false))
    returning c.id,c.status,c.phase,c.document_version_id,c.error_json
  ),
  updated_versions as (
    update public.invoice_document_versions v
    set snapshot_json=case when s.purpose='FINAL_ISSUE'
          then v.snapshot_json else s.snapshot_json_v5 end,
        snapshot_hash=case when s.purpose='FINAL_ISSUE'
          then v.snapshot_hash else s.snapshot_hash_v5 end,
        status=case when v.status='FAILED' then 'PLANNING' else v.status end
    from version_seed s where v.id=s.version_id
      and (s.purpose='FINAL_ISSUE' or s.presentation_valid)
    returning v.id
  ),
  linked as materialized (
    select c.*,s.purpose,s.version_id resolved_document_version_id
    from public.invoice_operation_chunks c join version_seed s on s.id=c.id
      and (s.purpose='FINAL_ISSUE' or s.presentation_valid)
  ),
  invoice_timesheets as materialized (
    select distinct l.id chunk_id,l.resolved_document_version_id document_version_id,
      l.operation_id,il.timesheet_id,t.submission_mode::text submission_mode,
      t.document_revision,t.manual_document_asset_id,
      coalesce(v.client_is_nhsp,false) client_is_nhsp,
      coalesce(v.client_no_timesheet_required,false) no_timesheet_required,
      coalesce(pc.effective_ts_attach_to_invoice,true) attach_timesheet,
      false frozen_source
    from linked l
    join public.invoice_lines il on l.entity_type='INVOICE' and il.invoice_id=l.entity_id
    join public.timesheets t on t.timesheet_id=il.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base v on v.timesheet_id=t.timesheet_id
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=t.timesheet_id
    where l.purpose<>'FINAL_ISSUE'
    union
    select l.id,l.resolved_document_version_id,l.operation_id,t.timesheet_id,
      t.submission_mode::text,t.document_revision,t.manual_document_asset_id,
      coalesce(v.client_is_nhsp,false),coalesce(v.client_no_timesheet_required,false),
      true,false
    from linked l join public.timesheets t
      on l.entity_type='TIMESHEET' and t.timesheet_id=l.entity_id and t.is_current
    left join public.v_timesheets_summary_base v on v.timesheet_id=t.timesheet_id
    union
    select l.id,l.resolved_document_version_id,l.operation_id,
      case when coalesce(x.value->>'timesheet_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (x.value->>'timesheet_id')::uuid end,
      x.value->>'submission_mode',
      case when coalesce(x.value->>'document_revision','')~'^[0-9]+$'
        then (x.value->>'document_revision')::bigint end,
      case when coalesce(x.value->>'manual_document_asset_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(x.value->>'manual_document_asset_id')::uuid end,
      lower(coalesce(x.value->>'client_is_nhsp','false')) in('true','t','1','yes'),
      lower(coalesce(x.value->>'no_timesheet_required','false')) in('true','t','1','yes'),
      lower(coalesce(x.value->>'attach_timesheet','true')) in('true','t','1','yes'),true
    from linked l
    join public.invoice_document_versions dv
      on dv.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(dv.snapshot_json->'timesheet_sources')='array'
        then dv.snapshot_json->'timesheet_sources' else '[]'::jsonb end) x(value)
    where l.entity_type='INVOICE' and l.purpose='FINAL_ISSUE'
      and coalesce(x.value->>'timesheet_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and coalesce(x.value->>'document_revision','')~'^[0-9]+$'
  ),
  evidence_candidates as materialized (
    select distinct it.chunk_id,it.document_version_id,it.operation_id,
      te.id source_id,'TIMESHEET_EVIDENCE'::text source_kind,te.storage_key original_r2_key,
      te.display_name,upper(coalesce(te.kind,'OTHER')) kind,
      coalesce(nullif(te.source_revision,''),encode(digest(concat_ws('|',
        te.id::text,te.storage_key,te.created_at::text),'sha256'),'hex'))
        source_revision,te.document_asset_id,te.created_at
    from invoice_timesheets it join public.timesheet_evidence te
      on te.timesheet_id=it.timesheet_id
    join linked root on root.id=it.chunk_id and root.entity_type='INVOICE'
    where not it.frozen_source
      and nullif(btrim(coalesce(te.storage_key,'')),'') is not null
      and upper(coalesce(te.kind,''))<>'TIMESHEET'
      and(
        upper(coalesce(te.kind,''))='MILEAGE' and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type',''))='MILEAGE')
        or upper(coalesce(te.kind,''))='TRAVEL' and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type','')) like '%TRAVEL%')
        or upper(coalesce(te.kind,''))='ACCOMMODATION' and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type','')) like '%ACCOMMODATION%')
        or upper(coalesce(te.kind,'')) not in(
          'MILEAGE','TRAVEL','ACCOMMODATION') and exists(
          select 1 from public.invoice_lines il
          where il.invoice_id=root.entity_id and il.timesheet_id=it.timesheet_id
            and upper(coalesce(il.meta_json->>'line_type','')) like '%EXPENSE%'))
    union
    select distinct it.chunk_id,it.document_version_id,it.operation_id,
      it.timesheet_id,'MANUAL_TIMESHEET',a.original_r2_key,
      coalesce(a.original_filename,'Manual timesheet'),'TIMESHEET',
      a.source_revision,a.id,a.created_at_utc
    from invoice_timesheets it join public.invoice_document_assets a
      on a.id=it.manual_document_asset_id
    where upper(coalesce(it.submission_mode,'')) in('MANUAL','QR')
      and it.attach_timesheet and not it.no_timesheet_required
    union
    select distinct l.id,l.resolved_document_version_id,l.operation_id,
      (x.value->>'evidence_id')::uuid,'TIMESHEET_EVIDENCE',
      coalesce(x.value->>'original_r2_key',x.value->>'storage_key'),
      coalesce(x.value->>'display_name',x.value->>'kind','Evidence'),
      upper(coalesce(x.value->>'kind','OTHER')),
      x.value->>'source_revision',
      (x.value->>'asset_id')::uuid,v.created_at_utc
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(v.snapshot_json->'supporting_manifest')='array'
        then v.snapshot_json->'supporting_manifest' else '[]'::jsonb end) x(value)
    where l.purpose='FINAL_ISSUE'
      and upper(coalesce(x.value->>'kind',''))<>'TIMESHEET'
      and coalesce(x.value->>'asset_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and coalesce(x.value->>'evidence_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),
  evidence_items as materialized (
    select e.chunk_id,e.document_version_id,e.operation_id,
      2000+row_number() over(partition by e.chunk_id
        order by case e.kind when 'TIMESHEET' then 0 when 'MILEAGE' then 1
          when 'TRAVEL' then 2 when 'ACCOMMODATION' then 3 else 4 end,
          e.created_at,e.source_id)::integer ordinal,
      e.source_id,e.source_kind,e.original_r2_key,e.display_name,e.kind,
      e.source_revision,e.document_asset_id
    from evidence_candidates e
  ),
  assets as materialized (
    select e.*,a.id asset_id,a.status asset_status,a.normalised_page_count,
      a.normalised_size_bytes,a.normalised_r2_key,a.normalised_manifest_json,
      a.normalised_sha256,a.normalised_manifest_hash,
      a.operation_id asset_operation_id
      ,exists(
        select 1 from public.invoice_operations ao
        where ao.id=a.operation_id
          and ao.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT'))
        asset_operation_active
    from evidence_items e left join public.invoice_document_assets a
      on a.id=e.document_asset_id
      and a.source_kind=e.source_kind and a.source_id=e.source_id
      and a.source_revision=e.source_revision and a.original_r2_key=e.original_r2_key
  ),
  render_items as materialized (
    select it.chunk_id,it.document_version_id,
      1000+row_number() over(partition by it.chunk_id order by it.timesheet_id)::integer ordinal,
      'ELECTRONIC_TIMESHEET'::text input_type,'TIMESHEET'::text source_entity_type,
      it.timesheet_id source_entity_id,it.document_revision::text source_revision,
      'Electronic timesheet'::text display_label,null::integer expected_page_count,
      'TIMESHEET_POLICY'::text inclusion_reason
    from invoice_timesheets it
    join linked root on root.id=it.chunk_id and root.entity_type='INVOICE'
    where upper(coalesce(it.submission_mode,''))='ELECTRONIC'
      and it.attach_timesheet and not it.client_is_nhsp and not it.no_timesheet_required
    union all
    select l.id,l.resolved_document_version_id,
      3000+row_number() over(partition by l.id
        order by r.source_system,r.import_id)::integer,
      case when upper(r.source_system)='NHSP' then 'NHSP_SUPPORT'
        else 'HEALTHROSTER_SUPPORT' end,
      upper(r.source_system),r.import_id,
      encode(digest(concat_ws('|',r.import_id::text,r.header_rows::text,
        r.header_columns::text,r.rows_json::text),'sha256'),'hex'),
      initcap(lower(r.source_system))||' supporting report',null::integer,
      'FROZEN_SOURCE_SUPPORT'
    from linked l join public.invoice_hr_source_rows r
      on l.entity_type='INVOICE' and r.invoice_id=l.entity_id
    where l.purpose<>'FINAL_ISSUE'
    union all
    select l.id,l.resolved_document_version_id,
      3000+x.ordinality::integer,
      case when upper(x.value->>'source_system')='NHSP' then 'NHSP_SUPPORT'
        else 'HEALTHROSTER_SUPPORT' end,
      upper(x.value->>'source_system'),
      (x.value->>'import_id')::uuid,
      encode(digest(concat_ws('|',x.value->>'import_id',
        (x.value->'header_rows')::text,(x.value->'header_columns')::text,
        (x.value->'rows')::text),'sha256'),'hex'),
      initcap(lower(x.value->>'source_system'))||' supporting report',
      null::integer,'FROZEN_SOURCE_SUPPORT'
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(v.snapshot_json->'source_support')='array'
        then v.snapshot_json->'source_support' else '[]'::jsonb end)
      with ordinality x(value,ordinality)
    where l.purpose='FINAL_ISSUE'
      and coalesce(x.value->>'import_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    union all
    select l.id,l.resolved_document_version_id,4000,'HIGHER_RATE_SUPPORT',
      'INVOICE',l.entity_id,
      encode(digest(coalesce(jsonb_agg(jsonb_build_object(
        'line_id',il.id,'source_key',il.source_key,'meta',il.meta_json)
        order by il.id)::text,'[]'),'sha256'),'hex'),
      'Higher-rate supporting schedule',null::integer,'HIGHER_RATE_POLICY'
    from linked l join public.invoice_lines il
      on l.entity_type='INVOICE' and il.invoice_id=l.entity_id
    where l.purpose<>'FINAL_ISSUE'
      and (upper(coalesce(il.meta_json->>'line_type','')) like '%HIGHER%'
        or il.meta_json ? 'higher_rate')
    group by l.id,l.resolved_document_version_id,l.entity_id
    union all
    select l.id,l.resolved_document_version_id,4000,'HIGHER_RATE_SUPPORT',
      'INVOICE',l.entity_id,
      encode(digest(coalesce(jsonb_agg(x.value order by x.ordinality)::text,
        '[]'),'sha256'),'hex'),
      'Higher-rate supporting schedule',null::integer,'HIGHER_RATE_POLICY'
    from linked l
    join public.invoice_document_versions v
      on v.id=l.resolved_document_version_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(v.snapshot_json->'lines')='array'
        then v.snapshot_json->'lines' else '[]'::jsonb end)
      with ordinality x(value,ordinality)
    where l.purpose='FINAL_ISSUE'
      and (upper(coalesce(x.value#>>'{business_meta,line_type}','')) like '%HIGHER%'
        or coalesce(x.value->'business_meta','{}'::jsonb)?'higher_rate')
    group by l.id,l.resolved_document_version_id,l.entity_id
  ),
  section_pages as materialized (
    select l.id chunk_id,l.resolved_document_version_id document_version_id,
      100::integer ordinal,'ATTACHMENT_INDEX'::text input_type,
      'DOCUMENT'::text source_entity_type,l.resolved_document_version_id source_entity_id,
      encode(digest(concat_ws('|',l.resolved_document_version_id::text,
        count(a.asset_id)::text,count(r.source_entity_id)::text),'sha256'),'hex') source_revision,
      null::uuid asset_id,null::uuid input_document_version_id,
      'Attachment index'::text display_label,null::integer expected_page_count,
      'ATTACHMENT_INDEX'::text inclusion_reason
    from linked l
    left join assets a on a.chunk_id=l.id
    left join render_items r on r.chunk_id=l.id
    where l.entity_type='INVOICE'
    group by l.id,l.resolved_document_version_id
    having count(a.asset_id)>0 or count(r.source_entity_id)>0
    union all
    select l.id,l.resolved_document_version_id,900,'SECTION_SEPARATOR',
      'DOCUMENT',l.resolved_document_version_id,
      encode(digest(l.resolved_document_version_id::text||
        '|ELECTRONIC_TIMESHEETS','sha256'),'hex'),
      null,null,'Timesheets',1,'SECTION_SEPARATOR'
    from linked l
    where exists(select 1 from render_items r
      where r.chunk_id=l.id and r.input_type='ELECTRONIC_TIMESHEET')
    union all
    select l.id,l.resolved_document_version_id,1900,'SECTION_SEPARATOR',
      'DOCUMENT',l.resolved_document_version_id,
      encode(digest(l.resolved_document_version_id::text||
        '|EVIDENCE','sha256'),'hex'),
      null,null,'Timesheet and evidence attachments',1,'SECTION_SEPARATOR'
    from linked l
    where exists(select 1 from assets a where a.chunk_id=l.id)
    union all
    select l.id,l.resolved_document_version_id,2900,'SECTION_SEPARATOR',
      'DOCUMENT',l.resolved_document_version_id,
      encode(digest(l.resolved_document_version_id::text||
        '|SOURCE_SUPPORT','sha256'),'hex'),
      null,null,'HealthRoster and NHSP support',1,'SECTION_SEPARATOR'
    from linked l
    where exists(select 1 from render_items r
      where r.chunk_id=l.id and r.input_type in(
        'HEALTHROSTER_SUPPORT','NHSP_SUPPORT'))
    union all
    select l.id,l.resolved_document_version_id,3900,'SECTION_SEPARATOR',
      'DOCUMENT',l.resolved_document_version_id,
      encode(digest(l.resolved_document_version_id::text||
        '|HIGHER_RATE_SUPPORT','sha256'),'hex'),
      null,null,'Higher-rate support',1,'SECTION_SEPARATOR'
    from linked l
    where exists(select 1 from render_items r
      where r.chunk_id=l.id and r.input_type='HIGHER_RATE_SUPPORT')
  ),
  manifest_items_raw as materialized (
    select l.id chunk_id,l.resolved_document_version_id document_version_id,0 ordinal,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE'
        else 'ELECTRONIC_TIMESHEET' end input_type,l.entity_type source_entity_type,
      l.entity_id source_entity_id,
      coalesce((select source_revision from public.invoice_document_versions v where v.id=l.resolved_document_version_id),'') source_revision,
      null::uuid asset_id,null::uuid input_document_version_id,'Invoice core' display_label,
      null::integer expected_page_count,'CORE' inclusion_reason
    from linked l
    where l.entity_type='INVOICE'
       or not exists(
         select 1 from invoice_timesheets it
         where it.chunk_id=l.id
           and upper(coalesce(it.submission_mode,'')) in('MANUAL','QR')
           and it.manual_document_asset_id is not null)
    union all
    select s.chunk_id,s.document_version_id,s.ordinal,s.input_type,
      s.source_entity_type,s.source_entity_id,s.source_revision,s.asset_id,
      s.input_document_version_id,s.display_label,s.expected_page_count,s.inclusion_reason
    from section_pages s
    union all
    select r.chunk_id,r.document_version_id,r.ordinal,r.input_type,
      r.source_entity_type,r.source_entity_id,r.source_revision,
      null::uuid,null::uuid,r.display_label,r.expected_page_count,r.inclusion_reason
    from render_items r
    union all
    select a.chunk_id,a.document_version_id,a.ordinal,'ASSET',a.source_kind,a.source_id,a.source_revision,
      a.asset_id,null,a.display_name,a.normalised_page_count,'CONFIGURED_EVIDENCE'
    from assets a
  ),
  manifest_items as materialized (
    select r.chunk_id,r.document_version_id,
      row_number() over(partition by r.chunk_id
        order by r.ordinal,r.input_type,r.source_entity_type,
          r.source_entity_id,r.asset_id)::integer-1 ordinal,
      r.input_type,r.source_entity_type,r.source_entity_id,r.source_revision,
      r.asset_id,r.input_document_version_id,r.display_label,
      r.expected_page_count,r.inclusion_reason
    from manifest_items_raw r
  ),
  manifests as materialized (
    select m.chunk_id,m.document_version_id,
      jsonb_agg(jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,
        'source_entity_type',m.source_entity_type,'source_entity_id',m.source_entity_id,
        'source_revision',m.source_revision,'document_asset_id',m.asset_id,
        'input_document_version_id',m.input_document_version_id,'display_label',m.display_label,
        'expected_page_count',m.expected_page_count,'inclusion_reason',m.inclusion_reason,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,
          m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,
          m.source_revision),'sha256'),'hex'))
        order by m.ordinal,m.source_entity_id) manifest_json
    from manifest_items m group by m.chunk_id,m.document_version_id
  ),
  version_updates as (
    update public.invoice_document_versions v
    set manifest_json=m.manifest_json,manifest_hash=encode(digest(m.manifest_json::text,'sha256'),'hex'),
      status='WAITING_FOR_INPUTS',
      expected_page_count=(select sum(coalesce(x.expected_page_count,0)) from manifest_items x where x.chunk_id=m.chunk_id)
    from manifests m where v.id=m.document_version_id returning v.id
  ),
  core_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,
      case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER' else 'SOURCE_RENDER' end,
      'RENDER',
      encode(digest(concat_ws('|',
        case when l.entity_type='INVOICE' then 'INVOICE_CORE_RENDER'
          else 'SOURCE_RENDER' end,l.resolved_document_version_id::text,
        '0',mi.input_type,mi.source_revision,
        (select template_version from public.invoice_document_versions v
          where v.id=l.resolved_document_version_id),'1'),'sha256'),'hex'),
      0,l.entity_type,l.entity_id,l.resolved_document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,
        'template_version',(select template_version from public.invoice_document_versions v where v.id=l.resolved_document_version_id),
        'source_chunk_key',encode(digest(concat_ws('|',l.resolved_document_version_id::text,
          '0',case when l.entity_type='INVOICE' then 'INVOICE_CORE' else 'ELECTRONIC_TIMESHEET' end,
          l.entity_type,l.entity_id::text,
          (select source_revision from public.invoice_document_versions v
            where v.id=l.resolved_document_version_id)),'sha256'),'hex')),
      o.control_version,v_now,v_now
    from linked l
    join manifest_items mi on mi.chunk_id=l.id and mi.ordinal=0
    join public.invoice_operations o on o.id=l.operation_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning id,operation_id
  ),
  source_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'SOURCE_RENDER','RENDER',
      encode(digest(concat_ws('|','SOURCE_RENDER',m.document_version_id::text,
        m.ordinal::text,m.input_type,m.source_revision,
        (select template_version from public.invoice_document_versions v
          where v.id=l.resolved_document_version_id),'1'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,
      m.source_entity_id,l.resolved_document_version_id,'QUEUED',l.priority,v_now,
      jsonb_build_object('render_kind',m.input_type,'source_revision',m.source_revision,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,
          m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,
          m.source_revision),'sha256'),'hex'),
        'template_version',(select template_version from public.invoice_document_versions v
          where v.id=l.resolved_document_version_id)),
      o.control_version,v_now,v_now
    from linked l join manifest_items m on m.chunk_id=l.id
    join public.invoice_operations o on o.id=l.operation_id
    where m.ordinal>0 and m.input_type not in('ASSET','ATTACHMENT_INDEX')
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id
  ),
  asset_chunks as (
    select null::uuid id,null::uuid operation_id,
      null::uuid document_asset_id
    where false
  ),
  asset_owner_update as (
    update public.invoice_document_assets a
    set operation_id=c.operation_id,status='INSPECTING',updated_at_utc=v_now
    from asset_chunks c
    where a.id=c.document_asset_id and a.status='DISCOVERED'
    returning a.id
  ),
  input_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,document_asset_id,status,priority,run_after_utc,payload_json,
      result_json,error_json,expected_page_count,actual_page_count,expected_byte_count,actual_byte_count,
      operation_control_version,created_at_utc,updated_at_utc)
    select l.operation_id,'DOCUMENT_INPUT','DEPENDENCY',
      encode(digest(concat_ws('|','DOCUMENT_INPUT',m.document_version_id::text,
        m.ordinal::text,m.input_type,m.source_revision,
        coalesce(m.asset_id::text,m.input_document_version_id::text,
          m.source_entity_id::text),'1'),'sha256'),'hex'),
      m.ordinal,m.source_entity_type,m.source_entity_id,
      l.resolved_document_version_id,m.asset_id,
      case when m.input_type='ASSET' and a.status='READY' then 'COMPLETE'
        when m.input_type='ASSET' and m.asset_id is null then 'BLOCKED'
        when m.input_type='ASSET' and a.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED') then 'BLOCKED'
        when m.input_type='ASSET' and not coalesce(ax.asset_operation_active,false)
          then 'BLOCKED'
        else 'WAITING' end,
      l.priority,v_now,
      jsonb_build_object('ordinal',m.ordinal,'input_type',m.input_type,
        'display_label',m.display_label,
        'source_revision',m.source_revision,'source_entity_type',m.source_entity_type,
        'source_entity_id',m.source_entity_id,
        'source_chunk_key',encode(digest(concat_ws('|',m.document_version_id::text,
          m.ordinal::text,m.input_type,m.source_entity_type,m.source_entity_id::text,
          m.source_revision),'sha256'),'hex')),
      case when m.input_type='ASSET' and a.status='READY' then jsonb_build_object(
        'r2_key',a.normalised_r2_key,'parts',a.normalised_manifest_json,
        'sha256',a.normalised_sha256,
        'normalised_manifest_hash',a.normalised_manifest_hash,
        'size_bytes',a.normalised_size_bytes,
        'page_count',a.normalised_page_count,'source_revision',m.source_revision,
        'document_asset_id',a.id) end,
      case when m.input_type='ASSET' and a.status in(
        'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        then jsonb_build_object('code','ASSET_DEPENDENCY_PERMANENT_FAILURE',
          'asset_status',a.status,'document_asset_id',a.id,'source_entity_id',m.source_entity_id)
        when m.input_type='ASSET' and m.asset_id is null
        then jsonb_build_object('code','ASSET_NOT_REGISTERED',
          'source_entity_id',m.source_entity_id,
          'source_revision',m.source_revision)
        when m.input_type='ASSET' and not coalesce(ax.asset_operation_active,false)
        then jsonb_build_object('code','ASSET_PREPARATION_NOT_STARTED',
          'document_asset_id',m.asset_id,'source_entity_id',m.source_entity_id,
          'source_revision',m.source_revision)
      end,
      m.expected_page_count,
      case when m.input_type='ASSET' and a.status='READY'
        then a.normalised_page_count end,
      case when m.input_type='ASSET' and a.status='READY'
        then a.normalised_size_bytes end,
      case when m.input_type='ASSET' and a.status='READY'
        then a.normalised_size_bytes end,
      o.control_version,v_now,v_now
    from linked l join manifest_items m on m.chunk_id=l.id
    left join public.invoice_document_assets a on a.id=m.asset_id
    left join assets ax on ax.chunk_id=l.id and ax.asset_id=m.asset_id
    join public.invoice_operations o on o.id=l.operation_id
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set
      status=case when excluded.status in('COMPLETE','BLOCKED') then excluded.status
        else invoice_operation_chunks.status end,
      result_json=coalesce(excluded.result_json,invoice_operation_chunks.result_json),
      error_json=coalesce(excluded.error_json,invoice_operation_chunks.error_json),
      actual_page_count=coalesce(excluded.actual_page_count,
        invoice_operation_chunks.actual_page_count),
      actual_byte_count=coalesce(excluded.actual_byte_count,
        invoice_operation_chunks.actual_byte_count)
      where invoice_operation_chunks.status not in('SUPERSEDED','CANCELLED')
    returning id
  ),
  advanced as (
    update public.invoice_operation_chunks c
    set document_version_id=l.resolved_document_version_id,phase='WAIT_FOR_INPUTS',status='WAITING',
      progress_json=jsonb_build_object('status_message','Waiting for document inputs',
        'manifest_items',(select jsonb_array_length(v.manifest_json) from public.invoice_document_versions v where v.id=l.resolved_document_version_id)),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from linked l where c.id=l.id
    returning c.id,c.status,c.phase,c.document_version_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'document_version_id',document_version_id,'error',error_json)),'[]'::jsonb)
  into v_part
  from(
    select id,status,phase,document_version_id,null::jsonb error_json from advanced
    union all
    select id,status,phase,document_version_id,error_json from missing_versions
  ) outcomes;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- WAIT_FOR_INPUTS: dependencies only; no R2 polling.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='WAIT_FOR_INPUTS'
  ),
  claim_scope as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,c.priority,
      o.control_version,v.template_version,
      o.config_json->'processor_policy' processor_policy
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    join public.invoice_document_versions v on v.id=c.document_version_id
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      (select array_agg(operation_id) from claim_scope),null,null,10000) g
    where g.replacement_chain_status='VALID'
  ),
  current_inputs as materialized (
    select d.*
    from current_graph g
    join public.invoice_operation_chunks d on d.id=g.current_chunk_id
    where d.chunk_type='DOCUMENT_INPUT'
  ),
  attachment_index_ready as materialized (
    select s.*,
      index_input.id index_input_chunk_id,index_input.sequence_no index_ordinal,
      index_input.payload_json->>'source_chunk_key' source_chunk_key,
      index_input.payload_json->>'display_label' index_display_label,
      index_input.payload_json->>'input_type' index_input_type,
      index_input.payload_json->>'snapshot_hash' snapshot_hash,
      count(other.id)::integer supporting_input_count,
      coalesce(sum(other.actual_page_count),0)::integer supporting_pages,
      coalesce(sum(other.actual_byte_count),0)::bigint supporting_bytes,
      coalesce(max(other.actual_page_count) filter(
        where other.payload_json->>'input_type'='INVOICE_CORE'),0)::integer
        core_page_count,
      jsonb_agg(jsonb_build_object(
        'input_chunk_id',other.id,'ordinal',other.sequence_no,
        'logical_source_key',other.payload_json->>'source_chunk_key',
        'label',other.payload_json->>'display_label',
        'input_type',other.payload_json->>'input_type',
        'page_count',other.actual_page_count)
        order by other.sequence_no,other.id)
        filter(where other.id is not null
          and other.payload_json->>'input_type' not in(
            'INVOICE_CORE','ATTACHMENT_INDEX','SECTION_SEPARATOR')) attachments,
      coalesce((
        select jsonb_agg(section.row_json order by section.ordinal,
          section.physical_part_no,section.input_chunk_id)
        from(
          select stream_input.sequence_no ordinal,
            stream_input.id input_chunk_id,
            part.ordinality::integer physical_part_no,
            jsonb_build_object(
              'input_chunk_id',stream_input.id,
              'ordinal',stream_input.sequence_no,
              'physical_part_no',part.ordinality,
              'logical_source_key',
                stream_input.payload_json->>'source_chunk_key',
              'input_type',stream_input.payload_json->>'input_type',
              'label',stream_input.payload_json->>'display_label',
              'page_count',case
                when stream_input.payload_json->>'input_type'=
                    'ATTACHMENT_INDEX'
                  then 0
                when coalesce(part.value->>'page_count','')~'^[1-9][0-9]*$'
                  then(part.value->>'page_count')::integer
                else stream_input.actual_page_count end,
              'is_displayed_attachment',
                stream_input.payload_json->>'input_type' not in(
                  'INVOICE_CORE','ATTACHMENT_INDEX','SECTION_SEPARATOR'),
              'is_separator',
                stream_input.payload_json->>'input_type'='SECTION_SEPARATOR')
              row_json
          from current_inputs stream_input
          cross join lateral jsonb_array_elements(
            case
              when stream_input.payload_json->>'input_type'='ATTACHMENT_INDEX'
                then jsonb_build_array(jsonb_build_object('page_count',0))
              when jsonb_typeof(stream_input.result_json->'parts')='array'
                  and jsonb_array_length(stream_input.result_json->'parts')>0
                then stream_input.result_json->'parts'
              else jsonb_build_array(jsonb_build_object(
                'page_count',stream_input.actual_page_count))
            end) with ordinality part(value,ordinality)
          where stream_input.operation_id=s.operation_id
            and stream_input.document_version_id=s.document_version_id
            and(
              stream_input.id=index_input.id
              or stream_input.status='COMPLETE')
        ) section
      ),'[]'::jsonb) pagination_stream
    from claim_scope s
    join current_inputs index_input
      on index_input.operation_id=s.operation_id
      and index_input.document_version_id=s.document_version_id
      and index_input.payload_json->>'input_type'='ATTACHMENT_INDEX'
      and index_input.status='WAITING'
    left join current_inputs other
      on other.operation_id=s.operation_id
      and other.document_version_id=s.document_version_id
      and other.id<>index_input.id
    group by s.chunk_id,s.operation_id,s.document_version_id,s.priority,
      s.control_version,s.template_version,s.processor_policy,
      index_input.id,index_input.sequence_no,index_input.payload_json
    having count(other.id)>0 and bool_and(other.status='COMPLETE'
      and coalesce(other.actual_page_count,0)>0
      and coalesce(other.actual_byte_count,0)>0)
  ),
  attachment_index_measure as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,
      payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select a.operation_id,'SOURCE_RENDER','ATTACHMENT_INDEX_MEASURE',
      encode(digest(concat_ws('|','ATTACHMENT_INDEX',a.document_version_id::text,
        a.index_ordinal::text,a.source_chunk_key,'MEASURE','1',
        a.template_version),'sha256'),'hex'),
      1,a.index_ordinal,'DOCUMENT',a.document_version_id,
      a.document_version_id,'QUEUED',a.priority,v_now,
      jsonb_build_object(
        'render_kind','ATTACHMENT_INDEX','layout_phase','MEASURE',
        'layout_pass',1,'max_layout_passes',
          (a.processor_policy#>>
            '{attachment_index,max_render_passes}')::integer,
        'template_version',a.template_version,
        'source_chunk_key',a.source_chunk_key,
        'presentation_model_schema_version',
          'ATTACHMENT_INDEX_PRESENTATION_V1',
        'presentation_model_hash',encode(digest(jsonb_build_object(
          'display_label',a.index_display_label,
          'input_type',a.index_input_type,
          'manifest_ordinal',a.index_ordinal)::text,'sha256'),'hex'),
        'snapshot_hash',a.snapshot_hash,
        'core_page_count',a.core_page_count,
        'supporting_input_count',a.supporting_input_count,
        'supporting_pages',a.supporting_pages,
        'supporting_bytes',a.supporting_bytes,
        'display_rows',coalesce(a.attachments,'[]'::jsonb),
        'attachments',coalesce(a.attachments,'[]'::jsonb),
        'pagination_stream',coalesce(a.pagination_stream,'[]'::jsonb),
        'determinism',jsonb_build_object(
          'policy_version',a.processor_policy->>'version',
          'layout_version',a.processor_policy#>>
            '{attachment_index,layout_version}',
          'renderer_version',a.processor_policy#>>
            '{attachment_index,renderer_version}',
          'template_version',a.processor_policy#>>
            '{attachment_index,template_version}',
          'css_font_identity',a.processor_policy#>>
            '{attachment_index,css_font_identity}',
          'page_geometry',a.processor_policy#>>
            '{attachment_index,page_geometry}',
          'locale_identity',a.processor_policy#>>
            '{attachment_index,locale_identity}')),
      a.control_version,v_now,v_now
    from attachment_index_ready a
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
      do nothing
    returning id
  ),
  state as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,
      count(d.id) dependencies,count(d.id) filter(where d.status='COMPLETE') ready,
      count(d.id) filter(where d.status in ('FAILED','DEAD_LETTER','BLOCKED')
        or(d.status='COMPLETE'
          and nullif(d.result_json->>'r2_key','') is null
          and jsonb_array_length(case
            when jsonb_typeof(d.result_json->'parts')='array'
              then d.result_json->'parts' else '[]'::jsonb end)=0)) failed
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join current_inputs d
      on d.operation_id=c.operation_id
      and d.document_version_id=c.document_version_id
    group by c.id,c.operation_id,c.document_version_id
  ),
  blocked as (
    update public.invoice_operation_chunks c set status='BLOCKED',phase='BLOCKED',
      error_json=jsonb_build_object('code','DOCUMENT_INPUT_FAILED',
        'failed_inputs',(select jsonb_agg(jsonb_build_object('chunk_id',d.id,'entity_id',d.entity_id,'error',d.error_json))
                         from current_inputs d where d.operation_id=c.operation_id
                           and d.document_version_id=c.document_version_id
                           and d.chunk_type='DOCUMENT_INPUT' and d.status in ('FAILED','DEAD_LETTER','BLOCKED'))),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from state s where c.id=s.chunk_id and s.failed>0 returning c.id
  ),
  ready_plans as materialized (
    select s.* from state s where s.dependencies>0 and s.dependencies=s.ready and s.failed=0
  ),
  physical_inputs as materialized (
    select d.id,d.operation_id,d.document_version_id,d.sequence_no,
      d.payload_json||jsonb_build_object(
        'physical_part_no',p.ordinality,
        'logical_input_chunk_id',d.id) payload_json,
      coalesce(nullif(p.value->>'r2_key',''),nullif(p.value->>'key',''))
        physical_r2_key,
      coalesce(nullif(p.value->>'sha256',''),
        nullif(d.result_json->>'sha256','')) physical_sha256,
      case when coalesce(p.value->>'page_count','')~'^[1-9][0-9]*$'
        then(p.value->>'page_count')::integer
        when jsonb_array_length(case
          when jsonb_typeof(d.result_json->'parts')='array'
            then d.result_json->'parts' else '[]'::jsonb end)=0
          then d.actual_page_count end physical_page_count,
      case when coalesce(p.value->>'size_bytes','')~'^[1-9][0-9]*$'
        then(p.value->>'size_bytes')::bigint
        when jsonb_array_length(case
          when jsonb_typeof(d.result_json->'parts')='array'
            then d.result_json->'parts' else '[]'::jsonb end)=0
          then d.actual_byte_count end physical_byte_count,
      p.ordinality::integer physical_part_no,
      d.result_json||jsonb_build_object(
        'r2_key',coalesce(nullif(p.value->>'r2_key',''),
          nullif(p.value->>'key','')),
        'sha256',coalesce(nullif(p.value->>'sha256',''),
          nullif(d.result_json->>'sha256','')))
        result_json
    from ready_plans r
    join current_inputs d
      on d.operation_id=r.operation_id and d.chunk_type='DOCUMENT_INPUT'
      and d.document_version_id=r.document_version_id
      and d.status='COMPLETE'
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(d.result_json->'parts')='array'
          and jsonb_array_length(d.result_json->'parts')>0
        then d.result_json->'parts'
        else jsonb_build_array(jsonb_build_object(
          'r2_key',d.result_json->>'r2_key',
          'sha256',d.result_json->>'sha256',
          'page_count',d.actual_page_count,
          'size_bytes',d.actual_byte_count))
      end) with ordinality p(value,ordinality)
    where nullif(coalesce(p.value->>'r2_key',p.value->>'key'),'') is not null
      and coalesce(p.value->>'sha256',d.result_json->>'sha256','')<>''
  ),
  physical_receipt_inputs as materialized (
    select p.*,
      coalesce(nullif(p.payload_json->>'source_chunk_key',''),
        p.id::text) logical_source_key,
      p.sequence_no logical_manifest_ordinal,
      encode(digest(jsonb_build_object(
        'receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
        'logical_source_key',coalesce(
          nullif(p.payload_json->>'source_chunk_key',''),p.id::text),
        'logical_manifest_ordinal',p.sequence_no,
        'physical_part_no',p.physical_part_no,
        'object_key',p.physical_r2_key,
        'stored_sha256',p.physical_sha256,
        'expected_page_count',p.physical_page_count,
        'expected_byte_count',p.physical_byte_count
      )::text,'sha256'),'hex') expected_physical_receipt
    from physical_inputs p
  ),
  logical_receipts as materialized (
    select p.operation_id,p.document_version_id,p.id logical_input_chunk_id,
      p.logical_source_key,p.logical_manifest_ordinal,
      count(*)::integer physical_part_count,
      encode(digest(jsonb_build_object(
        'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
        'logical_source_key',p.logical_source_key,
        'logical_manifest_ordinal',p.logical_manifest_ordinal,
        'ordered_physical_receipts',string_agg(
          p.expected_physical_receipt,'||'
          order by p.physical_part_no)
      )::text,'sha256'),'hex') expected_logical_receipt
    from physical_receipt_inputs p
    group by p.operation_id,p.document_version_id,p.id,
      p.logical_source_key,p.logical_manifest_ordinal
  ),
  receipt_inputs as materialized (
    select p.*,l.expected_logical_receipt,l.physical_part_count
    from physical_receipt_inputs p
    join logical_receipts l
      on l.operation_id=p.operation_id
     and l.document_version_id=p.document_version_id
     and l.logical_input_chunk_id=p.id
  ),
  ranked_inputs as materialized (
    select d.*,
      d.physical_page_count actual_page_count,
      d.physical_byte_count actual_byte_count,
      row_number() over(partition by d.operation_id,d.document_version_id
        order by d.sequence_no,d.physical_part_no,d.id)::integer input_no,
      case when coalesce(d.result_json->>'decoded_estimate_bytes','') ~ '^[0-9]{1,18}$'
        then(d.result_json->>'decoded_estimate_bytes')::bigint
        else coalesce(d.physical_byte_count,0)*4 end decoded_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_inputs}')::integer max_inputs,
      (o.config_json->'processor_policy'
        #>>'{merge,max_pages}')::integer max_pages,
      (o.config_json->'processor_policy'
        #>>'{merge,max_input_bytes}')::bigint max_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_estimated_decoded_bytes}')::bigint max_decoded_bytes
    from receipt_inputs d
    join public.invoice_operations o on o.id=d.operation_id
  ),
  oversized_inputs as materialized (
    select r.*
    from ranked_inputs r
    where coalesce(r.actual_page_count,0)<=0
       or coalesce(r.actual_byte_count,0)<=0
       or coalesce(r.decoded_bytes,0)<=0
       or r.actual_page_count>r.max_pages
       or r.actual_byte_count>r.max_bytes
       or r.decoded_bytes>r.max_decoded_bytes
  ),
  oversized_plans as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',
        error_json=jsonb_build_object(
          'code','MERGE_INPUT_EXCEEDS_POLICY',
          'document_version_id',bad.document_version_id,
          'input_chunk_id',bad.id,
          'page_count',bad.actual_page_count,
          'size_bytes',bad.actual_byte_count,
          'decoded_estimate_bytes',bad.decoded_bytes),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from (
      select distinct on(operation_id,document_version_id)
        operation_id,document_version_id,id,actual_page_count,
        actual_byte_count,decoded_bytes
      from oversized_inputs
      order by operation_id,document_version_id,input_no
    ) bad
    where c.operation_id=bad.operation_id
      and c.document_version_id=bad.document_version_id
      and c.chunk_type='DOCUMENT_PLAN'
      and c.status not in('COMPLETE','FAILED','DEAD_LETTER',
        'CANCELLED','SUPERSEDED')
    returning c.document_version_id
  ),
  oversized_versions as (
    update public.invoice_document_versions v
    set status='FAILED',
        error_json=jsonb_build_object('code','MERGE_INPUT_EXCEEDS_POLICY')
    where v.id in(select document_version_id from oversized_plans)
      and v.status<>'READY'
    returning v.id
  ),
  packable_inputs as materialized (
    select r.*
    from ranked_inputs r
    where not exists(
      select 1 from oversized_inputs bad
      where bad.operation_id=r.operation_id
        and bad.document_version_id=r.document_version_id)
  ),
  leaf_pack(
    operation_id,document_version_id,input_no,group_no,group_input_count,
    group_pages,group_bytes,group_decoded_bytes
  ) as (
    select d.operation_id,d.document_version_id,d.input_no,0,
      1,d.actual_page_count,d.actual_byte_count,d.decoded_bytes
    from packable_inputs d
    where d.input_no=1
    union all
    select n.operation_id,n.document_version_id,n.input_no,
      p.group_no+case when split.start_new then 1 else 0 end,
      case when split.start_new then 1 else p.group_input_count+1 end,
      case when split.start_new then n.actual_page_count
        else p.group_pages+n.actual_page_count end,
      case when split.start_new then n.actual_byte_count
        else p.group_bytes+n.actual_byte_count end,
      case when split.start_new then n.decoded_bytes
        else p.group_decoded_bytes+n.decoded_bytes end
    from leaf_pack p
    join packable_inputs n
      on n.operation_id=p.operation_id
      and n.document_version_id=p.document_version_id
      and n.input_no=p.input_no+1
    cross join lateral (
      select p.group_input_count+1>n.max_inputs
        or p.group_pages+n.actual_page_count>n.max_pages
        or p.group_bytes+n.actual_byte_count>n.max_bytes
        or p.group_decoded_bytes+n.decoded_bytes>n.max_decoded_bytes
        start_new
    ) split
  ),
  leaf_group_logical as materialized (
    select d.operation_id,d.document_version_id,p.group_no,
      d.logical_source_key,min(d.logical_manifest_ordinal) logical_ordinal,
      encode(digest(jsonb_build_object(
        'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
        'logical_source_key',d.logical_source_key,
        'logical_manifest_ordinal',min(d.logical_manifest_ordinal),
        'ordered_physical_receipts',string_agg(
          d.expected_physical_receipt,'||'
          order by d.logical_manifest_ordinal,d.physical_part_no,d.id)
      )::text,'sha256'),'hex') expected_logical_receipt
    from packable_inputs d
    join leaf_pack p
      on p.operation_id=d.operation_id
     and p.document_version_id=d.document_version_id
     and p.input_no=d.input_no
    group by d.operation_id,d.document_version_id,p.group_no,
      d.logical_source_key
  ),
  leaf_group_roots as materialized (
    select groups.operation_id,groups.document_version_id,groups.group_no,
      jsonb_agg(jsonb_build_object(
        'logical_source_key',groups.logical_source_key,
        'logical_manifest_ordinal',groups.logical_manifest_ordinal,
        'physical_part_no',groups.physical_part_no,
        'physical_receipt',groups.expected_physical_receipt)
        order by groups.input_no) expected_physical_receipts,
      encode(digest(string_agg(groups.expected_physical_receipt,'||'
        order by groups.input_no),'sha256'),'hex')
        expected_physical_receipt_root,
      encode(digest(string_agg(groups.expected_physical_receipt,'||'
        order by groups.input_no),'sha256'),'hex')
        expected_child_receipt_hash,
      (
        select encode(digest(string_agg(
          logical.expected_logical_receipt,'||'
          order by logical.logical_ordinal,logical.logical_source_key),
          'sha256'),'hex')
        from leaf_group_logical logical
        where logical.operation_id=groups.operation_id
          and logical.document_version_id=groups.document_version_id
          and logical.group_no=groups.group_no
      ) expected_logical_receipt_root
    from (
      select d.operation_id,d.document_version_id,p.group_no,d.input_no,
        d.logical_source_key,d.logical_manifest_ordinal,d.physical_part_no,
        d.expected_physical_receipt
      from packable_inputs d
      join leaf_pack p
        on p.operation_id=d.operation_id
       and p.document_version_id=d.document_version_id
       and p.input_no=d.input_no
    ) groups
    group by groups.operation_id,groups.document_version_id,groups.group_no
  ),
  leaf_groups as materialized (
    select d.operation_id,d.document_version_id,p.group_no,
      jsonb_agg(jsonb_build_object('input_chunk_id',d.id,
        'input_order',d.input_no,'ordinal',d.sequence_no,
        'physical_part_no',d.physical_part_no,
        'r2_key',d.physical_r2_key,'sha256',d.physical_sha256,
        'page_count',d.actual_page_count,'size_bytes',d.actual_byte_count,
        'logical_source_key',d.logical_source_key,
        'logical_manifest_ordinal',d.logical_manifest_ordinal,
        'expected_logical_receipt',d.expected_logical_receipt,
        'expected_physical_receipt',d.expected_physical_receipt,
        'decoded_estimate_bytes',d.result_json->>'decoded_estimate_bytes')
        order by d.sequence_no,d.physical_part_no,d.id) inputs,
      encode(digest(string_agg(concat_ws('|',d.input_no::text,d.id::text,
        d.physical_r2_key,d.physical_sha256,d.actual_page_count::text,
        d.actual_byte_count::text,''),'||'
        order by d.sequence_no,d.physical_part_no,d.id),
        'sha256'),'hex') ordered_input_hash,
      roots.expected_child_receipt_hash,
      roots.expected_logical_receipt_root,
      roots.expected_physical_receipt_root,
      roots.expected_physical_receipts,
      sum(coalesce(d.actual_page_count,0))::integer expected_pages,
      sum(coalesce(d.actual_byte_count,0))::bigint expected_bytes
    from packable_inputs d
    join leaf_pack p
      on p.operation_id=d.operation_id
      and p.document_version_id=d.document_version_id
      and p.input_no=d.input_no
    join leaf_group_roots roots
      on roots.operation_id=d.operation_id
     and roots.document_version_id=d.document_version_id
     and roots.group_no=p.group_no
    group by d.operation_id,d.document_version_id,p.group_no,
      roots.expected_child_receipt_hash,
      roots.expected_logical_receipt_root,
      roots.expected_physical_receipt_root,
      roots.expected_physical_receipts
    having count(*)<=max(d.max_inputs)
      and sum(d.actual_page_count)<=max(d.max_pages)
      and sum(d.actual_byte_count)<=max(d.max_bytes)
      and sum(d.decoded_bytes)<=max(d.max_decoded_bytes)
  ),
  independent_expectations as (
    update public.invoice_document_versions v
    set expected_page_count=e.expected_pages,
      status='ASSEMBLING',
      error_json=null
    from (
      select r.document_version_id,sum(r.actual_page_count)::integer expected_pages,
        encode(digest(string_agg(concat_ws('|',r.sequence_no::text,
          r.payload_json->>'source_chunk_key',r.physical_part_no::text,
          r.physical_sha256,r.actual_page_count::text),'||'
          order by r.sequence_no,r.physical_part_no),'sha256'),'hex')
          coverage_hash
      from packable_inputs r group by r.document_version_id
    ) e
    where v.id=e.document_version_id
    returning v.id,v.expected_page_count
  ),
  merge_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      expected_page_count,expected_byte_count,operation_control_version,created_at_utc,updated_at_utc)
    select g.operation_id,'PDF_MERGE','MERGE',
      encode(digest(concat_ws('|','PDF_MERGE',g.document_version_id::text,
        '0',g.group_no::text,g.ordered_input_hash,pg.plan_generation::text),
        'sha256'),'hex'),
      pg.plan_generation,g.group_no,0,'DOCUMENT',
      g.document_version_id,g.document_version_id,'QUEUED',550,v_now,
      jsonb_build_object('level',0,'inputs',g.inputs,
        'fan_in',jsonb_array_length(g.inputs),
        'ordered_input_hash',g.ordered_input_hash,
        'expected_child_receipt_hash',g.expected_child_receipt_hash,
        'expected_logical_receipt_root',g.expected_logical_receipt_root,
        'expected_physical_receipt_root',g.expected_physical_receipt_root,
        'expected_physical_receipts',g.expected_physical_receipts,
        'receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
        'plan_generation',pg.plan_generation),
      g.expected_pages,g.expected_bytes,o.control_version,v_now,v_now
    from leaf_groups g
    join public.invoice_operations o on o.id=g.operation_id
    cross join lateral (
      select coalesce((
        select prior.plan_generation
        from public.invoice_operation_chunks prior
        where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
          and prior.document_version_id=g.document_version_id
          and prior.level_no=0 and prior.sequence_no=g.group_no
          and prior.payload_json->>'ordered_input_hash'=g.ordered_input_hash
        order by prior.created_at_utc desc,prior.id desc limit 1),
        1+coalesce((select max(prior.plan_generation)
          from public.invoice_operation_chunks prior
          where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
            and prior.document_version_id=g.document_version_id
            and prior.level_no=0 and prior.sequence_no=g.group_no),0))
        plan_generation
    ) pg
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,level_no,sequence_no,plan_generation
  ),
  replaced_leaf_merges as (
    update public.invoice_operation_chunks stale
    set status='SUPERSEDED',phase='SUPERSEDED',completed_at_utc=v_now,
        failed_at_utc=null,
        replaced_by_chunk_id=fresh.id,replacement_required=true,
        result_json=coalesce(stale.result_json,'{}'::jsonb)
          ||jsonb_build_object('replacement_chunk_id',fresh.id),
        error_json=jsonb_build_object(
          'code','REPLACED_AFTER_UPSTREAM_RETRY',
          'replacement_chunk_id',fresh.id),
        updated_at_utc=v_now
    from merge_chunks fresh
    where stale.operation_id=fresh.operation_id
      and stale.chunk_type='PDF_MERGE'
      and stale.document_version_id=fresh.document_version_id
      and stale.level_no=fresh.level_no
      and stale.sequence_no=fresh.sequence_no
      and stale.id<>fresh.id and stale.status='BLOCKED'
      and stale.error_json->>'code'='UPSTREAM_RETRY_INVALIDATED'
    returning stale.id
  ),
  advanced as (
    update public.invoice_operation_chunks c
    set phase=case when s.failed>0 then 'BLOCKED'
                   when s.dependencies=s.ready and s.dependencies>0 then 'WAIT_FOR_MERGE'
                   else 'WAIT_FOR_INPUTS' end,
        status=case when s.failed>0 then 'BLOCKED' else 'WAITING' end,
        progress_json=jsonb_build_object('status_message',
          case when s.dependencies=s.ready and s.dependencies>0 then 'Inputs ready; merge queued' else 'Waiting for inputs' end,
          'inputs_ready',s.ready,'inputs_total',s.dependencies),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from state s where c.id=s.chunk_id
    returning c.id,c.status,c.phase,c.progress_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,
    'progress',progress_json)),'[]'::jsonb) into v_part from advanced;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- WAIT_FOR_MERGE seeds one bounded level at a time.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='WAIT_FOR_MERGE'
  ),
  claim_operations as materialized (
    select distinct c.operation_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
  ),
  current_graph as materialized (
    select g.*
    from private._invoice_current_chunks_batch(
      (select array_agg(operation_id) from claim_operations),
      null,null,10000) g
    where g.replacement_chain_status='VALID'
  ),
  current_merges as materialized (
    select m.*
    from current_graph g
    join public.invoice_operation_chunks m on m.id=g.current_chunk_id
    where m.chunk_type='PDF_MERGE'
  ),
  current_inputs as materialized (
    select d.*
    from current_graph g
    join public.invoice_operation_chunks d on d.id=g.current_chunk_id
    where d.chunk_type='DOCUMENT_INPUT'
  ),
  plan_state as materialized (
    select c.id chunk_id,c.operation_id,c.document_version_id,
      coalesce(max(m.level_no),0) current_level,
      count(m.id) filter(where m.level_no=(select max(m2.level_no)
        from current_merges m2 where m2.operation_id=c.operation_id
          and m2.document_version_id=c.document_version_id)) level_count,
      count(m.id) filter(where m.level_no=(select max(m2.level_no)
        from current_merges m2 where m2.operation_id=c.operation_id
          and m2.document_version_id=c.document_version_id)
        and m.status='COMPLETE') complete_count,
      count(m.id) filter(where m.status in ('FAILED','DEAD_LETTER','BLOCKED')) failed_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join current_merges m on m.operation_id=c.operation_id
      and m.document_version_id=c.document_version_id
      and m.chunk_type='PDF_MERGE'
    group by c.id,c.operation_id,c.document_version_id
  ),
  ranked_merges as materialized (
    select m.*,ps.current_level,
      row_number() over(partition by m.operation_id,m.document_version_id
        order by m.sequence_no,m.id)::integer input_no,
      coalesce(m.actual_byte_count,0)*4 decoded_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_inputs}')::integer max_inputs,
      (o.config_json->'processor_policy'
        #>>'{merge,max_pages}')::integer max_pages,
      (o.config_json->'processor_policy'
        #>>'{merge,max_input_bytes}')::bigint max_bytes,
      (o.config_json->'processor_policy'
        #>>'{merge,max_estimated_decoded_bytes}')::bigint max_decoded_bytes
    from plan_state ps join current_merges m on m.operation_id=ps.operation_id
      and m.document_version_id=ps.document_version_id
      and m.chunk_type='PDF_MERGE' and m.level_no=ps.current_level and m.status='COMPLETE'
    join public.invoice_operations o on o.id=m.operation_id
    where ps.failed_count=0 and ps.level_count=ps.complete_count and ps.level_count>1
  ),
  merge_pack(
    operation_id,document_version_id,input_no,group_no,group_input_count,
    group_pages,group_bytes,group_decoded_bytes
  ) as (
    select m.operation_id,m.document_version_id,m.input_no,0,
      1,m.actual_page_count,m.actual_byte_count,m.decoded_bytes
    from ranked_merges m
    where m.input_no=1
    union all
    select n.operation_id,n.document_version_id,n.input_no,
      p.group_no+case when split.start_new then 1 else 0 end,
      case when split.start_new then 1 else p.group_input_count+1 end,
      case when split.start_new then n.actual_page_count
        else p.group_pages+n.actual_page_count end,
      case when split.start_new then n.actual_byte_count
        else p.group_bytes+n.actual_byte_count end,
      case when split.start_new then n.decoded_bytes
        else p.group_decoded_bytes+n.decoded_bytes end
    from merge_pack p
    join ranked_merges n
      on n.operation_id=p.operation_id
      and n.document_version_id=p.document_version_id
      and n.input_no=p.input_no+1
    cross join lateral (
      select p.group_input_count+1>n.max_inputs
        or p.group_pages+n.actual_page_count>n.max_pages
        or p.group_bytes+n.actual_byte_count>n.max_bytes
        or p.group_decoded_bytes+n.decoded_bytes>n.max_decoded_bytes
        start_new
    ) split
  ),
  numbered_merges as materialized (
    select m.*,p.group_no,p.group_input_count input_order
    from ranked_merges m
    join merge_pack p
      on p.operation_id=m.operation_id
      and p.document_version_id=m.document_version_id
      and p.input_no=m.input_no
  ),
  next_group_physical as materialized (
    select m.operation_id,m.document_version_id,m.current_level,m.group_no,
      m.sequence_no child_sequence_no,m.id child_chunk_id,
      part.ordinality::integer child_physical_no,
      part.value physical_receipt
    from numbered_merges m
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(
          m.result_json#>'{merge_receipt,physical_receipts}')='array'
        then m.result_json#>'{merge_receipt,physical_receipts}'
        else '[]'::jsonb end) with ordinality part(value,ordinality)
  ),
  next_group_logical as materialized (
    select p.operation_id,p.document_version_id,p.current_level,p.group_no,
      p.physical_receipt->>'logical_source_key' logical_source_key,
      min((p.physical_receipt->>'logical_manifest_ordinal')::integer)
        logical_manifest_ordinal,
      encode(digest(jsonb_build_object(
        'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
        'logical_source_key',
          p.physical_receipt->>'logical_source_key',
        'logical_manifest_ordinal',
          min((p.physical_receipt->>
            'logical_manifest_ordinal')::integer),
        'ordered_physical_receipts',string_agg(
          p.physical_receipt->>'physical_receipt','||'
          order by(p.physical_receipt->>
            'logical_manifest_ordinal')::integer,
            (p.physical_receipt->>'physical_part_no')::integer,
            p.child_sequence_no,p.child_physical_no)
      )::text,'sha256'),'hex') logical_receipt
    from next_group_physical p
    where coalesce(p.physical_receipt->>'logical_manifest_ordinal','')
        ~'^[0-9]{1,9}$'
      and coalesce(p.physical_receipt->>'physical_part_no','')
        ~'^[1-9][0-9]{0,8}$'
      and coalesce(p.physical_receipt->>'physical_receipt','')
        ~'^[0-9a-f]{64}$'
      and nullif(p.physical_receipt->>'logical_source_key','') is not null
    group by p.operation_id,p.document_version_id,p.current_level,p.group_no,
      p.physical_receipt->>'logical_source_key'
  ),
  next_group_roots as materialized (
    select p.operation_id,p.document_version_id,p.current_level,p.group_no,
      count(*)::integer physical_receipt_count,
      jsonb_agg(p.physical_receipt order by
        (p.physical_receipt->>'logical_manifest_ordinal')::integer,
        (p.physical_receipt->>'physical_part_no')::integer,
        p.child_sequence_no,p.child_physical_no) physical_receipts,
      encode(digest(string_agg(
        p.physical_receipt->>'physical_receipt','||'
        order by(p.physical_receipt->>'logical_manifest_ordinal')::integer,
          (p.physical_receipt->>'physical_part_no')::integer,
          p.child_sequence_no,p.child_physical_no),
        'sha256'),'hex') expected_physical_receipt_root,
      (
        select encode(digest(string_agg(
          l.logical_receipt,'||'
          order by l.logical_manifest_ordinal,l.logical_source_key),
          'sha256'),'hex')
        from next_group_logical l
        where l.operation_id=p.operation_id
          and l.document_version_id=p.document_version_id
          and l.current_level=p.current_level
          and l.group_no=p.group_no
      ) expected_logical_receipt_root
    from next_group_physical p
    where coalesce(p.physical_receipt->>'logical_manifest_ordinal','')
        ~'^[0-9]{1,9}$'
      and coalesce(p.physical_receipt->>'physical_part_no','')
        ~'^[1-9][0-9]{0,8}$'
      and coalesce(p.physical_receipt->>'physical_receipt','')
        ~'^[0-9a-f]{64}$'
    group by p.operation_id,p.document_version_id,p.current_level,p.group_no
  ),
  next_groups as materialized (
    select m.operation_id,m.document_version_id,(m.current_level+1) next_level,m.group_no,
      jsonb_agg(jsonb_build_object('input_chunk_id',m.id,
        'input_order',m.input_order,'r2_key',m.result_json->>'r2_key',
        'sha256',m.result_json->>'sha256','page_count',m.actual_page_count,
        'size_bytes',m.actual_byte_count,
        'child_merge_receipt',m.result_json->'merge_receipt',
        'child_merge_receipt_hash',encode(digest(
          coalesce(m.result_json->'merge_receipt','{}'::jsonb)::text,
          'sha256'),'hex'),
        'logical_receipt_root',m.result_json#>>
          '{merge_receipt,combined_logical_receipt_root}',
        'physical_receipt_root',m.result_json#>>
          '{merge_receipt,combined_physical_receipt_root}')
        order by m.sequence_no,m.id) inputs,
      encode(digest(string_agg(concat_ws('|',m.input_no::text,m.id::text,
        m.result_json->>'r2_key',m.result_json->>'sha256',m.actual_page_count::text,
        m.actual_byte_count::text,encode(digest(
          coalesce(m.result_json->'merge_receipt','{}'::jsonb)::text,
          'sha256'),'hex')),'||' order by m.sequence_no,m.id),
        'sha256'),'hex') ordered_input_hash,
      encode(digest(string_agg(encode(digest(
        coalesce(m.result_json->'merge_receipt','{}'::jsonb)::text,
        'sha256'),'hex'),'||' order by m.sequence_no,m.id),
        'sha256'),'hex') expected_child_receipt_hash,
      roots.expected_logical_receipt_root,
      roots.expected_physical_receipt_root,
      roots.physical_receipts,
      sum(m.actual_page_count)::integer expected_pages,sum(m.actual_byte_count)::bigint expected_bytes
    from numbered_merges m
    join next_group_roots roots
      on roots.operation_id=m.operation_id
     and roots.document_version_id=m.document_version_id
     and roots.current_level=m.current_level
     and roots.group_no=m.group_no
    group by m.operation_id,m.document_version_id,m.current_level,m.group_no
      ,roots.expected_logical_receipt_root
      ,roots.expected_physical_receipt_root,roots.physical_receipts
    having count(*)<=max(m.max_inputs)
      and sum(m.actual_page_count)<=max(m.max_pages)
      and sum(m.actual_byte_count)<=max(m.max_bytes)
      and sum(m.decoded_bytes)<=max(m.max_decoded_bytes)
  ),
  next_merges as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      expected_page_count,expected_byte_count,operation_control_version,created_at_utc,updated_at_utc)
    select g.operation_id,'PDF_MERGE','MERGE',
      encode(digest(concat_ws('|','PDF_MERGE',g.document_version_id::text,
        g.next_level::text,g.group_no::text,g.ordered_input_hash,
        pg.plan_generation::text),
        'sha256'),'hex'),
      pg.plan_generation,g.group_no,g.next_level,'DOCUMENT',g.document_version_id,
      g.document_version_id,'QUEUED',550,v_now,
      jsonb_build_object('level',g.next_level,'inputs',g.inputs,
        'fan_in',jsonb_array_length(g.inputs),
        'ordered_input_hash',g.ordered_input_hash,
        'expected_child_receipt_hash',g.expected_child_receipt_hash,
        'expected_logical_receipt_root',g.expected_logical_receipt_root,
        'expected_physical_receipt_root',g.expected_physical_receipt_root,
        'expected_physical_receipts',g.physical_receipts,
        'receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
        'plan_generation',pg.plan_generation),
      g.expected_pages,g.expected_bytes,o.control_version,v_now,v_now
    from next_groups g join public.invoice_operations o on o.id=g.operation_id
    cross join lateral (
      select coalesce((
        select prior.plan_generation
        from public.invoice_operation_chunks prior
        where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
          and prior.document_version_id=g.document_version_id
          and prior.level_no=g.next_level and prior.sequence_no=g.group_no
          and prior.payload_json->>'ordered_input_hash'=g.ordered_input_hash
        order by prior.created_at_utc desc,prior.id desc limit 1),
        1+coalesce((select max(prior.plan_generation)
          from public.invoice_operation_chunks prior
          where prior.operation_id=g.operation_id and prior.chunk_type='PDF_MERGE'
            and prior.document_version_id=g.document_version_id
            and prior.level_no=g.next_level and prior.sequence_no=g.group_no),0))
        plan_generation
    ) pg
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,level_no,sequence_no,plan_generation
  ),
  replaced_next_merges as (
    update public.invoice_operation_chunks stale
    set status='SUPERSEDED',phase='SUPERSEDED',completed_at_utc=v_now,
        failed_at_utc=null,
        replaced_by_chunk_id=fresh.id,replacement_required=true,
        result_json=coalesce(stale.result_json,'{}'::jsonb)
          ||jsonb_build_object('replacement_chunk_id',fresh.id),
        error_json=jsonb_build_object(
          'code','REPLACED_AFTER_UPSTREAM_RETRY',
          'replacement_chunk_id',fresh.id),
        updated_at_utc=v_now
    from next_merges fresh
    where stale.operation_id=fresh.operation_id
      and stale.chunk_type='PDF_MERGE'
      and stale.document_version_id=fresh.document_version_id
      and stale.level_no=fresh.level_no
      and stale.sequence_no=fresh.sequence_no
      and stale.id<>fresh.id and stale.status='BLOCKED'
      and stale.error_json->>'code'='UPSTREAM_RETRY_INVALIDATED'
    returning stale.id
  ),
  final_merge as materialized (
    select ps.*,m.id merge_chunk_id,m.payload_json,m.result_json,
      m.actual_page_count,m.actual_byte_count
    from plan_state ps join current_merges m on m.operation_id=ps.operation_id
      and m.document_version_id=ps.document_version_id
      and m.chunk_type='PDF_MERGE' and m.level_no=ps.current_level and m.status='COMPLETE'
    where ps.level_count=1 and ps.complete_count=1 and ps.failed_count=0
  ),
  verify_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,expected_page_count,expected_byte_count,
      operation_control_version,created_at_utc,updated_at_utc)
    select f.operation_id,'DOCUMENT_VERIFY','VERIFY',
      encode(digest(concat_ws('|','DOCUMENT_VERIFY',
        f.document_version_id::text,f.result_json->>'sha256',
        v.manifest_hash,coverage.manifest_coverage_hash,
        f.payload_json->>'expected_logical_receipt_root',
        f.payload_json->>'expected_physical_receipt_root',
        pg.plan_generation::text),'sha256'),'hex'),
      pg.plan_generation,0,'DOCUMENT',f.document_version_id,f.document_version_id,
      'QUEUED',550,v_now,jsonb_build_object('candidate_chunk_id',f.merge_chunk_id,
        'candidate_r2_key',f.result_json->>'r2_key','candidate_sha256',f.result_json->>'sha256',
        'candidate_size_bytes',f.actual_byte_count,'candidate_page_count',f.actual_page_count,
        'manifest_hash',v.manifest_hash,
        'expected_coverage_hash',coverage.manifest_coverage_hash,
        'resolved_input_coverage_hash',coverage.input_coverage_hash,
        'expected_input_count',coverage.manifest_input_count,
        'resolved_input_count',coverage.resolved_input_count,
        'expected_physical_input_count',coverage.physical_input_count,
        'expected_physical_input_hash',coverage.physical_input_hash,
        'expected_logical_source_count',receipts.logical_source_count,
        'expected_logical_root_receipt',
          f.payload_json->>'expected_logical_receipt_root',
        'expected_physical_root_receipt',
          f.payload_json->>'expected_physical_receipt_root',
        'expected_ordered_input_root',
          f.payload_json->>'expected_child_receipt_hash',
        'root_merge_receipt_identity',encode(digest(
          jsonb_build_object(
            'receipt_contract','DOCUMENT_ROOT_RECEIPT_V3',
            'logical_root',
              f.payload_json->>'expected_logical_receipt_root',
            'physical_root',
              f.payload_json->>'expected_physical_receipt_root',
            'ordered_input_root',
              f.payload_json->>'expected_child_receipt_hash',
            'page_count',v.expected_page_count,
            'output_sha256',f.result_json->>'sha256')::text,
          'sha256'),'hex'),
        'receipt_contract','ACTUAL_BYTES_MERGE_RECEIPT_V3',
        'plan_generation',pg.plan_generation,
        'final_merge_receipt',f.result_json->'merge_receipt',
        'final_merge_receipt_hash',encode(digest(
          coalesce(f.result_json->'merge_receipt','{}'::jsonb)::text,
          'sha256'),'hex'),
        'independent_expected_page_count',v.expected_page_count),
      v.expected_page_count,f.actual_byte_count,
      o.control_version,v_now,v_now
    from final_merge f join public.invoice_document_versions v on v.id=f.document_version_id
    join public.invoice_operations o on o.id=f.operation_id
    join lateral (
      select
        (select encode(digest(string_agg(concat_ws('|',
            x.value->>'ordinal',x.value->>'source_chunk_key'),
            '||' order by (x.value->>'ordinal')::integer),
          'sha256'),'hex')
         from jsonb_array_elements(v.manifest_json) x(value)
         where coalesce(x.value->>'ordinal','')~'^[0-9]+$')
          manifest_coverage_hash,
        (select encode(digest(string_agg(concat_ws('|',r.sequence_no::text,
            r.payload_json->>'source_chunk_key'),
            '||' order by r.sequence_no,r.id),'sha256'),'hex')
         from current_inputs r
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE')
          input_coverage_hash,
        jsonb_array_length(v.manifest_json)::integer manifest_input_count,
        (select count(*)::integer
         from current_inputs r
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE')
          resolved_input_count,
        (select count(*)::integer
         from current_inputs r
         cross join lateral jsonb_array_elements(
           case when jsonb_typeof(r.result_json->'normalised_manifest')='array'
                  and jsonb_array_length(r.result_json->'normalised_manifest')>0
             then r.result_json->'normalised_manifest'
             else jsonb_build_array(jsonb_build_object(
               'r2_key',coalesce(r.result_json->>'r2_key',
                 r.result_json->>'normalised_r2_key'),
               'sha256',r.result_json->>'sha256')) end)
           with ordinality part(value,ordinality)
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE'
           and coalesce(part.value->>'r2_key','')<>'')
          physical_input_count,
        (select encode(digest(string_agg(concat_ws('|',
            r.sequence_no::text,part.ordinality::text,
            part.value->>'r2_key',
            coalesce(part.value->>'sha256',r.result_json->>'sha256','')),
            '||' order by r.sequence_no,part.ordinality),'sha256'),'hex')
         from current_inputs r
         cross join lateral jsonb_array_elements(
           case when jsonb_typeof(r.result_json->'normalised_manifest')='array'
                  and jsonb_array_length(r.result_json->'normalised_manifest')>0
             then r.result_json->'normalised_manifest'
             else jsonb_build_array(jsonb_build_object(
               'r2_key',coalesce(r.result_json->>'r2_key',
                 r.result_json->>'normalised_r2_key'),
               'sha256',r.result_json->>'sha256')) end)
           with ordinality part(value,ordinality)
         where r.operation_id=f.operation_id
           and r.document_version_id=f.document_version_id
           and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE'
           and coalesce(part.value->>'r2_key','')<>'')
          physical_input_hash
    ) coverage on coverage.manifest_coverage_hash=coverage.input_coverage_hash
      and coverage.manifest_input_count=coverage.resolved_input_count
    join lateral (
      with physical as materialized (
        select r.id input_chunk_id,r.sequence_no,
          coalesce(nullif(r.payload_json->>'source_chunk_key',''),
            r.id::text) logical_source_key,
          part.ordinality::integer physical_part_no,
          coalesce(nullif(part.value->>'r2_key',''),
            nullif(part.value->>'key',''),
            nullif(r.result_json->>'r2_key',''),
            nullif(r.result_json->>'normalised_r2_key','')) object_key,
          coalesce(nullif(part.value->>'sha256',''),
            nullif(r.result_json->>'sha256','')) stored_sha256,
          case when coalesce(part.value->>'page_count','')
              ~'^[1-9][0-9]*$'
            then(part.value->>'page_count')::integer
            else r.actual_page_count end page_count,
          case when coalesce(part.value->>'size_bytes','')
              ~'^[1-9][0-9]*$'
            then(part.value->>'size_bytes')::bigint
            else r.actual_byte_count end byte_count
        from current_inputs r
        cross join lateral jsonb_array_elements(
          case
            when jsonb_typeof(r.result_json->'parts')='array'
                and jsonb_array_length(r.result_json->'parts')>0
              then r.result_json->'parts'
            when jsonb_typeof(r.result_json->'normalised_manifest')='array'
                and jsonb_array_length(r.result_json->'normalised_manifest')>0
              then r.result_json->'normalised_manifest'
            else jsonb_build_array(jsonb_build_object(
              'r2_key',coalesce(r.result_json->>'r2_key',
                r.result_json->>'normalised_r2_key'),
              'sha256',r.result_json->>'sha256',
              'page_count',r.actual_page_count,
              'size_bytes',r.actual_byte_count))
          end) with ordinality part(value,ordinality)
        where r.operation_id=f.operation_id
          and r.document_version_id=f.document_version_id
          and r.chunk_type='DOCUMENT_INPUT' and r.status='COMPLETE'
      ),
      physical_receipts as materialized (
        select p.*,encode(digest(jsonb_build_object(
          'receipt_contract','ACTUAL_BYTES_OBJECT_RECEIPT_V3',
          'logical_source_key',p.logical_source_key,
          'logical_manifest_ordinal',p.sequence_no,
          'physical_part_no',p.physical_part_no,
          'object_key',p.object_key,
          'stored_sha256',p.stored_sha256,
          'expected_page_count',p.page_count,
          'expected_byte_count',p.byte_count
        )::text,'sha256'),'hex') physical_receipt
        from physical p
      ),
      logical_receipts as materialized (
        select p.input_chunk_id,p.sequence_no,p.logical_source_key,
          encode(digest(jsonb_build_object(
            'receipt_contract','LOGICAL_SOURCE_RECEIPT_V3',
            'logical_source_key',p.logical_source_key,
            'logical_manifest_ordinal',p.sequence_no,
            'ordered_physical_receipts',string_agg(
              p.physical_receipt,'||' order by p.physical_part_no)
          )::text,'sha256'),'hex') logical_receipt
        from physical_receipts p
        group by p.input_chunk_id,p.sequence_no,p.logical_source_key
      )
      select
        (select count(*)::integer from logical_receipts)
          logical_source_count,
        (select encode(digest(string_agg(l.logical_receipt,'||'
          order by l.sequence_no,l.logical_source_key),
          'sha256'),'hex') from logical_receipts l)
          expected_logical_root_receipt,
        (select encode(digest(string_agg(p.physical_receipt,'||'
          order by p.sequence_no,p.physical_part_no,p.input_chunk_id),
          'sha256'),'hex') from physical_receipts p)
          expected_physical_root_receipt,
        (select encode(digest(string_agg(concat_ws('|',
          p.sequence_no::text,p.logical_source_key,
          p.physical_part_no::text,p.physical_receipt),'||'
          order by p.sequence_no,p.physical_part_no,p.input_chunk_id),
          'sha256'),'hex') from physical_receipts p)
          expected_ordered_input_root
    ) receipts on receipts.logical_source_count=coverage.manifest_input_count
      and receipts.expected_logical_root_receipt=
        f.payload_json->>'expected_logical_receipt_root'
      and receipts.expected_physical_root_receipt=
        f.payload_json->>'expected_physical_receipt_root'
    cross join lateral (
      select coalesce((
        select prior.plan_generation
        from public.invoice_operation_chunks prior
        where prior.operation_id=f.operation_id
          and prior.chunk_type='DOCUMENT_VERIFY'
          and prior.document_version_id=f.document_version_id
          and prior.level_no=0 and prior.sequence_no=0
          and prior.payload_json->>'candidate_sha256'=f.result_json->>'sha256'
          and prior.payload_json->>'expected_physical_input_hash'=
            coverage.physical_input_hash
        order by prior.created_at_utc desc,prior.id desc limit 1),
        1+coalesce((select max(prior.plan_generation)
          from public.invoice_operation_chunks prior
          where prior.operation_id=f.operation_id
            and prior.chunk_type='DOCUMENT_VERIFY'
            and prior.document_version_id=f.document_version_id
            and prior.level_no=0 and prior.sequence_no=0),0))
        plan_generation
    ) pg
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
    do update set priority=greatest(
      public.invoice_operation_chunks.priority,excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id,operation_id,document_version_id,level_no,sequence_no,plan_generation
  ),
  replaced_verifications as (
    update public.invoice_operation_chunks stale
    set status='SUPERSEDED',phase='SUPERSEDED',completed_at_utc=v_now,
        failed_at_utc=null,
        replaced_by_chunk_id=fresh.id,replacement_required=true,
        result_json=coalesce(stale.result_json,'{}'::jsonb)
          ||jsonb_build_object('replacement_chunk_id',fresh.id),
        error_json=jsonb_build_object(
          'code','REPLACED_AFTER_UPSTREAM_RETRY',
          'replacement_chunk_id',fresh.id),
        updated_at_utc=v_now
    from verify_chunks fresh
    where stale.operation_id=fresh.operation_id
      and stale.chunk_type='DOCUMENT_VERIFY'
      and stale.document_version_id=fresh.document_version_id
      and stale.level_no=fresh.level_no
      and stale.sequence_no=fresh.sequence_no
      and stale.id<>fresh.id and stale.status='BLOCKED'
      and stale.error_json->>'code'='UPSTREAM_RETRY_INVALIDATED'
    returning stale.id
  ),
  advanced as (
    update public.invoice_operation_chunks c
    set phase=case when ps.failed_count>0 then 'BLOCKED'
                   when ps.level_count=1 and ps.complete_count=1 then 'COMPLETE'
                   else 'WAIT_FOR_MERGE' end,
        status=case when ps.failed_count>0 then 'BLOCKED'
                    when ps.level_count=1 and ps.complete_count=1 then 'WAITING'
                    else 'WAITING' end,
        error_json=case when ps.failed_count>0 then jsonb_build_object('code','MERGE_FAILED') else null end,
        progress_json=jsonb_build_object('status_message',
          case when ps.level_count=1 and ps.complete_count=1 then 'Final verification queued'
               else 'Waiting for merge level' end,'merge_level',ps.current_level,
          'parts_complete',ps.complete_count,'parts_total',ps.level_count),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from plan_state ps where c.id=ps.chunk_id returning c.id,c.status,c.phase,c.progress_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,
    'progress',progress_json)),'[]'::jsonb) into v_part from advanced;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- COMPLETE only observes a verified READY version.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id from jsonb_array_elements(p_claims) x
    where x->>'phase'='COMPLETE'
  ),
  ready as materialized (
    select c.*,v.r2_key,v.sha256,v.size_bytes,v.page_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoice_document_versions v on v.id=c.document_version_id and v.status='READY'
  ),
  invoice_ptr as (
    update public.invoices i set
      preview_document_version_id=case when v.purpose='DRAFT_PREVIEW' then v.id else i.preview_document_version_id end,
      invoice_pdf_r2_key=case when v.purpose='DRAFT_PREVIEW' then v.r2_key else i.invoice_pdf_r2_key end,
      invoice_pdf_generated_at_utc=case when v.purpose='DRAFT_PREVIEW' then v.verified_at_utc else i.invoice_pdf_generated_at_utc end,
      document_state=case when v.purpose='DRAFT_PREVIEW' then 'READY' else i.document_state end,
      active_document_operation_id=case when i.active_document_operation_id=v.operation_id then null else i.active_document_operation_id end,
      last_document_error_json=null,updated_at=v_now
    from ready r join public.invoice_document_versions v on v.id=r.document_version_id
    where r.entity_type='INVOICE' and i.id=r.entity_id returning i.id
  ),
  timesheet_ptr as (
    update public.timesheets t set current_document_version_id=v.id,
      manual_pdf_r2_key=case when t.manual_document_asset_id is not null
        then v.r2_key else t.manual_pdf_r2_key end,
      generated_pdf_at_utc=v.verified_at_utc,document_state='READY',
      active_document_operation_id=null,last_document_error_json=null,updated_at=v_now
    from ready r join public.invoice_document_versions v on v.id=r.document_version_id
    where r.entity_type='TIMESHEET' and t.timesheet_id=r.entity_id and t.is_current returning t.timesheet_id
  ),
  issue_requeue as (
    update public.invoice_operation_chunks issue
    set status='QUEUED',phase='FINALISE',run_after_utc=v_now,updated_at_utc=v_now
    from ready r
    where issue.chunk_type='ISSUE_INVOICE' and issue.phase='WAIT_DOCUMENT'
      and issue.document_version_id=r.document_version_id and issue.status='WAITING'
    returning issue.id
  ),
  completed as (
    update public.invoice_operation_chunks c set status='COMPLETE',
      completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=jsonb_build_object('document_version_id',r.document_version_id,'r2_key',r.r2_key,
        'sha256',r.sha256,'size_bytes',r.size_bytes,'page_count',r.page_count)
    from ready r where c.id=r.id returning c.id,c.status,c.phase,c.result_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'result',result_json)),'[]'::jsonb)
    into v_part from completed;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

revoke all on function private._invoice_document_advance_batch_v6_downstream(jsonb,timestamptz) from public,anon,authenticated;
grant execute on function private._invoice_document_advance_batch_v6_downstream(jsonb,timestamptz) to service_role;
