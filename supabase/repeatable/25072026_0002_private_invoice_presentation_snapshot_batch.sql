create or replace function private._invoice_presentation_snapshot_batch(
  p_requests jsonb,
  p_now_utc timestamptz default null
) returns table(
  request_key text,
  snapshot_schema_version text,
  presentation_model jsonb,
  timesheet_sources jsonb,
  supporting_sources jsonb,
  higher_rate_support jsonb,
  snapshot_json jsonb,
  snapshot_hash text,
  valid boolean,
  error_code text,
  error_detail jsonb
)
language sql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with
raw_requests as materialized (
  select x.ordinality::integer request_no,
    nullif(btrim(x.value->>'request_key'),'') request_key,
    upper(coalesce(nullif(btrim(x.value->>'entity_type'),''),'INVOICE')) entity_type,
    case when pg_input_is_valid(x.value->>'entity_id','uuid')
      then (x.value->>'entity_id')::uuid end entity_id,
    upper(coalesce(nullif(btrim(x.value->>'purpose'),''),'DRAFT_PREVIEW')) purpose,
    coalesce(nullif(btrim(x.value->>'template_version'),''),'invoice-professional-v1') template_version,
    case when pg_input_is_valid(x.value->>'issue_at_utc','timestamptz')
      then (x.value->>'issue_at_utc')::timestamptz end issue_at_utc,
    case when pg_input_is_valid(x.value->>'due_at_utc','timestamptz')
      then (x.value->>'due_at_utc')::timestamptz end due_at_utc,
    case when pg_input_is_valid(x.value->>'tax_point_utc','timestamptz')
      then (x.value->>'tax_point_utc')::timestamptz end tax_point_utc
  from jsonb_array_elements(
    case when jsonb_typeof(p_requests)='array' then p_requests else '[]'::jsonb end
  ) with ordinality x(value,ordinality)
),
requests as materialized (
  select r.*,
    count(*) over(partition by r.request_key) request_key_count
  from raw_requests r
),
invoice_scope as materialized (
  select r.request_no,r.request_key,r.entity_type,r.entity_id,r.purpose,
    r.template_version,r.request_key_count,
    r.issue_at_utc request_issue_at_utc,
    r.due_at_utc request_due_at_utc,
    r.tax_point_utc request_tax_point_utc,
    i.*,cl.name client_name,cl.invoice_address client_invoice_address,
    cl.payment_terms_days client_payment_terms_days,
    sd.agency_name,sd.registered_address,sd.company_reg_number,
    sd.vat_registration_number,sd.bank_name,sd.bank_sort_code,
    sd.bank_account_number
  from requests r
  join public.invoices i on r.entity_type='INVOICE' and i.id=r.entity_id
  left join public.clients cl on cl.id=i.client_id
  cross join (
    select *
    from public.settings_defaults
    where id=1
  ) sd
),
line_base as materialized (
  select i.request_key,l.*,
    round(coalesce(l.hours_day,0)::numeric*coalesce(l.charge_day,0)::numeric,2) day_net,
    round(coalesce(l.hours_night,0)::numeric*coalesce(l.charge_night,0)::numeric,2) night_net,
    round(coalesce(l.hours_sat,0)::numeric*coalesce(l.charge_sat,0)::numeric,2) sat_net,
    round(coalesce(l.hours_sun,0)::numeric*coalesce(l.charge_sun,0)::numeric,2) sun_net,
    round(coalesce(l.hours_bh,0)::numeric*coalesce(l.charge_bh,0)::numeric,2) bh_net
  from invoice_scope i
  join public.invoice_lines l on l.invoice_id=i.entity_id
),
line_components_unranked as materialized (
  select l.request_key,l.id source_invoice_line_id,l.source_key,
    l.created_at,l.vat_rate_pct,l.vat_amount line_vat,
    c.component_no,
    coalesce(nullif(l.description,''),c.label) ||
      case when c.component_no=6 and c.label<>'Adjustment'
        then ' — '||c.label else '' end description,
    coalesce(l.meta_json->>'reference',l.meta_json->>'booking_reference',
      l.meta_json->>'timesheet_reference','') reference,
    c.unit,c.quantity,c.unit_price,c.net_amount
  from line_base l
  cross join lateral (
    select *
    from (values
      (1,'Day hours','HOUR',coalesce(l.hours_day,0)::numeric,
        coalesce(l.charge_day,0)::numeric,l.day_net),
      (2,'Night hours','HOUR',coalesce(l.hours_night,0)::numeric,
        coalesce(l.charge_night,0)::numeric,l.night_net),
      (3,'Saturday hours','HOUR',coalesce(l.hours_sat,0)::numeric,
        coalesce(l.charge_sat,0)::numeric,l.sat_net),
      (4,'Sunday hours','HOUR',coalesce(l.hours_sun,0)::numeric,
        coalesce(l.charge_sun,0)::numeric,l.sun_net),
      (5,'Bank-holiday hours','HOUR',coalesce(l.hours_bh,0)::numeric,
        coalesce(l.charge_bh,0)::numeric,l.bh_net),
      (6,
        case
          when upper(coalesce(l.meta_json->>'line_type','')) like '%MILEAGE%' then 'Mileage'
          when upper(coalesce(l.meta_json->>'line_type','')) like '%TRAVEL%' then 'Travel'
          when upper(coalesce(l.meta_json->>'line_type','')) like '%ACCOMMODATION%' then 'Accommodation'
          when upper(coalesce(l.meta_json->>'line_type','')) like '%HIGHER%' then 'Higher-rate adjustment'
          when upper(coalesce(l.meta_json->>'line_type','')) like '%REVERS%' then 'Reversal'
          when upper(coalesce(l.meta_json->>'line_type','')) like '%CREDIT%' then 'Credit'
          else 'Adjustment'
        end,
        coalesce(nullif(l.meta_json->>'unit',''),'ITEM'),
        1::numeric,
        (coalesce(l.total_charge_ex_vat,0)::numeric
          -(l.day_net+l.night_net+l.sat_net+l.sun_net+l.bh_net)),
        (coalesce(l.total_charge_ex_vat,0)::numeric
          -(l.day_net+l.night_net+l.sat_net+l.sun_net+l.bh_net))
      )
    ) v(component_no,label,unit,quantity,unit_price,net_amount)
    where (v.component_no<6 and coalesce(v.quantity,0)<>0)
       or (v.component_no=6 and (
         coalesce(v.net_amount,0)<>0
         or not exists (
           select 1 from (values(l.hours_day),(l.hours_night),(l.hours_sat),
             (l.hours_sun),(l.hours_bh)) h(hours) where coalesce(h.hours,0)<>0
         )
       ))
  ) c
),
line_components as materialized (
  select c.*,
    row_number() over(partition by c.source_invoice_line_id
      order by c.component_no) component_rank,
    count(*) over(partition by c.source_invoice_line_id) component_count,
    sum(abs(c.net_amount)) over(partition by c.source_invoice_line_id) weight_total
  from line_components_unranked c
),
line_vat_allocated as materialized (
  select c.*,
    row_number() over(
      partition by c.request_key
      order by c.created_at,c.source_invoice_line_id,c.component_no) display_order,
    case when c.component_rank=c.component_count then
      coalesce(c.line_vat,0)::numeric
        -coalesce(sum(case when c.component_rank<c.component_count then
          round(coalesce(c.line_vat,0)::numeric *
            case when c.weight_total=0 then 0
              else abs(c.net_amount)/c.weight_total end,2)
        end) over(partition by c.source_invoice_line_id),0)
      else round(coalesce(c.line_vat,0)::numeric *
        case when c.weight_total=0 then 0
          else abs(c.net_amount)/c.weight_total end,2)
    end allocated_vat
  from line_components c
),
presentation_lines as materialized (
  select c.request_key,
    jsonb_agg(jsonb_build_object(
      'row_key',c.source_invoice_line_id::text||':'||c.component_no,
      'source_invoice_line_id',c.source_invoice_line_id,
      'source_key',c.source_key,
      'description',c.description,
      'reference',c.reference,
      'unit',c.unit,
      'quantity',c.quantity,
      'unit_price',c.unit_price,
      'net_amount',c.net_amount,
      'vat_rate',c.vat_rate_pct,
      'vat_amount',c.allocated_vat,
      'gross_amount',c.net_amount+c.allocated_vat,
      'display_order',c.display_order
    ) order by c.created_at,c.source_invoice_line_id,c.component_no) lines,
    sum(c.net_amount)::numeric line_net,
    sum(c.allocated_vat)::numeric line_vat,
    sum(c.net_amount+c.allocated_vat)::numeric line_gross
  from line_vat_allocated c
  group by c.request_key
),
vat_breakdowns as materialized (
  select c.request_key,
    jsonb_agg(jsonb_build_object(
      'rate',c.vat_rate_pct,
      'net_amount',sum(c.net_amount),
      'vat_amount',sum(c.allocated_vat),
      'gross_amount',sum(c.net_amount+c.allocated_vat)
    ) order by c.vat_rate_pct) vat_breakdown
  from line_vat_allocated c
  group by c.request_key
),
invoice_models as materialized (
  select i.request_key,
    jsonb_build_object(
      'schema_version','INVOICE_RENDER_MODEL_V1',
      'purpose',i.purpose,
      'document_type',case
        when i.type='CREDIT_NOTE' then 'CREDIT_NOTE'
        when lower(coalesce(i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL_INVOICE'
        else 'INVOICE' end,
      'is_draft',i.purpose<>'FINAL_ISSUE',
      'invoice_number',i.invoice_no,
      'issue_date',coalesce(i.request_issue_at_utc,i.issued_at_utc),
      'preview_date',case when i.purpose='DRAFT_PREVIEW'
        then coalesce(p_now_utc,now()) else null end,
      'tax_point',coalesce(i.request_tax_point_utc,i.request_issue_at_utc,
        case when pg_input_is_valid(i.header_snapshot_json->>'tax_point_utc','timestamptz')
          then (i.header_snapshot_json->>'tax_point_utc')::timestamptz end),
      'due_date',coalesce(i.request_due_at_utc,i.due_at_utc),
      'currency',coalesce(nullif(i.header_snapshot_json->>'currency',''),'GBP'),
      'supplier',jsonb_build_object(
        'legal_name',coalesce(nullif(i.header_snapshot_json->>'agency_name',''),i.agency_name),
        'trading_name',i.header_snapshot_json->>'agency_trading_name',
        'registered_address',coalesce(i.header_snapshot_json->'registered_address',
          to_jsonb(i.registered_address)),
        'company_registration_number',coalesce(
          nullif(i.header_snapshot_json->>'company_reg_number',''),i.company_reg_number),
        'vat_registration_number',coalesce(
          nullif(i.header_snapshot_json->>'vat_registration_number',''),i.vat_registration_number),
        'contact_email',i.header_snapshot_json#>>'{supplier_contact,email}',
        'contact_phone',i.header_snapshot_json#>>'{supplier_contact,phone}'),
      'customer',jsonb_build_object(
        'legal_name',coalesce(nullif(i.header_snapshot_json->>'client_name',''),i.client_name),
        'billing_address',coalesce(i.header_snapshot_json->'client_invoice_address',
          to_jsonb(i.client_invoice_address)),
        'account_reference',i.header_snapshot_json->>'client_account_reference'),
      'references',jsonb_build_object(
        'purchase_order',coalesce(i.header_snapshot_json->>'po_reference',
          i.header_snapshot_json->>'purchase_order'),
        'client_reference',i.header_snapshot_json->>'client_reference',
        'work_location',i.header_snapshot_json->>'work_location'),
      'candidate_summary',i.header_snapshot_json->>'candidate_summary',
      'branding',jsonb_build_object('logo',jsonb_build_object(
        'r2_key',coalesce(i.header_snapshot_json#>>'{agency_logo,r2_key}',
          i.header_snapshot_json#>>'{branding,logo,r2_key}'),
        'sha256',coalesce(i.header_snapshot_json#>>'{agency_logo,sha256}',
          i.header_snapshot_json#>>'{branding,logo,sha256}'),
        'size_bytes',coalesce(i.header_snapshot_json#>'{agency_logo,size_bytes}',
          i.header_snapshot_json#>'{branding,logo,size_bytes}'),
        'media_type',coalesce(i.header_snapshot_json#>>'{agency_logo,media_type}',
          i.header_snapshot_json#>>'{branding,logo,media_type}'))),
      'lines',coalesce(pl.lines,'[]'::jsonb),
      'vat_breakdown',coalesce(vb.vat_breakdown,'[]'::jsonb),
      'totals',jsonb_build_object(
        'net',i.subtotal_ex_vat,'vat',i.vat_amount,'gross',i.total_inc_vat,
        'amount_paid',coalesce(i.header_snapshot_json->'amount_paid','0'::jsonb),
        'amount_credited',coalesce(i.header_snapshot_json->'amount_credited','0'::jsonb),
        'amount_outstanding',coalesce(i.header_snapshot_json->'amount_outstanding',
          to_jsonb(i.total_inc_vat))),
      'payment',jsonb_build_object(
        'terms_days',coalesce(
          case when pg_input_is_valid(i.header_snapshot_json->>'payment_terms_days','integer')
            then (i.header_snapshot_json->>'payment_terms_days')::integer end,
          i.client_payment_terms_days,30),
        'terms_text',i.header_snapshot_json->>'payment_terms_text',
        'due_date_basis',i.header_snapshot_json->>'due_date_basis',
        'instructions',i.header_snapshot_json->>'payment_instructions',
        'account_name',coalesce(i.header_snapshot_json->>'bank_account_name',i.bank_name),
        'sort_code',coalesce(i.header_snapshot_json->>'bank_sort_code',i.bank_sort_code),
        'account_number',coalesce(i.header_snapshot_json->>'bank_account_number',
          i.bank_account_number),
        'remittance_reference',coalesce(i.header_snapshot_json->>'remittance_reference',
          i.invoice_no),
        'remittance_email',i.header_snapshot_json->>'remittance_email'),
      'credit_note',jsonb_build_object(
        'is_credit_note',i.type='CREDIT_NOTE',
        'original_invoice_id',i.original_invoice_id,
        'original_invoice_number',i.header_snapshot_json->>'original_invoice_number',
        'original_invoice_date',i.header_snapshot_json->'original_invoice_date',
        'reason',i.header_snapshot_json->>'credit_reason'),
      'self_bill',jsonb_build_object(
        'is_self_bill',lower(coalesce(i.header_snapshot_json->>'self_bill','false'))
          in('true','t','1','yes'),
        'legal_wording',i.header_snapshot_json->>'self_bill_wording'),
      'legal_wording',case
        when jsonb_typeof(i.header_snapshot_json->'legal_wording')='array'
          then i.header_snapshot_json->'legal_wording'
        when nullif(i.header_snapshot_json->>'legal_wording','') is not null
          then jsonb_build_array(i.header_snapshot_json->>'legal_wording')
        else '[]'::jsonb end,
      'attachment_policy',coalesce(i.header_snapshot_json->'attachment_policy','{}'::jsonb),
      'template_version',i.template_version,
      'locale','en-GB',
      'page_geometry','A4_PORTRAIT_210X297MM'
    ) presentation_model,
    pl.line_net,pl.line_vat,pl.line_gross,
    i.subtotal_ex_vat expected_net,i.vat_amount expected_vat,
    i.total_inc_vat expected_gross
  from invoice_scope i
  left join presentation_lines pl using(request_key)
  left join vat_breakdowns vb using(request_key)
),
timesheet_scope as materialized (
  select distinct r.request_key,t.*,
    summary.candidate_id,summary.candidate_name,
    summary.client_id,summary.client_name,
    case when r.entity_type='TIMESHEET' then r.entity_id else l.invoice_id end parent_entity_id
  from requests r
  join public.timesheets t on t.is_current and (
    r.entity_type='TIMESHEET' and t.timesheet_id=r.entity_id
    or r.entity_type='INVOICE' and exists(
      select 1 from public.invoice_lines l
      where l.invoice_id=r.entity_id and l.timesheet_id=t.timesheet_id))
  left join public.invoice_lines l on l.timesheet_id=t.timesheet_id
    and r.entity_type='INVOICE' and l.invoice_id=r.entity_id
  left join public.v_timesheets_summary summary
    on summary.timesheet_id=t.timesheet_id
),
timesheet_models as materialized (
  select t.request_key,t.timesheet_id,
    jsonb_build_object(
      'schema_version','TIMESHEET_RENDER_MODEL_V1',
      'timesheet_id',t.timesheet_id,
      'document_revision',t.document_revision,
      'candidate',jsonb_build_object(
        'id',coalesce(t.candidate_id::text,t.occupant_key_norm),
        'name',coalesce(nullif(t.candidate_name,''),t.occupant_key_norm)),
      'client',jsonb_build_object('id',t.client_id,'name',t.client_name),
      'contract',jsonb_build_object('id',t.contract_id,'reference',t.contract_id),
      'work',jsonb_build_object(
        'hospital',t.hospital_norm,'site',t.hospital_norm,'ward',t.ward_norm,
        'assignment',t.job_title_norm,'job_title',t.job_title_norm,
        'band',t.band,'shift_type',t.shift_label_norm),
      'week_ending_date',t.week_ending_date,
      'submission_mode',t.submission_mode,
      'sheet_scope',t.sheet_scope,
      'daily_schedule_rows',case
        when jsonb_typeof(t.actual_schedule_json)='array' then coalesce((
          select jsonb_agg(jsonb_build_object(
            'date',coalesce(x.value->'date',x.value->'work_date',x.value->'day'),
            'scheduled_start',coalesce(x.value->'scheduled_start',x.value->'scheduled_start_iso'),
            'scheduled_end',coalesce(x.value->'scheduled_end',x.value->'scheduled_end_iso'),
            'worked_start',coalesce(x.value->'worked_start',x.value->'start',x.value->'worked_start_iso'),
            'worked_end',coalesce(x.value->'worked_end',x.value->'end',x.value->'worked_end_iso'),
            'break_start',coalesce(x.value->'break_start',x.value->'break_start_iso'),
            'break_end',coalesce(x.value->'break_end',x.value->'break_end_iso'),
            'break_minutes',coalesce(x.value->'break_minutes','0'::jsonb),
            'reference',coalesce(x.value->'reference',x.value->'day_reference'),
            'hours',coalesce(x.value->'hours',x.value->'worked_hours'),
            'units',coalesce(x.value->'units',x.value->'additional_units'),
            'display_order',x.ordinality)
          order by x.ordinality)
          from jsonb_array_elements(t.actual_schedule_json)
            with ordinality x(value,ordinality)),'[]'::jsonb)
        else jsonb_build_array(jsonb_build_object(
          'date',t.week_ending_date,'scheduled_start',t.scheduled_start_iso,
          'scheduled_end',t.scheduled_end_iso,'worked_start',t.worked_start_iso,
          'worked_end',t.worked_end_iso,'break_start',t.break_start_iso,
          'break_end',t.break_end_iso,'break_minutes',t.break_minutes,
          'reference',t.reference_number,
          'hours',round(coalesce(t.worked_minutes,0)::numeric/60,2),
          'units',t.additional_units_week,'display_order',1)) end,
      'weekly_schedule_rows','[]'::jsonb,
      'references',jsonb_build_object(
        'whole',t.reference_number,
        'day',coalesce(t.day_references_json,'[]'::jsonb),
        'segment','[]'::jsonb),
      'additional_units',coalesce(t.additional_units_week,0),
      'authorisation',jsonb_build_object(
        'authorised',t.authorised_at_server is not null,
        'name',t.auth_name,'role',t.auth_job_title,
        'authorised_at_utc',t.authorised_at_server),
      'signatures',jsonb_build_object(
        'candidate',case when nullif(t.r2_nurse_key,'') is null then '{}'::jsonb
          else jsonb_build_object(
            'r2_key',t.r2_nurse_key,'sha256',t.img_sha256_nurse,
            'size_bytes',null,'media_type','image/png',
            'identity',coalesce(nullif(t.candidate_name,''),t.occupant_key_norm),
            'role','Candidate / nurse') end,
        'authoriser',case when nullif(t.r2_auth_key,'') is null then '{}'::jsonb
          else jsonb_build_object(
            'r2_key',t.r2_auth_key,'sha256',t.img_sha256_auth,
            'size_bytes',null,'media_type','image/png',
            'identity',t.auth_name,'role',t.auth_job_title) end),
      'qr',jsonb_build_object(
        'required',t.qr_status is not null,
        'signed',t.qr_signed_hash is not null,
        'status',t.qr_status,'signed_hash',t.qr_signed_hash,
        'signed_at_utc',t.qr_signed_at_utc,
        'verification_summary',case when t.qr_signed_hash is not null
          then 'QR signature verified in frozen source' else null end),
      'template_version','timesheet-professional-v1'
    ) render_model,
    t.submission_mode::text submission_mode,t.document_revision,
    t.manual_document_asset_id
  from timesheet_scope t
),
timesheet_aggregates as materialized (
  select request_key,
    jsonb_agg(jsonb_build_object(
      'timesheet_id',timesheet_id,
      'submission_mode',submission_mode,
      'document_revision',document_revision,
      'manual_document_asset_id',manual_document_asset_id,
      'render_model',render_model
    ) order by timesheet_id) timesheet_sources
  from timesheet_models group by request_key
),
support_rows as materialized (
  select r.request_key,s.source_system,s.import_id,
    case when upper(s.source_system) like '%NHSP%'
      then 'NHSP_PRESENTATION_V1' else 'HEALTHROSTER_PRESENTATION_V1' end schema_version,
    jsonb_agg(
      case when upper(s.source_system) like '%NHSP%' then jsonb_build_object(
        'worker',coalesce(x.value->>'worker',x.value->>'candidate',x.value->>'name'),
        'nhsp_shift_id',coalesce(x.value->>'nhsp_shift_id',x.value->>'shift_id'),
        'booking_reference',coalesce(x.value->>'booking_reference',x.value->>'booking',x.value->>'reference'),
        'site_ward',concat_ws(' / ',nullif(x.value->>'site',''),nullif(x.value->>'ward','')),
        'shift_date',coalesce(x.value->'shift_date',x.value->'date'),
        'shift_times',coalesce(x.value->>'shift_times',
          concat_ws(' – ',nullif(x.value->>'start',''),nullif(x.value->>'end',''))),
        'hours_units',coalesce(x.value->'hours',x.value->'units'),
        'source_identity',coalesce(x.value->>'source_identity',s.import_id::text),
        'validation_state',coalesce(x.value->>'validation_state',x.value->>'status'))
      else jsonb_build_object(
        'worker',coalesce(x.value->>'worker',x.value->>'candidate',x.value->>'name'),
        'assignment',coalesce(x.value->>'assignment',x.value->>'job_title'),
        'shift_date',coalesce(x.value->'shift_date',x.value->'date'),
        'shift_times',coalesce(x.value->>'shift_times',
          concat_ws(' – ',nullif(x.value->>'start',''),nullif(x.value->>'end',''))),
        'site',x.value->>'site','ward',x.value->>'ward',
        'reference',coalesce(x.value->>'reference',x.value->>'booking_reference'),
        'units_hours',coalesce(x.value->'units',x.value->'hours'),
        'validation_state',coalesce(x.value->>'validation_state',x.value->>'status'),
        'source_identity',coalesce(x.value->>'source_identity',s.import_id::text))
      end order by x.ordinality) rows
  from requests r
  join public.invoice_hr_source_rows s
    on r.entity_type='INVOICE' and s.invoice_id=r.entity_id
  cross join lateral jsonb_array_elements(
    case when jsonb_typeof(s.rows_json)='array' then s.rows_json else '[]'::jsonb end
  ) with ordinality x(value,ordinality)
  group by r.request_key,s.source_system,s.import_id
),
support_aggregates as materialized (
  select request_key,
    jsonb_agg(jsonb_build_object(
      'source_system',source_system,'import_id',import_id,
      'render_model',jsonb_build_object('schema_version',schema_version,'rows',rows)
    ) order by source_system,import_id) supporting_sources
  from support_rows group by request_key
),
higher_rate_aggregates as materialized (
  select r.request_key,
    jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1',
      'rows',coalesce(jsonb_agg(jsonb_build_object(
        'worker_source',coalesce(l.meta_json->>'worker',l.description),
        'shift_date',coalesce(l.meta_json->'shift_date',l.meta_json->'date'),
        'original_rate',l.meta_json->'original_rate',
        'applied_rate',coalesce(l.meta_json->'applied_rate',to_jsonb(l.charge_day)),
        'units',coalesce(l.meta_json->'units',to_jsonb(l.hours_day)),
        'display_amount',to_jsonb(l.total_charge_ex_vat),
        'reason',l.meta_json->>'reason',
        'approval_identity',coalesce(l.meta_json->>'approval_identity',
          l.meta_json->>'approved_by'),
        'reference',coalesce(l.meta_json->>'reference',l.source_key)
      ) order by l.created_at,l.id) filter(where l.id is not null),'[]'::jsonb)
    ) higher_rate_support
  from requests r
  left join public.invoice_lines l on r.entity_type='INVOICE'
    and l.invoice_id=r.entity_id
    and (upper(coalesce(l.meta_json->>'line_type','')) like '%HIGHER%'
      or coalesce(l.meta_json,'{}'::jsonb)?'higher_rate')
  group by r.request_key
),
assembled as materialized (
  select r.request_no,r.request_key,r.entity_type,r.entity_id,r.purpose,
    case when r.entity_type='TIMESHEET' then 'TIMESHEET_PRESENTATION_SNAPSHOT_V5'
      when r.purpose='FINAL_ISSUE' then 'FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5'
      else 'INVOICE_PRESENTATION_SNAPSHOT_V5' end snapshot_schema_version,
    case when r.entity_type='TIMESHEET' then tm.render_model
      else im.presentation_model end presentation_model,
    case when r.entity_type='TIMESHEET' then jsonb_build_array(jsonb_build_object(
        'timesheet_id',tm.timesheet_id,'submission_mode',tm.submission_mode,
        'document_revision',tm.document_revision,
        'manual_document_asset_id',tm.manual_document_asset_id,
        'render_model',tm.render_model))
      else coalesce(ta.timesheet_sources,'[]'::jsonb) end timesheet_sources,
    coalesce(sa.supporting_sources,'[]'::jsonb) supporting_sources,
    coalesce(hr.higher_rate_support,
      jsonb_build_object('schema_version','HIGHER_RATE_PRESENTATION_V1','rows','[]'::jsonb))
      higher_rate_support,
    r.request_key_count,
    im.line_net,im.line_vat,im.line_gross,
    im.expected_net,im.expected_vat,im.expected_gross
  from requests r
  left join invoice_models im using(request_key)
  left join timesheet_aggregates ta using(request_key)
  left join support_aggregates sa using(request_key)
  left join higher_rate_aggregates hr using(request_key)
  left join timesheet_models tm on tm.request_key=r.request_key
    and r.entity_type='TIMESHEET'
),
validated as materialized (
  select a.*,
    case
      when a.request_key is null then 'PRESENTATION_REQUEST_KEY_REQUIRED'
      when a.request_key_count<>1 then 'PRESENTATION_REQUEST_KEY_DUPLICATE'
      when a.entity_id is null then 'PRESENTATION_ENTITY_ID_INVALID'
      when a.entity_type not in('INVOICE','TIMESHEET') then 'PRESENTATION_ENTITY_TYPE_INVALID'
      when a.presentation_model is null then
        case when a.entity_type='TIMESHEET' then 'TIMESHEET_PRESENTATION_MODEL_INVALID'
          else 'INVOICE_PRESENTATION_MODEL_INVALID' end
      when a.entity_type='INVOICE' and (
        nullif(a.presentation_model#>>'{supplier,legal_name}','') is null
        or nullif(a.presentation_model#>>'{customer,legal_name}','') is null
      ) then 'INVOICE_PRESENTATION_REQUIRED_FIELD_MISSING'
      when a.entity_type='INVOICE' and
        jsonb_array_length(coalesce(a.presentation_model->'lines','[]'::jsonb))=0
        and coalesce(a.expected_net,0)<>0
        then 'INVOICE_PRESENTATION_MODEL_INVALID'
      when a.entity_type='INVOICE' and (
        round(coalesce(a.line_net,0),2)<>round(coalesce(a.expected_net,0),2)
        or round(coalesce(a.line_vat,0),2)<>round(coalesce(a.expected_vat,0),2)
        or round(coalesce(a.line_gross,0),2)<>round(coalesce(a.expected_gross,0),2)
      ) then 'INVOICE_PRESENTATION_LINE_TOTAL_MISMATCH'
    end error_code
  from assembled a
),
snapshots as materialized (
  select v.*,
    jsonb_build_object(
      'snapshot_schema_version',v.snapshot_schema_version,
      'presentation_model',v.presentation_model,
      'timesheet_sources',v.timesheet_sources,
      'supporting_sources',v.supporting_sources,
      'higher_rate_support',v.higher_rate_support
    ) snapshot_json
  from validated v
)
select s.request_key,s.snapshot_schema_version,s.presentation_model,
  s.timesheet_sources,s.supporting_sources,s.higher_rate_support,
  s.snapshot_json,
  encode(digest(s.snapshot_json::text,'sha256'),'hex') snapshot_hash,
  s.error_code is null valid,s.error_code,
  case when s.error_code='INVOICE_PRESENTATION_LINE_TOTAL_MISMATCH'
    then jsonb_build_object(
      'presentation_net',s.line_net,'expected_net',s.expected_net,
      'presentation_vat',s.line_vat,'expected_vat',s.expected_vat,
      'presentation_gross',s.line_gross,'expected_gross',s.expected_gross)
    else '{}'::jsonb end error_detail
from snapshots s
order by s.request_no;
$function$;

revoke all on function private._invoice_presentation_snapshot_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_presentation_snapshot_batch(jsonb,timestamptz)
  to service_role;
