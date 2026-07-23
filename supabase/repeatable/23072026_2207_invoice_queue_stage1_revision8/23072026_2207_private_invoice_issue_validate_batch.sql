drop function if exists private._invoice_issue_validate_batch(jsonb,date);

create function private._invoice_issue_validate_batch(
  p_requests jsonb,
  p_evaluation_date date
) returns table(
  request_key text,
  invoice_id uuid,
  hard_blocker_codes jsonb,
  document_dependency_codes jsonb,
  delivery_blocker_codes jsonb,
  warning_codes jsonb,
  can_issue_only boolean,
  can_issue_and_deliver boolean,
  route_policy_result jsonb,
  detail_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
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
    count(*) filter(where ref.is_required
      and nullif(btrim(ref.current_reference),'') is null)
      missing_required_reference
  from invoice_scope r
  left join references_batch ref on ref.invoice_id=r.invoice_id
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
      where oc.chunk_type='ISSUE_INVOICE' and oc.entity_type='INVOICE'
        and oc.entity_id=r.invoice_id
        and(r.operation_id is null or oc.operation_id<>r.operation_id)
        and oc.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
      conflicting_issue
  from raw r
  left join request_counts rkc on rkc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join line_totals lt on lt.invoice_id=r.invoice_id
  left join timesheet_checks tc on tc.invoice_id=r.invoice_id
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
$function$;

revoke all on function private._invoice_issue_validate_batch(jsonb,date)
  from public,anon,authenticated;
grant execute on function private._invoice_issue_validate_batch(jsonb,date)
  to service_role;
