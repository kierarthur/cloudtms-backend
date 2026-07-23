create or replace function public.invoice_batch_issue_candidates(
  p_allow_early boolean default false,
  p_limit integer default 2000
) returns jsonb
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
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
$function$;

revoke all on function public.invoice_batch_issue_candidates(boolean,integer)
  from public,anon,authenticated;
grant execute on function public.invoice_batch_issue_candidates(boolean,integer)
  to service_role;
