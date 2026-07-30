create or replace function public.trg_timesheet_document_invalidate()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_timesheet_ids uuid[]:=array[]::uuid[];
  v_timesheet_version_ids uuid[]:=array[]::uuid[];
  v_invoice_ids uuid[]:=array[]::uuid[];
  v_invoice_version_ids uuid[]:=array[]::uuid[];
  v_timesheet_document_operation_ids uuid[]:=array[]::uuid[];
  v_invoice_document_operation_ids uuid[]:=array[]::uuid[];
  v_issue_operation_ids uuid[]:=array[]::uuid[];
  v_delivery_operation_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
begin
  if tg_table_name='timesheets' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct n.timesheet_id),array[]::uuid[])
    into v_timesheet_ids
    from new_rows n join old_rows o on o.timesheet_id=n.timesheet_id
    where(n.is_current or o.is_current) and(
      n.booking_id is distinct from o.booking_id
      or n.occupant_key_norm is distinct from o.occupant_key_norm
      or n.hospital_norm is distinct from o.hospital_norm
      or n.ward_norm is distinct from o.ward_norm
      or n.job_title_norm is distinct from o.job_title_norm
      or n.shift_label_norm is distinct from o.shift_label_norm
      or n.scheduled_start_iso is distinct from o.scheduled_start_iso
      or n.scheduled_end_iso is distinct from o.scheduled_end_iso
      or n.worked_start_iso is distinct from o.worked_start_iso
      or n.worked_end_iso is distinct from o.worked_end_iso
      or n.break_start_iso is distinct from o.break_start_iso
      or n.break_end_iso is distinct from o.break_end_iso
      or n.break_minutes is distinct from o.break_minutes
      or n.worked_minutes is distinct from o.worked_minutes
      or n.week_ending_date is distinct from o.week_ending_date
      or n.auth_name is distinct from o.auth_name
      or n.auth_job_title is distinct from o.auth_job_title
      or n.authorised_at_server is distinct from o.authorised_at_server
      or n.r2_nurse_key is distinct from o.r2_nurse_key
      or n.r2_auth_key is distinct from o.r2_auth_key
      or n.img_sha256_nurse is distinct from o.img_sha256_nurse
      or n.img_sha256_auth is distinct from o.img_sha256_auth
      or n.reference_number is distinct from o.reference_number
      or n.status is distinct from o.status
      or n.version is distinct from o.version
      or n.is_current is distinct from o.is_current
      or n.revoked_at is distinct from o.revoked_at
      or n.contract_id is distinct from o.contract_id
      or n.submission_mode is distinct from o.submission_mode
      or n.line_type is distinct from o.line_type
      or n.sheet_scope is distinct from o.sheet_scope
      or n.actual_schedule_json is distinct from o.actual_schedule_json
      or n.additional_units_week is distinct from o.additional_units_week
      or n.additional_units_per_day is distinct from o.additional_units_per_day
      or n.day_references_json is distinct from o.day_references_json
      or n.qr_token is distinct from o.qr_token
      or n.qr_payload_json is distinct from o.qr_payload_json
      or n.qr_signed_hash is distinct from o.qr_signed_hash
      or n.qr_signed_at_utc is distinct from o.qr_signed_at_utc
      or n.qr_status is distinct from o.qr_status
      or n.qr_r2_key is distinct from o.qr_r2_key
      or n.candidate_hint_text is distinct from o.candidate_hint_text
      or n.band is distinct from o.band
      or n.is_adjustment is distinct from o.is_adjustment
      or n.parent_timesheet_id is distinct from o.parent_timesheet_id
      or n.correction_id is distinct from o.correction_id
      or n.correction_kind is distinct from o.correction_kind
      or n.adjustment_origin is distinct from o.adjustment_origin);
  elsif tg_table_name='timesheets_financials' and tg_op='INSERT' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from new_rows where is_current;
  elsif tg_table_name='timesheets_financials' and tg_op='DELETE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from old_rows where is_current;
  elsif tg_table_name='timesheets_financials' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids
    from (
      select n.timesheet_id
      from new_rows n join old_rows o on o.id=n.id
      where(n.is_current or o.is_current) and(
        n.timesheet_id is distinct from o.timesheet_id
        or n.timesheet_version is distinct from o.timesheet_version
        or n.basis is distinct from o.basis
        or n.is_current is distinct from o.is_current
        or n.is_stale is distinct from o.is_stale
        or n.worked_start_iso is distinct from o.worked_start_iso
        or n.worked_end_iso is distinct from o.worked_end_iso
        or n.break_start_iso is distinct from o.break_start_iso
        or n.break_end_iso is distinct from o.break_end_iso
        or n.break_minutes is distinct from o.break_minutes
        or n.candidate_id is distinct from o.candidate_id
        or n.client_id is distinct from o.client_id
        or n.role is distinct from o.role
        or n.band is distinct from o.band
        or n.policy_snapshot_json is distinct from o.policy_snapshot_json
        or n.rate_source_refs_json is distinct from o.rate_source_refs_json
        or n.hours_day is distinct from o.hours_day
        or n.hours_night is distinct from o.hours_night
        or n.hours_sat is distinct from o.hours_sat
        or n.hours_sun is distinct from o.hours_sun
        or n.hours_bh is distinct from o.hours_bh
        or n.pay_day is distinct from o.pay_day
        or n.pay_night is distinct from o.pay_night
        or n.pay_sat is distinct from o.pay_sat
        or n.pay_sun is distinct from o.pay_sun
        or n.pay_bh is distinct from o.pay_bh
        or n.charge_day is distinct from o.charge_day
        or n.charge_night is distinct from o.charge_night
        or n.charge_sat is distinct from o.charge_sat
        or n.charge_sun is distinct from o.charge_sun
        or n.charge_bh is distinct from o.charge_bh
        or n.total_hours is distinct from o.total_hours
        or n.total_pay_ex_vat is distinct from o.total_pay_ex_vat
        or n.total_charge_ex_vat is distinct from o.total_charge_ex_vat
        or n.margin_ex_vat is distinct from o.margin_ex_vat
        or n.processing_status is distinct from o.processing_status
        or n.expenses_pay_ex_vat is distinct from o.expenses_pay_ex_vat
        or n.expenses_charge_ex_vat is distinct from o.expenses_charge_ex_vat
        or n.expenses_description is distinct from o.expenses_description
        or n.expenses_evidence_manifest is distinct from o.expenses_evidence_manifest
        or n.mileage_pay_ex_vat is distinct from o.mileage_pay_ex_vat
        or n.mileage_charge_ex_vat is distinct from o.mileage_charge_ex_vat
        or n.mileage_pay_rate is distinct from o.mileage_pay_rate
        or n.mileage_charge_rate is distinct from o.mileage_charge_rate
        or n.mileage_evidence_manifest is distinct from o.mileage_evidence_manifest
        or n.actual_schedule_json is distinct from o.actual_schedule_json
        or n.actual_minutes_by_day_json is distinct from o.actual_minutes_by_day_json
        or n.additional_units_json is distinct from o.additional_units_json
        or n.additional_pay_ex_vat is distinct from o.additional_pay_ex_vat
        or n.additional_charge_ex_vat is distinct from o.additional_charge_ex_vat
        or n.additional_margin_ex_vat is distinct from o.additional_margin_ex_vat
        or n.invoice_breakdown_json is distinct from o.invoice_breakdown_json
        or n.nhsp_import_id is distinct from o.nhsp_import_id
        or n.has_rate_issue is distinct from o.has_rate_issue
        or n.hr_crosscheck_status is distinct from o.hr_crosscheck_status
        or n.hr_crosscheck_issues is distinct from o.hr_crosscheck_issues
        or n.external_source_rows_json is distinct from o.external_source_rows_json
        or n.mileage_units is distinct from o.mileage_units
        or n.travel_pay_ex_vat is distinct from o.travel_pay_ex_vat
        or n.travel_charge_ex_vat is distinct from o.travel_charge_ex_vat
        or n.accommodation_pay_ex_vat is distinct from o.accommodation_pay_ex_vat
        or n.accommodation_charge_ex_vat is distinct from o.accommodation_charge_ex_vat
        or n.other_pay_ex_vat is distinct from o.other_pay_ex_vat
        or n.other_charge_ex_vat is distinct from o.other_charge_ex_vat)
      union
      select o.timesheet_id
      from old_rows o join new_rows n on n.id=o.id
      where n.timesheet_id is distinct from o.timesheet_id
    ) changed
    where timesheet_id is not null;

    -- A source-edit transaction may synchronise only SEGMENTS.ref_num into the
    -- current financial projection. Suppress that second invalidation solely
    -- when every changed segment is an exact, unique copy of the authoritative
    -- current timesheet schedule. Any ambiguity or any other TSFIN change keeps
    -- the ordinary invalidation path.
    v_timesheet_ids:=array(
      select candidate_id
      from unnest(v_timesheet_ids) candidate_id
      where exists(
        select 1
        from new_rows n
        join old_rows o on o.id=n.id
        where (n.timesheet_id=candidate_id or o.timesheet_id=candidate_id)
          and not (
            (to_jsonb(n)-'invoice_breakdown_json'-'updated_at')
              is not distinct from
            (to_jsonb(o)-'invoice_breakdown_json'-'updated_at')
            and n.id=o.id
            and n.timesheet_id=o.timesheet_id
            and n.is_current and o.is_current
            and upper(coalesce(n.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            and upper(coalesce(o.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            and jsonb_typeof(n.invoice_breakdown_json->'segments')='array'
            and jsonb_typeof(o.invoice_breakdown_json->'segments')='array'
            and jsonb_array_length(n.invoice_breakdown_json->'segments')
                =jsonb_array_length(o.invoice_breakdown_json->'segments')
            and (
              select coalesce(jsonb_agg(seg.value-'ref_num' order by seg.ord),'[]'::jsonb)
              from jsonb_array_elements(n.invoice_breakdown_json->'segments')
                with ordinality seg(value,ord)
            ) is not distinct from (
              select coalesce(jsonb_agg(seg.value-'ref_num' order by seg.ord),'[]'::jsonb)
              from jsonb_array_elements(o.invoice_breakdown_json->'segments')
                with ordinality seg(value,ord)
            )
            and not exists(
              select 1
              from jsonb_array_elements(n.invoice_breakdown_json->'segments')
                with ordinality ns(value,ord)
              join jsonb_array_elements(o.invoice_breakdown_json->'segments')
                with ordinality os(value,ord) using(ord)
              where nullif(btrim(coalesce(ns.value->>'ref_num','')),'')
                      is distinct from
                    nullif(btrim(coalesce(os.value->>'ref_num','')),'')
                and not exists(
                  select 1
                  from public.timesheets ts
                  where ts.timesheet_id=n.timesheet_id
                    and ts.is_current
                    and jsonb_typeof(ts.actual_schedule_json)='array'
                    and (
                      (
                        nullif(btrim(coalesce(ns.value->>'segment_id','')),'') is not null
                        and (
                          select count(*)
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(sch.value->>'segment_id','')),'')
                            =nullif(btrim(coalesce(ns.value->>'segment_id','')),'')
                        )=1
                        and (
                          select nullif(btrim(coalesce(sch.value->>'ref_num','')),'')
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(sch.value->>'segment_id','')),'')
                            =nullif(btrim(coalesce(ns.value->>'segment_id','')),'')
                          limit 1
                        ) is not distinct from
                          nullif(btrim(coalesce(ns.value->>'ref_num','')),'')
                      )
                      or (
                        (
                          nullif(btrim(coalesce(ns.value->>'segment_id','')),'') is null
                          or (
                            select count(*)
                            from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                            where nullif(btrim(coalesce(sch.value->>'segment_id','')),'')
                              =nullif(btrim(coalesce(ns.value->>'segment_id','')),'')
                          )=0
                        )
                        and nullif(btrim(coalesce(
                          ns.value->>'start_utc',ns.value->>'start','')),'') is not null
                        and nullif(btrim(coalesce(
                          ns.value->>'end_utc',ns.value->>'end','')),'') is not null
                        and (
                          select count(*)
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(
                                  sch.value->>'start_utc',sch.value->>'start','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'start_utc',ns.value->>'start','')),'')
                            and nullif(btrim(coalesce(
                                  sch.value->>'end_utc',sch.value->>'end','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'end_utc',ns.value->>'end','')),'')
                        )=1
                        and (
                          select nullif(btrim(coalesce(sch.value->>'ref_num','')),'')
                          from jsonb_array_elements(ts.actual_schedule_json) sch(value)
                          where nullif(btrim(coalesce(
                                  sch.value->>'start_utc',sch.value->>'start','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'start_utc',ns.value->>'start','')),'')
                            and nullif(btrim(coalesce(
                                  sch.value->>'end_utc',sch.value->>'end','')),'')
                                =nullif(btrim(coalesce(
                                  ns.value->>'end_utc',ns.value->>'end','')),'')
                          limit 1
                        ) is not distinct from
                          nullif(btrim(coalesce(ns.value->>'ref_num','')),'')
                      )
                    )
                )
            )
          )
      )
    );
  elsif tg_table_name='timesheet_evidence' and tg_op='INSERT' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from new_rows;
  elsif tg_table_name='timesheet_evidence' and tg_op='DELETE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids from old_rows;
  elsif tg_table_name='timesheet_evidence' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct timesheet_id),array[]::uuid[])
    into v_timesheet_ids
    from (
      select n.timesheet_id
      from new_rows n join old_rows o on o.id=n.id
      where n.timesheet_id is distinct from o.timesheet_id
        or n.kind is distinct from o.kind
        or n.storage_key is distinct from o.storage_key
        or n.source_revision is distinct from o.source_revision
        or n.display_name is distinct from o.display_name
      union
      select o.timesheet_id
      from old_rows o join new_rows n on n.id=o.id
      where n.timesheet_id is distinct from o.timesheet_id
        or n.kind is distinct from o.kind
        or n.storage_key is distinct from o.storage_key
        or n.source_revision is distinct from o.source_revision
        or n.display_name is distinct from o.display_name
    ) changed
    where timesheet_id is not null;
  end if;

  if cardinality(v_timesheet_ids)=0 then return null; end if;

  update public.timesheets t
  set document_revision=t.document_revision+1,document_state='STALE',
    current_document_version_id=null,active_document_operation_id=null,
    last_document_error_json=null
  where t.timesheet_id=any(v_timesheet_ids) and t.is_current;

  with changed as materialized (
    update public.invoice_document_versions v
    set status='SUPERSEDED',superseded_at_utc=now(),
      error_json=jsonb_build_object(
        'code','TIMESHEET_SOURCE_CHANGED','timesheet_id',v.entity_id)
    where v.entity_type='TIMESHEET' and v.entity_id=any(v_timesheet_ids)
      and v.purpose='TIMESHEET'
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
    returning v.id,v.operation_id
  )
  select coalesce(array_agg(id),array[]::uuid[]),
    coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_timesheet_version_ids,v_timesheet_document_operation_ids
  from changed;

  update public.invoice_operation_chunks c
  set status='SUPERSEDED',phase='SUPERSEDED',
    replacement_required=false,replaced_by_chunk_id=null,
    lease_owner=null,lease_token=null,lease_expires_at_utc=null,
    completed_at_utc=now(),updated_at_utc=now(),
    error_json=jsonb_build_object(
      'code','TIMESHEET_SOURCE_CHANGED','timesheet_id',c.entity_id)
  where c.document_version_id=any(v_timesheet_version_ids)
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  update public.invoice_operations o
  set control_version=o.control_version+1,status='SUPERSEDED',phase='SUPERSEDED',
    completed_at_utc=now(),updated_at_utc=now(),
    error_json=jsonb_build_object('code','TIMESHEET_SOURCE_CHANGED'),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_timesheet_document_operation_ids)
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  select coalesce(array_agg(distinct l.invoice_id),array[]::uuid[])
  into v_invoice_ids
  from public.invoice_lines l
  join public.invoices i on i.id=l.invoice_id
  where (
      l.timesheet_id=any(v_timesheet_ids)
      or (
        l.timesheet_id is null
        and coalesce(l.meta_json->>'timesheet_id','') ~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        and (l.meta_json->>'timesheet_id')::uuid=any(v_timesheet_ids)
      )
    )
    and i.status in('DRAFT','ON_HOLD')
    and i.issued_at_utc is null
    and i.paid_at_utc is null;

  if cardinality(v_invoice_ids)>0 then
    update public.invoices i
    set document_revision=i.document_revision+1,document_state='STALE',
      preview_document_version_id=null,active_document_operation_id=null,
      issue_state=case
        when i.issue_state in('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
          then 'SUPERSEDED' else i.issue_state end,
      active_issue_operation_id=null,last_document_error_json=null
    where i.id=any(v_invoice_ids);

    with changed as materialized (
      update public.invoice_document_versions v
      set status='SUPERSEDED',superseded_at_utc=now(),
        error_json=jsonb_build_object(
          'code','TIMESHEET_SOURCE_CHANGED','invoice_id',v.entity_id)
      where v.entity_type='INVOICE' and v.entity_id=any(v_invoice_ids)
        and v.purpose in('DRAFT_PREVIEW','FINAL_ISSUE')
        and v.status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
      returning v.id,v.operation_id
    )
    select coalesce(array_agg(id),array[]::uuid[]),
      coalesce(array_agg(distinct operation_id),array[]::uuid[])
    into v_invoice_version_ids,v_invoice_document_operation_ids
    from changed;

    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      replacement_required=false,replaced_by_chunk_id=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=now(),updated_at_utc=now(),
      error_json=jsonb_build_object(
        'code','TIMESHEET_SOURCE_CHANGED','document_version_id',c.document_version_id)
    where c.document_version_id=any(v_invoice_version_ids)
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

    update public.invoice_operations o
    set control_version=o.control_version+1,updated_at_utc=now(),
      change_seq=nextval('public.invoice_operation_change_seq')
    where o.id=any(v_invoice_document_operation_ids)
      and o.operation_type='BUILD_DOCUMENT'
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

    with changed as materialized (
      update public.invoice_operation_chunks c
      set status='SUPERSEDED',phase='SUPERSEDED',
        replacement_required=false,replaced_by_chunk_id=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        completed_at_utc=now(),updated_at_utc=now(),
        error_json=jsonb_build_object(
          'code','TIMESHEET_SOURCE_CHANGED','invoice_id',c.entity_id)
      where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
        and c.entity_id=any(v_invoice_ids)
        and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      returning c.operation_id
    )
    select coalesce(array_agg(distinct operation_id),array[]::uuid[])
    into v_issue_operation_ids from changed;

    with changed as materialized (
      update public.invoice_operation_chunks c
      set status='SUPERSEDED',phase='SUPERSEDED',
        replacement_required=false,replaced_by_chunk_id=null,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        completed_at_utc=now(),updated_at_utc=now(),
        error_json=jsonb_build_object(
          'code','TIMESHEET_SOURCE_CHANGED','invoice_id',c.entity_id)
      where c.chunk_type='DELIVERY_PREPARE' and c.entity_type='INVOICE'
        and c.entity_id=any(v_invoice_ids)
        and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
      returning c.operation_id
    )
    select coalesce(array_agg(distinct operation_id),array[]::uuid[])
    into v_delivery_operation_ids from changed;
  end if;

  v_operation_ids:=array(
    select distinct x
    from unnest(coalesce(v_invoice_document_operation_ids,array[]::uuid[])
      ||coalesce(v_issue_operation_ids,array[]::uuid[])
      ||coalesce(v_delivery_operation_ids,array[]::uuid[])) x
    where x is not null);

  -- Keep the statement trigger bounded to direct invalidation.  Descendant
  -- aggregation is performed later by the worker/reconciliation path.
  update public.invoice_operations o
  set progress_json=jsonb_set(coalesce(o.progress_json,'{}'::jsonb),
        '{rollup_required}','true'::jsonb,true),
    updated_at_utc=now(),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_operation_ids)
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  return null;
end;
$function$;

revoke all on function public.trg_timesheet_document_invalidate()
  from public,anon,authenticated;
