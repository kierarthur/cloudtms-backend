-- Owner-only core used by the import-review RPC surface.
-- Public RPCs do not orchestrate other public RPCs; shared work is kept here.

create or replace function public._import_review_hash_v1(p_value text)
returns text
language sql
immutable
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
  select encode(extensions.digest(convert_to(coalesce(p_value,''),'UTF8'),'sha256'::text),'hex')
$function$;

create or replace function public._import_review_assert_actor_v1(p_actor_user_id uuid)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  if p_actor_user_id is null or not exists (
    select 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false)
  ) then
    raise exception 'IMPORT_REVIEW_ACTOR_INVALID' using errcode='42501';
  end if;
end
$function$;

create or replace function public._import_review_apply_envelope_core_v1(p_import_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_state public.import_review_states%rowtype;
  v_import public.hr_imports%rowtype;
  v_selected_ids text[];
  v_invalidation_ids text[];
  v_correction_units jsonb;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  select * into v_import from public.hr_imports where id=p_import_id;
  if v_state.import_id is null or v_import.id is null then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_selected_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_invalidation_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(jsonb_build_object(
    'action_id',d.action_id,'root_timesheet_id',d.timesheet_id,'source_row_key',d.source_identity,
    'correction_action',case when d.action_kind='APPLY_AMENDMENT' then 'CHANGED_HOURS' else 'CANCELLATION' end,
    'correction_shape',case when d.action_kind='APPLY_AMENDMENT' then 'REVERSAL_REPLACEMENT' else 'REVERSAL_ONLY' end
  ) order by d.action_id),'[]'::jsonb)
  into v_correction_units
  from public.import_review_decisions d
  cross join lateral (
    select public._import_review_timesheet_protection_core_v1(d.timesheet_id) as protection
  ) pr
  where d.import_id=p_import_id and d.is_current and d.selectable and d.selected
    and d.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION') and d.timesheet_id is not null
    and (coalesce((pr.protection->>'paid')::boolean,false)
      or coalesce((pr.protection->>'invoice_locked')::boolean,false))
    and coalesce((select cs.is_nhsp=true or (cs.requires_hr=true and cs.no_timesheet_required=true)
      from public.client_settings cs where cs.client_id=d.client_id
        and (cs.effective_from is null or cs.effective_from<=(statement_timestamp() at time zone 'Europe/London')::date)
      order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false);
  return jsonb_build_object(
    'schema_version','IMPORT_REVIEW_APPLY_V1','import_id',p_import_id,
    'selected_action_ids',to_jsonb(v_selected_ids),'coverage_fingerprint',v_import.coverage_fingerprint,
    'preview_fingerprint',v_state.preview_fingerprint,
    'reference_invalidation_action_ids',to_jsonb(v_invalidation_ids),
    'correction_units',v_correction_units);
end
$function$;

create or replace function public._import_review_validate_ui_state_v1(p_ui_state jsonb)
returns jsonb
language plpgsql
immutable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v jsonb:=coalesce(p_ui_state,'{}'::jsonb); v_key text;
begin
  if jsonb_typeof(v)<>'object' or pg_column_size(v)>65536 then
    raise exception 'IMPORT_REVIEW_UI_STATE_INVALID' using errcode='22023';
  end if;
  for v_key in select jsonb_object_keys(v) loop
    if v_key not in ('expanded_candidates','expanded_clients','expanded_weeks','expanded_shifts',
                     'active_section','scroll_anchor','show_no_action','show_automatic') then
      raise exception 'IMPORT_REVIEW_UI_STATE_KEY_NOT_ALLOWED' using errcode='22023',detail=v_key;
    end if;
  end loop;
  if v::text ~* '(recipient|email|amount|rate|timesheet_id|financial|action_id|selected)' then
    raise exception 'IMPORT_REVIEW_UI_STATE_CONTAINS_AUTHORITY' using errcode='22023';
  end if;
  if exists (
    select 1 from jsonb_each(v) e
    where jsonb_array_length(case when jsonb_typeof(e.value)='array' then e.value else '[]'::jsonb end)>500
  ) then raise exception 'IMPORT_REVIEW_UI_STATE_ARRAY_LIMIT_EXCEEDED' using errcode='22023'; end if;
  return v;
end
$function$;

create or replace function public._import_review_timesheet_protection_core_v1(p_timesheet_id uuid)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_active_draft boolean:=false;
  v_paid boolean:=false;
  v_invoice_locked boolean:=false;
  v_processing_status text;
begin
  if p_timesheet_id is null then
    return jsonb_build_object('active_pay_draft',false,'paid',false,'invoice_locked',false,'protected',false);
  end if;

  select coalesce(tf.paid_at_utc is not null,false),
         coalesce(tf.locked_by_invoice_id is not null,false),
         tf.processing_status::text
  into v_paid,v_invoice_locked,v_processing_status
  from public.timesheets_financials tf
  where tf.timesheet_id=p_timesheet_id and tf.is_current=true
  order by tf.updated_at desc nulls last
  limit 1;

  v_invoice_locked := coalesce(v_invoice_locked,false) or exists (
    select 1 from public.invoice_lines il
    join public.invoices i on i.id=il.invoice_id
    where il.timesheet_id=p_timesheet_id
      and (i.issued_at_utc is not null or upper(coalesce(i.status::text,''))<>'DRAFT')
  );

  v_active_draft := exists (
    select 1
    from (
      select pb.id
      from public.pay_batch_items pbi
      join public.pay_batch_candidates pbc on pbc.id=pbi.pay_batch_candidate_id
      join public.pay_batches pb on pb.id=pbc.pay_batch_id
      where pbi.timesheet_id=p_timesheet_id and not coalesce(pbi.is_voided,false)
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_batch_timesheet_snapshots pts
      join public.pay_batches pb on pb.id=pts.pay_batch_id
      where pts.timesheet_id=p_timesheet_id
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_payment_correction_items pci
      join public.pay_batches pb on pb.id=pci.pay_batch_id
      where pci.timesheet_id=p_timesheet_id and upper(btrim(coalesce(pci.status,'')))='APPLIED'
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.timesheet_payment_overrides tpo
      join public.pay_batches pb on pb.id=tpo.consumed_by_pay_batch_id
      where tpo.timesheet_id=p_timesheet_id and tpo.cleared_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_advances pa
      join public.pay_advance_reservations par on par.finance_case_id=pa.id
      join public.pay_batches pb on pb.id=par.pay_batch_id
      where pa.linked_timesheet_id=p_timesheet_id
        and upper(btrim(coalesce(par.status,''))) in ('RESERVED','COMMITTED')
        and par.released_at_utc is null and par.settled_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      union all
      select pb.id
      from public.pay_finance_case_components pfc
      join public.pay_advance_reservations par on par.finance_component_id=pfc.id
      join public.pay_batches pb on pb.id=par.pay_batch_id
      where pfc.linked_timesheet_id=p_timesheet_id
        and upper(btrim(coalesce(par.status,''))) in ('RESERVED','COMMITTED')
        and par.released_at_utc is null and par.settled_at_utc is null
        and public._pay_batch_status_is_active_reservation(pb.status)
      limit 1
    ) blockers
  );

  return jsonb_build_object(
    'active_pay_draft',v_active_draft,
    'paid',coalesce(v_paid,false),
    'invoice_locked',coalesce(v_invoice_locked,false),
    'processing_status',v_processing_status,
    'protected',v_active_draft or coalesce(v_paid,false) or coalesce(v_invoice_locked,false)
  );
end
$function$;

create or replace function public._import_review_action_catalog_core_v1(
  p_import_id uuid,
  p_preview_generation integer,
  p_max_actions integer default 500
)
returns table (
  action_id text,
  action_kind text,
  action_category text,
  target_key text,
  source_identity text,
  hr_row_id uuid,
  timesheet_id uuid,
  shift_id uuid,
  client_id uuid,
  candidate_id uuid,
  contract_id uuid,
  issue_id uuid,
  evidence_fingerprint text,
  selectable boolean,
  default_selected boolean,
  blocking boolean,
  summary_json jsonb
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare v_count integer; v_weekly_preview jsonb;
begin
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>500 then
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
  ), classified as (
    select m.*,
      con.contract_count,con.contract_id as resolved_contract_id,
      dgm.has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,
      res.resolved_timesheet_id as stored_timesheet_id,res.status as resolution_status,
      coalesce(case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id) as resolved_timesheet_id,
      nss.id as existing_shift_id,nss.timesheet_id as existing_shift_timesheet_id,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then true else false end as is_daily
    from mapped m
    left join lateral (
      select count(*)::integer contract_count,
             case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id
      from public.contracts c
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and m.date_local between c.start_date and c.end_date
    ) con on true
    left join lateral (
      select exists(select 1 from public.hr_daily_grade_role_mappings gm
        where gm.client_id=m.resolved_client_id and gm.incoming_grade_norm=m.grade_key and gm.active) has_grade_mapping
    ) dgm on true
    left join lateral (
      select count(*)::integer timesheet_count,
             array_agg(t.timesheet_id order by t.worked_start_iso,t.timesheet_id) timesheet_ids,
             case when count(*)=1 then (array_agg(t.timesheet_id order by t.timesheet_id))[1] end auto_timesheet_id
      from public.v_timesheets_daily_match t
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
    ) tsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.import_review_daily_timesheet_resolutions res
      on res.import_id=p_import_id and res.hr_row_id=m.id and res.status in ('CURRENT','APPLIED')
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,
      public._import_review_timesheet_protection_core_v1(coalesce(c.resolved_timesheet_id,c.existing_shift_timesheet_id)) as protection,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,coalesce(c.timesheet_ids::text,''),
        coalesce(c.payload_json::text,''))) as evidence_hash
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when f.contract_count>1 then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or coalesce((f.payload_json->>'break_mins')::integer,0) is distinct from (select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id)
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from facts f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY' or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
        or coalesce((m.protection->>'active_pay_draft')::boolean,false)) blocking,
      jsonb_strip_nulls(jsonb_build_object(
        'reason_code',case
          when m.resolved_candidate_id is null then 'CANDIDATE_UNRESOLVED'
          when m.resolved_client_id is null then 'CLIENT_UNRESOLVED'
          when coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when m.contract_count>1 then 'CONTRACT_AMBIGUOUS'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'TIMESHEET_NOT_FOUND'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'candidate_name',m.staff_label,'client_name',m.client_label,'work_date',m.date_local,
        'week_ending_date',m.date_local + ((7-extract(dow from m.date_local)::integer)%7),
        'start_time',m.start_time_local,'end_time',m.end_time_local,
        'break_minutes',coalesce((m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
        'hours_worked',m.hours_worked,'role',m.assignment_grade_norm,
        'timesheet_options',case when m.is_daily then to_jsonb(coalesce(m.timesheet_ids,array[]::uuid[])) else null end,
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
        when r.hours_worked is not null and r.worked_minutes is not null and abs((r.hours_worked*60)-r.worked_minutes)>1 then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_mins')::integer,(r.payload_json->>'break_minutes')::integer,0) is distinct from coalesce(r.break_minutes,0) then 'BREAK_MINUTES_MISMATCH'
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
    ), routed as (
      select p.*,c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from preview_rows p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=p.issue_fingerprint
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
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
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.issue_fingerprint,a.issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v1',a.row_json::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
        'failure_reasons',coalesce(a.row_json->'failure_reasons','[]'::jsonb),
        'days',coalesce(a.row_json->'days','[]'::jsonb),'comparisons',coalesce(a.row_json->'comparisons','[]'::jsonb),
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
    left join lateral (
      select cs.is_nhsp,cs.requires_hr,cs.no_timesheet_required
      from public.client_settings cs
      where cs.client_id=s.client_id
        and (cs.effective_from is null or cs.effective_from<=coalesce(s.week_ending_date,s.work_date))
      order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1
    ) authority on true
    where i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and s.source_system=i.source_system
      and ((i.source_system='NHSP'::public.hr_source_enum and coalesce(authority.is_nhsp,false))
        or (i.source_system='HEALTHROSTER'::public.hr_source_enum
          and upper(coalesce(i.import_scope,'')) not like '%DAILY%'
          and coalesce(authority.requires_hr,false) and coalesce(authority.no_timesheet_required,false)))
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

create or replace function public._import_review_refresh_core_v1(
  p_import_id uuid,
  p_expected_state_version bigint,
  p_actor_user_id uuid,
  p_max_actions integer default 500
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
declare
  v_state public.import_review_states%rowtype;
  v_generation integer;
  v_fingerprint text;
  v_changed integer:=0; v_retired integer:=0; v_inserted integer:=0;
  v_blockers integer; v_selected integer; v_status text; v_auto integer:=0;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then
    raise exception 'IMPORT_REVIEW_REFRESH_NOT_ALLOWED' using errcode='55000',detail=v_state.status;
  end if;
  if p_expected_state_version is not null and v_state.state_version<>p_expected_state_version then
    raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_state.state_version::text;
  end if;
  v_generation:=v_state.preview_generation+1;

  create temporary table if not exists pg_temp.review_next_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions) with no data;
  truncate pg_temp.review_next_actions;
  insert into pg_temp.review_next_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);

  -- Persist database-unambiguous Daily associations in the same normalised table.
  -- This is evidence linkage only and never edits the selected timesheet.
  insert into public.import_review_daily_timesheet_resolutions(
    import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,evidence_fingerprint,
    preview_generation,state_version,selected_by_user_id
  )
  select p_import_id,n.hr_row_id,n.timesheet_id,'AUTO_MATCHED','CURRENT',n.evidence_fingerprint,
    v_generation,v_state.state_version,p_actor_user_id
  from pg_temp.review_next_actions n
  where n.action_kind='NO_ACTION' and n.hr_row_id is not null and n.timesheet_id is not null
    and coalesce((n.summary_json->>'is_daily')::boolean,false)
  on conflict(import_id,hr_row_id) do update set
    resolved_timesheet_id=excluded.resolved_timesheet_id,resolution_method='AUTO_MATCHED',status='CURRENT',
    evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,
    state_version=excluded.state_version,selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,
    stale_at_utc=null,stale_reason_code=null,updated_at_utc=now()
  where public.import_review_daily_timesheet_resolutions.status<>'APPLIED'
    and (public.import_review_daily_timesheet_resolutions.resolved_timesheet_id is distinct from excluded.resolved_timesheet_id
      or public.import_review_daily_timesheet_resolutions.status<>'CURRENT');
  get diagnostics v_auto=row_count;
  if v_auto>0 then
    truncate pg_temp.review_next_actions;
    insert into pg_temp.review_next_actions
      select * from public._import_review_action_catalog_core_v1(p_import_id,v_generation,p_max_actions);
  end if;
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fingerprint from pg_temp.review_next_actions;

  update public.import_review_decisions d set is_current=false,refreshed_at_utc=now()
  where d.import_id=p_import_id and d.is_current
    and not exists(select 1 from pg_temp.review_next_actions n where n.action_id=d.action_id);
  get diagnostics v_retired=row_count;

  update public.import_review_decisions d set
    action_kind=n.action_kind,action_category=n.action_category,target_key=n.target_key,source_identity=n.source_identity,
    hr_row_id=n.hr_row_id,timesheet_id=n.timesheet_id,shift_id=n.shift_id,client_id=n.client_id,
    candidate_id=n.candidate_id,contract_id=n.contract_id,issue_id=n.issue_id,
    preview_generation=v_generation,evidence_fingerprint=n.evidence_fingerprint,selectable=n.selectable,
    default_selected=n.default_selected,
    selected=case when d.evidence_fingerprint=n.evidence_fingerprint and n.selectable then d.selected else false end,
    blocking=n.blocking,requires_reconfirmation=(d.evidence_fingerprint<>n.evidence_fingerprint and d.selected),
    is_current=true,summary_json=n.summary_json,refreshed_at_utc=now(),
    selected_at_utc=case when d.evidence_fingerprint=n.evidence_fingerprint then d.selected_at_utc else null end,
    selected_by_user_id=case when d.evidence_fingerprint=n.evidence_fingerprint then d.selected_by_user_id else null end
  from pg_temp.review_next_actions n
  where d.action_id=n.action_id;
  get diagnostics v_changed=row_count;

  insert into public.import_review_decisions(action_id,import_id,action_kind,action_category,target_key,source_identity,
    hr_row_id,timesheet_id,shift_id,client_id,candidate_id,contract_id,issue_id,preview_generation,evidence_fingerprint,
    selectable,default_selected,selected,blocking,summary_json)
  select n.action_id,p_import_id,n.action_kind,n.action_category,n.target_key,n.source_identity,n.hr_row_id,n.timesheet_id,
    n.shift_id,n.client_id,n.candidate_id,n.contract_id,n.issue_id,v_generation,n.evidence_fingerprint,n.selectable,
    n.default_selected,n.default_selected,n.blocking,n.summary_json
  from pg_temp.review_next_actions n
  where not exists(select 1 from public.import_review_decisions d where d.action_id=n.action_id);
  get diagnostics v_inserted=row_count;

  -- A changed daily source/target evidence association is stale unless already applied.
  update public.import_review_daily_timesheet_resolutions r set status='STALE',stale_at_utc=now(),
    stale_reason_code='EVIDENCE_CHANGED',updated_at_utc=now()
  where r.import_id=p_import_id and r.status='CURRENT' and not exists (
    select 1 from pg_temp.review_next_actions n
    where n.hr_row_id=r.hr_row_id and n.evidence_fingerprint=r.evidence_fingerprint);

  select count(*) filter(where blocking),count(*) filter(where selected)
  into v_blockers,v_selected from public.import_review_decisions
  where import_id=p_import_id and is_current;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,
    preview_generation=v_generation,preview_fingerprint=v_fingerprint,updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,'PREVIEW_REFRESHED',p_actor_user_id,jsonb_build_object(
    'preview_generation',v_generation,'preview_fingerprint',v_fingerprint,'inserted',v_inserted,
    'reconciled',v_changed,'retired',v_retired,'blockers',v_blockers));
  return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
    'import_id',p_import_id,'status',v_state.status,
    'state_version',v_state.state_version,'preview_generation',v_generation,'preview_fingerprint',v_fingerprint,
    'action_count',(select count(*) from pg_temp.review_next_actions),'blocker_count',v_blockers,'selected_count',v_selected,
    'inserted_count',v_inserted,'reconciled_count',v_changed,'retired_count',v_retired);
end
$function$;

revoke all on function public._import_review_hash_v1(text) from public,anon,authenticated,service_role;
revoke all on function public._import_review_assert_actor_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_validate_ui_state_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function public._import_review_timesheet_protection_core_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_action_catalog_core_v1(uuid,integer,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_refresh_core_v1(uuid,bigint,uuid,integer) from public,anon,authenticated,service_role;
revoke all on function public._import_review_apply_envelope_core_v1(uuid) from public,anon,authenticated,service_role;
