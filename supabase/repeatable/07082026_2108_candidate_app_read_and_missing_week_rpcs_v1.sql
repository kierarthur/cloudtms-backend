-- Candidate-safe bootstrap, Contract Timesheets reads and missing-week authority.

create or replace function public.candidate_app_bootstrap_v1(
  p_session_id uuid,
  p_environment text,
  p_expected_rotation integer,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate public.candidates%rowtype;
  v_flags jsonb;
  v_contract_enabled boolean:=false;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,p_expected_rotation,p_now_utc,false);
  if nullif(v_context->>'selected_candidate_id','') is not null then
    select * into v_candidate from public.candidates where id=(v_context->>'selected_candidate_id')::uuid;
    select exists(
      select 1
      from public.contract_weeks cw
      join public.contracts c on c.id=cw.contract_id and c.candidate_id=v_candidate.id
      left join lateral (
        select cs.week_ending_weekday
        from public.client_settings cs
        where cs.client_id=c.client_id
          and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
        order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc
        limit 1
      ) effective_client on true
      cross join lateral (
        select (
          (p_now_utc at time zone 'Europe/London')::date
          + mod(
              coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
              - extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer
              + 7,
              7
            )
        )::date as current_week_ending_date
      ) entitlement_window
      left join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
      where cw.week_ending_date >= (entitlement_window.current_week_ending_date-interval '6 months')::date
        and cw.week_ending_date <= entitlement_window.current_week_ending_date
        and (
          cw.status in ('OPEN','SUBMITTED','AUTHORISED','INVOICED')
          or tf.processing_status in (
            'UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING',
            'READY_FOR_HR','READY_FOR_INVOICE','PENDING_AUTH',
            'AWAITING_MANUAL_SIGNATURE','UNPROCESSED'
          )
          or cw.week_ending_date=entitlement_window.current_week_ending_date
        )
    ) into v_contract_enabled;
  end if;
  select candidate_app_feature_flags_json into v_flags from public.settings_defaults where id=1;
  return jsonb_build_object(
    'ok',true,
    'feature_contract_version','candidate-app-private-v1',
    'environment',v_context->>'environment',
    'account_id',v_context->'account_id',
    'selected_candidate_id',v_context->'selected_candidate_id',
    'selection_required',coalesce((v_context#>>'{eligibility,selection_required}')::boolean,false)
      and (v_context->>'selected_candidate_id') is null,
    'selectable_candidate_ids',case
      when upper(v_context->>'environment')='TEST' then v_context#>'{eligibility,candidate_ids}'
      else '[]'::jsonb end,
    'entitlements',jsonb_build_object(
      'contract',v_contract_enabled,
      'daily',nullif(btrim(coalesce(v_candidate.key_norm,'')),'') is not null,
      'gck_present',nullif(btrim(coalesce(v_candidate.key_norm,'')),'') is not null
    ),
    'notification_preferences',v_context->'notification_preferences',
    'feature_flags',coalesce(v_flags,'{}'::jsonb),
    'session',jsonb_build_object(
      'rotation',(v_context->>'rotation')::integer,
      'session_version',(v_context->>'session_version')::bigint,
      'expires_at_utc',v_context->'expires_at_utc',
      'absolute_expires_at_utc',v_context->'absolute_expires_at_utc'
    )
  );
end;
$function$;

create or replace function public.candidate_app_timesheet_page_v1(
  p_session_id uuid,
  p_environment text,
  p_cursor text default null,
  p_limit integer default 50,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_limit integer:=least(greatest(coalesce(p_limit,50),1),100);
  v_cursor_parts text[];
  v_cursor_rank integer;
  v_cursor_date date;
  v_cursor_id uuid;
  v_rows jsonb;
  v_next_cursor text;
  v_conflicts jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if nullif(btrim(coalesce(p_cursor,'')),'') is not null then
    begin
      v_cursor_parts:=string_to_array(p_cursor,'|');
      if cardinality(v_cursor_parts)<>4 or v_cursor_parts[1]<>'v1' then
        raise exception 'invalid cursor';
      end if;
      v_cursor_rank:=v_cursor_parts[2]::integer;
      if v_cursor_rank not in (0,1) then raise exception 'invalid cursor rank'; end if;
      v_cursor_date:=v_cursor_parts[3]::date;
      v_cursor_id:=v_cursor_parts[4]::uuid;
    exception when others then
      raise exception 'CANDIDATE_CURSOR_INVALID' using errcode='22023';
    end;
  end if;

  with candidate_weeks as materialized (
    select cw.*,c.client_id,c.candidate_id,c.weekly_timesheet_source,
           client.name as client_name,
           coalesce(nullif(t.job_title_norm,''),nullif(c.role,'')) as display_job_title,
           coalesce(nullif(t.band,''),nullif(c.band,'')) as display_band,
           coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0) as effective_week_ending_weekday,
           current_window.current_week_ending_date,
           t.parent_timesheet_id,t.status as timesheet_status,t.submission_mode,t.line_type,t.sheet_scope,t.is_current,
           t.additional_units_week,t.additional_units_per_day,
           tf.additional_units_json,tf.total_hours,tf.processing_status,tf.authorised_at_utc,tf.paid_at_utc,tf.locked_by_invoice_id,
           tf.expenses_pay_ex_vat,tf.expenses_charge_ex_vat,
           tf.mileage_units,tf.mileage_pay_ex_vat,tf.mileage_charge_ex_vat,
           tf.travel_pay_ex_vat,tf.travel_charge_ex_vat,
           tf.accommodation_pay_ex_vat,tf.accommodation_charge_ex_vat,
           tf.other_pay_ex_vat,tf.other_charge_ex_vat,
           private._candidate_record_capabilities_v1(t.timesheet_id,cw.id,'{}'::jsonb) as capabilities
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id and c.candidate_id=v_candidate_id
    join public.clients client on client.id=c.client_id
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id=c.client_id
        and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc
      limit 1
    ) effective_client on true
    cross join lateral (
      select (
        (p_now_utc at time zone 'Europe/London')::date
        +mod(
          coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
          -extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer+7,
          7
        )
      )::date as current_week_ending_date
    ) current_window
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
    left join lateral (
      select f.* from public.timesheets_financials f
      where f.timesheet_id=t.timesheet_id and f.is_current=true
      order by f.computed_at_utc desc nulls last,f.updated_at desc,f.id desc limit 1
    ) tf on true
    where t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null)
  ), expense_carriers as materialized (
    select expense_row.*,
      abs(coalesce(expense_row.expenses_pay_ex_vat,0))+abs(coalesce(expense_row.expenses_charge_ex_vat,0))+
      abs(coalesce(expense_row.mileage_units,0))+abs(coalesce(expense_row.mileage_pay_ex_vat,0))+
      abs(coalesce(expense_row.mileage_charge_ex_vat,0))+
      abs(coalesce(expense_row.travel_pay_ex_vat,0))+abs(coalesce(expense_row.travel_charge_ex_vat,0))+
      abs(coalesce(expense_row.accommodation_pay_ex_vat,0))+abs(coalesce(expense_row.accommodation_charge_ex_vat,0))+
      abs(coalesce(expense_row.other_pay_ex_vat,0))+abs(coalesce(expense_row.other_charge_ex_vat,0)) as expense_value
    from candidate_weeks expense_row
    where expense_row.capabilities->>'record_role'='EXPENSE_ONLY'
       or upper(coalesce(expense_row.line_type::text,'')) in ('EXPENSES','MILEAGE')
  ), expense_carrier_resolution as materialized (
    select carrier.id as carrier_contract_week_id,carrier.timesheet_id as carrier_timesheet_id,
      carrier.contract_id,carrier.week_ending_date,
      case when workflow_anchor.workflow_count>1 then null::uuid
        when workflow_anchor.workflow_count=1 and workflow_anchor.timesheet_id is null then null::uuid
        when workflow_anchor.timesheet_id is not null then workflow_anchor.timesheet_id
        when carrier.parent_timesheet_id is not null and parent_anchor.timesheet_id is null then null::uuid
        when parent_anchor.timesheet_id is not null then parent_anchor.timesheet_id
        when base_anchor.anchor_count>1 then null::uuid
        when base_anchor.timesheet_id is not null then base_anchor.timesheet_id
        when additional_anchor.anchor_count>1 then null::uuid
        else coalesce(parent_anchor.timesheet_id,additional_anchor.timesheet_id) end as display_timesheet_id,
      case
        when workflow_anchor.workflow_count>1 then 'AMBIGUOUS_WORKFLOW_ANCHOR'
        when workflow_anchor.workflow_count=1 and workflow_anchor.timesheet_id is null then 'INVALID_WORKFLOW_ANCHOR'
        when carrier.parent_timesheet_id is not null and parent_anchor.timesheet_id is null then 'INVALID_PARENT_ANCHOR'
        when base_anchor.anchor_count>1 then 'EXPENSE_DISPLAY_ANCHOR_AMBIGUOUS'
        when base_anchor.timesheet_id is null and additional_anchor.anchor_count>1 then 'EXPENSE_DISPLAY_ANCHOR_AMBIGUOUS'
        when coalesce(workflow_anchor.timesheet_id,parent_anchor.timesheet_id,base_anchor.timesheet_id,additional_anchor.timesheet_id) is null
          and carrier.expense_value<>0 then 'EXPENSE_DISPLAY_ANCHOR_NOT_FOUND'
        else null end as conflict_code,
      carrier.expenses_pay_ex_vat,carrier.mileage_pay_ex_vat,carrier.travel_pay_ex_vat,
      carrier.accommodation_pay_ex_vat,carrier.other_pay_ex_vat,carrier.expense_value
    from expense_carriers carrier
    left join lateral (
      select count(distinct workflow.id)::integer as workflow_count,
        min(anchor_row.timesheet_id::text)::uuid as timesheet_id
      from public.candidate_submission_workflows workflow
      left join candidate_weeks anchor_row on anchor_row.timesheet_id=workflow.anchor_timesheet_id
        and anchor_row.contract_id=carrier.contract_id
        and anchor_row.week_ending_date=carrier.week_ending_date
        and anchor_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
      where workflow.candidate_id=v_candidate_id
        and workflow.contract_id=carrier.contract_id
        and workflow.week_ending_date=carrier.week_ending_date
        and (workflow.target_timesheet_id=carrier.timesheet_id or workflow.contract_week_id=carrier.id)
        and workflow.state not in ('CANCELLED','SUPERSEDED','REJECTED')
    ) workflow_anchor on true
    left join lateral (
      select parent_row.timesheet_id
      from candidate_weeks parent_row
      where parent_row.timesheet_id=carrier.parent_timesheet_id
        and parent_row.contract_id=carrier.contract_id
        and parent_row.week_ending_date=carrier.week_ending_date
        and parent_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
      limit 1
    ) parent_anchor on true
    left join lateral (
      select count(*)::integer as anchor_count,
        min(hours_row.timesheet_id::text)::uuid as timesheet_id
      from candidate_weeks hours_row
      where hours_row.contract_id=carrier.contract_id
        and hours_row.week_ending_date=carrier.week_ending_date
        and hours_row.additional_seq=0
        and (
          coalesce(hours_row.total_hours,0)>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_json,'{}'::jsonb))>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_week,'{}'::jsonb))
            +private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_per_day,'{}'::jsonb))>0
        )
        and hours_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
    ) base_anchor on true
    left join lateral (
      select count(*)::integer as anchor_count,
        min(hours_row.timesheet_id::text)::uuid as timesheet_id
      from candidate_weeks hours_row
      where hours_row.contract_id=carrier.contract_id
        and hours_row.week_ending_date=carrier.week_ending_date
        and hours_row.additional_seq>0
        and (
          coalesce(hours_row.total_hours,0)>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_json,'{}'::jsonb))>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_week,'{}'::jsonb))
            +private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_per_day,'{}'::jsonb))>0
        )
        and hours_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
    ) additional_anchor on true
  ), expense_anchor_totals as materialized (
    select display_timesheet_id,
      sum(expenses_pay_ex_vat) expenses_pay_ex_vat,
      sum(mileage_pay_ex_vat) mileage_pay_ex_vat,
      sum(travel_pay_ex_vat) travel_pay_ex_vat,
      sum(accommodation_pay_ex_vat) accommodation_pay_ex_vat,
      sum(other_pay_ex_vat) other_pay_ex_vat
    from expense_carrier_resolution
    where display_timesheet_id is not null and conflict_code is null
    group by display_timesheet_id
  ), workflow_overlay as materialized (
    select resolved.display_timesheet_id,
      jsonb_agg(jsonb_build_object(
        'workflow_id',resolved.id,'workflow_kind',resolved.workflow_kind,'state',resolved.state,
        'target_timesheet_id',resolved.target_timesheet_id,'anchor_timesheet_id',resolved.anchor_timesheet_id,
        'rejection_reason',resolved.rejection_reason,'rejection_scope',resolved.rejection_scope,
        'required_resubmission_action',case
          when resolved.state<>'REJECTED' then null
          when resolved.workflow_kind='CONTRACT_EXPENSE'
            or resolved.rejection_scope='COMPLETE_EXPENSE_CLAIM'
            then 'RESUBMIT_EXPENSE_CLAIM'
          when resolved.workflow_kind='CONTRACT_COMBINED'
            then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
          else 'RESUBMIT_TIMESHEET' end,
        'updated_at_utc',resolved.updated_at_utc
      ) order by resolved.updated_at_utc desc,resolved.id) as workflows
    from (
      select w.*,
        case
          -- Rejection rotates the submitted timesheet to a replacement current
          -- version while the immutable workflow continues to reference the
          -- historical submitted target. Resolve rejected workflows through the
          -- current contract-week authority so the Candidate card retains the
          -- rejection reason, scope and server-owned recovery action.
          when w.state='REJECTED' and w.workflow_kind='CONTRACT_EXPENSE' then (
            select resolution.display_timesheet_id
            from expense_carrier_resolution resolution
            where resolution.carrier_contract_week_id=w.contract_week_id
              and resolution.conflict_code is null
            limit 1
          )
          when w.state='REJECTED' then (
            select current_week.timesheet_id
            from candidate_weeks current_week
            where current_week.id=w.contract_week_id
            limit 1
          )
          when w.workflow_kind='CONTRACT_EXPENSE' then coalesce(
          (select resolution.display_timesheet_id from expense_carrier_resolution resolution
            where resolution.carrier_timesheet_id=w.target_timesheet_id limit 1),
          w.anchor_timesheet_id
          )
          else coalesce(w.target_timesheet_id,w.anchor_timesheet_id)
        end as display_timesheet_id
      from public.candidate_submission_workflows w
      where w.candidate_id=v_candidate_id and w.state<>'SUPERSEDED'
    ) resolved
    where resolved.display_timesheet_id is not null
    group by resolved.display_timesheet_id
  ), visible as materialized (
    select base.*,
      coalesce(totals.expenses_pay_ex_vat,0) as overlay_expenses_pay_ex_vat,
      coalesce(totals.mileage_pay_ex_vat,0) as overlay_mileage_pay_ex_vat,
      coalesce(totals.travel_pay_ex_vat,0) as overlay_travel_pay_ex_vat,
      coalesce(totals.accommodation_pay_ex_vat,0) as overlay_accommodation_pay_ex_vat,
      coalesce(totals.other_pay_ex_vat,0) as overlay_other_pay_ex_vat,
      coalesce(workflows.workflows,'[]'::jsonb) as workflows,
      null::text as expense_overlay_conflict_code,
      (base.paid_at_utc is null) as unresolved,
      case when base.paid_at_utc is null then 1 else 0 end as unresolved_rank
    from candidate_weeks base
    left join expense_anchor_totals totals on totals.display_timesheet_id=base.timesheet_id
    left join workflow_overlay workflows on workflows.display_timesheet_id=base.timesheet_id
    where not exists(select 1 from expense_carriers carrier where carrier.id=base.id)
      and base.week_ending_date<=base.current_week_ending_date
      and (
        base.paid_at_utc is null
        or base.week_ending_date>=base.current_week_ending_date-49
      )
      and (
        v_cursor_rank is null
        or (
          case when base.paid_at_utc is null then 1 else 0 end,
          base.week_ending_date,
          base.id
        )<(v_cursor_rank,v_cursor_date,v_cursor_id)
      )
  ), page as materialized (
    select * from visible
    order by unresolved_rank desc,week_ending_date desc,id desc
    limit v_limit+1
  ), delivered as materialized (
    select page.*,
      (
        select workflow_item->>'state'
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state' in (
          'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
          'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
          'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
          'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
          'AWAITING_PAPER_RETURN','RECEIVED'
        )
        limit 1
      ) as active_workflow_state,
      (
        select workflow_item
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state'='REJECTED'
        limit 1
      ) as rejected_workflow
    from page
    order by unresolved_rank desc,week_ending_date desc,id desc
    limit v_limit
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'contract_week_id',d.id,
      'contract_id',d.contract_id,
      'timesheet_id',d.timesheet_id,
      'client_name',d.client_name,
      'job_title',d.display_job_title,
      'band',d.display_band,
      'week_ending_date',d.week_ending_date,
      'week_ending_weekday',btrim(to_char(d.week_ending_date,'FMDay')),
      'additional_seq',d.additional_seq,
      'contract_week_status',d.status,
      'timesheet_status',d.timesheet_status,
      'processing_status',d.processing_status,
      'paid',d.paid_at_utc is not null,
      'authorised',d.authorised_at_utc is not null,
      'total_hours',coalesce(d.total_hours,0),
      'expenses',jsonb_build_object(
        'expenses_pay_ex_vat',coalesce(d.overlay_expenses_pay_ex_vat,0),
        'mileage_pay_ex_vat',coalesce(d.overlay_mileage_pay_ex_vat,0),
        'travel_pay_ex_vat',coalesce(d.overlay_travel_pay_ex_vat,0),
        'accommodation_pay_ex_vat',coalesce(d.overlay_accommodation_pay_ex_vat,0),
        'other_pay_ex_vat',coalesce(d.overlay_other_pay_ex_vat,0)
      ),
      'expense_overlay_conflict_code',d.expense_overlay_conflict_code,
      'workflows',d.workflows,
      'record_role',d.capabilities->'record_role',
      'route_family',d.capabilities->'route_family',
      'candidate_status_code',case
        when d.paid_at_utc is not null then 'PAID'
        when d.authorised_at_utc is not null then 'AUTHORISED'
        when d.locked_by_invoice_id is not null or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          then 'INVOICED_NOT_PAID'
        when d.active_workflow_state is not null then d.active_workflow_state
        when d.rejected_workflow is not null then 'REJECTED'
        when d.processing_status is not null then d.processing_status::text
        else d.status::text end,
      'payment_state',case when d.paid_at_utc is not null then 'PAID' else 'UNPAID' end,
      'invoice_state',case
        when d.paid_at_utc is not null then 'PAID'
        when d.locked_by_invoice_id is not null or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          then 'INVOICED_NOT_PAID'
        else 'NOT_INVOICED' end,
      'manager_approval_state',(
        select workflow_item->>'state'
        from jsonb_array_elements(d.workflows) workflow_item
        where workflow_item->>'state' in (
          'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
          'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED','REFUSED'
        )
        limit 1
      ),
      'rejection_reason',case
        when d.paid_at_utc is not null or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.active_workflow_state is not null then null
        else nullif(d.rejected_workflow->>'rejection_reason','') end,
      'rejection_scope',case
        when d.paid_at_utc is not null or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.active_workflow_state is not null
          or d.rejected_workflow is null then d.capabilities->'reject_scope'
        else d.rejected_workflow->'rejection_scope' end,
      'rejection',case
        when d.paid_at_utc is not null or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.active_workflow_state is not null
          or d.rejected_workflow is null then null
        else jsonb_build_object(
          'workflow_id',d.rejected_workflow->'workflow_id',
          'reason',d.rejected_workflow->'rejection_reason',
          'scope',d.rejected_workflow->'rejection_scope',
          'required_action',d.rejected_workflow->'required_resubmission_action'
        ) end,
      'actions',jsonb_build_object(
        'can_edit_hours',d.capabilities->'can_edit_hours',
        'can_edit_expenses',d.capabilities->'can_edit_expenses',
        'candidate_paper_submission_allowed',d.capabilities->'candidate_paper_submission_allowed',
        'candidate_no_work_allowed',d.capabilities->'candidate_no_work_allowed',
        'can_reject_candidate_submission',d.capabilities->'can_reject_candidate_submission',
        'reject_scope',d.capabilities->'reject_scope'
      )
    ) order by d.unresolved_rank desc,d.week_ending_date desc,d.id desc),'[]'::jsonb),
    case when (select count(*) from page)>v_limit then
      (select 'v1|'||p.unresolved_rank::text||'|'||p.week_ending_date::text||'|'||p.id::text from delivered p
       order by p.unresolved_rank asc,p.week_ending_date asc,p.id asc limit 1)
    else null end,
    (
      select coalesce(jsonb_agg(jsonb_build_object(
        'contract_id',conflict.contract_id,
        'week_ending_date',conflict.week_ending_date,
        'code',conflict.conflict_code
      ) order by conflict.week_ending_date desc,conflict.contract_id),'[]'::jsonb)
      from expense_carrier_resolution conflict
      where conflict.conflict_code is not null
    )
  into v_rows,v_next_cursor,v_conflicts
  from delivered d;

  return jsonb_build_object(
    'ok',true,
    'items',v_rows,
    'next_cursor',v_next_cursor,
    'cursor_version','v1',
    'readiness_conflicts',coalesce(v_conflicts,'[]'::jsonb),
    'limit',v_limit
  );
end;
$function$;

create or replace function public.candidate_app_timesheet_detail_v1(
  p_session_id uuid,
  p_environment text,
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_workflow_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_capabilities jsonb;
  v_evidence jsonb;
  v_components jsonb;
  v_claims jsonb;
  v_document_state jsonb:='{}'::jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  if num_nonnulls(p_timesheet_id,p_contract_week_id,p_workflow_id)<>1 then
    raise exception 'CANDIDATE_DETAIL_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if p_workflow_id is not null then
    select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id and candidate_id=v_candidate_id;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    p_contract_week_id:=v_workflow.contract_week_id;
    p_timesheet_id:=case when v_workflow.state='REJECTED'
      then null else v_workflow.target_timesheet_id end;
  end if;
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_week from public.contract_weeks where timesheet_id=p_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_client from public.clients where id=v_contract.client_id;
  if p_timesheet_id is null then p_timesheet_id:=v_week.timesheet_id; end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    select * into v_fin from public.timesheets_financials where timesheet_id=p_timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(p_timesheet_id,v_week.id,'{}'::jsonb);

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',e.id,'kind',e.kind,'document_role',e.document_role,'display_name',e.display_name,
    'processing_state',e.processing_state,'created_at',e.created_at
  ) order by e.created_at,e.id),'[]'::jsonb)
  into v_evidence from public.timesheet_evidence e
  where e.timesheet_id=p_timesheet_id and e.processing_state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',c.id,'workflow_id',c.workflow_id,'workflow_generation',c.workflow_generation,
    'component_kind',c.component_kind,
    'expense_category',c.expense_category,'document_role',c.document_role,'state',c.state,
    'media_type',c.media_type,'byte_size',c.byte_size,'created_at_utc',c.created_at_utc,
    'required',c.required,'review_ordinal',c.review_ordinal,
    'review_document_ready',c.review_render_state='READY',
    'review_document_generation',c.workflow_generation,
    'review_page_count',c.review_page_count,
    'review_content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
    'final_signed_document_ready',c.final_signed_render_state='READY',
    'final_signed_page_count',c.final_signed_page_count
  ) order by c.created_at_utc,c.component_no),'[]'::jsonb)
  into v_components from public.candidate_submission_components c
  join public.candidate_submission_workflows w on w.id=c.workflow_id
  where w.candidate_id=v_candidate_id and (w.id=p_workflow_id or w.contract_week_id=v_week.id)
    and c.state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',w.id,'workflow_kind',w.workflow_kind,'state',w.state,'generation',w.generation,
    'route',w.route,'target_timesheet_id',w.target_timesheet_id,'issue_codes',w.issue_codes,
    'rejection_reason',w.rejection_reason,'rejection_scope',w.rejection_scope,
    'required_resubmission_action',case
      when w.state<>'REJECTED' then null
      when w.workflow_kind='CONTRACT_EXPENSE' or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'RESUBMIT_EXPENSE_CLAIM'
      when w.workflow_kind='CONTRACT_COMBINED' then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
      else 'RESUBMIT_TIMESHEET' end,
    'review_document_ready',exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=w.generation
        and review_component.required=true and review_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=w.generation
        and review_component.required=true and review_component.state<>'SUPERSEDED'
        and review_component.review_render_state<>'READY'),
    'review_document_generation',w.generation,
    'review_page_count',(select sum(coalesce(review_component.review_page_count,0))
      from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=w.generation
        and review_component.required=true and review_component.state<>'SUPERSEDED'),
    'manager_approval_state',coalesce((select approval.state
      from public.candidate_approval_requests approval where approval.workflow_id=w.id
        and approval.workflow_generation=w.generation
      order by approval.request_generation desc,approval.created_at_utc desc limit 1),w.state),
    'final_signed_document_ready',exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=w.generation
        and final_component.required=true and final_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=w.generation
        and final_component.required=true and final_component.state<>'SUPERSEDED'
        and final_component.final_signed_render_state<>'READY'),
    'updated_at_utc',w.updated_at_utc
  ) order by w.updated_at_utc desc,w.id desc),'[]'::jsonb)
  into v_claims from public.candidate_submission_workflows w
  where w.candidate_id=v_candidate_id and w.contract_id=v_contract.id
    and w.week_ending_date=v_week.week_ending_date and w.state<>'SUPERSEDED';

  select jsonb_build_object(
    'workflow_id',document_workflow.id,
    'workflow_generation',document_workflow.generation,
    'review_document_ready',coalesce(document_readiness.review_ready,false),
    'review_document_component_id',hours_component.id,
    'review_document_generation',hours_component.workflow_generation,
    'review_page_count',hours_component.review_page_count,
    'manager_approval_state',coalesce(latest_approval.state,document_workflow.state),
    'final_signed_document_ready',coalesce(document_readiness.final_ready,false)
  ) into v_document_state
  from public.candidate_submission_workflows document_workflow
  left join lateral (
    select component.* from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=document_workflow.generation
      and component.component_kind='HOURS_TIMESHEET' and component.state<>'SUPERSEDED'
    order by component.review_ordinal,component.id limit 1
  ) hours_component on true
  left join lateral (
    select
      count(*)>0 and bool_and(component.review_render_state='READY') as review_ready,
      count(*)>0 and bool_and(component.final_signed_render_state='READY') as final_ready
    from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=document_workflow.generation
      and component.required=true and component.state<>'SUPERSEDED'
  ) document_readiness on true
  left join lateral (
    select approval.* from public.candidate_approval_requests approval
    where approval.workflow_id=document_workflow.id
      and approval.workflow_generation=document_workflow.generation
    order by approval.request_generation desc,approval.created_at_utc desc limit 1
  ) latest_approval on true
  where document_workflow.candidate_id=v_candidate_id
    and (document_workflow.id=p_workflow_id or document_workflow.contract_week_id=v_week.id)
    and document_workflow.state<>'SUPERSEDED'
  order by (document_workflow.id=p_workflow_id) desc,document_workflow.updated_at_utc desc
  limit 1;

  return jsonb_build_object(
    'ok',true,
    'contract_week',jsonb_build_object(
      'id',v_week.id,'contract_id',v_contract.id,'week_ending_date',v_week.week_ending_date,
      'week_ending_weekday',btrim(to_char(v_week.week_ending_date,'FMDay')),
      'client_name',v_client.name,'job_title',v_contract.role,'band',v_contract.band,
      'additional_seq',v_week.additional_seq,'status',v_week.status,'planned_schedule_json',v_week.planned_schedule_json
    ),
    'timesheet',case when v_timesheet.timesheet_id is null then null else jsonb_build_object(
      'id',v_timesheet.timesheet_id,'status',v_timesheet.status,'submission_mode',v_timesheet.submission_mode,
      'sheet_scope',v_timesheet.sheet_scope,'actual_schedule_json',v_timesheet.actual_schedule_json,
      'additional_units_week',v_timesheet.additional_units_week,'qr_status',v_timesheet.qr_status,
      'canonical_work_date',case when v_timesheet.sheet_scope='DAILY' then private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date) else null end
    ) end,
    'hours',jsonb_build_object('total_hours',coalesce(v_fin.total_hours,0),'actual_schedule_json',v_fin.actual_schedule_json),
    'expenses',jsonb_build_object(
      'expenses_pay_ex_vat',coalesce(v_fin.expenses_pay_ex_vat,0),'expenses_description',v_fin.expenses_description,
      'mileage_units',coalesce(v_fin.mileage_units,0),'mileage_pay_ex_vat',coalesce(v_fin.mileage_pay_ex_vat,0),
      'travel_pay_ex_vat',coalesce(v_fin.travel_pay_ex_vat,0),
      'accommodation_pay_ex_vat',coalesce(v_fin.accommodation_pay_ex_vat,0),
      'other_pay_ex_vat',coalesce(v_fin.other_pay_ex_vat,0)
    ),
    'lifecycle',jsonb_build_object(
      'processing_status',v_fin.processing_status,'authorised_at_utc',v_fin.authorised_at_utc,
      'paid_at_utc',v_fin.paid_at_utc,'invoice_locked',v_fin.locked_by_invoice_id is not null
    ),
    'capabilities',v_capabilities,
    'manager_review',coalesce(v_document_state,'{}'::jsonb),
    'evidence',v_evidence,
    'components',v_components,
    'workflows',v_claims
  );
end;
$function$;

create or replace function public.candidate_missing_week_options_v1(
  p_session_id uuid,
  p_environment text,
  p_contract_id uuid,
  p_from date,
  p_to date,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_contract public.contracts%rowtype;
  v_first date;
  v_options jsonb;
  v_import_authority jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  if p_from is null or p_to is null or p_from>p_to or p_to-p_from>366 then
    raise exception 'CANDIDATE_MISSING_WEEK_RANGE_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  select * into v_contract from public.contracts where id=p_contract_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,null,null,p_from
  );
  if coalesce((v_import_authority->>'is_import_authoritative')::boolean,false) then
    return jsonb_build_object('ok',true,'contract_id',p_contract_id,'options','[]'::jsonb,'route_eligible',false);
  end if;
  v_first:=p_from+mod(v_contract.week_ending_weekday_snapshot-extract(dow from p_from)::integer+7,7);
  select coalesce(jsonb_agg(jsonb_build_object(
    'week_ending_date',g::date,'submission_mode',resolved.submission_mode,
    'route_family',case
      when resolved.submission_mode='ELECTRONIC' then 'ELECTRONIC'
      when coalesce((resolved.policy->>'paper_submission_enabled')::boolean,false) then 'QR'
      else 'MANUAL_NON_QR' end,
    'paper_submission_enabled',coalesce((resolved.policy->>'paper_submission_enabled')::boolean,false)
  ) order by g),'[]'::jsonb) into v_options
  from generate_series(v_first,least(p_to,v_contract.end_date),interval '7 days') g
  cross join lateral (
    select
      private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,g::date) as policy,
      private._candidate_submission_mode_v1(v_contract.client_id,v_contract.id,g::date) as submission_mode
  ) resolved
  where g::date between v_contract.start_date and v_contract.end_date
    and (
      resolved.submission_mode='ELECTRONIC'
      or (resolved.submission_mode='MANUAL'
        and coalesce((resolved.policy->>'paper_submission_enabled')::boolean,false))
    )
    and not exists(select 1 from public.contract_weeks cw where cw.contract_id=v_contract.id and cw.week_ending_date=g::date and cw.additional_seq=0);
  return jsonb_build_object('ok',true,'contract_id',p_contract_id,'options',v_options,
    'route_eligible',jsonb_array_length(v_options)>0);
end;
$function$;

create or replace function public.candidate_contract_week_add_missing_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_contract_id uuid,
  p_week_ending_date date,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_policy jsonb;
  v_mode public.submission_mode_enum;
  v_import_authority jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_week_ending_date is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_MISSING_WEEK_PAYLOAD_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,true);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  select * into v_contract from public.contracts where id=p_contract_id and candidate_id=v_candidate_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  if p_week_ending_date not between v_contract.start_date and v_contract.end_date
     or extract(dow from p_week_ending_date)::integer<>v_contract.week_ending_weekday_snapshot then
    raise exception 'CANDIDATE_MISSING_WEEK_DATE_INVALID' using errcode='22023';
  end if;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,null,null,p_week_ending_date
  );
  if coalesce((v_import_authority->>'is_import_authoritative')::boolean,false) then
    raise exception 'CANDIDATE_IMPORT_WEEK_CREATION_FORBIDDEN' using errcode='55000';
  end if;
  v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,p_week_ending_date);
  v_mode:=private._candidate_submission_mode_v1(v_contract.client_id,v_contract.id,p_week_ending_date);
  if not (v_mode='ELECTRONIC' or (v_mode='MANUAL'
    and coalesce((v_policy->>'paper_submission_enabled')::boolean,false))) then
    raise exception 'CANDIDATE_MISSING_WEEK_ROUTE_NOT_ALLOWED' using errcode='55000';
  end if;
  perform pg_advisory_xact_lock(hashtext(v_contract.id::text||'|'||p_week_ending_date::text||'|BASE_WEEK'));
  select * into v_week from public.contract_weeks
  where contract_id=v_contract.id and week_ending_date=p_week_ending_date and additional_seq=0 for update;
  if found then
    return jsonb_build_object('ok',true,'idempotent_replay',true,'contract_week_id',v_week.id,
      'week_ending_date',v_week.week_ending_date,'status',v_week.status);
  end if;
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json,planned_schedule_json,created_at,updated_at
  ) values (
    v_contract.id,p_week_ending_date,0,
    case when p_week_ending_date<=(p_now_utc at time zone 'Europe/London')::date then 'OPEN' else 'PLANNED' end,
    v_mode,'[]'::jsonb,'{}'::jsonb,
    private._candidate_week_schedule_from_template_v1(
      v_contract.std_schedule_json,p_week_ending_date,v_contract.start_date,v_contract.end_date
    ),p_now_utc,p_now_utc
  ) returning * into v_week;
  perform private._candidate_audit_v1('contract_week',v_week.id::text,'CANDIDATE_MISSING_WEEK_CREATED',null,
    jsonb_build_object('contract_id',v_contract.id,'week_ending_date',p_week_ending_date,'status',v_week.status),
    null,null,p_idempotency_key,p_now_utc);
  return jsonb_build_object('ok',true,'idempotent_replay',false,'contract_week_id',v_week.id,
    'week_ending_date',v_week.week_ending_date,'status',v_week.status,'submission_mode',v_mode);
exception when unique_violation then
  select * into v_week from public.contract_weeks
  where contract_id=p_contract_id and week_ending_date=p_week_ending_date and additional_seq=0;
  if found then return jsonb_build_object('ok',true,'idempotent_replay',true,'contract_week_id',v_week.id,
    'week_ending_date',v_week.week_ending_date,'status',v_week.status); end if;
  raise;
end;
$function$;

revoke all on function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_app_timesheet_page_v1(uuid,text,text,integer,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_missing_week_options_v1(uuid,text,uuid,date,date,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_contract_week_add_missing_atomic_v1(uuid,text,uuid,date,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz) to service_role;
grant execute on function public.candidate_app_timesheet_page_v1(uuid,text,text,integer,timestamptz) to service_role;
grant execute on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) to service_role;
grant execute on function public.candidate_missing_week_options_v1(uuid,text,uuid,date,date,timestamptz) to service_role;
grant execute on function public.candidate_contract_week_add_missing_atomic_v1(uuid,text,uuid,date,text,timestamptz) to service_role;
