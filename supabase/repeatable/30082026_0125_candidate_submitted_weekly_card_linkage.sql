-- Repeatable CloudTMS function/view authority: candidate_submitted_weekly_card_linkage
-- Keeps an accepted pre-manager Weekly workflow attached to its Contract Week
-- until the authoritative Timesheet row is materialised.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_app_timesheet_page_v1(
  p_session_id uuid,
  p_environment text,
  p_view text default 'CURRENT',
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
  v_view text:=upper(btrim(coalesce(p_view,'CURRENT')));
  v_snapshot_utc timestamptz:=p_now_utc;
  v_limit integer:=least(greatest(coalesce(p_limit,50),1),100);
  v_cursor_parts text[];
  v_cursor_view text;
  v_cursor_snapshot timestamptz;
  v_cursor_candidate_id uuid;
  v_cursor_date date;
  v_cursor_contract_id uuid;
  v_cursor_additional_seq integer;
  v_cursor_id uuid;
  v_rows jsonb;
  v_next_cursor text;
  v_conflicts jsonb;
  v_had_more boolean;
  v_daily_rows jsonb;
  v_combined jsonb;
  v_last jsonb;
begin
  if v_view not in ('CURRENT','HISTORY') then
    raise exception 'CANDIDATE_VIEW_INVALID' using errcode='22023';
  end if;
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if nullif(btrim(coalesce(p_cursor,'')),'') is not null then
    begin
      v_cursor_parts:=string_to_array(p_cursor,'|');
      if cardinality(v_cursor_parts)<>8 then
        raise exception 'CANDIDATE_CURSOR_INVALID' using errcode='22023';
      end if;
      if v_cursor_parts[1]<>'v2' then
        raise exception 'CANDIDATE_CURSOR_VERSION_UNSUPPORTED' using errcode='22023';
      end if;
      v_cursor_view:=upper(v_cursor_parts[2]);
      v_cursor_snapshot:=v_cursor_parts[3]::timestamptz;
      v_cursor_candidate_id:=v_cursor_parts[4]::uuid;
      v_cursor_date:=v_cursor_parts[5]::date;
      v_cursor_contract_id:=v_cursor_parts[6]::uuid;
      v_cursor_additional_seq:=v_cursor_parts[7]::integer;
      v_cursor_id:=v_cursor_parts[8]::uuid;
    exception when others then
      if sqlerrm in (
        'CANDIDATE_CURSOR_VERSION_UNSUPPORTED','CANDIDATE_CURSOR_INVALID'
      ) then raise; end if;
      raise exception 'CANDIDATE_CURSOR_INVALID' using errcode='22023';
    end;
    if v_cursor_view<>v_view then
      raise exception 'CANDIDATE_CURSOR_VIEW_MISMATCH' using errcode='22023';
    end if;
    if v_cursor_candidate_id<>v_candidate_id then
      raise exception 'CANDIDATE_CURSOR_CANDIDATE_MISMATCH' using errcode='22023';
    end if;
    if v_cursor_snapshot is null or v_cursor_snapshot>p_now_utc+interval '1 minute' then
      raise exception 'CANDIDATE_CURSOR_SNAPSHOT_INVALID' using errcode='22023';
    end if;
    if v_cursor_snapshot<p_now_utc-interval '24 hours' then
      raise exception 'CANDIDATE_CURSOR_EXPIRED' using errcode='22023';
    end if;
    v_snapshot_utc:=v_cursor_snapshot;
  end if;

  with candidate_weeks as materialized (
    select cw.*,c.client_id,c.candidate_id,c.weekly_timesheet_source,
           client.name as client_name,
           coalesce(nullif(t.job_title_norm,''),nullif(c.role,'')) as display_job_title,
           coalesce(nullif(t.band,''),nullif(c.band,'')) as display_band,
           coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0) as effective_week_ending_weekday,
           current_window.current_week_ending_date,
           t.booking_id,t.parent_timesheet_id,t.status as timesheet_status,t.submission_mode,t.line_type,t.sheet_scope,t.is_current,
           t.actual_schedule_json,t.worked_start_iso,t.worked_end_iso,t.candidate_workflow_id,
           t.additional_units_week,t.additional_units_per_day,
           tf.additional_units_json,tf.total_hours,tf.processing_status,tf.authorised_at_utc,
           case when effective_pay.pay_status_code='PAID' then effective_pay.paid_at_utc else null end as paid_at_utc,
           tf.locked_by_invoice_id,
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
        and cs.effective_from<=(v_snapshot_utc at time zone 'Europe/London')::date
      order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc
      limit 1
    ) effective_client on true
    cross join lateral (
      select (
        (v_snapshot_utc at time zone 'Europe/London')::date
        +mod(
          coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
          -extract(dow from (v_snapshot_utc at time zone 'Europe/London')::date)::integer+7,
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
    left join public.timesheet_summary_pay_state_cache summary_pay_cache
      on summary_pay_cache.timesheet_id=t.timesheet_id
    left join public.timesheet_pay_state pay_state
      on pay_state.timesheet_id=t.timesheet_id
    cross join lateral (
      select
        coalesce(
          case when coalesce(summary_pay_cache.summary_state_applies,false)
            then summary_pay_cache.summary_pay_status_code end,
          pay_state.summary_pay_status_code,
          case when pay_state.last_settled_at_utc is not null or tf.paid_at_utc is not null
            then 'PAID' else 'UNPAID' end
        )::text as pay_status_code,
        case
          when coalesce(summary_pay_cache.summary_state_applies,false)
            then summary_pay_cache.last_paid_at_utc
          when pay_state.summary_pay_status_code is not null
            or pay_state.summary_pay_icon_code is not null
            then pay_state.summary_pay_paid_at_utc
          else coalesce(pay_state.last_settled_at_utc,tf.paid_at_utc)
        end as paid_at_utc
    ) effective_pay
    where t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null)
  ), current_version_resolution as materialized (
    -- Candidate workflow and parent anchors are immutable historical UUIDs.
    -- Resolve every historical member through booking_id to the one current
    -- Candidate-safe row in that version family. A missing or ambiguous
    -- current member deliberately resolves to NULL so the caller fails closed.
    select history.timesheet_id as historical_timesheet_id,
      count(distinct current_week.timesheet_id)::integer as current_count,
      case when count(distinct current_week.timesheet_id)=1
        then min(current_week.timesheet_id::text)::uuid else null::uuid end
        as current_timesheet_id
    from public.timesheets history
    join public.timesheets current_row
      on nullif(btrim(coalesce(current_row.booking_id,'')),'')
        =nullif(btrim(coalesce(history.booking_id,'')),'')
      and current_row.contract_id is not distinct from history.contract_id
      and current_row.week_ending_date is not distinct from history.week_ending_date
      and current_row.is_current=true
      and current_row.archived_at_utc is null
    join candidate_weeks current_week
      on current_week.timesheet_id=current_row.timesheet_id
    where nullif(btrim(coalesce(history.booking_id,'')),'') is not null
    group by history.timesheet_id
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
      carrier.expenses_pay_ex_vat,carrier.mileage_units,carrier.mileage_pay_ex_vat,carrier.travel_pay_ex_vat,
      carrier.accommodation_pay_ex_vat,carrier.other_pay_ex_vat,carrier.expense_value
    from expense_carriers carrier
    left join lateral (
      select count(distinct workflow.id)::integer as workflow_count,
        min(anchor_row.timesheet_id::text)::uuid as timesheet_id
      from public.candidate_submission_workflows workflow
      left join current_version_resolution workflow_anchor_family
        on workflow_anchor_family.historical_timesheet_id=workflow.anchor_timesheet_id
      left join candidate_weeks anchor_row
        on anchor_row.timesheet_id=coalesce(
          workflow_anchor_family.current_timesheet_id,workflow.anchor_timesheet_id
        )
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
      left join current_version_resolution parent_family
        on parent_family.historical_timesheet_id=carrier.parent_timesheet_id
      where parent_row.timesheet_id=coalesce(
          parent_family.current_timesheet_id,carrier.parent_timesheet_id
        )
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
      sum(mileage_units) mileage_units,
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
        'claim_family',resolved.claim_family,
        'route',resolved.route,
        'draft_has_content',case
          when resolved.state not in ('CREATED','WORKER_DRAFT') then null
          else exists(
            select 1
            from public.candidate_submission_components component
            where component.workflow_id=resolved.id
              and component.workflow_generation=resolved.generation
              and component.superseded_at_utc is null
              and component.component_kind in (
                'HOURS_TIMESHEET','CANDIDATE_SIGNATURE','MILEAGE_FORM','EXPENSE_EVIDENCE'
              )
          )
        end,
        'target_timesheet_id',resolved.target_timesheet_id,'anchor_timesheet_id',resolved.anchor_timesheet_id,
        'rejection_reason',resolved.rejection_reason,'rejection_scope',resolved.rejection_scope,
        'required_resubmission_action',case
          when resolved.state<>'REJECTED' or not resolved.rejection_actionable then null
          when resolved.workflow_kind='CONTRACT_EXPENSE'
            or resolved.rejection_scope='COMPLETE_EXPENSE_CLAIM'
            then 'RESUBMIT_EXPENSE_CLAIM'
          when resolved.workflow_kind='CONTRACT_COMBINED'
            then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
          else 'RESUBMIT_TIMESHEET' end,
        'rejection_actionable',resolved.rejection_actionable,
        'updated_at_utc',resolved.updated_at_utc
      ) order by resolved.updated_at_utc desc,resolved.id) as workflows,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{hours_submission,canonical_tsfin_snapshot,total_hours}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{hours_submission,canonical_tsfin_snapshot,total_hours}','') is not null))[1]
        as submitted_total_hours,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,expenses_pay_ex_vat}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,expenses_pay_ex_vat}','') is not null))[1]
        as submitted_expenses_pay_ex_vat,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,mileage_units}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,mileage_units}','') is not null))[1]
        as submitted_mileage_units,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,mileage_pay_ex_vat}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,mileage_pay_ex_vat}','') is not null))[1]
        as submitted_mileage_pay_ex_vat,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,travel_pay_ex_vat}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,travel_pay_ex_vat}','') is not null))[1]
        as submitted_travel_pay_ex_vat,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,accommodation_pay_ex_vat}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,accommodation_pay_ex_vat}','') is not null))[1]
        as submitted_accommodation_pay_ex_vat,
      (array_agg(
        nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,other_pay_ex_vat}','')::numeric
        order by resolved.updated_at_utc desc,resolved.id
      ) filter (where resolved.state in (
        'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
        'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT',
        'READY_TO_FINALISE','AWAITING_PAPER_RETURN','RECEIVED'
      ) and nullif(resolved.immutable_submission_json#>>'{expense_submission,canonical_tsfin_snapshot,other_pay_ex_vat}','') is not null))[1]
        as submitted_other_pay_ex_vat
    from (
      select classified.*,
        case
          -- Rejection rotates the submitted timesheet to a replacement current
          -- version while the immutable workflow continues to reference the
          -- historical submitted target. Resolve rejected workflows through the
          -- current contract-week authority so the Candidate card retains the
          -- rejection reason, scope and server-owned recovery action.
          when classified.state='REJECTED'
            and classified.claim_family='EXPENSES' then (
            select resolution.display_timesheet_id
            from expense_carrier_resolution resolution
            where resolution.carrier_contract_week_id=classified.contract_week_id
              and resolution.conflict_code is null
            limit 1
          )
          when classified.state='REJECTED' then (
            select current_week.timesheet_id
            from candidate_weeks current_week
            where current_week.id=classified.contract_week_id
            limit 1
          )
          when classified.claim_family='EXPENSES' then coalesce(
          (select resolution.display_timesheet_id from expense_carrier_resolution resolution
            where resolution.carrier_timesheet_id=classified.target_timesheet_id limit 1),
          (select family.current_timesheet_id from current_version_resolution family
            where family.historical_timesheet_id=classified.anchor_timesheet_id
              and family.current_count=1),
          (select direct_anchor.timesheet_id from candidate_weeks direct_anchor
            where direct_anchor.timesheet_id=classified.anchor_timesheet_id limit 1)
          )
          else coalesce(
            (select family.current_timesheet_id from current_version_resolution family
              where family.historical_timesheet_id=coalesce(
                classified.target_timesheet_id,classified.anchor_timesheet_id
              ) and family.current_count=1),
            (select direct_target.timesheet_id from candidate_weeks direct_target
              where direct_target.timesheet_id=coalesce(
                classified.target_timesheet_id,classified.anchor_timesheet_id
              ) limit 1),
            (select draft_week.id from candidate_weeks draft_week
              where classified.state in (
                'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
                'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
                'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
                'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
                'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
              )
                and classified.target_timesheet_id is null
                and classified.anchor_timesheet_id is null
                and draft_week.id=classified.contract_week_id
              limit 1)
          )
        end as display_timesheet_id
      from (
        select w.*,
          case when w.workflow_kind='CONTRACT_EXPENSE'
              or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
            then 'EXPENSES' else 'HOURS' end as claim_family,
          case when w.state<>'REJECTED' then false
            else not private._candidate_rejection_replaced_v1(w.id)
          end as rejection_actionable
        from public.candidate_submission_workflows w
        where w.candidate_id=v_candidate_id and w.state<>'SUPERSEDED'
      ) classified
    ) resolved
    where resolved.display_timesheet_id is not null
    group by resolved.display_timesheet_id
  ), visible as materialized (
    select base.*,
      case when base.timesheet_id is null then coalesce(workflows.submitted_total_hours,base.total_hours,0)
        else coalesce(base.total_hours,0) end as overlay_total_hours,
      case when base.timesheet_id is null then coalesce(workflows.submitted_expenses_pay_ex_vat,totals.expenses_pay_ex_vat,base.expenses_pay_ex_vat,0)
        else coalesce(totals.expenses_pay_ex_vat,base.expenses_pay_ex_vat,0) end as overlay_expenses_pay_ex_vat,
      case when base.timesheet_id is null then coalesce(workflows.submitted_mileage_units,totals.mileage_units,base.mileage_units,0)
        else coalesce(totals.mileage_units,base.mileage_units,0) end as overlay_mileage_units,
      case when base.timesheet_id is null then coalesce(workflows.submitted_mileage_pay_ex_vat,totals.mileage_pay_ex_vat,base.mileage_pay_ex_vat,0)
        else coalesce(totals.mileage_pay_ex_vat,base.mileage_pay_ex_vat,0) end as overlay_mileage_pay_ex_vat,
      case when base.timesheet_id is null then coalesce(workflows.submitted_travel_pay_ex_vat,totals.travel_pay_ex_vat,base.travel_pay_ex_vat,0)
        else coalesce(totals.travel_pay_ex_vat,base.travel_pay_ex_vat,0) end as overlay_travel_pay_ex_vat,
      case when base.timesheet_id is null then coalesce(workflows.submitted_accommodation_pay_ex_vat,totals.accommodation_pay_ex_vat,base.accommodation_pay_ex_vat,0)
        else coalesce(totals.accommodation_pay_ex_vat,base.accommodation_pay_ex_vat,0) end as overlay_accommodation_pay_ex_vat,
      case when base.timesheet_id is null then coalesce(workflows.submitted_other_pay_ex_vat,totals.other_pay_ex_vat,base.other_pay_ex_vat,0)
        else coalesce(totals.other_pay_ex_vat,base.other_pay_ex_vat,0) end as overlay_other_pay_ex_vat,
      coalesce(workflows.workflows,'[]'::jsonb) as workflows,
      null::text as expense_overlay_conflict_code,
      membership.tab_bucket
    from candidate_weeks base
    left join expense_anchor_totals totals on totals.display_timesheet_id=base.timesheet_id
    -- Before submission a mutable workflow has no timesheet anchor. In that
    -- bounded state display_timesheet_id carries its immutable contract-week
    -- UUID, so the same card can present the unfinished draft truthfully.
    left join workflow_overlay workflows
      on workflows.display_timesheet_id=coalesce(base.timesheet_id,base.id)
    cross join lateral (
      select case
        when base.paid_at_utc is null or base.paid_at_utc>v_snapshot_utc then 'CURRENT'
        when base.paid_at_utc>=v_snapshot_utc-interval '7 days' then 'CURRENT'
        when base.paid_at_utc<v_snapshot_utc-interval '7 days'
          and base.week_ending_date between base.current_week_ending_date-105
            and base.current_week_ending_date then 'HISTORY'
        else 'EXCLUDED' end as tab_bucket
    ) membership
    where not exists(select 1 from expense_carriers carrier where carrier.id=base.id)
      and base.week_ending_date<=base.current_week_ending_date
      and membership.tab_bucket=v_view
      and (
        v_cursor_date is null
        or (
          base.week_ending_date,
          base.contract_id,
          base.additional_seq,
          base.id
        )<(v_cursor_date,v_cursor_contract_id,v_cursor_additional_seq,v_cursor_id)
      )
  ), page as materialized (
    select * from visible
    order by week_ending_date desc,contract_id desc,additional_seq desc,id desc
    limit v_limit+1
  ), delivered as materialized (
    select page.*,
      expense_presentation.is_expense_only,
      expense_presentation.expense_route_kind,
      expense_presentation.display_route_label,
      (
        select workflow_item->>'state'
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state' in (
          'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
          'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
          'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
          'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
          'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
        )
        limit 1
      ) as active_workflow_state,
      (
        select workflow_item
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state'='REJECTED'
          and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
        limit 1
      ) as rejected_workflow,
      (
        select coalesce(jsonb_agg(workflow_item order by
          workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'),'[]'::jsonb)
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state'='REJECTED'
          and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
      ) as actionable_rejections
    from page
    left join lateral (
      select upper(nullif(btrim(workflow_item->>'route'),'')) as route
      from jsonb_array_elements(page.workflows) workflow_item
      where workflow_item->>'claim_family'='EXPENSES'
      order by workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
      limit 1
    ) expense_workflow on true
    cross join lateral (
      select (
        coalesce(page.overlay_total_hours,0)=0::numeric
        and (
          upper(coalesce(page.line_type::text,'')) in ('EXPENSES','MILEAGE')
          or expense_workflow.route is not null
        )
        and coalesce(page.actual_schedule_json,'[]'::jsonb) in ('[]'::jsonb,'{}'::jsonb,'null'::jsonb)
        and not jsonb_path_exists(coalesce(page.additional_units_week,'{}'::jsonb),
          'lax $.** ? (@.type() == "number" && @ != 0)')
        and not jsonb_path_exists(coalesce(page.additional_units_per_day,'{}'::jsonb),
          'lax $.** ? (@.type() == "number" && @ != 0)')
        and not jsonb_path_exists(coalesce(page.additional_units_json,'{}'::jsonb),
          'lax $.** ? (@.type() == "number" && @ != 0)')
        and page.worked_start_iso is null
        and page.worked_end_iso is null
        and (
          abs(coalesce(page.overlay_expenses_pay_ex_vat,0::numeric))
          +abs(coalesce(page.overlay_mileage_units,0::numeric))
          +abs(coalesce(page.overlay_mileage_pay_ex_vat,0::numeric))
          +abs(coalesce(page.overlay_travel_pay_ex_vat,0::numeric))
          +abs(coalesce(page.overlay_accommodation_pay_ex_vat,0::numeric))
          +abs(coalesce(page.overlay_other_pay_ex_vat,0::numeric))
        )>0::numeric
      ) as is_expense_only
    ) expense_fact
    cross join lateral (
      select expense_fact.is_expense_only,
        case
          when not expense_fact.is_expense_only then 'UNKNOWN'
          when expense_workflow.route='PAPER' then 'QR'
          when expense_workflow.route in ('PHONE','EMAIL','ELECTRONIC') then 'ELECTRONIC'
          when page.submission_mode='MANUAL'::public.submission_mode_enum
            and upper(coalesce(page.timesheet_status::text,''))='SUBMITTED'
            and page.candidate_workflow_id is null then 'MANUAL'
          else 'UNKNOWN' end as expense_route_kind,
        case
          when not expense_fact.is_expense_only then null
          when expense_workflow.route='PAPER' then 'QR Expense'
          when expense_workflow.route in ('PHONE','EMAIL','ELECTRONIC') then 'Electronic Expense'
          when page.submission_mode='MANUAL'::public.submission_mode_enum
            and upper(coalesce(page.timesheet_status::text,''))='SUBMITTED'
            and page.candidate_workflow_id is null then 'Manual Expense'
          else 'Expense' end as display_route_label
    ) expense_presentation
    order by week_ending_date desc,contract_id desc,additional_seq desc,id desc
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
      'week_ending_label',private._candidate_week_ending_label_v1(d.week_ending_date),
      'week_ending_weekday',btrim(to_char(d.week_ending_date,'FMDay')),
      'additional_seq',d.additional_seq,
      'tab_bucket',d.tab_bucket,
      'effective_current_week_ending_date',d.current_week_ending_date,
      'paid_at_utc',case when d.paid_at_utc<=v_snapshot_utc then d.paid_at_utc else null end,
      'contract_week_status',d.status,
      'timesheet_status',d.timesheet_status,
      'processing_status',d.processing_status,
      'paid',d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc,
      'authorised',d.authorised_at_utc is not null,
      'is_expense_only',d.is_expense_only,
      'expense_route_kind',d.expense_route_kind,
      'display_route_label',d.display_route_label,
      'total_hours',coalesce(d.overlay_total_hours,0),
      'expenses',jsonb_build_object(
        'expenses_pay_ex_vat',coalesce(d.overlay_expenses_pay_ex_vat,0),
        'mileage_units',coalesce(d.overlay_mileage_units,0),
        'mileage_pay_ex_vat',coalesce(d.overlay_mileage_pay_ex_vat,0),
        'travel_pay_ex_vat',coalesce(d.overlay_travel_pay_ex_vat,0),
        'accommodation_pay_ex_vat',coalesce(d.overlay_accommodation_pay_ex_vat,0),
        'other_pay_ex_vat',coalesce(d.overlay_other_pay_ex_vat,0)
      ),
      'expense_overlay_conflict_code',d.expense_overlay_conflict_code,
      'workflows',d.workflows,
      'rejections',d.actionable_rejections,
      'record_role',d.capabilities->'record_role',
      'route_family',d.capabilities->'route_family',
      'candidate_status_code',private._candidate_status_code_v1(
        d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc,
        d.authorised_at_utc is not null,
        d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED',
        d.active_workflow_state,d.rejected_workflow is not null,
        d.processing_status::text,d.status::text
      ),
      'payment_state',case when d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc
        then 'PAID' else 'UNPAID' end,
      'invoice_state',case
        when d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc then 'PAID'
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
        when (d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc) or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED' then null
        else nullif(d.rejected_workflow->>'rejection_reason','') end,
      'rejection_scope',case
        when (d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc) or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.rejected_workflow is null then d.capabilities->'reject_scope'
        else d.rejected_workflow->'rejection_scope' end,
      'rejection',case
        when (d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc) or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.rejected_workflow is null then null
        else jsonb_build_object(
          'workflow_id',d.rejected_workflow->'workflow_id',
          'reason',d.rejected_workflow->'rejection_reason',
          'scope',d.rejected_workflow->'rejection_scope',
          'required_action',d.rejected_workflow->'required_resubmission_action'
        ) end,
      'primary_action',private._candidate_action_invocation_v1(private._candidate_timesheet_primary_action_v1(
        private._candidate_status_code_v1(
          d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc,
          d.authorised_at_utc is not null,
          d.locked_by_invoice_id is not null
            or upper(coalesce(d.timesheet_status::text,''))='INVOICED',
          d.active_workflow_state,d.rejected_workflow is not null,
          d.processing_status::text,d.status::text
        ),
        d.workflows,d.capabilities,d.timesheet_id,d.id
      )),
      'detail_target',case
        when d.rejected_workflow is not null then jsonb_build_object(
          'identity_kind','WORKFLOW','id',d.rejected_workflow->>'workflow_id',
          'path','/candidate-app/v1/workflows/'||(d.rejected_workflow->>'workflow_id')||'/timesheet-detail'
        )
        when d.timesheet_id is not null then jsonb_build_object(
          'identity_kind','TIMESHEET','id',d.timesheet_id,
          'path','/candidate-app/v1/timesheets/'||d.timesheet_id::text
        )
        else jsonb_build_object(
          'identity_kind','CONTRACT_WEEK','id',d.id,
          'path','/candidate-app/v1/contract-weeks/'||d.id::text||'/detail'
        ) end,
      'actions',jsonb_build_object(
        'can_edit_hours',d.capabilities->'can_edit_hours',
        'can_edit_expenses',d.capabilities->'can_edit_expenses',
        'candidate_paper_submission_allowed',d.capabilities->'candidate_paper_submission_allowed',
        'candidate_no_work_allowed',d.capabilities->'candidate_no_work_allowed',
        'can_reject_candidate_submission',d.capabilities->'can_reject_candidate_submission',
        'reject_scope',d.capabilities->'reject_scope'
      )
    ) order by d.week_ending_date desc,d.contract_id desc,d.additional_seq desc,d.id desc),'[]'::jsonb),
    case when (select count(*) from page)>v_limit then
      (select 'v2|'||v_view||'|'||to_char(v_snapshot_utc at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')||'|'||v_candidate_id::text||'|'
          ||p.week_ending_date::text||'|'||p.contract_id::text||'|'
          ||p.additional_seq::text||'|'||p.id::text
       from delivered p
       order by p.week_ending_date asc,p.contract_id asc,p.additional_seq asc,p.id asc limit 1)
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


  -- Add Daily current families after the unchanged weekly/expense projection.
  -- The zero UUID is a private pagination sort key, NEVER a Contract identity.
  -- Apply the global cursor and bounded limit before loading any Daily detail.
  v_had_more:=v_next_cursor is not null;
  with daily_candidates as materialized (
    select t.timesheet_id,t.week_ending_date,
      private._candidate_daily_work_date_v1(t.worked_start_iso,t.scheduled_start_iso,t.week_ending_date) as work_date,
      case when effective_pay.pay_status_code='PAID' then effective_pay.paid_at_utc else null end as paid_at_utc,
      ((v_snapshot_utc at time zone 'Europe/London')::date
        +mod(7-extract(dow from (v_snapshot_utc at time zone 'Europe/London')::date)::integer,7))::date as current_week
    from public.timesheets t
    join public.candidates c on c.id=v_candidate_id and c.active
    left join lateral (
      select fin.* from public.timesheets_financials fin where fin.timesheet_id=t.timesheet_id and fin.is_current
      order by fin.computed_at_utc desc nulls last,fin.updated_at desc,fin.id desc limit 1
    ) f on true
    left join public.timesheet_summary_pay_state_cache summary_pay_cache
      on summary_pay_cache.timesheet_id=t.timesheet_id
    left join public.timesheet_pay_state pay_state
      on pay_state.timesheet_id=t.timesheet_id
    cross join lateral (
      select
        coalesce(
          case when coalesce(summary_pay_cache.summary_state_applies,false)
            then summary_pay_cache.summary_pay_status_code end,
          pay_state.summary_pay_status_code,
          case when pay_state.last_settled_at_utc is not null or f.paid_at_utc is not null
            then 'PAID' else 'UNPAID' end
        )::text as pay_status_code,
        case
          when coalesce(summary_pay_cache.summary_state_applies,false)
            then summary_pay_cache.last_paid_at_utc
          when pay_state.summary_pay_status_code is not null
            or pay_state.summary_pay_icon_code is not null
            then pay_state.summary_pay_paid_at_utc
          else coalesce(pay_state.last_settled_at_utc,f.paid_at_utc)
        end as paid_at_utc
    ) effective_pay
    where t.sheet_scope='DAILY' and t.is_current and t.archived_at_utc is null
      and nullif(btrim(t.booking_id),'') is not null
      and (t.contract_id is null or exists(select 1 from public.contracts owned
        where owned.id=t.contract_id and owned.candidate_id=v_candidate_id))
      and (f.candidate_id=v_candidate_id or (
        f.candidate_id is null and t.candidate_hint_text->>'candidate_id'=v_candidate_id::text
        and ((nullif(btrim(c.key_norm),'') is not null
            and upper(btrim(t.occupant_key_norm))=upper(btrim(c.key_norm))
            and t.idempotency_key like 'candidate-daily-first:%')
          or exists(select 1 from public.candidate_submission_workflows w
            join public.timesheets origin on origin.timesheet_id=w.anchor_timesheet_id
            where w.environment=p_environment and w.candidate_id=v_candidate_id and w.workflow_kind='DAILY'
              and origin.booking_id=t.booking_id and origin.idempotency_key like 'candidate-daily-first:%'
              and origin.candidate_hint_text->>'candidate_id'=v_candidate_id::text
              and upper(btrim(origin.occupant_key_norm))=upper(btrim(t.occupant_key_norm))
              and w.creation_identity_json#>>'{request,daily_source,booking_id}'=t.booking_id))))
  ), selected_daily as materialized (
    select dc.* from daily_candidates dc
    where dc.week_ending_date<=dc.current_week
      and (case when dc.paid_at_utc is null or dc.paid_at_utc>v_snapshot_utc
          or dc.paid_at_utc>=v_snapshot_utc-interval '7 days' then 'CURRENT'
        when dc.week_ending_date between dc.current_week-105 and dc.current_week then 'HISTORY'
        else 'EXCLUDED' end)=v_view
      and (v_cursor_date is null or (dc.week_ending_date,'00000000-0000-0000-0000-000000000000'::uuid,
        extract(isodow from dc.work_date)::integer,dc.timesheet_id)
        <(v_cursor_date,v_cursor_contract_id,v_cursor_additional_seq,v_cursor_id))
    order by dc.week_ending_date desc,extract(isodow from dc.work_date) desc,dc.timesheet_id desc
    limit v_limit+1
  ), daily_details as materialized (
    select sd.*,public.candidate_app_timesheet_detail_v2(
      p_session_id,p_environment,sd.timesheet_id,null,null,v_snapshot_utc) as detail
    from selected_daily sd
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'sort_date',d.week_ending_date,'sort_contract','00000000-0000-0000-0000-000000000000',
    'sort_sequence',extract(isodow from d.work_date)::integer,'sort_id',d.timesheet_id,
    'card',jsonb_build_object(
      'contract_week_id',null,'contract_id',null,'timesheet_id',d.timesheet_id,
      'sheet_scope','DAILY','booking_id',d.detail#>>'{daily_shift,booking_id}','work_date',d.work_date,
      'client_name',coalesce(d.detail#>>'{daily_shift,hospital}',''),
      'job_title',d.detail#>>'{daily_shift,job_title}','band',null,
      'week_ending_date',d.week_ending_date,'week_ending_label',d.detail->>'week_ending_label',
      'week_ending_weekday',btrim(to_char(d.week_ending_date,'FMDay')),'additional_seq',0,
      'tab_bucket',v_view,'effective_current_week_ending_date',d.current_week,
      'contract_week_status',null,'timesheet_status',d.detail#>>'{timesheet,status}',
      'processing_status',d.detail#>>'{lifecycle,processing_status}',
      'paid_at_utc',case when d.paid_at_utc<=v_snapshot_utc then d.paid_at_utc else null end,
      'paid',coalesce(d.paid_at_utc<=v_snapshot_utc,false),
      'authorised',d.detail#>>'{lifecycle,authorised_at_utc}' is not null,
      'candidate_status_code',d.detail->>'candidate_status_code',
      'is_expense_only',coalesce((d.detail->>'is_expense_only')::boolean,false),
      'expense_route_kind',coalesce(d.detail->>'expense_route_kind','UNKNOWN'),
      'display_route_label',d.detail->>'display_route_label',
      'total_hours',d.detail#>'{hours,total_hours}',
      'expenses',jsonb_build_object('expenses_pay_ex_vat',0,'mileage_units',0,'mileage_pay_ex_vat',0,
        'travel_pay_ex_vat',0,'accommodation_pay_ex_vat',0,'other_pay_ex_vat',0),
      'expense_overlay_conflict_code',null,
      'workflows',d.detail->'workflows','rejections',d.detail->'rejections',
      'route_family',d.detail#>>'{capabilities,route_family}','record_role',d.detail#>>'{capabilities,record_role}',
      'manager_approval_state',case coalesce(d.detail#>>'{manager_approval,state}',
          d.detail#>>'{manager_review,manager_approval_state}')
        when 'APPROVED' then 'MANAGER_APPROVED'
        else coalesce(d.detail#>>'{manager_approval,state}',
          d.detail#>>'{manager_review,manager_approval_state}') end,
      'rejection_reason',d.detail#>>'{rejections,0,rejection_reason}',
      'rejection_scope',d.detail#>>'{rejections,0,rejection_scope}',
      'rejection',null,'primary_action',d.detail->'primary_action',
      'detail_target',jsonb_build_object('identity_kind','TIMESHEET','id',d.timesheet_id,
        'path','/candidate-app/v1/timesheets/'||d.timesheet_id::text),
      'actions',jsonb_build_object('can_edit_hours',d.detail#>'{capabilities,can_edit_hours}',
        'can_edit_expenses',false,'candidate_paper_submission_allowed',false,'candidate_no_work_allowed',false,
        'can_reject_candidate_submission',d.detail#>'{capabilities,can_reject_candidate_submission}',
        'reject_scope',d.detail#>'{capabilities,reject_scope}')
    ))),'[]'::jsonb) into v_daily_rows from daily_details d;

  select coalesce(jsonb_agg(item order by (item->>'sort_date')::date desc,
    (item->>'sort_contract')::uuid desc,(item->>'sort_sequence')::integer desc,(item->>'sort_id')::uuid desc),'[]'::jsonb)
  into v_combined from (
    select jsonb_build_object('card',card,'sort_date',card->>'week_ending_date',
      'sort_contract',card->>'contract_id','sort_sequence',card->'additional_seq','sort_id',card->>'contract_week_id') as item
    from jsonb_array_elements(v_rows) card
    union all select item from jsonb_array_elements(v_daily_rows) item
  ) merged;
  select coalesce(jsonb_agg(item->'card' order by ordinal),'[]'::jsonb) into v_rows
  from jsonb_array_elements(v_combined) with ordinality e(item,ordinal) where ordinal<=v_limit;
  v_last:=v_combined->(least(v_limit,jsonb_array_length(v_combined))-1);
  v_next_cursor:=case when v_last is not null and (v_had_more or jsonb_array_length(v_combined)>v_limit) then
    'v2|'||v_view||'|'||to_char(v_snapshot_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      ||'|'||v_candidate_id::text||'|'||(v_last->>'sort_date')||'|'||(v_last->>'sort_contract')
      ||'|'||(v_last->>'sort_sequence')||'|'||(v_last->>'sort_id')
    else null end;

  return jsonb_build_object(
    'ok',true,
    'view',v_view,
    'default_view','CURRENT',
    'snapshot_utc',v_snapshot_utc,
    'paid_current_cutoff_utc',v_snapshot_utc-interval '7 days',
    'items',v_rows,
    'next_cursor',v_next_cursor,
    'cursor_version','v2',
    'readiness_conflicts',coalesce(v_conflicts,'[]'::jsonb),
    'limit',v_limit
  );
end;
$function$;

alter function public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz) owner to postgres;
revoke all on function public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz) to service_role;

notify pgrst, 'reload schema';

commit;
